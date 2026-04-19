import { chromium } from "playwright";
import { google } from "googleapis";

const SHEET_ID = process.env.SHEET_ID;
const SHEET_INPUT_NAME = "Guzergahlar";
const SHEET_OUTPUT_NAME = "Farkli_Fiyatlar";
const NIGDE_ID = 398;
const NIGDE_NAME = "Niğde";

function getTomorrowDateTR() {
  const now = new Date();
  now.setDate(now.getDate() + 1);
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function parseGoogleCredentials() {
  const raw = process.env.GOOGLE_CREDENTIALS_JSON;
  if (!raw) {
    throw new Error("GOOGLE_CREDENTIALS_JSON eksik.");
  }
  return JSON.parse(raw);
}

async function getSheetsClient() {
  const credentials = parseGoogleCredentials();

  const auth = new google.auth.GoogleAuth({
    credentials,
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  return google.sheets({ version: "v4", auth });
}

async function readRoutes(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_INPUT_NAME}!A:C`
  });

  const rows = res.data.values || [];
  if (rows.length <= 1) return [];

  return rows.slice(1).map((row) => ({
    varis: row[0] || "",
    varisId: String(row[1] || "").trim(),
    referansFiyat: Number(row[2] || 0)
  }));
}

async function writeResults(sheets, rows) {
  const values = [
    ["Kalkis", "Varis", "Tarih", "Firma", "ObiletFiyati", "ReferansFiyat", "Fark", "JsonLink"],
    ...rows
  ];

  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_OUTPUT_NAME}!A:Z`
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_OUTPUT_NAME}!A1`,
    valueInputOption: "RAW",
    requestBody: { values }
  });
}

async function writeDebug(sheets, rows) {
  const values = [
    ["Varis", "VarisID", "ReferansFiyat", "JsonLink", "SeferSayisi", "Durum"],
    ...rows
  ];

  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range: `Debug!A:Z`
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `Debug!A1`,
    valueInputOption: "RAW",
    requestBody: { values }
  });
}

async function fetchJourneysForRoute(page, destinationId, dateStr) {
  const pageUrl = `https://www.obilet.com/seferler/${NIGDE_ID}-${destinationId}/${dateStr}`;
  const jsonUrl = `https://www.obilet.com/json/journeys/${NIGDE_ID}-${destinationId}/${dateStr}`;

  await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60000 });

  const data = await page.evaluate(async (url) => {
    const res = await fetch(url, {
      method: "GET",
      headers: {
        "accept": "application/json, text/plain, */*"
      },
      credentials: "include"
    });

    const text = await res.text();
    return {
      ok: res.ok,
      status: res.status,
      text
    };
  }, jsonUrl);

  if (!data.ok) {
    throw new Error(`HTTP ${data.status}`);
  }

  const parsed = JSON.parse(data.text);
  const journeys = Array.isArray(parsed.journeys) ? parsed.journeys : [];

  return {
    jsonUrl,
    journeys
  };
}

function normalizeJourneys(journeys) {
  const seen = new Set();
  const out = [];

  for (const item of journeys) {
    const company = String(item["partner-name"] || "").trim();
    const price = Number(item?.journey?.["internet-price"]);

    if (!company || !Number.isFinite(price)) continue;

    const key = `${company}|${price}`;
    if (seen.has(key)) continue;
    seen.add(key);

    out.push({ company, price });
  }

  return out;
}

async function main() {
  if (!SHEET_ID) {
    throw new Error("SHEET_ID eksik.");
  }

  const sheets = await getSheetsClient();
  const routes = await readRoutes(sheets);
  const tarih = getTomorrowDateTR();

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({
    locale: "tr-TR"
  });

  const resultRows = [];
  const debugRows = [];

  for (const route of routes) {
    const { varis, varisId, referansFiyat } = route;

    if (!varis || !varisId || !Number.isFinite(referansFiyat) || referansFiyat <= 0) {
      debugRows.push([varis, varisId, referansFiyat, "", 0, "Eksik veri"]);
      continue;
    }

    try {
      const { jsonUrl, journeys } = await fetchJourneysForRoute(page, varisId, tarih);
      const normalized = normalizeJourneys(journeys);

      debugRows.push([varis, varisId, referansFiyat, jsonUrl, normalized.length, "OK"]);

      for (const item of normalized) {
        if (item.price !== referansFiyat) {
          resultRows.push([
            NIGDE_NAME,
            varis,
            tarih,
            item.company,
            item.price,
            referansFiyat,
            item.price - referansFiyat,
            jsonUrl
          ]);
        }
      }
    } catch (err) {
      debugRows.push([varis, varisId, referansFiyat, "", 0, String(err.message || err)]);
    }
  }

  await browser.close();

  if (resultRows.length === 0) {
    resultRows.push(["Niğde", "-", tarih, "-", "-", "-", "-", "Farkli fiyat bulunamadi"]);
  }

  await writeResults(sheets, resultRows);
  await writeDebug(sheets, debugRows);

  console.log("Bitti.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});