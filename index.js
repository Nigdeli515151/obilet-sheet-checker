import { chromium } from "playwright";
import { google } from "googleapis";

const SHEET_ID = process.env.SHEET_ID;
const SHEET_INPUT_NAME = "Guzergahlar";
const SHEET_OUTPUT_NAME = "Farkli_Fiyatlar";
const SHEET_DEBUG_NAME = "Debug";

const NIGDE_ID = 398;
const NIGDE_NAME = "Niğde";

const CONCURRENCY = 4;
const RESPONSE_TIMEOUT_MS = 15000;
const PAGE_TIMEOUT_MS = 45000;
const RETRY_COUNT = 2;

function getTomorrowDateTR() {
  const now = new Date();
  now.setDate(now.getDate() + 1);
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function getNowTR() {
  return new Date().toLocaleString("tr-TR", {
    timeZone: "Europe/Istanbul"
  });
}

function parseGoogleCredentials() {
  const raw = process.env.GOOGLE_CREDENTIALS_JSON;
  if (!raw) throw new Error("GOOGLE_CREDENTIALS_JSON eksik.");
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

  return rows.slice(1).map((row, index) => ({
    index,
    varis: String(row[0] || "").trim(),
    varisId: String(row[1] || "").trim(),
    referansFiyat: Number(row[2] || 0)
  }));
}

function formatHour(value) {
  if (!value) return "";
  const s = String(value);
  const m = s.match(/T(\d{2}):(\d{2})/);
  if (m) return `${m[1]}:${m[2]}`;
  return s;
}

function normalizeJourneys(journeys) {
  const seen = new Set();
  const out = [];

  for (const item of journeys) {
    const company = String(item["partner-name"] || "").trim();
    const price = Number(item?.journey?.["internet-price"]);
    const departureRaw = item?.journey?.departure || "";
    const hour = formatHour(departureRaw);

    if (!company || !Number.isFinite(price) || !hour) continue;

    const key = `${company}|${hour}|${price}`;
    if (seen.has(key)) continue;
    seen.add(key);

    out.push({
      company,
      price,
      departureRaw,
      hour
    });
  }

  out.sort((a, b) => a.hour.localeCompare(b.hour));
  return out;
}

async function clearAndWriteRange(sheets, range, values) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId: SHEET_ID,
    range
  });

  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: range.split("!")[0] + "!A1",
    valueInputOption: "RAW",
    requestBody: { values }
  });
}

async function createPage(context) {
  const page = await context.newPage();
  page.setDefaultTimeout(PAGE_TIMEOUT_MS);

  await page.route("**/*", async (route) => {
    const type = route.request().resourceType();
    if (type === "image" || type === "font" || type === "media") {
      await route.abort();
      return;
    }
    await route.continue();
  });

  return page;
}

async function waitForJourneyJson(page, originId, destinationId, dateStr) {
  const targetPart = `/json/journeys/${originId}-${destinationId}/${dateStr}`;

  const response = await page.waitForResponse(
    (resp) => resp.url().includes(targetPart) && resp.status() === 200,
    { timeout: RESPONSE_TIMEOUT_MS }
  );

  const json = await response.json();
  const journeys = Array.isArray(json.journeys) ? json.journeys : [];

  return {
    url: response.url(),
    journeys
  };
}

async function fetchJourneysWithOnePage(page, originId, destinationId, dateStr) {
  const pageUrl = `https://www.obilet.com/seferler/${originId}-${destinationId}/${dateStr}`;

  const waitJsonPromise = waitForJourneyJson(page, originId, destinationId, dateStr);

  await page.goto(pageUrl, {
    waitUntil: "domcontentloaded",
    timeout: PAGE_TIMEOUT_MS
  });

  let result;
  try {
    result = await waitJsonPromise;
  } catch (firstErr) {
    const fallback = await page.evaluate(async ({ originId, destinationId, dateStr }) => {
      const url = `/json/journeys/${originId}-${destinationId}/${dateStr}`;
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
        url: res.url,
        text
      };
    }, { originId, destinationId, dateStr });

    if (!fallback.ok) {
      throw new Error(`JSON yakalanamadi. Fallback HTTP ${fallback.status}`);
    }

    const parsed = JSON.parse(fallback.text);
    result = {
      url: fallback.url,
      journeys: Array.isArray(parsed.journeys) ? parsed.journeys : []
    };
  }

  return {
    pageUrl,
    jsonUrl: result.url,
    journeys: result.journeys
  };
}

async function fetchJourneysWithRetry(page, originId, destinationId, dateStr, logger) {
  let lastError = null;

  for (let attempt = 1; attempt <= RETRY_COUNT; attempt++) {
    try {
      logger(`deneme ${attempt} basladi`);
      const data = await fetchJourneysWithOnePage(page, originId, destinationId, dateStr);
      logger(`deneme ${attempt} basarili`);
      return data;
    } catch (err) {
      lastError = err;
      logger(`deneme ${attempt} hata: ${String(err.message || err)}`);
      await page.waitForTimeout(1000 * attempt);
    }
  }

  throw lastError || new Error("Bilinmeyen hata");
}

function sortResultRows(rows) {
  rows.sort((a, b) => {
    const firma = String(a[0]).localeCompare(String(b[0]), "tr");
    if (firma !== 0) return firma;

    const yon = String(a[1]).localeCompare(String(b[1]), "tr");
    if (yon !== 0) return yon;

    const guzergah = String(a[2]).localeCompare(String(b[2]), "tr");
    if (guzergah !== 0) return guzergah;

    return String(a[4]).localeCompare(String(b[4]));
  });
}

async function main() {
  if (!SHEET_ID) throw new Error("SHEET_ID eksik.");

  const sheets = await getSheetsClient();
  const routes = await readRoutes(sheets);
  const tarih = getTomorrowDateTR();
  const sonKontrol = getNowTR();

  const browser = await chromium.launch({ headless: true });

  const contexts = [];
  const pages = [];

  for (let i = 0; i < CONCURRENCY; i++) {
    const context = await browser.newContext({
      locale: "tr-TR",
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    });
    contexts.push(context);
    pages.push(await createPage(context));
  }

  const jobs = [];
  let jobIndex = 0;

  for (const route of routes) {
    if (!route.varis || !route.varisId || !Number.isFinite(route.referansFiyat) || route.referansFiyat <= 0) {
      continue;
    }

    jobs.push({
      routeIndex: jobIndex++,
      routeName: route.varis,
      routeId: route.varisId,
      referansFiyat: route.referansFiyat,
      direction: "Gidis",
      originId: NIGDE_ID,
      originName: NIGDE_NAME,
      destinationId: route.varisId,
      destinationName: route.varis
    });

    jobs.push({
      routeIndex: jobIndex++,
      routeName: route.varis,
      routeId: route.varisId,
      referansFiyat: route.referansFiyat,
      direction: "Donus",
      originId: route.varisId,
      originName: route.varis,
      destinationId: NIGDE_ID,
      destinationName: NIGDE_NAME
    });
  }

  const debugMap = new Map();
  for (let i = 0; i < jobs.length; i++) {
    const j = jobs[i];
    debugMap.set(i, [
      j.routeName,
      j.routeId,
      j.referansFiyat,
      `${j.direction} | ${j.originName} -> ${j.destinationName}`,
      0,
      "Sirada"
    ]);
  }

  const resultRows = [];
  let writeChain = Promise.resolve();

  const syncSheets = async () => {
    const debugRows = Array.from(debugMap.entries())
      .sort((a, b) => a[0] - b[0])
      .map(([, row]) => row);

    const debugValues = [
      ["Varis", "VarisID", "ReferansFiyat", "YonGuzergah", "SeferSayisi", "Durum"],
      ...debugRows
    ];

    const resultValues = [
      ["Firma", "Yon", "Guzergah", "Tarih", "Saat", "Fiyat", "SonKontrol"],
      ...(resultRows.length
        ? resultRows
        : [["-", "-", "-", tarih, "-", "-", sonKontrol]])
    ];

    await clearAndWriteRange(sheets, `${SHEET_DEBUG_NAME}!A:Z`, debugValues);
    await clearAndWriteRange(sheets, `${SHEET_OUTPUT_NAME}!A:Z`, resultValues);
  };

  const queueSync = () => {
    writeChain = writeChain.then(syncSheets).catch((err) => {
      console.error("Sheet sync hatasi:", err);
    });
    return writeChain;
  };

  await queueSync();

  let cursor = 0;

  async function worker(workerId) {
    const page = pages[workerId];

    while (true) {
      const currentIndex = cursor;
      cursor += 1;

      if (currentIndex >= jobs.length) return;

      const job = jobs[currentIndex];
      const {
        routeName,
        routeId,
        referansFiyat,
        direction,
        originId,
        originName,
        destinationId,
        destinationName
      } = job;

      const pageUrl = `https://www.obilet.com/seferler/${originId}-${destinationId}/${tarih}`;
      const logPrefix = `[worker ${workerId + 1}] ${direction} ${originName} -> ${destinationName}`;

      const setDebug = async (status, seferSayisi = 0) => {
        debugMap.set(currentIndex, [
          routeName,
          routeId,
          referansFiyat,
          `${direction} | ${originName} -> ${destinationName}`,
          seferSayisi,
          status
        ]);
        console.log(`${logPrefix} -> ${status}`);
        await queueSync();
      };

      try {
        await setDebug("Basladi", 0);

        const logger = (msg) => console.log(`${logPrefix} -> ${msg}`);
        const { jsonUrl, journeys } = await fetchJourneysWithRetry(
          page,
          originId,
          destinationId,
          tarih,
          logger
        );

        const normalized = normalizeJourneys(journeys);
        await setDebug(`OK | ${jsonUrl}`, normalized.length);

        for (const item of normalized) {
          if (item.price < referansFiyat) {
            resultRows.push([
              item.company,
              direction,
              `${originName} -> ${destinationName}`,
              tarih,
              item.hour,
              item.price,
              sonKontrol
            ]);
          }
        }

        sortResultRows(resultRows);
        await queueSync();
      } catch (err) {
        await setDebug(`Hata | ${String(err.message || err)}`, 0);
      }

      await page.waitForTimeout(500);
    }
  }

  await Promise.all(
    Array.from({ length: CONCURRENCY }, (_, i) => worker(i))
  );

  await writeChain;

  for (const page of pages) await page.close();
  for (const context of contexts) await context.close();
  await browser.close();

  console.log("Tamamlandi.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
