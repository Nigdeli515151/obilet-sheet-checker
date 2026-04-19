import { chromium } from "playwright";
import { google } from "googleapis";

const SHEET_ID = process.env.SHEET_ID;
const SHEET_INPUT_NAME = "Guzergahlar";
const SHEET_OUTPUT_NAME = "Farkli_Fiyatlar";
const SHEET_DEBUG_NAME = "Debug";

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const NIGDE_ID = 398;
const NIGDE_NAME = "Niğde";

const CONCURRENCY = 3;
const RESPONSE_TIMEOUT_MS = 15000;
const PAGE_TIMEOUT_MS = 45000;
const RETRY_COUNT = 2;
const SYNC_EVERY_N_UPDATES = 4;

const MIN_DELAY_MS = 12000;
const MAX_DELAY_MS = 22000;

const WORKER_START_STAGGER_MS = 15000;
const CONTEXT_ROTATE_EVERY_JOBS = 3;

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
];

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

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function shuffleArray(arr) {
  const copy = [...arr];
  for (let i = copy.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy;
}

async function randomSleep(page, minMs = MIN_DELAY_MS, maxMs = MAX_DELAY_MS) {
  const waitMs = randInt(minMs, maxMs);
  await page.waitForTimeout(waitMs);
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
    range: `${SHEET_INPUT_NAME}!A:D`
  });

  const rows = res.data.values || [];
  if (rows.length <= 1) return [];

  return rows.slice(1).map((row, index) => {
    const aktifRaw = String(row[3] || "").trim().toLowerCase();

    return {
      index,
      varis: String(row[0] || "").trim(),
      varisId: String(row[1] || "").trim(),
      referansFiyat: Number(row[2] || 0),
      aktif:
        aktifRaw === "true" ||
        aktifRaw === "evet" ||
        aktifRaw === "1" ||
        aktifRaw === "x" ||
        aktifRaw === "aktif"
    };
  });
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

async function createContextAndPage(browser, workerId) {
  const context = await browser.newContext({
    locale: "tr-TR",
    userAgent: USER_AGENTS[workerId % USER_AGENTS.length]
  });

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

  return { context, page };
}

async function rotateWorkerResources(browser, workerState, workerId) {
  if (workerState.page) {
    try { await workerState.page.close(); } catch {}
  }
  if (workerState.context) {
    try { await workerState.context.close(); } catch {}
  }

  const fresh = await createContextAndPage(browser, workerId);
  workerState.context = fresh.context;
  workerState.page = fresh.page;
  workerState.jobsSinceRotate = 0;
}

async function waitForJourneyJson(page, originId, destinationId, dateStr, timeoutMs) {
  const targetPart = `/json/journeys/${originId}-${destinationId}/${dateStr}`;

  const response = await page.waitForResponse(
    (resp) => resp.url().includes(targetPart) && resp.status() === 200,
    { timeout: timeoutMs }
  );

  const json = await response.json();
  const journeys = Array.isArray(json.journeys) ? json.journeys : [];

  return {
    url: response.url(),
    journeys
  };
}

async function pageFetchJson(page, originId, destinationId, dateStr) {
  return await page.evaluate(async ({ originId, destinationId, dateStr }) => {
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
}

async function fetchJourneysRobust(page, context, originId, destinationId, dateStr, logger) {
  const pageUrl = `https://www.obilet.com/seferler/${originId}-${destinationId}/${dateStr}`;

  logger("sayfa aciliyor");
  const waitJsonPromise = waitForJourneyJson(page, originId, destinationId, dateStr, RESPONSE_TIMEOUT_MS);

  await page.goto(pageUrl, {
    waitUntil: "domcontentloaded",
    timeout: PAGE_TIMEOUT_MS
  });

  try {
    const result = await waitJsonPromise;
    return {
      pageUrl,
      jsonUrl: result.url,
      journeys: result.journeys,
      statusText: "OK"
    };
  } catch {
    logger("ilk response bekleme basarisiz, ekstra bekleme");
  }

  await page.waitForTimeout(6000);

  try {
    const lateResult = await waitForJourneyJson(page, originId, destinationId, dateStr, 8000);
    return {
      pageUrl,
      jsonUrl: lateResult.url,
      journeys: lateResult.journeys,
      statusText: "Gec geldi ama alindi"
    };
  } catch {
    logger("gec response da gelmedi, sayfa ici fetch deneniyor");
  }

  const fallback = await pageFetchJson(page, originId, destinationId, dateStr);

  if (fallback.ok) {
    const parsed = JSON.parse(fallback.text);
    return {
      pageUrl,
      jsonUrl: fallback.url,
      journeys: Array.isArray(parsed.journeys) ? parsed.journeys : [],
      statusText: "Sayfa ici fetch ile alindi"
    };
  }

  logger(`sayfa ici fetch basarisiz: HTTP ${fallback.status}, yeni sekme deneniyor`);

  const retryPage = await context.newPage();
  retryPage.setDefaultTimeout(PAGE_TIMEOUT_MS);

  try {
    const retryWaitPromise = waitForJourneyJson(retryPage, originId, destinationId, dateStr, RESPONSE_TIMEOUT_MS);
    await retryPage.goto(pageUrl, {
      waitUntil: "domcontentloaded",
      timeout: PAGE_TIMEOUT_MS
    });

    try {
      const retryResult = await retryWaitPromise;
      return {
        pageUrl,
        jsonUrl: retryResult.url,
        journeys: retryResult.journeys,
        statusText: "Yeniden denemede alindi"
      };
    } catch {
      logger("yeni sekmede response bekleme basarisiz, fetch tekrar deneniyor");
    }

    await retryPage.waitForTimeout(5000);

    const retryFallback = await pageFetchJson(retryPage, originId, destinationId, dateStr);

    if (retryFallback.ok) {
      const retryParsed = JSON.parse(retryFallback.text);
      return {
        pageUrl,
        jsonUrl: retryFallback.url,
        journeys: Array.isArray(retryParsed.journeys) ? retryParsed.journeys : [],
        statusText: "Yeniden deneme sayfa ici fetch ile alindi"
      };
    }

    if (retryFallback.status === 403) {
      throw new Error("Engellendi | HTTP 403");
    }

    if (retryFallback.status === 404) {
      throw new Error("Bulunamadi | HTTP 404");
    }

    throw new Error(`JSON alinamadi | HTTP ${retryFallback.status}`);
  } finally {
    try { await retryPage.close(); } catch {}
  }
}

async function fetchJourneysWithRetry(page, context, originId, destinationId, dateStr, logger) {
  let lastError = null;

  for (let attempt = 1; attempt <= RETRY_COUNT; attempt++) {
    try {
      logger(`deneme ${attempt} basladi`);
      const data = await fetchJourneysRobust(page, context, originId, destinationId, dateStr, logger);
      logger(`deneme ${attempt} basarili`);
      return data;
    } catch (err) {
      lastError = err;
      logger(`deneme ${attempt} hata: ${String(err.message || err)}`);
      await page.waitForTimeout(randInt(5000, 9000));
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

function isRetryableError(message) {
  const s = String(message || "").toLowerCase();
  if (s.includes("404") || s.includes("bulunamadi")) return false;

  return (
    s.includes("403") ||
    s.includes("engellendi") ||
    s.includes("zaman") ||
    s.includes("timeout") ||
    s.includes("json alinamadi") ||
    s.includes("response")
  );
}

function buildJobs(routes) {
  const jobs = [];
  let jobIndex = 0;

  for (const route of routes) {
    if (!route.aktif) {
      continue;
    }

    if (!route.varis || !route.varisId || !Number.isFinite(route.referansFiyat) || route.referansFiyat <= 0) {
      continue;
    }

    jobs.push({
      queueIndex: jobIndex++,
      routeName: route.varis,
      routeId: route.varisId,
      referansFiyat: route.referansFiyat,
      direction: "Gidis",
      originId: NIGDE_ID,
      originName: NIGDE_NAME,
      destinationId: route.varisId,
      destinationName: route.varis,
      pass: 1
    });

    jobs.push({
      queueIndex: jobIndex++,
      routeName: route.varis,
      routeId: route.varisId,
      referansFiyat: route.referansFiyat,
      direction: "Donus",
      originId: route.varisId,
      originName: route.varis,
      destinationId: NIGDE_ID,
      destinationName: NIGDE_NAME,
      pass: 1
    });
  }

  return shuffleArray(jobs);
}

function groupRowsByCompany(rows) {
  const map = new Map();

  for (const row of rows) {
    const [firma, yon, guzergah, tarih, saat, fiyat] = row;

    if (!map.has(firma)) {
      map.set(firma, []);
    }

    map.get(firma).push({
      yon,
      guzergah,
      tarih,
      saat,
      fiyat
    });
  }

  for (const [, items] of map) {
    items.sort((a, b) => {
      const routeCmp = String(a.guzergah).localeCompare(String(b.guzergah), "tr");
      if (routeCmp !== 0) return routeCmp;
      return String(a.saat).localeCompare(String(b.saat));
    });
  }

  return map;
}

function buildTelegramMessages(resultRows, sonKontrol) {
  if (!resultRows.length) {
    return [`Kontrol bitti.\nUygun fiyat bulunamadi.\nSon kontrol: ${sonKontrol}`];
  }

  const grouped = groupRowsByCompany(resultRows);
  const messages = [];

  let current = `Kontrol bitti.\nSon kontrol: ${sonKontrol}\n`;

  for (const [firma, items] of grouped.entries()) {
    let section = `\n${firma}\n`;

    for (const item of items) {
      section += `- ${item.yon} | ${item.guzergah} | ${item.saat} | ${item.fiyat} TL\n`;
    }

    if ((current + section).length > 3500) {
      messages.push(current.trim());
      current = section;
    } else {
      current += section;
    }
  }

  if (current.trim()) {
    messages.push(current.trim());
  }

  return messages;
}

async function sendTelegramMessage(text) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.log("Telegram bilgileri eksik, mesaj gonderilmedi.");
    return;
  }

  const res = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT_ID,
      text
    })
  });

  if (!res.ok) {
    const txt = await res.text();
    throw new Error(`Telegram gonderim hatasi: ${res.status} ${txt}`);
  }
}

async function sendTelegramResultList(resultRows, sonKontrol) {
  const messages = buildTelegramMessages(resultRows, sonKontrol);

  for (const msg of messages) {
    await sendTelegramMessage(msg);
  }
}

async function main() {
  if (!SHEET_ID) throw new Error("SHEET_ID eksik.");

  const sheets = await getSheetsClient();
  const routes = await readRoutes(sheets);
  const tarih = getTomorrowDateTR();
  const sonKontrol = getNowTR();

  const browser = await chromium.launch({ headless: true });

  const workerStates = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    workerStates.push({
      context: null,
      page: null,
      jobsSinceRotate: 0
    });
    await rotateWorkerResources(browser, workerStates[i], i);
  }

  const jobs = buildJobs(routes);

  const debugMap = new Map();
  for (const j of jobs) {
    debugMap.set(j.queueIndex, [
      j.routeName,
      j.routeId,
      j.referansFiyat,
      `${j.direction} | ${j.originName} -> ${j.destinationName}`,
      0,
      "Sirada | Tur 1"
    ]);
  }

  const resultRows = [];
  const failedForSecondPass = [];

  let writeChain = Promise.resolve();
  let pendingSyncCounter = 0;

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

  const queueSync = (force = false) => {
    pendingSyncCounter += 1;

    if (!force && pendingSyncCounter < SYNC_EVERY_N_UPDATES) {
      return writeChain;
    }

    pendingSyncCounter = 0;

    writeChain = writeChain.then(syncSheets).catch((err) => {
      console.error("Sheet sync hatasi:", err);
    });

    return writeChain;
  };

  async function runJob(workerId, workerState, job) {
    const {
      queueIndex,
      routeName,
      routeId,
      referansFiyat,
      direction,
      originId,
      originName,
      destinationId,
      destinationName,
      pass
    } = job;

    const logPrefix = `[worker ${workerId + 1}] tur ${pass} ${direction} ${originName} -> ${destinationName}`;

    const setDebug = async (status, seferSayisi = 0) => {
      debugMap.set(queueIndex, [
        routeName,
        routeId,
        referansFiyat,
        `${direction} | ${originName} -> ${destinationName}`,
        seferSayisi,
        `${status} | Tur ${pass}`
      ]);
      console.log(`${logPrefix} -> ${status}`);
      await queueSync(false);
    };

    if (workerState.jobsSinceRotate >= CONTEXT_ROTATE_EVERY_JOBS) {
      await setDebug("Context yenileniyor", 0);
      await rotateWorkerResources(browser, workerState, workerId);
      await workerState.page.waitForTimeout(randInt(5000, 9000));
    }

    try {
      await setDebug("Basladi", 0);

      const logger = (msg) => console.log(`${logPrefix} -> ${msg}`);
      const { jsonUrl, journeys, statusText } = await fetchJourneysWithRetry(
        workerState.page,
        workerState.context,
        originId,
        destinationId,
        tarih,
        logger
      );

      const normalized = normalizeJourneys(journeys);
      await setDebug(`${statusText} | ${jsonUrl}`, normalized.length);

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
      await queueSync(false);
    } catch (err) {
      const message = String(err.message || err);
      await setDebug(`Hata | ${message}`, 0);

      if (pass === 1 && isRetryableError(message)) {
        failedForSecondPass.push({ ...job, pass: 2 });
      }
    }

    workerState.jobsSinceRotate += 1;
    await randomSleep(workerState.page);
  }

  async function runPass(passJobs) {
    let cursor = 0;

    async function worker(workerId) {
      const workerState = workerStates[workerId];

      if (workerId > 0) {
        await workerState.page.waitForTimeout(WORKER_START_STAGGER_MS * workerId);
      }

      while (true) {
        const currentIndex = cursor;
        cursor += 1;

        if (currentIndex >= passJobs.length) return;

        const job = passJobs[currentIndex];
        await runJob(workerId, workerState, job);
      }
    }

    await Promise.all(
      Array.from({ length: CONCURRENCY }, (_, i) => worker(i))
    );
  }

  await queueSync(true);

  await runPass(jobs);

  if (failedForSecondPass.length > 0) {
    const secondPassJobs = shuffleArray(failedForSecondPass);

    for (let i = 0; i < workerStates.length; i++) {
      await rotateWorkerResources(browser, workerStates[i], i);
      await workerStates[i].page.waitForTimeout(randInt(7000, 12000));
    }

    await runPass(secondPassJobs);
  }

  await queueSync(true);
  await writeChain;

  for (const workerState of workerStates) {
    try { await workerState.page.close(); } catch {}
    try { await workerState.context.close(); } catch {}
  }

  await browser.close();

  await sendTelegramResultList(resultRows, sonKontrol);

  console.log("Tamamlandi.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
