/**
 * Xarid.uzex.uz API recon: открываем страницу /completed-deals/shop/national,
 * перехватываем все XHR/Fetch запросы, выводим URL + статус + кусочек тела.
 *
 * Запуск:  node recon.mjs
 */
import { chromium } from "playwright";
import fs from "node:fs";

const TARGET = "https://xarid.uzex.uz/completed-deals/shop/national";
const OUT = new URL("./network.log.json", import.meta.url).pathname.replace(/^\//, "");

const requests = [];

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({ locale: "ru-RU" });
const page = await ctx.newPage();

page.on("request", (req) => {
    if (!["xhr", "fetch"].includes(req.resourceType())) return;
    requests.push({
        ts: Date.now(), method: req.method(), url: req.url(),
        headers: req.headers(), postData: req.postData(),
    });
});

page.on("response", async (res) => {
    if (!["xhr", "fetch"].includes(res.request().resourceType())) return;
    const url = res.url();
    const status = res.status();
    const ct = res.headers()["content-type"] || "";
    let bodyPreview = "";
    try {
        if (ct.includes("application/json") || ct.includes("text/")) {
            const txt = await res.text();
            bodyPreview = txt.slice(0, 800);
        } else {
            bodyPreview = `[binary ${ct}]`;
        }
    } catch (e) {
        bodyPreview = `[err: ${e.message}]`;
    }
    const entry = requests.find(r => r.url === url && !r.status);
    if (entry) {
        entry.status = status;
        entry.contentType = ct;
        entry.body = bodyPreview;
    }
});

console.log("[*] open", TARGET);
await page.goto(TARGET, { waitUntil: "domcontentloaded", timeout: 30_000 });
console.log("[*] wait for XHRs to settle...");
await page.waitForTimeout(8_000);

// Try to interact (paginate, wait extra)
try {
    await page.keyboard.press("End");
    await page.waitForTimeout(3_000);
} catch {}

console.log(`[*] captured ${requests.length} XHR/Fetch requests`);
fs.writeFileSync(OUT, JSON.stringify(requests, null, 2));
console.log(`[*] written: ${OUT}`);

// Quick summary
console.log("\n=== unique endpoints ===");
const uniq = [...new Set(requests.map(r => `${r.method} ${r.url.replace(/\?.*$/, "")}`))].sort();
uniq.forEach(u => console.log("  " + u));

console.log("\n=== JSON responses preview ===");
const jsons = requests.filter(r => r.contentType?.includes("json")).slice(0, 6);
jsons.forEach(r => {
    console.log(`\n${r.method} ${r.url} → ${r.status}`);
    console.log("  body:", (r.body || "").slice(0, 300).replace(/\n/g, " "));
});

await browser.close();
