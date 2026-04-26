/**
 * Xarid recon v2: походить по странице — кликнуть фильтры, листание, etc.
 * Найти все endpoints которые активируются на пользовательских действиях.
 */
import { chromium } from "playwright";
import fs from "node:fs";

const requests = [];

const browser = await chromium.launch({ headless: true });
const ctx = await browser.newContext({
    locale: "ru-RU",
    userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130 Safari/537.36",
});
const page = await ctx.newPage();

page.on("response", async (res) => {
    if (!["xhr", "fetch"].includes(res.request().resourceType())) return;
    const url = res.url();
    if (url.includes("yandex") || url.includes("jivosite") || url.includes("google")) return;
    let body = "";
    try {
        const ct = res.headers()["content-type"] || "";
        if (ct.includes("json") || ct.includes("text/")) {
            body = (await res.text()).slice(0, 400);
        }
    } catch {}
    requests.push({
        method: res.request().method(),
        url, status: res.status(),
        postData: res.request().postData(),
        body,
    });
});

const TARGET = "https://xarid.uzex.uz/completed-deals/shop/national";
console.log("[*] open", TARGET);
await page.goto(TARGET, { waitUntil: "domcontentloaded", timeout: 30000 });

// Список путей для проб
const paths = [
    "/completed-deals/shop/national",
    "/completed-deals/shop/national-shop",
    "/competition-results",
    "/lots/national-shop",
    "/lots/list",
    "/announcements",
];
for (const p of paths) {
    try {
        const url = "https://xarid.uzex.uz" + p;
        console.log(`[*] visit ${p}`);
        await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20000 });
        await page.waitForTimeout(2000);
    } catch (e) {
        console.log(`  err: ${e.message.slice(0, 80)}`);
    }
}

console.log(`\n[*] captured ${requests.length} XHR/Fetch`);
fs.writeFileSync("network_v2.log.json", JSON.stringify(requests, null, 2));

console.log("\n=== Уникальные endpoints ===");
const seen = new Set();
for (const r of requests) {
    const key = `${r.method} ${r.url.split("?")[0]}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const ok = r.status === 200 ? "✓" : "✗";
    console.log(`  ${ok} ${r.method} ${r.url.split("?")[0]} → ${r.status}`);
    if (r.postData) console.log(`      POST: ${r.postData.slice(0, 200)}`);
    if (r.body && r.body.startsWith("[")) {
        try {
            const arr = JSON.parse(r.body + "]".repeat(3));  // best effort
            console.log(`      body: array (~${r.body.length} bytes preview)`);
        } catch {
            console.log(`      body: ${r.body.slice(0, 100)}`);
        }
    }
}

await browser.close();
