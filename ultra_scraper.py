import asyncio
import json
import re
import time
from playwright.async_api import async_playwright

OUTPUT_FILE = "fast_output.json"
CHUNK_SIZE = 1        # Scrape in batches of N
CONCURRENCY = 1       # Number of concurrent tasks
MAX_URLS = 10          # ✅ Change this to control how many URLs to scrape (like 1, 5, 50, 500...)

EMAIL_REGEX = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
SOCIAL_REGEX = r"https?://(?:www\.)?(facebook|instagram|twitter|linkedin|youtube|tiktok|pinterest|reddit|whatsapp)\.com[^\s\"'>]*"


async def scrape_page(playwright, url):
    browser = await playwright.chromium.launch(headless=True)
    page = await browser.new_page()
    result = {"source_url": url}

    try:
        await page.goto(url, timeout=30000)
        await page.wait_for_timeout(3000)
        content = await page.content()
        text = await page.inner_text("body")

        result.update({
            "name": await page.title(),
            "address": None,
            "phone": None,
            "email": None,
            "website": None,
            "social_links": [],
            "category": None,
            "rating": None,
            "hours": None,
            "description": None
        })

        rating_match = re.search(r"\d\.\d(?=\s+stars?)", text)
        if rating_match:
            result["rating"] = rating_match.group(0)

        email_match = re.search(EMAIL_REGEX, text)
        if email_match:
            result["email"] = email_match.group(0)

        phone_match = re.search(r"\+?\d[\d\s\-()]{7,}", text)
        if phone_match:
            result["phone"] = phone_match.group(0)

        category_match = re.search(r"\b(Gym|Restaurant|Cafe|Dentist|Hotel|Store|Salon|Doctor|School|Clinic|Bank)\b", text)
        if category_match:
            result["category"] = category_match.group(0)

        hours_match = re.findall(r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun).*?(\d{1,2}[:.]?\d{0,2}\s?[APMapm]{2})", text)
        if hours_match:
            result["hours"] = "; ".join([" ".join(x) for x in hours_match])

        address_spans = await page.query_selector_all("span")
        for span in address_spans:
            span_text = await span.inner_text()
            if re.search(r"\d+.*\b(St|Ave|Blvd|Rd|Ln|Street|Road|Avenue)\b", span_text):
                result["address"] = span_text
                break

        anchors = await page.query_selector_all("a")
        for a in anchors:
            href = await a.get_attribute("href")
            if href and "http" in href and "google.com" not in href:
                result["website"] = href
                break

        socials = re.findall(SOCIAL_REGEX, content)
        result["social_links"] = list(set(socials))

        meta_desc = await page.query_selector('meta[name="description"]')
        if meta_desc:
            result["description"] = await meta_desc.get_attribute("content")

    except Exception as e:
        result = {"source_url": url, "error": str(e)}

    await browser.close()
    return result


async def scrape_chunk(playwright, urls):
    sem = asyncio.Semaphore(CONCURRENCY)

    async def limited_scrape(url):
        async with sem:
            return await scrape_page(playwright, url)

    return await asyncio.gather(*(limited_scrape(url) for url in urls))


async def main():
    # ✅ Load URLs from file
    try:
        with open("cleaned_links.json", "r", encoding="utf-8") as f:
            urls = json.load(f)
            urls = urls[:MAX_URLS]  # ✅ Limit how many URLs to scrape here
    except Exception as e:
        print(f"❌ Error loading input file: {e}")
        return

    print(f"🔎 Starting scraping for {len(urls)} URL(s)...")

    results = []
    start = time.time()

    async with async_playwright() as playwright:
        for i in range(0, len(urls), CHUNK_SIZE):
            chunk = urls[i:i + CHUNK_SIZE]
            print(f"🔄 Processing chunk {i // CHUNK_SIZE + 1}...")
            results.extend(await scrape_chunk(playwright, chunk))

    duration = time.time() - start
    print(f"✅ Done in {duration:.2f} seconds")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    print(f"💾 Saved {len(results)} results to {OUTPUT_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
