import asyncio
from playwright.async_api import async_playwright
import csv
import random
import os
import pandas as pd
import json
import re
import argparse
import time
import requests

# --- Load secrets ---
if os.environ.get("GITHUB_ACTIONS") != "true":
    from dotenv import load_dotenv
    load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
HEADLESS = True  # Always run headless, including in cron

PET_TYPES = ["dog", "cat", "horse", "bird", "fish", "rabbit", "reptile", "poultry", "livestock", "small_pets"]

def normalize_pet(pet):
    pet = pet.lower().strip().replace("small pet", "small_pets")
    return pet.replace(" ", "_")

def split_location(location):
    parts = [p.strip() for p in location.rsplit(",", maxsplit=1)]
    return (parts[0], parts[1]) if len(parts) == 2 else (location, "")

def escape_markdown(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

async def wait_like_human(min_sec=0.2, max_sec=0.5):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def extract_pets(card):
    counts = {p: 0 for p in PET_TYPES}
    try:
        pet_items = await card.locator('ul[data-testid="animals-list"] li').all()
    except:
        return counts
    for item in pet_items:
        try:
            count = await item.locator('span[data-testid="Animal__count"]').text_content(timeout=1000)
            pet_type = await item.locator("svg title").text_content(timeout=1000)
            pet = normalize_pet(pet_type)
            if pet in counts:
                counts[pet] += int(count.strip())
        except:
            continue
    return counts

def format_telegram_message(rows):
    chunks = []
    for i in range(0, len(rows), 4):
        group = rows[i:i + 4]
        lines = ["🔔 *New TrustedHousesitters Listings:*", ""]
        for j, row in enumerate(group, i + 1):
            pets = ", ".join([f"{row[p]} {p}" for p in PET_TYPES if row.get(p, 0)])
            lines.append(f"{j}. *{escape_markdown(row['title'])}*")
            lines.append(f"   📍 {escape_markdown(row['town'])}, {escape_markdown(row['country'])}")
            lines.append(f"   📅 {escape_markdown(row['date_from'])} → {escape_markdown(row['date_to'])}")
            if pets:
                lines.append(f"   🐾 Pets: {escape_markdown(pets)}")
            if row['reviewing']:
                lines.append(f"   📝 Reviewing applications")
            lines.append(f"   🔗 [View listing]({escape_markdown(row['url'])})")
            lines.append("")
        chunks.append("\n".join(lines))
    return chunks

def send_telegram_message(text_chunks):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for idx, chunk in enumerate(text_chunks, start=1):
        print(f"\n--- Sending chunk {idx}/{len(text_chunks)} (len={len(chunk)}) ---")
        print(f"Preview: {repr(chunk[:200])}{'…' if len(chunk) > 200 else ''}")
        try:
            r = requests.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": chunk,
                "parse_mode": "Markdown",
                "disable_web_page_preview": False,
            })
            if r.status_code == 200:
                print(f"[{time.strftime('%X')}] 📬 Sent part {idx}/{len(text_chunks)}")
            else:
                print(f"[{time.strftime('%X')}] ❌ Failed part {idx}: {r.text}")
                print("Full chunk content causing error:")
                print(chunk)
        except Exception as e:
            print(f"[{time.strftime('%X')}] ⚠️ Telegram send exception: {e}")
            print("Full chunk content:")
            print(chunk)

async def main(test_mode=False):
    global_start = time.time()
    try:
        async with async_playwright() as p:
            # Choose browser engine
            # browser = await p.firefox.launch(headless=HEADLESS)  # uncomment to use Firefox
            browser = await p.chromium.launch(headless=HEADLESS)

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 720},
                locale="en-US"
            )

            # Stealth: prevent detection via navigator.webdriver
            await context.add_init_script("""Object.defineProperty(navigator, 'webdriver', { get: () => undefined })""")

            page = await context.new_page()

            print(f"[{time.strftime('%X')}] 🕵️ Navigating to search page...")
            await page.goto("https://www.trustedhousesitters.com/house-and-pet-sitting-assignments/", wait_until="networkidle")
            await wait_like_human(1, 2)

            print(f"[{time.strftime('%X')}] 🎯 Applying filters...")
            await page.screenshot(path="before_fill.png")
            textbox = page.get_by_role("textbox", name="Search for a location")
            await textbox.wait_for(timeout=10000)
            await textbox.fill("europe")

            await page.get_by_text("Europe").click()
            await wait_like_human()

            await page.get_by_role("button", name="Dates").click()
            for _ in range(6):
                if await page.locator("text=November 2025").is_visible():
                    break
                await page.get_by_role("button", name="chevron-right").click()
                await wait_like_human()
            await page.get_by_label("01 Nov 2025 Saturday").click()
            await page.get_by_role("button", name="chevron-right").click()
            await wait_like_human()
            await page.get_by_label("24 Dec 2025 Wednesday").click()
            await wait_like_human()
            await page.get_by_role("button", name="Apply").click()
            await wait_like_human(1, 2)

            print(f"[{time.strftime('%X')}] 🕵️ Scraping listings...")
            all_rows, page_number = [], 0

            while True:
                page_number += 1
                print(f"[{time.strftime('%X')}] 📄 Page {page_number} loading...")
                cards = await page.locator('div[data-testid="searchresults_grid_item"]').all()
                print(f"[{time.strftime('%X')}] 🔍 Found {len(cards)} listings.")
                if not cards:
                    break

                for i, card in enumerate(cards if not test_mode else cards[:2]):
                    t0 = time.time()
                    try:
                        title = await card.locator('h3[data-testid="ListingCard__title"]').text_content(timeout=1000)
                        location = await card.locator('span[data-testid="ListingCard__location"]').text_content(timeout=1000)
                        town, country = split_location(location)
                        try:
                            raw_date = await card.locator("div[class*='UnOOR'] > span").first.text_content(timeout=1000)
                        except:
                            raw_date = ""
                        date_from, date_to = (re.split(r"\s*[-–]\s*", raw_date.replace("+", "").strip()) + [""])[:2]
                        reviewing = await card.locator('span[data-testid="ListingCard__review__label"]').count() > 0
                        url_rel = await card.locator("a").get_attribute("href", timeout=1000)
                        pets = await extract_pets(card)
                        all_rows.append({
                            "url": "https://www.trustedhousesitters.com" + url_rel if url_rel else None,
                            "title": title.strip(),
                            "location": location.strip(),
                            "town": town,
                            "country": country,
                            "date_from": date_from,
                            "date_to": date_to,
                            "reviewing": reviewing,
                            "expired": False,
                            "new_this_run": False,
                            **pets
                        })
                        print(f"[{time.strftime('%X')}] ✅ Parsed card {i+1}/{len(cards)} in {time.time() - t0:.2f}s")
                    except Exception as e:
                        print(f"[{time.strftime('%X')}] ⚠️ Failed to parse card {i+1}: {e}")

                if test_mode:
                    print(f"[{time.strftime('%X')}] 🧪 Test mode: exiting after page 1.")
                    break

                try:
                    next_btn = page.get_by_role("link", name="Go to next page")
                    if await next_btn.is_enabled():
                        await next_btn.click()
                        await wait_like_human(1, 2)
                        await page.wait_for_selector('div[data-testid="searchresults_grid_item"]')
                    else:
                        break
                except:
                    break

            await browser.close()

            if not all_rows:
                print(f"[{time.strftime('%X')}] ❌ No listings scraped.")
                return

            csv_file = "sits.csv"
            json_file = "sits.json"

            if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
                old_df = pd.read_csv(csv_file)
                print(f"[{time.strftime('%X')}] 📁 Loaded {len(old_df)} previous listings.")
            else:
                old_df = pd.DataFrame()

            new_df = pd.DataFrame(all_rows)
            new_df["expired"] = False

            if not old_df.empty:
                old_urls = set(old_df["url"].dropna())
                new_df["new_this_run"] = ~new_df["url"].isin(old_urls)
                old_df["expired"] = True
                merged_df = pd.concat([
                    new_df,
                    old_df[~old_df["url"].isin(new_df["url"])]
                ], ignore_index=True)
            else:
                new_df["new_this_run"] = True
                merged_df = new_df

            for col in ["url", "title", "location", "town", "country", "date_from", "date_to", "reviewing", "expired", "new_this_run", *PET_TYPES]:
                if col not in merged_df:
                    merged_df[col] = 0 if col in PET_TYPES else ""

            merged_df.sort_values("date_from").to_csv(csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
            merged_df.to_json(json_file, orient="records", indent=2)
            print(f"[{time.strftime('%X')}] ✅ Saved CSV and JSON")

            new_sits = merged_df[merged_df["new_this_run"] == True]
            if not new_sits.empty:
                chunks = format_telegram_message(new_sits.to_dict(orient="records"))
                send_telegram_message(chunks)

            print(f"[{time.strftime('%X')}] 🏁 Done in {time.time() - global_start:.2f}s")

    except Exception as e:
        print(f"[{time.strftime('%X')}] ❌ Unhandled exception: {e}")
        try:
            if 'page' in locals() and page:
                html = await page.content()
                with open("crash_dump.html", "w", encoding="utf-8") as f:
                    f.write(html)
                print(f"[{time.strftime('%X')}] 💾 Saved crash HTML to crash_dump.html")
            else:
                print(f"[{time.strftime('%X')}] ⚠️ Page not available to dump HTML")
        except Exception as dump_err:
            print(f"[{time.strftime('%X')}] ⚠️ Failed to save crash HTML: {dump_err}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", type=lambda x: x.lower() != "false", default=False, nargs="?", const=True)
    args = parser.parse_args()
    asyncio.run(main(test_mode=args.test))
