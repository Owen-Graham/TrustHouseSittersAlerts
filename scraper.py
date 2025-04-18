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

# --- CONFIG ---
EMAIL = os.environ["THS_EMAIL"]
PASSWORD = os.environ["THS_PASSWORD"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]

PET_TYPES = ["dog", "cat", "horse", "bird", "fish", "rabbit", "reptile", "poultry", "livestock", "small_pets"]

def normalize_pet(pet):
    pet = pet.lower().strip()
    pet = pet.replace("small pet", "small_pets")
    return pet.replace(" ", "_")

def split_location(location):
    parts = [p.strip() for p in location.rsplit(",", maxsplit=1)]
    if len(parts) == 2:
        return parts[0], parts[1]
    return location, ""

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
            count_text = await item.locator('span[data-testid="Animal__count"]').text_content(timeout=1000)
            pet_type = await item.locator("svg title").text_content(timeout=1000)
            pet = normalize_pet(pet_type)
            if pet in counts:
                counts[pet] += int(count_text.strip())
        except:
            continue
    return counts

def format_telegram_message(new_rows):
    lines = ["🔔 *New TrustedHousesitters Listings:*", ""]
    for i, row in enumerate(new_rows, 1):
        pets = ", ".join([f"{row[p]} {p}" for p in PET_TYPES if row.get(p, 0)])
        lines.append(f"{i}. *{row['title']}*")
        lines.append(f"   📍 {row['town']}, {row['country']}")
        lines.append(f"   📅 {row['date_from']} → {row['date_to']}")
        if pets:
            lines.append(f"   🐾 Pets: {pets}")
        if row['reviewing']:
            lines.append(f"   📝 Reviewing applications")
        lines.append(f"   🔗 [View listing]({row['url']})")
        lines.append("")
    return "\n".join(lines)

def send_telegram_message(text, chunk_size=4000):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
    for idx, chunk in enumerate(chunks):
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
            "parse_mode": "Markdown",
            "disable_web_page_preview": False,
        }
        r = requests.post(url, json=payload)
        if r.status_code == 200:
            print(f"[{time.strftime('%X')}] 📬 Sent part {idx+1}/{len(chunks)}")
        else:
            print(f"[{time.strftime('%X')}] ❌ Failed part {idx+1}: {r.text}")

async def main(test_mode=False):
    global_start = time.time()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        print(f"[{time.strftime('%X')}] 🚀 Logging in...")
        await page.goto("https://www.trustedhousesitters.com/")
        await page.get_by_role("link", name="Log in").click()
        await page.get_by_role("textbox", name="Email").fill(EMAIL)
        await page.get_by_role("textbox", name="Password").fill(PASSWORD)
        await page.get_by_role("button", name="Log in").click()

        try:
            await page.wait_for_selector("input[placeholder*='Where would you like to go?']", timeout=15000)
        except:
            print(f"[{time.strftime('%X')}] ⚠️ Dashboard didn't load, forcing page manually.")
            await page.goto("https://www.trustedhousesitters.com/house-and-pet-sitting-assignments/")
            await page.wait_for_selector("input[placeholder*='Where would you like to go?']", timeout=10000)

        print(f"[{time.strftime('%X')}] 🎯 Applying filters...")
        await page.get_by_role("textbox", name="Search for a location").click()
        await page.get_by_role("textbox", name="Search for a location").fill("europe")
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

        print(f"[{time.strftime('%X')}] 🕵️ Starting scrape...")
        all_rows = []
        page_number = 0

        while True:
            page_number += 1
            print(f"[{time.strftime('%X')}] 📄 Page {page_number} loading...")
            cards = await page.locator('div[data-testid="searchresults_grid_item"]').all()
            print(f"[{time.strftime('%X')}] 🔍 Found {len(cards)} listings.")

            if not cards:
                break

            num_cards = len(cards) if not test_mode else min(2, len(cards))
            for i in range(num_cards):
                t0 = time.time()
                try:
                    card = cards[i]
                    title = await card.locator('h3[data-testid="ListingCard__title"]').text_content(timeout=1000)
                    location = await card.locator('span[data-testid="ListingCard__location"]').text_content(timeout=1000)
                    town, country = split_location(location)

                    try:
                        raw_date = await card.locator("div[class*='UnOOR'] > span").first.text_content(timeout=1000)
                    except:
                        raw_date = ""

                    raw_date = raw_date.replace("+", "").strip()
                    split_date = re.split(r"\s*[-–]\s*", raw_date)
                    date_from, date_to = (split_date + [""])[:2]

                    reviewing = await card.locator('span[data-testid="ListingCard__review__label"]').count() > 0
                    pets = await extract_pets(card)

                    try:
                        url_rel = await card.locator("a").get_attribute("href", timeout=1000)
                        url = "https://www.trustedhousesitters.com" + url_rel if url_rel else None
                    except:
                        url = None

                    row = {
                        "url": url,
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
                    }
                    all_rows.append(row)
                    print(f"[{time.strftime('%X')}] ✅ Parsed card {i+1}/{num_cards} in {time.time() - t0:.2f}s")
                except Exception as e:
                    print(f"[{time.strftime('%X')}] ⚠️ Error parsing listing {i}: {e}")

            if test_mode:
                print(f"[{time.strftime('%X')}] 🧪 Test mode active. Stopping after first page.")
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

        if os.path.exists(csv_file):
            old_df = pd.read_csv(csv_file)
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

        merged_df = merged_df.sort_values(by="date_from")
        merged_df.to_csv(csv_file, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        merged_df.to_json(json_file, orient="records", indent=2)

        print(f"[{time.strftime('%X')}] ✅ Saved CSV to {csv_file}")
        print(f"[{time.strftime('%X')}] ✅ Saved JSON to {json_file}")

        new_sits = merged_df[merged_df["new_this_run"] == True]
        if not new_sits.empty:
            msg = format_telegram_message(new_sits.to_dict(orient="records"))
            send_telegram_message(msg)

        print(f"[{time.strftime('%X')}] 🏁 Done in {time.time() - global_start:.2f}s")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", type=lambda x: x.lower() != "false", default=False, nargs="?", const=True)
    args = parser.parse_args()
    asyncio.run(main(test_mode=args.test))
