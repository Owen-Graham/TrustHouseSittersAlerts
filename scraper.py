import asyncio
from playwright.async_api import async_playwright
import csv
import os
import random
import pandas as pd
import re
import argparse
import time
import requests
from datetime import datetime, timedelta

# --- Load secrets ---
if os.environ.get("GITHUB_ACTIONS") != "true":
    from dotenv import load_dotenv
    load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
HEADLESS = True  # Always run headless, including in cron

# Configuration
PET_TYPES = ["dog", "cat", "horse", "bird", "fish", "rabbit", "reptile", "poultry", "livestock", "small_pets"]
EXCLUDED_COUNTRIES = ["United Kingdom", "Ireland"]  # No alerts for these
CONTENT_COLS = ["title", "location", "town", "country", "date_from", "date_to", "reviewing"] + PET_TYPES

df_csv_path = "sits.csv"
df_json_path = "sits.json"

def normalize_pet(pet):
    pet = pet.lower().strip().replace("small pet", "small_pets")
    return pet.replace(" ", "_")

def split_location(location):
    parts = [p.strip() for p in location.rsplit(",", maxsplit=1)]
    return (parts[0], parts[1]) if len(parts) == 2 else (location, "")

def escape_markdown(text):
    if not isinstance(text, str):
        return text
    return re.sub(r'([_\*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

async def wait_like_human(min_sec=0.2, max_sec=0.5):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

async def extract_pets(card):
    counts = {p: 0 for p in PET_TYPES}
    try:
        items = await card.locator('ul[data-testid="animals-list"] li').all()
    except:
        return counts
    for it in items:
        try:
            cnt = await it.locator('span[data-testid="Animal__count"]').text_content(timeout=1000)
            ptype = await it.locator("svg title").text_content(timeout=1000)
            key = normalize_pet(ptype)
            if key in counts:
                counts[key] += int(cnt.strip())
        except:
            continue
    return counts

def format_telegram_message(rows):
    chunks = []
    for i in range(0, len(rows), 4):
        group = rows[i:i+4]
        lines = ["🔔 *New Listings:*", ""]
        for idx, row in enumerate(group, start=i+1):
            pets = ", ".join(f"{row[p]} {p}" for p in PET_TYPES if row.get(p,0))
            lines.append(f"{idx}. *{escape_markdown(row['title'])}*")
            lines.append(f"   📍 {escape_markdown(row['town'])}, {escape_markdown(row['country'])}")
            lines.append(f"   📅 {escape_markdown(row['date_from'])} → {escape_markdown(row['date_to'])}")
            if pets:
                lines.append(f"   🐾 {escape_markdown(pets)}")
            if row.get('reviewing'):
                lines.append(f"   📝 Reviewing applications")
            lines.append(f"   🔗 [View listing]({row['url']})")
            lines.append("")
        chunks.append("\n".join(lines))
    return chunks

def send_telegram_message(chunks):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for part, chunk in enumerate(chunks, start=1):
        print(f"--- Sending part {part}/{len(chunks)} (len={len(chunk)}) ---")
        print(f"Preview: {repr(chunk[:200])}{'…' if len(chunk)>200 else ''}")
        try:
            res = requests.post(url, json={
                'chat_id': TELEGRAM_CHAT_ID,
                'text': chunk,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            })
            if res.status_code != 200:
                print(f"Failed part {part}: {res.text}")
                print(chunk)
        except Exception as e:
            print(f"Telegram exception part {part}: {e}")
            print(chunk)

async def main(test_mode=False):
    start_time = time.time()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
            viewport={'width':1280,'height':720}, locale='en-US'
        )
        await context.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        page = await context.new_page()

        print("Navigating to search page...")
        await page.goto("https://www.trustedhousesitters.com/house-and-pet-sitting-assignments/", wait_until='networkidle')
        await wait_like_human()

        print("Applying filters...")
        # Diagnostic screenshot before filling location
        await page.screenshot(path='before_fill.png')
        box = page.get_by_role('textbox', name='Search for a location')
        await box.wait_for(timeout=10000)
        await box.fill('europe')
        await page.get_by_text('Europe').click(); await wait_like_human()

        await page.get_by_role('button', name='Dates').click()
        for _ in range(6):
            if await page.locator('text=November 2025').is_visible(): break
            await page.get_by_role('button', name='chevron-right').click(); await wait_like_human()
        await page.get_by_label('01 Nov 2025 Saturday').click();
        await page.get_by_role('button', name='chevron-right').click(); await wait_like_human()
        await page.get_by_label('24 Dec 2025 Wednesday').click();
        await page.get_by_role('button', name='Apply').click(); await wait_like_human()

        print("Scraping listings...")
        await page.wait_for_selector('div[data-testid="searchresults_grid_item"]', timeout=30000)
        all_rows = []
        page_number = 1
        while True:
            cards = await page.locator('div[data-testid="searchresults_grid_item"]').all()
            print(f"🧮 Page {page_number}: found {len(cards)} listings")
            for card in (cards if not test_mode else cards[:2]):
                try:
                    title = await card.locator('h3[data-testid="ListingCard__title"]').text_content(timeout=1000)
                    loc = await card.locator('span[data-testid="ListingCard__location"]').text_content(timeout=1000)
                    town, country = split_location(loc)
                    raw = await card.locator('div[class*=\'UnOOR\']>span').first.text_content(timeout=1000)
                    date_from, date_to = (re.split(r"\s*[-–]\s*", raw.replace('+','').strip()) + ['',''])[:2]
                    reviewing = await card.locator('span[data-testid="ListingCard__review__label"]').count()>0
                    url = await card.locator('a').get_attribute('href', timeout=1000)
                    pets = await extract_pets(card)
                    all_rows.append({
                        'url': f"https://www.trustedhousesitters.com{url}",
                        'title': title.strip(), 'location': loc.strip(),
                        'town': town, 'country': country,
                        'date_from': date_from, 'date_to': date_to,
                        'reviewing': reviewing,
                        **pets
                    })
                except Exception as e:
                    print(f"Error parsing card: {e}")
            try:
                next_btn = page.get_by_role('link', name='Go to next page')
                if await next_btn.is_enabled():
                    await next_btn.click(); await wait_like_human(1,2)
                    page_number += 1
                    continue
            except:
                pass
            break
        await browser.close()

    # Early exit if no listings
    if not all_rows:
        print("❌ No listings scraped, exiting.")
        return

    # Load previous data from JSON
    if os.path.exists(df_json_path) and os.path.getsize(df_json_path) > 0:
        try:
            old_df = pd.read_json(df_json_path)
        except (ValueError, pd.errors.EmptyDataError):
            old_df = pd.DataFrame(columns=['url']+CONTENT_COLS+['first_seen','last_changed'])
    else:
        old_df = pd.DataFrame(columns=['url']+CONTENT_COLS+['first_seen','last_changed'])

    new_df = pd.DataFrame(all_rows)
    now = datetime.utcnow().isoformat() + 'Z'

    # Ensure timestamp cols on old_df
    if not old_df.empty:
        default_fs = (datetime.utcnow() - timedelta(seconds=1)).isoformat() + 'Z'
        old_df['first_seen'] = old_df.get('first_seen', default_fs)
        old_df['last_changed'] = old_df.get('last_changed', old_df['first_seen'])

    # Assign timestamps
    if old_df.empty:
        new_df['first_seen'] = now
        new_df['last_changed'] = now
    else:
        merged = old_df.set_index('url').combine_first(new_df.set_index('url'))
        for url, row in merged.iterrows():
            if url in new_df['url'].values:
                nr = new_df.loc[new_df['url']==url].iloc[0]
                orow = old_df.loc[old_df['url']==url].iloc[0] if url in old_df['url'].values else None
                changed = orow is None or any(nr[col] != orow[col] for col in CONTENT_COLS)
                merged.at[url,'last_changed'] = now if changed else row['last_changed']
        merged['first_seen'] = merged['first_seen'].fillna(now)
        new_df = merged.reset_index()

    new_df['new_this_run'] = new_df['first_seen'] == now

    # Mark expired rows
    old_urls = set(old_df['url'])
    new_urls = set(new_df['url'])
    expired_urls = old_urls - new_urls
    expired_rows = old_df[old_df['url'].isin(expired_urls)].copy() if expired_urls else pd.DataFrame()
    if not expired_rows.empty:
        expired_rows['expired'] = True
        expired_rows['new_this_run'] = False

    new_df['expired'] = False
    # Combine present and expired
    output_df = pd.concat([new_df, expired_rows], ignore_index=True)

    # Save data
    output_df.to_csv(df_csv_path, index=False, quoting=csv.QUOTE_NONNUMERIC)
    output_df.to_json(df_json_path, orient='records', indent=2)
    print(f"Saved CSV/JSON, total records: {len(output_df)}")

    # Alerts
    alert_df = new_df[new_df['new_this_run'] & ~new_df['country'].isin(EXCLUDED_COUNTRIES)]
    if not alert_df.empty:
        send_telegram_message(format_telegram_message(alert_df.to_dict('records')))

    print(f"Done in {time.time()-start_time:.2f}s")

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true')
    args = parser.parse_args()
    asyncio.run(main(test_mode=args.test))
