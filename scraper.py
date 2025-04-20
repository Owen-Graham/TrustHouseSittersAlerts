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
import logging
from datetime import datetime, timedelta

# --- Setup logging ---
LOG_PATH = "scraper.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)

# --- Load environment variables ---
if os.environ.get("GITHUB_ACTIONS") != "true":
    from dotenv import load_dotenv
    load_dotenv()

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
HEADLESS = True  # Set to True in production if desired

# --- Configuration ---
BASE_URL = "https://www.trustedhousesitters.com/house-and-pet-sitting-assignments/"
PET_TYPES = ["dog", "cat", "horse", "bird", "fish", "rabbit", "reptile", "poultry", "livestock", "small_pets"]
EXCLUDED_COUNTRIES = ["United Kingdom", "Ireland"]
CONTENT_COLS = ["title", "location", "town", "country", "date_from", "date_to", "reviewing"] + PET_TYPES
MODES = ['public_transport', 'car_included', None]
CSV_PATH = "sits.csv"
JSON_PATH = "sits.json"

# --- Utility functions ---
async def wait_like_human(min_sec=0.2, max_sec=0.5):
    await asyncio.sleep(random.uniform(min_sec, max_sec))

def normalize_pet(pet: str) -> str:
    pet = pet.lower().strip().replace("small pet", "small_pets")
    return pet.replace(" ", "_")

def split_location(location: str) -> tuple[str,str]:
    parts = [p.strip() for p in location.rsplit(",", maxsplit=1)]
    return (parts[0], parts[1]) if len(parts) == 2 else (location, "")

def escape_markdown(text: str) -> str:
    if not isinstance(text, str): return text
    return re.sub(r'([_\*\[\]()~`>#+\-=|{}.!])', r'\\\1', text)

async def extract_pets(card) -> dict:
    counts = {p: 0 for p in PET_TYPES}
    try:
        items = await card.locator('ul[data-testid="animals-list"] li').all()
    except Exception:
        return counts
    for it in items:
        try:
            cnt = await it.locator('span[data-testid="Animal__count"]').text_content(timeout=1000)
            ptype = await it.locator('svg title').text_content(timeout=1000)
            key = normalize_pet(ptype)
            if key in counts:
                counts[key] += int(cnt.strip())
        except Exception:
            continue
    return counts

def listing_id_from_url(url: str) -> str:
    m = re.search(r'/l/(\d+)(?:/|$)', url)
    return m.group(1) if m else url

# --- Telegram functions ---
def format_telegram_message(rows: list[dict]) -> list[str]:
    chunks = []
    for i in range(0, len(rows), 4):
        group = rows[i:i+4]
        lines = ["🔔 *New Listings:*", ""]
        for idx, row in enumerate(group, start=i+1):
            pets = ", ".join(f"{row[p]} {p}" for p in PET_TYPES if row.get(p,0))
            lines.append(f"{idx}. *{escape_markdown(row['title'])}*")
            lines.append(f"   📍 {escape_markdown(row['town'])}, {escape_markdown(row['country'])}")
            lines.append(f"   📅 {escape_markdown(row['date_from'])} → {escape_markdown(row['date_to'])}")
            if pets: lines.append(f"   🐾 {escape_markdown(pets)}")
            if row.get('reviewing'): lines.append(f"   📝 Reviewing applications")
            lines.append(f"   🔗 [View listing]({row['url']})")
            lines.append("")
        chunks.append("\n".join(lines))
    return chunks


def send_telegram_message(chunks: list[str]) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for part, chunk in enumerate(chunks, start=1):
        logging.info(f"Sending part {part}/{len(chunks)} (len={len(chunk)})")
        logging.debug(f"Preview: {chunk[:200]}{'…' if len(chunk)>200 else ''}")
        try:
            res = requests.post(url, json={
                'chat_id': TELEGRAM_CHAT_ID,
                'text': chunk,
                'parse_mode': 'Markdown',
                'disable_web_page_preview': False
            })
            if res.status_code != 200:
                logging.error(f"Failed part {part}: {res.text}\n{chunk}")
        except Exception:
            logging.exception(f"Telegram exception part {part}")

# --- Browser interactions ---
async def initial_search(page) -> None:
    logging.info("Initial search setup")
    await page.goto(BASE_URL, wait_until='networkidle')
    await wait_like_human()
    await page.screenshot(path='before_fill.png')
    box = page.locator("input[placeholder*='Where would you like to go?']")
    await box.wait_for(timeout=15000)
    await box.fill('europe'); await wait_like_human()
    await page.get_by_text('Europe').click(); await wait_like_human()
    await page.get_by_role('button', name='Dates').click()
    for _ in range(6):
        if await page.locator('text=November 2025').is_visible(): break
        await page.get_by_role('button', name='chevron-right').click(); await wait_like_human()
    await page.get_by_label('01 Nov 2025 Saturday').click()
    await page.get_by_role('button', name='chevron-right').click(); await wait_like_human()
    await page.get_by_label('24 Dec 2025 Wednesday').click()
    await page.get_by_role('button', name='Apply').click(); await wait_like_human()

async def apply_filters(page, mode) -> None:
    if mode is None: return
    logging.info(f"Applying filter: {mode}")
    await page.get_by_role("button", name="More Filters").click(); await wait_like_human()
    lbl_text = "Accessible by public transport" if mode=='public_transport' else "Use of car included"
    label = page.locator("label").filter(has_text=lbl_text)
    await label.locator('span').nth(2).click(); await wait_like_human()
    await page.get_by_role("button", name="Apply").click()
    await page.wait_for_selector('div[data-testid="searchresults_grid_item"]', timeout=10000)
    await wait_like_human()

async def scrape_run(page, test_mode=False) -> list[dict]:
    rows=[]; page_num=1
    while True:
        logging.info(f"Scraping page {page_num}")
        await page.wait_for_selector('div[data-testid="searchresults_grid_item"]', timeout=30000)
        cards = await page.locator('div[data-testid="searchresults_grid_item"]').all()
        logging.info(f"Found {len(cards)} cards on page {page_num}")
        if not cards: break
        for card in (cards if not test_mode else cards[:2]):
            try:
                title=await card.locator('h3[data-testid="ListingCard__title"]').text_content(timeout=1000)
                loc=await card.locator('span[data-testid="ListingCard__location"]').text_content(timeout=1000)
                town,country=split_location(loc)
                raw=await card.locator("div[class*='UnOOR'] > span").first.text_content(timeout=1000)
                d1,d2=(re.split(r"\s*[-–]\s*", raw.replace('+','').strip())+['',''])[:2]
                reviewing=await card.locator('span[data-testid="ListingCard__review__label"]').count()>0
                rel=await card.locator('a').get_attribute('href',timeout=1000)
                pets=await extract_pets(card)
                rows.append({
                    'url':f"https://www.trustedhousesitters.com{rel}",
                    'listing_id':listing_id_from_url(rel),
                    'date_range':f"{d1}→{d2}",
                    'title':title.strip(),'location':loc.strip(),
                    'town':town,'country':country,
                    'date_from':d1,'date_to':d2,
                    'reviewing':reviewing,
                    **pets
                })
            except Exception:
                logging.exception("Error parsing card")
        try:
            nxt=page.get_by_role('link',name='Go to next page')
            if await nxt.get_attribute('aria-disabled')!='true':
                await nxt.click(); await wait_like_human(); page_num+=1; continue
        except Exception:
            pass
        break
    return rows

async def main(test_mode=False) -> None:
    logging.info("Starting scrape")
    start_time=time.time()
    async with async_playwright() as p:
        browser=await p.chromium.launch(headless=HEADLESS)
        ctx=await browser.new_context(user_agent="Mozilla/5.0",viewport={'width':1280,'height':720},locale='en-US')
        await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        page=await ctx.new_page()
        runs={}
        for mode in MODES:
            try:
                await initial_search(page)
                await apply_filters(page,mode)
                runs[mode]=await scrape_run(page,test_mode)
            except Exception:
                logging.critical(f"Mode {mode} failed, dumping state",exc_info=True)
                html=await page.content(); open("crash_dump.html","w").write(html)
                await page.screenshot(path="crash_screenshot.png",full_page=True)
                raise
        await browser.close()
    base_df=pd.DataFrame(runs[None])
    now=datetime.utcnow().isoformat()+'Z'
    base_df['public_transport']=base_df['url'].isin([r['url'] for r in runs['public_transport']]).astype(bool)
    base_df['car_included']=base_df['url'].isin([r['url'] for r in runs['car_included']]).astype(bool)
    base_df['unique_key']=base_df['listing_id']+'|'+base_df['date_range']
    # Load history
    if os.path.exists(JSON_PATH) and os.path.getsize(JSON_PATH)>0:
        try: old_df=pd.read_json(JSON_PATH)
        except: logging.warning("Bad JSON, reset"); old_df=pd.DataFrame()
    else: old_df=pd.DataFrame()
    # Cast booleans and timestamps
    old_df['public_transport']=old_df.get('public_transport',False).fillna(False).astype(bool)
    old_df['car_included']=old_df.get('car_included',False).fillna(False).astype(bool)
    default_fs=(datetime.utcnow()-timedelta(seconds=1)).isoformat()+'Z'
    old_df['first_seen']=old_df.get('first_seen',default_fs)
    old_df['last_changed']=old_df.get('last_changed',old_df['first_seen'])
    if 'unique_key' not in old_df:
        old_df['unique_key']=old_df.apply(lambda r: listing_id_from_url(r['url'])+'|'+f"{r['date_from']}→{r['date_to']}",axis=1)
    merged=old_df.set_index('unique_key').combine_first(base_df.set_index('unique_key'))
    for uk,row in merged.iterrows():
        if uk in base_df['unique_key'].values:
            nr=base_df.loc[base_df['unique_key']==uk].iloc[0]
            orow=old_df.loc[old_df['unique_key']==uk].iloc[0] if uk in old_df['unique_key'].values else None
            changed=orow is None or any(nr[col]!=orow[col] for col in CONTENT_COLS+['public_transport','car_included'])
            merged.at[uk,'last_changed']=now if changed else row['last_changed']
    merged['first_seen']=merged['first_seen'].fillna(now)
    df=merged.reset_index()
    df['new_this_run']=df['first_seen']==now
    # Expired
    old_keys=set(old_df['unique_key']); new_keys=set(df['unique_key'])
    exp_keys=old_keys-new_keys
    exp_df=old_df[old_df['unique_key'].isin(exp_keys)].copy()
    exp_df['expired']=True; exp_df['new_this_run']=False
    df['expired']=False
    out_df=pd.concat([df,exp_df],ignore_index=True)
    # Save
    out_df.to_csv(CSV_PATH,index=False,quoting=csv.QUOTE_NONNUMERIC)
    out_df.to_json(JSON_PATH,orient='records',indent=2)
    logging.info(f"Saved {len(out_df)} records")
    alerts=df[(df['new_this_run']) & (~df['country'].isin(EXCLUDED_COUNTRIES))]
    if alerts.empty: logging.info("No new listings to alert")
    else: send_telegram_message(format_telegram_message(alerts.to_dict('records')))
    logging.info(f"Done in {time.time()-start_time:.2f}s")

if __name__=='__main__':
    try:
        parser=argparse.ArgumentParser(); parser.add_argument('--test',action='store_true')
        args=parser.parse_args(); asyncio.run(main(test_mode=args.test))
    except Exception:
        logging.critical("Unhandled exception in main",exc_info=True)
        raise
