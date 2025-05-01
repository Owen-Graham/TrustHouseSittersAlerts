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
import json
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
HEADLESS = True  # Set to False for debugging

# --- Configuration ---
BASE_URL = "https://www.trustedhousesitters.com/house-and-pet-sitting-assignments/"
PET_TYPES = ["dog", "cat", "horse", "bird", "fish", "rabbit", "reptile", "poultry", "livestock", "small_pets"]
CONTENT_COLS = ["title", "location", "town", "country", "date_from", "date_to", "reviewing"] + PET_TYPES
MODES = ['public_transport', 'car_included', None]
CSV_PATH = "sits.csv"
JSON_PATH = "sits.json"
PROFILES_PATH = "filter_profiles.json"


# --- Utility functions ---
async def wait_like_human(min_sec=0.2, max_sec=0.5):
    await asyncio.sleep(random.uniform(min_sec, max_sec))


def normalize_pet(pet: str) -> str:
    pet = pet.lower().strip().replace("small pet", "small_pets")
    return pet.replace(" ", "_")


def split_location(location: str) -> tuple[str, str]:
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


def load_profiles() -> dict:
    """Load search profiles from configuration file"""
    if not os.path.exists(PROFILES_PATH):
        logging.warning(f"Profiles file {PROFILES_PATH} not found. Using default profile.")
        return {
            "default": {
                "search": {
                    "location": "europe",
                    "date_from": "01 Nov 2025",
                    "date_to": "24 Dec 2025"
                },
                "filters": {
                    "excluded_countries": ["United Kingdom", "Ireland"],
                    "max_pets": {}
                },
                "notification": {
                    "header": "🏠 NEW SITS 🏠",
                    "icon": "🔔"
                }
            }
        }

    try:
        with open(PROFILES_PATH, 'r') as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Error loading profiles: {e}")
        return {"default": {"search": {"location": "europe", "date_from": "01 Nov 2025", "date_to": "24 Dec 2025"},
                            "filters": {"excluded_countries": ["United Kingdom", "Ireland"]}}}


# --- Telegram functions ---
def format_telegram_message(rows: list[dict], profile_config: dict) -> list[str]:
    chunks = []
    header = profile_config.get("notification", {}).get("header", "🔔 New Listings")
    icon = profile_config.get("notification", {}).get("icon", "🏠")

    for i in range(0, len(rows), 4):
        group = rows[i:i + 4]
        lines = [f"{header}", ""]
        for idx, row in enumerate(group, start=i + 1):
            pets = ", ".join(f"{row[p]} {p}" for p in PET_TYPES if row.get(p, 0))
            lines.append(f"{idx}. {icon} *{escape_markdown(row['title'])}*")
            lines.append(f"   📍 {escape_markdown(row['town'])}, {escape_markdown(row['country'])}")
            lines.append(f"   📅 {escape_markdown(row['date_from'])} → {escape_markdown(row['date_to'])}")
            # Pet count
            if pets:
                lines.append(f"   🐾 {escape_markdown(pets)}")
            # Review status
            if row.get('reviewing'):
                lines.append(f"   📝 Reviewing applications")
            # New fields: transport & car
            lines.append(f"   🚗 Car included: {'Yes' if row.get('car_included') else 'No'}")
            lines.append(f"   🚌 Public transport: {'Yes' if row.get('public_transport') else 'No'}")
            # Link
            lines.append(f"   🔗 [View listing]({row['url']})")
            lines.append("")
        chunks.append("\n".join(lines))
    return chunks


def send_telegram_message(chunks: list[str]) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logging.warning("Telegram credentials not set. Skipping notification.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    for part, chunk in enumerate(chunks, start=1):
        logging.info(f"Sending part {part}/{len(chunks)} (len={len(chunk)})")
        logging.debug(f"Preview: {chunk[:200]}{'…' if len(chunk) > 200 else ''}")
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
async def initial_search(page, profile_config) -> None:
    logging.info(f"Initial search setup for {profile_config['search']['location']}")
    await page.goto(BASE_URL, wait_until='networkidle')
    await wait_like_human()

    # Take a screenshot to help with debugging
    await page.screenshot(path=f"debug_initial_{profile_config['search']['location']}.png")

    # Fill location
    box = page.locator("input[placeholder*='Where would you like to go?']")
    await box.wait_for(timeout=15000)
    location = profile_config["search"]["location"]
    await box.fill(location)
    await wait_like_human()

    # For Europe, click the first exact match
    if location.lower() == "europe":
        await page.locator("text=Europe").first.click()
    # For Asia, be more specific to handle multiple matches
    elif location.lower() == "asia":
        await page.locator("span").filter(has_text="Asia").first.click()
    else:
        # For other locations, use the capitalized name
        await page.locator(f"text={location.capitalize()}").first.click()

    await wait_like_human()

    # Set dates
    await page.get_by_role('button', name='Dates').click()
    await wait_like_human()

    # Extract month and year from the dates
    date_from = profile_config["search"]["date_from"]  # e.g., "01 Nov 2025"
    date_to = profile_config["search"]["date_to"]  # e.g., "24 Dec 2025"

    # Parse the dates
    from_parts = date_from.split(" ")
    from_day = from_parts[0]
    from_month = from_parts[1]
    from_year = from_parts[2]

    to_parts = date_to.split(" ")
    to_day = to_parts[0]
    to_month = to_parts[1]
    to_year = to_parts[2]

    # Navigate to the correct month - try for up to 10 clicks
    found_month = False
    for _ in range(10):
        # Take a screenshot to see what we're looking at
        await page.screenshot(path=f"calendar_navigation_{_}.png")

        # Check for month in different formats - both full month name and abbreviated
        # For example "November 2025" and "Nov 2025"
        if await page.locator(f'text={from_month} {from_year}').is_visible() or \
                await page.locator(f'text={from_month.capitalize()} {from_year}').is_visible() or \
                await page.locator(f'text=November {from_year}').is_visible():
            found_month = True
            logging.info(f"Found target month after {_} clicks")
            break

        await page.get_by_role('button', name='chevron-right').click()
        await wait_like_human()

    if not found_month:
        logging.error(f"Could not find target month: {from_month} {from_year}")
        raise Exception(f"Month navigation failed")

    # Try different formats for date selection
    try:
        # First try original format
        await page.get_by_label(f'{from_day} {from_month} {from_year}').click()
    except Exception:
        try:
            # Try with leading zero
            await page.get_by_label(f'{int(from_day):02d} {from_month} {from_year}').click()
        except Exception:
            try:
                # Try with day of week
                for day_name in ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]:
                    try:
                        await page.get_by_label(f'{from_day} {from_month} {from_year} {day_name}').click()
                        break
                    except:
                        continue
            except Exception:
                # Last resort - find and click the day number in the current month view
                await page.locator(
                    f'.react-datepicker__day:not(.react-datepicker__day--outside-month):text("{from_day}")').first.click()

    await wait_like_human()

    # Navigate to end date month if needed
    if from_month != to_month or from_year != to_year:
        await page.get_by_role('button', name='chevron-right').click()
        await wait_like_human()

    # Select end date with similar fallback options
    try:
        await page.get_by_label(f'{to_day} {to_month} {to_year}').click()
    except Exception:
        try:
            await page.get_by_label(f'{int(to_day):02d} {to_month} {to_year}').click()
        except Exception:
            try:
                # Try with day of week
                for day_name in ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]:
                    try:
                        await page.get_by_label(f'{to_day} {to_month} {to_year} {day_name}').click()
                        break
                    except:
                        continue
            except Exception:
                # Last resort - find and click the day number
                await page.locator(
                    f'.react-datepicker__day:not(.react-datepicker__day--outside-month):text("{to_day}")').first.click()

    await wait_like_human()

    # Apply dates
    await page.get_by_role('button', name='Apply').click()
    await wait_like_human(1, 2)

    # Check if page has loaded with either results or no results message
    try:
        # Wait for either search results OR the no results message
        for _ in range(10):  # Try for a reasonable amount of time
            has_results = await page.locator('div[data-testid="searchresults_grid_item"]').count() > 0
            has_no_results = await page.locator('text=waiting on house and pet sitting opportunities').count() > 0

            if has_results or has_no_results:
                logging.info(
                    f"Page loaded for {profile_config['search']['location']} - Results: {has_results}, No results message: {has_no_results}")
                break

            await asyncio.sleep(1)  # Wait a second between checks
        else:
            # This executes if the loop completes without breaking
            logging.error(f"Timed out waiting for page to load: {profile_config['search']['location']}")
            await page.screenshot(path=f"error_page_load_{profile_config['search']['location']}.png")
            raise Exception("Page failed to load search results or no results message")

    except Exception as e:
        logging.error(f"Page failed to load search results: {e}")
        await page.screenshot(path=f"error_page_load_{profile_config['search']['location']}.png")
        raise


async def apply_filters(page, mode) -> None:
    if mode is None: return

    # Check if we have no results
    no_results = await page.locator("text=We're waiting on house and pet sitting opportunities").count() > 0
    if no_results:
        logging.info(f"No results available, skipping filter: {mode}")
        return

    logging.info(f"Applying filter: {mode}")

    try:
        # Click More Filters button
        more_filters = page.get_by_role("button", name="More Filters")
        await more_filters.wait_for(state="visible", timeout=10000)
        await more_filters.click()
        await wait_like_human()

        # Select the appropriate filter
        lbl_text = "Accessible by public transport" if mode == 'public_transport' else "Use of car included"

        # Look for the label text and click the associated checkbox
        filter_labels = await page.locator("label").filter(has_text=lbl_text).all()
        if filter_labels:
            # Click the checkbox (span inside the label)
            await filter_labels[0].locator('span').nth(2).click()
            await wait_like_human()
        else:
            logging.error(f"Filter option '{lbl_text}' not found")
            await page.screenshot(path=f"error_filter_not_found_{mode}.png")
            raise Exception(f"Filter option '{lbl_text}' not found")

        # Click Apply button
        apply_button = page.get_by_role("button", name="Apply")
        await apply_button.wait_for(state="visible", timeout=5000)
        await apply_button.click()
        await wait_like_human()

        # Wait for results or no results message using the same approach as in initial_search
        for _ in range(10):  # Try for a reasonable amount of time
            has_results = await page.locator('div[data-testid="searchresults_grid_item"]').count() > 0
            has_no_results = await page.locator('text=waiting on house and pet sitting opportunities').count() > 0

            if has_results or has_no_results:
                logging.info(f"Filter applied: {mode} - Results: {has_results}, No results message: {has_no_results}")
                break

            await asyncio.sleep(1)  # Wait a second between checks
        else:
            # This executes if the loop completes without breaking
            logging.error(f"Timed out waiting for page to load after applying filter: {mode}")
            await page.screenshot(path=f"error_filter_applied_{mode}.png")
            raise Exception("Page failed to load search results or no results message after applying filter")

    except Exception as e:
        logging.error(f"Error applying filter '{mode}': {e}")
        await page.screenshot(path=f"error_applying_filter_{mode}.png")
        raise


async def scrape_run(page, test_mode=False) -> list[dict]:
    rows = []
    page_num = 1

    # First check if we have no results
    no_results = await page.locator("text=We're waiting on house and pet sitting opportunities").count() > 0
    if no_results:
        logging.info("No search results available for this search")
        return rows  # Return empty list

    try:
        while True:
            logging.info(f"Scraping page {page_num}")

            # Wait for search results to load
            await page.wait_for_selector('div[data-testid="searchresults_grid_item"]', timeout=15000)
            await page.screenshot(path=f"debug_results_page_{page_num}.png")

            # Get all listing cards
            cards = await page.locator('div[data-testid="searchresults_grid_item"]').all()
            logging.info(f"Found {len(cards)} cards on page {page_num}")

            if not cards: break

            # Process each card
            for card_idx, card in enumerate(cards if not test_mode else cards[:2]):
                try:
                    title = await card.locator('h3[data-testid="ListingCard__title"]').text_content(timeout=1000)
                    loc = await card.locator('span[data-testid="ListingCard__location"]').text_content(timeout=1000)
                    town, country = split_location(loc)

                    # Get date range
                    date_elements = await card.locator("div[class*='UnOOR'] > span").all()
                    if date_elements:
                        raw = await date_elements[0].text_content(timeout=1000)
                        d1, d2 = (re.split(r"\s*[-–]\s*", raw.replace('+', '').strip()) + ['', ''])[:2]
                    else:
                        d1, d2 = '', ''

                    # Check if the listing is reviewing applications
                    reviewing = await card.locator('span[data-testid="ListingCard__review__label"]').count() > 0

                    # Get the listing URL
                    rel = await card.locator('a').get_attribute('href', timeout=1000)

                    # Extract pet information
                    pets = await extract_pets(card)

                    # Add the listing to our results
                    rows.append({
                        'url': f"https://www.trustedhousesitters.com{rel}",
                        'listing_id': listing_id_from_url(rel),
                        'date_range': f"{d1}→{d2}",
                        'title': title.strip(),
                        'location': loc.strip(),
                        'town': town,
                        'country': country,
                        'date_from': d1,
                        'date_to': d2,
                        'reviewing': reviewing,
                        **pets
                    })
                except Exception as e:
                    logging.exception(f"Error parsing card {card_idx} on page {page_num}: {e}")

            # Check if there's a next page - first check if the next button exists
            try:
                next_link = page.get_by_role('link', name='Go to next page')
                next_link_count = await next_link.count()

                # If there's no next page link at all, we're done
                if next_link_count == 0:
                    logging.info("No next page link found - this must be the last page")
                    break

                # If there is a next link, check if it's disabled
                is_disabled = await next_link.get_attribute('aria-disabled', timeout=5000)

                if is_disabled == 'true':
                    logging.info("Next page link is disabled - this is the last page")
                    break

                # Otherwise, click the next page button and continue
                await next_link.click()
                await wait_like_human()
                page_num += 1

            except Exception as e:
                # This could happen if we only have one page of results
                logging.info(f"No more pages or error navigating: {e}")
                break

    except Exception as e:
        logging.error(f"Error in scrape_run: {e}")
        await page.screenshot(path=f"error_scrape_run.png")

    return rows


def apply_profile_filters(df, profile_config):
    """Apply profile-specific filters to the dataframe"""
    filtered_df = df.copy()

    # Apply excluded countries filter
    excluded_countries = profile_config.get("filters", {}).get("excluded_countries", [])
    if excluded_countries:
        filtered_df = filtered_df[~filtered_df['country'].isin(excluded_countries)]

    # Apply max pets filter
    max_pets = profile_config.get("filters", {}).get("max_pets", {})
    for pet_type, max_count in max_pets.items():
        if pet_type in PET_TYPES:
            filtered_df = filtered_df[filtered_df[pet_type] <= max_count]

    return filtered_df


async def process_profile(profile_name, profile_config, test_mode=False):
    logging.info(f"Processing profile: {profile_name}")
    start_time = time.time()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800},
            locale='en-US'
        )
        await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")

        async def run_mode(mode):
            page = await ctx.new_page()
            try:
                await initial_search(page, profile_config)
                no_results = await page.locator("text=We're waiting on house and pet sitting opportunities").count() > 0
                if no_results:
                    logging.info(f"No results available for profile {profile_name}, mode {mode}")
                    return mode, []
                await apply_filters(page, mode)
                results = await scrape_run(page, test_mode)
                logging.info(f"Found {len(results)} results for {profile_name}, mode {mode}")
                return mode, results
            except Exception as e:
                logging.critical(f"Mode {mode} failed for profile {profile_name}: {e}", exc_info=True)
                html = await page.content()
                with open(f"crash_dump_{profile_name}_{mode}.html", "w") as f:
                    f.write(html)
                await page.screenshot(path=f"crash_screenshot_{profile_name}_{mode}.png", full_page=True)
                return mode, []

        results = await asyncio.gather(*(run_mode(mode) for mode in MODES))
        await browser.close()

    runs = dict(results)

    base_df = pd.DataFrame(runs.get(None, []))
    if base_df.empty:
        logging.warning(f"No results found for profile {profile_name}")
        return pd.DataFrame()

    now = datetime.utcnow().isoformat() + 'Z'
    public_transport_ids = [listing_id_from_url(r['url']) for r in runs.get('public_transport', [])]
    car_included_ids = [listing_id_from_url(r['url']) for r in runs.get('car_included', [])]

    for idx, row in base_df.iterrows():
        listing_id = row['listing_id']
        pt_match = listing_id in public_transport_ids
        car_match = listing_id in car_included_ids
        logging.info(f"Listing {listing_id} - Public transport: {pt_match}, Car included: {car_match}")

    base_df['public_transport'] = base_df['listing_id'].isin(public_transport_ids).astype(bool)
    base_df['car_included'] = base_df['listing_id'].isin(car_included_ids).astype(bool)
    base_df['unique_key'] = base_df['listing_id'] + '|' + base_df['date_range']
    base_df['profile'] = profile_name

    logging.info(f"Profile {profile_name} completed in {time.time() - start_time:.2f}s, found {len(base_df)} listings")
    return base_df

# --- Modify main to run profiles in parallel (unchanged if already done) ---
async def main(test_mode=False) -> None:
    logging.info("Starting scrape")
    start_time = time.time()

    profiles = load_profiles()
    logging.info(f"Loaded {len(profiles)} search profiles")

    async def run_profile(name, config):
        try:
            return name, await process_profile(name, config, test_mode)
        except Exception as e:
            logging.error(f"Failed to process profile {name}: {e}", exc_info=True)
            return name, e

    results = await asyncio.gather(*(run_profile(name, config) for name, config in profiles.items()))

    all_results = []
    for name, result in results:
        if isinstance(result, pd.DataFrame) and not result.empty:
            all_results.append(result)
        elif isinstance(result, Exception):
            logging.error(f"Profile {name} raised exception: {result}", exc_info=True)

    if not all_results:
        logging.warning("No results found for any profile")
        return

    base_df = pd.concat(all_results, ignore_index=True)

    if os.path.exists(JSON_PATH) and os.path.getsize(JSON_PATH) > 0:
        try:
            old_df = pd.read_json(JSON_PATH)
        except Exception as e:
            logging.warning(f"Bad JSON, reset: {e}")
            old_df = pd.DataFrame()
    else:
        old_df = pd.DataFrame()

    old_df['public_transport'] = old_df.get('public_transport', False).fillna(False).astype(bool)
    old_df['car_included'] = old_df.get('car_included', False).fillna(False).astype(bool)
    default_fs = (datetime.utcnow() - timedelta(seconds=1)).isoformat() + 'Z'
    old_df['first_seen'] = old_df.get('first_seen', default_fs)
    old_df['last_changed'] = old_df.get('last_changed', old_df['first_seen'])

    if 'unique_key' not in old_df:
        old_df['unique_key'] = old_df.apply(
            lambda r: listing_id_from_url(r['url']) + '|' + f"{r['date_from']}→{r['date_to']}", axis=1
        )

    merged = old_df.set_index('unique_key').combine_first(base_df.set_index('unique_key'))

    now = datetime.utcnow().isoformat() + 'Z'
    for uk, row in merged.iterrows():
        if uk in base_df['unique_key'].values:
            nr = base_df.loc[base_df['unique_key'] == uk].iloc[0]
            orow = old_df.loc[old_df['unique_key'] == uk].iloc[0] if uk in old_df['unique_key'].values else None
            changed = orow is None or any(
                nr[col] != orow[col] for col in CONTENT_COLS + ['public_transport', 'car_included']
            )
            merged.at[uk, 'last_changed'] = now if changed else row['last_changed']
            merged.at[uk, 'profile'] = nr['profile']

    merged['first_seen'] = merged['first_seen'].fillna(now)
    df = merged.reset_index()
    df['new_this_run'] = df['first_seen'] == now

    old_keys = set(old_df['unique_key'])
    new_keys = set(df['unique_key'])
    exp_keys = old_keys - new_keys
    exp_df = old_df[old_df['unique_key'].isin(exp_keys)].copy()
    exp_df['expired'] = True
    exp_df['new_this_run'] = False
    df['expired'] = False

    out_df = pd.concat([df, exp_df], ignore_index=True)

    out_df.to_csv(CSV_PATH, index=False, quoting=csv.QUOTE_NONNUMERIC)
    out_df.to_json(JSON_PATH, orient='records', indent=2)
    logging.info(f"Saved {len(out_df)} records")

    for profile_name, profile_config in profiles.items():
        profile_df = out_df[(out_df['new_this_run']) & (out_df['profile'] == profile_name)]
        profile_df = apply_profile_filters(profile_df, profile_config)

        if profile_df.empty:
            logging.info(f"No new listings to alert for profile {profile_name}")
        else:
            logging.info(f"Sending {len(profile_df)} alerts for profile {profile_name}")
            send_telegram_message(format_telegram_message(profile_df.to_dict('records'), profile_config))

    logging.info(f"Done in {time.time() - start_time:.2f}s")

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--test', action='store_true')
        args = parser.parse_args()
        asyncio.run(main(test_mode=args.test))
    except Exception:
        logging.critical("Unhandled exception in main", exc_info=True)
        raise
