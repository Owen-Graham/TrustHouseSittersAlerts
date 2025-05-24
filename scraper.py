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
# MODES = ['public_transport', 'car_included', None] # Removed as per refactoring for single-pass scraping
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
    # Uses Playwright's evaluate_all to efficiently extract pet details in a single browser-side operation,
    # minimizing back-and-forth communication between Playwright and the browser.
    js_script = """
    ulElement => {
        // Guard clause: Check if ulElement is a valid DOM element with querySelectorAll
        if (!ulElement || typeof ulElement.querySelectorAll !== 'function') {
            // console.warn('ulElement is not a valid DOM element or querySelectorAll is not a function.'); // Optional: for browser-side debugging
            return []; // Return empty array if not valid
        }

        const petData = [];
        const items = ulElement.querySelectorAll('li');
        items.forEach(item => {
            try {
                const countEl = item.querySelector('span[data-testid="Animal__count"]');
                const typeEl = item.querySelector('svg title');
                
                if (countEl && typeEl) {
                    petData.push({
                        count: countEl.textContent.trim(),
                        type: typeEl.textContent.trim()
                    });
                }
            } catch (e) {
                // Log error in browser console if needed, or ignore individual item errors
                console.error('Error processing pet item:', e);
            }
        });
        return petData;
    }
    """
    
    try:
        # Ensure the main list exists before trying to evaluate all
        animals_list_locator = card.locator('ul[data-testid="animals-list"]')
        if await animals_list_locator.count() == 0:
            logging.debug("No 'animals-list' found for a card.")
            return counts

        pet_details_list = await animals_list_locator.evaluate_all(js_script)
        
        for pet_detail in pet_details_list:
            try:
                key = normalize_pet(pet_detail['type'])
                if key in counts:
                    counts[key] += int(pet_detail['count'])
            except (ValueError, KeyError) as e:
                logging.warning(f"Could not parse pet detail: {pet_detail}. Error: {e}")
                continue
                
    except Exception as e:
        # This is the main exception handler for the evaluate_all call or its result processing
        logging.error(f"Error during extract_pets (evaluate_all or its processing): {e}. Returning zero pets for this listing.")
        # Ensure counts is the default all-zero dictionary for safety.
        counts = {p: 0 for p in PET_TYPES} 
        return counts # Return the zeroed counts
        
    return counts


def listing_id_from_url(url: str) -> str:
    # Modified regex to correctly capture ID even with query parameters.
    # Old regex: r'/l/(\d+)(?:/|$)'
    m = re.search(r'/l/(\d+)', url) 
    return m.group(1) if m else url

def format_date_for_url(date_str: str) -> str:
    """Converts 'DD Mon YYYY' (e.g., '01 Nov 2025') to 'YYYY-MM-DD'."""
    if not date_str:
        return ""
    try:
        dt_obj = datetime.strptime(date_str, "%d %b %Y")
        return dt_obj.strftime("%Y-%m-%d")
    except ValueError:
        logging.warning(f"Could not parse date string: {date_str}")
        return ""

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
async def initial_search(page, profile_config, test_mode=False) -> None: 
    location_query = profile_config["search"]["location"]
    date_from_str = profile_config["search"]["date_from"]
    date_to_str = profile_config["search"]["date_to"]

    formatted_date_from = format_date_for_url(date_from_str)
    formatted_date_to = format_date_for_url(date_to_str)

    # Method 1: Attempt to set dates directly via URL parameters for efficiency.
    # This is speculative as actual URL parameters are unknown without dev tools.
    # If successful, this bypasses complex UI interactions for date selection.
    url_to_try_with_dates = "" 
    
    logging.info(f"Initial search setup for {location_query}")
    await page.goto(BASE_URL, wait_until='networkidle')
    await wait_like_human()
    # Conditional screenshot: Captures the initial page if in test_mode.
    if test_mode:
        await page.screenshot(path=f"debug_initial_page_{location_query}.png")

    # Fill location (UI-based)
    box = page.locator("input[placeholder*='Where would you like to go?']")
    await box.wait_for(timeout=15000)
    await box.fill(location_query)
    await wait_like_human()

    if location_query.lower() == "europe":
        await page.locator("text=Europe").first.click()
    elif location_query.lower() == "asia":
        await page.locator("span").filter(has_text="Asia").first.click()
    else:
        await page.locator(f"text={location_query.capitalize()}").first.click()
    await wait_like_human()
    
    current_url_after_location = page.url
    logging.info(f"URL after location selection: {current_url_after_location}")

    # Attempt to use URL parameters for dates if available
    if formatted_date_from and formatted_date_to:
        date_params = f"availability_start_date={formatted_date_from}&availability_end_date={formatted_date_to}"
        if "?" in current_url_after_location: 
            url_to_try_with_dates = f"{current_url_after_location}&{date_params}"
        else: 
            url_to_try_with_dates = f"{current_url_after_location}?{date_params}"
        
        logging.info(f"Attempting to navigate with date URL: {url_to_try_with_dates}")
        try:
            await page.goto(url_to_try_with_dates, wait_until='networkidle', timeout=10000) 
            await wait_like_human(1,2) 
            # Conditional screenshot: Captures page after attempting URL-based date setting.
            if test_mode:
                await page.screenshot(path=f"debug_date_url_attempt_{location_query}.png")
            logging.info("Attempted navigation with URL-based date parameters.")
        except Exception as e_url_nav:
            logging.warning(f"URL-based date navigation failed: {e_url_nav}. Falling back to UI interaction.")
            if test_mode:
                 await page.screenshot(path=f"debug_date_url_nav_failed_{location_query}.png")
            # Ensure page is back at a known state if goto failed partway or timed out
            await page.goto(current_url_after_location, wait_until='networkidle', timeout=10000) 
            # Fallback to UI datepicker if URL method fails or is not applicable
            await initial_search_ui_datepicker(page, date_from_str, date_to_str, location_query, test_mode)
    else: 
        # Fallback to UI datepicker if formatted dates are not available (e.g. empty date strings in profile)
        logging.info("Formatted dates not available for URL parameters. Proceeding with UI-based date selection.")
        await initial_search_ui_datepicker(page, date_from_str, date_to_str, location_query, test_mode)

    # Final check: Ensure page has loaded with either results or a no-results message.
    # This is crucial for both URL parameter and UI date selection paths.
    try:
        for _ in range(10): 
            has_results = await page.locator('div[data-testid="searchresults_grid_item"]').count() > 0
            has_no_results = await page.locator('text=waiting on house and pet sitting opportunities').count() > 0
            if has_results or has_no_results:
                logging.info(
                    f"Page loaded for {location_query} - Results: {has_results}, No results message: {has_no_results}")
                break
            await asyncio.sleep(1) 
        else: # If loop completes without break
            logging.error(f"Timed out waiting for page to load search results or 'no results' message: {location_query}")
            await page.screenshot(path=f"error_page_load_final_check_{location_query}.png") # Error screenshot
            raise Exception("Page failed to load search results or no results message after all date setting attempts.")
    except Exception as e:
        logging.error(f"Exception while checking for page load after date setting: {e}")
        await page.screenshot(path=f"error_page_load_exception_{location_query}.png") # Error screenshot
        raise # Re-raise the exception to be caught by process_profile

async def initial_search_ui_datepicker(page, date_from_str, date_to_str, location_query, test_mode=False):
    """
    Handles the UI interactions for date selection using the datepicker.
    This function is called as a fallback if URL-based date setting fails or is not applicable.
    It includes optimized "calculated clicks" logic for month navigation and robust fallbacks.
    """
    logging.info(f"Profile {location_query}: Using UI datepicker for date selection.")
    await page.get_by_role('button', name='Dates').click()
    await wait_like_human()

    target_from_date = datetime.strptime(date_from_str, "%d %b %Y")
    target_to_date = datetime.strptime(date_to_str, "%d %b %Y")

    async def navigate_and_select_date(target_date: datetime, date_label_prefix: str):
        # Get current displayed month and year from the calendar for "calculated clicks" optimization.
        current_month_year_str = "" 
        try:
            # Attempt to find the current month/year displayed in the datepicker.
            # This is used to calculate the exact number of clicks needed to reach the target month.
            possible_selectors = [
                '.react-datepicker__current-month', '[class*="CurrentMonth"]', 
                '.DayPicker-Caption', '.rdp-caption_label' 
            ]
            for selector_idx, sel in enumerate(possible_selectors):
                if await page.locator(sel).count() > 0:
                    current_month_year_str = await page.locator(sel).first.text_content(timeout=2000)
                    logging.info(f"Found current month display with selector '{sel}': {current_month_year_str}")
                    break
                else:
                    logging.debug(f"Selector '{sel}' not found for current month display (attempt {selector_idx+1}).")
            
            if not current_month_year_str: 
                # Conditional screenshot: If current month detection fails for calculated clicks.
                if test_mode: 
                    await page.screenshot(path=f"debug_calendar_month_detection_failed_{date_label_prefix}_{location_query}.png")
                logging.warning("Could not determine current displayed month. Falling back to iterative month navigation.")
                # Fallback: Iterative clicking if current month cannot be determined.
                found_month_fallback = False
                for _ in range(12): # Max 12 clicks to find the month
                    # ... (iterative clicking logic as before) ...
                    month_name_full = target_date.strftime("%B")
                    month_name_abbr = target_date.strftime("%b")
                    year_full = str(target_date.year)
                    if (await page.locator(f'text={month_name_abbr} {year_full}').is_visible() or
                        await page.locator(f'text={month_name_full} {year_full}').is_visible()):
                        found_month_fallback = True
                        # Conditional screenshot: Month found during iterative fallback.
                        if test_mode: 
                            await page.screenshot(path=f"debug_calendar_fallback_month_found_{date_label_prefix}_{location_query}.png")
                        break
                    await page.get_by_role('button', name='chevron-right').click()
                    await wait_like_human(0.1, 0.2)
                if not found_month_fallback:
                    raise Exception(f"Iterative fallback month navigation failed for {date_label_prefix} date.")
            else: 
                # Optimized "calculated clicks" for month navigation.
                # Parses the displayed month/year to determine current calendar position.
                current_date_on_calendar = datetime.strptime(current_month_year_str.replace(",", ""), "%B %Y") 
                # Calculates month difference to target.
                month_diff = (target_date.year - current_date_on_calendar.year) * 12 + (target_date.month - current_date_on_calendar.month)
                nav_button_name = 'chevron-right' if month_diff > 0 else 'chevron-left'
                clicks_needed = abs(month_diff)

                logging.info(f"For {date_label_prefix} date ({target_date.strftime('%b %Y')}): Current calendar month {current_date_on_calendar.strftime('%b %Y')}, clicks needed: {clicks_needed} ({nav_button_name})")
                for i in range(clicks_needed):
                    await page.get_by_role('button', name=nav_button_name).click()
                    await wait_like_human(0.1, 0.2) 
                    # Conditional screenshot: After each navigation click in calculated mode.
                    if test_mode: 
                        await page.screenshot(path=f"debug_calendar_nav_click_{date_label_prefix}_{i+1}_{location_query}.png")
                    logging.debug(f"Navigation click {i+1} for {date_label_prefix} date.")
        
        except Exception as e:
            # Conditional screenshot: If calculated navigation logic itself errors out.
            if test_mode: 
                await page.screenshot(path=f"debug_calendar_calc_nav_error_fallback_start_{date_label_prefix}_{location_query}.png")
            logging.error(f"Error in calculated/new month navigation for {date_label_prefix} date: {e}. Attempting simpler fallback.")
            # Fallback to simpler iterative clicking if any part of advanced navigation fails.
            found_month_fallback = False
            for _ in range(12): 
                # ... (iterative clicking logic as before) ...
                month_name_full = target_date.strftime("%B") 
                month_name_abbr = target_date.strftime("%b") 
                year_full = str(target_date.year)
                if (await page.locator(f'text={month_name_abbr} {year_full}').is_visible() or
                    await page.locator(f'text={month_name_full} {year_full}').is_visible()):
                    found_month_fallback = True
                    # Conditional screenshot: Month found during the error's iterative fallback.
                    if test_mode: 
                         await page.screenshot(path=f"debug_calendar_error_fallback_month_found_{date_label_prefix}_{location_query}.png")
                    logging.info(f"Error fallback month navigation successful for {date_label_prefix} date.")
                    break
                await page.get_by_role('button', name='chevron-right').click() 
                await wait_like_human(0.1, 0.2)
            if not found_month_fallback:
                logging.error(f"Error fallback month navigation failed for {date_label_prefix} date: {target_date.strftime('%b %Y')}")
                raise Exception(f"Month navigation completely failed for {date_label_prefix} date.")

        # Select the day
        day_str = str(target_date.day)
        month_str_abbr = target_date.strftime("%b")
        year_str = str(target_date.year)
        day_selector_specific = f'.react-datepicker__day:not(.react-datepicker__day--outside-month):text-matches("^{int(day_str)}$")'
        try:
            await page.get_by_label(f'{day_str} {month_str_abbr} {year_str}', exact=False).click(timeout=3000)
        except Exception:
            try:
                await page.get_by_label(f'{int(day_str):02d} {month_str_abbr} {year_str}', exact=False).click(timeout=3000)
            except Exception:
                logging.info(f"get_by_label failed for {date_label_prefix} day {day_str}, trying specific CSS selector.")
                await page.locator(day_selector_specific).first.click() 
        await wait_like_human()

    await navigate_and_select_date(target_from_date, "from")
    await navigate_and_select_date(target_to_date, "to")

    await page.get_by_role('button', name='Apply').click()
    await wait_like_human(1, 2)


async def scrape_run(page, test_mode=False) -> list[dict]:
    rows = []
    page_num = 1

    no_results = await page.locator("text=We're waiting on house and pet sitting opportunities").count() > 0
    if no_results:
        logging.info("No search results available for this search")
        return rows 

    try:
        while True:
            logging.info(f"Scraping page {page_num}")
            await page.wait_for_selector('div[data-testid="searchresults_grid_item"]', timeout=15000)
            # Conditional screenshot: Captures each page of results if in test_mode.
            if test_mode:
                await page.screenshot(path=f"debug_results_page_{page_num}.png")

            cards = await page.locator('div[data-testid="searchresults_grid_item"]').all()
            logging.info(f"Found {len(cards)} cards on page {page_num}")
            if not cards: break

            for card_idx, card in enumerate(cards if not test_mode else cards[:2]):
                try:
                    title = await card.locator('h3[data-testid="ListingCard__title"]').text_content(timeout=1000)
                    loc = await card.locator('span[data-testid="ListingCard__location"]').text_content(timeout=1000)
                    town, country = split_location(loc)

                    # Get date range - Uses a regex to find a span matching common date range patterns.
                    # This is more resilient to changes in surrounding class names or DOM structure.
                    date_text_pattern = re.compile(r"(\w{3}\s\d{1,2}(?:,\s\d{4})?\s*[-–→]\s*\w{3}\s\d{1,2}(?:,\s\d{4})?|\d{1,2}\s\w{3}(?:,\s\d{4})?\s*[-–→]\s*\d{1,2}\s\w{3}(?:,\s\d{4})?)")
                    date_locator = card.locator('span').filter(has_text=date_text_pattern).first
                    d1, d2 = '', ''
                    try:
                        if await date_locator.count() > 0:
                            raw_date_text = await date_locator.text_content(timeout=1000)
                            if raw_date_text:
                                d1, d2 = (re.split(r"\s*[-–]\s*", raw_date_text.replace('+', '').strip()) + ['', ''])[:2]
                        else:
                            logging.debug("Date range span not found with regex pattern for a card.")
                    except Exception as e_date:
                        logging.warning(f"Could not extract date for card: {e_date}")

                    reviewing = await card.locator('span[data-testid="ListingCard__review__label"]').count() > 0

                    # Get the listing URL - Tries a sequence of common robust patterns for the main link.
                    # 1. Specific data-testid="listing-card-link" (speculative ideal).
                    # 2. Link wrapping the H3 title.
                    # 3. First direct anchor child of the card container.
                    # Falls back to the first available link if specific patterns fail.
                    url_loc = card.locator('a[data-testid="listing-card-link"]') \
                                 .or_(card.locator('a:has(h3[data-testid="ListingCard__title"])')) \
                                 .or_(card.locator('div[data-testid="searchresults_grid_item"] > a')) \
                                 .first
                    rel = ""
                    if await url_loc.count() > 0:
                         rel = await url_loc.get_attribute('href', timeout=1000)
                    else:
                        logging.warning("Primary URL locators failed, falling back to 'card.locator(\"a\").first'")
                        if await card.locator('a').count() > 0:
                           rel = await card.locator('a').first.get_attribute('href', timeout=1000)
                        else:
                            logging.error("Could not find any link for a card to extract URL.")
                            rel = "#error-no-url-found" # Placeholder for missing URL

                    pets = await extract_pets(card) # Optimized pet extraction

                    # Attempt to extract transport and car information from card details.
                    # These selectors are speculative and rely on common text patterns or potential data-testids.
                    public_transport_available = False
                    try:
                        pt_indicator_texts = ["Accessible by public transport", "Public transport nearby"]
                        pt_found = False
                        for text_indicator in pt_indicator_texts:
                            if await card.locator(f"text={text_indicator}").count() > 0:
                                pt_found = True
                                break
                        if not pt_found and await card.locator('[data-testid*="public-transport"]').count() > 0:
                            pt_found = True
                        public_transport_available = pt_found
                    except Exception as e_pt:
                        logging.debug(f"Could not determine public transport for card {card_idx} on page {page_num}: {e_pt}")

                    car_is_included = False
                    try:
                        ci_indicator_texts = ["Use of car included", "Car available"]
                        ci_found = False
                        for text_indicator in ci_indicator_texts:
                            if await card.locator(f"text={text_indicator}").count() > 0:
                                ci_found = True
                                break
                        if not ci_found and await card.locator('[data-testid*="car-included"]').count() > 0:
                            ci_found = True
                        car_is_included = ci_found
                    except Exception as e_car:
                        logging.debug(f"Could not determine car inclusion for card {card_idx} on page {page_num}: {e_car}")

                    # Append all extracted data for the current listing.
                    rows.append({
                        'url': f"https://www.trustedhousesitters.com{rel}",
                        'listing_id': listing_id_from_url(rel),
                        'date_range': f"{d1}→{d2}", 'title': title.strip(), 'location': loc.strip(),
                        'town': town, 'country': country, 'date_from': d1, 'date_to': d2,
                        'reviewing': reviewing, 'public_transport_available': public_transport_available,
                        'car_is_included': car_is_included, **pets
                    })
                except Exception as e:
                    logging.exception(f"Error parsing card {card_idx} on page {page_num}: {e}")

            # Pagination: Check for and navigate to the next page.
            try:
                next_link = page.get_by_role('link', name='Go to next page')
                if await next_link.count() == 0 or await next_link.get_attribute('aria-disabled') == 'true':
                    logging.info("Next page link not found or disabled - this is the last page.")
                    break
                await next_link.click()
                await wait_like_human()
                page_num += 1
            except Exception as e:
                logging.info(f"No more pages or error navigating to next page: {e}")
                break
    except Exception as e:
        logging.error(f"Error in scrape_run: {e}")
        await page.screenshot(path=f"error_scrape_run.png") # Error screenshot
    return rows


def apply_profile_filters(df, profile_config):
    """Apply profile-specific filters to the dataframe"""
    filtered_df = df.copy()
    excluded_countries = profile_config.get("filters", {}).get("excluded_countries", [])
    if excluded_countries:
        filtered_df = filtered_df[~filtered_df['country'].isin(excluded_countries)]
    max_pets = profile_config.get("filters", {}).get("max_pets", {})
    for pet_type, max_count in max_pets.items():
        if pet_type in PET_TYPES: # Ensure filtering only by recognized pet types
            filtered_df = filtered_df[filtered_df[pet_type] <= max_count]
    return filtered_df


async def process_profile(profile_name, profile_config, test_mode=False):
    # This function now performs a single search and scrape pass per profile.
    # It attempts to extract all necessary information, including transport and car details,
    # directly from the listing cards found in the initial search results.
    # This avoids multiple filter applications and page reloads for the same base search.
    logging.info(f"Processing profile: {profile_name}")
    overall_profile_processing_start_time = time.time() 
    all_listings_data = []

    async with async_playwright() as p:
        # Timing for browser launch and context setup
        pt_browser_launch_start = time.time()
        browser = await p.chromium.launch(headless=HEADLESS)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800},
            locale='en-US'
        )
        await ctx.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        logging.info(f"Profile {profile_name}: Browser launch & context creation took {time.time() - pt_browser_launch_start:.2f}s")
        
        page = await ctx.new_page()

        try:
            # Timing for initial search (location and date setting)
            pt_initial_search_start = time.time()
            await initial_search(page, profile_config, test_mode=test_mode) 
            logging.info(f"Profile {profile_name}: initial_search took {time.time() - pt_initial_search_start:.2f}s")
            
            no_results = await page.locator("text=We're waiting on house and pet sitting opportunities").count() > 0
            if no_results:
                logging.info(f"No results available for profile {profile_name} after initial search.")
                if browser.is_connected():
                    await browser.close()
                return pd.DataFrame()

            # Timing for scraping all listing data from results pages
            pt_scrape_run_start = time.time()
            all_listings_data = await scrape_run(page, test_mode)
            logging.info(f"Profile {profile_name}: scrape_run (scraped {len(all_listings_data)} raw results) took {time.time() - pt_scrape_run_start:.2f}s")

        except Exception as e:
            logging.critical(f"Scraping failed for profile {profile_name}: {e}", exc_info=True)
            html_content = await page.content() 
            with open(f"crash_dump_{profile_name}.html", "w") as f:
                f.write(html_content)
            await page.screenshot(path=f"crash_screenshot_{profile_name}.png", full_page=True) # Error screenshot
            if browser.is_connected():
                 await browser.close()
            return pd.DataFrame() 
        finally:
            if browser.is_connected(): 
                 await browser.close()

    # Timing for DataFrame creation and initial processing after scraping
    pt_df_creation_start = time.time()
    if not all_listings_data:
        logging.warning(f"No listings data retrieved for profile {profile_name}")
        return pd.DataFrame()

    base_df = pd.DataFrame(all_listings_data)
    if base_df.empty:
        logging.info(f"DataFrame is empty for {profile_name} after processing listings.")
        return base_df

    base_df['public_transport'] = base_df.get('public_transport_available', pd.Series(dtype=bool)).astype(bool)
    base_df['car_included'] = base_df.get('car_is_included', pd.Series(dtype=bool)).astype(bool)
    base_df['unique_key'] = base_df['listing_id'] + '|' + base_df['date_range']
    base_df['profile'] = profile_name
    
    # This loop can be verbose, consider removing or making conditional if too noisy for production.
    # For now, it's kept for detailed verification of the extracted boolean flags.
    # for idx, row in base_df.iterrows():
    #     logging.info(f"Listing {row['listing_id']} - Public transport: {row['public_transport']}, Car included: {row['car_included']}")
    logging.info(f"Profile {profile_name}: DataFrame creation and initial processing took {time.time() - pt_df_creation_start:.2f}s")

    logging.info(f"Profile {profile_name} completed in {time.time() - overall_profile_processing_start_time:.2f}s, processed {len(base_df)} listings")
    return base_df

# --- Main execution block ---
async def main(test_mode=False) -> None:
    logging.info("Starting scrape")
    overall_main_start_time = time.time() 

    # Timing for loading profile configurations
    mt_load_profiles_start = time.time()
    profiles = load_profiles()
    logging.info(f"Loaded {len(profiles)} search profiles in {time.time() - mt_load_profiles_start:.2f}s")

    # Inner function to wrap process_profile for asyncio.gather, does not need separate timing here.
    async def run_profile(name, config): 
        try:
            return name, await process_profile(name, config, test_mode)
        except Exception as e:
            logging.error(f"Failed to process profile {name}: {e}", exc_info=True)
            return name, e

    # Timing for processing all profiles concurrently
    mt_gather_profiles_start = time.time()
    results = await asyncio.gather(*(run_profile(name, config) for name, config in profiles.items()))
    logging.info(f"All profiles processed (asyncio.gather) in {time.time() - mt_gather_profiles_start:.2f}s")

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

    # Timing for loading old data from JSON
    if os.path.exists(JSON_PATH) and os.path.getsize(JSON_PATH) > 0:
        try:
            mt_load_old_df_start = time.time()
            old_df = pd.read_json(JSON_PATH)
            logging.info(f"Loaded old_df from {JSON_PATH} in {time.time() - mt_load_old_df_start:.2f}s")
        except Exception as e:
            logging.warning(f"Bad JSON, reset: {e}")
            old_df = pd.DataFrame()
    else:
        old_df = pd.DataFrame()

    # Ensure essential columns exist in old_df with correct dtypes before merging
    old_df['public_transport'] = old_df.get('public_transport', pd.Series(dtype=bool)).fillna(False).astype(bool)
    old_df['car_included'] = old_df.get('car_included', pd.Series(dtype=bool)).fillna(False).astype(bool)
    default_fs = (datetime.utcnow() - timedelta(seconds=1)).isoformat() + 'Z'
    old_df['first_seen'] = old_df.get('first_seen', default_fs)
    old_df['last_changed'] = old_df.get('last_changed', old_df['first_seen'])

    if 'unique_key' not in old_df.columns and not old_df.empty:
         old_df['unique_key'] = old_df.apply(
             lambda r: listing_id_from_url(r.get('url', '')) + '|' + f"{r.get('date_from', '')}→{r.get('date_to', '')}", axis=1
         )
    elif old_df.empty: # Ensure 'unique_key' column exists even if old_df is empty for consistent merging
        old_df['unique_key'] = pd.Series(dtype=str)

    # Timing for merging old and new data
    mt_merge_data_start = time.time()
    merged = old_df.set_index('unique_key').combine_first(base_df.set_index('unique_key'))
    logging.info(f"Merged old and new data in {time.time() - mt_merge_data_start:.2f}s")

    # Timing for final DataFrame processing (updates, new/expired flags)
    mt_final_df_processing_start = time.time()
    now = datetime.utcnow().isoformat() + 'Z'
    for uk, row in merged.iterrows():
        if uk in base_df['unique_key'].values: # Process only rows that were in the latest scrape
            nr = base_df.loc[base_df['unique_key'] == uk].iloc[0] # New row from current scrape
            orow_series_list = old_df.loc[old_df['unique_key'] == uk] if uk in old_df['unique_key'].values else []
            
            changed = True # Assume changed if it's a new row
            if len(orow_series_list) > 0: # If it existed in old data
                orow = orow_series_list.iloc[0]
                # Check for changes only if it's not a new row and columns exist in both
                changed = any(nr[col] != orow[col] for col in CONTENT_COLS + ['public_transport', 'car_included'] if col in orow and col in nr)
            
            merged.at[uk, 'last_changed'] = now if changed else row.get('last_changed', now) # Use existing if no change
            merged.at[uk, 'profile'] = nr['profile'] # Update profile from current scrape

    merged['first_seen'] = merged['first_seen'].fillna(now)
    df = merged.reset_index()
    df['new_this_run'] = df['first_seen'] == now

    # Expired data handling: identify listings no longer present in the scrape
    old_keys = set(old_df['unique_key'].dropna()) 
    new_keys = set(df['unique_key'].dropna())
    exp_keys = old_keys - new_keys
    exp_df = old_df[old_df['unique_key'].isin(exp_keys)].copy()
    if not exp_df.empty:
        exp_df['expired'] = True
        exp_df['new_this_run'] = False
    else: # Ensure columns exist even if exp_df is empty, for robust concat
        exp_df = pd.DataFrame(columns=df.columns.tolist() + ['expired'])

    df['expired'] = False
    out_df = pd.concat([df, exp_df], ignore_index=True)
    logging.info(f"Final DataFrame processing took {time.time() - mt_final_df_processing_start:.2f}s")

    out_df.to_csv(CSV_PATH, index=False, quoting=csv.QUOTE_NONNUMERIC)
    out_df.to_json(JSON_PATH, orient='records', indent=2)
    logging.info(f"Saved {len(out_df)} records")

    # Timing for sending all Telegram notifications
    mt_telegram_total_start = time.time()
    for profile_name, profile_config in profiles.items():
        # Filter for new, non-expired listings for the current profile before applying further filters
        profile_df = out_df[(out_df['new_this_run']) & (out_df['profile'] == profile_name) & (out_df['expired'] == False)] 
        profile_df = apply_profile_filters(profile_df, profile_config)

        if profile_df.empty:
            logging.info(f"No new listings to alert for profile {profile_name}")
        else:
            logging.info(f"Sending {len(profile_df)} alerts for profile {profile_name}")
            send_telegram_message(format_telegram_message(profile_df.to_dict('records'), profile_config))
    logging.info(f"Total time for sending all Telegram notifications took {time.time() - mt_telegram_total_start:.2f}s")
    
    logging.info(f"Done in {time.time() - overall_main_start_time:.2f}s")

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument('--test', action='store_true')
        args = parser.parse_args()
        asyncio.run(main(test_mode=args.test))
    except Exception:
        logging.critical("Unhandled exception in main", exc_info=True)
        raise
