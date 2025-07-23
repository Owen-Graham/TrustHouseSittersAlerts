# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **TrustedHousesitters scraper** that monitors house/pet sitting opportunities and sends Telegram notifications. The application scrapes house sitting listings from trustedhousesitters.com, applies configurable filters, and alerts users when new matching listings are found.

## Key Architecture

### Core Components

- **scraper.py** - Main application file containing all scraping logic, filtering, and notification systems
- **filter_profiles.json** - Configuration for different search profiles (locations, date ranges, filters, notification settings)
- **data/** - Directory containing persistent data files:
  - **data/sits.csv** - CSV export of scraped listings
  - **data/sits.json** - JSON storage with tracking of new/changed entries
- **debug/** - Directory containing debug and log files:
  - **debug/scraper.log** - Application logs
  - **debug/*.png** - Screenshots for debugging browser interactions
  - **debug/*.html** - HTML dumps of failed pages
- **requirements.txt** - Python dependencies (Playwright, pandas, requests, python-dotenv)

### Data Flow

1. **Search Execution**: For each profile, the scraper navigates to TrustedHousesitters, sets location/date filters
2. **Data Extraction**: Scrapes listing cards to extract title, location, dates, pet information, transport options
3. **Filtering**: Applies profile-specific filters (excluded countries, max pets)
4. **Change Detection**: Compares with previous data to identify new/modified listings
5. **Notifications**: Sends formatted Telegram messages for new listings matching profile criteria

### Profile System

Profiles are defined in `filter_profiles.json` with:
- **search**: location, date_from, date_to
- **filters**: excluded_countries, max_pets
- **notification**: custom header and icon for Telegram messages

Example profile structure:
```json
{
  "southern_europe": {
    "search": {
      "location": "Europe",
      "date_from": "27 Dec 2025", 
      "date_to": "15 Feb 2026"
    },
    "filters": {
      "excluded_countries": ["United Kingdom", "Ireland"],
      "max_pets": {}
    }
  }
}
```

## Development Commands

### Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
playwright install
```

### Running the Scraper
```bash
# Full run
python scraper.py

# Test mode (limited to 2 cards per page)
python scraper.py --test

# Using the convenience script
./run.sh
```

### Environment Variables
Required for Telegram notifications:
- `TELEGRAM_BOT_TOKEN` - Bot token from @BotFather
- `TELEGRAM_CHAT_ID` - Target chat ID for notifications

## Browser Automation Details

The scraper uses Playwright with careful human-like behavior:
- **Headless Mode**: Controlled by `HEADLESS` variable in scraper.py (set to False for debugging)
- **Wait Patterns**: Random delays between actions to mimic human interaction
- **Error Handling**: Extensive screenshot and HTML dumping on failures for debugging
- **Date Navigation**: Complex calendar navigation logic with multiple fallback methods

### Debugging Features

When errors occur, the scraper generates:
- Screenshots at key points (`debug/debug_*.png`, `debug/error_*.png`)
- HTML dumps of failed pages (`debug/crash_dump_*.html`, `debug/debug_*.html`)
- Detailed logging to `debug/scraper.log`

## GitHub Actions Integration

Automated via `.github/workflows/main.yml`:
- **Schedule**: Runs every 10 minutes during 08:00-00:00 Perth time (UTC+8)
- **Manual Trigger**: Available via workflow_dispatch
- **Auto-commit**: Updates data/sits.csv and data/sits.json with new data
- **Artifact Upload**: Preserves logs and debug files for troubleshooting

## Data Schema

### Core Fields (scraper.py:38)
```python
CONTENT_COLS = ["title", "location", "town", "country", "date_from", "date_to", "reviewing"] + PET_TYPES
PET_TYPES = ["dog", "cat", "horse", "bird", "fish", "rabbit", "reptile", "poultry", "livestock", "small_pets"]
```

### Tracking Fields
- `unique_key`: listing_id + date_range for deduplication
- `first_seen`: Timestamp when listing was first discovered
- `last_changed`: Timestamp of last content modification
- `new_this_run`: Boolean flag for current scraping session
- `expired`: Boolean flag for listings no longer available

## Common Issues & Solutions

### Date Selection Failures
The scraper includes multiple fallback methods for calendar date selection (lines 304-342, 367-406 in scraper.py). If date selection fails, check:
1. Date format in profile configuration
2. Calendar navigation logic
3. Screenshot files for visual debugging

### Profile Configuration
- Location strings must match TrustedHousesitters search autocomplete options
- Date format: "DD MMM YYYY" (e.g., "27 Dec 2025")
- Excluded countries use exact country names as they appear in listings

### Filter Application
Transport filters are applied after initial search:
- `public_transport`: "Accessible by public transport"
- `car_included`: "Use of car included"
- Filter failures generate debug screenshots for troubleshooting