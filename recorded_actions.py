import re
from playwright.sync_api import Playwright, sync_playwright, expect


def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("https://www.trustedhousesitters.com/house-and-pet-sitting-assignments/")
    page.get_by_role("textbox", name="Search for a location").click()
    page.get_by_role("textbox", name="Search for a location").fill("Europe")
    page.get_by_role("textbox", name="Search for a location").click()
    page.get_by_role("textbox", name="Search for a location").fill("Europe ")
    page.get_by_text("Europe").click()
    page.get_by_role("button", name="Dates").click()
    page.get_by_role("button", name="chevron-right").click()
    page.get_by_role("button", name="chevron-right").click()
    page.get_by_role("button", name="chevron-right").click()
    page.get_by_role("button", name="chevron-right").click()
    page.get_by_label("27 Dec 2025 Saturday").click()
    page.get_by_role("button", name="chevron-right").click()
    page.get_by_role("button", name="chevron-right").click()
    page.get_by_label("15 Feb 2026 Sunday").click()
    page.get_by_role("button", name="Apply").click()
    page.get_by_role("link", name="Go to next page").click()
    page.get_by_role("link", name="Go to next page").click()
    page.close()

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
