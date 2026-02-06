from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import csv
import re
import time
import tldextract
import random

# =========================
# CONFIG (EDIT ONLY THIS)
# =========================

TARGET_LEADS = 20      # 👈 change this anytime

SEARCH_KEYWORDS = [
    "real estate photographer",
    "real estate photography",
    "property photographer",
    "real estate videographer",
    "real estate drone photographer",
    "Matterport photographer",
    "real estate virtual tour"
]

STATES = [
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado",
    "Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho",
    "Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana",
    "Maine","Maryland","Massachusetts","Michigan","Minnesota",
    "Mississippi","Missouri","Montana","Nebraska","Nevada",
    "New Hampshire","New Jersey","New Mexico","New York",
    "North Carolina","North Dakota","Ohio","Oklahoma","Oregon",
    "Pennsylvania","Rhode Island","South Carolina","South Dakota",
    "Tennessee","Texas","Utah","Vermont","Virginia","Washington",
    "West Virginia","Wisconsin","Wyoming"
]

COUNTRY = "United States"
HEADLESS = False        # keep False to reduce Google blocks
SCROLL_TIMES = 6
SCROLL_DELAY = 2

FAST_MODE = True           # set False if Google starts blocking
MAX_CARDS_PER_SEARCH = 15  # limit businesses checked per search


# =========================
# REGEX
# =========================

EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
PHONE_REGEX = r"(\+1[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{4})"


# =========================
# HELPERS
# =========================

def extract_email(text):
    emails = re.findall(EMAIL_REGEX, text)
    return emails[0] if emails else None


def extract_phone(text):
    phones = re.findall(PHONE_REGEX, text)
    for phone in phones:
        digits = re.sub(r"\D", "", phone)
        if len(digits) >= 10:
            return phone
    return None


def extract_instagram(soup):
    for a in soup.find_all("a", href=True):
        if "instagram.com" in a["href"]:
            return a["href"]
    return None


def scrape_website(url):
    try:
        r = requests.get(
            url,
            timeout=5,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ")

        email = extract_email(text)
        phone = extract_phone(text)
        instagram = extract_instagram(soup)

        has_contact_page = bool(
            soup.find("a", href=lambda x: x and "contact" in x.lower())
        )

        return {
            "email": email,
            "phone": phone,
            "instagram": instagram,
            "has_instagram": bool(instagram),
            "has_contact_page": has_contact_page
        }
    except:
        return {}


def is_valid_lead(lead):
    return bool(lead.get("email")) and bool(lead.get("phone"))


# =========================
# GOOGLE MAPS SCRAPER
# =========================

def scrape_google_maps(page, keyword, state):
    results = []

    query = f"{keyword} in {state}, United States"
    url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
    page.goto(url, timeout=60000)
    time.sleep(5)

    scrolls = 3 if FAST_MODE else SCROLL_TIMES
    for _ in range(scrolls):
        page.mouse.wheel(0, 4000)
        time.sleep(SCROLL_DELAY)

    cards = page.locator('//div[@role="article"]')
    count = cards.count()
    if FAST_MODE:
        count = min(count, MAX_CARDS_PER_SEARCH)

    for i in range(count):
        try:
            card = cards.nth(i)

            name = card.locator('.fontHeadlineSmall').inner_text()
            phone = card.locator('text=/\\+1|\\(/').first.inner_text()
            website = card.locator('a[data-value="Website"]').get_attribute("href")

            address_text = ""
            try:
                address_text = card.locator('button[data-item-id="address"]').inner_text()
            except:
                pass

            city = ""
            if address_text:
                parts = [p.strip() for p in address_text.split(",")]
                if len(parts) >= 2:
                    city = parts[-3] if len(parts) >= 3 else parts[0]

            results.append({
                "business_name": name.strip(),
                "phone": phone.strip(),
                "website": website,
                "google_maps_url": page.url,
                "address": address_text,
                "city": city
            })
        except:
            continue

    return results


# =========================
# MAIN
# =========================

def main():
    leads = []
    seen = set()

    with sync_playwright() as p:
        random.shuffle(STATES)
        random.shuffle(SEARCH_KEYWORDS)
        browser = p.chromium.launch(headless=HEADLESS)
        page = browser.new_page()

        for state in STATES:
            for keyword in SEARCH_KEYWORDS:
                if len(leads) >= TARGET_LEADS:
                    break

                businesses = scrape_google_maps(page, keyword, state)
                random.shuffle(businesses)

                for biz in businesses:
                    if len(leads) >= TARGET_LEADS:
                        break

                    if not biz.get("website"):
                        continue

                    site_data = scrape_website(biz["website"])

                    lead = {
                        "business_name": biz["business_name"],
                        "email": site_data.get("email"),
                        "phone": site_data.get("phone") or biz.get("phone"),
                        "website": biz["website"],
                        "website_domain": tldextract.extract(biz["website"]).registered_domain,
                        "instagram": site_data.get("instagram"),
                        "has_instagram": site_data.get("has_instagram"),
                        "google_maps_url": biz["google_maps_url"],
                        "address": biz.get("address", ""),
                        "city": biz.get("city", ""),
                        "state": state,
                        "country": COUNTRY,
                        "has_contact_page": site_data.get("has_contact_page")
                    }

                    if not is_valid_lead(lead):
                        continue

                    key = (lead["website_domain"], lead["phone"])
                    if key in seen:
                        continue
                    seen.add(key)
                    leads.append(lead)
                    print(f"[+] {len(leads)} | {lead['business_name']}")

        browser.close()

    if not leads:
        print("No valid leads found.")
        return

    with open("us_real_estate_photographers.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=leads[0].keys())
        writer.writeheader()
        writer.writerows(leads)

    print(f"\n✅ Saved {len(leads)} leads to CSV")


if __name__ == "__main__":
    main()