import re
import json
import emoji
import requests
import gspread
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

# =========================
# CONFIG
# =========================
SHEET_NAME = "Raw Leads"                  # change if needed
SERVICE_ACCOUNT_FILE = "google_sheets_key.json"

OPENAI_API_KEY = "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"
MODEL = "gpt-4o-mini"

COL_CHANNEL_NAME = 1   # Column A
COL_YT_LINK = 2        # Column B
COL_FIRST_NAME = 4     # Column D
COL_EMAIL = 5          # Column E

client = OpenAI(api_key=OPENAI_API_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

CORP_SUFFIXES = [
    "inc", "ltd", "llc", "official", "studio",
    "network", "group", "company", "corp", "media"
]

# =========================
# CLEANING
# =========================

def clean_channel_name(name: str) -> str:
    if not name:
        return ""

    # remove emojis
    name = emoji.replace_emoji(name, "")

    # remove weird characters (numbers allowed)
    name = re.sub(r"[^\w\s\-]", "", name)

    # remove corporate suffixes
    for suf in CORP_SUFFIXES:
        name = re.sub(rf"\b{suf}\b", "", name, flags=re.I)

    # normalize spaces
    name = re.sub(r"\s+", " ", name).strip()

    # normalize casing
    return name.title()

# =========================
# SCRAPING
# =========================

def fetch_text(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "lxml")
            return soup.get_text(" ", strip=True)
    except Exception:
        pass
    return ""

def get_external_texts(yt_link: str):
    texts = []

    about_url = yt_link.rstrip("/") + "/about"

    about_text = fetch_text(about_url)

    if about_text:
        texts.append(about_text)

        soup = BeautifulSoup(about_text, "lxml")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "youtube.com" not in href:
                page_text = fetch_text(href)
                if page_text:
                    texts.append(page_text)
    return texts

# =========================
# AI LOGIC
# =========================

def ai_extract_first_name(payload: dict):
    prompt = f"""
You are extracting a creator's FIRST NAME.

Rules:
- Return only ONE first name
- Must be a real human name
- Ignore brands, companies, and media houses
- Use all context provided
- Normalize casing
- If uncertain, return null
- Do NOT hallucinate

Context:
{json.dumps(payload, indent=2)}
"""

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You extract first names accurately."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
    except Exception as e:
        print("OpenAI error:", e)
        return None

    result = response.choices[0].message.content.strip()
    return None if result.lower() == "null" else result

# =========================
# MAIN
# =========================

def main():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE, scope
    )

    gs_client = gspread.authorize(creds)
    sheet = gs_client.open(SHEET_NAME).sheet1
    rows = sheet.get_all_values()

    for i in range(1, len(rows)):
        row = rows[i]

        # Skip if Column D already has a value
        if len(row) >= COL_FIRST_NAME and row[COL_FIRST_NAME - 1].strip():
            continue

        channel_name = row[COL_CHANNEL_NAME - 1] if len(row) >= COL_CHANNEL_NAME else ""
        yt_link = row[COL_YT_LINK - 1] if len(row) >= COL_YT_LINK else ""
        email = row[COL_EMAIL - 1] if len(row) >= COL_EMAIL else ""

        external_texts = get_external_texts(yt_link) if yt_link else []

        payload = {
            "channel_name": channel_name,
            "email": email,
            "youtube_about_text": external_texts[0] if external_texts else "",
            "external_pages_text": external_texts
        }

        first_name = ai_extract_first_name(payload)

        if first_name:
            source = "Channel Name inference"
            name_lower = first_name.lower()

            if any(name_lower in text.lower() for text in external_texts[1:]):
                source = "External Website"
            elif external_texts and name_lower in external_texts[0].lower():
                source = "YouTube About"
            elif email and name_lower in email.lower():
                source = "Email"

            final_value = first_name
            print(f"Row {i + 1}: AI -> {final_value} (source: {source})")
        else:
            final_value = clean_channel_name(channel_name)
            print(f"Row {i + 1}: Fallback -> {final_value}")

        sheet.update_cell(i + 1, COL_FIRST_NAME, final_value)

    print("✅ Done")

if __name__ == "__main__":
    main()