consecutive_quota_errors = 0
stop_scraping = False
import random
import time
import sys
import signal
import os
import re
import json
from datetime import datetime, timezone
import requests

existing_ids_global = set()
# Always define this at the very top
unique_added_this_run = 0
# Tracks number of unique leads actually appended to Google Sheet this run
unique_appended_this_run = 0

from bs4 import BeautifulSoup
import pandas as pd

# === Slack Notification Setup ===
SLACK_WEBHOOK = "https://hooks.slack.com/services/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"

def send_slack_message(text):
    """Send a message to Slack channel via webhook."""
    headers = {"Content-Type": "application/json"}
    data = {"text": text}
    try:
        requests.post(SLACK_WEBHOOK, headers=headers, json=data)
    except Exception as e:
        print(f"[Slack] Failed to send notification: {e}")

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from openai import OpenAI

# Import langdetect for language detection
try:
    from langdetect import detect, DetectorFactory
    DetectorFactory.seed = 0
except ImportError:
    detect = None

# Ensure these counters always exist
added_now = 0
added_final = 0

# optional transcript library; not required
try:
    from youtube_transcript_api import YouTubeTranscriptApi
except Exception:
    YouTubeTranscriptApi = None

# === CONFIG ===

API_KEYS = [
    "AIzaSyDe7axLxXM73cBwgWVIiExaxFPo504cTPA",
    "AIzaSyDhe5odit54OSGWZ4BxDw4-5WO76uYBffE",
    "AIzaSyB2p8GXoG_AOm9TEDmqpoQiHoyO7IIe3eU",
    "AIzaSyC2m0NfOQKRuqDPhGucSw22ih3TrYbVoVY",
    "AIzaSyA8cBjcsrJQCtbh0fID0BUDdAZB8jwG5vI",
    "AIzaSyC3ziU3IdKt91DFt5XGE4HVepSYs4PDlA4"
]

# OpenAI key via env var preferred
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or "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"

# Parameters
MIN_SUBS = 7000
MAX_SUBS = 150000
MAX_VIDEO_AGE_DAYS = 180       # only consider videos <= 180 days old
TARGET_LEADS = 170              # collect 15 qualified leads (rating >= 7)
DELAY_BETWEEN_REQUESTS = 1.2   # seconds between API calls
AUTO_SAVE_EVERY = 5            # autosave after every N saved leads
MAX_VIDEOS_PER_PAGE = 50       # YouTube search maxResults (use 50 for broader coverage per call)
KEYWORD_TITLES_MIN = 25         # generate between 25 and 30 video-title keywords
KEYWORD_TITLES_MAX = 30

# Lock country suffixes: LOCATION_SUFFIXES mirrors the expanded ALLOWED_COUNTRIES set used in filtering.
LOCATION_SUFFIXES = ["US", "GB", "CA", "AU", "AE", "NZ"]

OUTPUT_EXCEL = "qualified_youtube_leads.xlsx"

# Google Sheets setup
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS = ServiceAccountCredentials.from_json_keyfile_name("google_sheets_key.json", SCOPE)
GSPREAD_CLIENT = gspread.authorize(CREDS)
SHEET = GSPREAD_CLIENT.open_by_key("1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE").worksheet("Raw Leads")

# instantiate clients
youtube_clients = [build('youtube', 'v3', developerKey=key) for key in API_KEYS]
openai_client = OpenAI(api_key=OPENAI_API_KEY)

STATE_FILE = "scraper_state.json"

# state
collected_channels = set()   # channels we've already processed in this run
qualified_leads = []         # only stores leads with rating >= 7

# ---------- helpers ----------

def signal_handler(sig, frame):
    print("\n[Exit] Received interrupt, stopping scraper gracefully...")
    # raise KeyboardInterrupt to let the main try/except/finally flow handle cleanup
    raise KeyboardInterrupt

signal.signal(signal.SIGINT, signal_handler)

def is_recent(published_at):
    try:
        published_date = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - published_date).days <= MAX_VIDEO_AGE_DAYS
    except Exception:
        return False

def is_english(text):
    if not text:
        return False
    # Use langdetect if available
    if detect:
        try:
            lang = detect(text)
            return lang == "en"
        except Exception:
            pass
    # Fallback: ascii ratio
    ascii_chars = sum(c.isascii() for c in text)
    return len(text) > 0 and (ascii_chars / len(text) > 0.8)

def safe_request_get(url, timeout=8):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        return requests.get(url, headers=headers, timeout=timeout)
    except Exception:
        return None

def scrape_website_for_email(url):
    if not url or not url.startswith("http"):
        return None
    try:
        r = safe_request_get(url)
        if not r or r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        mailtos = soup.select('a[href^=mailto]')
        if mailtos:
            return mailtos[0]['href'].replace('mailto:', '').strip()
        emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-z]{2,}", r.text)
        return emails[0] if emails else None
    except Exception:
        return None


def looks_high_ticket_simple(text):
    """
    Conservative high-ticket detector for scraper old: 
    - Accepts a string (bio text or URL).
    - Returns True if it contains platform anchors, high-ticket phrases, or explicit price >= $1000.
    """
    try:
        if not text:
            return False
        low = str(text).lower()
        # Quick domain/platform anchors
        anchors = [
            "skool.com","kajabi.com","teachable.com","thinkific.com","podia.com",
            "systeme.io","kartra.com","clickfunnels.com","calendly.com","skool",
            "stan.store","stanstore"
        ]
        for a in anchors:
            if a in low:
                return True
        # Selling phrases
        phrases = [
            "coaching", "mentorship", "masterclass", "apply", "book a call",
            "strategy session", "bootcamp", "enroll", "program", "academy",
            "membership", "masterclass", "one-on-one", "1:1", "private coaching"
        ]
        for p in phrases:
            if p in low:
                return True
        # Price detection: $1,000 or more patterns
        money_matches = re.findall(r'[\$€£]\s?[\d,]+(?:\.\d{2})?', low)
        for m in money_matches:
            num = re.sub(r'[^\d.]', '', m).replace(',', '')
            try:
                if float(num) >= 1000:
                    return True
            except:
                continue
        return False
    except Exception:
        return False



# ---------- OpenAI interactions ----------


def generate_keywords_with_openai(n_min=KEYWORD_TITLES_MIN, n_max=KEYWORD_TITLES_MAX):
    """
    Generate YouTube video title keywords using OpenAI (gpt-4o-mini) with detailed example prompt.
    Retries up to 3 times on failure, returns [] if all attempts fail.
    Ensures content is non-empty and valid JSON.
    """
    import json, time, random

    n_target = random.randint(n_min, n_max)

    prompt = """
You are an expert YouTube researcher helping a growth operator find creators who teach real, monetizable skills.

Your job: generate a large, diverse list of realistic YouTube video titles that represent creators whose audiences would pay for coaching, courses, or Skool communities.

These should be creators who teach, share, or demonstrate *a skill, method, or transformation* — not vloggers or entertainers.

Return ONLY a valid JSON array of objects.
Each object must have these keys:
{
  "title": "string — a realistic YouTube video title (6–12 words)",
  "niche": "string — short phrase naming the creator's main niche",
  "problem_solved": "string — short line describing the transformation or skill taught",
  "audience_profile": "string — who would pay for this (e.g. beginners, hobbyists, freelancers, men 20-35, etc.)",
  "monetization_fit": "one of ['course','skool_community','coaching','template_pack','membership','unknown']",
  "is_local_service": true/false (true if it depends on in-person/local delivery)
}

### GUIDELINES

1. **Core idea:** Pick niches where you could easily build a digital product or Skool community.
   - Anything where people learn, practice, or improve a skill.
   - Anything where people want transformation (health, mindset, performance, relationships, creative mastery).
   - Avoid vloggers, podcasts, pure entertainment, or local service businesses.

2. **Diversity requirement:** 
   - Don’t stick to the same 20–30 niches. Cover hundreds of possible fields across *arts, skills, hobbies, professions, and self-improvement topics*.
   - Go beyond the classic “health, wealth, relationships” categories. Include blue ocean niches where other growth operators rarely look — areas of genuine passion, creativity, and niche mastery that still have monetizable audiences.
   - Example categories to sample from:
     - Arts & Creative: calligraphy, journaling, drawing, painting, pottery, woodworking, crocheting, knitting, embroidery, resin art, candle making, soap making.
     - Music & Performance: guitar, piano, singing, music production, songwriting, DJing, dance, acting, public speaking.
     - Digital Skills: video editing, 3D printing, graphic design, animation, coding, app development, Photoshop, filmmaking.
     - Personal Growth: masculinity, mindset, fitness, meditation, productivity, confidence, habits, discipline, dating, breathwork.
     - Animals & Nature: dog training, horse care, aquarium setup, gardening, bonsai, aquascaping, permaculture, homesteading.
     - Food & Lifestyle (educational): sourdough baking, meal prep, barista skills, nutrition, fermentation, vegan cooking.
     - Education & Communication: language learning, pronunciation, public speaking, teaching skills, storytelling, coaching communication.
     - Handcraft & Repair: leatherwork, jewelry making, electronics repair, bike maintenance, car detailing, knife making, DIY home repair.
     - Niche Professions: UX/UI, freelancing, photography, drone cinematography, Shopify, eBay flipping, niche e-commerce, course creation, digital marketing (ethical).
     - Spiritual & Esoteric (educational only): tarot reading, astrology, manifestation, energy work, meditation, yoga teaching.
     - Blue Ocean Niches (uncommon but monetizable):
         - Memory improvement, handwriting analysis, lucid dreaming, creative journaling, emotional release, minimalism, decluttering, ethical hacking, drone repair, sound healing, intuitive eating, vocal toning, self-defense, woodworking restoration, AI voiceover tutorials, kinetic typography, AR/VR art, board game design, homestead architecture, mapmaking, foraging, mushroom cultivation, survival skills, somatic movement, philosophy simplification, life journaling, and historical re-enactment education.

3. **Bad/Skip niches:** 
   - Local-only services (plumbing, real estate, solar, pest control, salons, construction).
   - Medical or regulated services (doctors, dentists, therapists, clinics, pharmacies).
   - Lifestyle vlogs, general podcasts, entertainment/news channels.
   - Influencer or fashion/beauty content.
   - Anything that can’t scale digitally.

4. **Content style:**
   - Natural, real YouTube titles like:
     - “How I Fixed My Dog’s Separation Anxiety”
     - “Sourdough Starter Guide for Absolute Beginners”
     - “My Bullet Journaling System for Focus & Clarity”
     - “How I Learned 3D Printing in 30 Days”
     - “5 Mistakes New Woodworkers Make (and How to Avoid Them)”
     - “Learn Tarot Card Meanings the Simple Way”
     - “Guitar Warmups That Changed My Playing Forever”
     - “From Couch to Confident: Men’s Self-Mastery Routine”
   - Avoid clickbait or overly salesy tone.

5. **Monetization relevance:**
   - The creator should be *teaching or demonstrating* something their audience wants to master.
   - Their audience should logically pay for structured learning, accountability, templates, or coaching.
   - Assume these creators could afford $200–$500/month in software & tools.

6. **Output Requirements:**
   - Return 60–100 unique JSON objects.
   - Ensure at least 70% have `"is_local_service": false`.
   - Spread evenly across at least 10 broad categories, including at least 10 blue ocean niches.
   - Avoid duplicate or highly similar niches.

### EXAMPLE OUTPUT
[
  {"title":"How I Tamed My Reactive Dog in 3 Weeks","niche":"dog training","problem_solved":"stop barking and leash aggression","audience_profile":"pet owners who want obedient dogs","monetization_fit":"course","is_local_service":false},
  {"title":"My Journaling Routine for Mental Clarity","niche":"journaling","problem_solved":"reduce anxiety and build focus","audience_profile":"young professionals and students","monetization_fit":"skool_community","is_local_service":false},
  {"title":"Mastering Hand-Carved Furniture from Scratch","niche":"woodworking","problem_solved":"build furniture with hand tools","audience_profile":"DIY hobbyists and aspiring craftsmen","monetization_fit":"course","is_local_service":false},
  {"title":"How to Read Tarot Cards for Beginners","niche":"tarot reading","problem_solved":"understand symbolism and intuition","audience_profile":"spiritual learners","monetization_fit":"membership","is_local_service":false},
  {"title":"Learn Crochet Patterns in 7 Days","niche":"crocheting","problem_solved":"make wearable patterns easily","audience_profile":"craft lovers, homemakers","monetization_fit":"course","is_local_service":false},
  {"title":"Lucid Dreaming Techniques for Beginners","niche":"lucid dreaming","problem_solved":"control your dreams consciously","audience_profile":"spiritual self-explorers and meditators","monetization_fit":"skool_community","is_local_service":false},
  {"title":"Foraging Wild Edible Plants in Your Area","niche":"foraging","problem_solved":"identify and safely harvest wild foods","audience_profile":"nature enthusiasts and survivalists","monetization_fit":"course","is_local_service":false}
]
"""

    attempts, backoff = 3, 1
    for i in range(1, attempts + 1):
        try:
            resp = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"user","content":prompt}],
                temperature=0.5,
                max_tokens=800
            )

            content = getattr(resp.choices[0].message, "content", "").strip()
            if not content:
                print(f"[OpenAI Keywords] Empty response on attempt {i}.")
                if i < attempts:
                    time.sleep(backoff); backoff *= 2
                    continue
                return []

            try:
                # Remove Markdown code fences if present
                content = re.sub(r"^```(?:json)?\s*", "", content)
                content = re.sub(r"\s*```$", "", content)
                content = content.strip()
                keywords = json.loads(content)
                if isinstance(keywords, list) and keywords:
                    return [k for k in keywords if isinstance(k, str)]
                else:
                    print(f"[OpenAI Keywords] Response not a valid list (attempt {i}). Raw:", content)
            except Exception as e:
                print(f"[OpenAI Keywords] JSON parse error (attempt {i}): {e}")
                print("[OpenAI Keywords Raw Response]", content)

        except Exception as e:
            print(f"[OpenAI Keywords] Attempt {i} failed: {e}")

        if i < attempts:
            time.sleep(backoff); backoff *= 2
            continue
        else:
            print("[OpenAI Keywords] All attempts failed. Returning [].")
            return []

    return []

def rate_lead_with_openai(channel_title, channel_description, avg_views, titles_pipe, bio_links=""):
    """
    Improved evaluator that returns a structured JSON response with a rating, reason, and action.
    Prioritizes creators who teach monetizable skills and do not already sell high-ticket offers.
    """
    prompt = f"""
You are an expert talent evaluator for creator monetization. Based on the inputs below, produce a careful assessment about whether this YouTube channel is a high-potential lead for building a Skool community, course, or high-ticket offer that the creator does NOT already sell — or whether they already sell such products.

INPUT:
Channel Title: {channel_title}
Channel Description: {channel_description}
Recent Video Titles (pipe separated): {titles_pipe}
Average Views per Video: {avg_views}
Bio Links (pipe separated, if any): {bio_links}

Consider all of the following when judging:
- Is the creator teaching a repeatable skill or transformation (e.g., 3D printing, dog training, woodworking, guitar, lucid dreaming, tarot, AI voiceover, etc.) — NOT lifestyle vlogs, local-only services, medical/legal, or pure entertainment?
- Does the channel already advertise paid products, communities, or booking (domains like skool.com, kajabi.com, teachable.com, thinkific.com, podia.com, stan.store, calendly.com, linktr.ee, gumroad.com, buymeacoffee.com, patreon.com, or phrases like "book a call", "enroll", "join my", "coaching", "masterclass") in the bio or bio-links?
- Does any bio link URL path contain selling keywords (e.g., /coaching, /course, /academy) even if domain is first-party?
- Audience quality: are avg views and recent activity consistent with paying customers? (we want creators whose audience can plausibly spend on tools/ads ~$200–$500/mo)
- Language: non-English channels should be scored lower unless it's clearly in an English monetizable niche.

OUTPUT: Return ONLY a valid JSON object with these fields exactly:
{{"rating": <integer 0-10>,
  "reason_code": "<one of: HIGH_TICKET_PRESENT, ALREADY_MONETIZING, GOOD_PROSPECT, BORDERLINE, LOW_POTENTIAL, NON_ENGLISH>",
  "primary_signals": ["signal1","signal2",...],
  "recommended_action": "<one of: 'skip','outreach','review'>",
  "confidence": <float 0.0-1.0>}}

SCORING GUIDELINES (how to choose rating):
- 8–10: Strong, active teachable skill, audience engaged, no evidence of existing high-ticket/membership in bio/links — ideal outreach.
- 6–7: Good prospect (could be monetizing lightly or selling low-ticket products), still worth outreach but review first.
- 4–5: Borderline (low views, unclear product fit, or some ambiguous language) — flag for manual review.
- 0–3: Low potential (vlogs, local-only, regulated, or uninterested audience) or clearly already high-ticket seller.
- If bio/links contain clear high-ticket/platform indicators, set rating to 0–2 and reason_code HIGH_TICKET_PRESENT or ALREADY_MONETIZING.

EXAMPLES:
{{"rating":9,"reason_code":"GOOD_PROSPECT","primary_signals":["skill:3d printing","avg_views:2400","no_bio_link"],"recommended_action":"outreach","confidence":0.92}}

Important: if language is not English and you detect that from text, return reason_code "NON_ENGLISH" and rating <= 4.

Respond with JSON only and nothing else.
"""
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            temperature=0.0,
            max_tokens=220
        )
        content = resp.choices[0].message.content.strip()
        if "{" in content:
            content = content[content.index("{"):]
        result = json.loads(content)
        if isinstance(result, dict) and "rating" in result:
            return result
        else:
            print(f"[OpenAI Rate] Unexpected format: {content}")
            return {"rating": 4, "reason_code": "PARSE_ERROR", "primary_signals": [], "recommended_action": "review", "confidence": 0.5}
    except Exception as e:
        print(f"[OpenAI Rate] Error: {e}")
        return {"rating": 4, "reason_code": "ERROR", "primary_signals": [], "recommended_action": "review", "confidence": 0.5}

def determine_offer_with_openai(channel_title, channel_description, recent_titles_pipe, landing_snippet):
    """
    Returns a dict with keys: selling_type, target_audience, one_line_pitch.
    selling_type in [course,membership,skool,appointment,lead_magnet,product,service,None,unknown]
    """
    prompt = f"""
You are an assistant that extracts succinct product/offer info from a YouTube channel.

Input:
Channel Title: {channel_title}
Channel Description: {channel_description}
Recent Video Titles (pipe-separated): {recent_titles_pipe}
Landing Page Snippet: {landing_snippet}

Output ONLY valid JSON in this exact shape:
{{"selling_type":"one of [course,membership,skool,appointment,lead_magnet,product,service,saas,agency,None,unknown]",
 "target_audience":"one-line description",
 "one_line_pitch":"single sentence summary"}}

If unclear, use "unknown" or "None" for selling_type and keep other fields short.
"""
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role":"user","content":prompt}],
            temperature=0
        )
        content = resp.choices[0].message.content.strip()
        # extract JSON substring
        if "{" in content:
            content = content[content.index("{"):]
        parsed = json.loads(content)
        return parsed
    except Exception as e:
        print(f"[OpenAI Offer] Error parsing offer info: {e}")
        return {"selling_type":"unknown","target_audience":"","one_line_pitch":""}


# ---------- persistence ----------

def column_number_to_letter(n):
    """Convert a 1-based column index to Excel-style column letters (supports > 26)."""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

def save_leads():
    """Append qualified leads to Google Sheet (deduping by Channel ID if present, else by Channel URL).
       Does NOT write or modify the header row. Maps values to the sheet's header order."""
    global existing_ids_global, num_new_leads_this_run, EXISTING_KEY_FIELD, unique_appended_this_run

    if not qualified_leads:
        print("[Save] No qualified leads to save.")
        return 0

    # Read header row (Row 1). If missing, abort to avoid misaligned writes.
    try:
        header_row = SHEET.row_values(1)
    except Exception as e:
        print(f"[Save] Could not read header row: {e}")
        return 0

    if not header_row:
        print("[Save] Header row is empty. Aborting save to avoid corrupting the sheet.")
        return 0

    # Determine primary key field based on header presence
    use_channel_id = "Channel ID" in header_row
    key_field = "Channel ID" if use_channel_id else "Channel URL"

    # Fetch existing records to build a set of existing keys for de-duplication
    try:
        existing_records = SHEET.get_all_records()
        existing_keys = {str(r.get(key_field, "")).strip() for r in existing_records if str(r.get(key_field, "")).strip()}
    except Exception as e:
        print(f"[Save] Error reading Google Sheet: {e}")
        existing_records = []
        existing_keys = set()

    FIELD_MAPPING = {
        "Channel Name": "Channel Name",
        "Channel URL": "Channel URL",
        "Channel ID": "Channel ID",
        "First Name": "First Name",
        "Country": "Country",
        "Subscribers": "Subscribers",
        "Average Views (3 Recent Non-Shorts)": "Average Views (3 Recent Non-Shorts)",
        "YouTube Bio": "YouTube Bio",
        "Email": "Email",
        "Bio Links": "Bio Links",
        "Recent Titles (15 Non-Shorts)": "Recent Titles (15 Non-Shorts)",
        "Rating": "Rating",
        "Approved": "Approved",
        "Queue Summary": "Queue Summary",
        "Landing Page Copy": "Landing Page Copy",
        "Niche": "Niche",
        "Selling": "Selling",
        "Target Audience": "Target Audience",
        "One-Line Pitch": "One-Line Pitch",
        "Deliverable": "Deliverable",
        "Queue Angles": "Queue Angles",
        "Angles/Ideas": "Angles/Ideas",
        "Angles Approved": "Angles Approved",
        "Queue Copy": "Queue Copy",
        "Doc URL": "Doc URL",
        "Copy Approved": "Copy Approved",
        "Status": "Status",
        "Updated At": "Updated At",
        "Queue Doc": "Queue Doc",
        "Notes": "Notes"
    }
    # Map each lead dict into the sheet's header order; ignore extra fields; leave missing as ""
    rows_to_add = []
    added_keys = []

    for lead in qualified_leads:
        # Compute the key we will use for dedupe
        lead_key_value = ""
        if use_channel_id:
            lead_key_value = str(lead.get("Channel ID", "")).strip()
        else:
            lead_key_value = str(lead.get("Channel URL", "")).strip()

        # Normalize the key
        lead_key_value_norm = str(lead_key_value).strip().lower()
        # Skip if no key or already present in Raw or in global (all sheets)
        if not lead_key_value_norm or \
           lead_key_value_norm in {str(k).strip().lower() for k in existing_keys} or \
           lead_key_value_norm in {str(k).strip().lower() for k in existing_ids_global}:
            continue

        # Build row aligned to the sheet header using FIELD_MAPPING
        row_values = []
        for col_name in header_row:
            scraper_key = next((k for k, v in FIELD_MAPPING.items() if v == col_name), None)
            value = lead.get(scraper_key, "") if scraper_key else ""
            if isinstance(value, (list, dict)):
                value = json.dumps(value, ensure_ascii=False)
            row_values.append(value)
        rows_to_add.append(row_values)
        added_keys.append(lead_key_value)

    # Track number of new unique leads added this run
    num_new_leads_this_run = len(rows_to_add)

    if not rows_to_add:
        print("[Save] No additional leads to save (all already appended earlier).")
        return 0

    # Append new rows in one batch
    try:
        # If sheet has no header row, insert header first
        if SHEET.row_count < 1 or not SHEET.row_values(1):
            headers = list(FIELD_MAPPING.keys())
            SHEET.insert_row(headers, index=1)

        # Read header again in case inserted
        header_row = SHEET.row_values(1)
        num_columns = len(header_row)

        # Pad each row to match the sheet's column count
        rows_to_add_padded = [
            row + [""] * (num_columns - len(row)) for row in rows_to_add
        ]

        # Find the next available row by checking the "Channel Name" column
        try:
            channel_name_col_index = header_row.index("Channel Name") + 1  # 1-based index
        except ValueError:
            print("[Save] 'Channel Name' column not found in header, defaulting to next row.")
            channel_name_col_index = 1
        col_values = SHEET.col_values(channel_name_col_index)[1:]  # skip header
        first_empty_row_index = len([v for v in col_values if v.strip()]) + 2  # next row after last non-empty
        start_row = first_empty_row_index
        end_row = start_row + len(rows_to_add_padded) - 1

        # Ensure enough rows in sheet
        if SHEET.row_count < end_row:
            SHEET.add_rows(end_row - SHEET.row_count)

        # Determine the column letter dynamically for range (supports beyond 'Z')
        end_col_letter = column_number_to_letter(num_columns)
        cell_range = f"A{start_row}:{end_col_letter}{end_row}"

        SHEET.update(rows_to_add_padded, range_name=cell_range, value_input_option="RAW")

        print(f"[Save] Added {len(rows_to_add_padded)} new leads to Google Sheet.")
        # merge new Raw-sheet keys into the global set, without throwing away other sheets
        existing_keys.update(added_keys)

        # normalize keys (strip + lowercase)
        norm_existing_keys = {str(k).strip().lower() for k in existing_keys if k}
        normalized_global = {str(k).strip().lower() for k in existing_ids_global} if existing_ids_global else set()
        existing_ids_global.clear()
        existing_ids_global.update(normalized_global)
        existing_ids_global.update(norm_existing_keys)

        EXISTING_KEY_FIELD = "id" if use_channel_id else "url"
        global unique_appended_this_run
        unique_appended_this_run += len(rows_to_add_padded)
    except Exception as e:
        print(f"[Save] Error appending rows: {e}")
        return 0
    return len(rows_to_add)


# --- Save/Load Scraper State for Resume Support ---
def save_state():
    state = {
        "collected_channels": list(collected_channels),
        "qualified_leads": qualified_leads,
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
        print(f"[SaveState] Scraper state saved to {STATE_FILE}")
    except Exception as e:
        print(f"[SaveState] Error saving state: {e}")

def load_state():
    global collected_channels, qualified_leads
    if not os.path.exists(STATE_FILE):
        return
    try:
        if os.path.getsize(STATE_FILE) == 0:
            print("[LoadState] State file is empty, skipping.")
            return
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        collected_channels = set(state.get("collected_channels", []))
        # Do NOT restore qualified_leads; always start with empty in-memory list
        # qualified_leads[:] = state.get("qualified_leads", [])
        print(f"[LoadState] Loaded scraper state from {STATE_FILE}")
    except Exception as e:
        print(f"[LoadState] Error loading state: {e}")

# === UPDATED ===
# Main scraping loop: final-final flow, channel search, deduplication, batch fetch, heuristics before OpenAI, new ordering

print("Starting updated scraper... (CTRL+C to stop)")

api_index = 0

# Load state if exists
load_state()

# Fetch existing keys (Channel ID and Channel URL) from all relevant sheets for global deduplication
try:
    # Get all relevant sheets
    SHEET_BOOK = GSPREAD_CLIENT.open_by_key("1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE")
    worksheet_names = ["Raw Leads", "Processed Leads", "Invalid Leads"]
    all_ids_urls = set()
    for ws_name in worksheet_names:
        try:
            ws = SHEET_BOOK.worksheet(ws_name)
            records = ws.get_all_records()
            for r in records:
                ch_id = str(r.get("Channel ID", "")).strip()
                ch_url = str(r.get("Channel URL", "")).strip()
                if ch_id:
                    all_ids_urls.add(ch_id)
                if ch_url:
                    all_ids_urls.add(ch_url)
        except Exception as ws_e:
            print(f"[Init] Error fetching from sheet '{ws_name}': {ws_e}")
    existing_ids_global = all_ids_urls
    # Determine EXISTING_KEY_FIELD for current run based on Raw Leads header
    header_row_init = SHEET.row_values(1)
    use_channel_id_init = "Channel ID" in header_row_init
    EXISTING_KEY_FIELD = "id" if use_channel_id_init else "url"
except Exception as e:
    print(f"[Init] Error fetching existing keys from Google Sheets: {e}")
    existing_ids_global = set()
    EXISTING_KEY_FIELD = "id"

# === UPDATED: Helper functions for batch channel/video info ===
def get_channel_details_batch(youtube, channel_ids):
    """Fetch channel details in batch for a list of channel IDs."""
    out = {}
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        try:
            resp = youtube.channels().list(
                part='statistics,snippet,brandingSettings,contentDetails',
                id=",".join(batch)
            ).execute()
            for ch in resp.get("items", []):
                out[ch["id"]] = ch
        except Exception as e:
            print(f"[Batch] Error fetching channel batch: {e}")
    return out

def get_uploads_playlist_id(channel):
    """Get the uploads playlist ID from a channel resource."""
    return channel.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")

def get_recent_videos_from_playlist(youtube, playlist_id, max_results=20):
    """Fetch recent video IDs and titles from an uploads playlist."""
    videos = []
    next_token = None
    while len(videos) < max_results:
        try:
            resp = youtube.playlistItems().list(
                part="contentDetails,snippet",
                playlistId=playlist_id,
                maxResults=min(50, max_results - len(videos)),
                pageToken=next_token
            ).execute()
            for item in resp.get("items", []):
                vid = item.get("contentDetails", {}).get("videoId")
                title = item.get("snippet", {}).get("title", "")
                published_at = item.get("contentDetails", {}).get("videoPublishedAt", "")
                videos.append({"id": vid, "title": title, "publishedAt": published_at})
            next_token = resp.get("nextPageToken")
            if not next_token:
                break
        except Exception as e:
            print(f"[Playlist] Error fetching playlist videos: {e}")
            break
    return videos

def get_videos_details_batch(youtube, video_ids):
    """Fetch video details (view count, duration) for a list of video IDs."""
    out = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            resp = youtube.videos().list(
                part="statistics,contentDetails,snippet",
                id=",".join(batch)
            ).execute()
            for v in resp.get("items", []):
                out[v["id"]] = v
        except Exception as e:
            print(f"[Batch] Error fetching video batch: {e}")
    return out

# === END UPDATED helpers ===

# ADAPTIVE SHALLOW SEARCH STRATEGY

BLACKLIST_KEYWORDS = [
    "CRM", "automation platform", "marketing agency", "white-label software", "software tool"
]

def is_short_video(title, duration_seconds):
    if "#shorts" in (title or "").lower():
        return True
    if duration_seconds is not None and duration_seconds <= 60:
        return True
    return False

def save_all_state_periodically():
    added = 0
    try:
        res = save_leads()
        added = res or 0
    except Exception as e:
        print(f"[SaveAll] save_leads() failed: {e}")
        added = 0
    try:
        save_state()
    except Exception as e:
        print(f"[SaveAll] save_state() failed: {e}")
    return added

# === UPDATED: Main loop using channel search and new flow ===
try:
    num_new_leads_this_run = 0
    processed_items = 0
    # Only use OpenAI-generated video title queries with fallback examples (base keywords are no longer used)
    # Generate a single set of OpenAI video title keywords for this run
    openai_title_keywords = generate_keywords_with_openai()
    if not openai_title_keywords:
        print("[Main] OpenAI failed to supply any keywords after retries. Saving state and exiting gracefully.")
        save_all_state_periodically()
        sys.exit(0)
    random.shuffle(openai_title_keywords)
    exhausted_keywords = set()
    # Main loop: ensure we never overshoot TARGET_LEADS, stop immediately when reached
    consecutive_quota_errors = 0
    stop_scraping = False
    while unique_appended_this_run < TARGET_LEADS and not stop_scraping:
        if stop_scraping:
            print("Stopping scraper as all API keys are exhausted.")
            break
        # Pick a video title keyword not exhausted
        remaining = [k for k in openai_title_keywords if k not in exhausted_keywords]
        if not remaining:
            # regenerate a fresh batch if the current one is exhausted
            openai_title_keywords = generate_keywords_with_openai()
            random.shuffle(openai_title_keywords)
            exhausted_keywords.clear()
            continue
        video_title_keyword = random.choice(remaining)
        print(f"\n[TitleKW] Trying OpenAI video title keyword: {video_title_keyword}")
        title_keyword_lead_count = 0
        found_good_lead = False
        page_token = None
        for page_num in range(2):
            if unique_appended_this_run >= TARGET_LEADS or stop_scraping:
                stop_scraping = True
                break
            search_keyword = video_title_keyword
            youtube = youtube_clients[api_index]
            try:
                search_response = youtube.search().list(
                    q=search_keyword,
                    type='video',
                    part='id,snippet',
                    maxResults=MAX_VIDEOS_PER_PAGE,
                    pageToken=page_token,
                    order='relevance',
                    relevanceLanguage='en'
                ).execute()
                consecutive_quota_errors = 0
            except HttpError as e:
                err_text = str(e)
                print(f"[YouTube API] HttpError on key idx {api_index}: {e}")
                if "quotaExceeded" in err_text or "dailyLimitExceeded" in err_text or "quota" in err_text.lower():
                    api_index = (api_index + 1) % len(youtube_clients)
                    print(f"[YouTube API] quotaExceeded on key idx {api_index}. Switching to next API key (index {api_index})")
                    consecutive_quota_errors += 1

                    if consecutive_quota_errors >= len(youtube_clients):
                        print("[YouTube API] All API keys likely exhausted. Stopping scraper and saving state.")
                        try:
                            send_slack_message(":warning: Scraper stopped — all YouTube API keys exhausted.")
                        except Exception:
                            pass
                        try:
                            save_all_state_periodically()
                        except Exception:
                            print("Warning: could not save state before stopping.")
                        stop_scraping = True
                        break

                    time.sleep(DELAY_BETWEEN_REQUESTS * 3)
                    continue
                else:
                    print(f"[YouTube API] Non-quota HttpError: {e}")
                    time.sleep(DELAY_BETWEEN_REQUESTS * 2)
                    continue
            except Exception as e:
                print(f"[YouTube API] General error: {e}")
                time.sleep(DELAY_BETWEEN_REQUESTS * 2)
                continue
            next_token = search_response.get('nextPageToken', None)
            page_token = next_token
            items = search_response.get("items", [])
            # Deduplicate channel IDs early (for video search: extract channelId from snippet)
            channel_ids = []
            channel_id_to_video_item = {}
            for item in items:
                ch_id = item.get('snippet', {}).get('channelId')
                if not ch_id:
                    continue
                channel_url_str = f"https://www.youtube.com/channel/{ch_id}"
                cid_norm = str(ch_id).strip().lower()
                curl_norm = str(channel_url_str).strip().lower()
                already_in_sheet = (cid_norm in existing_ids_global) or (curl_norm in existing_ids_global)
                if (cid_norm in {c.lower() for c in collected_channels}) or already_in_sheet:
                    continue
                if ch_id not in channel_id_to_video_item:
                    channel_ids.append(ch_id)
                    channel_id_to_video_item[ch_id] = item
            # Batch-fetch channel details
            channel_details_map = get_channel_details_batch(youtube, channel_ids)
            for channel_id in channel_ids:
                if unique_appended_this_run >= TARGET_LEADS or stop_scraping:
                    stop_scraping = True
                    break
                processed_items += 1
                if processed_items % 100 == 0:
                    print(f"[Progress] Processed {processed_items} channels | Qualified this run: {num_new_leads_this_run}")
                ch = channel_details_map.get(channel_id)
                if not ch:
                    continue
                stats = ch.get("statistics", {})
                subs_str = stats.get("subscriberCount", "0")
                try:
                    subs_count = int(subs_str)
                except:
                    subs_count = 0

                # EARLY SUBSCRIBER CUTOFF
                if subs_count < MIN_SUBS or subs_count > MAX_SUBS:
                    continue

                snippet = ch.get("snippet", {})
                country = snippet.get("country", None)
                channel_title = snippet.get("title", "")
                channel_description = snippet.get("description", "")
                selling_clue = False
                avg_views = 0
                # Early filters: country, language, blacklist
                ALLOWED_COUNTRIES = [
                    # North America
                    "US", "CA",
                    # Europe (West, North, South, East, Balkans)
                    "GB", "IE", "FR", "DE", "IT", "ES", "PT", "NL", "BE", "LU", "AT", "CH",
                    "SE", "NO", "DK", "FI", "IS", "GR", "CY", "MT",
                    "PL", "CZ", "SK", "HU", "SI", "EE", "LV", "LT", "HR", "BG", "RO",
                    "RS", "BA", "MK", "ME", "AL", "UA", "BY", "RU", "MD", "GE", "AM", "AZ",
                    # Asia-Pacific
                    "AU", "NZ", "JP", "KR", "SG", "HK", "TW", "MN", "VN",
                    # Middle East high-income
                    "AE", "SA", "KW", "QA", "BH", "OM", "IL",
                    # Latin America (developed/upper-income)
                    "CL", "UY", "AR"
                ]
                if country and country.upper() not in ALLOWED_COUNTRIES:
                    continue
                # If country is missing/None, allow it
                if not (is_english(channel_title) or is_english(channel_description)):
                    continue
                desc_low = channel_description.lower()
                BLACKLIST_KEYWORDS = [
                    "CRM", "automation platform", "marketing agency", "white-label software", "software tool"
                ]
                if any(bk.lower() in desc_low for bk in BLACKLIST_KEYWORDS):
                    continue
                # Selling clue heuristics: selling phrases
                selling_phrases = [
                    "course", "coaching", "mentorship", "lead magnet", "skool", "membership", "masterclass",
                    "webinar", "consulting", "enroll", "join my program", "apply now", "enroll now", "sign up",
                    "free training", "program", "bootcamp", "challenge", "academy", "mastermind", "cohort",
                    "book a call", "strategy call", "free class", "training program"
                ]
                for phrase in selling_phrases:
                    if phrase in desc_low:
                        selling_clue = True
                        break
                # Also look for selling language in the channel title
                title_low = channel_title.lower()
                if not selling_clue:
                    for phrase in selling_phrases:
                        if phrase in title_low:
                            selling_clue = True
                            break
                # Platform anchor detection (channel_description and most recent non-short video description)
                platform_anchors = [
                    "teachable.com","kajabi.com","thinkific.com","gumroad.com","patreon.com","skool.com",
                    "stan.store","linktr.ee","beacons.ai","calendly.com","clickfunnels.com","systeme.io",
                    "kartra.com","samcart.com","podia.com","shopify.com","myshopify.com","buymeacoffee.com",
                    "ko-fi.com","udemy.com","coursera.org","substack.com","typeform.com","paypal.me",
                    "stripe.com","square.site","bigcartel.com","eventbrite.com"
                ]
                for anchor in platform_anchors:
                    if anchor in desc_low:
                        selling_clue = True
                        break
                # NEW: skip channels that already advertise high-ticket offers in bio/description
                if looks_high_ticket_simple(channel_description):
                    print(f"[Skip] Channel '{channel_title}' skipped — bio/description shows high-ticket signals.")
                    continue
                # Fetch uploads playlist and recent videos
                uploads_playlist_id = get_uploads_playlist_id(ch)
                if not uploads_playlist_id:
                    continue
                recent_videos = get_recent_videos_from_playlist(youtube, uploads_playlist_id, max_results=20)
                video_ids = [v["id"] for v in recent_videos if v.get("id")]
                videos_details_map = get_videos_details_batch(youtube, video_ids)
                # Filter out shorts and non-English, also check for platform anchors in recent video description
                non_shorts_video_data = []
                most_recent_video_desc = ""
                for idx, v in enumerate(recent_videos):
                    vid = v.get("id")
                    title = v.get("title", "")
                    published_at = v.get("publishedAt", "")
                    detail = videos_details_map.get(vid, {})
                    iso_dur = detail.get('contentDetails', {}).get('duration', 'PT0S')
                    match = re.match(r'PT((?P<h>\d+)H)?((?P<m>\d+)M)?((?P<s>\d+)S)?', iso_dur)
                    hours = int(match.group('h')) if match and match.group('h') else 0
                    minutes = int(match.group('m')) if match and match.group('m') else 0
                    seconds = int(match.group('s')) if match and match.group('s') else 0
                    total_seconds = hours * 3600 + minutes * 60 + seconds
                    if is_short_video(title, total_seconds):
                        continue
                    if not is_english(title):
                        continue
                    views = 0
                    try:
                        views = int(detail.get('statistics', {}).get('viewCount', 0))
                    except:
                        views = 0
                    non_shorts_video_data.append((title, views, total_seconds, vid, published_at))
                    # Only check the most recent non-short video description for platform anchors
                    if most_recent_video_desc == "" and isinstance(detail.get("snippet",{}).get("description",""), str):
                        most_recent_video_desc = detail.get("snippet",{}).get("description","")
                # Check for platform anchors in most recent non-short video description
                if most_recent_video_desc:
                    desc_video_low = most_recent_video_desc.lower()
                    for anchor in platform_anchors:
                        if anchor in desc_video_low:
                            selling_clue = True
                            break
                # If selling clue found via platform anchor, set rating high immediately
                if selling_clue:
                    rating = 10
                else:
                    rating = None
                non_shorts_video_data = non_shorts_video_data[:15]
                recent_titles = [x[0] for x in non_shorts_video_data]
                video_ids_nonshorts = [x[3] for x in non_shorts_video_data]
                published_ats = [x[4] for x in non_shorts_video_data]
                # recency: at least 2 non-shorts within last 180 days
                recent_nonshorts = [pub for pub in published_ats if pub and is_recent(pub)]
                if len(recent_nonshorts) < 2:
                    continue
                # avg views for 3 most recent non-shorts
                non_shorts_top3 = non_shorts_video_data[:3]
                if non_shorts_top3:
                    avg_views = sum(x[1] for x in non_shorts_top3) // len(non_shorts_top3)
                else:
                    avg_views = 0
                # Require at least a 300 floor for average views (no % of subs)
                if avg_views < 300:
                    continue
                titles_low = " ".join(recent_titles).lower()
                if any(bk.lower() in titles_low for bk in BLACKLIST_KEYWORDS):
                    continue
                # Positive signal: selling phrases in recent titles
                if not selling_clue:
                    for phrase in selling_phrases:
                        if phrase in titles_low:
                            selling_clue = True
                            break
                channel_url_str = f"https://www.youtube.com/channel/{channel_id}" if channel_id else ""
                # Check both Channel ID and Channel URL in global set
                cid_norm = str(channel_id).strip().lower()
                curl_norm = str(channel_url_str).strip().lower()
                already_in_sheet = (cid_norm in existing_ids_global) or (curl_norm in existing_ids_global)
                if (cid_norm in {c.lower() for c in collected_channels}) or already_in_sheet:
                    continue
                # If rating not set by platform/selling clue, use OpenAI
                if rating is None:
                    rating = rate_lead_with_openai(channel_title, channel_description, avg_views, "|".join(recent_titles[:15]))
                    print(f"[Rate] {channel_title} -> rating {rating}")
                    if rating is None:
                        rating = 4
                    if rating < 4:
                        continue
                # Extract sample video info
                sample_idx = 0
                sample_video_title = recent_titles[sample_idx] if recent_titles else ""
                sample_video_id = video_ids_nonshorts[sample_idx] if video_ids_nonshorts else ""
                sample_published_at = published_ats[sample_idx] if published_ats else ""
                all_links = re.findall(r'(https?://[^\s]+)', channel_description)
                bio_link = '||'.join(all_links) if all_links else ""
                email = None
                for single in (all_links or [])[:3]:
                    email = scrape_website_for_email(single)
                    if email:
                        break
                # Sort non_shorts_video_data by published date descending and pick the most recent for sample
                def parse_dt_safe(dtstr):
                    try:
                        return datetime.strptime(dtstr, "%Y-%m-%dT%H:%M:%SZ")
                    except Exception:
                        return datetime.min
                non_shorts_video_data_sorted = sorted(non_shorts_video_data, key=lambda x: parse_dt_safe(x[4]), reverse=True)
                sample_video_title = non_shorts_video_data_sorted[0][0] if non_shorts_video_data_sorted else ""
                sample_video_id = non_shorts_video_data_sorted[0][3] if non_shorts_video_data_sorted else ""
                sample_published_at = non_shorts_video_data_sorted[0][4] if non_shorts_video_data_sorted else ""
                qualified_leads.append({
                    "Channel Name": channel_title,
                    "Channel ID": channel_id,
                    "Subscribers": subs_count,
                    "Country": country,
                    "Channel URL": f"https://www.youtube.com/channel/{channel_id}",
                    "Last Video Published": sample_published_at,
                    "Sample Video Title": sample_video_title,
                    "Source Keyword": search_keyword,
                    "Email": email or "No",
                    "Average Views (3 Recent Non-Shorts)": avg_views,
                    "Rating": rating,
                    "YouTube Bio": channel_description,
                    "Bio Links": bio_link,
                    "Recent Titles (15 Non-Shorts)": "|".join(recent_titles[:15]),
                    "Selling": "",
                    "Target Audience": "",
                    "One-Line Pitch": "",
                    "Trigger Video Title": sample_video_title,
                    "Trigger Video URL": f"https://www.youtube.com/watch?v={sample_video_id}" if sample_video_id else ""
                })
                collected_channels.add(channel_id)
                num_new_leads_this_run += 1
                print(f"[Qualified+] {channel_title} | Rating {rating}")
                found_good_lead = True
                title_keyword_lead_count += 1
                # After adding, check if we reached the target leads
                if unique_appended_this_run >= TARGET_LEADS:
                    stop_scraping = True
                    break
                if len(qualified_leads) % AUTO_SAVE_EVERY == 0:
                    added_now = save_all_state_periodically()
                    added_now = added_now or 0
                    if unique_appended_this_run >= TARGET_LEADS:
                        stop_scraping = True
                        break
                if unique_appended_this_run >= TARGET_LEADS:
                    stop_scraping = True
                    break
                time.sleep(DELAY_BETWEEN_REQUESTS)
        # Mark keyword as exhausted if no lead found
        if title_keyword_lead_count == 0:
            exhausted_keywords.add(video_title_keyword)
        if random.random() < 0.2:
            save_all_state_periodically()
except KeyboardInterrupt:
    print("\n[Interrupted] Stopped by user.")
finally:
    print(f"\n[Finish] Collected {len(qualified_leads)} in-memory qualified leads. Saving to Google Sheet ...")
    try:
        added_final = save_all_state_periodically()
    except Exception:
        added_final = 0
    added_final = added_final or 0
    total_unique = unique_appended_this_run
    if total_unique == 0:
        print("[Save] No new unique leads were found.")
    print(f"Total new unique leads appended this run: {total_unique}")
    print("Done.")
    try:
        send_slack_message(f"✅ Scraper finished! Collected {total_unique} new leads.")
    except Exception as e:
        print(f"[Slack] Notification failed: {e}")