import random
import time
import sys
import signal
import os
import re
import json
from datetime import datetime, timezone
import requests

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
    "AIzaSyC2m0NfOQKRuqDPhGucSw22ih3TrYbVoVY"

]

# OpenAI key via env var preferred
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or "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"

# Parameters
MIN_SUBS = 5000
MAX_SUBS = 350000
MAX_VIDEO_AGE_DAYS = 180       # only consider videos <= 180 days old
TARGET_LEADS = 60              # collect 15 qualified leads (rating >= 7)
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
# Additional sheets for deduplication
INVALID_SHEET = GSPREAD_CLIENT.open_by_key("1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE").worksheet("Invalid Leads")
PROCESSED_SHEET = GSPREAD_CLIENT.open_by_key("1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE").worksheet("Processed Leads")

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



# ---------- OpenAI interactions ----------


# --- NEW FUNCTION: generate_keywords_with_openai ---
def generate_keywords_with_openai(n_min=KEYWORD_TITLES_MIN, n_max=KEYWORD_TITLES_MAX):
    """
    Generate a list of YouTube video title keywords using OpenAI (GPT-4o-mini).
    Falls back to a set of diverse monetization-related example titles if OpenAI fails.
    """
    n = random.randint(n_min, n_max)
    prompt = """
Generate 25–30 diverse YouTube creator video titles from all types of niches (gardening, photography, plumbing, cleaning, dancing, cooking, crafts, language learning, fitness, etc.).
Each title should be realistic, engaging, and assume the creator is already monetizing (selling courses, coaching, consulting, paid services, online programs, affiliate, etc.).
Every title must signal monetization or income (e.g., making money, clients, billing, profits, classes, services, selling products).
Sprinkle in a few that mention platforms/tools (e.g., Patreon, Shopify, Teachable, Kajabi, Stripe, Etsy, Calendly).
Return ONLY a valid JSON array of strings.

Examples:
1. "How I Make Money Teaching Gardening Online"
2. "Photography Client Billing Tips for Full-Time Freelancers"
3. "Top Cleaning Hacks That Help Me Earn $5k/Month"
4. "Plumbing Business Taxes Explained for Home Service Owners"
5. "Advanced Cooking Classes That Pay My Rent"
6. "How to Monetize Your Dance Tutorials on YouTube"
7. "Selling Gardening Courses on Teachable — My Monthly Income"
8. "How I Run Paid Zoom Cooking Workshops"
9. "Pet Care Coaching Profits with Kajabi and Stripe"
10. "Craft Store Earnings Selling DIY Kits on Shopify & Etsy"
"""
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7
        )
        content = resp.choices[0].message.content.strip()
        if not content:
            raise ValueError("Empty response from OpenAI")
        keywords = json.loads(content)
        if isinstance(keywords, list) and keywords:
            return keywords
    except Exception:
        pass
    # fallback: return seeded examples so scraper keeps running if OpenAI fails
    fallback_examples = [
        "How I Make Money Teaching Gardening Online",
        "Photography Client Billing Tips for Full-Time Freelancers",
        "Top Cleaning Hacks That Help Me Earn $5k/Month",
        "Plumbing Business Taxes Explained for Home Service Owners",
        "Advanced Cooking Classes That Pay My Rent",
        "How to Monetize Your Dance Tutorials on YouTube",
        "Selling Gardening Courses on Teachable — My Monthly Income",
        "How I Run Paid Zoom Cooking Workshops",
        "Pet Care Coaching Profits with Kajabi and Stripe",
        "Craft Store Earnings Selling DIY Kits on Shopify & Etsy"
    ]
    out = []
    for i in range(n):
        base = fallback_examples[i % len(fallback_examples)]
        out.append(base)
    random.shuffle(out)
    return out

def rate_lead_with_openai(channel_title, channel_description, avg_views, titles_pipe):
    """
    Asks OpenAI to rate the lead 0-10 (likelihood of offering paid product/monetization).
    Returns integer 0-10.
    """
    prompt = f"""
You are an expert evaluator. Rate from 0 to 10 (integer only) how likely this YouTube channel is to offer paid courses, coaching, lead magnets, booking calls, memberships or other paid products based on the information below.

Channel Title: {channel_title}
Channel Description: {channel_description}
Recent Video Titles (pipe separated): {titles_pipe}
Average Views per Video: {avg_views}

If the YouTube bio or recent video titles contain non-English text (such as Spanish, Hindi, or any language other than English), give a rating less than 5.
Consider presence of selling language, call-to-actions, professionalism, and view counts.
Respond with a single integer between 0 and 10 and nothing else.
"""
    try:
        resp = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            temperature=0
        )
        text = resp.choices[0].message.content.strip()
        m = re.search(r"\d+", text)
        if m:
            return int(m.group(0))
    except Exception as e:
        print(f"[OpenAI Rate] Error: {e}")
    return 0

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
            model="gpt-4o-mini",
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
    global existing_ids_global, num_new_leads_this_run, EXISTING_KEY_FIELD

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

        # Skip if no key or already present
        if not lead_key_value or lead_key_value in existing_keys:
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
        # Update global cache to include the newly added keys
        existing_keys.update(added_keys)
        existing_ids_global = set(existing_keys)
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

# Fetch existing keys from multiple Google Sheets into a global set (Channel ID preferred; fallback to Channel URL)
global existing_ids_global, EXISTING_KEY_FIELD

def get_existing_ids_from_sheets(sheets):
    """
    Given a list of gspread worksheets, return a set of unique Channel IDs (preferred) or Channel URLs.
    """
    ids = set()
    has_channel_id = False
    for ws in sheets:
        try:
            header = ws.row_values(1)
            use_channel_id = "Channel ID" in header
            if use_channel_id:
                has_channel_id = True
            recs = ws.get_all_records()
            if use_channel_id:
                ids.update(str(r.get("Channel ID", "")).strip() for r in recs if str(r.get("Channel ID", "")).strip())
            else:
                ids.update(str(r.get("Channel URL", "")).strip() for r in recs if str(r.get("Channel URL", "")).strip())
        except Exception as e:
            print(f"[Init] Error reading sheet {getattr(ws, 'title', 'unknown')}: {e}")
    return ids, has_channel_id

try:
    # Use both Invalid and Processed Leads sheets for deduplication
    sheets_to_check = [INVALID_SHEET, PROCESSED_SHEET]
    ids_collected, has_channel_id = get_existing_ids_from_sheets(sheets_to_check)
    existing_ids_global = ids_collected
    EXISTING_KEY_FIELD = "id" if has_channel_id else "url"
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
    random.shuffle(openai_title_keywords)
    exhausted_keywords = set()
    while unique_appended_this_run < TARGET_LEADS:
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
            except HttpError as e:
                print(f"[YouTube API] HttpError on key idx {api_index}: {e}")
                if "quotaExceeded" in str(e):
                    api_index = (api_index + 1) % len(youtube_clients)
                    print(f"[YouTube API] Switching to next API key (index {api_index})")
                    youtube = youtube_clients[api_index]
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
                if ch_id and ch_id not in channel_id_to_video_item:
                    channel_ids.append(ch_id)
                    channel_id_to_video_item[ch_id] = item
                # Batch-fetch channel details
                channel_details_map = get_channel_details_batch(youtube, channel_ids)
                for channel_id in channel_ids:
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
                    already_in_sheet = False
                    if EXISTING_KEY_FIELD == "id":
                        already_in_sheet = str(channel_id) in existing_ids_global
                    else:
                        already_in_sheet = channel_url_str in existing_ids_global
                    if (channel_id in collected_channels) or already_in_sheet:
                        continue
                    # If rating not set by platform/selling clue, use OpenAI
                    if rating is None:
                        rating = rate_lead_with_openai(channel_title, channel_description, avg_views, "|".join(recent_titles[:15]))
                        print(f"[Rate] {channel_title} -> rating {rating}")
                        if rating is None:
                            rating = 7
                        if rating < 7:
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
                    # Track in-memory qualified count only; sheet appends are now immediate
                    num_new_leads_this_run += 1
                    print(f"[Qualified+] {channel_title} | Rating {rating}")
                    found_good_lead = True
                    title_keyword_lead_count += 1
                    added_now = save_all_state_periodically()
                    if unique_appended_this_run >= TARGET_LEADS:
                        print(f"[Target Reached] {unique_appended_this_run} leads collected. Stopping.")
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
