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
unique_added_this_run = 0
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
    "AIzaSyCZbEWLcgnThO-cLfzuB411_WyDdPt3cHs",
    "AIzaSyA8cBjcsrJQCtbh0fID0BUDdAZB8jwG5vI",
    "AIzaSyC9rwhurm3JL6NkDXzvUWWc079lGsk3Vjk",
    "AIzaSyB2p8GXoG_AOm9TEDmqpoQiHoyO7IIe3eU",
    "AIzaSyC2m0NfOQKRuqDPhGucSw22ih3TrYbVoVY",
    "AIzaSyCsrVNe2Y91pgROG50s8B90AtVmUzi23R0",
    "AIzaSyC3ziU3IdKt91DFt5XGE4HVepSYs4PDlA4",
    "AIzaSyB1UdpVtyo5T0rUgEOlkcxodNYy0bhTwiA",
    "AIzaSyDOl-KJk6uRKXDE2FWiEMo6zgrJTqBmHDQ",
    "AIzaSyBpU1kDS1Lu6WBI76PbI50XeUfMxcTEdvY",
    "AIzaSyAze5ZozA0CX0nkc42PxIL6Rc6jQ4SerH0",
    "AIzaSyAEG5hUpyQM2BQD1NlsfmvZNPJDmZyW4Vw",
    "AIzaSyCuza6qvawjdeyAaaHT8USqPOHteJb9nuo",
    "AIzaSyByyMHMehHrCerrUBKBh2gqe0Dl9hkwDAg",
    "AIzaSyDIzKvn-JDfhQNovNCi-kXKOq7zWyOZPiA",
    "AIzaSyDe75esEJsvtjy2s48rVqBkCW2_gbt4isc",
    "AIzaSyDncyAIOlOjG9pjH8xYRLHWvGSg9srSMnA",
    "AIzaSyBbKXiBfaP5HgOeXr_hOJcTXPf9XNBOCbU",
    "AIzaSyA4rs-T12x3yv1XG7HcxDeA3Nb-_jyHuqo",
    "AIzaSyAwtp--S2i1mW8xjhkWF7U3fAgUou0LvsU",
    "AIzaSyAqhHPtUol73wQ-b9_VPOTLTZCG7aZ7Qdw"
]

# OpenAI key via env var preferred
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or "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"

# === UPDATED PARAMETERS ===
MIN_SUBS = 500  # Changed from 1000
MAX_SUBS = 25000  # Changed from 75000
MAX_VIDEO_AGE_DAYS = 180
TARGET_LEADS = 500  # Changed from 500 - target per run
DELAY_BETWEEN_REQUESTS = 1.2
AUTO_SAVE_EVERY = 5
MAX_VIDEOS_PER_PAGE = 50
KEYWORD_TITLES_MIN = 75  # Changed from 25
KEYWORD_TITLES_MAX = 100  # Changed from 30

# === EXPANDED COUNTRIES - First and Second World ===
LOCATION_SUFFIXES = [
    "US", "GB", "CA", "AU", "NZ",  # Primary English
    "SG", "IE", "ZA",  # Other English
    "AE",  # UAE
    "DE", "FR", "NL", "SE", "NO", "DK", "CH", "AT", "FI", "BE",  # Western Europe
    "ES", "IT", "PT",  # Southern Europe
    "PL", "CZ"  # Eastern Europe
]

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
collected_channels = set()
qualified_leads = []

# === 100 HYPER-NICHE KEYWORDS ===
HYPER_NICHE_KEYWORDS = [
    # Health & Fitness (10)
    "calisthenics transformation for beginners",
    "posture correction exercises for desk workers",
    "home workout routine for busy moms",
    "yoga flow for runners",
    "meal prep bodybuilding on a budget",
    "dumbbell only workout at home",
    "flexibility training for athletes",
    "core strengthening for lower back pain",
    "resistance band full body workout",
    "bodyweight training for skinny guys",
    
    # Creative Photography & Video (10)
    "mobile photography tips and tricks",
    "portrait photography lighting setup",
    "color grading tutorial davinci resolve",
    "photo editing lightroom workflow",
    "cinematic video filming techniques",
    "product photography at home setup",
    "street photography composition tips",
    "food photography for instagram",
    "real estate photography tutorial",
    "sunset photography camera settings",
    
    # Music & Audio (10)
    "music theory for beginners piano",
    "mixing vocals in logic pro",
    "fl studio beat making tutorial",
    "guitar scales for improvisation",
    "sound design ableton live",
    "mastering basics for beginners",
    "recording vocals at home setup",
    "music production tips for beginners",
    "chord progressions for songwriting",
    "audio mixing eq tutorial",
    
    # Excel & Data (10)
    "excel for accountants tutorial",
    "excel formulas for beginners",
    "pivot tables step by step",
    "excel dashboard tutorial for business",
    "vlookup and xlookup explained",
    "excel macros for automation",
    "data visualization in excel",
    "financial modeling in excel",
    "excel shortcuts for productivity",
    "power query tutorial beginners",
    
    # Professional Skills (10)
    "public speaking tips for introverts",
    "technical writing for software engineers",
    "presentation design tips powerpoint",
    "business email writing professional",
    "negotiation skills for salary",
    "linkedin profile optimization tips",
    "resume writing tips that work",
    "job interview preparation guide",
    "networking skills for career growth",
    "time management for professionals",
    
    # Student & Academic (10)
    "study techniques for medical students",
    "note taking methods for college",
    "speed reading tips and tricks",
    "exam preparation strategy tips",
    "writing essays for university",
    "math study tips calculus",
    "memorization techniques for students",
    "research paper writing guide",
    "online learning study tips",
    "focus and concentration for studying",
    
    # Tech & Software (10)
    "python programming for beginners",
    "web development tutorial html css",
    "figma design tutorial basics",
    "notion setup for students",
    "canva tutorial for beginners",
    "excel to google sheets migration",
    "wordpress website tutorial beginners",
    "photoshop basics for beginners",
    "video editing premiere pro tutorial",
    "coding interview preparation tips",
    
    # DIY & Crafts (10)
    "woodworking projects for small spaces",
    "resin art for beginners tutorial",
    "sewing basics hand stitching",
    "furniture restoration diy guide",
    "pottery wheel throwing basics",
    "leather craft wallet tutorial",
    "soap making cold process method",
    "candle making business at home",
    "embroidery for beginners patterns",
    "macrame wall hanging tutorial",
    
    # Home & Lifestyle (10)
    "minimalist living for families",
    "budgeting for young professionals",
    "meal planning for beginners",
    "organizing small bedroom ideas",
    "zero waste living tips",
    "capsule wardrobe how to build",
    "indoor plants care for beginners",
    "cleaning routine weekly schedule",
    "decluttering tips marie kondo method",
    "morning routine productive day",
    
    # Hobbies & Specialized (10)
    "aquarium setup for beginners freshwater",
    "chess openings for beginners",
    "golf swing basics for beginners",
    "bread baking sourdough starter",
    "coffee brewing techniques v60",
    "gardening vegetables for beginners",
    "calligraphy modern hand lettering",
    "skateboarding tricks for beginners",
    "knitting basics for beginners",
    "bonsai tree care for beginners"
]

# === BLACKLIST KEYWORDS - Expanded ===
BLACKLIST_KEYWORDS = [
    # Marketing/Business
    "marketing", "copywriting", "funnel", "sales", "smma", "agency",
    "clients", "monetize", "make money", "passive income", "side hustle",
    "dropshipping", "affiliate", "ecommerce", "shopify",
    
    # Entertainment
    "vlog", "prank", "challenge", "reaction", "mukbang", "asmr",
    "gaming", "let's play", "gameplay", "fortnite", "minecraft",
    
    # Medical/Clinical
    "doctor", "physician", "nurse", "therapist", "psychologist",
    "psychiatrist", "counselor", "medical", "clinical",
    
    # News/Commentary
    "news", "politics", "debate", "commentary", "podcast",
    
    # Kids
    "kids", "children", "toddler", "baby", "nursery"
]

# ---------- helpers ----------

def signal_handler(sig, frame):
    print("\n[Exit] Received interrupt, stopping scraper gracefully...")
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
    if detect:
        try:
            lang = detect(text)
            return lang == "en"
        except Exception:
            pass
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

def is_short_video(title, duration_seconds):
    """Returns True if video is a Short."""
    if duration_seconds <= 60:
        return True
    title_low = title.lower()
    if "#shorts" in title_low or "#short" in title_low:
        return True
    return False

# ---------- OpenAI interactions ----------

def generate_keywords_with_openai(n_min=KEYWORD_TITLES_MIN, n_max=KEYWORD_TITLES_MAX):
    """
    Generate YouTube video title keywords using OpenAI for hyper-niche personal brands.
    Returns list of keyword strings.
    """
    import json, time, random

    n_target = random.randint(n_min, n_max)

    prompt = """
You are generating seed YouTube VIDEO TITLES to discover UNDER-MONETIZED CREATORS who make serious, real-world transformation content — but are NOT marketers and are NOT selling yet.

Your job:
- Generate 75-100 realistic YouTube video titles.
- Each title should look like it came from a SMALL creator (3k–20k subscribers).
- Titles must be hyper-specific and niche (not broad topics).
- These creators teach, explain, or document personal change.
- They are thoughtful, grounded, and practical — not hype-driven.
- They are NOT marketers, business coaches, or monetization experts.
- They are NOT doctors, therapists, psychologists, or clinicians.
- They are NOT actively selling courses, coaching, or programs.

We are looking for creators who:
- Speak from lived experience or deep personal understanding.
- Focus on fixing real problems adults struggle with.
- Have trust, credibility, and audience resonance — but no backend yet.
- Would benefit massively from funnels, VSLs, email, or community systems built for them.

FOCUS AREAS (hyper-niche only):
1. Health & Fitness (specific methods: calisthenics, posture correction, meal prep for specific goals)
2. Creative Skills (mobile photography, color grading, sound design, music production)
3. Professional Skills (Excel for accountants, public speaking for introverts, technical writing)
4. Lifestyle & Personal Development (minimalism for families, budgeting for young professionals, study techniques)
5. Technical/Craft Skills (woodworking, 3D modeling, electronics, DIY projects)
6. Specific Transformations (career switching, language learning, skill improvement)
7. Academic/Learning (math tutoring, science experiments, research skills)
8. Creative Expression (journaling, writing, drawing, digital art)
9. Specialized Hobbies (aquariums, chess, golf, cooking, coffee brewing)
10. Home Skills (gardening, plant care, organizing, cleaning routines)

HARD EXCLUSIONS (STRICT):
NEVER generate titles that involve:
- Marketing, sales, funnels, copywriting, SEO, ads
- Making money, income claims, clients, monetization
- Agencies, SMMA, freelancing, business coaching
- Doctors, therapists, clinical authority
- Medical, psychiatric, or diagnostic framing
- Entertainment-only content (vlogs, pranks, gaming)
- News, politics, commentary

OUTPUT FORMAT (CRITICAL):
Return ONLY a valid JSON array of strings.
No explanations. No comments. No markdown.

Example format:
["calisthenics transformation for beginners", "posture exercises for desk workers", "mobile photography lighting tips"]

Now generate 75-100 different YouTube video titles that follow ALL rules above and return them as a JSON array of strings.
"""

    attempts, backoff = 3, 1
    for i in range(1, attempts + 1):
        try:
            resp = openai_client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role":"user","content":prompt}],
                temperature=0.7,
                max_tokens=1500
            )

            content = getattr(resp.choices[0].message, "content", "").strip()
            if not content:
                print(f"[OpenAI Keywords] Empty response on attempt {i}.")
                if i < attempts:
                    time.sleep(backoff); backoff *= 2
                    continue
                return []

            try:
                content = re.sub(r"^```(?:json)?\s*", "", content)
                content = re.sub(r"\s*```$", "", content)
                content = content.strip()
                keywords = json.loads(content)
                if isinstance(keywords, list) and keywords:
                    print(f"[OpenAI Keywords] Generated {len(keywords)} keywords")
                    return [k for k in keywords if isinstance(k, str)]
                else:
                    print(f"[OpenAI Keywords] Response not a valid list (attempt {i}).")
            except Exception as e:
                print(f"[OpenAI Keywords] JSON parse error (attempt {i}): {e}")

        except Exception as e:
            print(f"[OpenAI Keywords] Attempt {i} failed: {e}")

        if i < attempts:
            time.sleep(backoff); backoff *= 2
            continue
        else:
            print("[OpenAI Keywords] All attempts failed. Using fallback keywords.")
            return HYPER_NICHE_KEYWORDS[:50]  # Fallback to hardcoded keywords

    return HYPER_NICHE_KEYWORDS[:50]

def rate_lead_with_openai(channel_title, channel_description, avg_views, titles_pipe):
    """
    Asks OpenAI to rate the lead 0-10.
    HIGH rating (7-10) = Good fit, teaches skills, no existing offer
    LOW rating (0-3) = Already selling, marketing channel, or not teaching
    """
    prompt = f"""Rate this YouTube channel from 0 to 10 (integer only) based on how likely they are to become a good lead for a backend monetization operator who builds funnels, email systems, and high-ticket offers.

Give a HIGH rating (7–10) if:
- The creator teaches practical, hyper-specific skills or transformations
- They make educational, how-to, tutorial, or journey-style content
- They do NOT appear to be selling anything (no course mentions, no "work with me", no platform links)
- They are NOT a marketing, copywriting, agency, or business coaching channel
- They seem early-stage, authentic, teaching from experience
- Their niche is monetizable (people would pay for coaching/courses in this area)

Give a MID rating (4–6) if:
- The creator is educational but unclear or inconsistent
- Weak monetization hints but nothing concrete
- Average niche potential

Give a LOW rating (0–3) if:
- They teach marketing, sales, funnels, copywriting, or "how to make money"
- They already sell a course, coaching, or have strong CTAs
- They are a medical/clinical professional (doctor, therapist, nurse)
- They are mostly entertainment, vlogs, or non-English content
- They already have platform links (Skool, Kajabi, Circle, etc.)

Return ONLY a single integer between 0 and 10 with no extra words.

Channel Title: {channel_title}
Channel Description: {channel_description}
Recent Video Titles: {titles_pipe}
Average Views: {avg_views}
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
            rating = int(m.group(0))
            return rating
    except Exception as e:
        print(f"[OpenAI Rate] Error: {e}")
    return 0

# ---------- persistence ----------

def column_number_to_letter(n):
    """Convert a 1-based column index to Excel-style column letters."""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

def save_leads():
    """Append qualified leads to Google Sheet."""
    global existing_ids_global, unique_appended_this_run

    if not qualified_leads:
        print("[Save] No qualified leads to save.")
        return 0

    try:
        header_row = SHEET.row_values(1)
    except Exception as e:
        print(f"[Save] Could not read header row: {e}")
        return 0

    if not header_row:
        print("[Save] Header row is empty.")
        return 0

    use_channel_id = "Channel ID" in header_row
    key_field = "Channel ID" if use_channel_id else "Channel URL"

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
        "Country": "Country",
        "Subscribers": "Subscribers",
        "Average Views (3 Recent Non-Shorts)": "Average Views (3 Recent Non-Shorts)",
        "YouTube Bio": "YouTube Bio",
        "Email": "Email",
        "Bio Links": "Bio Links",
        "Recent Titles (15 Non-Shorts)": "Recent Titles (15 Non-Shorts)",
        "Rating": "Rating",
        "Source Keyword": "Source Keyword",
        "Last Video Published": "Last Video Published",
        "Sample Video Title": "Sample Video Title"
    }

    rows_to_add = []
    added_keys = []

    for lead in qualified_leads:
        lead_key_value = ""
        if use_channel_id:
            lead_key_value = str(lead.get("Channel ID", "")).strip()
        else:
            lead_key_value = str(lead.get("Channel URL", "")).strip()

        lead_key_value_norm = str(lead_key_value).strip().lower()
        
        if not lead_key_value_norm or \
           lead_key_value_norm in {str(k).strip().lower() for k in existing_keys} or \
           lead_key_value_norm in {str(k).strip().lower() for k in existing_ids_global}:
            continue

        row_values = []
        for col_name in header_row:
            scraper_key = next((k for k, v in FIELD_MAPPING.items() if v == col_name), None)
            value = lead.get(scraper_key, "") if scraper_key else ""
            if isinstance(value, (list, dict)):
                value = json.dumps(value, ensure_ascii=False)
            row_values.append(value)
        rows_to_add.append(row_values)
        added_keys.append(lead_key_value)

    if not rows_to_add:
        print("[Save] No new leads to add (all duplicates).")
        return 0

    try:
        header_row = SHEET.row_values(1)
        num_columns = len(header_row)

        rows_to_add_padded = [
            row + [""] * (num_columns - len(row)) for row in rows_to_add
        ]

        try:
            channel_name_col_index = header_row.index("Channel Name") + 1
            channel_name_col_letter = column_number_to_letter(channel_name_col_index)
            all_channel_names = SHEET.col_values(channel_name_col_index)
            next_row = len(all_channel_names) + 1
        except (ValueError, Exception):
            next_row = SHEET.row_count + 1

        if rows_to_add_padded:
            range_start = f"A{next_row}"
            SHEET.append_rows(rows_to_add_padded, value_input_option='RAW', table_range=range_start)
            print(f"[Save] Appended {len(rows_to_add_padded)} new leads to Google Sheet.")

        for k in added_keys:
            existing_ids_global.add(str(k).strip().lower())

        unique_appended_this_run += len(rows_to_add_padded)
        return len(rows_to_add_padded)

    except Exception as e:
        print(f"[Save] Error appending to sheet: {e}")
        return 0

def save_all_state_periodically():
    """Save leads and return count."""
    return save_leads()

# ---------- YouTube API helpers ----------

def get_random_youtube_client():
    """Return a random YouTube client from the pool."""
    return random.choice(youtube_clients)

def get_uploads_playlist_id(channel_id):
    """Get the uploads playlist ID for a channel."""
    try:
        youtube = get_random_youtube_client()
        resp = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        ).execute()
        items = resp.get("items", [])
        if items:
            return items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
    except Exception:
        # Silently skip - some channels have private/unavailable playlists
        pass
    return None

def get_recent_videos_from_playlist(youtube, playlist_id, max_results=20):
    """Get recent videos from a playlist."""
    videos = []
    try:
        resp = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=max_results
        ).execute()
        for item in resp.get("items", []):
            snippet = item.get("snippet", {})
            videos.append({
                "id": snippet.get("resourceId", {}).get("videoId"),
                "title": snippet.get("title", ""),
                "publishedAt": snippet.get("publishedAt", "")
            })
    except Exception:
        # Silently skip - playlist not found or private
        pass
    return videos

def get_videos_details_batch(youtube, video_ids):
    """Get video details in batch (up to 50 at once)."""
    if not video_ids:
        return {}
    
    details_map = {}
    
    # YouTube API allows max 50 IDs per request
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            resp = youtube.videos().list(
                part="contentDetails,statistics,snippet",
                id=",".join(batch)
            ).execute()
            for item in resp.get("items", []):
                details_map[item["id"]] = item
        except Exception:
            # Silently skip batch errors
            pass
    
    return details_map

def get_channels_details_batch(youtube, channel_ids):
    """Get channel details in batch (up to 50 at once) - OPTIMIZED."""
    if not channel_ids:
        return {}
    
    details_map = {}
    
    # YouTube API allows max 50 IDs per request
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        try:
            resp = youtube.channels().list(
                part="snippet,statistics,contentDetails",
                id=",".join(batch)
            ).execute()
            for item in resp.get("items", []):
                details_map[item["id"]] = item
        except Exception:
            # Silently skip batch errors
            pass
    
    return details_map

# ---------- main scraping logic ----------

# Load existing IDs from Google Sheet at startup
print("[Init] Loading existing channel IDs from Google Sheet...")
try:
    existing_records = SHEET.get_all_records()
    header_row = SHEET.row_values(1)
    use_channel_id = "Channel ID" in header_row
    
    if use_channel_id:
        for rec in existing_records:
            cid = str(rec.get("Channel ID", "")).strip().lower()
            if cid:
                existing_ids_global.add(cid)
    else:
        for rec in existing_records:
            curl = str(rec.get("Channel URL", "")).strip().lower()
            if curl:
                existing_ids_global.add(curl)
    
    print(f"[Init] Loaded {len(existing_ids_global)} existing channel IDs")
except Exception as e:
    print(f"[Init] Error loading existing IDs: {e}")

# Generate keywords
print("[Keywords] Generating hyper-niche keywords with OpenAI...")
video_title_keywords = generate_keywords_with_openai()

if not video_title_keywords:
    print("[Keywords] No keywords generated, using fallback list")
    video_title_keywords = HYPER_NICHE_KEYWORDS

print(f"[Keywords] Using {len(video_title_keywords)} keywords")

# Platform anchors to detect existing offers (we want to AVOID these)
platform_anchors = [
    "skool.com", "circle.so", "kajabi", "teachable", "thinkific",
    "podia", "gumroad", "patreon", "calendly", "cal.com",
    "book a call", "work with me", "join my program", "my course"
]

num_new_leads_this_run = 0
exhausted_keywords = set()
stop_scraping = False

try:
    for video_title_keyword in video_title_keywords:
        if stop_scraping:
            break
            
        if video_title_keyword in exhausted_keywords:
            continue
            
        print(f"\n[Keyword] Searching: {video_title_keyword}")
        
        title_keyword_lead_count = 0
        
        # Rotate through different countries
        for country_suffix in LOCATION_SUFFIXES:
            if stop_scraping:
                break
                
            try:
                youtube = get_random_youtube_client()
                
                # OPTIMIZED: Search for channels directly with regionCode filter
                search_response = youtube.search().list(
                    q=video_title_keyword,
                    type="channel",  # Search channels directly!
                    regionCode=country_suffix,  # Filter by country upfront
                    part="snippet",
                    maxResults=50,
                    order="relevance"
                ).execute()
                
                channel_ids_found = []
                for item in search_response.get("items", []):
                    ch_id = item.get("id", {}).get("channelId")
                    if ch_id:
                        channel_ids_found.append(ch_id)
                
                if not channel_ids_found:
                    continue
                
                print(f"[Search] Found {len(channel_ids_found)} channels for '{video_title_keyword}' in {country_suffix}")
                
                # OPTIMIZED: Batch get channel details (50 at once)
                channels_details = get_channels_details_batch(youtube, channel_ids_found)
                
                for channel_id, ch_data in channels_details.items():
                    if stop_scraping:
                        break
                    
                    snippet = ch_data.get("snippet", {})
                    stats = ch_data.get("statistics", {})
                    
                    channel_title = snippet.get("title", "")
                    channel_description = snippet.get("description", "")
                    country = snippet.get("country", country_suffix)
                    
                    # Check subscriber count IMMEDIATELY
                    try:
                        subs_count = int(stats.get("subscriberCount", 0))
                    except:
                        continue
                    
                    # Filter by subscriber range
                    if not (MIN_SUBS <= subs_count <= MAX_SUBS):
                        continue
                    
                    # Check for English content
                    if not is_english(channel_title):
                        continue
                    
                    # Check for platform anchors (AVOID channels with existing offers)
                    desc_low = channel_description.lower()
                    has_existing_offer = False
                    for anchor in platform_anchors:
                        if anchor in desc_low:
                            has_existing_offer = True
                            break
                    
                    if has_existing_offer:
                        print(f"[Skip] {channel_title} - has existing offer")
                        continue
                    
                    # Get uploads playlist
                    uploads_playlist_id = get_uploads_playlist_id(channel_id)
                    if not uploads_playlist_id:
                        continue
                    
                    recent_videos = get_recent_videos_from_playlist(youtube, uploads_playlist_id, max_results=20)
                    video_ids = [v["id"] for v in recent_videos if v.get("id")]
                    
                    videos_details_map = get_videos_details_batch(youtube, video_ids)
                    
                    # Filter out shorts and non-English
                    non_shorts_video_data = []
                    for v in recent_videos:
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
                        
                        try:
                            views = int(detail.get('statistics', {}).get('viewCount', 0))
                        except:
                            views = 0
                        
                        non_shorts_video_data.append((title, views, total_seconds, vid, published_at))
                    
                    non_shorts_video_data = non_shorts_video_data[:15]
                    recent_titles = [x[0] for x in non_shorts_video_data]
                    published_ats = [x[4] for x in non_shorts_video_data]
                    
                    # Check recency
                    recent_nonshorts = [pub for pub in published_ats if pub and is_recent(pub)]
                    if len(recent_nonshorts) < 2:
                        continue
                    
                    # Calculate average views
                    non_shorts_top3 = non_shorts_video_data[:3]
                    if non_shorts_top3:
                        avg_views = sum(x[1] for x in non_shorts_top3) // len(non_shorts_top3)
                    else:
                        avg_views = 0
                    
                    if avg_views < 300:
                        continue
                    
                    # Check blacklist
                    titles_low = " ".join(recent_titles).lower()
                    if any(bk.lower() in titles_low for bk in BLACKLIST_KEYWORDS):
                        continue
                    
                    channel_url_str = f"https://www.youtube.com/channel/{channel_id}"
                    
                    # Check if already processed
                    cid_norm = str(channel_id).strip().lower()
                    curl_norm = str(channel_url_str).strip().lower()
                    already_in_sheet = (cid_norm in existing_ids_global) or (curl_norm in existing_ids_global)
                    
                    if (cid_norm in {c.lower() for c in collected_channels}) or already_in_sheet:
                        continue
                    
                    # Rate with OpenAI
                    rating = rate_lead_with_openai(channel_title, channel_description, avg_views, "|".join(recent_titles[:15]))
                    print(f"[Rate] {channel_title} -> rating {rating}")
                    
                    if rating is None:
                        rating = 4
                    
                    if rating < 7:  # Only accept high-rated leads
                        continue
                    
                    # Get most recent video
                    def parse_dt_safe(dtstr):
                        try:
                            return datetime.strptime(dtstr, "%Y-%m-%dT%H:%M:%SZ")
                        except:
                            return datetime.min
                    
                    non_shorts_video_data_sorted = sorted(non_shorts_video_data, key=lambda x: parse_dt_safe(x[4]), reverse=True)
                    sample_video_title = non_shorts_video_data_sorted[0][0] if non_shorts_video_data_sorted else ""
                    sample_video_id = non_shorts_video_data_sorted[0][3] if non_shorts_video_data_sorted else ""
                    sample_published_at = non_shorts_video_data_sorted[0][4] if non_shorts_video_data_sorted else ""
                    
                    # Extract bio links
                    all_links = re.findall(r'(https?://[^\s]+)', channel_description)
                    bio_link = '||'.join(all_links) if all_links else ""
                    
                    # Try to scrape email
                    email = None
                    for single in (all_links or [])[:3]:
                        email = scrape_website_for_email(single)
                        if email:
                            break
                    
                    qualified_leads.append({
                        "Channel Name": channel_title,
                        "Channel ID": channel_id,
                        "Subscribers": subs_count,
                        "Country": country,
                        "Channel URL": channel_url_str,
                        "Last Video Published": sample_published_at,
                        "Sample Video Title": sample_video_title,
                        "Source Keyword": video_title_keyword,
                        "Email": email or "No",
                        "Average Views (3 Recent Non-Shorts)": avg_views,
                        "Rating": rating,
                        "YouTube Bio": channel_description,
                        "Bio Links": bio_link,
                        "Recent Titles (15 Non-Shorts)": "|".join(recent_titles[:15])
                    })
                    
                    collected_channels.add(channel_id)
                    num_new_leads_this_run += 1
                    title_keyword_lead_count += 1
                    
                    print(f"[Qualified+] {channel_title} | Rating {rating} | Subs: {subs_count}")
                    
                    # Check if reached target
                    if unique_appended_this_run >= TARGET_LEADS:
                        stop_scraping = True
                        break
                    
                    # Auto-save periodically
                    if len(qualified_leads) % AUTO_SAVE_EVERY == 0:
                        added_now = save_all_state_periodically()
                        if unique_appended_this_run >= TARGET_LEADS:
                            stop_scraping = True
                            break
                    
                    time.sleep(DELAY_BETWEEN_REQUESTS)
                    
            except Exception as e:
                print(f"[Error] {e}")
                continue
        
        # Mark keyword as exhausted if no leads found
        if title_keyword_lead_count == 0:
            exhausted_keywords.add(video_title_keyword)
        
        # Random auto-save
        if random.random() < 0.2:
            save_all_state_periodically()

except KeyboardInterrupt:
    print("\n[Interrupted] Stopped by user.")
finally:
    print(f"\n[Finish] Collected {len(qualified_leads)} in-memory qualified leads. Saving to Google Sheet...")
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