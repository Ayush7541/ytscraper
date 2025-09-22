# scraper copy.py  -- fixed version
import os
import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- Config ---
API_KEYS = [
    "AIzaSyDe7axLxXM73cBwgWVIiExaxFPo504cTPA",
    "AIzaSyDhe5odit54OSGWZ4BxDw4-5WO76uYBffE",
    "AIzaSyB2p8GXoG_AOm9TEDmqpoQiHoyO7IIe3eU",
    "AIzaSyC2m0NfOQKRuqDPhGucSw22ih3TrYbVoVY"
]

# Build YouTube clients (one per key). If any key is invalid, build may raise — handle that at runtime.
YOUTUBE_API_CLIENTS = []
for key in API_KEYS:
    try:
        YOUTUBE_API_CLIENTS.append(build('youtube', 'v3', developerKey=key))
    except Exception as e:
        print(f"[Init] Warning: failed to build youtube client for a key: {e}")

# Google Sheets setup - adjust json filename and sheet id as needed
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS = ServiceAccountCredentials.from_json_keyfile_name("google_sheets_key.json", SCOPE)
GSPREAD_CLIENT = gspread.authorize(CREDS)
SHEET = GSPREAD_CLIENT.open_by_key("1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE").worksheet("raw leads")

# Delay between requests
DELAY_BETWEEN_REQUESTS = 1.2

# --- Helpers ---

def is_english(text):
    if not text:
        return False
    try:
        ascii_chars = sum(c.isascii() for c in text)
        return len(text) > 0 and (ascii_chars / len(text) > 0.8)
    except Exception:
        return True  # be permissive if anything odd

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

# --- Channel ID Resolver (robust for /channel/, /@handle, /c/, /user/) ---
def resolve_channel_id(youtube, url):
    url = (url or "").strip()
    if not url:
        return None

    # /channel/UCxxxx...
    m = re.search(r'youtube\.com/(?:channel/)([A-Za-z0-9_-]{24,})', url)
    if m:
        return m.group(1)

    # /user/Username  -> legacy "forUsername"
    m = re.search(r'youtube\.com/user/([A-Za-z0-9._-]+)', url)
    if m:
        username = m.group(1)
        try:
            resp = youtube.channels().list(part="id", forUsername=username).execute()
            items = resp.get("items", [])
            if items:
                return items[0]["id"]
        except Exception:
            pass

    # /@handle  -> try forHandle, then fallback to search by @handle
    m = re.search(r'youtube\.com/@([A-Za-z0-9._-]+)', url)
    if m:
        handle = m.group(1)
        # Try channels().list with forHandle (newer)
        try:
            resp = youtube.channels().list(part="id", forHandle=handle).execute()
            items = resp.get("items", [])
            if items:
                return items[0]["id"]
        except Exception:
            pass
        # fallback: search by "@handle" and extract channelId
        try:
            resp = youtube.search().list(q=f"@{handle}", type="channel", maxResults=1, part="id").execute()
            items = resp.get("items", [])
            if items:
                cid = items[0].get("id", {}).get("channelId")
                if cid:
                    return cid
        except Exception:
            pass

    # /c/customName  -> search for channels matching that custom name (best-effort)
    m = re.search(r'youtube\.com/c/([A-Za-z0-9._-]+)', url)
    if m:
        custom = m.group(1)
        try:
            resp = youtube.search().list(q=custom, type="channel", maxResults=5, part="id").execute()
            items = resp.get("items", [])
            if items:
                cid = items[0].get("id", {}).get("channelId")
                if cid:
                    return cid
        except Exception:
            pass

    # If it looks like a plain channel id present somewhere (UC...)
    m = re.search(r'(UC[0-9A-Za-z_-]{20,})', url)
    if m:
        return m.group(1)

    return None

# --- Fetch channel details ---
def fetch_channel_details(youtube, channel_id):
    try:
        resp = youtube.channels().list(
            part="snippet,statistics,contentDetails,brandingSettings",
            id=channel_id
        ).execute()
        items = resp.get("items", [])
        if not items:
            print(f"[Debug] No items returned when fetching details for channel: {channel_id}. Full response: {resp}")
            return None
        return items[0]
    except HttpError as e:
        # Provide readable debug for HttpError (quota, forbidden, etc.)
        try:
            content = e.content.decode() if hasattr(e, "content") else str(e)
        except Exception:
            content = str(e)
        print(f"[HttpError] channels().list failed for {channel_id}: {content}")
        return None
    except Exception as e:
        print(f"[Error] fetch_channel_details unexpected error for {channel_id}: {e}")
        return None
def fetch_recent_videos_search(youtube, channel_id, max_results=30):
    """Faster, best-effort fetch of recent videos using search.list ordered by date.
    Falls back to uploads playlist when search returns nothing.
    Returns list of dicts: {id, title, publishedAt}.
    """
    videos = []
    try:
        resp = youtube.search().list(
            part="snippet",
            channelId=channel_id,
            order="date",
            type="video",
            maxResults=min(50, max_results)
        ).execute()
        for item in resp.get("items", []):
            vid = item.get("id", {}).get("videoId")
            title = item.get("snippet", {}).get("title", "")
            published_at = item.get("snippet", {}).get("publishedAt", "")
            if vid:
                videos.append({"id": vid, "title": title, "publishedAt": published_at})
    except HttpError as e:
        try:
            content = e.content.decode() if hasattr(e, "content") else str(e)
        except Exception:
            content = str(e)
        print(f"[HttpError] search().list failed for channel {channel_id}: {content}")
    except Exception as e:
        print(f"[Error] fetch_recent_videos_search for {channel_id}: {e}")
    return videos

def get_uploads_playlist_id(channel):
    return channel.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")

def fetch_playlist_videos(youtube, playlist_id, max_results=50):
    videos = []
    page_token = None
    while len(videos) < max_results:
        try:
            resp = youtube.playlistItems().list(
                part="contentDetails,snippet",
                playlistId=playlist_id,
                maxResults=min(50, max_results - len(videos)),
                pageToken=page_token
            ).execute()
            for item in resp.get("items", []):
                vid = item.get("contentDetails", {}).get("videoId")
                title = item.get("snippet", {}).get("title", "")
                published_at = item.get("contentDetails", {}).get("videoPublishedAt", "")
                videos.append({"id": vid, "title": title, "publishedAt": published_at})
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        except Exception:
            break
    return videos

def fetch_video_details(youtube, video_ids):
    out = {}
    if not video_ids:
        return out
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            resp = youtube.videos().list(
                part="statistics,contentDetails,snippet",
                id=",".join(batch)
            ).execute()
            for v in resp.get("items", []):
                out[v["id"]] = v
        except Exception:
            pass
    return out

def is_short_video(title, duration_seconds):
    if "#shorts" in (title or "").lower():
        return True
    if duration_seconds is not None and duration_seconds <= 60:
        return True
    return False

# --- Main ---
def main():
    # Input file detection
    input_csv = "input_channels.csv"
    input_xlsx = "input_channels.xlsx"
    if os.path.exists(input_csv):
        df = pd.read_csv(input_csv)
    elif os.path.exists(input_xlsx):
        df = pd.read_excel(input_xlsx)
    else:
        print("No input file found. Please provide input_channels.csv or input_channels.xlsx with a 'Channel URL' column.")
        return

    if "Channel URL" not in df.columns:
        print("Input file must contain a 'Channel URL' column.")
        return
    channel_urls = df["Channel URL"].dropna().unique().tolist()
    if not channel_urls:
        print("No channel URLs found in file.")
        return

    # Prepare header and field order
    FIELD_ORDER = [
        "Channel Name",
        "Channel URL",
        "Channel ID",
        "Creator Name",
        "Country",
        "Subscribers",
        "Average Views (3 Recent Non-Shorts)",
        "YouTube Bio",
        "Email",
        "Bio Links",
        "Recent Titles (15 Non-Shorts)"
    ]
    col_count = len(FIELD_ORDER)
    # Column letter for update range (works up to 26 columns; we only have 11)
    col_letter = chr(64 + col_count)  # 11 => 'K'

    # Ensure header exists and is correct (update row 1 rather than inserting duplicate)
    try:
        header_row = SHEET.row_values(1)
    except Exception:
        header_row = []
    if not header_row:
        SHEET.insert_row(FIELD_ORDER, index=1)
    else:
        # update first row to be consistent with FIELD_ORDER
        try:
            SHEET.update([FIELD_ORDER], range_name=f"A1:{col_letter}1")
        except Exception:
            # If update fails, attempt insert (best-effort)
            try:
                SHEET.insert_row(FIELD_ORDER, index=1)
            except Exception:
                pass

    # Read existing records safely for dedupe
    existing_records = []
    try:
        existing_records = SHEET.get_all_records()
        existing_ids = {str(r.get("Channel ID", "")).strip() for r in existing_records if str(r.get("Channel ID", "")).strip()}
    except Exception:
        existing_records = []
        existing_ids = set()

    results = []
    total = len(channel_urls)
    processed = 0
    appended = 0
    api_idx = 0

    for url in channel_urls:
        channel_start = time.time()
        # Guard: ensure we have YouTube API clients
        if not YOUTUBE_API_CLIENTS:
            print("[Error] No YouTube API clients available. Check API keys.")
            return

        ch = None
        channel_id = None
        tried_clients = 0
        max_try = len(YOUTUBE_API_CLIENTS)

        # Try resolving & fetching using rotated API clients to mitigate key-specific failures
        for _ in range(max_try):
            youtube = YOUTUBE_API_CLIENTS[api_idx % len(YOUTUBE_API_CLIENTS)]
            api_idx += 1
            tried_clients += 1

            # First try to resolve the channel id with this client (needed for /user, /@handle, /c/ cases)
            temp_id = resolve_channel_id(youtube, url)
            if not temp_id:
                # Try next client
                continue
            channel_id = temp_id

            # Quick dedupe check
            if channel_id in existing_ids:
                print(f"Skipping already existing channel: {channel_id}")
                break

            # Try fetching channel details with this client
            ch = fetch_channel_details(youtube, channel_id)
            if ch:
                break

        if not channel_id:
            print(f"Could not resolve channel ID for URL (after trying {tried_clients} clients): {url}")
            continue
        if channel_id in existing_ids:
            # already printed skip message above
            continue
        if not ch:
            print(f"Could not fetch details for channel (after trying {tried_clients} clients): {channel_id}")
            continue

        snippet = ch.get("snippet", {}) or {}
        stats = ch.get("statistics", {}) or {}
        branding = ch.get("brandingSettings", {}).get("channel", {}) or {}

        channel_url = f"https://www.youtube.com/channel/{channel_id}"
        channel_name = snippet.get("title", "")
        creator_name = branding.get("title", "") or ""
        country = snippet.get("country", "") or ""
        subscribers = stats.get("subscriberCount", "") or ""
        yt_bio = snippet.get("description", "") or ""

        # Extract links from bio & try scrape for email
        bio_links_list = re.findall(r'(https?://[^\s,]+)', yt_bio)
        bio_links = "||".join(bio_links_list)
        email = ""
        for link in bio_links_list[:3]:
            e = scrape_website_for_email(link)
            if e:
                email = e
                break

        # Attempt to quickly fetch recent videos using search (ordered by date) -- faster than walking uploads playlist
        videos = fetch_recent_videos_search(youtube, channel_id, max_results=40)
        if not videos:
            # fallback: try uploads playlist (older method)
            uploads_playlist_id = get_uploads_playlist_id(ch)
            if not uploads_playlist_id:
                print(f"No uploads playlist for channel: {channel_id}")
                continue
            videos = fetch_playlist_videos(youtube, uploads_playlist_id, max_results=50)

        video_ids = [v["id"] for v in videos if v.get("id")]
        video_details = fetch_video_details(youtube, video_ids)

        non_shorts = []
        for v in videos:
            vid = v.get("id")
            title = v.get("title", "") or ""
            published_at = v.get("publishedAt", "") or ""
            detail = video_details.get(vid, {}) or {}
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
            except Exception:
                views = 0
            non_shorts.append((title, views, total_seconds, vid, published_at))

        # Sort by published date descending (newest first)
        def parse_dt_safe(dtstr):
            try:
                return datetime.strptime(dtstr, "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                return datetime.min
        non_shorts = sorted(non_shorts, key=lambda x: parse_dt_safe(x[4]), reverse=True)
        recent_titles = [x[0] for x in non_shorts[:15]]

        # Average views of 3 most recent non-shorts
        top3 = non_shorts[:3]
        avg_views = sum(x[1] for x in top3) // len(top3) if top3 else 0

        results.append({
            "Channel Name": channel_name,
            "Channel URL": channel_url,
            "Channel ID": channel_id,
            "Creator Name": creator_name,
            "Country": country,
            "Subscribers": subscribers,
            "Average Views (3 Recent Non-Shorts)": avg_views,
            "YouTube Bio": yt_bio,
            "Email": email or "",
            "Bio Links": bio_links,
            "Recent Titles (15 Non-Shorts)": "|".join(recent_titles),
        })
        processed += 1
        print(f"Processed {processed}/{total}: {channel_name or channel_id}")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    # Append to Google Sheet (dedupe by Channel ID)
    # Refresh existing records just before append
    existing_records = []
    try:
        existing_records = SHEET.get_all_records()
        existing_ids = {str(r.get("Channel ID", "")).strip() for r in existing_records if str(r.get("Channel ID", "")).strip()}
    except Exception:
        existing_records = []
        existing_ids = set()

    rows_to_add = []
    for row in results:
        if row["Channel ID"] in existing_ids:
            continue
        row_values = [row.get(col, "") for col in FIELD_ORDER]
        rows_to_add.append(row_values)
        appended += 1

    if rows_to_add:
        try:
            # Get all sheet values
            all_vals = SHEET.get_all_values()
            # Start searching from row 2
            start_row = 2
            for idx, row in enumerate(all_vals[1:], start=2):
                if not any(str(cell).strip() for cell in row):
                    start_row = idx
                    break
            end_row = start_row + len(rows_to_add) - 1

            # Expand sheet if needed
            if end_row > SHEET.row_count:
                SHEET.add_rows(end_row - SHEET.row_count + 10)

            # Write rows into first available empty spot
            SHEET.update(rows_to_add, range_name=f"A{start_row}:{col_letter}{end_row}", value_input_option="RAW")
            print(f"[Sheet] Added {len(rows_to_add)} new rows starting at row {start_row}.")
        except Exception as e:
            print(f"[Sheet] Failed to insert rows at first empty spot: {e}. Falling back to row-by-row append.")
            r = start_row
            for rv in rows_to_add:
                try:
                    SHEET.insert_row(rv, index=r)
                    r += 1
                except Exception as e2:
                    print(f"[Sheet] Failed inserting row at {r}: {e2}")

    print(f"\nSummary: {processed} channels processed, {appended} new leads appended to Google Sheet.")

if __name__ == "__main__":
    main()