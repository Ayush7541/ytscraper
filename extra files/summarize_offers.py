"""
summarize_offers.py

Reads the Google Sheet ("raw leads"), selects rows where:
  - Approved == TRUE
  - Queue Summary == TRUE
  - and any of [Niche, Selling, Target Audience, One-Line Pitch] is empty

Builds context from:
  - YouTube Bio
  - Recent Titles (15 Non-Shorts)
  - Landing Page Copy (optional)

Calls OpenAI (gpt-4o-mini by default) with a strict JSON schema
and writes back:
  - Niche, Selling, Target Audience, One-Line Pitch
  - Status = Done: Summary (or Error: ...)
  - Updated At = timestamp (yyyy-mm-dd HH:MM)

After each row (success or error) it UNTICKS "Queue Summary" so it won't re-run.

It processes in batches (default 15) and loops until the queue is empty.

Usage:
  OPENAI_API_KEY=sk-... python summarize_offers.py --sheet-id YOUR_SHEET_ID --batch-size 15
"""

import os
import json
import time
import argparse
from datetime import datetime
import requests

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI
from gspread.utils import rowcol_to_a1
# Formatting utilities for cell background color
from gspread_formatting import format_cell_range, CellFormat, Color
# ----------- Hygiene, language, regex helpers -----------
from typing import Tuple
import re

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
try:
    from langdetect import detect
except Exception:
    detect = None

# ---------------- Configuration ----------------
SHEET_NAME = "raw leads"
OPENAI_MODEL = "gpt-4o-mini"
OPENAI_TEMPERATURE = 0.0
OPENAI_TIMEOUT = 60  # seconds
DEFAULT_SHEET_ID = "1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE"
DEFAULT_BATCH_SIZE = 15
FALLBACK_OPENAI_API_KEY = ""
PAUSE_BETWEEN_ROWS_SEC = 0.25
PAUSE_BETWEEN_BATCHES_SEC = 2.0

#
# --- Hardcoded overrides (optional) ---
# If you fill these, you can run the script with:  python summarize_offers.py
# without passing env vars or CLI args.
HARDCODE_OPENAI_API_KEY = "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"
HARDCODE_SHEET_ID = "1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE"
HARDCODE_BATCH_SIZE = 15

# Hygiene & control toggles
DRY_RUN = False
MAX_BIO_CHARS = 1200
MAX_TITLES_CHARS = 1200
PAUSE_RETRY_SEC = 1.0

# Column header names (must match row 1 text exactly)
COLN = {
    "approved": "Approved",
    "queue": "Queue Summary",
    "bio": "YouTube Bio",
    "titles": "Recent Titles (15 Non-Shorts)",
    "lp": "Landing Page Copy",
    "niche": "Niche",
    "selling": "Selling",
    "target": "Target Audience",
    "pitch": "One-Line Pitch",
    "status": "Status",
    "updated": "Updated At",
    "channel": "Channel Name",         # optional, used for context only
    "url": "Channel URL",              # optional, used for context only
    "summary_status": "Summary Status", # for future/optional use
}

def get_last_data_row(sh) -> int:
    """Return the last row index that contains any data (1-based, including header)."""
    try:
        all_vals = sh.get_all_values()
        return len(all_vals)
    except Exception:
        # Fallback: at least header
        return 1

def get_last_header_col(sh) -> int:
    """Return number of header columns based on row 1 values."""
    header = get_header_with_retry(sh)
    return len(header)

def a1_range(r: int, c_start: int, r_count: int, c_count: int) -> str:
    """Build an A1 range string for a rectangle."""
    start = rowcol_to_a1(r, c_start)
    end = rowcol_to_a1(r + r_count - 1, c_start + c_count - 1)
    return f"{start}:{end}"

# ----------- Input hygiene and language helpers -----------
EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "]+",
    flags=re.UNICODE,
)

def strip_emojis(s: str) -> str:
    return EMOJI_RE.sub("", s or "")

def normalize_title(t: str) -> str:
    t = strip_emojis(t or "")
    t = t.replace("|", " ").strip()
    t = re.sub(r"\s+", " ", t)
    return t

def dedupe_titles_pipe(titles_pipe: str, max_chars: int = 1200) -> str:
    parts = [normalize_title(p) for p in (titles_pipe or "").split("|") if p.strip()]
    seen = set()
    uniq = []
    for p in parts:
        key = p.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    joined = "|".join(uniq)
    return joined[:max_chars]

def looks_non_english(text: str) -> bool:
    text = text or ""
    if not text.strip():
        return False
    ascii_ratio = sum(1 for ch in text if ord(ch) < 128) / max(1, len(text))
    if ascii_ratio < 0.7:
        return True
    if detect:
        try:
            lang = detect(text[:4000])
            return lang and lang.lower() != "en"
        except Exception:
            return False
    return False

def infer_mini_bio_from_titles(titles_pipe: str) -> str:
    titles = [t.strip() for t in (titles_pipe or "").split("|") if t.strip()]
    if not titles:
        return ""
    words = re.findall(r"[A-Za-z][A-Za-z0-9]+", " ".join(titles))
    freq = {}
    for w in words:
        wl = w.lower()
        if len(wl) < 3:
            continue
        freq[wl] = freq.get(wl, 0) + 1
    top = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:8]
    keys = ", ".join([k for k,_ in top])
    return f"Channel likely covers: {keys}"

# ---------------- Helpers ----------------
def get_env(name: str, default: str = "") -> str:
    """Return env var or default (empty if missing)."""
    return os.getenv(name, default).strip()

def open_sheet(spreadsheet_id: str):
    """Authorize with service account and open the worksheet."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("google_sheets_key.json", scope)
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(spreadsheet_id)
    sh = ss.worksheet(SHEET_NAME)
    return sh

def header_index_map(sh):
    """Return header -> 1-based column index mapping."""
    header = get_header_with_retry(sh)
    return {name: (header.index(name) + 1) if name in header else None for name in header}  # map existing header names
# --------------- Retry Helper for Sheet Header ---------------
def get_header_with_retry(sh, retries=3, delay=5):
    """Attempt to get sh.row_values(1), retrying up to `retries` times with `delay` seconds between."""
    last_exc = None
    for attempt in range(1, retries+1):
        try:
            return sh.row_values(1)
        except Exception as e:
            last_exc = e
            if attempt < retries:
                print(f"[WARN] Failed to fetch header row (attempt {attempt}/{retries}), retrying in {delay}s...")
                time.sleep(delay)
            else:
                print(f"[ERROR] Failed to fetch header row after {retries} attempts.")
    raise RuntimeError(f"Could not fetch sheet header row after {retries} retries: {last_exc}")

def need_summary(row_vals: list, col: dict) -> bool:
    """Return True if row needs summarization and is queued."""
    def cell(key):
        idx = col.get(COLN[key])
        if not idx:
            return ""
        return (row_vals[idx-1] if idx-1 < len(row_vals) else "")

    approved = str(cell("approved")).strip().upper() in ("TRUE", "1", "YES")
    queued = str(cell("queue")).strip().upper() in ("TRUE", "1", "YES")

    if not (approved and queued):
        return False

    # Missing any of the target fields?
    niche = str(cell("niche")).strip()
    selling = str(cell("selling")).strip()
    target = str(cell("target")).strip()
    pitch = str(cell("pitch")).strip()
    return (not niche) or (not selling) or (not target) or (not pitch)

def cut(text: str, max_len: int = 1200) -> str:
    text = (text or "").strip()
    return text[:max_len]

def build_messages(bio: str, titles: str, lp_copy: str, channel: str = "", url: str = ""):
    """Construct system+user messages for OpenAI (with clamped, de-duped inputs)."""
    bio_clean = strip_emojis((bio or ""))[:MAX_BIO_CHARS]
    titles_clean = dedupe_titles_pipe((titles or ""), max_chars=MAX_TITLES_CHARS)
    if not bio_clean and titles_clean:
        bio_clean = infer_mini_bio_from_titles(titles_clean)

    system = (
        "You summarize YouTube creators for outbound personalization. "
        "Return STRICT JSON only (no prose, no markdown). "
        "If there is a conflict between BIO and TITLES, PREFER BIO. "
        "Be concise. If language appears non-English, still output English; "
        "use 'unknown' if selling type is unclear. Only use the allowed selling values."
    )

    user = f"""CONTEXT
Channel: {channel or ""} | {url or ""}
BIO:
{bio_clean}

RECENT TITLES (pipe-separated):
{titles_clean}

LANDING PAGE COPY (optional):
{cut(lp_copy)}

Return ONLY this JSON exactly:
{{
  "niche": "concise niche/topic",
  "selling": "one of [course, membership, skool, appointment, lead_mag net, product, patreon, community, consulting, coaching, unknown]",
  "target_audience": "one line describing who this is for",
  "one_line_pitch": "ONE sentence, persuasive, based on BIO > titles if conflict"
}}"""
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

def call_openai(client: OpenAI, messages):
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=OPENAI_TEMPERATURE,
            response_format={"type": "json_object"},
            messages=messages,
            timeout=OPENAI_TIMEOUT,
        )
        return resp.choices[0].message.content
    except Exception:
        time.sleep(PAUSE_RETRY_SEC)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=OPENAI_TEMPERATURE,
            response_format={"type": "json_object"},
            messages=messages,
            timeout=OPENAI_TIMEOUT,
        )
        return resp.choices[0].message.content

# ----------- Selling normalization -----------
ALLOWED_SELLING = {
    "course","membership","skool","appointment","lead_magnet","product",
    "patreon","community","consulting","coaching","unknown"
}

def normalize_selling(val: str) -> str:
    v = (val or "").strip().lower().replace("-", "_").replace(" ", "_")
    if v in ALLOWED_SELLING:
        return v.replace("_", " ")
    # soft mapping
    if v in {"lead magnet","leadmagnet"}:
        return "lead magnet"
    if any(x in v for x in ["call","booking","calendar","appointment"]):
        return "appointment"
    return "unknown"

def ensure_required_headers(colmap: dict):
    """Validate that required headers exist; raise if missing."""
    required = [
        COLN["approved"], COLN["queue"],
        COLN["bio"], COLN["titles"],
        COLN["niche"], COLN["selling"], COLN["target"], COLN["pitch"],
        COLN["status"], COLN["updated"],
    ]
    missing = [h for h in required if h not in colmap or not colmap[h]]
    if missing:
        raise RuntimeError(f"Missing required headers in sheet: {missing}")

def update_cells_batch(sh, updates):
    """
    updates: list of dicts with keys:
      - row (int)
      - col_name (header string)
      - value (any)
    This function groups updates per-row to minimize API calls.
    """
    if not updates:
        return

    ALLOWED_UPDATE_COLUMNS = {
        COLN["queue"],
        COLN["summary_status"],
        COLN["niche"],
        COLN["selling"],
        COLN["target"],
        COLN["pitch"],
        COLN["status"],
        COLN["updated"],
    }

    per_row = {}
    for u in updates:
        r = u["row"]
        per_row.setdefault(r, []).append(u)

    header = sh.row_values(1)

    for r, ups in per_row.items():
        filtered_ups = [u for u in ups if u["col_name"] in header and u["col_name"] in ALLOWED_UPDATE_COLUMNS]
        if not filtered_ups:
            continue

        # Map absolute column index -> value
        col_to_val = {}
        for u in filtered_ups:
            cidx = header.index(u["col_name"]) + 1
            val = u["value"]
            if isinstance(val, (list, dict)):
                val = json.dumps(val, ensure_ascii=False)
            col_to_val[cidx] = val

        # Sort into contiguous runs
        cols_sorted = sorted(col_to_val.keys())
        runs = []
        start = prev = None
        for c in cols_sorted:
            if prev is None or c == prev + 1:
                if start is None:
                    start = c
            else:
                runs.append((start, prev))
                start = c
            prev = c
        if start is not None:
            runs.append((start, prev))

        for c_start, c_end in runs:
            # Only update the specific cells provided, do not overwrite unrelated cells in the range.
            row_vals = []
            for c in range(c_start, c_end + 1):
                if c in col_to_val:
                    row_vals.append(col_to_val[c])
                else:
                    # Do not overwrite unrelated cells; skip updating this cell.
                    row_vals.append("")
            # Only update columns for which we have a value (i.e., where col_to_val is present)
            # If the run is not contiguous (i.e. some columns in the run are not being updated), we should only update the specific columns.
            # So, update each cell individually if not all columns in the run are present in col_to_val.
            all_cols_in_run = all(c in col_to_val for c in range(c_start, c_end + 1))
            if all_cols_in_run:
                rng = a1_range(r, c_start, 1, len(row_vals))
                sh.update(range_name=rng, values=[row_vals])
            else:
                # Only update the individual cells that are present in col_to_val
                for c in range(c_start, c_end + 1):
                    if c in col_to_val:
                        rng = a1_range(r, c, 1, 1)
                        sh.update(range_name=rng, values=[[col_to_val[c]]])

# ---------------- Core processing ----------------
def process_once(sh, colmap, client: OpenAI, limit: int) -> int:
    """Process up to `limit` queued rows. Returns count processed in this batch."""
    last_row = get_last_data_row(sh)
    if last_row < 2:
        return 0

    header = get_header_with_retry(sh)
    last_col = len(header)
    rng = a1_range(2, 1, last_row - 1, last_col)
    data = sh.get(rng) if last_row > 1 else []

    processed = 0
    for idx, row_vals in enumerate(data, start=2):
        if processed >= limit:
            break

        if not need_summary(row_vals, colmap):
            continue

        # Helper to read cell by header name
        def cell(hname):
            c = colmap.get(hname)
            return (row_vals[c-1] if c and (c-1) < len(row_vals) else "")

        # Pre-mark status
        updates = []
        if COLN["status"] in header:
            updates.append({"row": idx, "col_name": COLN["status"], "value": "Processing: Summary"})
        if COLN["updated"] in header:
            updates.append({"row": idx, "col_name": COLN["updated"], "value": datetime.now().strftime("%Y-%m-%d %H:%M")})
        update_cells_batch(sh, updates)

        bio = str(cell(COLN["bio"]))
        titles = str(cell(COLN["titles"]))
        lp = str(cell(COLN["lp"]))
        channel = str(cell(COLN["channel"])) if COLN["channel"] in header else ""
        url = str(cell(COLN["url"])) if COLN["url"] in header else ""

        # Capture originals for idempotence check
        orig_niche = str(cell(COLN["niche"]))
        orig_selling = str(cell(COLN["selling"]))
        orig_target = str(cell(COLN["target"]))
        orig_pitch = str(cell(COLN["pitch"]))
        orig_queue = str(cell(COLN["queue"])).strip().upper() in ("TRUE","1","YES")

        # Hygiene preprocessing
        bio_hyg = strip_emojis(bio)[:MAX_BIO_CHARS]
        titles_hyg = dedupe_titles_pipe(titles, MAX_TITLES_CHARS)
        if not bio_hyg and titles_hyg:
            bio_hyg = infer_mini_bio_from_titles(titles_hyg)

        # Non-English guard (skip before OpenAI)
        if looks_non_english(bio_hyg + " " + titles_hyg):
            updates = []
            if "Status Code" in header:
                updates.append({"row": idx, "col_name": "Status Code", "value": "NON_EN"})
            updates.append({"row": idx, "col_name": COLN["status"], "value": "Skipped: Non-English"})
            updates.append({"row": idx, "col_name": COLN["updated"], "value": datetime.now().strftime("%Y-%m-%d %H:%M")})
            # leave Queue Summary checked so you can re-queue after editing
            update_cells_batch(sh, updates)
            processed += 1
            time.sleep(PAUSE_BETWEEN_ROWS_SEC)
            continue

        try:
            if DRY_RUN:
                parsed = {
                    "niche": "sample niche",
                    "selling": "course",
                    "target_audience": "beginners in the niche",
                    "one_line_pitch": "Concise, persuasive pitch."
                }
            else:
                msgs = build_messages(bio_hyg, titles_hyg, lp, channel, url)
                raw = call_openai(client, msgs)
                try:
                    parsed = json.loads(raw)
                except Exception:
                    # Retry once with stricter instruction
                    strict_user = msgs[1]["content"] + "\n\nReturn STRICT JSON only. No commentary."
                    raw = call_openai(client, [
                        {"role": "system", "content": msgs[0]["content"]},
                        {"role": "user", "content": strict_user}
                    ])
                    parsed = json.loads(raw)

            niche_v = str(parsed.get("niche","")).strip()
            selling_v = normalize_selling(parsed.get("selling",""))
            target_v = str(parsed.get("target_audience","")).strip()
            pitch_v = str(parsed.get("one_line_pitch","")).strip()

            if not (niche_v or target_v or pitch_v):
                raise ValueError("Empty summary fields")

            # Concurrency guard: ensure row unchanged and still queued
            header_now = get_header_with_retry(sh)
            # Build A1 range that covers the 5 relevant cells in this row (queue + 4 targets)
            needed_cols = []
            for name in [COLN["queue"], COLN["niche"], COLN["selling"], COLN["target"], COLN["pitch"]]:
                if name in header_now:
                    needed_cols.append(header_now.index(name) + 1)
            if needed_cols:
                cmin, cmax = min(needed_cols), max(needed_cols)
                rng_check = a1_range(idx, cmin, 1, cmax - cmin + 1)
                cur_vals = (sh.get(rng_check) or [[""]])[0]
                # Map back to current values
                cur_map = {}
                for name in [COLN["queue"], COLN["niche"], COLN["selling"], COLN["target"], COLN["pitch"]]:
                    if name in header_now:
                        pos = header_now.index(name) - (cmin - 1)
                        cur_map[name] = cur_vals[pos] if 0 <= pos < len(cur_vals) else ""
                still_queued = str(cur_map.get(COLN["queue"], "")).strip().upper() in ("TRUE","1","YES")
                if not still_queued:
                    # Skip write, someone unqueued it mid-flight
                    updates = []
                    if "Status Code" in header_now:
                        updates.append({"row": idx, "col_name": "Status Code", "value": "SKIPPED"})
                    updates.append({"row": idx, "col_name": COLN["status"], "value": "Skipped: Row changed"})
                    updates.append({"row": idx, "col_name": COLN["updated"], "value": datetime.now().strftime("%Y-%m-%d %H:%M")})
                    update_cells_batch(sh, updates)
                    processed += 1
                    time.sleep(PAUSE_BETWEEN_ROWS_SEC)
                    continue

            updates = []
            updates.append({"row": idx, "col_name": COLN["niche"], "value": niche_v})
            updates.append({"row": idx, "col_name": COLN["selling"], "value": selling_v})
            updates.append({"row": idx, "col_name": COLN["target"], "value": target_v})
            updates.append({"row": idx, "col_name": COLN["pitch"], "value": pitch_v})
            updates.append({"row": idx, "col_name": COLN["status"], "value": "Done: Summary"})
            updates.append({"row": idx, "col_name": COLN["updated"], "value": datetime.now().strftime("%Y-%m-%d %H:%M")})
            if "Status Code" in header_now:
                updates.append({"row": idx, "col_name": "Status Code", "value": "OK"})
            # Untick the Queue Summary checkbox (set to FALSE)
            updates.append({"row": idx, "col_name": COLN["queue"], "value": False})
            update_cells_batch(sh, updates)
            # Apply light green background to the Queue Summary cell
            if COLN["queue"] in header_now:
                col_index = header_now.index(COLN["queue"]) + 1
                rng = a1_range(idx, col_index, 1, 1)
                fmt = CellFormat(backgroundColor=Color(red=0.85, green=0.94, blue=0.84))
                format_cell_range(sh, rng, fmt)

        except Exception as e:
            updates = []
            updates.append({"row": idx, "col_name": COLN["status"], "value": f"Error: {str(e)[:160]}"})
            updates.append({"row": idx, "col_name": COLN["updated"], "value": datetime.now().strftime("%Y-%m-%d %H:%M")})
            # Do NOT change Queue Summary here
            update_cells_batch(sh, updates)

        processed += 1
        time.sleep(PAUSE_BETWEEN_ROWS_SEC)

    return processed

def process_all(sh, client: OpenAI, batch_size: int) -> int:
    """Loop batches until no more rows qualify. Returns total processed."""
    header = get_header_with_retry(sh)
    colmap = {name: (header.index(name) + 1) if name in header else None for name in COLN.values()}
    ensure_required_headers(colmap)

    total = 0
    while True:
        done = process_once(sh, colmap, client, batch_size)
        total += done
        if done == 0:
            break
        time.sleep(PAUSE_BETWEEN_BATCHES_SEC)
    print(f"[Summary] processed={total}")
    return total

# ---------------- Entry point ----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sheet-id", default="", help="Google Sheet ID (from the URL)")
    parser.add_argument("--batch-size", type=int, default=0)
    args = parser.parse_args()

    # Resolve config (priority: hardcoded > CLI > defaults)
    sheet_id = (HARDCODE_SHEET_ID or args.sheet_id or DEFAULT_SHEET_ID).strip()

    # Open sheet first to determine batch size by ticked rows in "Queue Summary"
    sh = open_sheet(sheet_id)
    header = get_header_with_retry(sh)
    queue_col_idx = header.index(COLN["queue"]) + 1 if COLN["queue"] in header else None
    last_row = get_last_data_row(sh)
    queue_count = 0
    if queue_col_idx:
        if last_row > 1:
            rng = a1_range(2, queue_col_idx, last_row - 1, 1)
            try:
                queue_vals = sh.get(rng) or []
                for v in queue_vals:
                    val = (v[0] if v else "").strip().upper()
                    if val in ("TRUE", "1", "YES"):
                        queue_count += 1
            except Exception:
                queue_count = 0
    # Use count of ticked rows as batch size if any, else fallback to config/default
    if queue_count > 0:
        batch_size = queue_count
    else:
        batch_size = HARDCODE_BATCH_SIZE or args.batch_size or DEFAULT_BATCH_SIZE

    # OpenAI key resolution (priority: hardcoded > env > fallback)
    api_key = (HARDCODE_OPENAI_API_KEY or os.getenv("OPENAI_API_KEY", FALLBACK_OPENAI_API_KEY)).strip()
    if not api_key:
        raise RuntimeError("Missing OpenAI API key. Either set HARDCODE_OPENAI_API_KEY in this file, "
                           "or export OPENAI_API_KEY, or set FALLBACK_OPENAI_API_KEY.")
    client = OpenAI(api_key=api_key)

    print(f"[Summarizer] Sheet: {sheet_id} | Batch: {batch_size}")
    try:
        total = process_all(sh, client, batch_size)
        print(f"Processed {total} row(s).")
        try:
            send_slack_message(f"✅ Summarizer finished! Processed {total} row(s).")
        except Exception as e:
            print(f"[Slack] Notification failed: {e}")
    except KeyboardInterrupt:
        print("\n[Interrupted] Processing stopped by user (KeyboardInterrupt).")

if __name__ == "__main__":
    main()