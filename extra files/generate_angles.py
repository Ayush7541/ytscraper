# generate_angles.py
# Phase 3 – Ideas/Angles generator wired to your sheet + Custom-GPT style prompt.

import os, re, json, time, argparse, random
import requests

from datetime import datetime

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
from openai import OpenAI
from gspread.utils import rowcol_to_a1
from gspread_formatting import format_cell_range, CellFormat, Color

try:
    from langdetect import detect
except Exception:
    detect = None

# ========= YOUR DEFAULTS (edit once) =========
SHEET_NAME = "raw leads"
DEFAULT_SHEET_ID = "1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE"     # <-- paste your Sheet ID
DEFAULT_BATCH_SIZE = 15
FALLBACK_OPENAI_API_KEY = "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"  # <-- paste your actual API key here

# ========= Model & pacing =========
OPENAI_MODEL = "gpt-4o-mini"
OPENAI_TEMPERATURE = 0.5
OPENAI_TIMEOUT = 60
PAUSE_BETWEEN_ROWS_SEC = 0.25
PAUSE_BETWEEN_BATCHES_SEC = 2.0
MAX_TOKENS = 800
RETRY_MAX = 3

DRY_RUN = False                # if True, build prompts but don't call OpenAI or write results
MAX_BIO_CHARS = 1200
MAX_TITLES_CHARS = 1200
DIVERSITY_SIM_THRESHOLD = 0.7  # 0..1; higher = more similar (filter out >= threshold)

# ========= Column names (must match headers) =========
COLN = {
    "approved": "Approved",
    "queue_angles": "Queue Angles",
    "angles": "Angles/Ideas",
    "angles_approved": "Angles Approved",
    "deliverable": "Deliverable",

    "bio": "YouTube Bio",
    "titles": "Recent Titles (15 Non-Shorts)",
    "lp": "Landing Page Copy",

    "niche": "Niche",
    "selling": "Selling",
    "target": "Target Audience",
    "pitch": "One-Line Pitch",

    "status": "Status",
    "updated": "Updated At",

    "channel": "Channel Name",     # optional
    "url": "Channel URL",          # optional

    "status_code": "Status Code",     # optional hidden column, not required
    "seq_len": "Sequence Length",     # optional; to store parsed N
}

def get_last_data_row(sh) -> int:
    """Return last row index that has any data (1-based, including header)."""
    try:
        return len(sh.get_all_values())
    except Exception:
        return 1

def get_last_header_col(sh) -> int:
    """Return number of header columns based on row 1."""
    return len(sh.row_values(1))

def a1_range(r: int, c_start: int, r_count: int, c_count: int) -> str:
    """Build an A1 range string for a rectangle."""
    start = rowcol_to_a1(r, c_start)
    end = rowcol_to_a1(r + r_count - 1, c_start + c_count - 1)
    return f"{start}:{end}"

EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map
    "\U0001F1E0-\U0001F1FF"  # flags
    "]+",
    flags=re.UNICODE,
)

def strip_emojis(s: str) -> str:
    return EMOJI_RE.sub("", s or "")

def normalize_title(t: str) -> str:
    t = strip_emojis(t or "")
    t = t.replace("|", " ").strip()
    t = re.sub(r"\s+", " ", t)
    # drop bracketed tags and URLs for cleaner prompts
    t = re.sub(r"\[[^\]]+\]", "", t)
    t = re.sub(r"\([^\)]+\)", "", t)
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"\s+", " ", t).strip()
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

def jaccard(a: str, b: str) -> float:
    sa = set(re.findall(r"[a-z0-9]+", (a or "").lower()))
    sb = set(re.findall(r"[a-z0-9]+", (b or "").lower()))
    if not sa or not sb:
        return 0.0
    inter = len(sa & sb)
    union = len(sa | sb)
    return inter / union if union else 0.0

def looks_non_english(bio: str, titles: str) -> bool:
    text = ((bio or "") + " " + (titles or "")).strip()
    if len(text) < 60:
        return False
    ascii_ratio = sum(1 for ch in text if ord(ch) < 128) / len(text)
    if ascii_ratio < 0.7:
        return True
    if detect:
        try:
            lang = detect(text[:4000])
            return bool(lang) and lang.lower() != "en"
        except Exception:
            return False
    return False

# Helper to parse JSON and strip code fences if present
def parse_json_maybe(raw: str):
    s = (raw or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\n|\n```$", "", s, flags=re.S)
    return json.loads(s)

def infer_mini_bio_from_titles(titles_pipe: str) -> str:
    titles = [t.strip() for t in (titles_pipe or "").split("|") if t.strip()]
    if not titles:
        return ""
    # naive summary: top keywords
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

# ========= “Custom GPT” system prompt (mirrors your GPT instructions) =========
SYSTEM_PROMPT = """
MASTER PROMPT (for Angles Only)
Input You’ll Receive
For each request, you’ll be given:
• niche — What the creator’s channel is about.
• selling — What they’re selling (course, membership, Skool, appointment, lead magnet, product, Patreon, community, consulting, coaching, unknown).
• target_audience — Who this is for.
• one_line_pitch — A short persuasive pitch.
• deliverable — The type of copy to produce (see list below).
• Additional context — channel info, recent titles, bio, landing page copy.

Task: Generate ONLY the copy angles (not full copy). Each angle must be distinct (pain-driven, desire-driven, contrarian, curiosity, story-based, etc.).
Rules:
* Hook hard in the first line.
* Tease transformation, payoff, or revelation.
* No fluff. No corporate jargon.
* Keep each angle short, punchy, and in Shiv Shetti-style rhythm.
* Number the angles clearly.
Example Angles
1. Pain-driven:“You wake up tired. Avoid mirrors. Pretend the extra weight ‘isn’t that bad’... until the day your kid asks why you never play with them anymore. That’s the wake-up call. This program is how you fix it.”
2. Desire-driven:“Imagine walking into a room and feeling every eye notice you—not because of your weight, but because you carry yourself with that quiet confidence again. That’s the body you’ll rebuild in 12 weeks.”
3. Contrarian/Curiosity:“The biggest lie in fitness? That you need willpower. No—you need systems. My clients lose 20+ lbs not because they ‘try harder’… but because I strip away the hidden habits that keep them stuck.”

STRICT OUTPUT FORMAT FOR ANGLES
Return STRICT JSON only, no commentary.

JSON format:
{
  "deliverable": "1 Email | 1 YouTube Community Post | 3 Email Sequence/YT Community Post | 5 Email Sequence/YT Community Post | 7 Email Sequence/YT Community Post | Landing Page",
  "sequence_length": null or integer,
  "angles": [
    {"title": "short hook", "idea": "1–2 sentence angle with specific outcome/tension"}
  ]
}

Rules:
- If the deliverable is a sequence, return exactly N angles (N = 3, 5, or 7).
- Otherwise return exactly 3 angles.
- Each angle must be distinct (use diverse mechanisms: pain, desire, contrarian, curiosity, proof, behind-the-scenes).
- Prefer BIO over Titles if they conflict.
"""

# ========= Helpers =========
def open_sheet(spreadsheet_id: str):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("google_sheets_key.json", scope)
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(spreadsheet_id)
    return ss.worksheet(SHEET_NAME)

def cut(s: str, n: int) -> str:
    s = (s or "").strip()
    return s[:n]

# Helper to clean landing page copy
def clean_lp(lp: str) -> str:
    if not lp:
        return ""
    # remove common junk: webp/image refs, checkmarks, form fields
    lp = re.sub(r"\.webp", "", lp, flags=re.I)
    lp = re.sub(r"✓", "", lp)
    lp = re.sub(r"(?i)First name|Last name|Phone|Email|Address|Submit", "", lp)
    return lp.strip()

def build_colmap(header: list[str]) -> dict:
    return {name: (header.index(name) + 1) if name in header else None for name in COLN.values()}

def get(row, colmap, header_name):
    c = colmap.get(header_name)
    return (row[c-1] if c and (c-1) < len(row) else "")

def need_angles(row, colmap) -> bool:
    approved = str(get(row, colmap, COLN["approved"])).strip().upper() in ("TRUE","1","YES")
    queued   = str(get(row, colmap, COLN["queue_angles"])).strip().upper() in ("TRUE","1","YES")
    deliver  = str(get(row, colmap, COLN["deliverable"])).strip()
    current  = str(get(row, colmap, COLN["angles"])).strip()
    if not (approved and queued and deliver):
        return False
    # We allow overwrite; if you want to only fill empties, uncomment next line:
    # if current: return False
    return True

def parse_deliverable(raw: str):
    """
    Your allowed inputs:
      '1 Email'
      '1 YouTube Community Post'
      '3 Email Sequence/YT Community Post'
      '5 Email Sequence/YT Community Post'
      '7 Email Sequence/YT Community Post'
      'Landing Page'
    Returns (label, seq_len) where seq_len is None or int.
    """
    if not raw:
        return None, None
    s = raw.strip()

    if s == "1 Email":
        return s, None
    if s == "1 YouTube Community Post":
        return s, None
    if s == "Landing Page":
        return s, None

    m = re.match(r"^\s*([357])\s*Email Sequence/YT Community Post\s*$", s)
    if m:
        n = int(m.group(1))
        return s, n

    return None, None

def make_user_prompt(deliverable_label: str, seq_len, bio, titles, lp, niche, selling, target, pitch, channel, url):
    seq_line = f"sequence_length={seq_len}" if seq_len else "sequence_length=null"
    return f"""
DELIVERABLE: {deliverable_label} ({seq_line})

SOURCE-OF-TRUTH PRIORITY:
1) BIO (prefer over titles if conflicting)
2) Niche/Selling/Target/Pitch (anchor positioning)
3) Titles (specificity & recency)
4) Landing Page Copy (tone/offer)

Channel: {cut(channel,120)} | {cut(url,200)}
Niche: {cut(niche,200)}
Selling: {cut(selling,200)}
Target Audience: {cut(target,200)}
One-Line Pitch: {cut(pitch,300)}

BIO:
{cut(bio,MAX_BIO_CHARS)}

RECENT TITLES (pipe-separated):
{cut(titles,MAX_TITLES_CHARS)}

LANDING PAGE COPY (optional):
{cut(clean_lp(lp),600)}

Return STRICT JSON per the schema described in the system message.
"""

def call_openai(client: OpenAI, messages):
    delay = 0.8
    last_err = None
    for attempt in range(1, RETRY_MAX + 1):
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                response_format={"type": "json_object"},
                messages=messages,
                max_completion_tokens=MAX_TOKENS,
                timeout=OPENAI_TIMEOUT,
            )
            return resp.choices[0].message.content
        except Exception as e:
            last_err = e
            time.sleep(delay + random.uniform(0, 0.3))
            delay *= 1.6
    raise RuntimeError(f"OpenAI call failed after {RETRY_MAX} attempts: {repr(last_err)}")

def batch_update(sh, updates):
    """
    updates: list of dicts with keys:
      - row: int (1-based)
      - col: header string (must exist in row 1)
      - val: value (will JSON-dump lists/dicts)
    Groups updates per row; reads a minimal span; writes back once per row.
    """
    if not updates:
        return
    header = sh.row_values(1)
    for u in updates:
        if u["col"] not in header:
            continue
        col_index = header.index(u["col"]) + 1
        v = u["val"]
        if isinstance(v, (list, dict)):
            v = json.dumps(v, ensure_ascii=False)
        sh.update_cell(u["row"], col_index, v)

# ========= Core =========
def process_once(sh, colmap, client: OpenAI, limit: int) -> int:
    last_row = get_last_data_row(sh)
    if last_row < 2:
        return 0
    header = sh.row_values(1)
    last_col = len(header)
    rng = a1_range(2, 1, last_row - 1, last_col)
    data = sh.get(rng) if last_row > 1 else []

    processed = 0
    for r_idx, row in enumerate(data, start=2):
        if processed >= limit: break
        if not need_angles(row, colmap): continue

        deliverable_raw = str(get(row, colmap, COLN["deliverable"])).strip()
        label, seq_len = parse_deliverable(deliverable_raw)
        if not label:  # can’t understand deliverable
            continue

        raw_bio = str(get(row, colmap, COLN["bio"]))
        raw_titles = str(get(row, colmap, COLN["titles"]))

        bio = strip_emojis(raw_bio)[:MAX_BIO_CHARS]
        titles = dedupe_titles_pipe(raw_titles, max_chars=MAX_TITLES_CHARS)
        if not bio and titles:
            bio = infer_mini_bio_from_titles(titles)

        # Non-English safeguard (skip before calling OpenAI)
        if looks_non_english(bio, titles):
            ups = []
            if COLN.get("status_code") in header:
                ups.append({"row": r_idx, "col": COLN["status_code"], "val": "NON_EN"})
            if COLN["status"] in header:
                ups.append({"row": r_idx, "col": COLN["status"], "val": "Skipped: Non-English (leave Queue checked to retry)"})
            if COLN["updated"] in header:
                ups.append({"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")})
            # keep checkbox ON so you can re-queue manually if needed
            batch_update(sh, ups)
            continue

        lp = str(get(row, colmap, COLN["lp"]))
        niche = str(get(row, colmap, COLN["niche"]))
        selling = str(get(row, colmap, COLN["selling"]))
        target = str(get(row, colmap, COLN["target"]))
        pitch = str(get(row, colmap, COLN["pitch"]))
        channel = str(get(row, colmap, COLN["channel"]))
        url = str(get(row, colmap, COLN["url"]))

        orig_angles = str(get(row, colmap, COLN["angles"]))
        orig_queue = str(get(row, colmap, COLN["queue_angles"])).strip().upper() in ("TRUE","1","YES")

        # mark processing
        ups = []
        if COLN["status"] in header:
            ups.append({"row": r_idx, "col": COLN["status"], "val": "Processing: Angles"})
        if COLN["updated"] in header:
            ups.append({"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")})
        batch_update(sh, ups)

        try:
            if DRY_RUN:
                parsed = {"angles":[{"title":"(dry-run) Hook 1","idea":"Angle idea sample"},
                                    {"title":"(dry-run) Hook 2","idea":"Another angle"},
                                    {"title":"(dry-run) Hook 3","idea":"Third angle"}]}
            else:
                msgs = [
                    {"role":"system","content":SYSTEM_PROMPT.strip()},
                    {"role":"user","content": make_user_prompt(label, seq_len, bio, titles, lp, niche, selling, target, pitch, channel, url)}
                ]
                raw = call_openai(client, msgs)
                # Check for empty output and retry once if so
                if not raw or not raw.strip():
                    # Retry once if empty
                    strict_user = make_user_prompt(label, seq_len, bio, titles, lp, niche, selling, target, pitch, channel, url) + "\n\nReturn STRICT JSON only. No commentary."
                    raw = call_openai(client, [
                        {"role": "system", "content": SYSTEM_PROMPT.strip()},
                        {"role": "user", "content": strict_user}
                    ])
                try:
                    parsed = parse_json_maybe(raw)
                except Exception:
                    # Retry once with stricter instruction
                    strict_user = make_user_prompt(label, seq_len, bio, titles, lp, niche, selling, target, pitch, channel, url) + "\n\nReturn EXACTLY valid JSON. No commentary."
                    raw = call_openai(client, [
                        {"role":"system","content":SYSTEM_PROMPT.strip()},
                        {"role":"user","content": strict_user}
                    ])
                    # Check for empty output before parsing
                    if not raw or not raw.strip():
                        raise RuntimeError("OpenAI returned empty content after retry")
                    parsed = parse_json_maybe(raw)

            angles = parsed.get("angles", []) or []
            # enforce required count
            required_n = seq_len if seq_len else 3
            # diversity filter
            kept = []
            for a in angles:
                title = (a.get("title","") or "").strip()
                idea  = (a.get("idea","") or "").strip()
                if not title or not idea:
                    continue
                if any(jaccard(f"{k['title']} {k['idea']}", f"{title} {idea}") >= DIVERSITY_SIM_THRESHOLD for k in kept):
                    continue
                kept.append({"title": title, "idea": idea})
                if len(kept) >= required_n:
                    break

            # If not enough distinct angles, try one more regeneration for the missing count
            if len(kept) < required_n and not DRY_RUN:
                missing = required_n - len(kept)
                exclude_list = [f"{k['title']} — {k['idea']}" for k in kept]
                regen_user = make_user_prompt(label, seq_len, bio, titles, lp, niche, selling, target, pitch, channel, url) + \
                             f"\n\nProvide {missing} NEW distinct angles that are not semantically similar to these:\n- " + "\n- ".join(exclude_list) + \
                             "\nReturn STRICT JSON with only the missing angles."
                raw2 = call_openai(client, [
                    {"role":"system","content":SYSTEM_PROMPT.strip()},
                    {"role":"user","content": regen_user}
                ])
                try:
                    parsed2 = parse_json_maybe(raw2)
                    for a in (parsed2.get("angles", []) or []):
                        title = (a.get("title","") or "").strip()
                        idea  = (a.get("idea","") or "").strip()
                        if not title or not idea:
                            continue
                        if any(jaccard(f"{k['title']} {k['idea']}", f"{title} {idea}") >= DIVERSITY_SIM_THRESHOLD for k in kept):
                            continue
                        kept.append({"title": title, "idea": idea})
                        if len(kept) >= required_n:
                            break
                except Exception:
                    pass

            # Build final cell text
            bullets = []
            for i, a in enumerate(kept, 1):
                bullets.append(f"{i}. {a['title']} — {a['idea']}")
            angles_text = "\n".join(bullets)

            # Concurrency guard: re-fetch queue + angles before write
            header_now = sh.row_values(1)
            idx_angles = header_now.index(COLN["angles"]) + 1 if COLN["angles"] in header_now else None
            idx_queue  = header_now.index(COLN["queue_angles"]) + 1 if COLN["queue_angles"] in header_now else None
            safe_to_write = True
            if idx_angles:
                cur_angles_rng = a1_range(r_idx, idx_angles, 1, 1)
                cur_angles_data = sh.get(cur_angles_rng) or []
                cur_angles_val = cur_angles_data[0][0] if (cur_angles_data and cur_angles_data[0]) else ""
                if str(cur_angles_val).strip() != str(orig_angles).strip():
                    safe_to_write = False
            if idx_queue:
                cur_queue_rng = a1_range(r_idx, idx_queue, 1, 1)
                cur_queue_data = sh.get(cur_queue_rng) or []
                cur_queue_val = cur_queue_data[0][0] if (cur_queue_data and cur_queue_data[0]) else ""
                cur_queue_bool = str(cur_queue_val).strip().upper() in ("TRUE","1","YES")
                if not cur_queue_bool:
                    safe_to_write = False

            ups = []
            if not angles_text:
                # no usable output
                if COLN.get("status_code") in header:
                    ups.append({"row": r_idx, "col": COLN["status_code"], "val": "ERR_EMPTY"})
                ups.append({"row": r_idx, "col": COLN["status"], "val": "Error: No distinct angles"})
                ups.append({"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")})
                # keep queue ON for easy re-run
                batch_update(sh, ups)
                continue

            if safe_to_write:
                ups.append({"row": r_idx, "col": COLN["angles"], "val": angles_text})
                if COLN.get("status_code") in header:
                    ups.append({"row": r_idx, "col": COLN["status_code"], "val": "OK"})
                if COLN.get("seq_len") in header:
                    ups.append({"row": r_idx, "col": COLN["seq_len"], "val": (seq_len if seq_len else "")})
                ups.append({"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")})
                ups.append({"row": r_idx, "col": COLN["status"], "val": "Angles generated"})
                # Untick queue_angles after batch_update, and apply formatting below
                batch_update(sh, ups)
                # Untick queue_angles with formatting using gspread_formatting
                header_now = sh.row_values(1)
                if COLN["queue_angles"] in header_now:
                    q_col_idx = header_now.index(COLN["queue_angles"]) + 1
                    q_cell_a1 = a1_range(r_idx, q_col_idx, 1, 1)
                    try:
                        sh.update_acell(q_cell_a1, False)
                    except Exception:
                        pass
                    fmt = CellFormat(backgroundColor=Color(0.8, 0.94, 0.8))  # light green RGB
                    format_cell_range(sh, q_cell_a1, fmt)
            else:
                # Skipped due to concurrent change
                if COLN.get("status_code") in header:
                    ups.append({"row": r_idx, "col": COLN["status_code"], "val": "SKIPPED"})
                ups.append({"row": r_idx, "col": COLN["status"], "val": "Skipped: Row changed"})
                ups.append({"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")})
                batch_update(sh, ups)
                continue

        except Exception as e:
            print("[Angles][Exception]", repr(e))
            ups = []
            if COLN.get("status_code") in header:
                ups.append({"row": r_idx, "col": COLN["status_code"], "val": "ERR_EXC"})
            ups.append({"row": r_idx, "col": COLN["status"], "val": f"Error: {str(e)[:160]}"})
            ups.append({"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")})
            # keep checkbox ON for re-run
            batch_update(sh, ups)
            processed += 1
            time.sleep(PAUSE_BETWEEN_ROWS_SEC)
            continue

        processed += 1
        time.sleep(PAUSE_BETWEEN_ROWS_SEC)

    return processed

def process_all(sh, client: OpenAI, batch_size: int) -> int:
    header = sh.row_values(1)
    # build colmap for our defined headers
    colmap = {v: (header.index(v)+1) if v in header else None for v in COLN.values()}

    # required headers sanity
    required = [
        COLN["approved"], COLN["queue_angles"], COLN["deliverable"],
        COLN["bio"], COLN["titles"],
        COLN["niche"], COLN["selling"], COLN["target"], COLN["pitch"],
        COLN["angles"], COLN["status"], COLN["updated"]
    ]
    missing = [h for h in required if h not in header]
    if missing:
        raise RuntimeError(f"Missing required headers: {missing}")

    total = 0
    # Remove batching logic; process all rows with a single large limit
    done = process_once(sh, colmap, client, 999999)
    total += done
    print(f"[Angles] Batch done | processed={total}")
    return total

# ========= Entry =========
def main():
    # Use hardcoded defaults, ignore CLI args
    api_key = os.getenv("OPENAI_API_KEY", FALLBACK_OPENAI_API_KEY).strip()
    if not api_key:
        raise RuntimeError("Missing OpenAI API key. Set env or hardcode FALLBACK_OPENAI_API_KEY.")
    client = OpenAI(api_key=api_key)

    print(f"[Angles] Sheet: {DEFAULT_SHEET_ID} | Batch: ALL | Model: {OPENAI_MODEL}")

    sh = open_sheet(DEFAULT_SHEET_ID)
    total = process_all(sh, client, DEFAULT_BATCH_SIZE)
    print(f"Angles generated for {total} row(s).")
    try:
        send_slack_message(f"✅ Angles generation finished! Processed {total} row(s).")
    except Exception as e:
        print(f"[Slack] Notification failed: {e}")

if __name__ == "__main__":
    main()