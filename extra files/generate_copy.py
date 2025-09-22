"""
Updated generate_copy.py

Phase 4 – Copywriting + Google Doc export (OAuth).

Selects rows in "raw leads" where:
  - Approved == TRUE
  - Angles Approved == TRUE
  - Queue Copy == TRUE
  - Deliverable is set

Differences vs previous version:
  - Does NOT require "Copy (final)" to be empty (column can be removed).
  - Uses OAuth (installed app) instead of Service Account for Sheets/Drive/Docs.
  - After generating copy, creates (or reuses) a Google Doc in Drive, optionally inside a date subfolder (YYYY-MM-DD) under TARGET_FOLDER_ID.
  - Title format: "{Channel Name} — {Deliverable}" (illegal filename chars removed).
  - Inserts title as Heading 1, a horizontal rule, then the copy text.
  - Saves content hash in appProperties to detect duplicates; if same name+hash exists, reuses it.
  - Updates sheet with Doc URL, Status -> "Done: Export", clears "Copy Approved", colors that cell light green, sets Updated At.
  - Retries Drive/Docs API calls on 429/500/503 with exponential backoff.

Run:
  python generate_copy.py --sheet-id <SHEET_ID> [--batch-size N]

OAuth setup (one-time):
  - Place your OAuth client json as credentials.json next to this script (or set GOOGLE_OAUTH_CLIENT_JSON path).
  - The first run will prompt a browser flow and create token.json for reuse.
"""

import os
import re
import json
import time
import argparse
import random
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import requests

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
from gspread.utils import rowcol_to_a1

# Google OAuth & APIs
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from openai import OpenAI

try:
    from langdetect import detect
except Exception:
    detect = None

# ---------------------- EDIT THESE DEFAULTS ----------------------
SHEET_NAME = "raw leads"
DEFAULT_SHEET_ID = "1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE"  # optional default

# Folder where Docs will be created
TARGET_FOLDER_ID = os.getenv("TARGET_FOLDER_ID", "1CL83-QoTutLVJXWpRMyOCWDHSpA62wP_")  # REQUIRED: set via env or hardcode
GROUP_BY_DATE = True  # If True, put docs into YYYY-MM-DD subfolders under TARGET_FOLDER_ID

# OpenAI config (hardcoded model)
OPENAI_MODEL = "gpt-4o-mini"
OPENAI_TEMPERATURE = 1
OPENAI_TIMEOUT = 90
MAX_TOKENS = 10000  # raised default to 10k for long sequences/landing pages
RETRY_MAX = 3
PAUSE_BETWEEN_ROWS_SEC = 0.25
PAUSE_BETWEEN_BATCHES_SEC = 2.0
PAUSE_RETRY_SEC = 1.0

# OAuth files
CREDENTIALS_FILE = os.getenv("GOOGLE_OAUTH_CLIENT_JSON", "credentials.json")
TOKEN_FILE = os.getenv("GOOGLE_OAUTH_TOKEN_JSON", "token.json")

# Google API scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents",
]

# ---------------------- System Prompt ----------------------
SYSTEM_PROMPT = """
Follow everything below as an order, do not take them lightly.

Role & Style
- You are an elite copywriter trained in Shiv Shetti’s style — clear, raw, bold, persuasive.
- Your copy is direct, conversational, punchy.
- You avoid fluff. You build intrigue and tension, then release it with a strong call-to-action.

Tone Priorities
- Emotional depth: Tap into pain, desire, urgency.
- Conversational rhythm: Short sentences. One-liners. Ellipses for suspense…
- Relatable hooks: Start with a scenario, feeling, or hard truth.
- No corporate jargon. No “marketing speak.”

Input You’ll Receive
- niche — What the creator’s channel is about.
- selling — What they’re selling (course, membership, Skool, appointment, lead magnet, product, Patreon, community, consulting, coaching, unknown).
- target_audience — Who this is for.
- one_line_pitch — A short persuasive pitch.
- deliverable — The type of copy to produce (Email, YouTube Community Post, Sequences, Landing Page).
- Additional context — channel info, recent titles, bio, landing page copy.

Deliverables
1. 1 Email
2. 1 YouTube Community Post
3. 3 Email Sequence / YT Community Post
4. 5 Email Sequence / YT Community Post
5. 7 Email Sequence / YT Community Post
6. Landing Page

CTA Aim
Every deliverable must drive ONE clear next step:
- Visit Skool
- Buy course/product
- Book appointment
- Join membership
- Support on Patreon
- Claim bonus/offer
CTA must be specific, single-action, and flow naturally.


Copywriting (Phase 3)
When asked for full copy:
- Produce the exact number of pieces matching the deliverable.
- Style: emotional, concise, high-impact (Shiv Shetti).
- Include CTA naturally at the end (only one).
- End with a short P.S. line to nudge again.
- Never mention “subscribe,” “follow,” or “like” unless explicitly relevant.

Formatting Rules
- Sentences must average 8–12 words max.
- Every 1–2 sentences = new line (short paragraphs only).
- Use rhythm and whitespace to create flow and emphasis.
- Avoid dense blocks of text. Copy must “breathe.”
- Clean text only.
- Short paragraphs for readability.
- No markdown unless asked.
- No placeholders.


Creative Priorities
- Clarity over cleverness.
- Emotion before logic.
- Specificity over vagueness.
OUTPUT FORMAT REQUIREMENTS:
- Always return STRICT JSON only. No commentary, no plain text.

- For single_email or single_post deliverables:
  {
    "copy": {
      "subject": "...",
      "body": "140–200 words. Write in short, punchy paragraphs (1–2 sentences per paragraph). Avoid heavy text blocks.",
      "cta": "...",
      "ps": "..."
    }
  }

- For sequence deliverables (3/5/7):
  {
    "parts": [
      {
        "part": 1,
        "subject": "...",
        "body":"140–200 words. Write in short, punchy paragraphs (1–2 sentences per paragraph). Avoid heavy text blocks.",
        "cta": "...",
        "ps": "..."
      },
      {
        "part": 2,
        "subject": "...",
        "body": "...",
        "cta": "...",
        "ps": "..."
      }
      ...
    ]
  }

- For landing_page deliverables:
  {
    "copy": {
      "headline": "...",
      "subheadline": "...",
      "sections": [
        {"title": "...", "body": "..."},
        {"title": "...", "body": "..."}
      ],
      "cta": "...",
      "ps": "..."
    }
  }

- Absolutely no text outside JSON. Never return Markdown code fences.
- Each sequence part MUST be 140–200 words, singles also 140–200 words, landing page ≥ 800 words.

STRICT FORMATTING RULES:
- For sequences: return exactly N parts (N = deliverable length).
- Each sequence part MUST be between 140 and 200 words. No part may be shorter than 140 or longer than 200.
- For single emails/posts: MUST be between 140 and 200 words (including subject line). Never under 140 words.
- For landing pages: MUST be at least 800 words, split across multiple sections.
- If word count is outside the required range, regenerate until within the range.
"""

# ---------------------- Helpers ----------------------

def sanitize_title(s: str) -> str:
    s = (s or "").strip()
    # remove illegal Drive/Docs filename characters
    s = re.sub(r"[\\/:*?\"<>|]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or "Untitled"


def parse_json_maybe(raw: str) -> Any:
    s = (raw or "").strip()
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\n|\n```$", "", s, flags=re.S)
    try:
        return json.loads(s)
    except Exception:
        s2 = re.sub(r",\s*\}", "}", s)
        s2 = re.sub(r",\s*\]", "]", s2)
        return json.loads(s2)


def truthy(v: Any) -> bool:
    return str(v).strip().upper() in ("TRUE", "1", "YES")


# ---------------------- OAuth & Clients ----------------------

def get_google_creds() -> Credentials:
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())  # type: ignore
            except Exception:
                creds = None
        if not creds:
            if not os.path.exists(CREDENTIALS_FILE):
                raise RuntimeError(
                    f"Missing OAuth client file: {CREDENTIALS_FILE}. Download it from Google Cloud Console and place it next to this script.")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
    return creds


def open_sheet(spreadsheet_id: str):
    creds = get_google_creds()
    gc = gspread.authorize(creds)
    ss = gc.open_by_key(spreadsheet_id)
    return ss.worksheet(SHEET_NAME)


def build_services(creds: Credentials):
    drive = build("drive", "v3", credentials=creds)
    docs = build("docs", "v1", credentials=creds)
    return drive, docs


# ---------------------- Drive/Docs helpers ----------------------

def drive_retry(fn, *args, **kwargs):
    delay = 0.8
    last = None
    for _ in range(6):
        try:
            return fn(*args, **kwargs)
        except HttpError as e:
            last = e
            status = getattr(e, "status_code", None) or getattr(e, "resp", {}).get("status")
            try:
                status = int(status)
            except Exception:
                status = None
            if status in (429, 500, 503):
                time.sleep(delay + random.uniform(0, 0.3))
                delay = min(delay * 1.6, 8)
                continue
            raise
        except Exception as e:
            last = e
            time.sleep(delay)
            delay = min(delay * 1.6, 8)
    raise RuntimeError(f"Drive/Docs operation failed after retries: {last}")


def ensure_date_subfolder(drive, parent_id: str, datestr: str) -> str:
    q = f"name = '{datestr}' and mimeType = 'application/vnd.google-apps.folder' and '{parent_id}' in parents and trashed = false"
    res = drive_retry(drive.files().list, q=q, fields="files(id,name)")
    files = res.execute().get("files", [])
    if files:
        return files[0]["id"]
    body = {
        "name": datestr,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    created = drive_retry(drive.files().create, body=body, fields="id")
    return created.execute()["id"]


def find_existing_doc(drive, parent_id: str, title: str, content_hash: str) -> Optional[str]:
    # Search by name first under parent, then check appProperties contentHash
    q = (
        f"name = '{title.replace("'", "\\'")}' and '{parent_id}' in parents and mimeType = 'application/vnd.google-apps.document' and trashed = false"
    )
    res = drive_retry(drive.files().list, q=q, fields="files(id,name,appProperties)")
    for f in res.execute().get("files", []):
        props = f.get("appProperties", {}) or {}
        if props.get("contentHash") == content_hash:
            return f["id"]
    return None



# Helper to make a Google Doc public (viewable by anyone with the link)
def make_doc_public(drive, file_id):
    try:
        drive_retry(
            drive.permissions().create,
            fileId=file_id,
            body={"role": "reader", "type": "anyone"},
            fields="id"
        ).execute()
    except Exception as e:
        print(f"[Drive] Failed to set public permission on Doc {file_id}: {e}")

def create_or_update_doc(drive, docs, parent_id: str, title: str, body_text: str, content_hash: str) -> str:
    # Reuse existing by name+hash; otherwise create new doc and set appProperties
    existing = find_existing_doc(drive, parent_id, title, content_hash)
    if existing:
        return existing

    file_meta = {
        "name": title,
        "mimeType": "application/vnd.google-apps.document",
        "parents": [parent_id],
        "appProperties": {"contentHash": content_hash},
    }
    created = drive_retry(drive.files().create, body=file_meta, fields="id")
    file_id = created.execute()["id"]

    # Make the document public
    make_doc_public(drive, file_id)

    # Build Docs content: Title (H1), then body (no horizontal rule)
    requests = [
        {"insertText": {"location": {"index": 1}, "text": title + "\n\n"}},
        {"updateParagraphStyle": {
            "range": {"startIndex": 1, "endIndex": 1 + len(title) + 2},
            "paragraphStyle": {"namedStyleType": "HEADING_1"},
            "fields": "namedStyleType"
        }},
        {"insertText": {"location": {"index": 1 + len(title) + 2}, "text": "\n"}},
        {"insertText": {"location": {"index": 1 + len(title) + 3}, "text": body_text}}
    ]

    drive_retry(docs.documents().batchUpdate, documentId=file_id, body={"requests": requests}).execute()

    # Double-set appProperties (some domains strip on create)
    try:
        drive_retry(drive.files().update, fileId=file_id, body={"appProperties": {"contentHash": content_hash}}).execute()
    except Exception:
        pass

    return file_id


# ---------------------- Sheet helpers ----------------------
COLN: Dict[str, str] = {
    "approved": "Approved",
    "angles_approved": "Angles Approved",
    "queue_copy": "Queue Copy",
    "deliverable": "Deliverable",

    "bio": "YouTube Bio",
    "titles": "Recent Titles (15 Non-Shorts)",
    "lp": "Landing Page Copy",

    "niche": "Niche",
    "selling": "Selling",
    "target": "Target Audience",
    "pitch": "One-Line Pitch",

    "angles": "Angles/Ideas",
    "status": "Status",
    "updated": "Updated At",

    "channel": "Channel Name",
    "url": "Channel URL",

    "copy_approved": "Copy Approved",  # to be cleared after export
    "doc_url": "Doc URL",              # to be written
}


def a1_range(r: int, c_start: int, r_count: int, c_count: int) -> str:
    start = rowcol_to_a1(r, c_start)
    end = rowcol_to_a1(r + r_count - 1, c_start + c_count - 1)
    return f"{start}:{end}"


def get_last_data_row(sh) -> int:
    try:
        return len(sh.get_all_values())
    except Exception:
        return 1


def get(row: List[Any], colmap: Dict[str, int], header: str):
    c = colmap.get(header)
    return (row[c-1] if c and (c-1) < len(row) else "")


def need_copy(row: List[Any], colmap: Dict[str, int]) -> bool:
    approved = truthy(get(row, colmap, COLN["approved"]))
    angles_ok = truthy(get(row, colmap, COLN["angles_approved"]))
    queued = truthy(get(row, colmap, COLN["queue_copy"]))
    deliver = str(get(row, colmap, COLN["deliverable"])).strip()
    status = str(get(row, colmap, COLN["status"])).strip()
    doc_url = str(get(row, colmap, COLN["doc_url"])).strip()

    if status.lower().startswith("done") or doc_url:
        return False
    return approved and angles_ok and queued and bool(deliver)


# ---------------------- OpenAI ----------------------

def build_messages(deliverable_label, seq_len, bio, titles, lp, niche, selling, target, pitch, approved_angles_text):
    angles = []
    for line in (approved_angles_text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"^\d+\.\s*", "", line)
        parts = re.split(r"\s*[—–-]\s*", line, maxsplit=1)
        if len(parts) == 2:
            angles.append({"title": parts[0].strip(), "idea": parts[1].strip()})
        else:
            angles.append({"title": line, "idea": ""})

    user = {
        "task": "write_final_copy",
        "deliverable": deliverable_label,
        "sequence_length": seq_len,
        "niche": (niche or "").strip()[:200],
        "selling": (selling or "").strip()[:200],
        "target_audience": (target or "").strip()[:200],
        "one_line_pitch": (pitch or "").strip()[:300],
        "bio": (bio or "").strip()[:1400],
        "recent_titles": (titles or "").strip()[:1400],
        "landing_page_copy": (lp or "").strip()[:1000],
        "approved_angles": angles,
    }
    return [
        {"role": "system", "content": SYSTEM_PROMPT.strip()},
        {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
    ]


def call_openai(client: OpenAI, messages):
    delay = 0.8
    last_err = None
    opt_client = client.with_options(timeout=OPENAI_TIMEOUT)
    for attempt in range(RETRY_MAX):
        try:
            kwargs = {
                "model": OPENAI_MODEL,
                "messages": messages,
                "max_completion_tokens": MAX_TOKENS,
                "temperature": OPENAI_TEMPERATURE,
            }
            resp = opt_client.chat.completions.create(**kwargs)
            content = resp.choices[0].message.content or ""
            if not content.strip():
                raise RuntimeError("OpenAI returned empty response")
            try:
                return parse_json_maybe(content)
            except Exception:
                # fallback: retry once with stricter JSON instructions
                if attempt == RETRY_MAX - 1:
                    strict_user = json.loads(messages[1]["content"])
                    strict_user["strict"] = True
                    strict_user["instruction"] = "Return STRICT JSON only. No commentary."
                    resp2 = opt_client.chat.completions.create(
                        model=OPENAI_MODEL,
                        messages=[
                            {"role": "system", "content": messages[0]["content"]},
                            {"role": "user", "content": json.dumps(strict_user, ensure_ascii=False)}
                        ],
                        max_completion_tokens=MAX_TOKENS,
                        temperature=OPENAI_TEMPERATURE,
                    )
                    content2 = resp2.choices[0].message.content or ""
                    return parse_json_maybe(content2)
                else:
                    raise
        except Exception as e:
            last_err = e
            time.sleep(delay + random.uniform(0, 0.3))
            delay = min(delay * 1.6, 6)
    raise RuntimeError(f"OpenAI call failed after retries: {repr(last_err)}")


# ---------------------- Core processing ----------------------

def parse_deliverable(raw: str) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    if not raw:
        return None, None, None
    s = raw.strip().lower()
    if s == "1 email":
        return "1 Email", "single_email", None
    if s == "1 youtube community post":
        return "1 YouTube Community Post", "single_post", None
    if s == "landing page":
        return "Landing Page", "landing_page", None
    m = re.match(r"^\s*(3|5|7)\s*email\s*sequence/yt\s*community\s*post\s*$", s)
    if m:
        n = int(m.group(1))
        return f"{n} Email Sequence/YT Community Post", "sequence", n
    return raw, None, None


def build_final_text(kind: str, parsed: Dict[str, Any], seq_len: Optional[int]) -> str:
    if kind in ("single_email", "single_post"):
        copy_obj = parsed.get("copy", {}) or {}
        subject = (copy_obj.get("subject", "") or '').strip()
        body = (copy_obj.get("body", "") or '').strip()
        cta = (copy_obj.get("cta", "") or '').strip()
        ps = (copy_obj.get("ps", "") or '').strip()
        if kind == "single_email" and subject:
            final_text = f"Subject: {subject}\n\n{body}\n\nCTA: {cta}\n\nP.S. {ps}"
        else:
            final_text = f"{body}\n\nCTA: {cta}\n\nP.S. {ps}"
        return re.sub(r"\n{3,}", "\n\n", final_text).strip()
    if kind == "landing_page":
        copy_obj = parsed.get("copy", {}) or {}
        headline = (copy_obj.get("headline", "") or '').strip()
        subheadline = (copy_obj.get("subheadline", "") or '').strip()
        sections = copy_obj.get("sections", []) or []
        cta = (copy_obj.get("cta", "") or '').strip()
        ps = (copy_obj.get("ps", "") or '').strip()
        parts = []
        if headline:
            parts.append(f"H1: {headline}")
        if subheadline:
            parts.append(f"H2: {subheadline}")
        for s in sections:
            t = (s.get("title", "") or '').strip()
            b = (s.get("body", "") or '').strip()
            if t or b:
                parts.append(f"{t}\n{b}".strip())
        parts.append(f"CTA: {cta}")
        parts.append(f"P.S. {ps}")
        final_text = "\n\n".join(parts)
        return re.sub(r"\n{3,}", "\n\n", final_text).strip()
    # sequence
    req_n = int(seq_len or parsed.get("sequence_length") or 3)
    parts = parsed.get("parts", []) or []
    if len(parts) < req_n:
        raise ValueError(f"Missing parts: expected {req_n}, got {len(parts)}")
    out = []
    for part in parts[:req_n]:
        pno = part.get("part", "?")
        ttl = (part.get("title", "") or '').strip()
        subj = (part.get("subject", "") or '').strip()
        body = (part.get("body", "") or '').strip()
        cta = (part.get("cta", "") or '').strip()
        ps = (part.get("ps", "") or '').strip()
        head = ""
        if subj:
            head = f"Subject: {subj}"
        bloc = f"{head}\n\n{body}\n\nCTA: {cta}\n\nP.S. {ps}".strip()
        out.append(bloc)
    final_text = "\n\n====================\n\n".join(out)
    return re.sub(r"\n{3,}", "\n\n", final_text).strip()


def batch_update(sh, updates: List[Dict[str, Any]]):
    if not updates:
        return
    header = sh.row_values(1)
    per_row: Dict[int, List[Dict[str, Any]]] = {}
    for u in updates:
        per_row.setdefault(u["row"], []).append(u)
    for r, ups in per_row.items():
        ups_with_colidx = []
        for u in ups:
            if u["col"] in header:
                col_idx = header.index(u["col"]) + 1
                ups_with_colidx.append((col_idx, u))
        if not ups_with_colidx:
            continue
        ups_with_colidx.sort()
        grouped: List[List[Tuple[int, Dict[str, Any]]]] = []
        cur_group: List[Tuple[int, Dict[str, Any]]] = []
        last_col = None
        for col_idx, u in ups_with_colidx:
            if not cur_group:
                cur_group = [(col_idx, u)]
                last_col = col_idx
            elif col_idx == last_col + 1:
                cur_group.append((col_idx, u))
                last_col = col_idx
            else:
                grouped.append(cur_group)
                cur_group = [(col_idx, u)]
                last_col = col_idx
        if cur_group:
            grouped.append(cur_group)
        for group in grouped:
            if len(group) == 1:
                col_idx, u = group[0]
                rng = a1_range(r, col_idx, 1, 1)
                val = u["val"]
                if isinstance(val, (list, dict)):
                    val = json.dumps(val, ensure_ascii=False)
                sh.update(range_name=rng, values=[[val]])
            else:
                cmin = group[0][0]
                cmax = group[-1][0]
                width = cmax - cmin + 1
                rng = a1_range(r, cmin, 1, width)
                try:
                    got = sh.get(rng)
                    row_vals = got[0] if got and len(got) > 0 else []
                except Exception:
                    row_vals = []
                if len(row_vals) < width:
                    row_vals = row_vals + [""] * (width - len(row_vals))
                else:
                    row_vals = row_vals[:width]
                for col_idx, u in group:
                    idx = col_idx - cmin
                    val = u["val"]
                    if isinstance(val, (list, dict)):
                        val = json.dumps(val, ensure_ascii=False)
                    row_vals[idx] = val
                sh.update(range_name=rng, values=[row_vals])


def process_once(sh, colmap, client: OpenAI, drive, docs, limit: int) -> int:
    last_row = get_last_data_row(sh)
    if last_row < 2:
        return 0
    header = sh.row_values(1)
    last_col = len(header)
    rng = a1_range(2, 1, last_row - 1, last_col)
    data = sh.get(rng) if last_row > 1 else []

    processed = 0
    for r_idx, row in enumerate(data, start=2):
        if limit and processed >= limit:
            break
        if not need_copy(row, colmap):
            continue

        deliverable_raw = str(get(row, colmap, COLN["deliverable"])) .strip()
        deliverable_label, kind, seq_len = parse_deliverable(deliverable_raw)
        if not kind:
            continue

        ch_name = (get(row, colmap, COLN.get("channel", "Channel Name")) or "—").strip()
        print(f"[Copy] Row {r_idx} | {deliverable_label} | Channel: {ch_name}")

        bio = str(get(row, colmap, COLN["bio"]))
        titles = str(get(row, colmap, COLN["titles"]))
        lp = str(get(row, colmap, COLN["lp"]))
        niche = str(get(row, colmap, COLN["niche"]))
        selling = str(get(row, colmap, COLN["selling"]))
        target = str(get(row, colmap, COLN["target"]))
        pitch = str(get(row, colmap, COLN["pitch"]))
        approved_angles = str(get(row, colmap, COLN["angles"]))

        blob = (bio or "") + " " + (titles or "")
        ascii_ratio = sum(1 for ch in blob if ord(ch) < 128) / max(1, len(blob))
        looks_non_en = ascii_ratio < 0.7
        if detect and not looks_non_en:
            try:
                lang = detect(blob[:4000])
                looks_non_en = (lang and lang.lower() != "en")
            except Exception:
                pass
        if looks_non_en:
            ups = []
            if "Status Code" in header:
                ups.append({"row": r_idx, "col": "Status Code", "val": "NON_EN"})
            ups.append({"row": r_idx, "col": COLN["status"], "val": "Skipped: Non-English (leave Queue checked to retry)"})
            ups.append({"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")})
            batch_update(sh, ups)
            processed += 1
            time.sleep(PAUSE_BETWEEN_ROWS_SEC)
            continue

        # mark processing
        ups = []
        if COLN["status"] in header:
            ups.append({"row": r_idx, "col": COLN["status"], "val": "Processing: Copy"})
        if COLN["updated"] in header:
            ups.append({"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")})
        batch_update(sh, ups)

        try:
            msgs = build_messages(deliverable_label, seq_len, bio, titles, lp, niche, selling, target, pitch, approved_angles)
            parsed = call_openai(client, msgs)
            final_text = build_final_text(kind, parsed, seq_len)

            # Determine parent folder (date subfolder if enabled)
            parent = TARGET_FOLDER_ID
            if not parent:
                raise RuntimeError("Missing TARGET_FOLDER_ID. Set env TARGET_FOLDER_ID or hardcode in script.")
            if GROUP_BY_DATE:
                datestr = datetime.now().strftime("%Y-%m-%d")
                parent = ensure_date_subfolder(drive, TARGET_FOLDER_ID, datestr)

            title = sanitize_title(f"{ch_name} — {deliverable_label}")
            content_hash = hashlib.sha256((title + "\n" + final_text).encode("utf-8")).hexdigest()
            file_id = create_or_update_doc(drive, docs, parent, title, final_text, content_hash)
            doc_url = f"https://docs.google.com/document/d/{file_id}/view"

            ups = [
                {"row": r_idx, "col": COLN.get("doc_url", "Doc URL"), "val": doc_url},
                {"row": r_idx, "col": COLN["status"], "val": "Done: Export"},
                {"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")},
            ]
            header_now = sh.row_values(1)
            if "Status Code" in header_now:
                ups.append({"row": r_idx, "col": "Status Code", "val": "OK"})
            batch_update(sh, ups)

            # Clear Queue Copy and color cell light green
            try:
                if COLN["queue_copy"] in header_now:
                    idx_q = header_now.index(COLN["queue_copy"]) + 1
                    rng_cell = a1_range(r_idx, idx_q, 1, 1)
                    try:
                        sh.update_acell(rng_cell, False)
                    except Exception:
                        try:
                            sh.update_acell(rng_cell, "")
                        except Exception:
                            pass
                    try:
                        sh.format(rng_cell, {"backgroundColor": {"red": 0.88, "green": 0.96, "blue": 0.88}})
                    except Exception:
                        pass
            except Exception:
                pass

        except Exception as e:
            print("[Copy][Exception]", repr(e))
            ups = []
            if "Status Code" in header:
                ups.append({"row": r_idx, "col": "Status Code", "val": "ERR"})
            ups.append({"row": r_idx, "col": COLN["status"], "val": f"Error: {str(e)[:160]}"})
            ups.append({"row": r_idx, "col": COLN["updated"], "val": datetime.now().strftime("%Y-%m-%d %H:%M")})
            batch_update(sh, ups)
            processed += 1
            time.sleep(PAUSE_BETWEEN_ROWS_SEC)
            continue

        processed += 1
        time.sleep(PAUSE_BETWEEN_ROWS_SEC)

    return processed


def process_all(sh, client: OpenAI, drive, docs, batch_size: int) -> int:
    header = sh.row_values(1)
    required = [
        COLN["approved"], COLN["angles_approved"], COLN["queue_copy"], COLN["deliverable"],
        COLN["bio"], COLN["titles"], COLN["niche"], COLN["selling"], COLN["target"], COLN["pitch"],
        COLN["angles"], COLN["status"], COLN["updated"], COLN.get("doc_url", "Doc URL")
    ]
    missing = [h for h in required if h not in header]
    if missing:
        raise RuntimeError(f"Missing required headers: {missing}")

    colmap = {v: (header.index(v) + 1) if v in header else None for v in COLN.values()}

    total = 0
    while True:
        done = process_once(sh, colmap, client, drive, docs, batch_size)
        total += done
        if done == 0:
            break
        time.sleep(PAUSE_BETWEEN_BATCHES_SEC)
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sheet-id", default="", help="Google Sheet ID (from the URL)")
    parser.add_argument("--batch-size", type=int, default=0)
    args = parser.parse_args()

    sheet_id = (args.sheet_id or DEFAULT_SHEET_ID).strip()
    if not sheet_id:
        raise RuntimeError("Provide --sheet-id or set DEFAULT_SHEET_ID in the script.")

    # Hardcoded OpenAI API key (fallback if environment variable not set)
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        api_key = "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"  # TODO: replace with your actual key

    client = OpenAI(api_key=api_key)

    creds = get_google_creds()
    drive, docs = build_services(creds)

    sh = open_sheet(sheet_id)
    batch_size = args.batch_size if args.batch_size else 0

    print(f"[Copy] Sheet: {sheet_id} | Batch: {'All queued' if batch_size == 0 else batch_size} | Model: {OPENAI_MODEL}")
    total = process_all(sh, client, drive, docs, batch_size)
    print(f"Copy exported to Google Docs for {total} row(s).")
    try:
        send_slack_message(f"✅ Copy generation finished! Exported {total} row(s) to Google Docs.")
    except Exception as e:
        print(f"[Slack] Notification failed: {e}")


if __name__ == "__main__":
    main()
