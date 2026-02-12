import os
import asyncio
import time
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from openai import OpenAI

# ==============================
# CONFIG
# ==============================

OPENAI_API_KEY = "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"
MODEL = "gpt-4o-mini"
MAX_CONCURRENT_REQUESTS = 12

SPREADSHEET_ID = "1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE"
WORKSHEET_NAME = "Raw Leads"

BIO_COLUMN = "YouTube Bio"
TITLES_COLUMN = "Recent Titles (15 Non-Shorts)"
RATING_COLUMN = "Rating"
NICHE_COLUMN = "Niche"

# ==============================
# OPENAI CLIENT
# ==============================

client = OpenAI(api_key=OPENAI_API_KEY)

# ==============================
# GOOGLE SHEETS SETUP
# ==============================

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

CREDS = ServiceAccountCredentials.from_json_keyfile_name(
    "google_sheets_key.json",
    SCOPE
)

GSPREAD_CLIENT = gspread.authorize(CREDS)
SHEET = GSPREAD_CLIENT.open_by_key(SPREADSHEET_ID).worksheet(WORKSHEET_NAME)


# ==============================
# PROMPT BUILDER
# ==============================

def build_prompt(bio, titles):
    return f"""
You are evaluating whether a YouTube channel could support a paid course, community, cohort, certification, or coaching model ICP.

EVALUATION QUESTIONS:

1. Do premium ($1K+) courses already exist in this niche?
2. Do people pay for 1-on-1 coaching or consulting in this skill?
3. Would a paid community add value (accountability, feedback, critique, practice partners)?
4. Can a credible YouTuber teach this without requiring formal medical or licensed credentials?
5. Does mastering this skill create income potential, career advancement, or meaningful life leverage?

SCORING:
Evaluate all 5 questions carefully.
Score from 0 to 10 based on overall strength across these criteria.
Higher score = stronger high-ticket digital skill viability.

AUTOMATIC DISQUALIFICATION (Return 0 ONLY if clearly and primarily true):

- Pure gaming/streaming entertainment channel
- Pure reaction/entertainment channel with no skill progression
- Credential-required licensed professions (medical, legal, skincare recommendations)
- Corporate channels
- Channels that exist purely to sell or teach marketing/funnel/course-creation, copywriting services to other creators

OUTPUT FORMAT:
Return JSON in this exact format:
{{
  "score": <number 0-10>,
  "niche": "<short niche label (3-6 words)>"
}}

No explanation.
Only valid JSON.

CHANNEL BIO:
{bio}

RECENT TITLES:
{titles}
""".strip()


# ==============================
# BATCH MODE MAIN
# ==============================

def main():
    print("Fetching sheet data...")
    records = SHEET.get_all_records()

    print("Preparing rows...")
    rows_to_process = []

    for idx, record in enumerate(records, start=2):
        current_rating = record.get(RATING_COLUMN)
        current_niche = record.get(NICHE_COLUMN)

        if str(current_rating).strip() != "" and str(current_niche).strip() != "":
            continue

        bio = record.get(BIO_COLUMN, "")
        titles = record.get(TITLES_COLUMN, "")

        if not bio and not titles:
            continue

        prompt = build_prompt(bio or "", titles or "")

        rows_to_process.append((idx, prompt))

    print(f"Total rows to score: {len(rows_to_process)}")

    # --------------------------------
    # If previous batch output exists, skip creating new batch
    # --------------------------------
    result_filename = "batch_output.jsonl"

    if os.path.exists(result_filename):
        print("Existing batch_output.jsonl found. Skipping batch creation and parsing existing results...")
        process_existing_results(result_filename)
        return

    if not rows_to_process:
        print("Nothing to process.")
        return

    # --------------------------------
    # Create JSONL batch file
    # --------------------------------

    batch_filename = "batch_input.jsonl"

    with open(batch_filename, "w") as f:
        for row_index, prompt in rows_to_process:
            request = {
                "custom_id": f"row_{row_index}",
                "method": "POST",
                "url": "/v1/responses",
                "body": {
                    "model": MODEL,
                    "input": prompt,
                    "max_output_tokens": 50,
                    "temperature": 0
                }
            }
            import json
            f.write(json.dumps(request) + "\n")

    print("Uploading batch file...")

    batch_file = client.files.create(
        file=open(batch_filename, "rb"),
        purpose="batch"
    )

    print("Creating batch job...")

    batch = client.batches.create(
        input_file_id=batch_file.id,
        endpoint="/v1/responses",
        completion_window="24h"
    )

    print(f"Batch ID: {batch.id}")
    print("Waiting for batch to complete...")

    # --------------------------------
    # Poll for completion
    # --------------------------------

    while True:
        batch_status = client.batches.retrieve(batch.id)

        print(f"Status: {batch_status.status}")

        if batch_status.status in ["completed", "failed", "expired"]:
            break

        time.sleep(15)

    if batch_status.status != "completed":
        print("Batch failed or expired.")
        return

    print("Downloading results...")

    output_file_id = batch_status.output_file_id
    result_file = client.files.content(output_file_id)

    result_filename = "batch_output.jsonl"

    with open(result_filename, "wb") as f:
        f.write(result_file.read())

    # --------------------------------
    # Parse results and update sheet
    # --------------------------------

    import json

    updates = []

    with open(result_filename, "r") as f:
        for line in f:
            result = json.loads(line)

            row_index = int(result["custom_id"].replace("row_", ""))

            # Extract text from Responses API structure
            response_wrapper = result.get("response", {})
            response_body = response_wrapper.get("body", {})
            output_text = ""

            if "output" in response_body:
                for item in response_body.get("output", []):
                    for content_item in item.get("content", []):
                        if content_item.get("type") == "output_text":
                            output_text += content_item.get("text", "")

            response = output_text.strip()

            if response.startswith("```"):
                response = response.strip("`")
                response = response.replace("json", "", 1).strip()

            try:
                data = json.loads(response)
                score = int(data.get("score", 0))
                niche = str(data.get("niche", "")).strip()
            except Exception:
                score = 0
                niche = ""

            updates.append({
                "range": f"L{row_index}:O{row_index}",
                "values": [[score, niche]]
            })

    print("Writing results to sheet...")
    SHEET.batch_update(updates)

    print("All rows processed and saved successfully.")


def process_existing_results(result_filename):
    import json

    updates = []

    with open(result_filename, "r") as f:
        for line in f:
            try:
                result = json.loads(line)
            except Exception:
                continue

            row_index = None
            try:
                row_index = int(result.get("custom_id", "").replace("row_", ""))
            except Exception:
                continue

            response_wrapper = result.get("response", {})
            response_body = response_wrapper.get("body", {})
            output_text = ""

            if "output" in response_body:
                for item in response_body.get("output", []):
                    for content_item in item.get("content", []):
                        if content_item.get("type") == "output_text":
                            output_text += content_item.get("text", "")

            response = output_text.strip()

            if response.startswith("```"):
                response = response.strip("`")
                response = response.replace("json", "", 1).strip()

            try:
                data = json.loads(response)
                score = int(data.get("score", 0))
                niche = str(data.get("niche", "")).strip()
            except Exception:
                score = 0
                niche = ""

            if row_index:
                updates.append({
                    "range": f"L{row_index}:O{row_index}",
                    "values": [[score, niche]]
                })

    if updates:
        print("Writing parsed results to sheet...")
        SHEET.batch_update(updates)
        print("Sheet updated successfully.")
    else:
        print("No valid results found in existing batch file.")


if __name__ == "__main__":
    main()