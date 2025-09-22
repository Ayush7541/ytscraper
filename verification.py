import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json
import time

# === CONFIG ===
SHEET_KEY = "1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE"  # Replace with your sheet key
RAW_TAB_NAME = "Raw Leads"
INVALID_TAB_NAME = "Invalid Leads"
MV_API_KEY = "Jlgl2wmPBFbF5kIsETdZsbabl"
SLACK_WEBHOOK = "https://hooks.slack.com/services/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"

# === GOOGLE SHEETS SETUP ===
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS = ServiceAccountCredentials.from_json_keyfile_name("google_sheets_key.json", SCOPE)
GSPREAD_CLIENT = gspread.authorize(CREDS)

sheet = GSPREAD_CLIENT.open_by_key(SHEET_KEY)
raw_tab = sheet.worksheet(RAW_TAB_NAME)

# Create Invalid Leads tab if it doesn't exist
try:
    invalid_tab = sheet.worksheet(INVALID_TAB_NAME)
except gspread.WorksheetNotFound:
    invalid_tab = sheet.add_worksheet(title=INVALID_TAB_NAME, rows="100", cols="30")

# === HELPER: Bulk MillionVerifier API ===
def verify_emails_bulk(emails):
    url = f"https://api.millionverifier.com/api/v3/?api={MV_API_KEY}"
    payload = {"emails": emails}
    headers = {"Content-Type": "application/json"}
    max_retries = 3
    delay_seconds = 5
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            return data.get("result", {})
        except Exception as e:
            if attempt < max_retries:
                print(f"Bulk verification attempt {attempt} failed: {e}. Retrying in {delay_seconds} seconds...")
                time.sleep(delay_seconds)
            else:
                error_msg = f"Error during bulk verification after {max_retries} attempts: {e}"
                print(error_msg)
                send_slack_message(f":x: {error_msg}")
                return None

# === HELPER: Send Slack Notification ===
def send_slack_message(message):
    try:
        requests.post(SLACK_WEBHOOK, headers={"Content-Type": "application/json"}, json={"text": message})
    except Exception as e:
        print(f"[Slack] Notification failed: {e}")

# === PROCESS ===
def process_leads():
    header = raw_tab.row_values(1)
    email_col_idx = header.index("Email") + 1
    # Add Verification Status if missing
    if "Verification Status" not in header:
        raw_tab.update_cell(1, len(header) + 1, "Verification Status")
        status_col_idx = len(header) + 1
    else:
        status_col_idx = header.index("Verification Status") + 1

    rows = raw_tab.get_all_values()[1:]  # skip header

    # Extract emails to verify, ignoring empty or "no"
    emails_to_verify = []
    email_row_map = {}
    for i, row in enumerate(rows):
        email = row[email_col_idx - 1].strip()
        if email and email.lower() != "no":
            emails_to_verify.append(email)
            email_row_map[email] = i

    if not emails_to_verify:
        print("No valid emails to verify.")
        return

    # Bulk verify emails
    results = verify_emails_bulk(emails_to_verify)
    if results is None:
        return  # error already handled

    valid_rows = []
    invalid_rows = []
    unknown_rows = []

    # Update rows with verification status
    for email in emails_to_verify:
        result = results.get(email, "unknown")
        row_idx = email_row_map[email]
        row = rows[row_idx]
        # Ensure row has enough columns for status
        while len(row) < status_col_idx:
            row.append("")
        row[status_col_idx - 1] = result

        if result == "ok":
            valid_rows.append(row)
        elif result in ["bad", "disposable", "catch_all"]:
            invalid_rows.append(row)
        else:  # unknown or error
            unknown_rows.append(row)

    # Rows with no email or "no" remain unchanged and included as unknown_rows
    for i, row in enumerate(rows):
        email = row[email_col_idx - 1].strip()
        if not email or email.lower() == "no":
            # Ensure Verification Status column exists and empty
            while len(row) < status_col_idx:
                row.append("")
            row[status_col_idx - 1] = ""
            unknown_rows.append(row)

    # Update Invalid Leads tab
    if invalid_rows:
        invalid_tab.append_rows(invalid_rows, value_input_option="RAW")

    # Overwrite Raw Leads tab with header + valid + unknown rows
    all_rows_to_keep = valid_rows + unknown_rows
    raw_tab.resize(rows=1)  # keep header
    if all_rows_to_keep:
        raw_tab.append_rows(all_rows_to_keep, value_input_option="RAW")

    # Send Slack notification
    total_checked = len(emails_to_verify)
    total_valid = len(valid_rows)
    total_invalid = len(invalid_rows)
    total_unknown = len(unknown_rows)
    slack_message = (
        f"✅ Email Verification Complete!\n"
        f"Total Leads Checked: {total_checked}\n"
        f"Valid Emails: {total_valid}\n"
        f"Invalid Emails Moved: {total_invalid}\n"
        f"Unknown Emails Kept: {total_unknown}"
    )
    send_slack_message(slack_message)
    print(slack_message)

# === RUN ===
if __name__ == "__main__":
    process_leads()