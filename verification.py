import gspread
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json
import time

# === CONFIG ===
SHEET_KEY = "1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE"  # Replace with your sheet key
RAW_TAB_NAME = "Raw Leads"
INVALID_TAB_NAME = "Invalid Leads"
MV_API_KEY = "kMJzNxpD8LJy8zDLqU7QVXrWL"
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
    results = {}
    max_retries = 3
    delay_seconds = 5
    for email in emails:
        for attempt in range(1, max_retries + 1):
            try:
                url = f"https://api.millionverifier.com/api/v3/?api={MV_API_KEY}&email={email}"
                r = requests.get(url, timeout=30)
                r.raise_for_status()
                data = r.json()
                results[email] = data.get("result", "unknown")
                break
            except Exception as e:
                if attempt < max_retries:
                    print(f"Verification attempt {attempt} failed for {email}: {e}. Retrying in {delay_seconds} seconds...")
                    time.sleep(delay_seconds)
                else:
                    error_msg = f"Error during verification of {email} after {max_retries} attempts: {e}"
                    print(error_msg)
                    send_slack_message(f":x: {error_msg}")
                    results[email] = "unknown"
    return results

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

    # Add Verification Status if missing and refresh header so indices stay correct
    if "Verification Status" not in header:
        raw_tab.update_cell(1, len(header) + 1, "Verification Status")
        # refresh header to include the newly added column
        header = raw_tab.row_values(1)
    status_col_idx = header.index("Verification Status") + 1

    rows = raw_tab.get_all_values()[1:]  # skip header

    # Extract emails to verify, ignoring empty or "no". Keep row indices so duplicates are handled.
    emails_to_verify = []
    email_row_indices = []  # parallel array to map each email to its row index in `rows`
    for i, row in enumerate(rows):
        try:
            email = row[email_col_idx - 1].strip()
        except IndexError:
            email = ""
        if email and email.lower() != "no":
            emails_to_verify.append(email)
            email_row_indices.append(i)

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
    processed_indices = set()

    # Update rows with verification status - handle duplicates by using email_row_indices
    for k, email in enumerate(emails_to_verify):
        result = results.get(email, "unknown")
        row_idx = email_row_indices[k]
        row = rows[row_idx]

        # Ensure row has enough columns for status
        while len(row) < status_col_idx:
            row.append("")

        # Normalize the result string to avoid case/format mismatches from API (e.g. 'Ok', 'Invalid', 'catch-all')
        normalized = str(result).lower().strip().replace('-', '_').replace(' ', '_')

        # Set the visible status cell to the raw result we received (keeps original capitalization for debugging)
        row[status_col_idx - 1] = result
        processed_indices.add(row_idx)

        # Classify into buckets. Treat Invalid/Disposable/Unknown as invalid; Ok/Valid as valid.
        if normalized in ("ok", "good", "valid"):
            valid_rows.append(row)
        elif normalized in ("invalid", "disposable", "bad", "catch_all", "unknown", "accept_all", "role", "free"):
            invalid_rows.append(row)

    # Rows that were not processed (no email or explicitly 'no') should be kept in raw leads as invalid
    for i, row in enumerate(rows):
        if i in processed_indices:
            continue
        # Ensure Verification Status column exists and empty
        while len(row) < status_col_idx:
            row.append("")
        row[status_col_idx - 1] = ""
        invalid_rows.append(row)

    # Update Invalid Leads tab (include both invalid and unknown rows which are merged)
    if invalid_rows:
        invalid_tab.append_rows(invalid_rows, value_input_option="RAW")

    # Overwrite Raw Leads tab with header + valid rows only
    all_rows_to_keep = valid_rows

    # Safely keep only the header row without using resize (resize can fail if sheet has frozen rows).
    # Clear all rows below the header instead, then re-append the rows we want to keep.
    try:
        # Determine current number of rows in the worksheet
        current_rows = raw_tab.row_count
        if current_rows > 1:
            # Number of columns to use for the clear range (use header length as safe max)
            max_col = max(len(header), status_col_idx) if header else status_col_idx

            # Helper: convert 1-based column index to letter(s) (1 -> A, 27 -> AA)
            def col_idx_to_letter(idx):
                letters = ''
                while idx > 0:
                    idx, rem = divmod(idx - 1, 26)
                    letters = chr(65 + rem) + letters
                return letters

            last_col_letter = col_idx_to_letter(max_col)
            range_to_clear = f"A2:{last_col_letter}{current_rows}"

            # Clear everything below the header row while preserving header and frozen rows
            raw_tab.batch_clear([range_to_clear])
    except Exception as e:
        # If batch_clear fails for any reason, fall back to clearing the entire sheet and
        # rewriting the header row so the script can continue.
        print(f"[Warning] Could not clear rows using batch_clear: {e}. Falling back to clear() and re-writing header.")
        raw_tab.clear()
        raw_tab.append_row(header, value_input_option="RAW")

    # Append rows we want to keep (below header)
    if all_rows_to_keep:
        raw_tab.append_rows(all_rows_to_keep, value_input_option="RAW")

    # Send Slack notification
    total_checked = len(emails_to_verify)
    total_valid = len(valid_rows)
    total_invalid = len(invalid_rows)
    slack_message = (
        f"✅ Email Verification Complete!\n"
        f"Total Leads Checked: {total_checked}\n"
        f"Valid Emails: {total_valid}\n"
        f"Invalid/Unknown Emails Moved: {total_invalid}"
    )
    send_slack_message(slack_message)
    print(slack_message)

# === RUN ===
if __name__ == "__main__":
    process_leads()