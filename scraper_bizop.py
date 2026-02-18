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
SLACK_WEBHOOK = os.environ["SLACK_WEBHOOK"]

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

API_KEYS = json.loads(os.environ["YOUTUBE_API_KEYS"])

# OpenAI key via env var preferred
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]

# Parameters
MIN_SUBS = 5000
MAX_SUBS = 125000
MAX_VIDEO_AGE_DAYS = 90       # only consider videos <= 180 days old
TARGET_LEADS = 10000              # collect 15 qualified leads (rating >= 7)
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
google_creds_dict = json.loads(os.environ["GOOGLE_SHEETS_JSON"])
CREDS = ServiceAccountCredentials.from_json_keyfile_dict(google_creds_dict, SCOPE)
GSPREAD_CLIENT = gspread.authorize(CREDS)
SHEET = GSPREAD_CLIENT.open_by_key("1cbTEk9zmouLGUhnvzVxea6oChbj0WmeHKlqHG55Z0XE").worksheet("Raw Leads")

# instantiate clients
youtube_clients = [build('youtube', 'v3', developerKey=key) for key in API_KEYS]

# Track exhausted YouTube API keys (quota exceeded)
exhausted_api_indexes = set()
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


def generate_keywords_with_openai(n_min=KEYWORD_TITLES_MIN, n_max=KEYWORD_TITLES_MAX):
    """
    Generate YouTube video title keywords using OpenAI (gpt-4o-mini) with detailed example prompt.
    Retries up to 3 times on failure, returns [] if all attempts fail.
    Ensures content is non-empty and valid JSON.
    """
    import json, time, random

    n_target = random.randint(n_min, n_max)

    prompt = """
Generate YouTube video titles that implicitly define a complete, monetizable channel niche.

CRITICAL OBJECTIVE
These titles must represent creators who:
- Have real skill depth
- Teach structured knowledge or technique
- Have practitioner authority
- Likely monetize poorly or inconsistently
- Would NEED help building funnels, offers, courses, communities, and backend systems

Avoid creators who already understand marketing infrastructure.

CORE REQUIREMENT (NON-NEGOTIABLE)
Each title must clearly represent:
- A hyper-specific PRACTICE or CRAFT (not a topic, identity, or broad category)
- Something people actively DO and try to improve
- Narrow enough to build an entire channel around ONLY that thing
- A skill that could support a paid course, community, cohort, certification, or coaching model

VALIDATION TEST (APPLY INTERNALLY)

Only include the title if ALL answers are YES:

1. Do premium ($1K+) courses already exist in this niche?
2. Do people pay for 1-on-1 coaching or consulting in this skill?
3. Is there a clear transformation or measurable before/after result?
4. Would a paid community add value (accountability, feedback, critique, practice partners)?
5. Can a credible YouTuber teach this without requiring formal medical or licensed credentials?
6. Is this evergreen with ongoing demand (not trend-based)?
7. Can this skill realistically be learned digitally without mandatory in-person supervision?
8. Does mastering this skill create income potential, career advancement, or meaningful life leverage?

If any answer is NO, discard it.

STRICTLY AVOID:
- Funnels
- Digital marketing
- Copywriting
- SMMA
- Dropshipping
- Ecommerce strategy
- Shopify
- Make money online
- Crypto speculation
- Trading signals
- Productivity guru niches
- Entertainment
- Gaming
- Reaction channels
- Vlogs
- Meme content
- Board games
- Licensed medical doctors or clinical treatment

DEPTH REQUIREMENT
Go 2–3 layers deeper than the obvious category.

Bad:
- Piano lessons
- Fitness training
- Investing advice
- Dating tips

Good:
- Adult piano finger independence drills
- Running gait correction for amateur runners
- Dividend reinvestment discipline systems
- Conflict debrief frameworks for married couples

STRUCTURAL DIVERSITY RULE
Each title must come from a completely different domain.
Maximize industry and skill diversity.

Below are 200 example niche DIRECTIONS to inspire domain diversity.
Do NOT repeat them verbatim.
Use them only to understand the TYPE of creators desired.


1. Behavior correction systems for parents of ADHD children without medication dependency  
2. Teaching emotional regulation skills to children ages 4–10 through daily routines  
3. Co-parenting communication frameworks after high-conflict divorce  
4. Blended family boundary-setting and stepparent role definition systems  
5. Caregiver communication skills for adult children managing aging parents  
6. Biblical parenting methodology and scripture-based discipline systems  
7. Rebuilding parental authority after permissive parenting years  
8. Parenting gifted and twice-exceptional children through structured challenge systems  
9. Screen time boundary enforcement systems for families with young children  
10. Raising emotionally intelligent boys through deliberate daily practice  
11. Conflict de-escalation scripts and frameworks for married couples  
12. Rebuilding trust after infidelity through structured reconciliation systems  
13. Gottman-based communication skill drilling for couples without therapy  
14. Rebuilding intimacy after years of emotional disconnection in marriage  
15. Pre-marriage compatibility assessment and hard conversation frameworks  
16. Long-distance relationship maintenance systems and communication discipline  
17. Rebuilding social identity and daily structure after spouse loss  
18. Attachment style identification and behavior change for anxious partners  
19. Setting and enforcing boundaries in codependent relationship patterns  
20. Masculine leadership communication frameworks for long-term relationships  
21. Social confidence rebuilding for late-diagnosed ADHD and autism adults  
22. Dating standards calibration and filtering systems for women over 35  
23. Rejection desensitization and exposure systems for socially anxious men  
24. Re-entering dating after long-term relationship or divorce at 40+  
25. Conversation depth-building skills for people stuck in surface-level interactions  
26. Body language and presence calibration for professional and social settings  
27. Charisma micro-skill drilling — eye contact, vocal tonality, and warmth  
28. Assertiveness training systems for chronic people-pleasers  
29. Building genuine male friendships in adulthood through deliberate systems  
30. Social skill rebuilding for adults after extended isolation  
31. Corporate culture navigation for first-generation professionals  
32. Career re-entry positioning and interview systems for women after long gaps  
33. Burnout-to-pivot framework for mid-career professionals changing industries  
34. Salary negotiation systems for mid-career professionals who avoid conflict  
35. Executive presence and boardroom communication mastery  
36. Internal promotion strategy systems for corporate employees stuck at mid-level  
37. Performance review preparation and self-advocacy frameworks  
38. Building workplace visibility without political game-playing  
39. Professional boundary enforcement for people who over-deliver and undercharge  
40. Transitioning from individual contributor to first-time manager systems  
41. Pre-retirement purpose planning and identity transition for ages 55–65  
42. Faith deconstruction journaling and identity rebuilding frameworks  
43. Empty nest identity rebuilding for mothers after children leave home  
44. Post-military civilian life integration and career translation systems  
45. Rebuilding identity after leaving a high-control religious group or cult  
46. Career identity rebuilding after involuntary job loss at 50+  
47. Sobriety lifestyle design and identity reconstruction after quitting alcohol  
48. Rebuilding masculine identity after divorce for men over 40  
49. Second-chapter purpose mapping for corporate professionals near retirement  
50. Life redesign systems for adults after chronic illness diagnosis  
51. Medical writing skill-building for nurses, PAs, and allied health professionals  
52. Grant proposal writing for small nonprofit leaders and community organizers  
53. Expert witness report writing and testimony preparation for licensed professionals  
54. Academic journal writing and publication navigation for early-career researchers  
55. Policy memo and briefing writing for government and think tank professionals  
56. Technical documentation writing for non-native English software professionals  
57. Business case writing and internal proposal frameworks for corporate employees  
58. White paper writing for consultants and independent subject matter experts  
59. Regulatory submission writing for pharmaceutical and medical device professionals  
60. Legal brief writing skill-building for paralegals and law clerks  
61. Quran tajweed rules and recitation mastery for adult learners  
62. Biblical Hebrew reading and Old Testament exegesis for lay Christians  
63. Torah study methodology and Talmudic text interpretation for non-rabbinical learners  
64. Stoic journaling and philosophical practice systems for daily life  
65. Lectio Divina and contemplative prayer practice systems for Catholics  
66. Sanskrit pronunciation and Vedic chanting technique for yoga practitioners  
67. Integrating faith with modern psychology for practicing Christians  
68. Sabbath practice design and rest discipline systems for evangelical families  
69. Islamic jurisprudence self-study systems for English-speaking Muslims  
70. Applied Stoicism for emotional regulation and performance under pressure  
71. Perimenopause strength training and body composition self-management  
72. Chronic desk posture correction through daily movement reprogramming  
73. Sleep schedule rebuilding and architecture repair for shift workers  
74. Pelvic floor rehabilitation exercise systems for postpartum women  
75. Shoulder mobility restoration through movement reprogramming for desk workers  
76. Hip capsule mobility systems for athletes and sedentary professionals  
77. Scoliosis self-management through targeted corrective movement systems  
78. Breathwork protocols for anxiety and nervous system regulation  
79. Jaw tension and TMJ self-management through corrective movement  
80. Flat foot correction and arch rebuilding through progressive loading systems  
81. Running gait correction and injury prevention for amateur runners  
82. Calisthenics skill progression systems — handstand, planche, front lever  
83. Olympic weightlifting snatch and clean-and-jerk technique for self-coached athletes  
84. Kettlebell flow design and programming for independent practitioners  
85. Triathlon race pacing and transition optimization systems  
86. Sprint acceleration mechanics and block start technique for field athletes  
87. Competitive swimming stroke efficiency drilling for adult swimmers  
88. Rock climbing grip endurance and route reading systems  
89. Surf technique correction for intermediate surfers plateauing on waves  
90. Gymnastics ring and bar foundation skills for adult beginners  
91. Memory palace construction and competitive recall training systems  
92. Speed reading with comprehension and retention verification systems  
93. Mental math acceleration and estimation systems for professionals  
94. Deep work protocol design and attention span rebuilding for knowledge workers  
95. Cognitive reframing training for performance under pressure  
96. Stress inoculation and emotional regulation discipline for high-stakes professionals  
97. Dopamine discipline and distraction resistance frameworks for creatives  
98. Visualization and mental rehearsal systems for athletes and performers  
99. Learning acceleration and deliberate practice design for skill acquisition  
100. Focus stamina training and single-tasking discipline for knowledge workers  
101. Fingerstyle guitar arrangement and chord melody technique for self-taught players  
102. Jazz chord voicing and comping systems for non-classically trained guitarists  
103. Film score orchestration for indie composers working in DAWs  
104. Drum programming realism and groove design for beatmakers  
105. Vocal harmony stacking and layering technique in home production  
106. Bass guitar slap technique and groove construction for intermediate players  
107. Music theory application systems for producers without classical training  
108. Acoustic room treatment and home studio design on a budget  
109. Audio post-production dialogue editing for independent filmmakers  
110. Eurorack modular synthesis patching and sound design systems  
111. Digital portrait painting rendering systems in Procreate and Photoshop  
112. Character concept art development for game and animation pipelines  
113. Scientific and botanical illustration technique for publishing and licensing  
114. Textile surface pattern design for commercial fabric licensing  
115. Motion graphics and kinetic typography systems in After Effects  
116. 2D hand-drawn animation principles and timing for independent animators  
117. Digital sculpting anatomy systems in ZBrush for character artists  
118. Architectural visualization rendering workflow in Blender  
119. Storyboard art technique for film and advertising professionals  
120. Miniature painting precision and detailing systems for tabletop artists  
121. Wedding photography posing and natural light reading systems  
122. Documentary interview lighting and camera setup for independent filmmakers  
123. Color grading systems for cinematic commercial video production  
124. Real estate photography HDR processing and editing workflow  
125. Product photography lighting and compositing for independent sellers  
126. Drone cinematography shot design and movement systems  
127. Portrait lighting ratio mastery and modifier control  
128. Analog film photography and darkroom printing technique  
129. Astrophotography capture tracking and image stacking systems  
130. Underwater photography and housing technique for dive enthusiasts  
131. American English pronunciation systems for non-native professional speakers  
132. Accent coaching technique for actors and voice professionals  
133. Business Japanese conversation and professional writing systems  
134. Mandarin tonal accuracy and character writing systems for self-taught learners  
135. Heritage Spanish recovery systems for second-generation speakers  
136. ASL conversational fluency for hearing individuals in professional settings  
137. IPA phonetics and transcription mastery for language coaches  
138. Speechwriting structure and rhetorical craft for executives and leaders  
139. Competitive debate cross-examination technique and argumentation systems  
140. Public speaking anxiety elimination through structured exposure systems  
141. Furniture joinery and hand tool woodworking for independent craftspeople  
142. Japanese joinery systems and precision fitting for furniture makers  
143. Leather goods construction and hand saddle stitching for independent sellers  
144. Pottery wheel throwing and glaze chemistry for production potters  
145. Blacksmithing and knife forging technique for independent makers  
146. Tailoring and suit construction from scratch for aspiring clothiers  
147. Furniture restoration and antique refinishing for independent restorers  
148. Knife sharpening on whetstones to professional edges for service providers  
149. Artisan chocolate tempering and bonbon construction for small-batch producers  
150. Calligraphy flourishing and brush lettering mastery for commissioned artists  
151. Sourdough fermentation science and crust control for serious home bakers  
152. Artisan cheese aging and culture management for small-batch producers  
153. Espresso extraction and pressure profiling for home baristas and café owners  
154. Craft mead brewing refinement and competition preparation systems  
155. Fermentation mastery — kimchi, miso, koji, and kefir for health-focused practitioners  
156. Natural wine sensory evaluation and small-batch production systems  
157. Artisan pasta extrusion science and fresh dough technique  
158. Specialty coffee roasting calibration and cupping systems  
159. Fermented hot sauce formulation and flavor development for small producers  
160. Cocktail balance frameworks and flavor pairing for independent bartenders  
161. Beekeeping hive management and honey yield optimization systems  
162. Aquaponics system design and fish-plant nutrient balancing  
163. Permaculture design and food forest establishment for small land owners  
164. Indoor mushroom cultivation and small-scale commercial yield scaling  
165. Hydroponic nutrient balancing and system builds for home growers  
166. Microgreen commercial growing and yield scaling systems  
167. Seed saving genetics and open-pollinated variety selection systems  
168. Orchard pruning mastery and fruit tree management for homesteaders  
169. Rainwater harvesting and greywater system design for off-grid properties  
170. Bonsai shaping discipline and species-specific care systems  
171. Dividend portfolio structuring and DRIP discipline for long-term investors  
172. Real estate underwriting and rental cash flow analysis for first-time investors  
173. Retirement planning systems for self-employed and freelance professionals  
174. Debt payoff sprint systems using snowball and avalanche behavioral frameworks  
175. Zero-based budgeting and monthly cash flow forecasting systems  
176. Real estate house hacking and first rental acquisition systems  
177. Tax optimization strategy education for W2 and self-employed earners  
178. Financial statement analysis skill-building for non-finance operators  
179. Emergency fund engineering and capital allocation for early wealth builders  
180. Business acquisition analysis and due diligence walkthrough for searchers  
181. Advanced Excel financial modeling and dashboard building for analysts  
182. SQL query writing and database fundamentals for non-technical analysts  
183. Data visualization and executive-level storytelling for business professionals  
184. Operations workflow design and process documentation for small businesses  
185. 3D CAD mechanical drafting for independent product designers  
186. Prompt engineering and AI workflow integration for knowledge professionals  
187. Home energy efficiency retrofitting and insulation systems for homeowners  
188. Solar panel system sizing and installation fundamentals for homeowners  
189. Smart home automation design and device integration systems  
190. 3D printing design optimization and functional CAD for product makers  
191. Bushcraft and primitive survival skill progression for outdoor enthusiasts  
192. Wilderness navigation without GPS using map, compass, and terrain reading  
193. Emergency preparedness and disaster logistics planning for families  
194. Wilderness first response skills for outdoor trip leaders  
195. Freediving breath-hold training and equalization technique for divers  
196. Open water swimming navigation and endurance pacing systems  
197. Competitive archery form, release mechanics, and mental game systems  
198. Civilian situational awareness and threat assessment training  
199. Off-grid water purification system design and implementation  
200. Survival shelter construction and natural material sourcing systems  

OUTPUT RULES
- Titles ONLY
- Return ONLY a valid JSON array of strings
- No numbering
- No markdown
- No explanation
- Clear literal language
- Each title should feel like an entire channel niche
- Strong diversity between titles

Generate 25–30 distinct YouTube video titles that meet ALL rules above.
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
    worksheet_names = ["Raw Leads", "Processed Leads", "Invalid Leads", "Rated and Approved"]
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
    stop_scraping = False
    while unique_appended_this_run < TARGET_LEADS and not stop_scraping:
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
        for page_num in range(3):
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
            except HttpError as e:
                print(f"[YouTube API] HttpError on key idx {api_index}: {e}")
                if "quotaExceeded" in str(e):
                    exhausted_api_indexes.add(api_index)
                    print(f"[YouTube API] API key index {api_index} exhausted.")

                    # If all API keys are exhausted, stop the scraper gracefully
                    if len(exhausted_api_indexes) >= len(youtube_clients):
                        print("[YouTube API] All API keys exhausted. Stopping scraper.")
                        save_all_state_periodically()
                        send_slack_message("⛔ Scraper stopped: all YouTube API keys exhausted.")
                        sys.exit(0)

                    # Move to the next non-exhausted API key
                    for _ in range(len(youtube_clients)):
                        api_index = (api_index + 1) % len(youtube_clients)
                        if api_index not in exhausted_api_indexes:
                            break

                    youtube = youtube_clients[api_index]
                    print(f"[YouTube API] Switched to API key index {api_index}")

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
                    rating = 7  # Static rating; OpenAI rating disabled
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