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

1. Music production (Ableton, FL Studio, Logic Pro)
2. Mixing and mastering (professional audio engineering)
3. Singing and vocal technique (range, tone, breath control)
4. Voice acting and character voices
5. Voice deepening and vocal masculinization
6. Accent coaching and accent reduction
7. Podcast production and audio storytelling
8. Sound design (for games, film, podcasts)
9. Beat making (hip-hop, lo-fi, trap production)
10. Guitar mastery (fingerstyle, jazz, classical, shred)
11. Piano technique and musicality (classical, jazz, contemporary)
12. Bass guitar fundamentals (slap, fingerstyle, theory)
13. Drums and rhythm mastery (coordination, speed, groove)
14. Electronic music production (techno, house, drum & bass)
15. Songwriting composition (melody, lyrics, structure)
16. Film scoring fundamentals (orchestration, emotion, timing)
17. Jazz improvisation (scales, patterns, ear training)
18. Music theory for producers (harmony, chord progressions)
19. Vocal harmonization (arranging, stacking, blending)
20. Live performance technique (stage presence, equipment setup)
21. Digital illustration (Procreate, Photoshop, concept art)
22. Character design (games, animation, comics)
23. Portrait drawing (realism, anatomy, likeness)
24. UI/UX design (apps, websites, user experience)
25. Motion graphics (After Effects, kinetic typography)
26. 3D modeling and rendering (Blender, Cinema 4D)
27. Concept art for entertainment (games, film, books)
28. Storyboarding and visual storytelling
29. Logo design and brand identity
30. Packaging design (consumer products, branding)
31. Typography and lettering (font design, hand lettering)
32. Calligraphy mastery (traditional, modern, brush lettering)
33. Oil painting layering (classical technique, color theory)
34. Watercolor technique (washes, blending, control)
35. Ink illustration (pen work, cross-hatching, comics)
36. Portrait photography editing (retouching, color grading)
37. Landscape painting (composition, atmosphere, light)
38. Abstract art creation (composition, color, expression)
39. Mural painting technique (scaling, outdoor durability)
40. Album cover design (music industry, visual storytelling)
41. Photography for income (portraits, products, weddings)
42. Portrait photography (lighting, posing, connection)
43. Product photography (e-commerce, commercial work)
44. Photo editing mastery (Lightroom, Photoshop retouching)
45. Videography and cinematography (weddings, commercial)
46. Video editing for creators (Premiere, DaVinci Resolve)
47. Color grading (cinematic looks, mood creation)
48. Drone cinematography technique (aerial shots, regulations)
49. Street photography (composition, decisive moment, storytelling)
50. Food photography (styling, lighting, commercial work)
51. Real estate photography (interiors, HDR, virtual tours)
52. Wildlife photography (patience, equipment, tracking)
53. Event photography (weddings, corporate, candids)
54. Fashion photography (posing, lighting, editing)
55. Documentary filmmaking (storytelling, interviewing, ethics)
56. Spanish language mastery (conversational to fluent)
57. French language learning (pronunciation, grammar, culture)
58. Japanese language (conversation, kanji, business Japanese)
59. Mandarin Chinese (tones, characters, business communication)
60. Korean language (Hangul, conversation, K-culture)
61. English as a Second Language (ESL) - advanced fluency
62. American accent training (for non-native speakers)
63. British accent training (RP, professional English)
64. German language learning (grammar, pronunciation, business)
65. Italian language mastery (conversation, culture, pronunciation)
66. Portuguese language (Brazilian vs European, conversation)
67. Arabic language learning (Modern Standard, dialects)
68. Russian language fundamentals (Cyrillic, grammar, pronunciation)
69. Language pronunciation refinement (phonetics, accent reduction)
70. Sign language (ASL, BSL, communication for deaf community)
71. Calisthenics and bodyweight mastery (handstands, muscle-ups, planche)
72. Yoga teacher training (Vinyasa, Ashtanga, Yin)
73. Pilates instruction (mat and reformer techniques)
74. Flexibility and mobility training (splits, contortion basics)
75. Posture correction and alignment (desk workers, gamers)
76. Running form refinement (technique, injury prevention, efficiency)
77. Boxing footwork drills (stance, movement, defense)
78. Martial arts fundamentals (striking, grappling, discipline)
79. Dance instruction (salsa, bachata, hip-hop, contemporary)
80. Movement flow and natural movement (parkour-inspired, functional)
81. Grip strength progression (climbing, lifting, hand health)
82. Olympic weightlifting (snatch, clean & jerk, technique)
83. Powerlifting technique (squat, bench, deadlift form)
84. Kettlebell training (swings, Turkish get-ups, flows)
85. Functional fitness for longevity (mobility + strength for aging)
86. Strength training for older beginners (safe progression, joint health)
87. Mobility for martial artists (kicks, flexibility, injury prevention)
88. Swimming technique refinement (stroke efficiency, breathing, turns)
89. Rock climbing technique (footwork, route reading, strength)
90. Gymnastics foundations (rings, bars, floor work)
91. Breathwork and pranayama (Wim Hof, Buteyko, holotropic)
92. Meditation techniques (Vipassana, Transcendental, mindfulness)
93. Sleep optimization and insomnia solutions (sleep hygiene, routines)
94. Chronic pain management (back pain, neck pain, protocols)
95. Nervous system regulation (polyvagal theory, trauma release)
96. Cold exposure adaptation (ice baths, cold showers, Wim Hof)
97. Intermittent fasting coaching (protocols, meal timing, lifestyle)
98. Gut health and digestive wellness (microbiome, elimination diets)
99. Hormone optimization (thyroid, cortisol, testosterone naturally)
100. Pelvic floor training (pre/postnatal, incontinence, strength)
101. TMJ and jaw tension relief (exercises, awareness, pain management)
102. Biohacking fundamentals (tracking, supplements, optimization)
103. Stress response tracking (HRV, cortisol management, resilience)
104. Sunlight exposure discipline (vitamin D, circadian rhythm)
105. Ice bath routine structuring (protocol, benefits, recovery)
106. Qigong and Tai Chi (energy work, flowing movement)
107. Functional breathing (nasal breathing, CO2 tolerance)
108. Posture rehabilitation (anterior pelvic tilt, rounded shoulders)
109. Mobility training for desk workers (hip flexors, thoracic spine)
110. Walking endurance progression (cardiovascular base, longevity)
111. Dating skills for men (approach, conversation, confidence)
112. Dating skills for women (boundaries, screening, self-worth)
113. Online dating profile optimization (photos, bio, messaging strategy)
114. Texting and messaging strategy (timing, tone, escalation)
115. First date confidence and conversation (topics, presence, connection)
116. Breakup recovery and healing (processing, growth, moving forward)
117. Self-love and relationship with self (worthiness, boundaries, healing)
118. Marriage communication and conflict resolution (NVC, active listening)
119. Attachment theory and secure attachment (anxious, avoidant, healing)
120. Emotional intelligence in relationships (awareness, regulation, empathy)
121. Non-violent communication (NVC) (needs, requests, empathy)
122. Intimacy and connection building (emotional, physical, vulnerability)
123. Long-distance relationship strategies (communication, trust, visits)
124. Masculine leadership in marriage (direction, decisiveness, provision)
125. Feminine polarity expression (receptivity, radiance, energy)
126. Dating standards calibration (values, non-negotiables, filtering)
127. Boundary enforcement practice (saying no, consequences, self-respect)
128. Trust rebuilding dialogues (after betrayal, transparency, time)
129. Conflict resolution coaching (de-escalation, finding solutions)
130. Relationship audit frameworks (health check, areas for growth)
131. Personal finance for millennials (debt payoff, first investments)
132. Real estate investing for beginners (house hacking, first rental)
133. Stock market investing (fundamental analysis, long-term strategy)
134. Dividend investing and passive income (portfolio building, DRIP)
135. Retirement planning for self-employed (SEP IRA, solo 401k, strategies)
136. Tax optimization for W2 employees (deductions, strategies, planning)
137. Credit repair and credit building (score optimization, utilization)
138. Budgeting systems (zero-based, envelope, intentional spending)
139. Salary negotiation and career advancement (asking, positioning, leverage)
140. Financial independence (FIRE) (savings rate, withdrawal strategies)
141. Wealth psychology and money mindset (scarcity, abundance, beliefs)
142. Alternative investments (gold, art, collectibles, diversification)
143. Cash flow stabilization methods (emergency fund, income streams)
144. Debt payoff sprint systems (snowball, avalanche, motivation)
145. Side hustle income streams (service-based, scalable, non-digital)
146. Financial discipline for young earners (lifestyle inflation, investing early)
147. Wealth tracking systems (net worth, assets, progress monitoring)
148. Risk tolerance calibration (investing personality, portfolio allocation)
149. Financial mistake post-mortems (learning, adjusting, growth)
150. Personal asset mapping (inventory, protection, growth strategy)
151. Public speaking and presentation mastery (stage presence, structure)
152. Public speaking anxiety training (exposure, reframing, practice)
153. Storytelling for impact (business, personal brand, emotional arc)
154. Charisma and social confidence (presence, warmth, competence)
155. Debate and persuasive argumentation (logic, rhetoric, rebuttals)
156. Active listening and deep conversation (presence, curiosity, empathy)
157. Sales conversations (consultative, needs-based, closing)
158. Networking and relationship building (genuine connection, follow-up)
159. Personal branding and positioning (differentiation, messaging, consistency)
160. Leadership communication (vision, inspiration, feedback)
161. Voice tonality training (pitch, pace, power, projection)
162. Storytelling compression drills (brevity, impact, memorability)
163. Leadership presence posture (body language, gravitas, authority)
164. Charisma micro-skills (eye contact, smile, energy management)
165. Social dominance calibration (assertiveness without aggression)
166. Memory training and mnemonics (memory palaces, linking systems)
167. Memory palace training (method of loci, recall competitions)
168. Speed reading with comprehension (subvocalization, eye movement)
169. Deep work and focus training (attention span, flow states)
170. Learning optimization (accelerated learning, meta-learning strategies)
171. Critical thinking and logical reasoning (fallacies, analysis, clarity)
172. Creative problem solving (lateral thinking, frameworks, innovation)
173. Habit formation and behavior change (cues, systems, identity)
174. Habit stacking health systems (morning routines, keystone habits)
175. Discipline and willpower training (delayed gratification, consistency)
176. Confidence building and self-esteem (self-talk, evidence, identity)
177. Productivity systems (GTD, time blocking, energy management)
178. Decision journaling systems (tracking, analyzing, improving choices)
179. Visualization and mental rehearsal (sports, performance, outcomes)
180. Emotional regulation training (awareness, distress tolerance, regulation)
181. Emotional detachment discipline (stoicism, perspective, non-reactivity)
182. Focus stamina training (extended concentration, pomodoro progression)
183. ADHD structure routines (non-clinical organization, systems)
184. Logic reasoning exercises (syllogisms, formal logic, clarity)
185. Strategic thinking frameworks (chess, business, life planning)
186. Woodworking and furniture making (joinery, design, finishing)
187. Wood joinery technique (dovetails, mortise & tenon, Japanese joinery)
188. 3D printing design (CAD to functional prints, selling designs)
189. 3D printer calibration systems (troubleshooting, optimization, settings)
190. Knitting and advanced textile techniques (cables, colorwork, garment design)
191. Sewing and pattern making (garment construction, tailoring, alterations)
192. Embroidery (hand and machine, advanced stitches, design)
193. Leatherworking craft (tooling, stitching, bag/wallet making)
194. Blacksmithing basics (forging, tempering, knife making)
195. Knife sharpening craftsmanship (angles, stones, professional edges)
196. Pottery and ceramics (wheel throwing, hand building, glazing)
197. Carpentry finishing technique (sanding, staining, protective coats)
198. Home renovation instruction (drywall, flooring, basic contracting)
199. Automotive repair education (diagnostics, maintenance, troubleshooting)
200. Motorcycle maintenance (routine service, troubleshooting, customization)

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
        for page_num in range(2):
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