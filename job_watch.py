#!/usr/bin/env python3
"""
job_watch.py — a job radar with three polling tiers.

LAYER 1 — Direct ATS polling (Greenhouse, Lever, Ashby, SmartRecruiters,
Workable, Recruitee). These are your hand-picked target companies. Each has
a public, unauthenticated JSON feed that IS the source — no aggregation
lag. This layer runs every cycle (see job-watch.yml, every 15 min) and is
why you'll see a posting minutes after it goes up, not hours or days later.

LAYER 1B — Workday polling (NVIDIA, Qualcomm, Aptiv, Northrop Grumman, Blue
Origin, Boston Dynamics). Workday's job-search endpoint is public and
unauthenticated the same way the Layer 1 ATSs are, but it's search-based
(several POST requests per company per poll) rather than one cheap GET, so
it runs on its own, slower throttle instead of every cycle. See section 1b
for the mechanics.

LAYER 2 — Broad aggregators (Adzuna, RemoteOK, Remotive, HN "Who is
Hiring?"), for coverage beyond your hand-picked list. These are legitimate,
publicly documented, consent-based APIs — companies and job boards publish
to them on purpose for third-party reuse. This is deliberately NOT built to
scrape LinkedIn or Indeed: both explicitly prohibit automated scraping in
their terms of service, scraping them is a real legal exposure for whoever
runs the scraper, and their anti-bot measures make it fragile even before
the legal question. Layer 2 is throttled per-source to match each
provider's own rate-limit/ToS guidance (see comments on each fetcher) —
it will not hit them every 15 minutes even though the workflow runs that
often.

All three tiers share the same keyword filter, eligibility filters
(citizenship/clearance/PR, US-only location), and salary-floor logic, so a
match is a match regardless of where it came from.

Realistic freshness: Layer 1 postings are typically visible within
minutes. Layer 1b (Workday) lags by however long WORKDAY_MIN_HOURS_BETWEEN_
POLLS is set to. Layer 2 postings depend on the aggregator's own ingestion
pipeline (Adzuna and Remotive both pull from other sources with their own
lag), but between the polling cadence here and each provider's stated
refresh behavior, that comfortably lands within a 24-hour window, which was
the target.
"""

import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import urllib.error

# ---------------------------------------------------------------------------
# 1. TARGET COMPANIES (Layer 1 — direct ATS polling)
#
# ats:   "greenhouse" | "lever" | "ashby" | "smartrecruiters" | "workable" | "recruitee"
# token: the slug from the company's careers URL:
#          boards-api.greenhouse.io/v1/boards/{token}  -> ats="greenhouse"
#            (the public HTML board itself now lives at job-boards.greenhouse.io/{token} —
#            some companies have started rotating that token to a random,
#            non-guessable string as an anti-scraping measure. If a
#            greenhouse entry below starts 404ing, that's almost certainly
#            why; check the company's current /careers link for the new
#            token rather than assuming the API endpoint moved.)
#          jobs.lever.co/{token}                -> ats="lever"
#          jobs.ashbyhq.com/{token}             -> ats="ashby"
#          jobs.smartrecruiters.com/{token}     -> ats="smartrecruiters"
#          apply.workable.com/{token}           -> ats="workable"
#          {token}.recruitee.com                -> ats="recruitee"
#        Not on one of these six, and not on Workday either (see
#        WORKDAY_COMPANIES in section 1b below)? Layer 2 is your best
#        coverage for it; there's no clean feed for this script to poll
#        directly. Notably iCIMS (AMD, Joby Aviation) has no consistent
#        public JSON feed across companies the way the six above do.
# ---------------------------------------------------------------------------
COMPANIES = [
    # --- Autonomous vehicles / robotaxi -------------------------------
    {"name": "Waymo",              "ats": "greenhouse", "token": "waymo"},
    {"name": "Zoox",               "ats": "lever",       "token": "zoox"},
    {"name": "Aurora Innovation",  "ats": "greenhouse", "token": "aurorainnovation"},
    {"name": "Nuro",               "ats": "greenhouse", "token": "nuro"},

    # --- Autonomous trucking / delivery -------------------------------
    {"name": "Kodiak Robotics",    "ats": "greenhouse", "token": "kodiak"},
    {"name": "Waabi",              "ats": "lever",       "token": "waabi"},

    # --- Defense autonomy / drones / maritime ----------------------------
    {"name": "Anduril",            "ats": "greenhouse", "token": "andurilindustries"},
    # Skydio moved off Greenhouse to Ashby at some point — this is the fix
    # for the 404 you hit. Confirmed live at jobs.ashbyhq.com/skydio.
    {"name": "Skydio",             "ats": "ashby",       "token": "skydio"},
    {"name": "Saronic",            "ats": "lever",       "token": "saronic"},
    {"name": "Shield AI",          "ats": "lever",       "token": "shieldai"},
    # {"name": "Epirus",            "ats": "???", "token": "???"},   # TODO — couldn't confirm an ATS for this one

    # --- Humanoid / general-purpose robotics ----------------------------
    {"name": "Figure AI",          "ats": "greenhouse", "token": "figureai"},
    {"name": "Apptronik",          "ats": "greenhouse", "token": "apptronik"},
    {"name": "Skild AI",           "ats": "greenhouse", "token": "skildai-careers"},
    {"name": "1X Technologies",    "ats": "recruitee",   "token": "1x"},
    {"name": "Physical Intelligence", "ats": "ashby",    "token": "physicalintelligence"},
    # Boston Dynamics is on Workday, not one of the six GET-based ATSs —
    # see WORKDAY_COMPANIES below instead of here.

    # --- Simulation / vehicle software tooling ---------------------------
    # Applied Intuition rotated its public Greenhouse board token to a
    # long random string (job-boards.greenhouse.io/co58owxt...), which is
    # the other 404 you hit — this reads as a deliberate anti-scraping
    # move on their end, not a broken/moved feed, so it's disabled here
    # rather than hardcoding a token that's designed to be replaced.
    # Layer 2 (Adzuna/Remotive/HN) is the fallback for their postings.
    # {"name": "Applied Intuition", "ats": "greenhouse", "token": "???"},  # TODO — token rotates

    # --- Space / launch ----------------------------------------------------
    {"name": "Relativity Space",   "ats": "greenhouse", "token": "relativity"},  # verify on first run
    {"name": "Astranis",           "ats": "greenhouse", "token": "astranis"},
    # {"name": "Stoke Space",       "ats": "???", "token": "???"},   # TODO — couldn't confirm an ATS for this one

    # --- Custom career sites — Layer 1 can't reach these; Layer 2 covers
    # the gap. Left here as a visible reminder, not a working entry:
    # SpaceX, Joby Aviation (iCIMS), AMD (iCIMS/Jibe — checked; no public
    # feed), Rivian/RV Tech, Lockheed Martin (mid-migration to a new
    # careers platform as of this writing — worth re-checking later), and
    # Tesla (fully custom site — the old comment here guessed Workday;
    # checked, that guess was wrong, so removed from the Workday list too).
]

# ---------------------------------------------------------------------------
# 1b. WORKDAY COMPANIES (Layer 1b — throttled polling, not every 15 min)
#
# Workday's job-search API (the "CXS" endpoint) is a public, unauthenticated
# POST endpoint the same way Greenhouse/Lever/etc. are GET endpoints — it's
# just structured differently (search-based, not a single "list everything"
# call) and undocumented/unofficial rather than a published API. It's what
# NVIDIA, Qualcomm, Aptiv, Northrop Grumman, Blue Origin, and Boston
# Dynamics all run on, which covers a big chunk of "high-paying, matches my
# skills" that Greenhouse/Lever/Ashby simply don't reach.
#
# Because each company needs several search-term requests per poll (see
# fetch_workday_company below) rather than one cheap GET, this runs on its
# own throttle (WORKDAY_MIN_HOURS_BETWEEN_POLLS) instead of every cycle —
# same reasoning as Layer 2, different mechanism.
#
# tenant / wd_host / site come from the company's careers URL:
#   https://{tenant}.{wd_host}.myworkdayjobs.com/{site}
# e.g. https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite
#        -> tenant="nvidia", wd_host="wd5", site="NVIDIAExternalCareerSite"
# ---------------------------------------------------------------------------
WORKDAY_COMPANIES = [
    {"name": "NVIDIA",           "tenant": "nvidia",          "wd_host": "wd5", "site": "NVIDIAExternalCareerSite"},
    # Qualcomm's old Workday instance (qualcomm.wd5/wd12) now shows a
    # "we've moved sites" banner pointing to careers.qualcomm.com, and its
    # CXS API returns 422 on every search — consistent with the instance
    # being decommissioned post-migration, not a request-format bug here
    # (NVIDIA/Aptiv/etc. use the identical request shape and work fine).
    # careers.qualcomm.com looks like a fully custom/JS-rendered site;
    # couldn't identify the underlying ATS from the page source. Disabled
    # until that's confirmed rather than hammering a dead endpoint.
    # {"name": "Qualcomm",       "tenant": "qualcomm",        "wd_host": "wd5", "site": "External"},
    {"name": "Aptiv",            "tenant": "aptiv",           "wd_host": "wd5", "site": "APTIV_CAREERS"},
    {"name": "Northrop Grumman", "tenant": "ngc",             "wd_host": "wd1", "site": "Northrop_Grumman_External_Site"},
    {"name": "Blue Origin",      "tenant": "blueorigin",      "wd_host": "wd5", "site": "BlueOrigin"},
    {"name": "Boston Dynamics",  "tenant": "bostondynamics",  "wd_host": "wd1", "site": "Boston_Dynamics"},
]
WORKDAY_MIN_HOURS_BETWEEN_POLLS = 3
# 2. KEYWORDS — matched case-insensitively, on word boundaries (see
# KEYWORD_PATTERN below), against title + full description.
#
# Split into two groups purely for readability/maintenance; both are
# searched together (see ALL_KEYWORDS). CONTROLS_GNC_KEYWORDS is the
# original high-level controls/autonomy list. EMBEDDED_KEYWORDS was pulled
# from your resume (STM32 / embedded-Linux / sensor-fusion stack) and the
# RV Tech mid-point technical record (AUTOSAR / SIL-HIL / vehicle-bus
# stack) — extend it as your own skill set grows.
# ---------------------------------------------------------------------------
CONTROLS_GNC_KEYWORDS = [
    "controls engineer", "gnc", "guidance, navigation", "guidance and control",
    "simulation engineer", "simulation software", "hil", "hardware-in-the-loop",
    "hardware in the loop", "sil", "software-in-the-loop", "autonomy engineer",
    "autonomy software", "embedded controls", "flight software", "vehicle software",
    "robot learning", "digital twin", "sensor fusion", "state estimation",
    "kalman", "extended kalman filter", "ekf", "complementary filter",
    "model-based design", "mbd", "motion planning", "planning & controls",
    "planning and controls", "perception engineer", "autonomy platform",
    "ins/gnss", "visual-inertial odometry", "6-dof state estimation",
    "ros2", "ros 2", "dds middleware", "mavlink",
]

EMBEDDED_KEYWORDS = [
    # RTOS / bare-metal / firmware fundamentals
    "rtos", "freertos", "real-time operating system", "bare-metal", "bare metal",
    "bootloader", "watchdog timer", "interrupt service routine", "isr",
    "device driver", "board support package", "firmware", "firmware engineer",
    "embedded c++", "embedded c", "embedded systems engineer",
    "embedded software engineer", "safety-critical firmware", "microcontroller",

    # Vehicle / automotive bus protocols
    "can bus", "canbus", "controller area network", "can-fd", "can fd",
    "lin bus", "local interconnect network", "flexray", "automotive ethernet",
    "j1939", "vehicle network",

    # Automotive software standards & tooling
    "autosar", "iso 26262", "asil", "functional safety", "fusa",
    "canalyzer", "canape", "dbc file", "xcp protocol", "uds protocol",
    "unified diagnostic services", "zonal controller", "body control module",
    "requirements traceability", "jama connect",

    # Microcontrollers / low-level hardware
    "stm32", "arm cortex-m", "dma controller", "uart driver", "spi driver",
    "i2c driver", "pwm control",

    # Motor control / actuation
    "bldc", "brushless dc motor", "motor controller firmware", "esc firmware",
    "closed-loop motor control",
]

ALL_KEYWORDS = CONTROLS_GNC_KEYWORDS + EMBEDDED_KEYWORDS

# Compiled once. \b boundaries matter here: without them, short/ambiguous
# tokens like "isr" or "hil" would match inside unrelated words, and — the
# reason this moved off plain substring matching — a bare "can" or "lin"
# would match almost every job description in English. Every keyword above
# is safe as a whole word/phrase now; no more manual trailing-space tricks
# needed (the old "hil "/"sil " entries are gone for the same reason).
KEYWORD_PATTERN = re.compile(
    r'\b(?:' + '|'.join(sorted((re.escape(k) for k in ALL_KEYWORDS), key=len, reverse=True)) + r')\b',
    re.IGNORECASE,
)

# A short high-signal subset for aggregators that charge per-query or rate
# limit hard (Adzuna). Keep this tight — every extra term widens recall and
# narrows precision on APIs you can't call often.
CORE_KEYWORDS_FOR_AGGREGATORS = [
    "controls engineer", "GNC engineer", "guidance navigation control",
    "simulation engineer", "autonomy engineer", "embedded controls",
    "sensor fusion engineer", "robot learning", "firmware engineer",
    "embedded software engineer",
]

# ---------------------------------------------------------------------------
# 3. SALARY FLOOR — best-effort filter, not a hard gate. See prior version's
# reasoning: skip only when a published range's TOP is clearly below floor;
# keep everything else, labeled.
# ---------------------------------------------------------------------------
MIN_BASE_SALARY = 200_000

_SALARY_RE = re.compile(
    r'\$\s*([\d]{2,3}(?:,\d{3})?)(?:\.\d{2})?\s*([kK])?\s*(?:-|–|to|and)\s*'
    r'\$?\s*([\d]{2,3}(?:,\d{3})?)(?:\.\d{2})?\s*([kK])?'
)


def extract_salary_range(text):
    los, his = [], []
    for lo_raw, lo_k, hi_raw, hi_k in _SALARY_RE.findall(text or ""):
        lo = float(lo_raw.replace(",", ""))
        hi = float(hi_raw.replace(",", ""))
        if lo_k or lo < 1000:
            lo *= 1000
        if hi_k or hi < 1000:
            hi *= 1000
        if 30_000 <= lo <= 900_000 and 30_000 <= hi <= 900_000 and lo <= hi:
            los.append(lo)
            his.append(hi)
    if not los:
        return None
    return (min(los), max(his))


def clears_floor(salary_low_high, structured_min=None, structured_max=None):
    """Return False only when we're confident the role tops out below the
    floor. structured_min/max (from an API's own salary fields, e.g. Adzuna)
    take priority over regex-extracted text when both are present."""
    if structured_max is not None:
        return structured_max >= MIN_BASE_SALARY
    if salary_low_high is not None:
        return salary_low_high[1] >= MIN_BASE_SALARY
    return True  # unknown salary — don't punish it, just label it unknown


# ---------------------------------------------------------------------------
# 3b. ELIGIBILITY FILTERS — citizenship/clearance/PR requirements, and
# US-only location. Unlike the salary floor, these are hard gates: a role
# you're not eligible for shouldn't reach a notification at all, so both
# checks default to EXCLUDING when the signal is ambiguous rather than
# letting it through unlabeled the way clears_floor() does.
# ---------------------------------------------------------------------------

# Phrases that indicate the role requires citizenship, permanent residency,
# or a security clearance. Also covers ITAR/EAR "U.S. Person" language,
# which functionally requires the same thing (citizen or green-card
# holder) — extremely common in defense-adjacent postings (Anduril, Skydio,
# Saronic, etc.).
CITIZENSHIP_CLEARANCE_EXCLUSIONS = [
    "u.s. citizen", "us citizen", "united states citizen", "american citizen",
    "citizenship is required", "citizenship required", "must be a citizen",
    "must be a u.s. citizen", "u.s. citizenship required", "proof of citizenship",
    "permanent resident", "green card holder", "green card required",
    "lawful permanent resident", "must be a permanent resident",
    "security clearance", "secret clearance", "top secret clearance",
    "ts/sci", "ts-sci", "active clearance", "clearance required",
    "obtain and maintain a security clearance", "obtain a security clearance",
    "eligible to obtain a security clearance", "eligible for a security clearance",
    "u.s. person", "us person status", "must qualify as a u.s. person",
    "itar", "ear99", "export control regulations require",
]

_ELIGIBILITY_PATTERN = re.compile(
    r'\b(?:' + '|'.join(sorted((re.escape(k) for k in CITIZENSHIP_CLEARANCE_EXCLUSIONS), key=len, reverse=True)) + r')\b',
    re.IGNORECASE,
)


def requires_excluded_status(job):
    """True if the posting mentions a citizenship/PR/clearance requirement."""
    haystack = f"{job['title']} {job['content']}"
    return _ELIGIBILITY_PATTERN.search(haystack) is not None


# US-based-only location check. Best-effort like everything else that reads
# free-text location fields, but treated as a hard gate per the "all jobs
# must be US-based" requirement: unconfirmed locations are EXCLUDED, not
# passed through — the opposite default from clears_floor(). If you'd
# rather see unconfirmed-location postings than risk losing real ones,
# switch the "unknown -> exclude" branch below to "unknown -> include" and
# watch the skipped_location_unconfirmed counter to judge how often it
# fires.
US_STATE_ABBR = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
}
US_STATE_NAMES = [
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
    "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york",
    "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode island", "south carolina", "south dakota",
    "tennessee", "texas", "utah", "vermont", "virginia", "washington",
    "west virginia", "wisconsin", "wyoming",
]
US_MARKER_RE = re.compile(
    # (?!\w) instead of a trailing \b: literals like "u.s." end in a
    # non-word character, and \b right after a non-word char only holds if
    # a word char follows — which is never true at end-of-string/whitespace.
    r'\b(?:usa|u\.s\.a?\.?|united states|remote\s*[-,(]?\s*(?:us|usa))(?!\w)',
    re.IGNORECASE,
)
STATE_ABBR_RE = re.compile(r',\s*([A-Za-z]{2})\b')


def _classify_text_us(text):
    """Run the US/non-US heuristics against a single blob of text. Returns
    True/False/None; shared by the location field and the title+content
    fallback so both get the same ruleset."""
    if not text:
        return None
    if US_MARKER_RE.search(text):
        return True
    m = STATE_ABBR_RE.search(text)
    if m and m.group(1).upper() in US_STATE_ABBR:
        return True
    text_lower = text.lower()
    if any(state in text_lower for state in US_STATE_NAMES):
        return True
    if _NON_US_PATTERN.search(text):
        return False
    return None

NON_US_MARKERS = [
    "canada", "mexico", "united kingdom", " uk ", "england", "scotland",
    "wales", "northern ireland", "ireland", "germany", "france", "spain",
    "italy", "netherlands", "belgium", "switzerland", "poland", "portugal",
    "sweden", "norway", "denmark", "finland", "austria", "greece",
    "india", "pakistan", "bangladesh", "philippines", "vietnam",
    "singapore", "malaysia", "indonesia", "thailand", "china", "japan",
    "south korea", "taiwan", "hong kong", "australia", "new zealand",
    "brazil", "argentina", "chile", "colombia", "nigeria", "kenya",
    "south africa", "egypt", "israel", "uae", "dubai", "saudi arabia",
    "emea", "apac", "latam", "europe", "worldwide", "anywhere in the world",
]
_NON_US_PATTERN = re.compile(
    r'\b(?:' + '|'.join(sorted((re.escape(k.strip()) for k in NON_US_MARKERS), key=len, reverse=True)) + r')\b',
    re.IGNORECASE,
)


def classify_us_location(job):
    """Return True (confirmed US), False (confirmed non-US), or None
    (unconfirmed). Checks the structured location field first; if that's
    blank or inconclusive, falls back to title + the first 500 chars of
    content — useful for sources like HN "Who is Hiring" that have no
    structured location field and instead put it in the posting text
    itself (kept short deliberately, since scanning deep into a long
    description raises the odds of a stray ", XX"-shaped false positive)."""
    location = job.get("location", "") or ""
    result = _classify_text_us(location)
    if result is not None:
        return result

    fallback_text = f"{job.get('title', '')} {job.get('content', '')[:500]}"
    return _classify_text_us(fallback_text)


# ---------------------------------------------------------------------------
# 4. NOTIFICATION — ntfy.sh (zero signup).
# ---------------------------------------------------------------------------
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "")
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "seen_jobs.json")

# ---------------------------------------------------------------------------
# 4b. PERSISTENT LOG — Supabase, for the jobs.html "for later" page.
#
# Deliberately NOT a git commit (see job-watch.yml comments for why: a bot
# committing to the branch you also develop on WILL eventually collide with
# your own pushes). This is a plain HTTP POST to your Supabase project's
# REST API instead — same project your roadmap page already talks to, if
# you want to keep everything in one place. Requires SUPABASE_URL and
# SUPABASE_SERVICE_KEY (the service_role key, NOT the anon key — this needs
# to bypass row-level security to insert; the service_role key must only
# ever live in a GitHub secret, never in client-side HTML). Run
# supabase_setup.sql once before this will have anywhere to write to.
# ---------------------------------------------------------------------------
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")


def log_to_supabase(job, source_label, salary_label):
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return  # not configured — silently skip, notifications still work fine without this
    payload = json.dumps({
        "id": job["id"],
        "title": job["title"],
        "company": job.get("company", ""),
        "location": job.get("location", ""),
        "url": job["url"],
        "source": source_label,
        "salary_label": salary_label,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/job_watch_matches?on_conflict=id",
        data=payload,
        method="POST",
        headers={
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates",
        },
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        # Broad on purpose — same lesson as notify(): a raw socket
        # TimeoutError isn't a urllib.error.URLError and would otherwise
        # escape uncaught. Never let a Supabase hiccup take down the run.
        print(f"supabase log failed for {job['id']}: {e}", file=sys.stderr)


def http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "job-watch/2.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def strip_html(text):
    return re.sub(r"<[^>]+>", " ", text or "")


# ---------------------------------------------------------------------------
# 5a. LAYER 1 FETCHERS — direct ATS feeds. High confidence; field names
# verified against live documentation and real responses.
# ---------------------------------------------------------------------------
def fetch_greenhouse(token):
    data = http_get_json(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true")
    return [{
        "id": f"greenhouse:{token}:{j['id']}",
        "title": j.get("title", ""),
        "location": (j.get("location") or {}).get("name", ""),
        "url": j.get("absolute_url", ""),
        "content": j.get("content", "") or "",
        "salary_min": None, "salary_max": None,
    } for j in (data.get("jobs") or [])]


def fetch_lever(token):
    data = http_get_json(f"https://api.lever.co/v0/postings/{token}?mode=json")
    return [{
        "id": f"lever:{token}:{j.get('id', '')}",
        "title": j.get("text", ""),
        "location": (j.get("categories") or {}).get("location", ""),
        "url": j.get("hostedUrl", ""),
        "content": (j.get("descriptionPlain", "") or "") + " " + (j.get("description", "") or ""),
        "salary_min": None, "salary_max": None,
    } for j in data]


def fetch_ashby(token):
    data = http_get_json(f"https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=true")
    out = []
    # NB: Ashby returns these as an explicit JSON null when unset, not a
    # missing key — `.get(key, default)` only falls back on a MISSING key,
    # so `data.get("jobs", [])` still evaluates to None when the key is
    # present with a null value. `(data.get("jobs") or [])` catches both
    # cases. This was the actual cause of the "'NoneType' object is not
    # iterable" failures (compensationTierSummary is null on postings
    # without disclosed pay bands, which is common outside CA/NY/CO).
    for j in (data.get("jobs") or []):
        comp = j.get("compensation") or {}
        tier_summary = comp.get("compensationTierSummary") or []
        comp_str = " ".join(str(t) for t in tier_summary)
        out.append({
            "id": f"ashby:{token}:{j.get('id', '')}",
            "title": j.get("title", ""),
            "location": j.get("location", ""),
            "url": j.get("jobUrl", ""),
            "content": (j.get("descriptionPlain") or j.get("description") or "") + " " + comp_str,
            "salary_min": None, "salary_max": None,
        })
    return out


def fetch_smartrecruiters(token):
    # Best-effort — see note in previous version. List endpoint often lacks
    # full description text; verify on first run for your target companies.
    data = http_get_json(f"https://api.smartrecruiters.com/v1/companies/{token}/postings")
    out = []
    for j in (data.get("content") or []):
        loc = j.get("location") or {}
        loc_str = ", ".join(filter(None, [loc.get("city"), loc.get("region"), loc.get("country")]))
        jad = j.get("jobAd") if isinstance(j.get("jobAd"), dict) else {}
        desc = ((jad.get("sections") or {}).get("jobDescription") or {}).get("text", "")
        out.append({
            "id": f"smartrecruiters:{token}:{j.get('id', '')}",
            "title": j.get("name", ""),
            "location": loc_str,
            "url": j.get("applyUrl") or j.get("ref", ""),
            "content": desc,
            "salary_min": None, "salary_max": None,
        })
    return out


def fetch_workable(token):
    # Best-effort, same caveat as SmartRecruiters.
    data = http_get_json(f"https://apply.workable.com/api/v1/widget/accounts/{token}")
    out = []
    for j in (data.get("jobs") or []):
        loc = j.get("location") or {}
        loc_str = ", ".join(filter(None, [loc.get("city"), loc.get("country")]))
        out.append({
            "id": f"workable:{token}:{j.get('shortcode') or j.get('id', '')}",
            "title": j.get("title", ""),
            "location": loc_str,
            "url": j.get("url") or j.get("shortlink", ""),
            "content": j.get("description", "") or "",
            "salary_min": None, "salary_max": None,
        })
    return out


def fetch_recruitee(token):
    # https://{token}.recruitee.com/api/offers/ — public, unauthenticated,
    # no key. New addition alongside the original five (1X Technologies
    # runs on this, not Greenhouse/Lever/Ashby/SmartRecruiters/Workable).
    data = http_get_json(f"https://{token}.recruitee.com/api/offers/")
    out = []
    for j in (data.get("offers") or []):
        loc_str = j.get("location") or ", ".join(
            filter(None, [j.get("city"), j.get("state_code") or j.get("state"), j.get("country")])
        )
        content = " ".join(filter(None, [j.get("description", ""), j.get("requirements", "")]))
        out.append({
            "id": f"recruitee:{token}:{j.get('id', '')}",
            "title": j.get("title", ""),
            "location": loc_str,
            "url": j.get("careers_url") or j.get("careers_apply_url", ""),
            "content": strip_html(content),
            "salary_min": None, "salary_max": None,
        })
    return out


ATS_FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "smartrecruiters": fetch_smartrecruiters,
    "workable": fetch_workable,
    "recruitee": fetch_recruitee,
}

# ---------------------------------------------------------------------------
# 5a-bis. WORKDAY FETCHER — see WORKDAY_COMPANIES (section 1b) for why this
# is a separate, throttled code path instead of living in ATS_FETCHERS.
#
# Workday's job-search endpoint is search-based, not "list everything," so
# this runs one POST per term in CORE_KEYWORDS_FOR_AGGREGATORS (reused
# rather than maintaining a second near-duplicate keyword list) and
# de-duplicates results by externalPath. Each search is capped at one page
# (limit=20) — Workday ranks by relevance, and a niche technical term
# rarely has more than 20 genuinely relevant hits at a single company;
# raising this trades more requests for more recall.
#
# This is an unofficial-but-widely-used pattern (the same one several
# commercial job-scraping tools rely on), not a documented Workday API —
# verify the field names still match on your first run. If Workday changes
# the response shape, this fails the same way every other fetcher does
# here: logged to stderr, the rest of the run continues.
# ---------------------------------------------------------------------------
def fetch_workday_company(company):
    tenant, wd_host, site = company["tenant"], company["wd_host"], company["site"]
    base = f"https://{tenant}.{wd_host}.myworkdayjobs.com/wday/cxs/{tenant}/{site}"
    out = []
    seen_paths = set()
    for term in CORE_KEYWORDS_FOR_AGGREGATORS:
        body = json.dumps({"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": term}).encode("utf-8")
        req = urllib.request.Request(
            base + "/jobs", data=body, method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "job-watch/2.0"},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            # Broad and per-term on purpose: one bad search term shouldn't
            # cost you the other terms for this company, same philosophy
            # as the try/except around each Layer 1 company in main().
            print(f"workday search {term!r} failed for {company['name']}: {e}", file=sys.stderr)
            continue
        for p in (data.get("jobPostings") or []):
            path = p.get("externalPath", "")
            if not path or path in seen_paths:
                continue
            seen_paths.add(path)
            bullets = " ".join(p.get("bulletFields", []) or [])
            out.append({
                "id": f"workday:{tenant}:{path}",
                "title": p.get("title", ""),
                "location": p.get("locationsText", "") or p.get("primaryLocation", ""),
                "url": f"https://{tenant}.{wd_host}.myworkdayjobs.com/{site}{path}",
                # List-view only, no per-job description fetch — see the
                # section 1b comment on why. bulletFields often includes
                # the pay range chip on CA/NY/CO-transparency postings,
                # which is what lets extract_salary_range still find it.
                "content": f"{p.get('title', '')} {bullets}",
                "salary_min": None, "salary_max": None,
            })
        time.sleep(0.3)  # polite gap between per-term searches against the same tenant
    return out

# ---------------------------------------------------------------------------
# 5b. LAYER 2 FETCHERS — broad, legitimate aggregators. Each throttled to
# respect its own rate limits / ToS, independent of how often the workflow
# runs. Coverage beyond your hand-picked company list.
# ---------------------------------------------------------------------------

def fetch_adzuna():
    # https://developer.adzuna.com — free tier, registration required.
    # Requires ADZUNA_APP_ID / ADZUNA_APP_KEY. Rate limits apply on the free
    # tier (check your dashboard after registering); throttled to 2x/day
    # by the caller. salary_min and max_days_old are enforced SERVER-SIDE
    # here — Adzuna does the floor filtering for us on this source.
    app_id = os.environ.get("ADZUNA_APP_ID", "")
    app_key = os.environ.get("ADZUNA_APP_KEY", "")
    if not app_id or not app_key:
        print("skip Adzuna: ADZUNA_APP_ID/ADZUNA_APP_KEY not set", file=sys.stderr)
        return []
    what_or = " ".join(CORE_KEYWORDS_FOR_AGGREGATORS)
    params = {
        "app_id": app_id, "app_key": app_key,
        "what_or": what_or,
        "salary_min": str(MIN_BASE_SALARY),
        "sort_by": "date",
        "max_days_old": "2",
        "results_per_page": "50",
        "content-type": "application/json",
    }
    url = "https://api.adzuna.com/v1/api/jobs/us/search/1?" + urllib.parse.urlencode(params)
    data = http_get_json(url)
    out = []
    for j in (data.get("results") or []):
        company = (j.get("company") or {}).get("display_name", "")
        loc = (j.get("location") or {}).get("display_name", "")
        smin, smax = j.get("salary_min"), j.get("salary_max")
        predicted = bool(j.get("salary_is_predicted"))
        out.append({
            "id": f"adzuna:{j.get('id', '')}",
            "title": j.get("title", ""),
            "company": company,
            "location": loc,
            "url": j.get("redirect_url", ""),
            "content": j.get("description", "") or "",
            "salary_min": smin, "salary_max": smax,
            "salary_predicted": predicted,
        })
    return out


def fetch_remoteok():
    # https://remoteok.com/api — free, no key. ToS requires crediting
    # RemoteOK and linking directly to their job URL (done via the `url`
    # field below, unmodified) — keep that if you change this function.
    data = http_get_json("https://remoteok.com/api")
    out = []
    for j in data:
        if "id" not in j or "position" not in j:
            continue  # first element is a legal/metadata blob, not a job
        tags = " ".join(j.get("tags", []) or [])
        out.append({
            "id": f"remoteok:{j['id']}",
            "title": j.get("position", ""),
            "company": j.get("company", ""),
            "location": j.get("location", ""),
            "url": j.get("url", ""),
            "content": (j.get("description", "") or "") + " " + tags,
            "salary_min": j.get("salary_min"), "salary_max": j.get("salary_max"),
        })
    return out


def fetch_remotive():
    # https://remotive.com/api/remote-jobs — free, no key currently
    # required for basic queries. Remotive's own ToS asks for at most ~4
    # calls/day and states their data is already delayed ~24h from the
    # original posting by design — throttled hard by the caller (every 6h)
    # to respect that. Remote-only, so relevance to onsite-heavy hardware
    # roles is limited; included for the software/simulation-adjacent
    # remote roles that do exist.
    data = http_get_json("https://remotive.com/api/remote-jobs?search=controls%20OR%20simulation%20OR%20autonomy%20OR%20robotics")
    out = []
    for j in (data.get("jobs") or []):
        salary_text = j.get("salary", "") or ""
        extracted = extract_salary_range(salary_text)
        if extracted:
            smin, smax = extracted
        out.append({
            "id": f"remotive:{j.get('id', '')}",
            "title": j.get("title", ""),
            "company": j.get("company_name", ""),
            "location": j.get("candidate_required_location", ""),
            "url": j.get("url", ""),
            "content": strip_html(j.get("description", "")) + " " + salary_text,
            "salary_min": smin, "salary_max": smax,
        })
    return out


def fetch_hn_whoishiring():
    # Hacker News' monthly "Ask HN: Who is hiring?" thread via the public,
    # keyless Algolia HN Search API (hn.algolia.com) — legitimate,
    # documented, built for exactly this kind of use. High signal for
    # small/YC-adjacent companies that don't run a big ATS at all.
    try:
        latest = http_get_json(
            "https://hn.algolia.com/api/v1/search?tags=story,author_whoishiring&hitsPerPage=1"
        )
        hits = latest.get("hits") or []
        if not hits:
            return []
        story_id = hits[0]["objectID"]
    except Exception as e:
        print(f"HN: could not find latest Who is Hiring thread: {e}", file=sys.stderr)
        return []

    comments = http_get_json(
        f"https://hn.algolia.com/api/v1/search_by_date?tags=comment,story_{story_id}&hitsPerPage=500"
    )
    out = []
    for c in (comments.get("hits") or []):
        # Only top-level comments are postings; replies are discussion.
        if str(c.get("parent_id")) != str(story_id):
            continue
        text = strip_html(c.get("comment_text", ""))
        if not text.strip():
            continue
        first_line = text.strip().split("\n")[0][:80]
        out.append({
            "id": f"hn:{c.get('objectID', '')}",
            "title": first_line,
            "location": "",
            "url": f"https://news.ycombinator.com/item?id={c.get('objectID', '')}",
            "content": text,
            "salary_min": None, "salary_max": None,
        })
    return out


AGGREGATOR_FETCHERS = {
    # name: (fetch_fn, min_hours_between_calls)
    "adzuna": (fetch_adzuna, 12),
    "remoteok": (fetch_remoteok, 1),
    "remotive": (fetch_remotive, 6),
    "hn_whoishiring": (fetch_hn_whoishiring, 1),
}


def matches_keywords(job):
    haystack = f"{job['title']} {job['content']}"
    return KEYWORD_PATTERN.search(haystack) is not None


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def should_run(state, key, min_interval_hours):
    last = state.get(f"lastrun:{key}")
    if last is None:
        return True
    return (time.time() - last) / 3600.0 >= min_interval_hours


def mark_run(state, key):
    state[f"lastrun:{key}"] = time.time()


MAX_NOTIFICATIONS_PER_RUN = 25  # hard safety valve — see notify() and process_jobs()
_notif_count = 0


def notify(title, body, url):
    global _notif_count
    if not NTFY_TOPIC:
        print(f"[NOTIFY - set NTFY_TOPIC to actually send] {title}\n{body}\n{url}\n")
        return
    if _notif_count >= MAX_NOTIFICATIONS_PER_RUN:
        print(f"notify capped at {MAX_NOTIFICATIONS_PER_RUN} for this run, skipping: {title}", file=sys.stderr)
        return
    req = urllib.request.Request(
        f"https://ntfy.sh/{NTFY_TOPIC}",
        data=body.encode("utf-8"),
        headers={"Title": title.encode("utf-8"), "Click": url, "Priority": "high", "Tags": "briefcase"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        # Deliberately broad: a raw socket TimeoutError (not a URLError) is
        # exactly what crashed the whole run last time. One failed push must
        # never take the rest of the run down with it.
        print(f"notify failed: {e}", file=sys.stderr)
    finally:
        _notif_count += 1
        time.sleep(1.0)  # stay well under ntfy.sh's per-visitor rate limit


def process_jobs(jobs, source_label, state, counters):
    state_key = f"seen:{source_label}"
    is_cold_start = state_key not in state  # this source has never been polled before
    seen_ids = set(state.get(state_key, []))
    current_ids = set()
    for job in jobs:
        current_ids.add(job["id"])
        if job["id"] in seen_ids:
            continue
        if is_cold_start:
            # Everything currently open looks "new" on a first poll — that's
            # not the same thing as "just posted." Silently adopt it as the
            # baseline so only genuinely new postings from here on trigger
            # anything. This is what was missing before: the first-ever run
            # (or the first run after adding a new company) tried to notify
            # on every open req across every source all at once.
            continue
        if not matches_keywords(job):
            continue

        if requires_excluded_status(job):
            counters["skipped_citizenship_clearance"] += 1
            continue

        us_status = classify_us_location(job)
        if us_status is False:
            counters["skipped_non_us"] += 1
            continue
        if us_status is None:
            counters["skipped_location_unconfirmed"] += 1
            continue

        extracted = extract_salary_range(job["content"])
        ok = clears_floor(extracted, job.get("salary_min"), job.get("salary_max"))
        if not ok:
            counters["skipped_below_floor"] += 1
            continue

        if job.get("salary_min") is not None and job.get("salary_max") is not None:
            tag = " (predicted)" if job.get("salary_predicted") else ""
            salary_label = f"${int(job['salary_min']):,}-${int(job['salary_max']):,}{tag}"
        elif extracted:
            salary_label = f"${int(extracted[0]):,}-${int(extracted[1]):,}"
        else:
            salary_label = "salary not listed"

        company = job.get("company", "")
        display_title = f"{company}: {job['title']}" if company else job["title"]

        notify(title=display_title, body=f"{job['location']} | {salary_label} | via {source_label}", url=job["url"])
        log_to_supabase(job, source_label, salary_label)
        counters["new_matches"] += 1

    if is_cold_start and current_ids:
        counters["baseline_established"] = counters.get("baseline_established", 0) + 1
    state[state_key] = list(current_ids)


def main():
    state = load_state()
    counters = {
        "new_matches": 0,
        "skipped_below_floor": 0,
        "skipped_citizenship_clearance": 0,
        "skipped_non_us": 0,
        "skipped_location_unconfirmed": 0,
        "baseline_established": 0,
    }

    # --- Layer 1: direct ATS polling — every run, no throttle ---
    for company in COMPANIES:
        name, ats, token = company["name"], company["ats"], company["token"]
        fetcher = ATS_FETCHERS.get(ats)
        if fetcher is None or token in ("???", None, ""):
            print(f"skip {name}: not configured yet", file=sys.stderr)
            continue
        try:
            jobs = fetcher(token)
            for job in jobs:
                job["company"] = name
            process_jobs(jobs, f"{ats}:{token}", state, counters)
        except Exception as e:
            # Broad on purpose: one company's feed acting up (bad token, ATS
            # migration, timeout, or anything inside process_jobs/notify
            # that still somehow got through) must never take the rest of
            # the run down with it.
            print(f"failed for {name} ({ats}:{token}): {e}", file=sys.stderr)
        save_state(state)  # incremental — a later crash won't erase this company's progress
        time.sleep(0.5)

    # --- Layer 1b: Workday polling — throttled, unlike Layer 1 above.
    # Each company here costs several POST requests (one per search term)
    # instead of one cheap GET, so it runs on WORKDAY_MIN_HOURS_BETWEEN_POLLS
    # rather than every cycle. See section 1b for why these companies
    # aren't just in COMPANIES with the other five ATSs.
    for company in WORKDAY_COMPANIES:
        label = f"workday:{company['tenant']}:{company['site']}"
        if not should_run(state, label, WORKDAY_MIN_HOURS_BETWEEN_POLLS):
            continue
        try:
            jobs = fetch_workday_company(company)
            for job in jobs:
                job["company"] = company["name"]
            process_jobs(jobs, label, state, counters)
            mark_run(state, label)
        except Exception as e:
            print(f"failed for {company['name']} ({label}): {e}", file=sys.stderr)
        save_state(state)
        time.sleep(0.5)

    # --- Layer 2: broad aggregators — throttled per source ---
    for label, (fetcher, min_hours) in AGGREGATOR_FETCHERS.items():
        if not should_run(state, label, min_hours):
            continue
        try:
            jobs = fetcher()
            process_jobs(jobs, label, state, counters)
            mark_run(state, label)
        except Exception as e:
            print(f"failed for aggregator {label}: {e}", file=sys.stderr)
        save_state(state)
        time.sleep(0.5)

    print(f"done. {counters['new_matches']} new matching posting(s), "
          f"{counters['skipped_below_floor']} skipped (below salary floor), "
          f"{counters['skipped_citizenship_clearance']} skipped (citizenship/PR/clearance required), "
          f"{counters['skipped_non_us']} skipped (confirmed non-US location), "
          f"{counters['skipped_location_unconfirmed']} skipped (location unconfirmed), "
          f"{counters['baseline_established']} source(s) established a fresh baseline "
          f"(no notifications sent for those — that's expected on a first run).")


if __name__ == "__main__":
    main()