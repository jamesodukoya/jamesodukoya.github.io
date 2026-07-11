#!/usr/bin/env python3
"""
job_watch.py — a two-layer job radar.

LAYER 1 — Direct ATS polling (Greenhouse, Lever, Ashby, SmartRecruiters,
Workable). These are your hand-picked target companies. Each has a public,
unauthenticated JSON feed that IS the source — no aggregation lag. This
layer runs every cycle (see job-watch.yml, every 15 min) and is why you'll
see a posting minutes after it goes up, not hours or days later.

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

Both layers share the same keyword filter and salary-floor logic, so a
match is a match regardless of where it came from.

Realistic freshness: Layer 1 postings are typically visible within minutes.
Layer 2 postings depend on the aggregator's own ingestion pipeline (Adzuna
and Remotive both pull from other sources with their own lag), but between
the polling cadence here and each provider's stated refresh behavior, that
comfortably lands within a 24-hour window, which was the target.
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
# ats:   "greenhouse" | "lever" | "ashby" | "smartrecruiters" | "workable"
# token: the slug from the company's careers URL:
#          boards.greenhouse.io/{token}        -> ats="greenhouse"
#          jobs.lever.co/{token}                -> ats="lever"
#          jobs.ashbyhq.com/{token}             -> ats="ashby"
#          jobs.smartrecruiters.com/{token}     -> ats="smartrecruiters"
#          apply.workable.com/{token}           -> ats="workable"
#        Not on one of these five (a *.myworkdayjobs.com URL, or a fully
#        custom-built page)? Layer 2 is your best coverage for it; there's
#        no clean feed for this script to poll directly.
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
    {"name": "Skydio",             "ats": "greenhouse", "token": "skydio"},
    {"name": "Saronic",            "ats": "lever",       "token": "saronic"},
    # {"name": "Shield AI",         "ats": "???", "token": "???"},   # TODO
    # {"name": "Epirus",            "ats": "???", "token": "???"},   # TODO

    # --- Humanoid / general-purpose robotics ----------------------------
    {"name": "Figure AI",          "ats": "greenhouse", "token": "figureai"},
    {"name": "Apptronik",          "ats": "greenhouse", "token": "apptronik"},
    {"name": "Skild AI",           "ats": "greenhouse", "token": "skildai-careers"},
    # {"name": "1X Technologies",   "ats": "???", "token": "???"},   # TODO
    # {"name": "Physical Intelligence", "ats": "???", "token": "???"}, # TODO
    # {"name": "Boston Dynamics",   "ats": "???", "token": "???"},   # TODO

    # --- Simulation / vehicle software tooling ---------------------------
    {"name": "Applied Intuition",  "ats": "greenhouse", "token": "appliedintuition"},

    # --- Space / launch ----------------------------------------------------
    {"name": "Relativity Space",   "ats": "greenhouse", "token": "relativity"},  # verify on first run
    # {"name": "Stoke Space",       "ats": "???", "token": "???"},   # TODO
    # {"name": "Astranis",          "ats": "???", "token": "???"},   # TODO

    # --- Custom career sites — Layer 1 can't reach these; Layer 2 covers
    # the gap. Left here as a visible reminder, not a working entry:
    # SpaceX, Blue Origin, Joby Aviation (iCIMS), Rivian/RV Tech and
    # Tesla (likely Workday).
]

# ---------------------------------------------------------------------------
# 2. KEYWORDS — matched case-insensitively against title + full description.
# ---------------------------------------------------------------------------
KEYWORDS = [
    "controls engineer", "gnc", "guidance, navigation", "guidance and control",
    "simulation engineer", "simulation software", "hil ", "hardware-in-the-loop",
    "hardware in the loop", "sil ", "software-in-the-loop", "autonomy engineer",
    "autonomy software", "embedded controls", "flight software", "vehicle software",
    "robot learning", "digital twin", "sensor fusion", "state estimation",
    "kalman", "model-based design", "mbd", "motion planning", "planning & controls",
    "planning and controls", "perception engineer", "autonomy platform",
]

# A short high-signal subset for aggregators that charge per-query or rate
# limit hard (Adzuna). Keep this tight — every extra term widens recall and
# narrows precision on APIs you can't call often.
CORE_KEYWORDS_FOR_AGGREGATORS = [
    "controls engineer", "GNC engineer", "guidance navigation control",
    "simulation engineer", "autonomy engineer", "embedded controls",
    "sensor fusion engineer", "robot learning",
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
    } for j in data.get("jobs", [])]


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
    for j in data.get("jobs", []):
        comp = j.get("compensation") or {}
        comp_str = " ".join(str(t) for t in comp.get("compensationTierSummary", []) if isinstance(comp, dict))
        out.append({
            "id": f"ashby:{token}:{j.get('id', '')}",
            "title": j.get("title", ""),
            "location": j.get("location", ""),
            "url": j.get("jobUrl", ""),
            "content": (j.get("descriptionPlain", "") or j.get("description", "") or "") + " " + comp_str,
            "salary_min": None, "salary_max": None,
        })
    return out


def fetch_smartrecruiters(token):
    # Best-effort — see note in previous version. List endpoint often lacks
    # full description text; verify on first run for your target companies.
    data = http_get_json(f"https://api.smartrecruiters.com/v1/companies/{token}/postings")
    out = []
    for j in data.get("content", []):
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
    for j in data.get("jobs", []):
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


ATS_FETCHERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "ashby": fetch_ashby,
    "smartrecruiters": fetch_smartrecruiters,
    "workable": fetch_workable,
}

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
    for j in data.get("results", []):
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
    for j in data.get("jobs", []):
        smin, smax = None, None
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
        hits = latest.get("hits", [])
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
    for c in comments.get("hits", []):
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
    haystack = f"{job['title']} {job['content']}".lower()
    return any(k in haystack for k in KEYWORDS)


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
    counters = {"new_matches": 0, "skipped_below_floor": 0, "baseline_established": 0}

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
          f"{counters['baseline_established']} source(s) established a fresh baseline "
          f"(no notifications sent for those — that's expected on a first run).")


if __name__ == "__main__":
    main()