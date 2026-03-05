#!/usr/bin/env python3
"""
Defense Signals™ — The Opportunity Floor
Daily Intelligence Pipeline v1.0

Pulls SAM.gov opportunities, scores them, and generates:
  - opportunity_floor.json       (structured feed for Kajabi widget)
  - daily_email.html             (ActiveCampaign-ready HTML)
  - daily_email.txt              (plain text fallback)
  - linkedin_post.txt            (DS-governed LinkedIn post)
  - daily_article.md             (Signal Room™ daily brief)

ENV VARS REQUIRED:
  SAM_API_KEY      — Your SAM.gov API key
  OUTPUT_DIR       — Output folder path (default: ./output)
  STATE_FILE       — Timestamp persistence file (default: ./state.json)

OPTIONAL:
  WINDOW_HOURS     — Override lookback window in hours (default: 24)
  MAX_ITEMS        — Max items in outputs (default: 20)
  LOG_LEVEL        — DEBUG | INFO | WARNING (default: INFO)
"""

import os
import json
import time
import logging
import datetime as dt
import hashlib
from typing import List, Dict, Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("OpportunityFloor")

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

SAM_API_KEY = os.getenv("SAM_API_KEY", "").strip()
SAM_BASE_URL = os.getenv(
    "SAM_BASE_URL",
    "https://api.sam.gov/opportunities/v2/search"
)
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./output")
STATE_FILE = os.getenv("STATE_FILE", "./state.json")
WINDOW_HOURS = int(os.getenv("WINDOW_HOURS", "24"))
MAX_ITEMS = int(os.getenv("MAX_ITEMS", "20"))
MIN_ITEMS_WARN = 5  # Warn if fewer results than this

# ── Target Agencies ──────────────────────────

TARGET_AGENCIES: Dict[str, List[str]] = {
    "U.S. Army": [
        "army", "department of the army", "acc", "army contracting command",
        "army materiel command", "amc", "amcom", "acc-redstone"
    ],
    "U.S. Navy": [
        "navy", "department of the navy", "navsea", "navair",
        "spawar", "navsup", "nswc", "nuwc", "navfac"
    ],
    "DLA": [
        "defense logistics agency", "dla", "dla energy",
        "dla land and maritime", "dla aviation", "dla troop support"
    ],
    "U.S. Air Force": [
        "air force", "department of the air force", "afmc",
        "aflcmc", "aficc", "afrl", "space force", "ussf"
    ],
    "DHS": [
        "department of homeland security", "dhs", "cisa",
        "cbp", "uscg", "coast guard", "tsa", "fema", "ice"
    ],
}

# ── Target NAICS ─────────────────────────────

TARGET_NAICS: Dict[str, str] = {
    "541330": "Engineering Services",
    "541614": "Process, Physical Distribution, and Logistics Consulting",
    "541512": "Computer Systems Design Services",
    "541519": "Other Computer Related Services",
    "541690": "Other Scientific and Technical Consulting Services",
}

# ── Keywords ─────────────────────────────────

KEYWORDS: List[str] = [
    "logistics", "sustainment", "technical publications", "engineering support",
    "cybersecurity", "zero trust", "ot security", "scada", "ics",
    "industrial control", "network segmentation", "field service",
    "program management", "systems engineering", "technical data",
    "configuration management", "supply chain", "maintenance", "repair",
]

# ── Blocklist ────────────────────────────────

BLOCKLIST: List[str] = [
    "construction", "janitorial", "landscaping", "pest control",
    "food service", "food and beverage", "grounds maintenance",
    "custodial", "mowing", "snow removal", "trash removal",
]

# ── Set-Aside Preferences ────────────────────

PREFERRED_SET_ASIDES: List[str] = [
    "small business", "8(a)", "woman-owned", "wosb", "edwosb",
    "sdvosb", "service-disabled", "hubzone", "8a",
]

# ── Scoring Weights ──────────────────────────

SCORE_AGENCY_MATCH = 30
SCORE_NAICS_MATCH = 25
SCORE_KEYWORD_MATCH = 15
SCORE_SET_ASIDE_MATCH = 10
SCORE_RESPONSE_WINDOW_BONUS = 5
SCORE_BLOCKLIST_PENALTY = -50

RESPONSE_WINDOW_MIN_DAYS = 7
RESPONSE_WINDOW_MAX_DAYS = 21

# ─────────────────────────────────────────────
# HTTP SESSION WITH RETRY
# ─────────────────────────────────────────────

def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

SESSION = build_session()

# ─────────────────────────────────────────────
# STATE MANAGEMENT
# ─────────────────────────────────────────────

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def iso(d: dt.datetime) -> str:
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def sam_date(d: dt.datetime) -> str:
    """SAM.gov date format: MM/DD/YYYY"""
    return d.strftime("%m/%d/%Y")

def load_state() -> Dict[str, Any]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"Could not load state file: {e}. Starting fresh.")
    return {}

def save_state(state: Dict[str, Any]) -> None:
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except OSError as e:
        log.error(f"Failed to save state: {e}")

# ─────────────────────────────────────────────
# SCORING HELPERS
# ─────────────────────────────────────────────

def normalize(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def agency_match(agency_str: str) -> Optional[str]:
    a = normalize(agency_str)
    for bucket, needles in TARGET_AGENCIES.items():
        if any(n in a for n in needles):
            return bucket
    return None

def keyword_count(text: str) -> int:
    t = normalize(text)
    return sum(1 for kw in KEYWORDS if kw in t)

def blocklist_hit(text: str) -> bool:
    t = normalize(text)
    return any(b in t for b in BLOCKLIST)

def set_aside_match(sa: str) -> bool:
    s = normalize(sa)
    return any(p in s for p in PREFERRED_SET_ASIDES)

def response_window_bonus(resp_date_str: str) -> int:
    if not resp_date_str:
        return 0
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            rd = dt.datetime.strptime(resp_date_str[:len(fmt)+2].strip(), fmt)
            rd = rd.replace(tzinfo=dt.timezone.utc)
            days = (rd - now_utc()).days
            return SCORE_RESPONSE_WINDOW_BONUS if RESPONSE_WINDOW_MIN_DAYS <= days <= RESPONSE_WINDOW_MAX_DAYS else 0
        except ValueError:
            continue
    return 0

def compute_score(item: Dict[str, Any]) -> int:
    score = 0
    if agency_match(item.get("agency", "")):
        score += SCORE_AGENCY_MATCH
    if item.get("naics", "") in TARGET_NAICS:
        score += SCORE_NAICS_MATCH
    title_kws = keyword_count(item.get("title", ""))
    if title_kws > 0:
        score += SCORE_KEYWORD_MATCH
        if title_kws > 2:
            score += 5  # Bonus for strong keyword density
    if set_aside_match(item.get("set_aside", "")):
        score += SCORE_SET_ASIDE_MATCH
    if blocklist_hit(item.get("title", "")):
        score += SCORE_BLOCKLIST_PENALTY
    score += response_window_bonus(item.get("response_date", ""))
    return score

def score_label(score: int) -> str:
    if score >= 75:
        return "★★★ High Fit"
    elif score >= 50:
        return "★★ Moderate Fit"
    elif score >= 25:
        return "★ Low Fit"
    else:
        return "⚠ Review"

def build_why_it_matters(item: Dict[str, Any]) -> str:
    """Subscriber-facing signal — no internal references."""
    parts = []
    bucket = agency_match(item.get("agency", ""))
    naics = item.get("naics", "")
    naics_label = TARGET_NAICS.get(naics, "")
    kws = [kw for kw in KEYWORDS if kw in normalize(item.get("title", ""))]
    sa = item.get("set_aside", "")
    rd = item.get("response_date", "")

    # Agency + NAICS combined signal
    if bucket and naics_label:
        parts.append(f"{bucket} buy in {naics_label}")
    elif bucket:
        parts.append(f"{bucket} procurement")
    elif naics_label:
        parts.append(f"{naics_label} opportunity")

    # Keyword signals — translate to plain language
    kw_translations = {
        "logistics": "logistics support",
        "sustainment": "sustainment services",
        "cybersecurity": "cybersecurity scope",
        "zero trust": "Zero Trust architecture",
        "scada": "SCADA/ICS environment",
        "ics": "industrial control systems",
        "ot": "OT environment",
        "engineering support": "engineering support services",
        "technical publications": "technical documentation",
        "network segmentation": "network segmentation requirement",
    }
    translated = [kw_translations.get(kw, kw) for kw in kws[:2]]
    if translated:
        parts.append(f"Scope includes {', '.join(translated)}")

    # Set-aside signal
    if set_aside_match(sa):
        sa_lower = normalize(sa)
        if "woman" in sa_lower or "wosb" in sa_lower or "edwosb" in sa_lower:
            parts.append("WOSB set-aside — restricted competition")
        elif "8(a)" in sa_lower or "8a" in sa_lower:
            parts.append("8(a) set-aside — restricted competition")
        elif "service-disabled" in sa_lower or "sdvosb" in sa_lower:
            parts.append("SDVOSB set-aside — restricted competition")
        else:
            parts.append("Small business set-aside — restricted competition")

    # Response window urgency
    if rd and response_window_bonus(rd) > 0:
        parts.append("Decision window: 7–21 days")

    return ". ".join(parts) + "." if parts else "Review for alignment to your capability statement."

# ─────────────────────────────────────────────
# SAM.GOV API
# ─────────────────────────────────────────────

def sam_search(posted_from: dt.datetime, posted_to: dt.datetime) -> List[Dict[str, Any]]:
    """
    Paginated SAM.gov opportunity search.
    Pulls all pages within the date window.
    """
    if not SAM_API_KEY:
        raise RuntimeError("SAM_API_KEY env var is required.")

    all_results: List[Dict[str, Any]] = []
    page_size = 100
    offset = 0
    headers = {"Accept": "application/json"}

    base_params = {
        "api_key": SAM_API_KEY,
        "postedFrom": sam_date(posted_from),
        "postedTo": sam_date(posted_to),
        "limit": page_size,
        "ptype": "o,p,k,r",       # Solicitation, Presolicitation, Combined, Sources Sought
        "status": "active",
    }

    log.info(f"Querying SAM.gov: {base_params['postedFrom']} → {base_params['postedTo']}")

    while True:
        params = {**base_params, "offset": offset}
        try:
            resp = SESSION.get(SAM_BASE_URL, headers=headers, params=params, timeout=45)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", "10"))
                log.warning(f"Rate limited. Waiting {wait}s…")
                time.sleep(wait)
                continue

            if resp.status_code == 401:
                raise RuntimeError("SAM_API_KEY is invalid or expired.")

            resp.raise_for_status()
            data = resp.json()

        except requests.exceptions.RequestException as e:
            log.error(f"SAM API request failed at offset {offset}: {e}")
            break

        # SAM v2 uses opportunitiesData
        records = (
            data.get("opportunitiesData")
            or data.get("data")
            or []
        )

        if not records:
            log.info(f"No more records at offset {offset}. Total: {len(all_results)}")
            break

        all_results.extend(records)
        log.debug(f"Fetched {len(records)} records at offset {offset}. Running total: {len(all_results)}")

        if len(records) < page_size:
            break

        offset += page_size
        time.sleep(0.3)  # Polite pause between pages

    return all_results

# ─────────────────────────────────────────────
# NORMALIZATION
# ─────────────────────────────────────────────

def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Map SAM v2 field names to Opportunity Floor schema."""

    def pick(*keys, default=""):
        for k in keys:
            v = raw.get(k)
            if v:
                return str(v).strip()
        return default

    # SAM v2 nests some fields
    org = raw.get("organizationHierarchy") or {}
    dept = (
        pick("department", "fullParentPathName")
        or (org.get("cgac", {}) or {}).get("name", "")
    )
    sub = pick("subTier", "subAgency", "office")

    naics_val = pick("naicsCode", "naics")
    # Sometimes comes as list
    if isinstance(raw.get("naicsCode"), list):
        naics_val = str(raw["naicsCode"][0]) if raw["naicsCode"] else ""

    notice_id = pick("noticeId", "id", "opportunityId")
    sol_num = pick("solicitationNumber", "solnbr", "referenceNumber")

    ui_link = pick("uiLink", "link", "resourceLinks")
    if not ui_link and notice_id:
        ui_link = f"https://sam.gov/opp/{notice_id}/view"

    item = {
        "title": pick("title", "solicitationTitle", "subject"),
        "agency": dept,
        "subagency": sub,
        "notice_type": pick("noticeType", "type", "baseType"),
        "naics": naics_val,
        "set_aside": pick("typeOfSetAsideDescription", "setAside", "typeOfSetAside"),
        "posted_date": pick("postedDate", "publishDate", "createdDate"),
        "response_date": pick("responseDeadLine", "responseDate", "dueDate", "archiveDate"),
        "sam_url": ui_link,
        "solicitation_number": sol_num,
        "notice_id": notice_id,
    }

    item["score"] = compute_score(item)
    item["score_label"] = score_label(item["score"])
    item["agency_bucket"] = agency_match(item["agency"]) or item["agency"]
    item["why_it_matters"] = build_why_it_matters(item)
    return item

# ─────────────────────────────────────────────
# FILTERING
# ─────────────────────────────────────────────

def filter_and_rank(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered = []
    for it in items:
        # Hard blocklist filter
        if blocklist_hit(it["title"]):
            log.debug(f"BLOCKLIST: {it['title'][:60]}")
            continue

        # Must have positive score (agency, NAICS, or keyword match)
        if it["score"] <= 0:
            continue

        # NAICS filter: must be target NAICS or have keyword match
        naics_ok = it["naics"] in TARGET_NAICS
        kw_ok = keyword_count(it["title"]) > 0
        if not naics_ok and not kw_ok:
            continue

        filtered.append(it)

    filtered.sort(key=lambda x: x["score"], reverse=True)
    log.info(f"Filtered {len(items)} → {len(filtered)} qualifying opportunities")

    if len(filtered) < MIN_ITEMS_WARN:
        log.warning(f"Low result count ({len(filtered)}). Consider expanding window or NAICS list.")

    return filtered

# ─────────────────────────────────────────────
# RENDERERS
# ─────────────────────────────────────────────

DS_BRAND_COLOR = "#0a1628"
DS_ACCENT_COLOR = "#c8a951"
DS_FONT = "Arial, Helvetica, sans-serif"

def render_email_html(date_label: str, items: List[Dict[str, Any]]) -> str:
    rows = []
    for i, it in enumerate(items[:MAX_ITEMS], 1):
        url = it["sam_url"] or "#"
        bucket = it["agency_bucket"]
        naics = it["naics"] or "N/A"
        naics_label = TARGET_NAICS.get(naics, naics)
        sa = it["set_aside"] or "Full & Open"
        due = it["response_date"] or "See SAM"
        sol = it["solicitation_number"] or it["notice_id"] or ""
        label = it["score_label"]
        why = it["why_it_matters"]

        rows.append(f"""
        <tr style="border-bottom:1px solid #e8e8e8;">
          <td style="padding:14px 8px;vertical-align:top;font-size:13px;color:#888;font-weight:bold;min-width:24px;">{i}</td>
          <td style="padding:14px 8px 14px 0;vertical-align:top;">
            <div style="margin-bottom:4px;">
              <a href="{url}" style="color:{DS_BRAND_COLOR};font-weight:bold;font-size:15px;text-decoration:none;">{it['title']}</a>
            </div>
            <div style="font-size:12px;color:#555;margin-bottom:6px;">
              <strong>{bucket}</strong>
              {(' &nbsp;|&nbsp; ' + it['subagency']) if it['subagency'] else ''}
              &nbsp;|&nbsp; NAICS {naics} – {naics_label}
              &nbsp;|&nbsp; {sa}
            </div>
            <div style="font-size:12px;color:#333;margin-bottom:4px;">
              <strong>Due:</strong> {due}
              {'&nbsp;&nbsp;<strong>Sol#:</strong> ' + sol if sol else ''}
              &nbsp;&nbsp;<span style="background:{DS_ACCENT_COLOR};color:{DS_BRAND_COLOR};padding:2px 7px;border-radius:3px;font-size:11px;font-weight:bold;">{label}</span>
            </div>
            <div style="font-size:12px;color:#666;font-style:italic;">{why}</div>
          </td>
        </tr>""")

    rows_html = "\n".join(rows)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Recently Released RFPs | {date_label}</title>
</head>
<body style="margin:0;padding:0;background:#f4f4f4;font-family:{DS_FONT};">

<table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f4f4;padding:20px 0;">
  <tr><td align="center">
    <table width="640" cellpadding="0" cellspacing="0" style="background:#fff;border-radius:6px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.08);">

      <!-- HEADER -->
      <tr>
        <td style="background:{DS_BRAND_COLOR};padding:28px 32px;">
          <div style="color:{DS_ACCENT_COLOR};font-size:11px;letter-spacing:2px;text-transform:uppercase;margin-bottom:6px;">Defense Signals™ | The Opportunity Floor</div>
          <div style="color:#fff;font-size:22px;font-weight:bold;">Recently Released RFPs</div>
          <div style="color:#aab4c4;font-size:14px;margin-top:4px;">{date_label} &nbsp;·&nbsp; Signal Room™ Edition</div>
        </td>
      </tr>

      <!-- INTRO -->
      <tr>
        <td style="padding:24px 32px 8px;font-size:14px;color:#333;line-height:1.6;">
          Opportunities posted in the last 24 hours across Army, Navy, DLA, Air Force, and DHS—
          filtered and scored for logistics, engineering, and OT cybersecurity alignment.
          Scored by agency fit, NAICS alignment, keyword density, and set-aside preference.
        </td>
      </tr>

      <!-- OPPORTUNITIES -->
      <tr>
        <td style="padding:8px 32px 0;">
          <table width="100%" cellpadding="0" cellspacing="0">
            {rows_html}
          </table>
        </td>
      </tr>

      <!-- CTA -->
      <tr>
        <td style="padding:32px;background:#f9f9f9;border-top:2px solid {DS_ACCENT_COLOR};margin-top:20px;">
          <div style="font-size:15px;font-weight:bold;color:{DS_BRAND_COLOR};margin-bottom:8px;">
            Want a 48-hour capture action plan on any of these?
          </div>
          <div style="font-size:13px;color:#555;margin-bottom:16px;">
            Signal Room™ members receive full scoring breakdowns, incumbent intelligence, and same-day pursuit recommendations.
          </div>
          <a href="https://defensesignals.com/signal-room"
             style="background:{DS_BRAND_COLOR};color:{DS_ACCENT_COLOR};padding:12px 24px;border-radius:4px;font-weight:bold;font-size:14px;text-decoration:none;display:inline-block;">
            Join Signal Room™ →
          </a>
        </td>
      </tr>

      <!-- FOOTER -->
      <tr>
        <td style="padding:20px 32px;border-top:1px solid #eee;">
          <div style="font-size:11px;color:#aaa;line-height:1.6;">
            Defense Signals™ · Huntsville, Alabama<br>
            You're receiving this because you subscribed to The Opportunity Floor.<br>
            <a href="{{{{unsubscribe}}}}" style="color:#aaa;">Unsubscribe</a>
          </div>
        </td>
      </tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""

def render_email_text(date_label: str, items: List[Dict[str, Any]]) -> str:
    lines = [
        "DEFENSE SIGNALS™ | THE OPPORTUNITY FLOOR",
        f"Recently Released RFPs | {date_label}",
        "=" * 60,
        "",
        "Opportunities posted in the last 24 hours. Scored by agency fit,",
        "NAICS alignment, keyword density, and set-aside preference.",
        "",
    ]
    for i, it in enumerate(items[:MAX_ITEMS], 1):
        bucket = it["agency_bucket"]
        naics = it["naics"] or "N/A"
        sa = it["set_aside"] or "Full & Open"
        due = it["response_date"] or "See SAM"
        sol = it["solicitation_number"] or it["notice_id"] or ""
        lines += [
            f"{i}. {it['title']}",
            f"   Agency: {bucket}",
            f"   NAICS: {naics} | Set-Aside: {sa}",
            f"   Due: {due}" + (f" | Sol#: {sol}" if sol else ""),
            f"   Fit: {it['score_label']}",
            f"   Signal: {it['why_it_matters']}",
            f"   Link: {it['sam_url'] or 'https://sam.gov'}",
            "",
        ]
    lines += [
        "─" * 60,
        "Want a 48-hour capture action plan on any of these?",
        "Signal Room™ members receive full scoring, incumbent intel,",
        "and same-day pursuit recommendations.",
        "",
        "Join Signal Room™ → https://defensesignals.com/signal-room",
        "",
        "─" * 60,
        "Defense Signals™ · Huntsville, Alabama",
        "Unsubscribe: {{{unsubscribe}}}",
    ]
    return "\n".join(lines)

def render_linkedin_post(date_label: str, items: List[Dict[str, Any]]) -> str:
    """
    DS-07 LinkedIn governance compliant.
    LINKEDIN_STRATEGIC mode: 70–110 words, 2 paragraphs, no links, no CTA.
    """
    top = items[:5]
    bullets = []
    for it in top:
        bucket = it["agency_bucket"]
        title_short = it["title"][:60] + ("…" if len(it["title"]) > 60 else "")
        naics = it["naics"] or ""
        sa_note = f" [{it['set_aside']}]" if set_aside_match(it.get("set_aside", "")) else ""
        bullets.append(f"· {bucket} — {title_short} (NAICS {naics}){sa_note}")

    bullet_block = "\n".join(bullets)

    post = f"""Federal opportunity activity | {date_label}

{bullet_block}

Logistics, engineering, and OT cybersecurity drove the highest-scoring releases. Set-aside concentration in this window signals continued small business runway in these lanes. Signal Room™ — daily scored feed + capture framing.

#DefenseContracting #FederalAcquisition #SmallBusiness"""

    return post.strip()

def render_article_md(date_label: str, items: List[Dict[str, Any]]) -> str:
    top = items[:10]

    # Agency distribution summary
    from collections import Counter
    agency_counts = Counter(it["agency_bucket"] for it in items if it["agency_bucket"])
    top_agencies_str = ", ".join(f"{k} ({v})" for k, v in agency_counts.most_common(3))

    # Set-aside count
    sa_count = sum(1 for it in items if set_aside_match(it.get("set_aside", "")))

    # NAICS distribution
    naics_counts = Counter(it["naics"] for it in items if it["naics"] in TARGET_NAICS)
    top_naics_str = ", ".join(
        f"{TARGET_NAICS.get(k, k)} ({v})" for k, v in naics_counts.most_common(3)
    )

    opp_rows = []
    for i, it in enumerate(top, 1):
        bucket = it["agency_bucket"]
        naics = it["naics"] or "N/A"
        naics_label = TARGET_NAICS.get(naics, "")
        sa = it["set_aside"] or "Full & Open"
        due = it["response_date"] or "See SAM"
        sol = it["solicitation_number"] or it["notice_id"] or "N/A"
        opp_rows.append(f"""### {i}. {it['title']}

| Field | Value |
|-------|-------|
| Agency | {bucket} |
| NAICS | {naics} – {naics_label} |
| Set-Aside | {sa} |
| Due Date | {due} |
| Sol # | {sol} |
| Fit Score | {it['score_label']} |
| Signal | {it['why_it_matters']} |

[View on SAM.gov]({it['sam_url'] or 'https://sam.gov'})
""")

    opp_block = "\n".join(opp_rows)

    return f"""# The Opportunity Floor™ | {date_label}
*Defense Signals™ | Signal Room™ Edition*

---

## Today's Signal

The floor moved today. **{len(items)} opportunities** posted across {len(agency_counts)} agencies — **{sa_count} of {len(items)}** carry small business or WOSB set-asides. {top_agencies_str or "Multiple agencies"} drove today's volume. {"**" + str(sum(1 for it in items if response_window_bonus(it.get("response_date", "")) > 0)) + " opportunities close within 21 days** — those move first." if sum(1 for it in items if response_window_bonus(it.get("response_date", "")) > 0) else ""} Review the scored list below and match against your active capability statements.

---

## Top Opportunities

{opp_block}

---

## What to Watch

- **NAICS clustering**: Multiple buys in the same code within a short window often precedes an IDIQ or MAC recompete. Track agency + NAICS pairs, not individual solicitations.
- **OT/ICS language**: Any solicitation with SCADA, ICS, or network segmentation in the title is operating in a constrained vendor pool. Map to your Zero Trust and OT capability statements immediately.
- **Set-aside velocity**: High concentration of small business set-asides in a single agency window suggests an 8(a) or WOSB program office is active. Signal Room™ tracks these patterns week-over-week.
- **Response deadlines under 14 days**: These are often follow-ons or sole-source justifications in process. Pull the synopsis and look for J&A language.

---

## CTA

**Want a 48-hour capture action plan on any of these opportunities?**

Signal Room™ members receive full scoring breakdowns, incumbent intelligence, and same-day pursuit recommendations — delivered daily.

[Join Signal Room™ →](https://defensesignals.com/signal-room)

---
*Generated by Defense Signals™ Opportunity Floor Pipeline · {iso(now_utc())}*
"""

# ─────────────────────────────────────────────
# KAJABI WIDGET
# ─────────────────────────────────────────────

KAJABI_WIDGET_JS = r"""
<!-- ============================================================
     Defense Signals™ | The Opportunity Floor — Kajabi Widget
     Paste this into a Kajabi Custom Code block.
     Set FEED_URL to wherever you host opportunity_floor.json
     ============================================================ -->

<style>
  :root {
    --ds-navy: #0a1628;
    --ds-gold: #c8a951;
    --ds-bg: #f7f8fa;
    --ds-border: #e2e6ea;
    --ds-text: #1a2233;
  }
  #opp-floor-widget {
    font-family: Arial, Helvetica, sans-serif;
    max-width: 900px;
    margin: 0 auto;
  }
  #opp-floor-widget .ds-header {
    background: var(--ds-navy);
    color: #fff;
    padding: 20px 24px;
    border-radius: 6px 6px 0 0;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  #opp-floor-widget .ds-header h2 {
    margin: 0;
    font-size: 18px;
    color: var(--ds-gold);
    letter-spacing: 1px;
  }
  #opp-floor-widget .ds-header .ds-meta {
    font-size: 12px;
    color: #aab4c4;
  }
  #opp-floor-widget .ds-filters {
    background: #fff;
    border: 1px solid var(--ds-border);
    border-top: none;
    padding: 12px 24px;
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
    align-items: center;
  }
  #opp-floor-widget .ds-filters select,
  #opp-floor-widget .ds-filters input {
    padding: 6px 10px;
    border: 1px solid var(--ds-border);
    border-radius: 4px;
    font-size: 13px;
    color: var(--ds-text);
    background: #fff;
  }
  #opp-floor-widget .ds-filters label {
    font-size: 12px;
    color: #555;
    font-weight: bold;
  }
  #opp-floor-widget .ds-table-wrap {
    overflow-x: auto;
    border: 1px solid var(--ds-border);
    border-top: none;
    border-radius: 0 0 6px 6px;
  }
  #opp-floor-widget table {
    width: 100%;
    border-collapse: collapse;
    background: #fff;
    font-size: 13px;
  }
  #opp-floor-widget thead th {
    background: var(--ds-navy);
    color: var(--ds-gold);
    padding: 10px 14px;
    text-align: left;
    font-size: 11px;
    letter-spacing: 1px;
    text-transform: uppercase;
    white-space: nowrap;
  }
  #opp-floor-widget tbody tr:nth-child(even) { background: var(--ds-bg); }
  #opp-floor-widget tbody tr:hover { background: #eef3ff; }
  #opp-floor-widget tbody td {
    padding: 12px 14px;
    color: var(--ds-text);
    border-bottom: 1px solid var(--ds-border);
    vertical-align: top;
  }
  #opp-floor-widget .ds-title a {
    color: var(--ds-navy);
    font-weight: bold;
    text-decoration: none;
  }
  #opp-floor-widget .ds-title a:hover { color: var(--ds-gold); text-decoration: underline; }
  #opp-floor-widget .ds-signal {
    font-size: 11px;
    color: #666;
    font-style: italic;
    margin-top: 4px;
  }
  #opp-floor-widget .ds-score {
    display: inline-block;
    padding: 3px 8px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: bold;
    white-space: nowrap;
  }
  .score-high   { background: #d4edda; color: #155724; }
  .score-med    { background: #fff3cd; color: #856404; }
  .score-low    { background: #f8d7da; color: #721c24; }
  .score-review { background: #e2e3e5; color: #383d41; }
  #opp-floor-widget .ds-empty {
    padding: 32px;
    text-align: center;
    color: #888;
    font-size: 14px;
  }
  #opp-floor-widget .ds-loading {
    padding: 32px;
    text-align: center;
    color: #555;
  }
  #opp-floor-widget .ds-count {
    font-size: 12px;
    color: #aaa;
    padding: 8px 24px;
    background: #fff;
    border: 1px solid var(--ds-border);
    border-top: none;
  }
</style>

<div id="opp-floor-widget">
  <div class="ds-header">
    <h2>THE OPPORTUNITY FLOOR™</h2>
    <div class="ds-meta">Defense Signals™ · Signal Room™</div>
  </div>
  <div class="ds-filters">
    <div>
      <label>Agency</label><br>
      <select id="ds-filter-agency" onchange="dsRender()">
        <option value="">All Agencies</option>
        <option>U.S. Army</option>
        <option>U.S. Navy</option>
        <option>DLA</option>
        <option>U.S. Air Force</option>
        <option>DHS</option>
      </select>
    </div>
    <div>
      <label>NAICS</label><br>
      <select id="ds-filter-naics" onchange="dsRender()">
        <option value="">All NAICS</option>
        <option value="541330">541330 – Engineering</option>
        <option value="541614">541614 – Logistics</option>
        <option value="541512">541512 – Computer Systems</option>
        <option value="541519">541519 – Other IT</option>
        <option value="541690">541690 – Technical Consulting</option>
      </select>
    </div>
    <div>
      <label>Min Score</label><br>
      <input type="number" id="ds-filter-score" value="0" min="0" max="100" style="width:70px;" oninput="dsRender()">
    </div>
    <div>
      <label>Search Title</label><br>
      <input type="text" id="ds-filter-search" placeholder="keyword…" style="width:160px;" oninput="dsRender()">
    </div>
  </div>
  <div class="ds-count" id="ds-count">Loading…</div>
  <div class="ds-table-wrap">
    <div class="ds-loading" id="ds-loading">Loading Opportunity Floor…</div>
    <table id="ds-table" style="display:none;">
      <thead>
        <tr>
          <th>#</th>
          <th>Opportunity</th>
          <th>Agency</th>
          <th>NAICS</th>
          <th>Set-Aside</th>
          <th>Due Date</th>
          <th>Fit Score</th>
        </tr>
      </thead>
      <tbody id="ds-tbody"></tbody>
    </table>
    <div class="ds-empty" id="ds-empty" style="display:none;">No opportunities match your filters.</div>
  </div>
</div>

<script>
(function() {
  // ── CONFIG ──────────────────────────────────────
  const FEED_URL = 'https://YOUR_CDN_OR_HOST/opportunity_floor.json';
  // Replace with your actual hosted JSON URL.
  // Options: Cloudflare R2, S3, Kajabi file host, etc.
  // ────────────────────────────────────────────────

  let DS_DATA = [];

  function scoreClass(label) {
    if (!label) return 'score-review';
    if (label.includes('High'))     return 'score-high';
    if (label.includes('Moderate')) return 'score-med';
    if (label.includes('Low'))      return 'score-low';
    return 'score-review';
  }

  function dsRender() {
    const agency = document.getElementById('ds-filter-agency').value.toLowerCase();
    const naics  = document.getElementById('ds-filter-naics').value;
    const minS   = parseInt(document.getElementById('ds-filter-score').value) || 0;
    const search = document.getElementById('ds-filter-search').value.toLowerCase();

    const filtered = DS_DATA.filter(it => {
      if (agency && !(it.agency_bucket || '').toLowerCase().includes(agency)) return false;
      if (naics  && it.naics !== naics) return false;
      if ((it.score || 0) < minS) return false;
      if (search && !(it.title || '').toLowerCase().includes(search)) return false;
      return true;
    });

    const tbody = document.getElementById('ds-tbody');
    tbody.innerHTML = '';

    if (filtered.length === 0) {
      document.getElementById('ds-table').style.display = 'none';
      document.getElementById('ds-empty').style.display = 'block';
    } else {
      document.getElementById('ds-empty').style.display = 'none';
      document.getElementById('ds-table').style.display = 'table';

      filtered.forEach((it, i) => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td style="color:#999;font-size:12px;">${i + 1}</td>
          <td class="ds-title">
            <a href="${it.sam_url || '#'}" target="_blank" rel="noopener">${it.title || '—'}</a>
            ${it.why_it_matters ? `<div class="ds-signal">${it.why_it_matters}</div>` : ''}
          </td>
          <td>${it.agency_bucket || it.agency || '—'}</td>
          <td>${it.naics || '—'}</td>
          <td>${it.set_aside || 'Full & Open'}</td>
          <td style="white-space:nowrap;">${(it.response_date || 'See SAM').substring(0, 10)}</td>
          <td>
            <span class="ds-score ${scoreClass(it.score_label)}">${it.score_label || '—'}</span>
          </td>`;
        tbody.appendChild(tr);
      });
    }

    document.getElementById('ds-count').textContent =
      `Showing ${filtered.length} of ${DS_DATA.length} opportunities · Last updated: ${window._DS_GENERATED || '…'}`;
  }

  async function dsLoad() {
    try {
      const resp = await fetch(FEED_URL, { cache: 'no-cache' });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const feed = await resp.json();
      DS_DATA = (feed.items || []);
      window._DS_GENERATED = feed.generated_at || '';
      document.getElementById('ds-loading').style.display = 'none';
      dsRender();
    } catch(e) {
      document.getElementById('ds-loading').textContent =
        'Unable to load opportunities. Please try again later.';
      console.error('Opportunity Floor load error:', e);
    }
  }

  dsLoad();
})();
</script>
"""

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── Determine time window ──────────────────
    state = load_state()
    last_run = state.get("last_run_utc")
    window_end = now_utc()

    if last_run:
        try:
            window_start = dt.datetime.fromisoformat(last_run.replace("Z", "+00:00"))
            log.info(f"Resuming from last run: {last_run}")
        except ValueError:
            window_start = window_end - dt.timedelta(hours=WINDOW_HOURS)
    else:
        window_start = window_end - dt.timedelta(hours=WINDOW_HOURS)
        log.info(f"No prior state found. Using {WINDOW_HOURS}h lookback.")

    log.info(f"Window: {iso(window_start)} → {iso(window_end)}")

    # ── Fetch ──────────────────────────────────
    raw_records = sam_search(window_start, window_end)
    log.info(f"Raw records from SAM.gov: {len(raw_records)}")

    # ── Normalize + Filter + Rank ──────────────
    normalized = [normalize_record(r) for r in raw_records]
    final = filter_and_rank(normalized)[:MAX_ITEMS]

    if not final:
        log.warning("Zero qualifying opportunities after filtering. Check NAICS/agency/keyword config.")

    # ── Date label for outputs ─────────────────
    try:
        date_label = dt.datetime.now().strftime("%-m/%-d/%y")   # Linux
    except ValueError:
        date_label = dt.datetime.now().strftime("%#m/%#d/%y")   # Windows fallback

    # ── Fingerprint ────────────────────────────
    fp_input = json.dumps(final, sort_keys=True).encode()
    fingerprint = hashlib.sha256(fp_input).hexdigest()[:8]

    # ── Output: JSON feed ──────────────────────
    feed = {
        "generated_at": iso(window_end),
        "fingerprint": fingerprint,
        "window_start": iso(window_start),
        "window_end": iso(window_end),
        "filters": {
            "agencies": list(TARGET_AGENCIES.keys()),
            "naics": list(TARGET_NAICS.keys()),
            "keywords": KEYWORDS,
            "blocklist": BLOCKLIST,
        },
        "summary": {
            "total_raw": len(raw_records),
            "total_qualifying": len(normalized),
            "total_published": len(final),
        },
        "items": final,
    }

    def write(fname, content, mode="w"):
        path = os.path.join(OUTPUT_DIR, fname)
        with open(path, mode, encoding="utf-8") as f:
            if fname.endswith(".json"):
                json.dump(content, f, indent=2)
            else:
                f.write(content)
        log.info(f"Wrote: {path}")

    write("opportunity_floor.json", feed)
    write("daily_email.html", render_email_html(date_label, final))
    write("daily_email.txt", render_email_text(date_label, final))
    write("linkedin_post.txt", render_linkedin_post(date_label, final))
    write("daily_article.md", render_article_md(date_label, final))
    write("kajabi_widget.html", KAJABI_WIDGET_JS)

    # ── Persist state ──────────────────────────
    state["last_run_utc"] = iso(window_end)
    state["last_fingerprint"] = fingerprint
    state["last_item_count"] = len(final)
    save_state(state)

    print(f"\n✓ Opportunity Floor complete.")
    print(f"  Raw: {len(raw_records)} | Qualifying: {len(normalized)} | Published: {len(final)}")
    print(f"  Fingerprint: {fingerprint}")
    print(f"  Outputs → {OUTPUT_DIR}/\n")

if __name__ == "__main__":
    main()
