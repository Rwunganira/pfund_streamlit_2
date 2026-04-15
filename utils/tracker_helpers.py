"""
utils/tracker_helpers.py
=========================
Helper functions for the Management Action Tracker page:
  - Date parsing / normalization
  - DB schema bootstrap
  - Seed data definition and insertion
"""

import re
from datetime import date, datetime
from typing import Optional

import streamlit as st
from sqlalchemy import text

from utils.db import _get_engine


# ── Constants ──────────────────────────────────────────────────────────────────

STATUS_OPTIONS    = ["Pending", "In Progress", "Completed", "Overdue", "Blocked"]
PRIORITY_OPTIONS  = ["High", "Medium", "Low"]
ENTITY_OPTIONS    = ["WHO", "UNICEF", "RBC", "MoH", "CHAI", "FAO", "NRL", "Multi-Agency"]
CATEGORY_OPTIONS  = [
    "Procurement & Logistics",
    "Training & Capacity",
    "M&E",
    "Finance & Admin",
    "Knowledge & Governance",
]

STATUS_COLORS = {
    "Pending":     "#f39c12",
    "In Progress": "#3498db",
    "Completed":   "#2ecc71",
    "Overdue":     "#e74c3c",
    "Blocked":     "#9b59b6",
}

# ── Seed data ──────────────────────────────────────────────────────────────────

SEED_ACTIONS = [
    {
        "challenge":            "Implementation delays due to pending specifications, import licences, and approvals",
        "action":               "Fast-track finalization of equipment specifications and import licences",
        "responsible":          "RBC, WHO",
        "implementing_entity":  "RBC",
        "category":             "Procurement & Logistics",
        "timeline_original":    "ASAP",
        "status":               "Pending",
        "priority":             "High",
        "notes":                "",
    },
    {
        "challenge":            "L2TH activity budget shifted to equipment procurement",
        "action":               "MoH to write to WHO; WHO to submit change request to Pandemic Fund",
        "responsible":          "MoH, WHO, RBC",
        "implementing_entity":  "MoH",
        "category":             "Procurement & Logistics",
        "timeline_original":    "Immediate",
        "status":               "Pending",
        "priority":             "High",
        "notes":                "",
    },
    {
        "challenge":            "Tax exemption for procured vehicles not yet obtained",
        "action":               "NRL to submit exemption letter to Rwanda Revenue Authority (RRA)",
        "responsible":          "Kenny / NRL",
        "implementing_entity":  "NRL",
        "category":             "Procurement & Logistics",
        "timeline_original":    "ASAP",
        "status":               "Pending",
        "priority":             "High",
        "notes":                "",
    },
    {
        "challenge":            "Unused UNICEF balance remaining after tablet procurement",
        "action":               "Review reallocation options and confirm alignment with Pandemic Fund policy",
        "responsible":          "UNICEF, Samuel",
        "implementing_entity":  "UNICEF",
        "category":             "Finance & Admin",
        "timeline_original":    "Before reallocation",
        "status":               "Pending",
        "priority":             "Medium",
        "notes":                "",
    },
    {
        "challenge":            "POE staff training on Impuruza system not yet conducted",
        "action":               "Train 73 POE staff on Impuruza; HISP to create organizational units",
        "responsible":          "UNICEF/WHO, HISP",
        "implementing_entity":  "WHO",
        "category":             "Training & Capacity",
        "timeline_original":    "Before year-end",
        "status":               "Pending",
        "priority":             "High",
        "notes":                "",
    },
    {
        "challenge":            "Limited tablet monitoring at health centres",
        "action":               "Integrate tablet monitoring into district hospital supervision visits",
        "responsible":          "RBC, District Hospitals",
        "implementing_entity":  "RBC",
        "category":             "M&E",
        "timeline_original":    "Before next supervision cycle",
        "status":               "Pending",
        "priority":             "Medium",
        "notes":                "",
    },
    {
        "challenge":            "M&E Workshop not yet planned",
        "action":               "Conduct M&E workshop in December at RBC PHEO room",
        "responsible":          "WHO Country Office",
        "implementing_entity":  "WHO",
        "category":             "M&E",
        "timeline_original":    "ASAP",
        "status":               "Pending",
        "priority":             "High",
        "notes":                "",
    },
    {
        "challenge":            "WHO AFRO participation scope and funding not clarified",
        "action":               "Clarify attendance, purpose, and funding for WHO AFRO participation",
        "responsible":          "WHO – Samuel, Dr Lyndah",
        "implementing_entity":  "WHO",
        "category":             "M&E",
        "timeline_original":    "ASAP",
        "status":               "Pending",
        "priority":             "High",
        "notes":                "",
    },
    {
        "challenge":            "Knowledge platform approach not agreed upon",
        "action":               "Convene technical meetings on platform hosting and governance",
        "responsible":          "CHAI",
        "implementing_entity":  "CHAI",
        "category":             "Knowledge & Governance",
        "timeline_original":    "By Feb 2, 2026",
        "status":               "Pending",
        "priority":             "Medium",
        "notes":                "",
    },
    {
        "challenge":            "Dashboard upgrade needed for improved M&E visibility",
        "action":               "Add quarterly plans, milestones, and expenditure tracking to dashboard",
        "responsible":          "M&E / WHO – Samuel",
        "implementing_entity":  "WHO",
        "category":             "M&E",
        "timeline_original":    "By Feb 2, 2026",
        "status":               "Pending",
        "priority":             "Medium",
        "notes":                "",
    },
    {
        "challenge":            "L2TH renovation scope may exceed approved proposal",
        "action":               "Review L2TH renovation scope against approved proposal before procurement",
        "responsible":          "WHO & RBC",
        "implementing_entity":  "WHO",
        "category":             "Procurement & Logistics",
        "timeline_original":    "Before procurement commitment",
        "status":               "Pending",
        "priority":             "High",
        "notes":                "",
    },
    {
        "challenge":            "District-level event verification logistics and fund flow unclear",
        "action":               "Clarify fund flow mechanisms for district-level events",
        "responsible":          "WHO & RBC",
        "implementing_entity":  "WHO",
        "category":             "Finance & Admin",
        "timeline_original":    "By Feb 2, 2026",
        "status":               "Pending",
        "priority":             "Medium",
        "notes":                "",
    },
    {
        "challenge":            "EMR training transition from e-Ubuzima to IT procurement stalled",
        "action":               "Follow up with MoH on e-Ubuzima to IT procurement transition",
        "responsible":          "WHO",
        "implementing_entity":  "WHO",
        "category":             "Training & Capacity",
        "timeline_original":    "By Feb 2, 2026",
        "status":               "Pending",
        "priority":             "Medium",
        "notes":                "",
    },
    {
        "challenge":            "AM Stewardship training for 235 health workers not initiated",
        "action":               "Engage Rwanda Medical Association on RGB certification for stewardship training",
        "responsible":          "WHO/RBC",
        "implementing_entity":  "WHO",
        "category":             "Training & Capacity",
        "timeline_original":    "By Feb 2, 2026",
        "status":               "Pending",
        "priority":             "High",
        "notes":                "",
    },
    {
        "challenge":            "Project visibility and documentation insufficient",
        "action":               "Strengthen success stories and documentation for project visibility",
        "responsible":          "All team",
        "implementing_entity":  "Multi-Agency",
        "category":             "Knowledge & Governance",
        "timeline_original":    "Ongoing",
        "status":               "In Progress",
        "priority":             "Low",
        "notes":                "",
    },
    {
        "challenge":            "Technical Working Group not yet formalized",
        "action":               "Finalize concept note and clarify symposium funding",
        "responsible":          "Dr Leandre",
        "implementing_entity":  "FAO",
        "category":             "Knowledge & Governance",
        "timeline_original":    "Feb 2, 2026",
        "status":               "Pending",
        "priority":             "Medium",
        "notes":                "",
    },
    {
        "challenge":            "Implementing entity balance consolidation incomplete",
        "action":               "Consolidate IE balance reporting and identify funding gaps",
        "responsible":          "Project Lead + All IEs",
        "implementing_entity":  "Multi-Agency",
        "category":             "Finance & Admin",
        "timeline_original":    "End of year",
        "status":               "Pending",
        "priority":             "Medium",
        "notes":                "",
    },
    {
        "challenge":            "Pandemic Fund indicator feedback not yet submitted",
        "action":               "IEs to provide feedback on indicators for portal reporting",
        "responsible":          "Samuel & IE M&E",
        "implementing_entity":  "WHO",
        "category":             "M&E",
        "timeline_original":    "March 6, 2026",
        "status":               "Pending",
        "priority":             "Medium",
        "notes":                "",
    },
]


# ── Date parsing ───────────────────────────────────────────────────────────────

# Ordered list of (regex pattern, resolver callable) pairs.
# Patterns are checked in order; first match wins.
_RELATIVE_PATTERNS: list[tuple[str, object]] = [
    (r"\basap\b|\bimmediate\b|\burgent\b",              lambda: date.today()),
    (r"end\s+of\s+(the\s+)?year|year[\s-]end",          lambda: date(date.today().year, 12, 31)),
    (r"before\s+year[\s-]end|before\s+end\s+of",        lambda: date(date.today().year, 12, 31)),
    (r"\bongoing\b|\bcontinuous\b|\bpermanent\b",        lambda: None),
    (r"before\s+(next\s+)?(coordination\s+meeting"
     r"|supervision(\s+cycle)?|procurement(\s+commitment)?"
     r"|reallocation|end\s+of\s+(the\s+)?year)",        lambda: None),
    (r"\btbd\b|\bn/?a\b",                               lambda: None),
]


def parse_timeline(raw: str) -> Optional[date]:
    """
    Normalise a free-text timeline string into a concrete date (or None).

    Strategy:
    1. Check against the relative-phrase map (regex, case-insensitive).
    2. Strip ordinal suffixes and leading prepositions, then try dateutil fuzzy parse.
    3. Return None if nothing matches.
    """
    if not raw or not raw.strip():
        return None

    cleaned = raw.strip()

    for pattern, resolver in _RELATIVE_PATTERNS:
        if re.search(pattern, cleaned, re.IGNORECASE):
            return resolver()

    # Remove ordinal suffixes: "2nd" → "2", "6th" → "6"
    scrubbed = re.sub(r"(\d+)(st|nd|rd|th)\b", r"\1", cleaned, flags=re.IGNORECASE)
    # Remove leading prepositions: "By Feb 2" → "Feb 2"
    scrubbed = re.sub(r"^\s*(by|before)\s+", "", scrubbed, flags=re.IGNORECASE)

    try:
        from dateutil import parser as dup
        parsed = dup.parse(scrubbed, fuzzy=True, default=datetime(2026, 1, 1))
        return parsed.date()
    except Exception:
        return None


# ── DB schema bootstrap ────────────────────────────────────────────────────────

def ensure_tracker_table() -> None:
    """Create management_action_tracker table if it does not already exist."""
    with _get_engine().begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS management_action_tracker (
                id                  SERIAL PRIMARY KEY,
                challenge           TEXT        NOT NULL,
                action              TEXT        NOT NULL,
                responsible         TEXT,
                implementing_entity VARCHAR(100),
                category            VARCHAR(100),
                timeline_original   TEXT,
                timeline_parsed     DATE,
                status              VARCHAR(50)  NOT NULL DEFAULT 'Pending',
                priority            VARCHAR(20)  NOT NULL DEFAULT 'Medium',
                notes               TEXT,
                created_at          TIMESTAMP    NOT NULL DEFAULT NOW(),
                updated_at          TIMESTAMP    NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_mat_status
                ON management_action_tracker (status)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_mat_entity
                ON management_action_tracker (implementing_entity)
        """))


def seed_tracker_data(force: bool = False) -> int:
    """
    Insert seed rows into management_action_tracker.

    When force=False (default) the function is a no-op if any rows already
    exist.  When force=True the table is truncated first.

    Returns the number of rows inserted.
    """
    with _get_engine().connect() as conn:
        existing = conn.execute(
            text("SELECT COUNT(*) FROM management_action_tracker")
        ).scalar() or 0

    if existing and not force:
        return 0

    with _get_engine().begin() as conn:
        if force:
            conn.execute(
                text("TRUNCATE management_action_tracker RESTART IDENTITY")
            )
        for row in SEED_ACTIONS:
            parsed = parse_timeline(row["timeline_original"])
            conn.execute(text("""
                INSERT INTO management_action_tracker
                    (challenge, action, responsible, implementing_entity,
                     category, timeline_original, timeline_parsed,
                     status, priority, notes)
                VALUES
                    (:challenge, :action, :responsible, :implementing_entity,
                     :category, :timeline_original, :timeline_parsed,
                     :status, :priority, :notes)
            """), {
                **row,
                "timeline_parsed": parsed,
                "notes": row["notes"] or None,
            })

    return len(SEED_ACTIONS)
