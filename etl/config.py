"""
etl/config.py
=============
Single source of truth for all business logic constants and
classification functions used across ETL and the dashboard.

To change a rule (e.g. add a new qualitative stage or strategic area
keyword), edit ONLY this file.
"""

from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# QUALITATIVE STAGE → NUMERIC SCORE
# ──────────────────────────────────────────────────────────────────────────────
QUALITATIVE_SCORE_MAP: dict[str, int] = {
    "not started":  0,
    "planned":      10,
    "initiated":    25,
    "in progress":  50,
    "draft":        60,
    "submitted":    70,
    "under review": 75,
    "validated":    100,
    "finalized":    100,
    "completed":    100,
    "approved":     100,
}


# ──────────────────────────────────────────────────────────────────────────────
# STRATEGIC AREA KEYWORD RULES
# Order matters: first match wins.
# ──────────────────────────────────────────────────────────────────────────────
STRATEGIC_AREA_RULES: list[tuple[str, list[str]]] = [
    (
        "Surveillance",
        [
            "surveillance", "outbreak", "event-based", "event based",
            "detection", "reporting", "epidemiology", "epidemiological",
            "notif", "alert", "signal", "case detection",
            "early warning", "ebs", "ibs",
        ],
    ),
    (
        "Workforce Development",
        [
            "trained", "training", "recruit", "workforce", "staffing",
            "epidemiologist", "mentorship", "mentoring", "capacity building",
            "human resource", "health worker", "staff", "personnel",
            "biostatistician", "field officer",
        ],
    ),
    (
        "Laboratory Systems",
        [
            "laborator", "equipment", "diagnostic", "testing", "specimen",
            "biosafety", "lab complex", "reagent", "pcr", "sequencing",
            "bsl", "culture", "pathogen", "referral lab",
        ],
    ),
    (
        "Policy / Strategy / Governance",
        [
            "strategy", "policy", "guideline", "validation", "framework",
            "plan", "protocol", "legislation", "regulation", "coordination",
            "governance", "ihr", "naphs", "jee", "assessment",
        ],
    ),
    (
        "One Health / Wildlife / Environment",
        [
            "wildlife", "animal", "veterinary", "one health", "ecosystem",
            "zoonot", "environmental", "vector", "entomolog", "livestock",
            "agriculture", "food safety",
        ],
    ),
]

STRATEGIC_AREA_OTHER = "Other / Cross-Cutting"

ALL_STRATEGIC_AREAS: list[str] = (
    [label for label, _ in STRATEGIC_AREA_RULES] + [STRATEGIC_AREA_OTHER]
)

# Achievement category labels and display order
ACHIEVEMENT_ORDER: list[str] = [
    "Completed", "On Track", "At Risk", "Not Started"
]

ACHIEVEMENT_COLORS: dict[str, str] = {
    "Completed":   "#2ecc71",
    "On Track":    "#3498db",
    "At Risk":     "#f39c12",
    "Not Started": "#e74c3c",
}

# ──────────────────────────────────────────────────────────────────────────────
# QUANTITATIVE INDICATOR SUB-TYPE: NUMBER vs PERCENTAGE
# Percentage indicators must never be summed across rows.
# Their "progress" is the actual value itself, not actual/target × 100.
# ──────────────────────────────────────────────────────────────────────────────
PERCENTAGE_KEYWORDS: list[str] = [
    "percentage", "percent", " % ", "%)", "proportion",
    "rate of", "coverage rate", "numerator", "denominator",
    "/ denominator", "calculation: (numerator", "× 100", "x 100",
]


def classify_indicator_subtype(indicator_text: str, definition_text: str = "") -> str:
    """
    Classify a quantitative indicator as 'Number' or 'Percentage'.
    Qualitative indicators should not be passed to this function.

    Rules:
    - If text mentions percentage/proportion/rate keywords → 'Percentage'
    - Otherwise → 'Number'

    Number indicators: progress = (sum_actual / sum_target) × 100  [cumulative]
    Percentage indicators: progress = actual value directly          [point-in-time]
    """
    combined = (
        f"{str(indicator_text or '').lower()} {str(definition_text or '').lower()}"
    )
    if any(kw in combined for kw in PERCENTAGE_KEYWORDS):
        return "Percentage"
    return "Number"


# ──────────────────────────────────────────────────────────────────────────────
# CLASSIFICATION FUNCTIONS
# These are pure Python — no pandas, no Streamlit — so they are unit-testable.
# ──────────────────────────────────────────────────────────────────────────────

def map_strategic_area(indicator_text: str, activity_text: str = "",
                        definition_text: str = "") -> str:
    """Return strategic area label by keyword matching across text fields."""
    combined = " ".join([
        str(indicator_text or ""),
        str(activity_text  or ""),
        str(definition_text or ""),
    ]).lower()
    for label, keywords in STRATEGIC_AREA_RULES:
        if any(kw in combined for kw in keywords):
            return label
    return STRATEGIC_AREA_OTHER


def score_qualitative_stage(stage: str) -> int:
    """Return 0-100 numeric score for a qualitative stage string."""
    return QUALITATIVE_SCORE_MAP.get(str(stage).strip().lower(), 0)


def compute_achievement_category(
    completion_rate: float | None,
    is_qualitative: bool,
    qualitative_score: float | None,
    status_text: str = "",
) -> str:
    """
    Return achievement category string.
    Handles both quantitative (completion_rate) and qualitative (score) indicators.
    """
    if is_qualitative:
        score = qualitative_score if qualitative_score is not None else 0
        # Fall back to status text if score is 0 and status gives a hint
        if score == 0 and status_text:
            sl = status_text.lower()
            if any(w in sl for w in ("complet", "finaliz", "validat")):
                return "Completed"
            if any(w in sl for w in ("progress", "ongoing", "track")):
                return "On Track"
        if score >= 100:
            return "Completed"
        if score >= 70:
            return "On Track"
        if score > 0:
            return "At Risk"
        return "Not Started"
    else:
        if completion_rate is None:
            return "Not Started"
        if completion_rate >= 1.0:
            return "Completed"
        if completion_rate >= 0.70:
            return "On Track"
        if completion_rate > 0:
            return "At Risk"
        return "Not Started"
