"""
utils/helpers.py
================
Shared constants, CSS, and small utility functions used across all pages.
"""

import pandas as pd
import streamlit as st

# ── Display colour maps ────────────────────────────────────────────────────────

ACT_STATUS_COLORS = {
    "Completed":   "#2ecc71",
    "In Progress": "#3498db",
    "Planned":     "#95a5a6",
    "On Hold":     "#e67e22",
    "Cancelled":   "#e74c3c",
    "Delayed":     "#f39c12",
}

ACHIEVEMENT_STATUS_LABEL_MAP = {
    "completed":   "Completed",
    "on_track":    "On Track",
    "at_risk":     "At Risk",
    "not_started": "Not Started",
}

QUAL_STAGE_ORDER = [
    "Not Started", "Planned", "Initiated", "In Progress",
    "Draft", "Submitted", "Under Review",
    "Validated", "Finalized", "Completed", "Approved",
]

QUAL_STAGE_COLORS = {
    "Not Started":  "#e74c3c",
    "Planned":      "#e67e22",
    "Initiated":    "#f39c12",
    "In Progress":  "#f1c40f",
    "Draft":        "#2ecc71",
    "Submitted":    "#1abc9c",
    "Under Review": "#3498db",
    "Validated":    "#2980b9",
    "Finalized":    "#27ae60",
    "Completed":    "#27ae60",
    "Approved":     "#27ae60",
}

# ── CSS snippets ───────────────────────────────────────────────────────────────

MOBILE_CSS = """
<style>
/* Stack st.columns() vertically on phones / narrow screens */
@media (max-width: 768px) {
    [data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important;
    }
    [data-testid="column"] {
        min-width: 100% !important;
        flex: 1 1 100% !important;
    }
    [data-testid="metric-container"] {
        margin-bottom: 0.5rem;
    }
    [data-testid="collapsedControl"] {
        top: 0.5rem !important;
    }
    .js-plotly-plot, .plotly {
        width: 100% !important;
    }
    h1 { font-size: 1.4rem !important; }
    h2 { font-size: 1.2rem !important; }
    h3 { font-size: 1.1rem !important; }
    .block-container {
        padding-left: 0.75rem !important;
        padding-right: 0.75rem !important;
    }
}
/* On tablets (768–1024px), allow 2-wide grids but not 4- or 5-wide */
@media (min-width: 769px) and (max-width: 1024px) {
    [data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important;
    }
    [data-testid="column"] {
        min-width: 45% !important;
        flex: 1 1 45% !important;
    }
}
</style>
"""

AUTH_CSS = """
<style>
[data-testid="stMain"] > div:first-child {
    max-width: 480px;
    margin: 0 auto;
    padding-top: 2rem;
}
</style>
"""

# ── Utility functions ──────────────────────────────────────────────────────────

def safe_mean(s: pd.Series) -> float:
    s = s.dropna()
    return float(s.mean()) if not s.empty else 0.0


def show_download_button(df: pd.DataFrame, filename: str,
                         label: str = "⬇️ Download CSV") -> None:
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(label=label, data=csv, file_name=filename,
                       mime="text/csv", key=f"dl_{filename}")
