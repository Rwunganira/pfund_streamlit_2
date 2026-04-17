"""
views/management_tracker.py
============================
Management Action Tracker — Streamlit dashboard page.

Renders:
  - Summary metrics bar (total, pending, in-progress, completed, overdue, high-priority)
  - Status donut + category bar charts
  - Sidebar filters (status, category, entity, priority, date range)
  - Editable data table with save mechanism
  - Grouped views (by category, by implementing entity)
  - CSV export
  - Add-new-action form
"""

from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from utils.db import _get_engine
from utils.helpers import MOBILE_CSS, show_download_button
from utils.tracker_helpers import (
    CATEGORY_OPTIONS,
    ENTITY_OPTIONS,
    PRIORITY_OPTIONS,
    STATUS_COLORS,
    STATUS_OPTIONS,
    ensure_tracker_table,
    seed_tracker_data,
)


# ── Cached data loader ─────────────────────────────────────────────────────────

@st.cache_data(ttl=60, show_spinner=False)
def _load_tracker() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql(
            """
            SELECT id, challenge, action, responsible, implementing_entity,
                   category, timeline_original, timeline_parsed,
                   status, priority, notes
            FROM management_action_tracker
            ORDER BY id
            """,
            conn,
        )


# ── Save helper ────────────────────────────────────────────────────────────────

_EDITABLE_COLS = [
    "category", "implementing_entity", "responsible",
    "status", "priority", "timeline_parsed", "notes",
]


def _save_changes(original: pd.DataFrame, edited: pd.DataFrame) -> int:
    """
    Persist changed rows to the database.

    Compares editable columns as strings to detect changes (handles NaT/None).
    Returns the number of rows updated.
    """
    orig_cmp  = original[_EDITABLE_COLS].astype(str)
    edit_cmp  = edited[_EDITABLE_COLS].astype(str)
    changed_mask = (orig_cmp != edit_cmp).any(axis=1)
    changed_rows = edited[changed_mask]

    if changed_rows.empty:
        return 0

    with _get_engine().begin() as conn:
        for _, row in changed_rows.iterrows():
            tl = row["timeline_parsed"]
            if pd.isna(tl) or tl is None:
                tl = None
            elif hasattr(tl, "date"):
                tl = tl.date()

            conn.execute(text("""
                UPDATE management_action_tracker
                SET category            = :cat,
                    implementing_entity = :entity,
                    responsible         = :responsible,
                    status              = :status,
                    priority            = :priority,
                    timeline_parsed     = :tl,
                    notes               = :notes,
                    updated_at          = NOW()
                WHERE id = :id
            """), {
                "cat":         row["category"],
                "entity":      row["implementing_entity"],
                "responsible": None if pd.isna(row["responsible"]) else row["responsible"],
                "status":      row["status"],
                "priority":    row["priority"],
                "tl":          tl,
                "notes":       None if pd.isna(row["notes"]) else row["notes"],
                "id":          int(row["id"]),
            })

    _load_tracker.clear()
    return len(changed_rows)


# ── Filter helper ──────────────────────────────────────────────────────────────

def _apply_filters(
    df:         pd.DataFrame,
    statuses:   list[str],
    categories: list[str],
    entities:   list[str],
    priorities: list[str],
    date_start,
    date_end,
) -> pd.DataFrame:
    fdf = df.copy()
    if statuses:
        fdf = fdf[fdf["status"].isin(statuses)]
    if categories:
        fdf = fdf[fdf["category"].isin(categories)]
    if entities:
        fdf = fdf[fdf["implementing_entity"].isin(entities)]
    if priorities:
        fdf = fdf[fdf["priority"].isin(priorities)]
    if date_start:
        has_date = fdf["timeline_parsed"].notna()
        fdf = fdf[~has_date | (fdf["timeline_parsed"].dt.date >= date_start)]
    if date_end:
        has_date = fdf["timeline_parsed"].notna()
        fdf = fdf[~has_date | (fdf["timeline_parsed"].dt.date <= date_end)]
    return fdf


# ── Sub-section renderers ──────────────────────────────────────────────────────

def _render_metrics(df: pd.DataFrame) -> None:
    today    = date.today()
    total    = len(df)
    pending  = int((df["status"] == "Pending").sum())
    in_prog  = int((df["status"] == "In Progress").sum())
    done     = int((df["status"] == "Completed").sum())

    overdue_mask = (
        df["timeline_parsed"].notna()
        & ~df["status"].isin(["Completed", "Blocked"])
        & (df["timeline_parsed"].dt.date < today)
    )
    overdue  = int(overdue_mask.sum())
    high_pri = int((df["priority"] == "High").sum())

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Actions", total)
    c2.metric("Pending",       pending)
    c3.metric("In Progress",   in_prog)
    c4.metric("Completed",     done)
    c5.metric(
        "Overdue", overdue,
        delta=f"-{overdue}" if overdue else None,
        delta_color="inverse",
    )
    c6.metric("High Priority", high_pri)

    if overdue:
        st.warning(f"⚠️  {overdue} action(s) are past their deadline and not yet completed.")


def _render_charts(df: pd.DataFrame) -> None:
    c1, c2 = st.columns(2)

    with c1:
        status_counts = (
            df["status"]
            .value_counts()
            .rename_axis("Status")
            .reset_index(name="Count")
        )
        fig = px.pie(
            status_counts,
            names="Status",
            values="Count",
            title="Actions by Status",
            color="Status",
            color_discrete_map=STATUS_COLORS,
            hole=0.45,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(
            showlegend=False,
            margin=dict(t=45, b=5, l=5, r=5),
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        cat_counts = (
            df["category"]
            .value_counts()
            .rename_axis("Category")
            .reset_index(name="Count")
        )
        fig2 = px.bar(
            cat_counts,
            x="Count",
            y="Category",
            orientation="h",
            title="Actions by Category",
            color="Count",
            color_continuous_scale="Blues",
            text="Count",
        )
        fig2.update_traces(textposition="inside")
        fig2.update_layout(
            coloraxis_showscale=False,
            margin=dict(t=45, b=5, l=5, r=5),
            yaxis_title="",
            xaxis_title="# Actions",
        )
        st.plotly_chart(fig2, use_container_width=True)


def _column_config() -> dict:
    return {
        "id": st.column_config.NumberColumn(
            "ID", disabled=True, width="small"
        ),
        "challenge": st.column_config.TextColumn(
            "Challenge", disabled=True, width="large"
        ),
        "action": st.column_config.TextColumn(
            "Action", disabled=True, width="large"
        ),
        "category": st.column_config.SelectboxColumn(
            "Category", options=CATEGORY_OPTIONS, width="medium"
        ),
        "implementing_entity": st.column_config.SelectboxColumn(
            "Lead Entity", options=ENTITY_OPTIONS, width="small"
        ),
        "status": st.column_config.SelectboxColumn(
            "Status", options=STATUS_OPTIONS, width="small"
        ),
        "priority": st.column_config.SelectboxColumn(
            "Priority", options=PRIORITY_OPTIONS, width="small"
        ),
        "timeline_original": st.column_config.TextColumn(
            "Original Timeline", disabled=True, width="medium"
        ),
        "timeline_parsed": st.column_config.DateColumn(
            "Parsed Date", format="YYYY-MM-DD", width="small"
        ),
        "responsible": st.column_config.TextColumn(
            "Responsible", width="medium"
        ),
        "notes": st.column_config.TextColumn(
            "Notes", width="medium"
        ),
    }


_COL_ORDER = [
    "id", "challenge", "action", "category", "implementing_entity",
    "status", "priority", "timeline_original", "timeline_parsed",
    "responsible", "notes",
]


def _render_table_tab(fdf: pd.DataFrame, full_df: pd.DataFrame) -> None:
    display_df = fdf[_COL_ORDER].reset_index(drop=True)
    original_display = display_df.copy()

    edited = st.data_editor(
        display_df,
        column_config=_column_config(),
        hide_index=True,
        use_container_width=True,
        num_rows="fixed",
        key="tracker_editor",
    )

    btn_col, dl_col = st.columns([1, 4])
    with btn_col:
        if st.button("💾 Save Changes", type="primary"):
            try:
                n = _save_changes(original_display, edited)
                if n:
                    st.success(f"✅ {n} row(s) updated successfully.")
                    st.rerun()
                else:
                    st.info("No changes detected.")
            except Exception as exc:
                st.error(f"Save failed: {exc}")

    with dl_col:
        show_download_button(edited, "management_actions.csv", "⬇️ Export CSV")


def _render_grouped_tab(df: pd.DataFrame, group_col: str) -> None:
    if df.empty:
        st.info("No actions match the current filters.")
        return

    groups = df[group_col].fillna("(unset)").unique()
    for grp in sorted(groups):
        gdf = df[df[group_col].fillna("(unset)") == grp]
        status_parts = [
            f"{s}: **{int((gdf['status'] == s).sum())}**"
            for s in STATUS_OPTIONS
            if (gdf["status"] == s).sum() > 0
        ]
        summary = "  ·  ".join(status_parts)
        header = f"{grp} — {len(gdf)} action(s)   |   {summary}"

        with st.expander(header, expanded=False):
            st.dataframe(
                gdf[["id", "challenge", "action", "status", "priority",
                      "timeline_parsed", "responsible"]].reset_index(drop=True),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "id":               st.column_config.NumberColumn("ID",        width="small"),
                    "challenge":        st.column_config.TextColumn("Challenge",   width="large"),
                    "action":           st.column_config.TextColumn("Action",      width="large"),
                    "status":           st.column_config.TextColumn("Status",      width="small"),
                    "priority":         st.column_config.TextColumn("Priority",    width="small"),
                    "timeline_parsed":  st.column_config.DateColumn("Deadline",    format="YYYY-MM-DD", width="small"),
                    "responsible":      st.column_config.TextColumn("Responsible", width="medium"),
                },
            )


def _render_add_form() -> None:
    with st.expander("➕ Add New Action"):
        with st.form("add_action_form", clear_on_submit=True):
            challenge = st.text_area("Challenge *", height=80,
                                     placeholder="Describe the challenge or gap")
            action    = st.text_area("Action *", height=80,
                                     placeholder="Describe the specific action to take")
            responsible = st.text_input("Responsible Parties",
                                        placeholder="e.g. WHO, Samuel")

            ca, cb = st.columns(2)
            entity   = ca.selectbox("Lead Implementing Entity", ENTITY_OPTIONS)
            category = cb.selectbox("Category", CATEGORY_OPTIONS)

            cc, cd = st.columns(2)
            status   = cc.selectbox("Status",   STATUS_OPTIONS)
            priority = cd.selectbox("Priority", PRIORITY_OPTIONS)

            ce, cf = st.columns(2)
            tl_orig   = ce.text_input("Timeline (original text)",
                                      placeholder="e.g. By Feb 2, 2026")
            tl_parsed = cf.date_input("Parsed Date (optional)", value=None)

            notes = st.text_area("Notes", height=60)

            submitted = st.form_submit_button("Add Action", type="primary")

        if submitted:
            if not challenge.strip() or not action.strip():
                st.error("Challenge and Action are required fields.")
            else:
                try:
                    with _get_engine().begin() as conn:
                        conn.execute(text("""
                            INSERT INTO management_action_tracker
                                (challenge, action, responsible, implementing_entity,
                                 category, timeline_original, timeline_parsed,
                                 status, priority, notes)
                            VALUES
                                (:challenge, :action, :responsible, :entity,
                                 :category, :tl_orig, :tl_parsed,
                                 :status, :priority, :notes)
                        """), {
                            "challenge":  challenge.strip(),
                            "action":     action.strip(),
                            "responsible": responsible.strip() or None,
                            "entity":     entity,
                            "category":   category,
                            "tl_orig":    tl_orig.strip() or None,
                            "tl_parsed":  tl_parsed,
                            "status":     status,
                            "priority":   priority,
                            "notes":      notes.strip() or None,
                        })
                    _load_tracker.clear()
                    st.success("Action added successfully!")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Failed to add action: {exc}")


# ── Main render function ───────────────────────────────────────────────────────

def render_management_tracker() -> None:
    st.html(MOBILE_CSS)
    st.title("📋 Management Action Tracker")
    st.caption("Coordination Meeting — Action Items Follow-up & Status Dashboard")

    # Ensure the table exists and seed on first run
    try:
        ensure_tracker_table()
        seed_tracker_data()
    except Exception as exc:
        st.error(f"Database setup error: {exc}")
        return

    # ── Load data ──────────────────────────────────────────────────────────────
    with st.spinner("Loading actions…"):
        try:
            df = _load_tracker()
        except Exception as exc:
            st.error(f"Failed to load action items: {exc}")
            return

    if df.empty:
        st.warning("No action items found. Use the form below to add some.")
        _render_add_form()
        return

    # Normalise the parsed date column to pandas datetime (keeps NaT for NULLs)
    df["timeline_parsed"] = pd.to_datetime(df["timeline_parsed"], errors="coerce")

    # ── Sidebar filters ────────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("🔍 Filters")
        sel_status   = st.multiselect("Status",   STATUS_OPTIONS,   key="tr_status")
        sel_category = st.multiselect("Category", CATEGORY_OPTIONS, key="tr_cat")
        sel_entity   = st.multiselect("Entity",   ENTITY_OPTIONS,   key="tr_entity")
        sel_priority = st.multiselect("Priority", PRIORITY_OPTIONS, key="tr_priority")
        st.markdown("**Timeline Range**")
        date_start = st.date_input("From", value=None, key="tr_date_start")
        date_end   = st.date_input("To",   value=None, key="tr_date_end")
        st.divider()

        with st.expander("⚙️ Admin"):
            if st.button("🔄 Re-seed Table", use_container_width=True,
                         help="Truncates the table and re-inserts all seed rows"):
                try:
                    n = seed_tracker_data(force=True)
                    _load_tracker.clear()
                    st.success(f"Re-seeded {n} rows.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"Re-seed failed: {exc}")

    fdf = _apply_filters(
        df, sel_status, sel_category, sel_entity, sel_priority,
        date_start, date_end,
    )

    # ── Summary metrics ────────────────────────────────────────────────────────
    _render_metrics(fdf)
    st.divider()

    # ── Charts ─────────────────────────────────────────────────────────────────
    _render_charts(fdf)
    st.divider()

    # ── Tabbed views ───────────────────────────────────────────────────────────
    tab_flat, tab_cat, tab_entity = st.tabs([
        "📄 All Actions",
        "🗂️ By Category",
        "🏢 By Entity",
    ])

    with tab_flat:
        _render_table_tab(fdf, df)

    with tab_cat:
        _render_grouped_tab(fdf, "category")

    with tab_entity:
        _render_grouped_tab(fdf, "implementing_entity")

    st.divider()
    _render_add_form()
