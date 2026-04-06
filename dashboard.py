"""
Pandemic Fund — Combined M&E Dashboard
========================================
READ-ONLY display layer. All analytics are pre-computed by the ETL pipeline.

This file contains NO business logic, NO transforms, NO joins.
It only:
  - reads from mart_ tables
  - applies user filters
  - renders KPI cards, charts, and tables

Run ETL first:
    python -m etl.run_etl

Then start the dashboard:
    streamlit run dashboard.py
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import bcrypt
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

# Import display constants from the single source of truth
from etl.config import ACHIEVEMENT_ORDER, ACHIEVEMENT_COLORS

# ──────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Pandemic Fund M&E",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────────────────────
# MOBILE-RESPONSIVE CSS
# ──────────────────────────────────────────────────────────────────────────────
_MOBILE_CSS = """
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
    /* Give metric cards breathing room when stacked */
    [data-testid="metric-container"] {
        margin-bottom: 0.5rem;
    }
    /* Shrink sidebar toggle for easier tapping */
    [data-testid="collapsedControl"] {
        top: 0.5rem !important;
    }
    /* Ensure plotly charts don't overflow */
    .js-plotly-plot, .plotly {
        width: 100% !important;
    }
    /* Tighten header on mobile */
    h1 { font-size: 1.4rem !important; }
    h2 { font-size: 1.2rem !important; }
    h3 { font-size: 1.1rem !important; }
    /* Reduce padding so content fills the screen */
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

# ──────────────────────────────────────────────────────────────────────────────
# SHARED UTILITIES
# ──────────────────────────────────────────────────────────────────────────────

ACT_STATUS_COLORS = {
    "Completed":   "#2ecc71",
    "In Progress": "#3498db",
    "Planned":     "#95a5a6",
    "On Hold":     "#e67e22",
    "Cancelled":   "#e74c3c",
    "Delayed":     "#f39c12",
}


def _get_engine():
    raw_url = os.getenv("DATABASE_URL", "")
    if not raw_url:
        st.error("DATABASE_URL is not set. Add it to your .env file and restart.")
        st.stop()
    db_url = (
        raw_url.replace("postgres://", "postgresql://", 1)
        if raw_url.startswith("postgres://")
        else raw_url
    )
    return create_engine(db_url)


def show_download_button(df: pd.DataFrame, filename: str,
                          label: str = "⬇️ Download CSV") -> None:
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(label=label, data=csv, file_name=filename, mime="text/csv")


def safe_mean(s: pd.Series) -> float:
    s = s.dropna()
    return float(s.mean()) if not s.empty else 0.0


# ──────────────────────────────────────────────────────────────────────────────
# DATA LOADERS  — one SELECT per mart table, nothing else
# ──────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def load_mart_indicator_tracker() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_indicator_tracker", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_indicator_kpis() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_indicator_kpis", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_entity_performance() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_entity_performance", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_strategic_summary() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_strategic_summary", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_budget_performance() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_budget_performance", conn)


@st.cache_data(ttl=300, show_spinner=False)
def load_mart_activity_status() -> pd.DataFrame:
    with _get_engine().connect() as conn:
        return pd.read_sql("SELECT * FROM mart.mart_activity_status", conn)


# ══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 1 — PORTFOLIO ACTIVITIES
# ══════════════════════════════════════════════════════════════════════════════

def render_activities_dashboard() -> None:
    st.html(_MOBILE_CSS)
    st.title("📊 Portfolio M&E Dashboard")
    st.caption("Monitoring & Evaluation — Activities Performance Tracker")

    with st.spinner("Loading…"):
        bud_df    = load_mart_budget_performance()
        status_df = load_mart_activity_status()

    if bud_df.empty:
        st.warning("No budget data found. Run the ETL pipeline first.")
        return

    # ── Sidebar filters ───────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Filters")

        year_opts = sorted(bud_df["year_number"].dropna().unique().tolist())
        sel_years = st.multiselect("Budget Year", options=year_opts,
                                    default=year_opts, key="act_years",
                                    format_func=lambda x: f"Year {x}")
        active_years = sel_years or year_opts

        def _ms(label, col, key):
            opts = sorted(bud_df[col].dropna().unique().tolist())
            return st.multiselect(label, options=opts, key=key)

        sel_entity   = _ms("Implementing Entity", "entity_name",    "act_ent")
        sel_area     = _ms("Results Area",        "results_area",   "act_ra")
        sel_partner  = _ms("Delivery Partner",    "delivery_partner","act_dp")
        sel_category = _ms("Category",            "category",       "act_cat")
        st.divider()
        st.caption("Empty = show all.")

    # ── Apply filters ─────────────────────────────────────────────────────────
    fdf = bud_df[bud_df["year_number"].isin(active_years)].copy()
    if sel_entity:   fdf = fdf[fdf["entity_name"].isin(sel_entity)]
    if sel_area:     fdf = fdf[fdf["results_area"].isin(sel_area)]
    if sel_partner:  fdf = fdf[fdf["delivery_partner"].isin(sel_partner)]
    if sel_category: fdf = fdf[fdf["category"].isin(sel_category)]

    year_label = " + ".join(f"Year {y}" for y in active_years)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Overview",
        "💰 Budget & Performance",
        "📈 Status Analysis",
        "⚠️ Low-Performing Activities",
        "📅 Activity Timeline",
    ])

    # ── Tab 1: Overview ───────────────────────────────────────────────────────
    with tab1:
        st.subheader("Portfolio KPIs")
        if fdf.empty:
            st.warning("No activities match the current filters.")
        else:
            total_budget = fdf["budget_allocated"].sum()
            total_used   = fdf["budget_used"].sum()
            exec_rate    = total_used / total_budget if total_budget > 0 else 0.0
            avg_progress = safe_mean(fdf["progress"])
            n_acts       = fdf["activity_code"].nunique()

            st.caption(f"Showing: **{year_label}**")
            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Total Budget",   f"${total_budget:,.0f}")
            c2.metric("Total Used",     f"${total_used:,.0f}")
            c3.metric("Execution Rate", f"{exec_rate:.1%}")
            c4.metric("Avg Progress",   f"{avg_progress:.1f}%")
            c5.metric("Activities",     str(n_acts))

            st.divider()
            col_pie, col_bar = st.columns(2)

            with col_pie:
                st.markdown("#### Status Distribution")
                sc = fdf["status"].value_counts().reset_index()
                sc.columns = ["Status", "Count"]
                fig = px.pie(sc, values="Count", names="Status",
                             color="Status", color_discrete_map=ACT_STATUS_COLORS,
                             hole=0.4)
                fig.update_traces(textposition="inside", textinfo="percent+label")
                fig.update_layout(height=360)
                st.plotly_chart(fig, use_container_width=True)

            with col_bar:
                st.markdown("#### Execution Rate by Entity")
                ent = (
                    fdf.groupby("entity_name", dropna=False)
                    .apply(lambda g: pd.Series({
                        "budget": g["budget_allocated"].sum(),
                        "used":   g["budget_used"].sum(),
                    }), include_groups=False)
                    .reset_index()
                )
                ent["exec_pct"] = (
                    ent["used"] / ent["budget"].replace(0, float("nan")) * 100
                ).round(1).fillna(0)
                ent = ent.sort_values("exec_pct", ascending=True)
                fig2 = px.bar(ent, x="exec_pct", y="entity_name", orientation="h",
                              color="exec_pct",
                              color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                              range_color=[0, 100],
                              text=ent["exec_pct"].astype(str) + "%",
                              labels={"exec_pct": "Execution Rate (%)", "entity_name": ""})
                fig2.update_traces(textposition="outside")
                fig2.update_layout(height=360, showlegend=False,
                                   coloraxis_showscale=False)
                st.plotly_chart(fig2, use_container_width=True)

            st.divider()
            n_years = len(active_years)
            st.markdown(
                f"#### Filtered Activities"
                + (f" *(showing {n_years} year{'s' if n_years > 1 else ''} × {n_acts} activities = {len(fdf)} rows)*"
                   if n_years > 1 else f" *({n_acts} activities)*")
            )
            st.dataframe(fdf, use_container_width=True, height=400)
            show_download_button(fdf, "activities_overview.csv")

    # ── Tab 2: Budget & Performance ───────────────────────────────────────────
    with tab2:
        st.subheader(f"Budget & Performance — {year_label}")
        if fdf.empty:
            st.warning("No data for current filters.")
        else:
            for group_col, label in [("entity_name", "Implementing Entity"),
                                      ("results_area", "Results Area")]:
                st.markdown(f"#### By {label}")
                grp = (
                    fdf.groupby(group_col, dropna=False)
                    .agg(
                        budget_allocated=("budget_allocated", "sum"),
                        budget_used=("budget_used", "sum"),
                        avg_progress=("progress", "mean"),
                        activities=("activity_code", "nunique"),
                    )
                    .reset_index()
                )
                grp["execution_rate"] = (
                    grp["budget_used"] /
                    grp["budget_allocated"].replace(0, float("nan"))
                ).round(3).fillna(0)

                st.dataframe(
                    grp.style.format({
                        "budget_allocated": "{:,.0f}",
                        "budget_used":      "{:,.0f}",
                        "execution_rate":   "{:.1%}",
                        "avg_progress":     "{:.1f}",
                    }),
                    use_container_width=True,
                )

                fig = go.Figure()
                fig.add_bar(name="Budget", x=grp[group_col],
                            y=grp["budget_allocated"], marker_color="#4a90d9")
                fig.add_bar(name="Used",   x=grp[group_col],
                            y=grp["budget_used"],      marker_color="#27ae60")
                fig.update_layout(barmode="group", height=380,
                                  title=f"Budget vs Used by {label} ({year_label})",
                                  xaxis_title="", yaxis_title="Amount (USD)")
                st.plotly_chart(fig, use_container_width=True)
                st.divider()

    # ── Tab 3: Status Analysis ────────────────────────────────────────────────
    with tab3:
        st.subheader("Status Analysis")
        # activity status is not year-specific — use a single year to avoid
        # counting the same activity once per selected year
        status_year = max(active_years)
        sf = status_df[status_df["year_number"] == status_year].copy()
        if sel_entity:  sf = sf[sf["entity_name"].isin(sel_entity)]
        if sel_area:    sf = sf[sf["results_area"].isin(sel_area)]
        if len(active_years) > 1:
            st.caption(
                f"Status shown as of Year {status_year} "
                f"(activity status is not year-specific; using latest selected year)."
            )

        for group_col, label in [("entity_name", "Implementing Entity"),
                                  ("results_area", "Results Area")]:
            st.markdown(f"#### Status by {label}")
            pivot = (
                sf.groupby([group_col, "status"])["activity_count"]
                .sum()
                .unstack(fill_value=0)
            )
            if not pivot.empty:
                plot_df = pivot.reset_index().melt(
                    id_vars=group_col, var_name="Status", value_name="Count"
                )
                fig = px.bar(plot_df, x=group_col, y="Count", color="Status",
                             barmode="stack", text_auto=True,
                             color_discrete_map=ACT_STATUS_COLORS,
                             title=f"Activity Status by {label}")
                fig.update_layout(xaxis_title="", height=400)
                st.plotly_chart(fig, use_container_width=True)
            st.divider()

    # ── Tab 4: Low-Performing ─────────────────────────────────────────────────
    with tab4:
        st.subheader("Low-Performing Activities")
        cutoff_pct = st.slider("Execution Rate Cutoff (%)", 0, 100, 50, 5,
                                key="act_cutoff")
        cutoff = cutoff_pct / 100.0

        # Aggregate to one row per activity across selected years.
        # fdf has year×activity rows; averaging execution rate avoids
        # counting the same activity multiple times.
        act_agg = (
            fdf[fdf["status"] != "Planned"]
            .groupby("activity_code", dropna=False)
            .agg(
                proposed_activity=("proposed_activity", "first")
                if "proposed_activity" in fdf.columns else ("activity_code", "first"),
                entity_name=("entity_name", "first"),
                results_area=("results_area", "first"),
                category=("category", "first"),
                status=("status", "first"),
                budget_allocated=("budget_allocated", "sum"),
                budget_used=("budget_used", "sum"),
                execution_rate=("execution_rate", "mean"),
                progress=("progress", "mean"),
            )
            .reset_index()
        )
        # Recompute execution rate from aggregated budget to be accurate
        act_agg["execution_rate"] = (
            act_agg["budget_used"] /
            act_agg["budget_allocated"].replace(0, float("nan"))
        ).fillna(0)

        low_df = act_agg[act_agg["execution_rate"] < cutoff].sort_values("execution_rate")

        c1, c2, c3 = st.columns(3)
        c1.metric("Low-Performing Activities", len(low_df))
        c2.metric("Budget at Risk", f"${low_df['budget_allocated'].sum():,.0f}")
        c3.metric("Avg Exec Rate",
                  f"{low_df['execution_rate'].mean()*100:.1f}%" if not low_df.empty else "—")

        st.divider()
        if low_df.empty:
            st.success(f"✅ No activities below {cutoff_pct}% execution rate.")
        else:
            display_cols = [c for c in [
                "activity_code", "proposed_activity", "entity_name",
                "results_area", "category", "status",
                "budget_allocated", "budget_used", "execution_rate",
            ] if c in low_df.columns]
            st.dataframe(low_df[display_cols], use_container_width=True, height=440)
            show_download_button(low_df[display_cols], f"low_performing_{cutoff_pct}pct.csv")

    # ── Tab 5: Activity Timeline ──────────────────────────────────────────────
    with tab5:
        _act_tab_timeline(fdf)


def _act_tab_timeline(fdf: pd.DataFrame) -> None:
    st.subheader("Activity Timeline")
    st.caption("Activities plotted by start and end date. Red line = today.")

    # Deduplicate to one row per activity — fdf has one row per year×activity
    acts_df  = fdf.drop_duplicates(subset="activity_code", keep="first")
    gantt_df = acts_df[acts_df["start_date"].notna() & acts_df["end_date"].notna()].copy()
    n_missing = len(acts_df) - len(gantt_df)
    if gantt_df.empty:
        st.info("No activities have both start and end dates recorded.")
        return
    if n_missing:
        st.warning(
            f"{n_missing} of {len(acts_df)} activities are not shown — "
            "start/end dates not recorded in the source data."
        )

    gantt_df["start_date"] = pd.to_datetime(gantt_df["start_date"], errors="coerce")
    gantt_df["end_date"]   = pd.to_datetime(gantt_df["end_date"],   errors="coerce")
    gantt_df = gantt_df.dropna(subset=["start_date", "end_date"])

    today = pd.Timestamp.now().normalize()
    gantt_df["overdue"] = (
        (gantt_df["end_date"] < today) &
        (~gantt_df["status"].str.lower().str.contains("complet|finaliz", na=False))
    )

    # Summary KPIs
    n_overdue   = int(gantt_df["overdue"].sum())
    n_upcoming  = int((gantt_df["start_date"] > today).sum())
    n_active    = int(
        ((gantt_df["start_date"] <= today) & (gantt_df["end_date"] >= today)).sum()
    )
    c1, c2, c3 = st.columns(3)
    c1.metric("Currently Active",  n_active)
    c2.metric("Overdue",           n_overdue,  delta=f"-{n_overdue}" if n_overdue else None,
              delta_color="inverse")
    c3.metric("Not Yet Started",   n_upcoming)

    st.divider()

    # Colour by status; mark overdue in red
    gantt_df["display_status"] = gantt_df.apply(
        lambda r: "Overdue" if r["overdue"] else r["status"], axis=1
    )
    color_map = {**ACT_STATUS_COLORS, "Overdue": "#c0392b"}

    # Sort: overdue first, then by end_date
    gantt_df = gantt_df.sort_values(["overdue", "end_date"], ascending=[False, True])

    # Limit label to avoid crowding on mobile
    gantt_df["label"] = (
        gantt_df["activity_code"].astype(str) + " — " +
        gantt_df["proposed_activity"].fillna("").str[:40]
        if "proposed_activity" in gantt_df.columns
        else gantt_df["activity_code"].astype(str)
    )

    fig = px.timeline(
        gantt_df,
        x_start="start_date",
        x_end="end_date",
        y="label",
        color="display_status",
        color_discrete_map=color_map,
        hover_data={
            "entity_name":    True,
            "results_area":   True,
            "execution_rate": ":.1%",
            "start_date":     True,
            "end_date":       True,
            "label":          False,
        },
        title="Activity Gantt Chart",
    )
    fig.add_shape(
        type="line",
        x0=str(today.date()), x1=str(today.date()),
        y0=0, y1=1, yref="paper",
        line=dict(dash="dash", color="#e74c3c", width=2),
    )
    fig.add_annotation(
        x=str(today.date()), y=1.02, yref="paper",
        text="Today", showarrow=False,
        font=dict(color="#e74c3c", size=12),
    )
    chart_height = max(400, len(gantt_df) * 26 + 100)
    fig.update_layout(
        height=chart_height,
        yaxis_title="",
        xaxis_title="",
        legend_title="Status",
        margin=dict(l=10, r=10),
    )
    fig.update_yaxes(autorange="reversed")
    st.plotly_chart(fig, use_container_width=True)

    # Overdue table
    if n_overdue:
        st.divider()
        st.markdown(f"#### ⚠️ Overdue Activities ({n_overdue})")
        over_cols = [c for c in [
            "activity_code", "proposed_activity", "entity_name",
            "results_area", "end_date", "status", "execution_rate",
        ] if c in gantt_df.columns]
        st.dataframe(
            gantt_df[gantt_df["overdue"]][over_cols]
            .sort_values("end_date"),
            use_container_width=True,
        )
        show_download_button(
            gantt_df[gantt_df["overdue"]][over_cols],
            "overdue_activities.csv",
        )


# ══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD 2 — INDICATOR TRACKER
# ══════════════════════════════════════════════════════════════════════════════

def render_indicator_dashboard() -> None:
    st.html(_MOBILE_CSS)
    st.title("🦠 Pandemic Fund — Indicator M&E Dashboard")
    st.caption("Year 1 progress tracking | Pre-computed by ETL pipeline")

    with st.spinner("Loading…"):
        tracker_df  = load_mart_indicator_tracker()
        kpis_df     = load_mart_indicator_kpis()
        entity_df   = load_mart_entity_performance()
        strategic_df= load_mart_strategic_summary()

    if tracker_df.empty:
        st.warning("No indicator data found. Run the ETL pipeline first:  "
                   "`python -m etl.run_etl`")
        return

    # ── Sidebar filters ───────────────────────────────────────────────────────
    active_year = 1   # Extend: add year selector here when Year 2/3 data arrives

    with st.sidebar:
        st.subheader("Indicator Filters")
        st.caption("Empty = show all.")

        def _ms(label, col, key):
            opts = sorted(tracker_df[col].dropna().unique().tolist())
            return st.multiselect(label, options=opts, key=key) if opts else []

        entity_opts = sorted(tracker_df["entity_name"].dropna().unique().tolist())
        sel_entity  = st.multiselect("Implementing Entity", entity_opts, key="ind_ent") \
                      if entity_opts else []

        sel_type     = _ms("Indicator Type",   "indicator_type",        "ind_type")
        sel_status   = _ms("Status (Year 1)",  "status",                "ind_st")
        sel_area     = _ms("Strategic Area",   "strategic_area",        "ind_area")
        sel_activity = _ms("Activity Code",    "activity_code",         "ind_act")
        sel_naphs    = st.selectbox("NAPHS Indicator", ["All","Yes","No"],
                                    key="ind_naphs")
        st.divider()

    # ── Filter tracker ────────────────────────────────────────────────────────
    fdf = tracker_df[tracker_df["year_number"] == active_year].copy()
    if sel_entity:   fdf = fdf[fdf["entity_name"].isin(sel_entity)]
    if sel_type:     fdf = fdf[fdf["indicator_type"].isin(sel_type)]
    if sel_status:   fdf = fdf[fdf["status"].isin(sel_status)]
    if sel_area:     fdf = fdf[fdf["strategic_area"].isin(sel_area)]
    if sel_activity: fdf = fdf[fdf["activity_code"].isin(sel_activity)]
    if sel_naphs == "Yes": fdf = fdf[fdf["naphs_flag"] == True]
    elif sel_naphs == "No": fdf = fdf[fdf["naphs_flag"] == False]

    if fdf.empty:
        st.warning("No indicators match the current filters.")
        return

    # Filter pre-aggregated marts to match user selections
    f_entity    = entity_df[entity_df["year_number"] == active_year]
    f_strategic = strategic_df[strategic_df["year_number"] == active_year]
    if sel_entity:
        f_entity = f_entity[f_entity["entity_name"].isin(sel_entity)]

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Performance Overview",
        "🏢 Implementing Entity Analysis",
        "🗂️ Strategic Area Summary",
        "⚠️ Bottlenecks & Tracker",
        "🔬 NAPHS & Qualitative",
    ])

    with tab1: _ind_tab_overview(fdf, kpis_df, active_year)
    with tab2: _ind_tab_entity(fdf, f_entity, active_year)
    with tab3: _ind_tab_strategic(fdf, f_strategic, active_year)
    with tab4: _ind_tab_bottlenecks(fdf)
    with tab5: _ind_tab_naphs_qualitative(fdf)


# ── Tab 1: Overview ───────────────────────────────────────────────────────────

def _ind_tab_overview(fdf: pd.DataFrame, kpis_df: pd.DataFrame, year: int) -> None:
    st.subheader(f"Portfolio KPIs — Year {year} Indicators")
    st.caption("Completed ≥100% | On Track 70–<100% | At Risk >0–<70% | Not Started = 0%")

    # Always recompute from filtered fdf so entity/type/naphs filters apply
    total = len(fdf)
    cats  = fdf["achievement_category"].value_counts()
    def pct(lbl): return round(cats.get(lbl, 0) / total * 100, 1) if total else 0
    kpi = {
        "total_indicators": total,
        "quantitative":     int(fdf["quantitative_flag"].sum()),
        "qualitative":      int(fdf["qualitative_flag"].sum()),
        "pct_completed":    pct("Completed"),
        "pct_on_track":     pct("On Track"),
        "pct_at_risk":      pct("At Risk"),
        "pct_not_started":  pct("Not Started"),
        "avg_progress":     round(safe_mean(fdf["progress_pct"]), 1),
    }

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Indicators", kpi["total_indicators"])
    c2.metric("Quantitative",     kpi["quantitative"])
    c3.metric("Qualitative",      kpi["qualitative"])
    c4.metric("% Completed",      f"{kpi['pct_completed']}%")

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("% On Track",    f"{kpi['pct_on_track']}%")
    c6.metric("% At Risk",     f"{kpi['pct_at_risk']}%")
    c7.metric("% Not Started", f"{kpi['pct_not_started']}%")
    c8.metric("Avg Progress",  f"{kpi['avg_progress']}%")

    st.divider()

    col_a, col_b = st.columns(2)
    with col_a:
        counts = (
            fdf["achievement_category"].value_counts()
            .reindex(ACHIEVEMENT_ORDER, fill_value=0).reset_index()
        )
        counts.columns = ["Category", "Count"]
        fig = px.bar(counts, x="Category", y="Count", color="Category",
                     color_discrete_map=ACHIEVEMENT_COLORS,
                     title="Indicator Counts by Achievement Category",
                     text="Count")
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, height=380, xaxis_title="",
                          yaxis_title="Indicators")
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        type_prog = (
            fdf.groupby("indicator_type", dropna=False)["progress_pct"]
            .mean().dropna().reset_index()
            .rename(columns={"progress_pct": "avg_progress",
                             "indicator_type": "Type"})
            .sort_values("avg_progress", ascending=False)
        )
        if not type_prog.empty:
            fig2 = px.bar(type_prog, x="Type", y="avg_progress",
                          title="Average Progress by Indicator Type",
                          text=type_prog["avg_progress"].round(1).astype(str) + "%",
                          color="avg_progress",
                          color_continuous_scale=["#e74c3c","#f39c12","#2ecc71"],
                          range_color=[0,100])
            fig2.update_traces(textposition="outside")
            fig2.update_layout(showlegend=False, coloraxis_showscale=False,
                               height=380, yaxis_range=[0,115])
            st.plotly_chart(fig2, use_container_width=True)

    # Update trend
    if "last_progress_update" in fdf.columns:
        valid = pd.to_datetime(fdf["last_progress_update"], errors="coerce").dropna()
        if not valid.empty:
            monthly = (valid.dt.to_period("M").value_counts()
                       .sort_index().reset_index())
            monthly.columns = ["Month", "Count"]
            monthly["Month"] = monthly["Month"].astype(str)
            fig3 = px.bar(monthly, x="Month", y="Count",
                          title="Indicators Updated per Month", text="Count",
                          color_discrete_sequence=["#4a90d9"])
            fig3.update_traces(textposition="outside")
            fig3.update_layout(height=300)
            st.plotly_chart(fig3, use_container_width=True)

    st.divider()
    st.markdown("#### Indicator Detail Table")
    display_cols = [
        "activity_code", "entity_name", "indicator_text", "indicator_type",
        "strategic_area", "target", "actual", "progress_pct",
        "status", "achievement_category",
    ]
    display_cols = [c for c in display_cols if c in fdf.columns]
    st.dataframe(fdf[display_cols], use_container_width=True, height=420)
    show_download_button(fdf[display_cols], "overview_indicators.csv")


# ── Tab 2: Entity Analysis ────────────────────────────────────────────────────

def _ind_tab_entity(fdf: pd.DataFrame, entity_df: pd.DataFrame, year: int) -> None:
    st.subheader("Implementing Entity Analysis")

    # Always recompute from fdf so all active filters (type, naphs, area) apply
    entity_summary = (
        fdf.groupby("entity_name", dropna=False).agg(
            total_indicators=("indicator_id", "count"),
            avg_progress=("progress_pct", "mean"),
            completed=("achievement_category", lambda x: (x == "Completed").sum()),
            on_track=("achievement_category", lambda x: (x == "On Track").sum()),
            at_risk=("achievement_category", lambda x: (x == "At Risk").sum()),
            not_started=("achievement_category", lambda x: (x == "Not Started").sum()),
        ).reset_index()
    )
    entity_summary["avg_progress"] = entity_summary["avg_progress"].round(1)

    st.markdown("#### Summary by Entity")
    st.dataframe(entity_summary.sort_values("avg_progress", ascending=False),
                 use_container_width=True)
    show_download_button(entity_summary, "entity_summary.csv", "⬇️ Download Summary")

    st.divider()
    ca, cb = st.columns(2)
    with ca:
        st.markdown("#### Top Performers")
        st.dataframe(entity_summary.nlargest(5, "avg_progress"), use_container_width=True)
    with cb:
        st.markdown("#### Lowest Performers")
        st.dataframe(entity_summary.nsmallest(5, "avg_progress"), use_container_width=True)

    st.divider()

    # Horizontal bar: average progress by entity
    plot_ent = entity_summary.sort_values("avg_progress", ascending=True)
    fig = px.bar(plot_ent, x="avg_progress", y="entity_name", orientation="h",
                 title=f"Average Year {year} Progress by Entity",
                 text=plot_ent["avg_progress"].astype(str) + "%",
                 color="avg_progress",
                 color_continuous_scale=["#e74c3c","#f39c12","#2ecc71"],
                 range_color=[0,100])
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, coloraxis_showscale=False,
                      xaxis_range=[0,120],
                      height=max(380, len(plot_ent)*32+80),
                      margin=dict(l=200))
    st.plotly_chart(fig, use_container_width=True)

    # Stacked bar: status by entity (from recomputed summary)
    stack_df = entity_summary.melt(
        id_vars="entity_name",
        value_vars=["completed","on_track","at_risk","not_started"],
        var_name="status_col", value_name="count",
    )
    label_map = {"completed":"Completed","on_track":"On Track",
                 "at_risk":"At Risk","not_started":"Not Started"}
    stack_df["Category"] = stack_df["status_col"].map(label_map)
    fig2 = px.bar(stack_df, x="entity_name", y="count", color="Category",
                  color_discrete_map=ACHIEVEMENT_COLORS, barmode="stack",
                  title=f"Status Distribution by Entity (Year {year})",
                  text_auto=True,
                  category_orders={"Category": ACHIEVEMENT_ORDER})
    fig2.update_layout(xaxis_title="", height=420)
    st.plotly_chart(fig2, use_container_width=True)

    # Heatmap entity × strategic area
    st.divider()
    st.markdown("#### Heatmap: Avg Progress — Entity × Strategic Area")
    pivot = (
        fdf.groupby(["entity_name","strategic_area"], dropna=False)["progress_pct"]
        .mean().unstack(fill_value=float("nan"))
    )
    if not pivot.empty:
        fig3 = go.Figure(data=go.Heatmap(
            z=pivot.values.tolist(),
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale="RdYlGn", zmin=0, zmax=100,
            text=[[f"{v:.0f}%" if not pd.isna(v) else "—" for v in row]
                  for row in pivot.values],
            texttemplate="%{text}",
        ))
        fig3.update_layout(height=max(350, len(pivot)*30+150),
                           xaxis_title="Strategic Area", margin=dict(l=220))
        st.plotly_chart(fig3, use_container_width=True)

    # Drill-down
    st.divider()
    st.markdown("#### Drill-Down: Indicators for Selected Entity")
    entities = sorted(fdf["entity_name"].dropna().unique().tolist())
    if entities:
        sel = st.selectbox("Select Entity", entities, key="ind_drilldown")
        sub = fdf[fdf["entity_name"] == sel][[
            "activity_code","indicator_text","indicator_type",
            "strategic_area","target","actual","progress_pct",
            "status","achievement_category",
        ]]
        st.dataframe(sub, use_container_width=True, height=360)
        show_download_button(sub, f"entity_{sel[:20]}.csv")


# ── Tab 3: Strategic Summary ──────────────────────────────────────────────────

def _ind_tab_strategic(fdf: pd.DataFrame, strategic_df: pd.DataFrame, year: int) -> None:
    st.subheader("Strategic Area Summary")

    # Always recompute from fdf so all active filters apply
    strat_summary = (
        fdf.groupby("strategic_area", dropna=False).agg(
            num_indicators=("indicator_id", "count"),
            avg_progress=("progress_pct", "mean"),
            completed=("achievement_category", lambda x: (x == "Completed").sum()),
            on_track=("achievement_category", lambda x: (x == "On Track").sum()),
            at_risk=("achievement_category", lambda x: (x == "At Risk").sum()),
            not_started=("achievement_category", lambda x: (x == "Not Started").sum()),
        ).reset_index()
    )
    strat_summary["avg_progress"] = strat_summary["avg_progress"].round(1)

    st.dataframe(strat_summary.sort_values("avg_progress", ascending=False),
                 use_container_width=True)
    show_download_button(strat_summary, "strategic_summary.csv")

    st.divider()
    sorted_strat = strat_summary.sort_values("avg_progress", ascending=False)
    ca, cb = st.columns(2)
    with ca:
        fig = px.bar(
            sorted_strat, x="strategic_area", y="avg_progress",
            title=f"Average Progress by Strategic Area — Year {year}",
            text=sorted_strat["avg_progress"].astype(str) + "%",
            color="avg_progress",
            color_continuous_scale=["#e74c3c","#f39c12","#2ecc71"],
            range_color=[0,100],
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, coloraxis_showscale=False,
                          yaxis_range=[0,115], height=400)
        st.plotly_chart(fig, use_container_width=True)

    with cb:
        stack_df = strat_summary.melt(
            id_vars="strategic_area",
            value_vars=["completed","on_track","at_risk","not_started"],
            var_name="status_col", value_name="count",
        )
        label_map = {"completed":"Completed","on_track":"On Track",
                     "at_risk":"At Risk","not_started":"Not Started"}
        stack_df["Category"] = stack_df["status_col"].map(label_map)
        fig2 = px.bar(stack_df, x="strategic_area", y="count", color="Category",
                      color_discrete_map=ACHIEVEMENT_COLORS, barmode="stack",
                      title=f"Status by Strategic Area (Year {year})",
                      text_auto=True,
                      category_orders={"Category": ACHIEVEMENT_ORDER})
        fig2.update_layout(xaxis_title="", height=400)
        st.plotly_chart(fig2, use_container_width=True)

    # Key indicators per area
    st.divider()
    st.markdown("#### Key Indicators by Area")
    area_opts = sorted(fdf["strategic_area"].dropna().unique().tolist())
    if area_opts:
        sel_area = st.selectbox("Select Area", area_opts, key="ind_area_sel")
        area_df  = fdf[fdf["strategic_area"] == sel_area]
        area_cols = [
            "activity_code","entity_name","indicator_text","indicator_type",
            "target","actual","progress_pct","achievement_category",
            "qualitative_stage","last_progress_update",
        ]
        area_cols = [c for c in area_cols if c in area_df.columns]
        st.dataframe(area_df[area_cols], use_container_width=True, height=380)
        show_download_button(area_df[area_cols], f"area_{sel_area[:20]}.csv")

    # Management output summaries — from recomputed strat_summary
    st.divider()
    st.markdown("#### Management Output Summaries")
    output_areas = ["Surveillance", "Workforce Development", "Laboratory Systems"]
    cols = st.columns(len(output_areas))
    for col, area in zip(cols, output_areas):
        row = strat_summary[strat_summary["strategic_area"] == area]
        with col:
            st.markdown(f"**{area}**")
            if not row.empty:
                r = row.iloc[0]
                st.metric("Indicators",   int(r["num_indicators"]))
                st.metric("Completed",    int(r["completed"]))
                st.metric("Avg Progress", f"{r['avg_progress']}%")
            else:
                st.metric("Indicators", 0)


# ── Tab 4: Bottlenecks ────────────────────────────────────────────────────────

def _ind_tab_bottlenecks(fdf: pd.DataFrame) -> None:
    st.subheader("Bottlenecks & Detailed Indicator Tracker")
    st.caption("All flags pre-computed by the ETL pipeline.")

    base_cols = [
        "activity_code","entity_name","indicator_text","indicator_type",
        "strategic_area","target","actual","progress_pct","gap",
        "status","last_progress_update",
    ]
    base_cols = [c for c in base_cols if c in fdf.columns]

    # 1. No actuals
    st.markdown("### 1. Indicators with Target but No Actuals Recorded")
    ns = fdf[fdf["no_actuals_flag"] == True]
    if ns.empty:
        st.success("✅ No quantitative indicators with missing actuals.")
    else:
        st.warning(f"⚠️ {len(ns)} indicator(s) have targets but no actuals.")
        st.dataframe(ns[base_cols], use_container_width=True, height=280)
        show_download_button(ns[base_cols], "bottleneck_no_actuals.csv")

    st.divider()

    # 2. At Risk
    st.markdown("### 2. Indicators At Risk")
    ar = fdf[fdf["at_risk_flag"] == True]
    if ar.empty:
        st.success("✅ No indicators currently At Risk.")
    else:
        st.warning(f"⚠️ {len(ar)} indicator(s) are At Risk.")
        st.dataframe(ar[base_cols], use_container_width=True, height=280)
        show_download_button(ar[base_cols], "bottleneck_at_risk.csv")

    st.divider()

    # 3. Largest gaps
    st.markdown("### 3. Indicators with Largest Gaps")
    gap_df = fdf[fdf["gap"].notna()].sort_values("gap_rank")
    if gap_df.empty:
        st.info("No gap data available.")
    else:
        st.dataframe(gap_df[base_cols].head(15), use_container_width=True, height=280)
        show_download_button(gap_df[base_cols], "bottleneck_gaps.csv")

    st.divider()

    # 4. Stale
    st.markdown("### 4. Indicators Not Updated Recently")
    stale_days = st.slider("Flag not updated in last N days",
                            30, 365, 90, 30, key="ind_stale")
    stale_cutoff = pd.Timestamp.now() - pd.Timedelta(days=stale_days)
    lu = pd.to_datetime(fdf["last_progress_update"], errors="coerce")
    stale = fdf[lu.isna() | (lu < stale_cutoff)]
    if stale.empty:
        st.success(f"✅ All indicators updated in the last {stale_days} days.")
    else:
        st.warning(f"⚠️ {len(stale)} indicator(s) not updated in {stale_days} days.")
        st.dataframe(stale[base_cols], use_container_width=True, height=280)
        show_download_button(stale[base_cols], "bottleneck_stale.csv")

    st.divider()

    # 5. Qualitative not started
    st.markdown("### 5. Qualitative Indicators Still Not Started")
    qns = fdf[fdf["qualitative_flag"] & (fdf["achievement_category"] == "Not Started")]
    if qns.empty:
        st.success("✅ No qualitative indicators marked Not Started.")
    else:
        st.warning(f"⚠️ {len(qns)} qualitative indicator(s) still Not Started.")
        q_cols = [
            "activity_code","entity_name","indicator_text",
            "strategic_area","qualitative_stage","status","last_progress_update",
        ]
        q_cols = [c for c in q_cols if c in qns.columns]
        st.dataframe(qns[q_cols], use_container_width=True, height=280)
        show_download_button(qns[q_cols], "bottleneck_qual_ns.csv")

    st.divider()

    # Full tracker
    st.markdown("### Full Indicator Tracker (Sortable)")
    tracker_cols = [
        "activity_code","entity_name","indicator_text","indicator_type",
        "strategic_area","target","actual","progress_pct","gap",
        "status","achievement_category","last_progress_update",
    ]
    tracker_cols = [c for c in tracker_cols if c in fdf.columns]
    st.dataframe(fdf[tracker_cols], use_container_width=True, height=460)
    show_download_button(fdf[tracker_cols], "full_indicator_tracker.csv",
                         "⬇️ Download Full Tracker")


# ── Tab 5: NAPHS & Qualitative ────────────────────────────────────────────────

def _ind_tab_naphs_qualitative(fdf: pd.DataFrame) -> None:

    STAGE_ORDER = [
        "Not Started", "Planned", "Initiated", "In Progress",
        "Draft", "Submitted", "Under Review",
        "Validated", "Finalized", "Completed", "Approved",
    ]
    STAGE_COLORS = {
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

    # ── Section 1: NAPHS accountability breakdown ─────────────────────────────
    st.subheader("NAPHS Indicator Accountability")
    st.caption(
        "NAPHS-linked indicators by entity and strategic area — "
        "shows where accountability for national commitments sits."
    )

    naphs_df = fdf[fdf["naphs_flag"] == True]
    n_naphs  = len(naphs_df)
    n_total  = len(fdf)

    if naphs_df.empty:
        st.info("No NAPHS-linked indicators in the current filter.")
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("NAPHS Indicators", n_naphs)
        c2.metric("% of Portfolio",   f"{n_naphs/n_total*100:.0f}%")
        has_actual = naphs_df["actual"].notna()   # actual value only; eff_progress can be non-null without data
        c3.metric("Reporting Rate",   f"{has_actual.mean()*100:.0f}%")

        col_a, col_b = st.columns(2)

        with col_a:
            # NAPHS count by entity — who owns the most commitments
            ent_counts = (
                naphs_df.groupby("entity_name", dropna=False)
                .agg(
                    total=("indicator_id", "count"),
                    completed=("achievement_category",
                               lambda x: (x == "Completed").sum()),
                    on_track=("achievement_category",
                              lambda x: (x == "On Track").sum()),
                    at_risk=("achievement_category",
                             lambda x: (x == "At Risk").sum()),
                    not_started=("achievement_category",
                                 lambda x: (x == "Not Started").sum()),
                )
                .reset_index()
            )
            stack = ent_counts.melt(
                id_vars="entity_name",
                value_vars=["completed","on_track","at_risk","not_started"],
                var_name="cat", value_name="count",
            )
            label_map = {"completed":"Completed","on_track":"On Track",
                         "at_risk":"At Risk","not_started":"Not Started"}
            stack["Category"] = stack["cat"].map(label_map)
            fig = px.bar(
                stack, x="entity_name", y="count", color="Category",
                color_discrete_map=ACHIEVEMENT_COLORS, barmode="stack",
                text_auto=True,
                category_orders={"Category": ACHIEVEMENT_ORDER},
                title="NAPHS Indicators by Entity & Achievement",
            )
            fig.update_layout(height=380, xaxis_title="", legend_title="")
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            # NAPHS count by strategic area
            area_counts = (
                naphs_df.groupby("strategic_area", dropna=False)
                .size()
                .sort_values(ascending=True)
                .reset_index(name="count")
            )
            fig2 = px.bar(
                area_counts, x="count", y="strategic_area",
                orientation="h", text="count",
                title="NAPHS Indicators by Strategic Area",
                color_discrete_sequence=["#3498db"],
            )
            fig2.update_traces(textposition="outside")
            fig2.update_layout(height=380, yaxis_title="", xaxis_title="Count")
            st.plotly_chart(fig2, use_container_width=True)

        # NAPHS detail table
        naphs_cols = [c for c in [
            "entity_name", "strategic_area", "indicator_text",
            "indicator_type", "target", "actual",
            "progress_pct", "achievement_category", "status",
        ] if c in naphs_df.columns]
        st.dataframe(naphs_df[naphs_cols], use_container_width=True, height=300)
        show_download_button(naphs_df[naphs_cols], "naphs_indicators.csv")

    st.divider()

    # ── Section 2: Qualitative stage progress ────────────────────────────────
    st.subheader("Qualitative Indicator Stage Progress")
    st.caption(
        "Each qualitative indicator is at exactly one stage. "
        "This shows the current distribution — not a pipeline drop-off."
    )

    qual_df = fdf[fdf["qualitative_flag"] == True].copy()
    if qual_df.empty:
        st.info("No qualitative indicators in the current filter.")
        return

    total_qual = len(qual_df)
    has_stage  = qual_df["qualitative_stage"].notna().sum()
    st.caption(
        f"{total_qual} qualitative indicators total — "
        f"{has_stage} have a stage recorded, "
        f"{total_qual - has_stage} have no stage entered yet."
    )

    # Normalise stage strings
    qual_df["stage_norm"] = (
        qual_df["qualitative_stage"]
        .fillna("Not Started")
        .str.strip()
        .str.title()
    )
    qual_df["stage_norm"] = qual_df["stage_norm"].where(
        qual_df["stage_norm"].isin(STAGE_ORDER), "Not Started"
    )

    stage_counts = (
        qual_df["stage_norm"]
        .value_counts()
        .reindex(STAGE_ORDER, fill_value=0)
        .reset_index()
    )
    stage_counts.columns = ["Stage", "Count"]
    stage_counts = stage_counts[stage_counts["Count"] > 0]
    stage_counts["% of Total"] = (
        stage_counts["Count"] / total_qual * 100
    ).round(1).astype(str) + "%"
    stage_counts["Color"] = stage_counts["Stage"].map(STAGE_COLORS)

    col_f, col_t = st.columns([2, 1])
    with col_f:
        # Horizontal bar ordered by stage progression (most advanced at top)
        plot_sc = stage_counts.iloc[::-1]   # reverse so top = most advanced
        fig3 = px.bar(
            plot_sc, x="Count", y="Stage",
            orientation="h", text="Count",
            color="Stage",
            color_discrete_map=STAGE_COLORS,
            title="Qualitative Indicators — Current Stage Distribution",
        )
        fig3.update_traces(textposition="outside")
        fig3.update_layout(
            height=max(300, len(stage_counts) * 45 + 80),
            showlegend=False, yaxis_title="", xaxis_title="# Indicators",
        )
        st.plotly_chart(fig3, use_container_width=True)

    with col_t:
        st.markdown("#### Stage Breakdown")
        st.dataframe(
            stage_counts[["Stage","Count","% of Total"]].set_index("Stage"),
            use_container_width=True,
        )
        show_download_button(stage_counts[["Stage","Count","% of Total"]],
                             "qualitative_stages.csv")

    # Stage by entity
    st.divider()
    st.markdown("#### Stage Distribution by Entity")
    if "entity_name" in qual_df.columns:
        entity_stage = (
            qual_df.groupby(["entity_name", "stage_norm"])
            .size()
            .reset_index(name="count")
        )
        fig4 = px.bar(
            entity_stage, x="entity_name", y="count",
            color="stage_norm",
            color_discrete_map=STAGE_COLORS,
            category_orders={"stage_norm": STAGE_ORDER},
            title="Qualitative Indicator Stages by Entity",
            text_auto=True, barmode="stack",
        )
        fig4.update_layout(height=380, xaxis_title="", legend_title="Stage")
        st.plotly_chart(fig4, use_container_width=True)

    # Indicator-level detail
    st.divider()
    st.markdown("#### Qualitative Indicator Detail")
    q_cols = [c for c in [
        "entity_name", "strategic_area", "indicator_text",
        "qualitative_stage", "qualitative_score",
        "achievement_category", "status", "last_progress_update",
    ] if c in qual_df.columns]
    st.dataframe(
        qual_df[q_cols].sort_values("qualitative_score", ascending=False),
        use_container_width=True, height=320,
    )
    show_download_button(qual_df[q_cols], "qualitative_detail.csv")


# ══════════════════════════════════════════════════════════════════════════════
#  USER DATABASE
# ══════════════════════════════════════════════════════════════════════════════

def ensure_users_table() -> None:
    engine = _get_engine()
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS app_users (
                id            SERIAL PRIMARY KEY,
                username      VARCHAR(50)  UNIQUE NOT NULL,
                name          VARCHAR(100) NOT NULL,
                email         VARCHAR(150) UNIQUE,
                password_hash VARCHAR(255) NOT NULL,
                role          VARCHAR(20)  NOT NULL DEFAULT 'analyst',
                is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
                created_at    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_login    TIMESTAMP
            )
        """))
        conn.commit()
    engine.dispose()


def db_get_user(username: str) -> dict | None:
    engine = _get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM app_users WHERE username=:u AND is_active=TRUE"),
            {"u": username},
        ).fetchone()
    engine.dispose()
    return dict(row._mapping) if row else None


def db_username_exists(username: str) -> bool:
    engine = _get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM app_users WHERE username=:u"), {"u": username}
        ).fetchone()
    engine.dispose()
    return row is not None


def db_email_exists(email: str) -> bool:
    if not email:
        return False
    engine = _get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM app_users WHERE email=:e"), {"e": email}
        ).fetchone()
    engine.dispose()
    return row is not None


def db_register_user(username, name, email, password, role="analyst"):
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    engine  = _get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(
                text("INSERT INTO app_users (username,name,email,password_hash,role) "
                     "VALUES (:u,:n,:e,:p,:r)"),
                {"u": username, "n": name, "e": email or None,
                 "p": pw_hash,  "r": role},
            )
        engine.dispose()
        return True, "Account created successfully."
    except Exception as exc:
        engine.dispose()
        msg = str(exc).lower()
        if "unique" in msg or "duplicate" in msg:
            return False, "Username or email already in use."
        return False, f"Registration failed: {exc}"


def db_update_last_login(username: str) -> None:
    engine = _get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE app_users SET last_login=NOW() WHERE username=:u"),
            {"u": username},
        )
    engine.dispose()


def db_get_user_by_email(email: str) -> dict | None:
    """Return active user matching email (case-insensitive)."""
    engine = _get_engine()
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM app_users WHERE LOWER(email)=LOWER(:e) AND is_active=TRUE"),
            {"e": email},
        ).fetchone()
    engine.dispose()
    return dict(row._mapping) if row else None


def db_update_password(username: str, new_password: str) -> None:
    pw_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
    engine  = _get_engine()
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE app_users SET password_hash=:h WHERE username=:u"),
            {"h": pw_hash, "u": username},
        )
    engine.dispose()


# ══════════════════════════════════════════════════════════════════════════════
#  AUTHENTICATION
# ══════════════════════════════════════════════════════════════════════════════

_AUTH_CSS = """
<style>
[data-testid="stMain"] > div:first-child {
    max-width: 480px;
    margin: 0 auto;
    padding-top: 2rem;
}
</style>
"""


def _verify_password(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


def _do_login(username: str, password: str) -> bool:
    user = db_get_user(username)
    if not user:
        return False
    if _verify_password(password, user["password_hash"]):
        st.session_state.update({
            "authenticated": True,
            "username":      username,
            "display_name":  user["name"],
            "role":          user["role"],
        })
        db_update_last_login(username)
        return True
    return False


def render_login_page() -> None:
    if st.session_state.get("authenticated"):
        return
    st.html(_AUTH_CSS)
    st.title("🦠 Pandemic Fund M&E")
    st.caption("Monitoring & Evaluation Intelligence Dashboard")
    st.divider()
    st.subheader("Sign in")
    with st.form("login_form"):
        username  = st.text_input("Username")
        password  = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login", use_container_width=True)
    if submitted:
        if not username or not password:
            st.error("Please enter both username and password.")
        elif _do_login(username.strip(), password):
            st.rerun()
        else:
            st.error("Incorrect username or password.")
    st.divider()
    col_a, col_b = st.columns(2)
    with col_a:
        st.caption("Don't have an account?")
        if st.button("Create account →", key="go_register"):
            st.session_state["auth_page"] = "register"
            st.rerun()
    with col_b:
        st.caption("Forgot your password?")
        if st.button("Reset password →", key="go_reset"):
            st.session_state["auth_page"] = "forgot_password"
            st.rerun()
    st.stop()


def render_forgot_password_page() -> None:
    st.html(_AUTH_CSS)
    st.title("🦠 Pandemic Fund M&E")
    st.caption("Monitoring & Evaluation Intelligence Dashboard")
    st.divider()
    st.subheader("Reset Password")

    # Two-step: verify identity first, then show new-password form
    step = st.session_state.get("reset_step", "verify")

    if step == "verify":
        st.caption("Enter your username and registered email to verify your identity.")
        with st.form("reset_verify_form"):
            username  = st.text_input("Username")
            email     = st.text_input("Email address")
            submitted = st.form_submit_button("Verify identity", use_container_width=True)
        if submitted:
            if not username.strip() or not email.strip():
                st.error("Both username and email are required.")
            else:
                user = db_get_user(username.strip())
                if (
                    user
                    and user.get("email")
                    and user["email"].lower() == email.strip().lower()
                ):
                    st.session_state["reset_username"] = user["username"]
                    st.session_state["reset_step"]     = "new_password"
                    st.rerun()
                else:
                    st.error("No account found with that username and email combination.")

    elif step == "new_password":
        reset_user = st.session_state.get("reset_username", "")
        st.info(f"Setting new password for **{reset_user}**")
        with st.form("reset_newpw_form"):
            new_pw     = st.text_input("New Password", type="password",
                                        help="Minimum 8 characters")
            confirm_pw = st.text_input("Confirm New Password", type="password")
            submitted  = st.form_submit_button("Update password", use_container_width=True)
        if submitted:
            if len(new_pw) < 8:
                st.error("Password must be at least 8 characters.")
            elif new_pw != confirm_pw:
                st.error("Passwords do not match.")
            else:
                db_update_password(reset_user, new_pw)
                # clear reset state
                st.session_state.pop("reset_step",    None)
                st.session_state.pop("reset_username", None)
                st.success("Password updated. You can now sign in.")
                st.session_state["auth_page"] = "login"
                st.rerun()

    st.divider()
    if st.button("← Back to login", key="go_login_from_reset"):
        st.session_state.pop("reset_step",     None)
        st.session_state.pop("reset_username", None)
        st.session_state["auth_page"] = "login"
        st.rerun()
    st.stop()


def render_register_page() -> None:
    st.html(_AUTH_CSS)
    st.title("🦠 Pandemic Fund M&E")
    st.caption("Monitoring & Evaluation Intelligence Dashboard")
    st.divider()
    st.subheader("Create Account")
    with st.form("register_form"):
        name       = st.text_input("Full Name")
        username   = st.text_input("Username")
        email      = st.text_input("Email (optional)")
        password   = st.text_input("Password", type="password",
                                    help="Minimum 8 characters")
        confirm_pw = st.text_input("Confirm Password", type="password")
        submitted  = st.form_submit_button("Register", use_container_width=True)
    if submitted:
        errors = []
        if not name.strip():     errors.append("Full name is required.")
        if not username.strip(): errors.append("Username is required.")
        elif db_username_exists(username.strip()):
            errors.append("Username already taken.")
        if email.strip() and db_email_exists(email.strip()):
            errors.append("Email already registered.")
        if len(password) < 8:   errors.append("Password must be at least 8 characters.")
        if password != confirm_pw: errors.append("Passwords do not match.")
        if errors:
            for e in errors: st.error(e)
        else:
            ok, msg = db_register_user(
                username.strip(), name.strip(), email.strip(), password
            )
            if ok:
                st.success(f"✅ {msg} You can now sign in.")
                st.session_state["auth_page"] = "login"
                st.rerun()
            else:
                st.error(msg)
    st.divider()
    st.caption("Already have an account?")
    if st.button("← Back to login", key="go_login"):
        st.session_state["auth_page"] = "login"
        st.rerun()
    st.stop()


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    try:
        ensure_users_table()
    except Exception as exc:
        st.error(f"Database error: {exc}")
        st.stop()

    st.session_state.setdefault("authenticated", False)
    st.session_state.setdefault("auth_page",     "login")

    if not st.session_state["authenticated"]:
        if st.session_state["auth_page"] == "register":
            render_register_page()
        elif st.session_state["auth_page"] == "forgot_password":
            render_forgot_password_page()
        else:
            render_login_page()

    display_name = st.session_state.get("display_name", "User")
    role         = st.session_state.get("role", "analyst")

    with st.sidebar:
        st.markdown(f"👤 **{display_name}**")
        st.caption(f"Role: {role}")
        if st.button("Logout", key="logout_btn"):
            for k in ("authenticated","username","display_name","role"):
                st.session_state[k] = False if k == "authenticated" else ""
            st.session_state["auth_page"] = "login"
            st.rerun()
        st.divider()
        st.markdown("## 📊 Pandemic Fund M&E")
        st.divider()
        dashboard = st.radio(
            "Select Dashboard",
            ["📊 Portfolio Activities", "🦠 Indicator Tracker"],
            key="dashboard_selector",
        )
        st.divider()

    if dashboard == "📊 Portfolio Activities":
        render_activities_dashboard()
    else:
        render_indicator_dashboard()


if __name__ == "__main__":
    main()
