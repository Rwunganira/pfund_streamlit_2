"""
pages/indicators.py
===================
Indicator Tracker Dashboard — render_indicator_dashboard().
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from etl.config import ACHIEVEMENT_ORDER, ACHIEVEMENT_COLORS
from utils.helpers import (
    ACHIEVEMENT_STATUS_LABEL_MAP,
    QUAL_STAGE_ORDER,
    QUAL_STAGE_COLORS,
    MOBILE_CSS,
    safe_mean,
    show_download_button,
)
from utils.loaders import (
    load_mart_indicator_tracker,
    load_mart_indicator_kpis,
    load_mart_entity_performance,
    load_mart_strategic_summary,
)


def render_indicator_dashboard() -> None:
    st.html(MOBILE_CSS)
    st.title("🦠 Pandemic Fund — Indicator M&E Dashboard")
    st.caption("Year 1 progress tracking | Pre-computed by ETL pipeline")

    with st.spinner("Loading…"):
        tracker_df   = load_mart_indicator_tracker()
        kpis_df      = load_mart_indicator_kpis()      # noqa: F841 — kept for future use
        entity_df    = load_mart_entity_performance()   # noqa: F841
        strategic_df = load_mart_strategic_summary()    # noqa: F841

    if tracker_df.empty:
        st.warning("No indicator data found. Run the ETL pipeline first:  "
                   "`python -m etl.run_etl`")
        return

    # ── Sidebar filters ───────────────────────────────────────────────────────
    with st.sidebar:
        st.subheader("Indicator Filters")
        st.caption("Empty = show all.")

        year_opts_ind = sorted(tracker_df["year_number"].dropna().unique().tolist())
        active_year   = st.selectbox(
            "Year", options=year_opts_ind,
            format_func=lambda x: f"Year {x}",
            key="ind_year",
        )

        def _ms(label, col, key):
            opts = sorted(tracker_df[col].dropna().unique().tolist())
            return st.multiselect(label, options=opts, key=key) if opts else []

        entity_opts = sorted(tracker_df["entity_name"].dropna().unique().tolist())
        sel_entity  = (st.multiselect("Implementing Entity", entity_opts, key="ind_ent")
                       if entity_opts else [])

        sel_type     = _ms("Indicator Type",  "indicator_type",  "ind_type")
        sel_status   = _ms("Status (Year 1)", "status",          "ind_st")
        sel_area     = _ms("Strategic Area",  "strategic_area",  "ind_area")
        sel_activity = _ms("Activity Code",   "activity_code",   "ind_act")
        sel_naphs    = st.selectbox("NAPHS Indicator", ["All", "Yes", "No"],
                                    key="ind_naphs")
        st.divider()

    # ── Filter tracker ────────────────────────────────────────────────────────
    fdf = tracker_df[tracker_df["year_number"] == active_year].copy()
    if sel_entity:   fdf = fdf[fdf["entity_name"].isin(sel_entity)]
    if sel_type:     fdf = fdf[fdf["indicator_type"].isin(sel_type)]
    if sel_status:   fdf = fdf[fdf["status"].isin(sel_status)]
    if sel_area:     fdf = fdf[fdf["strategic_area"].isin(sel_area)]
    if sel_activity: fdf = fdf[fdf["activity_code"].isin(sel_activity)]
    if sel_naphs == "Yes":
        fdf = fdf[fdf["naphs_flag"] == True]
    elif sel_naphs == "No":
        fdf = fdf[fdf["naphs_flag"] == False]

    if fdf.empty:
        st.warning("No indicators match the current filters.")
        return

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Performance Overview",
        "🏢 Implementing Entity Analysis",
        "🗂️ Strategic Area Summary",
        "⚠️ Bottlenecks & Tracker",
        "🔬 NAPHS & Qualitative",
    ])

    with tab1: _ind_tab_overview(fdf, active_year)
    with tab2: _ind_tab_entity(fdf, active_year)
    with tab3: _ind_tab_strategic(fdf, active_year)
    with tab4: _ind_tab_bottlenecks(fdf)
    with tab5: _ind_tab_naphs_qualitative(fdf)


# ── Tab 1: Overview ───────────────────────────────────────────────────────────

def _ind_tab_overview(fdf: pd.DataFrame, year: int) -> None:
    st.subheader(f"Portfolio KPIs — Year {year} Indicators")
    st.caption("Completed ≥100% | On Track 70–<100% | At Risk >0–<70% | Not Started = 0%")

    total = len(fdf)
    cats  = fdf["achievement_category"].value_counts()

    def pct(lbl):
        return round(cats.get(lbl, 0) / total * 100, 1) if total else 0

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
                          color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                          range_color=[0, 100])
            fig2.update_traces(textposition="outside")
            fig2.update_layout(showlegend=False, coloraxis_showscale=False,
                               height=380, yaxis_range=[0, 115])
            st.plotly_chart(fig2, use_container_width=True)

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
    display_cols = [c for c in [
        "activity_code", "entity_name", "indicator_text", "indicator_type",
        "strategic_area", "target", "actual", "progress_pct",
        "status", "achievement_category",
    ] if c in fdf.columns]
    st.dataframe(fdf[display_cols], use_container_width=True, height=420)
    show_download_button(fdf[display_cols], "overview_indicators.csv")


# ── Tab 2: Entity Analysis ────────────────────────────────────────────────────

def _ind_tab_entity(fdf: pd.DataFrame, year: int) -> None:
    st.subheader("Implementing Entity Analysis")

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

    plot_ent = entity_summary.sort_values("avg_progress", ascending=True)
    fig = px.bar(plot_ent, x="avg_progress", y="entity_name", orientation="h",
                 title=f"Average Year {year} Progress by Entity",
                 text=plot_ent["avg_progress"].astype(str) + "%",
                 color="avg_progress",
                 color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
                 range_color=[0, 100])
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, coloraxis_showscale=False,
                      xaxis_range=[0, 120],
                      height=max(380, len(plot_ent) * 32 + 80),
                      margin=dict(l=200))
    st.plotly_chart(fig, use_container_width=True)

    stack_df = entity_summary.melt(
        id_vars="entity_name",
        value_vars=["completed", "on_track", "at_risk", "not_started"],
        var_name="status_col", value_name="count",
    )
    stack_df["Category"] = stack_df["status_col"].map(ACHIEVEMENT_STATUS_LABEL_MAP)
    fig2 = px.bar(stack_df, x="entity_name", y="count", color="Category",
                  color_discrete_map=ACHIEVEMENT_COLORS, barmode="stack",
                  title=f"Status Distribution by Entity (Year {year})",
                  text_auto=True,
                  category_orders={"Category": ACHIEVEMENT_ORDER})
    fig2.update_layout(xaxis_title="", height=420)
    st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    st.markdown("#### Heatmap: Avg Progress — Entity × Strategic Area")
    pivot = (
        fdf.groupby(["entity_name", "strategic_area"], dropna=False)["progress_pct"]
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
        fig3.update_layout(height=max(350, len(pivot) * 30 + 150),
                           xaxis_title="Strategic Area", margin=dict(l=220))
        st.plotly_chart(fig3, use_container_width=True)

    st.divider()
    st.markdown("#### Drill-Down: Indicators for Selected Entity")
    entities = sorted(fdf["entity_name"].dropna().unique().tolist())
    if entities:
        sel = st.selectbox("Select Entity", entities, key="ind_drilldown")
        drill_cols = [c for c in [
            "activity_code", "indicator_text", "indicator_type",
            "strategic_area", "target", "actual", "progress_pct",
            "status", "achievement_category",
        ] if c in fdf.columns]
        sub = fdf[fdf["entity_name"] == sel][drill_cols]
        st.dataframe(sub, use_container_width=True, height=360)
        show_download_button(sub, f"entity_{sel[:20]}.csv")


# ── Tab 3: Strategic Summary ──────────────────────────────────────────────────

def _ind_tab_strategic(fdf: pd.DataFrame, year: int) -> None:
    st.subheader("Strategic Area Summary")

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
            color_continuous_scale=["#e74c3c", "#f39c12", "#2ecc71"],
            range_color=[0, 100],
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, coloraxis_showscale=False,
                          yaxis_range=[0, 115], height=400)
        st.plotly_chart(fig, use_container_width=True)

    with cb:
        stack_df = strat_summary.melt(
            id_vars="strategic_area",
            value_vars=["completed", "on_track", "at_risk", "not_started"],
            var_name="status_col", value_name="count",
        )
        stack_df["Category"] = stack_df["status_col"].map(ACHIEVEMENT_STATUS_LABEL_MAP)
        fig2 = px.bar(stack_df, x="strategic_area", y="count", color="Category",
                      color_discrete_map=ACHIEVEMENT_COLORS, barmode="stack",
                      title=f"Status by Strategic Area (Year {year})",
                      text_auto=True,
                      category_orders={"Category": ACHIEVEMENT_ORDER})
        fig2.update_layout(xaxis_title="", height=400)
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    st.markdown("#### Key Indicators by Area")
    area_opts = sorted(fdf["strategic_area"].dropna().unique().tolist())
    if area_opts:
        sel_area = st.selectbox("Select Area", area_opts, key="ind_area_sel")
        area_df  = fdf[fdf["strategic_area"] == sel_area]
        area_cols = [c for c in [
            "activity_code", "entity_name", "indicator_text", "indicator_type",
            "target", "actual", "progress_pct", "achievement_category",
            "qualitative_stage", "last_progress_update",
        ] if c in area_df.columns]
        st.dataframe(area_df[area_cols], use_container_width=True, height=380)
        show_download_button(area_df[area_cols], f"area_{sel_area[:20]}.csv")

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

    base_cols = [c for c in [
        "activity_code", "entity_name", "indicator_text", "indicator_type",
        "strategic_area", "target", "actual", "progress_pct", "gap",
        "status", "last_progress_update",
    ] if c in fdf.columns]

    st.markdown("### 1. Indicators with Target but No Actuals Recorded")
    ns = fdf[fdf["no_actuals_flag"] == True]
    if ns.empty:
        st.success("✅ No quantitative indicators with missing actuals.")
    else:
        st.warning(f"⚠️ {len(ns)} indicator(s) have targets but no actuals.")
        st.dataframe(ns[base_cols], use_container_width=True, height=280)
        show_download_button(ns[base_cols], "bottleneck_no_actuals.csv")

    st.divider()

    st.markdown("### 2. Indicators At Risk")
    ar = fdf[fdf["at_risk_flag"] == True]
    if ar.empty:
        st.success("✅ No indicators currently At Risk.")
    else:
        st.warning(f"⚠️ {len(ar)} indicator(s) are At Risk.")
        st.dataframe(ar[base_cols], use_container_width=True, height=280)
        show_download_button(ar[base_cols], "bottleneck_at_risk.csv")

    st.divider()

    st.markdown("### 3. Indicators with Largest Gaps")
    gap_df = fdf[fdf["gap"].notna()].sort_values("gap_rank")
    if gap_df.empty:
        st.info("No gap data available.")
    else:
        st.dataframe(gap_df[base_cols].head(15), use_container_width=True, height=280)
        show_download_button(gap_df[base_cols], "bottleneck_gaps.csv")

    st.divider()

    st.markdown("### 4. Indicators Not Updated Recently")
    stale_days   = st.slider("Flag not updated in last N days",
                              30, 365, 90, 30, key="ind_stale")
    stale_cutoff = pd.Timestamp.now() - pd.Timedelta(days=stale_days)
    lu           = pd.to_datetime(fdf["last_progress_update"], errors="coerce")
    never_updated = fdf[lu.isna()]
    outdated      = fdf[lu.notna() & (lu < stale_cutoff)]
    stale         = pd.concat([never_updated, outdated])
    if stale.empty:
        st.success(f"✅ All indicators updated in the last {stale_days} days.")
    else:
        c1, c2 = st.columns(2)
        c1.metric("Never Updated",                 len(never_updated))
        c2.metric(f"Not Updated in {stale_days}d", len(outdated))
        st.dataframe(stale[base_cols], use_container_width=True, height=280)
        show_download_button(stale[base_cols], "bottleneck_stale.csv")

    st.divider()

    st.markdown("### 5. Qualitative Indicators Still Not Started")
    qns = fdf[fdf["qualitative_flag"] & (fdf["achievement_category"] == "Not Started")]
    if qns.empty:
        st.success("✅ No qualitative indicators marked Not Started.")
    else:
        st.warning(f"⚠️ {len(qns)} qualitative indicator(s) still Not Started.")
        q_cols = [c for c in [
            "activity_code", "entity_name", "indicator_text",
            "strategic_area", "qualitative_stage", "status", "last_progress_update",
        ] if c in qns.columns]
        st.dataframe(qns[q_cols], use_container_width=True, height=280)
        show_download_button(qns[q_cols], "bottleneck_qual_ns.csv")

    st.divider()

    st.markdown("### Full Indicator Tracker (Sortable)")
    tracker_cols = [c for c in [
        "activity_code", "entity_name", "indicator_text", "indicator_type",
        "strategic_area", "target", "actual", "progress_pct", "gap",
        "status", "achievement_category", "last_progress_update",
    ] if c in fdf.columns]
    st.dataframe(fdf[tracker_cols], use_container_width=True, height=460)
    show_download_button(fdf[tracker_cols], "full_indicator_tracker.csv",
                         "⬇️ Download Full Tracker")


# ── Tab 5: NAPHS & Qualitative ────────────────────────────────────────────────

def _ind_tab_naphs_qualitative(fdf: pd.DataFrame) -> None:

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
        c2.metric("% of Portfolio",   f"{n_naphs / n_total * 100:.0f}%")
        has_actual = naphs_df["actual"].notna()
        c3.metric("Reporting Rate",   f"{has_actual.mean() * 100:.0f}%")

        col_a, col_b = st.columns(2)

        with col_a:
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
                value_vars=["completed", "on_track", "at_risk", "not_started"],
                var_name="cat", value_name="count",
            )
            stack["Category"] = stack["cat"].map(ACHIEVEMENT_STATUS_LABEL_MAP)
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

    qual_df["stage_norm"] = (
        qual_df["qualitative_stage"]
        .fillna("Not Started")
        .str.strip()
        .str.title()
    )
    qual_df["stage_norm"] = qual_df["stage_norm"].where(
        qual_df["stage_norm"].isin(QUAL_STAGE_ORDER), "Not Started"
    )

    stage_counts = (
        qual_df["stage_norm"]
        .value_counts()
        .reindex(QUAL_STAGE_ORDER, fill_value=0)
        .reset_index()
    )
    stage_counts.columns = ["Stage", "Count"]
    stage_counts = stage_counts[stage_counts["Count"] > 0]
    stage_counts["% of Total"] = (
        stage_counts["Count"] / total_qual * 100
    ).round(1).astype(str) + "%"
    stage_counts["Color"] = stage_counts["Stage"].map(QUAL_STAGE_COLORS)

    col_f, col_t = st.columns([2, 1])
    with col_f:
        plot_sc = stage_counts.iloc[::-1]
        fig3 = px.bar(
            plot_sc, x="Count", y="Stage",
            orientation="h", text="Count",
            color="Stage",
            color_discrete_map=QUAL_STAGE_COLORS,
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
            stage_counts[["Stage", "Count", "% of Total"]].set_index("Stage"),
            use_container_width=True,
        )
        show_download_button(stage_counts[["Stage", "Count", "% of Total"]],
                             "qualitative_stages.csv")

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
            color_discrete_map=QUAL_STAGE_COLORS,
            category_orders={"stage_norm": QUAL_STAGE_ORDER},
            title="Qualitative Indicator Stages by Entity",
            text_auto=True, barmode="stack",
        )
        fig4.update_layout(height=380, xaxis_title="", legend_title="Stage")
        st.plotly_chart(fig4, use_container_width=True)

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
