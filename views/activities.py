"""
pages/activities.py
===================
Portfolio Activities Dashboard — render_activities_dashboard().
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from utils.helpers import ACT_STATUS_COLORS, MOBILE_CSS, safe_mean, show_download_button
from utils.loaders import load_mart_budget_performance, load_mart_activity_status


def render_activities_dashboard() -> None:
    st.html(MOBILE_CSS)
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

        year_opts   = sorted(bud_df["year_number"].dropna().unique().tolist())
        sel_years   = st.multiselect("Budget Year", options=year_opts,
                                     default=year_opts, key="act_years",
                                     format_func=lambda x: f"Year {x}")
        active_years = sel_years or year_opts

        def _ms(label, col, key):
            opts = sorted(bud_df[col].dropna().unique().tolist())
            return st.multiselect(label, options=opts, key=key)

        sel_entity   = _ms("Implementing Entity", "entity_name",     "act_ent")
        sel_area     = _ms("Results Area",         "results_area",    "act_ra")
        sel_partner  = _ms("Delivery Partner",     "delivery_partner","act_dp")
        sel_category = _ms("Category",             "category",        "act_cat")
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
                sc = (fdf.drop_duplicates("activity_code")["status"]
                      .value_counts().reset_index())
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
        status_year = max(active_years)
        sf = status_df[status_df["year_number"] == status_year].copy()
        if sel_entity: sf = sf[sf["entity_name"].isin(sel_entity)]
        if sel_area:   sf = sf[sf["results_area"].isin(sel_area)]

        ignored  = [f for f, v in [("Delivery Partner", sel_partner),
                                    ("Category", sel_category)] if v]
        captions = []
        if len(active_years) > 1:
            captions.append(
                f"Status as of Year {status_year} (latest selected; "
                "status is not year-specific)."
            )
        if ignored:
            captions.append(
                f"Note: {', '.join(ignored)} filter(s) not applied here "
                "— not tracked in status data."
            )
        for c in captions:
            st.caption(c)

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
            show_download_button(low_df[display_cols],
                                 f"low_performing_{cutoff_pct}pct.csv")

    # ── Tab 5: Activity Timeline ──────────────────────────────────────────────
    with tab5:
        _act_tab_timeline(fdf)


# ── Timeline tab (extracted for readability) ──────────────────────────────────

def _act_tab_timeline(fdf: pd.DataFrame) -> None:
    st.subheader("Activity Timeline")
    st.caption("Activities plotted by start and end date. Red line = today.")

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

    n_overdue  = int(gantt_df["overdue"].sum())
    n_upcoming = int((gantt_df["start_date"] > today).sum())
    n_active   = int(
        ((gantt_df["start_date"] <= today) & (gantt_df["end_date"] >= today)).sum()
    )
    c1, c2, c3 = st.columns(3)
    c1.metric("Currently Active", n_active)
    c2.metric("Overdue", n_overdue,
              delta=f"-{n_overdue}" if n_overdue else None, delta_color="inverse")
    c3.metric("Not Yet Started", n_upcoming)

    st.divider()

    gantt_df["display_status"] = gantt_df.apply(
        lambda r: "Overdue" if r["overdue"] else r["status"], axis=1
    )
    color_map = {**ACT_STATUS_COLORS, "Overdue": "#c0392b"}
    gantt_df  = gantt_df.sort_values(["overdue", "end_date"], ascending=[False, True])

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

    if n_overdue:
        st.divider()
        st.markdown(f"#### ⚠️ Overdue Activities ({n_overdue})")
        over_cols = [c for c in [
            "activity_code", "proposed_activity", "entity_name",
            "results_area", "end_date", "status", "execution_rate",
        ] if c in gantt_df.columns]
        st.dataframe(
            gantt_df[gantt_df["overdue"]][over_cols].sort_values("end_date"),
            use_container_width=True,
        )
        show_download_button(
            gantt_df[gantt_df["overdue"]][over_cols],
            "overdue_activities.csv",
        )
