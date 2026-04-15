"""
Pandemic Fund — Combined M&E Dashboard
========================================
Entry point only. All logic lives in pages/ and utils/.

Run ETL first:
    python -m etl.run_etl

Then start the dashboard:
    streamlit run dashboard.py
"""

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import streamlit as st

st.set_page_config(
    page_title="Pandemic Fund M&E",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

from utils.db import ensure_users_table
from views.auth import render_login_page, render_register_page, render_forgot_password_page
from views.activities import render_activities_dashboard
from views.indicators import render_indicator_dashboard
from views.management_tracker import render_management_tracker


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
            for k in ("authenticated", "username", "display_name", "role"):
                st.session_state[k] = False if k == "authenticated" else ""
            st.session_state["auth_page"] = "login"
            st.rerun()
        st.divider()
        st.markdown("## 📊 Pandemic Fund M&E")
        st.divider()
        dashboard = st.radio(
            "Select Dashboard",
            ["📊 Portfolio Activities", "🦠 Indicator Tracker", "📋 Management Tracker"],
            key="dashboard_selector",
        )
        st.divider()

    if dashboard == "📊 Portfolio Activities":
        render_activities_dashboard()
    elif dashboard == "🦠 Indicator Tracker":
        render_indicator_dashboard()
    else:
        render_management_tracker()


if __name__ == "__main__":
    main()
