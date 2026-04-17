"""
Pandemic Fund — Combined M&E Dashboard
========================================
Entry point only. All logic lives in views/ and utils/.

Authentication is handled by the Flask app (flask_auth blueprint).
Flask issues a JWT → redirects here with ?token=<jwt> → validated on load.

Run ETL first:
    python -m etl.run_etl

Start the dashboard:
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
from views.auth import bootstrap_auth
from views.activities import render_activities_dashboard
from views.indicators import render_indicator_dashboard
from views.management_tracker import render_management_tracker
from views.admin import render_admin_panel


def main() -> None:
    # Ensure DB schema is up to date
    try:
        ensure_users_table()
    except Exception as exc:
        st.error(f"Database error: {exc}")
        st.stop()

    # Validate JWT from query params or existing session — stops if not authed
    bootstrap_auth()

    display_name = st.session_state.get("display_name", "User")
    role         = st.session_state.get("role", "analyst")

    # Build nav options based on role
    options = ["📊 Portfolio Activities", "🦠 Indicator Tracker", "📋 Management Tracker"]
    if role == "admin":
        options.append("⚙️ Admin Panel")

    with st.sidebar:
        st.markdown(f"👤 **{display_name}**")
        st.caption(f"Role: {role}")
        if st.button("Logout", key="logout_btn"):
            st.session_state.clear()
            st.rerun()
        st.divider()
        st.markdown("## 📊 Pandemic Fund M&E")
        st.divider()
        dashboard = st.radio(
            "Select Dashboard",
            options,
            key="dashboard_selector",
        )
        st.divider()

    if dashboard == "📊 Portfolio Activities":
        render_activities_dashboard()
    elif dashboard == "🦠 Indicator Tracker":
        render_indicator_dashboard()
    elif dashboard == "📋 Management Tracker":
        render_management_tracker()
    elif dashboard == "⚙️ Admin Panel":
        render_admin_panel()


if __name__ == "__main__":
    main()
