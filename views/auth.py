"""
views/auth.py
=============
Streamlit auth gate — validates the JWT issued by the Flask auth app.
No login/register forms live here anymore; all auth happens in Flask.
"""

import os
import streamlit as st

from utils.jwt_utils import validate_dashboard_token
from utils.helpers import AUTH_CSS

FLASK_AUTH_URL = os.getenv("FLASK_AUTH_URL", "http://localhost:5000")


def bootstrap_auth() -> bool:
    """
    Called once at app startup.

    1. If a ?token=<jwt> query param is present, validate it and store
       user info in session_state, then clear the param from the URL.
    2. If already authenticated in session_state, do nothing.
    3. If neither, show the "please login" gate and call st.stop().

    Returns True if the user is authenticated.
    """
    # ── Step 1: token in URL ──────────────────────────────────────────────────
    params = st.query_params
    raw_token = params.get("token", "")

    if raw_token:
        payload = validate_dashboard_token(raw_token)
        if payload:
            st.session_state.update({
                "authenticated": True,
                "username":      payload["sub"],
                "display_name":  payload["name"],
                "role":          payload["role"],
                "email":         payload.get("email", ""),
            })
            # Remove token from URL (security: don't leave it in browser history)
            st.query_params.clear()
            st.rerun()
        else:
            # Token present but invalid / expired — show error then gate
            st.session_state["authenticated"] = False
            _render_gate(error="Your session has expired. Please sign in again.")
            return False

    # ── Step 2: already authenticated ────────────────────────────────────────
    if st.session_state.get("authenticated"):
        return True

    # ── Step 3: not authenticated — show gate ────────────────────────────────
    _render_gate()
    return False


def _render_gate(error: str = "") -> None:
    """Full-page 'please login via Flask' screen."""
    st.html(AUTH_CSS)
    st.title("🦠 Pandemic Fund M&E")
    st.caption("Monitoring & Evaluation Intelligence Dashboard")
    st.divider()

    if error:
        st.warning(error)

    st.info("Please sign in to access the dashboard.")

    login_url = f"{FLASK_AUTH_URL}/auth/login"
    st.markdown(
        f'<a href="{login_url}" target="_self">'
        f'<button style="background:#2c3e50;color:#fff;border:none;'
        f'padding:0.6rem 1.5rem;border-radius:6px;font-size:1rem;cursor:pointer">'
        f'Sign in →</button></a>',
        unsafe_allow_html=True,
    )
    st.stop()
