"""
views/auth.py
=============
Streamlit auth gate — validates the JWT issued by the Flask auth app.
Persists the token in a browser cookie so page refreshes don't require re-login.
"""

import os
from datetime import datetime, timedelta

import extra_streamlit_components as stx
import streamlit as st

from utils.jwt_utils import validate_dashboard_token
from utils.helpers import AUTH_CSS

FLASK_AUTH_URL = os.getenv("FLASK_AUTH_URL", "http://localhost:5000")

_COOKIE_NAME = "pfund_auth_token"
_COOKIE_EXPIRY_HOURS = 8


def get_cookie_manager():
    return stx.CookieManager(key="__pfund_cookie_mgr__")


def bootstrap_auth() -> bool:
    """
    Called once at app startup.

    1. If a ?token=<jwt> query param is present, validate it, store user info
       in session_state, save token to cookie, then clear the URL param.
    2. If already authenticated in session_state, do nothing.
    3. If a valid auth cookie exists, restore session_state from it.
    4. If none of the above, show the login gate and call st.stop().

    Returns True if the user is authenticated.
    """
    cm = get_cookie_manager()

    # ── Step 1: token in URL ──────────────────────────────────────────────────
    raw_token = st.query_params.get("token", "")

    if raw_token:
        payload = validate_dashboard_token(raw_token)
        if payload:
            _set_session(payload)
            cm.set(
                _COOKIE_NAME,
                raw_token,
                expires_at=datetime.now() + timedelta(hours=_COOKIE_EXPIRY_HOURS),
            )
            st.query_params.clear()
            st.rerun()
        else:
            st.session_state["authenticated"] = False
            _render_gate(error="Your session has expired. Please sign in again.")
            return False

    # ── Step 2: already authenticated in session ──────────────────────────────
    if st.session_state.get("authenticated"):
        return True

    # ── Step 3: check cookie ──────────────────────────────────────────────────
    stored_token = cm.get(cookie=_COOKIE_NAME)
    if stored_token:
        payload = validate_dashboard_token(stored_token)
        if payload:
            _set_session(payload)
            return True
        else:
            # Cookie exists but JWT has expired — clear it
            cm.delete(_COOKIE_NAME)

    # ── Step 4: not authenticated — show gate ────────────────────────────────
    _render_gate()
    return False


def logout() -> None:
    """Clear session and delete the auth cookie, then rerun."""
    get_cookie_manager().delete(_COOKIE_NAME)
    st.session_state.clear()
    st.rerun()


def _set_session(payload: dict) -> None:
    st.session_state.update({
        "authenticated": True,
        "username":      payload["sub"],
        "display_name":  payload["name"],
        "role":          payload["role"],
        "email":         payload.get("email", ""),
    })


def _render_gate(error: str = "") -> None:
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
