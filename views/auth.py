"""
pages/auth.py
=============
Login, registration, and password-reset pages.
"""

import bcrypt
import streamlit as st

from utils.db import (
    db_get_user,
    db_get_user_by_email,
    db_username_exists,
    db_email_exists,
    db_register_user,
    db_update_last_login,
    db_update_password,
)
from utils.helpers import AUTH_CSS


# ── Internal helpers ──────────────────────────────────────────────────────────

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


# ── Page renderers ────────────────────────────────────────────────────────────

def render_login_page() -> None:
    if st.session_state.get("authenticated"):
        return
    st.html(AUTH_CSS)
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


def render_register_page() -> None:
    st.html(AUTH_CSS)
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
        if len(password) < 8:      errors.append("Password must be at least 8 characters.")
        if password != confirm_pw: errors.append("Passwords do not match.")
        if errors:
            for e in errors:
                st.error(e)
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


def render_forgot_password_page() -> None:
    st.html(AUTH_CSS)
    st.title("🦠 Pandemic Fund M&E")
    st.caption("Monitoring & Evaluation Intelligence Dashboard")
    st.divider()
    st.subheader("Reset Password")

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
                st.session_state.pop("reset_step",     None)
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
