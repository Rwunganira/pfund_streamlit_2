"""
flask_auth/routes.py
====================
Auth routes:
  GET/POST  /auth/login
  GET/POST  /auth/register
  GET       /auth/verify-email?token=<token>&u=<username>
  GET/POST  /auth/forgot-password
  GET/POST  /auth/reset-password?token=<token>&u=<username>
  GET       /auth/logout
  GET       /auth/dashboard   ← issues JWT and redirects to Streamlit
"""

import os
import secrets
from datetime import datetime, timedelta

import bcrypt
from flask import redirect, render_template, request, session, url_for

from flask_auth import auth_bp
from utils.db import (
    db_get_user,
    db_get_user_by_email,
    db_username_exists,
    db_email_exists,
    db_register_user,
    db_update_last_login,
    db_update_password,
    db_set_token,
    db_verify_token,
    db_mark_email_verified,
)
from utils.email_utils import send_otp_email
from utils.jwt_utils import create_dashboard_token


STREAMLIT_URL = os.getenv("STREAMLIT_URL", "http://localhost:8501")


def _hash(pw: str) -> str:
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()


def _check(plain: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


def _gen_token() -> str:
    return secrets.token_urlsafe(32)


def _expiry(hours: int = 1) -> datetime:
    return datetime.utcnow() + timedelta(hours=hours)


# ── Login ─────────────────────────────────────────────────────────────────────

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        user = db_get_user(username)
        if not user or not _check(password, user["password_hash"]):
            error = "Incorrect username or password."
        elif not user.get("email_verified"):
            # Re-send verification link
            token = _gen_token()
            db_set_token(username, token, _expiry(hours=24))
            _send_verify_link(user, token)
            error = "Please verify your email first. A new link has been sent."
        else:
            session["username"] = username
            db_update_last_login(username)
            return redirect(url_for("auth.dashboard"))

    return render_template("auth/login.html", error=error)


# ── Register ──────────────────────────────────────────────────────────────────

@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    errors = []
    if request.method == "POST":
        name       = request.form.get("name", "").strip()
        username   = request.form.get("username", "").strip()
        email      = request.form.get("email", "").strip()
        password   = request.form.get("password", "")
        confirm_pw = request.form.get("confirm_password", "")

        if not name:                        errors.append("Full name is required.")
        if not username:                    errors.append("Username is required.")
        elif db_username_exists(username):  errors.append("Username already taken.")
        if not email:                       errors.append("Email is required.")
        elif db_email_exists(email):        errors.append("Email already registered.")
        if len(password) < 8:              errors.append("Password must be at least 8 characters.")
        if password != confirm_pw:         errors.append("Passwords do not match.")

        if not errors:
            ok, msg = db_register_user(username, name, email, password)
            if ok:
                token = _gen_token()
                db_set_token(username, token, _expiry(hours=24))
                _send_verify_link(db_get_user(username), token)
                return render_template("auth/verify_email.html",
                                       sent=True, email=email)
            else:
                errors.append(msg)

    return render_template("auth/register.html", errors=errors)


# ── Email verification ────────────────────────────────────────────────────────

def _send_verify_link(user: dict, token: str) -> None:
    link = url_for("auth.verify_email", u=user["username"], token=token,
                   _external=True)
    send_otp_email(user["email"], user["name"], link, purpose="verify_link")


@auth_bp.route("/verify-email")
def verify_email():
    username = request.args.get("u", "")
    token    = request.args.get("token", "")

    if db_verify_token(username, token):
        db_mark_email_verified(username)
        return render_template("auth/verify_email.html",
                               success=True, login_url=url_for("auth.login"))
    return render_template("auth/verify_email.html",
                           expired=True, login_url=url_for("auth.login"))


# ── Forgot password ───────────────────────────────────────────────────────────

@auth_bp.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    sent = False
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        user  = db_get_user_by_email(email)
        if user:
            token = _gen_token()
            db_set_token(user["username"], token, _expiry(hours=1))
            link  = url_for("auth.reset_password",
                            u=user["username"], token=token, _external=True)
            send_otp_email(user["email"], user["name"], link, purpose="reset_link")
        sent = True   # always show success to prevent email enumeration

    return render_template("auth/forgot_password.html", sent=sent)


# ── Reset password ────────────────────────────────────────────────────────────

@auth_bp.route("/reset-password", methods=["GET", "POST"])
def reset_password():
    username = request.args.get("u", "")
    token    = request.args.get("token", "")
    error    = None

    if not db_verify_token(username, token):
        return render_template("auth/reset_password.html",
                               expired=True, login_url=url_for("auth.login"))

    if request.method == "POST":
        new_pw     = request.form.get("password", "")
        confirm_pw = request.form.get("confirm_password", "")

        if len(new_pw) < 8:
            error = "Password must be at least 8 characters."
        elif new_pw != confirm_pw:
            error = "Passwords do not match."
        else:
            db_update_password(username, new_pw)
            db_mark_email_verified(username)   # clears token
            return render_template("auth/reset_password.html",
                                   success=True, login_url=url_for("auth.login"))

    return render_template("auth/reset_password.html",
                           username=username, token=token, error=error)


# ── Logout ────────────────────────────────────────────────────────────────────

@auth_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("auth.login"))


# ── Dashboard redirect ────────────────────────────────────────────────────────

@auth_bp.route("/dashboard")
def dashboard():
    """Issue a short-lived JWT and redirect to Streamlit."""
    username = session.get("username")
    if not username:
        return redirect(url_for("auth.login"))

    user  = db_get_user(username)
    if not user:
        session.clear()
        return redirect(url_for("auth.login"))

    jwt_token = create_dashboard_token(
        username = user["username"],
        name     = user["name"],
        role     = user["role"],
        email    = user["email"],
    )
    return redirect(f"{STREAMLIT_URL}?token={jwt_token}")
