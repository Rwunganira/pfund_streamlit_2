"""
utils/db.py
===========
Database engine (singleton) and all user-table CRUD functions.
"""

import os

import bcrypt
import streamlit as st
from sqlalchemy import create_engine, text


@st.cache_resource
def _get_engine():
    # WAREHOUSE_URL → local/cloud warehouse where mart.* tables live
    # Falls back to DATABASE_URL for Streamlit Cloud deployments where a single
    # DB is used (mart schema must exist there in that case).
    raw_url = os.getenv("WAREHOUSE_URL") or os.getenv("DATABASE_URL", "")
    if not raw_url:
        st.error(
            "Neither WAREHOUSE_URL nor DATABASE_URL is set. "
            "Add the correct URL to your .env file and restart."
        )
        st.stop()
    db_url = (
        raw_url.replace("postgres://", "postgresql://", 1)
        if raw_url.startswith("postgres://")
        else raw_url
    )
    is_remote = "amazonaws.com" in db_url or "heroku" in db_url
    connect_args = {"sslmode": "require"} if is_remote else {}
    return create_engine(db_url, pool_pre_ping=True, connect_args=connect_args)


# ── Schema bootstrap ───────────────────────────────────────────────────────────

def ensure_users_table() -> None:
    with _get_engine().connect() as conn:
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


# ── User queries ───────────────────────────────────────────────────────────────

def db_get_user(username: str) -> dict | None:
    with _get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM app_users WHERE username=:u AND is_active=TRUE"),
            {"u": username},
        ).fetchone()
    return dict(row._mapping) if row else None


def db_username_exists(username: str) -> bool:
    with _get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM app_users WHERE username=:u"), {"u": username}
        ).fetchone()
    return row is not None


def db_email_exists(email: str) -> bool:
    if not email:
        return False
    with _get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT 1 FROM app_users WHERE email=:e"), {"e": email}
        ).fetchone()
    return row is not None


def db_register_user(username, name, email, password, role="analyst"):
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    try:
        with _get_engine().begin() as conn:
            conn.execute(
                text("INSERT INTO app_users (username,name,email,password_hash,role) "
                     "VALUES (:u,:n,:e,:p,:r)"),
                {"u": username, "n": name, "e": email or None,
                 "p": pw_hash,  "r": role},
            )
        return True, "Account created successfully."
    except Exception as exc:
        msg = str(exc).lower()
        if "unique" in msg or "duplicate" in msg:
            return False, "Username or email already in use."
        return False, f"Registration failed: {exc}"


def db_update_last_login(username: str) -> None:
    with _get_engine().begin() as conn:
        conn.execute(
            text("UPDATE app_users SET last_login=NOW() WHERE username=:u"),
            {"u": username},
        )


def db_get_user_by_email(email: str) -> dict | None:
    with _get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM app_users "
                 "WHERE LOWER(email)=LOWER(:e) AND is_active=TRUE"),
            {"e": email},
        ).fetchone()
    return dict(row._mapping) if row else None


def db_update_password(username: str, new_password: str) -> None:
    pw_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
    with _get_engine().begin() as conn:
        conn.execute(
            text("UPDATE app_users SET password_hash=:h WHERE username=:u"),
            {"h": pw_hash, "u": username},
        )
