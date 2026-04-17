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
                id                 SERIAL PRIMARY KEY,
                username           VARCHAR(50)  UNIQUE NOT NULL,
                name               VARCHAR(100) NOT NULL,
                email              VARCHAR(150) UNIQUE NOT NULL,
                password_hash      VARCHAR(255) NOT NULL,
                role               VARCHAR(20)  NOT NULL DEFAULT 'analyst',
                is_active          BOOLEAN      NOT NULL DEFAULT TRUE,
                email_verified     BOOLEAN      NOT NULL DEFAULT FALSE,
                verification_token VARCHAR(10),
                token_expires_at   TIMESTAMP,
                created_at         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
                last_login         TIMESTAMP
            )
        """))
        # Migration: add new columns if table already exists without them
        for stmt in [
            "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN NOT NULL DEFAULT FALSE",
            "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS verification_token VARCHAR(10)",
            "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS token_expires_at TIMESTAMP",
        ]:
            conn.execute(text(stmt))
        conn.commit()


# ── User queries ───────────────────────────────────────────────────────────────

def db_get_user(username: str) -> dict | None:
    with _get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM app_users WHERE username=:u AND is_active=TRUE"),
            {"u": username},
        ).fetchone()
    return dict(row._mapping) if row else None


def db_get_user_by_email(email: str) -> dict | None:
    with _get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM app_users WHERE LOWER(email)=LOWER(:e) AND is_active=TRUE"),
            {"e": email},
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
            text("SELECT 1 FROM app_users WHERE LOWER(email)=LOWER(:e)"), {"e": email}
        ).fetchone()
    return row is not None


def db_register_user(username, name, email, password, role="analyst"):
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    try:
        with _get_engine().begin() as conn:
            conn.execute(
                text("INSERT INTO app_users "
                     "(username, name, email, password_hash, role, email_verified) "
                     "VALUES (:u, :n, :e, :p, :r, FALSE)"),
                {"u": username, "n": name, "e": email, "p": pw_hash, "r": role},
            )
        return True, "Account created."
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


def db_update_password(username: str, new_password: str) -> None:
    pw_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
    with _get_engine().begin() as conn:
        conn.execute(
            text("UPDATE app_users SET password_hash=:h WHERE username=:u"),
            {"h": pw_hash, "u": username},
        )


# ── OTP / verification ─────────────────────────────────────────────────────────

def db_set_token(username: str, token: str, expires_at) -> None:
    with _get_engine().begin() as conn:
        conn.execute(
            text("UPDATE app_users "
                 "SET verification_token=:t, token_expires_at=:e "
                 "WHERE username=:u"),
            {"t": token, "e": expires_at, "u": username},
        )


def db_verify_token(username: str, token: str) -> bool:
    """Return True if token matches and has not expired."""
    from datetime import datetime
    with _get_engine().connect() as conn:
        row = conn.execute(
            text("SELECT verification_token, token_expires_at "
                 "FROM app_users WHERE username=:u"),
            {"u": username},
        ).fetchone()
    if not row:
        return False
    stored, expires = row[0], row[1]
    if stored != token:
        return False
    if expires and datetime.utcnow() > expires:
        return False
    return True


def db_mark_email_verified(username: str) -> None:
    with _get_engine().begin() as conn:
        conn.execute(
            text("UPDATE app_users "
                 "SET email_verified=TRUE, "
                 "    verification_token=NULL, token_expires_at=NULL "
                 "WHERE username=:u"),
            {"u": username},
        )


# ── Admin functions ────────────────────────────────────────────────────────────

def db_list_users() -> list[dict]:
    with _get_engine().connect() as conn:
        rows = conn.execute(text(
            "SELECT id, username, name, email, role, is_active, "
            "       email_verified, created_at, last_login "
            "FROM app_users ORDER BY created_at DESC"
        )).fetchall()
    return [dict(r._mapping) for r in rows]


def db_set_user_active(username: str, active: bool) -> None:
    with _get_engine().begin() as conn:
        conn.execute(
            text("UPDATE app_users SET is_active=:a WHERE username=:u"),
            {"a": active, "u": username},
        )


def db_set_user_role(username: str, role: str) -> None:
    with _get_engine().begin() as conn:
        conn.execute(
            text("UPDATE app_users SET role=:r WHERE username=:u"),
            {"r": role, "u": username},
        )
