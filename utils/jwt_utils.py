"""
utils/jwt_utils.py
==================
Shared JWT helpers used by both Flask (sign) and Streamlit (validate).

Environment variable required (same value in both apps):
    JWT_SECRET_KEY   — long random string, e.g. `python -c "import secrets; print(secrets.token_hex(32))"`
"""

import os
from datetime import datetime, timedelta, timezone

import jwt  # PyJWT


_SECRET = None


def _secret() -> str:
    global _SECRET
    if _SECRET is None:
        _SECRET = os.getenv("JWT_SECRET_KEY", "")
        if not _SECRET:
            raise EnvironmentError(
                "JWT_SECRET_KEY is not set. "
                "Add it to .env and to Streamlit / Heroku secrets."
            )
    return _SECRET


def create_dashboard_token(
    username: str,
    name: str,
    role: str,
    email: str,
    expires_minutes: int = 60,
) -> str:
    """
    Create a signed JWT to be passed as ?token=<jwt> when redirecting
    from Flask to Streamlit.
    """
    payload = {
        "sub":   username,
        "name":  name,
        "role":  role,
        "email": email,
        "iat":   datetime.now(timezone.utc),
        "exp":   datetime.now(timezone.utc) + timedelta(minutes=expires_minutes),
    }
    return jwt.encode(payload, _secret(), algorithm="HS256")


def validate_dashboard_token(token: str) -> dict | None:
    """
    Validate a dashboard JWT.
    Returns the payload dict on success, None on failure / expiry.
    """
    try:
        payload = jwt.decode(token, _secret(), algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None
