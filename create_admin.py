"""
One-time script: promote an existing user to admin and mark email verified.
Usage:
    python create_admin.py <username>
"""
import sys
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from sqlalchemy import create_engine, text

if len(sys.argv) < 2:
    raise SystemExit("Usage: python create_admin.py <username>")

username = sys.argv[1]

raw_url = os.getenv("WAREHOUSE_URL") or os.getenv("DATABASE_URL", "")
if not raw_url:
    raise SystemExit("DATABASE_URL / WAREHOUSE_URL not set in .env")

db_url = raw_url.replace("postgres://", "postgresql://", 1)
is_remote = "amazonaws.com" in db_url
connect_args = {"sslmode": "require"} if is_remote else {}

engine = create_engine(db_url, connect_args=connect_args)

with engine.begin() as conn:
    row = conn.execute(
        text("SELECT username, name, role FROM app_users WHERE username=:u"),
        {"u": username},
    ).fetchone()

    if not row:
        raise SystemExit(f"User '{username}' not found in app_users.")

    conn.execute(
        text("UPDATE app_users "
             "SET role='admin', email_verified=TRUE "
             "WHERE username=:u"),
        {"u": username},
    )

print(f"✅ '{username}' ({row[1]}) is now admin and email-verified.")
engine.dispose()
