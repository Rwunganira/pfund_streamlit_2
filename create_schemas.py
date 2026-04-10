"""One-time script: creates mart/stg/dwh schemas on the remote database."""
import os
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from sqlalchemy import create_engine, text

url = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
if not url:
    raise SystemExit("DATABASE_URL not set in .env")

# Heroku Postgres requires SSL
if "?" not in url:
    url += "?sslmode=require"
engine = create_engine(url, connect_args={"sslmode": "require"})
with engine.begin() as conn:
    for schema in ("stg", "dwh", "mart"):
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        print(f"  schema '{schema}' ready")

print("Done.")
engine.dispose()
