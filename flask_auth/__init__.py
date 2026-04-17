"""
flask_auth
==========
Auth blueprint for the rwpfund Flask app.

Register in your Flask app:

    from flask_auth import auth_bp
    app.register_blueprint(auth_bp)

Required env vars (same .env as Streamlit):
    DATABASE_URL / WAREHOUSE_URL
    JWT_SECRET_KEY
    STREAMLIT_URL      e.g. https://your-app.streamlit.app
    SMTP_USER, SMTP_PASSWORD, SMTP_HOST, SMTP_PORT, FROM_EMAIL
"""

from flask import Blueprint

auth_bp = Blueprint(
    "auth",
    __name__,
    template_folder="templates",
    url_prefix="/auth",
)

from flask_auth import routes  # noqa: E402, F401 — registers routes on bp
