"""
utils/email_utils.py
====================
OTP email sender used for email verification and password reset.

Required environment variables:
    SMTP_HOST      (default: smtp.gmail.com)
    SMTP_PORT      (default: 587)
    SMTP_USER      Gmail address or SMTP username
    SMTP_PASSWORD  Gmail App Password (not your account password)
    FROM_EMAIL     Defaults to SMTP_USER

Gmail setup:
    1. Enable 2FA on your Google account
    2. Go to myaccount.google.com → Security → App Passwords
    3. Generate a password for "Mail" and paste it into SMTP_PASSWORD
"""

import os
import secrets
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def generate_otp(length: int = 6) -> str:
    """Return a cryptographically random numeric OTP string."""
    return "".join(str(secrets.randbelow(10)) for _ in range(length))


def otp_expiry(minutes: int = 15) -> datetime:
    return datetime.utcnow() + timedelta(minutes=minutes)


def send_otp_email(to_email: str, name: str, otp: str, purpose: str = "verify") -> bool:
    """
    Send an OTP email. Returns True on success, False on failure.

    purpose: "verify"  → email verification after registration
             "reset"   → password reset
    """
    smtp_host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "")
    smtp_pass = os.getenv("SMTP_PASSWORD", "")
    from_email = os.getenv("FROM_EMAIL", smtp_user)

    if not smtp_user or not smtp_pass:
        return False   # email not configured — caller handles the fallback

    # otp can be a 6-digit code OR a full URL (for link-based flows)
    is_link = otp.startswith("http")

    subjects = {
        "verify":       "Pandemic Fund M&E — Verify your email",
        "verify_link":  "Pandemic Fund M&E — Confirm your email",
        "reset":        "Pandemic Fund M&E — Password reset code",
        "reset_link":   "Pandemic Fund M&E — Reset your password",
    }
    subject = subjects.get(purpose, "Pandemic Fund M&E — Action required")

    if is_link:
        action_label = "Reset Password" if "reset" in purpose else "Verify Email"
        body = f"""
        <p>Click the button below. The link expires in <strong>1 hour</strong>.</p>
        <div style="text-align:center;margin:32px 0">
          <a href="{otp}"
             style="background:#2c3e50;color:#fff;padding:14px 28px;
                    border-radius:6px;text-decoration:none;font-size:1rem">
            {action_label}
          </a>
        </div>
        <p style="color:#7f8c8d;font-size:0.85em">
          Or copy this link: <a href="{otp}">{otp}</a>
        </p>"""
    else:
        intro = ("Thank you for registering. Use the code below to verify your email."
                 if "verify" in purpose
                 else "We received a password reset request. Use the code below.")
        body = f"""
        <p>{intro}</p>
        <div style="background:#f0f4f8;border-radius:8px;padding:24px;
                    text-align:center;margin:24px 0">
          <span style="font-size:36px;font-weight:bold;
                       letter-spacing:8px;color:#2c3e50">{otp}</span>
        </div>
        <p style="color:#7f8c8d;font-size:0.85em">
          Expires in <strong>15 minutes</strong>.
        </p>"""

    html = f"""
    <div style="font-family:Arial,sans-serif;max-width:480px;margin:0 auto">
      <h2 style="color:#2c3e50">Pandemic Fund M&amp;E</h2>
      <p>Hello <strong>{name}</strong>,</p>
      {body}
      <p style="color:#7f8c8d;font-size:0.85em">
        If you did not request this, you can safely ignore this email.
      </p>
    </div>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_email
    msg["To"]      = to_email
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(from_email, to_email, msg.as_string())
        return True
    except Exception:
        return False
