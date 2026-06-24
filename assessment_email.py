"""SMTP email sender + reminder template for the Distributor Operational
Assessment app. Mirrors the pattern already used in po_portal/utils/email_utils.py
(same Gmail SMTP approach, same st.secrets keys) so both apps share one
convention instead of inventing a second one.
"""
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import streamlit as st


def _smtp_cfg():
    return {
        "host": "smtp.gmail.com",
        "port": 587,
        "user": st.secrets["smtp"]["user"],
        "pass": st.secrets["smtp"]["password"],
    }


def get_app_url() -> str:
    """Falls back to a placeholder if no secrets.toml exists at all (e.g. local
    mock testing) — st.secrets raises on first access in that case, even via .get()."""
    try:
        return st.secrets.get("app", {}).get("url", "https://your-app.streamlit.app").rstrip("/")
    except Exception:
        return "https://your-app.streamlit.app"


def send_email(to_list, subject, html_body) -> bool:
    """Send one HTML email to one or more recipients. Returns True on success."""
    if not to_list:
        return False
    cfg = _smtp_cfg()
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = cfg["user"]
    msg["To"] = ", ".join(to_list)
    msg.attach(MIMEText(html_body, "html"))
    try:
        with smtplib.SMTP(cfg["host"], cfg["port"]) as srv:
            srv.starttls()
            srv.login(cfg["user"], cfg["pass"])
            srv.sendmail(cfg["user"], to_list, msg.as_string())
        return True
    except Exception as e:
        st.warning(f"⚠️ Email failed to send: {e}")
        return False


def build_ass_reminder_email(full_name: str, period: str) -> tuple[str, str]:
    """Returns (subject, html_body) reminding one Area Sales Supervisor that
    they haven't submitted their assessment for the given period yet."""
    url = get_app_url()
    subject = f"⏰ Reminder: Submit Your ASS Assessment — {period}"
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <meta charset="utf-8">
    <style>
      body {{ font-family: Arial, sans-serif; background:#F5F7FA; margin:0; padding:0; }}
      .wrapper {{ max-width:600px; margin:32px auto; background:#fff;
                  border-radius:12px; overflow:hidden;
                  box-shadow:0 2px 12px rgba(0,0,0,.08); }}
      .header {{ background:#1E6B8A; padding:28px 36px; }}
      .header h1 {{ color:#fff; margin:0; font-size:1.3rem; }}
      .header p  {{ color:#BDE4F4; margin:4px 0 0; font-size:.85rem; }}
      .body   {{ padding:28px 36px; color:#333; line-height:1.6; }}
      .cta {{ display:inline-block; margin-top:20px; padding:12px 28px;
              background:#1E6B8A; color:#fff !important; text-decoration:none;
              border-radius:8px; font-weight:700; font-size:.95rem; }}
      .footer {{ background:#F0F4F8; padding:14px 36px;
                 color:#999; font-size:.78rem; text-align:center; }}
    </style>
    </head>
    <body>
    <div class="wrapper">
      <div class="header">
        <h1>📋 Distributor Operational Assessment</h1>
        <p>SKINTIFIC</p>
      </div>
      <div class="body">
        <h2 style="margin-top:0;color:#1E3A4A">Assessment Reminder — {period}</h2>
        <p>Hi {full_name},</p>
        <p>Our records show you have <strong>not yet submitted</strong> your
        Area Sales Supervisor assessment for <strong>{period}</strong> in the
        Distributor Operational Assessment app.</p>
        <p>Please log in and complete your submission for each of your
        assigned distributors as soon as possible.</p>
        <a href="{url}" class="cta">📋 Submit Now</a>
        <p style="margin-top:24px;font-size:.85rem;color:#777;">
        If you've already submitted or believe this is a mistake, please
        contact your administrator.</p>
      </div>
      <div class="footer">
        This is an automated reminder from the Distributor Operational Assessment app.<br>
        Do not reply to this email.
      </div>
    </div>
    </body>
    </html>
    """
    return subject, html
