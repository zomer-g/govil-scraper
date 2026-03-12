"""
Google OAuth2 SSO authentication for Gov.il Scraper.

Only admin users (listed in ADMIN_EMAILS env var) can trigger scrapes.
The collections API is fully public — no auth required.
"""

import os
import logging
import functools

from flask import (
    Blueprint, redirect, url_for, session, request, jsonify, current_app,
)
from authlib.integrations.flask_client import OAuth

logger = logging.getLogger(__name__)

auth_bp = Blueprint("auth", __name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def _get_admin_emails() -> set:
    """Parse comma-separated admin emails from env var."""
    raw = os.environ.get("ADMIN_EMAILS", "")
    return {e.strip().lower() for e in raw.split(",") if e.strip()}


def init_oauth(app):
    """Register Google OAuth with the Flask app. Call once at startup."""
    # secret_key should already be set in app.py; fallback just in case
    if not app.secret_key:
        app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.urandom(32).hex()

    oauth = OAuth(app)
    oauth.register(
        name="google",
        client_id=os.environ.get("GOOGLE_CLIENT_ID", ""),
        client_secret=os.environ.get("GOOGLE_CLIENT_SECRET", ""),
        server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
        client_kwargs={"scope": "openid email profile"},
    )
    app.extensions["oauth"] = oauth
    return oauth


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------

def get_current_user() -> dict | None:
    """Return the logged-in user dict from the session, or None."""
    return session.get("user")


def is_admin() -> bool:
    """Check if the current session user is an admin."""
    user = get_current_user()
    if not user:
        return False
    email = (user.get("email") or "").lower()
    return email in _get_admin_emails()


def admin_required(f):
    """Decorator: require admin login for an API endpoint."""
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not is_admin():
            return jsonify({"error": "נדרשת הרשאת מנהל. יש להתחבר עם חשבון Google מורשה."}), 403
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@auth_bp.route("/auth/login")
def login():
    """Redirect to Google OAuth2 consent screen."""
    oauth = current_app.extensions.get("oauth")
    if not oauth:
        return jsonify({"error": "OAuth not configured"}), 500
    client_id = os.environ.get("GOOGLE_CLIENT_ID", "")
    if not client_id:
        return redirect("/?auth_error=not_configured")
    try:
        redirect_uri = url_for("auth.callback", _external=True)
        logger.info("OAuth login redirect_uri=%s", redirect_uri)
        return oauth.google.authorize_redirect(redirect_uri)
    except Exception as e:
        logger.exception("OAuth login redirect failed")
        return redirect("/?auth_error=callback_failed")


@auth_bp.route("/auth/callback")
def callback():
    """Handle the OAuth2 callback from Google."""
    oauth = current_app.extensions.get("oauth")
    if not oauth:
        return jsonify({"error": "OAuth not configured"}), 500

    try:
        token = oauth.google.authorize_access_token()
        user_info = token.get("userinfo") or oauth.google.userinfo()
    except Exception as e:
        logger.exception("OAuth callback failed")
        return redirect("/?auth_error=callback_failed")

    email = (user_info.get("email") or "").lower()
    name = user_info.get("name", email)

    session["user"] = {
        "email": email,
        "name": name,
        "picture": user_info.get("picture", ""),
    }

    admin_emails = _get_admin_emails()
    if email not in admin_emails:
        logger.warning("Non-admin login attempt: %s", email)
        session.pop("user", None)
        return redirect("/?auth_error=not_admin")

    logger.info("Admin logged in: %s", email)
    return redirect("/")


@auth_bp.route("/auth/logout")
def logout():
    """Clear the session."""
    session.pop("user", None)
    return redirect("/")


@auth_bp.route("/auth/me")
def me():
    """Return current user info (or null)."""
    user = get_current_user()
    if user:
        return jsonify({"user": user, "is_admin": is_admin()})
    return jsonify({"user": None, "is_admin": False})
