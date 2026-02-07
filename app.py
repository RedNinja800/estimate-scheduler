
import os
import sqlite3
import requests
import time
import threading
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory, g
from flask_cors import CORS
from base64 import b64encode

app = Flask(__name__, static_folder="static")
CORS(app)

DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scheduler.db")

# ── RFMS API Config ──────────────────────────
RFMS_BASE_URL = "https://api.rfms.online/v2"
RFMS_STORE_QUEUE = os.environ.get("RFMS_STORE_QUEUE", "")
RFMS_API_KEY = os.environ.get("RFMS_API_KEY", "")
RFMS_ENABLED = bool(RFMS_STORE_QUEUE and RFMS_API_KEY)

# Phase-1: customer search/create only, no opportunity creation
RFMS_DEFAULT_STORE_NUMBER = int(os.environ.get("RFMS_DEFAULT_STORE_NUMBER", "1"))
RFMS_DEFAULT_SALESPERSON = os.environ.get("RFMS_DEFAULT_SALESPERSON", "ROBERT JENNINGS")

# Session management
rfms_session = {"token": "", "expires": 0, "lock": threading.Lock()}


def rfms_basic_auth(username, password):
    raw = f"{username}:{password}".encode()
    return "Basic " + b64encode(raw).decode()


def rfms_get_session():
    with rfms_session["lock"]:
        now = time.time()
        if rfms_session["token"] and rfms_session["expires"] > now + 60:
            return rfms_session["token"]
        try:
            resp = requests.post(
                RFMS_BASE_URL + "/session/begin",
                headers={
                    "Content-Type": "application/json",
                    "Authorization": rfms_basic_auth(RFMS_STORE_QUEUE, RFMS_API_KEY),
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("sessionToken")
            if not token:
                app.logger.warning("RFMS session/begin returned no sessionToken: %s", data)
                return ""
            rfms_session["token"] = token
            rfms_session["expires"] = now + 1200  # ~20 minutes
            return token
        except Exception as e:
            app.logger.warning("RFMS session/begin failed: %s", str(e))
            return ""


def rfms_call(method, endpoint, payload=None):
    if not RFMS_ENABLED:
        return None, "RFMS not configured"
    token = rfms_get_session()
    if not token:
        return None, "Could not get RFMS session"

    url = RFMS_BASE_URL + "/" + endpoint.lstrip("/")
    try:
        resp = requests.request(
            method,
            url,
            headers={
                "Content-Type": "application/json",
                "Authorization": rfms_basic_auth(RFMS_STORE_QUEUE, token),
            },
            json=payload,
            timeout=30,
        )
        ctype = resp.headers.get("Content-Type", "")
        if "application/json" in ctype:
            body = resp.json() if resp.content else {}
        else:
            body = {"raw": resp.text[:2000]}

        if not resp.ok:
            msg = None
            if isinstance(body, dict):
                msg = body.get("Message") or body.get("message") or body.get("error")
            return body, f"RFMS error {resp.status_code}: {msg or resp.reason}"
        return body, ""
    except Exception as e:
        return None, "RFMS request failed: " + str(e)


# ── Database ─────────────
