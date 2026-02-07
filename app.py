import os, sqlite3, requests, time, threading
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

# Phase-1 hardcoding (per your instructions)
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
                headers={"Content-Type": "application/json", "Authorization": rfms_basic_auth(RFMS_STORE_QUEUE, RFMS_API_KEY)},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("sessionToken")
            if not token:
                app.logger.warning("RFMS session/begin returned no sessionToken: %s", data)
                return ""
            rfms_session["token"] = token
            rfms_session["expires"] = now + 1200  # refresh every 20m
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
            headers={"Content-Type": "application/json", "Authorization": rfms_basic_auth(RFMS_STORE_QUEUE, token)},
            json=payload,
            timeout=30,
        )
        # RFMS sometimes returns HTML on invalid URL; keep text for debugging
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


# ── Database ──────────────────────────────────
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA busy_timeout=5000")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db:
        db.close()


def init_db():
    db = sqlite3.connect(DATABASE)
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS regions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sort_order INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS estimators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            color TEXT NOT NULL DEFAULT '#3b82f6',
            region_id INTEGER NOT NULL DEFAULT 1,
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (region_id) REFERENCES regions(id)
        );
        CREATE TABLE IF NOT EXISTS bookings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            time_slot TEXT NOT NULL,
            estimator_id INTEGER NOT NULL,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            phone TEXT NOT NULL,
            email TEXT DEFAULT '',
            address TEXT DEFAULT '',
            city TEXT DEFAULT '',
            state TEXT DEFAULT '',
            zip TEXT DEFAULT '',
            flooring_type TEXT DEFAULT '',
            rooms INTEGER DEFAULT 1,
            notes TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            created_by TEXT DEFAULT '',
            rfms_customer_id TEXT DEFAULT '',
            rfms_opportunity_id TEXT DEFAULT '',
            FOREIGN KEY (estimator_id) REFERENCES estimators(id),
            UNIQUE(date, time_slot, estimator_id)
        );
        CREATE TABLE IF NOT EXISTS booking_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            booking_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            changed_by TEXT NOT NULL DEFAULT '',
            details TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY (booking_id) REFERENCES bookings(id)
        );
        CREATE TABLE IF NOT EXISTS time_off (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            estimator_id INTEGER NOT NULL,
            date TEXT,
            day_of_week INTEGER,
            label TEXT DEFAULT 'Off',
            recurring INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (estimator_id) REFERENCES estimators(id)
        );
        """
    )
    cur = db.execute("SELECT COUNT(*) FROM regions")
    if cur.fetchone()[0] == 0:
        for i, name in enumerate(["North", "South", "Central", "Costco"], 1):
            db.execute("INSERT INTO regions (name, sort_order) VALUES (?,?)", (name, i))
        db.execute("INSERT INTO estimators (name,color,region_id,sort_order) VALUES ('Estimator 1','#3b82f6',1,1)")
        db.execute("INSERT INTO estimators (name,color,region_id,sort_order) VALUES ('Estimator 2','#ef4444',1,2)")
        db.commit()
    db.close()


init_db()


# ── RFMS Helpers ──────────────────────────────
def rfms_find_customer_id_by_phone(phone):
    payload = {
        "searchText": str(phone),
        "includeCustomers": "true",
        "includeProspects": "true",
        "includeInactive": "false",
    }
    data, err = rfms_call("POST", "customers/find", payload)
    if err:
        return "", err
    # standard wrapper
    result = data.get("result") if isinstance(data, dict) else None
    if isinstance(result, list) and result:
        c0 = result[0]
        cid = c0.get("customerSourceId") or c0.get("customerId")
        return str(cid) if cid else "", ""
    if isinstance(result, dict):
        customers = result.get("customers") or []
        if customers:
            cid = customers[0].get("customerSourceId") or customers[0].get("customerId")
            return str(cid) if cid else "", ""
    return "", ""


def rfms_create_or_update_customer(booking, store_number, salesperson):
    payload = {
        "customerType": "RETAIL",
        "entryType": "Customer",
        "customerAddress": {
            "lastName": booking.get("last_name", ""),
            "firstName": booking.get("first_name", ""),
            "address1": booking.get("address", ""),
            "address2": "",
            "city": booking.get("city", ""),
            "state": booking.get("state", ""),
            "postalCode": booking.get("zip", ""),
            "county": "",
        },
        "shipToAddress": {
            "lastName": booking.get("last_name", ""),
            "firstName": booking.get("first_name", ""),
            "address1": booking.get("address", ""),
            "address2": "",
            "city": booking.get("city", ""),
            "state": booking.get("state", ""),
            "postalCode": booking.get("zip", ""),
            "county": "",
        },
        "phone1": booking.get("phone", ""),
        "email": booking.get("email", ""),
        "taxStatus": "Tax",
        "taxMethod": "SalesTax",
        "preferredSalesperson1": salesperson,
        "preferredSalesperson2": "",
        "storeNumber": int(store_number),
    }
    data, err = rfms_call("POST", "customers", payload)
    if err:
        return "", err
    # customer create returns wrapper; sometimes id embedded
    result = data.get("result") if isinstance(data, dict) else None
    if isinstance(result, dict):
        cid = result.get("customerId") or result.get("customerSourceId")
        return str(cid) if cid else "", ""
    return "", ""


def rfms_create_crm_opportunity(customer_id, booking, store_number, salesperson, estimator_name):
    notes = (
        "MEASURE SCHEDULED\n"
        f"Date: {booking.get('date','')}\n"
        f"Time: {booking.get('time_slot','')}\n"
        f"Estimator: {estimator_name}\n"
        f"Flooring: {booking.get('flooring_type','')}\n"
        f"Rooms: {booking.get('rooms','')}\n"
        f"Address: {booking.get('address','')}, {booking.get('city','')} {booking.get('state','')} {booking.get('zip','')}\n"
        f"Notes: {booking.get('notes','')}"
    )

    payload = {
        "useCRM": True,
        "createOrder": False,
        "customerid": int(customer_id) if str(customer_id).isdigit() else customer_id,
        "notes": notes,
        "storeNumber": int(store_number),
        "preferredSalesperson1": salesperson,
        "preferredSalesperson2": "",
    }

    data, err = rfms_call("POST", "opportunity", payload)
    if err:
        return "", err
    result = data.get("result") if isinstance(data, dict) else None
    # doc says: returns new document number
    if isinstance(result, (str, int)):
        return str(result), ""
    if isinstance(result, dict):
        for k in ("documentNumber", "number", "id", "opportunityId"):
            if k in result and result[k] is not None:
                return str(result[k]), ""
    return "", "No opportunity number returned"


# ── Serve frontend ────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/rfms/status", methods=["GET"])
def rfms_status():
    return jsonify({
        "enabled": RFMS_ENABLED,
        "base_url": RFMS_BASE_URL,
        "default_store_number": RFMS_DEFAULT_STORE_NUMBER,
        "default_salesperson": RFMS_DEFAULT_SALESPERSON,
    })


@app.route("/api/rfms/test", methods=["GET"])
def rfms_test():
    if not RFMS_ENABLED:
        return jsonify({"ok": False, "error": "RFMS not configured. Set RFMS_STORE_QUEUE and RFMS_API_KEY."})
    token = rfms_get_session()
    return jsonify({"ok": bool(token), "session_preview": (token[:20] + "...") if token else ""})


# ── REGIONS / ESTIMATORS / BOOKINGS / TIMEOFF (same as before, simplified) ──
@app.route("/api/regions", methods=["GET"])
def get_regions():
    db = get_db()
    rows = db.execute("SELECT * FROM regions ORDER BY sort_order, id").fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/estimators", methods=["GET"])
def get_estimators():
    region_id = request.args.get("region_id")
    db = get_db()
    if region_id:
        rows = db.execute(
            "SELECT e.*, r.name AS region_name FROM estimators e JOIN regions r ON e.region_id=r.id "
            "WHERE e.active=1 AND e.region_id=? ORDER BY e.sort_order,e.id",
            (region_id,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT e.*, r.name AS region_name FROM estimators e JOIN regions r ON e.region_id=r.id "
            "WHERE e.active=1 ORDER BY e.region_id,e.sort_order,e.id"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/bookings", methods=["GET"])
def get_bookings():
    date_from = request.args.get("from", "")
    date_to = request.args.get("to", "")
    region_id = request.args.get("region_id", "")
    db = get_db()
    q = (
        "SELECT b.*, e.name AS estimator_name, e.color AS estimator_color, e.region_id "
        "FROM bookings b JOIN estimators e ON b.estimator_id=e.id WHERE 1=1"
    )
    params = []
    if date_from and date_to:
        q += " AND b.date>=? AND b.date<=?"; params += [date_from, date_to]
    if region_id:
        q += " AND e.region_id=?"; params.append(region_id)
    q += " ORDER BY b.date, b.time_slot"
    rows = db.execute(q, params).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/bookings", methods=["POST"])
def create_booking():
    data = request.json
    db = get_db()

    date = (data.get("date") or "").strip()
    time_slot = (data.get("time_slot") or "").strip()
    estimator_id = data.get("estimator_id")
    first_name = (data.get("first_name") or "").strip()
    last_name = (data.get("last_name") or "").strip()
    phone = (data.get("phone") or "").strip()
    created_by = (data.get("created_by") or "").strip()

    if not all([date, time_slot, estimator_id, first_name, last_name, phone]):
        return jsonify({"error": "Missing required fields."}), 400

    existing = db.execute(
        "SELECT id FROM bookings WHERE date=? AND time_slot=? AND estimator_id=?",
        (date, time_slot, estimator_id),
    ).fetchone()
    if existing:
        return jsonify({"error": "This slot was already booked. Please refresh."}), 409

    # RFMS sync
    rfms_customer_id = ""
    rfms_opportunity_id = ""
    rfms_log = []

    est_row = db.execute("SELECT name FROM estimators WHERE id=?", (estimator_id,)).fetchone()
    estimator_name = est_row["name"] if est_row else ""

    if RFMS_ENABLED:
        # 1) find customer
        rfms_customer_id, err = rfms_find_customer_id_by_phone(phone)
        if err:
            rfms_log.append("RFMS customer find failed: " + err)
        elif rfms_customer_id:
            rfms_log.append("Found RFMS customerId: " + rfms_customer_id)

        # 2) create customer if not found
        if not rfms_customer_id:
            rfms_customer_id, err = rfms_create_or_update_customer(
                data,
                store_number=RFMS_DEFAULT_STORE_NUMBER,
                salesperson=RFMS_DEFAULT_SALESPERSON,
            )
            if err:
                rfms_log.append("RFMS customer create failed: " + err)
            elif rfms_customer_id:
                rfms_log.append("Created RFMS customerId: " + rfms_customer_id)

        # 3) create CRM opportunity
        if rfms_customer_id:
            rfms_opportunity_id, err = rfms_create_crm_opportunity(
                rfms_customer_id,
                data,
                store_number=RFMS_DEFAULT_STORE_NUMBER,
                salesperson=RFMS_DEFAULT_SALESPERSON,
                estimator_name=estimator_name,
            )
            if err:
                rfms_log.append("RFMS opportunity create failed: " + err)
            elif rfms_opportunity_id:
                rfms_log.append("Created RFMS opportunity: " + rfms_opportunity_id)

    cur = db.execute(
        "INSERT INTO bookings (date,time_slot,estimator_id,first_name,last_name,phone,email,address,city,state,zip,"
        "flooring_type,rooms,notes,created_at,created_by,rfms_customer_id,rfms_opportunity_id) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            date,
            time_slot,
            estimator_id,
            first_name,
            last_name,
            phone,
            data.get("email", ""),
            data.get("address", ""),
            data.get("city", ""),
            data.get("state", ""),
            data.get("zip", ""),
            data.get("flooring_type", ""),
            data.get("rooms", 1),
            data.get("notes", ""),
            datetime.now().isoformat(),
            created_by,
            rfms_customer_id,
            rfms_opportunity_id,
        ),
    )
    db.commit()
    bid = cur.lastrowid

    detail = "Estimate created"
    if rfms_log:
        detail += ". " + "; ".join(rfms_log)

    db.execute(
        "INSERT INTO booking_log (booking_id,action,changed_by,details,created_at) VALUES (?,?,?,?,?)",
        (bid, "Created", created_by or "Unknown", detail, datetime.now().isoformat()),
    )
    db.commit()

    return jsonify({
        "ok": True,
        "id": bid,
        "rfms_customer_id": rfms_customer_id,
        "rfms_opportunity_id": rfms_opportunity_id,
        "rfms_log": rfms_log,
    }), 201


@app.route("/api/bookings/<int:bid>/log", methods=["GET"])
def get_booking_log(bid):
    db = get_db()
    rows = db.execute("SELECT * FROM booking_log WHERE booking_id=? ORDER BY created_at DESC", (bid,)).fetchall()
    return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
