import os, sqlite3
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory, g
from flask_cors import CORS

app = Flask(__name__, static_folder="static")
CORS(app)

DATABASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scheduler.db")


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
    db.executescript("""
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
            FOREIGN KEY (estimator_id) REFERENCES estimators(id),
            UNIQUE(date, time_slot, estimator_id)
        );
    """)
    cur = db.execute("SELECT COUNT(*) FROM regions")
    if cur.fetchone()[0] == 0:
        for i, name in enumerate(["North", "South", "Central", "Costco"], 1):
            db.execute("INSERT INTO regions (name, sort_order) VALUES (?, ?)", (name, i))
        db.execute("INSERT INTO estimators (name, color, region_id, sort_order) VALUES ('Estimator 1', '#3b82f6', 1, 1)")
        db.execute("INSERT INTO estimators (name, color, region_id, sort_order) VALUES ('Estimator 2', '#ef4444', 1, 2)")
        db.commit()
    db.close()


init_db()


# ── Serve frontend ────────────────────────────
@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ── REGIONS ───────────────────────────────────
@app.route("/api/regions", methods=["GET"])
def get_regions():
    db = get_db()
    rows = db.execute("SELECT * FROM regions ORDER BY sort_order, id").fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/regions", methods=["POST"])
def save_regions():
    data = request.json
    db = get_db()
    for i, reg in enumerate(data):
        name = reg.get("name", "").strip()
        if not name:
            continue
        rid = reg.get("id")
        if rid:
            db.execute("UPDATE regions SET name=?, sort_order=? WHERE id=?", (name, i, rid))
        else:
            db.execute("INSERT INTO regions (name, sort_order) VALUES (?, ?)", (name, i))
    db.commit()
    return jsonify({"ok": True})


# ── ESTIMATORS ────────────────────────────────
@app.route("/api/estimators", methods=["GET"])
def get_estimators():
    region_id = request.args.get("region_id")
    db = get_db()
    if region_id:
        rows = db.execute(
            "SELECT e.*, r.name AS region_name FROM estimators e "
            "JOIN regions r ON e.region_id = r.id "
            "WHERE e.active = 1 AND e.region_id = ? "
            "ORDER BY e.sort_order, e.id",
            (region_id,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT e.*, r.name AS region_name FROM estimators e "
            "JOIN regions r ON e.region_id = r.id "
            "WHERE e.active = 1 "
            "ORDER BY e.region_id, e.sort_order, e.id"
        ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/estimators", methods=["POST"])
def save_estimators():
    data = request.json
    region_id = data.get("region_id")
    ests = data.get("estimators", [])
    db = get_db()
    db.execute("UPDATE estimators SET active = 0 WHERE region_id = ?", (region_id,))
    for i, est in enumerate(ests):
        name = est.get("name", "").strip()
        color = est.get("color", "#3b82f6")
        if not name:
            continue
        eid = est.get("id")
        if eid:
            db.execute(
                "UPDATE estimators SET name=?, color=?, active=1, sort_order=?, region_id=? WHERE id=?",
                (name, color, i, region_id, eid),
            )
        else:
            db.execute(
                "INSERT INTO estimators (name, color, active, sort_order, region_id) VALUES (?,?,1,?,?)",
                (name, color, i, region_id),
            )
    db.commit()
    return jsonify({"ok": True})


# ── BOOKINGS ─────────────────────────────────
@app.route("/api/bookings", methods=["GET"])
def get_bookings():
    date_from = request.args.get("from", "")
    date_to = request.args.get("to", "")
    region_id = request.args.get("region_id", "")
    db = get_db()
    query = (
        "SELECT b.*, e.name AS estimator_name, e.color AS estimator_color, e.region_id "
        "FROM bookings b JOIN estimators e ON b.estimator_id = e.id WHERE 1=1"
    )
    params = []
    if date_from and date_to:
        query += " AND b.date >= ? AND b.date <= ?"
        params += [date_from, date_to]
    if region_id:
        query += " AND e.region_id = ?"
        params.append(region_id)
    query += " ORDER BY b.date, b.time_slot"
    rows = db.execute(query, params).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route("/api/bookings", methods=["POST"])
def create_booking():
    data = request.json
    db = get_db()
    date = data.get("date", "").strip()
    time_slot = data.get("time_slot", "").strip()
    estimator_id = data.get("estimator_id")
    first_name = data.get("first_name", "").strip()
    last_name = data.get("last_name", "").strip()
    phone = data.get("phone", "").strip()

    if not all([date, time_slot, estimator_id, first_name, last_name, phone]):
        return jsonify({"error": "Missing required fields."}), 400

    existing = db.execute(
        "SELECT id FROM bookings WHERE date=? AND time_slot=? AND estimator_id=?",
        (date, time_slot, estimator_id),
    ).fetchone()
    if existing:
        return jsonify({"error": "This slot was already booked. Please refresh."}), 409

    try:
        db.execute(
            "INSERT INTO bookings "
            "(date,time_slot,estimator_id,first_name,last_name,phone,"
            "email,address,city,state,zip,flooring_type,rooms,notes,created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                date, time_slot, estimator_id, first_name, last_name, phone,
                data.get("email", ""), data.get("address", ""),
                data.get("city", ""), data.get("state", ""),
                data.get("zip", ""), data.get("flooring_type", ""),
                data.get("rooms", 1), data.get("notes", ""),
                datetime.now().isoformat(),
            ),
        )
        db.commit()
        return jsonify({"ok": True}), 201
    except sqlite3.IntegrityError:
        return jsonify({"error": "Slot just booked by someone else. Please refresh."}), 409


@app.route("/api/bookings/<int:bid>", methods=["DELETE"])
def delete_booking(bid):
    db = get_db()
    db.execute("DELETE FROM bookings WHERE id=?", (bid,))
    db.commit()
    return jsonify({"ok": True})


@app.route("/api/bookings/count", methods=["GET"])
def booking_count():
    db = get_db()
    row = db.execute("SELECT COUNT(*) AS cnt FROM bookings").fetchone()
    return jsonify({"count": row["cnt"]})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
