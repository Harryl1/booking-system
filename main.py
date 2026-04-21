import os
import sqlite3
import uuid
from functools import wraps
from datetime import datetime
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    session,
    abort,
    g,
)
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", os.urandom(32))
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
# Enable only behind HTTPS in production:
# app.config["SESSION_COOKIE_SECURE"] = True

DB_PATH = "bookings.db"

ROLE_AGENT = "agent"
ROLE_ASSESSOR = "assessor"

STATUS_NEW = "new"
STATUS_ASSIGNED = "assigned"
STATUS_COMPLETED = "completed"

LEAD_STATUS_NEW = "new"
LEAD_STATUS_CONTACTED = "contacted"
LEAD_STATUS_VALUATION_BOOKED = "valuation booked"
LEAD_STATUS_QUALIFIED = "qualified"
LEAD_STATUS_LOST = "lost"

VALID_LEAD_STATUSES = {
    LEAD_STATUS_NEW,
    LEAD_STATUS_CONTACTED,
    LEAD_STATUS_VALUATION_BOOKED,
    LEAD_STATUS_QUALIFIED,
    LEAD_STATUS_LOST,
}
VALID_ROLES = {ROLE_AGENT, ROLE_ASSESSOR}
VALID_BOOKING_STATUSES = {STATUS_NEW, STATUS_ASSIGNED, STATUS_COMPLETED}
VALID_LEAD_STATUSES = {
    LEAD_STATUS_NEW,
    LEAD_STATUS_CONTACTED,
    LEAD_STATUS_VALUATION_BOOKED,
    LEAD_STATUS_QUALIFIED,
    LEAD_STATUS_LOST,
}
VALID_PROPERTY_TYPES = {"flat", "terraced", "semi detached", "detached"}


# -----------------------
# Database
# -----------------------
def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


@app.teardown_appcontext
def close_db(exception=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()

    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT NOT NULL,
            address TEXT NOT NULL,
            valuation INTEGER NOT NULL,
            source TEXT NOT NULL DEFAULT 'website',
            status TEXT NOT NULL DEFAULT 'new',
            created_at TEXT NOT NULL,
            contacted_at TEXT,
            valuation_booked_at TEXT,
            notes TEXT
        );

        CREATE TABLE IF NOT EXISTS bookings (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            agent_name TEXT NOT NULL,
            address TEXT NOT NULL,
            property_type TEXT NOT NULL,
            bedrooms INTEGER NOT NULL,
            preferred_date TEXT NOT NULL,
            price INTEGER NOT NULL,
            status TEXT NOT NULL,
            assigned_assessor_id INTEGER,
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (assigned_assessor_id) REFERENCES users (id)
        );
    """)

    db.commit()


# -----------------------
# Auth / Access Helpers
# -----------------------
def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return redirect("/login")
        return view_func(*args, **kwargs)
    return wrapper


def role_required(role):
    def decorator(view_func):
        @wraps(view_func)
        def wrapper(*args, **kwargs):
            if "user_id" not in session:
                return redirect("/login")
            if session.get("role") != role:
                abort(403)
            return view_func(*args, **kwargs)
        return wrapper
    return decorator


# -----------------------
# CSRF Helpers
# -----------------------
def get_csrf_token():
    token = session.get("csrf_token")
    if not token:
        token = str(uuid.uuid4())
        session["csrf_token"] = token
    return token


def validate_csrf():
    session_token = session.get("csrf_token")
    form_token = request.form.get("csrf_token")
    if not session_token or not form_token or session_token != form_token:
        abort(403, "Invalid CSRF token")


# -----------------------
# Validation Helpers
# -----------------------
def validate_required_text(value, field_name, max_length=255):
    if value is None:
        abort(400, f"Missing {field_name}")
    value = value.strip()
    if not value:
        abort(400, f"{field_name} is required")
    if len(value) > max_length:
        abort(400, f"{field_name} is too long")
    return value


def validate_int(value, field_name, min_value=None, max_value=None):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        abort(400, f"Invalid {field_name}")

    if min_value is not None and parsed < min_value:
        abort(400, f"{field_name} must be at least {min_value}")
    if max_value is not None and parsed > max_value:
        abort(400, f"{field_name} must be at most {max_value}")
    return parsed


def validate_date(value, field_name):
    if not value:
        abort(400, f"{field_name} is required")
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        abort(400, f"Invalid {field_name}")
    return value


def validate_role(role):
    if role not in VALID_ROLES:
        abort(400, "Invalid role")
    return role


def validate_property_type(property_type):
    if property_type is None:
        abort(400, "Property type is required")
    property_type = property_type.strip().lower()
    if property_type not in VALID_PROPERTY_TYPES:
        abort(400, "Invalid property type")
    return property_type


def validate_lead_status(status):
    if status is None:
        abort(400, "Lead status is required")
    status = status.strip().lower()
    if status not in VALID_LEAD_STATUSES:
        abort(400, "Invalid lead status")
    return status


def calculate_price(property_type, bedrooms):
    property_type = validate_property_type(property_type)

    if property_type == "flat":
        return 75
    if property_type == "terraced":
        return 80
    if property_type == "semi detached":
        return 90
    if property_type == "detached":
        return 100 if bedrooms <= 3 else 120

    abort(400, "Invalid property type")


# -----------------------
# Query Helpers
# -----------------------
def get_assessors(db):
    return db.execute(
        "SELECT id, username FROM users WHERE role = ? ORDER BY username ASC",
        (ROLE_ASSESSOR,)
    ).fetchall()


def get_assessor_map(assessors):
    return {assessor["id"]: assessor["username"] for assessor in assessors}


def get_booking_for_agent(db, booking_id, user_id):
    return db.execute(
        "SELECT * FROM bookings WHERE id = ? AND user_id = ?",
        (booking_id, user_id)
    ).fetchone()


def get_booking_for_assessor(db, booking_id, assessor_id):
    return db.execute(
        "SELECT * FROM bookings WHERE id = ? AND assigned_assessor_id = ?",
        (booking_id, assessor_id)
    ).fetchone()


# -----------------------
# Auth Routes
# -----------------------
@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        validate_csrf()

        username = validate_required_text(
            request.form.get("username"),
            "username",
            max_length=100
        )
        password = request.form.get("password", "")
        role = validate_role(request.form.get("role"))

        if len(password) < 6:
            abort(400, "Password must be at least 6 characters")

        hashed_password = generate_password_hash(password)

        db = get_db()
        try:
            db.execute(
                "INSERT INTO users (username, password, role) VALUES (?, ?, ?)",
                (username, hashed_password, role)
            )
            db.commit()
        except sqlite3.IntegrityError:
            return "User already exists", 400

        return redirect("/login")

    return render_template(
        "register.html",
        csrf_token=get_csrf_token()
    )


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        validate_csrf()

        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        db = get_db()
        user = db.execute(
            "SELECT * FROM users WHERE username = ?",
            (username,)
        ).fetchone()

        if user and check_password_hash(user["password"], password):
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]
            get_csrf_token()
            return redirect("/")

        return "Invalid login", 401

    return render_template(
        "login.html",
        csrf_token=get_csrf_token()
    )


@app.route("/logout")
@login_required
def logout():
    session.clear()
    return redirect("/login")


# -----------------------
# AgencyHub Dashboard
# -----------------------
@app.route("/")
@login_required
def agencyhub():
    db = get_db()
    today = datetime.now().strftime("%Y-%m-%d")

    total_leads = db.execute(
        "SELECT COUNT(*) FROM leads"
    ).fetchone()[0]

    new_leads = db.execute(
        "SELECT COUNT(*) FROM leads WHERE status = ?",
        (LEAD_STATUS_NEW,)
    ).fetchone()[0]

    contacted_leads = db.execute(
        "SELECT COUNT(*) FROM leads WHERE status = ?",
        (LEAD_STATUS_CONTACTED,)
    ).fetchone()[0]

    qualified_leads = db.execute(
        "SELECT COUNT(*) FROM leads WHERE status = ?",
        (LEAD_STATUS_QUALIFIED,)
    ).fetchone()[0]

    valuation_booked_leads = db.execute(
        "SELECT COUNT(*) FROM leads WHERE status = ?",
        (LEAD_STATUS_VALUATION_BOOKED,)
    ).fetchone()[0]

    lost_leads = db.execute(
        "SELECT COUNT(*) FROM leads WHERE status = ?",
        (LEAD_STATUS_LOST,)
    ).fetchone()[0]

    recent_leads = db.execute("""
        SELECT *
        FROM leads
        ORDER BY id DESC
        LIMIT 5
    """).fetchall()

    upcoming_jobs = db.execute("""
        SELECT *
        FROM bookings
        WHERE preferred_date >= ?
        ORDER BY preferred_date ASC, rowid DESC
        LIMIT 5
    """, (today,)).fetchall()

    overdue_jobs = db.execute("""
        SELECT *
        FROM bookings
        WHERE preferred_date < ? AND status != ?
        ORDER BY preferred_date ASC, rowid DESC
        LIMIT 5
    """, (today, STATUS_COMPLETED)).fetchall()

    recent_bookings = db.execute("""
        SELECT *
        FROM bookings
        ORDER BY rowid DESC
        LIMIT 5
    """).fetchall()

    total_epc_jobs_all = db.execute(
        "SELECT COUNT(*) FROM bookings"
    ).fetchone()[0]

    total_epc_completed_all = db.execute(
        "SELECT COUNT(*) FROM bookings WHERE status = ?",
        (STATUS_COMPLETED,)
    ).fetchone()[0]

    total_epc_assigned_all = db.execute(
        "SELECT COUNT(*) FROM bookings WHERE status = ?",
        (STATUS_ASSIGNED,)
    ).fetchone()[0]

    total_epc_new_all = db.execute(
        "SELECT COUNT(*) FROM bookings WHERE status = ?",
        (STATUS_NEW,)
    ).fetchone()[0]

    total_revenue_all = db.execute(
        "SELECT COALESCE(SUM(price), 0) FROM bookings WHERE status = ?",
        (STATUS_COMPLETED,)
    ).fetchone()[0]

    if session["role"] == ROLE_AGENT:
        my_total_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE user_id = ?",
            (session["user_id"],)
        ).fetchone()[0]

        my_new_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE user_id = ? AND status = ?",
            (session["user_id"], STATUS_NEW)
        ).fetchone()[0]

        my_assigned_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE user_id = ? AND status = ?",
            (session["user_id"], STATUS_ASSIGNED)
        ).fetchone()[0]

        my_completed_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE user_id = ? AND status = ?",
            (session["user_id"], STATUS_COMPLETED)
        ).fetchone()[0]

        my_revenue = db.execute(
            "SELECT COALESCE(SUM(price), 0) FROM bookings WHERE user_id = ? AND status = ?",
            (session["user_id"], STATUS_COMPLETED)
        ).fetchone()[0]
    else:
        my_total_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE assigned_assessor_id = ?",
            (session["user_id"],)
        ).fetchone()[0]

        my_new_jobs = 0

        my_assigned_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE assigned_assessor_id = ? AND status = ?",
            (session["user_id"], STATUS_ASSIGNED)
        ).fetchone()[0]

        my_completed_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE assigned_assessor_id = ? AND status = ?",
            (session["user_id"], STATUS_COMPLETED)
        ).fetchone()[0]

        my_revenue = 0

    lead_conversion_rate = 0
    if total_leads > 0:
        lead_conversion_rate = round((valuation_booked_leads / total_leads) * 100, 1)

    epc_completion_rate = 0
    if total_epc_jobs_all > 0:
        epc_completion_rate = round((total_epc_completed_all / total_epc_jobs_all) * 100, 1)

    return render_template(
        "agencyhub.html",
        today=today,
        total_leads=total_leads,
        new_leads=new_leads,
        contacted_leads=contacted_leads,
        qualified_leads=qualified_leads,
        valuation_booked_leads=valuation_booked_leads,
        lost_leads=lost_leads,
        recent_leads=recent_leads,
        upcoming_jobs=upcoming_jobs,
        overdue_jobs=overdue_jobs,
        recent_bookings=recent_bookings,
        total_epc_jobs_all=total_epc_jobs_all,
        total_epc_new_all=total_epc_new_all,
        total_epc_assigned_all=total_epc_assigned_all,
        total_epc_completed_all=total_epc_completed_all,
        total_revenue_all=total_revenue_all,
        my_total_jobs=my_total_jobs,
        my_new_jobs=my_new_jobs,
        my_assigned_jobs=my_assigned_jobs,
        my_completed_jobs=my_completed_jobs,
        my_revenue=my_revenue,
        lead_conversion_rate=lead_conversion_rate,
        epc_completion_rate=epc_completion_rate,
        csrf_token=get_csrf_token()
    )

# -----------------------
# Leads Module
# -----------------------
@app.route("/leads")
@login_required
def leads():
    db = get_db()

    status_filter = request.args.get("status", "all").strip().lower()
    source_filter = request.args.get("source", "all").strip().lower()
    search_query = request.args.get("search", "").strip()

    where_clauses = []
    params = []

    if status_filter != "all":
        if status_filter in VALID_LEAD_STATUSES:
            where_clauses.append("status = ?")
            params.append(status_filter)

    if source_filter != "all":
        where_clauses.append("LOWER(source) = ?")
        params.append(source_filter)

    if search_query:
        where_clauses.append("""
            (
                LOWER(name) LIKE ?
                OR LOWER(email) LIKE ?
                OR LOWER(phone) LIKE ?
                OR LOWER(address) LIKE ?
            )
        """)
        like_term = f"%{search_query.lower()}%"
        params.extend([like_term, like_term, like_term, like_term])

    query = "SELECT * FROM leads"
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    query += " ORDER BY id DESC"

    leads = db.execute(query, tuple(params)).fetchall()

    total_leads = db.execute("SELECT COUNT(*) FROM leads").fetchone()[0]

    new_leads = db.execute(
        "SELECT COUNT(*) FROM leads WHERE status = ?",
        (LEAD_STATUS_NEW,)
    ).fetchone()[0]

    contacted_leads = db.execute(
        "SELECT COUNT(*) FROM leads WHERE status = ?",
        (LEAD_STATUS_CONTACTED,)
    ).fetchone()[0]

    valuation_booked_leads = db.execute(
        "SELECT COUNT(*) FROM leads WHERE status = ?",
        (LEAD_STATUS_VALUATION_BOOKED,)
    ).fetchone()[0]

    conversion_rate = round(
        (valuation_booked_leads / total_leads) * 100, 1
    ) if total_leads else 0

    return render_template(
        "leads.html",
        leads=leads,
        total_leads=total_leads,
        new_leads=new_leads,
        contacted_leads=contacted_leads,
        valuation_booked_leads=valuation_booked_leads,
        conversion_rate=conversion_rate,
        status_filter=status_filter,
        source_filter=source_filter,
        search_query=search_query,
        lead_statuses=list(VALID_LEAD_STATUSES),
        csrf_token=get_csrf_token()
    )
@app.route("/leads/mark-contacted/<int:lead_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def mark_lead_contacted(lead_id):
    validate_csrf()

    contacted_at = request.form.get("contacted_at")
    notes = request.form.get("notes", "")

    db = get_db()

    db.execute("""
        UPDATE leads
        SET status = ?, contacted_at = ?, notes = ?
        WHERE id = ?
    """, (
        LEAD_STATUS_CONTACTED,
        contacted_at,
        notes,
        lead_id
    ))

    db.commit()
    return redirect("/leads")
@app.route("/leads/book-valuation/<int:lead_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def book_valuation(lead_id):
    validate_csrf()

    valuation_booked_at = request.form.get("valuation_booked_at")
    notes = request.form.get("notes", "")

    db = get_db()

    db.execute("""
        UPDATE leads
        SET status = ?, valuation_booked_at = ?, notes = ?
        WHERE id = ?
    """, (
        LEAD_STATUS_VALUATION_BOOKED,
        valuation_booked_at,
        notes,
        lead_id
    ))

    db.commit()
    return redirect("/leads")


@app.route("/leads/add-test", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def add_test_lead():
    validate_csrf()

    db = get_db()
    conn.execute("""
        INSERT INTO leads (
            name, email, phone, address, valuation,
            source, status, created_at, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "John Smith",
        "john@email.com",
        "07123456789",
        "123 Test Street",
        250000,
        "facebook",
        LEAD_STATUS_NEW,
        datetime.now().strftime("%Y-%m-%d"),
        ""
    ))
    db.commit()

    return redirect("/leads")


@app.route("/leads/update-status/<int:lead_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def update_lead_status(lead_id):
    validate_csrf()

    new_status = validate_lead_status(request.form.get("status"))
    db = get_db()

    lead = db.execute(
        "SELECT * FROM leads WHERE id = ?",
        (lead_id,)
    ).fetchone()

    if lead is None:
        return "Lead not found", 404

    db.execute(
        "UPDATE leads SET status = ? WHERE id = ?",
        (new_status, lead_id)
    )
    db.commit()

    return redirect("/leads")


# -----------------------
# EPCHub
# -----------------------
@app.route("/epc", methods=["GET", "POST"])
@login_required
def epc_dashboard():
    db = get_db()
    assessors = get_assessors(db)
    assessor_map = get_assessor_map(assessors)

    today = datetime.now().strftime("%Y-%m-%d")

    if request.method == "POST":
        validate_csrf()

        if session.get("role") != ROLE_AGENT:
            return "Only agents can create bookings", 403

        agent_name = validate_required_text(
            request.form.get("agent_name"),
            "agent name",
            max_length=100
        )
        address = validate_required_text(
            request.form.get("address"),
            "address",
            max_length=255
        )
        property_type = validate_property_type(
            request.form.get("property_type")
        )
        bedrooms = validate_int(
            request.form.get("bedrooms"),
            "bedrooms",
            min_value=0,
            max_value=50
        )
        preferred_date = validate_date(
            request.form.get("preferred_date"),
            "preferred date"
        )

        price = calculate_price(property_type, bedrooms)

        db.execute("""
            INSERT INTO bookings (
                id,
                user_id,
                agent_name,
                address,
                property_type,
                bedrooms,
                preferred_date,
                price,
                status,
                assigned_assessor_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(uuid.uuid4()),
            session["user_id"],
            agent_name,
            address,
            property_type,
            bedrooms,
            preferred_date,
            price,
            STATUS_NEW,
            None
        ))
        db.commit()

        return redirect("/epc")

    status_filter = request.args.get("status", "all").strip().lower()
    search_query = request.args.get("search", "").strip()

    if status_filter != "all" and status_filter not in VALID_BOOKING_STATUSES:
        status_filter = "all"

    if session["role"] == ROLE_AGENT:
        where_clauses = ["user_id = ?"]
        params = [session["user_id"]]

        if status_filter != "all":
            where_clauses.append("status = ?")
            params.append(status_filter)

        if search_query:
            where_clauses.append("(LOWER(address) LIKE ? OR LOWER(agent_name) LIKE ?)")
            like_term = f"%{search_query.lower()}%"
            params.extend([like_term, like_term])

        query = f"""
            SELECT *
            FROM bookings
            WHERE {' AND '.join(where_clauses)}
            ORDER BY preferred_date ASC, rowid DESC
        """
        bookings = db.execute(query, tuple(params)).fetchall()

        total_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE user_id = ?",
            (session["user_id"],)
        ).fetchone()[0]

        new_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE user_id = ? AND status = ?",
            (session["user_id"], STATUS_NEW)
        ).fetchone()[0]

        assigned_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE user_id = ? AND status = ?",
            (session["user_id"], STATUS_ASSIGNED)
        ).fetchone()[0]

        completed_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE user_id = ? AND status = ?",
            (session["user_id"], STATUS_COMPLETED)
        ).fetchone()[0]

        total_revenue = db.execute(
            "SELECT COALESCE(SUM(price), 0) FROM bookings WHERE user_id = ? AND status = ?",
            (session["user_id"], STATUS_COMPLETED)
        ).fetchone()[0]
    else:
        where_clauses = ["assigned_assessor_id = ?"]
        params = [session["user_id"]]

        if status_filter != "all":
            where_clauses.append("status = ?")
            params.append(status_filter)

        if search_query:
            where_clauses.append("(LOWER(address) LIKE ? OR LOWER(agent_name) LIKE ?)")
            like_term = f"%{search_query.lower()}%"
            params.extend([like_term, like_term])

        query = f"""
            SELECT *
            FROM bookings
            WHERE {' AND '.join(where_clauses)}
            ORDER BY preferred_date ASC, rowid DESC
        """
        bookings = db.execute(query, tuple(params)).fetchall()

        total_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE assigned_assessor_id = ?",
            (session["user_id"],)
        ).fetchone()[0]

        new_jobs = 0

        assigned_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE assigned_assessor_id = ? AND status = ?",
            (session["user_id"], STATUS_ASSIGNED)
        ).fetchone()[0]

        completed_jobs = db.execute(
            "SELECT COUNT(*) FROM bookings WHERE assigned_assessor_id = ? AND status = ?",
            (session["user_id"], STATUS_COMPLETED)
        ).fetchone()[0]

        total_revenue = 0

    return render_template(
        "epc.html",
        bookings=bookings,
        assessors=assessors,
        assessor_map=assessor_map,
        status_filter=status_filter,
        search_query=search_query,
        total_jobs=total_jobs,
        new_jobs=new_jobs,
        assigned_jobs=assigned_jobs,
        completed_jobs=completed_jobs,
        total_revenue=total_revenue,
        csrf_token=get_csrf_token(),
        today = today,
    )


@app.route("/epc/assign/<booking_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def assign_booking(booking_id):
    validate_csrf()

    assessor_id = validate_int(
        request.form.get("assessor_id"),
        "assessor id",
        min_value=1
    )

    db = get_db()
    booking = get_booking_for_agent(db, booking_id, session["user_id"])

    if booking is None:
        return "Booking not found", 404

    if booking["status"] != STATUS_NEW:
        return "Only new bookings can be assigned", 400

    assessor = db.execute(
        "SELECT id FROM users WHERE id = ? AND role = ?",
        (assessor_id, ROLE_ASSESSOR)
    ).fetchone()

    if assessor is None:
        return "Invalid assessor", 400

    db.execute("""
        UPDATE bookings
        SET assigned_assessor_id = ?, status = ?
        WHERE id = ? AND user_id = ?
    """, (
        assessor_id,
        STATUS_ASSIGNED,
        booking_id,
        session["user_id"]
    ))
    db.commit()

    return redirect("/epc")


@app.route("/epc/complete/<booking_id>", methods=["POST"])
@login_required
@role_required(ROLE_ASSESSOR)
def complete_booking(booking_id):
    validate_csrf()

    db = get_db()
    booking = get_booking_for_assessor(db, booking_id, session["user_id"])

    if booking is None:
        return "Booking not found", 404

    if booking["status"] != STATUS_ASSIGNED:
        return "Only assigned bookings can be completed", 400

    db.execute("""
        UPDATE bookings
        SET status = ?
        WHERE id = ? AND assigned_assessor_id = ?
    """, (
        STATUS_COMPLETED,
        booking_id,
        session["user_id"]
    ))
    db.commit()

    return redirect("/epc")


@app.route("/epc/delete/<booking_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def delete_booking(booking_id):
    validate_csrf()

    db = get_db()
    booking = get_booking_for_agent(db, booking_id, session["user_id"])

    if booking is None:
        return "Booking not found", 404

    if booking["status"] != STATUS_NEW:
        return "Only new bookings can be deleted", 400

    db.execute(
        "DELETE FROM bookings WHERE id = ? AND user_id = ?",
        (booking_id, session["user_id"])
    )
    db.commit()

    return redirect("/epc")


@app.route("/epc/edit/<booking_id>", methods=["GET", "POST"])
@login_required
@role_required(ROLE_AGENT)
def edit_booking(booking_id):
    db = get_db()
    booking = get_booking_for_agent(db, booking_id, session["user_id"])

    if booking is None:
        return "Booking not found", 404

    if booking["status"] != STATUS_NEW:
        return "Only new bookings can be edited", 400

    if request.method == "POST":
        validate_csrf()

        agent_name = validate_required_text(
            request.form.get("agent_name"),
            "agent name",
            max_length=100
        )
        address = validate_required_text(
            request.form.get("address"),
            "address",
            max_length=255
        )
        property_type = validate_property_type(
            request.form.get("property_type")
        )
        bedrooms = validate_int(
            request.form.get("bedrooms"),
            "bedrooms",
            min_value=0,
            max_value=50
        )
        preferred_date = validate_date(
            request.form.get("preferred_date"),
            "preferred date"
        )

        price = calculate_price(property_type, bedrooms)

        db.execute("""
            UPDATE bookings
            SET agent_name = ?,
                address = ?,
                property_type = ?,
                bedrooms = ?,
                preferred_date = ?,
                price = ?
            WHERE id = ? AND user_id = ?
        """, (
            agent_name,
            address,
            property_type,
            bedrooms,
            preferred_date,
            price,
            booking_id,
            session["user_id"]
        ))
        db.commit()

        return redirect("/epc")

    return render_template(
        "edit.html",
        booking=booking,
        csrf_token=get_csrf_token()
    )


# -----------------------
# Error Handlers
# -----------------------
@app.errorhandler(400)
def bad_request(error):
    return str(error), 400


@app.errorhandler(403)
def forbidden(error):
    return str(error), 403


@app.errorhandler(404)
def not_found(error):
    return str(error), 404

from flask import request, jsonify
from datetime import datetime
import sqlite3

@app.get("/debug-leads")
def debug_leads():
    conn = sqlite3.connect("bookings.db")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM leads ORDER BY rowid DESC LIMIT 10")
    rows = cursor.fetchall()
    conn.close()

    return jsonify([dict(row) for row in rows])

@app.route("/save-lead", methods=["GET", "POST"])
def save_lead():
    if request.method == "GET":
        return jsonify({"status": "save-lead route is working"})

    data = request.get_json(force=True)
    print("INCOMING LEAD:", data)

    name = (data.get("name") or data.get("full_name") or "").strip()
    email = (data.get("email") or "").strip()
    phone = (data.get("phone") or "").strip()
    address = (data.get("address") or "").strip()
    valuation = data.get("valuation", 0)
    source = (data.get("source") or "property_tool").strip()
    created_at = data.get("created_at") or datetime.now().isoformat()
    notes = (data.get("notes") or "").strip()

    if not name:
        return jsonify({"success": False, "error": "Missing name"}), 400
    if not email:
        return jsonify({"success": False, "error": "Missing email"}), 400
    if not phone:
        return jsonify({"success": False, "error": "Missing phone"}), 400
    if not address:
        return jsonify({"success": False, "error": "Missing address"}), 400

    try:
        valuation = int(float(valuation))

        db = get_db()
        db.execute("""
            INSERT INTO leads (
                name, email, phone, address, valuation,
                source, status, created_at, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name,
            email,
            phone,
            address,
            valuation,
            source,
            LEAD_STATUS_NEW,
            created_at,
            notes
        ))

        db.commit()
        print("LEAD SAVED SUCCESSFULLY")

        return jsonify({"success": True})

    except Exception as e:
        print("ERROR SAVING LEAD:", str(e))
        return jsonify({"success": False, "error": str(e)}), 500
# -----------------------
# App Entrypoint
# -----------------------
if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(debug=True)