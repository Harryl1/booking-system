import os
import sqlite3
import uuid
import secrets
import csv
import io
import smtplib
from functools import wraps
from datetime import datetime, timedelta
from email.message import EmailMessage
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    session,
    abort,
    g,
    jsonify,
    send_file,
    make_response,
)
import requests
from werkzeug.security import generate_password_hash, check_password_hash

from pdf_report import generate_pdf_report
from property_tool import calculate_property_decision, get_real_valuation, to_float

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "Generated_reports")
STATIC_DIR = os.path.join(BASE_DIR, "static")

app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", os.urandom(32))
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get("SESSION_COOKIE_SECURE", "0") == "1"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=8)

DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "bookings.db"))
INTERNAL_API_TOKEN = os.environ.get("INTERNAL_API_TOKEN", "")
REPORT_RETENTION_DAYS = int(os.environ.get("REPORT_RETENTION_DAYS", "30"))
LEAD_RETENTION_DAYS = int(os.environ.get("LEAD_RETENTION_DAYS", "365"))
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "*")
PRIVACY_NOTICE_URL = os.environ.get("PRIVACY_NOTICE_URL", "")
RATE_LIMIT_WINDOW_SECONDS = int(os.environ.get("RATE_LIMIT_WINDOW_SECONDS", "60"))
RATE_LIMIT_MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "30"))
LEAD_NOTIFICATION_EMAIL = os.environ.get("LEAD_NOTIFICATION_EMAIL", "")
CUSTOMER_EMAIL_FROM = os.environ.get("CUSTOMER_EMAIL_FROM", "")
SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "1") == "1"

RATE_LIMIT_BUCKETS = {}

os.makedirs(REPORTS_DIR, exist_ok=True)


@app.after_request
def add_api_cors_headers(response):
    if request.path.startswith("/api/property/"):
        response.headers["Access-Control-Allow-Origin"] = FRONTEND_ORIGIN
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    return response

ROLE_AGENT = "agent"
ROLE_ASSESSOR = "assessor"

STATUS_NEW = "new"
STATUS_ASSIGNED = "assigned"
STATUS_COMPLETED = "completed"

LEAD_STATUS_NEW = "new"
LEAD_STATUS_CONTACTED = "contacted"
LEAD_STATUS_VALUATION_BOOKED = "valuation booked"
LEAD_STATUS_QUALIFIED = "qualified"
LEAD_STATUS_ATTEMPTED = "attempted"
LEAD_STATUS_APPOINTMENT_BOOKED = "appointment booked"
LEAD_STATUS_WON = "won"
LEAD_STATUS_LOST = "lost"

VALID_LEAD_STATUSES = {
    LEAD_STATUS_NEW,
    LEAD_STATUS_CONTACTED,
    LEAD_STATUS_VALUATION_BOOKED,
    LEAD_STATUS_QUALIFIED,
    LEAD_STATUS_ATTEMPTED,
    LEAD_STATUS_APPOINTMENT_BOOKED,
    LEAD_STATUS_WON,
    LEAD_STATUS_LOST,
}
VALID_ROLES = {ROLE_AGENT, ROLE_ASSESSOR}
VALID_BOOKING_STATUSES = {STATUS_NEW, STATUS_ASSIGNED, STATUS_COMPLETED}
VALID_LEAD_STATUSES = {
    LEAD_STATUS_NEW,
    LEAD_STATUS_CONTACTED,
    LEAD_STATUS_VALUATION_BOOKED,
    LEAD_STATUS_QUALIFIED,
    LEAD_STATUS_ATTEMPTED,
    LEAD_STATUS_APPOINTMENT_BOOKED,
    LEAD_STATUS_WON,
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
            notes TEXT,
            lead_stage TEXT,
            is_hot_lead INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT,
            assigned_agent_id INTEGER,
            report_filename TEXT,
            report_token TEXT,
            report_expires_at TEXT,
            marketing_consent INTEGER NOT NULL DEFAULT 0,
            privacy_notice_accepted_at TEXT,
            retention_until TEXT,
            source_page TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            lead_score INTEGER NOT NULL DEFAULT 0,
            next_follow_up_at TEXT,
            FOREIGN KEY (assigned_agent_id) REFERENCES users (id)
        );

        CREATE TABLE IF NOT EXISTS lead_notes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            user_id INTEGER,
            note TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (lead_id) REFERENCES leads (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );

        CREATE TABLE IF NOT EXISTS lead_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            user_id INTEGER,
            title TEXT NOT NULL,
            due_at TEXT NOT NULL,
            completed_at TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (lead_id) REFERENCES leads (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id TEXT,
            created_at TEXT NOT NULL,
            ip_address TEXT,
            FOREIGN KEY (user_id) REFERENCES users (id)
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
def ensure_lead_action_columns():
    db = get_db()

    existing_columns = {
        row["name"]
        for row in db.execute("PRAGMA table_info(leads)").fetchall()
    }

    if "lead_stage" not in existing_columns:
        db.execute("ALTER TABLE leads ADD COLUMN lead_stage TEXT")

    if "is_hot_lead" not in existing_columns:
        db.execute("ALTER TABLE leads ADD COLUMN is_hot_lead INTEGER NOT NULL DEFAULT 0")

    if "updated_at" not in existing_columns:
        db.execute("ALTER TABLE leads ADD COLUMN updated_at TEXT")

    optional_columns = {
        "assigned_agent_id": "INTEGER",
        "report_filename": "TEXT",
        "report_token": "TEXT",
        "report_expires_at": "TEXT",
        "marketing_consent": "INTEGER NOT NULL DEFAULT 0",
        "privacy_notice_accepted_at": "TEXT",
        "retention_until": "TEXT",
        "source_page": "TEXT",
        "utm_source": "TEXT",
        "utm_medium": "TEXT",
        "utm_campaign": "TEXT",
        "lead_score": "INTEGER NOT NULL DEFAULT 0",
        "next_follow_up_at": "TEXT",
    }

    for column_name, column_definition in optional_columns.items():
        if column_name not in existing_columns:
            db.execute(f"ALTER TABLE leads ADD COLUMN {column_name} {column_definition}")

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


def validate_internal_api_token():
    if not INTERNAL_API_TOKEN and app.debug:
        return
    if not INTERNAL_API_TOKEN:
        abort(503, "Internal API token is not configured")

    supplied_token = (
        request.headers.get("X-Internal-Token")
        or request.headers.get("Authorization", "").replace("Bearer ", "", 1)
    )
    if not supplied_token or not secrets.compare_digest(supplied_token, INTERNAL_API_TOKEN):
        abort(403, "Invalid internal API token")


def client_ip():
    forwarded_for = request.headers.get("X-Forwarded-For", "")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    return request.remote_addr or "unknown"


def apply_rate_limit(key):
    if RATE_LIMIT_MAX_REQUESTS <= 0:
        return
    now = datetime.now()
    bucket = RATE_LIMIT_BUCKETS.setdefault(key, [])
    cutoff = now - timedelta(seconds=RATE_LIMIT_WINDOW_SECONDS)
    bucket[:] = [seen_at for seen_at in bucket if seen_at > cutoff]
    if len(bucket) >= RATE_LIMIT_MAX_REQUESTS:
        abort(429, "Too many requests. Please try again shortly.")
    bucket.append(now)


def write_audit_log(action, entity_type, entity_id=None):
    try:
        db = get_db()
        db.execute("""
            INSERT INTO audit_logs (
                user_id, action, entity_type, entity_id, created_at, ip_address
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            session.get("user_id"),
            action,
            entity_type,
            str(entity_id) if entity_id is not None else None,
            datetime.now().isoformat(),
            client_ip(),
        ))
        db.commit()
    except Exception:
        pass


def add_lead_note(lead_id, note, user_id=None):
    note = (note or "").strip()
    if not note:
        return
    db = get_db()
    db.execute("""
        INSERT INTO lead_notes (lead_id, user_id, note, created_at)
        VALUES (?, ?, ?, ?)
    """, (
        lead_id,
        user_id if user_id is not None else session.get("user_id"),
        note,
        datetime.now().isoformat(),
    ))


def create_follow_up_task(lead_id, title, due_at=None, user_id=None):
    db = get_db()
    due_at = due_at or (datetime.now() + timedelta(days=1)).isoformat()
    db.execute("""
        INSERT INTO lead_tasks (lead_id, user_id, title, due_at, created_at)
        VALUES (?, ?, ?, ?, ?)
    """, (
        lead_id,
        user_id,
        title,
        due_at,
        datetime.now().isoformat(),
    ))
    db.execute(
        "UPDATE leads SET next_follow_up_at = ? WHERE id = ?",
        (due_at, lead_id)
    )


def calculate_lead_score(data, marketing_consent=False):
    score = 10
    if marketing_consent:
        score += 10
    if data.get("phone"):
        score += 10
    if data.get("help_requested"):
        score += min(len(data.get("help_requested") or []) * 10, 30)
    if to_float(data.get("net_proceeds", 0)) > 50000:
        score += 15
    if to_float(data.get("max_budget", 0)) > 300000:
        score += 10
    if data.get("plan") == "buy":
        score += 10
    return min(score, 100)


def send_email(to_address, subject, body):
    if not SMTP_HOST or not CUSTOMER_EMAIL_FROM or not to_address:
        return False

    message = EmailMessage()
    message["From"] = CUSTOMER_EMAIL_FROM
    message["To"] = to_address
    message["Subject"] = subject
    message.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as smtp:
        if SMTP_USE_TLS:
            smtp.starttls()
        if SMTP_USERNAME and SMTP_PASSWORD:
            smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
        smtp.send_message(message)
    return True


def notify_new_lead(lead_id, name, email, phone, address, lead_score):
    if not LEAD_NOTIFICATION_EMAIL:
        return
    body = (
        f"New property tool lead #{lead_id}\n\n"
        f"Name: {name}\n"
        f"Email: {email}\n"
        f"Phone: {phone}\n"
        f"Address: {address}\n"
        f"Lead score: {lead_score}/100\n\n"
        f"Open leads: https://booking-system-b13f.onrender.com/leads"
    )
    try:
        send_email(LEAD_NOTIFICATION_EMAIL, f"New property lead: {name}", body)
    except Exception:
        pass


def send_customer_confirmation(email, report_url):
    if not email or not report_url:
        return
    body = (
        "Thanks for using the Equiome property tool.\n\n"
        f"Your personalised report is ready here:\n{report_url}\n\n"
        "This report is an indicative estimate only and is not financial, mortgage, or legal advice."
    )
    try:
        send_email(email, "Your Equiome property report", body)
    except Exception:
        pass


def agent_lead_clause(alias="leads"):
    if session.get("role") != ROLE_AGENT:
        return "", []
    user_id = session.get("user_id")
    prefix = f"{alias}." if alias else ""
    return f"({prefix}assigned_agent_id IS NULL OR {prefix}assigned_agent_id = ?)", [user_id]


def retention_date(days):
    return (datetime.now() + timedelta(days=days)).date().isoformat()


def cleanup_expired_reports():
    db = get_db()
    now = datetime.now().isoformat()
    expired = db.execute("""
        SELECT id, report_filename
        FROM leads
        WHERE report_filename IS NOT NULL
          AND report_expires_at IS NOT NULL
          AND report_expires_at < ?
    """, (now,)).fetchall()

    for lead in expired:
        report_path = os.path.join(REPORTS_DIR, lead["report_filename"])
        if os.path.exists(report_path):
            try:
                os.remove(report_path)
            except OSError:
                pass
        db.execute("""
            UPDATE leads
            SET report_filename = NULL,
                report_token = NULL,
                report_expires_at = NULL
            WHERE id = ?
        """, (lead["id"],))

    db.commit()


def cleanup_expired_leads():
    db = get_db()
    today = datetime.now().date().isoformat()
    expired = db.execute("""
        SELECT id, report_filename
        FROM leads
        WHERE retention_until IS NOT NULL
          AND retention_until < ?
          AND email NOT LIKE 'deleted+%@local'
    """, (today,)).fetchall()

    for lead in expired:
        if lead["report_filename"]:
            report_path = os.path.join(REPORTS_DIR, lead["report_filename"])
            if os.path.exists(report_path):
                try:
                    os.remove(report_path)
                except OSError:
                    pass
        db.execute("""
            UPDATE leads
            SET name = 'Deleted lead',
                email = ?,
                phone = '',
                address = '[removed after retention period]',
                notes = 'Personal data removed after retention period.',
                report_filename = NULL,
                report_token = NULL,
                report_expires_at = NULL,
                marketing_consent = 0
            WHERE id = ?
        """, (f"deleted+{lead['id']}@local", lead["id"]))

    db.commit()


@app.before_request
def apply_retention_housekeeping():
    if request.path.startswith("/api/property/"):
        apply_rate_limit(f"{client_ip()}:{request.path}")
    if request.endpoint == "static":
        return
    if session.get("last_retention_check") == datetime.now().date().isoformat():
        return
    cleanup_expired_reports()
    cleanup_expired_leads()
    session["last_retention_check"] = datetime.now().date().isoformat()


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


def scoped_lead_where(extra_clause="", extra_params=()):
    clauses = []
    params = []
    scope_clause, scope_params = agent_lead_clause("")
    if scope_clause:
        clauses.append(scope_clause)
        params.extend(scope_params)
    if extra_clause:
        clauses.append(extra_clause)
        params.extend(extra_params)
    if not clauses:
        return "", []
    return " WHERE " + " AND ".join(clauses), params


def count_scoped_leads(db, extra_clause="", extra_params=()):
    where_sql, params = scoped_lead_where(extra_clause, extra_params)
    return db.execute(f"SELECT COUNT(*) FROM leads{where_sql}", tuple(params)).fetchone()[0]


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
    db = get_db()
    user_count = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if user_count > 0 and "user_id" not in session:
        abort(403, "User registration is restricted")
    if user_count > 0 and session.get("role") != ROLE_AGENT:
        abort(403, "Only agents can create users")

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
        apply_rate_limit(f"{client_ip()}:login")
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

    total_leads = count_scoped_leads(db)
    new_leads = count_scoped_leads(db, "status = ?", (LEAD_STATUS_NEW,))
    contacted_leads = count_scoped_leads(db, "status = ?", (LEAD_STATUS_CONTACTED,))
    qualified_leads = count_scoped_leads(db, "status = ?", (LEAD_STATUS_QUALIFIED,))
    valuation_booked_leads = count_scoped_leads(db, "status = ?", (LEAD_STATUS_VALUATION_BOOKED,))
    lost_leads = count_scoped_leads(db, "status = ?", (LEAD_STATUS_LOST,))
    won_leads = count_scoped_leads(db, "status = ?", (LEAD_STATUS_WON,))
    hot_leads = count_scoped_leads(db, "is_hot_lead = 1 OR lead_score >= ?", (60,))

    task_scope_clause, task_scope_params = agent_lead_clause("leads")
    task_where = "lead_tasks.completed_at IS NULL AND date(lead_tasks.due_at) <= date(?)"
    task_params = [today]
    if task_scope_clause:
        task_where += f" AND {task_scope_clause}"
        task_params.extend(task_scope_params)
    tasks_due = db.execute(f"""
        SELECT COUNT(*)
        FROM lead_tasks
        JOIN leads ON leads.id = lead_tasks.lead_id
        WHERE {task_where}
    """, tuple(task_params)).fetchone()[0]

    recent_where_sql, recent_params = scoped_lead_where()
    recent_leads = db.execute(f"""
        SELECT *
        FROM leads
        {recent_where_sql}
        ORDER BY lead_score DESC, id DESC
        LIMIT 5
    """, tuple(recent_params)).fetchall()

    source_where_sql, source_params = scoped_lead_where()
    source_breakdown = db.execute(f"""
        SELECT source, COUNT(*) AS total
        FROM leads
        {source_where_sql}
        GROUP BY source
        ORDER BY total DESC
        LIMIT 5
    """, tuple(source_params)).fetchall()

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
        epc_required_leads=valuation_booked_leads,
        converted_leads=won_leads,
        won_leads=won_leads,
        lost_leads=lost_leads,
        hot_leads=hot_leads,
        tasks_due=tasks_due,
        source_breakdown=source_breakdown,
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
    priority_filter = request.args.get("priority", "all").strip().lower()

    where_clauses = []
    params = []

    scope_clause, scope_params = agent_lead_clause("")
    if scope_clause:
        where_clauses.append(scope_clause)
        params.extend(scope_params)

    if status_filter != "all":
        if status_filter in VALID_LEAD_STATUSES:
            where_clauses.append("status = ?")
            params.append(status_filter)

    if source_filter != "all":
        where_clauses.append("LOWER(source) = ?")
        params.append(source_filter)

    if priority_filter == "hot":
        where_clauses.append("(is_hot_lead = 1 OR lead_score >= ?)")
        params.append(60)
    elif priority_filter == "due":
        where_clauses.append("next_follow_up_at IS NOT NULL AND date(next_follow_up_at) <= date(?)")
        params.append(datetime.now().strftime("%Y-%m-%d"))

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

    query += " ORDER BY lead_score DESC, id DESC"

    leads = db.execute(query, tuple(params)).fetchall()
    lead_ids = [lead["id"] for lead in leads]
    notes_by_lead = {}
    tasks_by_lead = {}
    if lead_ids:
        placeholders = ",".join("?" for _ in lead_ids)
        note_rows = db.execute(f"""
            SELECT lead_id, note, created_at
            FROM lead_notes
            WHERE lead_id IN ({placeholders})
            ORDER BY created_at DESC
        """, tuple(lead_ids)).fetchall()
        for row in note_rows:
            notes_by_lead.setdefault(row["lead_id"], []).append(row)

        task_rows = db.execute(f"""
            SELECT id, lead_id, title, due_at, completed_at
            FROM lead_tasks
            WHERE lead_id IN ({placeholders})
            ORDER BY completed_at IS NOT NULL, due_at ASC
        """, tuple(lead_ids)).fetchall()
        for row in task_rows:
            tasks_by_lead.setdefault(row["lead_id"], []).append(row)

    total_leads = count_scoped_leads(db)
    new_leads = count_scoped_leads(db, "status = ?", (LEAD_STATUS_NEW,))
    contacted_leads = count_scoped_leads(db, "status = ?", (LEAD_STATUS_CONTACTED,))
    valuation_booked_leads = count_scoped_leads(db, "status = ?", (LEAD_STATUS_VALUATION_BOOKED,))
    hot_leads = count_scoped_leads(db, "is_hot_lead = 1 OR lead_score >= ?", (60,))

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
        priority_filter=priority_filter,
        search_query=search_query,
        lead_statuses=list(VALID_LEAD_STATUSES),
        hot_leads=hot_leads,
        notes_by_lead=notes_by_lead,
        tasks_by_lead=tasks_by_lead,
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
    scope_clause, scope_params = agent_lead_clause("")
    where_sql = "id = ?"
    params = [lead_id]
    if scope_clause:
        where_sql += f" AND {scope_clause}"
        params.extend(scope_params)

    cursor = db.execute(f"""
        UPDATE leads
        SET status = ?, contacted_at = ?, notes = ?
        WHERE {where_sql}
    """, (
        LEAD_STATUS_CONTACTED,
        contacted_at,
        notes,
        *params
    ))
    if cursor.rowcount == 0:
        abort(404, "Lead not found")

    add_lead_note(lead_id, notes or f"Marked contacted on {contacted_at}.")
    create_follow_up_task(
        lead_id,
        "Follow up after contact",
        due_at=(datetime.now() + timedelta(days=2)).isoformat(),
        user_id=session.get("user_id"),
    )
    db.commit()
    write_audit_log("marked_contacted", "lead", lead_id)
    return redirect("/leads")
@app.route("/leads/book-valuation/<int:lead_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def book_valuation(lead_id):
    validate_csrf()

    valuation_booked_at = request.form.get("valuation_booked_at")
    notes = request.form.get("notes", "")

    db = get_db()
    scope_clause, scope_params = agent_lead_clause("")
    where_sql = "id = ?"
    params = [lead_id]
    if scope_clause:
        where_sql += f" AND {scope_clause}"
        params.extend(scope_params)

    cursor = db.execute(f"""
        UPDATE leads
        SET status = ?, valuation_booked_at = ?, notes = ?
        WHERE {where_sql}
    """, (
        LEAD_STATUS_VALUATION_BOOKED,
        valuation_booked_at,
        notes,
        *params
    ))
    if cursor.rowcount == 0:
        abort(404, "Lead not found")

    add_lead_note(lead_id, notes or f"Valuation booked for {valuation_booked_at}.")
    db.commit()
    write_audit_log("booked_valuation", "lead", lead_id)
    return redirect("/leads")


@app.route("/leads/add-test", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def add_test_lead():
    validate_csrf()

    db = get_db()
    db.execute("""
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

    scope_clause, scope_params = agent_lead_clause("")
    where_sql = "id = ?"
    params = [lead_id]
    if scope_clause:
        where_sql += f" AND {scope_clause}"
        params.extend(scope_params)

    lead = db.execute(f"SELECT * FROM leads WHERE {where_sql}", tuple(params)).fetchone()

    if lead is None:
        return "Lead not found", 404

    db.execute(f"UPDATE leads SET status = ? WHERE {where_sql}", (new_status, *params))
    add_lead_note(lead_id, f"Status changed to {new_status}.")
    db.commit()
    write_audit_log("updated_status", "lead", lead_id)

    return redirect("/leads")


@app.route("/leads/add-note/<int:lead_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def add_note_to_lead(lead_id):
    validate_csrf()
    note = validate_required_text(request.form.get("note"), "note", max_length=1000)
    db = get_db()

    scope_clause, scope_params = agent_lead_clause("")
    where_sql = "id = ?"
    params = [lead_id]
    if scope_clause:
        where_sql += f" AND {scope_clause}"
        params.extend(scope_params)

    lead = db.execute(f"SELECT id FROM leads WHERE {where_sql}", tuple(params)).fetchone()
    if lead is None:
        abort(404, "Lead not found")

    add_lead_note(lead_id, note)
    db.execute(
        "UPDATE leads SET notes = ?, updated_at = ? WHERE id = ?",
        (note, datetime.now().isoformat(), lead_id)
    )
    db.commit()
    write_audit_log("added_note", "lead", lead_id)
    return redirect("/leads")


@app.route("/leads/tasks/complete/<int:task_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def complete_lead_task(task_id):
    validate_csrf()
    db = get_db()
    task = db.execute("""
        SELECT lead_tasks.id, lead_tasks.lead_id
        FROM lead_tasks
        JOIN leads ON leads.id = lead_tasks.lead_id
        WHERE lead_tasks.id = ?
    """, (task_id,)).fetchone()

    if task is None:
        abort(404, "Task not found")

    db.execute(
        "UPDATE lead_tasks SET completed_at = ? WHERE id = ?",
        (datetime.now().isoformat(), task_id)
    )
    db.commit()
    write_audit_log("completed_task", "lead_task", task_id)
    return redirect("/leads")


@app.get("/leads/export.csv")
@login_required
@role_required(ROLE_AGENT)
def export_leads_csv():
    db = get_db()
    where_sql, params = scoped_lead_where()
    rows = db.execute(f"""
        SELECT id, name, email, phone, address, valuation, source, utm_source,
               utm_medium, utm_campaign, status, lead_stage, lead_score,
               marketing_consent, created_at, next_follow_up_at, retention_until
        FROM leads
        {where_sql}
        ORDER BY id DESC
    """, tuple(params)).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "name", "email", "phone", "address", "valuation", "source",
        "utm_source", "utm_medium", "utm_campaign", "status", "lead_stage",
        "lead_score", "marketing_consent", "created_at", "next_follow_up_at",
        "retention_until",
    ])
    for row in rows:
        writer.writerow([row[key] for key in row.keys()])

    response = make_response(output.getvalue())
    response.headers["Content-Type"] = "text/csv"
    response.headers["Content-Disposition"] = "attachment; filename=leads.csv"
    write_audit_log("exported_csv", "lead")
    return response


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

@app.get("/debug-leads")
@login_required
@role_required(ROLE_AGENT)
def debug_leads():
    db = get_db()
    where_sql, params = scoped_lead_where()
    rows = db.execute(f"SELECT * FROM leads{where_sql} ORDER BY rowid DESC LIMIT 10", tuple(params)).fetchall()
    return jsonify([dict(row) for row in rows])


@app.post("/api/property/value")
def property_value():
    data = request.get_json(force=True)
    address = (data.get("address") or "").strip()
    property_type = (data.get("property_type") or "").strip()

    if not address:
        return jsonify({"error": "Address is required."}), 400

    try:
        valuation = get_real_valuation(address, property_type)
        return jsonify(valuation)
    except requests.RequestException as error:
        return jsonify({"error": f"Valuation API request failed: {str(error)}"}), 502
    except Exception:
        return jsonify({"error": "Unexpected valuation error."}), 500


@app.post("/api/property/calculate")
def property_calculate():
    data = request.get_json(force=True)
    try:
        return jsonify(calculate_property_decision(data))
    except ValueError as error:
        return jsonify({"error": str(error)}), 400
    except Exception:
        return jsonify({"error": "Unexpected calculator error."}), 500


@app.post("/api/property/lead")
def property_lead():
    data = request.get_json(force=True)
    return save_lead_payload(data, create_report=True)


@app.post("/api/property/lead-action")
def property_lead_action():
    data = request.get_json(force=True)

    lead_id = data.get("lead_id")
    email = (data.get("email") or "").strip().lower()
    action = (data.get("action") or "").strip()
    updated_at = datetime.now().isoformat()

    if action not in ["valuation_requested", "contact_requested"]:
        return jsonify({"success": False, "error": "Invalid action"}), 400
    if not lead_id or not email:
        return jsonify({"success": False, "error": "Lead id and email are required"}), 400

    lead_stage = action
    new_note_line = f"[{updated_at}] lead action: {lead_stage}"

    db = get_db()
    lead = db.execute("""
        SELECT id, notes
        FROM leads
        WHERE id = ? AND LOWER(email) = ?
    """, (lead_id, email)).fetchone()

    if lead is None:
        return jsonify({"success": False, "error": "Lead not found"}), 404

    existing_notes = lead["notes"] or ""
    updated_notes = (existing_notes + "\n" + new_note_line).strip()

    db.execute("""
        UPDATE leads
        SET lead_stage = ?,
            is_hot_lead = 1,
            updated_at = ?,
            notes = ?
        WHERE id = ?
    """, (lead_stage, updated_at, updated_notes, lead_id))
    add_lead_note(lead_id, f"Website action: {lead_stage}", user_id=None)
    create_follow_up_task(
        lead_id,
        "Respond to website follow-up request",
        due_at=(datetime.now() + timedelta(hours=4)).isoformat(),
        user_id=None,
    )
    db.commit()
    write_audit_log("website_action", "lead", lead_id)

    return jsonify({
        "success": True,
        "lead_id": lead_id,
        "lead_stage": lead_stage,
    })


@app.get("/reports/<report_token>")
def get_private_report(report_token):
    token = (report_token or "").strip()
    if not token:
        abort(404)

    db = get_db()
    lead = db.execute("""
        SELECT report_filename, report_expires_at
        FROM leads
        WHERE report_token = ?
    """, (token,)).fetchone()

    if lead is None:
        abort(404)

    if lead["report_expires_at"] and lead["report_expires_at"] < datetime.now().isoformat():
        abort(410, "Report link has expired")

    filename = lead["report_filename"]
    if not filename:
        abort(404)

    filepath = os.path.join(REPORTS_DIR, filename)
    if not os.path.exists(filepath):
        abort(404)

    return send_file(filepath, mimetype="application/pdf", as_attachment=True)


def create_lead_report(data):
    report_token = secrets.token_urlsafe(32)
    filename = f"report_{uuid.uuid4().hex}.pdf"
    filepath = os.path.join(REPORTS_DIR, filename)

    selected_services = data.get("help_requested") or data.get("selected_services") or []
    if isinstance(selected_services, str):
        selected_services = [selected_services.strip()] if selected_services.strip() else []

    pdf_data = {
        "name": data.get("name") or data.get("full_name"),
        "email": data.get("email"),
        "address": data.get("address"),
        "valuation_low": to_float(data.get("valuation_low", 0)),
        "valuation_high": to_float(data.get("valuation_high", data.get("valuation", 0))),
        "moving_costs": to_float(data.get("moving_costs", 0)),
        "net_proceeds": to_float(data.get("net_proceeds", 0)),
        "borrowing_power": to_float(data.get("borrowing_power", 0)),
        "max_budget": to_float(data.get("max_budget", 0)),
        "recommendation": data.get("recommendation") or "No recommendation available.",
        "selected_services": selected_services,
    }

    logo_path = os.path.join(STATIC_DIR, "logo.png")
    if not os.path.exists(logo_path):
        logo_path = None

    generate_pdf_report(pdf_data, filepath, logo_path=logo_path)
    report_expires_at = (datetime.now() + timedelta(days=REPORT_RETENTION_DAYS)).isoformat()
    return filename, report_token, report_expires_at


def save_lead_payload(data, create_report=False):
    name = (data.get("name") or data.get("full_name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    phone = (data.get("phone") or "").strip()
    address = (data.get("address") or "").strip()
    valuation = data.get("valuation") or data.get("valuation_high") or 0
    source = (data.get("source") or "property_tool").strip()
    source_page = (data.get("source_page") or data.get("page_url") or "").strip()
    utm_source = (data.get("utm_source") or "").strip()
    utm_medium = (data.get("utm_medium") or "").strip()
    utm_campaign = (data.get("utm_campaign") or "").strip()
    created_at = data.get("created_at") or datetime.now().isoformat()
    notes = (data.get("notes") or "").strip()
    marketing_consent = 1 if data.get("marketing_consent") else 0
    privacy_notice_accepted = 1 if data.get("privacy_notice_accepted") else 0
    privacy_notice_accepted_at = datetime.now().isoformat() if privacy_notice_accepted else None
    retention_until = data.get("retention_until") or retention_date(LEAD_RETENTION_DAYS)

    if not name:
        return jsonify({"success": False, "error": "Missing name"}), 400
    if not email:
        return jsonify({"success": False, "error": "Missing email"}), 400
    if not phone:
        return jsonify({"success": False, "error": "Missing phone"}), 400
    if not address:
        return jsonify({"success": False, "error": "Missing address"}), 400
    if not privacy_notice_accepted:
        return jsonify({"success": False, "error": "Privacy notice acceptance is required"}), 400

    report_filename = None
    report_token = None
    report_expires_at = None

    try:
        valuation = int(float(valuation))
        if create_report:
            report_filename, report_token, report_expires_at = create_lead_report(data)
        lead_score = calculate_lead_score(data, bool(marketing_consent))

        db = get_db()
        assigned_agent_id = session.get("user_id") if session.get("role") == ROLE_AGENT else None
        cursor = db.execute("""
            INSERT INTO leads (
                name, email, phone, address, valuation,
                source, status, created_at, notes,
                lead_stage, is_hot_lead, updated_at,
                assigned_agent_id, report_filename, report_token, report_expires_at,
                marketing_consent, privacy_notice_accepted_at, retention_until,
                source_page, utm_source, utm_medium, utm_campaign, lead_score, next_follow_up_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name,
            email,
            phone,
            address,
            valuation,
            source,
            LEAD_STATUS_NEW,
            created_at,
            notes,
            "report_generated" if create_report else "lead_saved",
            0,
            created_at,
            assigned_agent_id,
            report_filename,
            report_token,
            report_expires_at,
            marketing_consent,
            privacy_notice_accepted_at,
            retention_until,
            source_page,
            utm_source,
            utm_medium,
            utm_campaign,
            lead_score,
            (datetime.now() + timedelta(days=1)).isoformat(),
        ))

        lead_id = cursor.lastrowid
        add_lead_note(lead_id, "Lead captured from property decision tool.", user_id=assigned_agent_id)
        if notes:
            add_lead_note(lead_id, notes, user_id=assigned_agent_id)
        create_follow_up_task(
            lead_id,
            "First follow-up call",
            due_at=(datetime.now() + timedelta(days=1)).isoformat(),
            user_id=assigned_agent_id,
        )
        db.commit()
        write_audit_log("created", "lead", lead_id)

        response = {
            "success": True,
            "lead_id": lead_id,
        }
        if report_token:
            response["pdf_url"] = f"/reports/{report_token}"
            response["report_expires_at"] = report_expires_at
            send_customer_confirmation(email, request.host_url.rstrip("/") + response["pdf_url"])
        notify_new_lead(lead_id, name, email, phone, address, lead_score)
        return jsonify(response)

    except Exception as error:
        return jsonify({"success": False, "error": str(error)}), 500

@app.route("/save-lead", methods=["GET", "POST"])
def save_lead():
    validate_internal_api_token()

    if request.method == "GET":
        return jsonify({"status": "save-lead route is working"})

    data = request.get_json(force=True)
    return save_lead_payload(data)

@app.route("/update-lead", methods=["POST"])
def update_lead():
    validate_internal_api_token()
    data = request.get_json(force=True)

    email = (data.get("email") or "").strip().lower()
    lead_stage = (data.get("lead_stage") or "").strip()
    is_hot_lead = 1 if data.get("is_hot_lead") else 0
    updated_at = (data.get("updated_at") or datetime.now().isoformat()).strip()

    if not email:
        return jsonify({"success": False, "error": "Missing email"}), 400

    if not lead_stage:
        return jsonify({"success": False, "error": "Missing lead_stage"}), 400

    try:
        db = get_db()

        lead = db.execute(
            "SELECT id, notes FROM leads WHERE LOWER(email) = ? ORDER BY id DESC LIMIT 1",
            (email,)
        ).fetchone()

        if lead is None:
            return jsonify({"success": False, "error": "Lead not found"}), 404

        existing_notes = lead["notes"] or ""
        new_note_line = f"[{updated_at}] lead action: {lead_stage}"
        updated_notes = (existing_notes + "\n" + new_note_line).strip()

        db.execute("""
            UPDATE leads
            SET lead_stage = ?,
                is_hot_lead = ?,
                updated_at = ?,
                notes = ?
            WHERE id = ?
        """, (
            lead_stage,
            is_hot_lead,
            updated_at,
            updated_notes,
            lead["id"]
        ))

        db.commit()

        return jsonify({
            "success": True,
            "message": "Lead updated",
            "email": email,
            "lead_stage": lead_stage,
            "is_hot_lead": bool(is_hot_lead)
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
def bootstrap_app():
    with app.app_context():
        init_db()
        ensure_lead_action_columns()
        cleanup_expired_reports()
        cleanup_expired_leads()


bootstrap_app()


# -----------------------
# App Entrypoint
# -----------------------
if __name__ == "__main__":
    app.run(debug=os.environ.get("FLASK_DEBUG", "0") == "1")
