import os
import sqlite3
import uuid
import secrets
import csv
import io
import smtplib
import re
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

try:
    import psycopg
except ImportError:
    psycopg = None

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORTS_DIR = os.path.join(BASE_DIR, "Generated_reports")
STATIC_DIR = os.path.join(BASE_DIR, "static")
IS_PRODUCTION = (
    os.environ.get("FLASK_ENV") == "production"
    or os.environ.get("APP_ENV") == "production"
    or bool(os.environ.get("RENDER"))
)

app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", os.urandom(32))
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = os.environ.get(
    "SESSION_COOKIE_SECURE",
    "1" if IS_PRODUCTION else "0",
) == "1"
app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(hours=8)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
DB_PATH = os.environ.get("DB_PATH", os.path.join(BASE_DIR, "bookings.db"))
INTERNAL_API_TOKEN = os.environ.get("INTERNAL_API_TOKEN", "")
REPORT_RETENTION_DAYS = int(os.environ.get("REPORT_RETENTION_DAYS", "30"))
LEAD_RETENTION_DAYS = int(os.environ.get("LEAD_RETENTION_DAYS", "365"))
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "*")
APP_BASE_URL = os.environ.get("APP_BASE_URL", "https://booking-system-b13f.onrender.com")
PRIVACY_NOTICE_URL = os.environ.get("PRIVACY_NOTICE_URL", "")
AGENT_TERMS_URL = os.environ.get("AGENT_TERMS_URL", "")
ENABLE_TEST_TOOLS = os.environ.get("ENABLE_TEST_TOOLS", "0") == "1"
ENABLE_CSV_EXPORTS = os.environ.get("ENABLE_CSV_EXPORTS", "0") == "1"
CSV_EXPORT_MAX_ROWS = int(os.environ.get("CSV_EXPORT_MAX_ROWS", "500"))
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
def add_security_headers(response):
    if request.path.startswith("/api/property/"):
        response.headers["Access-Control-Allow-Origin"] = FRONTEND_ORIGIN
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"

    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=()",
    )
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "img-src 'self' data:; "
        "style-src 'self' 'unsafe-inline'; "
        "script-src 'self' 'unsafe-inline'; "
        "connect-src 'self' https://property-decision-tool.onrender.com https://booking-system-b13f.onrender.com; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self'",
    )
    if session.get("user_id") or request.path.startswith("/reports/"):
        response.headers.setdefault("Cache-Control", "no-store")
        response.headers.setdefault("Pragma", "no-cache")
    return response

ROLE_AGENT = "agent"
ROLE_ASSESSOR = "assessor"
ROLE_PLATFORM_ADMIN = "platform_admin"

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
VALID_ROLES = {ROLE_AGENT, ROLE_ASSESSOR, ROLE_PLATFORM_ADMIN}
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
VALID_SELLING_TIMEFRAMES = {"0-3 months", "3-6 months", "6-9 months", "9-12 months", "just exploring", ""}
SERVICE_VALUATION = "valuation"
SERVICE_EPC = "epc"
SERVICE_SOLICITOR = "solicitor"
SERVICE_MORTGAGE = "mortgage"
VALID_SERVICE_TYPES = {
    SERVICE_VALUATION,
    SERVICE_EPC,
    SERVICE_SOLICITOR,
    SERVICE_MORTGAGE,
}
SERVICE_LABELS = {
    SERVICE_VALUATION: "Valuation",
    SERVICE_EPC: "EPC",
    SERVICE_SOLICITOR: "Solicitor",
    SERVICE_MORTGAGE: "Mortgage",
}
SERVICE_ALIASES = {
    "agent_valuation": SERVICE_VALUATION,
    "local_agent_valuation": SERVICE_VALUATION,
    "valuation": SERVICE_VALUATION,
    "epc": SERVICE_EPC,
    "epc_booking": SERVICE_EPC,
    "conveyancing_quote": SERVICE_SOLICITOR,
    "solicitor": SERVICE_SOLICITOR,
    "legal": SERVICE_SOLICITOR,
    "mortgage_advice": SERVICE_MORTGAGE,
    "mortgage": SERVICE_MORTGAGE,
}
REFERRAL_STATUS_NEW = "new"
REFERRAL_STATUS_REFERRED = "referred"
REFERRAL_STATUS_IN_PROGRESS = "in progress"
REFERRAL_STATUS_COMPLETED = "completed"
REFERRAL_STATUS_DECLINED = "declined"
VALID_REFERRAL_STATUSES = {
    REFERRAL_STATUS_NEW,
    REFERRAL_STATUS_REFERRED,
    REFERRAL_STATUS_IN_PROGRESS,
    REFERRAL_STATUS_COMPLETED,
    REFERRAL_STATUS_DECLINED,
}


# -----------------------
# Database
# -----------------------
class CompatRow(dict):
    def __init__(self, columns, values):
        super().__init__(zip(columns, values))
        self._columns = list(columns)

    def __getitem__(self, key):
        if isinstance(key, int):
            return super().__getitem__(self._columns[key])
        return super().__getitem__(key)


class PostgresCursor:
    def __init__(self, cursor):
        self.cursor = cursor
        self._columns = [
            getattr(column, "name", column[0])
            for column in cursor.description
        ] if cursor.description else []

    @property
    def rowcount(self):
        return self.cursor.rowcount

    @property
    def lastrowid(self):
        row = self.fetchone()
        return row["id"] if row and "id" in row else None

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is None:
            return None
        return CompatRow(self._columns, row)

    def fetchall(self):
        return [CompatRow(self._columns, row) for row in self.cursor.fetchall()]


class PostgresConnection:
    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql, params=()):
        sql = to_postgres_sql(sql)
        cursor = self.connection.execute(sql, tuple(params or ()))
        return PostgresCursor(cursor)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


def using_postgres():
    return bool(DATABASE_URL)


def is_integrity_error(error):
    if isinstance(error, sqlite3.IntegrityError):
        return True
    return psycopg is not None and isinstance(error, psycopg.IntegrityError)


def to_postgres_sql(sql):
    converted = sql.strip()
    converted = converted.replace("INSERT OR IGNORE INTO", "INSERT INTO")
    converted = converted.replace("GROUP_CONCAT(service_type || ':' || status, '; ')", "STRING_AGG(service_type || ':' || status, '; ')")
    converted = converted.replace("rowid DESC", "id DESC")
    converted = converted.replace("?", "%s")

    lower_sql = converted.lower()
    if lower_sql.startswith("insert into service_referrals") and "on conflict" not in lower_sql:
        converted += " ON CONFLICT (lead_id, service_type) DO NOTHING"
    if lower_sql.startswith("insert into branch_territories") and "on conflict" not in lower_sql:
        converted += " ON CONFLICT (organisation_id, postcode_prefix) DO NOTHING"
    if (
        (lower_sql.startswith("insert into leads") or lower_sql.startswith("insert into organisations"))
        and "returning" not in lower_sql
        and "on conflict" not in lower_sql
    ):
        converted += " RETURNING id"

    return converted


def table_columns(db, table_name):
    if using_postgres():
        rows = db.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
        """, (table_name,)).fetchall()
        return {row["column_name"] for row in rows}

    return {
        row["name"]
        for row in db.execute(f"PRAGMA table_info({table_name})").fetchall()
    }


def get_db():
    if "db" not in g:
        if using_postgres():
            if psycopg is None:
                raise RuntimeError("DATABASE_URL is set, but psycopg is not installed")
            g.db = PostgresConnection(psycopg.connect(DATABASE_URL))
        else:
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

    if using_postgres():
        postgres_statements = [
            """
            CREATE TABLE IF NOT EXISTS organisations (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                subscription_plan TEXT NOT NULL DEFAULT 'starter',
                billing_status TEXT NOT NULL DEFAULT 'trial',
                lead_allowance INTEGER NOT NULL DEFAULT 50,
                trial_ends_at TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                role TEXT NOT NULL,
                organisation_id INTEGER REFERENCES organisations (id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS branch_territories (
                id SERIAL PRIMARY KEY,
                organisation_id INTEGER NOT NULL REFERENCES organisations (id),
                label TEXT NOT NULL,
                postcode_prefix TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE (organisation_id, postcode_prefix)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS leads (
                id SERIAL PRIMARY KEY,
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
                assigned_agent_id INTEGER REFERENCES users (id),
                report_filename TEXT,
                report_token TEXT,
                report_expires_at TEXT,
                marketing_consent INTEGER NOT NULL DEFAULT 0,
                privacy_notice_accepted_at TEXT,
                referral_consent_accepted_at TEXT,
                referral_fee_disclosure_accepted_at TEXT,
                retention_until TEXT,
                source_page TEXT,
                utm_source TEXT,
                utm_medium TEXT,
                utm_campaign TEXT,
                selling_timeframe TEXT,
                lead_score INTEGER NOT NULL DEFAULT 0,
                next_follow_up_at TEXT,
                organisation_id INTEGER REFERENCES organisations (id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lead_notes (
                id SERIAL PRIMARY KEY,
                lead_id INTEGER NOT NULL REFERENCES leads (id),
                user_id INTEGER REFERENCES users (id),
                note TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS lead_tasks (
                id SERIAL PRIMARY KEY,
                lead_id INTEGER NOT NULL REFERENCES leads (id),
                user_id INTEGER REFERENCES users (id),
                title TEXT NOT NULL,
                due_at TEXT NOT NULL,
                completed_at TEXT,
                created_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users (id),
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT,
                created_at TEXT NOT NULL,
                ip_address TEXT
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS service_referrals (
                id SERIAL PRIMARY KEY,
                lead_id INTEGER NOT NULL REFERENCES leads (id),
                service_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'new',
                assigned_to TEXT,
                referred_at TEXT,
                completed_at TEXT,
                fee_expected INTEGER NOT NULL DEFAULT 0,
                fee_received INTEGER NOT NULL DEFAULT 0,
                notes TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT,
                UNIQUE (lead_id, service_type)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS bookings (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users (id),
                agent_name TEXT NOT NULL,
                address TEXT NOT NULL,
                property_type TEXT NOT NULL,
                bedrooms INTEGER NOT NULL,
                preferred_date TEXT NOT NULL,
                price INTEGER NOT NULL,
                status TEXT NOT NULL,
                assigned_assessor_id INTEGER REFERENCES users (id)
            )
            """,
        ]
        for statement in postgres_statements:
            db.execute(statement)
        db.commit()
        return

    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            organisation_id INTEGER,
            FOREIGN KEY (organisation_id) REFERENCES organisations (id)
        );

        CREATE TABLE IF NOT EXISTS organisations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            subscription_plan TEXT NOT NULL DEFAULT 'starter',
            billing_status TEXT NOT NULL DEFAULT 'trial',
            lead_allowance INTEGER NOT NULL DEFAULT 50,
            trial_ends_at TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS branch_territories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            organisation_id INTEGER NOT NULL,
            label TEXT NOT NULL,
            postcode_prefix TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (organisation_id) REFERENCES organisations (id),
            UNIQUE (organisation_id, postcode_prefix)
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
            referral_consent_accepted_at TEXT,
            referral_fee_disclosure_accepted_at TEXT,
            retention_until TEXT,
            source_page TEXT,
            utm_source TEXT,
            utm_medium TEXT,
            utm_campaign TEXT,
            selling_timeframe TEXT,
            lead_score INTEGER NOT NULL DEFAULT 0,
            next_follow_up_at TEXT,
            organisation_id INTEGER,
            FOREIGN KEY (assigned_agent_id) REFERENCES users (id),
            FOREIGN KEY (organisation_id) REFERENCES organisations (id)
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

        CREATE TABLE IF NOT EXISTS service_referrals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            service_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'new',
            assigned_to TEXT,
            referred_at TEXT,
            completed_at TEXT,
            fee_expected INTEGER NOT NULL DEFAULT 0,
            fee_received INTEGER NOT NULL DEFAULT 0,
            notes TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT,
            FOREIGN KEY (lead_id) REFERENCES leads (id),
            UNIQUE (lead_id, service_type)
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

    existing_columns = table_columns(db, "leads")

    if "lead_stage" not in existing_columns:
        db.execute("ALTER TABLE leads ADD COLUMN lead_stage TEXT")

    if "is_hot_lead" not in existing_columns:
        db.execute("ALTER TABLE leads ADD COLUMN is_hot_lead INTEGER NOT NULL DEFAULT 0")

    if "updated_at" not in existing_columns:
        db.execute("ALTER TABLE leads ADD COLUMN updated_at TEXT")

    optional_columns = {
        "organisation_id": "INTEGER",
        "assigned_agent_id": "INTEGER",
        "report_filename": "TEXT",
        "report_token": "TEXT",
        "report_expires_at": "TEXT",
        "marketing_consent": "INTEGER NOT NULL DEFAULT 0",
        "privacy_notice_accepted_at": "TEXT",
        "referral_consent_accepted_at": "TEXT",
        "referral_fee_disclosure_accepted_at": "TEXT",
        "retention_until": "TEXT",
        "source_page": "TEXT",
        "utm_source": "TEXT",
        "utm_medium": "TEXT",
        "utm_campaign": "TEXT",
        "selling_timeframe": "TEXT",
        "lead_score": "INTEGER NOT NULL DEFAULT 0",
        "next_follow_up_at": "TEXT",
    }

    for column_name, column_definition in optional_columns.items():
        if column_name not in existing_columns:
            db.execute(f"ALTER TABLE leads ADD COLUMN {column_name} {column_definition}")

    user_columns = table_columns(db, "users")
    if "organisation_id" not in user_columns:
        db.execute("ALTER TABLE users ADD COLUMN organisation_id INTEGER")

    ensure_default_organisation(db)

    db.commit()


def ensure_default_organisation(db):
    now = datetime.now().isoformat()
    default_org = db.execute(
        "SELECT id FROM organisations ORDER BY id ASC LIMIT 1"
    ).fetchone()
    if default_org is None:
        cursor = db.execute("""
            INSERT INTO organisations (
                name, subscription_plan, billing_status, lead_allowance, trial_ends_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            "Equiome Demo Agency",
            "pro",
            "trial",
            500,
            (datetime.now() + timedelta(days=30)).date().isoformat(),
            now,
        ))
        organisation_id = cursor.lastrowid
    else:
        organisation_id = default_org["id"]

    db.execute(
        "UPDATE users SET organisation_id = ? WHERE organisation_id IS NULL",
        (organisation_id,)
    )
    db.execute(
        "UPDATE leads SET organisation_id = ? WHERE organisation_id IS NULL",
        (organisation_id,)
    )


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


def admin_or_agent_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if "user_id" not in session:
            return redirect("/login")
        if session.get("role") not in {ROLE_AGENT, ROLE_PLATFORM_ADMIN}:
            abort(403)
        return view_func(*args, **kwargs)
    return wrapper


def is_platform_admin():
    return session.get("role") == ROLE_PLATFORM_ADMIN


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
    requested_services = normalise_requested_services(data.get("help_requested") or data.get("selected_services"))
    if requested_services:
        score += min(len(requested_services) * 10, 30)
    if to_float(data.get("net_proceeds", 0)) > 50000:
        score += 15
    if to_float(data.get("max_budget", 0)) > 300000:
        score += 10
    if data.get("plan") == "buy":
        score += 10
    selling_timeframe = (data.get("selling_timeframe") or "").strip().lower()
    timeframe_scores = {
        "0-3 months": 30,
        "3-6 months": 20,
        "6-9 months": 10,
        "9-12 months": 5,
        "just exploring": 0,
    }
    score += timeframe_scores.get(selling_timeframe, 0)
    return min(score, 100)


def lead_score_factors(lead, referrals=None):
    referrals = referrals or []
    factors = []
    if lead["marketing_consent"]:
        factors.append("Marketing consent captured")
    if lead["phone"]:
        factors.append("Phone number supplied")
    if referrals:
        factors.append(f"{len(referrals)} service interest(s) requested")
    if lead["selling_timeframe"]:
        factors.append(f"Selling timeframe: {lead['selling_timeframe']}")
    if to_float(lead["valuation"], 0) >= 250000:
        factors.append("Material property value")
    if (lead["lead_score"] or 0) >= 60:
        factors.append("High-priority lead score")
    if not factors:
        factors.append("Basic contact details captured")
    return factors


def best_next_action(lead, tasks=None, referrals=None):
    tasks = tasks or []
    referrals = referrals or []
    open_tasks = [task for task in tasks if not task["completed_at"]]
    if open_tasks:
        return f"Complete task: {open_tasks[0]['title']}"
    if lead["status"] == LEAD_STATUS_NEW and lead["selling_timeframe"] == "0-3 months":
        return "Call immediately: seller says they may move within 0-3 months"
    if lead["status"] == LEAD_STATUS_NEW:
        return "Call the lead and qualify their moving timeline"
    if lead["status"] == LEAD_STATUS_CONTACTED:
        return "Book a valuation appointment or mark the next follow-up"
    if lead["status"] in {LEAD_STATUS_VALUATION_BOOKED, LEAD_STATUS_APPOINTMENT_BOOKED}:
        return "Prepare valuation notes and confirm referral opportunities"
    if referrals:
        return "Review open service referrals and update fee status"
    if lead["status"] == LEAD_STATUS_WON:
        return "Record revenue and keep referral follow-up current"
    if lead["status"] == LEAD_STATUS_LOST:
        return "Add loss reason and close remaining tasks"
    return "Add a note with the latest outcome"


def contact_priority_label(lead):
    score = lead["lead_score"] or 0
    if score >= 70:
        return "Hot"
    if score >= 45:
        return "Warm"
    return "New"


def normalise_service_type(service):
    service_key = (service or "").strip().lower()
    return SERVICE_ALIASES.get(service_key)


def normalise_requested_services(services):
    cleaned = []
    if isinstance(services, str):
        services = [services]
    for service in services or []:
        service_type = normalise_service_type(service)
        if service_type and service_type not in cleaned:
            cleaned.append(service_type)
    return cleaned


def create_service_referrals(lead_id, services):
    db = get_db()
    created = []
    now = datetime.now().isoformat()
    for service_type in normalise_requested_services(services):
        db.execute("""
            INSERT OR IGNORE INTO service_referrals (
                lead_id, service_type, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?)
        """, (
            lead_id,
            service_type,
            REFERRAL_STATUS_NEW,
            now,
            now,
        ))
        created.append(service_type)
    return created


def get_referrals_for_leads(db, lead_ids):
    referrals_by_lead = {}
    if not lead_ids:
        return referrals_by_lead

    placeholders = ",".join("?" for _ in lead_ids)
    rows = db.execute(f"""
        SELECT *
        FROM service_referrals
        WHERE lead_id IN ({placeholders})
        ORDER BY created_at ASC
    """, tuple(lead_ids)).fetchall()

    for row in rows:
        referrals_by_lead.setdefault(row["lead_id"], []).append(row)
    return referrals_by_lead


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


def notify_new_lead(lead_id, name, email, phone, address, lead_score, selling_timeframe=""):
    if not LEAD_NOTIFICATION_EMAIL:
        return
    body = (
        f"New property tool lead #{lead_id}\n\n"
        f"Name: {name}\n"
        f"Email: {email}\n"
        f"Phone: {phone}\n"
        f"Address: {address}\n"
        f"Selling timeframe: {selling_timeframe or 'Not supplied'}\n"
        f"Lead score: {lead_score}/100\n\n"
        f"Recommended action: call and qualify the seller while the enquiry is fresh.\n\n"
        f"Open lead: {APP_BASE_URL}/leads/{lead_id}\n"
        f"Open leads: {APP_BASE_URL}/leads"
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
        "A local property expert can help confirm your valuation range and next steps.\n\n"
        "This report is an indicative estimate only and is not financial, mortgage, or legal advice."
    )
    try:
        send_email(email, "Your Equiome property report", body)
    except Exception:
        pass


def agent_lead_clause(alias="leads"):
    if session.get("role") != ROLE_AGENT:
        return "", []
    organisation_id = session.get("organisation_id")
    if not organisation_id:
        return "", []
    prefix = f"{alias}." if alias else ""
    return f"{prefix}organisation_id = ?", [organisation_id]


def current_organisation_id():
    return session.get("organisation_id")


def extract_postcode_prefix(address):
    text = (address or "").upper()
    match = re.search(r"\b([A-Z]{1,2}\d[A-Z\d]?)\s*\d[A-Z]{2}\b", text)
    if match:
        return match.group(1)
    compact = re.sub(r"[^A-Z0-9 ]", " ", text)
    parts = compact.split()
    for part in parts:
        if re.match(r"^[A-Z]{1,2}\d[A-Z\d]?$", part):
            return part
    return ""


def default_organisation_id(db):
    row = db.execute("SELECT id FROM organisations ORDER BY id ASC LIMIT 1").fetchone()
    return row["id"] if row else None


def organisation_for_address(db, address):
    prefix = extract_postcode_prefix(address)
    if prefix:
        territory = db.execute("""
            SELECT organisation_id
            FROM branch_territories
            WHERE ? LIKE postcode_prefix || ?
            ORDER BY LENGTH(postcode_prefix) DESC
            LIMIT 1
        """, (prefix, "%")).fetchone()
        if territory:
            return territory["organisation_id"]
    return default_organisation_id(db)


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
          AND email NOT LIKE ?
    """, (today, "deleted+%@local")).fetchall()

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
                marketing_consent = 0,
                referral_consent_accepted_at = NULL,
                referral_fee_disclosure_accepted_at = NULL
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


def truthy(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "accepted"}
    return bool(value)


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


def validate_selling_timeframe(value):
    value = (value or "").strip().lower()
    labels = {
        "0-3 months": "0-3 months",
        "3-6 months": "3-6 months",
        "6-9 months": "6-9 months",
        "9-12 months": "9-12 months",
        "just exploring": "Just exploring",
        "exploring": "Just exploring",
        "": "",
    }
    if value not in labels:
        abort(400, "Invalid selling timeframe")
    return labels[value]


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


def admin_scope_clause(alias="leads"):
    if is_platform_admin():
        return "", []
    return agent_lead_clause(alias)


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
        organisation_id = session.get("organisation_id") or default_organisation_id(db)

        try:
            db.execute(
                "INSERT INTO users (username, password, role, organisation_id) VALUES (?, ?, ?, ?)",
                (username, hashed_password, role, organisation_id)
            )
            db.commit()
        except Exception as error:
            db.rollback()
            if not is_integrity_error(error):
                raise
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
            session.permanent = True
            session["user_id"] = user["id"]
            session["username"] = user["username"]
            session["role"] = user["role"]
            session["organisation_id"] = user["organisation_id"]
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


@app.get("/privacy")
def privacy_notice():
    return render_template(
        "privacy.html",
        retention_days=LEAD_RETENTION_DAYS,
        report_retention_days=REPORT_RETENTION_DAYS,
    )


@app.get("/agent-terms")
def agent_terms():
    return render_template("agent_terms.html")


# -----------------------
# equiome dashboard
# -----------------------
@app.route("/")
@login_required
def agencyhub():
    if session.get("role") == ROLE_ASSESSOR:
        return redirect("/epc")

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

    referral_scope_clause, referral_scope_params = agent_lead_clause("leads")
    referral_where = ""
    referral_params = []
    if referral_scope_clause:
        referral_where = f"WHERE {referral_scope_clause}"
        referral_params.extend(referral_scope_params)
    referral_breakdown = db.execute(f"""
        SELECT service_referrals.service_type,
               service_referrals.status,
               COUNT(*) AS total
        FROM service_referrals
        JOIN leads ON leads.id = service_referrals.lead_id
        {referral_where}
        GROUP BY service_referrals.service_type, service_referrals.status
        ORDER BY service_referrals.service_type ASC, service_referrals.status ASC
    """, tuple(referral_params)).fetchall()

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
        referral_breakdown=referral_breakdown,
        service_labels=SERVICE_LABELS,
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
@admin_or_agent_required
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
    referrals_by_lead = {}
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
        referrals_by_lead = get_referrals_for_leads(db, lead_ids)

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
        referrals_by_lead=referrals_by_lead,
        service_labels=SERVICE_LABELS,
        referral_statuses=list(VALID_REFERRAL_STATUSES),
        enable_test_tools=ENABLE_TEST_TOOLS,
        csv_exports_enabled=ENABLE_CSV_EXPORTS,
        csrf_token=get_csrf_token()
    )


@app.get("/leads/<int:lead_id>")
@login_required
@admin_or_agent_required
def lead_detail(lead_id):
    db = get_db()
    scope_clause, scope_params = agent_lead_clause("")
    where_sql = "id = ?"
    params = [lead_id]
    if scope_clause:
        where_sql += f" AND {scope_clause}"
        params.extend(scope_params)

    lead = db.execute(f"SELECT * FROM leads WHERE {where_sql}", tuple(params)).fetchone()
    if lead is None:
        abort(404, "Lead not found")

    notes = db.execute("""
        SELECT lead_notes.*, users.username
        FROM lead_notes
        LEFT JOIN users ON users.id = lead_notes.user_id
        WHERE lead_notes.lead_id = ?
        ORDER BY lead_notes.created_at DESC
    """, (lead_id,)).fetchall()
    tasks = db.execute("""
        SELECT *
        FROM lead_tasks
        WHERE lead_id = ?
        ORDER BY completed_at IS NOT NULL, due_at ASC
    """, (lead_id,)).fetchall()
    referrals = db.execute("""
        SELECT *
        FROM service_referrals
        WHERE lead_id = ?
        ORDER BY created_at ASC
    """, (lead_id,)).fetchall()

    write_audit_log("viewed", "lead", lead_id)
    return render_template(
        "lead_detail.html",
        lead=lead,
        notes=notes,
        tasks=tasks,
        referrals=referrals,
        next_action=best_next_action(lead, tasks, referrals),
        score_factors=lead_score_factors(lead, referrals),
        priority_label=contact_priority_label(lead),
        lead_statuses=list(VALID_LEAD_STATUSES),
        referral_statuses=list(VALID_REFERRAL_STATUSES),
        service_labels=SERVICE_LABELS,
        csrf_token=get_csrf_token(),
    )


@app.get("/referrals")
@login_required
@admin_or_agent_required
def referrals():
    db = get_db()
    status_filter = request.args.get("status", "all").strip().lower()
    service_filter = request.args.get("service", "all").strip().lower()

    where_clauses = []
    params = []
    scope_clause, scope_params = agent_lead_clause("leads")
    if scope_clause:
        where_clauses.append(scope_clause)
        params.extend(scope_params)
    if status_filter != "all" and status_filter in VALID_REFERRAL_STATUSES:
        where_clauses.append("service_referrals.status = ?")
        params.append(status_filter)
    if service_filter != "all" and service_filter in VALID_SERVICE_TYPES:
        where_clauses.append("service_referrals.service_type = ?")
        params.append(service_filter)

    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    rows = db.execute(f"""
        SELECT service_referrals.*, leads.name, leads.email, leads.phone, leads.address, leads.lead_score,
               leads.referral_consent_accepted_at,
               leads.referral_fee_disclosure_accepted_at
        FROM service_referrals
        JOIN leads ON leads.id = service_referrals.lead_id
        {where_sql}
        ORDER BY service_referrals.status ASC, leads.lead_score DESC, service_referrals.created_at DESC
    """, tuple(params)).fetchall()

    return render_template(
        "referrals.html",
        referrals=rows,
        status_filter=status_filter,
        service_filter=service_filter,
        referral_statuses=list(VALID_REFERRAL_STATUSES),
        service_types=list(VALID_SERVICE_TYPES),
        service_labels=SERVICE_LABELS,
        csv_exports_enabled=ENABLE_CSV_EXPORTS,
        csrf_token=get_csrf_token(),
    )


@app.get("/tasks")
@login_required
@admin_or_agent_required
def tasks():
    db = get_db()
    view_filter = request.args.get("view", "open").strip().lower()
    where_clauses = []
    params = []
    scope_clause, scope_params = agent_lead_clause("leads")
    if scope_clause:
        where_clauses.append(scope_clause)
        params.extend(scope_params)
    if view_filter == "due":
        where_clauses.append("lead_tasks.completed_at IS NULL AND date(lead_tasks.due_at) <= date(?)")
        params.append(datetime.now().date().isoformat())
    elif view_filter == "completed":
        where_clauses.append("lead_tasks.completed_at IS NOT NULL")
    else:
        where_clauses.append("lead_tasks.completed_at IS NULL")

    where_sql = "WHERE " + " AND ".join(where_clauses)
    rows = db.execute(f"""
        SELECT lead_tasks.*, leads.name, leads.email, leads.phone, leads.address, leads.lead_score
        FROM lead_tasks
        JOIN leads ON leads.id = lead_tasks.lead_id
        {where_sql}
        ORDER BY lead_tasks.completed_at IS NOT NULL, lead_tasks.due_at ASC
    """, tuple(params)).fetchall()

    return render_template(
        "tasks.html",
        tasks=rows,
        view_filter=view_filter,
        csrf_token=get_csrf_token(),
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
    if not ENABLE_TEST_TOOLS:
        abort(404)
    validate_csrf()

    db = get_db()
    db.execute("""
        INSERT INTO leads (
            name, email, phone, address, valuation,
            source, status, created_at, notes, organisation_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        "John Smith",
        "john@email.com",
        "07123456789",
        "123 Test Street",
        250000,
        "facebook",
        LEAD_STATUS_NEW,
        datetime.now().strftime("%Y-%m-%d"),
        "",
        current_organisation_id() or default_organisation_id(db),
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


@app.route("/leads/add-task/<int:lead_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def add_task_to_lead(lead_id):
    validate_csrf()
    title = validate_required_text(request.form.get("title"), "task", max_length=255)
    due_date = request.form.get("due_date") or ""
    due_time = request.form.get("due_time") or "09:00"
    if due_date:
        validate_date(due_date, "due date")
        due_at = f"{due_date}T{due_time}"
    else:
        due_at = (datetime.now() + timedelta(days=1)).isoformat()

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

    create_follow_up_task(lead_id, title, due_at=due_at, user_id=session.get("user_id"))
    add_lead_note(lead_id, f"Task added: {title}")
    db.commit()
    write_audit_log("added_task", "lead", lead_id)
    return redirect(f"/leads/{lead_id}")


@app.route("/referrals/update/<int:referral_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def update_referral(referral_id):
    validate_csrf()
    status = (request.form.get("status") or "").strip().lower()
    assigned_to = (request.form.get("assigned_to") or "").strip()
    notes = (request.form.get("notes") or "").strip()
    fee_expected = validate_int(
        request.form.get("fee_expected") or 0,
        "expected fee",
        min_value=0,
        max_value=1000000,
    )
    fee_received = validate_int(
        request.form.get("fee_received") or 0,
        "received fee",
        min_value=0,
        max_value=1000000,
    )

    if status not in VALID_REFERRAL_STATUSES:
        abort(400, "Invalid referral status")

    db = get_db()
    referral = db.execute("""
        SELECT service_referrals.*, leads.id AS lead_id, leads.organisation_id,
               leads.referral_consent_accepted_at,
               leads.referral_fee_disclosure_accepted_at
        FROM service_referrals
        JOIN leads ON leads.id = service_referrals.lead_id
        WHERE service_referrals.id = ?
    """, (referral_id,)).fetchone()

    if referral is None:
        abort(404, "Referral not found")
    if session.get("role") == ROLE_AGENT and referral["organisation_id"] != current_organisation_id():
        abort(404, "Referral not found")
    if status in {REFERRAL_STATUS_REFERRED, REFERRAL_STATUS_IN_PROGRESS, REFERRAL_STATUS_COMPLETED} and not referral["referral_consent_accepted_at"]:
        abort(400, "Referral consent has not been captured for this lead")
    if (fee_expected > 0 or fee_received > 0) and not referral["referral_fee_disclosure_accepted_at"]:
        abort(400, "Referral fee disclosure has not been captured for this lead")

    referred_at = referral["referred_at"]
    completed_at = referral["completed_at"]
    now = datetime.now().isoformat()
    if status in {REFERRAL_STATUS_REFERRED, REFERRAL_STATUS_IN_PROGRESS} and not referred_at:
        referred_at = now
    if status == REFERRAL_STATUS_COMPLETED and not completed_at:
        completed_at = now

    db.execute("""
        UPDATE service_referrals
        SET status = ?,
            assigned_to = ?,
            referred_at = ?,
            completed_at = ?,
            fee_expected = ?,
            fee_received = ?,
            notes = ?,
            updated_at = ?
        WHERE id = ?
    """, (
        status,
        assigned_to,
        referred_at,
        completed_at,
        fee_expected,
        fee_received,
        notes,
        now,
        referral_id,
    ))
    add_lead_note(
        referral["lead_id"],
        f"{SERVICE_LABELS.get(referral['service_type'], referral['service_type'])} referral updated to {status}."
    )
    db.commit()
    write_audit_log("updated_referral", "service_referral", referral_id)
    return redirect(request.referrer or "/referrals")


@app.route("/leads/tasks/complete/<int:task_id>", methods=["POST"])
@login_required
@role_required(ROLE_AGENT)
def complete_lead_task(task_id):
    validate_csrf()
    db = get_db()
    task = db.execute("""
        SELECT lead_tasks.id, lead_tasks.lead_id, leads.organisation_id
        FROM lead_tasks
        JOIN leads ON leads.id = lead_tasks.lead_id
        WHERE lead_tasks.id = ?
    """, (task_id,)).fetchone()

    if task is None:
        abort(404, "Task not found")
    if session.get("role") == ROLE_AGENT and task["organisation_id"] != current_organisation_id():
        abort(404, "Task not found")

    db.execute(
        "UPDATE lead_tasks SET completed_at = ? WHERE id = ?",
        (datetime.now().isoformat(), task_id)
    )
    db.commit()
    write_audit_log("completed_task", "lead_task", task_id)
    return redirect(request.referrer or "/tasks")


@app.post("/leads/export.csv")
@login_required
@role_required(ROLE_AGENT)
def export_leads_csv():
    if not ENABLE_CSV_EXPORTS:
        abort(404)
    validate_csrf()
    apply_rate_limit(f"{client_ip()}:{session.get('user_id')}:export_csv")
    db = get_db()
    where_sql, params = scoped_lead_where()
    rows = db.execute(f"""
        SELECT id, name, email, phone, address, valuation, source, utm_source,
               utm_medium, utm_campaign, selling_timeframe, status, lead_stage, lead_score,
               marketing_consent, referral_consent_accepted_at, referral_fee_disclosure_accepted_at,
               created_at, next_follow_up_at, retention_until,
               (
                   SELECT GROUP_CONCAT(service_type || ':' || status, '; ')
                   FROM service_referrals
                   WHERE service_referrals.lead_id = leads.id
               ) AS referrals
        FROM leads
        {where_sql}
        ORDER BY id DESC
        LIMIT ?
    """, tuple([*params, CSV_EXPORT_MAX_ROWS])).fetchall()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "id", "name", "email", "phone", "address", "valuation", "source",
        "utm_source", "utm_medium", "utm_campaign", "selling_timeframe", "status", "lead_stage",
        "lead_score", "marketing_consent", "referral_consent_accepted_at",
        "referral_fee_disclosure_accepted_at", "created_at", "next_follow_up_at",
        "retention_until", "referrals",
    ])
    for row in rows:
        writer.writerow([row[key] for key in row.keys()])

    response = make_response(output.getvalue())
    response.headers["Content-Type"] = "text/csv"
    response.headers["Content-Disposition"] = "attachment; filename=leads.csv"
    response.headers["Cache-Control"] = "no-store"
    write_audit_log("exported_csv", "lead")
    return response


@app.route("/settings/organisation", methods=["GET", "POST"])
@login_required
@role_required(ROLE_AGENT)
def organisation_settings():
    db = get_db()
    organisation_id = current_organisation_id() or default_organisation_id(db)
    if not organisation_id:
        abort(404, "Organisation not found")

    if request.method == "POST":
        validate_csrf()
        action = (request.form.get("action") or "").strip()

        if action == "update_org":
            name = validate_required_text(request.form.get("name"), "organisation name", max_length=150)
            subscription_plan = validate_required_text(
                request.form.get("subscription_plan"),
                "subscription plan",
                max_length=50,
            )
            billing_status = validate_required_text(
                request.form.get("billing_status"),
                "billing status",
                max_length=50,
            )
            lead_allowance = validate_int(
                request.form.get("lead_allowance"),
                "lead allowance",
                min_value=0,
                max_value=100000,
            )
            trial_ends_at = (request.form.get("trial_ends_at") or "").strip() or None
            db.execute("""
                UPDATE organisations
                SET name = ?,
                    subscription_plan = ?,
                    billing_status = ?,
                    lead_allowance = ?,
                    trial_ends_at = ?
                WHERE id = ?
            """, (
                name,
                subscription_plan,
                billing_status,
                lead_allowance,
                trial_ends_at,
                organisation_id,
            ))
            db.commit()
            write_audit_log("updated", "organisation", organisation_id)

        elif action == "add_territory":
            label = validate_required_text(request.form.get("label"), "territory label", max_length=100)
            postcode_prefix = validate_required_text(
                request.form.get("postcode_prefix"),
                "postcode prefix",
                max_length=10,
            ).upper().replace(" ", "")
            db.execute("""
                INSERT OR IGNORE INTO branch_territories (
                    organisation_id, label, postcode_prefix, created_at
                ) VALUES (?, ?, ?, ?)
            """, (
                organisation_id,
                label,
                postcode_prefix,
                datetime.now().isoformat(),
            ))
            db.commit()
            write_audit_log("added", "branch_territory", postcode_prefix)

        return redirect("/settings/organisation")

    organisation = db.execute(
        "SELECT * FROM organisations WHERE id = ?",
        (organisation_id,)
    ).fetchone()
    territories = db.execute("""
        SELECT *
        FROM branch_territories
        WHERE organisation_id = ?
        ORDER BY postcode_prefix ASC
    """, (organisation_id,)).fetchall()
    users = db.execute("""
        SELECT id, username, role
        FROM users
        WHERE organisation_id = ?
        ORDER BY username ASC
    """, (organisation_id,)).fetchall()
    lead_count = db.execute(
        "SELECT COUNT(*) FROM leads WHERE organisation_id = ?",
        (organisation_id,)
    ).fetchone()[0]

    return render_template(
        "organisation_settings.html",
        organisation=organisation,
        territories=territories,
        users=users,
        lead_count=lead_count,
        csrf_token=get_csrf_token(),
    )


@app.get("/admin")
@login_required
@admin_or_agent_required
def admin_dashboard():
    db = get_db()
    today = datetime.now().date().isoformat()
    lead_scope, lead_params = admin_scope_clause("leads")
    lead_where = f"WHERE {lead_scope}" if lead_scope else ""

    totals = {
        "leads": db.execute(
            f"SELECT COUNT(*) FROM leads {lead_where}",
            tuple(lead_params)
        ).fetchone()[0],
        "hot_leads": db.execute(
            f"SELECT COUNT(*) FROM leads {lead_where + (' AND ' if lead_where else 'WHERE ')}(is_hot_lead = 1 OR lead_score >= 60)",
            tuple(lead_params)
        ).fetchone()[0],
        "reports": db.execute(
            f"SELECT COUNT(*) FROM leads {lead_where + (' AND ' if lead_where else 'WHERE ')}report_token IS NOT NULL",
            tuple(lead_params)
        ).fetchone()[0],
    }

    referral_scope, referral_params = admin_scope_clause("leads")
    referral_where = f"WHERE {referral_scope}" if referral_scope else ""
    referral_rows = db.execute(f"""
        SELECT service_referrals.service_type,
               service_referrals.status,
               COUNT(*) AS total,
               COALESCE(SUM(service_referrals.fee_expected), 0) AS expected,
               COALESCE(SUM(service_referrals.fee_received), 0) AS received
        FROM service_referrals
        JOIN leads ON leads.id = service_referrals.lead_id
        {referral_where}
        GROUP BY service_referrals.service_type, service_referrals.status
        ORDER BY service_referrals.service_type, service_referrals.status
    """, tuple(referral_params)).fetchall()

    source_rows = db.execute(f"""
        SELECT source, COUNT(*) AS total
        FROM leads
        {lead_where}
        GROUP BY source
        ORDER BY total DESC
        LIMIT 10
    """, tuple(lead_params)).fetchall()

    task_scope, task_params = admin_scope_clause("leads")
    task_where = "lead_tasks.completed_at IS NULL AND date(lead_tasks.due_at) <= date(?)"
    task_query_params = [today]
    if task_scope:
        task_where += f" AND {task_scope}"
        task_query_params.extend(task_params)
    tasks_due = db.execute(f"""
        SELECT COUNT(*)
        FROM lead_tasks
        JOIN leads ON leads.id = lead_tasks.lead_id
        WHERE {task_where}
    """, tuple(task_query_params)).fetchone()[0]

    won_leads = db.execute(
        f"SELECT COUNT(*) FROM leads {lead_where + (' AND ' if lead_where else 'WHERE ')}status = ?",
        tuple([*lead_params, LEAD_STATUS_WON])
    ).fetchone()[0]
    appointment_leads = db.execute(
        f"SELECT COUNT(*) FROM leads {lead_where + (' AND ' if lead_where else 'WHERE ')}status IN (?, ?)",
        tuple([*lead_params, LEAD_STATUS_VALUATION_BOOKED, LEAD_STATUS_APPOINTMENT_BOOKED])
    ).fetchone()[0]
    referral_totals = db.execute(f"""
        SELECT COALESCE(SUM(service_referrals.fee_expected), 0) AS expected,
               COALESCE(SUM(service_referrals.fee_received), 0) AS received
        FROM service_referrals
        JOIN leads ON leads.id = service_referrals.lead_id
        {referral_where}
    """, tuple(referral_params)).fetchone()
    conversion_metrics = {
        "appointment_rate": round((appointment_leads / totals["leads"]) * 100, 1) if totals["leads"] else 0,
        "won_rate": round((won_leads / totals["leads"]) * 100, 1) if totals["leads"] else 0,
        "referral_expected": referral_totals["expected"],
        "referral_received": referral_totals["received"],
    }

    organisations = []
    if is_platform_admin():
        organisations = db.execute("""
            SELECT organisations.*,
                   COUNT(leads.id) AS lead_count
            FROM organisations
            LEFT JOIN leads ON leads.organisation_id = organisations.id
            GROUP BY organisations.id
            ORDER BY organisations.name ASC
        """).fetchall()

    return render_template(
        "admin_dashboard.html",
        totals=totals,
        referral_rows=referral_rows,
        source_rows=source_rows,
        tasks_due=tasks_due,
        conversion_metrics=conversion_metrics,
        organisations=organisations,
        service_labels=SERVICE_LABELS,
    )


@app.route("/settings/email", methods=["GET", "POST"])
@login_required
@admin_or_agent_required
def email_settings():
    message = ""
    if request.method == "POST":
        validate_csrf()
        test_to = validate_required_text(request.form.get("test_to"), "test email", max_length=255)
        try:
            sent = send_email(
                test_to,
                "Equiome email test",
                "This is a test email from your Equiome booking system."
            )
            message = "Test email sent." if sent else "SMTP is not configured yet."
        except Exception as error:
            message = f"Email failed: {error}"

    return render_template(
        "email_settings.html",
        smtp_configured=bool(SMTP_HOST and CUSTOMER_EMAIL_FROM),
        smtp_host=SMTP_HOST,
        customer_email_from=CUSTOMER_EMAIL_FROM,
        lead_notification_email=LEAD_NOTIFICATION_EMAIL,
        message=message,
        csrf_token=get_csrf_token(),
    )


@app.get("/admin/system")
@login_required
@admin_or_agent_required
def system_readiness():
    secret_key = os.environ.get("SECRET_KEY", "")
    checks = [
        ("SECRET_KEY set", bool(secret_key)),
        ("SECRET_KEY is not a short/default value", len(secret_key) >= 32),
        ("INTERNAL_API_TOKEN set", bool(INTERNAL_API_TOKEN)),
        ("SESSION_COOKIE_SECURE enabled", app.config["SESSION_COOKIE_SECURE"]),
        ("FRONTEND_ORIGIN restricted", FRONTEND_ORIGIN != "*"),
        ("CSV exports disabled by default", not ENABLE_CSV_EXPORTS),
        ("Privacy notice route available", True),
        ("Test tools disabled", not ENABLE_TEST_TOOLS),
        ("SMTP configured", bool(SMTP_HOST and CUSTOMER_EMAIL_FROM)),
        ("Using managed database", bool(os.environ.get("DATABASE_URL"))),
    ]
    return render_template("system_readiness.html", checks=checks)


@app.get("/billing")
@login_required
@admin_or_agent_required
def billing():
    db = get_db()
    organisation_id = current_organisation_id() or default_organisation_id(db)
    organisation = db.execute(
        "SELECT * FROM organisations WHERE id = ?",
        (organisation_id,)
    ).fetchone()
    lead_count = db.execute(
        "SELECT COUNT(*) FROM leads WHERE organisation_id = ?",
        (organisation_id,)
    ).fetchone()[0]
    lead_allowance = organisation["lead_allowance"] or 0
    lead_usage_percent = round((lead_count / lead_allowance) * 100, 1) if lead_allowance else 0
    leads_remaining = max(lead_allowance - lead_count, 0) if lead_allowance else 0
    referral_fees = db.execute("""
        SELECT COALESCE(SUM(fee_expected), 0) AS expected,
               COALESCE(SUM(fee_received), 0) AS received
        FROM service_referrals
        JOIN leads ON leads.id = service_referrals.lead_id
        WHERE leads.organisation_id = ?
    """, (organisation_id,)).fetchone()

    return render_template(
        "billing.html",
        organisation=organisation,
        lead_count=lead_count,
        lead_usage_percent=lead_usage_percent,
        leads_remaining=leads_remaining,
        referral_fees=referral_fees,
    )


# -----------------------
# EPC jobs
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
    if not token or not re.match(r"^[A-Za-z0-9_-]{32,128}$", token):
        abort(404)

    db = get_db()
    lead = db.execute("""
        SELECT id, report_filename, report_expires_at
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

    write_audit_log("downloaded_report", "lead", lead["id"])
    response = send_file(filepath, mimetype="application/pdf", as_attachment=True)
    response.headers["Cache-Control"] = "no-store"
    return response


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
        "monthly_payment_estimate": to_float(data.get("monthly_payment_estimate", 0)),
        "selling_timeframe": data.get("selling_timeframe") or "Not supplied",
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
    selling_timeframe = validate_selling_timeframe(data.get("selling_timeframe"))
    created_at = data.get("created_at") or datetime.now().isoformat()
    notes = (data.get("notes") or "").strip()
    requested_services = normalise_requested_services(data.get("help_requested") or data.get("selected_services"))
    marketing_consent = 1 if truthy(data.get("marketing_consent")) else 0
    privacy_notice_accepted = 1 if truthy(data.get("privacy_notice_accepted")) else 0
    referral_consent_accepted = truthy(
        data.get("referral_consent_accepted")
        or data.get("referral_consent")
        or data.get("third_party_referral_consent")
    )
    referral_fee_disclosure_accepted = truthy(
        data.get("referral_fee_disclosure_accepted")
        or data.get("referral_fee_disclosure")
        or data.get("referral_fee_notice_accepted")
    )
    privacy_notice_accepted_at = datetime.now().isoformat() if privacy_notice_accepted else None
    referral_consent_accepted_at = datetime.now().isoformat() if referral_consent_accepted else None
    referral_fee_disclosure_accepted_at = datetime.now().isoformat() if referral_fee_disclosure_accepted else None
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
    if requested_services and not referral_consent_accepted:
        return jsonify({
            "success": False,
            "error": "Referral consent is required before selected services can be passed to partners"
        }), 400

    report_filename = None
    report_token = None
    report_expires_at = None

    try:
        valuation = int(float(valuation))
        if create_report:
            report_filename, report_token, report_expires_at = create_lead_report(data)
        lead_score = calculate_lead_score(data, bool(marketing_consent))

        db = get_db()
        organisation_id = (
            current_organisation_id()
            if session.get("role") == ROLE_AGENT
            else organisation_for_address(db, address)
        )
        assigned_agent_id = session.get("user_id") if session.get("role") == ROLE_AGENT else None
        cursor = db.execute("""
            INSERT INTO leads (
                name, email, phone, address, valuation,
                source, status, created_at, notes,
                lead_stage, is_hot_lead, updated_at,
                assigned_agent_id, report_filename, report_token, report_expires_at,
                marketing_consent, privacy_notice_accepted_at,
                referral_consent_accepted_at, referral_fee_disclosure_accepted_at, retention_until,
                source_page, utm_source, utm_medium, utm_campaign, selling_timeframe, lead_score, next_follow_up_at,
                organisation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            referral_consent_accepted_at,
            referral_fee_disclosure_accepted_at,
            retention_until,
            source_page,
            utm_source,
            utm_medium,
            utm_campaign,
            selling_timeframe,
            lead_score,
            (datetime.now() + timedelta(days=1)).isoformat(),
            organisation_id,
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
        created_referrals = create_service_referrals(lead_id, requested_services)
        if created_referrals:
            add_lead_note(
                lead_id,
                "Requested services: " + ", ".join(SERVICE_LABELS.get(service, service) for service in created_referrals),
                user_id=assigned_agent_id,
            )
            add_lead_note(
                lead_id,
                "Referral sharing consent captured."
                + (" Referral fee disclosure accepted." if referral_fee_disclosure_accepted else ""),
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
        notify_new_lead(lead_id, name, email, phone, address, lead_score, selling_timeframe)
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
