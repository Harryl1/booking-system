import secrets

from flask import Blueprint, abort, redirect, render_template, request, session
from werkzeug.security import check_password_hash, generate_password_hash


def create_auth_blueprint(deps):
    auth = Blueprint("auth", __name__)
    get_db = deps["get_db"]
    login_required = deps["login_required"]
    apply_rate_limit = deps["apply_rate_limit"]
    client_ip = deps["client_ip"]
    validate_csrf = deps["validate_csrf"]
    validate_required_text = deps["validate_required_text"]
    validate_role = deps["validate_role"]
    validate_admin_username = deps["validate_admin_username"]
    default_organisation_id = deps["default_organisation_id"]
    is_integrity_error = deps["is_integrity_error"]
    get_csrf_token = deps["get_csrf_token"]
    role_platform_admin = deps["role_platform_admin"]
    role_assessor = deps["role_assessor"]
    admin_setup_token = deps["admin_setup_token"]
    lead_retention_days = deps["lead_retention_days"]
    report_retention_days = deps["report_retention_days"]

    @auth.route("/register", methods=["GET", "POST"])
    def register():
        db = get_db()
        platform_admin_count = db.execute(
            "SELECT COUNT(*) FROM users WHERE role = ?",
            (role_platform_admin,)
        ).fetchone()[0]
        setup_mode = platform_admin_count == 0

        if setup_mode:
            if not admin_setup_token:
                abort(503, "Admin setup token is not configured")
            supplied_setup_token = request.values.get("setup_token", "").strip()
            if not secrets.compare_digest(supplied_setup_token, admin_setup_token):
                abort(403, "Admin setup token is required")
        else:
            if "user_id" not in session:
                abort(403, "User registration is restricted")
            if session.get("role") != role_platform_admin:
                abort(403, "Only Equiome admins can create users")

        if request.method == "POST":
            validate_csrf()

            username = validate_required_text(
                request.form.get("username"),
                "username",
                max_length=100
            )
            password = request.form.get("password", "")
            role = role_platform_admin if setup_mode else validate_role(request.form.get("role"))

            if len(password) < 12:
                abort(400, "Password must be at least 12 characters")
            if role == role_platform_admin:
                validate_admin_username(username)

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
            csrf_token=get_csrf_token(),
            setup_mode=setup_mode,
            setup_token=request.values.get("setup_token", ""),
            allow_platform_admin=session.get("role") == role_platform_admin,
        )

    @auth.route("/login", methods=["GET", "POST"])
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
                return redirect("/epc" if user["role"] == role_assessor else "/")

            return "Invalid login", 401

        return render_template(
            "login.html",
            csrf_token=get_csrf_token()
        )

    @auth.route("/logout")
    @login_required
    def logout():
        session.clear()
        return redirect("/login")

    @auth.get("/privacy")
    def privacy_notice():
        return render_template(
            "privacy.html",
            retention_days=lead_retention_days,
            report_retention_days=report_retention_days,
        )

    @auth.get("/agent-terms")
    def agent_terms():
        return render_template("agent_terms.html")

    return auth
