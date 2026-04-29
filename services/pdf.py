import os
import secrets
import uuid
from datetime import datetime, timedelta

from pdf_report import generate_pdf_report
from property_tool import to_float


def create_lead_report(data, reports_dir, static_dir, report_retention_days):
    report_token = secrets.token_urlsafe(32)
    filename = f"report_{uuid.uuid4().hex}.pdf"
    filepath = os.path.join(reports_dir, filename)

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

    logo_path = os.path.join(static_dir, "logo.png")
    if not os.path.exists(logo_path):
        logo_path = None

    generate_pdf_report(pdf_data, filepath, logo_path=logo_path)
    report_expires_at = (datetime.now() + timedelta(days=report_retention_days)).isoformat()
    return filename, report_token, report_expires_at
