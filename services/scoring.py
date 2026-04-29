from property_tool import to_float


SERVICE_VALUATION = "valuation"
SERVICE_EPC = "epc"
SERVICE_SOLICITOR = "solicitor"
SERVICE_MORTGAGE = "mortgage"
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


def calculate_lead_score(data, marketing_consent=False):
    score = 10
    if marketing_consent:
        score += 10
    if data.get("phone"):
        score += 10
    requested_services = normalise_requested_services(data.get("help_requested") or data.get("selected_services"))
    if SERVICE_VALUATION in requested_services:
        score += 20
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


def calculate_referral_score(
    data,
    marketing_consent=False,
    referral_consent=False,
    referral_fee_disclosure=False,
):
    score = 0
    requested_services = normalise_requested_services(data.get("help_requested") or data.get("selected_services"))
    service_scores = {
        SERVICE_MORTGAGE: 30,
        SERVICE_SOLICITOR: 25,
        SERVICE_EPC: 15,
        SERVICE_VALUATION: 10,
    }
    for service in requested_services:
        score += service_scores.get(service, 0)
    if referral_consent:
        score += 15
    if referral_fee_disclosure:
        score += 10
    if marketing_consent:
        score += 5
    if to_float(data.get("net_proceeds", 0)) > 50000:
        score += 10
    if to_float(data.get("max_budget", 0)) > 300000:
        score += 10
    if data.get("plan") == "buy":
        score += 10
    selling_timeframe = (data.get("selling_timeframe") or "").strip().lower()
    timeframe_scores = {
        "0-3 months": 15,
        "3-6 months": 10,
        "6-9 months": 5,
        "9-12 months": 3,
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
    if any(referral["service_type"] == SERVICE_VALUATION for referral in referrals):
        factors.append("Local valuation requested")
    if lead["selling_timeframe"]:
        factors.append(f"Selling timeframe: {lead['selling_timeframe']}")
    if to_float(lead["valuation"], 0) >= 250000:
        factors.append("Material property value")
    if (lead["lead_score"] or 0) >= 60:
        factors.append("High-priority lead score")
    if not factors:
        factors.append("Basic contact details captured")
    return factors


def referral_score_factors(lead, referrals=None):
    referrals = referrals or []
    factors = []
    service_names = [
        SERVICE_LABELS.get(referral["service_type"], referral["service_type"])
        for referral in referrals
    ]
    if service_names:
        factors.append("Requested services: " + ", ".join(service_names))
    if lead["referral_consent_accepted_at"]:
        factors.append("Referral sharing consent captured")
    if lead["referral_fee_disclosure_accepted_at"]:
        factors.append("Referral fee disclosure accepted")
    if any(referral["service_type"] == SERVICE_MORTGAGE for referral in referrals):
        factors.append("Mortgage referral opportunity")
    if any(referral["service_type"] == SERVICE_SOLICITOR for referral in referrals):
        factors.append("Conveyancing or solicitor referral opportunity")
    if any(referral["service_type"] == SERVICE_EPC for referral in referrals):
        factors.append("EPC referral opportunity")
    if lead["selling_timeframe"] in {"0-3 months", "3-6 months"}:
        factors.append(f"Near-term moving timeframe: {lead['selling_timeframe']}")
    if not factors:
        factors.append("No referral opportunity captured yet")
    return factors


def best_next_action(lead, tasks=None, referrals=None):
    tasks = tasks or []
    referrals = referrals or []
    open_tasks = [task for task in tasks if not task["completed_at"]]
    if open_tasks:
        return f"Complete task: {open_tasks[0]['title']}"
    if lead["status"] == "new" and lead["selling_timeframe"] == "0-3 months":
        return "Call immediately: seller says they may move within 0-3 months"
    if lead["status"] == "new":
        return "Call the lead and qualify their moving timeline"
    if lead["status"] == "contacted":
        return "Book a valuation appointment or mark the next follow-up"
    if lead["status"] in {"valuation booked", "appointment booked"}:
        return "Prepare valuation notes and confirm referral opportunities"
    if referrals:
        return "Review open service referrals and update fee status"
    if lead["status"] == "won":
        return "Record revenue and keep referral follow-up current"
    if lead["status"] == "lost":
        return "Add loss reason and close remaining tasks"
    return "Add a note with the latest outcome"


def contact_priority_label(lead):
    score = lead["lead_score"] or 0
    if score >= 70:
        return "Hot"
    if score >= 45:
        return "Warm"
    return "New"
