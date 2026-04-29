import json
import os
import re
from datetime import datetime
from urllib.parse import urlparse

import requests

from property_tool import calculate_property_decision, get_real_valuation, to_float


CHATBOT_PHASE_OPEN = "open"
CHATBOT_PHASE_DISCOVER = "discover"
CHATBOT_PHASE_CALCULATE = "calculate"
CHATBOT_PHASE_OFFER = "offer"
CHATBOT_PHASE_HANDOFF = "handoff"
CHATBOT_PHASE_DONE = "done"

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

CHATBOT_LLM_ENABLED = os.environ.get("CHATBOT_LLM_ENABLED", "0") == "1"
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
CHATBOT_LLM_MODEL = os.environ.get("CHATBOT_LLM_MODEL", "gpt-4o-mini")

NUMBER_WORDS = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fourty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
}


def normalise_requested_services(services):
    normalised = []
    for service in services or []:
        service_type = SERVICE_ALIASES.get(str(service).strip().lower())
        if service_type and service_type not in normalised:
            normalised.append(service_type)
    return normalised


def parse_number_words(text):
    total = 0
    current = 0
    found = False
    for word in re.sub(r"[^a-z\s-]", " ", (text or "").lower()).replace("-", " ").split():
        if word in NUMBER_WORDS:
            current += NUMBER_WORDS[word]
            found = True
        elif word == "hundred" and current:
            current *= 100
            found = True
        elif word in {"thousand", "grand"} and current:
            total += current * 1000
            current = 0
            found = True
        elif word in {"million", "millions"} and current:
            total += current * 1000000
            current = 0
            found = True
    return total + current if found else None


def parse_money(value, default=0):
    text = (value or "").strip().lower().replace(",", "")
    if text in {"", "skip", "none", "no", "nil", "zero", "owned outright", "outright"}:
        return default

    match = re.search(r"(\d+(?:\.\d+)?)\s*(k|m|thousand|grand|million|millions)?\b", text)
    if match:
        amount = float(match.group(1))
        suffix = match.group(2) or ""
        if suffix in {"k", "thousand", "grand"}:
            amount *= 1000
        elif suffix in {"m", "million", "millions"}:
            amount *= 1000000
        return amount

    words_amount = parse_number_words(text)
    if words_amount is not None:
        return float(words_amount)
    return default


def chatbot_money(value):
    return f"£{round(to_float(value, 0)):,.0f}"


def chatbot_yes(value):
    text = re.sub(r"[^a-z\s]", " ", (value or "").strip().lower())
    tokens = set(text.split())
    yes_tokens = {"yes", "y", "yeah", "yep", "ok", "okay", "sure", "please", "agree", "accepted", "fine"}
    return bool(tokens & yes_tokens) or "i agree" in text or "go ahead" in text


def chatbot_no(value):
    text = re.sub(r"[^a-z\s]", " ", (value or "").strip().lower())
    tokens = set(text.split())
    no_tokens = {"no", "n", "nope", "nah", "decline", "declined", "skip", "none"}
    return bool(tokens & no_tokens) or "no thanks" in text or "not now" in text


def chatbot_property_type(value):
    text = (value or "").strip().lower().replace("_", " ").replace("-", " ")
    if "flat" in text or "apartment" in text:
        return "flat"
    if "terrace" in text:
        return "terraced"
    if "semi" in text:
        return "semi-detached"
    if "detached" in text:
        return "detached"
    return ""


def chatbot_timeframe(value):
    text = (value or "").strip().lower()
    if any(phrase in text for phrase in ["just exploring", "just curious", "only curious", "not sure", "no rush", "browsing"]):
        return "Just exploring"
    if re.search(r"\b(0\s*-\s*3|0\s*to\s*3|under\s*3|less than\s*3|within\s*3|within\s*three|next few|asap|soon|immediately)\b", text):
        return "0-3 months"
    if re.search(r"\b(3\s*-\s*6|3\s*to\s*6|three\s*to\s*six|around\s*3|in\s*3|around\s*three|in\s*three|within\s*6|within\s*six|next\s*6)\b", text):
        return "3-6 months"
    if re.search(r"\b(6\s*-\s*9|6\s*to\s*9|six\s*to\s*nine|around\s*6|in\s*6|around\s*six|within\s*9|within\s*nine|next\s*9)\b", text):
        return "6-9 months"
    if re.search(r"\b(9\s*-\s*12|9\s*to\s*12|nine\s*to\s*twelve|around\s*9|in\s*9|around\s*nine|in\s*12|within\s*12|within\s*twelve|year|next year)\b", text):
        return "9-12 months"
    return ""


def chatbot_plan(value):
    text = (value or "").strip().lower()
    if "rent" in text:
        return "rent"
    if "buy" in text or "buying" in text or "purchase" in text or "next property" in text or "new home" in text:
        return "buy"
    if "explor" in text or "not sure" in text or "unsure" in text:
        return "exploring"
    return ""


def chatbot_motivation_category(value):
    text = (value or "").strip().lower()
    if any(word in text for word in ["upsize", "bigger", "baby", "family", "bedroom"]):
        return "upsizing"
    if any(word in text for word in ["downsize", "smaller", "bungalow", "retire"]):
        return "downsizing"
    if any(word in text for word in ["relocat", "work", "job", "move area"]):
        return "relocating"
    if any(word in text for word in ["equity", "release money", "cash"]):
        return "equity_release"
    if any(word in text for word in ["divorce", "separat", "split"]):
        return "separation"
    if any(word in text for word in ["financial", "struggling", "payment", "afford"]):
        return "financial"
    if any(word in text for word in ["curious", "explor", "nosy", "browsing"]):
        return "exploring"
    if chatbot_plan(text) == "buy":
        return "buying_next"
    return ""


def chatbot_preferred_contact(value):
    text = (value or "").strip().lower()
    if "email" in text:
        return "email"
    if "phone" in text or "call" in text:
        return "phone"
    if "text" in text or "sms" in text:
        return "text"
    return ""


def chatbot_detect_objection(value):
    text = (value or "").strip().lower()
    if any(word in text for word in ["low", "high", "wrong", "expected", "estimate", "valuation", "worth more", "worth less"]):
        return value.strip()
    return ""


def chatbot_infer_fields(data, message):
    plan = chatbot_plan(message)
    if plan and not data.get("plan"):
        data["plan"] = plan

    timeframe = chatbot_timeframe(message)
    if timeframe and not data.get("selling_timeframe"):
        data["selling_timeframe"] = timeframe

    property_type = chatbot_property_type(message)
    if property_type and not data.get("property_type"):
        data["property_type"] = property_type

    motivation_category = chatbot_motivation_category(message)
    if motivation_category and not data.get("motivation_category"):
        data["motivation_category"] = motivation_category

    preferred_contact = chatbot_preferred_contact(message)
    if preferred_contact and not data.get("preferred_contact"):
        data["preferred_contact"] = preferred_contact

    if data.get("calculation"):
        objection = chatbot_detect_objection(message)
        if objection:
            objections = data.setdefault("objections_raised", [])
            if objection not in objections:
                objections.append(objection)

    if any(word in message.strip().lower() for word in ["divorce", "separat", "bereav", "passed away", "financial pressure"]):
        data["sensitive_context"] = True


def chatbot_services(value):
    text = (value or "").strip().lower()
    if text in {"no", "none", "nothing", "skip", "no thanks"}:
        return []
    services = []
    if "valuation" in text or "agent" in text:
        services.append(SERVICE_VALUATION)
    if "mortgage" in text or "broker" in text:
        services.append(SERVICE_MORTGAGE)
    if "solicitor" in text or "convey" in text or "legal" in text:
        services.append(SERVICE_SOLICITOR)
    if "epc" in text or "energy" in text:
        services.append(SERVICE_EPC)
    if "all" in text:
        services = [SERVICE_VALUATION, SERVICE_MORTGAGE, SERVICE_SOLICITOR, SERVICE_EPC]
    return normalise_requested_services(services)


def chatbot_special_response(message):
    text = (message or "").strip().lower()
    if any(phrase in text for phrase in ["are you real", "real person", "human", "am i speaking to"]):
        return "I’m Aria, Equiome’s automated property assistant. I can collect the details for your report, and a real person can follow up if you ask for one."
    if any(phrase in text for phrase in ["what rate", "mortgage rate", "interest rate", "can i borrow", "will i get a mortgage", "specific mortgage"]):
        return "I can give a broad affordability estimate, but I can’t give regulated mortgage advice. A qualified mortgage adviser would need to look at your circumstances properly."
    return ""


def chatbot_summary(data):
    motivation = data.get("motivation") or "No motivation supplied"
    motivation_category = data.get("motivation_category") or "uncategorised"
    timeframe = data.get("selling_timeframe") or "No timeframe supplied"
    plan = data.get("plan") or "No next-step plan supplied"
    services = data.get("help_requested") or []
    service_text = ", ".join(SERVICE_LABELS.get(service, service) for service in services) or "no services requested"
    objections = "; ".join(data.get("objections_raised") or []) or "none captured"
    return (
        f"Chatbot lead. Motivation: {motivation}. Category: {motivation_category}. "
        f"Plan: {plan}. Timeframe: {timeframe}. Requested services: {service_text}. "
        f"Condition notes: {data.get('condition') or 'not supplied'}. Objections: {objections}."
    )


def chatbot_add_message(db, session_token, role, message):
    db.execute("""
        INSERT INTO chatbot_messages (session_token, role, message, created_at)
        VALUES (?, ?, ?, ?)
    """, (session_token, role, message, datetime.now().isoformat()))


def chatbot_update(db, session_token, data, phase, status="active", lead_id=None, ai_summary=None):
    db.execute("""
        UPDATE chatbot_conversations
        SET captured_data = ?,
            phase = ?,
            status = ?,
            lead_id = COALESCE(?, lead_id),
            ai_summary = COALESCE(?, ai_summary),
            updated_at = ?
        WHERE session_token = ?
    """, (
        json.dumps(data),
        phase,
        status,
        lead_id,
        ai_summary,
        datetime.now().isoformat(),
        session_token,
    ))


def chatbot_host_from_url(value):
    try:
        parsed = urlparse(value or "")
        return (parsed.hostname or "").lower().strip(".")
    except Exception:
        return ""


def chatbot_domain_allowed(allowed_domain, source_page):
    allowed_domain = (allowed_domain or "").lower().strip(".")
    if not allowed_domain:
        return True
    source_host = chatbot_host_from_url(source_page)
    return source_host == allowed_domain or source_host.endswith(f".{allowed_domain}")


def get_chatbot_organisation(db, organisation_id):
    try:
        organisation_id = int(organisation_id)
    except (TypeError, ValueError):
        return None
    return db.execute(
        "SELECT id, chatbot_enabled, chatbot_allowed_domain FROM organisations WHERE id = ?",
        (organisation_id,)
    ).fetchone()


def chatbot_polish_reply(data, phase, draft, logger=None):
    if not CHATBOT_LLM_ENABLED or not OPENAI_API_KEY:
        return draft
    if "£" in draft or phase in {CHATBOT_PHASE_CALCULATE, CHATBOT_PHASE_DONE}:
        return draft
    phase_tone = {
        CHATBOT_PHASE_OPEN: "Clear and calm. Help the homeowner know what to answer next.",
        CHATBOT_PHASE_DISCOVER: "Unhurried and gently curious. Do not sound pushy.",
        CHATBOT_PHASE_OFFER: "Light, optional, and commercially neutral. Never pressure the user.",
        CHATBOT_PHASE_HANDOFF: "Warm and concise.",
    }.get(phase, "Warm and concise.")
    try:
        response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": CHATBOT_LLM_MODEL,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Rewrite the assistant message for a UK property chatbot called Aria. "
                            "Keep the same meaning and ask for the same information. "
                            "Use warm, concise British English and a natural estate-agency register. "
                            f"Tone for this phase: {phase_tone} "
                            "Do not add facts, figures, advice, promises, or extra questions. "
                            "Do not use regulated mortgage advice language. If the context is sensitive, be calm and plain. "
                            "Avoid phrases such as absolutely, certainly, I understand, no worries, guarantee, definitely, "
                            "legal advice, financial advice, and I am a real person. "
                            "Do not use exclamation marks. Return only the rewritten message."
                        ),
                    },
                    {
                        "role": "user",
                        "content": f"Captured data: {json.dumps(data, ensure_ascii=True)[:1500]}\nMessage: {draft}",
                    },
                ],
                "temperature": 0.4,
                "max_tokens": 120,
            },
            timeout=8,
        )
        response.raise_for_status()
        polished = response.json()["choices"][0]["message"]["content"].strip()
        if not polished or "£" in polished or len(polished) > 500:
            return draft
        forbidden = [
            "guarantee",
            "definitely",
            "financial advice",
            "legal advice",
            "i am a real person",
            "absolutely",
            "certainly",
            "i understand",
            "no worries",
        ]
        if any(term in polished.lower() for term in forbidden):
            return draft
        return polished
    except Exception:
        if logger:
            logger.exception("Chatbot LLM rewrite failed")
        return draft


def chatbot_reply(data, phase, draft, logger=None):
    return chatbot_polish_reply(data, phase, draft, logger=logger)


def chatbot_question(data):
    if not data.get("address"):
        data["awaiting"] = "address"
        return CHATBOT_PHASE_OPEN, "What’s the address of the property?"
    if not data.get("property_type"):
        data["awaiting"] = "property_type"
        return CHATBOT_PHASE_OPEN, "Got it. Is that a flat, terraced, semi-detached, or detached property?"
    if not data.get("bedrooms"):
        data["awaiting"] = "bedrooms"
        return CHATBOT_PHASE_DISCOVER, "How many bedrooms does it have?"
    if not data.get("condition"):
        data["awaiting"] = "condition"
        return CHATBOT_PHASE_DISCOVER, "Would you say it’s recently updated, in good order, or could do with some work?"
    if not data.get("motivation"):
        data["awaiting"] = "motivation"
        return CHATBOT_PHASE_DISCOVER, "What’s prompting you to think about moving?"
    if not data.get("selling_timeframe"):
        data["awaiting"] = "selling_timeframe"
        return CHATBOT_PHASE_DISCOVER, "How soon are you hoping to move? For example 0-3 months, 3-6 months, 6-9 months, 9-12 months, or just exploring."
    if data.get("mortgage") is None:
        data["awaiting"] = "mortgage"
        return CHATBOT_PHASE_DISCOVER, "Roughly how much is left on the mortgage? If it’s owned outright, just say none."
    if not data.get("plan"):
        data["awaiting"] = "plan"
        return CHATBOT_PHASE_DISCOVER, "Are you thinking of buying somewhere, renting next, or just exploring?"
    if data.get("plan") in {"buy", "exploring"} and data.get("income") is None:
        data["awaiting"] = "income"
        return CHATBOT_PHASE_DISCOVER, "Roughly what’s the household annual income? A broad figure is fine."
    if data.get("plan") == "buy" and data.get("target_price") is None:
        data["awaiting"] = "target_price"
        return CHATBOT_PHASE_DISCOVER, "Do you have a target price in mind for the next property? You can say skip."
    data["awaiting"] = ""
    return CHATBOT_PHASE_CALCULATE, ""


def chatbot_apply_answer(data, message):
    field = data.get("awaiting")
    chatbot_infer_fields(data, message)
    if field == "address":
        data["address"] = message.strip()
        chatbot_infer_fields(data, message)
    elif field == "property_type":
        parsed = chatbot_property_type(message)
        if not parsed:
            return "Please choose flat, terraced, semi-detached, or detached."
        data["property_type"] = parsed
    elif field == "bedrooms":
        match = re.search(r"\d+", message)
        if not match:
            return "Please enter the number of bedrooms."
        data["bedrooms"] = int(match.group(0))
    elif field == "condition":
        data["condition"] = message.strip()
    elif field == "motivation":
        data["motivation"] = message.strip()
        if not data.get("motivation_category"):
            data["motivation_category"] = chatbot_motivation_category(message)
    elif field == "selling_timeframe":
        parsed = chatbot_timeframe(message)
        if not parsed:
            return "Please choose 0-3 months, 3-6 months, 6-9 months, 9-12 months, or just exploring."
        data["selling_timeframe"] = parsed
    elif field == "mortgage":
        if message.strip().lower() in {"none", "no", "owned outright", "outright", "0"}:
            data["mortgage"] = 0
        else:
            data["mortgage"] = parse_money(message, 0)
    elif field == "plan":
        parsed = chatbot_plan(message)
        if not parsed:
            return "Please say buying, renting, or just exploring."
        data["plan"] = parsed
    elif field == "income":
        data["income"] = parse_money(message, 0)
    elif field == "target_price":
        if message.strip().lower() in {"skip", "no", "none", "not sure"}:
            data["target_price"] = 0
        else:
            data["target_price"] = parse_money(message, 0)
    elif field in {"service_valuation", "service_mortgage", "service_solicitor", "service_epc"}:
        if "all" in message.strip().lower():
            data["help_requested"] = [SERVICE_VALUATION, SERVICE_MORTGAGE, SERVICE_SOLICITOR, SERVICE_EPC]
            data["skip_remaining_services"] = True
        elif chatbot_no(message) and field == "service_valuation" and any(term in message.strip().lower() for term in ["none", "no thanks", "nothing"]):
            data["help_requested"] = []
            data["skip_remaining_services"] = True
        elif chatbot_yes(message):
            service = field.replace("service_", "")
            requested = data.setdefault("help_requested", [])
            if service not in requested:
                requested.append(service)
        elif not chatbot_no(message):
            parsed_services = chatbot_services(message)
            if parsed_services:
                requested = data.setdefault("help_requested", [])
                for service in parsed_services:
                    if service not in requested:
                        requested.append(service)
            else:
                return "Please reply yes or no, or say all or none."
    elif field == "full_name":
        if chatbot_no(message):
            data["declined_contact_details"] = True
            data["awaiting"] = ""
            return ""
        data["full_name"] = message.strip()
    elif field == "email":
        if chatbot_no(message):
            data["declined_contact_details"] = True
            data["awaiting"] = ""
            return ""
        data["email"] = message.strip().lower()
    elif field == "phone":
        if chatbot_no(message):
            data["declined_contact_details"] = True
            data["awaiting"] = ""
            return ""
        data["phone"] = message.strip()
        preferred_contact = chatbot_preferred_contact(message)
        if preferred_contact:
            data["preferred_contact"] = preferred_contact
    elif field == "privacy":
        data["privacy_notice_accepted"] = chatbot_yes(message)
        if not data["privacy_notice_accepted"]:
            return "I need privacy notice acceptance before I can save and send the report. Please reply yes if you’re happy to proceed."
    elif field == "referral_consent":
        data["referral_consent_accepted"] = chatbot_yes(message)
        data["referral_fee_disclosure_accepted"] = chatbot_yes(message)
    elif field == "marketing":
        data["marketing_consent"] = chatbot_yes(message)
    return ""


def chatbot_calculate(data):
    valuation = get_real_valuation(data["address"], data["property_type"])
    calculation = calculate_property_decision({
        "valuation": valuation,
        "mortgage": data.get("mortgage", 0),
        "plan": data.get("plan", ""),
        "target_price": data.get("target_price", 0),
        "income": data.get("income", 0),
        "partner_income": 0,
    })
    data["valuation"] = valuation
    data["calculation"] = calculation
    data["awaiting"] = "service_valuation"
    data.setdefault("help_requested", [])
    return (
        f"Based on what you’ve told me, the property is likely worth between "
        f"{chatbot_money(valuation.get('low'))} and {chatbot_money(valuation.get('high'))}. "
        f"After the mortgage and typical selling costs, the estimated available equity is "
        f"{chatbot_money(calculation.get('net_proceeds'))}. "
        f"The estimated next budget is {chatbot_money(calculation.get('max_budget'))}. "
        "Would you like a local agent to help confirm the property value?"
    )


def chatbot_prepare_lead_payload(data, transcript):
    valuation = data.get("valuation") or {}
    calculation = data.get("calculation") or {}
    services = data.get("help_requested") or []
    summary = chatbot_summary(data)
    notes = summary + "\n\nTranscript:\n" + transcript
    return {
        "full_name": data.get("full_name"),
        "email": data.get("email"),
        "phone": data.get("phone"),
        "help_requested": services,
        "address": data.get("address"),
        "property_type": data.get("property_type"),
        "selling_timeframe": data.get("selling_timeframe"),
        "valuation_low": valuation.get("low", 0),
        "valuation_high": valuation.get("high", valuation.get("estimated_value", 0)),
        "moving_costs": (calculation.get("moving_costs") or {}).get("total", 0),
        "net_proceeds": calculation.get("net_proceeds", 0),
        "borrowing_power": calculation.get("borrowing_power", 0),
        "max_budget": calculation.get("max_budget", 0),
        "monthly_payment_estimate": calculation.get("monthly_payment_estimate", 0),
        "recommendation": calculation.get("recommendation", ""),
        "referral_consent_accepted": bool(data.get("referral_consent_accepted")),
        "referral_fee_disclosure_accepted": bool(data.get("referral_fee_disclosure_accepted")),
        "privacy_notice_accepted": bool(data.get("privacy_notice_accepted")),
        "marketing_consent": bool(data.get("marketing_consent")),
        "source": "chatbot",
        "source_page": data.get("source_page", ""),
        "notes": notes,
    }, summary


def chatbot_continue(data):
    if data.get("declined_contact_details"):
        data["awaiting"] = ""
        return CHATBOT_PHASE_DONE, "No problem. I won’t save your contact details or create a lead. You can start again whenever you want a full report or follow-up."
    if data.get("calculation") and data.get("skip_remaining_services"):
        data["awaiting"] = "full_name"
        return CHATBOT_PHASE_OFFER, "Thanks. To send your report and arrange any follow-up, what’s your full name?"
    if data.get("calculation") and data.get("awaiting") == "service_valuation":
        data["awaiting"] = "service_mortgage"
        return CHATBOT_PHASE_OFFER, "Would it help to have a mortgage adviser call you about your next budget?"
    if data.get("awaiting") == "service_mortgage":
        data["awaiting"] = "service_solicitor"
        return CHATBOT_PHASE_OFFER, "Would you like a conveyancing or solicitor quote as well?"
    if data.get("awaiting") == "service_solicitor":
        data["awaiting"] = "service_epc"
        return CHATBOT_PHASE_OFFER, "Do you need help arranging an EPC?"
    if data.get("awaiting") == "service_epc":
        data["awaiting"] = "full_name"
        return CHATBOT_PHASE_OFFER, "Thanks. To send your report and arrange any follow-up, what’s your full name? You can say no thanks if you’d rather stop here."
    if data.get("awaiting") == "full_name" and data.get("full_name"):
        data["awaiting"] = "email"
        return CHATBOT_PHASE_OFFER, "And what email should I send the report to?"
    if data.get("awaiting") == "email" and data.get("email"):
        data["awaiting"] = "phone"
        return CHATBOT_PHASE_OFFER, "What phone number should the agent or partner use if follow-up is needed?"
    if data.get("awaiting") == "phone" and data.get("phone"):
        data["awaiting"] = "privacy"
        return CHATBOT_PHASE_OFFER, "Please confirm you’re happy with the privacy notice and for us to save your details to generate the report. Reply yes to continue."
    if data.get("awaiting") == "privacy" and data.get("privacy_notice_accepted"):
        if data.get("help_requested"):
            data["awaiting"] = "referral_consent"
            return CHATBOT_PHASE_OFFER, "Because you selected partner services, are you happy for us to share your details with trusted property professionals? Some partners may pay us a referral fee, which does not change what you pay."
        data["awaiting"] = "marketing"
        return CHATBOT_PHASE_OFFER, "Would you like occasional property updates from the agent? This is optional."
    if data.get("awaiting") == "referral_consent":
        if not data.get("referral_consent_accepted"):
            data["help_requested"] = []
            data["referral_fee_disclosure_accepted"] = False
        data["awaiting"] = "marketing"
        return CHATBOT_PHASE_OFFER, "Would you like occasional property updates from the agent? This is optional."
    if data.get("awaiting") == "marketing" and "marketing_consent" in data:
        data["awaiting"] = ""
        return CHATBOT_PHASE_HANDOFF, ""
    return chatbot_question(data)
