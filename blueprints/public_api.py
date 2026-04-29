import json
import secrets
from datetime import datetime, timedelta

import requests
from flask import Blueprint, current_app, jsonify, request

from property_tool import calculate_property_decision, get_real_valuation
from services.chatbot import (
    CHATBOT_PHASE_CALCULATE,
    CHATBOT_PHASE_DONE,
    CHATBOT_PHASE_HANDOFF,
    CHATBOT_PHASE_OPEN,
    CHATBOT_PHASE_OFFER,
    chatbot_add_message,
    chatbot_apply_answer,
    chatbot_calculate,
    chatbot_continue,
    chatbot_domain_allowed,
    chatbot_prepare_lead_payload,
    chatbot_reply,
    chatbot_special_response,
    chatbot_update,
    get_chatbot_organisation,
)


def create_public_api_blueprint(deps):
    public_api = Blueprint("public_api", __name__)
    get_db = deps["get_db"]
    client_ip = deps["client_ip"]
    apply_rate_limit = deps["apply_rate_limit"]
    apply_daily_ip_limit = deps["apply_daily_ip_limit"]
    default_organisation_id = deps["default_organisation_id"]
    save_lead_payload = deps["save_lead_payload"]
    add_lead_note = deps["add_lead_note"]
    create_follow_up_task = deps["create_follow_up_task"]
    write_audit_log = deps["write_audit_log"]
    truthy = deps["truthy"]
    chatbot_max_turns = deps["chatbot_max_turns"]
    chatbot_daily_ip_limit = deps["chatbot_daily_ip_limit"]
    lead_daily_ip_limit = deps["lead_daily_ip_limit"]

    @public_api.post("/api/chatbot/start")
    def chatbot_start():
        ip_address = client_ip()
        apply_rate_limit(f"{ip_address}:chatbot_start", max_requests=20)
        apply_daily_ip_limit(f"{ip_address}:chatbot_start", chatbot_daily_ip_limit)
        payload = request.get_json(silent=True) or {}
        db = get_db()
        session_token = secrets.token_urlsafe(32)
        now = datetime.now().isoformat()
        organisation = get_chatbot_organisation(db, payload.get("organisation_id"))
        if organisation is None:
            organisation_id = default_organisation_id(db)
            organisation = get_chatbot_organisation(db, organisation_id)
        if organisation is None or not truthy(organisation["chatbot_enabled"]):
            return jsonify({"error": "Chatbot is not enabled."}), 403

        source_page = (payload.get("source_page") or "").strip()
        origin = request.headers.get("Origin", "")
        if not chatbot_domain_allowed(organisation["chatbot_allowed_domain"], origin or source_page):
            current_app.logger.warning(
                "Blocked chatbot start for organisation %s from source %s",
                organisation["id"],
                origin or source_page,
            )
            return jsonify({"error": "Chatbot is not enabled for this website."}), 403

        organisation_id = organisation["id"]
        data = {
            "source_page": source_page,
            "awaiting": "address",
        }
        db.execute("""
            INSERT INTO chatbot_conversations (
                session_token, organisation_id, phase, status, captured_data, source_page, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            session_token,
            organisation_id,
            CHATBOT_PHASE_OPEN,
            "active",
            json.dumps(data),
            data["source_page"],
            now,
            now,
        ))
        message = chatbot_reply(
            data,
            CHATBOT_PHASE_OPEN,
            "Hi, I’m Aria, Equiome’s property assistant. I’ll ask a few questions and then put together your personalised report. What’s the address of the property?",
            logger=current_app.logger,
        )
        chatbot_add_message(db, session_token, "assistant", message)
        db.commit()
        return jsonify({"session_token": session_token, "phase": CHATBOT_PHASE_OPEN, "message": message})

    @public_api.post("/api/chatbot/message")
    def chatbot_message():
        apply_rate_limit(f"{client_ip()}:chatbot_message", max_requests=60)
        payload = request.get_json(force=True)
        session_token = (payload.get("session_token") or "").strip()
        user_message = (payload.get("message") or "").strip()
        if not session_token or not user_message:
            return jsonify({"error": "Session token and message are required."}), 400

        db = get_db()
        conversation = db.execute(
            "SELECT * FROM chatbot_conversations WHERE session_token = ?",
            (session_token,)
        ).fetchone()
        if conversation is None:
            return jsonify({"error": "Conversation not found."}), 404
        if conversation["status"] == "completed":
            return jsonify({"phase": CHATBOT_PHASE_DONE, "message": "This chat is already complete."})
        message_count = db.execute(
            "SELECT COUNT(*) AS total FROM chatbot_messages WHERE session_token = ?",
            (session_token,)
        ).fetchone()["total"]
        if chatbot_max_turns > 0 and message_count >= chatbot_max_turns:
            data = json.loads(conversation["captured_data"] or "{}")
            data["turn_cap_reached"] = True
            assistant_message = "I’m going to pause this chat here so it doesn’t go in circles. You can start a fresh chat if you’d like to continue."
            chatbot_add_message(db, session_token, "assistant", assistant_message)
            chatbot_update(db, session_token, data, CHATBOT_PHASE_DONE, status="closed")
            db.commit()
            return jsonify({"phase": CHATBOT_PHASE_DONE, "message": assistant_message}), 429

        data = json.loads(conversation["captured_data"] or "{}")
        special_response = chatbot_special_response(user_message)
        if special_response:
            chatbot_add_message(db, session_token, "user", user_message)
            assistant_message = chatbot_reply(data, conversation["phase"], special_response, logger=current_app.logger)
            chatbot_add_message(db, session_token, "assistant", assistant_message)
            chatbot_update(db, session_token, data, conversation["phase"])
            db.commit()
            return jsonify({"phase": conversation["phase"], "message": assistant_message})

        chatbot_add_message(db, session_token, "user", user_message)
        validation_message = chatbot_apply_answer(data, user_message)
        if validation_message:
            validation_message = chatbot_reply(data, conversation["phase"], validation_message, logger=current_app.logger)
            chatbot_add_message(db, session_token, "assistant", validation_message)
            chatbot_update(db, session_token, data, conversation["phase"])
            db.commit()
            return jsonify({"phase": conversation["phase"], "message": validation_message})

        phase, assistant_message = chatbot_continue(data)
        if phase == CHATBOT_PHASE_CALCULATE:
            try:
                assistant_message = chatbot_calculate(data)
                phase = CHATBOT_PHASE_OFFER
            except Exception:
                current_app.logger.exception("Chatbot calculation failed")
                return jsonify({"error": "I couldn’t calculate that right now. Please try again shortly."}), 500
        elif phase == CHATBOT_PHASE_DONE and data.get("declined_contact_details"):
            chatbot_add_message(db, session_token, "assistant", assistant_message)
            chatbot_update(db, session_token, data, phase, status="closed")
            db.commit()
            return jsonify({"phase": phase, "message": assistant_message})
        elif phase == CHATBOT_PHASE_HANDOFF:
            messages = db.execute(
                "SELECT role, message FROM chatbot_messages WHERE session_token = ? ORDER BY id ASC",
                (session_token,)
            ).fetchall()
            transcript = "\n".join(f"{row['role']}: {row['message']}" for row in messages)
            lead_payload, summary = chatbot_prepare_lead_payload(data, transcript)
            result = save_lead_payload(lead_payload, create_report=True)
            status_code = 200
            response = result
            if isinstance(result, tuple):
                response, status_code = result
            response_data = response.get_json(silent=True) or {}
            if status_code >= 400 or not response_data.get("success"):
                chatbot_add_message(db, session_token, "assistant", "I couldn’t save the report just now. Please try again in a moment.")
                chatbot_update(db, session_token, data, CHATBOT_PHASE_OFFER)
                db.commit()
                return response
            lead_id = response_data.get("lead_id")
            data["lead_id"] = lead_id
            data["pdf_url"] = response_data.get("pdf_url")
            if data.get("pdf_url", "").startswith("/"):
                data["pdf_url"] = request.host_url.rstrip("/") + data["pdf_url"]
            assistant_message = "Right, I’ve got everything I need. Your report is being generated now and the team will follow up where requested."
            if data.get("pdf_url"):
                assistant_message += f" You can open it here: {data['pdf_url']}"
            phase = CHATBOT_PHASE_DONE
            chatbot_add_message(db, session_token, "assistant", assistant_message)
            chatbot_update(db, session_token, data, phase, status="completed", lead_id=lead_id, ai_summary=summary)
            db.commit()
            return jsonify({
                "phase": phase,
                "message": assistant_message,
                "lead_id": lead_id,
                "pdf_url": data.get("pdf_url"),
            })

        assistant_message = chatbot_reply(data, phase, assistant_message, logger=current_app.logger)
        chatbot_add_message(db, session_token, "assistant", assistant_message)
        chatbot_update(db, session_token, data, phase)
        db.commit()
        return jsonify({"phase": phase, "message": assistant_message})

    @public_api.post("/api/chatbot/end")
    def chatbot_end():
        payload = request.get_json(force=True)
        session_token = (payload.get("session_token") or "").strip()
        if not session_token:
            return jsonify({"error": "Session token is required."}), 400
        db = get_db()
        conversation = db.execute(
            "SELECT * FROM chatbot_conversations WHERE session_token = ?",
            (session_token,)
        ).fetchone()
        if conversation is None:
            return jsonify({"error": "Conversation not found."}), 404
        data = json.loads(conversation["captured_data"] or "{}")
        data["ended_by_user"] = True
        chatbot_update(db, session_token, data, CHATBOT_PHASE_DONE, status="closed")
        db.commit()
        return jsonify({"success": True})

    @public_api.post("/api/property/value")
    def property_value():
        apply_rate_limit(f"{client_ip()}:property_value", max_requests=20)
        data = request.get_json(force=True)
        address = (data.get("address") or "").strip()
        property_type = (data.get("property_type") or "").strip()

        if not address:
            return jsonify({"error": "Address is required."}), 400

        try:
            valuation = get_real_valuation(address, property_type)
            return jsonify(valuation)
        except requests.RequestException:
            current_app.logger.exception("Valuation API request failed")
            return jsonify({"error": "Valuation service is temporarily unavailable."}), 502
        except Exception:
            current_app.logger.exception("Unexpected valuation error")
            return jsonify({"error": "Unexpected valuation error."}), 500

    @public_api.post("/api/property/calculate")
    def property_calculate():
        apply_rate_limit(f"{client_ip()}:property_calculate", max_requests=30)
        data = request.get_json(force=True)
        try:
            return jsonify(calculate_property_decision(data))
        except ValueError as error:
            return jsonify({"error": str(error)}), 400
        except Exception:
            current_app.logger.exception("Unexpected calculator error")
            return jsonify({"error": "Unexpected calculator error."}), 500

    @public_api.post("/api/property/lead")
    def property_lead():
        ip_address = client_ip()
        apply_rate_limit(f"{ip_address}:property_lead", max_requests=10)
        apply_daily_ip_limit(f"{ip_address}:property_lead", lead_daily_ip_limit)
        data = request.get_json(force=True)
        return save_lead_payload(data, create_report=True)

    @public_api.post("/api/property/lead-action")
    def property_lead_action():
        apply_rate_limit(f"{client_ip()}:property_lead_action", max_requests=20)
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

        try:
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
        except Exception:
            current_app.logger.exception("Failed to save property lead action")
            return jsonify({"success": False, "error": "Could not save that request right now."}), 500

        return jsonify({
            "success": True,
            "lead_id": lead_id,
            "lead_stage": lead_stage,
        })

    return public_api
