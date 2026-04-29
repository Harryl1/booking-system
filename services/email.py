import smtplib
from email.message import EmailMessage


def send_email(
    to_address,
    subject,
    body,
    smtp_host="",
    smtp_port=587,
    customer_email_from="",
    smtp_username="",
    smtp_password="",
    smtp_use_tls=True,
):
    if not smtp_host or not customer_email_from or not to_address:
        return False

    message = EmailMessage()
    message["From"] = customer_email_from
    message["To"] = to_address
    message["Subject"] = subject
    message.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as smtp:
        if smtp_use_tls:
            smtp.starttls()
        if smtp_username and smtp_password:
            smtp.login(smtp_username, smtp_password)
        smtp.send_message(message)
    return True


def notify_new_lead(
    lead_id,
    name,
    email,
    phone,
    address,
    lead_score,
    selling_timeframe="",
    *,
    notification_email="",
    app_base_url="",
    email_config=None,
    logger=None,
):
    if not notification_email:
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
        f"Open lead: {app_base_url}/leads/{lead_id}\n"
        f"Open leads: {app_base_url}/leads"
    )
    try:
        send_email(notification_email, f"New property lead: {name}", body, **(email_config or {}))
    except Exception:
        if logger:
            logger.exception("Failed to send new lead notification")


def send_customer_confirmation(email, report_url, *, email_config=None, logger=None):
    if not email or not report_url:
        return
    body = (
        "Thanks for using the Equiome property tool.\n\n"
        f"Your personalised report is ready here:\n{report_url}\n\n"
        "A local property expert can help confirm your valuation range and next steps.\n\n"
        "This report is an indicative estimate only and is not financial, mortgage, or legal advice."
    )
    try:
        send_email(email, "Your Equiome property report", body, **(email_config or {}))
    except Exception:
        if logger:
            logger.exception("Failed to send customer confirmation")
