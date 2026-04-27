import os
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    HRFlowable,
    Image
)

# ==========================================
# Brand configuration
# ==========================================

BRAND = {
    "name": "equiome",
    "primary": colors.HexColor("#12344D"),
    "accent": colors.HexColor("#2A7F9E"),
    "text": colors.HexColor("#243342"),
    "subtext": colors.HexColor("#5B6573"),
    "border": colors.HexColor("#D7DEE7"),
    "card_bg": colors.HexColor("#F5F7FA"),
    "highlight_bg": colors.HexColor("#EAF4F8"),
    "white": colors.white,
}

SERVICE_LABELS = {
    "valuation": "Local agent valuation",
    "agent_valuation": "Local agent valuation",
    "local_agent_valuation": "Local agent valuation",
    "epc": "EPC assessment",
    "epc_booking": "EPC assessment",
    "solicitor": "Solicitor quote",
    "legal": "Solicitor quote",
    "mortgage": "Mortgage advice",
    "mortgage_advice": "Mortgage advice",
    "conveyancing_quote": "Conveyancing quote",
}


# ==========================================
# Helpers
# ==========================================

def safe_text(value, fallback="—"):
    if value is None:
        return fallback
    text = str(value).strip()
    return text if text else fallback


def format_currency(value, prefix="£", decimals=0):
    try:
        if value is None:
            return f"{prefix}0"
        return f"{prefix}{float(value):,.{decimals}f}"
    except Exception:
        return f"{prefix}0"


def format_currency_range(low, high, prefix="£", decimals=0):
    return f"{format_currency(low, prefix, decimals)} – {format_currency(high, prefix, decimals)}"


def ensure_parent_dir(filepath):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)


def normalise_services(services):
    cleaned = []
    for service in services or []:
        label = SERVICE_LABELS.get(service, service)
        label = safe_text(label, "").strip()
        if label and label not in cleaned:
            cleaned.append(label)
    return cleaned


def build_styles():
    styles = getSampleStyleSheet()

    styles.add(ParagraphStyle(
        name="ReportTitle",
        parent=styles["Heading1"],
        fontName="Helvetica-Bold",
        fontSize=21,
        leading=24,
        textColor=BRAND["primary"],
        spaceAfter=2,
    ))

    styles.add(ParagraphStyle(
        name="ReportSubtitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=13,
        textColor=BRAND["subtext"],
        spaceAfter=0,
    ))

    styles.add(ParagraphStyle(
        name="SectionHeading",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=14,
        textColor=BRAND["primary"],
        spaceAfter=8,
        spaceBefore=4,
    ))

    styles.add(ParagraphStyle(
        name="Body",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        textColor=BRAND["text"],
        spaceAfter=6,
    ))

    styles.add(ParagraphStyle(
        name="Small",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=11,
        textColor=BRAND["subtext"],
    ))

    styles.add(ParagraphStyle(
        name="MetricLabel",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=10,
        textColor=BRAND["subtext"],
        spaceAfter=4,
    ))

    styles.add(ParagraphStyle(
        name="MetricValue",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=16,
        leading=18,
        textColor=BRAND["primary"],
    ))

    styles.add(ParagraphStyle(
        name="MetricValueHighlight",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=20,
        textColor=BRAND["accent"],
    ))

    styles.add(ParagraphStyle(
        name="SummaryKicker",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=9,
        leading=11,
        textColor=BRAND["accent"],
        spaceAfter=6,
    ))

    styles.add(ParagraphStyle(
        name="SummaryHeadline",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        textColor=BRAND["primary"],
        spaceAfter=6,
    ))

    styles.add(ParagraphStyle(
        name="SummaryBody",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        leading=14,
        textColor=BRAND["text"],
    ))

    styles.add(ParagraphStyle(
        name="Footer",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8,
        leading=10,
        textColor=BRAND["subtext"],
        alignment=TA_RIGHT,
    ))

    return styles


# ==========================================
# Page chrome
# ==========================================

def draw_page_chrome(canvas, doc):
    canvas.saveState()

    page_width, page_height = A4

    canvas.setStrokeColor(BRAND["accent"])
    canvas.setLineWidth(2)
    canvas.line(doc.leftMargin, page_height - 12 * mm, page_width - doc.rightMargin, page_height - 12 * mm)

    canvas.setStrokeColor(BRAND["border"])
    canvas.setLineWidth(0.5)
    canvas.line(doc.leftMargin, 14 * mm, page_width - doc.rightMargin, 14 * mm)

    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(BRAND["subtext"])
    canvas.drawString(doc.leftMargin, 9 * mm, f"{BRAND['name']} property report")
    canvas.drawRightString(page_width - doc.rightMargin, 9 * mm, f"Page {canvas.getPageNumber()}")

    canvas.restoreState()


# ==========================================
# Building blocks
# ==========================================

def metric_card(label, value, styles, width, highlight=False):
    bg = BRAND["highlight_bg"] if highlight else BRAND["card_bg"]
    value_style = styles["MetricValueHighlight"] if highlight else styles["MetricValue"]

    t = Table(
        [[Paragraph(label, styles["MetricLabel"])],
         [Paragraph(value, value_style)]],
        colWidths=[width]
    )

    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), bg),
        ("BOX", (0, 0), (-1, -1), 0.8, BRAND["border"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    return t


def summary_box(report_data, styles, content_width):
    headline = f"You could buy up to {format_currency(report_data.get('max_budget'))}"

    body_text = (
        f"Based on your current position, your estimated available equity is "
        f"<b>{format_currency(report_data.get('net_proceeds'))}</b> and your indicative "
        f"borrowing power is <b>{format_currency(report_data.get('borrowing_power'))}</b>. "
        f"This uses standard affordability assumptions and estimated selling costs."
    )

    table = Table(
        [[Paragraph("AT A GLANCE", styles["SummaryKicker"])],
         [Paragraph(headline, styles["SummaryHeadline"])],
         [Paragraph(body_text, styles["SummaryBody"])]],
        colWidths=[content_width]
    )

    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND["highlight_bg"]),
        ("BOX", (0, 0), (-1, -1), 0.8, BRAND["border"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 14),
        ("RIGHTPADDING", (0, 0), (-1, -1), 14),
        ("TOPPADDING", (0, 0), (-1, -1), 12),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
    ]))
    return table


def next_best_move(report_data):
    recommendation = safe_text(report_data.get("recommendation"), "")
    if recommendation and recommendation != "—":
        return recommendation
    if float(report_data.get("max_budget") or 0) > 0:
        return "Confirm your sale price with a local valuation and sense-check your borrowing position before viewing your next property."
    return "Start with a local valuation so your onward plans are based on a more reliable sale price."


def detail_row(label, value, styles, total_width):
    label_width = 58 * mm
    value_width = total_width - label_width

    t = Table(
        [[
            Paragraph(f"<b>{label}</b>", styles["Body"]),
            Paragraph(safe_text(value), styles["Body"])
        ]],
        colWidths=[label_width, value_width]
    )

    t.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return t


def boxed_section(title, body_flowables, styles, content_width):
    inner = [[Paragraph(title, styles["SectionHeading"])]]
    for item in body_flowables:
        inner.append([item])

    t = Table(inner, colWidths=[content_width])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND["card_bg"]),
        ("BOX", (0, 0), (-1, -1), 0.8, BRAND["border"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 14),
        ("RIGHTPADDING", (0, 0), (-1, -1), 14),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    return t


# ==========================================
# Main report function
# ==========================================

def generate_pdf_report(report_data, filepath, logo_path=None):
    """
    report_data expected keys:
    {
        "name": "Jane Smith",
        "email": "jane@example.com",
        "address": "10 High Street, London",
        "valuation_low": 425000,
        "valuation_high": 450000,
        "moving_costs": 12000,
        "net_proceeds": 433000,
        "borrowing_power": 280000,
        "max_budget": 713000,
        "recommendation": "Based on your estimated equity and borrowing position...",
        "selected_services": ["Local agent valuation", "Mortgage advice", "Conveyancing quote"]
    }
    """

    ensure_parent_dir(filepath)

    doc = SimpleDocTemplate(
        filepath,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=20 * mm,
        bottomMargin=20 * mm,
    )

    styles = build_styles()
    story = []
    content_width = A4[0] - doc.leftMargin - doc.rightMargin
    selected_services = normalise_services(report_data.get("selected_services") or [])

    # ------------------------------------------
    # Header / branding
    # ------------------------------------------
    header_left = []

    if logo_path and os.path.exists(logo_path):
        try:
            img = Image(logo_path)
            max_width = 60 * mm
            aspect = img.imageHeight / float(img.imageWidth)
            img.drawWidth = max_width
            img.drawHeight = max_width * aspect
            img.hAlign = "LEFT"
            header_left.append(img)
            header_left.append(Spacer(1, 8))
        except Exception:
            pass

    header_left.append(Paragraph("Your Property Report", styles["ReportTitle"]))
    header_left.append(Spacer(1, 2))
    header_left.append(Paragraph(
        "A personalised view of your equity and next-property budget.",
        styles["ReportSubtitle"]
    ))

    prepared_for = (
        f"<b>Prepared for</b><br/>"
        f"{safe_text(report_data.get('name'))}<br/>"
        f"{safe_text(report_data.get('email'))}<br/>"
        f"{datetime.now().strftime('%d %B %Y')}"
    )

    header_right = Table(
        [[Paragraph(prepared_for, styles["Body"])]],
        colWidths=[content_width * 0.32]
    )
    header_right.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), BRAND["card_bg"]),
        ("BOX", (0, 0), (-1, -1), 0.8, BRAND["border"]),
        ("LEFTPADDING", (0, 0), (-1, -1), 10),
        ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))

    header_table = Table(
        [[header_left, header_right]],
        colWidths=[content_width * 0.66, content_width * 0.34]
    )
    header_table.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
    ]))

    story.append(header_table)
    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=0.6, color=BRAND["border"]))
    story.append(Spacer(1, 16))

    # ------------------------------------------
    # Page 1 – Summary
    # ------------------------------------------
    story.append(summary_box(report_data, styles, content_width))
    story.append(Spacer(1, 16))

    story.append(Paragraph("Estimated financial position", styles["SectionHeading"]))

    card_gap = 8
    col_width = (content_width - card_gap) / 2

    row_1 = Table([[
        metric_card(
            "Estimated property value",
            format_currency_range(
                report_data.get("valuation_low"),
                report_data.get("valuation_high")
            ),
            styles,
            col_width
        ),
        metric_card(
            "Estimated sale and moving costs",
            format_currency(report_data.get("moving_costs")),
            styles,
            col_width
        )
    ]], colWidths=[col_width, col_width])

    row_1.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    row_2 = Table([[
        metric_card(
            "Estimated available equity",
            format_currency(report_data.get("net_proceeds")),
            styles,
            col_width
        ),
        metric_card(
            "Estimated borrowing power",
            format_currency(report_data.get("borrowing_power")),
            styles,
            col_width
        )
    ]], colWidths=[col_width, col_width])

    row_2.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    row_3 = Table([[
        metric_card(
            "Estimated maximum purchase budget",
            format_currency(report_data.get("max_budget")),
            styles,
            content_width,
            highlight=True
        )
    ]], colWidths=[content_width])

    row_3.setStyle(TableStyle([
        ("LEFTPADDING", (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    story.append(row_1)
    story.append(Spacer(1, 8))
    story.append(row_2)
    story.append(Spacer(1, 8))
    story.append(row_3)
    story.append(Spacer(1, 16))

    recommendation_text = safe_text(
        next_best_move(report_data),
        "We recommend reviewing your figures with a local expert before making any property decisions."
    )

    recommendation_box = boxed_section(
        "Recommended next step",
        [Paragraph(recommendation_text, styles["Body"])],
        styles,
        content_width
    )
    story.append(recommendation_box)
    story.append(Spacer(1, 16))

    cost_items = [
        detail_row("Estimated moving costs", format_currency(report_data.get("moving_costs")), styles, content_width),
        detail_row("Estimated equity after costs", format_currency(report_data.get("net_proceeds")), styles, content_width),
        detail_row("Indicative borrowing power", format_currency(report_data.get("borrowing_power")), styles, content_width),
        detail_row("Estimated monthly payment", format_currency(report_data.get("monthly_payment_estimate")), styles, content_width),
    ]
    story.append(boxed_section("Cost and affordability snapshot", cost_items, styles, content_width))
    story.append(Spacer(1, 16))

    assumptions_items = [
        detail_row("Client name", safe_text(report_data.get("name")), styles, content_width),
        detail_row("Email", safe_text(report_data.get("email")), styles, content_width),
        detail_row("Address", safe_text(report_data.get("address")), styles, content_width),
        detail_row(
            "Valuation range",
            format_currency_range(report_data.get("valuation_low"), report_data.get("valuation_high")),
            styles,
            content_width
        ),
        detail_row(
            "Available equity",
            format_currency(report_data.get("net_proceeds")),
            styles,
            content_width
        ),
        detail_row(
            "Maximum budget",
            format_currency(report_data.get("max_budget")),
            styles,
            content_width
        ),
        detail_row(
            "Selling timeframe",
            safe_text(report_data.get("selling_timeframe"), "Not supplied"),
            styles,
            content_width
        ),
    ]

    assumptions_box = boxed_section(
        "Report summary",
        assumptions_items,
        styles,
        content_width
    )
    story.append(assumptions_box)

    if selected_services:
        story.append(Spacer(1, 10))
        service_lines = [Paragraph("You asked to hear about:", styles["Body"])]
        for service in selected_services:
            service_lines.append(Paragraph(f"• {safe_text(service)}", styles["Body"]))

        services_box = boxed_section(
            "Next-step services",
            service_lines,
            styles,
            content_width
        )
        story.append(services_box)

    cta_lines = [
        Paragraph("<b>Ready to confirm your numbers?</b>", styles["Body"]),
        Paragraph("A local valuation and mortgage review will help turn these indicative figures into a clearer moving plan.", styles["Body"]),
    ]
    story.append(Spacer(1, 10))
    story.append(boxed_section("Recommended follow-up", cta_lines, styles, content_width))

    # ------------------------------------------
    # Guidance & next steps
    # ------------------------------------------
    story.append(Spacer(1, 12))
    story.append(Paragraph("What this means for you", styles["SectionHeading"]))
    story.append(Paragraph(
        "Based on your estimated equity and borrowing position, this report gives an "
        "indicative view of what your next move could look like. These figures are designed "
        "to help you understand your position before speaking to an agent or mortgage advisor.",
        styles["Body"]
    ))
    story.append(Spacer(1, 6))

    story.append(Paragraph("Understanding your numbers", styles["SectionHeading"]))

    explain_items = [
        Paragraph(
            "<b>Property value:</b> Your estimated valuation range is based on available data and assumptions. "
            "Actual sale price may vary depending on market conditions.",
            styles["Body"]
        ),
        Paragraph(
            "<b>Available equity:</b> This is what you may have left after repaying your mortgage and "
            "estimated selling costs.",
            styles["Body"]
        ),
        Paragraph(
            "<b>Borrowing power:</b> This is an indicative estimate based on standard affordability multiples. "
            "A lender may offer more or less depending on your income profile, commitments, deposit position, "
            "and credit assessment.",
            styles["Body"]
        ),
        Paragraph(
            "<b>Maximum budget:</b> This combines your estimated available equity with your indicative borrowing "
            "capacity to show the upper end of what your next purchase could look like.",
            styles["Body"]
        ),
    ]

    for item in explain_items:
        story.append(item)
        story.append(Spacer(1, 3))

    story.append(Spacer(1, 6))
    story.append(Paragraph("Next steps", styles["SectionHeading"]))

    next_steps = []

    if "Local agent valuation" in selected_services:
        next_steps.append("Arrange a local agent valuation to refine your likely sale price.")
    if "Mortgage advice" in selected_services:
        next_steps.append("Speak to a mortgage advisor to confirm your borrowing range and monthly costs.")
    if "Conveyancing quote" in selected_services:
        next_steps.append("Request a conveyancing quote so you can compare likely moving costs more accurately.")
    if "EPC assessment" in selected_services:
        next_steps.append("Arrange an EPC assessment if you are preparing to sell or let the property.")

    if not next_steps:
        next_steps = [
            "Arrange a local valuation to sense-check your likely sale price.",
            "Review your mortgage options to confirm your realistic next-step budget.",
        ]

    for step in next_steps:
        story.append(Paragraph(f"• {step}", styles["Body"]))
        story.append(Spacer(1, 4))

    story.append(Spacer(1, 8))
    story.append(Paragraph(
        "If you would like help reviewing your options or taking the next step, "
        "you can request a call and we’ll guide you through your specific situation.",
        styles["Body"]
    ))

    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=0.6, color=BRAND["border"]))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "This report is an indicative estimate only and should not be treated as financial, mortgage, "
        "or legal advice. Final figures may vary depending on lender criteria, local market conditions, "
        "sale price achieved, and transaction costs.",
        styles["Small"]
    ))

    doc.build(
        story,
        onFirstPage=draw_page_chrome,
        onLaterPages=draw_page_chrome
    )

    return filepath
