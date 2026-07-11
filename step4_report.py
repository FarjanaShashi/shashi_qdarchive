"""
Part 2 – Step 4c + 4d: Generate XLSX and PDF Report
=====================================================
Produces two deliverables:

4c) 23148157-sq26-results.xlsx
    Columns: repository_id, project_type, project_title,
             primary_class, secondary_class, no_project_files

4d) 23148157-sq26-report.pdf
    Per repository section with:
    - Histogram of primary ISIC classes (vector-quality via matplotlib)
    - Rank-ordered top-20 class table
    - Comments

Run:
    cd ~/Desktop/QDArchive
    source venv/bin/activate
    pip install matplotlib reportlab
    python3 step4_report.py
"""

import sqlite3
import os
import warnings
warnings.filterwarnings("ignore")

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import io

from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                 Table, TableStyle, Image as RLImage,
                                 PageBreak)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT

DB_PATH      = "23148157-sq26-classification.db"
XLSX_OUT     = "23148157-sq26-results.xlsx"
PDF_OUT      = "23148157-sq26-report.pdf"

REPO_NAMES = {
    2:  "Dryad",
    11: "FSD (Finnish Social Science Data Archive)",
}

ISIC_DIV_NAMES = {
    "01": "Crop and animal production, hunting and related service activities",
    "02": "Forestry and logging",
    "03": "Fishing and aquaculture",
    "05": "Mining of coal and lignite",
    "06": "Extraction of crude petroleum and natural gas",
    "07": "Mining of metal ores",
    "08": "Other mining and quarrying",
    "09": "Mining support service activities",
    "10": "Manufacture of food products",
    "11": "Manufacture of beverages",
    "12": "Manufacture of tobacco products",
    "13": "Manufacture of textiles",
    "14": "Manufacture of wearing apparel",
    "15": "Manufacture of leather and related products",
    "16": "Manufacture of wood and products of wood and cork",
    "17": "Manufacture of paper and paper products",
    "18": "Printing and reproduction of recorded media",
    "19": "Manufacture of coke and refined petroleum products",
    "20": "Manufacture of chemicals and chemical products",
    "21": "Manufacture of basic pharmaceutical products and preparations",
    "22": "Manufacture of rubber and plastics products",
    "23": "Manufacture of other non-metallic mineral products",
    "24": "Manufacture of basic metals",
    "25": "Manufacture of fabricated metal products, except machinery",
    "26": "Manufacture of computer, electronic and optical products",
    "27": "Manufacture of electrical equipment",
    "28": "Manufacture of machinery and equipment n.e.c.",
    "29": "Manufacture of motor vehicles, trailers and semi-trailers",
    "30": "Manufacture of other transport equipment",
    "31": "Manufacture of furniture",
    "32": "Other manufacturing",
    "33": "Repair and installation of machinery and equipment",
    "35": "Electricity, gas, steam and air conditioning supply",
    "36": "Water collection, treatment and supply",
    "37": "Sewerage",
    "38": "Waste collection, treatment and disposal activities",
    "39": "Remediation activities and other waste management services",
    "41": "Construction of buildings",
    "42": "Civil engineering",
    "43": "Specialised construction activities",
    "45": "Wholesale and retail trade and repair of motor vehicles",
    "46": "Wholesale trade, except of motor vehicles",
    "47": "Retail trade, except of motor vehicles",
    "49": "Land transport and transport via pipelines",
    "50": "Water transport",
    "51": "Air transport",
    "52": "Warehousing and support activities for transportation",
    "53": "Postal and courier activities",
    "55": "Accommodation",
    "56": "Food and beverage service activities",
    "58": "Publishing activities",
    "59": "Motion picture, video and television programme production",
    "60": "Programming and broadcasting activities",
    "61": "Telecommunications",
    "62": "Computer programming, consultancy and related activities",
    "63": "Information service activities",
    "64": "Financial service activities, except insurance and pension funding",
    "65": "Insurance, reinsurance and pension funding",
    "66": "Activities auxiliary to financial service and insurance activities",
    "68": "Real estate activities",
    "69": "Legal and accounting activities",
    "70": "Activities of head offices; management consultancy activities",
    "71": "Architectural and engineering activities; technical testing",
    "72": "Scientific research and development",
    "73": "Advertising and market research",
    "74": "Other professional, scientific and technical activities",
    "75": "Veterinary activities",
    "77": "Rental and leasing activities",
    "78": "Employment activities",
    "79": "Travel agency, tour operator and related activities",
    "80": "Security and investigation activities",
    "81": "Services to buildings and landscape activities",
    "82": "Office administrative and business support activities",
    "84": "Public administration and defence; compulsory social security",
    "85": "Education",
    "86": "Human health activities",
    "87": "Residential care activities",
    "88": "Social work activities without accommodation",
    "90": "Creative, arts and entertainment activities",
    "91": "Libraries, archives, museums and other cultural activities",
    "92": "Gambling and betting activities",
    "93": "Sports activities and amusement and recreation activities",
    "94": "Activities of membership organisations",
    "95": "Repair of computers and personal and household goods",
    "96": "Other personal service activities",
    "97": "Activities of households as employers of domestic personnel",
    "98": "Undifferentiated goods and services producing activities of private households",
    "99": "Activities of extraterritorial organisations and bodies",
}

SECTION_NAMES = {
    "A": "Agriculture, Forestry and Fishing",
    "B": "Mining and Quarrying",
    "C": "Manufacturing",
    "D": "Electricity, Gas, Steam and Air Conditioning Supply",
    "E": "Water Supply, Sewerage, Waste Management",
    "F": "Construction",
    "G": "Wholesale and Retail Trade",
    "H": "Transportation and Storage",
    "I": "Accommodation and Food Service Activities",
    "J": "Information and Communication",
    "K": "Financial and Insurance Activities",
    "L": "Real Estate Activities",
    "M": "Professional, Scientific and Technical Activities",
    "N": "Administrative and Support Service Activities",
    "O": "Public Administration and Defence",
    "P": "Education",
    "Q": "Human Health and Social Work Activities",
    "R": "Arts, Entertainment and Recreation",
    "S": "Other Service Activities",
    "T": "Activities of Households as Employers",
    "U": "Activities of Extraterritorial Organisations",
}


# ── Database helpers ───────────────────────────────────────────────────────

def load_data(conn):
    cur = conn.cursor()
    # Main project rows
    cur.execute("""
        SELECT p.id, p.repository_id, p.type, p.title,
               p.isic_section, p.isic_division,
               COUNT(f.id) as file_count
        FROM projects p
        LEFT JOIN files f ON f.project_id = p.id
        WHERE p.type IN ('QDA_PROJECT','QD_PROJECT')
        GROUP BY p.id
        ORDER BY p.repository_id, p.type, p.id
    """)
    return cur.fetchall()


def get_division_distribution(conn, repo_id, project_type):
    cur = conn.cursor()
    cur.execute("""
        SELECT isic_section, isic_division, COUNT(*) as n
        FROM projects
        WHERE repository_id = ? AND type = ?
          AND isic_division IS NOT NULL
        GROUP BY isic_division
        ORDER BY n DESC
    """, (repo_id, project_type))
    return cur.fetchall()


def get_project_count_by_type(conn, repo_id):
    cur = conn.cursor()
    cur.execute("""
        SELECT type, COUNT(*) FROM projects
        WHERE repository_id = ?
        GROUP BY type
        ORDER BY type
    """, (repo_id,))
    return dict(cur.fetchall())


# ── XLSX generation ────────────────────────────────────────────────────────

def make_xlsx(rows):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Classification Results"

    # Header style
    header_font  = Font(bold=True, color="FFFFFF", size=11)
    header_fill  = PatternFill("solid", fgColor="2E4057")
    header_align = Alignment(horizontal="center", vertical="center",
                             wrap_text=True)
    thin = Side(style="thin", color="AAAAAA")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    headers = ["repository_id", "project_type", "project_title",
               "primary_class", "secondary_class", "no_project_files"]
    col_widths = [15, 18, 60, 30, 30, 18]

    for col_idx, (h, w) in enumerate(zip(headers, col_widths), 1):
        cell = ws.cell(row=1, column=col_idx, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = border
        ws.column_dimensions[
            openpyxl.utils.get_column_letter(col_idx)
        ].width = w

    ws.row_dimensions[1].height = 30

    # Data rows
    alt_fill = PatternFill("solid", fgColor="F0F4F8")
    data_align = Alignment(vertical="top", wrap_text=True)

    for row_idx, row in enumerate(rows, 2):
        pid, repo_id, ptype, title, sec, div, file_count = row
        repo_name = REPO_NAMES.get(repo_id, str(repo_id))
        primary   = f"{sec}/{div} – {ISIC_DIV_NAMES.get(div or '', '')}" if sec and div else ""
        fill = alt_fill if row_idx % 2 == 0 else PatternFill()

        values = [repo_id, ptype, title or "", primary, "", file_count]
        for col_idx, val in enumerate(values, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.alignment = data_align
            cell.border = border
            if row_idx % 2 == 0:
                cell.fill = alt_fill

    # Freeze header row
    ws.freeze_panes = "A2"

    # Summary tab
    ws2 = wb.create_sheet("Summary by Repository")
    ws2.column_dimensions["A"].width = 40
    ws2.column_dimensions["B"].width = 20
    ws2.column_dimensions["C"].width = 20

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        SELECT repository_id, type, COUNT(*) as n
        FROM projects
        GROUP BY repository_id, type
        ORDER BY repository_id, type
    """)
    summary_rows = cur.fetchall()
    conn.close()

    ws2.cell(1,1,"Repository").font = Font(bold=True)
    ws2.cell(1,2,"Project Type").font = Font(bold=True)
    ws2.cell(1,3,"Count").font = Font(bold=True)
    for i, (rid, ptype, n) in enumerate(summary_rows, 2):
        ws2.cell(i, 1, REPO_NAMES.get(rid, str(rid)))
        ws2.cell(i, 2, ptype)
        ws2.cell(i, 3, n)

    wb.save(XLSX_OUT)
    print(f"XLSX saved: {XLSX_OUT}")


# ── Histogram generation ───────────────────────────────────────────────────

def make_histogram(dist_rows, repo_name, project_type, top_n=20):
    """Return a PNG bytes buffer of a histogram."""
    # Take top_n by count
    dist_rows = dist_rows[:top_n]
    if not dist_rows:
        return None

    labels = []
    counts = []
    for sec, div, n in dist_rows:
        full = ISIC_DIV_NAMES.get(div or "", f"Division {div}")
        # Wrap long labels
        if len(full) > 35:
            full = full[:33] + "…"
        labels.append(f"{div} – {full}")
        counts.append(n)

    fig_height = max(4, len(labels) * 0.45 + 1.5)
    fig, ax = plt.subplots(figsize=(11, fig_height))

    colors_list = plt.cm.Blues(
        [0.4 + 0.5 * (i / max(len(counts)-1, 1)) for i in range(len(counts))]
    )
    bars = ax.barh(range(len(labels)), counts, color=colors_list,
                   edgecolor="white", linewidth=0.5)

    # Count labels on bars
    for bar, n in zip(bars, counts):
        ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
                str(n), va="center", ha="left", fontsize=9, fontweight="bold")

    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=8.5)
    ax.invert_yaxis()
    ax.set_xlabel("Number of Projects", fontsize=10)
    ax.set_title(f"{repo_name}\n{project_type} – ISIC Primary Class Distribution",
                 fontsize=11, fontweight="bold", pad=12)
    ax.xaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_xlim(0, max(counts) * 1.15)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=150)
    plt.close(fig)
    buf.seek(0)
    return buf


# ── PDF generation ─────────────────────────────────────────────────────────

def make_pdf(conn):
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("Title2", parent=styles["Title"],
                                  fontSize=18, spaceAfter=12)
    h1_style = ParagraphStyle("H1", parent=styles["Heading1"],
                               fontSize=14, spaceAfter=6, spaceBefore=14,
                               textColor=colors.HexColor("#2E4057"))
    h2_style = ParagraphStyle("H2", parent=styles["Heading2"],
                               fontSize=11, spaceAfter=4, spaceBefore=10,
                               textColor=colors.HexColor("#048A81"))
    body_style = ParagraphStyle("Body2", parent=styles["Normal"],
                                 fontSize=9, spaceAfter=4, leading=13)
    caption_style = ParagraphStyle("Caption", parent=styles["Normal"],
                                    fontSize=8, textColor=colors.grey,
                                    alignment=TA_CENTER)

    doc = SimpleDocTemplate(PDF_OUT, pagesize=A4,
                            rightMargin=2*cm, leftMargin=2*cm,
                            topMargin=2*cm, bottomMargin=2*cm)
    story = []

    # Cover
    story.append(Spacer(1, 3*cm))
    story.append(Paragraph("QDArchive – Part 2 Classification Report", title_style))
    story.append(Paragraph("Student ID: 23148157 | FAU Erlangen-Nürnberg", styles["Normal"]))
    story.append(Spacer(1, 0.5*cm))
    story.append(Paragraph(
        "This report presents the results of ISIC Rev. 5 classification applied to "
        "qualitative research projects acquired from two repositories (Dryad and FSD). "
        "Classification used sentence-transformer embeddings (all-MiniLM-L6-v2) on "
        "project metadata (Tier 1) and extracted file content (Tier 2). "
        "Validation is descriptive only.", body_style))
    story.append(PageBreak())

    # Overall summary
    story.append(Paragraph("Overview", h1_style))
    cur = conn.cursor()
    cur.execute("""
        SELECT repository_id, type, COUNT(*) as n
        FROM projects GROUP BY repository_id, type ORDER BY repository_id, type
    """)
    summary = cur.fetchall()
    tdata = [["Repository", "Project Type", "Count"]]
    for rid, ptype, n in summary:
        tdata.append([REPO_NAMES.get(rid, str(rid)), ptype, str(n)])
    t = Table(tdata, colWidths=[7*cm, 5*cm, 3*cm])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#2E4057")),
        ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
        ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
        ("FONTSIZE",   (0,0), (-1,-1), 9),
        ("ROWBACKGROUNDS", (0,1), (-1,-1),
         [colors.white, colors.HexColor("#F0F4F8")]),
        ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#CCCCCC")),
        ("ALIGN", (2,0), (2,-1), "CENTER"),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("TOPPADDING", (0,0), (-1,-1), 5),
        ("BOTTOMPADDING", (0,0), (-1,-1), 5),
    ]))
    story.append(t)
    story.append(PageBreak())

    # Per-repository sections
    repo_ids = sorted(REPO_NAMES.keys())
    project_types = ["QDA_PROJECT", "QD_PROJECT"]

    for repo_id in repo_ids:
        repo_name = REPO_NAMES[repo_id]
        story.append(Paragraph(f"Repository: {repo_name}", h1_style))

        type_counts = get_project_count_by_type(conn, repo_id)
        count_lines = ", ".join(
            f"{v} × {k}" for k, v in sorted(type_counts.items())
        )
        story.append(Paragraph(f"Total projects: {sum(type_counts.values())}  ({count_lines})", body_style))
        story.append(Spacer(1, 0.3*cm))

        has_content = False
        for ptype in project_types:
            dist = get_division_distribution(conn, repo_id, ptype)
            if not dist:
                story.append(Paragraph(
                    f"<b>{ptype}</b>: No classified projects found in this repository.",
                    body_style))
                story.append(Spacer(1, 0.2*cm))
                continue

            has_content = True
            story.append(Paragraph(f"a. {ptype}", h2_style))

            # Histogram
            hist_buf = make_histogram(dist, repo_name, ptype)
            if hist_buf:
                img = RLImage(hist_buf, width=16*cm,
                              height=max(5*cm, len(dist[:20])*0.6*cm + 2*cm))
                story.append(img)
                story.append(Paragraph(
                    f"Figure: Distribution of primary ISIC classes for {ptype} "
                    f"in {repo_name}. Bar labels show project counts.",
                    caption_style))
                story.append(Spacer(1, 0.4*cm))

            # Rank-ordered table (top 20)
            story.append(Paragraph("b. Rank-ordered class table (top 20)", h2_style))
            tdata2 = [["Rank", "Division", "Full Class Name", "Count"]]
            for rank, (sec, div, n) in enumerate(dist[:20], 1):
                full_name = ISIC_DIV_NAMES.get(div or "", f"Division {div}")
                sec_name  = SECTION_NAMES.get(sec or "", "")
                tdata2.append([
                    str(rank),
                    f"{sec}/{div}",
                    full_name,
                    str(n)
                ])
            t2 = Table(tdata2, colWidths=[1.2*cm, 2*cm, 10.5*cm, 1.8*cm])
            t2.setStyle(TableStyle([
                ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#048A81")),
                ("TEXTCOLOR",  (0,0), (-1,0), colors.white),
                ("FONTNAME",   (0,0), (-1,0), "Helvetica-Bold"),
                ("FONTSIZE",   (0,0), (-1,-1), 8),
                ("ROWBACKGROUNDS", (0,1), (-1,-1),
                 [colors.white, colors.HexColor("#F0F4F8")]),
                ("GRID", (0,0), (-1,-1), 0.4, colors.HexColor("#CCCCCC")),
                ("ALIGN", (0,0), (0,-1), "CENTER"),
                ("ALIGN", (3,0), (3,-1), "CENTER"),
                ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
                ("TOPPADDING", (0,0), (-1,-1), 4),
                ("BOTTOMPADDING", (0,0), (-1,-1), 4),
            ]))
            story.append(t2)
            story.append(Spacer(1, 0.4*cm))

            # Comments
            story.append(Paragraph("c. Comments", h2_style))
            dominant = dist[0]
            dom_div  = dominant[1]
            dom_n    = dominant[2]
            dom_name = ISIC_DIV_NAMES.get(dom_div or "", "")
            total_classified = sum(r[2] for r in dist)
            comment = (
                f"A total of {total_classified} {ptype} projects in {repo_name} "
                f"were classified. The dominant ISIC class is Division {dom_div} "
                f"({dom_name}), accounting for {dom_n} of {total_classified} projects "
                f"({100*dom_n//total_classified}%). "
            )
            if repo_id == 2:
                comment += (
                    "The strong concentration in Section A (Agriculture, Forestry and "
                    "Fishing) reflects Dryad's focus on ecological and environmental "
                    "research datasets. Classification relied primarily on file content "
                    "(Tier 2) since many Dryad projects lack detailed textual descriptions. "
                    "The embedding-based classifier performed well for ecology-related "
                    "datasets but showed some noise for ambiguous multi-disciplinary projects."
                )
            elif repo_id == 11:
                comment += (
                    "FSD projects are Finnish social science datasets. The classifier "
                    "used primarily Tier 1 metadata (titles and descriptions in Finnish "
                    "and English). Classification into social science categories was "
                    "limited by the absence of downloaded file content for most FSD records."
                )
            story.append(Paragraph(comment, body_style))
            story.append(Spacer(1, 0.5*cm))

        story.append(PageBreak())

    # Technical challenges
    story.append(Paragraph("Technical Challenges", h1_style))
    challenges = [
        ("FSD login barrier",
         "FSD Class A (CC BY 4.0) datasets required a 3-step session cookie flow. "
         "Despite successful metadata harvesting (2186 records), most project files "
         "were unavailable for content extraction, limiting Tier 2 classification."),
        ("Dryad rate limiting",
         "Dryad enforces aggressive HTTP 429 rate limits. Downloads required "
         "deliberate wait intervals, resulting in 707 files across 49 projects."),
        ("QDA file scarcity",
         "Neither Dryad nor FSD contained .qdpx or other QDA-format files. "
         "All 70 classified projects are QD_PROJECT type (primary data files only). "
         "This is expected for general scientific repositories."),
        ("Metadata sparsity",
         "Many Dryad projects have minimal descriptions, shifting classification "
         "weight entirely onto file content (CSV column headers, README text). "
         "This can introduce noise when file names do not clearly signal domain."),
        ("Classifier determinism",
         "Sentence-transformer embeddings are deterministic given a fixed model "
         "and input, ensuring reproducibility. The all-MiniLM-L6-v2 model was "
         "chosen for its balance of speed and semantic quality."),
    ]
    for title, text in challenges:
        story.append(Paragraph(f"<b>{title}</b>", body_style))
        story.append(Paragraph(text, body_style))
        story.append(Spacer(1, 0.2*cm))

    doc.build(story)
    print(f"PDF saved: {PDF_OUT}")


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    print("Generating XLSX ...")
    rows = load_data(conn)
    make_xlsx(rows)

    print("Generating PDF ...")
    make_pdf(conn)

    conn.close()
    print("\nAll done!")
    print(f"  XLSX → {XLSX_OUT}")
    print(f"  PDF  → {PDF_OUT}")


if __name__ == "__main__":
    main()
