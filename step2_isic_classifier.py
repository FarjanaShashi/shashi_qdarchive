"""
Part 2 – Step 2: ISIC Rev. 5 Classifier
=========================================
Classifies every QDA_PROJECT and QD_PROJECT (and their primary data files)
into ISIC Rev. 5 sections and divisions using sentence-transformers.

Strategy:
  Tier 1 – metadata: title + description + keywords (always available)
  Tier 2 – file content: text extracted from .txt .rtf .docx .pdf .csv
            .xlsx .json .xml files on disk (used when available)
  For .qdpx files (ZIP archives) – unzip and extract nested primary files

The classifier embeds the combined text and finds the closest ISIC division
description using cosine similarity.

Results are written to:
  projects.isic_section, projects.isic_division, projects.tags
  files.isic_section, files.isic_division, files.tags
  (files columns only filled for primary data files in QDA/QD projects)

Run:
    cd ~/Desktop/QDArchive
    source venv/bin/activate
    python3 step2_isic_classifier.py
"""

import sqlite3
import os
import re
import json
import zipfile
import tempfile
import warnings
warnings.filterwarnings("ignore")

# ── Text extraction imports ────────────────────────────────────────────────
try:
    from docx import Document as DocxDocument
    HAS_DOCX = True
except ImportError:
    HAS_DOCX = False

try:
    from pdfminer.high_level import extract_text as pdf_extract
    HAS_PDF = True
except ImportError:
    HAS_PDF = False

try:
    from striprtf.striprtf import rtf_to_text
    HAS_RTF = True
except ImportError:
    HAS_RTF = False

try:
    import openpyxl
    HAS_XLSX = True
except ImportError:
    HAS_XLSX = False

import csv

# ── ML imports ─────────────────────────────────────────────────────────────
from sentence_transformers import SentenceTransformer, util

# ── Config ─────────────────────────────────────────────────────────────────
DB_PATH       = "23148157-sq26-classification.db"
DOWNLOADS_DIR = os.path.expanduser("~/Desktop/QDArchive/downloads")
MODEL_NAME    = "all-MiniLM-L6-v2"   # fast, good quality, ~80MB
MAX_CHARS     = 3000   # max chars to take from any single file
MAX_FILE_TEXT = 8000   # max total chars from all files for one project

# Repository ID → folder name mapping
REPO_FOLDERS = {
    2:  "dryad",
    11: "fsd",
}

# Primary data extensions (for file-level classification)
PRIMARY_EXTENSIONS = {
    ".txt", ".rtf", ".docx", ".doc", ".odt", ".pdf",
    ".mp3", ".wav", ".m4a", ".mp4", ".mov", ".avi",
    ".jpg", ".jpeg", ".png", ".tif", ".tiff",
    ".xlsx", ".xls", ".json", ".xml",
}

# QDA extensions (ZIP archives that may contain primary files)
QDA_EXTENSIONS = {
    ".qdpx", ".nvp", ".nvpx", ".atlproj", ".mx22", ".mx24",
    ".hpr7", ".hpr6", ".rqda",
}

# ── ISIC Rev. 5 taxonomy (sections + divisions) ────────────────────────────
# Source: UN Statistics Division ISIC Rev. 5
# Each entry: (section_code, division_code, description)
ISIC_DIVISIONS = [
    # A – Agriculture, Forestry and Fishing
    ("A","01","Crop and animal production, hunting and related service activities"),
    ("A","02","Forestry and logging"),
    ("A","03","Fishing and aquaculture"),
    # B – Mining and Quarrying
    ("B","05","Mining of coal and lignite"),
    ("B","06","Extraction of crude petroleum and natural gas"),
    ("B","07","Mining of metal ores"),
    ("B","08","Other mining and quarrying"),
    ("B","09","Mining support service activities"),
    # C – Manufacturing
    ("C","10","Manufacture of food products"),
    ("C","11","Manufacture of beverages"),
    ("C","12","Manufacture of tobacco products"),
    ("C","13","Manufacture of textiles"),
    ("C","14","Manufacture of wearing apparel"),
    ("C","15","Manufacture of leather and related products"),
    ("C","16","Manufacture of wood and products of wood and cork"),
    ("C","17","Manufacture of paper and paper products"),
    ("C","18","Printing and reproduction of recorded media"),
    ("C","19","Manufacture of coke and refined petroleum products"),
    ("C","20","Manufacture of chemicals and chemical products"),
    ("C","21","Manufacture of basic pharmaceutical products and preparations"),
    ("C","22","Manufacture of rubber and plastics products"),
    ("C","23","Manufacture of other non-metallic mineral products"),
    ("C","24","Manufacture of basic metals"),
    ("C","25","Manufacture of fabricated metal products, except machinery"),
    ("C","26","Manufacture of computer, electronic and optical products"),
    ("C","27","Manufacture of electrical equipment"),
    ("C","28","Manufacture of machinery and equipment n.e.c."),
    ("C","29","Manufacture of motor vehicles, trailers and semi-trailers"),
    ("C","30","Manufacture of other transport equipment"),
    ("C","31","Manufacture of furniture"),
    ("C","32","Other manufacturing"),
    ("C","33","Repair and installation of machinery and equipment"),
    # D – Electricity, Gas, Steam
    ("D","35","Electricity, gas, steam and air conditioning supply"),
    # E – Water Supply
    ("E","36","Water collection, treatment and supply"),
    ("E","37","Sewerage"),
    ("E","38","Waste collection, treatment and disposal activities"),
    ("E","39","Remediation activities and other waste management services"),
    # F – Construction
    ("F","41","Construction of buildings"),
    ("F","42","Civil engineering"),
    ("F","43","Specialised construction activities"),
    # G – Wholesale and Retail Trade
    ("G","45","Wholesale and retail trade and repair of motor vehicles"),
    ("G","46","Wholesale trade, except of motor vehicles"),
    ("G","47","Retail trade, except of motor vehicles"),
    # H – Transportation and Storage
    ("H","49","Land transport and transport via pipelines"),
    ("H","50","Water transport"),
    ("H","51","Air transport"),
    ("H","52","Warehousing and support activities for transportation"),
    ("H","53","Postal and courier activities"),
    # I – Accommodation and Food Service
    ("I","55","Accommodation"),
    ("I","56","Food and beverage service activities"),
    # J – Information and Communication
    ("J","58","Publishing activities"),
    ("J","59","Motion picture, video and television programme production"),
    ("J","60","Programming and broadcasting activities"),
    ("J","61","Telecommunications"),
    ("J","62","Computer programming, consultancy and related activities"),
    ("J","63","Information service activities"),
    # K – Financial and Insurance
    ("K","64","Financial service activities, except insurance and pension funding"),
    ("K","65","Insurance, reinsurance and pension funding"),
    ("K","66","Activities auxiliary to financial service and insurance activities"),
    # L – Real Estate
    ("L","68","Real estate activities"),
    # M – Professional, Scientific and Technical
    ("M","69","Legal and accounting activities"),
    ("M","70","Activities of head offices; management consultancy activities"),
    ("M","71","Architectural and engineering activities; technical testing"),
    ("M","72","Scientific research and development"),
    ("M","73","Advertising and market research"),
    ("M","74","Other professional, scientific and technical activities"),
    ("M","75","Veterinary activities"),
    # N – Administrative and Support Service
    ("N","77","Rental and leasing activities"),
    ("N","78","Employment activities"),
    ("N","79","Travel agency, tour operator and related activities"),
    ("N","80","Security and investigation activities"),
    ("N","81","Services to buildings and landscape activities"),
    ("N","82","Office administrative and business support activities"),
    # O – Public Administration and Defence
    ("O","84","Public administration and defence; compulsory social security"),
    # P – Education
    ("P","85","Education"),
    # Q – Human Health and Social Work
    ("Q","86","Human health activities"),
    ("Q","87","Residential care activities"),
    ("Q","88","Social work activities without accommodation"),
    # R – Arts, Entertainment and Recreation
    ("R","90","Creative, arts and entertainment activities"),
    ("R","91","Libraries, archives, museums and other cultural activities"),
    ("R","92","Gambling and betting activities"),
    ("R","93","Sports activities and amusement and recreation activities"),
    # S – Other Service Activities
    ("S","94","Activities of membership organisations"),
    ("S","95","Repair of computers and personal and household goods"),
    ("S","96","Other personal service activities"),
    # T – Households as Employers
    ("T","97","Activities of households as employers of domestic personnel"),
    ("T","98","Undifferentiated goods and services producing activities of private households"),
    # U – Extraterritorial Organisations
    ("U","99","Activities of extraterritorial organisations and bodies"),
]

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


# ── Text extraction functions ──────────────────────────────────────────────

def extract_txt(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()[:MAX_CHARS]
    except Exception:
        return ""

def extract_rtf(path: str) -> str:
    if not HAS_RTF:
        return extract_txt(path)
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.read()
        return rtf_to_text(raw)[:MAX_CHARS]
    except Exception:
        return extract_txt(path)

def extract_docx(path: str) -> str:
    if not HAS_DOCX:
        return ""
    try:
        doc = DocxDocument(path)
        return " ".join(p.text for p in doc.paragraphs)[:MAX_CHARS]
    except Exception:
        return ""

def extract_pdf(path: str) -> str:
    if not HAS_PDF:
        return ""
    try:
        text = pdf_extract(path, maxpages=5)
        return (text or "")[:MAX_CHARS]
    except Exception:
        return ""

def extract_csv(path: str) -> str:
    try:
        rows = []
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i > 20:
                    break
                rows.append(" ".join(str(c) for c in row))
        return " ".join(rows)[:MAX_CHARS]
    except Exception:
        return ""

def extract_xlsx(path: str) -> str:
    if not HAS_XLSX:
        return ""
    try:
        wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
        parts = []
        for ws in wb.worksheets[:2]:
            for i, row in enumerate(ws.iter_rows(values_only=True)):
                if i > 20:
                    break
                parts.append(" ".join(str(c) for c in row if c is not None))
        return " ".join(parts)[:MAX_CHARS]
    except Exception:
        return ""

def extract_json(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            data = json.load(f)
        return json.dumps(data, ensure_ascii=False)[:MAX_CHARS]
    except Exception:
        return extract_txt(path)

def extract_xml(path: str) -> str:
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            raw = f.read()
        # Strip XML tags, keep text
        text = re.sub(r"<[^>]+>", " ", raw)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:MAX_CHARS]
    except Exception:
        return ""

def extract_file_text(path: str) -> str:
    """Extract text from a file based on its extension."""
    ext = os.path.splitext(path)[1].lower()
    if ext in (".txt", ".md"):
        return extract_txt(path)
    elif ext == ".rtf":
        return extract_rtf(path)
    elif ext in (".docx", ".doc"):
        return extract_docx(path)
    elif ext == ".pdf":
        return extract_pdf(path)
    elif ext in (".csv", ".tsv", ".tab"):
        return extract_csv(path)
    elif ext in (".xlsx", ".xls"):
        return extract_xlsx(path)
    elif ext == ".json":
        return extract_json(path)
    elif ext == ".xml":
        return extract_xml(path)
    return ""

def extract_from_qdpx(path: str) -> str:
    """Unzip a QDA archive and extract text from nested primary files."""
    texts = []
    try:
        with zipfile.ZipFile(path, "r") as zf:
            with tempfile.TemporaryDirectory() as tmpdir:
                zf.extractall(tmpdir)
                for root, _, files in os.walk(tmpdir):
                    for fname in files:
                        ext = os.path.splitext(fname)[1].lower()
                        if ext in {".txt",".rtf",".docx",".pdf",".csv",
                                   ".xlsx",".json",".xml"}:
                            fpath = os.path.join(root, fname)
                            t = extract_file_text(fpath)
                            if t.strip():
                                texts.append(t[:1000])
    except Exception:
        pass
    return " ".join(texts)[:MAX_CHARS]


def get_file_text(path: str) -> str:
    ext = os.path.splitext(path)[1].lower()
    if ext in QDA_EXTENSIONS:
        return extract_from_qdpx(path)
    return extract_file_text(path)


# ── Build text for a project ───────────────────────────────────────────────

def build_project_text(row: dict, keywords: list[str],
                       file_rows: list[dict], repo_folder: str,
                       project_folder: str) -> str:
    """Combine Tier 1 metadata + Tier 2 file content into one string."""
    parts = []

    # Tier 1: metadata
    if row["title"]:
        parts.append(row["title"])
    if row["description"]:
        parts.append(row["description"][:1500])
    if keywords:
        parts.append("Keywords: " + ", ".join(keywords))

    # Tier 2: file content
    file_text_parts = []
    project_dir = os.path.join(DOWNLOADS_DIR, repo_folder, project_folder)

    for fr in file_rows:
        if fr["status"] != "SUCCEEDED":
            continue
        fpath = os.path.join(project_dir, fr["file_name"])
        if not os.path.exists(fpath):
            continue
        t = get_file_text(fpath)
        if t.strip():
            file_text_parts.append(t)
        if sum(len(x) for x in file_text_parts) > MAX_FILE_TEXT:
            break

    if file_text_parts:
        parts.append(" ".join(file_text_parts)[:MAX_FILE_TEXT])

    return " ".join(parts).strip()


def build_file_text(file_row: dict, repo_folder: str,
                    project_folder: str) -> str:
    """Build text for a single primary data file."""
    parts = [file_row["file_name"].replace("_", " ").replace("-", " ")]
    if file_row["status"] == "SUCCEEDED":
        fpath = os.path.join(DOWNLOADS_DIR, repo_folder,
                             project_folder, file_row["file_name"])
        if os.path.exists(fpath):
            t = get_file_text(fpath)
            if t.strip():
                parts.append(t)
    return " ".join(parts)


# ── Classifier ─────────────────────────────────────────────────────────────

def generate_tags(text: str, isic_section: str, isic_division: str,
                  isic_desc: str) -> str:
    """Generate comma-separated tags from text + ISIC label."""
    tags = set()
    # Add ISIC section and division words as tags
    for word in re.findall(r"\b[a-zA-Z]{4,}\b", isic_desc):
        tags.add(word.lower())
    # Add high-frequency content words from the text (simple heuristic)
    words = re.findall(r"\b[a-zA-Z]{5,}\b", text[:2000])
    freq: dict[str, int] = {}
    for w in words:
        w = w.lower()
        freq[w] = freq.get(w, 0) + 1
    top = sorted(freq.items(), key=lambda x: -x[1])[:10]
    for w, _ in top:
        tags.add(w)
    # Remove generic stop words
    stopwords = {"which","their","there","these","those","about","after",
                 "before","other","would","could","should","having",
                 "being","doing","where","while","under","study","data",
                 "research","analysis","using","based","between","through"}
    tags -= stopwords
    return ",".join(sorted(tags))[:500]


class ISICClassifier:
    def __init__(self):
        print(f"Loading sentence-transformer model: {MODEL_NAME} ...")
        self.model = SentenceTransformer(MODEL_NAME)
        self.divisions = ISIC_DIVISIONS
        # Pre-encode all division descriptions
        desc_texts = [desc for _, _, desc in self.divisions]
        print("Encoding ISIC division descriptions ...")
        self.div_embeddings = self.model.encode(
            desc_texts, convert_to_tensor=True, show_progress_bar=False
        )
        print(f"Ready — {len(self.divisions)} ISIC divisions loaded.\n")

    def classify(self, text: str) -> tuple[str, str, str]:
        """
        Returns (isic_section, isic_division_code, division_description).
        Falls back to 'M/72 Scientific research' for empty text.
        """
        text = text.strip()
        if not text:
            return "M", "72", "Scientific research and development"

        embedding = self.model.encode(text[:512], convert_to_tensor=True)
        scores = util.cos_sim(embedding, self.div_embeddings)[0]
        best_idx = int(scores.argmax())
        sec, div, desc = self.divisions[best_idx]
        return sec, div, desc


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    classifier = ISICClassifier()

    # Load projects to classify (QDA + QD only)
    cur.execute("""
        SELECT id, repository_id, title, description, language,
               download_project_folder
        FROM projects
        WHERE type IN ('QDA_PROJECT', 'QD_PROJECT')
        ORDER BY repository_id, id
    """)
    projects = cur.fetchall()
    print(f"Projects to classify: {len(projects)}")

    for i, proj in enumerate(projects, 1):
        pid = proj["id"]
        repo_id = proj["repository_id"]
        repo_folder = REPO_FOLDERS.get(repo_id, str(repo_id))
        project_folder = proj["download_project_folder"] or str(pid)

        # Load keywords
        cur.execute("SELECT keyword FROM keywords WHERE project_id = ?", (pid,))
        keywords = [r[0] for r in cur.fetchall()]

        # Load files
        cur.execute("""
            SELECT id, file_name, file_type, status
            FROM files WHERE project_id = ?
        """, (pid,))
        file_rows = [dict(r) for r in cur.fetchall()]

        # ── Classify project ───────────────────────────────────────────────
        proj_text = build_project_text(
            dict(proj), keywords, file_rows, repo_folder, project_folder
        )
        sec, div, desc = classifier.classify(proj_text)
        tags = generate_tags(proj_text, sec, div, desc)

        cur.execute("""
            UPDATE projects
            SET isic_section = ?, isic_division = ?, tags = ?
            WHERE id = ?
        """, (sec, div, tags, pid))

        # ── Classify primary data files ────────────────────────────────────
        for fr in file_rows:
            ext = os.path.splitext(fr["file_name"])[1].lower()
            if ext not in PRIMARY_EXTENSIONS:
                continue
            file_text = build_file_text(fr, repo_folder, project_folder)
            # Fall back to project text if file text is very short
            combined = file_text if len(file_text) > 100 else proj_text
            f_sec, f_div, f_desc = classifier.classify(combined)
            f_tags = generate_tags(combined, f_sec, f_div, f_desc)
            cur.execute("""
                UPDATE files
                SET isic_section = ?, isic_division = ?, tags = ?
                WHERE id = ?
            """, (f_sec, f_div, f_tags, fr["id"]))

        if i % 10 == 0 or i == len(projects):
            conn.commit()
            print(f"  [{i}/{len(projects)}] repo={repo_id} "
                  f"pid={pid} → {sec}/{div} ({desc[:40]}...)")

    conn.commit()

    # ── Summary ────────────────────────────────────────────────────────────
    print("\n=== ISIC Section distribution (QDA + QD projects) ===")
    cur.execute("""
        SELECT isic_section, COUNT(*) as n
        FROM projects
        WHERE type IN ('QDA_PROJECT','QD_PROJECT')
          AND isic_section IS NOT NULL
        GROUP BY isic_section
        ORDER BY n DESC
    """)
    for row in cur.fetchall():
        sec = row[0]
        name = SECTION_NAMES.get(sec, "?")
        print(f"  {sec} – {name:<45} {row[1]:>4}")

    print("\n=== Top ISIC Divisions ===")
    cur.execute("""
        SELECT isic_division, COUNT(*) as n
        FROM projects
        WHERE type IN ('QDA_PROJECT','QD_PROJECT')
          AND isic_division IS NOT NULL
        GROUP BY isic_division
        ORDER BY n DESC
        LIMIT 15
    """)
    for row in cur.fetchall():
        div = row[0]
        desc = next((d for s,d2,d in ISIC_DIVISIONS if d2==div), "?")
        print(f"  Division {div} – {desc[:50]:<50} {row[1]:>4}")

    conn.close()
    print(f"\nDone! Results saved to {DB_PATH}")


if __name__ == "__main__":
    main()
