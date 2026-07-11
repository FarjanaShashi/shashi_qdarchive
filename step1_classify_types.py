"""
Part 2 – Step 1: Schema Migration + PROJECT_TYPE Classification
===============================================================
1. Copies 23148157-seeding.db → 23148157-sq26-classification.db
2. Adds classification columns to projects and files tables
3. Labels every project with one of:
       QDA_PROJECT   – has ≥1 known QDA file extension
       QD_PROJECT    – has ≥1 primary data file (no QDA files)
       OTHER_PROJECT – has other data files (no QDA or primary)
       NOT_A_PROJECT – no useful files found

Run:
    cd ~/Desktop/QDArchive
    source venv/bin/activate
    python3 step1_classify_types.py
"""

import sqlite3
import shutil
import os

SRC_DB  = "23148157-seeding.db"
DEST_DB = "23148157-sq26-classification.db"

# ── File extension sets ────────────────────────────────────────────────────

# QDA tool file formats (analysis data files)
QDA_EXTENSIONS = {
    # REFI-QDA universal exchange format
    ".qdpx",
    # NVivo
    ".nvp", ".nvpx", ".nvcx",
    # ATLAS.ti
    ".atlproj", ".hpr7", ".hpr6", ".hpr5",
    # MAXQDA
    ".mx22", ".mx24", ".mxd",
    # Dedoose (exported bundles)
    ".dedoose",
    # f4analyse
    ".f4a",
    # HyperRESEARCH
    ".hrx",
    # Quirkos
    ".qrk",
    # Transana
    ".tra",
    # RQDA (R package)
    ".rqda",
    # Qualrus
    ".qlr",
    # QDAMiner
    ".ppj",
}

# Primary data files (qualitative source material)
PRIMARY_EXTENSIONS = {
    # Text / documents
    ".txt", ".rtf", ".docx", ".doc", ".odt",
    # PDF
    ".pdf",
    # Audio
    ".mp3", ".wav", ".m4a", ".aac", ".flac", ".ogg", ".wma",
    # Video
    ".mp4", ".mov", ".avi", ".mkv", ".wmv", ".webm",
    # Images (where qualitative coding applies)
    ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".bmp",
    # Spreadsheets used as interview data logs
    ".xlsx", ".xls", ".ods",
    # Structured text
    ".json", ".xml",
}

# Other valid data files (not QDA, not primary, but real data)
OTHER_EXTENSIONS = {
    ".csv", ".tsv", ".tab",
    ".sav", ".dta", ".por",        # SPSS, Stata
    ".rdata", ".rds",              # R
    ".mat",                        # MATLAB
    ".h5", ".hdf5",               # HDF5
    ".zip", ".tar", ".gz", ".7z", # archives
    ".html", ".htm",
    ".md",
}


def get_project_type(extensions: set[str]) -> str:
    """Apply the cascade rule to a set of lowercased extensions."""
    if extensions & QDA_EXTENSIONS:
        return "QDA_PROJECT"
    if extensions & PRIMARY_EXTENSIONS:
        return "QD_PROJECT"
    if extensions & OTHER_EXTENSIONS:
        return "OTHER_PROJECT"
    return "NOT_A_PROJECT"


def main():
    # ── 1. Copy database ───────────────────────────────────────────────────
    if not os.path.exists(SRC_DB):
        raise FileNotFoundError(f"Source database not found: {SRC_DB}")

    print(f"Copying {SRC_DB} → {DEST_DB} ...")
    shutil.copy2(SRC_DB, DEST_DB)
    print("Copy done.")

    conn = sqlite3.connect(DEST_DB)
    cur  = conn.cursor()

    # ── 2. Schema migration ────────────────────────────────────────────────
    print("Adding classification columns ...")

    existing_project_cols = {
        row[1] for row in cur.execute("PRAGMA table_info(projects)")
    }
    existing_file_cols = {
        row[1] for row in cur.execute("PRAGMA table_info(files)")
    }

    project_additions = [
        ("type",         "TEXT"),
        ("isic_section", "TEXT"),
        ("isic_division","TEXT"),
        ("tags",         "TEXT"),
    ]
    file_additions = [
        ("isic_section", "TEXT"),
        ("isic_division","TEXT"),
        ("tags",         "TEXT"),
    ]

    for col, dtype in project_additions:
        if col not in existing_project_cols:
            cur.execute(f"ALTER TABLE projects ADD COLUMN {col} {dtype}")
            print(f"  projects.{col} added")
        else:
            print(f"  projects.{col} already exists – skipping")

    for col, dtype in file_additions:
        if col not in existing_file_cols:
            cur.execute(f"ALTER TABLE files ADD COLUMN {col} {dtype}")
            print(f"  files.{col} added")
        else:
            print(f"  files.{col} already exists – skipping")

    conn.commit()

    # ── 3. Load all file extensions per project ───────────────────────────
    print("\nLoading file extensions per project ...")
    cur.execute("""
        SELECT project_id, LOWER(file_type)
        FROM files
        WHERE status = 'SUCCEEDED'
    """)
    rows = cur.fetchall()

    project_extensions: dict[int, set[str]] = {}
    for project_id, file_type in rows:
        if file_type:
            ext = file_type if file_type.startswith(".") else f".{file_type}"
            project_extensions.setdefault(project_id, set()).add(ext)

    # ── 4. Assign PROJECT_TYPE ─────────────────────────────────────────────
    print("Classifying projects ...")
    cur.execute("SELECT id FROM projects")
    all_project_ids = [row[0] for row in cur.fetchall()]

    counts = {"QDA_PROJECT": 0, "QD_PROJECT": 0,
              "OTHER_PROJECT": 0, "NOT_A_PROJECT": 0}

    for pid in all_project_ids:
        exts = project_extensions.get(pid, set())
        ptype = get_project_type(exts)
        cur.execute("UPDATE projects SET type = ? WHERE id = ?", (ptype, pid))
        counts[ptype] += 1

    conn.commit()
    conn.close()

    # ── 5. Summary ─────────────────────────────────────────────────────────
    print("\n=== PROJECT_TYPE distribution ===")
    total = sum(counts.values())
    for ptype, n in counts.items():
        pct = 100 * n / total if total else 0
        print(f"  {ptype:<20} {n:>5}  ({pct:.1f}%)")
    print(f"  {'TOTAL':<20} {total:>5}")
    print(f"\nDone! Classification database saved as: {DEST_DB}")


if __name__ == "__main__":
    main()
