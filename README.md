# QDArchive Seeding Project

**Student:** Farjana Islam Shashi  
**Student ID:** 23148157  
**GitHub:** FarjanaShashi  
**University:** FAU Erlangen-Nürnberg  
**Supervisor:** Prof. Dr. Dirk Riehle  
**Course:** Seminar Project (15 ECTS)  
**Deadline:** April 17, 2026  

---

## Overview

This project seeds the [QDArchive](https://qdarchive.org) repository by harvesting qualitative data analysis (QDA) files and metadata from assigned data repositories. The goal is to collect datasets that contain QDA project files (e.g. NVivo, ATLAS.ti, MAXQDA) and store their metadata in a structured SQLite database.

**Assigned repositories:**
- **Dryad** (Repository ID: 2) — https://datadryad.org
- **FSD / Finnish Social Science Data Archive** (Repository ID: 11) — https://www.fsd.tuni.fi

---

## Repository Structure

```
.
├── 23148157-seeding.db       # SQLite metadata database (submission file)
├── metadata.db               # Working copy of the database
├── setup_database.py         # Creates the SQLite schema
├── dryad_downloader.py       # Downloader for Dryad
├── fsd_downloader.py         # Downloader for FSD
├── zenodo_downloader.py      # Downloader for Zenodo (prior work)
├── qdr_downloader.py         # Downloader for QDR (prior work)
├── dataverse_no_downloader.py# Downloader for DataverseNO (prior work)
├── config.py                 # API credentials (excluded from version control)
└── downloads/                # Downloaded dataset files
    ├── dryad/                # Dryad datasets
    └── fsd/                  # FSD datasets
```

---

## Database Schema

The metadata is stored in a 5-table SQLite database following the QDArchive schema:

| Table | Description |
|---|---|
| `projects` | One row per downloaded project/dataset |
| `files` | One row per file within a project |
| `keywords` | Keywords associated with each project |
| `person_role` | Authors/contributors and their roles |
| `licenses` | License information per project |

### File Download Status Values
- `SUCCEEDED` — File downloaded successfully
- `FAILED_LOGIN_REQUIRED` — File requires authentication
- `FAILED_SERVER_UNRESPONSIVE` — Server error or rate limit
- `FAILED_TOO_LARGE` — File skipped (audio/video or >200MB)

---

## Results Summary

| Repository | Projects | Files Downloaded |
|---|---|---|
| Dryad | 49 | 707 |
| FSD | 2186 | 129 |
| **Total** | **2235** | **836** |

---

## How to Run

### Prerequisites
```bash
python3 -m venv venv
source venv/bin/activate
pip install requests tqdm
```

### Setup Database
```bash
python3 setup_database.py
```

### Run Downloaders
```bash
# Dryad (requires API credentials in config.py)
caffeinate python3 dryad_downloader.py

# FSD (no credentials required for Class A datasets)
python3 fsd_downloader.py
```

### API Credentials
Dryad requires OAuth2 client credentials. Store them in `config.py`:
```python
DRYAD_CLIENT_ID = "your_client_id"
DRYAD_CLIENT_SECRET = "your_client_secret"
```
`config.py` is excluded from version control but included in the repository submission as approved by the supervisor.

---

## Downloader Details

### Dryad
- Uses OAuth2 client credentials flow
- Searches using QDA-related keywords (NVivo, MAXQDA, ATLAS.ti, qdpx, etc.)
- Skips audio/video files and files over 200MB
- Handles rate limiting with automatic retry and delays

### FSD (Finnish Social Science Data Archive)
- Harvests all metadata via OAI-PMH (`https://services.fsd.tuni.fi/v0/oai`)
- Downloads only **Class A (CC BY 4.0)** datasets — openly available without login
- Uses a 3-step download flow: visit terms page → get DIP redirect URL → download zip
- 2186 metadata records harvested, 129 Class A datasets downloaded
