import requests
import sqlite3
import os
import time
from datetime import datetime
from tqdm import tqdm

BASE_DIR = 'downloads/zenodo'
REPOSITORY_ID = 1
REPOSITORY_URL = 'https://zenodo.org'

QUERIES = [
    # REFI-QDA standard
    "qdpx", "qdc",
    # MaxQDA
    "mqda", "mqbac", "mqtc", "mqex", "mqmtr",
    "mx24", "mx22", "mx20", "mx18", "mx12",
    "mx11", "mx5", "mx4", "mx3", "mx2", "m2k",
    "loa", "sea", "mtr", "mod",
    # NVivo
    "nvp", "nvpx",
    # ATLAS.ti
    "atlasproj", "hpr7",
    # QDA Miner
    "ppj", "pprj", "qlt",
    # f4analyse
    "f4p",
    # Quirkos
    "qpd",
    # Broader keyword queries
    "qualitative research",
    "qualitative data analysis",
    "interview study",
    "focus group",
    "thematic analysis",
    "coded interviews",
    "nvivo",
    "maxqda",
    "atlas.ti",
    "dedoose",
    "qualitative coding",
    "grounded theory",
    "content analysis",
    "discourse analysis",
    "narrative analysis",
]

SKIP_EXTENSIONS = [
    '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv',
    '.mp3', '.wav', '.aac', '.flac', '.ogg', '.wma',
    '.m4v', '.m4a', '.webm'
]


def create_safe_dirname(title):
    safe = title.lower()
    safe = ''.join(c if c.isalnum() or c == ' ' else ' ' for c in safe)
    safe = '-'.join(safe.split())
    return safe[:60]


def insert_project(query_string, project_url, title, description, language,
                   doi, upload_date, download_project_folder, version):
    conn = sqlite3.connect('metadata.db')
    c = conn.cursor()
    c.execute('''
        INSERT INTO projects (
            query_string, repository_id, repository_url, project_url,
            version, title, description, language, doi,
            upload_date, download_date,
            download_repository_folder, download_project_folder,
            download_version_folder, download_method
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        query_string, REPOSITORY_ID, REPOSITORY_URL, project_url,
        version, title, description, language, doi,
        upload_date, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'zenodo', download_project_folder, None, 'API-CALL'
    ))
    project_id = c.lastrowid
    conn.commit()
    conn.close()
    return project_id


def insert_file(project_id, file_name, status='SUCCEEDED'):
    ext = os.path.splitext(file_name)[1].lstrip('.').lower() or 'unknown'
    conn = sqlite3.connect('metadata.db')
    c = conn.cursor()
    c.execute('INSERT INTO files (project_id, file_name, file_type, status) VALUES (?, ?, ?, ?)',
              (project_id, file_name, ext, status))
    conn.commit()
    conn.close()


def insert_keywords(project_id, keywords):
    conn = sqlite3.connect('metadata.db')
    c = conn.cursor()
    for kw in keywords:
        if kw:
            c.execute('INSERT INTO keywords (project_id, keyword) VALUES (?, ?)',
                      (project_id, kw))
    conn.commit()
    conn.close()


def insert_person(project_id, name, role):
    conn = sqlite3.connect('metadata.db')
    c = conn.cursor()
    c.execute('INSERT INTO person_role (project_id, name, role) VALUES (?, ?, ?)',
              (project_id, name, role))
    conn.commit()
    conn.close()


def insert_license(project_id, license_str):
    conn = sqlite3.connect('metadata.db')
    c = conn.cursor()
    c.execute('INSERT INTO licenses (project_id, license) VALUES (?, ?)',
              (project_id, license_str))
    conn.commit()
    conn.close()


def project_already_exists(project_url):
    conn = sqlite3.connect('metadata.db')
    c = conn.cursor()
    c.execute('SELECT id FROM projects WHERE project_url = ?', (project_url,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


def download_file(url, filepath):
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code}")
    total = int(response.headers.get('content-length', 0))
    with open(filepath, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, leave=False) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            bar.update(len(chunk))


def search_zenodo(query):
    records = []
    page = 1
    while True:
     url = f"https://zenodo.org/api/records?q={query}&size=100&page={page}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"  Search error: {response.status_code}")
            break
        data = response.json()
        hits = data.get('hits', {}).get('hits', [])
        total = data.get('hits', {}).get('total', 0)
        if not hits:
            break
        records.extend(hits)
        print(f"  Page {page}: {len(hits)} records (total: {total})")
        if len(records) >= total:
            break
        page += 1
        time.sleep(1)
    return records


def process_records(records, query):
    downloaded = 0

    for record in records:
        metadata = record.get('metadata', {})
        title = metadata.get('title', 'unknown')
        record_url = record.get('links', {}).get('html', '')
        doi = metadata.get('doi', '') or record.get('doi', '')
        if doi and not doi.startswith('http'):
            doi = f"https://doi.org/{doi}"
        description = metadata.get('description', '')
        language = metadata.get('language', '')
        version = metadata.get('version', '')
        upload_date = metadata.get('publication_date', '')
        license_id = metadata.get('license', {}).get('id', 'unknown')
        keywords = [kw.get('tag', '') for kw in metadata.get('keywords', [])] if isinstance(metadata.get('keywords', []), list) else []
        creators = metadata.get('creators', [])
        files = record.get('files', [])

        if not record_url:
            continue

        existing_id = project_already_exists(record_url)
        if existing_id:
            print(f"  Skipping (already in DB): {title[:50]}")
            continue

        print(f"\n  Dataset: {title[:60]}")
        print(f"  Files: {len(files)}")

        # Create folder
        dirname = create_safe_dirname(title)
        dirpath = os.path.join(BASE_DIR, dirname)
        os.makedirs(dirpath, exist_ok=True)

        # Insert project
        project_id = insert_project(
            query_string=query,
            project_url=record_url,
            title=title,
            description=description,
            language=language,
            doi=doi,
            upload_date=upload_date,
            download_project_folder=dirname,
            version=version
        )

        # Insert keywords
        insert_keywords(project_id, keywords)

        # Insert authors
        for creator in creators:
            name = creator.get('name', 'UNKNOWN')
            insert_person(project_id, name, 'AUTHOR')

        # Insert license
        insert_license(project_id, license_id)

        # Download files
        for file in files:
            filename = file.get('key', 'unknown')
            file_url = file.get('links', {}).get('self', '')

            # Skip audio/video
            file_ext = os.path.splitext(filename)[1].lower()
            if file_ext in SKIP_EXTENSIONS:
                print(f"    Skipping (audio/video): {filename}")
                insert_file(project_id, filename, 'FAILED_TOO_LARGE')
                continue

            if not file_url:
                insert_file(project_id, filename, 'FAILED_SERVER_UNRESPONSIVE')
                continue

            filepath = os.path.join(dirpath, filename)
            print(f"    Downloading: {filename}")
            try:
                download_file(file_url, filepath)
                insert_file(project_id, filename, 'SUCCEEDED')
                downloaded += 1
            except Exception as e:
                print(f"    Error: {e}")
                insert_file(project_id, filename, 'FAILED_SERVER_UNRESPONSIVE')

            time.sleep(1)

        time.sleep(2)

    return downloaded


def main():
    os.makedirs(BASE_DIR, exist_ok=True)
    total_downloaded = 0
    seen_ids = set()

    for query in QUERIES:
        print(f"\n{'='*50}")
        print(f"Searching Zenodo for: {query}")
        print('='*50)

        records = search_zenodo(query)
        print(f"Found {len(records)} records")

        new_records = [r for r in records if r.get('links', {}).get('html', '') not in seen_ids]
        for r in records:
            seen_ids.add(r.get('links', {}).get('html', ''))

        print(f"New (not yet processed): {len(new_records)}")

        if new_records:
            count = process_records(new_records, query)
            total_downloaded += count
            print(f"Downloaded {count} files for '{query}'")

        time.sleep(3)

    print(f"\n{'='*50}")
    print(f"DONE! Total files downloaded from Zenodo: {total_downloaded}")
    print('='*50)


if __name__ == '__main__':
    main()