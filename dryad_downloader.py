import requests
import sqlite3
import os
import time
from datetime import datetime
from tqdm import tqdm
from urllib.parse import quote

BASE_URL = "https://datadryad.org/api/v2"
BASE_DIR = "downloads/dryad"
REPOSITORY_ID = 2
REPOSITORY_URL = "https://datadryad.org"

# Dryad API credentials
from config import DRYAD_CLIENT_ID, DRYAD_CLIENT_SECRET
CLIENT_ID = DRYAD_CLIENT_ID
CLIENT_SECRET = DRYAD_CLIENT_SECRET

_token = None

def get_token():
    global _token
    response = requests.post(
        "https://datadryad.org/oauth/token",
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials"
        },
        headers={"Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"}
    )
    if response.status_code != 200:
        raise Exception(f"Failed to get token: {response.status_code} {response.text}")
    _token = response.json()["access_token"]
    print("Got new token!")
    return _token

def get_headers():
    global _token
    if not _token:
        get_token()
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {_token}",
        "Content-Type": "application/json"
    }

QUERIES = [
    # REFI-QDA standard
    "qdpx", "qdc",
    # MaxQDA
    "mqda", "mqbac", "mqtc", "mqex", "mqmtr",
    "mx24", "mx24bac", "mc24", "mex24",
    "mx22", "mex22", "mx20", "mx18", "mx12",
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
    "qdacity",
    "qualitative coding",
    "grounded theory",
    "content analysis",
    "discourse analysis",
    "narrative analysis",
]


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
        'dryad', download_project_folder, None, 'API-CALL'
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


def wait_if_rate_limited(response):
    if response.status_code == 429:
        print("    Rate limited! Waiting 180 seconds...")
        time.sleep(180)
        return True
    return False


def download_file(url, filepath):
    response = requests.get(url, stream=True, headers=get_headers(), allow_redirects=True)
    if wait_if_rate_limited(response):
        response = requests.get(url, stream=True, headers=get_headers(), allow_redirects=True)
    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code}")
    total = int(response.headers.get('content-length', 0))
    with open(filepath, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, leave=False) as bar:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            bar.update(len(chunk))


def get_files_for_dataset(dataset):
    """Get files using the version href from the dataset's _links"""
    links = dataset.get('_links', {})
    version_href = links.get('stash:version', {}).get('href', '')

    if not version_href:
        print(f"    No version href in links: {list(links.keys())}")
        return []

    files_url = f"https://datadryad.org{version_href}/files"
    print(f"    Fetching files: {files_url}")

    response = requests.get(files_url, headers=get_headers())
    print(f"    Status: {response.status_code}")

    if wait_if_rate_limited(response):
        response = requests.get(files_url, headers=get_headers())

    if response.status_code != 200:
        return []

    data = response.json()
    files = data.get('_embedded', {}).get('stash:files', [])
    print(f"    Files found: {len(files)}")
    if files:
        print(f"    First file keys: {list(files[0].keys())}")
        print(f"    First file _links: {list(files[0].get('_links', {}).keys())}")
    return files


def search_dryad(query):
    datasets = []
    page = 1
    while True:
        url = f"{BASE_URL}/search?q={quote(query)}&per_page=100&page={page}"
        response = requests.get(url, headers=get_headers())
        if wait_if_rate_limited(response):
            response = requests.get(url, headers=get_headers())
        if response.status_code != 200:
            print(f"  Search error: {response.status_code}")
            break
        data = response.json()
        total = data.get('total', 0)
        items = data.get('_embedded', {}).get('stash:datasets', [])
        if not items:
            break
        datasets.extend(items)
        print(f"  Page {page}: {len(items)} datasets (total: {total})")
        if len(datasets) >= total:
            break
        page += 1
        time.sleep(2)
    return datasets


def process_datasets(datasets, query):
    downloaded_files = 0

    for dataset in datasets:
        title = dataset.get('title', 'unknown')
        doi = dataset.get('identifier', '')
        project_url = f"https://datadryad.org/dataset/{quote(doi, safe='')}" if doi else ''
        description = dataset.get('abstract', '')
        language = dataset.get('language', '')
        version = str(dataset.get('versionNumber', '')) or None
        upload_date = dataset.get('publicationDate', '')
        keywords = dataset.get('keywords', [])
        authors = dataset.get('authors', [])

        if not doi:
            continue

        existing_id = project_already_exists(project_url)
        if existing_id:
            print(f"  Skipping (already in DB): {title[:50]}")
            continue

        print(f"\n  Dataset: {title[:60]}")

        doi_suffix = doi.split('/')[-1] if '/' in doi else doi
        dirpath = os.path.join(BASE_DIR, doi_suffix)
        os.makedirs(dirpath, exist_ok=True)

        project_id = insert_project(
            query_string=query,
            project_url=project_url,
            title=title,
            description=description,
            language=language,
            doi=f"https://doi.org/{doi}" if not doi.startswith('http') else doi,
            upload_date=upload_date,
            download_project_folder=doi_suffix,
            version=version
        )

        insert_keywords(project_id, keywords)

        for author in authors:
            first = author.get('firstName', '')
            last = author.get('lastName', '')
            name = f"{first} {last}".strip() or 'UNKNOWN'
            insert_person(project_id, name, 'AUTHOR')

        insert_license(project_id, 'CC0')

        # Get files using version href
        files = get_files_for_dataset(dataset)

        if files:
            for file in files:
                filename = file.get('path') or file.get('filename') or file.get('name') or 'unknown'
                # Skip audio and video files
                SKIP_EXTENSIONS = [
                    '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv',
                    '.mp3', '.wav', '.aac', '.flac', '.ogg', '.wma',
                    '.m4v', '.m4a', '.webm'
                ]
                file_ext = os.path.splitext(filename)[1].lower()
                if file_ext in SKIP_EXTENSIONS:
                    print(f"    Skipping (audio/video): {filename}")
                    insert_file(project_id, filename, 'FAILED_TOO_LARGE')
                    continue
                # Get download URL from file's _links
                file_links = file.get('_links', {})
                download_href = file_links.get('stash:download', {}).get('href', '')
                if not download_href:
                    print(f"    No download link for: {filename}")
                    continue
                file_url = f"https://datadryad.org{download_href}"
                filepath = os.path.join(dirpath, filename)
                print(f"    Downloading: {filename}")
                try:
                    download_file(file_url, filepath)
                    insert_file(project_id, filename, 'SUCCEEDED')
                    downloaded_files += 1
                except Exception as e:
                    print(f"    Error: {e}")
                    if '429' in str(e):
                        insert_file(project_id, filename, 'FAILED_SERVER_UNRESPONSIVE')
                    else:
                        insert_file(project_id, filename, 'FAILED_SERVER_UNRESPONSIVE')
                time.sleep(30)
        else:
            print(f"    No files found for this dataset, skipping download.")

        time.sleep(10)

    return downloaded_files


def main():
    os.makedirs(BASE_DIR, exist_ok=True)
    total_downloaded = 0
    seen_dois = set()

    # Get initial token
    get_token()

    for query in QUERIES:
        print(f"\n{'='*50}")
        print(f"Searching Dryad for: {query}")
        print('='*50)

        datasets = search_dryad(query)
        print(f"Found {len(datasets)} datasets")

        new_datasets = [d for d in datasets if d.get('identifier') not in seen_dois]
        for d in datasets:
            seen_dois.add(d.get('identifier'))

        print(f"New (not yet processed): {len(new_datasets)}")

        if new_datasets:
            count = process_datasets(new_datasets, query)
            total_downloaded += count
            print(f"Downloaded {count} files for '{query}'")

        time.sleep(30)

    print(f"\n{'='*50}")
    print(f"DONE! Total files downloaded from Dryad: {total_downloaded}")
    print('='*50)


if __name__ == '__main__':
    main()
