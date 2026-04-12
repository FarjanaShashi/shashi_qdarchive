import requests
import sqlite3
import os
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from tqdm import tqdm

BASE_DIR = 'downloads/fsd'
REPOSITORY_ID = 11
REPOSITORY_URL = 'https://www.fsd.tuni.fi'
OAI_URL = 'https://services.fsd.tuni.fi/v0/oai'

NS = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
}

SKIP_EXTENSIONS = [
    '.mp4', '.mov', '.avi', '.mkv', '.wmv', '.flv',
    '.mp3', '.wav', '.aac', '.flac', '.ogg', '.wma',
    '.m4v', '.m4a', '.webm'
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
        'fsd', download_project_folder, None, 'API-CALL'
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
                      (project_id, kw.strip()))
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


def get_access_class_from_page(fsd_id):
    url = f"https://services.fsd.tuni.fi/catalogue/{fsd_id}?lang=en"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return 'UNKNOWN', 'unknown'

        text = r.text

        if 'CC BY 4.0' in text or 'openly available for all' in text.lower() or 'class a' in text.lower():
            return 'A', 'CC BY 4.0'
        elif 'available for research, teaching and study' in text.lower():
            return 'B', 'restricted'
        elif 'available for research only' in text.lower():
            return 'C', 'restricted'
        elif 'available only by permission' in text.lower():
            return 'D', 'restricted'

        if '"accessClass":"A"' in text or '"accessClass": "A"' in text:
            return 'A', 'CC BY 4.0'
        elif '"accessClass":"B"' in text or '"accessClass": "B"' in text:
            return 'B', 'restricted'
        elif '"accessClass":"C"' in text or '"accessClass": "C"' in text:
            return 'C', 'restricted'
        elif '"accessClass":"D"' in text or '"accessClass": "D"' in text:
            return 'D', 'restricted'

        return 'UNKNOWN', 'unknown'

    except Exception as e:
        print(f"    Could not fetch catalogue page for {fsd_id}: {e}")
        return 'UNKNOWN', 'unknown'


def harvest_all_records():
    print("Harvesting metadata from FSD via OAI-PMH...")
    records = []
    resumption_token = None
    page = 1

    while True:
        if resumption_token:
            params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}
        else:
            params = {'verb': 'ListRecords', 'metadataPrefix': 'oai_dc'}

        response = requests.get(OAI_URL, params=params)

        if response.status_code != 200:
            print(f"  OAI-PMH error: {response.status_code}")
            break

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as e:
            print(f"  XML parse error: {e}")
            print(f"  Response preview: {response.content[:500]}")
            break

        record_elements = root.findall('.//oai:record', NS)
        records.extend(record_elements)
        print(f"  Page {page}: harvested {len(record_elements)} records (total: {len(records)})")

        token_el = root.find('.//oai:resumptionToken', NS)
        if token_el is not None and token_el.text:
            resumption_token = token_el.text
            page += 1
            time.sleep(2)
        else:
            break

    print(f"Total records harvested: {len(records)}")
    return records


def parse_record(record):
    header = record.find('oai:header', NS)
    metadata = record.find('oai:metadata', NS)

    if header is None or metadata is None:
        return None

    if header.get('status', '') == 'deleted':
        return None

    dc = metadata.find('oai_dc:dc', NS)
    if dc is None:
        return None

    def get_all(tag):
        return [el.text for el in dc.findall(f'dc:{tag}', NS) if el.text]

    title = get_all('title')
    description = get_all('description')
    creator = get_all('creator')
    subject = get_all('subject')
    date = get_all('date')
    language = get_all('language')
    identifier_list = get_all('identifier')

    fsd_id = ''
    project_url = ''
    doi = ''
    for ident in identifier_list:
        if 'services.fsd' in ident and 'catalogue' in ident:
            project_url = ident
        elif ident.startswith('https://doi.org') or ident.startswith('http://doi.org'):
            doi = ident
        elif ident.startswith('FSD') and len(ident) < 15 and ' ' not in ident and '/' not in ident:
            fsd_id = ident

    if not fsd_id:
        for ident in identifier_list:
            if ident.startswith('FSD') and len(ident) < 15:
                fsd_id = ident

    if not project_url and fsd_id:
        project_url = f"https://services.fsd.tuni.fi/catalogue/{fsd_id}?lang=en"

    return {
        'fsd_id': fsd_id,
        'title': title[0] if title else 'unknown',
        'description': ' '.join(description[:2]),
        'creators': creator,
        'keywords': subject,
        'date': date[0] if date else '',
        'language': language[0] if language else '',
        'project_url': project_url,
        'doi': doi,
    }


def try_download_class_a(fsd_id, dirpath, project_id):
    zip_filename = f"{fsd_id}.zip"
    zip_path = os.path.join(dirpath, zip_filename)

    if os.path.exists(zip_path):
        print(f"    Already downloaded: {zip_filename}")
        insert_file(project_id, zip_filename, 'SUCCEEDED')
        return True

    print(f"    Attempting download: {zip_filename}")
    try:
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0'})

        # Step 1: Visit terms page to get session cookie
        terms_url = (
            f"https://services.fsd.tuni.fi/catalogue/{fsd_id}"
            f"?tab=download&lang=en&study_language=en&accept_terms=true"
        )
        session.get(terms_url, timeout=15)

        # Step 2: Hit download endpoint to get DIP redirect
        download_url = "https://services.fsd.tuni.fi/catalogue/download?lang=en&study_language=en"
        r2 = session.get(download_url, timeout=30)

        # Step 3: Extract DIP URL from meta refresh redirect
        dip_match = re.search(r'url=(https://services\.fsd\.tuni\.fi/catalogue/dip[^"\'>\s]+)', r2.text)
        if not dip_match:
            print(f"    No DIP URL found - login may be required")
            insert_file(project_id, zip_filename, 'FAILED_LOGIN_REQUIRED')
            return False

        dip_url = dip_match.group(1).replace('&amp;', '&')
        print(f"    DIP URL found, downloading...")

        # Step 4: Download the actual zip
        r3 = session.get(dip_url, stream=True, timeout=120)
        ct = r3.headers.get('content-type', '')

        if r3.status_code == 200 and ('zip' in ct or 'octet' in ct or 'stream' in ct):
            total = int(r3.headers.get('content-length', 0))
            with open(zip_path, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True, leave=False) as bar:
                for chunk in r3.iter_content(chunk_size=8192):
                    f.write(chunk)
                    bar.update(len(chunk))
            insert_file(project_id, zip_filename, 'SUCCEEDED')
            print(f"    Downloaded: {zip_filename} ({total} bytes)")
            return True
        else:
            print(f"    Unexpected response: {r3.status_code} {ct}")
            insert_file(project_id, zip_filename, 'FAILED_LOGIN_REQUIRED')
            return False

    except Exception as e:
        print(f"    Error: {e}")
        insert_file(project_id, zip_filename, 'FAILED_SERVER_UNRESPONSIVE')
        return False


def process_records(records):
    downloaded = 0
    metadata_saved = 0
    class_a_count = 0

    for i, record_el in enumerate(records):
        record = parse_record(record_el)
        if not record:
            continue

        project_url = record['project_url']
        if not project_url:
            continue

        if project_already_exists(project_url):
            continue

        fsd_id = record['fsd_id']
        title = record['title']

        print(f"\n  [{i+1}/{len(records)}] {title[:60]}")
        print(f"    Checking access class for {fsd_id}...")
        availability, license_str = get_access_class_from_page(fsd_id)
        print(f"    Access class: {availability}")

        if availability == 'A':
            class_a_count += 1

        folder_name = fsd_id if fsd_id else 'unknown'
        dirpath = os.path.join(BASE_DIR, folder_name)
        os.makedirs(dirpath, exist_ok=True)

        project_id = insert_project(
            query_string='OAI-PMH harvest',
            project_url=project_url,
            title=title,
            description=record['description'],
            language=record['language'],
            doi=record['doi'],
            upload_date=record['date'],
            download_project_folder=folder_name,
            version=None
        )

        insert_keywords(project_id, record['keywords'])

        for creator in record['creators']:
            insert_person(project_id, creator, 'AUTHOR')

        insert_license(project_id, license_str)

        metadata_saved += 1

        if availability == 'A' and fsd_id:
            success = try_download_class_a(fsd_id, dirpath, project_id)
            if success:
                downloaded += 1
        else:
            print(f"    Metadata only (Class {availability})")

        time.sleep(1.5)

    return downloaded, metadata_saved, class_a_count


def main():
    os.makedirs(BASE_DIR, exist_ok=True)
    print("Starting FSD downloader...")
    print("Strategy: Harvest ALL metadata via OAI-PMH, download Class A only")
    print()

    records = harvest_all_records()
    print(f"\nProcessing {len(records)} records...")
    downloaded, metadata_saved, class_a_count = process_records(records)

    print(f"\n{'='*50}")
    print(f"DONE!")
    print(f"Metadata saved: {metadata_saved}")
    print(f"Class A datasets found: {class_a_count}")
    print(f"Files downloaded: {downloaded}")
    print('='*50)


if __name__ == '__main__':
    main()