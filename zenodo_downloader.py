import requests
import sqlite3
import os
from datetime import datetime
from tqdm import tqdm

# QDA file extensions we are looking for
QDA_EXTENSIONS = ['.qdpx', '.nvpx', '.nvp', '.atlproj', '.mx', '.mx20', '.mex', '.qda']

# Where to save files
BASE_DIR = 'downloads/zenodo'

def create_safe_dirname(title):
    """Turn a dataset title into a safe folder name"""
    safe = title.lower()
    safe = ''.join(c if c.isalnum() or c == ' ' else ' ' for c in safe)
    safe = '-'.join(safe.split())
    return safe[:60]  # max 60 characters

def add_to_database(url, local_dir, local_file, license, uploader_name, uploader_email):
    """Add one row to the database"""
    conn = sqlite3.connect('metadata.db')
    c = conn.cursor()
    c.execute('''INSERT INTO downloads VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
        url,
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        local_dir,
        local_file,
        'Zenodo',
        license,
        uploader_name,
        uploader_email
    ))
    conn.commit()
    conn.close()

def download_file(url, filepath):
    """Download a single file"""
    response = requests.get(url, stream=True)
    total = int(response.headers.get('content-length', 0))
    with open(filepath, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True) as bar:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)
            bar.update(len(chunk))

def search_zenodo():
    """Search Zenodo for QDA files"""
    print("Searching Zenodo for QDA files...")
    
    # Search terms
    queries = ['qdpx', 'qualitative data analysis nvivo', 'atlas.ti qualitative', 'maxqda qualitative']
    
    found_records = []
    
    for query in queries:
        print(f"\nSearching for: {query}")
        url = f"https://zenodo.org/api/records?q={query}&size=25&status=published"
        response = requests.get(url)
        
        if response.status_code != 200:
            print(f"Error searching Zenodo: {response.status_code}")
            continue
            
        records = response.json().get('hits', {}).get('hits', [])
        print(f"Found {len(records)} records")
        found_records.extend(records)
    
    return found_records

def process_records(records):
    """Go through each record and download QDA files"""
    downloaded = 0
    
    for record in records:
        title = record.get('metadata', {}).get('title', 'unknown')
        files = record.get('files', [])
        record_url = record.get('links', {}).get('html', '')
        license = record.get('metadata', {}).get('license', {}).get('id', 'unknown')
        
        # Get uploader info
        creators = record.get('metadata', {}).get('creators', [])
        uploader_name = creators[0].get('name', '') if creators else ''
        uploader_email = ''
        
        # Check if any file is a QDA file
        qda_files = [f for f in files if any(f['key'].lower().endswith(ext) for ext in QDA_EXTENSIONS)]
        
        if not qda_files:
            continue
        
        print(f"\nFound QDA dataset: {title}")
        
        # Create folder for this dataset
        dirname = create_safe_dirname(title)
        dirpath = os.path.join(BASE_DIR, dirname)
        os.makedirs(dirpath, exist_ok=True)
        
        # Download ALL files in this dataset
        for file in files:
            filename = file['key']
            file_url = file['links']['self']
            filepath = os.path.join(dirpath, filename)
            
            print(f"  Downloading: {filename}")
            try:
                download_file(file_url, filepath)
                add_to_database(record_url, dirname, filename, license, uploader_name, uploader_email)
                downloaded += 1
            except Exception as e:
                print(f"  Error downloading {filename}: {e}")
    
    return downloaded

def main():
    records = search_zenodo()
    print(f"\nTotal records found: {len(records)}")
    downloaded = process_records(records)
    print(f"\nDone! Downloaded {downloaded} files total.")

if __name__ == '__main__':
    main()