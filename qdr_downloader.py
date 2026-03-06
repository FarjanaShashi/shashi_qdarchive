import requests
import sqlite3
import os
from datetime import datetime
from tqdm import tqdm

# Your QDR API token
API_TOKEN = "fcf71cab-f0bb-44e7-aa9b-bb1e34a00cf5"
BASE_URL = "https://data.qdr.syr.edu"
BASE_DIR = "downloads/qdr"

QDA_EXTENSIONS = ['.qdpx', '.nvpx', '.nvp', '.atlproj', '.mx', '.mx20', '.mex', '.qda']

def create_safe_dirname(title):
    safe = title.lower()
    safe = ''.join(c if c.isalnum() or c == ' ' else ' ' for c in safe)
    safe = '-'.join(safe.split())
    return safe[:60]

def add_to_database(url, local_dir, local_file, license, uploader_name, uploader_email):
    conn = sqlite3.connect('metadata.db')
    c = conn.cursor()
    c.execute('''INSERT INTO downloads VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
        url,
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        local_dir,
        local_file,
        'QDR',
        license,
        uploader_name,
        uploader_email
    ))
    conn.commit()
    conn.close()

def download_file(url, filepath):
    headers = {"X-Dataverse-key": API_TOKEN}
    response = requests.get(url, headers=headers, stream=True)
    total = int(response.headers.get('content-length', 0))
    with open(filepath, 'wb') as f, tqdm(total=total, unit='B', unit_scale=True) as bar:
        for chunk in response.iter_content(chunk_size=1024):
            f.write(chunk)
            bar.update(len(chunk))

def search_qdr():
    print("Searching QDR for QDA files...")
    headers = {"X-Dataverse-key": API_TOKEN}
    
    queries = ['qdpx', 'qualitative data analysis', 'nvivo', 'atlas.ti', 'maxqda']
    all_datasets = []
    
    for query in queries:
        print(f"\nSearching for: {query}")
        url = f"{BASE_URL}/api/search?q={query}&type=dataset&per_page=25"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            continue
        
        data = response.json()
        items = data.get('data', {}).get('items', [])
        print(f"Found {len(items)} datasets")
        all_datasets.extend(items)
    
    return all_datasets

def process_datasets(datasets):
    downloaded = 0
    
    for dataset in datasets:
        title = dataset.get('name', 'unknown')
        global_id = dataset.get('global_id', '')
        
        if not global_id:
            continue
        
        # Get files for this dataset
        headers = {"X-Dataverse-key": API_TOKEN}
        files_url = f"{BASE_URL}/api/datasets/:persistentId/versions/:latest/files?persistentId={global_id}"
        response = requests.get(files_url, headers=headers)
        
        if response.status_code != 200:
            continue
        
        files = response.json().get('data', [])
        
        # Check if any QDA files exist
        qda_files = [f for f in files if any(
            f.get('dataFile', {}).get('filename', '').lower().endswith(ext)
            for ext in QDA_EXTENSIONS
        )]
        
        if not qda_files:
            continue
        
        print(f"\nFound QDA dataset: {title}")
        
        # Create folder
        dirname = create_safe_dirname(title)
        dirpath = os.path.join(BASE_DIR, dirname)
        os.makedirs(dirpath, exist_ok=True)
        
        # Get license and author info
        dataset_url = f"{BASE_URL}/dataset.xhtml?persistentId={global_id}"
        license = dataset.get('license', 'QDR Standard Access')
        authors = dataset.get('authors', [])
        uploader_name = authors[0] if authors else ''
        
        # Download all files
        for file in files:
            file_data = file.get('dataFile', {})
            filename = file_data.get('filename', '')
            file_id = file_data.get('id', '')
            
            if not filename or not file_id:
                continue
            
            filepath = os.path.join(dirpath, filename)
            file_url = f"{BASE_URL}/api/access/datafile/{file_id}"
            
            print(f"  Downloading: {filename}")
            try:
                download_file(file_url, filepath)
                add_to_database(dataset_url, dirname, filename, license, uploader_name, '')
                downloaded += 1
            except Exception as e:
                print(f"  Error: {e}")
    
    return downloaded

def main():
    datasets = search_qdr()
    print(f"\nTotal datasets found: {len(datasets)}")
    downloaded = process_datasets(datasets)
    print(f"\nDone! Downloaded {downloaded} files from QDR.")

if __name__ == '__main__':
    main()