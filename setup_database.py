import sqlite3

# Create database
conn = sqlite3.connect('metadata.db')
c = conn.cursor()

# Create table
c.execute('''
CREATE TABLE IF NOT EXISTS downloads (
    url            TEXT NOT NULL,
    timestamp      TEXT NOT NULL,
    local_dir      TEXT NOT NULL,
    local_file     TEXT NOT NULL,
    repository     TEXT,
    license        TEXT,
    uploader_name  TEXT,
    uploader_email TEXT
)
''')

conn.commit()
conn.close()
print("Database created successfully!")