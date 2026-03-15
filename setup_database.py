import sqlite3

def create_database():
    conn = sqlite3.connect('metadata.db')
    c = conn.cursor()

    # PROJECTS table
    c.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id                        INTEGER PRIMARY KEY AUTOINCREMENT,
            query_string              TEXT,
            repository_id             INTEGER NOT NULL,
            repository_url            TEXT NOT NULL,
            project_url               TEXT NOT NULL,
            version                   TEXT,
            title                     TEXT NOT NULL,
            description               TEXT,
            language                  TEXT,
            doi                       TEXT,
            upload_date               TEXT,
            download_date             TIMESTAMP NOT NULL,
            download_repository_folder TEXT NOT NULL,
            download_project_folder   TEXT NOT NULL,
            download_version_folder   TEXT,
            download_method           TEXT NOT NULL
        )
    ''')

    # FILES table
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id  INTEGER NOT NULL,
            file_name   TEXT NOT NULL,
            file_type   TEXT NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
    ''')

    # KEYWORDS table
    c.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id  INTEGER NOT NULL,
            keyword     TEXT NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
    ''')

    # PERSON_ROLE table
    c.execute('''
        CREATE TABLE IF NOT EXISTS person_role (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id  INTEGER NOT NULL,
            name        TEXT NOT NULL,
            role        TEXT NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
    ''')

    # LICENSES table
    c.execute('''
        CREATE TABLE IF NOT EXISTS licenses (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id  INTEGER NOT NULL,
            license     TEXT NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects(id)
        )
    ''')

    conn.commit()
    conn.close()
    print("Database created successfully with new schema!")

if __name__ == '__main__':
    create_database()