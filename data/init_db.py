#!/usr/bin/env python3
import sqlite3
import os

# Ensure db directory exists
os.makedirs("/english-handwritten/data/db", exist_ok=True)
db_path = "/english-handwritten/data/db/extraction.db"

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create tables
cursor.executescript('''
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_type TEXT NOT NULL,
    year TEXT,
    office_location TEXT,
    confidence TEXT,
    extraction_notes TEXT,
    source_path TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS index1_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    serial_number TEXT,
    name_of_person TEXT,
    family_details TEXT,
    police_station TEXT,
    religion TEXT,
    occupation TEXT,
    interest_of_person TEXT,
    where_registered TEXT,
    book_1_volume TEXT,
    book_2_page TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents (id)
);

CREATE TABLE IF NOT EXISTS index2_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    document_id INTEGER NOT NULL,
    serial_number TEXT,
    property_name TEXT,
    pargana_town_thana TEXT,
    location TEXT,
    nature_of_transaction TEXT,
    where_registered TEXT,
    book_1_volume TEXT,
    book_1_page TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (document_id) REFERENCES documents (id)
);
''')

conn.commit()
conn.close()
print("Database initialized!")