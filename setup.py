import duckdb
import os

# Set your data directory path here
DATA_DIR = "data/archive"

# Connect to DuckDB
con = duckdb.connect("oulad.db")

# Create table from CSV
con.execute(f"""
    CREATE TABLE IF NOT EXISTS student_vle AS
    SELECT * FROM read_csv_auto('{DATA_DIR}/studentVle.csv')
""")

print("✅ Database setup complete.")
print(f"Total rows loaded: {con.execute('SELECT COUNT(*) FROM student_vle').fetchone()[0]:,}")
con.close()
