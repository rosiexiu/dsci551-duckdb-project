"""
DSCI 551 Project - Notebook Enhancements
DuckDB vs MySQL comparison + advanced queries + storage analysis

Run this to generate all the new data for the enhanced notebook.
"""

import duckdb
import pandas as pd
import os
import time
import subprocess
import json

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(PROJECT_DIR, "data", "archive")
DB_PATH = os.path.join(PROJECT_DIR, "oulad.db")
CSV_PATH = os.path.join(DATA_DIR, "studentVle.csv")

con = duckdb.connect(DB_PATH)

# ============================================================
# 1. BENCHMARK: DuckDB vs MySQL query comparison
# ============================================================
print("=" * 60)
print("BENCHMARK: DuckDB vs MySQL Query Performance")
print("=" * 60)

queries = {
    "Q1: Weekly click totals (GROUP BY date)": """
        SELECT date, SUM(sum_click) AS total_clicks
        FROM student_vle
        GROUP BY date
        ORDER BY date
    """,
    "Q2: Module-level avg engagement (GROUP BY module)": """
        SELECT code_module, AVG(sum_click) AS avg_clicks, COUNT(*) AS total_records
        FROM student_vle
        GROUP BY code_module
        ORDER BY avg_clicks DESC
    """,
    "Q3: Top sites by total clicks (GROUP BY + LIMIT)": """
        SELECT id_site, COUNT(*) AS visit_count, SUM(sum_click) AS total_clicks
        FROM student_vle
        GROUP BY id_site
        ORDER BY total_clicks DESC
        LIMIT 20
    """
}

results_data = []

for qname, qsql in queries.items():
    print(f"\n--- {qname} ---")
    
    # DuckDB (columnar, no indexes)
    times = []
    for _ in range(5):
        start = time.time()
        con.execute(qsql).fetchall()
        times.append(time.time() - start)
    duckdb_avg = sum(times) / len(times)
    print(f"  DuckDB:    {duckdb_avg*1000:.1f}ms avg (over 5 runs)")
    
    # MySQL (row-based, with indexes)
    times = []
    for _ in range(3):
        start = time.time()
        result = subprocess.run(
            ["mysql", "-u", "root", "-N", "-B", "oulad", "-e", qsql],
            capture_output=True, text=True, timeout=120
        )
        times.append(time.time() - start)
    mysql_avg = sum(times) / len(times)
    print(f"  MySQL:     {mysql_avg*1000:.1f}ms avg (over 3 runs, with indexes)")
    
    speedup = mysql_avg / duckdb_avg if duckdb_avg > 0 else 0
    print(f"  Speedup:   {speedup:.1f}x")
    
    results_data.append({
        "query": qname,
        "duckdb_ms": round(duckdb_avg * 1000, 1),
        "mysql_ms": round(mysql_avg * 1000, 1),
        "speedup_x": round(speedup, 1)
    })

benchmark_df = pd.DataFrame(results_data)
print("\n\n📊 BENCHMARK SUMMARY:")
print(benchmark_df.to_string(index=False))


# ============================================================
# 2. STORAGE COMPARISON: compression analysis
# ============================================================
print("\n" + "=" * 60)
print("STORAGE: Compression & Size Comparison")
print("=" * 60)

duckdb_size = os.path.getsize(DB_PATH)
csv_size_mb = os.path.getsize(CSV_PATH) / 1024 / 1024

result = subprocess.run(
    ["mysql", "-u", "root", "-N", "-B", "oulad", "-e",
     "SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) FROM information_schema.tables WHERE table_schema = 'oulad' AND table_name = 'student_vle_indexed'"],
    capture_output=True, text=True
)
mysql_indexed_mb = float(result.stdout.strip())

result = subprocess.run(
    ["mysql", "-u", "root", "-N", "-B", "oulad", "-e",
     "SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) FROM information_schema.tables WHERE table_schema = 'oulad' AND table_name = 'student_vle'"],
    capture_output=True, text=True
)
mysql_noindex_mb = float(result.stdout.strip())

print(f"\n  Raw CSV:              {csv_size_mb:.1f} MB")
print(f"  DuckDB (columnar):     {duckdb_size/1024/1024:.1f} MB  (compression: {csv_size_mb/(duckdb_size/1024/1024):.1f}x vs CSV)")
print(f"  MySQL (no index):      {mysql_noindex_mb:.1f} MB")
print(f"  MySQL (w/ indexes):    {mysql_indexed_mb:.1f} MB")
duckdb_vs_mysql = mysql_noindex_mb / (duckdb_size/1024/1024)
print(f"  DuckDB vs MySQL:       {duckdb_vs_mysql:.1f}x smaller on disk")


# ============================================================
# 3. ADVANCED QUERIES: Multi-table JOIN analysis
# ============================================================
print("\n" + "=" * 60)
print("ADVANCED: Multi-Table Student Engagement & Performance")
print("=" * 60)

print("Loading all OULAD tables for JOIN queries...")
for fname in ["studentInfo.csv", "studentAssessment.csv", "assessments.csv", "courses.csv", "studentRegistration.csv", "vle.csv"]:
    tbl = fname.replace(".csv", "")
    file_path = os.path.join(DATA_DIR, fname)
    con.execute(f"CREATE OR REPLACE TABLE {tbl} AS SELECT * FROM read_csv_auto('{file_path}')")
    cnt = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
    print(f"  {tbl}: {cnt:,} rows")

# Query 1: Analyze student engagement by demographic + performance
adv_query_1 = """
SELECT 
    si.gender,
    si.age_band,
    si.region,
    COUNT(DISTINCT sv.id_student) AS student_count,
    ROUND(AVG(sv.sum_click), 1) AS avg_clicks_per_interaction,
    ROUND(AVG(sa.score), 1) AS avg_assessment_score
FROM student_vle sv
JOIN studentInfo si ON sv.id_student = si.id_student 
    AND sv.code_module = si.code_module 
    AND sv.code_presentation = si.code_presentation
JOIN studentAssessment sa ON sv.id_student = sa.id_student
    AND sa.id_assessment IN (
        SELECT id_assessment FROM assessments 
        WHERE code_module = sv.code_module 
        AND code_presentation = sv.code_presentation
    )
GROUP BY si.gender, si.age_band, si.region
HAVING student_count > 10
ORDER BY avg_assessment_score DESC
LIMIT 15
"""

start = time.time()
result_adv1 = con.execute(adv_query_1).df()
adv_time1 = time.time() - start
print(f"\nQuery 1 (Student Demographics + Performance): {adv_time1*1000:.1f}ms")
print(result_adv1.to_string(index=False))

# Query 2: Course-level engagement analysis
adv_query_2 = """
SELECT 
    si.final_result,
    c.code_module,
    ROUND(AVG(sr.date_unregistration), 0) AS avg_unreg_day,
    COUNT(DISTINCT si.id_student) AS student_count,
    ROUND(SUM(sv.sum_click) / NULLIF(COUNT(DISTINCT si.id_student), 0), 0) AS avg_clicks_per_student
FROM studentInfo si
JOIN courses c ON si.code_module = c.code_module AND si.code_presentation = c.code_presentation
JOIN studentRegistration sr ON si.id_student = sr.id_student 
    AND si.code_module = sr.code_module 
    AND si.code_presentation = sr.code_presentation
LEFT JOIN student_vle sv ON si.id_student = sv.id_student 
    AND si.code_module = sv.code_module 
    AND si.code_presentation = sv.code_presentation
GROUP BY si.final_result, c.code_module
ORDER BY si.final_result, student_count DESC
LIMIT 20
"""

start = time.time()
result_adv2 = con.execute(adv_query_2).df()
adv_time2 = time.time() - start
print(f"\nQuery 2 (Course Performance Analysis): {adv_time2*1000:.1f}ms")
print(result_adv2.to_string(index=False))


# ============================================================
# 4. DETAILED EXPLAIN PLANS
# ============================================================
print("\n" + "=" * 60)
print("EXPLAIN PLANS: Internal Execution Details")
print("=" * 60)

plan_q1_df = con.execute("""
    EXPLAIN
    SELECT date, SUM(sum_click) AS total_clicks
    FROM student_vle
    GROUP BY date
    ORDER BY date
""").df()
plan_q1_text = plan_q1_df.iloc[0, 0]

plan_q3_df = con.execute("""
    EXPLAIN
    SELECT id_site, COUNT(*) AS visit_count, SUM(sum_click) AS total_clicks
    FROM student_vle
    GROUP BY id_site
    ORDER BY total_clicks DESC
    LIMIT 20
""").df()
plan_q3_text = plan_q3_df.iloc[0, 0]

print("\n📋 EXPLAIN Plan (Q1 - Column Pruning):")
print(plan_q1_text[:2500])

print("\n\n📋 EXPLAIN Plan (Q3 - TOP_N optimization):")
print(plan_q3_text[:2000])


# ============================================================
# 5. Column compression info
# ============================================================
print("\n" + "=" * 60)
print("COMPRESSION: DuckDB column statistics")
print("=" * 60)

uniq = con.execute("""
    SELECT 'code_module' as col, COUNT(DISTINCT code_module)::VARCHAR as distinct_vals, 
           (SELECT COUNT(*) FROM student_vle)::VARCHAR as total_rows FROM student_vle
    UNION ALL
    SELECT 'code_presentation', COUNT(DISTINCT code_presentation)::VARCHAR, 
           (SELECT COUNT(*) FROM student_vle)::VARCHAR FROM student_vle
    UNION ALL
    SELECT 'id_student', COUNT(DISTINCT id_student)::VARCHAR, 
           (SELECT COUNT(*) FROM student_vle)::VARCHAR FROM student_vle
    UNION ALL
    SELECT 'id_site', COUNT(DISTINCT id_site)::VARCHAR, 
           (SELECT COUNT(*) FROM student_vle)::VARCHAR FROM student_vle
    UNION ALL
    SELECT 'date', COUNT(DISTINCT date)::VARCHAR, 
           (SELECT COUNT(*) FROM student_vle)::VARCHAR FROM student_vle
    UNION ALL
    SELECT 'sum_click', COUNT(DISTINCT sum_click)::VARCHAR, 
           (SELECT COUNT(*) FROM student_vle)::VARCHAR FROM student_vle
""").df()
print("\nColumn cardinality:")
print(uniq.to_string(index=False))

storage_info = con.execute("PRAGMA storage_info('student_vle')").df()
print(f"\nDuckDB segment storage info (columns with compression):")
# Show unique compression per column
col_compression = storage_info[['column_name', 'segment_type', 'compression_type']].drop_duplicates()
print(col_compression.to_string(index=False))

con.close()


# ============================================================
# SAVE ALL RESULTS TO JSON
# ============================================================
output = {
    "benchmark": benchmark_df.to_dict(orient="records"),
    "storage": {
        "raw_csv_mb": round(csv_size_mb, 1),
        "duckdb_mb": round(duckdb_size/1024/1024, 1),
        "mysql_noindex_mb": mysql_noindex_mb,
        "mysql_indexed_mb": mysql_indexed_mb,
        "compression_vs_csv_x": round(csv_size_mb / (duckdb_size/1024/1024), 1),
        "compression_vs_mysql_x": round(mysql_noindex_mb / (duckdb_size/1024/1024), 1)
    },
    "explain_q1": plan_q1_text[:3000],
    "explain_q3": plan_q3_text[:2000],
    "adv_query_1_ms": round(adv_time1 * 1000, 1),
    "adv_query_2_ms": round(adv_time2 * 1000, 1),
}

with open(os.path.join(PROJECT_DIR, "enhancement_results.json"), "w") as f:
    json.dump(output, f, indent=2)

print(f"\nResults saved to enhancement_results.json")

print("\n" + "=" * 60)
print("✅ ALL ENHANCEMENTS COMPLETE!")
print("=" * 60)
