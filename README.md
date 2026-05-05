# Learning Analytics Backend with DuckDB

A learning analytics backend that demonstrates how DuckDB's columnar storage
architecture supports aggregation-heavy educational workloads. Built for
DSCI 551 (Spring 2026) at the University of Southern California.

## Dataset

This project uses the **Open University Learning Analytics Dataset (OULAD)**,
which contains over 10 million student VLE (Virtual Learning Environment)
clickstream events linked to demographic, assessment, and course outcome data.

**Download link:** https://www.kaggle.com/datasets/anlgrbz/student-demographics-online-education-dataoulad

**Requirements to download:**
- A Kaggle account (free) is needed to download the dataset.
- The full dataset contains 7 CSV files (see `data/archive/` structure below).

**Setup instructions:**
1. Download the dataset from Kaggle (430 MB compressed).
2. Extract the CSV files into `data/archive/`:
   ```
   data/archive/
   ├── assessments.csv
   ├── courses.csv
   ├── studentAssessment.csv
   ├── studentInfo.csv
   ├── studentRegistration.csv
   ├── studentVle.csv          # ~454 MB — the main fact table
   └── vle.csv
   ```
3. Only `studentVle.csv` is required if you just want to run the core queries.
   The notebook also loads the other 6 tables for multi-table JOIN analysis.

**Note:** The dataset is not committed to GitHub due to its size (454 MB). The
database file `oulad.db` is also excluded via `.gitignore` and regenerated
automatically when you run the setup.

## Setup

### Prerequisites

- Python 3.10+
- pip

### Installation

```bash
# Install dependencies
pip install duckdb pandas matplotlib jupyter
```

### Running the notebook

```bash
# Launch the main notebook
jupyter notebook oulad_duckdb.ipynb
```

Then run all cells from top to bottom. The notebook will:
1. Create the DuckDB database file (`oulad.db`) from the CSV data.
2. Run 5 analytical queries over 10.6M student interaction records.
3. Display benchmark results comparing DuckDB with MySQL.
4. Show EXPLAIN plans to reveal DuckDB's internal execution architecture.

### Alternative: run without a notebook

```bash
# Set up the database from CSV
python setup.py

# Run benchmark analysis
python analysis_enhancements.py
```

### Reproducing benchmark results

The benchmarks in Section 5 (comparing DuckDB vs MySQL) are embedded in the
notebook (`Cell 17`). To reproduce:
1. Ensure DuckDB and MySQL 9.6 are both installed.
2. Load the same OULAD `studentVle.csv` into MySQL (`student_vle` table).
3. Run Cell 17 in `oulad_duckdb.ipynb`.
4. Results are cached in `enhancement_results.json`.

Expected results on Apple Silicon (M-series) hardware with SSD storage:
| Query | DuckDB | MySQL 9.6 | Speedup |
|-------|--------|-----------|---------|
| Q1: GROUP BY date (aggregation) | ~3.8 ms | ~2370 ms | ~618x |
| Q2: GROUP BY module (avg + count) | ~12 ms | ~2956 ms | ~247x |
| Q3: GROUP BY + ORDER BY + LIMIT | ~14 ms | ~2716 ms | ~192x |

Actual timings may vary depending on hardware, OS, and system load.

### Secret keys / credentials

This project does not require any API keys, credentials, tokens, or
environment variables. All data is loaded from local CSV files.

## Project Structure

```
├── README.md                        # This file
├── oulad_duckdb.ipynb               # Main analysis notebook (29 cells)
├── setup.py                         # Standalone database setup script
├── analysis_enhancements.py         # Benchmark analysis script
├── build_notebook.py                # Notebook builder script
├── enhancement_results.json         # Cached benchmark timings
├── oulad_analytics.png              # Output visualization
├── .gitignore                       # Ignores .db, data/, __pycache__/
├── data/
│   └── archive/                     # Place OULAD CSV files here (not tracked)
├── oulad.db                         # DuckDB database (auto-generated, not tracked)
└── DSCI551_Final_Report.docx        # Final project report
```

## Pipeline overview

1. **Load** — CSV files are read by DuckDB's `read_csv_auto` and stored in
   columnar format in `oulad.db`.
2. **Query** — Five analytical queries (GROUP BY aggregations, multi-table
   JOINs) are executed against the DuckDB database.
3. **Compare** — Identical queries are run against MySQL 9.6 to compare
   storage and query performance.
4. **Explain** — DuckDB's `EXPLAIN` and `EXPLAIN ANALYZE` reveal internal
   operators (column pruning, perfect hash aggregation, TOP-N, vectorized
   scan, compression/decompression operators).
5. **Visualize** — Query results are plotted with Matplotlib to show
   engagement trends and module-level patterns.

## Focus Area

Columnar Storage Architecture: how DuckDB's column-oriented storage
enables efficient aggregation queries over 10 million student interaction
records, achieving 192x–618x speedup over MySQL (InnoDB) for analytical
workloads.

## Report

The final report (`DSCI551_Final_Report.docx`) documents:
- Project motivation and the OULAD dataset
- DuckDB's internal architecture (storage, indexing, execution)
- Application design (5 query categories across 7 relational tables)
- Mapping internals to application performance
- Empirical comparison with MySQL and theoretical comparison with MongoDB
- Limitations, lessons learned, and future work

**Author:** Rosie Xiu, USC DSCI 551 — Spring 2026
