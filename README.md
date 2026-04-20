# Learning Analytics Backend with DuckDB

A learning analytics backend that demonstrates how DuckDB's columnar storage
architecture supports aggregation-heavy educational workloads.

## Dataset

This project uses the Open University Learning Analytics Dataset (OULAD).

Download from: https://www.kaggle.com/datasets/anlgrbz/student-demographics-online-education-dataoulad

Place `studentVle.csv` in the `data/archive/` folder.

## Setup

1. Install dependencies:
   pip install duckdb pandas matplotlib jupyter

2. Run the setup script to load data into DuckDB:
   python setup.py

3. Open the notebook:
   jupyter notebook oulad_duckdb.ipynb

4. Run all cells from top to bottom.

## Project Structure

- oulad_duckdb.ipynb   # Main analysis notebook
- setup.py             # Database setup script
- data/archive/        # Place studentVle.csv here
- oulad.db             # DuckDB database (auto-generated)

## Focus Area

Columnar Storage Architecture: how DuckDB's column-oriented storage
enables efficient aggregation queries over 10 million student interaction records.
