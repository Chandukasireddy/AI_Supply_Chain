"""
Initialises the local DuckDB database from schema.sql and seed_data.sql.
Run once before starting the app:  python mock_data/init_db.py
"""
import os
import pathlib
import duckdb

DB_PATH = os.environ.get("MOCK_DB_PATH", "mock_data/db/supply_chain.duckdb")
SCHEMA  = pathlib.Path(__file__).parent / "schema.sql"
SEED    = pathlib.Path(__file__).parent / "seed_data.sql"


def init():
    pathlib.Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(DB_PATH)

    conn.execute(SCHEMA.read_text())
    print("Schema applied.")

    # Idempotent seed: skip if products already populated
    if conn.execute("SELECT COUNT(*) FROM products").fetchone()[0] == 0:
        conn.execute(SEED.read_text())
        print("Seed data inserted.")
    else:
        print("Seed data already present — skipped.")

    conn.close()
    print(f"Database ready at {DB_PATH}")


if __name__ == "__main__":
    init()
