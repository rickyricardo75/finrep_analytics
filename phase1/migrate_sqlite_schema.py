import sqlite3, sys
from pathlib import Path

DB = Path(r"C:\Users\Dick\pyproj_finrep\phase1\phase1.db")

def ensure_columns(cur, table, col_defs):
    existing = {r[1].lower() for r in cur.execute(f"PRAGMA table_info({table})")}
    for name, ddl in col_defs:
        if name.lower() not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")

conn = sqlite3.connect(DB)
cur = conn.cursor()

# Bring CDM_Portfolio to the expected schema
ensure_columns(cur, "CDM_Portfolio", [
    ("name", "name VARCHAR(256)"),
    ("base_ccy", "base_ccy CHAR(3)"),
    ("strategy", "strategy VARCHAR(128)"),
    ("legal_entity", "legal_entity VARCHAR(128)"),
    ("pm_name", "pm_name VARCHAR(128)"),
    ("rm_name", "rm_name VARCHAR(128)"),
    ("parent_portfolio_sk", "parent_portfolio_sk INTEGER"),
    ("row_eff_datetime", "row_eff_datetime TIMESTAMP"),
    ("row_end_datetime", "row_end_datetime TIMESTAMP"),
    ("is_current", "is_current BOOLEAN DEFAULT 1")
])

conn.commit()
conn.close()
print("Schema migration complete.")
