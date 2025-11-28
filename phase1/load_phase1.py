
import argparse
import sqlite3
import pandas as pd
from pathlib import Path

def run_sql(conn, sql_text: str):
    cur = conn.cursor()
    for stmt in [s.strip() for s in sql_text.split(';') if s.strip()]:
        cur.execute(stmt)
    conn.commit()

def main(db_path: str, ddl_file: str, transaction_map: str, accounting_map: str, assetclass_map: str):
    db = Path(db_path)
    ddl = Path(ddl_file).read_text()
    conn = sqlite3.connect(db)
    try:
        run_sql(conn, ddl)
        # Load reference CSVs
        pd.read_csv(transaction_map).to_sql("REF_TransactionMap", conn, if_exists="replace", index=False)
        pd.read_csv(accounting_map).to_sql("REF_AccountingMap", conn, if_exists="replace", index=False)
        pd.read_csv(assetclass_map).to_sql("REF_AssetClassMap", conn, if_exists="replace", index=False)
        print("DDL applied and reference tables loaded.")
    finally:
        conn.close()

if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Phase 1: create schemas and load reference maps")
    p.add_argument("--db", default="phase1.db", help="SQLite database file (default: phase1.db)")
    p.add_argument("--ddl", default="phase1_schema_ddl.sql", help="DDL SQL file path")
    p.add_argument("--ref_tx", default="ref_transaction_map.csv", help="REF_TransactionMap CSV path")
    p.add_argument("--ref_acc", default="ref_accounting_map.csv", help="REF_AccountingMap CSV path")
    p.add_argument("--ref_ac", default="ref_assetclass_map.csv", help="REF_AssetClassMap CSV path")
    args = p.parse_args()
    main(args.db, args.ddl, args.ref_tx, args.ref_acc, args.ref_ac)
