import sqlite3
from pathlib import Path

DB = Path(r"C:\Users\Dick\pyproj_finrep\phase1\phase1.db")

def ensure_columns(cur, table, col_defs):
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table}(dummy_col_for_init TEXT)")
    existing = {r[1].lower() for r in cur.execute(f"PRAGMA table_info({table})")}
    for name, ddl in col_defs:
        if name.lower() not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")

conn = sqlite3.connect(DB)
cur = conn.cursor()

# ----- CDM tables (as before) -----
ensure_columns(cur, "CDM_Portfolio", [
    ("portfolio_sk", "portfolio_sk INTEGER"),
    ("portfolio_nk", "portfolio_nk VARCHAR(64)"),
    ("name", "name VARCHAR(256)"),
    ("base_ccy", "base_ccy CHAR(3)"),
    ("strategy", "strategy VARCHAR(128)"),
    ("legal_entity", "legal_entity VARCHAR(128)"),
    ("pm_name", "pm_name VARCHAR(128)"),
    ("rm_name", "rm_name VARCHAR(128)"),
    ("parent_portfolio_sk", "parent_portfolio_sk INTEGER"),
    ("row_eff_datetime", "row_eff_datetime TIMESTAMP"),
    ("row_end_datetime", "row_end_datetime TIMESTAMP"),
    ("is_current", "is_current BOOLEAN DEFAULT 1"),
])

ensure_columns(cur, "CDM_SecurityMaster", [
    ("security_sk", "security_sk INTEGER"),
    ("security_nk", "security_nk VARCHAR(128)"),
    ("isin", "isin VARCHAR(12)"),
    ("sedol", "sedol VARCHAR(7)"),
    ("ticker", "ticker VARCHAR(64)"),
    ("mic", "mic VARCHAR(4)"),
    ("name", "name VARCHAR(512)"),
    ("type", "type VARCHAR(64)"),
    ("subtype", "subtype VARCHAR(64)"),
    ("ccy", "ccy CHAR(3)"),
    ("issuer_sk", "issuer_sk INTEGER"),
    ("class_sk", "class_sk INTEGER"),
    ("row_eff_datetime", "row_eff_datetime TIMESTAMP"),
    ("row_end_datetime", "row_end_datetime TIMESTAMP"),
    ("is_current", "is_current BOOLEAN DEFAULT 1"),
])

ensure_columns(cur, "CDM_Holdings", [
    ("holding_sk", "holding_sk INTEGER"),
    ("value_date", "value_date DATE"),
    ("portfolio_sk", "portfolio_sk INTEGER"),
    ("security_sk", "security_sk INTEGER"),
    ("qty", "qty DECIMAL(38,10)"),
    ("price", "price DECIMAL(38,10)"),
    ("mv_native", "mv_native DECIMAL(38,10)"),
    ("mv_base", "mv_base DECIMAL(38,10)"),
    ("accrued_interest", "accrued_interest DECIMAL(38,10)"),
    ("price_source", "price_source VARCHAR(32)"),
    ("asof_datetime", "asof_datetime TIMESTAMP"),
])

ensure_columns(cur, "CDM_PortfolioDailyValues", [
    ("pdv_sk", "pdv_sk INTEGER"),
    ("value_date", "value_date DATE"),
    ("portfolio_sk", "portfolio_sk INTEGER"),
    ("eval_ccy", "eval_ccy CHAR(3)"),
    ("value_start", "value_start DECIMAL(38,10)"),
    ("value_end", "value_end DECIMAL(38,10)"),
    ("inflow", "inflow DECIMAL(38,10)"),
    ("outflow", "outflow DECIMAL(38,10)"),
    ("adj_inflow", "adj_inflow DECIMAL(38,10)"),
    ("pl_native", "pl_native DECIMAL(38,10)"),
    ("pl_base", "pl_base DECIMAL(38,10)"),
    ("avg_capital", "avg_capital DECIMAL(38,10)"),
    ("index_val", "index_val DECIMAL(38,10)"),
    ("prev_index", "prev_index DECIMAL(38,10)"),
])

ensure_columns(cur, "CDM_Transactions", [
    ("txn_sk", "txn_sk INTEGER"),
    ("trade_id", "trade_id VARCHAR(64)"),
    ("trade_date", "trade_date DATE"),
    ("settle_date", "settle_date DATE"),
    ("portfolio_sk", "portfolio_sk INTEGER"),
    ("security_sk", "security_sk INTEGER"),
    ("txn_type", "txn_type VARCHAR(32)"),
    ("qty", "qty DECIMAL(38,10)"),
    ("price", "price DECIMAL(38,10)"),
    ("gross_amt_native", "gross_amt_native DECIMAL(38,10)"),
    ("gross_amt_base", "gross_amt_base DECIMAL(38,10)"),
    ("fees_tax_native", "fees_tax_native DECIMAL(38,10)"),
    ("fees_tax_base", "fees_tax_base DECIMAL(38,10)"),
])

ensure_columns(cur, "CDM_CashAgenda", [
    ("cag_sk", "cag_sk INTEGER"),
    ("event_date", "event_date DATE"),
    ("evaluation_date", "evaluation_date DATE"),
    ("portfolio_sk", "portfolio_sk INTEGER"),
    ("security_sk", "security_sk INTEGER"),
    ("cash_type", "cash_type VARCHAR(32)"),
    ("native_ccy", "native_ccy CHAR(3)"),
    ("amt_native", "amt_native DECIMAL(38,10)"),
    ("amt_base", "amt_base DECIMAL(38,10)"),
    ("pre_tax_native", "pre_tax_native DECIMAL(38,10)"),
    ("pre_tax_eval", "pre_tax_eval DECIMAL(38,10)"),
    ("after_tax_native", "after_tax_native DECIMAL(38,10)"),
])

# ----- NEW: ensure SRC tables have the columns the CDM loader selects -----
ensure_columns(cur, "SRC_DailyValues", [
    ("portfolio_nk", "portfolio_nk VARCHAR(64)"),
    ("value_date", "value_date DATE"),
    ("eval_ccy", "eval_ccy CHAR(3)"),
    ("value_start", "value_start DECIMAL(38,10)"),
    ("value_end", "value_end DECIMAL(38,10)"),
    ("inflow", "inflow DECIMAL(38,10)"),
    ("outflow", "outflow DECIMAL(38,10)"),
    ("adj_inflow", "adj_inflow DECIMAL(38,10)"),
    ("pl_native", "pl_native DECIMAL(38,10)"),
    ("avg_capital", "avg_capital DECIMAL(38,10)"),
    ("index_val", "index_val DECIMAL(38,10)"),
    ("prev_index", "prev_index DECIMAL(38,10)"),
])

ensure_columns(cur, "SRC_CashAgenda", [
    ("event_date", "event_date DATE"),
    ("evaluation_date", "evaluation_date DATE"),
    ("portfolio_nk", "portfolio_nk VARCHAR(64)"),
    ("security_nk", "security_nk VARCHAR(64)"),
    ("cash_flow_type_src", "cash_flow_type_src VARCHAR(64)"),
    ("asset_bucket_src", "asset_bucket_src VARCHAR(64)"),
    ("native_ccy", "native_ccy CHAR(3)"),
    ("amount_raw", "amount_raw DECIMAL(38,10)"),
    ("pre_tax_native", "pre_tax_native DECIMAL(38,10)"),
    ("pre_tax_eval", "pre_tax_eval DECIMAL(38,10)"),
    ("after_tax_native", "after_tax_native DECIMAL(38,10)"),
])

# Movements and Holdings already had required columns in your loader/DDL
# If needed later, we can add guards here too.

conn.commit()
conn.close()
print("All CDM + SRC schemas migrated to expected columns.")
