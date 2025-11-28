import sqlite3
from pathlib import Path
from datetime import datetime

BASE = Path(r"C:\Users\Dick\pyproj_finrep\phase1")
DB   = BASE / "phase1.db"

def run(cur, sql):
    cur.executescript(sql)

def count(cur, table):
    return cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

conn = sqlite3.connect(DB)
cur  = conn.cursor()

# --- DIMENSIONS: rebuild minimal current snapshots (idempotent) ---
# means delete & reload today’s state for Portfolio/Security dims so re-running yields the same result (no duplicates)
run(cur, """
DELETE FROM CDM_Portfolio;
DELETE FROM CDM_SecurityMaster;

INSERT INTO CDM_Portfolio (portfolio_nk, name, base_ccy, strategy, legal_entity, pm_name, rm_name,
                           parent_portfolio_sk, row_eff_datetime, row_end_datetime, is_current)
SELECT DISTINCT x.portfolio_nk, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       CURRENT_TIMESTAMP, NULL, 1
FROM (
  SELECT portfolio_nk FROM SRC_Holdings
  UNION
  SELECT portfolio_nk FROM SRC_Movements
  UNION
  SELECT portfolio_nk FROM SRC_DailyValues
  UNION
  SELECT portfolio_nk FROM SRC_CashAgenda
) x
WHERE x.portfolio_nk IS NOT NULL AND TRIM(x.portfolio_nk) <> '';

INSERT INTO CDM_SecurityMaster (security_nk, isin, sedol, ticker, mic, name, type, subtype, ccy,
                                issuer_sk, class_sk, row_eff_datetime, row_end_datetime, is_current)
SELECT DISTINCT s.security_nk, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
       NULL, NULL, CURRENT_TIMESTAMP, NULL, 1
FROM (
  SELECT security_nk FROM SRC_Holdings
  UNION
  SELECT security_nk FROM SRC_CashAgenda
) s
WHERE s.security_nk IS NOT NULL AND TRIM(s.security_nk) <> '';
""")

# --- FACTS: truncate & (re)load from SRC_* ---
run(cur, """
DELETE FROM CDM_Holdings;
DELETE FROM CDM_PortfolioDailyValues;
DELETE FROM CDM_Transactions;
DELETE FROM CDM_CashAgenda;

-- HOLDINGS
INSERT INTO CDM_Holdings (value_date, portfolio_sk, security_sk, qty, price, mv_native, mv_base)
SELECT sh.value_date,
       p.portfolio_sk,
       s.security_sk,
       sh.qty_raw,
       sh.price_raw,
       CASE WHEN sh.qty_raw IS NOT NULL AND sh.price_raw IS NOT NULL
            THEN sh.qty_raw * sh.price_raw END AS mv_native,
       NULL AS mv_base
FROM SRC_Holdings sh
JOIN CDM_Portfolio p ON p.portfolio_nk = sh.portfolio_nk
LEFT JOIN CDM_SecurityMaster s ON s.security_nk = sh.security_nk;

-- PORTFOLIO DAILY VALUES
INSERT INTO CDM_PortfolioDailyValues (value_date, portfolio_sk, eval_ccy,
                                      value_start, value_end, inflow, outflow,
                                      adj_inflow, pl_native, pl_base, avg_capital,
                                      index_val, prev_index)
SELECT sd.value_date,
       p.portfolio_sk,
       sd.eval_ccy,
       sd.value_start,
       sd.value_end,
       sd.inflow,
       sd.outflow,
       sd.adj_inflow,
       sd.pl_native,
       NULL AS pl_base,
       sd.avg_capital,
       sd.index_val,
       sd.prev_index
FROM SRC_DailyValues sd
JOIN CDM_Portfolio p ON p.portfolio_nk = sd.portfolio_nk;


-- TRANSACTIONS (normalize txn_type via REF_TransactionMap; security optional)
INSERT INTO CDM_Transactions (trade_id, trade_date, settle_date, portfolio_sk, security_sk,
                              txn_type, qty, price, gross_amt_native, gross_amt_base,
                              fees_tax_native, fees_tax_base)
SELECT sm.trade_id,
       sm.trade_date,
       sm.settle_date,
       p.portfolio_sk,
       NULL AS security_sk,                        -- not present in SRC_Movements extract
       COALESCE(r.txn_type, 'OTHER') AS txn_type,
       NULL AS qty,                                -- not provided (cash vs security dependent)
       sm.price_raw,
       sm.amount_raw AS gross_amt_native,          -- use existing column
       NULL AS gross_amt_base,
       NULL AS fees_tax_native,
       NULL AS fees_tax_base
FROM SRC_Movements sm
JOIN CDM_Portfolio p ON p.portfolio_nk = sm.portfolio_nk
LEFT JOIN REF_TransactionMap r
  ON LOWER(TRIM(r.transaction_src)) = LOWER(TRIM(sm.transaction_src));



-- CASH AGENDA
INSERT INTO CDM_CashAgenda (event_date, evaluation_date, portfolio_sk, security_sk,
                            cash_type, native_ccy, amt_native, amt_base,
                            pre_tax_native, pre_tax_eval, after_tax_native)
SELECT sc.event_date,
       sc.evaluation_date,
       p.portfolio_sk,
       s.security_sk,
       UPPER(TRIM(sc.cash_flow_type_src)) AS cash_type,
       sc.native_ccy,
       sc.amount_raw,
       NULL AS amt_base,
       sc.pre_tax_native,
       sc.pre_tax_eval,
       sc.after_tax_native
FROM SRC_CashAgenda sc
JOIN CDM_Portfolio p ON p.portfolio_nk = sc.portfolio_nk
LEFT JOIN CDM_SecurityMaster s ON s.security_nk = sc.security_nk;
""")

conn.commit()

# --- SIMPLE DQ & COUNTS ---
tables = [
    "CDM_Portfolio", "CDM_SecurityMaster",
    "CDM_Holdings", "CDM_PortfolioDailyValues",
    "CDM_Transactions", "CDM_CashAgenda"
]
print("Row counts:")
for t in tables:
    print(f"  {t:28s}", count(cur, t))

# Leftover SRC rows without Portfolio match (should be zero)
def leftover(src, port_col):
    q = f"""
    SELECT COUNT(*) FROM {src} s
    LEFT JOIN CDM_Portfolio p ON p.portfolio_nk = s.{port_col}
    WHERE s.{port_col} IS NOT NULL AND TRIM(s.{port_col}) <> '' AND p.portfolio_sk IS NULL;
    """
    return cur.execute(q).fetchone()[0]

print("\nUnmatched portfolios (should be 0):")
for src, col in [("SRC_Holdings","portfolio_nk"),
                 ("SRC_Movements","portfolio_nk"),
                 ("SRC_DailyValues","portfolio_nk"),
                 ("SRC_CashAgenda","portfolio_nk")]:
    print(f"  {src:16s}", leftover(src, col))

# Date ranges (sanity)
def minmax(table, col):
    q = f"SELECT MIN({col}), MAX({col}) FROM {table}"
    return cur.execute(q).fetchone()

print("\nDate ranges:")
print("  Holdings        ", minmax("CDM_Holdings","value_date"))
print("  DailyValues     ", minmax("CDM_PortfolioDailyValues","value_date"))
print("  Transactions TD ", minmax("CDM_Transactions","trade_date"))
print("  Transactions SD ", minmax("CDM_Transactions","settle_date"))
print("  CashAgenda      ", minmax("CDM_CashAgenda","event_date"))

cur.close()
conn.close()
print("\nMove 3 complete.")
