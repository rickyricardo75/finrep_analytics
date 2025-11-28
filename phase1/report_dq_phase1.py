import sqlite3, csv
from pathlib import Path
from datetime import datetime

BASE = Path(r"C:\Users\Dick\pyproj_finrep\phase1")
DB   = BASE / "phase1.db"
OUT  = BASE / "reports"
OUT.mkdir(exist_ok=True)

def q(cur, sql):
    return cur.execute(sql).fetchall()

def write_csv(path, rows, headers):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)

conn = sqlite3.connect(DB)
cur  = conn.cursor()

# --- Table counts
tables = ["SRC_Holdings","SRC_Movements","SRC_DailyValues","SRC_CashAgenda","SRC_GenericCalendar",
          "CDM_Portfolio","CDM_SecurityMaster","CDM_Holdings","CDM_PortfolioDailyValues","CDM_Transactions","CDM_CashAgenda"]
counts = [(t, q(cur, f"SELECT COUNT(*) FROM {t}")[0][0]) for t in tables]
write_csv(OUT/"table_counts.csv", counts, ["table","row_count"])

# --- Null/required field checks in SRC
null_checks = [
    ("SRC_Holdings", "portfolio_nk", "value_date"),
    ("SRC_Movements","portfolio_nk", "trade_date"),
    ("SRC_DailyValues","portfolio_nk","value_date"),
    ("SRC_CashAgenda","portfolio_nk","event_date"),
]
rows = []
for t, c1, c2 in null_checks:
    n1 = q(cur, f"SELECT COUNT(*) FROM {t} WHERE {c1} IS NULL OR TRIM(COALESCE({c1},''))=''")[0][0]
    n2 = q(cur, f"SELECT COUNT(*) FROM {t} WHERE {c2} IS NULL")[0][0]
    rows.append((t,c1,n1))
    rows.append((t,c2,n2))
write_csv(OUT/"src_null_checks.csv", rows, ["table","column","null_or_blank_rows"])

# --- Orphan portfolio lookups (CDM facts)
orph = []
for t, col in [("CDM_Holdings","portfolio_sk"),
               ("CDM_PortfolioDailyValues","portfolio_sk"),
               ("CDM_Transactions","portfolio_sk"),
               ("CDM_CashAgenda","portfolio_sk")]:
    cnt = q(cur, f"SELECT COUNT(*) FROM {t} WHERE {col} IS NULL")[0][0]
    orph.append((t,col,cnt))
write_csv(OUT/"cdm_orphan_portfolios.csv", orph, ["table","column","null_rows"])

# --- Optional: security nulls where expected
sec_nulls = []
for t in ["CDM_Holdings","CDM_CashAgenda","CDM_Transactions"]:
    cnt = q(cur, f"SELECT COUNT(*) FROM {t} WHERE security_sk IS NULL")[0][0]
    sec_nulls.append((t,"security_sk",cnt))
write_csv(OUT/"cdm_security_nulls.csv", sec_nulls, ["table","column","null_rows"])

# --- Duplicate keys (light)
dupes = []
dupes.append(("CDM_PortfolioDailyValues (portfolio_sk,value_date)",
              q(cur, """SELECT COUNT(*) FROM (
                           SELECT portfolio_sk, value_date, COUNT(*) c
                           FROM CDM_PortfolioDailyValues
                           GROUP BY 1,2 HAVING c>1
                        )""")[0][0]))
dupes.append(("CDM_Holdings (portfolio_sk,security_sk,value_date,asof_datetime)",
              q(cur, """SELECT COUNT(*) FROM (
                           SELECT COALESCE(portfolio_sk,-1), COALESCE(security_sk,-1), value_date, COALESCE(asof_datetime,''),
                                  COUNT(*) c
                           FROM CDM_Holdings
                           GROUP BY 1,2,3,4 HAVING c>1
                        )""")[0][0]))
dupes.append(("CDM_Transactions (portfolio_sk,trade_id)",
              q(cur, """SELECT COUNT(*) FROM (
                           SELECT portfolio_sk, trade_id, COUNT(*) c
                           FROM CDM_Transactions
                           GROUP BY 1,2 HAVING c>1
                        )""")[0][0]))
dupes.append(("CDM_CashAgenda (portfolio_sk,security_sk,event_date,cash_type)",
              q(cur, """SELECT COUNT(*) FROM (
                           SELECT COALESCE(portfolio_sk,-1), COALESCE(security_sk,-1), event_date, COALESCE(cash_type,''),
                                  COUNT(*) c
                           FROM CDM_CashAgenda
                           GROUP BY 1,2,3,4 HAVING c>1
                        )""")[0][0]))
write_csv(OUT/"cdm_duplicate_checks.csv", dupes, ["check","duplicate_groups"])

# --- Distribution by date (daily values) & by portfolio
by_date = q(cur, """SELECT value_date, COUNT(*) FROM CDM_PortfolioDailyValues
                    GROUP BY value_date ORDER BY value_date""")
write_csv(OUT/"pdv_by_date.csv", by_date, ["value_date","rows"])

by_port = q(cur, """SELECT p.portfolio_nk, COUNT(*) FROM CDM_PortfolioDailyValues d
                    JOIN CDM_Portfolio p ON p.portfolio_sk = d.portfolio_sk
                    GROUP BY p.portfolio_nk ORDER BY 2 DESC""")
write_csv(OUT/"pdv_by_portfolio.csv", by_port, ["portfolio_nk","rows"])

# --- Sample ranges (already printed earlier, but export too)
ranges = [
    ("CDM_Holdings","value_date"),
    ("CDM_PortfolioDailyValues","value_date"),
    ("CDM_Transactions","trade_date"),
    ("CDM_Transactions","settle_date"),
    ("CDM_CashAgenda","event_date")
]
r_rows=[]
for t, c in ranges:
    mn, mx = q(cur, f"SELECT MIN({c}), MAX({c}) FROM {t}")[0]
    r_rows.append((t,c,mn,mx))
write_csv(OUT/"date_ranges.csv", r_rows, ["table","column","min","max"])

cur.close(); conn.close()

print("DQ complete. CSVs written to:", OUT)
