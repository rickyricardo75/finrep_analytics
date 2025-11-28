# This script loads your 11 files into the SRC_* tables in phase1.db, mapping the key columns
# we defined. It auto-detects delimiter and tries encodings in this order: UTF-8 → cp1252 → latin1

import csv, hashlib, sys
from pathlib import Path
import pandas as pd
import sqlite3
from datetime import datetime

BASE = Path(r"C:\Users\Dick\pyproj_finrep\phase1")
DB   = BASE / "phase1.db"
SRC  = BASE / r"data\source"

# --- helpers ---
def read_csv_any(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "cp1252", "latin1"):
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            continue
    raise RuntimeError(f"Failed to read {path} with utf-8/cp1252/latin1")

def pick(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    out = {}
    cols_lower = {c.lower(): c for c in df.columns}
    for src_name, tgt in mapping.items():
        # accept exact or case-insensitive match
        c = cols_lower.get(src_name.lower())
        if c is not None:
            out[tgt] = df[c]
    return pd.DataFrame(out)

def to_date(series: pd.Series) -> pd.Series:
    # handle common Swiss/EU/ISO formats
    return pd.to_datetime(series, errors="coerce", dayfirst=True).dt.date

def to_num(series: pd.Series) -> pd.Series:
    # safe to numeric (handles commas as thousands if present)
    s = series.astype(str).str.replace("\u00A0", "").str.replace(" ", "")
    return pd.to_numeric(s.str.replace(",", ""), errors="coerce")

def append_sql(conn, table: str, df: pd.DataFrame):
    # only keep columns that exist in the table schema
    cols = set(r[1] for r in conn.execute(f"PRAGMA table_info({table})"))
    df = df[[c for c in df.columns if c in cols]]
    if df.empty:
        return 0
    df.to_sql(table, conn, if_exists="append", index=False)
    return len(df)

# --- file locations ---
files = {
    # portfolio
    "Holdings": SRC / r"portfolio\Holdings.csv",
    "Movements": SRC / r"portfolio\Movements.csv",
    # performance
    "DailyValues": SRC / r"performance\DailyValues.csv",
    "CashAgenda": SRC / r"performance\CashAgenda.csv",
    # calendars
    "DailyValuesCalendar": SRC / r"calendars\DailyValuesCalendar.csv",
    "MovementsCalendar": SRC / r"calendars\MovementsCalendar.csv",
    "CashAgendaCalendar": SRC / r"calendars\CashAgendaCalendar.csv",
}

# --- load sequence ---
conn = sqlite3.connect(DB)

counts = {}

# 1) Holdings -> SRC_Holdings
if files["Holdings"].exists():
    df = read_csv_any(files["Holdings"])
    m = {
        "Portfolio number": "portfolio_nk",
        "Evaluation date": "value_date",
        "ISIN": "security_nk",
        "Identification number": "position_id",
        "Description": "security_name",
        "Quantity / Amount": "qty_raw",
        "Price": "price_raw",
        "Trade currency": "native_ccy",
    }
    d = pick(df, m)
    if "value_date" in d: d["value_date"] = to_date(d["value_date"])
    for x in ("qty_raw","price_raw"):
        if x in d: d[x] = to_num(d[x])
    counts["SRC_Holdings"] = append_sql(conn, "SRC_Holdings", d)

# 2) Movements -> SRC_Movements
if files["Movements"].exists():
    df = read_csv_any(files["Movements"])
    m = {
        "Transaction number": "trade_id",
        "Portfolio number": "portfolio_nk",
        "Value date": "settle_date",
        "Purchase date": "trade_date",
        "Transaction": "transaction_src",
        "Accounting": "accounting_src",
        "Price": "price_raw",
        "Amount": "amount_raw",
        "Currency": "native_ccy",
    }
    d = pick(df, m)
    for c in ("settle_date","trade_date"):
        if c in d: d[c] = to_date(d[c])
    for x in ("price_raw","amount_raw"):
        if x in d: d[x] = to_num(d[x])
    counts["SRC_Movements"] = append_sql(conn, "SRC_Movements", d)

# 3) DailyValues -> SRC_DailyValues
if files["DailyValues"].exists():
    df = read_csv_any(files["DailyValues"])
    m = {
        "Portfolio number": "portfolio_nk",
        "Evaluation date": "value_date",
        "Currency": "eval_ccy",
        "Value end": "value_end",
        "Inflow": "inflow",
        "Outflow": "outflow",
    }
    d = pick(df, m)
    if "value_date" in d: d["value_date"] = to_date(d["value_date"])
    for x in ("value_end","inflow","outflow"):
        if x in d: d[x] = to_num(d[x])
    counts["SRC_DailyValues"] = append_sql(conn, "SRC_DailyValues", d)

# 4) CashAgenda -> SRC_CashAgenda
if files["CashAgenda"].exists():
    df = read_csv_any(files["CashAgenda"])
    m = {
        "Date": "event_date",
        "Evaluation date": "evaluation_date",
        "Portfolio number": "portfolio_nk",
        "ISIN": "security_nk",
        "Type of cash flow": "cash_flow_type_src",
        "Cash flow type": "asset_bucket_src",
        "Currency": "native_ccy",
        "Quantity / Amount": "amount_raw",
    }
    d = pick(df, m)
    for c in ("event_date","evaluation_date"):
        if c in d: d[c] = to_date(d[c])
    if "amount_raw" in d: d["amount_raw"] = to_num(d["amount_raw"])
    counts["SRC_CashAgenda"] = append_sql(conn, "SRC_CashAgenda", d)

# 5) Calendars -> SRC_GenericCalendar (append all)
def load_calendar(p: Path):
    if not p.exists(): return 0
    df = read_csv_any(p)
    # tolerant date column naming
    candidates = [c for c in df.columns if c.strip().lower() in ("date","cal_date")]
    if not candidates:
        # handle accidental split like "Da","e"
        if {"Da","e"}.issubset(set(df.columns)):
            df["Date"] = df["Da"].astype(str)+df["e"].astype(str)
            candidates = ["Date"]
        else:
            raise RuntimeError(f"No Date column found in {p}")
    df = df.rename(columns={candidates[0]:"cal_date"})
    # coerce date and optional ints/bools
    df["cal_date"] = to_date(df["cal_date"])
    for col in ("day","month","week","quarter","year"):
        if col in df.columns: df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("End of Month","End of Year","is_month_end","is_year_end"):
        if col in df.columns:
            if col in ("End of Month","End of Year"):
                new = "is_month_end" if "Month" in col else "is_year_end"
                df[new] = df[col].astype(str).str.strip().str.lower().isin(("1","true","yes","y"))
                df = df.drop(columns=[col])
    keep = [c for c in ("cal_date","day","month","week","quarter","year","is_month_end","is_year_end") if c in df.columns]
    d = df[keep]
    return append_sql(conn, "SRC_GenericCalendar", d)

cal_count = 0
for name in ("DailyValuesCalendar","MovementsCalendar","CashAgendaCalendar"):
    cal_count += load_calendar(files[name])
counts["SRC_GenericCalendar"] = cal_count

conn.close()
print("Rows loaded:", counts)
