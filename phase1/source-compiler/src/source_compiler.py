import json, csv, re
from pathlib import Path
from typing import Dict, List
import pandas as pd
import sqlite3

# Optional YAML; falls back to JSON if not installed
try:
    import yaml  # pip install pyyaml
except Exception:
    yaml = None

BASE = Path(r"C:\Users\Dick\pyproj_finrep\phase1")
DB   = BASE / "phase1.db"
QDIR = BASE / r"source-compiler\quarantine"
QDIR.mkdir(exist_ok=True, parents=True)

def load_config(path: Path) -> dict:
    if path.suffix.lower() in (".yml", ".yaml"):
        if not yaml:
            raise RuntimeError("PyYAML not installed. Either install: pip install pyyaml OR use a .json config.")
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    else:
        return json.loads(path.read_text(encoding="utf-8"))

def sniff_read_csv(path: Path, encs: List[str], delims: List[str]) -> pd.DataFrame:
    for enc in encs:
        try:
            # Try pandas auto-sniff first
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            # Try explicit delims
            for d in delims:
                try:
                    return pd.read_csv(path, sep=d, engine="python", encoding=enc)
                except Exception:
                    continue
    raise RuntimeError(f"Failed to read {path} with encodings={encs} and delims={delims}")

def normalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    norm = {}
    for c in df.columns:
        c2 = (
            str(c)
            .strip()
            .lower()
            .replace("\u00A0", " ")
            .replace("  "," ")
        )
        norm[c2] = c
    df.columns = list(norm.keys())
    return df

def alias_columns(df: pd.DataFrame, header_aliases: Dict[str, List[str]]) -> Dict[str, str]:
    # Build reverse alias → canonical
    rev = {}
    for canon, aliases in header_aliases.items():
        for a in [canon] + aliases:
            a2 = str(a).strip().lower()
            rev[a2] = canon
    mapping = {}
    for c in df.columns:
        canon = rev.get(c)
        if canon:
            mapping[canon] = c
    return mapping  # canonical -> df-column-name(normalized)

def parse_dates(s: pd.Series, fmts: List[str], dayfirst: bool) -> pd.Series:
    s0 = s.astype(str).str.strip()
    vals = pd.Series(pd.NaT, index=s.index)

    # Excel serials
    serial = pd.to_numeric(s0, errors="coerce")
    mask = serial.ge(25569) & serial.lt(60000)
    vals.loc[mask] = pd.to_datetime("1899-12-30") + pd.to_timedelta(serial[mask], unit="D")

    remain = vals.isna()
    for f in fmts:
        hit = pd.to_datetime(s0[remain], format=f, errors="coerce")
        vals.loc[remain] = vals.loc[remain].fillna(hit)
        remain = vals.isna()
        if not remain.any():
            break

    if remain.any():
        vals.loc[remain] = pd.to_datetime(s0[remain], errors="coerce", dayfirst=dayfirst)
    return vals.dt.date

def parse_numbers(s: pd.Series) -> pd.Series:
    # Locale tolerant: remove NBSP, spaces, currency, handle parentheses
    x = s.astype(str).str.replace("\u00A0","", regex=False).str.strip()
    x = x.str.replace(r"^\((.*)\)$", r"-\1", regex=True)
    x = x.str.replace(r"[^\d,\.\-]", "", regex=True)
    # Prefer dot as decimal; remove thousand-separators
    # If both comma & dot exist, assume comma thousands and dot decimal
    def _norm(v):
        if v.count(",")>0 and v.count(".")>0:
            return v.replace(",","")
        elif v.count(",")>0 and v.count(".")==0:
            return v.replace(",",".")
        return v
    x = x.apply(_norm)
    return pd.to_numeric(x, errors="coerce")

def append_sql(conn, table: str, df: pd.DataFrame) -> int:
    cols = set(r[1] for r in conn.execute(f"PRAGMA table_info({table})"))
    keep = [c for c in df.columns if c in cols]
    if not keep:
        return 0
    df[keep].to_sql(table, conn, if_exists="append", index=False)
    return len(df)

def quarantine_write(name: str, reason: str, df: pd.DataFrame):
    out = QDIR / f"{name}_{reason}.csv"
    df.to_csv(out, index=False, encoding="utf-8")

def process_file(cfg: dict, item: dict, conn) -> int:
    path = BASE / item["path"]
    encs = cfg.get("encoding_priority", ["utf-8","cp1252","latin1"])
    delims = cfg.get("delimiter_priority", [",",";","|","\t"])
    hdr_aliases = cfg.get("header_aliases", {})
    fmts = cfg.get("date_format_priority", ["%Y-%m-%d","%d.%m.%Y","%d/%m/%Y","%m/%d/%Y","%Y%m%d"])
    dayfirst = bool(cfg.get("dayfirst_default", True))
    dq = cfg.get("dq_thresholds", {})
    min_date_rate = float(dq.get("min_date_parse_rate", 0.9))

    if not path.exists():
        return 0

    df = sniff_read_csv(path, encs, delims)
    df = normalize_headers(df)

    alias_map = alias_columns(df, hdr_aliases)
    # Build canonical dataframe from alias map
    canon_df = pd.DataFrame()
    for canon, df_col in alias_map.items():
        canon_df[canon] = df[df_col]

    # Calendar files: normalize to SRC_GenericCalendar
    if item.get("calendar", False):
        # detect a date column
        if "value_date" in canon_df.columns:
            canon_df["cal_date"] = parse_dates(canon_df["value_date"], fmts, dayfirst)
        elif "date" in canon_df.columns:
            canon_df["cal_date"] = parse_dates(canon_df["date"], fmts, dayfirst)
        else:
            # Try any column literally called 'date' in raw df
            if "date" in df.columns:
                canon_df["cal_date"] = parse_dates(df["date"], fmts, dayfirst)
        # Optional ints/bools
        for col in ("day","month","week","quarter","year"):
            if col in canon_df.columns:
                canon_df[col] = pd.to_numeric(canon_df[col], errors="coerce").astype("Int64")
        for col in ("is_month_end","is_year_end"):
            if col in canon_df.columns:
                canon_df[col] = canon_df[col].astype(str).str.strip().str.lower().isin(("1","true","yes","y"))
        to_keep = [c for c in ("cal_date","day","month","week","quarter","year","is_month_end","is_year_end") if c in canon_df.columns]
        canon_df = canon_df[to_keep]
        # Check parse rate
        if "cal_date" in canon_df.columns:
            rate = float(canon_df["cal_date"].notna().mean())
            if rate < min_date_rate:
                quarantine_write(item["name"], "calendar_low_date_parse", canon_df)
                return 0
        return append_sql(conn, item["target_table"], canon_df)

    # Non-calendar files
    # Parse dates & numbers for known canonical fields
    for dcol in ("value_date","trade_date","settle_date","event_date","evaluation_date"):
        if dcol in canon_df.columns:
            canon_df[dcol] = parse_dates(canon_df[dcol], fmts, dayfirst)
    for ncol in ("qty_raw","price_raw","value_end","inflow","outflow","amount_raw"):
        if ncol in canon_df.columns:
            canon_df[ncol] = parse_numbers(canon_df[ncol])

    # Required field coverage
    req = item.get("required", [])
    missing_rows = pd.Series(False, index=canon_df.index)
    for r in req:
        if r not in canon_df.columns:
            canon_df[r] = pd.NA
        if r in ("value_date","trade_date","settle_date","event_date","evaluation_date"):
            missing_rows |= canon_df[r].isna()
        else:
            missing_rows |= canon_df[r].astype(str).str.strip().eq("")

    # Quarantine missing required
    if missing_rows.any():
        quarantine_write(item["name"], "missing_required", canon_df[missing_rows])
        canon_df = canon_df[~missing_rows]

    # Map canonical -> target columns
    colmap = item.get("map", {})
    out_df = pd.DataFrame()
    for canon, tgt in colmap.items():
        if canon in canon_df.columns:
            out_df[tgt] = canon_df[canon]

    return append_sql(conn, item["target_table"], out_df)

def run_compiler(config_path: Path):
    cfg = load_config(config_path)
    conn = sqlite3.connect(DB)
    totals = {}
    try:
        for t in cfg.get("truncate_before_load", []):
            try:
                conn.execute(f"DELETE FROM {t}")
            except Exception:
                pass
        conn.commit()

        for item in cfg.get("files", []):
            count = process_file(cfg, item, conn)
            totals[item["name"]] = count
    finally:
        conn.commit()
        conn.close()
    return totals

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--config", default=str(BASE / r"source-compiler\config\source_generic.yaml"))
    args = p.parse_args()
    totals = run_compiler(Path(args.config))
    print("Compiler totals:", totals)
    print("Quarantine folder:", QDIR)
