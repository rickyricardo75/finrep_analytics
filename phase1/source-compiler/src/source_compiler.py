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
            # Auto-sniff delimiter
            return pd.read_csv(path, sep=None, engine="python", encoding=enc)
        except Exception:
            for d in delims:
                try:
                    return pd.read_csv(path, sep=d, engine="python", encoding=enc)
                except Exception:
                    continue
    raise RuntimeError(f"Failed to read {path} with encodings={encs} and delims={delims}")

# Start udpate - Reader plugin (Excel + basic PDF hook)
def read_excel(path, sheet=0, header_row=0, skiprows=None, encs=None, delims=None):
    # Multi-row headers flattening is handled after read
    df = pd.read_excel(path, sheet_name=sheet, header=header_row, skiprows=skiprows or [])
    # If header became a MultiIndex, join with ' '
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(x) for x in tup if str(x)!='nan']).strip() for tup in df.columns]
    return df

def read_pdf_tables(path, pages="1", flavor="lattice"):
    # Keep simple to avoid heavy system deps: try pdfplumber first (pip install pdfplumber)
    try:
        import pdfplumber
    except Exception:
        raise RuntimeError("PDF support requires 'pip install pdfplumber' (or use CSV/XLSX).")
    out = []
    with pdfplumber.open(str(path)) as pdf:
        sel = []
        if isinstance(pages,str):
            # parse simple "1,3-5"
            for chunk in pages.split(","):
                if "-" in chunk:
                    a,b = chunk.split("-")
                    sel += list(range(int(a)-1, int(b)))  # 0-based
                else:
                    sel.append(int(chunk)-1)
        else:
            sel = [p-1 for p in pages]
        for i in sel:
            table = pdf.pages[i].extract_table()
            if table and len(table)>1:
                hdr = table[0]
                rows = table[1:]
                out.append(pd.DataFrame(rows, columns=hdr))
    if not out:
        return pd.DataFrame()
    return pd.concat(out, ignore_index=True)

def load_dataframe(item, cfg):
    path = BASE / item["path"]
    encs = cfg.get("encoding_priority", ["utf-8","cp1252","latin1"])
    delims = cfg.get("delimiter_priority", [",",";","|","\t"])
    ftype = (item.get("file_type") or "csv").lower()

    if ftype == "csv":
        # allow per-file override as string or list
        enc_override = item.get("encoding")
        del_override = item.get("delimiter")

        enc_list = (
            enc_override if isinstance(enc_override, list)
            else ([enc_override] if enc_override else [])
        )
        if enc_list:
            encs = enc_list + [e for e in encs if e not in enc_list]

        del_list = (
            del_override if isinstance(del_override, list)
            else ([del_override] if del_override else [])
        )
        if del_list:
            delims = del_list + [d for d in delims if d not in del_list]

        # try sniff first, then forced delimiters across encodings
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=encs[0])
        except Exception:
            pass

        last_err = None
        for enc in encs:
            for d in delims:
                try:
                    return pd.read_csv(path, sep=d, engine="python", encoding=enc)
                except Exception as e:
                    last_err = e
                    continue
        raise last_err or RuntimeError(f"Failed to read {path}")

    elif ftype in ("xlsx","xls"):
        return read_excel(path, sheet=item.get("sheet", cfg.get("file_defaults",{}).get("sheet",0)),
                          header_row=item.get("header_row", cfg.get("file_defaults",{}).get("header_row",0)),
                          skiprows=item.get("skiprows", cfg.get("file_defaults",{}).get("skiprows",[])))
    elif ftype == "pdf":
        return read_pdf_tables(path,
                               pages=item.get("pdf_pages", cfg.get("file_defaults",{}).get("pdf_pages","1")),
                               flavor=item.get("pdf_flavor", cfg.get("file_defaults",{}).get("pdf_flavor","lattice")))
    else:
        raise RuntimeError(f"Unsupported file_type={ftype} for {item.get('name')}")

# end update

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
    mask = serial.ge(25569) & serial.lt(80000)
    vals.loc[mask] = pd.to_datetime("1899-12-30") + pd.to_timedelta(serial[mask], unit="D")

    remain = vals.isna()

    # Exact formats (dates + datetimes commonly seen)
    fmts_ext = fmts + ["%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S", "%Y.%m.%d %H.%M.%S"]
    for f in fmts_ext:
        hit = pd.to_datetime(s0[remain], format=f, errors="coerce")
        vals.loc[remain] = vals.loc[remain].fillna(hit)
        remain = vals.isna()
        if not remain.any():
            break

    # ISO 8601 (with timezone)
    if remain.any():
        vals.loc[remain] = pd.to_datetime(s0[remain], errors="coerce", utc=False, dayfirst=dayfirst)

    return vals.dt.date  # truncate to date for SRC/CDM


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

    df = load_dataframe(item, cfg)
    # Guard against sniffer splitting 'Date' -> 'Da'/'te'
    if item.get("calendar", False) and df.shape[1] == 2:
        cols_l = [str(c).strip().lower() for c in df.columns]
        if set(cols_l) == {"da", "te"} or set(cols_l) == {"da", "e"}:
            df = pd.DataFrame({"Date": df.iloc[:, 0].astype(str) + df.iloc[:, 1].astype(str)})

    df = normalize_headers(df)

    alias_map = alias_columns(df, hdr_aliases)
    # Build canonical dataframe from alias map
    canon_df = pd.DataFrame()
    for canon, df_col in alias_map.items():
        canon_df[canon] = df[df_col]

    # --- REQUIRED-ANY SUPPORT (e.g., value_date OR evaluation_date) ---
    req_any = item.get("required_any", [])
    # normalize date columns early
    for dcol in ("value_date","evaluation_date","trade_date","settle_date","event_date"):
        if dcol in canon_df.columns:
            canon_df[dcol] = parse_dates(canon_df[dcol], fmts, dayfirst)

    # rows that satisfy at least one in each OR-group
    ok_mask = pd.Series(True, index=canon_df.index)
    for group in req_any:
        group_ok = pd.Series(False, index=canon_df.index)
        for col in group:
            if col in canon_df.columns:
                if col in ("value_date","evaluation_date","trade_date","settle_date","event_date"):
                    group_ok |= canon_df[col].notna()
                else:
                    group_ok |= canon_df[col].astype(str).str.strip().ne("")
        ok_mask &= group_ok

    # apply standard required (AND) checks
    req = item.get("required", [])
    missing_rows = pd.Series(False, index=canon_df.index)
    for r in req:
        if r not in canon_df.columns:
            canon_df[r] = pd.NA
        if r in ("value_date","trade_date","settle_date","event_date","evaluation_date"):
            missing_rows |= canon_df[r].isna()
        else:
            missing_rows |= canon_df[r].astype(str).str.strip().eq("")

    # combine: row is bad if AND-missing OR fails any OR-group
    missing_rows |= ~ok_mask

    # If value_date is still missing but evaluation_date is present, map it just-in-time for SRC schema
    if "value_date" in canon_df.columns or "evaluation_date" in canon_df.columns:
        vd = canon_df.get("value_date")
        ed = canon_df.get("evaluation_date")
        if vd is None:
            canon_df["value_date"] = ed
        else:
            canon_df["value_date"] = vd.where(vd.notna(), ed)

    # Quarantine missing required
    if missing_rows.any():
        quarantine_write(item["name"], "missing_required", canon_df[missing_rows])
        canon_df = canon_df[~missing_rows]

    # Calendar files: normalize to SRC_GenericCalendar
    if item.get("calendar", False):
        # If aliasing didn't give us value_date or date, but the source file is a single-column calendar,
        # take that lone column as the date column deterministically.
        if "value_date" not in canon_df.columns and "date" not in canon_df.columns:
            if df.shape[1] == 1:
                # take first (and only) column as 'date'
                canon_df["date"] = df.iloc[:, 0]
            elif "date" in df.columns:
                canon_df["date"] = df["date"]

        # detect a date column (canonical or raw)
        if "value_date" in canon_df.columns:
            canon_df["date"] = parse_dates(canon_df["value_date"], fmts, dayfirst)
        elif "date" in canon_df.columns:
            canon_df["date"] = parse_dates(canon_df["date"], fmts, dayfirst)
        else:
            # last resort: if source still has exactly one column, use it
            if df.shape[1] == 1:
                canon_df["date"] = parse_dates(df.iloc[:, 0], fmts, dayfirst)

        # Optional ints/bools
        for col in ("day","month","week","quarter","year"):
            if col in canon_df.columns:
                canon_df[col] = pd.to_numeric(canon_df[col], errors="coerce").astype("Int64")
        for col in ("is_month_end","is_year_end"):
            if col in canon_df.columns:
                canon_df[col] = canon_df[col].astype(str).str.strip().str.lower().isin(("1","true","yes","y"))
        to_keep = [c for c in ("date","day","month","week","quarter","year","is_month_end","is_year_end") if c in canon_df.columns]
        canon_df = canon_df[to_keep]

        # Check parse rate
        if "date" in canon_df.columns:
            rate = float(canon_df["date"].notna().mean())
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
