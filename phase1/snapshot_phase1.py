import zipfile, time, sys, hashlib, os
from pathlib import Path
from datetime import datetime

BASE = Path(r"C:\Users\Dick\pyproj_finrep\phase1")
DB   = BASE / "phase1.db"

# Save the zip at project root to keep phase1 clean (change to BASE if you prefer inside the folder)
OUT  = BASE.parent / f"phase1_snapshot_{time.strftime('%Y%m%d_%H%M')}.zip"

def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def gather_db_summary(db_path: Path) -> str:
    import sqlite3
    lines = []
    if not db_path.exists():
        return "DB not found; skipping DB summary."
    try:
        conn = sqlite3.connect(db_path)
        cur  = conn.cursor()
        # table counts
        tables = ["SRC_Holdings","SRC_Movements","SRC_DailyValues","SRC_CashAgenda","SRC_GenericCalendar",
                  "CDM_Portfolio","CDM_SecurityMaster","CDM_Holdings","CDM_PortfolioDailyValues","CDM_Transactions","CDM_CashAgenda"]
        lines.append("Table counts:")
        for t in tables:
            try:
                cnt = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                lines.append(f"  {t:28s} {cnt}")
            except Exception as e:
                lines.append(f"  {t:28s} n/a ({e})")
        # date ranges
        lines.append("")
        lines.append("Date ranges:")
        checks = [
            ("CDM_Holdings","value_date"),
            ("CDM_PortfolioDailyValues","value_date"),
            ("CDM_Transactions","trade_date"),
            ("CDM_Transactions","settle_date"),
            ("CDM_CashAgenda","event_date"),
        ]
        for t,c in checks:
            try:
                mn,mx = cur.execute(f"SELECT MIN({c}), MAX({c}) FROM {t}").fetchone()
                lines.append(f"  {t:28s} {c:14s} {mn} .. {mx}")
            except Exception as e:
                lines.append(f"  {t:28s} {c:14s} n/a ({e})")
        conn.close()
    except Exception as e:
        lines.append(f"DB summary error: {e}")
    return "\n".join(lines)

def build_manifest() -> str:
    lines = []
    lines.append("==== Leman Quest – Phase 1 Snapshot MANIFEST ====")
    lines.append(f"Created at     : {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"Snapshot file  : {OUT}")
    lines.append(f"Python         : {sys.version.split()[0]}  ({sys.executable})")
    lines.append(f"Working folder : {BASE}")
    lines.append("")
    # inventory
    lines.append("Included files:")
    include = []
    # top-level files
    for name in ["phase1.db"]:
        p = BASE / name
        if p.exists(): include.append(p)
    include += list(BASE.glob("*.py"))
    include += list(BASE.glob("*.sql"))
    for folder in ["reports", "data/source"]:
        p = BASE / folder
        if p.exists():
            include += [f for f in p.rglob("*") if f.is_file()]
    for f in sorted(include):
        try:
            size = f.stat().st_size
            digest = sha256_of(f)
            lines.append(f"  {f.relative_to(BASE)} | {size} bytes | sha256={digest}")
        except Exception as e:
            lines.append(f"  {f.relative_to(BASE)} | error: {e}")
    lines.append("")
    lines.append(gather_db_summary(DB))
    lines.append("")
    lines.append("Notes:")
    lines.append("- This snapshot is a portable backup of DB, scripts, reports, and sources.")
    lines.append("- Restore by unzipping into a workspace; run scripts with the same Python if possible.")
    return "\n".join(lines)

manifest_text = build_manifest()

with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
    # write MANIFEST.txt first
    z.writestr("MANIFEST.txt", manifest_text)
    # top-level
    for name in ["phase1.db"]:
        p = BASE / name
        if p.exists():
            z.write(p, arcname=p.name)
    for p in BASE.glob("*.py"):
        z.write(p, arcname=p.name)
    for p in BASE.glob("*.sql"):
        z.write(p, arcname=p.name)
    # folders
    for folder in ["reports", "data/source"]:
        p = BASE / folder
        if p.exists():
            for f in p.rglob("*"):
                if f.is_file():
                    z.write(f, arcname=str(f.relative_to(BASE)))

print(f"Snapshot written to: {OUT}")
