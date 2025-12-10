from pathlib import Path

root = Path(r"C:\Users\Dick\pyproj_finrep")
docs = root / "docs"
docs.mkdir(exist_ok=True)

README = (
"# Leman Quest — Financial Reporting (Phase 1)\n\n"
"Phase-1 delivers a reproducible pipeline that consolidates heterogeneous sources into a Common Data Model (CDM) with a "
"Source Compiler to normalize headers, dates, encodings, and delimiters.\n\n"
"## Outcomes\n"
"- SQLite DB: `phase1/phase1.db`\n"
"- Landing tables: `SRC_*`\n"
"- Harmonized tables: `CDM_*`\n"
"- Source Compiler (Move-5): `phase1/source-compiler/`\n\n"
"## Moves (1→5)\n"
"1) Schemas & refs → DDL + seed reference maps\n"
"2) Load landing → raw sources into `SRC_*`\n"
"3) Build CDM → dimensions & facts (`CDM_*`)\n"
"4) Housekeeping → snapshot + manifest\n"
"5) Source Compiler → normalization into `SRC_*` (idempotent)\n\n"
"## Calendars\n"
"All calendar files are neutral; they load to `SRC_GenericCalendar(date, …)`. Semantic joins (e.g., evaluation_date, ticket_date) happen later.\n\n"
"## Quick run (Windows CMD)\n"
"    cd C:\\Users\\Dick\\pyproj_finrep\\phase1\n"
"    call C:\\Users\\Dick\\pyproj_finrep\\finrep_env\\Scripts\\activate.bat\n"
"    python load_phase1.py --db phase1.db --ddl phase1_schema_ddl.sql --ref_tx ref_transaction_map.csv --ref_acc ref_accounting_map.csv --ref_ac ref_assetclass_map.csv\n"
"    python load_src_phase1.py --db phase1.db\n"
"    python load_cdm_phase1.py\n"
"    cd source-compiler\\src && python run_compiler_move5.py && cd ..\\..\n"
"    python check_src_counts.py\n"
)

CONTRIB = (
"# CONTRIBUTING (Phase 1)\n\n"
"## Environment\n"
"- Windows CMD only.\n"
"- Python 3.11.x in `C:\\Users\\Dick\\pyproj_finrep\\finrep_env`.\n\n"
"## Ground rules\n"
"1. Don’t rename functions/vars used by downstream steps.\n"
"2. Prefer config/script changes over schema edits.\n"
"3. Keep loads idempotent (truncate relevant `SRC_*` first when re-ingesting).\n"
"4. No large binaries in Git (envs, wheels, .pyd, .exe).\n\n"
"## Git (feature → PR) — template for later\n"
"    cd C:\\Users\\Dick\\pyproj_finrep\n"
"    git status\n"
"    git fetch origin\n"
"    git checkout -B feature/your-change origin/main\n"
"    git add -A\n"
"    git commit -m \"Your change: what/why\"\n"
"    git pull --rebase origin main\n"
"    git push -u origin feature/your-change\n\n"
"## Checks before PR\n"
"- `python check_src_counts.py` shows expected non-zero SRC counts.\n"
"- CDM built (`python load_cdm_phase1.py`) with sane date ranges.\n"
"- Quarantine only contains justified rejects.\n"
)

RUNBOOK = (
"# Phase-1 Runbook (Moves 1→5)\n\n"
"## Prereqs\n"
"    cd C:\\Users\\Dick\\pyproj_finrep\\phase1\n"
"    call C:\\Users\\Dick\\pyproj_finrep\\finrep_env\\Scripts\\activate.bat\n"
"    pip install -r requirements_phase1.txt\n\n"
"### Move-1 — Schemas & Refs\n"
"    python load_phase1.py --db phase1.db --ddl phase1_schema_ddl.sql --ref_tx ref_transaction_map.csv --ref_acc ref_accounting_map.csv --ref_ac ref_assetclass_map.csv\n\n"
"### Move-2 — Landing (SRC_*)\n"
"    python load_src_phase1.py --db phase1.db\n\n"
"### Move-3 — CDM\n"
"    python load_cdm_phase1.py\n\n"
"### Move-4 — Snapshot\n"
"    python snapshot_phase1.py\n\n"
"### Move-5 — Source Compiler\n"
"    cd C:\\Users\\Dick\\pyproj_finrep\\phase1\\source-compiler\\src\n"
"    python run_compiler_move5.py\n"
"    cd ..\\..\n"
"    python check_src_counts.py\n\n"
"### Useful one-liners (CMD-safe)\n\n"
"Truncate SRC tables:\n"
"    python -c \"import sqlite3; c=sqlite3.connect('phase1.db'); c.executescript('DELETE FROM SRC_Holdings;DELETE FROM SRC_Movements;DELETE FROM SRC_DailyValues;DELETE FROM SRC_CashAgenda;DELETE FROM SRC_GenericCalendar;'); c.commit(); c.close(); print('SRC tables truncated.')\"\n\n"
"Date ranges:\n"
"    python -c \"import sqlite3; c=sqlite3.connect('phase1.db'); cur=c.cursor(); print('Holdings:',cur.execute('SELECT MIN(value_date),MAX(value_date) FROM SRC_Holdings').fetchone()); print('DailyValues:',cur.execute('SELECT MIN(value_date),MAX(value_date) FROM SRC_DailyValues').fetchone()); print('Movements:',cur.execute('SELECT MIN(COALESCE(settle_date,trade_date)),MAX(COALESCE(settle_date,trade_date)) FROM SRC_Movements').fetchone()); print('Calendar rows:',cur.execute('SELECT COUNT(*) FROM SRC_GenericCalendar').fetchone()[0]); c.close()\"\n"
)

(root / "README.md").write_text(README, encoding="utf-8")
(root / "CONTRIBUTING.md").write_text(CONTRIB, encoding="utf-8")
(docs / "phase1_runbook.md").write_text(RUNBOOK, encoding="utf-8")

print("Wrote: README.md, CONTRIBUTING.md, docs/phase1_runbook.md")
