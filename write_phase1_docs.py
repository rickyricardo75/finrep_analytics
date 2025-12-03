from pathlib import Path

root = Path(r"C:\Users\Dick\pyproj_finrep")
docs = root / "docs"
docs.mkdir(exist_ok=True)

README = """# Leman Quest — Financial Reporting (Phase 1)

Phase-1 delivers a reproducible pipeline that consolidates heterogeneous sources into a Common Data Model (CDM) with a Source Compiler to normalize headers, dates, encodings, and delimiters.

## Outcomes
- SQLite DB: `phase1/phase1.db`
- Landing tables: `SRC_*`
- Harmonized tables: `CDM_*`
- Source Compiler (Move-5): `phase1/source-compiler/`

## Moves (1→5)
1) Schemas & refs → DDL + seed reference maps
2) Load landing → raw sources into `SRC_*`
3) Build CDM → dimensions & facts (`CDM_*`)
4) Housekeeping → snapshot + manifest
5) Source Compiler → normalization into `SRC_*` (idempotent)

## Calendars
All calendar files are neutral; they load to `SRC_GenericCalendar(date, …)`. Semantic joins (e.g., evaluation_date, ticket_date) happen later.

## Quick run (Windows CMD)
```bat
cd C:\Users\Dick\pyproj_finrep\phase1
call C:\Users\Dick\pyproj_finrep\finrep_env\Scripts\activate.bat

REM Move-1
python load_phase1.py --db phase1.db --ddl phase1_schema_ddl.sql --ref_tx ref_transaction_map.csv --ref_acc ref_accounting_map.csv --ref_ac ref_assetclass_map.csv

REM Move-2
python load_src_phase1.py --db phase1.db

REM Move-3
python load_cdm_phase1.py

REM Move-5
cd source-compiler\src
python run_compiler_move5.py
cd ..\..

REM Sanity
python check_src_counts.py
