# Python Components — Phase-1

## Root / Phase-1
- **load_phase1.py** — Creates DB, applies DDLs, loads reference maps (transaction/accounting/asset-class).
- **load_src_phase1.py** — Loads raw CSVs into `SRC_*` with robust date/number parsing; idempotent (truncate list).
- **load_cdm_phase1.py** — Builds CDM tables (dimensions + facts) from `SRC_*`; enforces keys & NOT NULL constraints.
- **migrate_sqlite_schema_all.py** — One-shot migration to add/alter columns in CDM for compatibility.
- **snapshot_phase1.py** — Zips DB + scripts + configs into a dated snapshot with a MANIFEST.
- **check_src_counts.py** — Prints row counts for `SRC_*` (landing tables); used for quick validation.
- **audit_calendars.py** — Deep probe: shows header lines, size, sniffed delimiters, parse rates for calendars.
- **debug_calendar_probe.py** — Minimal reproducer for calendar parsing (imports compiler functions).

## Source Compiler (`phase1\source-compiler\src`)
- **source_compiler.py** — Engine  
  - Config loader (YAML/JSON)  
  - Multi-format readers (CSV, XLSX, PDF via `pdfplumber`)  
  - Header normalization + alias resolution  
  - Date & number parsing (Excel serials, EU/US styles)  
  - Calendar handling (`calendar: true` → `SRC_GenericCalendar`)  
  - DQ: required-field filtering & quarantine  
  - Append to target tables; idempotent truncation per config
- **run_compiler_move5.py** — Entrypoint that loads config and runs the compiler.
- **make_fixtures.py** — Generates synthetic fixtures from DB; now also emits a small **PDF** table (smoke test).
- **config\source_generic.yaml** — File list, header aliases, required columns, mapping, DQ thresholds, and truncate list.
- **quarantine\*** — Rejected rows (missing required, low date-parse rate, etc.) for manual review.

## Dependencies (Phase-1)
`pandas`, `openpyxl`, `pdfplumber`, `pyyaml`
