# Phase-1 Runbook (Moves 1→5)

## Prereqs
    cd C:\Users\Dick\pyproj_finrep\phase1
    call C:\Users\Dick\pyproj_finrep\finrep_env\Scripts\activate.bat
    pip install -r requirements_phase1.txt

### Move-1 — Schemas & Refs
    python load_phase1.py --db phase1.db --ddl phase1_schema_ddl.sql --ref_tx ref_transaction_map.csv --ref_acc ref_accounting_map.csv --ref_ac ref_assetclass_map.csv

### Move-2 — Landing (SRC_*)
    python load_src_phase1.py --db phase1.db

### Move-3 — CDM
    python load_cdm_phase1.py

### Move-4 — Snapshot
    python snapshot_phase1.py

### Move-5 — Source Compiler
    cd C:\Users\Dick\pyproj_finrep\phase1\source-compiler\src
    python run_compiler_move5.py
    cd ..\..
    python check_src_counts.py

### Useful one-liners (CMD-safe)

Truncate SRC tables:
    python -c "import sqlite3; c=sqlite3.connect('phase1.db'); c.executescript('DELETE FROM SRC_Holdings;DELETE FROM SRC_Movements;DELETE FROM SRC_DailyValues;DELETE FROM SRC_CashAgenda;DELETE FROM SRC_GenericCalendar;'); c.commit(); c.close(); print('SRC tables truncated.')"

Date ranges:
    python -c "import sqlite3; c=sqlite3.connect('phase1.db'); cur=c.cursor(); print('Holdings:',cur.execute('SELECT MIN(value_date),MAX(value_date) FROM SRC_Holdings').fetchone()); print('DailyValues:',cur.execute('SELECT MIN(value_date),MAX(value_date) FROM SRC_DailyValues').fetchone()); print('Movements:',cur.execute('SELECT MIN(COALESCE(settle_date,trade_date)),MAX(COALESCE(settle_date,trade_date)) FROM SRC_Movements').fetchone()); print('Calendar rows:',cur.execute('SELECT COUNT(*) FROM SRC_GenericCalendar').fetchone()[0]); c.close()"
