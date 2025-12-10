# Windows CMD Cookbook — Phase-1

> Windows CMD only. No PowerShell. One-liners only.

## Environment
- Activate venv:  
  `call C:\Users\Dick\pyproj_finrep\finrep_env\Scripts\activate.bat`
- Go to phase1:  
  `cd C:\Users\Dick\pyproj_finrep\phase1`

## Moves 1→5
1) Schemas + refs  
`python load_phase1.py --db phase1.db --ddl phase1_schema_ddl.sql --ref_tx ref_transaction_map.csv 
 --ref_acc ref_accounting_map.csv --ref_ac ref_assetclass_map.csv`

2) Landing (SRC)  
`python load_src_phase1.py --db phase1.db`

3) CDM build  
`python load_cdm_phase1.py`

4) Snapshot  
`python snapshot_phase1.py`

5) Compiler (normalize heterogenous sources)  
`cd source-compiler\src && python run_compiler_move5.py && cd ..\..`

## Checks
- SRC counts:  
  `python check_src_counts.py`
- Date ranges (one-shot):  
  `python -c "import sqlite3; c=sqlite3.connect('phase1.db'); cur=c.cursor(); print('Holdings:',cur.execute('SELECT MIN(value_date),MAX(value_date) FROM SRC_Holdings').fetchone()); print('DailyValues:',cur.execute('SELECT MIN(value_date),MAX(value_date) FROM SRC_DailyValues').fetchone()); print('Movements:',cur.execute('SELECT MIN(COALESCE(settle_date,trade_date)),MAX(COALESCE(settle_date,trade_date)) FROM SRC_Movements').fetchone()); print('Calendar rows:',cur.execute('SELECT COUNT(*) FROM SRC_GenericCalendar').fetchone()[0]); c.close()"`

## Maintenance
- Truncate SRC tables (idempotent reload):  
  `python -c "import sqlite3; c=sqlite3.connect('phase1.db'); c.executescript('DELETE FROM SRC_Holdings;DELETE FROM SRC_Movements;DELETE FROM SRC_DailyValues;DELETE FROM SRC_CashAgenda;DELETE FROM SRC_GenericCalendar;'); c.commit(); c.close(); print('SRC tables truncated.')"`

## Git (feature → PR → tag)
- Push feature branch:  
  `git push -u origin feature/source-compiler-v1`
- Open PR:  
  `start https://github.com/rickyricardo75/finrep_analytics/compare/main...feature/source-compiler-v1?expand=1`
- After merge, create tag:  
  `git fetch origin & git checkout main & git pull & git tag v1-phase1 & git push origin v1-phase1`
