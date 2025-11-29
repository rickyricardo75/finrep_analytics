
Phase 1 — Move 1: Create schemas from DDL + load reference maps

Files generated:
- DDL: /mnt/data/phase1_schema_ddl.sql
- Loader script: /mnt/data/load_phase1.py
- Seed CSVs:
  - /mnt/data/ref_transaction_map.csv
  - /mnt/data/ref_accounting_map.csv
  - /mnt/data/ref_assetclass_map.csv

Run (Windows, PyCharm terminal in this folder):
> python load_phase1.py --db phase1.db --ddl phase1_schema_ddl.sql --ref_tx ref_transaction_map.csv --ref_acc ref_accounting_map.csv --ref_ac ref_assetclass_map.csv
