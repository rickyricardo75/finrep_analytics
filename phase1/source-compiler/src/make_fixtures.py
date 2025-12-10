# smoke test: uses your DB as truth, create fixtures (CSV/XLSX/PDF variants) from your DB

import sqlite3
from pathlib import Path
import pandas as pd

BASE = Path(r"C:\Users\Dick\pyproj_finrep\phase1")
OUT  = BASE / r"source-compiler\samples"
OUT.mkdir(exist_ok=True, parents=True)

c = sqlite3.connect(BASE/"phase1.db")

cases = [
    ("Holdings", "SELECT portfolio_nk as 'Portfolio number', value_date as 'Evaluation date', security_nk as 'ISIN', position_id as 'Identification number', security_name as 'Description', qty_raw as 'Quantity / Amount', price_raw as 'Price', native_ccy as 'Trade currency' FROM SRC_Holdings LIMIT 200"),
    ("Movements","SELECT trade_id as 'Transaction number', portfolio_nk as 'Portfolio number', settle_date as 'Value date', trade_date as 'Purchase date', transaction_src as 'Transaction', accounting_src as 'Accounting', price_raw as 'Price', amount_raw as 'Amount', native_ccy as 'Currency' FROM SRC_Movements LIMIT 200"),
    ("DailyValues","SELECT portfolio_nk as 'Portfolio number', value_date as 'Evaluation date', eval_ccy as 'Currency', value_end as 'Value end', inflow as 'Inflow', outflow as 'Outflow' FROM SRC_DailyValues LIMIT 200"),
]
for name, sql in cases:
    df = pd.read_sql_query(sql, c)
    # CSV variants: comma and semicolon
    df.to_csv(OUT/f"{name}_comma.csv", index=False, encoding="utf-8")
    df.to_csv(OUT/f"{name}_semicolon.csv", index=False, sep=";", encoding="cp1252")
    # Excel variant with different sheet/header row
    with pd.ExcelWriter(OUT/f"{name}.xlsx") as xw:
        df.to_excel(xw, sheet_name=name, index=False, startrow=0)
c.close()
print("Fixtures written to:", OUT)

# --- PDF fixture (optional) -----------------------------------------------
# Requires: pip install reportlab
try:
    from reportlab.platypus import SimpleDocTemplate, Table
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors

    def _write_pdf_table(df, out_path):
        data = [list(df.columns)] + df.astype(str).values.tolist()
        doc = SimpleDocTemplate(str(out_path), pagesize=A4, leftMargin=24, rightMargin=24, topMargin=24, bottomMargin=24)
        tbl = Table(data, repeatRows=1)
        # light grid to help lattice extraction
        tbl.setStyle([
            ("GRID", (0,0), (-1,-1), 0.5, colors.black),
            ("BACKGROUND", (0,0), (-1,0), colors.lightgrey),
        ])
        doc.build([tbl])

    # create a small PDF from the first 50 rows of DailyValues fixture
    pdf_src = OUT / "DailyValues.pdf"
    try:
    	base_df = pd.read_csv(OUT / "DailyValues.csv").head(50)
    except Exception:
    	base_df = pd.read_excel(OUT / "DailyValues.xlsx").head(50)
    _write_pdf_table(base_df, pdf_src)
    print(f"PDF fixture written: {pdf_src}")
except Exception as e:
    print(f"[skip PDF] {e}  (install with: pip install reportlab)")

