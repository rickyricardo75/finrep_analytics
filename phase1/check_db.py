# this py is only to check tables work as expected
import sqlite3
c = sqlite3.connect("phase1.db")
cur = c.cursor()
print(cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall())
print("REF_TransactionMap rows:", cur.execute("SELECT COUNT(*) FROM REF_TransactionMap").fetchone()[0])
print("REF_AccountingMap rows:", cur.execute("SELECT COUNT(*) FROM REF_AccountingMap").fetchone()[0])
print("REF_AssetClassMap rows:", cur.execute("SELECT COUNT(*) FROM REF_AssetClassMap").fetchone()[0])
c.close()
