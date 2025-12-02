import sqlite3
c=sqlite3.connect("phase1.db");cur=c.cursor()
print("SRC_Holdings:",cur.execute("SELECT COUNT(*) FROM SRC_Holdings").fetchone()[0])
print("SRC_Movements:",cur.execute("SELECT COUNT(*) FROM SRC_Movements").fetchone()[0])
print("SRC_DailyValues:",cur.execute("SELECT COUNT(*) FROM SRC_DailyValues").fetchone()[0])
print("SRC_CashAgenda:",cur.execute("SELECT COUNT(*) FROM SRC_CashAgenda").fetchone()[0])
print("SRC_GenericCalendar:",cur.execute("SELECT COUNT(*) FROM SRC_GenericCalendar").fetchone()[0])
c.close()
