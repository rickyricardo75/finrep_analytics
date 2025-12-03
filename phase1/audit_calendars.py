import os, pandas as pd, csv, codecs 
from pathlib import Path 
BASE=Path(r"C:\Users\Dick\pyproj_finrep\phase1") 
FILES=[ 
  ("DailyValuesCalendar", BASE/"data/source/calendars/DailyValuesCalendar.csv"), 
  ("MovementsCalendar",  BASE/"data/source/calendars/MovementsCalendar.csv"), 
  ("CashAgendaCalendar", BASE/"data/source/calendars/CashAgendaCalendar.csv"), 
] 
def sniff(p): 
  try: df=pd.read_csv(p, sep=None, engine="python", nrows=5); return df 
  except Exception as e: return e 
def read_forced(p, sep, enc): 
  try: return pd.read_csv(p, sep=sep, encoding=enc, nrows=5) 
  except Exception as e: return e 
def read_firstcol_named_date(p, enc): 
  try: return pd.read_csv(p, usecols=[0], header=0, names=["Date"], encoding=enc) 
  except Exception as e: return e 
def bom(p): 
  with open(p, "rb") as f: head=f.read(3); return head==codecs.BOM_UTF8 
for name, path in FILES: 
  print("="*72); print(name, "->", path) 
  if not path.exists(): print("MISSING"); continue 
  print("size(bytes)=", path.stat().st_size, "BOM_UTF8=", bom(path)) 
  # raw first 2 lines 
  with open(path, "r", encoding="utf-8", errors="ignore") as f: 
    raw=[next(f,"").rstrip("\n") for _ in range(2)] 
  print("raw[0] header:", raw[0]); print("raw[1] sample:", raw[1]) 
  # pandas views 
  df_sniff = sniff(path) 
  if isinstance(df_sniff, Exception): print("sniff ERR:", df_sniff) 
  else: print("sniff cols:", list(df_sniff.columns), "shape:", df_sniff.shape) 
  df_comma = read_forced(path, ",", "utf-8") 
  print("comma cols:", list(df_comma.columns) if not isinstance(df_comma,Exception) else ("ERR",df_comma)) 
  df_sc    = read_forced(path, ";", "utf-8") 
  print("semi  cols:", list(df_sc.columns)    if not isinstance(df_sc,Exception)    else ("ERR",df_sc)) 
  df_named = read_firstcol_named_date(path, "utf-8") 
  if not isinstance(df_named, Exception): 
    print("named-firstcol cols:", list(df_named.columns), "rows:", len(df_named)) 
    d=pd.to_datetime(df_named["Date"], errors="coerce", dayfirst=True) 
    print("named-firstcol parse%%:", float(d.notna().mean())) 
  else: 
    print("named-firstcol ERR:", df_named) 
