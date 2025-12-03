from pathlib import Path 
import sqlite3, pandas as pd 
import source_compiler as sc 
cfg = sc.load_config(Path(r"C:\Users\Dick\pyproj_finrep\phase1\source-compiler\config\source_generic.yaml")) 
conn = sqlite3.connect(sc.DB) 
def probe(name): 
    item = next(x for x in cfg["files"] if x["name"]==name) 
    p = sc.BASE / item["path"] 
    print("\n===", name, "===") 
    print("path exists:", p.exists(), "->", p) 
    df = sc.load_dataframe(item, cfg) 
    print("raw cols:", list(df.columns), "shape:", df.shape) 
    df_n = sc.normalize_headers(df.copy()) 
    print("norm cols:", list(df_n.columns)) 
    amap = sc.alias_columns(df_n, cfg.get("header_aliases", {})) 
    print("alias map (canon->src):", amap) 
    canon_df = pd.DataFrame({k: df_n[v] for k,v in amap.items()}) 
    print("canon cols:", list(canon_df.columns)) 
    # calendar logic - detect source date 
    if "value_date" in canon_df.columns: 
        cal = sc.parse_dates(canon_df["value_date"], cfg.get("date_format_priority", []), bool(cfg.get("dayfirst_default", True))) 
        print("value_date parse rate:", float(cal.notna().mean())) 
    elif "date" in canon_df.columns: 
        cal = sc.parse_dates(canon_df["date"], cfg.get("date_format_priority", []), bool(cfg.get("dayfirst_default", True))) 
        print("date parse rate:", float(cal.notna().mean())) 
    else: 
        # fallbacks matching compiler behavior 
        if df.shape[1]==1: 
            cal = sc.parse_dates(df.iloc[:,0], cfg.get("date_format_priority", []), bool(cfg.get("dayfirst_default", True))) 
            print("single-col fallback parse rate:", float(cal.notna().mean())) 
        else: 
            cal = None; print("no date column found") 
    # run the real process and report inserted count 
    cnt = sc.process_file(cfg, item, conn) 
    print("inserted rows:", cnt) 
for n in ["DailyValuesCalendar","MovementsCalendar","CashAgendaCalendar"]: 
    probe(n) 
conn.close() 
