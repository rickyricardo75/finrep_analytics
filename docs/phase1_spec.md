# Phase-1 Specification — Leman Quest (LMNQ)

## Scope
Phase-1 builds a reproducible pipeline to standardize heterogeneous inputs (CSV/XLSX/PDF) into **SRC_*** 
landing tables, transform to a **Common Data Model (CDM)**, and validate with a Source Compiler.

---

## 1. Data Mapping (Canonical → Table/Column)

### 1.1 Landing (SRC_*)
**SRC_Holdings**
- Keys: `portfolio_nk`, `security_nk`, `position_id`, `value_date`
- Columns:  
  `portfolio_nk (str)`, `security_nk (str)`, `position_id (str)`,  
  `value_date (date)`, `evaluation_date (date)`,  
  `amount_raw (decimal)`, `price_raw (decimal)`,  
  `native_ccy (char3)`, … (optional vendor fields: `price_date`, `asset_class`, `bank`, etc.)
- Semantics: position snapshot per `value_date`. `evaluation_date` is the *file run/extract* date.

**SRC_Movements**
- Keys: `portfolio_nk`, `security_nk`, `trade_date` (or `settle_date`), optional `position_id`
- Columns:  
  `portfolio_nk (str)`, `security_nk (str)`, `trade_date (date)`, `settle_date (date)`,  
  `inflow (decimal)`, `outflow (decimal)`, `amount_raw (decimal)`,  
  `native_ccy (char3)`, other descriptors.
- Semantics: cash/security transactions; if `settle_date` missing, use `trade_date`.

**SRC_DailyValues**
- Keys: `portfolio_nk`, `value_date`
- Columns:  
  `portfolio_nk (str)`, `value_date (date)`, `eval_ccy (char3)`,  
  `value_start (decimal)`, `adj_inflow (decimal)`, `value_end (decimal)`,  
  `inflow (decimal)`, `outflow (decimal)`, `pl_native (decimal)`,  
  `avg_capital (decimal)`, `index_val (decimal)`, `prev_index (decimal)`
- Semantics: portfolio-level NAV / flow timeline in **evaluation currency** (`eval_ccy`).

**SRC_CashAgenda**
- Keys: `portfolio_nk`, `event_date`, optional `security_nk`
- Columns:  
  `event_date (date)`, `evaluation_date (date)`, `portfolio_nk (str)`,  
  `security_nk (str)`, `cash_flow_type_src (str)`, `asset_bucket_src (str)`,  
  `native_ccy (char3)`, `amount_raw (decimal)`,  
  `pre_tax_native (decimal)`, `pre_tax_eval (decimal)`, `after_tax_native (decimal)`
- Semantics: future/actualized cash events; `evaluation_date` is when the file was run.

**SRC_GenericCalendar**
- Columns:  
  `date (date)`, `day (int)`, `month (int)`, `week (int)`, `quarter (int)`, `year (int)`,  
  `is_month_end (bool)`, `is_year_end (bool)`
- Semantics: pure calendar; **no business meaning** attached to `date`. Join semantics are defined in 
  the model layer (see §5).

---

### 1.2 CDM (Harmonized)
**CDM_Portfolio** — portfolio master (nk → sk, attributes: strategy, manager, …)  
**CDM_SecurityMaster** — security attributes (ISIN, SEDOL, ticker, MIC, issuer, class)  
**CDM_Holdings** — normalized positions per date/security/portfolio (SCD-lite)  
**CDM_Transactions** — normalized cash/security transactions  
**CDM_PortfolioDailyValues** — normalized NAV/flow timeseries  
**CDM_CashAgenda** — normalized cash agenda events

> Note: in current build, `CDM_Holdings`/`CDM_Transactions` show `0` due to input scope:
- the CDM build only populated the parts we fully wired (PortfolioDailyValues and CashAgenda). 
- The Holdings and Transactions CDM loads didn’t insert rows because of input/scope gaps—specifically:
  - The CDM insert blocks for Holdings/Transactions are either disabled/placeholder, or
  - Their joins/NOT-NULLs require fields we didn’t standardize yet (e.g., resolving security_sk via 
    SecurityMaster, ensuring required measures/keys like position_id, qty/price, or consistent portfolio_nk), 
    so the inserts filter out all rows.
- Your SRC_Holdings (9,996) and SRC_Movements (9,627) are fine; they just haven’t been transformed into 
  CDM tables yet under the stricter rules. When we expand scope (map securities/portfolios consistently 
  and finalize the CDM insert logic), those CDM tables will fill.
> other tables are populated and validated.

---

## 2. Field-by-Field Dictionary (Core)

### 2.1 Keys & Identifiers 
| Name           | Type    | Tables                                      | Meaning                                  |
|----------------|---------|---------------------------------------------|------------------------------------------|
| `portfolio_nk` | string  | SRC_* , CDM_*                               | Natural key of the portfolio             |
| `security_nk`  | string  | SRC_Holdings, SRC_Movements, SRC_CashAgenda | Natural key of the security (e.g., ISIN) |
| `position_id`  | string  | SRC_Holdings                                | Vendor position identifier               |
---

### 2.2 Dates
| Name               | Type | Tables                         | Meaning                                    |
|--------------------|------|--------------------------------|--------------------------------------------|
| `value_date`       | date | SRC_Holdings, SRC_DailyValues  | Valuation / as-of date                     |
| `evaluation_date`  | date | SRC_*                          | Extraction / run date of the source file   |
| `trade_date`       | date | SRC_Movements                  | Trade date                                 |
| `settle_date`      | date | SRC_Movements                  | Settlement date                            |
| `event_date`       | date | SRC_CashAgenda                 | Cash agenda event date                     |
| `date`             | date | SRC_GenericCalendar            | Pure calendar date (no business meaning)   |
---

### 2.3 Currencies
| Name        | Type   | Tables                                      | Meaning                      |
|-------------|--------|---------------------------------------------|------------------------------|
| `eval_ccy`  | char(3)| SRC_DailyValues                             | Evaluation currency          |
| `native_ccy`| char(3)| SRC_Holdings, SRC_Movements, SRC_CashAgenda | Native (trade) currency      |
---

### 2.4 Amounts, Prices & Flows
| Name            | Type       | Tables                                      | Meaning                      |
|-----------------|------------|---------------------------------------------|------------------------------|
| `amount_raw`    | decimal    | SRC_Holdings, SRC_Movements, SRC_CashAgenda | Raw quantity / amount        |
| `price_raw`     | decimal    | SRC_Holdings                                | Raw price                    |
| `inflow`        | decimal    | SRC_DailyValues, SRC_Movements              | Cash inflow                  |
| `outflow`       | decimal    | SRC_DailyValues, SRC_Movements              | Cash outflow                 |
| `value_start`   | decimal    | SRC_DailyValues                             | Start-of-period value        |
| `adj_inflow`    | decimal    | SRC_DailyValues                             | Adjusted inflows             |
| `value_end`     | decimal    | SRC_DailyValues                             | End-of-period value          |
| `pl_native`     | decimal    | SRC_DailyValues                             | P&L in native terms          |
| `avg_capital`   | decimal    | SRC_DailyValues                             | Average invested capital     |
| `index_val`     | decimal    | SRC_DailyValues                             | Optional derived index value |
| `prev_index`    | decimal    | SRC_DailyValues                             | Prior index value            |
---

### 2.5 Cash Agenda Attributes
| Name               | Type   | Tables          | Meaning                            |
|--------------------|--------|-----------------|------------------------------------|
|`cash_flow_type_src`| string | SRC_CashAgenda  | Vendor cash flow type              |
| `asset_bucket_src` | string | SRC_CashAgenda  | Vendor asset bucket classification |
---

### 2.6 Calendar Attributes
| Name           | Type  | Tables               | Meaning                 |
|----------------|-------|----------------------|-------------------------|
| `day`          | int   | SRC_GenericCalendar  | Day of month            |
| `month`        | int   | SRC_GenericCalendar  | Month number            |
| `week`         | int   | SRC_GenericCalendar  | Week of year            |
| `quarter`      | int   | SRC_GenericCalendar  | Quarter number          |
| `year`         | int   | SRC_GenericCalendar  | Year                    |
| `is_month_end` | bool  | SRC_GenericCalendar  | Month-end flag          |
| `is_year_end`  | bool  | SRC_GenericCalendar  | Year-end flag           |
---

## 3. ERD (Entity–Relationship Diagram) Narrative (high-level)
- **Portfolio (CDM_Portfolio)** 1..N **DailyValues (CDM_PortfolioDailyValues)**
- **Portfolio** 1..N **Holdings (CDM_Holdings)** (by `value_date`)
- **Portfolio** 1..N **Transactions (CDM_Transactions)**
- **Portfolio** 1..N **CashAgenda (CDM_CashAgenda)**
- **SecurityMaster** referenced by **Holdings**/**Transactions**/**CashAgenda** via `security_nk`→`security_sk`
- **GenericCalendar** is a standalone dimension; joined to fact tables by explicit semantics (see §5).
---

## 4. Load Rules
- **Idempotency:** truncate relevant `SRC_*` before re-ingest; CDM builds are repeatable.
- **Date parsing:** robust parser supports exact formats (`%Y-%m-%d`, `%d.%m.%Y`, `%d/%m/%Y`, `%Y%m%d`, 
  `%Y-%m-%d %H:%M:%S`, etc.) and Excel serials; `dayfirst=True`.
- **Encodings:** try `utf-8`, `cp1252`, `latin1` in priority order.
- **Delimiters:** auto-sniff (`sep=None`), fallback to `, ; | \t`.
- **Quarantine policy:** rows failing **required** fields or below **min date parse rate** are written to 
  `phase1\source-compiler\quarantine\*.csv`.
- **PDF:** supported via `pdfplumber`; table extraction may yield blank repeated cells; acceptable for smoke tests.
---

## 5. Calendar Semantics
- Calendar files are **neutral**; `SRC_GenericCalendar.date` is a pure date axis.
- Join semantics are defined in the model/report:
  - DailyValues: join `value_date` → `GenericCalendar.date`
  - Movements: join `COALESCE(settle_date, trade_date)` → `GenericCalendar.date`
  - CashAgenda: join `event_date` → `GenericCalendar.date`
- Historical Power BI wiring (e.g., “ticket date”) is documented for reference; **not** enforced at the table level.
---

## 6. Compiler Configuration (Summary)
- Config: `phase1\source-compiler\config\source_generic.yaml`
- Per file:
  - `path`: relative path under `phase1`
  - `file_type`: `csv|xlsx|pdf`
  - `required`: canonical required fields to keep
  - `map`: canonical→target mapping (resolves header aliases)
  - Calendar files set `calendar: true` and target `SRC_GenericCalendar`
- Global:
  - `encoding_priority`, `delimiter_priority`, `date_format_priority`, `dayfirst_default`
  - `dq_thresholds.min_date_parse_rate` (e.g., `0.90`)
  - `truncate_before_load`: list of `SRC_*` to wipe before load
---

## 7. Runbook (Moves 1→5)
- **Move-1:** create schemas & load reference maps (DDL + seed refs)  
- **Move-2:** load landing `SRC_*` from provided sources  
- **Move-3:** build CDM (dimensions + facts)  
- **Move-4:** housekeeping (snapshot zip + manifest)  
- **Move-5:** Source Compiler (normalize heterogeneous inputs to `SRC_*`)  

**Known-good Phase-1 tag:** `v1-phase1`.
