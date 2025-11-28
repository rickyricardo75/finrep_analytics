
-- Phase 1 Schema DDL (SRC, CDM, REF) — subset to start
CREATE TABLE IF NOT EXISTS REF_TransactionMap (
  transaction_src VARCHAR(128) PRIMARY KEY,
  txn_type VARCHAR(32) NOT NULL
);
CREATE TABLE IF NOT EXISTS REF_AccountingMap (
  accounting_src VARCHAR(128) PRIMARY KEY,
  posting_class VARCHAR(64) NOT NULL
);
CREATE TABLE IF NOT EXISTS REF_AssetClassMap (
  asset_bucket_src VARCHAR(64) PRIMARY KEY,
  class_name VARCHAR(128)
);
CREATE TABLE IF NOT EXISTS SRC_GenericCalendar (
  cal_date DATE,
  day INT,
  month INT,
  week INT,
  quarter INT,
  year INT,
  is_month_end BOOLEAN,
  is_year_end BOOLEAN
);
CREATE TABLE IF NOT EXISTS SRC_Holdings (
  portfolio_nk VARCHAR(64),
  value_date DATE,
  security_nk VARCHAR(64),
  position_id VARCHAR(64),
  security_name VARCHAR(512),
  qty_raw DECIMAL(38,10),
  price_raw DECIMAL(38,10),
  native_ccy CHAR(3)
);
CREATE TABLE IF NOT EXISTS SRC_Movements (
  trade_id VARCHAR(64),
  portfolio_nk VARCHAR(64),
  settle_date DATE,
  trade_date DATE,
  transaction_src VARCHAR(128),
  accounting_src VARCHAR(128),
  price_raw DECIMAL(38,10),
  amount_raw DECIMAL(38,10),
  native_ccy CHAR(3)
);
CREATE TABLE IF NOT EXISTS SRC_DailyValues (
  portfolio_nk VARCHAR(64),
  value_date DATE,
  eval_ccy CHAR(3),
  value_end DECIMAL(38,10),
  inflow DECIMAL(38,10),
  outflow DECIMAL(38,10)
);
CREATE TABLE IF NOT EXISTS SRC_CashAgenda (
  event_date DATE,
  evaluation_date DATE,
  portfolio_nk VARCHAR(64),
  security_nk VARCHAR(64),
  cash_flow_type_src VARCHAR(64),
  asset_bucket_src VARCHAR(64),
  native_ccy CHAR(3),
  amount_raw DECIMAL(38,10)
);
CREATE TABLE IF NOT EXISTS CDM_Portfolio (
  portfolio_sk INTEGER PRIMARY KEY AUTOINCREMENT,
  portfolio_nk VARCHAR(64) NOT NULL,
  name VARCHAR(256),
  base_ccy CHAR(3)
);
CREATE TABLE IF NOT EXISTS CDM_Calendar (
  cal_date DATE PRIMARY KEY,
  market VARCHAR(16) DEFAULT 'GEN',
  is_business_day BOOLEAN,
  is_month_end BOOLEAN,
  is_quarter_end BOOLEAN,
  is_year_end BOOLEAN,
  day INT, week INT, month INT, quarter INT, year INT
);
CREATE TABLE IF NOT EXISTS CDM_PortfolioDailyValues (
  pdv_sk INTEGER PRIMARY KEY AUTOINCREMENT,
  value_date DATE NOT NULL,
  portfolio_sk INTEGER NOT NULL,
  eval_ccy CHAR(3),
  value_start DECIMAL(38,10),
  value_end DECIMAL(38,10),
  inflow DECIMAL(38,10),
  outflow DECIMAL(38,10)
);
CREATE TABLE IF NOT EXISTS CDM_Holdings (
  holding_sk INTEGER PRIMARY KEY AUTOINCREMENT,
  value_date DATE NOT NULL,
  portfolio_sk INTEGER NOT NULL,
  security_sk INTEGER,
  qty DECIMAL(38,10),
  price DECIMAL(38,10),
  mv_native DECIMAL(38,10),
  mv_base DECIMAL(38,10)
);
CREATE TABLE IF NOT EXISTS CDM_Transactions (
  txn_sk INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id VARCHAR(64) NOT NULL,
  trade_date DATE,
  settle_date DATE,
  portfolio_sk INTEGER NOT NULL,
  security_sk INTEGER,
  txn_type VARCHAR(32) NOT NULL,
  qty DECIMAL(38,10),
  price DECIMAL(38,10),
  gross_amt_native DECIMAL(38,10),
  gross_amt_base DECIMAL(38,10)
);
CREATE TABLE IF NOT EXISTS CDM_CashAgenda (
  cag_sk INTEGER PRIMARY KEY AUTOINCREMENT,
  event_date DATE NOT NULL,
  evaluation_date DATE,
  portfolio_sk INTEGER NOT NULL,
  security_sk INTEGER,
  cash_type VARCHAR(32) NOT NULL,
  native_ccy CHAR(3),
  amt_native DECIMAL(38,10),
  amt_base DECIMAL(38,10)
);
