-- External table DDL — kept here as documentation; the runtime equivalent
-- is created by HiveTableManager.createExternalIfMissing.
-- ${database}, ${table}, ${warehouse_path} 를 환경에 맞게 치환하여 사용.

CREATE DATABASE IF NOT EXISTS ${database};

CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (
  event_time_utc       TIMESTAMP,
  event_time_kst       TIMESTAMP,
  event_type           STRING,
  product_id           BIGINT,
  category_id          BIGINT,
  category_code        STRING,
  brand                STRING,
  price                DOUBLE,
  user_id              BIGINT,
  source_user_session  STRING,
  session_id           STRING,
  session_start_kst    TIMESTAMP
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION '${warehouse_path}'
TBLPROPERTIES (
  'parquet.compression'  = 'SNAPPY',
  'external.table.purge' = 'false'
);

-- 신규 파티션 등록 (재처리 후 호출).
MSCK REPAIR TABLE ${database}.${table};
