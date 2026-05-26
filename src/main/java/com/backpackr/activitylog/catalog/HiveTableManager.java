package com.backpackr.activitylog.catalog;

import com.backpackr.activitylog.config.ActivityLogConfig;
import org.apache.spark.sql.SparkSession;

/**
 * External Hive 테이블 생애주기 관리.
 *
 * CREATE EXTERNAL TABLE IF NOT EXISTS 라서 매 실행마다 호출해도 안전.
 * LOCATION 이 Hive warehouse 밖에 있어서 DROP TABLE 해도 데이터 파일은
 * 남는다 → 메타데이터 실수로부터 데이터를 보호.
 *
 * 추가 기간 백필도 같은 잡을 새 인자로 재실행 + MSCK REPAIR 만으로 끝.
 */
public final class HiveTableManager {

    private HiveTableManager() {}

    public static void createExternalIfMissing(SparkSession spark, ActivityLogConfig c) {
        spark.sql("CREATE DATABASE IF NOT EXISTS " + c.database());
        String ddl = """
                CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
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
                LOCATION '%s'
                TBLPROPERTIES (
                  'parquet.compression'  = 'SNAPPY',
                  'external.table.purge' = 'false'
                )
                """.formatted(c.database(), c.tableName(), c.warehousePath());
        spark.sql(ddl);
    }

    /** MSCK REPAIR 는 파일시스템에 존재하는 파티션을 메타스토어에 추가만 한다 (멱등). */
    public static void repairPartitions(SparkSession spark, ActivityLogConfig c) {
        spark.sql("MSCK REPAIR TABLE " + c.database() + "." + c.tableName());
    }
}
