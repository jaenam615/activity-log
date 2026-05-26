package com.example.activitylog.catalog;

import com.example.activitylog.config.ActivityLogConfig;
import org.apache.spark.sql.SparkSession;

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

    public static void repairPartitions(SparkSession spark, ActivityLogConfig c) {
        spark.sql("MSCK REPAIR TABLE " + c.database() + "." + c.tableName());
    }
}
