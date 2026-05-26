package com.example.activitylog.io;

import com.example.activitylog.config.ActivityLogConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * event_date 별로 파티션을 나눠 parquet + snappy 로 저장.
 *
 * 핵심 설정:
 *   SparkSession.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
 *   덕분에 동일 날짜 범위로 재실행하면 그 파티션 디렉터리만 통째로 교체되고
 *   다른 파티션은 손대지 않는다 → 재처리/장애복구가 idempotent.
 */
public final class ActivityLogWriter {

    private ActivityLogWriter() {}

    public static void writePartitioned(Dataset<Row> df, ActivityLogConfig c) {
        // CLI 로 지정된 기간만 추출. (CSV 가 월 단위라 11/01 이전 데이터가 함께
        // 들어와도 안전하게 잘려나간다.)
        Dataset<Row> filtered = df
                .filter(col("event_date").geq(lit(c.startDate().toString())))
                .filter(col("event_date").leq(lit(c.endDate().toString())));

        filtered.write()
                .mode("overwrite")
                .partitionBy("event_date")
                .option("compression", "snappy")
                .parquet(c.warehousePath());
    }
}
