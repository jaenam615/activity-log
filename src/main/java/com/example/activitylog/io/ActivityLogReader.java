package com.example.activitylog.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Kaggle "ecommerce behavior data from multi-category store" CSV 리더.
 *
 * 스키마를 명시적으로 강제하는 이유:
 *   (1) 손상된 행이 다음 컬럼을 밀어내는 사고를 막는다.
 *   (2) Spark 의 자동 스키마 추론(inferSchema=true) 은 전체 데이터를 한 번 더
 *       스캔하기 때문에 14GB 입력에서 그 비용이 매우 큽니다. 스키마를 직접
 *       주면 한 번에 끝.
 */
public final class ActivityLogReader {

    private ActivityLogReader() {}

    /** 원본 CSV 헤더 순서 그대로의 스키마. */
    public static final StructType SOURCE_SCHEMA = DataTypes.createStructType(List.of(
            DataTypes.createStructField("event_time",    DataTypes.StringType, true),
            DataTypes.createStructField("event_type",    DataTypes.StringType, true),
            DataTypes.createStructField("product_id",    DataTypes.LongType,   true),
            DataTypes.createStructField("category_id",   DataTypes.LongType,   true),
            DataTypes.createStructField("category_code", DataTypes.StringType, true),
            DataTypes.createStructField("brand",         DataTypes.StringType, true),
            DataTypes.createStructField("price",         DataTypes.DoubleType, true),
            DataTypes.createStructField("user_id",       DataTypes.LongType,   true),
            DataTypes.createStructField("user_session",  DataTypes.StringType, true)
    ));

    public static Dataset<Row> read(SparkSession spark, List<String> paths) {
        if (paths.isEmpty()) {
            throw new IllegalArgumentException("at least one input path is required");
        }
        return spark.read()
                .option("header", "true")
                .option("mode", "PERMISSIVE")   // 손상 행은 null 로 두고 진행
                .schema(SOURCE_SCHEMA)
                .csv(paths.toArray(new String[0]));
    }
}
