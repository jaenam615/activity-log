package com.example.activitylog;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

/**
 * 모든 테스트가 공유하는 로컬 SparkSession.
 * SparkSession.builder().getOrCreate() 는 이미 만들어진 세션을 재사용하므로
 * 여러 테스트 클래스가 같은 SparkSession 인스턴스를 안전하게 공유합니다.
 */
public abstract class SparkTestBase {

    protected static SparkSession spark;

    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName("activity-log-tests")
                .master("local[2]")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    /**
     * ActivityLogReader.SOURCE_SCHEMA 의 9개 컬럼 순서대로 채운 원본 Row.
     * 테스트가 검증하지 않는 컬럼(product_id, category_id 등) 은 더미 상수로 채움.
     */
    protected static Row rawRow(String time, long userId) {
        return RowFactory.create(
                time, "view", 1L, 2L, null, null, 0.0, userId, "src"
        );
    }
}
