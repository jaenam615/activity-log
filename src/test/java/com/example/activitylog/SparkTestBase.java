package com.example.activitylog;

import com.example.activitylog.config.SparkConfigs;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;

public abstract class SparkTestBase {

    protected static SparkSession spark;

    @BeforeAll
    static void initSpark() {
        spark = SparkSession.builder()
                .appName(SparkConfigs.APP_NAME_TEST)
                .master(SparkConfigs.TEST_MASTER)
                .config(SparkConfigs.SESSION_TIMEZONE_KEY, SparkConfigs.SESSION_TIMEZONE_VALUE)
                .config(SparkConfigs.SHUFFLE_PARTITIONS_KEY, SparkConfigs.SHUFFLE_PARTITIONS_TEST_VALUE)
                .config(SparkConfigs.PARTITION_OVERWRITE_MODE_KEY, SparkConfigs.PARTITION_OVERWRITE_MODE_VALUE)
                .config(SparkConfigs.UI_ENABLED_KEY, SparkConfigs.UI_ENABLED_TEST_VALUE)
                .getOrCreate();
    }

    protected static Row rawRow(String time, long userId) {
        return RowFactory.create(
                time, "view", 1L, 2L, null, null, 0.0, userId, "src"
        );
    }
}
