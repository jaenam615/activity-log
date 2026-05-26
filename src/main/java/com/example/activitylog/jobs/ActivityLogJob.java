package com.example.activitylog.jobs;

import com.example.activitylog.catalog.HiveTableManager;
import com.example.activitylog.config.ActivityLogConfig;
import com.example.activitylog.config.SparkConfigs;
import com.example.activitylog.io.ActivityLogReader;
import com.example.activitylog.io.ActivityLogWriter;
import com.example.activitylog.recovery.BatchCheckpoint;
import com.example.activitylog.transform.SessionAssigner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public final class ActivityLogJob {

    private ActivityLogJob() {}

    public static void main(String[] args) {
        ActivityLogConfig config = ActivityLogConfig.parse(args);
        SparkSession spark = buildSession();
        BatchCheckpoint checkpoint = new BatchCheckpoint(spark, config.checkpointPath());
        String runId = checkpoint.start(config);
        try {
            run(spark, config);
            checkpoint.success(runId);
        } catch (Throwable t) {
            checkpoint.fail(runId, t);
            switch (t) {
                case RuntimeException re -> throw re;
                case Error err           -> throw err;
                default                  -> throw new RuntimeException(t);
            }
        } finally {
            spark.stop();
        }
    }

    static void run(SparkSession spark, ActivityLogConfig config) {
        HiveTableManager.createExternalIfMissing(spark, config);

        Dataset<Row> raw       = ActivityLogReader.read(spark, config.inputPaths());
        Dataset<Row> sessioned = SessionAssigner.assign(raw, config.sessionGapMinutes());
        ActivityLogWriter.writePartitioned(sessioned, config);

        if (config.repairPartitions()) {
            HiveTableManager.repairPartitions(spark, config);
        }
    }

    private static SparkSession buildSession() {
        return SparkSession.builder()
                .appName(SparkConfigs.APP_NAME_INGEST)
                .enableHiveSupport()
                .config(SparkConfigs.SESSION_TIMEZONE_KEY, SparkConfigs.SESSION_TIMEZONE_VALUE)
                .config(SparkConfigs.PARTITION_OVERWRITE_MODE_KEY, SparkConfigs.PARTITION_OVERWRITE_MODE_VALUE)
                .getOrCreate();
    }
}
