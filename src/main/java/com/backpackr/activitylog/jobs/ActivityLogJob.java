package com.backpackr.activitylog.jobs;

import com.backpackr.activitylog.catalog.HiveTableManager;
import com.backpackr.activitylog.config.ActivityLogConfig;
import com.backpackr.activitylog.io.ActivityLogReader;
import com.backpackr.activitylog.io.ActivityLogWriter;
import com.backpackr.activitylog.recovery.BatchCheckpoint;
import com.backpackr.activitylog.transform.SessionAssigner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 적재 잡 엔트리 포인트. 파이프라인을 순서대로 조립하고, BatchCheckpoint 가
 * 필요로 하는 try/catch 를 소유합니다 (예외가 어디서 나든 fail 마커를 남기기 위해).
 *
 * 예시 submit:
 *   spark-submit \
 *     --class com.backpackr.activitylog.jobs.ActivityLogJob \
 *     --master yarn --deploy-mode cluster \
 *     activity-log-0.1.0.jar \
 *     --input s3a://raw/2019-Oct.csv,s3a://raw/2019-Nov.csv \
 *     --warehouse-path s3a://warehouse/activity_log \
 *     --database analytics --table activity_log \
 *     --start-date 2019-10-01 --end-date 2019-11-30 \
 *     --checkpoint-path s3a://warehouse/activity_log
 */
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
            // 원본 예외를 그대로 위로 전파 — 운영 모니터링이 비정상 종료를 감지하도록.
            if (t instanceof RuntimeException re) throw re;
            if (t instanceof Error err) throw err;
            throw new RuntimeException(t);
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
                .appName("ActivityLogIngest")
                .enableHiveSupport()
                // 내부 timestamp 연산은 UTC 로 통일. KST 변환은 SessionAssigner 가 명시적으로 수행.
                .config("spark.sql.session.timeZone", "UTC")
                // 핵심: 동일 일자 범위 재실행 시 그 파티션만 갈아끼우기 (idempotency).
                .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                .getOrCreate();
    }
}
