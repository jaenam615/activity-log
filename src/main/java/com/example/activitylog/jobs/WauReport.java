package com.example.activitylog.jobs;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;

/**
 * 외부 Hive 테이블을 두 가지 방식으로 집계해 WAU 산출:
 *   (a) DISTINCT user_id  / ISO 주
 *   (b) DISTINCT session_id / ISO 주
 *
 * date_trunc('WEEK', d) 는 ISO-8601 — 주의 시작은 월요일.
 * 두 정의가 같은 주 버킷을 쓰므로 결과를 1:1 로 비교 가능.
 *
 * 실행 예:
 *   spark-submit --class com.example.activitylog.jobs.WauReport <jar> \
 *     --database analytics --table activity_log \
 *     --from 2019-10-01 --to 2019-11-30
 */
public final class WauReport {

    private WauReport() {}

    /**
     * 날짜는 LocalDate 로 받아 (1) CLI 인자 형식이 잘못되면 parse 단계에서 즉시 실패,
     * (2) SQL 에 박힐 때 항상 ISO 형식 (yyyy-MM-dd) 임을 보장 — injection 위험 차단.
     */
    public record Args(String database, String tableName, LocalDate from, LocalDate to) {}

    public static void main(String[] argv) {
        Args args = parse(argv);
        SparkSession spark = SparkSession.builder()
                .appName("ActivityLogWauReport")
                .enableHiveSupport()
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate();
        try {
            Dataset<Row> byUser    = wauByUser(spark, args);
            Dataset<Row> bySession = wauBySession(spark, args);

            System.out.println("\n=== WAU by user_id ===");
            byUser.show(200, false);
            System.out.println("\n=== WAU by session_id ===");
            bySession.show(200, false);
        } finally {
            spark.stop();
        }
    }

    public static Dataset<Row> wauByUser(SparkSession spark, Args a) {
        return spark.sql("""
                SELECT
                  date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
                  COUNT(DISTINCT user_id) AS wau_users
                FROM %s.%s
                WHERE event_date BETWEEN DATE '%s' AND DATE '%s'
                GROUP BY date_trunc('WEEK', event_date)
                ORDER BY week_start_kst
                """.formatted(a.database(), a.tableName(), a.from(), a.to()));
    }

    public static Dataset<Row> wauBySession(SparkSession spark, Args a) {
        return spark.sql("""
                SELECT
                  date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
                  COUNT(DISTINCT session_id) AS wau_sessions
                FROM %s.%s
                WHERE event_date BETWEEN DATE '%s' AND DATE '%s'
                GROUP BY date_trunc('WEEK', event_date)
                ORDER BY week_start_kst
                """.formatted(a.database(), a.tableName(), a.from(), a.to()));
    }

    private static Args parse(String[] argv) {
        String database = "default";
        String tableName = "activity_log";
        LocalDate from = null;
        LocalDate to = null;
        int i = 0;
        while (i < argv.length) {
            String key = argv[i++];
            if (i >= argv.length) throw new IllegalArgumentException("missing value for " + key);
            String value = argv[i++];
            switch (key) {
                case "--database" -> database = value;
                case "--table"    -> tableName = value;
                case "--from"     -> from = LocalDate.parse(value);
                case "--to"       -> to   = LocalDate.parse(value);
                default -> throw new IllegalArgumentException("unknown arg: " + key);
            }
        }
        if (from == null || to == null) {
            throw new IllegalArgumentException("--from and --to are required");
        }
        if (to.isBefore(from)) {
            throw new IllegalArgumentException("--to must be >= --from");
        }
        return new Args(database, tableName, from, to);
    }
}
