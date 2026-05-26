package com.example.activitylog.jobs;

import com.example.activitylog.config.CliFlags;
import com.example.activitylog.config.Defaults;
import com.example.activitylog.config.SparkConfigs;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;

public final class WauReport {

    private WauReport() {}

    public record Args(String database, String tableName, LocalDate from, LocalDate to) {}

    public static void main(String[] argv) {
        Args args = parse(argv);
        SparkSession spark = SparkSession.builder()
                .appName(SparkConfigs.APP_NAME_WAU)
                .enableHiveSupport()
                .config(SparkConfigs.SESSION_TIMEZONE_KEY, SparkConfigs.SESSION_TIMEZONE_VALUE)
                .getOrCreate();
        try {
            Dataset<Row> byUser    = wauByUser(spark, args);
            Dataset<Row> bySession = wauBySession(spark, args);

            System.out.println("\n=== WAU by user_id ===");
            byUser.show(Defaults.WAU_SHOW_ROWS, false);
            System.out.println("\n=== WAU by session_id ===");
            bySession.show(Defaults.WAU_SHOW_ROWS, false);
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
        String database = Defaults.DEFAULT_DATABASE;
        String tableName = Defaults.DEFAULT_TABLE;
        LocalDate from = null;
        LocalDate to = null;
        int i = 0;
        while (i < argv.length) {
            String key = argv[i++];
            if (i >= argv.length) throw new IllegalArgumentException("missing value for " + key);
            String value = argv[i++];
            switch (key) {
                case CliFlags.DATABASE -> database = value;
                case CliFlags.TABLE    -> tableName = value;
                case CliFlags.FROM     -> from = LocalDate.parse(value);
                case CliFlags.TO       -> to   = LocalDate.parse(value);
                default -> throw new IllegalArgumentException("unknown arg: " + key);
            }
        }
        if (from == null || to == null) {
            throw new IllegalArgumentException(CliFlags.FROM + " and " + CliFlags.TO + " are required");
        }
        if (to.isBefore(from)) {
            throw new IllegalArgumentException(CliFlags.TO + " must be >= " + CliFlags.FROM);
        }
        return new Args(database, tableName, from, to);
    }
}
