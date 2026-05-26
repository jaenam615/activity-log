package com.example.activitylog.jobs;

import com.example.activitylog.SparkTestBase;
import com.example.activitylog.io.ActivityLogReader;
import com.example.activitylog.transform.SessionAssigner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WauReportTest extends SparkTestBase {

    @Test
    @DisplayName("WAU by user / by session 가 동일 주차에서 일관된 결과를 낸다")
    void wauByUserAndBySessionAreConsistent() throws Exception {
        File tmp = Files.createTempDirectory("activity-log-wau-").toFile();
        tmp.deleteOnExit();
        String warehouse = tmp.toURI().toString().replaceAll("/$", "");

        // 2019-10-07(월) ~ 2019-10-13(일) KST 주차
        Dataset<Row> df = spark.createDataFrame(List.of(
                // user 1: 1분 차이 → 한 세션, 2019-10-07 KST
                rawRow("2019-10-07 00:00:00 UTC", 1L),
                rawRow("2019-10-07 00:01:00 UTC", 1L),
                // user 1: 15분 후 → 새 세션 (같은 KST 날짜)
                rawRow("2019-10-07 00:15:00 UTC", 1L),
                // user 2: 한 세션, 같은 주
                rawRow("2019-10-08 02:00:00 UTC", 2L),
                // user 1: 다음 주
                rawRow("2019-10-14 02:00:00 UTC", 1L)
        ), ActivityLogReader.SOURCE_SCHEMA);

        Dataset<Row> sessioned = SessionAssigner.assign(df, 5);
        sessioned.write()
                .mode("overwrite")
                .partitionBy("event_date")
                .option("compression", "snappy")
                .parquet(warehouse);

        // 외부 테이블 SELECT 처럼 다시 읽어와 임시 뷰로 등록.
        String table = "activity_log_test";
        spark.read().parquet(warehouse).createOrReplaceTempView(table);

        Map<String, Long> byUser = collectMap(spark.sql("""
                SELECT date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
                       COUNT(DISTINCT user_id) AS wau_users
                FROM %s
                WHERE event_date BETWEEN DATE '2019-10-01' AND DATE '2019-10-31'
                GROUP BY date_trunc('WEEK', event_date)
                ORDER BY week_start_kst
                """.formatted(table)));

        Map<String, Long> bySession = collectMap(spark.sql("""
                SELECT date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
                       COUNT(DISTINCT session_id) AS wau_sessions
                FROM %s
                WHERE event_date BETWEEN DATE '2019-10-01' AND DATE '2019-10-31'
                GROUP BY date_trunc('WEEK', event_date)
                ORDER BY week_start_kst
                """.formatted(table)));

        // date_trunc('WEEK', d) 는 월요일 시작.
        assertEquals(2L, byUser.get("2019-10-07"), "got " + byUser);
        assertEquals(1L, byUser.get("2019-10-14"), "got " + byUser);

        // user1: 같은 KST 날짜에 세션 2개. user2: 세션 1개. → 3.
        assertEquals(3L, bySession.get("2019-10-07"), "got " + bySession);
        assertEquals(1L, bySession.get("2019-10-14"), "got " + bySession);
    }

    private static Map<String, Long> collectMap(Dataset<Row> df) {
        Map<String, Long> m = new HashMap<>();
        for (Row r : df.collectAsList()) {
            m.put(r.getString(0), r.getLong(1));
        }
        return m;
    }
}
