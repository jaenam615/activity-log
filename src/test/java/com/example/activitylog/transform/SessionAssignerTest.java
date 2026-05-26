package com.example.activitylog.transform;

import com.example.activitylog.SparkTestBase;
import com.example.activitylog.config.Defaults;
import com.example.activitylog.io.ActivityLogReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SessionAssignerTest extends SparkTestBase {

    private static Dataset<Row> rawDF(List<Row> rows) {
        return spark.createDataFrame(rows, ActivityLogReader.SOURCE_SCHEMA);
    }

    @Test
    @DisplayName("5분 이상 간격이면 새 세션을 생성한다")
    void gapOver5MinCreatesNewSession() {
        Dataset<Row> df = rawDF(List.of(
                rawRow("2019-10-01 00:00:00 UTC", 1L),
                rawRow("2019-10-01 00:04:00 UTC", 1L),
                rawRow("2019-10-01 00:10:00 UTC", 1L),
                rawRow("2019-10-01 00:30:00 UTC", 1L),
                rawRow("2019-10-01 00:11:00 UTC", 2L)
        ));

        Dataset<Row> out = SessionAssigner.assign(df, Defaults.DEFAULT_SESSION_GAP_MINUTES);

        Map<Long, Integer> perUser = new HashMap<>();
        for (Row r : out.select("user_id", "session_id").distinct().collectAsList()) {
            perUser.merge(r.getLong(0), 1, Integer::sum);
        }
        assertEquals(3, perUser.get(1L), "user 1 should have 3 sessions; got: " + perUser);
        assertEquals(1, perUser.get(2L), "user 2 should have 1 session; got: " + perUser);
    }

    @Test
    @DisplayName("정확히 5분 간격은 새 세션 (>= 경계 조건)")
    void exactly5MinIsAlsoNewSession() {
        Dataset<Row> df = rawDF(List.of(
                rawRow("2019-10-01 00:00:00 UTC", 7L),
                rawRow("2019-10-01 00:05:00 UTC", 7L)
        ));
        long sessions = SessionAssigner.assign(df, Defaults.DEFAULT_SESSION_GAP_MINUTES)
                .select("session_id").distinct().count();
        assertEquals(2, sessions);
    }

    @Test
    @DisplayName("session_id 는 (user_id, session_start_epoch) 로 결정적")
    void sessionIdIsDeterministic() {
        Dataset<Row> df = rawDF(List.of(
                rawRow("2019-10-01 00:00:00 UTC", 1L),
                rawRow("2019-10-01 00:01:00 UTC", 1L)
        ));
        List<Row> ids = SessionAssigner.assign(df, Defaults.DEFAULT_SESSION_GAP_MINUTES)
                .select("session_id").distinct().collectAsList();
        assertEquals(1, ids.size());
        assertEquals("1_1569888000", ids.getFirst().getString(0));
    }

    @Test
    @DisplayName("event_date 는 KST 기준으로 산출된다")
    void eventDateIsInKst() {
        Dataset<Row> df = rawDF(List.of(rawRow("2019-10-31 15:30:00 UTC", 9L)));
        String d = SessionAssigner.assign(df, Defaults.DEFAULT_SESSION_GAP_MINUTES)
                .selectExpr("CAST(event_date AS STRING) AS d")
                .head().getString(0);
        assertEquals("2019-11-01", d);
    }

    @Test
    @DisplayName("정렬되지 않은 입력에서도 세션 경계가 정확하다")
    void unsortedInputIsHandled() {
        Dataset<Row> df = rawDF(List.of(
                rawRow("2019-10-01 00:30:00 UTC", 1L),
                rawRow("2019-10-01 00:00:00 UTC", 1L),
                rawRow("2019-10-01 00:04:00 UTC", 1L),
                rawRow("2019-10-01 00:10:00 UTC", 1L)
        ));
        long sessions = SessionAssigner.assign(df, Defaults.DEFAULT_SESSION_GAP_MINUTES)
                .select("session_id").distinct().count();
        assertEquals(3, sessions);
    }
}
