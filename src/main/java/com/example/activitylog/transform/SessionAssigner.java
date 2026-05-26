package com.example.activitylog.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

/**
 * 사용자별 세션 부여.
 *
 * 규칙: 같은 user_id 안에서 직전 이벤트와의 간격이 gapMinutes 이상이면
 *       그 시점부터 새 세션을 시작.
 *
 * session_id 포맷: "{user_id}_{session_start_epoch_seconds}".
 *   - 같은 입력에 대해 같은 ID 가 재생산되어 재처리(idempotency) 안전.
 *
 * 동작 원리 — "gap-and-island" 패턴:
 *   1) lag() 로 직전 이벤트의 timestamp 를 가져온다.
 *   2) 직전이 없거나 갭 >= gapMinutes 이면 is_new_session = 1, 아니면 0.
 *   3) sum(is_new_session) over (user 순) 로 누적합 → 1, 1, 2, 3 ... 같은
 *      세션 시퀀스 번호가 자동으로 만들어진다.
 *   4) (user_id, session_seq) 단위로 min(event_ts) 가 곧 세션 시작 시각.
 */
public final class SessionAssigner {

    private SessionAssigner() {}

    public static final String KST_ZONE = "Asia/Seoul";

    public static Dataset<Row> assign(Dataset<Row> raw, int gapMinutes) {
        if (gapMinutes <= 0) {
            throw new IllegalArgumentException("gapMinutes must be > 0");
        }

        // event_time = "YYYY-MM-DD HH:mm:ss UTC" 문자열.
        // 끝의 " UTC" 를 제거한 뒤, naive timestamp 로 파싱하고
        // to_utc_timestamp(..., "UTC") 로 "이 값은 UTC 였다" 라고 못박는다.
        // → SparkSession 의 timeZone 설정에 무관하게 일관된 UTC 값 보장.
        Dataset<Row> withTs = raw
                .withColumn("event_ts_utc",
                        to_utc_timestamp(
                                to_timestamp(
                                        regexp_replace(col("event_time"), " UTC$", ""),
                                        "yyyy-MM-dd HH:mm:ss"
                                ),
                                "UTC"
                        )
                )
                .withColumn("event_ts_kst", from_utc_timestamp(col("event_ts_utc"), KST_ZONE))
                .withColumn("event_date",   to_date(col("event_ts_kst")))
                .filter(col("event_ts_utc").isNotNull().and(col("user_id").isNotNull()));

        // 한 사용자 내에서 시간 순 정렬한 윈도우.
        WindowSpec userWin = Window.partitionBy("user_id").orderBy("event_ts_utc");
        long gapSeconds = (long) gapMinutes * 60L;

        Dataset<Row> withSeq = withTs
                .withColumn("prev_ts_utc", lag("event_ts_utc", 1).over(userWin))
                .withColumn("gap_sec",
                        when(col("prev_ts_utc").isNull(), lit(null).cast("long"))
                                .otherwise(
                                        col("event_ts_utc").cast("long")
                                                .minus(col("prev_ts_utc").cast("long"))
                                )
                )
                .withColumn("is_new_session",
                        when(
                                col("prev_ts_utc").isNull().or(col("gap_sec").geq(lit(gapSeconds))),
                                lit(1)
                        ).otherwise(lit(0))
                )
                // 누적합 → 1, 1, 2, 3 ... 자동으로 세션 시퀀스 번호가 된다.
                .withColumn("session_seq", sum("is_new_session").over(userWin));

        // (user, session_seq) 묶음 안에서의 최소 timestamp = 세션 시작 시각.
        WindowSpec sessionWin = Window.partitionBy("user_id", "session_seq");

        Dataset<Row> withSession = withSeq
                .withColumn("session_start_utc", min("event_ts_utc").over(sessionWin))
                .withColumn("session_start_kst", from_utc_timestamp(col("session_start_utc"), KST_ZONE))
                .withColumn("session_id",
                        concat_ws("_",
                                col("user_id").cast("string"),
                                unix_timestamp(col("session_start_utc")).cast("string")
                        )
                );

        // 출력 컬럼 순서/이름은 외부 테이블 DDL 과 정확히 일치시킨다.
        return withSession.select(
                col("event_ts_utc").alias("event_time_utc"),
                col("event_ts_kst").alias("event_time_kst"),
                col("event_type"),
                col("product_id"),
                col("category_id"),
                col("category_code"),
                col("brand"),
                col("price"),
                col("user_id"),
                col("user_session").alias("source_user_session"),
                col("session_id"),
                col("session_start_kst"),
                col("event_date")
        );
    }
}
