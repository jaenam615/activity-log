package com.example.activitylog.transform;

import com.example.activitylog.config.Defaults;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public final class SessionAssigner {

    private SessionAssigner() {}

    public static Dataset<Row> assign(Dataset<Row> raw, int gapMinutes) {
        if (gapMinutes <= 0) {
            throw new IllegalArgumentException("gapMinutes must be > 0");
        }

        Dataset<Row> withTs = raw
                .withColumn("event_ts_utc",
                        to_utc_timestamp(
                                to_timestamp(
                                        regexp_replace(col("event_time"), Defaults.EVENT_TIME_UTC_SUFFIX, ""),
                                        Defaults.EVENT_TIME_FORMAT
                                ),
                                Defaults.UTC_ZONE_ID
                        )
                )
                .withColumn("event_ts_kst", from_utc_timestamp(col("event_ts_utc"), Defaults.KST_ZONE_ID))
                .withColumn("event_date",   to_date(col("event_ts_kst")))
                .filter(col("event_ts_utc").isNotNull().and(col("user_id").isNotNull()));

        WindowSpec userWin = Window.partitionBy("user_id").orderBy("event_ts_utc");
        long gapSeconds = (long) gapMinutes * Defaults.SECONDS_PER_MINUTE;

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
                .withColumn("session_seq", sum("is_new_session").over(userWin));

        WindowSpec sessionWin = Window.partitionBy("user_id", "session_seq");

        Dataset<Row> withSession = withSeq
                .withColumn("session_start_utc", min("event_ts_utc").over(sessionWin))
                .withColumn("session_start_kst", from_utc_timestamp(col("session_start_utc"), Defaults.KST_ZONE_ID))
                .withColumn("session_id",
                        concat_ws("_",
                                col("user_id").cast("string"),
                                unix_timestamp(col("session_start_utc")).cast("string")
                        )
                );

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
