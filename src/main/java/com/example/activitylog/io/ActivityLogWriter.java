package com.example.activitylog.io;

import com.example.activitylog.config.ActivityLogConfig;
import com.example.activitylog.config.Defaults;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

public final class ActivityLogWriter {

    private ActivityLogWriter() {}

    public static void writePartitioned(Dataset<Row> df, ActivityLogConfig c) {
        Dataset<Row> filtered = df
                .filter(col("event_date").geq(lit(c.startDate().toString())))
                .filter(col("event_date").leq(lit(c.endDate().toString())));

        filtered.write()
                .mode(Defaults.WRITE_MODE_OVERWRITE)
                .partitionBy("event_date")
                .option(Defaults.COMPRESSION_OPTION, Defaults.COMPRESSION_SNAPPY)
                .parquet(c.warehousePath());
    }
}
