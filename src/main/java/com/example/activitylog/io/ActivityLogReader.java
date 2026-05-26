package com.example.activitylog.io;

import com.example.activitylog.config.Defaults;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public final class ActivityLogReader {

    private ActivityLogReader() {}

    public static final StructType SOURCE_SCHEMA = DataTypes.createStructType(List.of(
            DataTypes.createStructField("event_time",    DataTypes.StringType, true),
            DataTypes.createStructField("event_type",    DataTypes.StringType, true),
            DataTypes.createStructField("product_id",    DataTypes.LongType,   true),
            DataTypes.createStructField("category_id",   DataTypes.LongType,   true),
            DataTypes.createStructField("category_code", DataTypes.StringType, true),
            DataTypes.createStructField("brand",         DataTypes.StringType, true),
            DataTypes.createStructField("price",         DataTypes.DoubleType, true),
            DataTypes.createStructField("user_id",       DataTypes.LongType,   true),
            DataTypes.createStructField("user_session",  DataTypes.StringType, true)
    ));

    public static Dataset<Row> read(SparkSession spark, List<String> paths) {
        if (paths.isEmpty()) {
            throw new IllegalArgumentException("at least one input path is required");
        }
        return spark.read()
                .option(Defaults.CSV_HEADER_OPTION, Defaults.CSV_HEADER_VALUE)
                .option(Defaults.CSV_MODE_OPTION, Defaults.CSV_MODE_PERMISSIVE)
                .schema(SOURCE_SCHEMA)
                .csv(paths.toArray(new String[0]));
    }
}
