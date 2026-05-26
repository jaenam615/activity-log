package com.example.activitylog.config;

public final class SparkConfigs {

    private SparkConfigs() {}

    public static final String SESSION_TIMEZONE_KEY   = "spark.sql.session.timeZone";
    public static final String SESSION_TIMEZONE_VALUE = "UTC";

    public static final String PARTITION_OVERWRITE_MODE_KEY   = "spark.sql.sources.partitionOverwriteMode";
    public static final String PARTITION_OVERWRITE_MODE_VALUE = "dynamic";

    public static final String SHUFFLE_PARTITIONS_KEY        = "spark.sql.shuffle.partitions";
    public static final String SHUFFLE_PARTITIONS_TEST_VALUE = "4";

    public static final String UI_ENABLED_KEY        = "spark.ui.enabled";
    public static final String UI_ENABLED_TEST_VALUE = "false";

    public static final String APP_NAME_INGEST = "ActivityLogIngest";
    public static final String APP_NAME_WAU    = "ActivityLogWauReport";
    public static final String APP_NAME_TEST   = "activity-log-tests";

    public static final String TEST_MASTER = "local[2]";
}
