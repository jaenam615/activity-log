package com.example.activitylog.config;

public final class Defaults {

    private Defaults() {}

    public static final String DEFAULT_DATABASE = "default";
    public static final String DEFAULT_TABLE    = "activity_log";

    public static final int  DEFAULT_SESSION_GAP_MINUTES = 5;
    public static final long SECONDS_PER_MINUTE          = 60L;

    public static final boolean DEFAULT_REPAIR_PARTITIONS = true;

    public static final String KST_ZONE_ID = "Asia/Seoul";
    public static final String UTC_ZONE_ID = "UTC";

    public static final String EVENT_TIME_FORMAT     = "yyyy-MM-dd HH:mm:ss";
    public static final String EVENT_TIME_UTC_SUFFIX = " UTC$";

    public static final String COMPRESSION_OPTION   = "compression";
    public static final String COMPRESSION_SNAPPY   = "snappy";
    public static final String WRITE_MODE_OVERWRITE = "overwrite";

    public static final String CSV_HEADER_OPTION   = "header";
    public static final String CSV_HEADER_VALUE    = "true";
    public static final String CSV_MODE_OPTION     = "mode";
    public static final String CSV_MODE_PERMISSIVE = "PERMISSIVE";

    public static final String CHECKPOINT_JOBS_DIR             = "_jobs";
    public static final String CHECKPOINT_STATE_PREFIX         = "state=";
    public static final String CHECKPOINT_MARKER_FILE          = "marker.json";
    public static final int    CHECKPOINT_RUN_ID_SUFFIX_LENGTH = 8;

    public static final int WAU_SHOW_ROWS = 200;
}
