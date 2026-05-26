package com.example.activitylog.config;

public final class CliFlags {

    private CliFlags() {}

    public static final String INPUT               = "--input";
    public static final String WAREHOUSE_PATH      = "--warehouse-path";
    public static final String DATABASE            = "--database";
    public static final String TABLE               = "--table";
    public static final String START_DATE          = "--start-date";
    public static final String END_DATE            = "--end-date";
    public static final String CHECKPOINT_PATH     = "--checkpoint-path";
    public static final String SESSION_GAP_MINUTES = "--session-gap-minutes";
    public static final String REPAIR_PARTITIONS   = "--repair-partitions";

    public static final String FROM = "--from";
    public static final String TO   = "--to";

    public static final String INPUT_PATH_SEPARATOR = ",";
}
