package com.example.activitylog.config;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

public record ActivityLogConfig(
        List<String> inputPaths,
        String warehousePath,
        String database,
        String tableName,
        LocalDate startDate,
        LocalDate endDate,
        String checkpointPath,
        int sessionGapMinutes,
        boolean repairPartitions
) {

    public static ActivityLogConfig parse(String[] argv) {
        List<String> inputPaths = List.of();
        String warehousePath = null;
        String database = Defaults.DEFAULT_DATABASE;
        String tableName = Defaults.DEFAULT_TABLE;
        LocalDate startDate = null;
        LocalDate endDate = null;
        String checkpointPath = null;
        int sessionGapMinutes = Defaults.DEFAULT_SESSION_GAP_MINUTES;
        boolean repairPartitions = Defaults.DEFAULT_REPAIR_PARTITIONS;

        int i = 0;
        while (i < argv.length) {
            String key = argv[i++];
            if (i >= argv.length) {
                throw new IllegalArgumentException("missing value for " + key);
            }
            String value = argv[i++];
            switch (key) {
                case CliFlags.INPUT ->
                        inputPaths = Arrays.stream(value.split(CliFlags.INPUT_PATH_SEPARATOR)).map(String::trim).toList();
                case CliFlags.WAREHOUSE_PATH      -> warehousePath = value;
                case CliFlags.DATABASE            -> database = value;
                case CliFlags.TABLE               -> tableName = value;
                case CliFlags.START_DATE          -> startDate = LocalDate.parse(value);
                case CliFlags.END_DATE            -> endDate = LocalDate.parse(value);
                case CliFlags.CHECKPOINT_PATH     -> checkpointPath = value;
                case CliFlags.SESSION_GAP_MINUTES -> sessionGapMinutes = Integer.parseInt(value);
                case CliFlags.REPAIR_PARTITIONS   -> repairPartitions = Boolean.parseBoolean(value);
                default -> throw new IllegalArgumentException("unknown arg: " + key);
            }
        }

        if (inputPaths.isEmpty())   throw new IllegalArgumentException(CliFlags.INPUT + " is required");
        if (warehousePath == null)  throw new IllegalArgumentException(CliFlags.WAREHOUSE_PATH + " is required");
        if (startDate == null)      throw new IllegalArgumentException(CliFlags.START_DATE + " is required");
        if (endDate == null)        throw new IllegalArgumentException(CliFlags.END_DATE + " is required");
        if (checkpointPath == null) throw new IllegalArgumentException(CliFlags.CHECKPOINT_PATH + " is required");
        if (endDate.isBefore(startDate))
            throw new IllegalArgumentException(CliFlags.END_DATE + " must be >= " + CliFlags.START_DATE);
        if (sessionGapMinutes <= 0)
            throw new IllegalArgumentException(CliFlags.SESSION_GAP_MINUTES + " must be > 0");

        return new ActivityLogConfig(
                inputPaths, warehousePath, database, tableName,
                startDate, endDate, checkpointPath, sessionGapMinutes, repairPartitions
        );
    }
}
