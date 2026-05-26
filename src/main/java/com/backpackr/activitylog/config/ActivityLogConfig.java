package com.backpackr.activitylog.config;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;

/**
 * CLI 인자 + 검증을 한 곳에 모아 둔 설정 객체.
 *
 * record 는 Java 14+ 기능으로, Kotlin 의 data class 와 거의 동일합니다.
 * - 모든 필드가 자동으로 final.
 * - equals / hashCode / toString 자동 생성.
 * - 접근자는 메서드 호출 형식 (예: config.startDate()) — 필드 아닌 메서드.
 */
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

    /** main(String[] args) 의 args 를 직접 파싱. 외부 라이브러리 없이 단순한 switch 로. */
    public static ActivityLogConfig parse(String[] argv) {
        List<String> inputPaths = List.of();
        String warehousePath = null;
        String database = "default";
        String tableName = "activity_log";
        LocalDate startDate = null;
        LocalDate endDate = null;
        String checkpointPath = null;
        int sessionGapMinutes = 5;
        boolean repairPartitions = true;

        int i = 0;
        while (i < argv.length) {
            String key = argv[i++];
            if (i >= argv.length) {
                throw new IllegalArgumentException("missing value for " + key);
            }
            String value = argv[i++];
            switch (key) {
                case "--input" ->
                        inputPaths = Arrays.stream(value.split(",")).map(String::trim).toList();
                case "--warehouse-path"      -> warehousePath = value;
                case "--database"            -> database = value;
                case "--table"               -> tableName = value;
                case "--start-date"          -> startDate = LocalDate.parse(value);
                case "--end-date"            -> endDate = LocalDate.parse(value);
                case "--checkpoint-path"     -> checkpointPath = value;
                case "--session-gap-minutes" -> sessionGapMinutes = Integer.parseInt(value);
                case "--repair-partitions"   -> repairPartitions = Boolean.parseBoolean(value);
                default -> throw new IllegalArgumentException("unknown arg: " + key);
            }
        }

        if (inputPaths.isEmpty())   throw new IllegalArgumentException("--input is required");
        if (warehousePath == null)  throw new IllegalArgumentException("--warehouse-path is required");
        if (startDate == null)      throw new IllegalArgumentException("--start-date is required");
        if (endDate == null)        throw new IllegalArgumentException("--end-date is required");
        if (checkpointPath == null) throw new IllegalArgumentException("--checkpoint-path is required");
        if (endDate.isBefore(startDate))
            throw new IllegalArgumentException("--end-date must be >= --start-date");
        if (sessionGapMinutes <= 0)
            throw new IllegalArgumentException("--session-gap-minutes must be > 0");

        return new ActivityLogConfig(
                inputPaths, warehousePath, database, tableName,
                startDate, endDate, checkpointPath, sessionGapMinutes, repairPartitions
        );
    }
}
