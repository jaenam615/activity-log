package com.example.activitylog.recovery;

import com.example.activitylog.config.ActivityLogConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

/**
 * 파일 시스템 기반 배치 상태 마커 — 복구 장치 (recovery primitive) 의 핵심.
 *
 * 레이아웃:
 *   {basePath}/_jobs/state=started/{runId}/marker.json
 *   {basePath}/_jobs/state=success/{runId}/marker.json
 *   {basePath}/_jobs/state=failed/{runId}/marker.json
 *   {basePath}/_jobs/state=recovered/{runId}/marker.json
 *
 * 복구 흐름:
 *   1) 잡 시작 시 _jobs/state=started/ 를 스캔. 거기 남아 있는 디렉터리는
 *      직전 실행이 종료 마커를 남기지 못하고 죽었다는 뜻 → state=recovered/
 *      로 옮기고 로그에 안내.
 *   2) 출력 writer 가 dynamic partition overwrite 이므로, 동일 인자로 재실행
 *      하면 부분적으로 쓴 파티션도 그대로 덮어씌워진다 (멱등).
 *   3) 성공 시 started → success, 예외 시 started → failed 로 atomic rename.
 *
 * 체크포인트 보조 작업의 실패가 원본 잡 실패를 가리지 않도록 모든 secondary
 * 예외는 catch + log.
 */
public final class BatchCheckpoint {

    private static final Logger log = LoggerFactory.getLogger(BatchCheckpoint.class);

    private final SparkSession spark;
    private final String basePath;
    private final Path jobsRoot;

    public BatchCheckpoint(SparkSession spark, String basePath) {
        this.spark = spark;
        this.basePath = basePath;
        this.jobsRoot = new Path(basePath + "/_jobs");
    }

    private FileSystem fs() throws IOException {
        return FileSystem.get(URI.create(basePath), spark.sparkContext().hadoopConfiguration());
    }

    private Path stateDir(String state) {
        return new Path(jobsRoot, "state=" + state);
    }

    private Path runDir(String runId, String state) {
        return new Path(stateDir(state), runId);
    }

    /** 새 runId 발급. 부수 효과: 기존 started/ 항목은 recovered/ 로 이동. */
    public String start(ActivityLogConfig c) {
        try {
            recoverCrashedRuns();
            String runId = LocalDate.now() + "_" + UUID.randomUUID().toString().substring(0, 8);
            writeMarker(runDir(runId, "started"), startedBody(c));
            log.info("BatchCheckpoint START runId={}", runId);
            return runId;
        } catch (IOException e) {
            throw new RuntimeException("checkpoint start failed", e);
        }
    }

    public void success(String runId) {
        try {
            boolean moved = renameState(runId, "started", "success");
            if (moved) log.info("BatchCheckpoint SUCCESS runId={}", runId);
        } catch (IOException e) {
            log.error("BatchCheckpoint success rename failed: {}", e.getMessage());
        }
    }

    public void fail(String runId, Throwable t) {
        try {
            FileSystem fs = fs();
            Path src = runDir(runId, "started");
            Path dst = runDir(runId, "failed");
            fs.mkdirs(dst.getParent());
            if (fs.exists(src)) fs.rename(src, dst);
            writeMarker(dst, failedBody(t));
            log.error("BatchCheckpoint FAIL runId={}: {}", runId, t.getMessage());
        } catch (Throwable secondary) {
            log.error("BatchCheckpoint fail-marker write failed: {}", secondary.getMessage());
        }
    }

    private void recoverCrashedRuns() throws IOException {
        FileSystem fs = fs();
        Path started = stateDir("started");
        if (!fs.exists(started)) return;
        FileStatus[] entries = fs.listStatus(started);
        if (entries.length == 0) return;
        log.warn(
                "BatchCheckpoint detected {} crashed run(s); moving to state=recovered. "
                        + "Re-run the same --start-date/--end-date to backfill any missing data "
                        + "(writes are idempotent under dynamic partition overwrite).",
                entries.length
        );
        for (FileStatus st : entries) {
            if (!st.isDirectory()) continue;
            String runId = st.getPath().getName();
            try {
                renameState(runId, "started", "recovered");
            } catch (IOException e) {
                log.warn("failed to mark {} recovered: {}", runId, e.getMessage());
            }
        }
    }

    private boolean renameState(String runId, String from, String to) throws IOException {
        FileSystem fs = fs();
        Path src = runDir(runId, from);
        Path dst = runDir(runId, to);
        if (!fs.exists(src)) return false;
        fs.mkdirs(dst.getParent());
        return fs.rename(src, dst);
    }

    private void writeMarker(Path dir, String body) throws IOException {
        FileSystem fs = fs();
        fs.mkdirs(dir);
        try (OutputStream out = fs.create(new Path(dir, "marker.json"), /* overwrite = */ true)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private String startedBody(ActivityLogConfig c) {
        return "{\"state\":\"started\""
                + ",\"timestamp\":\"" + Instant.now() + "\""
                + ",\"startDate\":\"" + c.startDate() + "\""
                + ",\"endDate\":\"" + c.endDate() + "\""
                + ",\"sessionGapMinutes\":" + c.sessionGapMinutes()
                + ",\"inputs\":" + jsonArr(c.inputPaths())
                + "}";
    }

    private String failedBody(Throwable t) {
        String msg = t.getMessage() != null ? t.getMessage() : t.getClass().getName();
        return "{\"state\":\"failed\""
                + ",\"timestamp\":\"" + Instant.now() + "\""
                + ",\"error\":" + jsonStr(msg)
                + "}";
    }

    private static String jsonStr(String s) {
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\"";
    }

    private static String jsonArr(List<String> xs) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < xs.size(); i++) {
            if (i > 0) sb.append(",");
            sb.append(jsonStr(xs.get(i)));
        }
        return sb.append("]").toString();
    }
}
