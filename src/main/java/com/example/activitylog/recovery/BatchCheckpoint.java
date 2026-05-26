package com.example.activitylog.recovery;

import com.example.activitylog.config.ActivityLogConfig;
import com.example.activitylog.config.Defaults;
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

@SuppressWarnings("resource")
public final class BatchCheckpoint {

    private static final Logger log = LoggerFactory.getLogger(BatchCheckpoint.class);

    private final SparkSession spark;
    private final String basePath;
    private final Path jobsRoot;

    public BatchCheckpoint(SparkSession spark, String basePath) {
        this.spark = spark;
        this.basePath = basePath;
        this.jobsRoot = new Path(basePath + "/" + Defaults.CHECKPOINT_JOBS_DIR);
    }

    private FileSystem fs() throws IOException {
        return FileSystem.get(URI.create(basePath), spark.sparkContext().hadoopConfiguration());
    }

    private Path stateDir(JobState state) {
        return new Path(jobsRoot, Defaults.CHECKPOINT_STATE_PREFIX + state.dirName());
    }

    private Path runDir(String runId, JobState state) {
        return new Path(stateDir(state), runId);
    }

    public String start(ActivityLogConfig c) {
        try {
            recoverCrashedRuns();
            String runId = LocalDate.now() + "_"
                    + UUID.randomUUID().toString().substring(0, Defaults.CHECKPOINT_RUN_ID_SUFFIX_LENGTH);
            writeMarker(runDir(runId, JobState.STARTED), startedBody(c));
            log.info("BatchCheckpoint START runId={}", runId);
            return runId;
        } catch (IOException e) {
            throw new RuntimeException("checkpoint start failed", e);
        }
    }

    public void success(String runId) {
        try {
            boolean moved = renameState(runId, JobState.SUCCESS);
            if (moved) log.info("BatchCheckpoint SUCCESS runId={}", runId);
        } catch (IOException e) {
            log.error("BatchCheckpoint success rename failed: {}", e.getMessage());
        }
    }

    public void fail(String runId, Throwable t) {
        try {
            FileSystem fs = fs();
            Path src = runDir(runId, JobState.STARTED);
            Path dst = runDir(runId, JobState.FAILED);
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
        Path started = stateDir(JobState.STARTED);
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
                renameState(runId, JobState.RECOVERED);
            } catch (IOException e) {
                log.warn("failed to mark {} recovered: {}", runId, e.getMessage());
            }
        }
    }

    private boolean renameState(String runId, JobState to) throws IOException {
        FileSystem fs = fs();
        Path src = runDir(runId, JobState.STARTED);
        Path dst = runDir(runId, to);
        if (!fs.exists(src)) return false;
        fs.mkdirs(dst.getParent());
        return fs.rename(src, dst);
    }

    private void writeMarker(Path dir, String body) throws IOException {
        FileSystem fs = fs();
        fs.mkdirs(dir);
        try (OutputStream out = fs.create(new Path(dir, Defaults.CHECKPOINT_MARKER_FILE), true)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private String startedBody(ActivityLogConfig c) {
        return "{\"state\":\"" + JobState.STARTED.dirName() + "\""
                + ",\"timestamp\":\"" + Instant.now() + "\""
                + ",\"startDate\":\"" + c.startDate() + "\""
                + ",\"endDate\":\"" + c.endDate() + "\""
                + ",\"sessionGapMinutes\":" + c.sessionGapMinutes()
                + ",\"inputs\":" + jsonArr(c.inputPaths())
                + "}";
    }

    private String failedBody(Throwable t) {
        String msg = t.getMessage() != null ? t.getMessage() : t.getClass().getName();
        return "{\"state\":\"" + JobState.FAILED.dirName() + "\""
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
