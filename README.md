# Activity Log Pipeline

이커머스 이벤트 로그를 KST 일자 파티션 + 세션 단위로 가공해 Hive External Table 로
적재하고, 그 위에서 WAU 를 산출하는 Spark 배치 애플리케이션.

**Stack** — Java 21 · Spark 4.0.2 · Gradle 9 (Kotlin DSL) · Docker

데이터셋은 Kaggle ["eCommerce behavior data from multi-category store"](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
의 2019-Oct (≈5.3GB) · 2019-Nov (≈9.0GB) CSV.

---

## Quick start

호스트의 Java/Spark/Hadoop 버전과 무관하게 동일 환경에서 돌리도록 Docker 를 1st-class
실행 경로로 설계했습니다.

```bash
# 1) Kaggle CSV 두 개를 ./data/ 에 배치
mkdir -p data
# data/2019-Oct.csv, data/2019-Nov.csv

# 2) 적재 잡 + WAU 리포트 한 번에 실행
docker compose up --build report
```

- `ingest` 가 success 로 끝난 뒤 `report` 가 이어서 실행됩니다 (`depends_on` 의
  `service_completed_successfully`).
- 개별 실행도 가능:
  ```bash
  docker compose run --rm ingest   # CSV → Hive External Table 적재
  docker compose run --rm report   # 적재된 테이블에서 WAU 출력
  ```
- 기간/테이블명/입력 경로는 `docker-compose.yml` 의 `command:` 블록에서 수정. CLI 플래그
  정의는 `config/CliFlags.java` 한 곳에 모여 있습니다.

### 컨테이너 구성

| 컴포넌트 | 역할 |
| --- | --- |
| `Dockerfile` | 멀티 스테이지: `eclipse-temurin:21-jdk` 에서 `./gradlew shadowJar` → `apache/spark:4.0.2-java21` 위에 thin jar 만 얹음. |
| `docker-compose.yml` | `ingest` / `report` 두 서비스. `warehouse_data`, `metastore_data` named volume 으로 parquet · Hive Derby metastore 공유. |
| `.dockerignore` | `build/`, `.gradle/`, `data/`, `.idea/` 등 컨텍스트에서 제외. |

> Spark 4.0 부터 Java 21 을 공식 지원하므로 `--add-opens` 같은 JVM hack 없이 동작합니다.

---

## 아키텍처

```
2019-Oct.csv ┐
2019-Nov.csv ┤  1) ActivityLogReader     스키마 강제 적용 CSV 로딩
             ▼
             ┤  2) SessionAssigner       UTC→KST, 5분 갭 규칙 session_id
             ▼
             ┤  3) ActivityLogWriter     partitionBy(event_date), parquet+snappy
             ▼                              + dynamic partition overwrite
             ┤  4) HiveTableManager      CREATE EXTERNAL ... + MSCK REPAIR
             ▼
       {warehouse}/event_date=YYYY-MM-DD/*.snappy.parquet
             │
             │  WauReport.wauByUser / wauBySession
             ▼
       주차별 WAU 두 종류

   BatchCheckpoint 가 1~4 전체를
   {warehouse}/_jobs/state={started|success|failed|recovered}/{runId}/marker.json
   로 감싸 장애 복구를 보조.
```

책임 기반 레이어드 패키지 구조:

| 패키지 | 역할 | 핵심 파일 |
| --- | --- | --- |
| `jobs/`      | Spark 잡 진입점 (main) | `ActivityLogJob`, `WauReport` |
| `config/`    | 설정/CLI 파싱 | `ActivityLogConfig`, `CliFlags` |
| `io/`        | 외부 데이터 입출력 | `ActivityLogReader`, `ActivityLogWriter` |
| `transform/` | 비즈니스 로직 변환 | `SessionAssigner` |
| `catalog/`   | Hive 메타스토어 관리 | `HiveTableManager` |
| `recovery/`  | 운영 신뢰성 / 장애 복구 | `BatchCheckpoint` |

요구사항 변경 시 손댈 위치가 한 곳으로 좁혀집니다:

- 세션 규칙 변경 → `transform/`
- Iceberg/Delta 로 전환 → `catalog/`, `io/Writer`
- 장애 알림 추가 → `recovery/`
- 스토리지 교체 (S3 / GCS) → `io/`, `catalog/`

---

## 세션 부여 규칙

핵심은 SQL/Spark 에서 자주 쓰는 **gap-and-island** 패턴입니다.

```
입력 (user=1)  │ lag(prev_ts) │ gap_sec │ is_new │ sum(is_new) = session_seq
──────────────┼──────────────┼─────────┼────────┼──────────────────────────
00:00         │ null         │ null    │   1    │   1
00:04         │ 00:00        │  240    │   0    │   1
00:10         │ 00:04        │  360    │   1    │   2     ← 6분 갭, 새 세션
00:30         │ 00:10        │ 1200    │   1    │   3     ← 새 세션
```

이후 `(user_id, session_seq)` 묶음에서 `min(event_ts) = session_start` 를 뽑아
`session_id = "{user_id}_{session_start_epoch_seconds}"` 로 만듭니다.

- **결정성** — 같은 입력은 항상 같은 `session_id` 가 나오므로 재처리가 안전.
- **KST 파티션** — `event_time` 의 `" UTC"` 접미 제거 → `to_utc_timestamp(...,"UTC")`
  로 UTC 임을 못박은 뒤 `from_utc_timestamp(..., "Asia/Seoul")` 로 KST 변환,
  날짜 부분만 떼어 파티션 키로 사용. 예: `2019-10-31 15:30 UTC` →
  `2019-11-01 00:30 KST` → `event_date=2019-11-01`.
- **경계 조건** — `>=` 5분 기준이라 정확히 5분 간격도 새 세션.
- **알려진 한계** — 세션이 입력 범위 경계를 가로지를 때 시작 이벤트가 범위 밖이면
  `session_start` 가 잘릴 수 있음. 운영에서는 재처리 일자 범위의 직전 1일 CSV 를
  같이 포함시켜 보정.

---

## WAU 쿼리

`date_trunc('WEEK', event_date)` 는 ISO-8601 — **월요일 시작**. 두 쿼리가 같은
주 버킷을 쓰므로 1:1 비교 가능합니다.

### user_id 기준

```sql
SELECT
  date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
  COUNT(DISTINCT user_id) AS wau_users
FROM analytics.activity_log
WHERE event_date BETWEEN DATE '2019-10-01' AND DATE '2019-11-30'
GROUP BY date_trunc('WEEK', event_date)
ORDER BY week_start_kst;
```

### session_id 기준

```sql
SELECT
  date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
  COUNT(DISTINCT session_id) AS wau_sessions
FROM analytics.activity_log
WHERE event_date BETWEEN DATE '2019-10-01' AND DATE '2019-11-30'
GROUP BY date_trunc('WEEK', event_date)
ORDER BY week_start_kst;
```

`wau_sessions / wau_users` 가 "주간 1인당 평균 방문 횟수" 의 근사치 — 한 유저가
한 주에 여러 번 이탈/재진입할수록 비율이 커집니다.

---

## 장애 복구

`BatchCheckpoint` 가 두 축으로 작동합니다.

1. **데이터 측 멱등** — `spark.sql.sources.partitionOverwriteMode=dynamic` 으로
   동일 `--start-date`/`--end-date` 재실행 시 해당 일자 파티션만 덮어쓰고 다른
   파티션은 그대로. 잡이 도중에 죽어도 같은 인자로 재실행하면 데이터 무결성이
   유지됩니다.
2. **상태 마커** — 잡 시작 시 `_jobs/state=started/{runId}/marker.json` 을 기록,
   성공 시 `state=success/` 로, 예외 시 `state=failed/` 로 atomic rename.
   다음 실행 시 `state=started/` 에 남아 있는 항목은 직전 실행이 graceful
   shutdown 없이 죽었다는 신호 → 자동으로 `state=recovered/` 로 옮기고 로그에
   안내.
3. **메타데이터 멱등** — `CREATE EXTERNAL TABLE IF NOT EXISTS` 와
   `MSCK REPAIR TABLE` 모두 반복 호출에 안전.

운영 시나리오:

- 동일 기간 재적재 → 같은 명령 다시 실행. 데이터 중복/손실 없음.
- 추가 기간 → 새 `--start-date`/`--end-date`. MSCK REPAIR 가 신규 파티션 자동 등록.
- 카탈로그 손상 → `MSCK REPAIR TABLE` 단독 실행으로 모든 파티션 재등록.

---

## 로컬 개발

호스트에 Java 21 만 있으면 Docker 없이도 돌릴 수 있습니다.

```bash
./gradlew test          # Spark 임베디드, KST 경계 / 세션 결정성 / WAU 라운드트립 검증
./gradlew runIngest     # 샘플 CSV 로 ActivityLogJob 실행
./gradlew runWau        # 적재된 warehouse 에서 WauReport 실행
./gradlew shadowJar     # build/libs/activity-log-0.1.0.jar (thin assembly)
```

`spark-submit` 으로 직접 띄울 수도 있습니다:

```bash
spark-submit \
  --class com.example.activitylog.jobs.ActivityLogJob \
  --master 'local[*]' \
  build/libs/activity-log-0.1.0.jar \
  --input            data/2019-Oct.csv,data/2019-Nov.csv \
  --warehouse-path   file:///tmp/warehouse/activity_log \
  --checkpoint-path  file:///tmp/warehouse/activity_log \
  --database         analytics \
  --table            activity_log \
  --start-date       2019-10-01 \
  --end-date         2019-11-30
```

S3 로 옮기려면 `s3a://...` 경로 + Hadoop AWS 의존성을 추가하면 됩니다.

---

## 디렉터리

```
activity-log/
├── Dockerfile              # 멀티 스테이지 (gradle 빌더 → apache/spark 런타임)
├── docker-compose.yml      # ingest / report
├── build.gradle.kts
├── src/
│   ├── main/java/com/example/activitylog/
│   │   ├── jobs/           # ActivityLogJob, WauReport (main)
│   │   ├── config/         # CliFlags, ActivityLogConfig, Defaults, SparkConfigs
│   │   ├── io/             # ActivityLogReader, ActivityLogWriter
│   │   ├── transform/      # SessionAssigner  (gap-and-island)
│   │   ├── catalog/        # HiveTableManager
│   │   └── recovery/       # BatchCheckpoint, JobState
│   ├── main/resources/
│   │   ├── ddl/activity_log.sql
│   │   └── sql/{wau_by_user,wau_by_session}.sql
│   └── test/java/com/example/activitylog/
│       ├── transform/SessionAssignerTest.java
│       └── jobs/WauReportTest.java
└── README.md
```
