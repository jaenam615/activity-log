# 백패커/아이디어스 DE 사전 과제 — Activity Log

Kaggle "ecommerce behavior data from multi-category store"(2019-Oct / 2019-Nov)
원본 CSV 를 Hive External Table 로 적재하고, KST 일자 파티션 위에서 WAU 를
계산하는 Spark 애플리케이션입니다.

스택: **Java 21 (Corretto) · Spark 4.0.2 · Gradle 9 (Kotlin DSL)**

## 1. 언어 선택 — Java

- 과제가 **Scala 또는 Java** 만 허용함.
- Spark API 는 Java/Scala 둘 다 1급 지원. 동일 로직을 Java 로 작성해도 표현력·
  성능 차이는 없습니다 (`Dataset<Row>` API, Window 함수 모두 그대로).
- 본 과제는 **"인터뷰에서 코드 라인 단위 설명"** 이 명시되어 있으므로, 코드
  양이 조금 늘더라도 가독성·설명 용이성이 더 중요하다고 판단해 Java 를 선택.
- 부수 효과:
  - `record` (Java 14+) 로 설정 객체를 immutable 하게 표현.
  - 텍스트 블록 `"""..."""` (Java 15+) 으로 SQL/DDL 가독성 확보.
  - switch expression (Java 17+) 으로 CLI 파싱이 간결.

## 2. 아키텍처 한눈에 보기

```
2019-Oct.csv ┐
2019-Nov.csv ┤  1) ActivityLogReader     — 스키마 강제 적용 CSV 로딩
             ▼
             ┤  2) SessionAssigner       — UTC→KST, 5분 갭 규칙 session_id
             ▼
             ┤  3) ActivityLogWriter     — partitionBy(event_date), parquet+snappy
             ▼                              + dynamic partition overwrite
             ┤  4) HiveTableManager      — CREATE EXTERNAL ... + MSCK REPAIR
             ▼
       {warehouse}/event_date=YYYY-MM-DD/*.snappy.parquet
             │
             │  WauReport.wauByUser / wauBySession (SELECT 쿼리)
             ▼
       주차별 WAU 두 가지

   BatchCheckpoint 가 1~4 전체를
   {warehouse}/_jobs/state={started|success|failed|recovered}/{runId}/marker.json
   로 감싸 장애 복구를 보조합니다.
```

패키지 구조 — 책임 기반 레이어드:

| 패키지 | 역할 | 파일 |
| --- | --- | --- |
| `jobs/`      | Spark 잡 진입점 (main) | `ActivityLogJob`, `WauReport` |
| `config/`    | 설정/CLI 파싱 | `ActivityLogConfig` |
| `io/`        | 외부 데이터 입출력 | `ActivityLogReader`, `ActivityLogWriter` |
| `transform/` | 비즈니스 로직 변환 (★ 핵심) | `SessionAssigner` |
| `catalog/`   | Hive 메타스토어 관리 | `HiveTableManager` |
| `recovery/`  | 운영 신뢰성 / 장애 복구 | `BatchCheckpoint` |

각 패키지의 책임이 한 줄로 설명되도록 분리했습니다. 요구사항 변형 시 손댈 위치가 명확해집니다:

- "세션 규칙 변경" → `transform/` 만
- "Iceberg 테이블로 전환" → `catalog/`, `io/Writer` 만
- "장애 알림 추가" → `recovery/` 만
- "S3 대신 GCS" → `io/`, `catalog/` 만

`resources/` 의 SQL/DDL 은 문서/참고용:

- `resources/sql/wau_by_user.sql`, `wau_by_session.sql` — WAU 쿼리
- `resources/ddl/activity_log.sql` — External Table DDL (런타임 DDL 은 `catalog.HiveTableManager` 가 생성)

## 3. 빌드 / 실행

### 3-0. 사전 준비

- **Java 21** (Corretto 21 / Microsoft 21 / OpenJDK 21 어느 것이든 OK)
- Gradle 은 `./gradlew` 래퍼로 자동 다운로드되므로 별도 설치 불필요.

### 3-1. 테스트

```bash
./gradlew test
```
6 케이스 통과 (KST 일자 추출, 5분 갭 경계 조건, 세션 ID 결정성, 비정렬 입력,
WAU 라운드트립).

### 3-2. fat jar 빌드

```bash
./gradlew shadowJar
# → build/libs/activity-log-0.1.0.jar  (Spark 의존성 제외한 thin assembly)
```

### 3-3. 적재 잡 실행

```bash
spark-submit \
  --class com.example.activitylog.jobs.ActivityLogJob \
  --master 'local[*]' \
  build/libs/activity-log-0.1.0.jar \
  --input            data/raw/2019-Oct.csv,data/raw/2019-Nov.csv \
  --warehouse-path   file:///tmp/warehouse/activity_log \
  --checkpoint-path  file:///tmp/warehouse/activity_log \
  --database         analytics \
  --table            activity_log \
  --start-date       2019-10-01 \
  --end-date         2019-11-30
```

CSV 는 Kaggle 페이지에서 받아 `data/raw/` 에 두세요 (`2019-Oct.csv` ≈ 5.3GB,
`2019-Nov.csv` ≈ 9.0GB). S3 로 바꾸려면 `s3a://...` 와 Hadoop AWS 의존성을
함께 추가하면 됩니다.

### 3-4. WAU 리포트 실행

```bash
spark-submit \
  --class com.example.activitylog.jobs.WauReport \
  --master 'local[*]' \
  build/libs/activity-log-0.1.0.jar \
  --database analytics --table activity_log \
  --from 2019-10-01 --to 2019-11-30
```

> ℹ️ Spark 4.0 부터 Java 21 을 공식 지원하므로 `--add-opens` 같은 JVM hack 이
> 필요 없습니다. 운영 클러스터의 Spark 가 4.0 이상인지 사전 확인 권장.

### 3-5. Docker 로 실행 (재현성 보장)

호스트의 Java/Spark/Hadoop 버전과 무관하게 동일한 환경에서 두 잡을 돌리려면:

```bash
# 0) Kaggle CSV 두 개를 ./data/ 에 배치
mkdir -p data
# data/2019-Oct.csv, data/2019-Nov.csv

# 1) 이미지 빌드 (멀티 스테이지: gradle 빌더 → apache/spark:4.0.2-java21 런타임)
docker compose build

# 2) 적재 잡
docker compose run --rm ingest

# 3) WAU 리포트
docker compose run --rm report

# 또는 한 줄: ingest 가 성공한 직후 report 까지 자동 실행
docker compose up --build report
```

구조 요약:

| 컴포넌트 | 역할 |
| --- | --- |
| `Dockerfile` | `eclipse-temurin:21-jdk` 에서 `./gradlew shadowJar` → `apache/spark:4.0.2-java21` 위에 jar 만 얹는 멀티 스테이지. |
| `docker-compose.yml` | `ingest`/`report` 두 서비스. `warehouse_data`, `metastore_data` named volume 으로 parquet · Hive Derby metastore 공유. |
| `.dockerignore` | `build/`, `.gradle/`, `data/`, `.idea/` 등 빌드 컨텍스트에서 제외. |

날짜 범위·테이블 이름·입력 경로 등은 `docker-compose.yml` 의 `command:` 블록에서
수정합니다 (코드의 CLI 플래그명은 `config/CliFlags.java` 한 곳에서만 정의).

## 4. 요구사항 ↔ 구현 매핑

| 요구사항 | 구현 위치 |
| --- | --- |
| ① 사용자 activity 로그를 Hive table 로 제공하기 위한 Spark Application | `ActivityLogJob` + `HiveTableManager` |
| ② KST 기준 daily partition 처리 | `SessionAssigner` 가 `from_utc_timestamp(..., "Asia/Seoul")` 로 KST 변환 후 `to_date` → `event_date` → `partitionBy("event_date")` |
| ③ 동일 user_id 내 5분 이상 간격 = 새 세션 ID | `SessionAssigner.assign` 의 `lag` + `sum(is_new_session)` window. `>=` 조건이라 정확히 5분도 새 세션 |
| ④ 재처리 후 parquet, snappy | `ActivityLogWriter` 의 `.option("compression","snappy").parquet(...)` + DDL `TBLPROPERTIES('parquet.compression'='SNAPPY')` |
| ⑤ External Table 방식, 추가 기간 대응 | `CREATE EXTERNAL TABLE IF NOT EXISTS ... LOCATION '...'` + `MSCK REPAIR TABLE`. `--start-date/--end-date` 만 바꿔 재실행하면 신규 파티션 자동 등록 |
| ⑥ 배치 장애시 복구 장치 | `BatchCheckpoint` 가 상태 마커 (`started/success/failed/recovered`) 를 기록하고, 시작 시 미완료(`started`) 항목을 감지. 데이터 측은 dynamic partition overwrite 로 idempotent |
| ⑦ Hive 외부 테이블로 WAU 산출 | `WauReport.wauByUser` / `wauBySession`, `resources/sql/wau_by_*.sql` |
| ⑦-a user_id 기준 WAU | 본 문서 §6 또는 `wau_by_user.sql` |
| ⑦-b 세션 ID 기준 WAU | 본 문서 §6 또는 `wau_by_session.sql` |
| ⑦-c 결과값 + 쿼리 | §6 (실 데이터 실행 결과 채우기) |
| ⑧ Scala/Java 중 선택 + 사유 | 본 문서 §1 |

## 5. 세션 부여 규칙 상세

핵심은 **"gap-and-island"** 라고 부르는 SQL/Spark 의 클래식 패턴입니다.

```
입력 (user=1) │ lag(prev_ts) │ gap_sec │ is_new │ sum(is_new) = session_seq
─────────────┼─────────────┼─────────┼────────┼─────────────────────────
00:00         │ null        │ null    │   1    │   1
00:04         │ 00:00       │  240    │   0    │   1
00:10         │ 00:04       │  360    │   1    │   2     ← 6분 갭, 새 세션
00:30         │ 00:10       │ 1200    │   1    │   3     ← 새 세션
```

이후 `(user_id, session_seq)` 묶음에서 `min(event_ts) = session_start` 를 뽑아
`session_id = "{user_id}_{session_start_epoch_seconds}"` 로 만듭니다.

- **결정성**: 같은 입력은 같은 session_id 가 나오므로 재처리 안전.
- **KST 변환**: `event_time` 의 " UTC" 접미를 제거하고 `to_utc_timestamp(...,"UTC")`
  로 UTC 임을 못박은 뒤, `from_utc_timestamp(..., "Asia/Seoul")` 로 KST 로 옮겨
  날짜 부분만 떼서 파티션 키로 사용. 예: `2019-10-31 15:30 UTC` →
  `2019-11-01 00:30 KST` → `event_date=2019-11-01`.
- **한계 (정직하게 기록)**: 세션이 일/입력 경계를 가로지를 때, 시작 이벤트가
  입력 범위 밖이면 session_start 가 잘릴 수 있습니다. 운영에서는 재처리할 일자
  범위의 직전 1일 CSV 를 입력에 포함시켜 보정합니다.

## 6. WAU 정의·쿼리·결과

`date_trunc('WEEK', event_date)` 는 ISO-8601 — **월요일 시작**. 두 쿼리가 같은
주 버킷을 쓰므로 1:1 비교 가능합니다.

### 6-a. user_id 기준

```sql
SELECT
  date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
  COUNT(DISTINCT user_id) AS wau_users
FROM analytics.activity_log
WHERE event_date BETWEEN DATE '2019-10-01' AND DATE '2019-11-30'
GROUP BY date_trunc('WEEK', event_date)
ORDER BY week_start_kst;
```

### 6-b. session_id 기준

```sql
SELECT
  date_format(date_trunc('WEEK', event_date), 'yyyy-MM-dd') AS week_start_kst,
  COUNT(DISTINCT session_id) AS wau_sessions
FROM analytics.activity_log
WHERE event_date BETWEEN DATE '2019-10-01' AND DATE '2019-11-30'
GROUP BY date_trunc('WEEK', event_date)
ORDER BY week_start_kst;
```

### 6-c. 결과값

본 저장소에 데이터셋(약 14GB) 을 포함하지 않으므로, 동일 환경에서 재현하려면
§3.3 (적재) → §3.4 (리포트) 를 순서대로 실행하면 됩니다. 아래 표는 그 실행
결과로 채워집니다.

WAU by user_id:

| week_start_kst (Mon) | wau_users |
| --- | --- |
| 2019-09-30 | _execute WauReport to fill_ |
| 2019-10-07 | _..._ |
| 2019-10-14 | _..._ |
| 2019-10-21 | _..._ |
| 2019-10-28 | _..._ |
| 2019-11-04 | _..._ |
| 2019-11-11 | _..._ |
| 2019-11-18 | _..._ |
| 2019-11-25 | _..._ |

WAU by session_id:

| week_start_kst (Mon) | wau_sessions |
| --- | --- |
| 2019-09-30 | _execute WauReport to fill_ |
| ... | _..._ |

> 비교 포인트: 한 유저가 한 주에 여러 번 이탈/재진입하면 `wau_sessions` 가
> `wau_users` 보다 큽니다. 비율 `wau_sessions / wau_users` 가 "주간 1인당 평균
> 방문 횟수" 의 근사치.

## 7. 장애 복구 시나리오

`BatchCheckpoint` 가 두 축으로 작동합니다.

1. **데이터 측 멱등** — `spark.sql.sources.partitionOverwriteMode=dynamic` 이
   설정되어 있어, 동일 `--start-date`/`--end-date` 로 재실행 시 그 일자
   파티션만 다시 쓰고 다른 일자는 그대로 둡니다. 잡이 절반 망해도 같은 인자로
   재실행 = 데이터 무결성 유지.
2. **상태 마커** — 잡 시작 시 `_jobs/state=started/{runId}/marker.json` 을
   기록하고, 성공 시 `state=success/` 로, 예외 시 `state=failed/` 로 atomic
   rename. 잡이 시작될 때 `state=started/` 에 남아 있는 항목은 직전 실행이
   graceful shutdown 없이 죽었다는 신호 → 자동으로 `state=recovered/` 로
   옮기고 로그에 안내 (운영자는 동일 인자로 재실행하면 됨).
3. **메타데이터 멱등** — `CREATE EXTERNAL TABLE IF NOT EXISTS`,
   `MSCK REPAIR TABLE` 모두 여러 번 호출해도 안전.

운영 가이드:
- 동일 기간 재적재: 그냥 같은 명령 다시 실행. 데이터 중복/손실 없음.
- 추가 기간: 새 `--start-date`/`--end-date` 로 실행. MSCK REPAIR 가 자동으로
  새 파티션을 메타스토어에 등록.
- 카탈로그 손상: `MSCK REPAIR TABLE` 단독 실행으로 파일시스템상 모든 파티션
  재등록 가능.

## 8. 테스트

`./gradlew test` (Java 21 + Spark 4.0.2 임베디드) — 6 케이스:

`SessionAssignerTest`
- ✅ 5분 이상 간격이면 새 세션을 생성한다
- ✅ 정확히 5분 간격은 새 세션 (`>=` 경계 조건)
- ✅ session_id 는 (user_id, session_start_epoch) 로 결정적
- ✅ event_date 는 KST 기준으로 산출된다
- ✅ 정렬되지 않은 입력에서도 세션 경계가 정확하다

`WauReportTest`
- ✅ WAU by user / by session 가 동일 주차에서 일관된 결과를 낸다
  (실제 parquet 파티션을 만들고 `wau_by_user.sql`/`wau_by_session.sql` 패턴의
  쿼리를 돌려 기대값 검증)

## 9. AI 도구 사용 내역

- **도구**: Claude Code (Claude Opus 4.7)
- **활용 범위**
  - `build.gradle.kts` boilerplate (Java 21 toolchain, shadow plugin) 초안 생성.
  - `SessionAssigner` 의 gap-and-island 패턴 (lag + cumulative sum) 초안.
    Claude 가 제안한 후 직접 검토하고 KST 변환 위치, `>=` 경계 조건,
    session_id 포맷(epoch second 기반) 을 결정·수정.
  - 테스트 시드 데이터 작성.
  - README 표/다이어그램 골격.
- **직접 설계·검증**
  - **언어 전환**: 초안은 Scala 로 작성했으나 본인이 Scala 를 모르고 인터뷰에서
    라인 단위 설명이 필요하다는 점을 인식하고 Java 로 완전 재작성. 이 과정에서
    동작/구조는 1:1 매핑이지만, 모든 코드를 직접 읽고 이해한 뒤 재작성.
  - **빌드 도구**: sbt → Gradle Kotlin DSL 전환 결정. (Kotlin DSL 이 본인에게
    더 익숙)
  - session_id 를 `epoch_seconds` 기반으로 정한 것 — 재처리 idempotency 와의
    상호작용 검토 후 결정.
  - `BatchCheckpoint` 의 상태 전이 (`started`→`recovered|success|failed`)
    설계와 atomic rename 가정 명시.
  - dynamic partition overwrite + `MSCK REPAIR` 조합으로 idempotency 를
    보장한다는 의사결정.
  - `event_date` 가 KST 자정 경계인지 (UTC 와 어긋나는지) 를 테스트로 명시
    검증.
- **프롬프트 전략**
  - "요구사항 → 각 컴포넌트 책임 정의 → 각 함수 시그니처 합의 → 구현 요청"
    순서로 작게 쪼개 진행.
  - 매 단계마다 `./gradlew test` 로 즉시 검증 → 다음 단계.
  - Scala 로 짠 초안이 본인 설명 가능성을 충족하지 못한다고 판단한 시점에
    즉시 Java 로 갈아엎고, 라인 단위 설명에 도움이 되는 주석을 코드 안에 같이
    배치하도록 프롬프트 조정.

## 10. 디렉터리 구조

```
activity-log/
├── build.gradle.kts
├── settings.gradle.kts
├── gradle.properties
├── gradlew                     # Gradle wrapper (POSIX)
├── gradlew.bat                 # Gradle wrapper (Windows)
├── gradle/wrapper/...
├── Dockerfile                  # 멀티 스테이지 빌드 (gradle → apache/spark)
├── docker-compose.yml          # ingest / report 두 서비스
├── .dockerignore
├── src/
│   ├── main/
│   │   ├── java/com/example/activitylog/
│   │   │   ├── jobs/
│   │   │   │   ├── ActivityLogJob.java   # 적재 잡 main
│   │   │   │   └── WauReport.java        # WAU 리포트 main
│   │   │   ├── config/
│   │   │   │   ├── ActivityLogConfig.java
│   │   │   │   ├── CliFlags.java          # --input, --warehouse-path ...
│   │   │   │   ├── Defaults.java          # 5분 갭, KST/UTC, snappy ...
│   │   │   │   └── SparkConfigs.java      # spark.sql.* 키/값
│   │   │   ├── io/
│   │   │   │   ├── ActivityLogReader.java
│   │   │   │   └── ActivityLogWriter.java
│   │   │   ├── transform/
│   │   │   │   ├── Columns.java           # 컬럼명 단일 출처
│   │   │   │   └── SessionAssigner.java   # ★ 핵심 비즈니스 로직
│   │   │   ├── catalog/
│   │   │   │   └── HiveTableManager.java
│   │   │   └── recovery/
│   │   │       ├── BatchCheckpoint.java
│   │   │       └── JobState.java          # started/success/failed/recovered
│   │   └── resources/
│   │       ├── ddl/activity_log.sql
│   │       └── sql/
│   │           ├── wau_by_user.sql
│   │           └── wau_by_session.sql
│   └── test/java/com/example/activitylog/
│       ├── SparkTestBase.java              # 공유 베이스 (루트 유지)
│       ├── transform/
│       │   └── SessionAssignerTest.java
│       └── jobs/
│           └── WauReportTest.java
└── README.md
```
