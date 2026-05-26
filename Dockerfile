# syntax=docker/dockerfile:1.7

# =============================================================================
# 1단계 — 빌더: Java 21 위에서 ./gradlew shadowJar 로 fat jar 생성.
#
# - eclipse-temurin:21-jdk 를 베이스로 사용 (Corretto/Microsoft 와 동등한 OpenJDK 21).
# - gradle 은 wrapper(`./gradlew`) 가 자동 다운로드 → 호스트의 gradle 버전과 무관하게
#   gradle-wrapper.properties 에 명시된 버전(9.5.1) 으로 빌드되어 재현성 확보.
# - .dockerignore 가 build/, .gradle/, .idea/ 등을 걸러 컨텍스트 크기를 줄임.
# =============================================================================
FROM eclipse-temurin:21-jdk AS builder

WORKDIR /build

# 의존성 캐시 효율: gradle 설정 파일만 먼저 복사해 두면 src 만 바뀌었을 때
# gradle 의존성 다운로드 레이어를 재사용.
COPY gradlew gradlew.bat ./
COPY gradle ./gradle
COPY settings.gradle.kts build.gradle.kts gradle.properties ./

RUN chmod +x gradlew && ./gradlew --version

COPY src ./src

# --no-daemon: 컨테이너 빌드 한 번이면 끝나므로 daemon 띄울 필요 없음.
# -x test: 빌드 이미지에서 테스트는 별도 단계로 분리하는 게 일반적.
RUN ./gradlew shadowJar --no-daemon -x test

# =============================================================================
# 2단계 — 런타임: Apache 공식 Spark 4.0.2 (Java 21 variant).
#
# - 이미 spark-submit, JVM 21, Hadoop 클라이언트 라이브러리가 모두 포함되어 있음.
# - 우리는 빌더에서 만든 thin jar(약 50KB; Spark 의존성은 compileOnly) 만 얹는다.
# =============================================================================
FROM apache/spark:4.0.2-java21

# /opt/app 에 앱 자산을 모은다. Spark 가 쓰는 임베디드 Hive Derby metastore(`metastore_db/`)
# 와 로그(`derby.log`) 가 WORKDIR 아래로 떨어지므로, 컴포즈에서 이 경로를 볼륨으로
# 마운트하면 두 잡(ingest/report) 이 같은 메타스토어를 공유할 수 있다.
ENV APP_HOME=/opt/app
WORKDIR ${APP_HOME}

# 베이스 이미지는 비-root `spark` 사용자(UID 185) 로 동작. 호스트 바인드 마운트에
# 쓰기 권한 문제가 생기지 않도록 런타임에서 root 로 복귀.
USER root
RUN mkdir -p ${APP_HOME} && chmod -R 0777 ${APP_HOME}

COPY --from=builder /build/build/libs/activity-log-0.1.0.jar ${APP_HOME}/activity-log.jar

# 기본 동작: spark-submit 를 entrypoint 로 두고, 컴포즈/run 에서 인자만 넘기면 됨.
#   docker run --rm <image> --class com.example.activitylog.jobs.ActivityLogJob \
#     --master 'local[*]' /opt/app/activity-log.jar --input ... --warehouse-path ...
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--help"]
