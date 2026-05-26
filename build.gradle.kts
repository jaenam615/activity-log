// Gradle Kotlin DSL (build.gradle.kts) — Kotlin 문법과 동일합니다.
plugins {
    java
    // shadow 플러그인은 fat jar (모든 의존성 포함 단일 JAR) 를 만들어 줍니다.
    // gradleup.shadow 8.3.6 은 Gradle 9 호환 (구 johnrengelman/shadow 는 미지원).
    id("com.gradleup.shadow") version "8.3.6"
}

group = "com.backpackr"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val sparkVersion = "3.5.1"
// Spark 가 Scala 로 작성되어 있어서 jar 이름에 Scala 버전이 붙습니다 (_2.13).
val sparkScalaSuffix = "_2.13"

repositories {
    mavenCentral()
}

dependencies {
    // 운영(spark-submit) 환경에서는 클러스터가 이미 Spark 를 가지고 있으므로
    // 컴파일 시에만 필요하고 fat jar 에는 포함하지 않습니다 (compileOnly).
    compileOnly("org.apache.spark:spark-core$sparkScalaSuffix:$sparkVersion")
    compileOnly("org.apache.spark:spark-sql$sparkScalaSuffix:$sparkVersion")
    compileOnly("org.apache.spark:spark-hive$sparkScalaSuffix:$sparkVersion")

    // 테스트에서는 임베디드로 Spark 를 실행하므로 runtime 까지 필요합니다.
    testImplementation("org.apache.spark:spark-core$sparkScalaSuffix:$sparkVersion")
    testImplementation("org.apache.spark:spark-sql$sparkScalaSuffix:$sparkVersion")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "2g"
    // Spark 3.5 는 공식적으로 Java 17 까지만 지원합니다. Java 21 에서 돌리려면
    // 내부 모듈 접근을 위한 --add-opens 플래그가 필요합니다.
    jvmArgs(
        "-XX:+IgnoreUnrecognizedVMOptions",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }
}

tasks.shadowJar {
    archiveClassifier = ""
    mergeServiceFiles()
    manifest {
        attributes("Main-Class" to "com.backpackr.activitylog.jobs.ActivityLogJob")
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}
