plugins {
    java
    idea
    id("com.gradleup.shadow") version "8.3.6"
}

group = "com.example"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

val sparkVersion = "4.0.2"
val sparkScalaSuffix = "_2.13"

repositories {
    mavenCentral()
}

dependencies {
    compileOnly("org.apache.spark:spark-core$sparkScalaSuffix:$sparkVersion")
    compileOnly("org.apache.spark:spark-sql$sparkScalaSuffix:$sparkVersion")
    compileOnly("org.apache.spark:spark-hive$sparkScalaSuffix:$sparkVersion")

    testImplementation("org.apache.spark:spark-core$sparkScalaSuffix:$sparkVersion")
    testImplementation("org.apache.spark:spark-sql$sparkScalaSuffix:$sparkVersion")
    testImplementation("org.apache.spark:spark-hive$sparkScalaSuffix:$sparkVersion")

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "2g"
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }
}

tasks.shadowJar {
    archiveClassifier = ""
    mergeServiceFiles()
    manifest {
        attributes("Main-Class" to "com.example.activitylog.jobs.ActivityLogJob")
    }
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

val sampleInputs = "data/raw-sample/2019-Oct-sample.csv,data/raw-sample/2019-Nov-sample.csv"
val sampleWarehouse = "file://${projectDir}/warehouse"

tasks.register<JavaExec>("runIngest") {
    group = "application"
    description = "Run ActivityLogJob locally on sample CSVs (uses test classpath for Spark)"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass = "com.example.activitylog.jobs.ActivityLogJob"
    maxHeapSize = "4g"
    systemProperty("spark.master", "local[*]")
    systemProperty("spark.ui.enabled", "false")
    systemProperty("spark.sql.shuffle.partitions", "8")
    jvmArgs(
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    )
    args = listOf(
        "--input", sampleInputs,
        "--warehouse-path", sampleWarehouse,
        "--checkpoint-path", sampleWarehouse,
        "--database", "analytics",
        "--table", "activity_log",
        "--start-date", "2019-10-01",
        "--end-date", "2019-11-30",
    )
}

tasks.register<JavaExec>("runWau") {
    group = "application"
    description = "Run WauReport locally on the ingested warehouse"
    classpath = sourceSets["test"].runtimeClasspath
    mainClass = "com.example.activitylog.jobs.WauReport"
    maxHeapSize = "2g"
    systemProperty("spark.master", "local[*]")
    systemProperty("spark.ui.enabled", "false")
    systemProperty("spark.sql.shuffle.partitions", "8")
    jvmArgs(
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
    )
    args = listOf(
        "--database", "analytics",
        "--table", "activity_log",
        "--from", "2019-10-01",
        "--to", "2019-11-30",
    )
}
