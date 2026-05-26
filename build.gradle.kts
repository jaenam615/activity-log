plugins {
    java
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
