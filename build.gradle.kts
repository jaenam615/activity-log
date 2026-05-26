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

// IntelliJ 가 compileOnly 의존성을 PROVIDED 스코프로 인식하게 강제.
// 안 그러면 Scala trait 등 transitive 클래스를 못 찾아 "Cannot access ..." 에러가 뜸.
idea {
    module {
        scopes["PROVIDED"]?.get("plus")?.add(configurations.compileOnly.get())
    }
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
