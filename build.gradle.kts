plugins {
    id("java-library")
}

group = "com.tavari"
version = "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.11.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()

    jvmArgs(
        "-Xmx4G",
        "-Xms1G",
        "-XX:+UseG1GC",
        "-XX:+ParallelRefProcEnabled",
        "-XX:MaxGCPauseMillis=20",
        "-XX:G1HeapRegionSize=4M",    // smaller regions for high task churn
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+DisableExplicitGC"
    )
}