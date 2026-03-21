plugins {
    id("java")
}

group = "com.polling.jdbc.connector"
version = "1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_23
    targetCompatibility = JavaVersion.VERSION_23
}

repositories {
    mavenCentral()
}

val flinkVersion = "2.2.0"

dependencies {
    // Flink
    compileOnly("org.apache.flink:flink-core:$flinkVersion")
    compileOnly("org.apache.flink:flink-streaming-java:$flinkVersion")
    compileOnly("org.apache.flink:flink-connector-base:$flinkVersion")

    // Logging
    implementation("org.slf4j:slf4j-api:2.0.16")

    // JDBC drivers
    implementation("com.mysql:mysql-connector-j:9.6.0")
    implementation("org.postgresql:postgresql:42.7.10")

    // Test
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("com.h2database:h2:2.4.240")
    testImplementation("org.awaitility:awaitility:4.2.2")
    testImplementation("org.apache.flink:flink-core:$flinkVersion")
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    testImplementation("org.apache.flink:flink-connector-base:$flinkVersion")
}

tasks.test {
    useJUnitPlatform()
}
