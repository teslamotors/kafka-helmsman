java_test(
    name = "FreshnessTrackerTest",
    srcs = ["FreshnessTrackerTest.java"],
    deps = [
        "//3rdparty/jvm/junit",
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//kafka_consumer_freshness_tracker/src/main/java/com/tesla/data/consumer/freshness",
    ],
)

java_test(
    name = "BurrowTest",
    srcs = ["BurrowTest.java"],
    deps = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/squareup/okhttp3:okhttp",
        "//3rdparty/jvm/junit",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
        "//kafka_consumer_freshness_tracker/src/main/java/com/tesla/data/consumer/freshness",
    ],
)

java_test(
    name = "ConsumerFreshnessTest",
    srcs = ["ConsumerFreshnessTest.java"],
    resources = [
        "//kafka_consumer_freshness_tracker/src/test/resources:test_deps",
    ],
    deps = [
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/io/prometheus:simpleclient",
        "//3rdparty/jvm/junit",
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
        "//kafka_consumer_freshness_tracker/src/main/java/com/tesla/data/consumer/freshness",
    ],
)
