java_test(
    name = "TopicServiceImplTest",
    srcs = [
        "TopicServiceImplTest.java",
    ],
    deps = [
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
        "//kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer",
        "@junit_junit//jar",
    ],
)

java_test(
    name = "TopicEnforcerTest",
    srcs = [
        "TopicEnforcerTest.java",
    ],
    deps = [
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
        "//kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer",
    ],
)

java_test(
    name = "ConfigDriftTest",
    srcs = [
        "ConfigDriftTest.java",
    ],
    deps = [
        "//kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer",
    ],
)
