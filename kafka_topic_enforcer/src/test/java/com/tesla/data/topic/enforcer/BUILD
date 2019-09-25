java_test(
    name = "MainTest",
    srcs = [
        "MainTest.java",
    ],
    deps = [
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer",
    ],
)

java_test(
    name = "TopicServiceImplTest",
    srcs = [
        "TopicServiceImplTest.java",
    ],
    deps = [
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer",
    ],
)

java_test(
    name = "EnforcerTest",
    srcs = [
        "EnforcerTest.java",
    ],
    deps = [
        "//3rdparty/jvm/org/mockito:mockito_core",
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
