java_test(
    name = "CommandTest",
    srcs = [
        "CommandTest.java",
    ],
    deps = [
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer",
    ],
)

java_test(
    name = "EnforceCommandTest",
    srcs = [
        "EnforceCommandTest.java",
    ],
    deps = [
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
        "//3rdparty/jvm/io/prometheus:simpleclient_httpserver",
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
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
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

java_test(
    name = "ClusterTopicsTest",
    srcs = [
        "ClusterTopicsTest.java",
    ],
    deps = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/fasterxml/jackson/dataformat:jackson_dataformat_yaml",
        "//kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer",
    ],
)
