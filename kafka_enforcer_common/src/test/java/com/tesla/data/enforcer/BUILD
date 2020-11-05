java_library(
    name = "dummy",
    srcs = [
        "Dummy.java",
        "DummyEnforcer.java",
    ],
    deps = [
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)

java_test(
    name = "ClusterEntitiesTest",
    srcs = [
        "ClusterEntitiesTest.java",
    ],
    deps = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/fasterxml/jackson/dataformat:jackson_dataformat_yaml",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)

java_test(
    name = "BaseCommandTest",
    srcs = [
        "BaseCommandTest.java",
    ],
    deps = [
        ":dummy",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)

java_test(
    name = "EnforceCommandTest",
    srcs = [
        "EnforceCommandTest.java",
    ],
    deps = [
        ":dummy",
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
        "//3rdparty/jvm/io/prometheus:simpleclient_httpserver",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)

java_test(
    name = "EnforcerTest",
    srcs = [
        "EnforcerTest.java",
    ],
    deps = [
        ":dummy",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)
