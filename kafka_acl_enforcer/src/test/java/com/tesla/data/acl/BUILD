package(default_visibility = ["//:__subpackages__"])

java_library(
    name = "acl_config",
    srcs = ["AclConfig.java"],
    deps = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/fasterxml/jackson/dataformat:jackson_dataformat_yaml",
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//kafka_acl_enforcer/src/main/java/com/tesla/data/acl/mixin",
    ],
)

java_test(
    name = "AclEnforcerTest",
    srcs = ["AclEnforcerTest.java"],
    deps = [
        ":acl_config",
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
        "//3rdparty/jvm/ch/qos/logback:logback_core",
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        "//kafka_acl_enforcer/src/main/java/com/tesla/data/acl:enforcer",
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)
