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
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//kafka_acl_enforcer/src/main/java/com/tesla/data/acl:enforcer",
    ],
)
