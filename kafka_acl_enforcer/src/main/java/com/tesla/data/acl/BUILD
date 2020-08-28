package(default_visibility = ["//:__subpackages__"])

java_library(
    name = "enforcer",
    srcs = glob(["*.java"]),
    deps = [
        "//3rdparty/jvm/com/beust:jcommander",
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
        "//kafka_acl_enforcer/src/main/java/com/tesla/data/acl/mixin",
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)

java_binary(
    name = "Main",
    main_class = "com.tesla.data.acl.Main",
    visibility = ["//visibility:public"],
    runtime_deps = [
        ":enforcer",
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
    ],
)
