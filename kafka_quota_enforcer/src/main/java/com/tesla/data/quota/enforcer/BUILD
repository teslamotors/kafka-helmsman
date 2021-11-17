package(default_visibility = ["//visibility:public"])

java_library(
    name = "enforcer",
    srcs = glob(["*.java"]),
    deps = [
        "//3rdparty/jvm/com/beust:jcommander",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_annotations",
        "//3rdparty/jvm/org/apache/kafka:kafka_2_12",
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)

java_binary(
    name = "Main",
    main_class = "com.tesla.data.quota.enforcer.Main",
    runtime_deps = [
        ":enforcer",
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
    ],
)
