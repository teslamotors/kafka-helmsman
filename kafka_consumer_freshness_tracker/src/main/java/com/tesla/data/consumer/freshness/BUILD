package(default_visibility = ["//:__subpackages__"])

java_library(
    name = "freshness",
    srcs = glob(["*.java"]),
    deps = [
        "//3rdparty/jvm/com/beust:jcommander",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/fasterxml/jackson/dataformat:jackson_dataformat_yaml",
        "//3rdparty/jvm/com/squareup/okhttp3:okhttp",
        "//3rdparty/jvm/io/prometheus:simpleclient",
        "//3rdparty/jvm/io/prometheus:simpleclient_hotspot",
        "//3rdparty/jvm/io/prometheus:simpleclient_httpserver",
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
    ],
)

java_binary(
    name = "ConsumerFreshness",
    main_class = "com.tesla.data.consumer.freshness.ConsumerFreshness",
    visibility = ["//visibility:public"],
    runtime_deps = [
        ":freshness",
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
    ],
)
