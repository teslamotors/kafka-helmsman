java_library(
    name = "enforcer",
    srcs = glob(["*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//3rdparty/jvm/com/beust:jcommander",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_annotations",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/fasterxml/jackson/dataformat:jackson_dataformat_yaml",
        "//3rdparty/jvm/com/fasterxml/jackson/module:jackson_module_parameter_names",
        "//3rdparty/jvm/io/prometheus:simpleclient",
        "//3rdparty/jvm/io/prometheus:simpleclient_httpserver",
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        "//3rdparty/jvm_shaded/com/google/guava_shaded",
    ],
)
