package(default_visibility = ["//:__subpackages__"])

java_library(
    name = "mixin",
    srcs = glob(["*.java"]),
    deps = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_annotations",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/org/apache/kafka:kafka_clients",
    ],
)
