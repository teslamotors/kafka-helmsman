java_library(
    name = "kafka_2_12",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/org/apache/kafka/kafka_2_12",
    ],
    runtime_deps = [
        ":kafka_clients",
        "//3rdparty/jvm/com/101tec:zkclient",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/typesafe/scala_logging:scala_logging_2_12",
        "//3rdparty/jvm/com/yammer/metrics:metrics_core",
        "//3rdparty/jvm/net/sf/jopt_simple",
        "//3rdparty/jvm/org/apache/zookeeper",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/scala_lang:scala_reflect",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
    ],
)

java_library(
    name = "kafka_clients",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/org/apache/kafka/kafka_clients",
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/github/luben:zstd_jni",
        "//3rdparty/jvm/org/lz4:lz4_java",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        "//3rdparty/jvm/org/xerial/snappy:snappy_java",
    ],
)

java_library(
    name = "kafka_clients_jar_test",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/org/apache/kafka/kafka_clients_jar_test",
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/github/luben:zstd_jni",
        "//3rdparty/jvm/org/lz4:lz4_java",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
        "//3rdparty/jvm/org/xerial/snappy:snappy_java",
    ],
)
