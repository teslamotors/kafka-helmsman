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
