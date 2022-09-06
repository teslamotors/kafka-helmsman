java_library(
    name = "zookeeper",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/org/apache/zookeeper/zookeeper",
    ],
    runtime_deps = [
        ":zookeeper_jute",
        "//3rdparty/jvm/io/netty:netty_handler",
        "//3rdparty/jvm/io/netty:netty_transport_native_epoll",
        "//3rdparty/jvm/org/apache/yetus:audience_annotations",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
    ],
)

java_library(
    name = "zookeeper_jute",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/apache/zookeeper/zookeeper_jute",
    ],
    runtime_deps = [
        "//3rdparty/jvm/org/apache/yetus:audience_annotations",
    ],
)
