java_library(
    name = "collector",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/prometheus/jmx/collector",
    ],
    runtime_deps = [
        "//3rdparty/jvm/io/prometheus:simpleclient",
        "//3rdparty/jvm/org/yaml:snakeyaml",
    ],
)

java_library(
    name = "jmx_prometheus_javaagent",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/prometheus/jmx/jmx_prometheus_javaagent",
    ],
)
