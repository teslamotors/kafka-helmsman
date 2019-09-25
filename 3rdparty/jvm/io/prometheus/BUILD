java_library(
    name = "simpleclient",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/prometheus/simpleclient",
    ],
)

java_library(
    name = "simpleclient_common",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/prometheus/simpleclient_common",
    ],
    runtime_deps = [
        ":simpleclient",
    ],
)

java_library(
    name = "simpleclient_hotspot",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/prometheus/simpleclient_hotspot",
    ],
    runtime_deps = [
        ":simpleclient",
    ],
)

java_library(
    name = "simpleclient_httpserver",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/io/prometheus/simpleclient_httpserver",
    ],
    runtime_deps = [
        ":simpleclient",
        ":simpleclient_common",
    ],
)
