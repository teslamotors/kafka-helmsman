java_library(
    name = "jackson_annotations",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/fasterxml/jackson/core/jackson_annotations",
    ],
)

java_library(
    name = "jackson_core",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/fasterxml/jackson/core/jackson_core",
    ],
)

java_library(
    name = "jackson_databind",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/fasterxml/jackson/core/jackson_databind",
    ],
    runtime_deps = [
        ":jackson_annotations",
        ":jackson_core",
        "//3rdparty/jvm/net/bytebuddy:byte_buddy",
    ],
)
