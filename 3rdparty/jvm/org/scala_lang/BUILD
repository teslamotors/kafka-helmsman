java_library(
    name = "scala_library",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/org/scala_lang/scala_library",
    ],
)

java_library(
    name = "scala_reflect",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/scala_lang/scala_reflect",
    ],
    runtime_deps = [
        ":scala_library",
    ],
)
