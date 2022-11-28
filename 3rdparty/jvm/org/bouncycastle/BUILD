java_library(
    name = "bcpkix_jdk15on",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/org/bouncycastle/bcpkix_jdk15on",
    ],
    runtime_deps = [
        ":bcprov_jdk15on",
        ":bcutil_jdk15on",
    ],
)

java_library(
    name = "bcprov_jdk15on",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/org/bouncycastle/bcprov_jdk15on",
    ],
)

java_library(
    name = "bcutil_jdk15on",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/org/bouncycastle/bcutil_jdk15on",
    ],
    runtime_deps = [
        ":bcprov_jdk15on",
    ],
)
