java_library(
    name = "mockwebserver",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/squareup/okhttp3/mockwebserver",
    ],
    runtime_deps = [
        ":okhttp",
        "//3rdparty/jvm/junit",
        "//3rdparty/jvm/org/bouncycastle:bcprov_jdk15on",
    ],
)

java_library(
    name = "okhttp",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/squareup/okhttp3/okhttp",
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/squareup/okio",
    ],
)
