java_library(
    name = "mockito_all",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/org/mockito/mockito_all",
    ],
)

java_library(
    name = "mockito_core",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/org/mockito/mockito_core",
    ],
    runtime_deps = [
        "//3rdparty/jvm/org/hamcrest:hamcrest_core",
        "//3rdparty/jvm/org/objenesis",
    ],
)
