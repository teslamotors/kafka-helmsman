java_library(
    name = "failureaccess",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/google/guava/failureaccess",
    ],
)

java_library(
    name = "guava",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/google/guava/guava",
    ],
    runtime_deps = [
        ":failureaccess",
        ":listenablefuture",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/errorprone:error_prone_annotations",
        "//3rdparty/jvm/com/google/j2objc:j2objc_annotations",
        "//3rdparty/jvm/org/checkerframework:checker_qual",
    ],
)

java_library(
    name = "guava_testlib",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/google/guava/guava_testlib",
    ],
    runtime_deps = [
        ":guava",
        "//3rdparty/jvm/com/google/code/findbugs:jsr305",
        "//3rdparty/jvm/com/google/errorprone:error_prone_annotations",
        "//3rdparty/jvm/com/google/j2objc:j2objc_annotations",
        "//3rdparty/jvm/junit",
        "//3rdparty/jvm/org/checkerframework:checker_qual",
    ],
)

java_library(
    name = "listenablefuture",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/com/google/guava/listenablefuture",
    ],
)
