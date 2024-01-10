load("@com_github_johnynek_bazel_jar_jar//:jar_jar.bzl", "jar_jar")

jar_jar(
    name = "guava_failureaccess_jar_jar",
    input_jar = "//external:jar/com/google/guava/failureaccess",
    rules = "shade_rule",
)

java_import(
    name = "guava_failureaccess_shaded",
    jars = ["guava_failureaccess_jar_jar.jar"],
)

jar_jar(
    name = "guava_jar_jar",
    input_jar = "//external:jar/com/google/guava/guava",
    rules = "shade_rule",
)

java_import(
    name = "guava_shaded",
    jars = ["guava_jar_jar.jar"],
    visibility = ["//visibility:public"],
    exports = [":guava_failureaccess_shaded"],
)

jar_jar(
    name = "guava_testlib_jar_jar",
    input_jar = "//external:jar/com/google/guava/guava_testlib",
    rules = "shade_rule",
)

java_import(
    name = "guava_testlib_shaded",
    jars = ["guava_testlib_jar_jar.jar"],
    visibility = ["//visibility:public"],
)
