sh_binary(
    name = "buildifier",
    srcs = ["buildifier.sh"],
    data = [
        "@buildifier_linux//file",
        "@buildifier_mac//file",
    ],
)

java_binary(
    name = "checkstyle_java",
    main_class = "com.puppycrawl.tools.checkstyle.Main",
    visibility = ["//visibility:public"],
    runtime_deps = [
        "//3rdparty/jvm/com/puppycrawl/tools:checkstyle",
    ],
)
