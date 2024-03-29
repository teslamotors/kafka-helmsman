java_library(
    name = "jackson_module_parameter_names",
    visibility = [
        "//visibility:public",
    ],
    exports = [
        "//external:jar/com/fasterxml/jackson/module/jackson_module_parameter_names",
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
    ],
)

scala_import(
    name = "jackson_module_scala",
    jars = [
        "//external:jar/com/fasterxml/jackson/module/jackson_module_scala_2_12",
    ],
    visibility = [
        "//visibility:public",
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_annotations",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/thoughtworks/paranamer",
        "//3rdparty/jvm/org/scala_lang:scala_library",
    ],
)
