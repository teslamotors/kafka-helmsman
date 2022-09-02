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

java_library(
    name = "jackson_module_paranamer",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/com/fasterxml/jackson/module/jackson_module_paranamer",
    ],
    runtime_deps = [
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/com/thoughtworks/paranamer",
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
        ":jackson_module_paranamer",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_annotations",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_core",
        "//3rdparty/jvm/com/fasterxml/jackson/core:jackson_databind",
        "//3rdparty/jvm/org/scala_lang:scala_library",
        "//3rdparty/jvm/org/scala_lang:scala_reflect",
    ],
)
