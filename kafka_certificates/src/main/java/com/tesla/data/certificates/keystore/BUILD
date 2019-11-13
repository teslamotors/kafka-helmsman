package(default_visibility = ["//:__subpackages__"])

java_library(
    name = "keystore",
    srcs = glob(["*.java"]),
    deps = [
        "//3rdparty/jvm/com/beust:jcommander",
        "//3rdparty/jvm/org/apache/commons:commons_lang3",
        "//3rdparty/jvm/org/bouncycastle:bcpkix_jdk15on",
        "//3rdparty/jvm/org/bouncycastle:bcprov_jdk15on",
        "//3rdparty/jvm/org/slf4j:slf4j_api",
    ],
)

java_binary(
    name = "KafkaClientKeystores",
    main_class = "com.tesla.data.certificates.keystore.KafkaClientKeystores",
    runtime_deps = [
        ":keystore",
        "//3rdparty/jvm/ch/qos/logback:logback_classic",
    ],
)
