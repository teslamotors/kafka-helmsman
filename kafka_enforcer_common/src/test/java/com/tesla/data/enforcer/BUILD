java_library(
    name = "dummy",
    srcs = [
        "Dummy.java",
        "DummyEnforcer.java",
    ],
    deps = [
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)

java_test(
    name = "EnforcerTest",
    srcs = [
        "EnforcerTest.java",
    ],
    deps = [
        ":dummy",
        "//3rdparty/jvm/org/mockito:mockito_core",
        "//kafka_enforcer_common/src/main/java/com/tesla/data/enforcer",
    ],
)
