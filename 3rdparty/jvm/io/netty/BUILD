java_library(
    name = "netty_buffer",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/netty/netty_buffer",
    ],
    runtime_deps = [
        ":netty_common",
    ],
)

java_library(
    name = "netty_codec",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/netty/netty_codec",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
    ],
)

java_library(
    name = "netty_common",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/netty/netty_common",
    ],
)

java_library(
    name = "netty_handler",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/netty/netty_handler",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_codec",
        ":netty_common",
        ":netty_resolver",
        ":netty_transport",
    ],
)

java_library(
    name = "netty_resolver",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/netty/netty_resolver",
    ],
    runtime_deps = [
        ":netty_common",
    ],
)

java_library(
    name = "netty_transport",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/netty/netty_transport",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_resolver",
    ],
)

java_library(
    name = "netty_transport_classes_epoll",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/netty/netty_transport_classes_epoll",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
        ":netty_transport_native_unix_common",
    ],
)

java_library(
    name = "netty_transport_native_epoll",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/netty/netty_transport_native_epoll",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
        ":netty_transport_classes_epoll",
        ":netty_transport_native_unix_common",
    ],
)

java_library(
    name = "netty_transport_native_unix_common",
    visibility = [
        "//3rdparty/jvm:__subpackages__",
    ],
    exports = [
        "//external:jar/io/netty/netty_transport_native_unix_common",
    ],
    runtime_deps = [
        ":netty_buffer",
        ":netty_common",
        ":netty_transport",
    ],
)
