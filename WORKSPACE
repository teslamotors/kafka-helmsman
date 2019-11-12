# Maven
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("//3rdparty:workspace.bzl", "maven_dependencies")

maven_dependencies()

bazel_jar_jar_version = "16e48f319048e090a2fe7fd39a794312d191fc6f"

http_archive(
    name = "com_github_johnynek_bazel_jar_jar",
    sha256 = "ee227e7f304e9b7f26d033af677f31066f68b1c94ee8f8d04fbecfb371c3caef",
    strip_prefix = "bazel_jar_jar-%s" % bazel_jar_jar_version,
    url = "https://github.com/johnynek/bazel_jar_jar/archive/%s.zip" % bazel_jar_jar_version,
)

load("@com_github_johnynek_bazel_jar_jar//:jar_jar.bzl", "jar_jar_repositories")

jar_jar_repositories()

# Build tools
http_file(
    name = "buildifier_linux",
    executable = True,
    sha256 = "6e6aea35b2ea2b4951163f686dfbfe47b49c840c56b873b3a7afe60939772fc1",
    urls = ["https://github.com/bazelbuild/buildtools/releases/download/0.25.0/buildifier"],
)

http_file(
    name = "buildifier_mac",
    executable = True,
    sha256 = "677a4e6dd247bee0ea336e7bdc94bc0b62d8f92c9f6a2f367b9a3ae1468b27ac",
    urls = ["https://github.com/bazelbuild/buildtools/releases/download/0.25.0/buildifier.mac"],
)
