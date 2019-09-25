# Maven
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("//3rdparty:workspace.bzl", "maven_dependencies")

maven_dependencies()

# Shaded jars
http_archive(
    name = "com_github_johnynek_bazel_jar_jar",
    sha256 = "89187b4b5de0fdc058fe5f96e96c4232e409ef6bac42da609ad57238ae00bd0d",
    strip_prefix = "bazel_jar_jar-master",
    url = "https://github.com/johnynek/bazel_jar_jar/archive/master.zip",
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
