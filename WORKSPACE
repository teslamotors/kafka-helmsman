load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("//3rdparty:workspace.bzl", "maven_dependencies")

#-- Skylib begin --#
http_archive(
    name = "bazel_skylib",
    sha256 = "f7be3474d42aae265405a592bb7da8e171919d74c16f082a5457840f06054728",
    url = "https://github.com/bazelbuild/bazel-skylib/releases/download/1.2.1/bazel-skylib-1.2.1.tar.gz",
)

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()

#-- Skylib end --#

#-- Java start --#
RULES_JAVA_VERSION = "7.3.2"

http_archive(
    name = "rules_java",
    sha256 = "3121a00588b1581bd7c1f9b550599629e5adcc11ba9c65f482bbd5cfe47fdf30",
    urls = [
        "https://github.com/bazelbuild/rules_java/releases/download/%s/rules_java-%s.tar.gz" % (RULES_JAVA_VERSION, RULES_JAVA_VERSION),
    ],
)

load("@rules_java//java:repositories.bzl", "rules_java_dependencies", "rules_java_toolchains")

rules_java_dependencies()

rules_java_toolchains()
#-- Java end --#

#-- Maven start --#
maven_dependencies()

git_repository(
    name = "com_github_johnynek_bazel_jar_jar",
    commit = "171f268569384c57c19474b04aebe574d85fde0d",
    remote = "https://github.com/johnynek/bazel_jar_jar.git",
    shallow_since = "1594234634 -1000",
)

load("@com_github_johnynek_bazel_jar_jar//:jar_jar.bzl", "jar_jar_repositories")

jar_jar_repositories()
#-- Maven end --#

#-- Build tools start --#
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
#-- Build tools end --#

#-- Scala begin --#
# version of the rules themselves, update this as needed to match the bazel version,
# based on https://github.com/bazelbuild/rules_scala#bazel-compatible-versions
RULES_SCALA_VERSION = "972fdf2b3bda64138db34a630a9910eee96b4d8a"

http_archive(
    name = "io_bazel_rules_scala",
    sha256 = "d1eb5719d7a082c30c3041c8e87a31d41115a9147da718b395b80926f9e47708",
    strip_prefix = "rules_scala-%s" % RULES_SCALA_VERSION,
    url = "https://github.com/bazelbuild/rules_scala/archive/%s.tar.gz" % RULES_SCALA_VERSION,
)

load("@io_bazel_rules_scala//:scala_config.bzl", "scala_config")

scala_config(scala_version = "2.12.16")

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")

scala_repositories()

load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")

scala_register_toolchains()

load("@io_bazel_rules_scala//testing:scalatest.bzl", "scalatest_repositories", "scalatest_toolchain")

scalatest_repositories()

scalatest_toolchain()
#-- Scala end --#

#-- Protobuf begin --#
RULES_PROTO_VERSION = "c0b62f2f46c85c16cb3b5e9e921f0d00e3101934"

http_archive(
    name = "rules_proto",
    sha256 = "84a2120575841cc99789353844623a2a7b51571a54194a54fc41fb0a51454069",
    strip_prefix = "rules_proto-%s" % RULES_PROTO_VERSION,
    url = "https://github.com/bazelbuild/rules_proto/archive/%s.zip" % RULES_PROTO_VERSION,
)

load("@rules_proto//proto:repositories.bzl", "rules_proto_dependencies", "rules_proto_toolchains")

rules_proto_dependencies()

rules_proto_toolchains()
#-- Protobuf end --#
