# Maven
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive", "http_file")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")
load("//3rdparty:workspace.bzl", "maven_dependencies")

maven_dependencies()

git_repository(
    name = "com_github_johnynek_bazel_jar_jar",
    commit = "171f268569384c57c19474b04aebe574d85fde0d",
    remote = "git://github.com/johnynek/bazel_jar_jar.git",
    shallow_since = "1594234634 -1000",
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

#-- Scala begin --#
# version of the rules themselves, update this as needed to match the bazel version,
# based on https://github.com/bazelbuild/rules_scala#bazel-compatible-versions
rules_scala_version = "a2f5852902f5b9f0302c727eead52ca2c7b6c3e2"  # update this as needed

http_archive(
    name = "io_bazel_rules_scala",
    sha256 = "8c48283aeb70e7165af48191b0e39b7434b0368718709d1bced5c3781787d8e7",
    strip_prefix = "rules_scala-%s" % rules_scala_version,
    type = "zip",
    url = "https://github.com/bazelbuild/rules_scala/archive/%s.zip" % rules_scala_version,
)

#-- Skylib begin --#
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "bazel_skylib",
    sha256 = "97e70364e9249702246c0e9444bccdc4b847bed1eb03c5a3ece4f83dfe6abc44",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
        "https://github.com/bazelbuild/bazel-skylib/releases/download/1.0.2/bazel-skylib-1.0.2.tar.gz",
    ],
)

load("@bazel_skylib//:workspace.bzl", "bazel_skylib_workspace")

bazel_skylib_workspace()
#-- Skylib end --#

# register default scala toolchain
load("@io_bazel_rules_scala//scala:toolchains.bzl", "scala_register_toolchains")

scala_register_toolchains()

load("@io_bazel_rules_scala//scala:scala.bzl", "scala_repositories")

# Specific SHA256 version of scala from maven, eg. https://repo1.maven.org/maven2/org/scala-lang/scala-library/2.11.12/,
# then get the JAR and run `sha256sum` to generate the SHA here. These are the current defaults, but are specified here
# against future upgrades and compatibility. See https://github.com/bazelbuild/rules_scala#selecting-scala-version for
# more details.
scala_repositories((
    "2.11.12",
    {
        "scala_compiler": "3e892546b72ab547cb77de4d840bcfd05c853e73390fed7370a8f19acb0735a0",
        "scala_library": "0b3d6fd42958ee98715ba2ec5fe221f4ca1e694d7c981b0ae0cd68e97baf6dce",
        "scala_reflect": "6ba385b450a6311a15c918cf8688b9af9327c6104f0ecbd35933cfcd3095fe04",
    },
))
