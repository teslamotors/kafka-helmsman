#!/usr/bin/env bash
# Generates the data used by the stamping feature in bazel.

# TODO: https://github.com/bazelbuild/rules_docker/issues/54#issuecomment-311125784
# TODO: Do we need to prefix these with 'stable'?

BUILD_GIT_HASH_SHORT="$(git rev-parse --short=8 HEAD)"
# Jenkins checks out the branch in detached HEAD state which would
# fail the git rev-parse based approach to get the branch name,
# however it exposes the active branch as an environment variable.
if [ -z "$GIT_BRANCH" ]; then
  BUILD_GIT_CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD | sed 's/\//_/')"
else
  BUILD_GIT_CURRENT_BRANCH="$GIT_BRANCH"
fi

echo STABLE_BUILD_STRING "${BUILD_GIT_CURRENT_BRANCH}-${BUILD_GIT_HASH_SHORT}"
