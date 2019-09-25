#!/bin/bash

# A shell wrapper for https://github.com/bazelbuild/buildtools/tree/master/buildifier
# This wrapper allows us to integrate buildifier into the workspace and invoke
# it as 'bazel run //:buildifier'

# Bazel managed runtime dependencies as placed inside RUN_FILES
RUN_FILES=${BASH_SOURCE[0]}.runfiles

if [[ "$OSTYPE" == "darwin"* ]]; then
  $RUN_FILES/buildifier_mac/file/downloaded "$@"
else
  $RUN_FILES/buildifier_linux/file/downloaded "$@"
fi
