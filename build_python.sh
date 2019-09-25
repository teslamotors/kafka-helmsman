#!/bin/bash
set -e

# THIS IS ONLY FOR PYTHON BUILDS

if ! [ -x "$(command -v tox)" ]; then
  echo 'tox is not installed!'
  exit 1
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
for project in `find $SCRIPT_DIR -name "tox.ini" -print0 | xargs -0 -n1 dirname`; do
  if [ "$1" = "package" ] ; then
    echo "building $project"
    cd $project && rm -rf $project/dist $project/*.egg-info
    python3 setup.py --quiet sdist
  else
    echo "testing $project"
    tox -q -c $project/tox.ini
  fi
done
