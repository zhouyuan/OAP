#!/usr/bin/env bash

set -eu

CURRENT_DIR=$(cd "$(dirname "$BASH_SOURCE")"; pwd)
echo $CURRENT_DIR

cd ${CURRENT_DIR}
if [ -d build ]; then
    rm -r build
fi
mkdir build
cd build
cmake .. -DTESTS=1
make

set +eu

