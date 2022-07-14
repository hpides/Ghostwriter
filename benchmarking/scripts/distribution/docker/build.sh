#!/usr/bin/env bash
set -e
set -o pipefail

echo "Start building..."
cd /ghostwriter/benchmarking
rm -rf build
mkdir -p build
cd build

echo "Running cmake..."
cmake -DCMAKE_BUILD_TYPE=Release -G "CodeBlocks - Unix Makefiles" /ghostwriter/benchmarking/build ../..

echo "Running make"
make -j 2
