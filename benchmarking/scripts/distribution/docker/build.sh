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
make -j 2 benchmark_storage_node benchmark_broker_node benchmark_producer benchmark_consumer ysb_ghostwriter_producer ysb_ghostwriter_consumer

echo "Finished building!"
