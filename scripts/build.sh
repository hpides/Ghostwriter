#!/bin/bash

echo "Start installing..."

cd /ghostwriter
mkdir -p docker-build
cd docker-build
cmake ..

/usr/local/bin/cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_MAKE_PROGRAM=/opt/rh/devtoolset-9/root/bin/make -DCMAKE_C_COMPILER=/opt/rh/devtoolset-9/root/bin/gcc -DCMAKE_CXX_COMPILER=/opt/rh/devtoolset-9/root/bin/g++ -G "CodeBlocks - Unix Makefiles" /ghostwriter/docker-build
make -j 2

echo "All done..."
