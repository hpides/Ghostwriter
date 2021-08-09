#!/usr/bin/env bash
set -e
set -o pipefail

echo "Wrapping scripts..."
cd ../
tar -cvzf /tmp/scripts.tar.gz common throughput

echo "Clearing outdated files..."
ssh hendrik.makait@nvram01.delab.i.hpi.de 'cd ghostwriter/benchmarking && rm -rf scripts && rm -rf executables'

echo "Shipping packages..."
scp /tmp/scripts.tar.gz hendrik.makait@nvram01.delab.i.hpi.de:/hpi/fs00/home/hendrik.makait/ghostwriter/benchmarking/

echo "Unwrapping scripts..."
ssh hendrik.makait@nvram01.delab.i.hpi.de 'cd ghostwriter/benchmarking && mkdir scripts && mkdir executables && tar -xvzf scripts.tar.gz -C scripts/'

echo "Finished shipping!"