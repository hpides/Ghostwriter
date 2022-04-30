#!/usr/bin/env bash
set -e
set -o pipefail

echo "Wrapping scripts..."
cd $GHOSTWRITER_HOME/benchmarking/scripts
tar -czf /tmp/scripts.tar.gz common throughput ysb

echo "Wrapping binaries..."
cd $GHOSTWRITER_HOME/benchmarking/build
tar -czf /tmp/binaries.tar.gz \
  benchmark_storage_node \
  benchmark_broker_node \
  benchmark_producer \
  benchmark_consumer \
  ysb_ghostwriter_producer \
  ysb_ghostwriter_consumer \
  ysb_local_runner \
  lib/libhdr_histogram.so.5.0.0 \
  lib/liboperatorJITLib.so

echo "Clearing outdated files..."
ssh hendrik.makait@nvram01.delab.i.hpi.de 'cd ghostwriter/benchmarking && rm -rf scripts && rm -rf binaries'

echo "Shipping packages..."
scp /tmp/scripts.tar.gz hendrik.makait@nvram01.delab.i.hpi.de:/hpi/fs00/home/hendrik.makait/ghostwriter/benchmarking/
scp /tmp/binaries.tar.gz hendrik.makait@nvram01.delab.i.hpi.de:/hpi/fs00/home/hendrik.makait/ghostwriter/benchmarking/

echo "Unwrapping scripts..."
ssh hendrik.makait@nvram01.delab.i.hpi.de 'cd ghostwriter/benchmarking && mkdir scripts && tar -xzf scripts.tar.gz -C scripts/'

echo "Unwrapping binaries..."
ssh hendrik.makait@nvram01.delab.i.hpi.de 'cd ghostwriter/benchmarking && mkdir binaries && tar -xzf binaries.tar.gz -C binaries/'

echo "Finished shipping!"
