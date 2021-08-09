#!/usr/bin/env bash

echo "Starting storage node"
echo "====================="

numactl --cpunodebind 1 --membind 1 ${HOME}/ghostwriter/benchmarking/binaries/benchmark_storage_node &> /tmp/gw_storage.log &
echo $! > /tmp/gw_storage.pid

sleep 1
kill -0 $(cat /tmp/gw_storage.pid 2> /dev/null)
