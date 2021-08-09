#!/usr/bin/env bash

echo "Starting storage node"
echo "====================="

numactl --cpunodebind 2,3 --membind 2,3 ${HOME}/ghostwriter/executables/benchmark_storage_node &> /tmp/gw_storage.log &
echo $! > /tmp/gw_storage.pid

sleep 1
kill -0 $(cat /tmp/gw_storage.pid 2> /dev/null)
