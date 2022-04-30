#!/usr/bin/env bash

REGION_SIZE=${1}
TYPE=${2}
LOG_DIR=${3}

echo "Starting storage node"
echo "====================="

numactl --cpunodebind 2 --membind 2 ${HOME}/ghostwriter/benchmarking/binaries/benchmark_storage_node --region-size ${REGION_SIZE} --type ${TYPE} &> /tmp/gw_storage.log &
echo $! > /tmp/gw_storage.pid

sleep 1
kill -0 $(cat /tmp/gw_storage.pid 2> /dev/null)
