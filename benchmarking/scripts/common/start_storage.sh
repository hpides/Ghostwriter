#!/usr/bin/env bash

REGION_SIZE=${1}
TYPE=${2}
DEVICE=${3}
LOG_DIR=${4}
NUMA_NODE=${5}

echo "Starting storage node"
echo "====================="

numactl --cpunodebind ${NUMA_NODE} --membind ${NUMA_NODE} ${GHOSTWRITER_BINARY_DIR}/benchmark_storage_node \
  --region-size ${REGION_SIZE} \
  --type ${TYPE} \
  --device ${DEVICE} &> $LOG_DIR/gw_storage.log &
echo $! > /tmp/gw_storage.pid

sleep 1
kill -0 $(cat /tmp/gw_storage.pid 2> /dev/null)
