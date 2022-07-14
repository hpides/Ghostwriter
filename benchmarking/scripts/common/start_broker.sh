#!/usr/bin/env bash

STORAGE_NODE_IP=${1}
LOG_DIR=${2}

echo "Starting broker node"
echo "====================="

numactl --cpunodebind 1 --membind 1 ${HOME}/ghostwriter/benchmarking/binaries/benchmark_broker_node \
  --storage-node-ip ${STORAGE_NODE_IP} &> $LOG_DIR/gw_broker.log &
echo $! > /tmp/gw_broker.pid

sleep 1
kill -0 $(cat /tmp/gw_broker.pid 2> /dev/null)
