#!/usr/bin/env bash

STORAGE_NODE_IP=${1}
BROKER_NODE_IP=${2}
BATCH_SIZE=${3}
DATA_SIZE=${4}
LOG_DIR=${5}
MODE=${6}

echo "Starting client"
echo "====================="

numactl --cpunodebind 1 --membind 1 ${GHOSTWRITER_BINARY_DIR}/round_trip_test \
  --storage-node-ip $STORAGE_NODE_IP \
  --broker-node-ip $BROKER_NODE_IP \
  --batch-size $BATCH_SIZE \
  --data-size $DATA_SIZE \
  --mode $MODE &> $LOG_DIR/gw_client.log &
echo $! > /tmp/gw_client.pid

sleep 1
kill -0 $(cat /tmp/gw_client.pid 2> /dev/null)
