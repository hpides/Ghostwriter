#!/usr/bin/env bash

STORAGE_NODE_IP=${1}
BROKER_NODE_IP=${2}
BATCH_SIZE=${3}
DATA_SIZE=${4}
WARMUP_FRACTION=${5}
RATE_LIMIT=${6}
LOG_DIR=${7}
MODE=${8}
NUMA_NODE=${9}

echo "Starting producer"
echo "====================="

numactl --cpunodebind ${NUMA_NODE} --membind ${NUMA_NODE} ${GHOSTWRITER_BINARY_DIR}/benchmark_producer \
  --storage-node-ip $STORAGE_NODE_IP \
  --broker-node-ip $BROKER_NODE_IP \
  --batch-size $BATCH_SIZE \
  --data-size $DATA_SIZE \
  --warmup-fraction $WARMUP_FRACTION \
  --rate-limit $RATE_LIMIT \
  --log-dir $LOG_DIR \
  --mode $MODE &> ${LOG_DIR}/gw_producer.log &
echo $! > /tmp/gw_producer.pid

sleep 1
kill -0 $(cat /tmp/gw_producer.pid 2> /dev/null)
