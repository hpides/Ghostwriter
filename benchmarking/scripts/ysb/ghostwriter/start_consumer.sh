#!/usr/bin/env bash

STORAGE_NODE_IP=${1}
BROKER_NODE_IP=${2}
BATCH_SIZE=${3}
DATA_SIZE=${4}
WARMUP_FRACTION=${5}
LOG_DIR=${6}
MODE=${7}
NUMA_NODE=${8}

echo "Starting consumer"
echo "====================="

numactl --cpunodebind ${NUMA_NODE} --membind ${NUMA_NODE} ${GHOSTWRITER_BINARY_DIR}/ysb_ghostwriter_consumer \
  --storage-node-ip $STORAGE_NODE_IP \
  --broker-node-ip $BROKER_NODE_IP \
  --batch-size $BATCH_SIZE \
  --data-size $DATA_SIZE \
  --warmup-fraction $WARMUP_FRACTION \
  --log-dir $LOG_DIR \
  --mode $MODE &> ${LOG_DIR}/gw_consumer.log &
echo $! > /tmp/gw_consumer.pid
