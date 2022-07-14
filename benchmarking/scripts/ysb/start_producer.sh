#!/usr/bin/env bash

STORAGE_NODE_IP=${1}
BROKER_NODE_IP=${2}
BATCH_SIZE=${3}
DATA_SIZE=${4}
WARMUP_FRACTION=${5}
RATE_LIMIT=${6}
INPUT=${7}
LOG_DIR=${8}
MODE=${9}

echo "Starting producer"
echo "====================="

numactl --cpunodebind 1 --membind 1 ${HOME}/ghostwriter/benchmarking/binaries/ysb_ghostwriter_producer \
  --storage-node-ip $STORAGE_NODE_IP \
  --broker-node-ip $BROKER_NODE_IP \
  --batch-size $BATCH_SIZE \
  --data-size $DATA_SIZE \
  --warmup-fraction $WARMUP_FRACTION \
  --rate-limit $RATE_LIMIT \
  --input $INPUT \
  --log-dir $LOG_DIR \
  --mode $MODE &> ${LOG_DIR}/gw_producer.log &
echo $! > /tmp/gw_producer.pid

sleep 1
kill -0 $(cat /tmp/gw_producer.pid 2> /dev/null)
