#!/usr/bin/env bash

BROKER_NODE_IP=${1}
BATCH_SIZE=${2}
DATA_SIZE=${3}
WARMUP_FRACTION=${4}
LOG_DIR=${5}
NUMA_NODE=${6}

echo "Starting consumer"
echo "====================="

numactl --cpunodebind ${NUMA_NODE} --membind ${NUMA_NODE} ${GHOSTWRITER_BINARY_DIR}/ysb_kafka_consumer \
  --broker-node-ip $BROKER_NODE_IP \
  --batch-size $BATCH_SIZE \
  --data-size $DATA_SIZE \
  --warmup-fraction $WARMUP_FRACTION \
  --log-dir $LOG_DIR &> ${LOG_DIR}/kafka_consumer.log &
echo $! > /tmp/kafka_consumer.pid
