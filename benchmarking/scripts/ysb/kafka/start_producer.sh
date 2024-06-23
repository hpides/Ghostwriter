#!/usr/bin/env bash

BROKER_NODE_IP=${1}
BATCH_SIZE=${2}
DATA_SIZE=${3}
WARMUP_FRACTION=${4}
RATE_LIMIT=${5}
INPUT=${6}
LOG_DIR=${7}
NUMA_NODE=${8}

echo "Starting producer"
echo "====================="

numactl --cpunodebind ${NUMA_NODE} --membind ${NUMA_NODE} ${GHOSTWRITER_BINARY_DIR}/ysb_kafka_producer \
  --broker-node-ip $BROKER_NODE_IP \
  --batch-size $BATCH_SIZE \
  --data-size $DATA_SIZE \
  --warmup-fraction $WARMUP_FRACTION \
  --rate-limit $RATE_LIMIT \
  --input $INPUT \
  --log-dir $LOG_DIR &> ${LOG_DIR}/kafka_producer.log &
echo $! > /tmp/kafka_producer.pid

sleep 1
kill -0 $(cat /tmp/kafka_producer.pid 2> /dev/null)
