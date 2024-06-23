from __future__ import annotations

import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any
from benchmarking.deployment import ClusterNode, GHOSTWRITER_DIR
from benchmarking.brokers.base import Broker
from benchmarking.ssh import ssh_command

@dataclass
class KafkaConfig:
    broker_node: ClusterNode

KAFKA_HOME_DIR = Path("/scratch/hendrik.makait/kafka")

class KafkaBroker(Broker):
    config: KafkaConfig
    log_path: Path
    _exit_stack: ExitStack | None
    _broker_ctx: Any

    def __init__(self, config: KafkaConfig, script_base_path: Path, log_path: Path):
        self.config = config
        self.log_path = log_path 
        self.script_base_path = script_base_path
        self._exit_stack = None
    
    def __enter__(self) -> "KafkaBroker":
        if self._exit_stack is not None:
            raise RuntimeError("Cannot enter again")
        self._exit_stack = ExitStack()
        self._exit_stack.enter_context(self._zookeeper_context_manager())
        self._exit_stack.enter_context(self._kafka_context_manager())
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._exit_stack.close()

    @contextmanager
    def _zookeeper_context_manager(self):
        self._stop_zookeeper()
        try:
            self._start_zookeeper()
            yield

        finally:
            self._stop_zookeeper()

    def _start_zookeeper(self) -> None:
        command = f"{KAFKA_HOME_DIR / 'bin/zookeeper-server-start.sh'} -daemon {KAFKA_HOME_DIR / 'config/zookeeper.properties'}"
        status, output = ssh_command(self.config.broker_node.url, command)
        assert status == 0, f"Zookeeper failed to start: \n{output}"
        time.sleep(5)
    
    def _stop_zookeeper(self) -> None:
        command = f"{KAFKA_HOME_DIR / 'bin/zookeeper-server-stop.sh'}"
        ssh_command(self.config.broker_node.url, command)
        ssh_command(self.config.broker_node.url, "rm -rf /tmp/zookeeper")

    @contextmanager
    def _kafka_context_manager(self):
        self._stop_kafka()
        try:
            self._start_kafka()
            yield

        finally:
            self._stop_kafka()

    def _start_kafka(self) -> None:
        command = f"KAFKA_HEAP_OPTS=\"-Xmx8G -Xms8G\" numactl --membind 0 --cpunodebind 0  {KAFKA_HOME_DIR / 'bin/kafka-server-start.sh'} -daemon {Path(GHOSTWRITER_DIR) / 'benchmarking/config/kafka/server.properties'}"
        status, output = ssh_command(self.config.broker_node.url, command)
        assert status == 0, f"Kafka failed to start: \n{output}"
        time.sleep(30)
    
    def _stop_kafka(self) -> None:
        command = f"{KAFKA_HOME_DIR / 'bin/kafka-server-stop.sh'}"
        ssh_command(self.config.broker_node.url, command)
        time.sleep(10)
        ssh_command(self.config.broker_node.url, "rm -rf /mnt/pmem0/hendrik.makait/kafka")
        time.sleep(30)
