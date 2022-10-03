import time
from dataclasses import dataclass
from pathlib import Path
from typing import NewType
from benchmarking.benchmarks.base import Benchmark, Consumer, Producer
from benchmarking.brokers.ghostwriter import GhostwriterBroker, GhostwriterConfig
from benchmarking.deployment import ClusterNode
from ssh import ssh_command
YSBBenchmark = NewType("YSBBenchmark", Benchmark)

YSBProducer = NewType("YSBProducer", Producer)

YSBConsumer = NewType("YSBConsumer", Consumer)


@dataclass
class GhostwriterYSBConfig(GhostwriterConfig):
    producer_node: ClusterNode
    consumer_node: ClusterNode
    data_size: int
    batch_size: int
    warmup_fraction: float
    rate_limit: int
    base_path: Path
    log_path: Path


class GhostwriterYSBProducer(YSBProducer):
    config = GhostwriterYSBConfig

    def __init__(self, config: GhostwriterYSBConfig):
        self.config = config

    def start(self):
        script_path = self.config.base_path / "benchmarking/scripts/ysb/start_producer.sh"
        data_path = self.config.base_path / "benchmarking/data/ysb250M.bin"
        command = " ".join((str(script_path), 
            self.config.storage_node.ip,
            self.config.broker_node.ip,
            str(self.config.batch_size),
            str(self.config.data_size),
            str(self.config.warmup_fraction),
            str(self.config.rate_limit),
            str(data_path),
            str(self.config.log_path),
            "exclusive"))
        print(command)
        status, output = ssh_command(self.config.producer_node.url, command)
        assert status == 0, f"Producer node failed to start: \n{output}"
        return output, status
    
    def finish(self):
        status = 0
        while status == 0:
            time.sleep(1)
            status, output = ssh_command(self.config.producer_node.url, "kill -0 $(cat /tmp/gw_producer.pid 2> /dev/null)")
        print("Producer finished!")
    
    def stop(self):
        status, output = ssh_command(self.config.producer_node.url, "kill -15 $(cat /tmp/gw_producer.pid 2> /dev/null)")
        if status != 0:
            status, output = ssh_command(self.config.producer_node.url, "kill -9 $(cat /tmp/gw_producer.pid 2> /dev/null)")
        print("Producer stopped!")


class GhostwriterYSBConsumer(YSBConsumer):
    config: GhostwriterYSBConfig

    def __init__(config: GhostwriterYSBConfig):
        self.config = config

    def start(self):
        script_path = self.config.base_path / "benchmarking/scripts/ysb/start_consumer.sh"
        command = " ".join((str(script_path),
        self.config.storage_node.ip,
        self.config.broker_node.ip,
        str(self.config.batch_size),
        str(self.config.data_size),
        str(self.config.warmup_fraction),
        str(self.config.log_path),
        "exclusive"))
        print(command)
        status, output = ssh_command(self.config.consumer_node.url, command)
        assert status == 0, f"Consumer node failed to start: \n{output}"
        return output, status

    def finish(self):
        status = 0
        while status == 0:
            time.sleep(1)
            status, output = ssh_command(self.config.consumer_node.url, "kill -0 $(cat /tmp/gw_consumer.pid 2> /dev/null)")
        print("Consumer finished!")
    
    def stop(self):
        status, output = ssh_command(self.config.consumer_node.url, "kill -15 $(cat /tmp/gw_consumer.pid 2> /dev/null)")
        if status != 0:
            status, output = ssh_command(self.config.consumer_node.url, "kill -9 $(cat /tmp/gw_consumer.pid 2> /dev/null)")
        print("Consumer stopped!")


class GhostwriterYSBBenchmark(YSBBenchmark):
    config: GhostwriterYSBConfig
    _broker: GhostwriterBroker
    _producer: GhostwriterYSBProducer
    _consumer: GhostwriterYSBConsumer

    def __init__(self, config: GhostwriterYSBConfig):
        self.config = config
        self._broker = GhostwriterBroker(config, config.base_path, config.log_path)
        self._producer = GhostwriterYSBProducer(config)
        self._consumer = GhostwriterYSBConsumer(config)
    
    @property
    def broker(self) -> GhostwriterBroker:
        return self._broker

    @property
    def producer(self) -> GhostwriterYSBProducer:
        return self._producer
    
    @property
    def consumer(self) -> GhostwriterYSBConsumer:
        return self._consumer
