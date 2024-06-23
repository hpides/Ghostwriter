import dataclasses
import json
import time
from dataclasses import dataclass
from pathlib import Path

from benchmarking.brokers.kafka import KafkaBroker, KafkaConfig
from benchmarking.deployment import (BASE_DIR, ClusterNode, GHOSTWRITER_DIR, create_log_path,
                                     create_remote_dir)
from benchmarking.ssh import download, ssh_command

from .base import YSBBenchmark, YSBProducer, YSBConsumer, GiB

@dataclass
class KafkaYSBConfig(KafkaConfig):
    producer_node: ClusterNode
    consumer_node: ClusterNode
    data_size: int
    batch_size: int
    rate_limit: int
    data_path: Path
    script_path: Path
    output_path: Path
    log_path: Path
    interleaved: bool
    warmup_fraction: float = 0.2

    def metadata(self):
        return {
            "broker_node": dataclasses.asdict(self.broker_node),
            "producer_node": dataclasses.asdict(self.producer_node),
            "consumer_node": dataclasses.asdict(self.consumer_node),
            "data_size": self.data_size,
            "batch_size": self.batch_size,
            "rate_limit": self.rate_limit,
            "interleaved": self.interleaved,
            "warmup_fraction": self.warmup_fraction,
            "system": "kafka",
        }


class KafkaYSBProducer(YSBProducer):
    config = KafkaYSBConfig

    def __init__(self, config: KafkaYSBConfig, log_path: Path):
        self.config = config
        self.log_path = log_path

    def start(self):
        script_path = self.config.script_path / "benchmarking/scripts/ysb/kafka/start_producer.sh"
        data_path = "/tmp/hendrik.makait/ysb10M.bin"
        command = " ".join((str(script_path),
            self.config.broker_node.ip,
            str(self.config.batch_size),
            str(self.config.data_size),
            str(self.config.warmup_fraction),
            str(self.config.rate_limit),
            str(data_path),
            str(self.log_path),
            str(self.config.producer_node.numa_node)))
        print(command)
        status, output = ssh_command(self.config.producer_node.url, command)
        assert status == 0, f"Producer node failed to start: \n{output}"
        return output, status
    
    def finish(self):
        status = 0
        while status == 0:
            time.sleep(30)
            status, output = ssh_command(self.config.producer_node.url, "kill -0 $(cat /tmp/kafka_producer.pid 2> /dev/null)")
        print("Producer finished!")
    
    def stop(self):
        status, output = ssh_command(self.config.producer_node.url, "kill -15 $(cat /tmp/kafka_producer.pid 2> /dev/null)")
        if status != 0:
            status, output = ssh_command(self.config.producer_node.url, "kill -9 $(cat /tmp/kafka_producer.pid 2> /dev/null)")
        print("Producer stopped!")


class KafkaYSBConsumer(YSBConsumer):
    config: KafkaYSBConfig

    def __init__(self, config: KafkaYSBConfig, log_path: Path):
        self.config = config
        self.log_path = log_path

    def start(self):
        script_path = self.config.script_path / "benchmarking/scripts/ysb/kafka/start_consumer.sh"
        command = " ".join((str(script_path),
        self.config.broker_node.ip,
        str(self.config.batch_size),
        str(self.config.data_size),
        str(self.config.warmup_fraction),
        str(self.log_path),
        str(self.config.consumer_node.numa_node)))
        print(command)
        time.sleep(2)
        status, output = ssh_command(self.config.consumer_node.url, command)
        assert status == 0, f"Consumer node failed to start: \n{output}"
        return output, status

    def finish(self):
        status = 0
        while status == 0:
            time.sleep(30)
            status, output = ssh_command(self.config.consumer_node.url, "kill -0 $(cat /tmp/kafka_consumer.pid 2> /dev/null)")
        print("Consumer finished!")
    
    def stop(self):
        status, output = ssh_command(self.config.consumer_node.url, "kill -15 $(cat /tmp/kafka_consumer.pid 2> /dev/null)")
        if status != 0:
            status, output = ssh_command(self.config.consumer_node.url, "kill -9 $(cat /tmp/kafka_consumer.pid 2> /dev/null)")
        print("Consumer stopped!")


class KafkaYSB(YSBBenchmark):
    config: KafkaYSBConfig
    _broker: KafkaBroker
    _producer: KafkaYSBProducer
    _consumer: KafkaYSBConsumer
    _log_path: Path

    def __init__(self, config: KafkaYSBConfig):
        super().__init__()
        self.config = config
        self._log_path = config.log_path / str(self.id)
        create_remote_dir(self._log_path)
        self._broker = KafkaBroker(config, config.script_path, self._log_path)
        self._producer = KafkaYSBProducer(config, self._log_path)
        self._consumer = KafkaYSBConsumer(config, self._log_path)
    
    def log_metadata(self) -> None:
        metadata = self.config.metadata()
        metadata_path = self._log_path / "metadata.json"
        command = f"echo {json.dumps(json.dumps(metadata))} > {metadata_path}"
        ssh_command("summon.delab.i.hpi.de", command)
    
    @property
    def broker(self) -> KafkaBroker:
        return self._broker

    @property
    def producer(self) -> KafkaYSBProducer:
        return self._producer
    
    @property
    def consumer(self) -> KafkaYSBConsumer:
        return self._consumer

    @property
    def interleaved(self) -> bool:
        return self.config.interleaved

def create_kafka_benchmark(rate_limit: int, data_size: int, batch_size: int, base_log_path: Path, interleaved: bool):
    config = KafkaYSBConfig(
        broker_node=ClusterNode.from_name("nx04"),
        producer_node=ClusterNode.from_name("cx21"),
        consumer_node=ClusterNode.from_name("cx19"),
        data_size=data_size,
        batch_size=batch_size,
        rate_limit=rate_limit,
        data_path=Path(BASE_DIR),
        script_path=Path(GHOSTWRITER_DIR),
        output_path=Path(BASE_DIR),
        log_path=base_log_path,
        interleaved=interleaved,
    )
    return KafkaYSB(config)
