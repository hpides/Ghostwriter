import time
from dataclasses import dataclass
from pathlib import Path
from benchmarking.benchmarks.base import Benchmark, Consumer, Producer
from benchmarking.brokers.ghostwriter import GhostwriterBroker, GhostwriterConfig, StorageType
from benchmarking.deployment import BASE_DIR, ClusterNode, create_log_path
from benchmarking.ssh import ssh_command

class YSBBenchmark(Benchmark):
    pass

class YSBProducer(Producer):
    pass

class YSBConsumer(Consumer):
    pass


@dataclass
class GhostwriterYSBConfig(GhostwriterConfig):
    producer_node: ClusterNode
    consumer_node: ClusterNode
    data_size: int
    batch_size: int
    rate_limit: int
    base_path: Path
    log_path: Path
    interleaved: bool
    warmup_fraction: float = 0.0


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

    def __init__(self, config: GhostwriterYSBConfig):
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


class GhostwriterYSB(YSBBenchmark):
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

    @property
    def interleaved(self) -> bool:
        return self.config.interleaved


KB = 1000
MB = 1000 * KB
GB = 1000 * MB


KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB


def ysb_benchmark_suite() -> None:
    max_batch_size = 128 * KiB
    min_batch_size = 1024 * 8
    data_size = 1024 * 1024 * 1024 * 80
    batch_size = max_batch_size
    base_log_path = create_log_path("ysb")

    while batch_size >= min_batch_size:
        config = GhostwriterYSBConfig(
            storage_node=ClusterNode.from_name("nvram-01"),
            broker_node=ClusterNode.from_name("node-03"),
            producer_node=ClusterNode.from_name("node-04"),
            consumer_node=ClusterNode.from_name("node-05"),
            region_size=int((((data_size  * 1.10) // GiB) + 1) * GiB),
            storage_type=StorageType.PERSISTENT,
            data_size=data_size,
            batch_size=batch_size,
            rate_limit=18 * GB,
            base_path=Path(BASE_DIR),
            log_path=create_log_path(base_log_path / batch_size),
            interleaved=False,
        )
        benchmark = GhostwriterYSB(config)
        benchmark.run()
        batch_size //= 2

def main():
    ysb_benchmark_suite()


if __name__ == "__main__":
    main()