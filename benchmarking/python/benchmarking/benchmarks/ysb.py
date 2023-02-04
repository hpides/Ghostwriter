import time
import dataclasses
import json
from dataclasses import dataclass
from pathlib import Path
import pandas as pd

from benchmarking.benchmarks.base import Benchmark, Consumer, Producer
from benchmarking.brokers.ghostwriter import (GhostwriterBroker,
                                              GhostwriterConfig, StorageType, Mode)
from benchmarking.deployment import (BASE_DIR, ClusterNode, create_log_path,
                                     create_remote_dir)
from benchmarking.ssh import ssh_command, download


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

    def metadata(self):
        return {
            "storage_node": dataclasses.asdict(self.storage_node),
            "broker_node": dataclasses.asdict(self.broker_node),
            "producer_node": dataclasses.asdict(self.producer_node),
            "consumer_node": dataclasses.asdict(self.consumer_node),
            "data_size": self.data_size,
            "batch_size": self.batch_size,
            "rate_limit": self.rate_limit,
            "interleaved": self.interleaved,
            "warmup_fraction": self.warmup_fraction,
        }


class GhostwriterYSBProducer(YSBProducer):
    config = GhostwriterYSBConfig

    def __init__(self, config: GhostwriterYSBConfig, log_path: Path):
        self.config = config
        self.log_path = log_path

    def start(self):
        script_path = self.config.base_path / "benchmarking/scripts/ysb/start_producer.sh"
        data_path = self.config.base_path / "benchmarking/data/ysb10M.bin"
        command = " ".join((str(script_path), 
            self.config.storage_node.ip,
            self.config.broker_node.ip,
            str(self.config.batch_size),
            str(self.config.data_size),
            str(self.config.warmup_fraction),
            str(self.config.rate_limit),
            str(data_path),
            str(self.log_path),
            self.config.mode.value,
            str(self.config.producer_node.numa_node)))
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

    def __init__(self, config: GhostwriterYSBConfig, log_path: Path):
        self.config = config
        self.log_path = log_path

    def start(self):
        script_path = self.config.base_path / "benchmarking/scripts/ysb/start_consumer.sh"
        command = " ".join((str(script_path),
        self.config.storage_node.ip,
        self.config.broker_node.ip,
        str(self.config.batch_size),
        str(self.config.data_size),
        str(self.config.warmup_fraction),
        str(self.log_path),
        self.config.mode.value,
        str(self.config.consumer_node.numa_node)))
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
    _log_path: Path

    def __init__(self, config: GhostwriterYSBConfig):
        super().__init__()
        self.config = config
        self._log_path = config.log_path / str(self.id)
        create_remote_dir(self._log_path)
        self._broker = GhostwriterBroker(config, config.base_path, self._log_path)
        self._producer = GhostwriterYSBProducer(config, self._log_path)
        self._consumer = GhostwriterYSBConsumer(config, self._log_path)
    
    def log_metadata(self) -> None:
        metadata = self.config.metadata()
        metadata_path = self._log_path / "metadata.json"
        command = f"echo {json.dumps(json.dumps(metadata))} > {metadata_path}"
        ssh_command("summon.delab.i.hpi.de", command)
    
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
    max_batch_size = 1 * MiB
    min_batch_size = 1 * KiB
    data_size = GiB * 80
    for interleaved in [False, True]:
        batch_size = max_batch_size
        base_log_path = create_log_path("ysb")

        while batch_size >= min_batch_size:
            config = GhostwriterYSBConfig(
                storage_node=ClusterNode.from_name("nvram-02"),
                broker_node=ClusterNode.from_name("node-06"),
                producer_node=ClusterNode.from_name("node-07"),
                consumer_node=ClusterNode.from_name("node-08"),
                region_size=int((((data_size  * 1.10) // GiB) + 1) * GiB),
                storage_type=StorageType.PERSISTENT,
                data_size=data_size,
                batch_size=batch_size,
                rate_limit=18 * GB,
                base_path=Path(BASE_DIR),
                mode=Mode.CONCURRENT,
                log_path=base_log_path,
                interleaved=interleaved,
            )
            benchmark = GhostwriterYSB(config)
            benchmark.run()
            batch_size //= 2

def sustainable_throughput_suite() -> None:
    max_batch_size = 1 * MiB
    min_batch_size = 256 * KiB
    data_size = 80 * GiB
    tps = []
    for interleaved in [True, False]:
        batch_size = max_batch_size
        base_log_path = create_log_path("ysb")
        
        while batch_size >= min_batch_size:
            tp = sustainable_throughput(batch_size, interleaved, data_size, base_log_path)
            tps.append((interleaved, batch_size, tp))
    print(tps)    

def sustainable_throughput(batch_size: int, interleaved: bool, data_size: int, base_log_path: Path) -> int:
    left = 1 * MiB
    right = 20 * GiB

    while (right - left) > (right * 0.01):
        rate_limit = (left + right) // 2
        sustainable = True
        config = GhostwriterYSBConfig(
            storage_node=ClusterNode.from_name("nvram-02"),
            broker_node=ClusterNode.from_name("node-06"),
            producer_node=ClusterNode.from_name("node-07"),
            consumer_node=ClusterNode.from_name("node-08"),
            region_size=int((((data_size  * 1.10) // GiB) + 1) * GiB),
            storage_type=StorageType.PERSISTENT,
            data_size=data_size,
            batch_size=batch_size,
            rate_limit=rate_limit,
            base_path=Path(BASE_DIR),
            mode=Mode.CONCURRENT,
            log_path=base_log_path,
            interleaved=interleaved,
        )
        benchmark = GhostwriterYSB(config)
        benchmark.run()
        download(benchmark._log_path / "benchmark_producer_throughput.csv")
        df_producer_throughput = pd.read_csv("benchmark_producer_throughput.csv", sep="\t")
        avg_throughput = df_producer_throughput["Throughput in MiB/s"].mean() * MiB
        sustainable = avg_throughput > 0.95 * rate_limit
        download(benchmark._log_path / "benchmark_consumer_throughput.csv")
        df_producer_throughput = pd.read_csv("benchmark_consumer_throughput.csv", sep="\t")
        avg_throughput = df_producer_throughput["Throughput in MiB/s"].mean() * MiB
        sustainable = avg_throughput > 0.95 * rate_limit
        if sustainable:
            left = rate_limit
        else: 
            right = rate_limit
    return left


def main():
    ysb_benchmark_suite()


if __name__ == "__main__":
    main()