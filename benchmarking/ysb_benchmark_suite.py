import os
import time
from dataclasses import dataclass
from datetime import datetime

from deployment import BASE_DIR, ClusterNode, create_remote_dir
from ghostwriter import StorageType, start_broker, start_storage, stop_broker, stop_storage
from ssh import ssh_command
from utils import GB
@dataclass(frozen=True)
class DeploymentConfig:
    storage: ClusterNode
    broker: ClusterNode
    producer: ClusterNode
    consumer: ClusterNode

    @classmethod
    def create(cls, storage_name: str, broker_name: str, producer_name: str, consumer_name: str):
        return cls(ClusterNode.from_name(storage_name), ClusterNode.from_name(broker_name),
                   ClusterNode.from_name(producer_name), ClusterNode.from_name(consumer_name))


def create_log_dir() -> str:
    log_dir = compose_log_dir()
    create_remote_dir(log_dir)
    return log_dir


def ysb_benchmark_suite(config: DeploymentConfig):
    log_dir = create_log_dir()
    max_batch_size = 1024 * 128 # 1024 * 8
    min_batch_size = 1024 * 8
    batch_size = max_batch_size
    data_size = 3_200_000_000 # 1024 * 1024 * 1024 * 80
    region_size = int((((data_size * 1.10) // GB) + 1) * GB)
    storage_type = StorageType.VOLATILE
    while batch_size >= min_batch_size:
        run_experiment(batch_size, data_size, 18 * GB, region_size, storage_type, config, log_dir)
        batch_size = batch_size // 2
        break


def run_experiment(batch_size: int, data_size: int, rate_limit: int, region_size: int, storage_type: StorageType, config: DeploymentConfig, log_dir: str):
    print(f"MAXIMUM THROUGHPUT BENCHMARK: BATCH SIZE: {batch_size} - DATA SIZE: {data_size} - RATE LIMIT: {rate_limit} - REGION SIZE: {region_size} - STORAGE TYPE: {storage_type}")
    log_dir = os.path.join(log_dir, f"{batch_size:07}")
    create_remote_dir(log_dir)
    start_storage(region_size, storage_type, config.storage, log_dir)
    time.sleep(180)  # TODO: Improve assertion of successful startup
    start_broker(config.storage, config.broker, log_dir)
    time.sleep(10)  # TODO: Improve assertion of successful startup
    # start producer
    # TODO: Implement different start options depending on whether we test prod/con in isolation or combined.
    start_producer(batch_size, config, data_size, rate_limit, log_dir)
    wait_until_producer_finishes(config)
    # TODO: Assert successful completion!
    start_consumer(batch_size, config, data_size, log_dir)
    wait_until_consumer_finishes(config)
    stop_broker(config.broker)
    stop_storage(config.storage)
    time.sleep(120)

def start_producer(batch_size, config, data_size, rate_limit, log_dir):
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/ysb/start_producer.sh"),
        config.storage.ip,
        config.broker.ip,
        str(batch_size),
        str(data_size),
        str(0),
        str(rate_limit),
        os.path.join(BASE_DIR, "benchmarking/data/ysb250M.bin"),
        log_dir,
        "exclusive"))
    print(command)
    status, output = ssh_command(config.producer.url, command)
    assert status == 0, f"Producer node failed to start: \n{output}"
    return output, status


def wait_until_producer_finishes(config):
    status = 0
    while status == 0:
        time.sleep(1)
        status, output = ssh_command(config.producer.url, "kill -0 $(cat /tmp/gw_producer.pid 2> /dev/null)")
    print("Producer finished!")


def start_consumer(batch_size, config, data_size, log_dir):
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/ysb/start_consumer.sh"),
        config.storage.ip,
        config.broker.ip,
        str(batch_size),
        str(data_size),
        str(0), # TODO: Make parameter
        log_dir,
        "exclusive"))
    print(command)
    status, output = ssh_command(config.consumer.url, command)
    assert status == 0, f"Consumer node failed to start: \n{output}"
    return output, status


def wait_until_consumer_finishes(config):
    status = 0
    while status == 0:
        time.sleep(1)
        status, output = ssh_command(config.producer.url, "kill -0 $(cat /tmp/gw_consumer.pid 2> /dev/null)")
    print("Consumer finished!")


def compose_log_dir() -> str:
    now = datetime.now()
    return os.path.join(BASE_DIR, "benchmarking", "ysb_benchmark_suite", f"{now.year:04}", f"{now.month:02}",
                        f"{now.day:02}", now.strftime("%H%M%S"))


def compose_log_path(log_dir, batch_size: int, data_size: int, rate_limit: int) -> str:
    return os.path.join(log_dir, f"{batch_size}-batch-size_{data_size}-data-size_{rate_limit}-rate-limit")


if __name__ == "__main__":
    config = DeploymentConfig.create("nvram-04", "node-17", "node-18", "node-21")
    ysb_benchmark_suite(config)
