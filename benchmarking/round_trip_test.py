import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Tuple

from deployment import BASE_DIR, ClusterNode, create_remote_dir
from ghostwriter import (StorageType, start_broker, start_storage, stop_broker,
                         stop_storage)
from ssh import ssh_command
from utils import GB


@dataclass(frozen=True)
class DeploymentConfig:
    storage: ClusterNode
    broker: ClusterNode
    client: ClusterNode

    @classmethod
    def create(cls, storage_name: str, broker_name: str, client_name: str):
        return cls(
            ClusterNode.from_name(storage_name), 
            ClusterNode.from_name(broker_name), 
            ClusterNode.from_name(client_name),
        )

def create_log_dir():
    log_dir = compose_log_dir()
    create_remote_dir(log_dir)
    return log_dir

def round_trip_test(config: DeploymentConfig):
    log_dir = create_log_dir()
    batch_size = 1024 * 132
    data_size = 3_2000_000_000
    region_size = int((((data_size * 1.10) // GB) + 1) * GB)
    storage_type = StorageType.VOLATILE
    run_experiment(batch_size, data_size, region_size, storage_type, config, log_dir)

def run_experiment(batch_size: int, data_size: int, region_size: int, storage_type: StorageType, config: DeploymentConfig, log_dir: str):
    print(f"ROUND TRIP TEST: BATCH SIZE: {batch_size} - DATA SIZE: {data_size} - REGION SIZE: {region_size} - STORAGE TYPE: {storage_type}")
    log_dir = os.path.join(log_dir, f"{batch_size:07}")
    create_remote_dir(log_dir)
    start_storage(region_size, storage_type, config.storage, log_dir)
    time.sleep(180)  # TODO: Improve assertion of successful startup
    start_broker(config.storage, config.broker, log_dir)
    time.sleep(10)  # TODO: Improve assertion of successful startup
    # start producer
    # TODO: Implement different start options depending on whether we test prod/con in isolation or combined.
    start_client(batch_size, config, data_size, log_dir)
    wait_until_client_finishes(config)
    # TODO: Assert successful completion!
    stop_broker(config.broker)
    stop_storage(config.storage)
    time.sleep(120)

def start_client(batch_size: int, config: DeploymentConfig, data_size: int, log_dir: str) -> Tuple[str, int]:
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/tests/round_trip_test/start_client.sh"),
        config.storage.ip,
        config.broker.ip,
        str(batch_size),
        str(data_size),
        log_dir,
        "exclusive"))
    print(command)
    status, output = ssh_command(config.client.url, command)
    assert status == 0, f"Client node failed to start: \n{output}"
    return output, status


def wait_until_client_finishes(config: DeploymentConfig) -> None:
    status = 0
    while status == 0:
        time.sleep(1)
        status, output = ssh_command(config.client.url, "kill -0 $(cat /tmp/gw_client.pid 2> /dev/null)")
    print("Client finished!")

def compose_log_dir() -> str:
    now = datetime.now()
    return os.path.join(BASE_DIR, "benchmarking", "round_trip_test", f"{now.year:04}", f"{now.month:02}", f"{now.day:02}", now.strftime("%H%M%S"))

if __name__ == "__main__":
    config = DeploymentConfig.create("nvram-04", "node-17", "node-18")
    round_trip_test(config)
