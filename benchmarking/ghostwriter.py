import os
from enum import Enum

from deployment import BASE_DIR, ClusterNode
from ssh import ssh_command


class StorageType(str, Enum):
    PERSISTENT = "persistent"
    VOLATILE = "volatile"


def start_broker(storage: ClusterNode, broker: ClusterNode, log_dir: str):
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/common/start_broker.sh"), storage.ip, log_dir))
    print(command)
    status, output = ssh_command(broker.url, command)
    assert status == 0, f"Broker node failed to start: \n{output}"


def start_storage(region_size: int, storage_type: StorageType, storage: ClusterNode, log_dir: str):
    command = os.path.join(BASE_DIR, "benchmarking/scripts/common/start_storage.sh")
    command = " ".join((command, str(region_size), storage_type.value, log_dir))
    print(command)
    status, output = ssh_command(storage.url,
                                 command)
    assert status == 0, f"Storage node failed to start: \n{output}"


def stop_storage(storage: ClusterNode):
    command = os.path.join(BASE_DIR, "benchmarking/scripts/common/stop_storage.sh")
    ssh_command(storage.url, command)


def stop_broker(broker: ClusterNode):
    command = os.path.join(BASE_DIR, "benchmarking/scripts/common/stop_broker.sh")
    ssh_command(broker.url, command)
