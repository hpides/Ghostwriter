import os
import click
import paramiko
import socket
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from multiprocessing import Pipe, Process
from typing import Tuple
import time

STORAGE_NODE = ""
BROKER_NODE = ""
PRODUCER_NODE = ""
CONSUMER_NODE = ""

BASE_DIR = "/hpi/fs00/home/hendrik.makait/ghostwriter/"

NAME_TO_IB_IP = {
    "nvram-03": "10.150.1.166",
    "nvram-04": "10.150.1.167",
    "node-17": "10.150.1.170",
    "node-18": "10.150.1.171",
    "node-19": "10.150.1.172",
    "node-20": "10.150.1.173",
    "node-21": "10.150.1.174",
    "node-22": "10.150.1.175",
    "node-23": "10.150.1.176",
}

NAME_TO_DELAB_IP = {
    "nvram-01": "172.20.32.11",
    "nvram-03": "172.20.32.66",
    "node-03": "172.20.32.32",
    "node-04": "172.20.32.33",
    "node-05": "172.20.32.34",
    "node-20": "172.20.32.73",
    "node-21": "172.20.32.74",
    "node-22": "172.20.32.75",
}


@dataclass(frozen=True)
class ClusterNode:
    name: str
    ip: str

    @property
    def url(self):
        return f"{self.name}.delab.i.hpi.de"

    @classmethod
    def from_name(cls, name: str):
        ip = cls.resolve_ip(name)
        return cls(name, ip)

    @classmethod
    def resolve_ip(cls, name: str) -> str:
        return NAME_TO_DELAB_IP[name]


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


def create_log_dir(config: DeploymentConfig) -> str:
    log_dir = compose_log_dir()
    create_remote_dir(config.storage.url, log_dir)
    return log_dir


def create_remote_dir(url: str, dir: str):
    ssh_command(url, f"mkdir -p {dir}")


def ysb_benchmark_suite(config: DeploymentConfig):
    log_dir = create_log_dir(config)
    max_batch_size = 1024 * 1024 * 8
    min_batch_size = 1024 * 256
    batch_size = max_batch_size
    while batch_size >= min_batch_size:
        run_experiment(batch_size, 1024 * 1024 * 1024 * 80, 1024 * 1024 * 1024 * 18, config, log_dir)
        break  # TODO: Remove
        batch_size = batch_size // 2


def run_experiment(batch_size: int, data_size: int, rate_limit: int, config: DeploymentConfig, log_dir: str):
    print(f"MAXIMUM THROUGHPUT BENCHMARK: BATCH SIZE: {batch_size} - DATA SIZE: {data_size} - RATE LIMIT: {rate_limit}")
    log_dir = os.path.join(log_dir, f"{batch_size:07}")
    create_remote_dir(config.storage.url, log_dir)
    start_storage(config, log_dir)
    time.sleep(20)  # TODO: Improve assertion of successful startup
    start_broker(config, log_dir)
    time.sleep(10)  # TODO: Improve assertion of successful startup
    # start producer
    # TODO: Implement different start options depending on whether we test prod/con in isolation or combined.
    start_producer(batch_size, config, data_size, rate_limit, log_dir)
    wait_until_producer_finishes(config)
    # TODO: Assert successful completion!
    start_consumer(batch_size, config, data_size, rate_limit, log_dir)
    wait_until_consumer_finishes(config)
    stop_broker(config)
    stop_storage(config)


def start_broker(config, log_dir: str):
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/common/start_broker.sh"), config.storage.ip, log_dir))
    print(command)
    status, output = ssh_command(config.broker.url, command)
    assert status == 0, f"Broker node failed to start: \n{output}"


def start_storage(config, log_dir: str):
    command = os.path.join(BASE_DIR, "benchmarking/scripts/common/start_storage.sh")
    command = " ".join((command, log_dir))
    print(command)
    status, output = ssh_command(config.storage.url,
                                 command)
    assert status == 0, f"Storage node failed to start: \n{output}"


def stop_storage(config):
    command = os.path.join(BASE_DIR, "benchmarking/scripts/common/stop_storage.sh")
    ssh_command(config.storage.url, command)


def stop_broker(config):
    command = os.path.join(BASE_DIR, "benchmarking/scripts/common/stop_broker.sh")
    ssh_command(config.broker.url, command)


def start_producer(batch_size, config, data_size, rate_limit, log_dir):
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/throughput/start_producer.sh"),
        config.storage.ip,
        config.broker.ip,
        str(batch_size),
        str(data_size),
        str(0.1),
        str(rate_limit),
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


def start_consumer(batch_size, config, data_size, rate_limit, log_dir):
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/throughput/start_consumer.sh"),
        config.storage.ip,
        config.broker.ip,
        str(batch_size),
        str(data_size),
        str(rate_limit),
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


def ssh_command(host, command, timeout=None, verbose=False, user="hendrik.makait") -> Tuple[int, str]:
    ssh = None
    try:
        ssh, stdout, stderr = _ssh_command(host, command, timeout=timeout, user=user)
        # Wait for command to finish
        output = str(stdout.read(), "utf-8")
        status = stdout.channel.recv_exit_status()
        if verbose:
            print(f"Channel return code for command {command} is {status}")
        return status, output
    except paramiko.SSHException as e:
        print(f"SSHException {e}")
        raise
    except socket.timeout:
        print("SSH Pipe timed out...")
    finally:
        if ssh is not None:
            ssh.close()


def _ssh_command(host, command, timeout, user):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    retries = 0
    max_num_retries = 3
    while retries < max_num_retries:
        try:
            ssh.connect(host, username=user)
            break
        except (paramiko.SSHException, OSError) as e:
            retries += 1
            if retries == max_num_retries:
                raise e

    _, stdout, stderr = ssh.exec_command(command, timeout=timeout)
    return ssh, stdout, stderr


def compose_log_dir() -> str:
    now = datetime.now()
    return os.path.join(BASE_DIR, "benchmarking", "ysb_benchmark_suite", f"{now.year:04}", f"{now.month:02}",
                        f"{now.day:02}", now.strftime("%H%M%S"))


def compose_log_path(log_dir, batch_size: int, data_size: int, rate_limit: int) -> str:
    return os.path.join(log_dir, f"{batch_size}-batch-size_{data_size}-data-size_{rate_limit}-rate-limit")


if __name__ == "__main__":
    config = DeploymentConfig.create("nvram-01", "node-03", "node-04", "node-05")
    ysb_benchmark_suite(config)
