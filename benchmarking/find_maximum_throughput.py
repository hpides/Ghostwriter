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
        return NAME_TO_IB_IP[name]


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


def find_maximum_throughput(config: DeploymentConfig):
    # run single experiment
    # eval
    # repeat
    run_experiment(1024 * 1024, 1024 * 1024 * 1024 * 80, 1024 * 1024 * 1024 * 18, config)


def run_experiment(batch_size: int, data_size: int, rate_limit: int, config: DeploymentConfig):
    print(f"MAXIMUM THROUGHPUT BENCHMARK: BATCH SIZE: {batch_size} - DATA SIZE: {data_size} - RATE LIMIT: {rate_limit}")
    try:
        send_pipe, recv_pipe = Pipe(False)
        run_process = Process(target=_run_experiment,
                              args=(batch_size, data_size, rate_limit))
        pass
    except Exception as e:

        print(f"Exception: {e}")
    # create log dir
    # start storage
    command = os.path.join(BASE_DIR, "benchmarking/scripts/common/start_storage.sh")
    status, output = ssh_command(config.storage.url,
                                 command)  # f"{BASE_DIR}/benchmarking/scripts/common/start_storage.sh", verbose=True)
    assert status == 0, f"Storage node failed to start: \n{output}"
    time.sleep(30) # TODO: Improve assertion of successful startup
    # start broker
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/common/start_broker.sh"), config.storage.ip))
    status, output = ssh_command(config.broker.url, command)
    assert status == 0, f"Broker node failed to start: \n{output}"
    time.sleep(20) # TODO: Improve assertion of successful startup
    # start producer
    # TODO: Implement different start options depending on whether we test prod/con in isolation or combined.
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/throughput/start_producer.sh"),
        config.storage.ip,
        config.broker.ip,
        str(batch_size),
        str(data_size),
        str(0.1),
        str(rate_limit),
        "/tmp/benchmark_producer",
        "exclusive"))
    status, output = ssh_command(config.producer.url, command)
    assert status == 0, f"Producer node failed to start: \n{output}"
    # wait to finish
    status = 0
    while status == 0:
        status, output = ssh_command(config.producer.url, "kill -0 $(cat /tmp/gw_producer.pid 2> /dev/null)")
    print("Producer finished!")
    # TODO: Assert successful completion!
    # start consumer
    command = " ".join((
        os.path.join(BASE_DIR, "benchmarking/scripts/throughput/start_consumer.sh"),
        config.storage.ip,
        config.broker.ip,
        str(batch_size),
        str(data_size),
        str(rate_limit),
        "/tmp/benchmark_consumer",
        "exclusive"))
    status, output = ssh_command(config.consumer.url, command)
    assert status == 0, f"Consumer node failed to start: \n{output}"
    status = 0
    while status == 0:
        status, output = ssh_command(config.producer.url, "kill -0 $(cat /tmp/gw_consumer.pid 2> /dev/null)")
    print("Consumer finished!")
    # wait to finish
    # # complete_command = ""
    # ssh_command("nvram02", "kill -0 $(cat /tmp/gw_broker.pid 2> /dev/null)")
    command = os.path.join(BASE_DIR, "benchmarking/scripts/common/stop_broker.sh")
    ssh_command(config.broker.url, command)
    command = os.path.join(BASE_DIR, "benchmarking/scripts/common/stop_storage.sh")
    ssh_command(config.storage.url, command)


def _run_experiment(batch_size: int, data_size: int, rate_limit: int):
    log_dir = compose_log_dir(batch_size, data_size, rate_limit)
    os.makedirs(log_dir)


pass


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


def compose_log_dir(experiment_name: str, batch_size: int, data_size: int, rate_limit: int) -> str:
    now = datetime.now()
    return os.path.join(BASE_DIR, "benchmark-runs", experiment_name,
                        f"{now:%Y-%m-%d-%H%M}_{batch_size}-batch-size_{data_size}-data-size_{rate_limit}-rate-limit")


if __name__ == "__main__":
    config = DeploymentConfig.create("nvram-03", "node-20", "node-21", "node-22")
    find_maximum_throughput(config)
