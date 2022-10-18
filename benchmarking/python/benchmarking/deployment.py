import os
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from benchmarking.ssh import ssh_command

BASE_DIR = "/hpi/fs00/home/hendrik.makait/ghostwriter/"

NAME_TO_IB_IP = {
    "nvram-01": "10.150.1.11",
    "nvram-02": "10.150.1.12",
    "nvram-04": "10.150.1.67",
    "node-01": "10.150.1.30",
    "node-02": "10.150.1.31",
    "node-03": "10.150.1.32",
    "node-04": "10.150.1.33",
    "node-05": "10.150.1.34",
    "node-17": "10.150.1.70",
    "node-18": "10.150.1.71",
    "node-21": "10.150.1.74",
    "node-22": "10.150.1.75",
    "node-23": "10.150.1.76",
    "node-24": "10.150.1.77",
}

NAME_TO_DELAB_IP = {
    "nvram-01": "172.20.32.11",
    "nvram-02": "172.20.32.12",
    "nvram-03": "172.20.32.66",
    "nvram-04": "172.20.32.67",
    "node-01": "172.20.32.30",
    "node-02": "172.20.32.31",
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
        return NAME_TO_IB_IP[name]


def create_remote_dir(dir: str):
    ssh_command("summon.delab.i.hpi.de", f"mkdir -p {dir}")


def compose_log_path(suite: str) -> Path:
    now = datetime.now()
    return Path(BASE_DIR) / "benchmarking" / suite / now.strftime("%Y-%m-%d") / now.strftime("-%H%M%S")

def create_log_path(suite: str):
    log_path = compose_log_path(suite)
    create_remote_dir(log_path)
    return log_path