import os
from datetime import datetime
from dataclasses import dataclass
from pathlib import Path
from benchmarking.ssh import ssh_command
import socket

BASE_DIR = "/hpi/fs00/home/hendrik.makait/ghostwriter/"
GHOSTWRITER_DIR = "/scratch/hendrik.makait/ghostwriter/"
HOSTS = {
    "nx03": 66,
    "nx04": 67,
    **{f"cx{17 + i}": 70 + i for i in range(16)}
}

NETWORKS = {
    "Cluster": "172.20.26",
    "DELAB": "172.20.32",
    "IB": "10.150.1",
}


def resolve_ip(host: str, net: str) -> str:
    return f"{NETWORKS[net]}.{HOSTS[host]}"


NAME_TO_NUMA_NODE = {
    "nx03": 1,
    "nx04": 0,
    **{f"cx{17 + i}": 0 for i in range(16)}
}

@dataclass(frozen=True)
class ClusterNode:
    name: str
    ip: str
    numa_node: int

    @property
    def url(self):
        return f"{self.name}.delab.i.hpi.de"

    @classmethod
    def from_name(cls, name: str):
        ip = cls.resolve_ip(name)
        numa_node = NAME_TO_NUMA_NODE[name]
        return cls(name, ip, numa_node)

    @classmethod
    def resolve_ip(cls, name: str) -> str:
        return resolve_ip(name, "IB")
    
def create_remote_dir(dir: str):
    ssh_command("summon.delab.i.hpi.de", f"mkdir -p {dir}")

def compose_log_path(suite: str) -> Path:
    now = datetime.now()
    return Path(BASE_DIR) / "benchmarking" / suite / now.strftime("%Y-%m-%d") / now.strftime("%H%M%S")

def create_log_path(suite: str):
    log_path = compose_log_path(suite)
    create_remote_dir(log_path)
    return log_path
