from __future__ import annotations

import time
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any
from benchmarking.deployment import ClusterNode
from benchmarking.brokers.base import Broker
from benchmarking.ssh import ssh_command

class StorageType(str, Enum):
    PERSISTENT = "persistent"
    VOLATILE = "volatile"

class Mode(str, Enum):
    CONCURRENT = "concurrent"
    EXCLUSIVE = "exclusive"

@dataclass
class GhostwriterConfig:
    storage_node: ClusterNode
    broker_node: ClusterNode
    region_size: int
    storage_type: StorageType
    mode: Mode


class GhostwriterBroker(Broker):
    config: GhostwriterConfig
    log_path: Path
    _exit_stack: ExitStack | None
    _broker_ctx: Any

    def __init__(self, config: GhostwriterConfig, script_base_path: Path, log_path: Path):
        self.config = config
        self.log_path = log_path 
        self.script_base_path = script_base_path
        self._exit_stack = None
    
    def __enter__(self) -> "GhostwriterBroker":
        if self._exit_stack is not None:
            raise RuntimeError("Cannot enter again")
        self._exit_stack = ExitStack()
        self._storage_ctx = self._storage_context_manager()
        self._exit_stack.enter_context(self._storage_context_manager())
        self._exit_stack.enter_context(self._broker_context_manager())
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._exit_stack.close()
        script_path = self.script_base_path / "benchmarking/scripts/common/stop_storage.sh"
        ssh_command(self.config.storage_node.url, str(script_path))

    @contextmanager
    def _storage_context_manager(self) -> None:
        try:
            script_path = self.script_base_path / "benchmarking/scripts/common/start_storage.sh"
            command = " ".join((str(script_path), str(self.config.region_size), self.config.storage_type.value, str(self.log_path), str(self.config.storage_node.numa_node)))
            print(command)
            status, output = ssh_command(self.config.storage_node.url,
                                    command)
            assert status == 0, f"Storage node failed to start: \n{output}"
            time.sleep(30)
        
            yield
        
        finally:
            script_path = self.script_base_path / "benchmarking/scripts/common/stop_storage.sh"
            ssh_command(self.config.storage_node.url, str(script_path))
            time.sleep(30)

    @contextmanager
    def _broker_context_manager(self) -> None:
        try:
            script_path = self.script_base_path / "benchmarking/scripts/common/start_broker.sh"
            command = " ".join((str(script_path), self.config.storage_node.ip, str(self.log_path), str(self.config.mode.value), str(self.config.broker_node.numa_node)))
            print(command)
            status, output = ssh_command(self.config.broker_node.url, command)
            assert status == 0, f"Broker node failed to start: \n{output}"
            time.sleep(10)
        
            yield

        finally:
            script_path = self.script_base_path / "benchmarking/scripts/common/stop_broker.sh"
            ssh_command(self.config.broker_node.url, str(script_path))

