from contextlib import ExitStack, contextmanager
import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from deployment import ClusterNode
from benchmarking.brokers.base import Broker
from ssh import ssh_command
class StorageType(str, Enum):
    PERSISTENT = "persistent"
    VOLATILE = "volatile"

@dataclass
class GhostwriterConfig:
    storage_node: ClusterNode
    broker_node: ClusterNode
    region_size: int
    storage_type: StorageType


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

    def __exit__(self) -> None:
        self._exit_stack.close()
        script_path = self.script_base_path / "benchmarking/scripts/common/stop_storage.sh"
        ssh_command(self.config.storage_node.url, str(script_path))

    @contextmanager
    def _storage_context_manager(self) -> None:
        try:
            script_path = self.script_base_path / "benchmarking/scripts/common/start_storage.sh"
            command = " ".join((str(script_path), str(self.ghostwriter_config.region_size), self.ghostwriter_config.storage_type.value, str(self.log_path)))
            print(command)
            status, output = ssh_command(self.config.storage_node.url,
                                    command)
            assert status == 0, f"Storage node failed to start: \n{output}"
        
            yield
        
        finally:
            script_path = self.script_base_path / "benchmarking/scripts/common/stop_storage.sh"
            ssh_command(self.config.storage_node.url, str(script_path))

    @contextmanager
    def _broker_context_manager(self) -> None:
        try:
            script_path = self.script_base_path / "benchmarking/scripts/common/start_broker.sh"
            command = " ".join((str(script_path), self.config.storage_node.ip, str(self.log_path)))
            print(command)
            status, output = ssh_command(self.config.broker_node.url, command)
            assert status == 0, f"Broker node failed to start: \n{output}"
        
            yield

        finally:
            script_path = self.script_base_path / "benchmarking/scripts/common/stop_broker.sh"
            ssh_command(self.broker_node.url, str(script_path))

