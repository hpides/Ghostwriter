from abc import ABC, abstractmethod, abstractproperty
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import NewType
from benchmarking.brokers.ghostwriter import GhostwriterBroker
from benchmarking.deployment import ClusterNode
from benchmarking.ghostwriter import StorageType
from benchmarking.brokers.base import Broker
from benchmarking.ysb_benchmark_suite import DeploymentConfig
from benchmarking.ssh import ssh_command

class Client(ABC):
    def start(self) -> None:
        raise NotImplementedError
    
    def finish(self) -> None:
        raise NotImplementedError
    
    def stop(self) -> None:
        raise NotImplementedError

    def __enter__(self) -> "Client":
        self.start()
        return self
    
    def __exit__(self):
        self.stop()


Producer = NewType("Producer", Client)

Consumer = NewType("Consumer", Client)

class Benchmark(ABC):
    @abstractproperty
    def broker(self) -> Broker:
        raise NotImplementedError
    
    @abstractproperty
    def producer(self) -> Producer:
        raise NotImplementedError
    
    @abstractproperty
    def consumer(self) -> Consumer:
        raise NotImplementedError

    def run(self):
        with self.broker:
            if self.interleaved:
                self._run_interleaved()
            else:
                self._run_sequential()
    
    def _run_sequential(self):
        with self.producer:
            self.producer.finish()
        with self.consumer:
            self.consumer.finish()
    
    def _run_interleaved(self):
        with self.consumer:
            with self.producer:
                self.producer.finish()
                self.consumer.finish()


