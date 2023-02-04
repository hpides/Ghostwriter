from abc import ABC, abstractproperty, abstractmethod
from benchmarking.brokers.base import Broker
from uuid import uuid4, UUID
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
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()


class Producer(Client):
    pass

class Consumer(Client):
    pass

class Benchmark(ABC):
    id: UUID

    def __init__(self):
        self.id = uuid4()

    @abstractproperty
    def broker(self) -> Broker:
        raise NotImplementedError
    
    @abstractproperty
    def producer(self) -> Producer:
        raise NotImplementedError
    
    @abstractproperty
    def consumer(self) -> Consumer:
        raise NotImplementedError
    
    @abstractproperty
    def interleaved(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def log_metadata(self) -> None:
        raise NotImplementedError

    def run(self):
        self.log_metadata()
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
