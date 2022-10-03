from abc import ABC, abstractmethod

class Broker(ABC):
    @abstractmethod
    def __enter__(self) -> "Broker":
        raise NotImplementedError
    
    @abstractmethod
    def __exit__(self) -> None:
        raise NotImplementedError
