from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class DestinationBase(ABC):
    """Basic representation of a data sink"""

    config: dict
    batch_ts: datetime

    @abstractmethod
    def write(self) -> None:
        """Write a batch of data into a DLT table"""
