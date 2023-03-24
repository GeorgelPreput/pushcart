from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SourceBase(ABC):
    """Basic representation of a data source"""

    config: dict
    run_ts: datetime

    @abstractmethod
    def get_increment(self) -> None:
        """Load a batch of data into a DLT view"""
