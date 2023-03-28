from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class TransformationBase(ABC):
    """Basic representation of a transformation stage"""

    config: dict
    batch_ts: datetime

    @abstractmethod
    def transform(self) -> None:
        """Apply a SQL or config-based transformation on a dataset and expose it as a DLT view"""
