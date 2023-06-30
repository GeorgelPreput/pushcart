"""Abstract base class for data destination plugins.

Classes
-------
DestinationBase
    Abstract base class for data destination plugins. This class should be extended by
    any class that aims to serve as a data sink. It provides a basic representation of
    a data sink and requires the implementation of the `write` method.

Attributes
----------
config : dict
    Configuration parameters for the data sink.
batch_ts : datetime
    The timestamp when the data writing batch started.

Methods
-------
write()
    Abstract method that needs to be implemented by subclasses. This method should
    write a batch of data into a DLT table.

"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class DestinationBase(ABC):
    """Basic representation of a data sink."""

    config: dict
    batch_ts: datetime

    @abstractmethod
    def write(self) -> None:
        """Write a batch of data into a DLT table."""
