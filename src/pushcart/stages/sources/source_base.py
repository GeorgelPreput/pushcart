"""Abstract base class for data source plugins.

Classes
-------
SourceBase
    Abstract base class for data source plugins. This class should be extended by any
    class that aims to serve as a data source. It provides a basic representation of
    a data source and requires the implementation of the `get_increment` method.

Attributes
----------
config : dict
    Configuration parameters for the data source.
run_ts : datetime
    The timestamp when the data extraction run started.

Methods
-------
get_increment()
    Abstract method that needs to be implemented by subclasses. This method should load
    a batch of data into a DLT view.

"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class SourceBase(ABC):
    """Basic representation of a data source."""

    config: dict
    run_ts: datetime

    @abstractmethod
    def get_increment(self) -> None:
        """Load a batch of data into a DLT view."""
