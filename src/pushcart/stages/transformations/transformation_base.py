"""Abstract base class for data transformation plugins.

Classes
-------
TransformationBase
    Abstract base class for data transformation plugins. This class should be extended
    by any class that aims to serve as a data transformation stage. It provides a
    basic representation of a transformation stage and requires the implementation of
    the `transform` method.

Attributes
----------
config : dict
    Configuration parameters for the transformation stage.
batch_ts : datetime
    The timestamp when the data transformation batch started.

Methods
-------
transform()
    Abstract method that needs to be implemented by subclasses. This method should
    apply a SQL or config-based transformation on a dataset and expose it as a DLT
    view.

"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime


@dataclass
class TransformationBase(ABC):
    """Basic representation of a transformation stage."""

    config: dict
    batch_ts: datetime

    @abstractmethod
    def transform(self) -> None:
        """Apply a SQL or config-based transformation on a dataset and expose it as a DLT view."""
