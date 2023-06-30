"""Abstract base class for pipeline stages."""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class StageBase(ABC):
    """Basic representation of a pipeline.

    Parameters
    ----------
    config : dict
        Dictionary of parameters to configure pipeline stage.
    """

    config: dict

    @abstractmethod
    def __call__(self) -> DataFrame:
        """Run the stage payload.

        Returns
        -------
        DataFrame
            Spark DataFrame containing results of operation.
        """
