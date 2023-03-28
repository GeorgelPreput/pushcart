from dataclasses import dataclass

from pushcart.stages.sources.source_base import SourceBase


@dataclass
class DeltaSource(SourceBase):
    def get_increment(self) -> None:
        pass
