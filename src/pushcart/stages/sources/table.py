from dataclasses import dataclass

from pushcart.stages.sources.source_base import SourceBase


@dataclass
class TableSource(SourceBase):
    def get_increment(self) -> None:
        pass
