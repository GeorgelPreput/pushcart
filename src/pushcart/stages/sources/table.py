from dataclasses import dataclass

from pushcart.stages.stage_base import StageBase


@dataclass
class TableSource(StageBase):
    def __call__(self) -> None:
        pass
