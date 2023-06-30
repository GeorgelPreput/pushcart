from dataclasses import dataclass

from pushcart.stages.stage_base import StageBase


@dataclass
class DeltaSource(StageBase):
    def __call__(self) -> None:
        pass
