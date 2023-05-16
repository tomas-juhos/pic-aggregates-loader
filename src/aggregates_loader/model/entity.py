"""Entity file."""

from enum import Enum


class Entity(str, Enum):
    """Entities."""

    aggregate_base = "aggregate_base"

    def __repr__(self) -> str:
        return str(self.value)
