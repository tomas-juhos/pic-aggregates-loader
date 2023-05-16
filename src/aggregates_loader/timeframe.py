"""Timeframes enum class."""
from enum import Enum


class TimeFrame(str, Enum):
    """Possible timeframes for aggregates."""

    weekly = "weekly"
    monthly = "monthly"
