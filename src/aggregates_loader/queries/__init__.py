"""Queries implementation."""

from .aggregate_base import Queries as AggregateBaseQueries
from .winsorized_returns import Queries as WinsorizedReturnsQueries


__all__ = ["AggregateBaseQueries", "WinsorizedReturnsQueries"]
