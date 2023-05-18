"""Winsorized returns queries."""

from .base import BaseQueries


class Queries(BaseQueries):
    """Winsorized returns queries class."""

    UPSERT = (
        "INSERT INTO {timeframe}_base ("
        "       datadate, "
        "       gvkey, "
        "       winsorized_5_rtn"
        ") VALUES %s "
        "ON CONFLICT (datadate, gvkey) DO "
        "UPDATE SET "
        "       winsorized_5_rtn=EXCLUDED.winsorized_5_rtn; "
    )
