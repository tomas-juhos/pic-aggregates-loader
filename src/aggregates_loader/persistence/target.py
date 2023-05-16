"""Target."""

from typing import List, Tuple

import psycopg2
import psycopg2.extensions
from psycopg2.extras import execute_values

import aggregates_loader.date_helpers as date_helpers

PERIOD_ENDS = {
    "weekly": date_helpers.is_week_end,
    "monthly": date_helpers.is_month_end,
}


class Target:
    """Target class."""

    def __init__(self, connection_string: str) -> None:
        self._connection_string = connection_string
        self._connection = psycopg2.connect(connection_string)
        self._connection.autocommit = False
        self._tx_cursor = None

    @property
    def cursor(self) -> psycopg2.extensions.cursor:
        """Generate cursor.

        Returns:
            Cursor.
        """
        if self._tx_cursor is not None:
            cursor = self._tx_cursor
        else:
            cursor = self._connection.cursor()

        return cursor

    def commit_transaction(self) -> None:
        """Commits a transaction."""
        self._connection.commit()

    def execute(self, query: str, records: List[Tuple]) -> None:
        """Execute batch of records into database.

        Args:
            query: query to execute.
            records: records to persist.
        """
        cursor = self.cursor
        execute_values(cur=cursor, sql=query, argslist=records)

    def get_last_persisted_date(self, timeframe) -> List[Tuple]:
        """Fetch last persisted date for the given timeframe.

        Returns:
            Last persisted date.
        """
        cursor = self.cursor
        query = ("SELECT MAX(datadate) " "FROM {timeframe}_base; ").format(
            timeframe=timeframe
        )

        cursor.execute(query)
        date = cursor.fetchone()

        return date[0] if date else None

    def get_max_dps(self, month):
        """Gets the maximum datapoints from the monthly_base table."""
        cursor = self.cursor
        query = "SELECT MAX(dps) FROM monthly_base WHERE datadate BETWEEN %s AND %s;"
        cursor.execute(query, (month[0], month[1]))
        dps = cursor.fetchone()

        return dps[0] if dps else None

    def get_non_traded_gvkeys(self, dps):
        cursor = self.cursor
        query = "SELECT gvkey from monthly_base WHERE dps < %s;"
        cursor.execute(query, (dps,))
        gvkeys = cursor.fetchall()

        return gvkeys if gvkeys else None
