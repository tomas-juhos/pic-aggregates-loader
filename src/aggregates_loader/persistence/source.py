"""Source."""

from typing import List, Tuple

import psycopg2
import psycopg2.extensions
from psycopg2.extras import execute_values


class Source:
    """Source class."""

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

    def get_records(self, date_range) -> List[Tuple]:
        """Fetch records with the provided keys.

        Args:
            date_range: date range to get records from.

        Returns:
            List of records with matching keys.
        """
        cursor = self.cursor
        query = "SELECT * " "FROM daily_base " "WHERE datadate between %s and %s; "

        cursor.execute(query, (date_range[0], date_range[1]))
        res = cursor.fetchall()

        return res if res else None
