from time import sleep
from typing import Iterator, Any
import psycopg
from psycopg.rows import dict_row

from open_dictionary.llm.define import define
from open_dictionary.utils.env_loader import get_env

class DatabaseAccess:
    """Database access layer for dictionary tables."""

    def __init__(self):
        self.connection_string = get_env('DATABASE_URL')

    def _get_connection(self):
        """Get database connection."""
        return psycopg.connect(self.connection_string) # type: ignore

    def iterate_table(self, table_name: str, batch_size: int = 20) -> Iterator[dict[str, Any]]:
        """Iterate over all rows in a table using server-side cursor for memory efficiency.

        Args:
            table_name: Name of the table to iterate
            batch_size: Number of rows to fetch per batch

        Yields:
            Dictionary containing row data with column names as keys
        """
        with self._get_connection() as conn:
            with conn.cursor(row_factory=dict_row, name='fetch_cursor') as cursor:
                cursor.execute(f"SELECT * FROM {table_name}") # type: ignore

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    for row in rows:
                        yield row

    