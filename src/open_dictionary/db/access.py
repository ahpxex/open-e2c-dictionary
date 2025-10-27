from typing import Iterator, Any, Sequence, Tuple, Union
import uuid
import psycopg
from psycopg.rows import dict_row
from psycopg import sql
from psycopg.sql import Composable

from open_dictionary.utils.env_loader import get_env

ColumnSpec = Union[str, Tuple[str, Composable]]

class DatabaseAccess:
    """Database access layer for dictionary tables."""

    def __init__(self):
        self.connection_string = get_env('DATABASE_URL')

    def _get_connection(self):
        """Get database connection."""
        return psycopg.connect(self.connection_string) # type: ignore

    def get_connection(self):
        """Return a new psycopg connection using the configured DSN."""
        return self._get_connection()

    def iterate_table(
        self,
        table_name: str,
        batch_size: int = 20,
        *,
        columns: Sequence[ColumnSpec] | None = None,
        where: Composable | None = None,
        order_by: Sequence[str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Iterate over all rows in a table using server-side cursor for memory efficiency.

        Args:
            table_name: Name of the table to iterate
            batch_size: Number of rows to fetch per batch
            columns: Specific columns to select (defaults to all)
            where: Optional SQL WHERE clause (Composable) to filter rows
            order_by: Optional list of columns to order the results

        Yields:
            Dictionary containing row data with column names as keys
        """
        def _compile_column_spec(column: ColumnSpec) -> Composable:
            if isinstance(column, tuple):
                alias, expression = column
                if not isinstance(expression, Composable):
                    raise TypeError("Expression must be a psycopg Composable instance")
                return sql.Composed(
                    [sql.SQL("("), expression, sql.SQL(") AS "), sql.Identifier(alias)]
                )

            return sql.Identifier(column)

        if columns:
            compiled_columns = [_compile_column_spec(col) for col in columns]
            column_clause = sql.SQL(", ").join(compiled_columns)
        else:
            column_clause = sql.SQL("*")

        query = sql.SQL("SELECT {columns} FROM {table}").format(
            columns=column_clause,
            table=sql.Identifier(table_name),
        )

        if where is not None:
            query += sql.SQL(" WHERE ") + where

        if order_by:
            order_clause = sql.SQL(", ").join(sql.Identifier(col) for col in order_by)
            query += sql.SQL(" ORDER BY ") + order_clause

        cursor_name = f"fetch_cursor_{uuid.uuid4().hex}"

        with self._get_connection() as conn:
            with conn.cursor(row_factory=dict_row, name=cursor_name) as cursor:
                cursor.execute(query) # type: ignore

                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break

                    for row in rows:
                        yield row

    
