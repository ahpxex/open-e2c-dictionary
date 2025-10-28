"""Utilities for streaming Wiktionary JSONL data into PostgreSQL."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Iterator, Sequence

import psycopg
from psycopg import sql

from .progress import StreamingProgress


UTF8_BOM = b"\xef\xbb\xbf"


class JsonlProcessingError(Exception):
    """Raised when the JSONL input contains invalid JSON content."""


def iter_json_lines(file_path: Path) -> Iterator[tuple[str, int]]:
    """Yield JSON rows and byte offsets from a JSONL file, skipping blank lines."""

    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(f"No JSONL file found at {path}")

    with path.open("rb", buffering=1024 * 1024) as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue

            if line_number == 1 and raw_line.startswith(UTF8_BOM):
                raw_line = raw_line[len(UTF8_BOM) :]

            json_bytes = raw_line.rstrip(b"\r\n")
            if not json_bytes:
                continue

            try:
                json_text = json_bytes.decode("utf-8")
            except UnicodeDecodeError as exc:  # pragma: no cover - defensive
                message = f"Invalid UTF-8 sequence on line {line_number}: {exc!s}"
                raise JsonlProcessingError(message) from exc

            try:
                json.loads(json_text)
            except json.JSONDecodeError as exc:  # pragma: no cover - defensive
                message = (
                    f"Invalid JSON on line {line_number}: {exc.msg} (column {exc.colno})"
                )
                raise JsonlProcessingError(message) from exc

            bytes_read = handle.tell()
            yield json_text, bytes_read
def _identifier_from_dotted(qualified_name: str) -> sql.Identifier:
    """Return a psycopg identifier from a dotted path like ``schema.table``."""

    parts = [segment.strip() for segment in qualified_name.split(".") if segment.strip()]
    if not parts:
        raise ValueError("Identifier name cannot be empty")
    return sql.Identifier(*parts)


def _ensure_table_structure(
    cursor: psycopg.Cursor,
    table_identifier: sql.Identifier,
    column_identifier: sql.Identifier,
) -> None:
    """Create the destination table if missing."""

    create_sql = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {} (
            id BIGSERIAL PRIMARY KEY,
            {} JSONB NOT NULL
        )
        """
    ).format(table_identifier, column_identifier)

    cursor.execute(create_sql)


def _sanitize_language_code(code: str) -> str:
    safe = re.sub(r"[^0-9A-Za-z_]+", "_", code).strip("_")
    return safe.lower()


def partition_dictionary_by_language(
    conninfo: str,
    *,
    source_table: str,
    column_name: str,
    lang_field: str = "lang_code",
    table_prefix: str = "dictionary_lang",
    target_schema: str | None = None,
    drop_existing: bool = False,
    languages: Sequence[str] | None = None,
) -> list[str]:
    """Split rows in ``source_table`` into per-language tables based on ``lang_field``."""

    created_tables: list[str] = []
    table_identifier = _identifier_from_dotted(source_table)
    column_identifier = sql.Identifier(column_name)

    with psycopg.connect(conninfo) as connection:
        with connection.cursor() as cursor:
            if languages:
                language_codes = [code for code in dict.fromkeys(languages) if code]
            else:
                select_distinct = sql.SQL(
                    """
                    SELECT DISTINCT {column}->>%s AS lang_code
                    FROM {table}
                    WHERE {column} ? %s
                      AND {column}->>%s IS NOT NULL
                      AND {column}->>%s <> ''
                    ORDER BY lang_code
                    """
                ).format(column=column_identifier, table=table_identifier)

                cursor.execute(select_distinct, (lang_field, lang_field, lang_field, lang_field))
                language_codes = [row[0] for row in cursor.fetchall() if row and row[0]]

            if not language_codes:
                print(
                    "No language codes found; skipping partition step.",
                    file=sys.stderr,
                )
                return created_tables

            total_languages = len(language_codes)
            print(
                f"Partitioning {total_languages} language set(s) from {source_table}.{column_name}...",
                file=sys.stderr,
            )

            seen_tables: set[tuple[str | None, str]] = set()
            for idx, code in enumerate(language_codes, start=1):
                prefix = f"[{idx}/{total_languages}] "
                safe_code = _sanitize_language_code(code)
                if not safe_code:
                    print(
                        prefix
                        + f"Skipping language code '{code}' because it cannot form a valid table name.",
                        file=sys.stderr,
                    )
                    continue

                table_name = f"{table_prefix}_{safe_code}"
                if target_schema:
                    table_key = (target_schema, table_name)
                    target_identifier = sql.Identifier(target_schema, table_name)
                    display_name = f"{target_schema}.{table_name}"
                else:
                    table_key = (None, table_name)
                    target_identifier = sql.Identifier(table_name)
                    display_name = table_name

                if table_key in seen_tables:
                    print(
                        prefix
                        + f"Skipping language code '{code}' because it maps to an existing table name {display_name}.",
                        file=sys.stderr,
                    )
                    continue
                seen_tables.add(table_key)

                if drop_existing:
                    drop_sql = sql.SQL("DROP TABLE IF EXISTS {}").format(target_identifier)
                    cursor.execute(drop_sql)
                    connection.commit()

                create_sql = sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {} (
                        id BIGINT PRIMARY KEY,
                        {} JSONB NOT NULL
                    )
                    """
                ).format(target_identifier, column_identifier)
                cursor.execute(create_sql)

                insert_sql = sql.SQL(
                    """
                    INSERT INTO {target} (id, {column})
                    SELECT id, {column}
                    FROM {source}
                    WHERE {column}->>%s = %s
                    ON CONFLICT (id) DO NOTHING
                    """
                ).format(
                    target=target_identifier,
                    column=column_identifier,
                    source=table_identifier,
                )

                cursor.execute(insert_sql, (lang_field, code))
                connection.commit()

                inserted = cursor.rowcount if cursor.rowcount != -1 else None
                inserted_text = f" ({inserted} rows)" if inserted is not None else ""
                print(
                    f"{prefix}Partitioned '{code}' -> {display_name}{inserted_text}",
                    file=sys.stderr,
                )
                created_tables.append(display_name)

    return created_tables


def copy_jsonl_to_postgres(
    jsonl_path: Path,
    conninfo: str,
    table_name: str,
    column_name: str,
    truncate: bool = False,
) -> int:
    """Stream JSON rows from ``jsonl_path`` into ``table_name.column_name``.

    Returns the number of rows copied.
    """

    table_identifier = _identifier_from_dotted(table_name)
    if not column_name.strip():
        raise ValueError("Column name cannot be empty")

    column_identifier = sql.Identifier(column_name)

    rows_written = 0
    total_bytes = jsonl_path.stat().st_size
    progress = StreamingProgress(total_bytes, label=f"COPY {table_name}")
    latest_bytes_processed = 0

    with psycopg.connect(conninfo) as connection:
        with connection.cursor() as cursor:
            _ensure_table_structure(cursor, table_identifier, column_identifier)

            if truncate:
                cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(table_identifier))

            copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT text)").format(
                table_identifier,
                column_identifier,
            )
            copy_command = copy_sql.as_string(connection)

            with cursor.copy(copy_command) as copy:  # type: ignore[arg-type]
                for json_text, bytes_processed in iter_json_lines(jsonl_path):
                    copy.write_row((json_text,))
                    rows_written += 1
                    latest_bytes_processed = bytes_processed
                    progress.report(rows_written, latest_bytes_processed)

    progress.finalize(rows_written, latest_bytes_processed)

    return rows_written

__all__ = [
    "JsonlProcessingError",
    "iter_json_lines",
    "partition_dictionary_by_language",
    "copy_jsonl_to_postgres",
]
