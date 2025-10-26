"""Utilities for loading JSONL data into PostgreSQL."""

from __future__ import annotations

import argparse
import contextlib
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Iterator

import psycopg
from dotenv import load_dotenv
from psycopg import sql


UTF8_BOM = b"\xef\xbb\xbf"
DEFAULT_WIKTIONARY_URL = "https://kaikki.org/dictionary/raw-wiktextract-data.jsonl.gz"


class ByteProgressPrinter:
    """Print coarse-grained progress updates for byte-oriented operations."""

    def __init__(
        self,
        label: str,
        total_bytes: int,
        *,
        min_bytes_step: int = 64 * 1024 * 1024,
        min_time_step: float = 5.0,
    ) -> None:
        self.label = label
        self.total_bytes = max(total_bytes, 0)
        self.min_bytes_step = max(min_bytes_step, 1)
        self.min_time_step = max(min_time_step, 0.0)
        self._last_report_time = time.monotonic()
        self._last_report_bytes = 0

    def report(self, processed_bytes: int, *, force: bool = False) -> None:
        if processed_bytes < 0:  # pragma: no cover - defensive
            return

        now = time.monotonic()
        bytes_increment = processed_bytes - self._last_report_bytes

        if not force and processed_bytes < self.total_bytes:
            if (
                bytes_increment < self.min_bytes_step
                and (now - self._last_report_time) < self.min_time_step
            ):
                return
        elif not force and bytes_increment <= 0:
            return

        percent_text = ""
        if self.total_bytes:
            percent = min(100.0, (processed_bytes / self.total_bytes) * 100)
            percent_text = f"{percent:5.1f}% | "

        gib_processed = processed_bytes / (1024**3)
        message = f"{self.label}: {percent_text}{gib_processed:.2f} GiB"
        print(message, file=sys.stderr, flush=True)

        self._last_report_time = now
        self._last_report_bytes = processed_bytes

    def finalize(self, processed_bytes: int) -> None:
        if processed_bytes == 0:
            return

        self.report(processed_bytes, force=True)


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
                message = (
                    f"Invalid UTF-8 sequence on line {line_number}: {exc!s}"
                )
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


class ProgressReporter:
    """Emit periodic progress updates while streaming large JSONL files."""

    def __init__(
        self,
        total_bytes: int,
        *,
        min_bytes_step: int = 64 * 1024 * 1024,
        min_rows_step: int = 50_000,
        min_time_step: float = 5.0,
    ) -> None:
        self.total_bytes = max(total_bytes, 0)
        self.min_bytes_step = max(min_bytes_step, 1)
        self.min_rows_step = max(min_rows_step, 1)
        self.min_time_step = max(min_time_step, 0.0)
        self._last_report_time = time.monotonic()
        self._last_report_bytes = 0
        self._last_report_rows = 0

    def maybe_report(self, rows: int, bytes_processed: int, *, force: bool = False) -> None:
        if rows < 0 or bytes_processed < 0:  # pragma: no cover - defensive
            return

        now = time.monotonic()
        bytes_increment = bytes_processed - self._last_report_bytes
        rows_increment = rows - self._last_report_rows

        if not force:
            if bytes_processed < self.total_bytes:
                if (
                    bytes_increment < self.min_bytes_step
                    and rows_increment < self.min_rows_step
                    and (now - self._last_report_time) < self.min_time_step
                ):
                    return
            else:
                if bytes_increment <= 0 and rows_increment <= 0:
                    return

        percent_text = ""
        if self.total_bytes:
            percent = min(100.0, (bytes_processed / self.total_bytes) * 100)
            percent_text = f"{percent:5.1f}% | "

        gib_processed = bytes_processed / (1024**3)
        message = (
            f"Progress: {percent_text}{rows:,} rows | {gib_processed:.2f} GiB read"
        )
        print(message, file=sys.stderr, flush=True)

        self._last_report_time = now
        self._last_report_bytes = bytes_processed
        self._last_report_rows = rows

    def finalize(self, rows: int, bytes_processed: int) -> None:
        if rows == 0 and bytes_processed == 0:
            return

        self.maybe_report(rows, bytes_processed, force=True)


def download_file(
    url: str,
    destination: Path,
    *,
    overwrite: bool = False,
    chunk_size: int = 32 * 1024 * 1024,
) -> Path:
    """Download ``url`` to ``destination`` with streaming progress feedback."""

    dest_path = Path(destination)
    if dest_path.exists() and dest_path.is_dir():
        raise IsADirectoryError(f"Destination {dest_path} is a directory")

    if dest_path.exists() and not overwrite:
        print(f"Download skipped; {dest_path} already exists.", file=sys.stderr)
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with contextlib.ExitStack() as stack:
        response = stack.enter_context(urllib.request.urlopen(url))
        total_size = int(response.headers.get("Content-Length", "0") or 0)
        progress = ByteProgressPrinter("Downloading", total_size)

        with dest_path.open("wb") as out_handle:
            downloaded = 0
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                out_handle.write(chunk)
                downloaded += len(chunk)
                progress.report(downloaded)

    progress.finalize(downloaded)
    return dest_path


def extract_gzip(
    source: Path,
    destination: Path,
    *,
    overwrite: bool = False,
    chunk_size: int = 32 * 1024 * 1024,
) -> Path:
    """Extract a .gz archive to ``destination`` with streaming progress."""

    source_path = Path(source)
    if not source_path.is_file():
        raise FileNotFoundError(f"Source archive {source_path} does not exist")

    dest_path = Path(destination)
    if dest_path.exists() and dest_path.is_dir():
        raise IsADirectoryError(f"Destination {dest_path} is a directory")

    if dest_path.exists() and not overwrite:
        print(f"Extraction skipped; {dest_path} already exists.", file=sys.stderr)
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    total_size = source_path.stat().st_size
    progress = ByteProgressPrinter("Extracting", total_size)

    with source_path.open("rb") as raw_handle, gzip.GzipFile(fileobj=raw_handle) as gz_handle:
        with dest_path.open("wb") as out_handle:
            while True:
                chunk = gz_handle.read(chunk_size)
                if not chunk:
                    break
                out_handle.write(chunk)
                progress.report(raw_handle.tell())

    progress.finalize(total_size)
    return dest_path


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
) -> list[str]:
    """Split rows in ``source_table`` into per-language tables based on ``lang_field``."""

    created_tables: list[str] = []
    table_identifier = _identifier_from_dotted(source_table)
    column_identifier = sql.Identifier(column_name)

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

    with psycopg.connect(conninfo) as connection:
        with connection.cursor() as cursor:
            cursor.execute(select_distinct, (lang_field, lang_field, lang_field, lang_field))
            language_codes = [row[0] for row in cursor.fetchall() if row and row[0]]

            if not language_codes:
                print(
                    "No language codes found; skipping partition step.",
                    file=sys.stderr,
                )
                return created_tables

            for code in language_codes:
                safe_code = _sanitize_language_code(code)
                if not safe_code:
                    print(
                        f"Skipping language code '{code}' because it cannot form a valid table name.",
                        file=sys.stderr,
                    )
                    continue

                table_name = f"{table_prefix}_{safe_code}"
                if target_schema:
                    target_identifier = sql.Identifier(target_schema, table_name)
                    display_name = f"{target_schema}.{table_name}"
                else:
                    target_identifier = sql.Identifier(table_name)
                    display_name = table_name

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
                print(f"Partitioned '{code}' -> {display_name}{inserted_text}", file=sys.stderr)
                created_tables.append(display_name)

    return created_tables


def run_pipeline(
    *,
    workdir: Path,
    conninfo: str,
    table_name: str,
    column_name: str,
    url: str = DEFAULT_WIKTIONARY_URL,
    truncate: bool = False,
    skip_download: bool = False,
    skip_extract: bool = False,
    skip_partition: bool = False,
    overwrite_download: bool = False,
    overwrite_extract: bool = False,
    lang_field: str = "lang_code",
    table_prefix: str = "dictionary_lang",
    target_schema: str | None = None,
    drop_existing_partitions: bool = False,
) -> None:
    """Execute the full download → extract → load → partition workflow."""

    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    parsed = urllib.parse.urlparse(url)
    filename = Path(parsed.path or "wiktextract.jsonl.gz").name
    gz_path = workdir / filename
    jsonl_path = gz_path.with_suffix("")

    if not skip_download:
        download_file(url, gz_path, overwrite=overwrite_download)
    else:
        print(f"Skipping download step; reusing {gz_path}", file=sys.stderr)

    if not gz_path.exists():
        raise FileNotFoundError(f"Expected archive {gz_path} after download step")

    if not skip_extract:
        extract_gzip(gz_path, jsonl_path, overwrite=overwrite_extract)
    else:
        print(f"Skipping extract step; reusing {jsonl_path}", file=sys.stderr)

    if not jsonl_path.exists():
        raise FileNotFoundError(f"Expected JSONL file {jsonl_path} after extract step")

    rows_copied = copy_jsonl_to_postgres(
        jsonl_path=jsonl_path,
        conninfo=conninfo,
        table_name=table_name,
        column_name=column_name,
        truncate=truncate,
    )
    print(
        f"Finished loading {rows_copied:,} rows into {table_name}.{column_name}",
        file=sys.stderr,
    )

    if skip_partition:
        print("Partition step skipped by configuration.", file=sys.stderr)
        return

    partition_dictionary_by_language(
        conninfo,
        source_table=table_name,
        column_name=column_name,
        lang_field=lang_field,
        table_prefix=table_prefix,
        target_schema=target_schema,
        drop_existing=drop_existing_partitions,
    )


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
    progress = ProgressReporter(total_bytes)
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

            with cursor.copy(copy_command) as copy:
                for json_text, bytes_processed in iter_json_lines(jsonl_path):
                    copy.write_row((json_text,))
                    rows_written += 1
                    latest_bytes_processed = bytes_processed
                    progress.maybe_report(rows_written, latest_bytes_processed)

    progress.finalize(rows_written, latest_bytes_processed)

    return rows_written


COMMAND_NAMES = {"download", "extract", "load", "partition", "pipeline"}


def _add_database_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to the .env file containing the database URL (default: .env).",
    )
    parser.add_argument(
        "--database-url-var",
        default="DATABASE_URL",
        help="Environment variable name holding the connection string.",
    )


def _get_conninfo(args: argparse.Namespace) -> str:
    env_file = getattr(args, "env_file", None)
    if env_file:
        load_dotenv(env_file)

    var_name = getattr(args, "database_url_var", "DATABASE_URL")
    if not var_name:
        raise RuntimeError("Database URL environment variable name cannot be empty")

    conninfo = os.getenv(var_name)
    if not conninfo:
        raise RuntimeError(
            f"Environment variable {var_name} is not set. Ensure your .env file is loaded."
        )

    return conninfo


def _cmd_download(args: argparse.Namespace) -> int:
    try:
        destination = download_file(
            args.url,
            args.output,
            overwrite=args.overwrite,
        )
    except urllib.error.URLError as exc:  # pragma: no cover - network failure
        args._parser.error(f"Download failed: {exc.reason or exc}")
    except OSError as exc:
        args._parser.error(str(exc))

    print(f"Downloaded file to {destination}")
    return 0


def _cmd_extract(args: argparse.Namespace) -> int:
    try:
        output = extract_gzip(
            args.input,
            args.output,
            overwrite=args.overwrite,
        )
    except (FileNotFoundError, IsADirectoryError) as exc:
        args._parser.error(str(exc))
    except OSError as exc:
        args._parser.error(str(exc))

    print(f"Extracted archive to {output}")
    return 0


def _cmd_load(args: argparse.Namespace) -> int:
    try:
        conninfo = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    try:
        rows_copied = copy_jsonl_to_postgres(
            jsonl_path=args.input,
            conninfo=conninfo,
            table_name=args.table,
            column_name=args.column,
            truncate=args.truncate,
        )
    except (FileNotFoundError, JsonlProcessingError) as exc:
        args._parser.error(str(exc))
    except (psycopg.Error, ValueError) as exc:
        args._parser.error(f"Database error: {exc}")

    print(f"Copied {rows_copied} rows into {args.table}.{args.column}")
    return 0


def _cmd_partition(args: argparse.Namespace) -> int:
    try:
        conninfo = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    try:
        created = partition_dictionary_by_language(
            conninfo,
            source_table=args.table,
            column_name=args.column,
            lang_field=args.lang_field,
            table_prefix=args.prefix,
            target_schema=args.target_schema,
            drop_existing=args.drop_existing,
        )
    except (psycopg.Error, ValueError) as exc:
        args._parser.error(f"Database error: {exc}")

    if created:
        print("Created/updated tables:")
        for table in created:
            print(f"- {table}")
    else:
        print("No language-specific tables were created.")
    return 0


def _cmd_pipeline(args: argparse.Namespace) -> int:
    try:
        conninfo = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    try:
        run_pipeline(
            workdir=args.workdir,
            conninfo=conninfo,
            table_name=args.table,
            column_name=args.column,
            url=args.url,
            truncate=args.truncate,
            skip_download=args.skip_download,
            skip_extract=args.skip_extract,
            skip_partition=args.skip_partition,
            overwrite_download=args.overwrite_download,
            overwrite_extract=args.overwrite_extract,
            lang_field=args.lang_field,
            table_prefix=args.prefix,
            target_schema=args.target_schema,
            drop_existing_partitions=args.drop_existing_partitions,
        )
    except (FileNotFoundError, JsonlProcessingError) as exc:
        args._parser.error(str(exc))
    except urllib.error.URLError as exc:  # pragma: no cover - network failure
        args._parser.error(f"Download failed: {exc.reason or exc}")
    except (psycopg.Error, ValueError) as exc:
        args._parser.error(f"Database error: {exc}")

    print("Pipeline completed successfully.")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Utilities for downloading, extracting, and loading Wiktionary dumps.",
    )
    subparsers = parser.add_subparsers(dest="command")

    download_parser = subparsers.add_parser(
        "download",
        help="Download the raw Wiktionary dump (.jsonl.gz).",
    )
    download_parser.add_argument(
        "--url",
        default=DEFAULT_WIKTIONARY_URL,
        help="Source URL for the Wiktionary dump (default: official raw dataset).",
    )
    download_parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw-wiktextract-data.jsonl.gz"),
        help="Where to store the downloaded archive (default: data/raw-wiktextract-data.jsonl.gz).",
    )
    download_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the existing archive if it already exists.",
    )
    download_parser.set_defaults(func=_cmd_download, _parser=download_parser)

    extract_parser = subparsers.add_parser(
        "extract",
        help="Extract the downloaded .jsonl.gz archive to a plain JSONL file.",
    )
    extract_parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/raw-wiktextract-data.jsonl.gz"),
        help="Path to the .jsonl.gz archive (default: data/raw-wiktextract-data.jsonl.gz).",
    )
    extract_parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/raw-wiktextract-data.jsonl"),
        help="Where to write the decompressed JSONL file (default: data/raw-wiktextract-data.jsonl).",
    )
    extract_parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the extracted JSONL if it already exists.",
    )
    extract_parser.set_defaults(func=_cmd_extract, _parser=extract_parser)

    load_parser = subparsers.add_parser(
        "load",
        help="Load a JSONL file into PostgreSQL using COPY.",
    )
    load_parser.add_argument("input", type=Path, help="Path to the JSONL file to load.")
    load_parser.add_argument(
        "--table",
        default="dictionary",
        help="Target table name (default: dictionary).",
    )
    load_parser.add_argument(
        "--column",
        default="data",
        help="Target JSON/JSONB column name (default: data).",
    )
    load_parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate the destination table before inserting new rows.",
    )
    _add_database_options(load_parser)
    load_parser.set_defaults(func=_cmd_load, _parser=load_parser)

    partition_parser = subparsers.add_parser(
        "partition",
        help="Split the main dictionary table into per-language tables.",
    )
    partition_parser.add_argument(
        "--table",
        default="dictionary",
        help="Source table containing the JSONB data (default: dictionary).",
    )
    partition_parser.add_argument(
        "--column",
        default="data",
        help="JSONB column to inspect for language codes (default: data).",
    )
    partition_parser.add_argument(
        "--lang-field",
        default="lang_code",
        help="JSON key inside each entry that stores the language code (default: lang_code).",
    )
    partition_parser.add_argument(
        "--prefix",
        default="dictionary_lang",
        help="Prefix for generated tables (default: dictionary_lang).",
    )
    partition_parser.add_argument(
        "--target-schema",
        help="Optional schema to place the generated tables in (default: current search_path).",
    )
    partition_parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop and recreate each language table before inserting rows.",
    )
    _add_database_options(partition_parser)
    partition_parser.set_defaults(func=_cmd_partition, _parser=partition_parser)

    pipeline_parser = subparsers.add_parser(
        "pipeline",
        help="Run the full download → extract → load → partition workflow.",
    )
    pipeline_parser.add_argument(
        "--workdir",
        type=Path,
        default=Path("data"),
        help="Working directory for downloaded/extracted files (default: data).",
    )
    pipeline_parser.add_argument(
        "--url",
        default=DEFAULT_WIKTIONARY_URL,
        help="Source URL for the Wiktionary dump (default: official raw dataset).",
    )
    pipeline_parser.add_argument(
        "--table",
        default="dictionary",
        help="Destination table for the raw entries (default: dictionary).",
    )
    pipeline_parser.add_argument(
        "--column",
        default="data",
        help="Destination JSONB column name (default: data).",
    )
    pipeline_parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate the destination table before inserting new rows.",
    )
    pipeline_parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading if the archive is already present.",
    )
    pipeline_parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip extraction if the JSONL file already exists.",
    )
    pipeline_parser.add_argument(
        "--skip-partition",
        action="store_true",
        help="Skip creating per-language tables after loading.",
    )
    pipeline_parser.add_argument(
        "--overwrite-download",
        action="store_true",
        help="Force re-download even if the archive already exists.",
    )
    pipeline_parser.add_argument(
        "--overwrite-extract",
        action="store_true",
        help="Force re-extraction even if the JSONL already exists.",
    )
    pipeline_parser.add_argument(
        "--lang-field",
        default="lang_code",
        help="JSON key inside each entry that stores the language code (default: lang_code).",
    )
    pipeline_parser.add_argument(
        "--prefix",
        default="dictionary_lang",
        help="Prefix for generated language tables (default: dictionary_lang).",
    )
    pipeline_parser.add_argument(
        "--target-schema",
        help="Optional schema to place generated tables in (default: current search_path).",
    )
    pipeline_parser.add_argument(
        "--drop-existing-partitions",
        action="store_true",
        help="Drop existing language tables before rebuilding them.",
    )
    _add_database_options(pipeline_parser)
    pipeline_parser.set_defaults(func=_cmd_pipeline, _parser=pipeline_parser)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()

    if argv is None:
        argv_list = sys.argv[1:]
    else:
        argv_list = list(argv)

    if argv_list and not argv_list[0].startswith("-") and argv_list[0] not in COMMAND_NAMES:
        argv_list = ["load", *argv_list]

    args = parser.parse_args(argv_list)

    func = getattr(args, "func", None)
    if func is None:
        parser.print_help()
        return 1

    return func(args)


if __name__ == "__main__":
    sys.exit(main())
