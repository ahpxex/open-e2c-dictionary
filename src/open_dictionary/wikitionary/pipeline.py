"""Workflow helpers for streaming Wiktionary dumps into PostgreSQL."""

from __future__ import annotations

import sys
import urllib.parse
from pathlib import Path

from .downloader import DEFAULT_WIKTIONARY_URL, download_wiktionary_dump
from .extract import extract_wiktionary_dump
from .transform import copy_jsonl_to_postgres, partition_dictionary_by_language


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
        print(
            f"Downloading Wiktionary dump from {url} to {gz_path}...",
            file=sys.stderr,
        )
        download_wiktionary_dump(
            gz_path,
            url=url,
            overwrite=overwrite_download,
        )
    else:
        print(f"Skipping download step; reusing {gz_path}", file=sys.stderr)

    if not gz_path.exists():
        raise FileNotFoundError(f"Expected archive {gz_path} after download step")

    if not skip_extract:
        print(
            f"Extracting {gz_path} to {jsonl_path}...",
            file=sys.stderr,
        )
        extract_wiktionary_dump(
            gz_path,
            jsonl_path,
            overwrite=overwrite_extract,
        )
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


__all__ = ["run_pipeline"]
