"""Extraction helpers for the Wiktionary JSONL archive."""

from __future__ import annotations

import gzip
import sys
from pathlib import Path

from .progress import ByteProgressPrinter


def extract_wiktionary_dump(
    source: Path,
    destination: Path,
    *,
    overwrite: bool = False,
    chunk_size: int = 32 * 1024 * 1024,
) -> Path:
    """Extract a Wiktionary ``.jsonl.gz`` archive to ``destination``."""

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

    with source_path.open("rb") as raw_handle:
        with gzip.GzipFile(fileobj=raw_handle) as gz_handle:
            with dest_path.open("wb") as out_handle:
                while True:
                    chunk = gz_handle.read(chunk_size)
                    if not chunk:
                        break
                    out_handle.write(chunk)
                    progress.report(raw_handle.tell())

    progress.finalize(total_size)
    return dest_path


__all__ = ["extract_wiktionary_dump"]
