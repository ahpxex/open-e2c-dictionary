"""Streaming download helpers for the Wiktionary dataset."""

from __future__ import annotations

import sys
import urllib.error
import urllib.request
from pathlib import Path

from .progress import ByteProgressPrinter


DEFAULT_WIKTIONARY_URL = "https://kaikki.org/dictionary/raw-wiktextract-data.jsonl.gz"


def download_wiktionary_dump(
    destination: Path,
    *,
    url: str = DEFAULT_WIKTIONARY_URL,
    overwrite: bool = False,
    chunk_size: int = 32 * 1024 * 1024,
) -> Path:
    """Download a Wiktionary dump to ``destination`` with streaming progress."""

    dest_path = Path(destination)
    if dest_path.exists() and dest_path.is_dir():
        raise IsADirectoryError(f"Destination {dest_path} is a directory")

    if dest_path.exists() and not overwrite:
        print(f"Download skipped; {dest_path} already exists.", file=sys.stderr)
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    try:
        with urllib.request.urlopen(url) as response:
            total_size = int(response.headers.get("Content-Length", "0") or 0)
            progress = ByteProgressPrinter("Downloading", total_size)

            with dest_path.open("wb") as out_handle:
                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    out_handle.write(chunk)
                    downloaded += len(chunk)
                    progress.report(downloaded)

            progress.finalize(downloaded)

    except urllib.error.URLError as exc:  # pragma: no cover - network failure guard
        raise RuntimeError(f"Failed to download Wiktionary dump: {exc}") from exc
    return dest_path


__all__ = ["DEFAULT_WIKTIONARY_URL", "download_wiktionary_dump"]
