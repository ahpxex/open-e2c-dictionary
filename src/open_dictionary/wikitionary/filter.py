"""Business logic for filtering Wiktionary entries into language-specific tables."""

from __future__ import annotations

import sys
from typing import Sequence

from .transform import partition_dictionary_by_language


def filter_languages(
    conninfo: str,
    *,
    source_table: str,
    column_name: str,
    languages: Sequence[str],
    lang_field: str = "lang_code",
    table_prefix: str = "dictionary_lang",
    target_schema: str | None = None,
    drop_existing: bool = False,
) -> list[str]:
    """Create language-specific tables for the requested ``languages`` only."""

    if not languages:
        raise ValueError("At least one language code must be provided.")

    normalized: list[str] = []
    include_all = False
    for raw_code in languages:
        code = (raw_code or "").strip()
        if not code:
            continue
        if code.lower() == "all":
            include_all = True
            break
        normalized.append(code)

    language_list: Sequence[str] | None
    if include_all:
        print(
            f"[filter] Materializing all languages from {source_table}.{column_name}...",
            file=sys.stderr,
            flush=True,
        )
        language_list = None
    else:
        if not normalized:
            raise ValueError("At least one non-empty language code must be provided.")
        display_codes = ", ".join(normalized[:5])
        if len(normalized) > 5:
            display_codes += ", ..."
        print(
            (
                f"[filter] Materializing {len(normalized)} language(s) "
                f"({display_codes}) from {source_table}.{column_name}..."
            ),
            file=sys.stderr,
            flush=True,
        )
        language_list = normalized

    return partition_dictionary_by_language(
        conninfo,
        source_table=source_table,
        column_name=column_name,
        lang_field=lang_field,
        table_prefix=table_prefix,
        target_schema=target_schema,
        drop_existing=drop_existing,
        languages=language_list,
    )


__all__ = [
    "filter_languages",
]
