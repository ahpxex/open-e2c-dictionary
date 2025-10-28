"""CLI helpers for filtering Wiktionary entries into language-specific tables."""

from __future__ import annotations

import argparse
from typing import Sequence

import psycopg

from .transform import (
    _add_database_options,
    _get_conninfo,
    partition_dictionary_by_language,
)


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
        language_list = None
    else:
        if not normalized:
            raise ValueError("At least one non-empty language code must be provided.")
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


def _cmd_filter(args: argparse.Namespace) -> int:
    try:
        conninfo = _get_conninfo(args)
    except RuntimeError as exc:
        args._parser.error(str(exc))

    try:
        created = filter_languages(
            conninfo,  # type: ignore[arg-type]
            source_table=args.table,
            column_name=args.column,
            languages=args.languages,
            lang_field=args.lang_field,
            table_prefix=args.table_prefix,
            target_schema=args.target_schema,
            drop_existing=args.drop_existing,
        )
    except ValueError as exc:
        args._parser.error(str(exc))
    except psycopg.Error as exc:
        args._parser.error(f"Database error: {exc}")

    if created:  # type: ignore[truthy-bool]
        print("Created/updated tables:")
        for table in created:
            print(f"- {table}")
    else:
        print("No tables were created.")
    return 0


def register_filter_parser(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "filter",
        help="Filter existing dictionary entries into language-specific tables.",
    )
    parser.add_argument(
        "languages",
        nargs="+",
        help="Language codes to materialize (e.g. en zh fr).",
    )
    parser.add_argument(
        "--table",
        default="dictionary_all",
        help="Source table containing the raw entries (default: dictionary_all).",
    )
    parser.add_argument(
        "--column",
        default="data",
        help="JSONB column storing the dictionary payloads (default: data).",
    )
    parser.add_argument(
        "--lang-field",
        default="lang_code",
        help="JSON key containing the language code (default: lang_code).",
    )
    parser.add_argument(
        "--table-prefix",
        default="dictionary_lang",
        help="Base name for materialized tables; language code is appended (default: dictionary_lang).",
    )
    parser.add_argument(
        "--target-schema",
        help="Optional schema for the materialized tables (default: current search_path).",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing destination tables before inserting rows.",
    )
    _add_database_options(parser)
    parser.set_defaults(func=_cmd_filter, _parser=parser)


__all__ = [
    "filter_languages",
    "register_filter_parser",
]
