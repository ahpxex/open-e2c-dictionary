from __future__ import annotations

import json
from decimal import Decimal
from functools import lru_cache
from typing import Any, Optional, Sequence, Tuple
import time

from psycopg import sql
from psycopg.cursor import Cursor
from wordfreq import zipf_frequency

from open_dictionary.db.access import DatabaseAccess

DEFAULT_TABLE = "dictionary_en"
FETCH_BATCH_SIZE = 5000
UPDATE_BATCH_SIZE = 5000
PROGRESS_EVERY_ROWS = 20_000
PROGRESS_EVERY_SECONDS = 30.0


def enrich_common_score(
    table_name: str = DEFAULT_TABLE,
    *,
    fetch_batch_size: int = FETCH_BATCH_SIZE,
    update_batch_size: int = UPDATE_BATCH_SIZE,
    progress_every_rows: int = PROGRESS_EVERY_ROWS,
    progress_every_seconds: float = PROGRESS_EVERY_SECONDS,
    recompute_existing: bool = False,
) -> None:
    """Populate the common_score column on ``table_name`` using wordfreq data.

    The routine streams rows via a server-side cursor to keep memory usage flat,
    batches UPDATE statements to stay efficient on very large tables, and skips
    rows that were already processed.
    """
    data_access = DatabaseAccess()

    _ensure_common_score_column(data_access, table_name)

    where_clause = None
    if not recompute_existing:
        where_clause = sql.SQL("{} IS NULL").format(sql.Identifier("common_score"))

    processed = 0
    updated = 0
    pending_updates: list[tuple[int, Optional[float]]] = []
    start_time = time.monotonic()

    print(
        f"[common_score] starting table={table_name} "
        f"fetch_batch={fetch_batch_size} update_batch={update_batch_size} "
        f"progress_rows={progress_every_rows} progress_seconds={progress_every_seconds} "
        f"recompute_existing={recompute_existing}",
        flush=True,
    )

    with data_access.get_connection() as update_conn:
        with update_conn.cursor() as cursor:
            last_log_time = start_time
            for row in data_access.iterate_table(
                table_name,
                batch_size=fetch_batch_size,
                columns=(
                    "id",
                    "common_score",
                    ("word", sql.SQL("data->>'word'")),
                ),
                where=where_clause,
                order_by=("id",),
            ):
                processed += 1
                emit_progress = False

                if processed == 1:
                    emit_progress = True

                update_payload = _build_update_payload(row)
                if update_payload is not None:
                    pending_updates.append(update_payload)

                if len(pending_updates) >= update_batch_size:
                    batch_count = _flush_updates(cursor, table_name, pending_updates)
                    update_conn.commit()
                    updated += batch_count
                    pending_updates.clear()
                    emit_progress = True

                now = time.monotonic()

                if progress_every_rows and processed % progress_every_rows == 0:
                    emit_progress = True
                if progress_every_seconds and (now - last_log_time) >= progress_every_seconds:
                    emit_progress = True

                if emit_progress:
                    _report_progress(processed, updated, start_time)
                    last_log_time = now

            if pending_updates:
                batch_count = _flush_updates(cursor, table_name, pending_updates)
                update_conn.commit()
                updated += batch_count
                pending_updates.clear()
                _report_progress(processed, updated, start_time)

    _report_completion(processed, updated, start_time)


def _ensure_common_score_column(data_access: DatabaseAccess, table_name: str) -> None:
    with data_access.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL(
                    """
                    ALTER TABLE {table}
                    ADD COLUMN IF NOT EXISTS common_score DOUBLE PRECISION
                    """
                ).format(table=sql.Identifier(table_name))
            )


def _build_update_payload(row: dict[str, Any]) -> Tuple[int, Optional[float]] | None:
    row_id = row.get("id")
    if row_id is None:
        return None

    existing = row.get("common_score")
    normalized_existing = _to_float(existing)

    word = _extract_word(row)
    score = _score_for_word(word)

    if normalized_existing is None and score is None:
        return None

    if normalized_existing is not None and score is not None:
        if abs(normalized_existing - score) < 1e-9:
            return None

    return int(row_id), score


def _extract_word(row: dict[str, Any]) -> Optional[str]:
    direct_word = row.get("word")
    candidate = _normalize_word(direct_word)
    if candidate:
        return candidate

    data = row.get("data")
    if isinstance(data, dict):
        candidate = _normalize_word(data.get("word"))
        if candidate:
            return candidate
    elif isinstance(data, str):
        try:
            decoded = json.loads(data)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, dict):
            candidate = _normalize_word(decoded.get("word"))
            if candidate:
                return candidate

    return None


def _normalize_word(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    if not stripped:
        return None
    return stripped.lower()


def _score_for_word(word: Optional[str]) -> Optional[float]:
    if not word:
        return None
    score = _cached_zipf_frequency(word)
    if score <= 0.0:
        return 0.0
    return score


@lru_cache(maxsize=None)
def _cached_zipf_frequency(word: str) -> float:
    return float(zipf_frequency(word, "en"))


def _flush_updates(
    cursor: Cursor[Any],
    table_name: str,
    payloads: Sequence[tuple[int, Optional[float]]],
) -> int:
    if not payloads:
        return 0
    values_sql = sql.SQL(", ").join(
        sql.SQL("(%s::bigint, %s::double precision)") for _ in payloads
    )
    update_sql = sql.SQL(
        """
        UPDATE {table} AS t
        SET common_score = v.score
        FROM (VALUES {values}) AS v(id, score)
        WHERE t.id = v.id
        """
    ).format(
        table=sql.Identifier(table_name),
        values=values_sql,
    )
    params: list[Any] = []
    for row_id, score in payloads:
        params.extend((row_id, score))

    cursor.execute(update_sql, params)
    return len(payloads)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float):
        return value
    if isinstance(value, Decimal):
        return float(value)
    return None


def _report_progress(processed: int, updated: int, start_time: float) -> None:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    rate = processed / elapsed
    print(
        f"[common_score] processed={processed:,} updated={updated:,} "
        f"elapsed={elapsed:,.1f}s rate={rate:,.0f} rows/s",
        flush=True,
    )


def _report_completion(processed: int, updated: int, start_time: float) -> None:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    avg_rate = processed / elapsed if processed else 0.0
    print(
        f"[common_score] completed: processed={processed:,} updated={updated:,} "
        f"elapsed={elapsed:,.1f}s avg_rate={avg_rate:,.0f} rows/s",
        flush=True,
    )


if __name__ == "__main__":
    enrich_common_score()
