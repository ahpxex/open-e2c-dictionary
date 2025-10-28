from __future__ import annotations

import time
from typing import Any, Sequence

from psycopg import sql
from psycopg.cursor import Cursor

# 假设这个模块存在并且可以正确配置数据库连接
# 注意：您需要确保 open_dictionary.db.access 模块在您的环境中可用
from open_dictionary.db.access import DatabaseAccess

FETCH_BATCH_SIZE = 5000
DELETE_BATCH_SIZE = 5000
PROGRESS_EVERY_ROWS = 20_000
PROGRESS_EVERY_SECONDS = 30.0


def clean_dictionary_data(
    table_name: str,
    *,
    fetch_batch_size: int = FETCH_BATCH_SIZE,
    delete_batch_size: int = DELETE_BATCH_SIZE,
    progress_every_rows: int = PROGRESS_EVERY_ROWS,
    progress_every_seconds: float = PROGRESS_EVERY_SECONDS,
) -> None:
    """
    从字典表中删除不符合质量标准的词条行。

    该函数会删除满足以下任一条件的词条：
    1.  `common_score` 精确为零。
    2.  单词本身 (`data`->'word') 包含任何数字 (0-9)。
    3.  单词本身是长度超过1的全大写词 (例如 "UNESCO")。
    4.  单词本身包含特殊字符（允许字母, 撇号, 空格, 连字符）。
    5.  词条的标签 (`data`->'tags') 包含 "archaic", "obsolete", "dated", "古旧", 或 "废弃"。
    """

    data_access = DatabaseAccess()
    processed = 0
    deleted = 0
    pending_ids: list[int] = []
    start_time = time.monotonic()

    print(
        f"[cleaner] starting table={table_name} "
        f"fetch_batch={fetch_batch_size} delete_batch={delete_batch_size} "
        f"progress_rows={progress_every_rows} progress_seconds={progress_every_seconds}",
        flush=True,
    )

    # 构建复杂的 WHERE 子句来一次性筛选所有不合格的词条
    # 这种方法比在 Python 中进行判断效率高得多，因为它将过滤工作完全交给了数据库
    conditions = [
        # 1. 删除 common_score 为 0 的词条
        sql.SQL("common_score = 0"),
        
        # 2. 删除单词中包含数字的词条
        # data->>'word' 从 jsonb 字段 'data' 中以文本形式提取 'word' 的值
        # ~ 是 PostgreSQL 的正则表达式匹配操作符
        sql.SQL("data->>'word' ~ '[0-9]'"),

        # 3. 删除全是大写的词条（长度大于1，以避免删除 "I", "A" 等）
        # 同时检查是否真的包含大写字母，以避免非字母字符串被误判
        sql.SQL("LENGTH(data->>'word') > 1 AND data->>'word' = UPPER(data->>'word') AND data->>'word' ~ '[A-Z]'"),

        # 4. 删除包含特殊字符的词条
        # 正则表达式 [^a-zA-Z' -] 匹配任何不是字母、撇号、空格或连字符的字符
        # 注意在 SQL 字符串中，撇号需要写成 '' 来转义
        sql.SQL("data->>'word' ~ '[^a-zA-Z'' -]'"),
        
        # 5. 删除包含古旧、废弃等标签的词条
        # data->'tags' 获取 jsonb 字段 'data' 中的 'tags' 数组
        # ?| 操作符检查左边的 jsonb 数组是否包含右边 text 数组中的任何一个元素
        sql.SQL("data->'tags' ?| array['archaic', 'obsolete', 'dated']")
    ]
    
    # 使用 OR 将所有条件连接起来，满足任意一个条件即被选中
    where_clause = sql.SQL(" OR ").join(conditions)

    with data_access.get_connection() as delete_conn:
        with delete_conn.cursor() as cursor:
            last_log_time = start_time

            print(f"[cleaner] Executing query with WHERE clause: {where_clause.as_string(cursor)}", flush=True)

            # 使用构建好的 where_clause 来迭代所有需要删除的行
            for row in data_access.iterate_table(
                table_name,
                batch_size=fetch_batch_size,
                columns=("id",),
                where=where_clause,
                order_by=("id",),
            ):
                row_id = row.get("id")
                if row_id is None:
                    continue

                processed += 1
                emit_progress = processed == 1

                pending_ids.append(int(row_id))

                if len(pending_ids) >= delete_batch_size:
                    batch_count = _flush_deletions(cursor, table_name, pending_ids)
                    delete_conn.commit()
                    deleted += batch_count
                    pending_ids.clear()
                    emit_progress = True

                now = time.monotonic()

                if progress_every_rows and processed % progress_every_rows == 0:
                    emit_progress = True
                if progress_every_seconds and (now - last_log_time) >= progress_every_seconds:
                    emit_progress = True

                if emit_progress:
                    _report_progress(processed, deleted, start_time)
                    last_log_time = now

            if pending_ids:
                batch_count = _flush_deletions(cursor, table_name, pending_ids)
                delete_conn.commit()
                deleted += batch_count
                pending_ids.clear()
                _report_progress(processed, deleted, start_time)

    _report_completion(processed, deleted, start_time)


def _flush_deletions(
    cursor: Cursor[Any],
    table_name: str,
    ids: Sequence[int],
) -> int:
    if not ids:
        return 0

    values_sql = sql.SQL(", ").join(sql.SQL("(%s::bigint)") for _ in ids)
    delete_sql = sql.SQL(
        """
        DELETE FROM {table} AS t
        USING (VALUES {values}) AS v(id)
        WHERE t.id = v.id
        """
    ).format(
        table=sql.Identifier(table_name),
        values=values_sql,
    )

    cursor.execute(delete_sql, ids)
    return cursor.rowcount


def _report_progress(processed: int, deleted: int, start_time: float) -> None:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    processed_rate = processed / elapsed
    deleted_rate = deleted / elapsed if deleted else 0.0
    print(
        f"[cleaner] processed={processed:,} deleted={deleted:,} "
        f"elapsed={elapsed:,.1f}s rate={processed_rate:,.0f} rows/s "
        f"delete_rate={deleted_rate:,.0f} rows/s",
        flush=True,
    )


def _report_completion(processed: int, deleted: int, start_time: float) -> None:
    elapsed = max(time.monotonic() - start_time, 1e-6)
    processed_rate = processed / elapsed if processed else 0.0
    deleted_rate = deleted / elapsed if deleted else 0.0
    print(
        f"[cleaner] completed processed={processed:,} deleted={deleted:,} "
        f"elapsed={elapsed:,.1f}s avg_rate={processed_rate:,.0f} rows/s "
        f"delete_rate={deleted_rate:,.0f} rows/s",
        flush=True,
    )
__all__ = [
    "FETCH_BATCH_SIZE",
    "DELETE_BATCH_SIZE",
    "PROGRESS_EVERY_ROWS",
    "PROGRESS_EVERY_SECONDS",
    "clean_dictionary_data",
]
