"""Progress helpers for long-running Wiktionary data operations."""

from __future__ import annotations

import sys
import time


class ByteProgressPrinter:
    """Emit coarse progress updates for byte-oriented streaming tasks."""

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
        """Report the number of processed bytes if thresholds are met."""

        if processed_bytes < 0:  # Defensive guard for unexpected inputs
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
        """Ensure a final progress update is displayed when finished."""

        if processed_bytes == 0:
            return

        self.report(processed_bytes, force=True)


class StreamingProgress:
    """Progress reporter for streaming row + byte oriented workloads."""

    def __init__(
        self,
        total_bytes: int,
        *,
        label: str = "Progress",
        min_bytes_step: int = 64 * 1024 * 1024,
        min_rows_step: int = 50_000,
        min_time_step: float = 5.0,
    ) -> None:
        self.total_bytes = max(total_bytes, 0)
        self.label = label
        self.min_bytes_step = max(min_bytes_step, 1)
        self.min_rows_step = max(min_rows_step, 1)
        self.min_time_step = max(min_time_step, 0.0)
        self._last_report_time = time.monotonic()
        self._last_report_bytes = 0
        self._last_report_rows = 0

    def report(self, rows: int, bytes_processed: int, *, force: bool = False) -> None:
        """Emit a progress message when thresholds are crossed."""

        if rows < 0 or bytes_processed < 0:
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
        rate = 0.0
        elapsed = now - self._last_report_time
        if elapsed > 0 and rows_increment > 0:
            rate = rows_increment / elapsed

        message = (
            f"{self.label}: {percent_text}{rows:,} rows | "
            f"{gib_processed:.2f} GiB read | {rate:,.0f} rows/s"
        )
        print(message, file=sys.stderr, flush=True)

        self._last_report_time = now
        self._last_report_bytes = bytes_processed
        self._last_report_rows = rows

    def finalize(self, rows: int, bytes_processed: int) -> None:
        """Ensure a final progress message is emitted."""

        if rows == 0 and bytes_processed == 0:
            return

        self.report(rows, bytes_processed, force=True)


__all__ = ["ByteProgressPrinter", "StreamingProgress"]
