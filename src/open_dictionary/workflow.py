from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
import logging
import sys
import time

from open_dictionary.db.access import DatabaseAccess
from open_dictionary.db.sqlite_manager import SQLiteManager
from open_dictionary.llm.define import define, Definition

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger(__name__)


class ProgressReporter:
    """Report progress of definition generation with statistics."""

    def __init__(
        self,
        *,
        min_time_step: float = 5.0,
        min_count_step: int = 10,
    ):
        self.min_time_step = max(min_time_step, 0.0)
        self.min_count_step = max(min_count_step, 1)
        self._last_report_time = time.monotonic()
        self._last_report_count = 0
        self._start_time = time.monotonic()

    def maybe_report(
        self,
        processed: int,
        failed: int,
        *,
        force: bool = False
    ) -> None:
        """Report progress if enough time/items have passed."""
        now = time.monotonic()
        count_increment = processed - self._last_report_count

        if not force:
            if (
                count_increment < self.min_count_step
                and (now - self._last_report_time) < self.min_time_step
            ):
                return

        elapsed = now - self._start_time
        total = processed + failed
        rate = processed / elapsed if elapsed > 0 else 0

        message = (
            f"Progress: {processed:,} processed | {failed:,} failed | "
            f"{total:,} total | {rate:.1f} items/sec"
        )
        logger.info(message)

        self._last_report_time = now
        self._last_report_count = processed

    def finalize(self, processed: int, failed: int) -> None:
        """Print final statistics."""
        elapsed = time.monotonic() - self._start_time
        total = processed + failed
        rate = processed / elapsed if elapsed > 0 else 0

        logger.info("=" * 60)
        logger.info(f"Processing complete!")
        logger.info(f"Total processed: {processed:,}")
        logger.info(f"Total failed: {failed:,}")
        logger.info(f"Total items: {total:,}")
        logger.info(f"Success rate: {(processed/total*100 if total > 0 else 0):.1f}%")
        logger.info(f"Total time: {elapsed:.1f} seconds")
        logger.info(f"Average rate: {rate:.1f} items/sec")
        logger.info("=" * 60)


def process_single_word(word_data: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    """Process a single word definition request.

    Args:
        word_data: Dictionary containing word data from PostgreSQL

    Returns:
        Tuple of (word, definition_dict) or None if processing failed
    """
    try:
        logger.debug(f"Processing word data keys: {list(word_data.keys())}")
        definition = define(word_data)
        result = (definition.word, definition.model_dump())
        logger.debug(f"Successfully processed word: {definition.word}")
        return result
    except Exception as e:
        logger.error(f"Failed to process word '{word_data.get('word', 'unknown')}': {e}", exc_info=True)
        return None


def run_parallel_definitions(
    table_name: str = "dictionary_en",
    batch_size: int = 50,
    max_workers: int = 50,
    sqlite_path: str = "data/dictionary.sqlite",
    limit: int | None = None,
):
    """Process dictionary entries in parallel and store in SQLite.

    This function reads from PostgreSQL, sends definition requests to LLM in parallel,
    and writes results to SQLite.

    Args:
        table_name: Name of the PostgreSQL table to read from
        batch_size: Number of rows to fetch from PostgreSQL per batch
        max_workers: Maximum number of parallel LLM requests
        sqlite_path: Path to SQLite database file
        limit: Optional limit on number of words to process
    """
    db_access = DatabaseAccess()
    sqlite_manager = SQLiteManager(sqlite_path)
    progress = ProgressReporter()

    logger.info(f"Starting parallel definition processing with {max_workers} workers")
    logger.info(f"Reading from PostgreSQL table: {table_name}")
    logger.info(f"Writing to SQLite: {sqlite_path}")
    if limit:
        logger.info(f"Processing limit: {limit:,} words")

    processed_count = 0
    failed_count = 0
    pending_batch = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Iterator to track all submitted futures
        future_to_word = {}

        # Iterate through PostgreSQL table
        row_iterator = db_access.iterate_table(
            table_name=table_name,
            batch_size=batch_size,
        )

        for row in row_iterator:
            # Check limit
            if limit and processed_count >= limit:
                break

            # Extract the data field if present (PostgreSQL stores JSON in 'data' column)
            word_data = row.get('data', row)
            word_key = word_data.get('word', 'unknown') if isinstance(word_data, dict) else 'unknown'

            # Submit word for processing
            future = executor.submit(process_single_word, word_data)
            future_to_word[future] = word_key

            # When we have max_workers futures pending, wait for some to complete
            if len(future_to_word) >= max_workers:
                # Wait for at least one to complete
                for future in as_completed(list(future_to_word.keys())):
                    # Process this completed future and break to continue submitting
                    if future not in future_to_word:
                        continue

                    word_key = future_to_word.pop(future)
                    result = future.result()

                    if result:
                        word, definition = result
                        pending_batch.append((word, definition))
                        processed_count += 1
                        logger.debug(f"Added '{word}' to pending batch (size: {len(pending_batch)})")

                        # Write batch when it reaches batch_size
                        if len(pending_batch) >= batch_size:
                            logger.debug(f"Writing batch of {len(pending_batch)} definitions to SQLite")
                            sqlite_manager.insert_definitions_batch(pending_batch)
                            logger.info(f"Wrote batch to SQLite. Total in DB: {sqlite_manager.count_definitions()}")
                            pending_batch = []

                        # Report progress
                        progress.maybe_report(processed_count, failed_count)
                    else:
                        failed_count += 1
                        logger.warning(f"Failed to process: {word_key}")
                        progress.maybe_report(processed_count, failed_count)

                    # Break after processing one to continue submitting more work
                    break

        # Wait for remaining futures
        for future in as_completed(future_to_word.keys()):
            word_key = future_to_word[future]
            result = future.result()

            if result:
                word, definition = result
                pending_batch.append((word, definition))
                processed_count += 1
                progress.maybe_report(processed_count, failed_count)
            else:
                failed_count += 1
                logger.warning(f"Failed to process: {word_key}")
                progress.maybe_report(processed_count, failed_count)

        # Write any remaining definitions
        if pending_batch:
            logger.info(f"Writing final batch of {len(pending_batch)} definitions to SQLite")
            sqlite_manager.insert_definitions_batch(pending_batch)
            logger.info(f"Final batch written. Total in DB: {sqlite_manager.count_definitions()}")

    # Final statistics
    progress.finalize(processed_count, failed_count)
    final_count = sqlite_manager.count_definitions()
    logger.info(f"Total definitions in SQLite: {final_count:,}")

    if final_count != processed_count:
        logger.warning(f"Mismatch: processed {processed_count} but only {final_count} in database!")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate dictionary definitions using LLM in parallel."
    )
    parser.add_argument(
        "--table",
        default="dictionary_en",
        help="PostgreSQL table to read dictionary entries from (default: dictionary_en).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of rows to fetch from PostgreSQL per batch (default: 50).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Maximum number of parallel LLM requests (default: 50).",
    )
    parser.add_argument(
        "--sqlite-path",
        default="data/dictionary.sqlite",
        help="Path to SQLite database file for storing definitions (default: data/dictionary.sqlite).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit on number of words to process (for testing).",
    )

    args = parser.parse_args()

    run_parallel_definitions(
        table_name=args.table,
        batch_size=args.batch_size,
        max_workers=args.workers,
        sqlite_path=args.sqlite_path,
        limit=args.limit,
    )

