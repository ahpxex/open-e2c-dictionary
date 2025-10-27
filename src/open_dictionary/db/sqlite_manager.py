import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
import json


class SQLiteManager:
    """Manager for SQLite database with JSON1 support for storing definitions."""

    def __init__(self, db_path: str = "data/dictionary.sqlite"):
        """Initialize SQLite manager.

        Args:
            db_path: Path to SQLite database file
        """
        path_str = str(db_path)
        self._use_memory_db = path_str == ":memory:"
        self._memory_connection: sqlite3.Connection | None = None

        if self._use_memory_db:
            # Keep a persistent connection open for in-memory databases so the schema
            # and rows survive multiple operations.
            self.db_path = path_str
            self._memory_connection = sqlite3.connect(path_str, check_same_thread=False)
        else:
            self.db_path = Path(path_str)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        with self._connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS definitions (
                    word TEXT PRIMARY KEY,
                    definition JSON NOT NULL
                )
            """)
            conn.commit()

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Yield a SQLite connection, keeping in-memory DBs alive."""
        if self._use_memory_db:
            assert self._memory_connection is not None
            yield self._memory_connection
        else:
            conn = sqlite3.connect(self.db_path)
            try:
                yield conn
            finally:
                conn.close()

    def insert_definition(self, word: str, definition: dict[str, Any]):
        """Insert a single definition into the database.

        Args:
            word: The word being defined
            definition: The definition data as a dictionary
        """
        with self._connection() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO definitions (word, definition) VALUES (?, ?)",
                (word, json.dumps(definition, ensure_ascii=False))
            )
            conn.commit()

    def insert_definitions_batch(self, definitions: list[tuple[str, dict[str, Any]]]):
        """Insert multiple definitions in a batch.

        Args:
            definitions: List of (word, definition_dict) tuples
        """
        with self._connection() as conn:
            conn.executemany(
                "INSERT OR REPLACE INTO definitions (word, definition) VALUES (?, ?)",
                [(word, json.dumps(defn, ensure_ascii=False)) for word, defn in definitions]
            )
            conn.commit()

    def get_definition(self, word: str) -> dict[str, Any] | None:
        """Get definition for a word.

        Args:
            word: The word to look up

        Returns:
            Definition dictionary or None if not found
        """
        with self._connection() as conn:
            cursor = conn.execute(
                "SELECT definition FROM definitions WHERE word = ?",
                (word,)
            )
            row = cursor.fetchone()
            return json.loads(row[0]) if row else None

    def count_definitions(self) -> int:
        """Count total definitions in database.

        Returns:
            Number of definitions
        """
        with self._connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM definitions")
            return cursor.fetchone()[0]

    def close(self) -> None:
        """Close any persistent SQLite connections."""
        if self._memory_connection is not None:
            self._memory_connection.close()
            self._memory_connection = None

    def __del__(self):  # pragma: no cover - best effort cleanup
        try:
            self.close()
        except Exception:
            pass


def test_sqlite_manager():
    """Test function to verify SQLite manager works correctly."""
    import tempfile
    import os

    # Create a temporary database
    with tempfile.NamedTemporaryFile(delete=False, suffix='.sqlite') as f:
        test_db = f.name

    try:
        print(f"Testing with database: {test_db}")
        manager = SQLiteManager(test_db)

        # Test single insert
        test_def = {"word": "test", "pos": "noun", "definition": "A trial or examination"}
        manager.insert_definition("test", test_def)
        print(f"After single insert: {manager.count_definitions()} definitions")

        # Test batch insert
        batch = [
            ("word1", {"word": "word1", "meaning": "first"}),
            ("word2", {"word": "word2", "meaning": "second"}),
            ("word3", {"word": "word3", "meaning": "third"}),
        ]
        manager.insert_definitions_batch(batch)
        print(f"After batch insert: {manager.count_definitions()} definitions")

        # Test retrieval
        retrieved = manager.get_definition("test")
        print(f"Retrieved definition: {retrieved}")

        # Test in-memory database support
        memory_manager = SQLiteManager(":memory:")
        memory_manager.insert_definition("memory_word", {"word": "memory_word"})
        print(f"In-memory count: {memory_manager.count_definitions()} definitions")
        print(f"In-memory retrieval: {memory_manager.get_definition('memory_word')}")
        memory_manager.close()

        print("All tests passed!")
    finally:
        # Clean up
        if os.path.exists(test_db):
            os.unlink(test_db)


if __name__ == "__main__":
    test_sqlite_manager()
