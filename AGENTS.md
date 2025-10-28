# Repository Guidelines

This is a full tool sets for building a open dictionary, based on wikitionary data.

## Project Structure & Module Organization

- Core logic lives in `src/open_dictionary`. The CLI entry point defined in `pyproject.toml` resolves to `open_dictionary:main`, which dispatches into `src/open_dictionary/cli.py`; keep any new commands registered there while delegating business logic to feature modules.
- Data access helpers sit under `src/open_dictionary/db` (for example `access.py`) and should remain focused on PostgreSQL streaming semantics.
- Wiktionary ingestion utilities are split by concern under `src/open_dictionary/wikitionary/`: `downloader.py`, `extract.py`, `transform.py` (streaming COPY + table helpers), `pipeline.py` (orchestration), `filter.py` (language table materialization), and `progress.py` (shared progress reporters).
- LLM-facing enrichments live in `src/open_dictionary/llm`, while cross-cutting utilities (environment loading, helpers) belong in `src/open_dictionary/utils`.
- Runtime artifacts such as dumps or extracted JSONL files are expected in a local `data/` directory (not tracked); scripts should accept paths rather than hard-code locations.

## Build, Test, and Development Commands

- `uv sync` installs all dependencies declared in `pyproject.toml`.
- `uv run open-dictionary download --output data/raw-wiktextract-data.jsonl.gz` streams the upstream Wiktextract snapshot.
- `uv run open-dictionary pipeline --workdir data --table dictionary --column data --truncate` executes download → extract → load → partition in one shot; add `--skip-*` flags for partial runs.
- `uv run open-dictionary filter en zh --table dictionary_all --column data` copies only selected languages into `dictionary_lang_*` tables; pass `all` as the first positional argument to materialize every language code.
- `uv run python -m pytest` is the expected test runner once suites are added; for now, rely on targeted CLI runs against a disposable PostgreSQL database.

## Coding Style & Naming Conventions

- Target Python 3.12+, four-space indentation, and `snake_case` for functions, modules, and CLI subcommand names.
- Prefer type hints and `pydantic` models for structured payloads (see `llm/define.py`), and keep side effects behind small helpers for easier testing.
- Environment keys (`DATABASE_URL`, `LLM_KEY`, `LLM_API`, `LLM_MODEL`) are loaded through `utils.env_loader`; never fetch them ad hoc inside command bodies.

## Testing Guidelines

- Focus on integration tests that exercise the CLI contract end-to-end with a seeded PostgreSQL container; isolate I/O with temp directories under `tmp_path`.
- Name test modules `test_<feature>.py` and colocate fixtures under `tests/conftest.py` once the suite exists.
- Validate large operations by asserting row counts, emitted table names, and LLM scaffolding errors rather than snapshotting full JSON.

## Commit & Pull Request Guidelines

- Follow the existing history: concise imperative subject lines (e.g. “Add DB iterator”), optional body wrapped at ~72 chars.
- Reference issue IDs in the body when available and note required migrations or manual steps.
- PRs should describe the dataset used for validation, include command transcripts (`uv run …`) for any pipelines executed, and, when UI/CLI behavior changes, attach representative logs or screenshots.

## Environment & Security Tips

- Keep `.env` files local; share example variables via documentation rather than version control.
- Never commit API keys or database URLs. If sensitive configuration is required in CI, use repository secrets and reference them through environment loader helpers.
