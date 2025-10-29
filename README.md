# Open English Dictionary

## Rebuilding process WIP

## Currently, this project is being rebuilt.

New features are:

- Streamlined process + pipeline integration
- Wiktionary grounding + LLM explain
  - Enormous words data across multiple languages
  - Extremely detailed definitions
- New distribution format will be: jsonl, sqlite and more are to be determined
- Options are available to select specific category of words

**Behold and stay tuned!**

## Prerequisites

- Install project dependencies: `uv sync`
- Configure a `.env` file with `DATABASE_URL`
- Ensure a PostgreSQL database is reachable via that URL

## Run The Wiktionary Workflow

Download the compressed dump:

```bash
uv run open-dictionary download --output data/raw-wiktextract-data.jsonl.gz
```

Extract the JSONL file:

```bash
uv run open-dictionary extract \
  --input data/raw-wiktextract-data.jsonl.gz \
  --output data/raw-wiktextract-data.jsonl
```

Stream the JSONL into PostgreSQL (`dictionary_all.data` is JSONB):

```bash
uv run open-dictionary load data/raw-wiktextract-data.jsonl \
  --table dictionary_all \
  --column data \
  --truncate
```

Run everything end-to-end with optional partitioning:

```bash
uv run open-dictionary pipeline \
  --workdir data \
  --table dictionary_all \
  --column data \
  --truncate
```

Split rows by language code into per-language tables when needed:

```bash
uv run open-dictionary partition \
  --table dictionary_all \
  --column data \
  --lang-field lang_code
```

Materialize a smaller set of languages into dedicated tables with a custom prefix:

```bash
uv run open-dictionary filter en zh \
  --table dictionary_all \
  --column data \
  --table-prefix dictionary_filtered
```

Pass `all` to emit every language into its own table:

```bash
uv run open-dictionary filter all --table dictionary_all --column data
```

Remove low-quality rows (zero common score, numeric tokens, legacy tags) directly in PostgreSQL:

```bash
uv run open-dictionary db-clean --table dictionary_filtered_en
```

Populate the `common_score` column with word frequency data (re-run with `--recompute-existing` to refresh scores):

```bash
uv run open-dictionary db-commonness --table dictionary_filtered_en
```

Each command streams data in chunks to handle the 10M+ line dataset efficiently.
