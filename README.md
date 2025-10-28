# Open English Dictionary

Utilities for downloading and loading the Wiktionary dataset into PostgreSQL.

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

Each command streams data in chunks to handle the 10M+ line dataset efficiently.
