# Open English → Chinese Dictionary

We are building the strongest open English–Chinese dictionary by ingesting Wiktionary content, storing the raw entries in PostgreSQL, and layering Chinese-specific enrichment on top. This repository contains the reproducible pipeline for downloading the upstream dump, extracting the 20 GB+ JSONL file, importing it into PostgreSQL, and splitting the data into language-specific tables for downstream processing.

---

## Workflow Overview

1. **Download** the official [`raw-wiktextract-data.jsonl.gz`](https://kaikki.org/dictionary/raw-wiktextract-data.jsonl.gz) archive (≈2.5 GB compressed).
2. **Extract** the archive into a 20 GB+ JSON Lines file (~10 million rows).
3. **Load** each JSON record into a PostgreSQL table (`id BIGSERIAL`, `data JSONB`).
4. **Partition** the table into smaller per-language tables using the `lang_code` field so that downstream processors can focus on the languages they need (for us: English → Chinese).

All steps are wrapped in an executable CLI so the workflow can be repeated by anyone with the right environment.

---

## Prerequisites

- Python 3.12+
- `uv` (or another tool) to install project dependencies defined in `pyproject.toml`
- PostgreSQL 14+ with a database that you can write to
- `.env` file that exposes `DATABASE_URL`, for example:

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/open_dictionary
```

Run `uv sync` once to install the Python dependencies, then continue with the workflow commands below.

---

## Step-by-Step Commands

All commands run through the packaged CLI entry point `open-dictionary`.

### 1. Download the dump

```bash
uv run open-dictionary download --output data/raw-wiktextract-data.jsonl.gz
```

The downloader streams the response to disk and prints periodic progress (GiB) to `stderr`. Use `--overwrite` to force a redownload.

### 2. Extract the JSONL

```bash
uv run open-dictionary extract --input data/raw-wiktextract-data.jsonl.gz --output data/raw-wiktextract-data.jsonl
```

Extraction is also streamed to avoid loading the full archive into memory. Progress is reported as the compressed bytes are consumed.

### 3. Load into PostgreSQL

```bash
uv run open-dictionary load \
  data/raw-wiktextract-data.jsonl \
  --table dictionary \
  --column data \
  --truncate
```

- Validates each JSON line and streams directly into PostgreSQL via `COPY`.
- Default destination: table `dictionary` with JSONB column `data` (auto-created if missing).
- Shows import progress to `stderr` (rows, GiB read, % completed).

### 4. Partition by language code

```bash
uv run open-dictionary partition \
  --table dictionary \
  --column data \
  --lang-field lang_code \
  --prefix dictionary_lang
```

- Reads distinct language codes from `data->>'lang_code'`.
- Creates tables like `dictionary_lang_en`, `dictionary_lang_zh`, etc. (or places them in `--target-schema`).
- Uses `ON CONFLICT` to make the command idempotent; add `--drop-existing` to rebuild from scratch.

### One-shot pipeline

```bash
uv run open-dictionary pipeline \
  --workdir data \
  --table dictionary \
  --column data \
  --truncate
```

`pipeline` runs download → extract → load → partition in sequence. Flags allow you to skip individual stages (e.g., `--skip-download`, `--skip-partition`) or overwrite existing files.

---

## Database Layout

- **`dictionary`** – canonical table
  - `id BIGSERIAL PRIMARY KEY`
  - `data JSONB NOT NULL`
- **`dictionary_lang_<code>`** – per-language clones created by the partition step (same columns, `id` preserved from `dictionary`).

Because the importer validates JSON line-by-line, corrupt rows fail fast with a clear message that includes the line number. Progress indicators are coarse-grained so they remain useful even on 20 GB+ inputs.

---

## Tips & Troubleshooting

- Running the importer on such a large file is I/O-bound; place the working directory on fast storage (NVMe or similar) for best results.
- If you already have the archive/JSONL files, use the `--skip-*` flags on `pipeline` or run the individual subcommands to avoid redundant work.
- Use `psql` or your preferred client to create indexes or views on the partitioned tables to support downstream translation logic.

---

## Contributing

- Improvements to the pipeline are welcome—especially around performance, monitoring, or tooling for downstream enrichment.
- Please open an issue or pull request with reproducible steps so others can benefit from your changes.

---

Happy hacking! 一起打造最强大的开放式英汉词典。 🇬🇧 → 🇨🇳
