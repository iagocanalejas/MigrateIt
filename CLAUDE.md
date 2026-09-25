# CLAUDE.md

This file provides guidance for Claude Code when working with code in this repository.

## Project

**migrateit** — A CLI database migration tool for PostgreSQL and SQLite. Manages database schema changes through versioned SQL files with support for dependency trees, squashing, rollback, and hash-based change detection.

## Python & Code Style Guidelines

- **Environment**: Never create anything outside of this repository.
- **Code Organization**: Use `migrateit/` for source code.
- **Testing**: Write tests under `tests/`.
- **Type Checking**: Ensure all code is type-checked with `mypy`.
- **Linting**: Use `ruff` for linting and formatting.
- **Code Formatting**: Use `ruff` for consistent code formatting.
- **Type Annotations (Strict)**:
  - Every function parameter, return type, and class attribute **must** be explicitly typed.
  - Use standard library generics (`list[str]`, `dict[str, int]`, `tuple[int, ...]`) and standard union syntax (`str | None`).
  - Use `Self` for methods returning instance references.
  - Avoid `Any`. Use precise types or generics.
- **Tooling Configuration**:
  - `mypy`, `ruff`, and `pytest` settings are centralized in `pyproject.toml`.
  - `pre-commit` manages Git hooks via `.pre-commit-config.yaml`.
- **Imports**: Group imports strictly as Standard Library → External → Internal, handled automatically by `ruff`.
- **Testing**: Write type-annotated test functions under `tests/` mirroring the core package layout.

## AI Assistant Operational Rules

- Always run `mypy .` and `ruff check .` to verify changes before completing a task.
- Ensure any newly added code is type annotated.
- Ensure any newly added code maintains 100% code coverage.

## Essential Commands

```bash
# Setup
python3.14 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

# Run tests
pytest

# Run a specific test
pytest tests/cli/init_test.py -v

# Type check
mypy .

# Lint & format
ruff check . && ruff format .

# Pre-commit hooks
pre-commit run --all-files

# Run the CLI
migrateit init postgres
migrateit init sqlite
migrateit new migration_name
migrateit new migration_name -d dep1 dep2
migrateit show
migrateit show -l
migrateit show --validate-sql
migrateit migrate
migrateit migrate --fake
migrateit migrate --update-hash
migrateit migrate migration_name
migrateit rollback 0000
migrateit squash 0001 0005
```

## Architecture

### Package structure (`migrateit/`)

```
migrateit/
├── cli.py            # Command implementations (cmd_init, cmd_new, cmd_run, cmd_squash, cmd_show)
├── main.py           # CLI entry point: argparse subcommand parsing, connection setup, dispatch
├── tree.py           # File/tree operations: migration file I/O, changelog persistence, DAG building, plan computation
├── constants.py      # Version, defaults for root dir and table name
├── clients/          # Database client layer
│   ├── _protocol.py  # SqlClientProtocol (Protocol defining the database client interface)
│   ├── _client.py    # SqlClient[T] (generic ABC holding connection + config + env var names)
│   ├── psql.py       # PsqlClient(SqlClient[Connection]) — PostgreSQL implementation
│   └── sqlite.py     # SqliteClient(SqlClient[sqlite3.Connection]) — SQLite implementation
├── models/           # Domain dataclasses
│   ├── config.py     # MigrateItConfig (table_name, migrations_dir, changelog)
│   ├── migration.py  # Migration (name, initial flag, parents list), MigrationStatus enum
│   └── changelog.py  # ChangelogFile (version, database type, migration list), SupportedDatabase enum
└── reporters/        # Output and error handling
    ├── output.py     # Raw byte output, ExitStack-based multi-stream logging, DAG/tree printing
    ├── errors.py     # FatalError, error_handler context manager
    ├── logs.py       # LoggingHandler, logging_handler context manager
    └── _utils.py     # Color constants, force_bytes, format_color helpers
```

### Key architectural patterns

1. **Strategy/Protocol pattern for database clients**: `SqlClientProtocol` in `_protocol.py` defines the interface. `SqlClient[T]` is a generic ABC holding a typed `connection` and `MigrateItConfig`. `PsqlClient` and `SqliteClient` are the concrete implementations today. New databases implement `SqlClientProtocol` and subclass `SqlClient`.

2. **Migration DAG**: Migrations form a directed acyclic graph via `parents` lists on `Migration` objects. `build_migrations_tree()` in `tree.py` builds an `OrderedDict[str, list[Migration]]` (parent → children). `build_migration_plan()` does a topological traversal (BFS) to produce an execution plan, supporting forward migrations, rollbacks, and target-specific runs.

3. **Changelog as source of truth**: `changelog.json` on disk tracks all migrations (names, parents, initial flag). The database's changelog table tracks applied migrations with SHA-256 hashes. Status is computed by diffing file-system migrations against database records, yielding `APPLIED`, `NOT_APPLIED`, `REMOVED` (in DB but not on disk), or `CONFLICT` (hash mismatch).

4. **Migration file format**: SQL files follow `NNNN_name.sql` convention. Forward and rollback SQL are separated by the `-- Rollback migration` tag. The `_get_content_hash()` method splits on this tag and computes a SHA-256 of the full content.

5. **Command flow**: `main.py` parses args via argparse → opens DB connection → constructs typed client (`PsqlClient` or `SqliteClient`) → dispatches to command functions in `cli.py`. Each command function calls tree utilities for file I/O and client methods for DB operations. Rollbacks and migrations share `cmd_run()` with different flags.

6. **Context manager patterns**: `error_handler()` wraps the entire CLI execution for consistent error reporting. `logging_handler()` installs a colored logging handler. `ExitStack` is used for multi-stream output (terminal + log file) and safe cleanup.

### Config

- `MIGRATEIT_MIGRATIONS_DIR` (default: `migrateit`) — root directory for migrations
- `MIGRATEIT_MIGRATIONS_TABLE` (default: `MIGRATEIT_CHANGELOG`) — name of the changelog table
- DB credentials via `DB_URL`, `DB_FILE` (SQLite only), `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME`, `DB_TIMEOUT_SECONDS`

## Supported Databases

| Database   | Driver    | Notes                              |
| ---------- | --------- | ---------------------------------- |
| PostgreSQL | `psycopg` | Full feature support, cursor-based |
| SQLite     | `sqlite3` | Stdlib, `executescript`-based      |
