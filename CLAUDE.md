# CLAUDE.md

## Project

**migrateit** — A CLI database migration tool for PostgreSQL, SQLite, and MySQL. Manages database schema changes through versioned SQL files with support for dependency trees, squashing, rollback, schema export, and hash-based change detection.

## Python & Code Style Guidelines

- **Target Version**: Python 3.13+.
- **Code Organization**: Use `migrateit/` for source code, `tests/` for tests.
- **Type Annotations (Strict)**:
  - Every function parameter, return type, and class attribute **must** be explicitly typed.
  - Use standard library generics (`list[str]`, `dict[str, int]`, `tuple[int, ...]`) and standard union syntax (`str | None`).
  - Use `Self` for methods returning instance references.
  - Avoid `Any`. Use precise types or generics.
- **Tooling Configuration**:
  - `mypy`, `ruff`, `pyright`, and `pytest` settings are centralized in `pyproject.toml`.
  - `pre-commit` manages Git hooks via `.pre-commit-config.yaml`.
- **Testing**: Write type-annotated test functions under `tests/` mirroring the core package layout. Test fixtures are parameterized across all supported databases (postgres, mysql, sqlite).

## AI Assistant Operational Rules

- Always run `mypy .` and `ruff check .` to verify changes before completing a task.
- Ensure any newly added code is type annotated.
- Ensure any newly added code maintains 100% code coverage.

## Essential Commands

```
# Run the CLI
migrateit init postgres
migrateit init sqlite
migrateit init mysql
migrateit new migration_name
migrateit new migration_name -d dep1 dep2
migrateit show
migrateit show -l
migrateit show --validate-sql
migrateit export                       # export full database schema to a migration
migrateit export my_full_export        # with a custom name
migrateit migrate
migrateit migrate --fake               # mark as applied without running SQL
migrateit migrate --update-hash
migrateit migrate migration_name       # run a specific migration
migrateit rollback 0000
migrateit rollback --fake              # fake a rollback (mark as undone)
migrateit squash 0001 0005
```

## Architecture

### Key architectural patterns

1. **Strategy/Protocol pattern for database clients**: `SqlClientProtocol` in `_protocol.py` defines the interface with 11 abstract methods. `SqlClient[T]` is a generic ABC holding a typed `connection` and `MigrateItConfig`. Three concrete implementations exist: `PsqlClient` (PostgreSQL, uses cursor-based execution), `SqliteClient` (SQLite, uses `executescript`), and `MySqlClient` (MySQL, uses `mysql-connector-python` with cursor-based execution). New databases implement `SqlClientProtocol` and subclass `SqlClient`.

2. **Migration DAG**: Migrations form a directed acyclic graph via `parents` lists on `Migration` objects. `build_migration_plan()` in `changelog.py` (on `ChangelogFile`) does a BFS topological traversal to produce an execution plan, supporting forward migrations, rollbacks, target-specific runs, and bottom-up plans. `find_path()` in `tree.py` finds paths in the migration tree for squashing.

3. **Changelog as source of truth**: `changelog.json` on disk tracks all migrations (names, parents, initial flag, database type). The database's changelog table tracks applied migrations with SHA-256 hashes. Status is computed by diffing file-system migrations against database records, yielding `APPLIED`, `NOT_APPLIED`, `REMOVED` (in DB but not on disk), or `CONFLICT` (hash mismatch).

4. **Migration file format**: SQL files follow `NNNN_name.sql` convention with a header comment. Forward and rollback SQL are separated by the `-- Rollback migration` tag. The `_get_migration_content_and_hash()` method splits on this tag and computes a SHA-256 of the full content.

5. **Connection model**: `connection.py` defines a unified `Connection` type as a union of `_PsqlConnection`, `_SqliteConnection`, and `_MySqlConnection`. `get_connection(database: SupportedDatabase)` opens and returns the appropriate typed connection based on the database type from the changelog.

6. **Context manager patterns**: `error_handler()` wraps the entire CLI execution for consistent error reporting. `logging_handler()` installs a colored logging handler. `ExitStack` is used for multi-stream output (terminal + log file) and safe cleanup.

7. **SQL validation**: `validate_sql_syntax()` uses `sqlfluff` with per-database dialects to validate migration SQL. Works without a live database connection. The `_patch_sql_statement()` base method adds `IF NOT EXISTS` guards for `CREATE TABLE`, `DROP TABLE`, and `ALTER TABLE ADD COLUMN` statements to make validation more permissive.

### Config

- `MIGRATEIT_MIGRATIONS_DIR` (default: `migrateit`) — root directory for migrations
- `MIGRATEIT_MIGRATIONS_TABLE` (default: `MIGRATEIT_CHANGELOG`) — name of the changelog table
- DB credentials via environment variables:

| Variable   | PostgreSQL                | MySQL                     | SQLite    |
| ---------- | ------------------------- | ------------------------- | --------- |
| URL        | `DB_URL`                  | `DB_URL`                  | `DB_URL`  |
| File (alt) | —                         | —                         | `DB_FILE` |
| Host       | `DB_HOST` (localhost)     | `DB_HOST` (localhost)     | —         |
| Port       | `DB_PORT` (5432)          | `DB_PORT` (3306)          | —         |
| User       | `DB_USER` (postgres)      | `DB_USER` (root)          | —         |
| Password   | `DB_PASS`                 | `DB_PASS`                 | —         |
| Database   | `DB_NAME` (postgres)      | `DB_NAME` (migrateit)     | —         |
| Timeout    | `DB_TIMEOUT_SECONDS` (30) | `DB_TIMEOUT_SECONDS` (30) | —         |

## Supported Databases

| Database   | Driver                   | Notes                              |
| ---------- | ------------------------ | ---------------------------------- |
| PostgreSQL | `psycopg`                | Full feature support, cursor-based |
| SQLite     | `sqlite3` (stdlib)       | `executescript`-based              |
| MySQL      | `mysql-connector-python` | Full feature support, cursor-based |

## Test Markers

| Marker     | Description                             |
| ---------- | --------------------------------------- |
| `postgres` | Tests requiring a PostgreSQL connection |
| `sqlite`   | Tests requiring a SQLite connection     |
| `mysql`    | Tests requiring a MySQL connection      |
| `unit`     | Fast unit tests with no database        |
