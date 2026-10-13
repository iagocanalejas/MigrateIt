# CLAUDE.md

## Project

**migrateit** — A CLI database migration tool for PostgreSQL, SQLite, MySQL, MariaDB, and Oracle. Manages database schema changes through versioned SQL files with support for dependency trees, squashing, rollback, schema export, hash-based change detection, and migration dropping.

## Python & Code Style Guidelines

- **Target Version**: Python 3.13+.
- **Code Organization**: Use `migrateit/` for source code, `tests/` for tests.
- **Type Annotations (Strict)**:
  - Every function parameter, return type, and class attribute **must** be explicitly typed.
  - Use standard library generics (`list[str]`, `dict[str, int]`, `tuple[int, ...]`) and standard union syntax (`str | None`).
  - Use `Self` for methods returning instance references.
  - Use `type` aliases for connection types (e.g., `type _PsqlConnection = psycopg.Connection`).
  - Avoid `Any`. Use precise types or generics.
- **Tooling Configuration**:
  - `mypy`, `ruff`, `pyright`, and `pytest` settings are centralized in `pyproject.toml`.
  - `pre-commit` manages Git hooks via `.pre-commit-config.yaml`.
- **Testing**: Write type-annotated test functions under `tests/` mirroring the core package layout. Test fixtures are parameterized across all supported databases (postgres, mysql, sqlite, oracle).
- **mypy overrides**: Third-party stubs (e.g., `inquirer.*`) use `ignore_missing_imports = true`.

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
migrateit init mariadb
migrateit init oracle
migrateit new migration_name
migrateit new migration_name -d dep1 dep2
migrateit new migration_name --interactive
migrateit new migration_name --no-edit
migrateit show
migrateit show -l
migrateit show --validate-sql
migrateit export                       # export full database schema to a migration
migrateit export my_full_export        # with a custom name
migrateit migrate
migrateit migrate --fake               # mark as applied without running SQL
migrateit migrate --update-hash        # update hash in the database
migrateit migrate --plan-only          # dry run without applying
migrateit migrate migration_name       # run a specific migration
migrateit rollback 0000
migrateit rollback --fake              # fake a rollback (mark as undone)
migrateit rollback --plan-only         # dry run rollback
migrateit squash 0001 0005
migrateit squash 0001 0005 -n custom_name
migrateit squash 0001                  # squashes from 0001 to last
migrateit drop migration_name          # rollback and delete a migration
```

## Architecture

### Key architectural patterns

1. **Protocol/ABC pattern for database clients**: `SqlClientProtocol` in `_protocol.py` defines the interface with 13 abstract methods plus an `export_items` property. A separate `SqlConnectionProtocol` defines connection-level methods (`execute`, `execute_for_one`, `execute_for_rows`, `placeholder`). `SqlClient[T: Connection]` is a generic ABC combining both protocols, holding a typed `connection` and `MigrateItConfig`. Four concrete implementations exist: `PsqlClient` (PostgreSQL, uses cursor-based execution), `SqliteClient` (SQLite, uses `executescript`), `MySqlClient` (MySQL/MariaDB, uses `mysql-connector-python` with cursor-based execution), and `OracleClient` (Oracle, uses `oracledb` with cursor-based execution and named parameters). New databases implement both protocols and subclass `SqlClient`.

2. **Three-layer data/access architecture**:
   - **Data model** (`ChangelogFile` in `models/changelog.py`): Pure dataclass holding migration metadata (`version`, `database`, `migrations`, `path`). Provides data accessors (`root` — initial migration, `migrations_tree` — DAG) and serialization (`to_dict`, `to_json`, `from_json`). Contains no operation methods.
   - **Operations** (`MigrationManager` in `models/migration_manager.py`): Wraps a `ChangelogFile` and provides all migration management methods: `create_new_migration()`, `get_migration_by_name()`, `exist_migration_by_name()`, `build_migration_plan()` (BFS topological traversal), `find_path()` (DAG path finding), `print_list()`, `print_dag()`. Accessed via `client.migrations` on `SqlClient`.
   - **Persistence** (`ChangelogRepository` in `models/repository.py`): Handles all disk I/O — `load()`, `save()`, `create()`. Convenience functions `create_changelog_file()` and `load_changelog_file()` delegate to the repository.

3. **Migration DAG**: Migrations form a directed acyclic graph via `parents` lists on `Migration` objects. `MigrationManager.build_migration_plan()` does a BFS topological traversal to produce an execution plan, supporting forward migrations, rollbacks, target-specific runs, and bottom-up plans. `MigrationManager.find_path()` finds paths in the migration tree for squashing.

4. **Changelog as source of truth**: `changelog.json` on disk tracks all migrations (names, parents, initial flag, database type). The database's changelog table tracks applied migrations with SHA-256 hashes. Status is computed by diffing file-system migrations against database records, yielding `APPLIED`, `NOT_APPLIED`, `REMOVED` (in DB but not on disk), or `CONFLICT` (hash mismatch).

5. **Migration file format**: SQL files follow `NNNN_name.sql` convention with a header comment (`-- Migration {name}\n-- Created on {timestamp}`). Forward and rollback SQL are separated by the `-- Rollback migration` tag. `get_content_and_hash()` on `Migration` splits on this tag and computes a SHA-256 of the full content, returning `(forward_sql, rollback_sql, hash)`.

6. **Connection model**: `models/connection.py` defines typed connection aliases (`_PsqlConnection`, `_SqliteConnection`, `_MySqlConnection`, `_OracleConnection`) and a union `Connection` type. `get_connection(database: SupportedDatabase)` opens and returns the appropriate typed connection. `get_client(config, connection)` in `_client.py` dispatches to the correct concrete client (`MySqlClient`, `PsqlClient`, `SqliteClient`, or `OracleClient`).

7. **Context manager patterns**: `error_handler()` and `logging_handler()` are context managers in `reporters/` used in `main.py` to wrap CLI execution. `ExitStack` is no longer used — the old multi-stream pattern was replaced by individual context managers.

8. **SQL validation**: `validate_sql_syntax()` uses `sqlfluff` with per-database dialects to validate migration SQL. Works without a live database connection. The `_patch_sql_statement()` base method in `SqlClient` adds `IF NOT EXISTS` guards for `CREATE TABLE`, `DROP TABLE`, and `ALTER TABLE ADD COLUMN` statements to make validation more permissive. MariaDB uses the same MySQL dialect. Oracle uses the `oracle` dialect and strips `IF EXISTS` guards (Oracle does not support them).

9. **Schema export**: `export_database_schema()` in `SqlClient` iterates over `export_items` (per-database property defining metadata queries and row processors) to generate forward DDL and reversed rollback DDL into a migration file. Oracle exports tables/columns, indexes, constraints (PK/Unique), foreign keys, views, sequences, and triggers.

10. **Oracle client**: `OracleClient` in `clients/oracle.py` uses `oracledb` with cursor-based execution. Oracle uses named parameters (`:param` → `:1`, `:2`, ...) instead of positional placeholders. The changelog table uses a `SEQUENCE` for auto-incrementing IDs and `NUMBER(1)` for the `squashed` flag. Oracle uses `MERGE INTO ... USING ...` for upserts and `SELECT ... FROM DUAL` for scalar queries. The `_OracleLocker` uses a lock table with `NUMBER GENERATED BY DEFAULT AS IDENTITY` primary key. Oracle exports metadata from `all_tab_columns`, `all_indexes`, `all_constraints`, `all_views`, `user_sequences`, and `user_triggers`. SQL validation uses the `oracle` dialect via sqlfluff.

### Config

- **Migrations root**: `MIGRATEIT_MIGRATIONS_DIR` (default: `migrateit`) — root directory containing `changelog.json` and `migrations/`.
- **Changelog table**: `MIGRATEIT_MIGRATIONS_TABLE` (default: `MIGRATEIT_CHANGELOG`).
- **Migrations directory**: `migrateit/migrations/` (inside root).
- **Changelog file**: `migrateit/changelog.json`.

DB credentials are read via `VARNAME_DB_*` prefixed env variables with hardcoded defaults in each client class:

| Variable          | PostgreSQL                | MySQL                     | SQLite    | Oracle                    |
| ----------------- | ------------------------- | ------------------------- | --------- | ------------------------- |
| `DB_URL`          | `DB_URL`                  | `DB_URL`                  | `DB_URL`  | `DB_URL` (DSN string)     |
| File (alt)        | —                         | —                         | `DB_FILE` | —                         |
| Host              | `DB_HOST` (localhost)     | `DB_HOST` (localhost)     | —         | `DB_HOST` (localhost)     |
| Port              | `DB_PORT` (5432)          | `DB_PORT` (3306)          | —         | `DB_PORT` (1521)          |
| User              | `DB_USER` (postgres)      | `DB_USER` (root)          | —         | `DB_USER` (oracle)        |
| Password          | `DB_PASS`                 | `DB_PASS`                 | —         | `DB_PASS` (password)      |
| Database          | `DB_NAME` (migrateit)     | `DB_NAME` (migrateit)     | —         | `DB_NAME` (service_name)  |
| Timeout (seconds) | `DB_TIMEOUT_SECONDS` (30) | `DB_TIMEOUT_SECONDS` (30) | —         | —                         |
| Additional        | —                         | —                         | —         | `DB_SID` (SID, alternative to service_name) |

## Supported Databases

| Database   | Driver                   | Notes                              |
| ---------- | ------------------------ | ---------------------------------- |
| PostgreSQL | `psycopg`                | Full feature support, cursor-based |
| SQLite     | `sqlite3` (stdlib)       | `executescript`-based              |
| MySQL      | `mysql-connector-python` | Full feature support, cursor-based |
| MariaDB    | `mysql-connector-python` | Same dialect as MySQL              |
| Oracle     | `oracledb`               | Full feature support, cursor-based |

## Test Markers

| Marker     | Description                             |
| ---------- | --------------------------------------- |
| `postgres` | Tests requiring a PostgreSQL connection |
| `sqlite`   | Tests requiring a SQLite connection     |
| `mysql`    | Tests requiring a MySQL connection      |
| `mariadb`  | Tests requiring a MariaDB connection    |
| `oracle`   | Tests requiring an Oracle connection    |
| `unit`     | Fast unit tests with no database        |
