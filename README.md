```
##########################################
 __  __ _                 _       ___ _
|  \/  (_) __ _ _ __ __ _| |_ ___|_ _| |_
| |\/| | |/ _` | '__/ _` | __/ _ \| || __|
| |  | | | (_| | | | (_| | ||  __/| || |_
|_|  |_|_|\__, |_|  \__,_|\__\___|___|\__|
          |___/
##########################################
```

Handle database migrations with ease. Manage your database changes with simple SQL files.
Make the migration process easier, more manageable and repeatable.

# How does this work

### Installation

```sh
pip install migrateit
```

### Configuration

Configurations can be changed as environment variables.

```ini
# basic configuration
MIGRATEIT_MIGRATIONS_TABLE=MIGRATEIT_CHANGELOG
MIGRATEIT_MIGRATIONS_DIR=migrateit        # directory for migration files

# database connection variables
DB_URL=postgresql://postgres:postgres@localhost:5432/postgres
DB_FILE=migrateit.db                      # SQLite only (default)
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASS=postgres
DB_TIMEOUT_SECONDS=30
```

### Usage

```sh
# Initialize MigrateIt — creates:
#   - 'migrations' directory
#   - 'changelog.json' file
#   - first migration file (table creation + rollback)
migrateit init postgres
# or
migrateit init sqlite

# Create a new migration file
migrateit new first_migration

# Create a migration with dependencies
migrateit new add_email -d 0000

# Add your SQL commands to the migration file
echo "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);" > migrateit/0001_first_migration.sql

# Show pending migrations
migrateit show
migrateit show -l

# Run the migrations
migrateit migrate

# Run a specific migration
migrateit migrate 0001

# Rollback a migration
migrateit rollback 0001

# Squash migrations into a single file
migrateit squash 0001 0005

# Fake a migration (mark as applied without running SQL)
migrateit migrate --fake

# Update migration hash without re-running
migrateit migrate --update-hash
```

# Example

```sql
-- Migration 0000_user.sql
-- Created on 2025-05-15T19:55:18.711752

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    given_name TEXT,
    family_name TEXT,
    picture TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rollback migration

DROP TABLE IF EXISTS users;
```

# Help

```sh
usage: migrateit init [-h] {postgres,sqlite}

positional arguments:
  {postgres,sqlite}   Database type to use

options:
  -h, --help          show this help message and exit
```

```sh
usage: migrateit new [-h] [-d [DEPENDENCIES ...]] [--no-edit] [name]

positional arguments:
  name                  Name of the new migration

options:
  -h, --help            show this help message and exit
  -d, --dependencies [DEPENDENCIES ...]
                        List of migration names that this migration depends on.
  --no-edit             Avoid opening the migration file in an editor after creation.
```

```sh
usage: migrateit migrate [-h] [--fake] [--update-hash] [name]

positional arguments:
  name           Name of the migration to run

options:
  -h, --help     show this help message and exit
  --fake         Fakes the migration marking it as ran.
  --update-hash  Update the hash of the migration.
```

```sh
usage: migrateit show [-h] [-l] [--validate-sql]

options:
  -h, --help        show this help message and exit
  -l, --list        Display migrations in a list format.
  --validate-sql    Validate SQL migration syntax.
```

```sh
usage: migrateit rollback [-h] [name]

positional arguments:
  name          Name of the migration to rollback

options:
  -h, --help    show this help message and exit
```

```sh
usage: migrateit squash [-h] [-n NAME] start_migration [end_migration]

positional arguments:
  start_migration  Name of the first migration to squash from (inclusive).
  end_migration    Name of the last migration to squash to (inclusive). If not provided, the last migration is used.

options:
  -h, --help       show this help message and exit
  -n, --name NAME  Name of the new squashed migration file. If not provided, a default name will be generated.
```
