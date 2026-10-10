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
# BASIC CONFIGURATION
MIGRATEIT_MIGRATIONS_TABLE=MIGRATEIT_CHANGELOG
MIGRATEIT_MIGRATIONS_DIR=migrateit        # directory for migration files

# DATABASE CONNECTION

# psql
DB_URL=postgresql://postgres:postgres@localhost:5432/postgres
# or
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASS=postgres

# sqlite
DB_URL=sqlite:///migrateit.db
# or
DB_FILE=migrateit.db

# mysql
DB_URL=mysql://root:@localhost:3306/migrateit
# or
DB_HOST=localhost
DB_PORT=3306
DB_NAME=migrateit
DB_USER=root
DB_PASS=

# common
DB_TIMEOUT_SECONDS=30
```

### Usage

```sh
# Initialize MigrateIt — creates:
#   - 'migrations' directory
#   - 'changelog.json' file
#   - first migration file (table creation + rollback)
migrateit init postgres
migrateit init sqlite
migrateit init mysql

# Create a new migration file
migrateit new first_migration

# Create a migration with dependencies
migrateit new add_email -d 0000

# Create a migration choosing dependencies interactively
migrateit new add_email -i

# Add your SQL commands to the migration file
echo "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT);" > migrateit/0001_first_migration.sql

# Show pending migrations
migrateit show
migrateit show -l

# Export the full database schema to a migration file
migrateit export
migrateit export my_full_export    # with a custom name

# Run the migrations
migrateit migrate

# Print the migration plan without applying
migrateit migrate --plan-only

# Run a specific migration
migrateit migrate 0001

# Rollback a migration
migrateit rollback 0001

# Squash migrations into a single file
migrateit squash 0001 0005

# Fake a migration (mark as applied without running SQL)
migrateit migrate --fake 0001

# Fake a rollback (mark as undone without running SQL)
migrateit rollback --fake 0001

# Update migration hash without re-running
migrateit migrate --update-hash 0001

# Drop a migration
migrateit drop 0001
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
usage: migrateit init [-h] {postgres,sqlite,mysql,mariadb}

positional arguments:
  {postgres,sqlite,mysql,mariadb}   Database type to use

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
  -i, --interactive     Choose dependencies interactively.
  --no-edit             Avoid opening the migration file in an editor after creation.
```

```sh
usage: migrateit migrate [-h] [--fake] [--plan-only] [--update-hash] [name]

positional arguments:
  name           Name of the migration to run

options:
  -h, --help     show this help message and exit
  --fake         Fakes the migration marking it as ran.
  --plan-only    Dry run the migration without applying it.
  --update-hash  Update the hash of the migration.
```

```sh
usage: migrateit export [-h] [name]

positional arguments:
  name           Name of the migration containing the database SQL dump.

options:
  -h, --help     show this help message and exit
```

```sh
usage: migrateit show [-h] [-l] [--validate-sql]

options:
  -h, --help        show this help message and exit
  -l, --list        Display migrations in a list format.
  --validate-sql    Validate SQL migration syntax.
```

```sh
usage: migrateit rollback [-h] [--fake] [--plan-only] [name]

positional arguments:
  name          Name of the migration to rollback

options:
  -h, --help    show this help message and exit
  --fake        Fakes the rollback marking it as undone.
  --plan-only    Dry run the migration without applying it.
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

```sh
usage: migrateit drop [-h] name

positional arguments:
  name        Name of the migration to drop.

options:
  -h, --help  show this help message and exit
```
