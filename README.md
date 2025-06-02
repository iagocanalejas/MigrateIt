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

Handle database migrations with ease managing your database changes with simple SQL files.
Make the migration process easier, more manageable and repeteable.

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
MIGRATEIT_MIGRATIONS_DIR=migrateit

# change the database connection variables
VARNAME_DB_URL=DB_URL
VARNAME_DB_HOST=DB_HOST
VARNAME_DB_PORT=DB_PORT
VARNAME_DB_USER=DB_USER
VARNAME_DB_PASS=DB_PASS
VARNAME_DB_NAME=DB_NAME


# database configuration
DB_URL=postgresql://postgres:postgres@localhost:5432/postgres
# -------- or ----------
DB_HOST=localhost
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASS=postgres
```

### Usage

```sh
# initialize MigrateIt to create:
# - 'migrations' directory inside the MIGRATIONS_DIR
# - 'changelog.json' file inside the MIGRATIONS_DIR
# - first migration file with the migrateit table creation and rollback
migrateit init postgres

# create a new migration file
migrateit new first_migration

# add your sql commands to the migration file
echo "CREATE TABLE test (id SERIAL PRIMARY KEY, name VARCHAR(50));" > migrateit/migrations/0000_first_migration.sql

# show pending migrations
migrateit show
migrateit show -l

# run the migrations
migrateit migrate

# or run a given migration
migrateit migrate 0000

# rollback a migration
migrateit rollback 0000
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
usage: migrateit new [-h] [--no-edit] name

positional arguments:
  name        Name of the new migration

options:
  -h, --help  show this help message and exit
  --no-edit   Avoid opening the migration file in an editor after creation.
```

```sh
usage: migrateit migrate [-h] [--fake] [--update-hash] [--rollback] [name]

positional arguments:
  name           Name of the migration to run

options:
  -h, --help     show this help message and exit
  --fake         Fakes the migration marking it as ran.
  --update-hash  Update the hash of the migration.
  --rollback     Undo the given migration and all its applied childs.
```

```sh
usage: migrateit show [-h] [-l] [--validate-sql]

options:
  -h, --help      show this help message and exit
  -l, --list      Display migrations in a list format.
  --validate-sql  Validate SQL migration sintax.
```
