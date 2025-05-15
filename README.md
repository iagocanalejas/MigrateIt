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

```sh
# basic configuration
MIGRATIONS_TABLE=MIGRATEIT_CHANGELOG
MIGRATIONS_DIR=migrateit

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
# - database table in the configured database
# - 'migrations' directory inside the MIGRATIONS_DIR
# - 'changelog.json' file inside the MIGRATIONS_DIR
migrateit init

# create a new migration file
migrateit newmigration first_migration

# add your sql commands to the migration file
echo "CREATE TABLE test (id SERIAL PRIMARY KEY, name VARCHAR(50));" > migrateit/0001_first_migration.sql

# show pending migrations
migrateit showmigrations

# run the migrations
migrateit migrate

# run a given migration
migrateit migrate 0001

# fake a migration (will save the migration as applied without running it)
migrateit 0001 --fake
```
