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
MIGRATEIT_DATABASE=postgresql
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
```

# HELP

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
