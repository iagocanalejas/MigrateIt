How does this work

TODO: check how to load db configurations for now harcoded
TODO: work on changelog.json


```sh
# run
python migrateit showmigrations
# show executed and pending migrations

python migrateit newmigration
```
```sh
# run
python migrateit migrate

# migrateit will read your 'db_src' path (./db/migrations/) and load all sql files
# migrateit will then check the database for the already executed migrations
# migrateit will execute the migrations that are not already executed
```
