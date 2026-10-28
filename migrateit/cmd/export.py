from typing import Any

from migrateit.clients._client import SqlClient
from migrateit.reporters import write_line
from migrateit.reporters.output import write_line_error


def cmd_export(client: SqlClient[Any], name: str | None) -> int:
    if not name:
        name = f"{client.database.value}_backup"

    write_line(f"Saving full database to migration '{name}'.")
    migration = client.migrations.create_new_migration(
        name=name,
        dependencies=(client.changelog.root.name,),
        isolated=True,
    )
    write_line(f"Migration created in {migration.path}")

    try:
        client.export_database_schema(migration)
        write_line(f"Full database exported to {migration.path}")
    except Exception as e:
        write_line_error(f"Failed to export database: {e}")
        migration.path.unlink()
        return 1
    return 0
