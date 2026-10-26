from __future__ import annotations

from typing import Any

from migrateit.clients._client import SqlClient
from migrateit.models.repository import ChangelogRepository
from migrateit.reporters import write_line


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

    client.export_database_schema(migration)
    ChangelogRepository.save(client.changelog)
    write_line(f"Full database exported to {migration.path}")
    return 0
