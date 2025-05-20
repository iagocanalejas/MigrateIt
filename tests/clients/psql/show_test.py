from unittest.mock import patch

from tests.clients.psql._base_test import BasePsqlTest

from migrateit.clients import PsqlClient
from migrateit.models import ChangelogFile, Migration, MigrationStatus


class TestPsqlClientShowMigrations(BasePsqlTest):
    def setUp(self):
        super().setUp()
        self.client.create_migrations_table()

    def _insert_migration_row(self, name, hash_value):
        with self.connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {self.TEST_MIGRATIONS_TABLE} (migration_name, change_hash) VALUES (%s, %s)",
                (name, hash_value),
            )
        self.connection.commit()

    @patch.object(PsqlClient, "_get_content_hash")
    def test_show_migrations_applied_and_not_applied(self, mock_get_content_hash):
        migration_applied = Migration(name="001_init.sql")
        migration_not_applied = Migration(name="002_more.sql")

        mock_get_content_hash.return_value = ("dummy_content", "dummy_reverse_content", "hash1")
        self._insert_migration_row("001_init.sql", "hash1")

        changelog = ChangelogFile(version=1, migrations=[migration_applied, migration_not_applied])
        self.client.config.changelog = changelog

        result = self.client.retrieve_migration_statuses()

        expected = {
            migration_applied.name: MigrationStatus.APPLIED,
            migration_not_applied.name: MigrationStatus.NOT_APPLIED,
        }
        self.assertEqual(result, expected)

    @patch.object(PsqlClient, "_get_content_hash")
    def test_show_migrations_conflict_and_removed(self, mock_get_content_hash):
        mock_get_content_hash.return_value = ("dummy_content", "dummy_reverse_content", "expected_hash")

        self._insert_migration_row("001_init.sql", "different_hash")  # mismatch
        self._insert_migration_row("ghost.sql", "ghost_hash")

        changelog = ChangelogFile(version=1, migrations=[Migration(name="001_init.sql")])
        self.client.config.changelog = changelog

        result = self.client.retrieve_migration_statuses()

        self.assertEqual(result["001_init.sql"], MigrationStatus.CONFLICT)
        self.assertEqual(result["ghost.sql"], MigrationStatus.REMOVED)

    @patch.object(PsqlClient, "_get_content_hash")
    def test_show_migrations_order_error(self, mock_get_content_hash):
        mock_get_content_hash.side_effect = [
            ("dummy_content", "dummy_reverse_content", "hash2"),  # for 002_second.sql
            ("dummy_content", "dummy_reverse_content", "hash1"),  # for 001_second.sql
        ]
        self._insert_migration_row("002_second.sql", "hash2")
        changelog = ChangelogFile(
            version=1,
            migrations=[
                Migration(name="001_first.sql", initial=True, parents=[]),
                Migration(name="002_second.sql", parents=["001_first.sql"]),
            ],
        )
        self.client.config.changelog = changelog

        statuses = self.client.retrieve_migration_statuses()
        with self.assertRaises(ValueError) as cm:
            self.client.validate_migrations(statuses)
        self.assertIn("is applied before", str(cm.exception))
