from unittest.mock import patch

from tests.clients.psql._base_test import BasePsqlTest

from migrateit.clients import PsqlClient
from migrateit.models import Migration, MigrationsFile, MigrationStatus


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

        mock_get_content_hash.return_value = ("dummy_path", "hash1")
        self._insert_migration_row("001_init.sql", "hash1")

        changelog = MigrationsFile(version=1, migrations=[migration_applied, migration_not_applied])

        result = self.client.retrieve_migrations(changelog)

        expected = [(migration_applied, MigrationStatus.APPLIED), (migration_not_applied, MigrationStatus.NOT_APPLIED)]
        self.assertEqual(result, expected)

    @patch.object(PsqlClient, "_get_content_hash")
    def test_show_migrations_conflict_and_removed(self, mock_get_content_hash):
        mock_get_content_hash.return_value = ("dummy_path", "expected_hash")

        self._insert_migration_row("001_init.sql", "different_hash")  # mismatch
        self._insert_migration_row("ghost.sql", "ghost_hash")

        changelog = MigrationsFile(version=1, migrations=[Migration(name="001_init.sql")])

        # Act
        result = self.client.retrieve_migrations(changelog)

        # Assert
        self.assertEqual(result[0][1], MigrationStatus.CONFLICT)
        self.assertEqual(result[1][1], MigrationStatus.REMOVED)

    @patch.object(PsqlClient, "_get_content_hash")
    def test_show_migrations_order_error(self, mock_get_content_hash):
        mock_get_content_hash.side_effect = [
            ("dummy_path", "hash2"),  # for 002_second.sql
            ("dummy_path", "hash1"),  # for 001_second.sql
        ]
        self._insert_migration_row("002_second.sql", "hash2")
        self._insert_migration_row("001_first.sql", "hash1")
        changelog = MigrationsFile(
            version=1,
            migrations=[
                Migration(name="001_first.sql"),
                Migration(name="002_second.sql"),
            ],
        )

        # Act & Assert
        with self.assertRaises(ValueError) as cm:
            self.client.retrieve_migrations(changelog)
        self.assertIn("not in the same order", str(cm.exception))

    def test_show_migrations_not_applied_in_middle_error(self):
        self._insert_migration_row("001_first.sql", "hash1")
        self._insert_migration_row("002_second.sql", "hash2")
        changelog = MigrationsFile(
            version=1,
            migrations=[
                Migration(name="001_first.sql"),
                Migration(name="003_third.sql"),
                Migration(name="002_second.sql"),
            ],
        )

        with patch.object(
            PsqlClient,
            "_get_content_hash",
            side_effect=[
                ("dummy_path", "hash1"),  # match for 001
                ("dummy_path", "hashX"),  # fake for 003
                ("dummy_path", "hash2"),  # fake for 002
            ],
        ):
            # Act & Assert
            with self.assertRaises(ValueError) as cm:
                self.client.retrieve_migrations(changelog)
            self.assertIn("NOT_APPLIED migrations must be at the end", str(cm.exception))
