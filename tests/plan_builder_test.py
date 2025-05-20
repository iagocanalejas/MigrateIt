import shutil
import tempfile
import unittest
from collections import OrderedDict
from pathlib import Path

from migrateit.models.changelog import ChangelogFile
from migrateit.models.migration import Migration, MigrationStatus
from migrateit.tree import build_migration_plan


class TestBuildMigrationPlan(unittest.TestCase):
    def setUp(self):
        self.m1 = Migration(name="0001_init.sql", initial=True, parents=[])
        self.m2 = Migration(name="0002_add_users.sql", parents=["0001_init.sql"])
        self.m3 = Migration(name="0003_add_orders.sql", parents=["0001_init.sql"])
        self.m4 = Migration(name="0004_add_queries.sql", parents=["0002_add_users.sql", "0003_add_orders.sql"])
        self.m5 = Migration(name="0005_add_rows.sql", parents=["0004_add_queries.sql"])

        self.temp_dir = Path(tempfile.mkdtemp())
        self.migrations = [self.m1, self.m2, self.m3, self.m4, self.m5]
        self.changelog = ChangelogFile(version=1, migrations=self.migrations, path=self.temp_dir / "changelog.json")

        self.migration_tree = OrderedDict(
            {
                "0001_init.sql": [self.m2, self.m3],
                "0002_add_users.sql": [self.m4],
                "0003_add_orders.sql": [self.m4],
                "0004_add_queries.sql": [self.m5],
                "0005_add_rows.sql": [],
            }
        )

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)

    def test_plan_applies_unapplied_migrations(self):
        statuses = {
            "0001_init.sql": MigrationStatus.APPLIED,
            "0002_add_users.sql": MigrationStatus.NOT_APPLIED,
            "0003_add_orders.sql": MigrationStatus.NOT_APPLIED,
            "0004_add_queries.sql": MigrationStatus.NOT_APPLIED,
            "0005_add_rows.sql": MigrationStatus.NOT_APPLIED,
        }

        plan = build_migration_plan(self.changelog, self.migration_tree, statuses)
        self.assertEqual(
            [m.name for m in plan],
            ["0002_add_users.sql", "0003_add_orders.sql", "0004_add_queries.sql", "0005_add_rows.sql"],
        )

    def test_plan_all_applied_returns_empty(self):
        statuses = {
            "0001_init.sql": MigrationStatus.APPLIED,
            "0002_add_users.sql": MigrationStatus.APPLIED,
            "0003_add_orders.sql": MigrationStatus.APPLIED,
            "0004_add_queries.sql": MigrationStatus.APPLIED,
            "0005_add_rows.sql": MigrationStatus.APPLIED,
        }

        plan = build_migration_plan(self.changelog, self.migration_tree, statuses)
        self.assertEqual(plan, [])

    def test_bottom_up_plan(self):
        statuses = {
            "0001_init.sql": MigrationStatus.NOT_APPLIED,
            "0002_add_users.sql": MigrationStatus.NOT_APPLIED,
            "0003_add_orders.sql": MigrationStatus.NOT_APPLIED,
            "0004_add_queries.sql": MigrationStatus.NOT_APPLIED,
            "0005_add_rows.sql": MigrationStatus.NOT_APPLIED,
        }

        plan = build_migration_plan(
            self.changelog,
            self.migration_tree,
            statuses,
            target_migration=self.m4,
            is_rollback=False,
        )

        self.assertEqual(
            [m.name for m in plan],
            ["0001_init.sql", "0002_add_users.sql", "0003_add_orders.sql", "0004_add_queries.sql"],
        )

    def test_rollback_plan(self):
        statuses = {
            "0001_init.sql": MigrationStatus.APPLIED,
            "0002_add_users.sql": MigrationStatus.APPLIED,
            "0003_add_orders.sql": MigrationStatus.NOT_APPLIED,
            "0004_add_queries.sql": MigrationStatus.APPLIED,
            "0005_add_rows.sql": MigrationStatus.APPLIED,
        }

        plan = build_migration_plan(
            self.changelog,
            self.migration_tree,
            statuses,
            target_migration=self.m2,
            is_rollback=True,
        )

        self.assertEqual([m.name for m in plan], ["0005_add_rows.sql", "0004_add_queries.sql", "0002_add_users.sql"])

    def test_rollback_skips_unapplied(self):
        statuses = {
            "0001_init.sql": MigrationStatus.APPLIED,
            "0002_add_users.sql": MigrationStatus.NOT_APPLIED,
            "0003_add_orders.sql": MigrationStatus.NOT_APPLIED,
            "0004_add_queries.sql": MigrationStatus.NOT_APPLIED,
            "0005_add_rows.sql": MigrationStatus.NOT_APPLIED,
        }

        plan = build_migration_plan(
            self.changelog,
            self.migration_tree,
            statuses,
            target_migration=self.m1,
            is_rollback=True,
        )

        self.assertEqual([m.name for m in plan], ["0001_init.sql"])

    def test_raises_if_target_missing_in_rollback(self):
        statuses = {
            "0001_init.sql": MigrationStatus.APPLIED,
        }

        with self.assertRaises(AssertionError):
            build_migration_plan(
                self.changelog,
                self.migration_tree,
                statuses,
                target_migration=None,
                is_rollback=True,
            )
