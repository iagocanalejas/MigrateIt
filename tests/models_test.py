import unittest

from migrateit.models import ChangelogFile, Migration


class TestChangelogModel(unittest.TestCase):
    def setUp(self):
        self.m1 = Migration(name="0001_initial", initial=True)
        self.m2 = Migration(name="0002_add_users", parents=["0001_initial"])
        self.m3 = Migration(name="0003_add_orders", parents=["0002_add_users"])
        self.m4 = Migration(name="0004_add_payments", parents=["0002_add_users"])
        self.changelog = ChangelogFile(version=1, migrations=[self.m1, self.m2, self.m3, self.m4])

    def test_bottom_up_migration_by_name_exact(self):
        result = self.changelog.get_migration_by_name("0002_add_users")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.name, "0002_add_users")

    def test_bottom_up_migration_by_name_prefix_only(self):
        result = self.changelog.get_migration_by_name("0003")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.name, "0003_add_orders")

    def test_bottom_up_migration_by_name_with_path(self):
        result = self.changelog.get_migration_by_name("/some/dir/0001_initial.py")
        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.name, "0001_initial")

    def test_build_migration_plan_leaf(self):
        plan = self.changelog.build_migration_plan(self.m3)
        self.assertEqual(plan, [self.m1, self.m2, self.m3])

        plan = self.changelog.build_migration_plan(self.m4)
        self.assertEqual(plan, [self.m1, self.m2, self.m4])

    def test_build_migration_plan_root(self):
        plan = self.changelog.build_migration_plan(self.m1)
        self.assertEqual(plan, [self.m1])

    def test_build_migration_plan_with_missing_parent(self):
        m5 = Migration(name="0005_broken", parents=["999"])
        changelog = ChangelogFile(version=1, migrations=[self.m1, self.m2, self.m3, self.m4, m5])
        plan = changelog.build_migration_plan(m5)
        self.assertEqual(plan, [m5])

    def test_top_down_plan_from_root(self):
        plan = self.changelog.build_migration_plan()
        self.assertEqual(plan, [self.m1, self.m2, self.m3, self.m4])
