from tests.clients.psql._base_test import BasePsqlTest


class TestPsqlClientInit(BasePsqlTest):
    def test_check_migrations_table_exist_false(self):
        self.assertFalse(self.client.is_migrations_table_created())

    def test_create_and_check_table(self):
        self.client.create_migrations_table()
        self.assertTrue(self.client.is_migrations_table_created())

    def test_create_table_twice_fails(self):
        self.client.create_migrations_table()
        with self.assertRaises(AssertionError):
            self.client.create_migrations_table()
