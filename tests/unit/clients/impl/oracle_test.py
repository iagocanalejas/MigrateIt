from unittest.mock import MagicMock

import pytest

from migrateit.clients.oracle import OracleClient
from migrateit.models.changelog import Changelog
from migrateit.models.config import MigrateItConfig, SupportedDatabase


def _make_mock_client() -> OracleClient:
    mock_conn: MagicMock = MagicMock()
    changelog = Changelog(version=1, database=SupportedDatabase.ORACLE, migrations=[])
    config = MigrateItConfig(changelog=changelog)
    return OracleClient(mock_conn, config)


@pytest.mark.unit
def test_execute() -> None:
    client = _make_mock_client()
    with pytest.raises(NotImplementedError):
        client.execute("CREATE OR REPLACE FUNCTION my_func() RETURNS INT AS $$ $$")
