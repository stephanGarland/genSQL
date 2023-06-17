from argparse import Namespace
import pytest
from unittest.mock import Mock, patch

from exceptions.exceptions import SchemaValidationError
from gensql import runner


@pytest.fixture
def runner_object():
    mock_args = Mock(spec=Namespace)
    mock_args.num = 1000
    mock_schema = {
        "user_id": {
            "type": "bigint unsigned",
            "nullable": "false",
            "auto_increment": "true",
            "primary_key": "true",
        },
        "full_name": {"type": "varchar", "width": "255", "nullable": "false"},
        "external_id": {
            "type": "bigint unsigned",
            "nullable": "false",
            "unique": "true",
            "default": "0",
        },
        "last_modified": {"type": "timestamp", "nullable": "false", "default": "now()"},
    }
    mock_tbl_name = "test"
    mock_tbl_cols = {
        "full_name": {"type": "varchar", "width": "255", "nullable": "false"},
        "external_id": {
            "type": "bigint unsigned",
            "nullable": "false",
            "unique": "true",
            "default": "0",
        },
        "last_modified": {"type": "timestamp", "nullable": "false", "default": "now()"},
    }
    mock_tbl_create = (
        "CREATE TABLE `test` (\n  `user_id` bigint unsigned NOT NULL AUTO_INCREMENT,\n "
        "`first_name` varchar (255) NOT NULL,\n  `last_name` varchar (255) NULL,\n "
        "`email` varchar (255) NULL,\n  `city` varchar (255) NULL,\n  `country` varchar (255) NULL,\n "
        "`created_at` timestamp NOT NULL DEFAULT NOW(),\n  `last_updated_at` timestamp NULL DEFAULT NULL "
        "ON UPDATE NOW(),\n  PRIMARY KEY (`user_id`),\n  UNIQUE KEY email (`email`)\n) ENGINE=InnoDB "
        "AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
    )
    mock_unique_cols = []
    r = runner.Runner(
        mock_args,
        mock_schema,
        mock_tbl_name,
        mock_tbl_cols,
        mock_tbl_create,
        mock_unique_cols,
    )
    return r


@patch("gensql.runner.Runner.sample")
def test_runner(runner_object):
    r = runner_object
    mock_full_name = Mock(spec="Garland, Stephan")
    mock_external_id = Mock(spec=42)
    mock_last_modified = Mock(spec="1995-05-23 01:23:45")
    # with patch.object(r, "sample") as mock_sample:
    # mock_sample.side_effect = ["1995-05-23 01:23:45", 42, "Garland, Stephan"]
    # result = r.make_row(1, True)
    side_effect = ["1995-05-23 01:23:45", 42, "Garland, Stephan"]
    result = r.make_row(side_effect=side_effect)
    assert result == None
