from argparse import Namespace
import pytest
from unittest.mock import Mock

from exceptions.exceptions import SchemaValidationError
from gensql import validator


@pytest.fixture
def validator_object():
    mock_args = Mock(spec=Namespace)
    mock_args.num = 1000
    v = validator.Validator(mock_args)
    return v


def test_parse_schema(validator_object):
    v = validator_object
    v.args.input = "./schema_inputs/skeleton.json"
    parsed_schema = v.parse_schema()
    parsed_schema_ref = {
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
    assert parsed_schema == parsed_schema_ref


def test_validate_good_schema(validator_object):
    v = validator_object
    v.args.input = "./schema_inputs/skeleton.json"
    parsed_schema = v.parse_schema()
    assert v.validate_schema(parsed_schema) is True


def test_validate_bad_schema(validator_object):
    v = validator_object
    v.args.input = "./schema_inputs/broken.json"
    parsed_schema = v.parse_schema()
    with pytest.raises(
        SchemaValidationError, match="found errors validating schema - see above"
    ) as e:
        v.validate_schema(parsed_schema)
