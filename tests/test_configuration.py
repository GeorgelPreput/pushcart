from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pushcart.configuration import (
    Configuration,
    Destination,
    Source,
    Transformation,
    Validation,
    _get_multiple_validations_with_same_rule,
)


class TestValidation:
    @given(st.builds(Validation))
    def test_validation_happy_path(self, validation):
        """
        Tests that the validation_action field matches one of the three allowed values
        and that validation_rule is a string.
        """
        assert validation.validation_action in ["LOG", "DROP", "FAIL"]
        assert isinstance(validation.validation_rule, str)

    def test_validation_rule_string(self):
        """
        Tests that the validation_rule field is a string.
        """
        with pytest.raises(ValueError) as e:
            Validation(validation_rule=123, validation_action="LOG")
        assert "str type expected" in str(e.value)

    def test_validation_rule_min_length(self):
        """
        Tests that the validation_rule field has a minimum length of 1.
        """
        with pytest.raises(ValueError) as e:
            Validation(validation_rule="", validation_action="LOG")
        assert "ensure this value has at least 1 characters" in str(e.value)


class TestGetMultipleValidationsWithSameRule:
    def test_one_rule_multiple_validations(self):
        """
        Tests that the function groups validations correctly when the validations list contains only one rule with multiple validations.
        """
        validations = [
            {"validation_rule": "rule1", "validation_action": "action1"},
            {"validation_rule": "rule1", "validation_action": "action2"},
            {"validation_rule": "rule1", "validation_action": "action3"},
        ]
        expected_output = {"rule1": ["action1", "action2", "action3"]}
        assert _get_multiple_validations_with_same_rule(validations) == expected_output

    def test_different_rules(self):
        """
        Tests that the function groups validations correctly when the validations list contains validations with different rules.
        """
        validations = [
            {"validation_rule": "rule1", "validation_action": "action1"},
            {"validation_rule": "rule2", "validation_action": "action2"},
            {"validation_rule": "rule1", "validation_action": "action3"},
            {"validation_rule": "rule2", "validation_action": "action4"},
        ]
        expected_output = {
            "rule1": ["action1", "action3"],
            "rule2": ["action2", "action4"],
        }
        assert _get_multiple_validations_with_same_rule(validations) == expected_output

    def test_empty_list(self):
        """
        Tests that the function returns an empty dictionary when the validations list is empty.
        """
        validations = []
        expected_output = {}
        assert _get_multiple_validations_with_same_rule(validations) == expected_output

    def test_one_validation(self):
        """
        Tests that the function returns an empty dictionary when the validations list contains only one validation.
        """
        validations = [{"validation_rule": "rule1", "validation_action": "action1"}]
        result = _get_multiple_validations_with_same_rule(validations)
        assert result == {}

    def test_invalid_data_types(self):
        """
        Tests that the function handles invalid data types in the validations list.
        """
        validations = [
            {"validation_rule": "rule1", "validation_action": "action1"},
            "invalid_data",
        ]

        with pytest.raises(TypeError):
            _get_multiple_validations_with_same_rule(validations)

    def test_duplicate_validations(self):
        """
        Tests that the function handles duplicate validations in the validations list.
        """
        validations = [
            {"validation_rule": "rule1", "validation_action": "action1"},
            {"validation_rule": "rule2", "validation_action": "action2"},
            {"validation_rule": "rule1", "validation_action": "action3"},
            {"validation_rule": "rule2", "validation_action": "action4"},
            {"validation_rule": "rule3", "validation_action": "action5"},
            {"validation_rule": "rule4", "validation_action": "action6"},
            {"validation_rule": "rule4", "validation_action": "action7"},
            {"validation_rule": "rule4", "validation_action": "action8"},
        ]
        result = _get_multiple_validations_with_same_rule(validations)
        assert result == {
            "rule1": ["action1", "action3"],
            "rule2": ["action2", "action4"],
            "rule4": ["action6", "action7", "action8"],
        }


class TestSource:
    def test_create_source_with_valid_data(self):
        """
        Tests that a source object can be created with valid data, type, and into fields.
        """
        data = "./tests/data/example.csv"
        type = "csv"
        into = "my_view"
        source = Source(data=data, type=type, into=into)
        assert source.data == data
        assert source.type == type
        assert source.into == into

    def test_create_source_with_optional_params_and_validations(self):
        """
        Tests that a source object can be created with optional params and validations fields.
        """
        data = "./tests/data/example.csv"
        type = "csv"
        into = "my_view"
        params = {"delimiter": ","}
        validation_rule = "column_1 > 0"
        validation_action = "LOG"
        validations = [
            {"validation_rule": validation_rule, "validation_action": validation_action}
        ]
        source = Source(
            data=data, type=type, into=into, params=params, validations=validations
        )
        assert source.data == data
        assert source.type == type
        assert source.into == into
        assert source.params == params
        assert source.validations == [
            Validation(
                validation_rule=validation_rule, validation_action=validation_action
            )
        ]

    def test_create_source_with_invalid_data(self):
        """
        Tests that a source object cannot be created with empty strings in the data,
        type and into fields.
        """
        with pytest.raises(ValueError):
            Source(data="", type="csv", into="my_view")
        with pytest.raises(ValueError):
            Source(data="./tests/data/example.csv", type="", into="my_view")
        with pytest.raises(ValueError):
            Source(data="./tests/data/example.csv", type="csv", into="")

    def test_source_allows_optional_params_and_validations(self):
        """
        Test that the Source class allows for optional parameters and validations to be
        associated with the data source
        """
        source_with_params = Source(
            data="data/example.csv",
            type="csv",
            into="my_view",
            params={"delimiter": ","},
        )
        source_with_validations = Source(
            data="data/example.csv",
            type="csv",
            into="my_view",
            validations=[Validation(validation_rule="rule1", validation_action="LOG")],
        )

        assert source_with_params.params is not None
        assert source_with_validations.validations is not None


class TestTransformation:
    def test_transformation_with_config_and_validations(self):
        """
        Tests that a transformation with a config file and validations is correctly
        instantiated and validated.
        """
        transformation = Transformation(
            data="some data",
            into="some output",
            config="./tests/data/example.yaml",
            sql_query=None,
            validations=[
                {"validation_rule": "rule1", "validation_action": "LOG"},
                {"validation_rule": "rule2", "validation_action": "DROP"},
            ],
        )
        assert transformation.data == "some data"
        assert transformation.into == "some output"
        assert transformation.config == Path("./tests/data/example.yaml")
        assert transformation.validations[0].validation_rule == "rule1"
        assert transformation.validations[0].validation_action == "LOG"
        assert transformation.validations[1].validation_rule == "rule2"
        assert transformation.validations[1].validation_action == "DROP"

    def test_transformation_without_validations(self):
        """
        Tests that a transformation without validations is correctly instantiated and
        validated.
        """
        transformation = Transformation(
            data="some data",
            into="some output",
            sql_query="SELECT * FROM table",
        )
        assert transformation.data == "some data"
        assert transformation.into == "some output"
        assert transformation.sql_query == "SELECT * FROM table"
        assert transformation.validations == []

    def test_transformation_with_empty_fields(self):
        """
        Tests that a transformation with empty data or into fields is correctly
        validated.
        """
        with pytest.raises(ValueError):
            Transformation(
                data="",
                into="my_output_view",
                sql_query="SELECT * FROM table",
            )
        with pytest.raises(ValueError):
            Transformation(
                data="my_input_view",
                into="",
                sql_query="SELECT * FROM table",
            )

    def test_transformation_with_both_config_and_sql_query_defined(self):
        """
        Tests that a transformation with both config and sql query fields defined is
        correctly validated.
        """
        with pytest.raises(ValueError):
            Transformation(
                data="my_input_view",
                into="my_output_view",
                config="./tests/data/example.yaml",
                sql_query="SELECT * FROM table",
            )

    def test_transformation_with_no_config_or_sql_query_defined(self):
        """
        Tests that a transformation with neither config nor sql query fields defined is
        correctly validated.
        """
        with pytest.raises(ValueError):
            Transformation(data="test", into="test")

    def test_multiple_validations_with_same_rule(self):
        """
        Tests that multiple validation objects with the same validation rule are
        correctly handled.
        """
        validation1 = Validation(validation_rule="rule1", validation_action="LOG")
        validation2 = Validation(validation_rule="rule1", validation_action="DROP")

        with pytest.raises(ValueError) as e:
            Transformation(
                data="my_input_view",
                into="my_output_view",
                config="./tests/data/example.yaml",
                validations=[validation1, validation2],
            )
        assert "Different actions for the same validation" in str(e.value)


class TestDestination:
    def test_valid_input_data(self):
        """
        Tests that the destination class can be instantiated with valid input data.
        """
        dest = Destination(
            data="my_data",
            into="my_table",
            path="/path/to/destination",
            mode="upsert",
            keys=["key1", "key2"],
            sequence_by="seq_col",
            validations=[
                {"validation_rule": "col1 > 0", "validation_action": "LOG"},
                {"validation_rule": "col2 < 100", "validation_action": "DROP"},
            ],
        )
        assert dest.data == "my_data"
        assert dest.into == "my_table"
        assert dest.path == "/path/to/destination"
        assert dest.mode == "upsert"
        assert dest.keys == ["key1", "key2"]
        assert dest.sequence_by == "seq_col"
        assert len(dest.validations) == 2

    def test_missing_required_fields(self):
        """
        Tests that the destination class raises a validation error when required fields
        are missing.
        """
        with pytest.raises(TypeError):
            Destination(data="my_data", mode="append")
        with pytest.raises(ValueError):
            Destination(data="my_data", mode="append", into=None)

    def test_invalid_mode_value(self):
        """
        Tests that the destination class raises a ValueError when an invalid mode value
        is provided.
        """
        with pytest.raises(ValueError):
            Destination(
                data="my_data",
                into="my_table",
                path="/path/to/destination",
                mode="invalid_mode",
            )

    def test_missing_keys_or_sequence_by_for_upsert(self):
        """
        Tests that the destination class raises a ValueError when mode is upsert but
        keys or sequence_by fields are missing.
        """
        with pytest.raises(KeyError):
            Destination(
                data="test_data",
                into="test_into",
                path="test_path",
                mode="upsert",
            )

    def test_invalid_input_data_for_fields_with_constraints(self):
        """
        Tests that the destination class raises a ValueError when invalid input data is
        provided for fields with constraints.
        """
        with pytest.raises(ValueError):
            Destination(
                data="",
                into="test_into",
                path="test_path",
                mode="append",
                keys=["test_key"],
                sequence_by="test_sequence",
            )

    def test_multiple_validations_with_same_rule(self):
        """
        Tests that the destination class raises a ValueError when there are multiple
        validations with the same rule.
        """
        with pytest.raises(ValueError):
            Destination(
                data="test_data",
                into="test_into",
                path="test_path",
                mode="append",
                keys=["test_key"],
                sequence_by="test_sequence",
                validations=[
                    Validation(validation_rule="rule1", validation_action="LOG"),
                    Validation(validation_rule="rule1", validation_action="DROP"),
                ],
            )


class TestConfiguration:
    def test_all_fields_defined(self):
        """
        Tests that a configuration object can be created with all three fields defined.
        """
        config_dict = {
            "sources": [{"data": "path/to/data", "type": "csv", "into": "temp_table"}],
            "transformations": [
                {
                    "data": "temp_table",
                    "into": "output_table",
                    "sql_query": "SELECT * FROM temp_table",
                }
            ],
            "destinations": [
                {"data": "output_table", "into": "delta_table", "mode": "append"}
            ],
        }
        config = Configuration(**config_dict)

        assert config.sources == [
            Source(
                data="path/to/data",
                type="csv",
                into="temp_table",
            )
        ]
        assert config.transformations == [
            Transformation(
                data="temp_table",
                into="output_table",
                sql_query="SELECT * FROM temp_table",
            )
        ]
        assert config.destinations == [
            Destination(data="output_table", into="delta_table", mode="append")
        ]

    def test_invalid_stage_object(self):
        """
        Tests that a valueerror is raised when an invalid stage object is provided to the configuration class.
        """
        with pytest.raises(ValueError):
            invalid_source = {
                "data": "path/to/data",
                "type": "csv",
                "into": "temp_table",
            }
            Configuration(**{"sources": invalid_source})

    def test_empty_configuration_object(self):
        """
        Tests that an empty configuration object cannot be created.
        """
        with pytest.raises(ValueError) as e:
            Configuration()

        assert "No stage definition found" in str(e.value)
