from copy import deepcopy
from datetime import datetime
from typing import Any, Optional

import pytest
from pydantic import BaseModel, ValidationError
from result import Err, Ok

import pydian.partials as p
from pydian.validation import RGC, Rule, RuleGroup, validate
from pydian.validation.pydantic import create_pydantic_model
from pydian.validation.specific import InRange, IsOptional, IsRequired, IsType


def test_pydantic(simple_data: dict[str, Any]) -> None:
    """
    A pydantic implementation: ensure interop and consistent behavior!
    """

    class PatientData(BaseModel):
        id: str
        active: bool
        birthdate: Optional[str] = None  # Need to provide a default to be optional

    class PatientWrapper(BaseModel):
        patient: PatientData

    class DataWrapper(BaseModel):
        data: PatientWrapper
        # `list_data` field is ignored when present (which is what we want)

    invalid_data = {"a": "b"}
    invalid_data_field_missing = {"data": {"patient": {"id": "abc"}}}
    invalid_data_type_mismatch = {"data": {"patient": {"id": 123, "active": True}}}
    invalid_data_type_mismatch_optional = {
        "data": {"patient": {"id": "123", "active": True, "birthdate": datetime(2024, 9, 12)}}
    }

    def _test_invalid_cases(model: type) -> None:
        with pytest.raises(ValidationError):
            model(**invalid_data)
        with pytest.raises(ValidationError):
            model(**invalid_data_field_missing)
        with pytest.raises(ValidationError):
            model(**invalid_data_type_mismatch)
        with pytest.raises(ValidationError):
            model(**invalid_data_type_mismatch_optional)

    assert DataWrapper(**simple_data)
    _test_invalid_cases(DataWrapper)

    # Generate the same model with `create_model`
    v_pass_map = {
        "data": {
            "patient": {
                "id": str,
                "active": bool,
                "birthdate": str & IsOptional(),
            }
        }
    }
    # NOTE: as-is, this will just validate the `data` layer (the nested classes aren't created)
    DataWrapperDyn = create_pydantic_model("DataWrapperDyn", v_pass_map)
    DataWrapperDyn(**simple_data)
    assert DataWrapperDyn(**simple_data)
    _test_invalid_cases(DataWrapperDyn)


def test_validation_map_gen() -> None:
    v_pass_map = {
        "patient": {
            "id": str,
            "active": bool,
            "birthdate": str & IsOptional(),
        }
    }

    assert v_pass_map == {
        "patient": {
            "id": str,
            "active": bool,
            "birthdate": RuleGroup([IsType(str), IsOptional()], RGC.AT_LEAST_ONE),
        }
    }

    # Add a level of nesting, and cast dict to RuleGroup
    v_pass_map_updated = {"data": IsRequired() & v_pass_map}

    assert v_pass_map_updated == {
        "data": RuleGroup(
            [
                IsRequired(),
                IsType(dict),
                RuleGroup(
                    [
                        IsType(dict, at_key="patient"),
                        RuleGroup(
                            [
                                IsType(str, at_key="id"),
                                IsType(bool, at_key="active"),
                                RuleGroup(
                                    [IsType(str), IsOptional()],
                                    RGC.AT_LEAST_ONE,
                                    at_key="birthdate",
                                ),
                            ],
                            RGC.ALL,
                            at_key="patient",
                        ),
                    ],
                    RGC.ALL,
                ),
            ],
            RGC.ALL,
        )
    }

    v_pass_list = {"list_data": InRange(3, 5) & [dict]}  # Conditions in [] are OR-ed by default

    assert v_pass_list == {
        "list_data": [
            InRange(3, 5),
            IsType(list),
            RuleGroup([IsType(dict, at_key="[*]")], RGC.AT_LEAST_ONE),
        ]
    }

    v_pass_list_multi = {"list_data": InRange(2, 10) & [dict, p.not_equivalent("abc")]}

    assert v_pass_list_multi == {
        "list_data": [
            InRange(2, 10),
            IsType(list),
            RuleGroup([IsType(dict), Rule(p.not_equivalent("abc"))], RGC.AT_LEAST_ONE),
        ]
    }

    list_check_dict_discrete = {
        "list_data": InRange(3, 10)
        & [v_pass_map_updated["data"]]  # checking against this data schema
    }

    expected_v_pass_map_data_validator = deepcopy(v_pass_map_updated["data"])

    assert list_check_dict_discrete == {
        "list_data": RuleGroup(
            [InRange(3, 10), IsType(list), expected_v_pass_map_data_validator], RGC.ALL
        )
    }


def test_validate(simple_data: dict[str, Any]) -> None:
    # Example of pass
    v_pass_map = {
        "data": {
            "patient": {
                "id": str,
                "active": bool,
                "_some_new_key": str & IsOptional(),
            }
        }
    }
    v_ok = validate(simple_data, v_pass_map)
    assert isinstance(v_ok, Ok)

    # Example of fail
    v_fail_map = deepcopy(v_pass_map)
    v_fail_map["data"]["patient"]["_some_new_key"] &= IsRequired()  # Change field to required
    v_err_missing_key = validate(simple_data, v_fail_map)
    assert isinstance(v_err_missing_key, Err)

    # Example of fail -- will still validate when present, and ignore if not present
    v_fail_map["data"] &= IsOptional()
    v_err_validate_when_present = validate(simple_data, v_fail_map)
    assert isinstance(v_err_validate_when_present, Err)

    # List validation -- example of pass
    # NOTE: If a field is not included in validation dict, then it's ignored
    #       E.g. here, we ignore the `simple_data["data"]` field by omission
    list_check_dict = {"list_data": InRange(3, 5) & [dict]}
    v_ok_list = validate(simple_data, list_check_dict)
    assert isinstance(v_ok_list, Ok)

    # List validation -- some failure cases
    list_check_dict_fail_range = {"list_data": InRange(1, 2) & [dict]}
    v_err_list_range = validate(simple_data, list_check_dict_fail_range)
    assert isinstance(v_err_list_range, Err)

    list_check_dict_fail_item_type = {"list_data": InRange(1, 2) & [str]}
    v_err_list_item_type = validate(simple_data, list_check_dict_fail_item_type)
    assert isinstance(v_err_list_item_type, Err)

    # List validation -- make it more discrete
    # TODO: interesting semantic q, should `InRange` measure passing objects?
    #       Thinking no, but good to think abt later (e.g. pipelines)
    list_check_dict_discrete = {
        "list_data": InRange(3, 10) & [v_pass_map["data"]]  # checking against this data schema
    }
    v_ok_list_discrete = validate(simple_data, list_check_dict_discrete)
    assert isinstance(v_ok_list_discrete, Ok)


def test_validation_readme_examples() -> None:
    is_str = IsType(str)
    assert is_str("Abc") == Ok((is_str, "Abc", True))
    assert is_str(123) == Err((is_str, 123, False))

    is_nonempty = lambda d: len(d) > 0  # `InRange(0)` also works here
    is_nonempty_str = is_str & is_nonempty  # Creates a `RuleGroup`
    assert isinstance(is_nonempty_str("Abc"), Ok)
    assert isinstance(is_nonempty_str(""), Err)

    valid_dict = {"a": {"b": is_nonempty_str & IsOptional()}}
    assert isinstance(validate({"a": {"b": "Abc"}}, valid_dict), Ok)
    assert isinstance(validate({"a": {"c": 123}}, valid_dict), Ok)
    assert isinstance(validate({"a": {"b": ""}}, valid_dict), Err)
