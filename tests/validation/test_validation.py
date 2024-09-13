from datetime import datetime
from typing import Any, Optional

import pytest
from pydantic import BaseModel, ValidationError
from result import Err, Ok

from pydian.validation import RC, RuleGroup, validate
from pydian.validation.pydantic import create_pydantic_model
from pydian.validation.specific import InRange, IsOptional, IsRequired, IsType


# TODO: add tests for pydantic validation generation (expand RuleGroup, or keep unnested?)
# NEXT STEP:
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
        "data": IsRequired()
        & {
            "patient": IsRequired()
            & {
                "id": IsRequired() & str,
                "active": IsRequired() & bool,
                "birthdate": str,  # implicitly optional
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
        "patient": IsRequired()
        & {
            "id": IsRequired() & str,
            "active": IsRequired() & bool,
            "_some_new_key": str,  # implicitly optional
        }
    }

    assert v_pass_map == {
        "patient": RuleGroup(
            [
                IsRequired(),
                IsType(dict),
                RuleGroup(
                    [
                        IsType(str, constraint=RC.REQUIRED, at_key="id"),
                        IsType(bool, constraint=RC.REQUIRED, at_key="active"),
                        IsType(str, at_key="_some_new_key"),
                    ]
                ),
            ]
        )
    }

    # Add a level of nesting
    v_pass_map = {"data": IsRequired() & v_pass_map}

    assert v_pass_map == {
        "data": RuleGroup(
            [
                IsRequired(),
                IsType(dict),
                RuleGroup(
                    [
                        # Each key in dict is it's own separate RuleGroup (!)
                        RuleGroup(
                            [
                                IsRequired(),
                                IsType(dict),
                                RuleGroup(
                                    [
                                        IsType(str, constraint=RC.REQUIRED, at_key="id"),
                                        IsType(
                                            bool,
                                            constraint=RC.REQUIRED,
                                            at_key="active",
                                        ),
                                        IsType(str, at_key="_some_new_key"),
                                    ]
                                ),
                            ],
                            at_key="patient",
                        )
                    ]
                ),
            ]
        )
    }


def test_validate(simple_data: dict[str, Any]) -> None:
    # Example of pass
    v_pass_map = {
        "data": IsRequired()
        & {
            "patient": IsRequired()
            & {
                "id": IsRequired() & str,
                "active": IsRequired() & bool,
                "_some_new_key": str,  # implicitly optional
            }
        }
    }
    v_ok = validate(simple_data, v_pass_map)
    assert isinstance(v_ok, Ok)

    # Example of fail
    v_fail_map = {
        "data": IsRequired()
        & {
            "patient": IsRequired()
            & {
                "id": IsRequired() & str,
                "active": IsRequired() & bool,
                "_some_new_key": IsRequired() & str,  # changed to required!
            }
        }
    }
    v_err_missing_key = validate(simple_data, v_fail_map)
    assert isinstance(v_err_missing_key, Err)

    # Example of fail -- will still validate when present, and ignore if not present
    v_fail_map["data"] &= IsOptional()
    v_err_validate_when_present = validate(simple_data, v_fail_map)
    assert isinstance(v_err_validate_when_present, Err)

    # List validation -- example of pass
    # NOTE: If a field is not included in validation dict, then it's ignored
    #       E.g. here, we ignore the `simple_data["data"]` field by omission
    list_check_dict = {"list_data": IsRequired() & InRange(3, 5) & [dict]}
    v_ok_list = validate(simple_data, list_check_dict)
    assert isinstance(v_ok_list, Ok)

    # List validation -- some failure cases
    list_check_dict_fail_range = {"list_data": IsRequired() & InRange(1, 2) & [dict]}
    v_err_list_range = validate(simple_data, list_check_dict_fail_range)
    assert isinstance(v_err_list_range, Err)

    list_check_dict_fail_item_type = {"list_data": IsRequired() & InRange(1, 2) & [str]}
    v_err_list_item_type = validate(simple_data, list_check_dict_fail_item_type)
    assert isinstance(v_err_list_item_type, Err)

    # List validation -- make it more discrete
    # TODO: interesting semantic q, should `InRange` measure passing objects?
    #       Thinking no, but good to think abt later (e.g. pipelines)
    list_check_dict_discrete = {
        "list_data": IsRequired()
        & InRange(3, 10)
        & [v_pass_map["data"]]  # This is the `RuleGroup` for a patient object
    }
    v_ok_list_discrete = validate(simple_data, list_check_dict_discrete)
    assert isinstance(v_ok_list_discrete, Ok)
