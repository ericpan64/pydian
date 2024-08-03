from typing import Any, Optional

from pydantic import BaseModel
from result import Err, Ok

from pydian.validation import RC, RGC, Rule, RuleGroup, validate
from pydian.validation.specific import InRange, IsRequired, IsType, NotRequired


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

    assert DataWrapper(**simple_data)


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
    v_res = validate(simple_data, v_pass_map)
    assert isinstance(v_res, Ok)

    # Example of fail
    v_pass_map = {
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
    v_err_missing_key = validate(simple_data, v_pass_map)
    assert isinstance(v_err_missing_key, Err)

    # Example of fail -- will still validate when present, and ignore if not present
    v_pass_map["data"] &= NotRequired()
    v_err_validate_when_present = validate(simple_data, v_pass_map)
    assert isinstance(v_err_validate_when_present, Err)

    # NEXT STEP: Add list validation
    ...
