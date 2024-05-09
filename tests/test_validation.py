from typing import Any, Optional

from pydantic import BaseModel
from result import Err, Ok

from pydian.rules import (
    InRange,
    IsRequired,
    NotRequired,
    Rule,
    RuleSet,
    RuleSetConstraint,
)
from pydian.validation import validate


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


def test_validate(simple_data: dict[str, Any]) -> None:
    # Example of pass
    # v_pass = {
    #     "data": IsRequired()
    #     & {
    #         "patient": IsRequired()
    #         & {
    #             "id": IsRequired() & str,
    #             "active": IsRequired() & bool,
    #             "_some_new_key": str,  # implicitly optional
    #         }
    #     }
    # }
    v_pass = {
        "patient": IsRequired()
        & {
            "id": IsRequired() & str,
            "active": IsRequired() & bool,
            "_some_new_key": str,  # implicitly optional
        }
    }
    assert isinstance(validate(simple_data["data"], v_pass), Ok)

    # Example of fail
    # TODO: This is failing due to RuleSet eval, fix with other tests!
    v_second = {
        "data": IsRequired()
        & {
            "patient": IsRequired()
            & {
                "id": IsRequired() & str,
                "active": IsRequired() & bool,
                "_some_new_key": str & IsRequired(),  # _Changed to required_
            }
        }
    }
    assert isinstance(validate(simple_data, v_second), Err)

    # Example of fail -- will still validate when present, and ignore if not present
    v_second["data"] &= NotRequired()
    assert isinstance(validate(simple_data, v_second), Err)
    assert isinstance(validate(dict(), v_second), Ok)


# NOTE: this will implicitly get tested with above, since `validate` will drive key usage
# def test_ruleset_at_key(simple_data: dict[str, Any]) -> None:

#     is_str_key = Rule(lambda x: isinstance(x, str), at_key="id")
#     is_str_nested_key = Rule(lambda x: isinstance(x, str), at_key="patient.id")

#     # Test `Rule`.at_key
#     rs_default_key = RuleSet({is_str_key, is_str_nested_key})

#     # Expect two exceptions (missing outer key)
#     assert rs_default_key(simple_data) == Err({is_str_key, is_str_nested_key})

#     # Expect two passes (since nesting is now in-place correctly)
#     rs_default_key._key = "data"
#     assert rs_default_key(simple_data) == Ok({is_str_key, is_str_nested_key})


#     # # Test generation of `RuleSet` with `at_key` set for each `Rule`
#     # rs_map = {
#     #     "data": IsRequired() & {
#     #         "patient": IsRequired() & {
#     #             "id": is_str_key,
#     #             "active": IsRequired() & bool,
#     #         }
#     #     }
#     # }

#     # assert rs_map["data"] == RuleSet({
#     #     IsRequired(),
#     #     IsRequired(at_key)
#     # })
