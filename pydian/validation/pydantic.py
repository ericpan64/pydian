from typing import Any, Callable

from pydantic import BaseModel, Field, ValidationError, create_model, field_validator
from result import Err

from .rules import Rule, RuleGroup
from .specific import IsType


def create_pydantic_model(model_name: str, v_map: dict[str, Any]) -> type[BaseModel]:
    field_definitions: dict[str, Any] = {}
    field_validators: dict[str, classmethod] = {}

    # Add type checks for the model and index validators
    for k, r_rg in v_map.items():
        rules: Rule | RuleGroup | tuple[Rule] = (
            r_rg if isinstance(r_rg, RuleGroup) else Rule.init_specific(r_rg)
        )
        if isinstance(rules, Rule):
            rules = (rules,)
        typ = next((r._type for r in rules if isinstance(r, IsType)), Any)
        field_definitions[k] = (typ, Field(description=f"dyn generated from found `IsType`"))

        non_type_rules = [rule for rule in rules if not isinstance(rule, IsType)]
        if non_type_rules:
            dyn_field_validator = _as_classmethod(non_type_rules)
            field_validators[f"{k}_validator"] = field_validator(k)(dyn_field_validator)  # type: ignore

    model = create_model(model_name, **field_definitions, __validators__=field_validators)  # type: ignore
    return model


def _as_classmethod(rl: list[Rule | RuleGroup]) -> Callable:
    """
    Takes the given list, and wraps it as a class function that just runs the list

    This needs to exist so we have the `cls` parameter as a member function,
        and to raise a pydantic `ValidationError` to actually work correctly
    """
    return lambda cls, d: _check_err(cls, rl, d) or d  # type: ignore


def _check_err(cls, rl: list[Rule | RuleGroup], rl_input: Any) -> None:
    """
    For the given rules, tries to run them. If an `Err` is returned, raise an error
    """
    for r in rl:
        res = r(rl_input)
        if isinstance(res, Err):
            errors = [
                {
                    "loc": (
                        f" ... for the dynamically-generated {cls}",
                    ),  # Use a tuple for location
                    "msg": f"Error for {res} on dynamic class {cls}",
                    "type": "value_error",
                    "input": rl_input,
                    "ctx": {"error": str(res)},  # Add context with 'error' key
                }
            ]
            raise ValidationError.from_exception_data("Dynamic class validation error", errors)  # type: ignore
    return None
