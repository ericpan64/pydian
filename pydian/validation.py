from typing import Any

from pydantic import create_model
from result import Err, Ok

from .dicts import get
from .rules import Rule, RuleSet


def validate(
    source: dict[str, Any], validation_map: dict[str, Rule | RuleSet]
) -> Ok[dict] | Err[RuleSet]:
    """
    Performs valiudation on the `source` dict. Enforces corresponding `Rule`s and `RuleSet`s
      at a given key
    """

    # Try applying each rule at the given key
    failed_rules = []
    for k, r_rs in validation_map.items():
        # Add key information when available
        if r_rs._key:
            k = f"{k}.{r_rs._key}"
        # Run rules on source[k]
        res = r_rs(get(source, k))
        if isinstance(res, Err):
            failed_rules.append(r_rs)

    if failed_rules:
        return Err(RuleSet(failed_rules))

    return Ok(source)


# TODO: Implement this and add to pydantic test

# def to_pydantic(validation_map: dict[str, Rule | RuleSet]) -> dict[str, Any]:
#     """
#     Convert a validation_map to a Pydantic model.

#     OK. Try the following algo:
#     1. First-pass of validation_map:
#         - Parse-out structural schema
#         - Parse-out custom Rule/RuleSet requirements
#     2. Use pydantic `create_model` to create custom class at runtime
#         - Validate `source` against custom class
#     3. Run additional rules from first-pass
#         - Each key information should be saved accordingly
#     """
#     return create_model(...)
