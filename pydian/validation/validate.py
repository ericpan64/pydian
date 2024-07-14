from typing import Any

from pydantic import create_model
from result import Err, Ok

from ..dicts import get
from .rules import Rule, RuleGroup


def validate(
    source: dict[str, Any], validation_map: dict[str, Rule | RuleGroup]
) -> Ok[dict] | Err[RuleGroup]:
    """
    Performs valiudation on the `source` dict. Enforces corresponding `Rule`s and `RuleGroup`s
      at a given key
    """

    # Try applying each rule at the given key
    failed_r_rg = []
    for k, r_rg in validation_map.items():
        # Add key information when available
        if r_rg._key:
            k = f"{k}.{r_rg._key}"
        # Run rules on source[k]
        # NOTE: need to handle nested keys somehow
        #   A `RuleSet` key applies to all child items
        #   A `Rule` key is terminal, and applies to current object
        curr_level = get(source, k)
        res = r_rg(curr_level)
        if isinstance(res, Err):
            failed_r_rg.append(r_rg)

    if failed_r_rg:
        return Err(RuleGroup(failed_r_rg))

    return Ok(source)


# TODO: Implement this and add to pydantic test

# def to_pydantic(validation_map: dict[str, Rule | RuleGroup]) -> dict[str, Any]:
#     """
#     Convert a validation_map to a Pydantic model.

#     OK. Try the following algo:
#     1. First-pass of validation_map:
#         - Parse-out structural schema
#         - Parse-out custom Rule/RuleGroup requirements
#     2. Use pydantic `create_model` to create custom class at runtime
#         - Validate `source` against custom class
#     3. Run additional rules from first-pass
#         - Each key information should be saved accordingly
#     """
#     return create_model(...)
