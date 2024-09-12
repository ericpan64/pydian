from typing import Any

from result import Err, Ok

from ..dicts import get
from .rules import RGC, Rule, RuleGroup


def validate(
    source: dict[str, Any], validation_map: dict[str, Rule | RuleGroup]
) -> Ok[dict] | Err[RuleGroup | str]:
    """
    Performs valiudation on the `source` dict. Enforces corresponding `Rule`s and `RuleGroup`s
      at a given key
    """

    # Try applying each rule at the given key
    failed_r_rg = RuleGroup([], RGC.ALL_RULES)
    for k, r_rg in validation_map.items():
        # Run rules on source[k], return `Err` if get fails
        # Append key information when available
        if r_rg._key:
            k = f"{k}.{r_rg._key}"
        curr_level = get(source, k, default=Err(f"Failed to get key {k} from: {source}"))
        if isinstance(curr_level, Err):
            return curr_level
        # Execute rules, then append `Err` if it comes up
        res = r_rg(curr_level)
        if isinstance(res, Err):
            failed_r_rg.append(res.err_value)  # type: ignore

    if failed_r_rg:
        return Err(failed_r_rg)

    return Ok(source)
