from typing import Any, Callable, Mapping

from result import Err, Ok

from ..dicts import get
from .rules import Rule, RuleGroup


def validate(
    source: dict[str, Any],
    validation_map: Mapping[str, Callable | dict[str, Any] | list[Any]],
) -> Ok[dict | list] | Err[list[tuple]]:
    """
    Performs valiudation on the `source` dict. Enforces corresponding `Rule`s and `RuleGroup`s
      at a given key.

    NOTE: This _does_ mutate the corresponding `validation_map` (specifically adds info to the
      `_iter_over` field of Rule | RuleGroup), so it's _not_ a pure function.
    """
    # Try applying each rule at the given key
    failed_r_rg: list[tuple] = []
    for k, v in validation_map.items():
        # Run rules on source[k], return `Err` if get fails
        curr_source = get(source, k, default=None)
        match v:
            case Rule() | RuleGroup():
                res = v(curr_source)
            case dict() | list():
                if curr_source is None:
                    raise RuntimeError(f"Failed to process key {k} from: {source}")
                # Do recursive call. Expect this to return Ok/Err
                if isinstance(v, list):
                    # TODO: this needs to pass a dict, make sure the call here is correct
                    v_dict = {k: v}
                    res = validate(curr_source, v_dict)  # type: ignore
                else:
                    res = validate(curr_source, v)  # type: ignore
            case _:
                if callable(v):
                    # Wrap in a `Rule` so it returns Ok/Err
                    res = Rule.init_specific(v)(curr_source)
                else:
                    raise TypeError(f"Expected `v` to be Callable or nested dict, got: {type(v)}")

        if isinstance(res, Err):
            err_tup: tuple = res.err_value
            failed_r_rg.append(err_tup)

    if failed_r_rg:
        return Err(failed_r_rg)

    return Ok(source)
