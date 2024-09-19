from typing import Any, Callable

from result import Err, Ok

from ..dicts import get
from .rules import RGC, Rule, RuleGroup


def validate(
    source: dict[str, Any] | list[Any],
    validation_map: dict[str, Callable | dict[str, Any] | list[Any]],
) -> Ok[dict | list] | Err[list[tuple]]:
    """
    Performs valiudation on the `source` dict. Enforces corresponding `Rule`s and `RuleGroup`s
      at a given key.

    NOTE: This _does_ mutate the corresponding `validation_map` (specifically adds info to the
      `_parent_key` field of Rule | RuleGroup), so it's _not_ a pure function.
    """
    res = _validate_recursive(source, validation_map, None)
    if isinstance(res, Ok):
        return Ok(source)
    else:
        return res


def _validate_recursive(
    source: dict[str, Any] | list[Any],
    validation_map: dict[str, Callable | dict[str, Any] | list[Any]],
    _parent_key: str | None = None,
) -> Ok[tuple] | Err[list[tuple]]:
    """
    Ok. Recurse through `source`. Set `_parent_key` at the start of the function
      The current key is `_key`, and `_parent_key` is the keys that came before the current call

      The `_parent_key` is saved for debugging, and also identifying the list case. That's it

    And as NOTE-ed, this mutates items in `validation_map` (just `_parent_key`)
    """
    # Try applying each rule at the given key
    failed_r_rg: list[tuple] = []
    for k, v in validation_map.items():
        # Run rules on source[k], return `Err` if get fails
        curr_source = get(source, k, default=None)
        match v:
            case Rule() | RuleGroup():
                # Execute rules, then append `Err` if it comes up
                v._parent_key = f"{_parent_key}.{k}" if _parent_key else k
                res = v(curr_source)
            case dict() | list():
                if curr_source is None:
                    raise RuntimeError(f"Failed to process key {k} from: {source}")
                # Do recursive call. Expect this to return Ok/Err
                if isinstance(v, list):
                    # TODO: this needs to pass a dict, make sure the call here is correct
                    v_dict = {k: v}
                    res = _validate_recursive(curr_source, v_dict, f"{k}[*]")  # type: ignore
                else:
                    res = _validate_recursive(curr_source, v, k)  # type: ignore
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

    return Ok((source, None, None))
