from collections.abc import Iterable
from typing import Any, Sequence

from ..lib.types import DROP, ApplyFunc, ConditionalCheck
from ..lib.util import flatten_sequence
from .mapper import _MapperContextStrict
from .util import _nested_get, get_tokenized_keypath


def get(
    source: dict[str, Any] | list[Any],
    key: str,
    default: Any = None,
    apply: ApplyFunc | Iterable[ApplyFunc] | None = None,
    only_if: ConditionalCheck | None = None,
    drop_level: DROP | None = None,
    flatten: bool = False,
    strict: bool | None = None,
) -> Any:
    """
    Gets a value from the source dictionary using a `.` syntax.
    Handles None-checking (instead of raising error, returns default).

    `key` notes:
     - Use `.` to chain gets
     - Index and slice into lists, e.g. `[0]`, `[-1]`, `[:1]`, etc.
     - Iterate through a list using `[*]`
     - Get multiple items using `(firstKey,secondKey)` syntax (outputs as a tuple)
       The keys within the tuple can also be chained with `.`

    Optional param notes:
    - `default`: Return value if `key` results in a `None` (before other params apply)
    - `apply`: Use to safely chain operations on a successful get
    - `only_if`: Use to conditionally decide if the result should be kept + `apply`-ed.
    - `drop_level`: Use to specify conditional dropping if get results in None.
    - `flatten`: Use to flatten the final result (e.g. nested lists)
    - `strict`: Use to throw `ValueError` instead of returning `None` (also available at `Mapper`-level)
    """
    # Check if within the context manager
    strict = strict or _MapperContextStrict.get()

    if source:
        res = _nested_get(source, key, default)
        if strict:
            _enforce_strict(res, key, source)
    else:
        res = default

    if flatten and isinstance(res, Sequence):
        res = type(res)(flatten_sequence(res))  # type: ignore

    if res is not None and only_if:
        res = res if only_if(res) else None
        if strict:
            _enforce_strict(res, key, source)

    if res is not None and apply:
        if not isinstance(apply, Iterable):
            apply = (apply,)
        for fn in apply:
            try:
                res = fn(res)
                if strict:
                    _enforce_strict(res, key, source)
            except Exception as e:
                raise RuntimeError(f"`apply` call {fn} failed for value: {res} at key: {key}, {e}")
            if res is None:
                break

    if drop_level and res is None:
        res = drop_level

    return res


def _enforce_strict(res: Any, key: str, source: dict[str, Any] | list[Any]) -> None:
    # At this point, we'll check if `res` is None based on a missed `get`
    #  UNLESS the case where the value is deliberately `None` (we check for that below)
    if res is None:
        # Check for case where value is deliberately `None`, otherwise return error
        tokenized_keypath = get_tokenized_keypath(key)
        nested_val: Any = source
        MISSING_VAL_INDICATOR = "__NOTFOUND__"
        for k in tokenized_keypath:
            if nested_val == MISSING_VAL_INDICATOR:
                break
            match k:
                case "*":
                    # TODO: handle list unwraps - here we'll just stop checking
                    nested_val = MISSING_VAL_INDICATOR
                case _:
                    nested_val = (
                        nested_val[k]
                        if (isinstance(k, int) or k in nested_val)
                        else MISSING_VAL_INDICATOR
                    )
        if nested_val is not None:
            raise ValueError(f"_Strict mode_: invalid key: {key}")
