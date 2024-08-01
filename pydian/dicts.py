import traceback
from typing import Any, Callable, Iterable, Sequence

from .globs import SharedMapperState, _Global_Mapper_State_Dict
from .lib.types import DROP, KEEP, ApplyFunc, ConditionalCheck
from .lib.util import default_dsl, encode_stack_trace, flatten_list


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
    # Grab context from `Mapper` classes (if relevant)
    mapper_state = _get_global_mapper_config()
    # For `strict`, prefer Mapper setting or take local setting
    strict = (mapper_state.strict if mapper_state else None) or strict

    if source:
        res = _nested_get(source, key, default)
        if strict:
            _enforce_strict(res, strict, key, source)
    else:
        res = default

    if flatten and isinstance(res, list):
        res = flatten_list(res)

    if res is not None and only_if:
        res = res if only_if(res) else None
        if strict:
            _enforce_strict(res, strict, key, source)

    if res is not None and apply:
        if not isinstance(apply, Iterable):
            apply = (apply,)
        for fn in apply:
            try:
                res = fn(res)
                if strict:
                    _enforce_strict(res, strict, key, source)
            except Exception as e:
                raise RuntimeError(f"`apply` call {fn} failed for value: {res} at key: {key}, {e}")
            if res is None:
                break

    if drop_level and res is None:
        res = drop_level

    return res


def _enforce_strict(
    res: Any, strict: bool | None, key: str, source: dict[str, Any] | list[Any]
) -> None:
    if strict and res is None:
        # Check if value is deliberately `None`, otherwise return error
        tokenized_keypath = _get_tokenized_keypath(key)
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


def _get_global_mapper_config() -> SharedMapperState | None:
    curr_trace = traceback.format_stack()
    # Iterate through all mappers, and check stack trace with key str
    for m_id, sms in _Global_Mapper_State_Dict.items():
        if len(curr_trace) <= sms._trace_len:
            continue
        if m_id == encode_stack_trace(curr_trace[: sms._trace_len]):
            return sms
    return None


def _nested_get(
    source: dict[str, Any] | list[Any],
    key: str | Any,
    default: Any = None,
    dsl_fn: Callable[[dict[str, Any] | list[Any], Any], Any] = default_dsl,
) -> Any:
    """
    Expects `.`-delimited string and tries to get the item in the dict.

    If using pydian defaults, the following benefits apply:
    - Tuple support

    If you use a custom `dsl_fn`, then logic is entrusted to that function (wgpcgr).
    """
    # Assume `key: str`. If not, then trust the custom `dsl_fn` to handle it
    # Handle tuple syntax (if they ask for a tuple, return a tuple)
    if isinstance(key, str) and ("(" in key and ")" in key):
        key = key.replace("(", "[").replace(")", "]")
        res = dsl_fn(source, key)
        if isinstance(res, list):
            res = tuple(res)
    else:
        res = dsl_fn(source, key)

    # DSL-independent cleanup
    if isinstance(res, list):
        res = [r if r is not None else default for r in res]
    if res is None:
        res = default

    return res


def _nested_set(
    source: dict[str, Any], tokenized_key_list: Sequence[str | int], target: Any
) -> dict[str, Any] | None:
    """
    Returns a copy of source with the replace if successful, else None.
    """
    res: Any = source
    try:
        for k in tokenized_key_list[:-1]:
            res = res[k]
        res[tokenized_key_list[-1]] = target
    except IndexError:
        return None
    return source


def _get_tokenized_keypath(key: str) -> tuple[str | int, ...]:
    """
    Returns a keypath with str and ints separated. Prefer tuples so it is hashable.

    E.g.: "a[0].b[-1].c" -> ("a", 0, "b", -1, "c")
    """
    tokenized_key = key.replace("[", ".").replace("]", "")
    keypath = tokenized_key.split(".")
    return tuple(int(k) if k.removeprefix("-").isnumeric() else k for k in keypath)


def drop_keys(source: dict[str, Any], keys_to_drop: Iterable[str]) -> dict[str, Any]:
    """
    Returns the dictionary with the requested keys set to `None`.

    If a key is a duplicate, then lookup fails so that key is skipped.

    DROP values are checked and handled here.
    """
    res = source
    seen_keys = set()
    for key in keys_to_drop:
        curr_keypath = _get_tokenized_keypath(key)
        if curr_keypath not in seen_keys:
            if v := _nested_get(res, key):
                # Check if value has a DROP object
                if isinstance(v, DROP):
                    # If "out of bounds", raise an error
                    if v.value > 0 or -1 * v.value > len(curr_keypath):
                        raise RuntimeError(f"Error: DROP level {v} at {key} is invalid")
                    curr_keypath = curr_keypath[: v.value]
                    # Handle case for dropping entire object
                    if len(curr_keypath) == 0:
                        return dict()
                if updated := _nested_set(res, curr_keypath, None):
                    res = updated
                seen_keys.add(curr_keypath)
        else:
            seen_keys.add(curr_keypath)
    return res


def impute_enum_values(source: dict[str, Any], keys_to_impute: set[str]) -> dict[str, Any]:
    """
    Returns the dictionary with the Enum values set to their corresponding `.value`
    """
    res = source
    for key in keys_to_impute:
        curr_val = _nested_get(res, key)
        if isinstance(curr_val, KEEP):
            literal_val = curr_val.value
            res = _nested_set(res, _get_tokenized_keypath(key), literal_val)  # type: ignore
    return res
