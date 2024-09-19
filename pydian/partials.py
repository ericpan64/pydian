from collections.abc import Callable, Container, Iterable, Reversible
from functools import partial
from itertools import islice
from typing import Any, Type, TypeVar

from result import Err, Ok

import pydian
from pydian.lib.types import DROP, ApplyFunc, ConditionalCheck

"""
`pydian` Wrappers
"""


def get(
    key: str,
    default: Any = None,
    apply: ApplyFunc | Iterable[ApplyFunc] | None = None,
    only_if: ConditionalCheck | None = None,
    drop_level: DROP | None = None,
    flatten: bool | None = None,
) -> ApplyFunc:
    """
    Partial wrapper around the Pydian `get` function
    """
    kwargs = {
        "key": key,
        "default": default,
        "apply": apply,
        "only_if": only_if,
        "drop_level": drop_level,
        "flatten": flatten,
    }
    return partial(pydian.get, **kwargs)


def pipe(*funcs: ApplyFunc) -> ApplyFunc:
    """
    Custom wrapper that applies the functions in-order and returns a result
    """

    # TODO: Make this result type, and also be ergonomic for the user!
    #       Think through with pipeline module (i.e. graceful failure case)
    def run_pipe(val: Any) -> Any:
        for func in funcs:
            val = func(val)
        return val

    return partial(run_pipe)  # TODO: I don't think I need the `partial` here, test to confirm


"""
Generic Wrappers
"""


def do(func: Callable, *args: Any, **kwargs: Any) -> ApplyFunc:
    """
    Generic partial wrapper for functions.

    Starts at the second parameter when using *args (as opposed to the first).
    """
    return lambda x: func(x, *args, **kwargs)


def echo(v: Any) -> ApplyFunc:
    """
    Function that returns the value exactly as-is
    """
    return lambda _: v


def length(n: int) -> ApplyFunc:
    return lambda v: len(v) == n


def add(value: Any, before: bool = False) -> ApplyFunc:
    if before:
        return lambda v: value + v
    return lambda v: v + value


def subtract(value: Any, before: bool = False) -> ApplyFunc:
    if before:
        return lambda v: value - v
    return lambda v: v - value


def multiply(value: Any, before: bool = False) -> ApplyFunc:
    if before:
        return lambda v: value * v
    return lambda v: v * value


def divide(value: Any, before: bool = False) -> ApplyFunc:
    if before:
        return lambda v: value / v
    return lambda v: v / value


T = TypeVar("T", list[Any], tuple[Any])


def keep(n: int) -> ApplyFunc | Callable[[T], T]:
    return lambda it: it[:n]


def index(idx: int) -> ApplyFunc | Callable[[Reversible], Any]:
    def get_index(obj: Reversible, i: int) -> Any:
        if i >= 0:
            it = iter(obj)
        else:
            i = (i + 1) * -1
            it = reversed(obj)
        return next(islice(it, i, i + 1), None)

    return partial(get_index, i=idx)


def equals(value: Any) -> ConditionalCheck:
    # if type(value) == pl.DataFrame:
    #     return lambda df: df.equals(value)
    return lambda v: v == value


def gt(value: Any) -> ConditionalCheck:
    return lambda v: v > value


def lt(value: Any) -> ConditionalCheck:
    return lambda v: v < value


def gte(value: Any) -> ConditionalCheck:
    return lambda v: v >= value


def lte(value: Any) -> ConditionalCheck:
    return lambda v: v <= value


def equivalent(value: Any) -> ConditionalCheck:
    return lambda v: v is value


def contains(value: Any) -> ConditionalCheck:
    return lambda container: value in container


def contained_in(container: Container) -> ConditionalCheck:
    return lambda v: v in container


def not_equal(value: Any) -> ConditionalCheck:
    return lambda v: v != value


def not_equivalent(value: Any) -> ConditionalCheck:
    return lambda v: v is not value


def not_contains(value: Any) -> ConditionalCheck:
    return lambda container: value not in container


def not_contained_in(container: Container) -> ConditionalCheck:
    return lambda v: v not in container


def isinstance_of(type_: Type) -> ConditionalCheck:
    return lambda v: isinstance(v, type_)


"""
stdlib Wrappers
"""


def map_to_list(func: Callable) -> ApplyFunc | Callable[[Iterable], list[Any]]:
    """
    Partial wrapper for `map`, then casts to a list
    """
    _map_to_list: Callable = lambda fn, it: list(map(fn, it))
    return partial(_map_to_list, func)


def filter_to_list(func: Callable) -> ApplyFunc | Callable[[Iterable], list[Any]]:
    """
    Partial wrapper for `filter`, then casts to a list
    """
    _filter_to_list: Callable = lambda fn, it: list(filter(fn, it))
    return partial(_filter_to_list, func)
