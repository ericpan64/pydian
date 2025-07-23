from collections.abc import Callable, Container, Iterable, Reversible
from functools import partial
from itertools import islice
from operator import add as op_add, sub as op_sub, mul as op_mul, truediv as op_truediv
from operator import eq, gt as op_gt, lt as op_lt, ge, le, ne, contains
from typing import Any, Type, TypeVar

from result import Err, Ok

import pydian
from pydian.lib.types import DROP, ApplyFunc, ConditionalCheck

"""
`pydian` Wrappers
"""


def _flip(func: Callable[[Any, Any], Any]) -> Callable[[Any, Any], Any]:
    """Flips the order of arguments for a binary function"""
    return lambda a, b: func(b, a)


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


# TODO: Add `select` partial (1 param). Design choice on whether to support compound types


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


class _Echo:
    """Callable that always returns a fixed value"""
    def __init__(self, value: Any):
        self.value = value
    
    def __call__(self, _: Any) -> Any:
        return self.value


def do(func: Callable, *args: Any, **kwargs: Any) -> ApplyFunc:
    """
    Generic partial wrapper for functions.

    Starts at the second parameter when using *args (as opposed to the first).
    """
    # Need to use lambda here because partial would bind the first parameter
    return lambda x: func(x, *args, **kwargs)


def echo(v: Any) -> ApplyFunc:
    """
    Function that returns the value exactly as-is
    """
    return _Echo(v)


def length(n: int) -> ApplyFunc:
    return partial(lambda val, num: len(val) == num, num=n)


def add(value: Any, before: bool = False) -> ApplyFunc:
    if before:
        return partial(op_add, value)
    return partial(_flip(op_add), value)


def subtract(value: Any, before: bool = False) -> ApplyFunc:
    if before:
        return partial(op_sub, value)
    return partial(_flip(op_sub), value)


def multiply(value: Any, before: bool = False) -> ApplyFunc:
    if before:
        return partial(op_mul, value)
    return partial(_flip(op_mul), value)


def divide(value: Any, before: bool = False) -> ApplyFunc:
    if before:
        return partial(op_truediv, value)
    return partial(_flip(op_truediv), value)


T = TypeVar("T", list[Any], tuple[Any])


def keep(n: int) -> ApplyFunc | Callable[[T], T]:
    return partial(lambda num, it: it[:num], num=n)


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
    return partial(eq, value)


def gt(value: Any) -> ConditionalCheck:
    return partial(_flip(op_gt), value)


def lt(value: Any) -> ConditionalCheck:
    return partial(_flip(op_lt), value)


def gte(value: Any) -> ConditionalCheck:
    return partial(_flip(ge), value)


def lte(value: Any) -> ConditionalCheck:
    return partial(_flip(le), value)


def equivalent(value: Any) -> ConditionalCheck:
    return partial(lambda val, v: v is val, val=value)


def contains(value: Any) -> ConditionalCheck:
    return partial(contains, value)


def contained_in(container: Container) -> ConditionalCheck:
    return partial(lambda cont, v: v in cont, cont=container)


def not_equal(value: Any) -> ConditionalCheck:
    return partial(ne, value)


def not_equivalent(value: Any) -> ConditionalCheck:
    return partial(lambda val, v: v is not val, val=value)


def not_contains(value: Any) -> ConditionalCheck:
    return partial(lambda val, container: val not in container, val=value)


def not_contained_in(container: Container) -> ConditionalCheck:
    return partial(lambda cont, v: v not in cont, cont=container)


def isinstance_of(type_: Type) -> ConditionalCheck:
    return partial(lambda t, v: isinstance(v, t), t=type_)


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
