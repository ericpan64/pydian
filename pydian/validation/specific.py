from copy import deepcopy
from typing import Any, Callable

import pydian.partials as p

from .rules import RC, RGC, Rule, RuleGroup


class IsRequired(Rule):
    """
    A rule where the current field is required

    For `RuleGroup`: keep default contraint
    """

    def __init__(self, at_key: str | None = None):
        # For each rule, make it required
        super().__init__(p.not_equivalent(None), RC.REQUIRED, at_key=at_key)

    def __and__(self, other: Rule | RuleGroup | Any, swap_order: bool = False) -> Rule | RuleGroup:
        """
        Returns a `RuleGroup` with the `REQUIRED` constraint applied to the single rule
        """
        match other:
            case Rule():
                res = deepcopy(other)
                res._constraint = RC.REQUIRED
            case _:
                # Check callable case here (cast into a `Rule`)
                if not isinstance(other, RuleGroup) and callable(other):
                    res = Rule.init_specific(other, RC.REQUIRED)  # type: ignore
                else:
                    if swap_order:
                        res = super().__rand__(other)
                    else:
                        res = super().__and__(other)
        return res

    def __rand__(self, other):
        return self.__and__(other, swap_order=True)


class IsOptional(Rule):
    """
    Optional: can be `None`, but validate if it's there.

    When combined with another rule, allows the case where the field is `None`.

    NOTE: This doesn't make sense to run on its own (unless you want a `None` check)
    """

    def __init__(self, at_key: str | None = None):
        # Initialize with dummy placeholder rule
        super().__init__(p.equivalent(None), at_key=at_key)

    def __and__(
        self, other: Rule | RuleGroup | Callable | type | Any, swap_order: bool = False
    ) -> Rule | RuleGroup:
        """
        Wraps in a `RuleGroup` where only 1 thing needs to pass (includes optional `None` check)
          This makes sure that condition is still run when data is present
        """
        # Wrap in a `RuleGroup` that has `None` equivalence as passing condition
        if callable(other):
            if swap_order:
                items = [Rule.init_specific(other), self]
            else:
                items = [self, Rule.init_specific(other)]
            res = RuleGroup(items, RGC.AT_LEAST_ONE)
        else:
            # Use `OR` here since we want `AT_LEAST_ONE` condition
            # ... this only happens for `IsOptional`... since it's special...
            res = super().__or__(other)
        return res

    def __rand__(self, other: Rule | RuleGroup | Any):
        return self.__and__(other, True)


class InRange(Rule):
    def __init__(
        self, lower: int | None = None, upper: int | None = None, at_key: str | None = None
    ):
        """
        Used to check if an list is within a size range, e.g.
            [
                str
            ] & InRange(3, 5)
          is a list of 3 to 5 `str` values

        """
        match (lower, upper):
            case (int(), None):
                fn = lambda l: len(l) >= lower
            case (None, int()):
                fn = lambda l: len(l) <= upper
            case (int(), int()):
                fn = lambda l: lower <= len(l) <= upper
            case (None, None):
                raise ValueError("Need to specify lower and/or upper bound: none received!")
        super().__init__(fn, at_key=at_key)


class MaxCount(Rule):
    def __init__(
        self,
        upper: int,
        constraint: RC | None = None,
        at_key: str | None = None,
    ):
        super().__init__(p.pipe(len, p.lte(upper)), constraint, at_key)


class MinCount(Rule):
    def __init__(
        self,
        lower: int,
        constraint: RC | None = None,
        at_key: str | None = None,
    ):
        super().__init__(p.pipe(len, p.gte(lower)), constraint, at_key)


class IsType(Rule):
    _type: type | None = None  # Store this for pydantic conversion

    def __init__(
        self,
        typ: type,
        constraint: RC | None = None,
        at_key: str | None = None,
    ):
        self._type = typ
        super().__init__(p.isinstance_of(typ), constraint, at_key)


class InSet(Rule):
    """IDEA: have this be the enum variant. E.g. one of these literals"""

    def __init__(self, s: set[Any]):
        super().__init__(p.contained_in(s))
