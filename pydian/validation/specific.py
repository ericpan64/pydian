from copy import deepcopy
from typing import Any

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

    def __and__(self, other: Rule | RuleGroup | Any) -> Rule | RuleGroup:
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
                    res = Rule.init_specific(other, RC.REQUIRED)
                else:
                    res = super().__and__(other)
        return res

    def __rand__(self, other):
        return self.__and__(other)


class NotRequired(Rule):
    """
    When combined with another rule, removes the Required constraint.

    Adds a dummy rule that doesn't do anything explicitly, though might loosen RuleGroup constraints
      (e.g. `AT_LEAST_ONE` will always pass since we're adding one rule that always passes)
    """

    def __init__(self, at_key: str | None = None):
        # Initialize with dummy placeholder rule
        super().__init__(lambda _: True, at_key=at_key)

    def __and__(self, other: Rule | RuleGroup | Any) -> Rule | RuleGroup:
        """
        For a `Rule`: remove `REQUIRED`
        For a `RuleGroup`: set to `ALL_WHEN_DATA_PRESENT` -- this means it's optional, but validate if-present
        """
        match other:
            case Rule():
                res = deepcopy(other)
                if res._constraint is RC.REQUIRED:
                    res._constraint = None
            case RuleGroup():
                # Loops through and removes the `IsRequired` rule
                res = deepcopy(other)  # type: ignore
                try:
                    res.remove(IsRequired())  # type: ignore
                except ValueError:
                    pass
            case _:
                res = super().__and__(other)
        return res

    def __rand__(self, other: Rule | RuleGroup | Any):
        return self.__and__(other)


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
    def __init__(
        self,
        typ: type,
        constraint: RC | None = None,
        at_key: str | None = None,
    ):
        super().__init__(p.isinstance_of(typ), constraint, at_key)


class InSet(Rule):
    """IDEA: have this be the enum variant. E.g. one of these literals"""

    def __init__(self, s: set[Any]):
        super().__init__(p.contained_in(s))
