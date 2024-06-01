from __future__ import (  # Used to recursively type annotate (e.g. `Rule` in `class Rule`)
    annotations,
)

import inspect
from collections.abc import Callable, Collection, Iterable
from copy import deepcopy
from enum import Enum
from typing import Any

from result import Err, Ok

import pydian.partials as p


class RuleConstraint(Enum):
    """
    A constraint for a single `Rule` (`RuleGroup` is responsible for application)

    Optional by default (i.e. no value specified)
    """

    REQUIRED = 1
    # ONLY_IF = lambda fn: (RuleConstraint.REQUIRED, fn)  # Usage: RuleConstraint.ONLY_IF(some_fn)


class RuleGroupConstraint(Enum):
    """
    Constraints on top of current `RuleGroup`

    Required by default (i.e. `ALL_OF`)

    Groups of >3 explicitlyrequired are discouraged!"""

    ALL_OF = -1
    OPTIONAL = 0
    ONE_OF = 1
    TWO_OF = 2
    THREE_OF = 3


class Rule:
    """
    A `Rule` is a callable that either returns:
    - Ok(truthy_result)
    - Err(str + falsy_result)

    A `Rule` can have additional constraints. These will ONLY be applied if contained within a `RuleGroup`

    `Rule`s can be combined to create `RuleGroup`s using: `&`, `|`
    """

    def _raise_undefined_rule_err():  # type: ignore
        raise ValueError("Rule was not defined!")

    _fn: Callable = lambda _: Rule._raise_undefined_rule_err()
    _constraints: set[RuleConstraint] = None  # type: ignore
    _key: str | None = None

    def __init__(
        self,
        fn: Callable,
        constraints: RuleConstraint | set[RuleConstraint] | None = None,
        at_key: str | None = None,
    ):
        self._fn = fn
        self._key = at_key  # Managed by `RuleGroup`
        self._constraints = set()
        match constraints:
            case RuleConstraint():
                self._constraints.add(constraints)
            case Collection():
                for c in constraints:
                    self._constraints.add(c)

    def __call__(self, *args) -> Ok[Any] | Err[str]:
        """
        Call specified rule

        NOTE: the key nesting needs to be enforced at the `RuleGroup` level,
            since an individual Rule doesn't have enough information available to it
        """
        try:
            res = self._fn(*args)
            if res:
                return Ok(res)
        except Exception as e:
            return Err(f"Rule {self} failed with exception: {e}")
        return Err(f"Rule {self} failed, got falsy value: {res}")

    def __repr__(self) -> str:
        # NOTE: can only grab source for saved files, not in repl
        #  So the function needs to be saved on a file
        #  See: https://stackoverflow.com/a/335159
        try:
            return f"{self._fn}:`{inspect.getsource(self._fn)}`"
        except:
            return f"{self._fn} (Rule)"

    def __hash__(self):
        return hash((self._fn, frozenset(self._constraints), self._key))

    def __eq__(self, other: Rule | Any):
        if isinstance(other, Rule):
            # Rules are the same based on the code and the applied constraints
            # TODO: I _think_ this will work, though need to test more thoroughly
            #   e.g. this won't work for built-in callables like `str`, `bool`
            try:
                return other._fn.__code__.co_code == self._fn.__code__.co_code
            except:
                return other._fn == self._fn
        return NotImplemented

    @staticmethod
    def init_specific(
        v: Any, constraint: RuleConstraint | None = None, at_key: str | None = None
    ) -> Rule:
        """
        Generically returns a more specific rule when possible
        """
        # TODO: think about other cases + abstractions?
        if isinstance(v, type):
            res = IsType(v, at_key)  # type: ignore
        else:
            res = Rule(v, constraint, at_key)  # type: ignore
        return res

    @staticmethod
    def combine(
        rule: Rule,
        other: Rule | RuleGroup | Any,
        set_constraint: RuleGroupConstraint = RuleGroupConstraint.ALL_OF,
    ) -> RuleGroup:
        """
        Combines a `Rule` with another value. By default, `RuleGroupConstraint.ALL_OF` is used,
          so all contained optional `Rule`s become REQUIRED by default.

        The new RuleGroup will contain the original Rule + extras based on the cases below.

        Here are the expected cases:
        1. & Rule | RuleGroup -> Add it
        2. & type -> Add type check
        3. & dict -> Add 1) type check, 2) contents of dict
        4. & list -> Add 1) type check, 2) contents of list
        5. & Callable -> Add the callable as a Rule
        6. & some primitive -> Add an equality check for the primitive
        """
        res = RuleGroup(rule, set_constraint)
        match other:
            case Rule() | RuleGroup():
                res.append(other)
            case type():
                res.append(IsType(other))
            case dict():
                # Add the typecheck
                res.append(IsType(dict))
                # For each item in dict, save key information and add to res
                drs = _dict_to_rulegroup(other)
                res.append(drs)
            case list():
                # Add the typecheck
                res.append(IsType(list))
                # For each item in list, save key information and add to res
                lrs = _list_to_rulegroup(other)
                res.append(lrs)
            case _:
                if callable(other):
                    res.append(Rule.init_specific(other))
                else:
                    # Exact value check
                    res.append(Rule(p.equals(other)))
        return res

    def __and__(self, other: Rule | RuleGroup | Any):
        return Rule.combine(self, other)

    def __rand__(self, other: Rule | RuleGroup | Any):
        # Operation intended to be commutative
        return self.__and__(other)

    def __or__(self, other: Rule | RuleGroup | Any):
        return Rule.combine(self, other, RuleGroupConstraint.ONE_OF)

    def __ror__(self, other: Rule | RuleGroup | Any):
        # Operation intended to be commutative
        return self.__or__(other)


class RuleGroup(list):
    """
    A `RuleGroup` is a callable list that can contain:
    - One or many `Rule`s
    - Other `RuleGroup`s (ending in a terminal `RuleGroup` of just `Rule`s at some point)

    When called, a `RuleGroup` evaluates all contained `Rule`/`RuleGroups` and combines the result into:
    - Ok([rules_passed, ...])
    - Err([rules_failed, ...])

    A constraint defines whether the result is `Ok` or `Err` based on contained `Rule`/`RuleGroups`.
      Additionally, individual `Rule`s may have constraints which the `RuleGroup` manages
    """

    _set_constraint: RuleGroupConstraint = RuleGroupConstraint.ALL_OF  # default
    _n_rules: int = 0
    # NOTE: _key is needed here to save nesting information
    #   e.g. a user-specified `RuleGroup` shouldn't need to specify `_key` for each rule,
    #   rather we should infer that during parsing
    _key: str | None = None

    def __init__(
        self,
        items: Rule | RuleGroup | Callable | Collection[Rule | RuleGroup | Callable] | None = None,
        constraint: RuleGroupConstraint = RuleGroupConstraint.ALL_OF,
        at_key: str | None = None,
    ):
        self._set_constraint = constraint
        self._key = at_key

        # Type-check and handle items
        if items:
            res = []
            # Check items
            if not isinstance(items, Iterable):
                items = [items]
            for it in items:
                if not (isinstance(it, Rule) or isinstance(it, RuleGroup)):
                    # Add a new `Rule` wrapper if applicable
                    if callable(it):
                        it = Rule.init_specific(it)
                    else:
                        raise ValueError(
                            f"All items in a `RuleGroup` must be `Rule`s or `RuleGroup`s, got: {type(it)}"
                        )
                res.append(it)
                if isinstance(it, RuleGroup):
                    self._n_rules += it._n_rules
                else:
                    self._n_rules += 1
            super().__init__(res)
        else:
            super().__init__()

    def append(self, item: Rule | RuleGroup | Callable):
        if not (isinstance(item, Rule) or isinstance(item, RuleGroup)):
            if callable(item):
                item = Rule.init_specific(item)
            else:
                raise ValueError(f"All items in a `RuleGroup` must be `Rule`s, got: {type(item)}")
        if isinstance(item, RuleGroup):
            self._n_rules += len(item)
        else:
            self._n_rules += 1
        super().append(item)

    @staticmethod
    def combine(
        first: RuleGroup,
        other: Rule | RuleGroup | Any,
        set_constraint: RuleGroupConstraint = RuleGroupConstraint.ALL_OF,
    ) -> RuleGroup:
        """
        Combines a `RuleGroup` with another value. By default, `RuleGroupConstraint.ALL_OF` is used,
          so all contained optional `Rule`s become REQUIRED by default.

        The same rules apply as `Rule.combine`, except rules are generally copied into the first `RuleGroup`
          as opposed to creating a new "parent" `RuleGroup`.

        Here are the expected cases:
        1. & RuleGroup -> Make a new RuleGroup with both existing ones (i.e. add a nesting parent)
        (same as for Rule.combine, except add it to the current RuleGroup)
        2. & Rule -> Add it to current RuleGroup
        3. & type -> Add type check
        4. & dict -> Add 1) type check, 2) contents of dict
        5. & list -> Add 1) type check, 2) contents of list
        6. & Callable -> Add the callable as a Rule
        7. & some primitive -> Add an equality check for the primitive
        """
        if isinstance(other, RuleGroup):
            return RuleGroup((first, other), set_constraint)
        res = deepcopy(first)
        match other:
            case Rule():
                res.append(other)
            case type():
                res.append(IsType(other))
            case dict():
                res.append(IsType(dict))  # Type check
                drs = _dict_to_rulegroup(other)
                res.append(drs)
            case list():
                res.append(IsType(list))  # Type check
                lrs = _list_to_rulegroup(other)
                res.append(lrs)
            case _:
                if callable(other):
                    res.append(Rule(other))
                else:
                    res.append(Rule(p.equals(other)))
        return res

    def __call__(self, *args) -> Ok[RuleGroup] | Err[RuleGroup]:
        rules_passed, rules_failed = RuleGroup(), RuleGroup()
        failed_required_rule = False

        # Apply key unnesting logic

        # Chain calls for each contained rule
        for rule_or_rs in self:
            try:
                # NOTE: Both `Ok`, `Err` are truthy as-is, want `Err` to fail rule
                #   `<Err>.unwrap()` throws exception
                if rule_or_rs(*args).unwrap():
                    self._consume_rules_inplace(rule_or_rs, rules_passed)
                else:
                    self._consume_rules_inplace(rule_or_rs, rules_failed)
                    if isinstance(rule_or_rs, Rule) and (
                        RuleConstraint.REQUIRED in rule_or_rs._constraints
                    ):
                        failed_required_rule = True
            except:
                # TODO: include other info about exception?
                self._consume_rules_inplace(rule_or_rs, rules_failed)
                if isinstance(rule_or_rs, Rule) and (
                    RuleConstraint.REQUIRED in rule_or_rs._constraints
                ):
                    failed_required_rule = True

        # Check result and return
        res: Ok | Err | None = None
        if failed_required_rule:
            res = Err(rules_failed)
        elif (
            self._set_constraint is RuleGroupConstraint.ALL_OF
            and len(rules_passed) == self._n_rules
        ) or (
            self._set_constraint is not RuleGroupConstraint.ALL_OF
            and len(rules_passed) >= self._set_constraint.value
        ):
            rules_passed = _unnest_rulegroup(rules_passed)
            res = Ok(rules_passed)
        else:
            rules_failed = _unnest_rulegroup(rules_failed)
            res = Err(rules_failed)
        return res

    def _consume_rules_inplace(self, source: RuleGroup | Rule, target: RuleGroup) -> None:
        """
        Adds rules to target RuleGroup
        """
        if isinstance(source, RuleGroup):
            for r in source:
                target.append(r)
        elif isinstance(source, Rule):
            target.append(source)
        else:
            raise RuntimeError(f"Type error when calling RuleGroup, got: {type(source)}")

    def __hash__(self):
        return hash(tuple(self))

    def __and__(self, other: Rule | RuleGroup | Any):
        return RuleGroup.combine(self, other)

    def __rand__(self, other: Rule | RuleGroup | Any):
        # Expect commutative
        return self.__and__(other)

    def __or__(self, other: Rule | RuleGroup | Any):
        return RuleGroup.combine(self, other, RuleGroupConstraint.ONE_OF)

    def __ror__(self, other: Rule | RuleGroup | Any):
        # Expect commutative
        return self.__or__(other)


""" Custom Rules """


class IsRequired(Rule):
    """
    A rule where the current field is required

    For `RuleGroup`s: flips `OPTIONAL` -> `ALL_OF`, otherwise ignore more specific constraint
    """

    def __init__(self, at_key: str | None = None):
        # For each rule, make it required
        super().__init__(p.not_equivalent(None), RuleConstraint.REQUIRED, at_key=at_key)

    def __and__(self, other: Rule | RuleGroup | Any) -> Rule | RuleGroup:
        """
        Returns the same type as `other`
        """
        match other:
            case Rule():
                res = deepcopy(other)
                res._constraints.add(RuleConstraint.REQUIRED)  # type: ignore
            case _:
                # Check callable case here (cast into a `Rule`)
                if not isinstance(other, RuleGroup) and callable(other):
                    res = Rule.init_specific(other, RuleConstraint.REQUIRED)
                else:
                    res = super().__and__(other)
                    res._set_constraint = RuleGroupConstraint.ALL_OF  # type: ignore
        return res

    def __rand__(self, other):
        return self.__and__(other)


class NotRequired(Rule):
    """
    When combined with another rule, removes the Required constraint
    """

    def __init__(self, at_key: str | None = None):
        # Initialize with dummy placeholder rule
        super().__init__(lambda _: True, at_key=at_key)

    def __and__(self, other: Rule | RuleGroup | Any) -> Rule | RuleGroup:
        """
        For a `Rule`: remove `REQUIRED`
        For a `RuleGroup`: set to `OPTIONAL` -- this means it's optional, but validate if-present
        """
        match other:
            case Rule():
                res = deepcopy(other)
                if RuleConstraint.REQUIRED in res._constraints:
                    res._constraints.remove(RuleConstraint.REQUIRED)  # type: ignore
            case RuleGroup():
                res = deepcopy(other)  # type: ignore
                res._set_constraint = RuleGroupConstraint.OPTIONAL  # type: ignore
            case _:
                res = super().__and__(other)
                res._set_constraint = RuleGroupConstraint.OPTIONAL  # type: ignore
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
        constraints: RuleConstraint | set[RuleConstraint] | None = None,
        at_key: str | None = None,
    ):
        super().__init__(p.lte(upper), constraints, at_key)


class MinCount(Rule):
    def __init__(
        self,
        lower: int,
        constraints: RuleConstraint | set[RuleConstraint] | None = None,
        at_key: str | None = None,
    ):
        super().__init__(p.gte(lower), constraints, at_key)


class IsType(Rule):
    def __init__(
        self,
        typ: type,
        constraints: RuleConstraint | set[RuleConstraint] | None = None,
        at_key: str | None = None,
    ):
        super().__init__(p.isinstance_of(typ), constraints, at_key)


class InSet(Rule):
    """IDEA: have this be the enum variant. E.g. one of these literals"""

    def __init__(self, s: set[Any]):
        super().__init__(p.contained_in(s))


""" Helper Functions """


def _unnest_rulegroup(rs: RuleGroup) -> RuleGroup:
    """
    Removes an unused outer nesting
    """
    res = rs
    if rs._set_constraint is not RuleGroupConstraint.OPTIONAL and len(rs) == 1:
        (item,) = rs
        if isinstance(item, RuleGroup):
            res = item
    return res


def _list_to_rulegroup(
    l: list[Callable | Rule | RuleGroup | dict | list], key_prefix: str | None = None
) -> RuleGroup:
    """
    Given a list, compress it into a single `RuleGroup`

    An item in the list should pass at least one of the callables in the `RuleGroup`.
      For example: `{ 'k': [ str, bool ]}` -- at key 'k', it can contain a list `str` or `bool`
      (for more specific constraints: use a nested `RuleGroup`)
    """
    # TODO: Check the key is getting applied correctly
    res = RuleGroup(constraint=RuleGroupConstraint.ONE_OF, at_key=key_prefix)
    for it in l:
        # TODO: does this even work / is this even needed?
        at_key = f"[*]"  # Should be applied to every item in the list
        match it:
            case Rule():
                it._key = at_key
                res.append(it)
            case RuleGroup():
                it._key = at_key
                res = it
            case dict():
                res.append(_dict_to_rulegroup(it, at_key))
            case list():
                res.append(_list_to_rulegroup(it, at_key))
            case _:
                if callable(it):
                    res.append(Rule.init_specific(it, at_key=at_key))
                else:
                    # Exact value check
                    res.append(Rule(p.equals(it), at_key=at_key))
    return res


def _dict_to_rulegroup(d: dict[str, Rule | RuleGroup], key_prefix: str | None = None) -> RuleGroup:
    """
    Given a dict, compress it into a single `RuleGroup` with key information saved

    NOTE: expect this dict to be 1-layer (i.e. not contain other dicts) -- other dicts should
      be encompassed in a `RuleGroup`
    """
    res = RuleGroup(at_key=key_prefix)
    for k, v in d.items():
        at_key = k
        match v:
            case Rule() | RuleGroup():
                v._key = at_key
                res.append(v)
            case dict():
                res.append(IsType(dict, at_key=at_key))
                res.append(_dict_to_rulegroup(v, key_prefix=at_key))
            case list():
                res.append(IsType(list, at_key=k))
                res.append(_list_to_rulegroup(v, key_prefix=at_key))
            case _:
                if callable(v):
                    res.append(Rule.init_specific(v, at_key=at_key))
                else:
                    # Exact value check
                    res.append(Rule(p.equals(v), at_key=k))
    return res
