from __future__ import (  # Used to recursively type annotate (e.g. `Rule` in `class Rule`)
    annotations,
)

import inspect
from collections.abc import Callable, Collection, Iterable
from enum import Enum
from typing import Any

from result import Err, Ok

import pydian.partials as p

from ..dicts import get


class RC(Enum):
    """
    Rule Constraint (RC): A constraint for a single `Rule` (`RuleGroup` is responsible for application)

    Optional by default (i.e. no value specified)
    """

    REQUIRED = 1
    # ONLY_IF = lambda fn: (RC.REQUIRED, fn)  # Usage: RC.ONLY_IF(some_fn)
    # ONLY_AFTER = ... # Usage: ONLY_AFTER(other_rule) ... or some way to identify...


class RGC(Enum):
    """
    RuleGroup Constraint (RGC): Constraints on top of current `RuleGroup`

    Groups of >3 explicitly required are discouraged - KISS!

    RGC takes precedent over RC (i.e. a `RGC` setting will override a `RC` setting when applicable)
    """

    ALL_REQUIRED_RULES = -2  # NOTE: This makes the default _optional_
    ALL_RULES = -1  # NOTE: This makes the default _required_
    ALL_WHEN_KEY_PRESENT = 0  # NOTE: This makes the default _required if data is present_
    AT_LEAST_ONE = 1
    AT_LEAST_TWO = 2
    AT_LEAST_THREE = 3


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
    _constraint: RC | None = None
    _key: str | None = None

    def __init__(
        self,
        fn: Callable,
        constraint: RC | None = None,
        at_key: str | None = None,
    ):
        self._fn = fn
        self._key = at_key  # Managed by `RuleGroup`
        if constraint:
            self._constraint = constraint

    def __call__(self, source: Any, *args) -> Ok[Any] | Err[str]:
        """
        Call specified rule

        NOTE: the key nesting needs to be enforced at the `RuleGroup` level,
            since an individual Rule doesn't have enough information available to it
        """
        # NOTE: Only apply key logic for `dict`s. Something something, design choice!
        if isinstance(source, dict) and self._key:
            source = get(source, self._key)
        try:
            res = self._fn(source, *args)
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
            return f"(Rule) {inspect.getsource(self._fn).strip()}`"
        except:
            return f"(Rule) {self._fn}"

    def __hash__(self):
        return hash((self._fn, self._constraint, self._key))

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
    def init_specific(v: Any, constraint: RC | None = None, at_key: str | None = None) -> Rule:
        """
        Generically returns a more specific rule when possible
        """
        # TODO: consider `InRange` (take range obj) and `InSet` (take set object)
        from .specific import IsType  # Import here to avoid circular import

        if isinstance(v, type):
            res = IsType(v, at_key)  # type: ignore
        else:
            res = Rule(v, constraint, at_key)  # type: ignore
        return res

    def __and__(self, other: Rule | RuleGroup | Any):
        return RuleGroup.combine(self, other)

    def __rand__(self, other: Rule | RuleGroup | Any):
        # Operation intended to be commutative
        return self.__and__(other)

    def __or__(self, other: Rule | RuleGroup | Any):
        return RuleGroup.combine(self, other, RGC.AT_LEAST_ONE)

    def __ror__(self, other: Rule | RuleGroup | Any):
        # Operation intended to be commutative
        return self.__or__(other)


class RuleGroup(list):
    """
    A `RuleGroup` is a callable list that can contain:
    - One or many `Rule`s
    - Other `RuleGroup`s (ending in a terminal `RuleGroup` of just `Rule`s at some point)

    The specified `RGC` (RuleGroup Constraint) determines the logic for the result outcome.
      Additionally, individual `Rule`s may have constraints which the `RuleGroup` manages
    """

    _group_constraint: RGC | None = None  # type: ignore
    _n_rules: int = (
        0  # TODO: this is buggy... define behavior and reduce ambiguity (at some point)!
    )
    # NOTE: _key is needed here to save nesting information
    #   e.g. a user-specified `RuleGroup` shouldn't need to specify `_key` for each rule,
    #   rather we should infer that during parsing
    _key: str | None = None

    def __init__(
        self,
        items: Rule | RuleGroup | Callable | Collection[Rule | RuleGroup | Callable] | None = None,
        constraint: RGC = RGC.ALL_RULES,
        at_key: str | None = None,
    ):
        self._key = at_key
        self._group_constraint = constraint

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
        first: Rule | RuleGroup,
        other: Rule | RuleGroup | Any,
        constraint: RGC = RGC.ALL_WHEN_KEY_PRESENT,
    ) -> RuleGroup:
        """
        Combines a `RuleGroup` with another value. By default, all data is optional by default

        Here are the expected cases:
        1. & RuleGroup -> Make a new RuleGroup with both existing ones (i.e. add a nesting parent)
        2. & Rule -> Add it to current RuleGroup
        3. & type -> Add type check
        4. & dict -> Add 1) type check, 2) contents of dict
        5. & list -> Add 1) type check, 2) contents of list
        6. & Callable -> Add the callable as a Rule
        7. & some primitive -> Add an equality check for the primitive
        """
        if isinstance(other, RuleGroup):
            return RuleGroup((first, other), constraint)
        res = RuleGroup(first, constraint)
        match other:
            case Rule():
                res.append(other)
            case type():
                res.append(Rule.init_specific(other))
            case dict():
                res.append(Rule.init_specific(dict))  # Type check
                drs = _dict_to_rulegroup(other)
                res.append(drs)
            case list():
                res.append(Rule.init_specific(list))  # Type check
                lrs = _list_to_rulegroup(other)
                res.append(lrs)
            case _:
                if callable(other):
                    res.append(Rule(other))
                else:
                    res.append(Rule(p.equals(other)))
        return res

    def __call__(self, source: Any, *args) -> Ok[RuleGroup] | Err[RuleGroup]:
        rules_passed, rules_failed = RuleGroup(), RuleGroup()

        # Apply key unnesting logic
        # NOTE: only when source is a dict. Design choice!
        if isinstance(source, dict) and self._key:
            source = get(source, self._key)

        # Chain calls for each contained rule
        for rule_or_rg in self:
            try:
                # NOTE: Both `Ok`, `Err` are truthy as-is, want `Err` to fail rule
                #   `<Err>.unwrap()` throws exception
                if rule_or_rg(source, *args).unwrap():
                    self._consume_rules_inplace(rule_or_rg, rules_passed)
                else:
                    self._consume_rules_inplace(rule_or_rg, rules_failed)
            except:
                # TODO: include other info about exception?
                self._consume_rules_inplace(rule_or_rg, rules_failed)

        # Check result and return
        res: Ok | Err = Err(rules_failed)
        ## Check for failed required rules -- return Err early if so
        for r in rules_failed:
            if isinstance(r, Rule) and (r._constraint is RC.REQUIRED):
                return Err(rules_failed)
        ## Check `ALL_RULES`, otherwise check number based on value

        def __handle_rules(condition: bool) -> Ok[RuleGroup] | Err[RuleGroup]:
            """
            Helper function - retains context of `rules_passed`, `rules_failed`
            """
            if condition:
                rules = _unnest_rulegroup(rules_passed)
                return Ok(rules)
            else:
                rules = _unnest_rulegroup(rules_failed)
                return Err(rules)

        match self._group_constraint:
            case RGC.ALL_RULES:
                res = __handle_rules(len(rules_passed) == self._n_rules)
            case RGC.AT_LEAST_ONE | RGC.AT_LEAST_TWO | RGC.AT_LEAST_THREE:
                res = __handle_rules(len(rules_passed) >= self._group_constraint.value)
            case _:
                # TODO: Handle more RuleGroup constraints
                raise RuntimeError(f"Unsupported RuleGroup constraint: {self._group_constraint}")
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

    def __repr__(self) -> str:
        return f"(RuleGroup) {self}"

    def __and__(self, other: Rule | RuleGroup | Any):
        return RuleGroup.combine(self, other)

    def __rand__(self, other: Rule | RuleGroup | Any):
        # Expect commutative
        return self.__and__(other)

    def __or__(self, other: Rule | RuleGroup | Any):
        return RuleGroup.combine(self, other, RGC.AT_LEAST_ONE)

    def __ror__(self, other: Rule | RuleGroup | Any):
        # Expect commutative
        return self.__or__(other)


""" Helper Functions """


def _unnest_rulegroup(rs: RuleGroup) -> RuleGroup:
    """
    Removes an unused outer nesting
    """
    res = rs
    if rs._group_constraint is not RGC.ALL_WHEN_KEY_PRESENT and len(rs) == 1:
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
    res = RuleGroup(constraint=RGC.AT_LEAST_ONE, at_key=key_prefix)
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
                res.append(Rule.init_specific(dict, at_key=at_key))
                res.append(_dict_to_rulegroup(v, key_prefix=at_key))
            case list():
                res.append(Rule.init_specific(list, at_key=k))
                res.append(_list_to_rulegroup(v, key_prefix=at_key))
            case _:
                if callable(v):
                    res.append(Rule.init_specific(v, at_key=at_key))
                else:
                    # Exact value check
                    res.append(Rule(p.equals(v), at_key=k))
    return res
