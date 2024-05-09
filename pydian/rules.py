from __future__ import annotations

from collections.abc import Callable, Collection, Iterable
from copy import deepcopy
from enum import Enum
from typing import Any

from result import Err, Ok

import pydian.partials as p

from .dicts import get


class RuleConstraint(Enum):
    """
    A constraint for a single `Rule` (`RuleSet` is responsible for application)

    Optional by default (i.e. no value specified)
    """

    REQUIRED = 1
    # ONLY_IF = lambda fn: (RuleConstraint.REQUIRED, fn)  # Usage: RuleConstraint.ONLY_IF(some_fn)


class RuleSetConstraint(Enum):
    """
    Constraints on top of current `RuleSet`

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

    A `Rule` can have additional constraints. These will ONLY be applied if contained within a `RuleSet`

    `Rule`s can be combined to create `RuleSet`s using: `&`, `|`
    """

    def _raise_undefined_rule_err():  # type: ignore
        raise ValueError("Rule was not defined!")

    _fn: Callable = lambda _: Rule._raise_undefined_rule_err()
    _constraints: set[RuleConstraint] = None  # type: ignore
    _key: str | None = None

    def __init__(
        self,
        fn: Callable,
        constraints: RuleConstraint | Collection[RuleConstraint] | None = None,
        at_key: str | None = None,
    ):
        self._fn = fn
        self._key = at_key  # Managed by `RuleSet`
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

        NOTE: the key nesting needs to be enforced at the `RuleSet` level,
            since an individual Rule doesn't have enough information available to it
        """
        try:
            res = self._fn(*args)
            if res:
                return Ok(res)
        except Exception as e:
            return Err(f"Rule {self} failed with exception: {e}")
        return Err(f"Rule {self} failed, got falsy value: {res}")

    # TODO: be careful with making this iterable by default -- not really a container
    # def __iter__(self):
    #     """Iterate over the constraints"""
    #     return iter(self._constraints)

    def __hash__(self):
        return hash((self._fn, frozenset(self._constraints), self._key))

    def __eq__(self, other: Rule | Any):
        if isinstance(other, Rule):
            # Rules are the same based on the code and the applied constraints
            # TODO: This will not work if there are different variable names for lambdas,
            #    e.g. the `__code__` object for `lambda x: None` is different from `lambda y: None`
            return (other._fn.__code__ == self._fn.__code__) and (
                other._constraints == self._constraints
            )
        return NotImplemented

    @staticmethod
    def combine(
        rule: Rule,
        other: Rule | RuleSet | Any,
        set_constraint: RuleSetConstraint = RuleSetConstraint.ALL_OF,
    ) -> RuleSet:
        """
        Combines a `Rule` with another value. By default, `RuleSetConstraint.ALL_OF` is used,
          so all contained optional `Rule`s become REQUIRED by default.

        Here are the expected cases:
        - `Rule` r1 & `Rule` r2 -> `RuleSet` {r1, r2}
        - `Rule` r1 & `RuleSet` rs2 -> `RuleSet` {r1, rs2}
        - `Rule` r1 & `dict` d2 -> `RuleSet` {r1, dt2, drs2},
            where `dt2` is a typecheck, and `drs2` is a `RuleSet` derived from the contents of `d2`
            `drs2` has the corresponding `key_prefix` property filled with the key information
        - `Rule` r1 & `list` l2 -> `RuleSet` {r1, lt2, lrs2}
            where `lt2` is a typecheck,  and `lrs2` is a `RuleSet` derived from the contents of `l2`
            `lrs2` has `key_prefix` specifying which items of the list it should be applied to.
                `[]` -> List itself,
                `[:]` -> All items within the list,
                ... else support regular list slicing
        - `Rule` r1 & `Callable` c2 -> `RuleSet` {r1, rc2}
            where `rc2` is the Callable wrapped as an optional `Rule`
        - `Rule` r1 & `Any` a2 -> `RuleSet` {r1, ae2}
            where `ae2` is an equality check
        """
        res = RuleSet(rule, set_constraint)
        match other:
            case Rule() | RuleSet():
                res.add(other)
            case type():
                res.add(IsType(other))
            case dict():
                # Add the typecheck
                res.add(Rule(lambda x: isinstance(x, dict)))
                # For each item in dict, save key information and add to res
                drs = _dict_to_ruleset(other)
                res.add(drs)
            case list():
                # Add the typecheck
                res.add(Rule(lambda x: isinstance(x, list)))
                # For each item in list, save key information and add to res
                lrs = _list_to_ruleset(other)
                res.add(lrs)
            case _:
                if callable(other):
                    res.add(Rule(other))
                else:
                    # Exact value check
                    res.add(Rule(p.equals(other)))
        return res

    def __and__(self, other: Rule | RuleSet | Any):
        return Rule.combine(self, other)

    def __rand__(self, other: Rule | RuleSet | Any):
        # Operation intended to be commutative
        return self.__and__(other)

    def __or__(self, other: Rule | RuleSet | Any):
        return Rule.combine(self, other, RuleSetConstraint.ONE_OF)

    def __ror__(self, other: Rule | RuleSet | Any):
        # Operation intended to be commutative
        return self.__or__(other)


class RuleSet(set):
    """
    A `RuleSet` is a callable set that can contain:
    - One or many `Rule`s
    - Other `RuleSet`s (ending in a terminal `RuleSet` of just `Rule`s at some point)

    When called, a `RuleSet` evaluates all contained `Rule`/`RuleSets` and combines the result into:
    - Ok([rules_passed, ...])
    - Err([rules_failed, ...])

    A constraint defines whether the result is `Ok` or `Err` based on contained `Rule`/`RuleSets`.
      Additionally, individual `Rule`s may have constraints which the `RuleSet` manages
    """

    _set_constraint: RuleSetConstraint = RuleSetConstraint.ALL_OF  # default
    _n_rules: int = 0
    # NOTE: _key is needed here to save nesting information
    #   e.g. a user-specified `RuleSet` shouldn't need to specify `_key` for each rule,
    #   rather we should infer that during parsing
    _key: str | None = None

    def __init__(
        self,
        items: Rule | RuleSet | Collection[Rule | RuleSet] | None = None,
        constraint: RuleSetConstraint = RuleSetConstraint.ALL_OF,
        at_key: str | None = None,
    ):
        self._set_constraint = constraint
        self._key = at_key
        # Type-check and handle items
        if items:
            # Check items
            if isinstance(items, Iterable):
                for it in items:
                    if not (isinstance(it, Rule) or isinstance(it, RuleSet)):
                        raise ValueError(
                            f"All items in a `RuleSet` must be `Rule`s or `RuleSet`s, got: {type(it)}"
                        )
                    if isinstance(it, RuleSet):
                        self._n_rules += it._n_rules
                    else:
                        self._n_rules += 1
            else:
                # Wrap in iterable so `set` call passes
                items = [items]
            super().__init__(items)
        else:
            super().__init__()

    def add(self, item: Rule | RuleSet):
        if not (isinstance(item, Rule) or isinstance(item, RuleSet)):
            raise ValueError(f"All items in a `RuleSet` must be `Rule`s, got: {type(item)}")
        super().add(item)

    @staticmethod
    def combine(
        first: RuleSet,
        other: Rule | RuleSet | Any,
        set_constraint: RuleSetConstraint = RuleSetConstraint.ALL_OF,
    ) -> RuleSet:
        """
        Combines a `RuleSet` with another value. By default, `RuleSetConstraint.ALL_OF` is used,
          so all contained optional `Rule`s become REQUIRED by default.

        The same rules apply as `Rule.combine`, except rules are generally copied into the first `RuleSet`
          as opposed to creating a new "parent" `RuleSet`.

          The only time a parent `RuleSet` is generated is when both `first` and `other` are each
          independent `RuleSet`s.

        Here are the expected cases:
        - `RuleSet` rs1 & `RuleSet` rs2 -> `RuleSet` {rs1, rs2}     # parent `RuleSet` containing rs1, rs2
            - TODO: some sort of nesting optimization? E.g. remove layers?
        - `RuleSet` rs1 & `Rule` r2 -> `RuleSet` {*rs1, r2}         # r2 added to rs1
        - `RuleSet` rs1 & `dict` d2 -> `RuleSet` {*rs1, dt2, drs2}, # dt2, drs2 added to  rs1
            where `dt2` is a typecheck, and `drs2` is a `RuleSet` derived from the contents of `d2`
            `drs2` has the corresponding `key_prefix` property filled with the key information
        - `RuleSet` rs1 & `list` l2 -> `RuleSet` {*rs1, lt2, lrs2}  # lt2, lrs2 added to  rs1
            where `lt2` is a typecheck,  and `lrs2` is a `RuleSet` derived from the contents of `l2`
            `lrs2` has `key_prefix` specifying which items of the list it should be applied to.
                `[]` -> List itself,
                `[:]` -> All items within the list,
                ... else support regular list slicing
        - `RuleSet` rs1 & `Callable` c2 -> `RuleSet` {*rs1, rc2}      # rc2 added to rs1
            where `rc2` is the Callable wrapped as an optional `Rule`
        - `RuleSet` rs1 & `Any` a2 -> `RuleSet` {*rs1, ae2}           # ae2 added to rs1
            where `ae2` is an equality check for the value a2
        """
        if isinstance(other, RuleSet):
            return RuleSet((first, other), set_constraint)
        res = deepcopy(first)
        match other:
            case Rule():
                res.add(other)
            case type():
                res.add(IsType(other))
            case dict():
                res.add(IsType(dict))  # Type check
                drs = _dict_to_ruleset(other)
                res.add(drs)
            case list():
                res.add(IsType(list))  # Type check
                lrs = _list_to_ruleset(other)
                res.add(lrs)
            case _:
                if callable(other):
                    res.add(Rule(other))
                else:
                    res.add(Rule(p.equals(other)))
        return res

    def __call__(self, *args) -> Ok[RuleSet] | Err[RuleSet]:
        rules_passed, rules_failed = RuleSet(), RuleSet()
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
            self._set_constraint is RuleSetConstraint.ALL_OF and len(rules_passed) == self._n_rules
        ) or (
            self._set_constraint is not RuleSetConstraint.ALL_OF
            and len(rules_passed) >= self._set_constraint.value
        ):
            rules_passed = _unnest_ruleset(rules_passed)
            res = Ok(rules_passed)
        else:
            rules_failed = _unnest_ruleset(rules_failed)
            res = Err(rules_failed)
        return res

    def _consume_rules_inplace(self, source: RuleSet | Rule, target: RuleSet) -> None:
        """
        Adds rules to target RuleSet
        """
        if isinstance(source, RuleSet):
            for r in source:
                target.add(r)
        elif isinstance(source, Rule):
            target.add(source)
        else:
            raise RuntimeError(f"Type error when calling RuleSet, got: {type(source)}")

    def __hash__(self):
        return hash(frozenset(self))

    def __and__(self, other: Rule | RuleSet | Any):
        return RuleSet.combine(self, other)

    def __rand__(self, other: Rule | RuleSet | Any):
        # Expect commutative
        return self.__and__(other)

    def __or__(self, other: Rule | RuleSet | Any):
        return RuleSet.combine(self, other, RuleSetConstraint.ONE_OF)

    def __ror__(self, other: Rule | RuleSet | Any):
        # Expect commutative
        return self.__or__(other)


""" Custom Rules """


class IsRequired(Rule):
    """
    A rule where the current field is required

    For `RuleSet`s: flips `OPTIONAL` -> `ALL_OF`, otherwise ignore more specific constraint
    """

    def __init__(self, at_key: str | None = None):
        # For each rule, make it required
        super().__init__(p.not_equivalent(None), RuleConstraint.REQUIRED, at_key=at_key)

    def __and__(self, other: Rule | RuleSet | Any) -> Rule | RuleSet:
        """
        Returns the same type as `other`
        """
        match other:
            case Rule():
                res = deepcopy(other)
                res._constraints.add(RuleConstraint.REQUIRED)  # type: ignore
            case _:
                res = super().__and__(other)
                res._set_constraint = RuleSetConstraint.ALL_OF  # type: ignore
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

    def __and__(self, other: Rule | RuleSet | Any) -> Rule | RuleSet:
        """
        For a `Rule`: remove `REQUIRED`
        For a `RuleSet`: set to `OPTIONAL` -- this means it's optional, but validate if-present
        """
        match other:
            case Rule():
                res = deepcopy(other)
                if RuleConstraint.REQUIRED in res._constraints:
                    res._constraints.remove(RuleConstraint.REQUIRED)  # type: ignore
            case RuleSet():
                res = deepcopy(other)  # type: ignore
                res._set_constraint = RuleSetConstraint.OPTIONAL  # type: ignore
            case _:
                res = super().__and__(other)
                res._set_constraint = RuleSetConstraint.OPTIONAL  # type: ignore
        return res

    def __rand__(self, other: Rule | RuleSet | Any):
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
    def __init__(self, upper: int):
        super().__init__(p.lte(upper))


class MinCount(Rule):
    def __init__(self, lower: int):
        super().__init__(p.gte(lower))


class IsType(Rule):
    def __init__(self, typ: type):
        super().__init__(p.isinstance_of(typ))


class InSet(Rule):
    """IDEA: have this be the enum variant. E.g. one of these literals"""

    def __init__(self, s: set[Any]):
        super().__init__(p.contained_in(s))


""" Helper Functions """


def _unnest_ruleset(rs: RuleSet) -> RuleSet:
    """
    Removes an unused outer nesting
    """
    res = rs
    if rs._set_constraint is not RuleSetConstraint.OPTIONAL and len(rs) == 1:
        (item,) = rs
        if isinstance(item, RuleSet):
            res = item
    return res


def _list_to_ruleset(
    l: list[Callable | Rule | RuleSet | dict | list], key_prefix: str = ""
) -> RuleSet:
    """
    Given a list, compress it into a single `RuleSet`

    An item in the list should pass at least one of the callables in the `RuleSet`.
      For example: `{ 'k': [ str, bool ]}` -- at key 'k', it can contain a list `str` or `bool`
      (for more specific constraints: use a nested `RuleSet`)
    """
    # TODO TODO TODO: Need to handle key logic here!
    res = RuleSet(constraint=RuleSetConstraint.ONE_OF)
    for it in l:
        at_key = f"{key_prefix}[*]"  # Should be applied to every item in the list
        match it:
            case Rule():
                it._key = at_key
                res.add(it)
            case RuleSet():
                res = it
            case dict():
                res.add(_dict_to_ruleset(it, at_key))
            case list():
                res.add(_list_to_ruleset(it, at_key))
            case _:
                if callable(it):
                    res.add(Rule(it, at_key=at_key))
                else:
                    # Exact value check
                    res.add(Rule(p.equals(it), at_key))
    return res


def _dict_to_ruleset(d: dict[str, Rule | RuleSet], key_prefix: str = "") -> RuleSet:
    """
    Given a dict, compress it into a single `RuleSet` with key information saved

    NOTE: expect this dict to be 1-layer (i.e. not contain other dicts) -- other dicts should
      be encompassed in a `RuleSet`
    """
    # TODO TODO TODO: Handle key nesting here. I.e. `get` into ".".join([key_prefix, rule_key])
    res = RuleSet()
    for k, v in d.items():
        if key_prefix:
            k = f"{key_prefix}.{k}"
        match v:
            case Rule():
                v._key = k
                res.add(v)
            case RuleSet():
                res.add(v)
            case dict():
                res.add(Rule(p.isinstance_of(dict), at_key=k))
                res.add(_dict_to_ruleset(v, key_prefix=k))
            case list():
                res.add(Rule(p.isinstance_of(list), at_key=k))
                res.add(_list_to_ruleset(v, key_prefix=k))
            case _:
                if callable(v):
                    res.add(Rule(v, at_key=k))
                else:
                    # Exact value check
                    res.add(Rule(p.equals(v), at_key=k))
    return res
