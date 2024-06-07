from __future__ import (  # Used to recursively type annotate (e.g. `Rule` in `class Rule`)
    annotations,
)

import inspect

from collections import defaultdict
from collections.abc import Callable, Collection, Iterable
from copy import deepcopy
from enum import Enum
from typing import Any

from result import Err, Ok

import pydian.partials as p
from pydian.dicts import get

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

    ALL_REQUIRED_RULES = -2  # NOTE: This implicitly makes everything _optional_ by default
    ALL_RULES = -1           # NOTE: This implicitly makes everything _required_ by default
    WHEN_KEY_IS_PRESENT = 0  # NOTE: This means optional, but be strict if it's there
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
    _constraints: set[RC] = None  # type: ignore
    _key: str | None = None

    def __init__(
        self,
        fn: Callable,
        constraints: RC | Collection[RC] | None = None,
        at_key: str | None = None,
    ):
        self._fn = fn
        self._key = at_key  # Managed by `RuleGroup`
        self._constraints = set()
        match constraints:
            case RC():
                self._constraints.add(constraints)
            case Collection():
                for c in constraints:
                    if isinstance(c, RC):
                        self._constraints.add(c)

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
            return f"{self._fn}:`{inspect.getsource(self._fn)}`"
        except:
            return f"{self._fn} (Rule)"

    def __hash__(self):
        return hash((self._fn, frozenset(self._constraints), self._key))

    def __eq__(self, other: Rule | Any):
        if isinstance(other, Rule):
            # Rules are the same based on the code and the applied constraints
            # HACK: I _think_ this will work, though need to test more thoroughly
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
        # TODO: consider cases for `InRange`, `InSet`
        from .custom import IsType # NOTE: avoiding circular imports
        if isinstance(v, type):
            res = IsType(v, at_key)  # type: ignore
        else:
            res = Rule(v, constraint, at_key)  # type: ignore
        return res

    @staticmethod
    def combine(
        rule: Rule,
        other: Rule | RuleGroup | Any,
        set_constraint: RGC | Collection[RGC] = (RGC.ALL_REQUIRED_RULES, RGC.WHEN_KEY_IS_PRESENT),
    ) -> RuleGroup:
        """
        Combines a `Rule` with another value. By default, `RGC.ALL_REQUIRED_RULES` is used,
          so all contained optional `Rule`s become optional by default (implicitly).

        The new RuleGroup will contain the original Rule + extras based on the cases below.

        Here are the expected cases:
        1. & Rule | RuleGroup -> Add it
        2. & type -> Add type check
        3. & dict -> Add 1) type check, 2) contents of dict
        4. & list -> Add 1) type check, 2) contents of list
        5. & Callable -> Add the callable as a Rule
        6. & some primitive -> Add an equality check for the primitive
        """
        # TODO: Merge this with `RuleGroup.combine`, it's basically the same! (do after tests work)

        from .custom import IsType # Do this to avoid circular import
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
        return Rule.combine(self, other, RGC.AT_LEAST_ONE)

    def __ror__(self, other: Rule | RuleGroup | Any):
        # Operation intended to be commutative
        return self.__or__(other)


class RuleGroup(defaultdict):
    """
    A `RuleGroup` is a callable dict that can contains `Rule`s and/or other `RuleGroup`s

    The typing of the dict: `dict[str, list[Rule | RuleGroup]]`. 
    
    The key indicates a particular key prefix that should apply to all corresponding rules in the list.
      The default is the empty string, meaning the Rule has any applicable key information.

    When called, a `RuleGroup` evaluates all contained `Rule`/`RuleGroups` and combines the result into:
    - Ok([rules_passed])
    - Err([rules_failed])

    The structure of a "RuleGroup":
    {
        "": [Rule | RuleGroup, ...]
        "_key_prefix_for_following": [Rule | RuleGroup, ...]
    }

    A constraint defines whether the result is `Ok` or `Err` based on contained `Rule`/`RuleGroups`.
      Additionally, individual `Rule`s may have constraints which the `RuleGroup` manages
    """

    _default_key: str = ""
    _group_constraints: set[RGC] = None  # type: ignore
    _n_rules: int = 0

    def __init__(
        self,
        items: Rule | RuleGroup | Callable | Collection[Rule | RuleGroup | Callable] | None = None,
        constraints: RGC | Collection[RGC] = RGC.ALL_RULES,
        at_key: str = "",
    ):
        self._default_key = at_key

        # Init and add constraints
        self._group_constraints = set()
        if constraints:
            if not isinstance(constraints, Collection):
                constraints = (constraints,)
            for c in constraints:
                self._group_constraints.add(c)

        # Type-check and handle items
        res = {self._default_key: []}
        if items:
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
                res[at_key].append(it)
                if isinstance(it, RuleGroup):
                    self._n_rules += it._n_rules
                else:
                    self._n_rules += 1
        super().__init__(list, res)

    # TODO: Rename this to avoid ambiguity with a list
    # ... `add_item`?
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
        default_list: list = self[self._default_key]
        default_list.append(item)

    @staticmethod
    def combine(
        first: RuleGroup,
        other: Rule | RuleGroup | Any,
        set_constraint: RGC | Collection[RGC] = (RGC.ALL_REQUIRED_RULES, RGC.WHEN_KEY_IS_PRESENT),
    ) -> RuleGroup:
        """
        Combines a `RuleGroup` with another value. By default, all data is optional by default

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
        # TODO: Preserve key information!
        if isinstance(other, RuleGroup):
            return RuleGroup((first, other), set_constraint)
        res = deepcopy(first)
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

    def __call__(self, source: Any, *args: Any) -> Ok[list[RuleGroup]] | Err[list[RuleGroup]]:
        rules_passed, rules_failed = list(), list()

        # Chain calls for each contained rule
        for k, r_rg_list in self.items():
            # Apply key unnesting logic
            # NOTE: only when source is a dict. Design choice!
            if isinstance(source, dict) and self._default_key != Ellipsis:
                r_rg_input = get(source, self._default_key)
            else:
                r_rg_input = source
            for r_rg in r_rg_list:
                try:
                    # NOTE: Both `Ok`, `Err` are truthy as-is, want `Err` to fail rule
                    #   `<Err>.unwrap()` throws exception
                    if r_rg(r_rg_input, *args).unwrap():
                        self._consume_rules_inplace(r_rg, rules_passed)
                    else:
                        self._consume_rules_inplace(r_rg, rules_failed)
                except:
                    # TODO: include other info about exception?
                    self._consume_rules_inplace(r_rg, rules_failed)

        # Check result and return
        # rules_passed = _unnest_rulegroup(rules_passed)
        # rules_failed = _unnest_rulegroup(rules_failed)
        res: Ok | Err = Ok(rules_passed)
        ## Check all cases for failures, otherwise pass!
        for rgc in self._group_constraints:
            match rgc:
                # `ALL_RULES` takes precedent over all other cases, so we can exit early
                case RGC.ALL_RULES:
                    ## Check success criteria
                    if len(rules_passed) == self._n_rules:
                        res = Ok(rules_passed)
                        break
                    else:
                        res = Err(rules_failed)
                        break
                case RGC.ALL_REQUIRED_RULES:
                    ## Check for failed required rules -- return Err early if so
                    for r_rg in rules_failed:
                        # TODO: Need to include `RuleGroup` case (which is also getting defined here...)
                        if isinstance(r_rg, Rule) and (RC.REQUIRED in r_rg._constraints):
                            res = Err(rules_failed)
                case RGC.WHEN_KEY_IS_PRESENT:
                    # Ok. Here, we have the rules that passed/failed.
                    # Basically, this is saying a non-required rule that failed
                    #  should disqualify `Ok` if the key was actually there
                    #  (when the key isn't there, having the rule fail is fine)
                    
                    # So: for each of the failed rules, figure-out which key it was at. 
                    #  Then check that key was in the source data...
                    # ... either need to pass this data up, or pass the `WHEN_KEY_IS_PRESENT` down...
                    failed_keys = set()
                    for r_rg in rules_failed:
                        if isinstance(r_rg, Rule) and r_rg._key:
                            failed_keys.add(r_rg._key)
                        elif isinstance(r_rg, RuleGroup):
                            # TODO: Test this!
                            for k in _get_all_seen_keys(r_rg):
                                failed_keys.add(k)
                        else:
                            raise TypeError(f"Got unexpected type when evaluating RuleGroup: {type(r_rg)}")
                    for k in failed_keys:
                        if get(source, k):
                            res = Err(rules_failed)
                            break
                case RGC.AT_LEAST_ONE | RGC.AT_LEAST_TWO | RGC.AT_LEAST_THREE:
                    if len(rules_passed) < rgc.value:
                        res = Err(rules_failed)
                case _val:
                    raise RuntimeError(f"Got unexpected runtime RuleGroup Constraint: {_val}")

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
    if rs._group_constraints is not RGC.WHEN_KEY_IS_PRESENT and len(rs) == 1:
        (item,) = rs
        if isinstance(item, RuleGroup):
            res = item
    return res

def _get_all_seen_keys(rg: RuleGroup, key_prefix: str = "") -> list[str]:
    """
    Iterate throguh a nested RuleGroup and try to reconstruct key-related data
        (this should be present for nested groups)
    """
    res = []
    for k, v in rg.items():
        if k is not Ellipsis and key_prefix:
            k = f"{key_prefix}.{k}"
        for r_rg in v:
            if isinstance(r_rg, Rule) and r_rg._key:
                if k is not Ellipsis:
                    res.append(f"{k}.{r_rg._key}")
                else:
                    res.append(r_rg._key)
            elif isinstance(r_rg, RuleGroup):
                new_prefix = k if k is not Ellipsis else key_prefix
                res.append(*_get_all_seen_keys(r_rg, new_prefix))
            else:
                raise RuntimeError(f"Got unexpeted type: {type(r_rg)}")
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
    res = RuleGroup(constraints=RGC.AT_LEAST_ONE, at_key=key_prefix)
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
