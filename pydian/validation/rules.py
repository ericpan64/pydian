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

    # TODO: Consider conditional feature -- `ONLY_IF(...)`, `ALL_IF(...)` instead of `ALL_WHEN_DATA_PRESENT`
    #       Would need to think through if they take generic callables, Rules, etc.

    ALL_REQUIRED_RULES = -2  # NOTE: This makes rules default _optional_
    ALL_RULES = -1  # NOTE: This makes rules default _required_
    ALL_WHEN_DATA_PRESENT = 0  # NOTE: This makes rules default _required when data is present_
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
        self._key = at_key
        if constraint:
            self._constraint = constraint

    def __call__(
        self, source: Any, *args
    ) -> Ok[tuple[Rule, Any, Any]] | Err[tuple[Rule, Any, Any]]:
        """
        Call specified rule and wraps as a `Result` type.

        This will return one of the tuple variants of:
         1. Ok((current Rule, input, output))
         2. Err((current Rule, input, output))
         3. Err((current Rule, input, exception))
        """
        # NOTE: Only apply key logic for `dict`s. Something something, design choice!
        # Also: if passed an `Ok`, unwrap it by default
        if isinstance(source, dict) and self._key:
            curr_source = get(source, self._key)
        elif isinstance(source, Ok):
            curr_source = source.unwrap()
        else:
            curr_source = source
        try:
            # TODO: Handle list case (key = "[*]")
            if self._key and "[*]" in self._key:
                assert isinstance(curr_source, list), "Err: did not get a `list` for `[*]` key!"
                # Ok. We run the `_fn` for each item in the list, and return the results if all passed
                #   If there's any fail, then exit early, and the last item is a fail
                is_all_truthy = True
                res = []
                for it in curr_source:
                    it_res = self._fn(it)
                    res.append(it_res)
                    is_all_truthy = is_all_truthy and bool(it_res)
                    if not is_all_truthy:
                        break
                if not is_all_truthy:
                    raise RuntimeError(f"Got failure when evaluating item {len(res)}: {res[-1]}")
            else:
                res = self._fn(curr_source, *args)
            if res:
                return Ok((self, source, res))
        except Exception as e:
            return Err((self, "ERROR", e))
        return Err((self, source, res))

    def __repr__(self) -> str:
        # NOTE: can only grab source for saved files, not in repl
        #  So the function needs to be saved on a file
        #  See: https://stackoverflow.com/a/335159
        try:
            return f"<Rule> {inspect.getsource(self._fn).strip()}, {tuple(c.cell_contents for c in self._fn.__closure__)}"  # type: ignore
        except:
            try:
                return f"<Rule> {inspect.getsource(self._fn).strip()}"
            except:
                return f"<Rule> {self._fn}"

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
        # Don't re-wrap an existing Rule/RuleGroup
        if isinstance(v, Rule | RuleGroup):
            return v

        # TODO: consider `InRange` (take range obj) and `InSet` (take set object)
        from .specific import IsType  # Import here to avoid circular import

        if isinstance(v, type):
            res = IsType(v, constraint, at_key)  # type: ignore
        else:
            res = Rule(v, constraint, at_key)  # type: ignore
        return res

    def __and__(self, other: Rule | RuleGroup | Any):
        return RuleGroup.combine(self, other)

    def __rand__(self, other: Rule | RuleGroup | Any):
        # Order matters since it's a list
        return RuleGroup.combine(other, self)

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

    _constraint: RGC | None = None  # type: ignore
    _n_rules: int = 0
    # NOTE: _key is needed here to save nesting information from operations like `&` with a `dict`
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
        self._constraint = constraint

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
            self._n_rules += item._n_rules
        else:
            self._n_rules += 1
        super().append(item)

    def extend(self, item: RuleGroup | Iterable[Rule | RuleGroup | Callable]):
        if isinstance(item, RuleGroup):
            self._n_rules += item._n_rules
        super().extend(item)

    @staticmethod
    def combine(
        first: Rule | RuleGroup,
        other: Rule | RuleGroup | Any,
        constraint: RGC = RGC.ALL_RULES,
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

    def __call__(
        self, source: Any, *args
    ) -> Ok[tuple[RuleGroup, Any, RuleGroup]] | Err[tuple[RuleGroup, Any, RuleGroup]]:
        """
        Calls all embedded `Rule | RuleGroup`s and returns a corresponding result.

        This will return one of the tuple variants:
          - Ok((current RuleGroup, input, passed RuleGroup))
          - Err((current RuleGroup, input, failed RuleGroup))

        If the `RuleGroup` is nested: the returned `RuleGroup` will maintain the original structure.

        If there are nested `RuleGroup`s, then expect the inner-most failed `RuleGroup` to have
          discrete information on why the case failed (i.e. the original nested structure is kept).

          E.g. for a RuleGroup of: `[A, B, [C, D, E]]` and `B, D, E` fails,
            expect: `Err([B, [D, E]])` as the return value.

            I.e., we know that Rule `B` and RuleGroup `[C, D, E]` failed with `[D, E]` specifically
            (and that information is retained as a RuleGroup)

          E.g. for a RuleGroup of: `[A, [B, [C, D, E]]]` and `B, D, E` fails,
            expect: `Err([[B, [D, E]]])` as the return value
        """
        # Apply key unnesting logic
        # NOTE: only when source is a dict. Design choice!
        if isinstance(source, dict) and self._key:
            curr_source = get(source, self._key)
        else:
            curr_source = source

        # Run each rule and save results
        # NOTE: This nests results in a RuleGroup by default. For recursive calls, we'll unnest this below
        rg_passed, rg_failed = RuleGroup(constraint=RGC.ALL_RULES), RuleGroup(
            constraint=RGC.ALL_RULES
        )
        for curr_item in self:
            # Handle list key case -- have each embedded item run over the entire list
            if self._key and "[*]" == self._key:
                curr_item._key = f"{curr_item._key}[*]" if curr_item._key else "[*]"
            # Run the rule(s)
            curr_res = curr_item(curr_source, *args)
            # Handle the different cases
            match curr_item, curr_res:
                case (Rule(), Ok()):
                    rg_passed.append(curr_item)
                case (Rule(), Err()):
                    rg_failed.append(curr_item)
                case (RuleGroup(), Ok()):
                    rg_passed.append(curr_res.ok_value[-1])
                case (RuleGroup(), Err()):
                    rg_failed.append(curr_res.err_value[-1])
                case _:
                    raise RuntimeError(
                        f"Unexpected type or result: {type(curr_item)}, {type(curr_res)}"
                    )

        ## Check for failed required rules -- return Err early if so
        if _contains_required_rule(rg_failed):
            return Err((self, source, rg_failed))

        # Check result and return
        res: Ok[RuleGroup] | Err[RuleGroup]
        # NOTE: `_n_rules` is the total number of discrete rules (including nested).
        #      Thus, for `ALL_RULES` we check every rule, and `AT_LEAST_x` we check top-level groups
        passed_case = (self, source, rg_passed)
        failed_case = (self, source, rg_failed)
        match self._constraint:
            case RGC.ALL_RULES:
                # TODO make less strict -- if group sizes match
                #   ... consider `ALL_RULES` -> `ALL` rename, so avoid pedantic case...
                res = Ok(passed_case) if rg_passed._n_rules == self._n_rules else Err(failed_case)
            case RGC.AT_LEAST_ONE | RGC.AT_LEAST_TWO | RGC.AT_LEAST_THREE:
                res = (
                    Ok(passed_case)
                    if len(rg_passed) >= self._constraint.value
                    else Err(failed_case)
                )
            case RGC.ALL_REQUIRED_RULES:
                # Since we have above check for required rules, we know all rules have passed here
                res = Ok(passed_case)
            case RGC.ALL_WHEN_DATA_PRESENT:
                # For each failed rule, check if data was present. If so, return `Err`
                res = (
                    Ok(passed_case)
                    if not _rulegroup_applies(rg_failed, curr_source)
                    else Err(failed_case)
                )
            case _:
                raise RuntimeError(f"Unsupported RuleGroup constraint: {self._constraint}")
        return res

    def __hash__(self):
        return hash(tuple(self))

    def __repr__(self) -> str:
        return f"<RuleGroup> {[r for r in self]}"

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


def _contains_required_rule(rg: RuleGroup | Rule) -> bool:
    """
    Returns `True` if `rg` contains at least 1 Rule with required constraint
    """
    if isinstance(rg, Rule):
        return rg._constraint == RC.REQUIRED
    return any(_contains_required_rule(r) for r in rg)


def _rulegroup_applies(rg: RuleGroup | Rule, source: dict[str, Any]) -> bool:
    """
    Returns `True` if `rg` applies to _any_ part of `source`
      This is mainly to handle the `ALL_WHEN_DATA_PRESENT` logic

    If there is no key-level data in `rg`, then conservatively assume there is overlap
    """
    if isinstance(rg, Rule):
        return rg._key is None or get(source, rg._key) is not None
    return any(_rulegroup_applies(r, source) for r in rg)


def _list_to_rulegroup(
    l: list[Callable | Rule | RuleGroup | dict | list], key_prefix: str | None = None
) -> RuleGroup | Rule:
    """
    Given a list, compress it into a single `RuleGroup`
      (or for the `[*]` case: a `Rule` containing a single `RuleGroup`)

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
                # Wrap in a `Rule` so the `[*]` logic is applied
                res = Rule(it, at_key=at_key)  # type: ignore
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
        match v:
            case Rule() | RuleGroup():
                v._key = k
                res.append(v)
            case dict():
                res.append(Rule.init_specific(dict, at_key=k))
                res.append(_dict_to_rulegroup(v, key_prefix=k))
            case list():
                res.append(Rule.init_specific(list, at_key=k))
                res.append(_list_to_rulegroup(v, key_prefix=k))
            case _:
                if callable(v):
                    res.append(Rule.init_specific(v, at_key=k))
                else:
                    # Exact value check
                    res.append(Rule(p.equals(v), at_key=k))
    return res
