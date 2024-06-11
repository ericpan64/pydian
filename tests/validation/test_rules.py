from copy import deepcopy

from result import Err, Ok

import pydian.partials as p
from pydian.validation import (  # InRange,; NotRequired,
    RGC,
    IsRequired,
    IsType,
    Rule,
    RuleGroup,
)


def test_rule() -> None:
    # Test an individual rule -- make sure call -> `Result`
    is_str = Rule(lambda x: isinstance(x, str))
    SOME_STR = "Abc"
    SOME_INT = 1
    assert is_str(SOME_STR) == Ok(True)
    assert isinstance(is_str(SOME_INT), Err)

    # Test exceptions
    # NOTE: intentionally swapped order of checks below to test exception case
    is_nonempty_list = Rule(lambda x: len(x) > 0 and isinstance(x, list))
    SOME_NONEMPTY_LIST = [True]
    assert is_nonempty_list(SOME_NONEMPTY_LIST) == Ok(True)
    assert isinstance(is_nonempty_list(SOME_INT), Err)  # Exception case: `len` called in `int`


def test_rulegroup() -> None:
    # Test `RuleGroup` consecutive calls
    # NOTE: the more specific the rule the better (while also keeping it simple, silly)
    #   Have the right-level of property checking for rules is a key design choice
    #   Rules should be _independent_ and _unordered_ -- i.e. order of execution not guaranteed
    is_str = Rule(lambda x: isinstance(x, str))
    is_nonempty = Rule(lambda x: len(x) > 0)
    starts_with_upper = Rule(lambda x: x[0].isupper())

    all_rules = [is_str, is_nonempty, starts_with_upper]
    rs_all = RuleGroup(all_rules)
    rs_one = RuleGroup(all_rules, constraint=RGC.ONE_OF)
    rs_two = RuleGroup(all_rules, constraint=RGC.TWO_OF)
    rs_three = RuleGroup(all_rules, constraint=RGC.THREE_OF)

    PASS_ALL_STR = "Abc"
    PASS_ONE_STR = ""
    PASS_TWO_STR = "abc"
    PASS_NONE = 42

    # Test `RuleGroup`
    assert rs_all(PASS_ALL_STR) == Ok(all_rules)
    assert rs_all(PASS_ONE_STR) == Err([is_nonempty, starts_with_upper])
    assert rs_all(PASS_TWO_STR) == Err([starts_with_upper])
    assert rs_all(PASS_NONE) == Err(all_rules)

    assert rs_one(PASS_ALL_STR) == Ok(all_rules)
    assert rs_one(PASS_ONE_STR) == Ok([is_str])
    assert rs_one(PASS_TWO_STR) == Ok([is_str, is_nonempty])
    assert rs_one(PASS_NONE) == Err(all_rules)

    assert rs_two(PASS_ALL_STR) == Ok(all_rules)
    assert rs_two(PASS_ONE_STR) == Err([is_nonempty, starts_with_upper])
    assert rs_two(PASS_TWO_STR) == Ok([is_str, is_nonempty])
    assert rs_two(PASS_NONE) == Err(all_rules)

    # same as `rs_all`
    assert rs_three(PASS_ALL_STR) == Ok(all_rules)
    assert rs_three(PASS_ONE_STR) == Err([is_nonempty, starts_with_upper])
    assert rs_three(PASS_TWO_STR) == Err([starts_with_upper])
    assert rs_three(PASS_NONE) == Err(all_rules)


def test_nested_rulegroup() -> None:
    is_str = Rule(lambda x: isinstance(x, str))
    starts_with_upper = Rule(lambda x: x[0].isupper())
    is_not_list = Rule(lambda x: not isinstance(x, list))
    is_nonempty = Rule(lambda x: len(x) > 0)

    PASS_ALL_STR = "Abc"
    PASS_ONE_STR = "abc"
    PASS_NONE: list = []
    rs_str_nonempty = RuleGroup([is_str, is_nonempty])
    rs_notlist_upper = RuleGroup([is_not_list, starts_with_upper])

    # NOTE: returns groups of `RuleGroup`s within an outer `RuleGroup`.
    # TODO: decide on if this should keep RuleGroup nesting.
    #   Maybe add a fn to remove RuleGroup nesting separately? I.e. why lose info preemtively
    nested_rs = RuleGroup([rs_str_nonempty, rs_notlist_upper])
    res_rs_all_rules = RuleGroup([is_str, is_nonempty, is_not_list, starts_with_upper])

    assert nested_rs(PASS_ALL_STR) == Ok(res_rs_all_rules)
    assert nested_rs(PASS_ONE_STR) == Err(rs_notlist_upper)
    assert nested_rs(PASS_NONE) == Err(res_rs_all_rules)


def test_rule_constraint() -> None:
    is_str_required = Rule(lambda x: isinstance(x, str)) & IsRequired()
    is_nonempty = Rule(lambda x: len(x) > 0)

    # Test `Rule`.constraint
    rs_one = RuleGroup([is_str_required, is_nonempty], RGC.ONE_OF)
    PASS_IS_STR_REQ = ""
    PASS_NONEMPTY: list[int] = [1, 2, 3]

    # We pass the RuleGroup since condition is met, and all REQUIRED rules are included
    assert rs_one(PASS_IS_STR_REQ) == Ok([is_str_required])
    # We fail the RuleGroup since a REQUIRED rule does not pass
    assert rs_one(PASS_NONEMPTY) == Err([is_str_required])


def test_combine_rule() -> None:
    some_rule = Rule(p.gte(0))
    some_other_rule = Rule(p.lt(10))
    some_other_rulegroup = RuleGroup([some_rule, some_other_rule])

    # with Rules
    combined_r_r = some_rule & some_other_rule
    assert combined_r_r == RuleGroup([some_rule, some_other_rule])

    # with RuleGroup
    combined_r_rg = some_rule & some_other_rulegroup
    assert combined_r_rg == RuleGroup([some_rule, some_other_rulegroup])

    # with dict
    combined_r_dict = some_rule & {"A": Rule(p.gt(1)), "B": Rule(p.lt(2))}
    assert combined_r_dict == RuleGroup(
        [some_rule, IsType(dict), RuleGroup([Rule(p.gt(1), at_key="A"), Rule(p.lt(2), at_key="B")])]
    )

    # with list
    # NOTE: the list case is a bit weird, we only expect some kind of rules to join with it
    combined_r_list = IsRequired() & [str, bool]  # a list[str | bool]
    assert combined_r_list == RuleGroup(
        [
            IsRequired(),
            IsType(list),
            RuleGroup([IsType(str), IsType(bool)], RGC.ONE_OF),
        ]
    )

    # with callable
    combined_r_callable = some_rule & int
    assert combined_r_callable == RuleGroup([some_rule, IsType(int)])

    # with primitive
    combined_r_primitive = some_rule & Rule(p.equals(5))
    assert combined_r_primitive == RuleGroup([some_rule, p.equals(5)])


def test_combine_rulegroup() -> None:
    some_rulegroup = RuleGroup([Rule(p.not_equivalent(0)), int])
    some_other_rule = Rule(p.lt(10))
    some_other_rulegroup = RuleGroup([some_rulegroup, some_other_rule])

    # with Rules
    combined_rg_r = some_rulegroup & some_other_rule
    some_rg_copy = deepcopy(some_rulegroup)
    some_rg_copy.append(some_other_rule)
    assert combined_rg_r == some_rg_copy

    # with RuleGroup
    combined_rg_rg = some_rulegroup & some_other_rulegroup
    assert combined_rg_rg == RuleGroup([some_rulegroup, some_other_rulegroup])

    # with dict
    combined_rg_dict = some_rulegroup & {"A": Rule(p.gt(1)), "B": Rule(p.lt(2))}
    assert combined_rg_dict == RuleGroup(
        [
            some_rulegroup[0],
            some_rulegroup[1],
            IsType(dict),
            RuleGroup([Rule(p.gt(1), at_key="A"), Rule(p.lt(2), at_key="B")]),
        ]
    )

    # with list
    # NOTE: the list case is a bit weird, we only expect some kind of rules to join with it
    combined_rg_list = some_rulegroup & [str, bool]  # a list[str | bool]
    assert combined_rg_list == RuleGroup(
        [
            some_rulegroup[0],
            some_rulegroup[1],
            IsType(list),
            RuleGroup([IsType(str), IsType(bool)], RGC.ONE_OF),
        ]
    )

    # with callable
    combined_rg_callable = some_rulegroup & int
    some_rg_copy = deepcopy(some_rulegroup)
    some_rg_copy.append(int)
    assert combined_rg_callable == some_rg_copy

    # with primitive
    combined_r_primitive = some_rulegroup & Rule(p.equals(5))
    some_rg_copy = deepcopy(some_rulegroup)
    some_rg_copy.append(p.equals(5))
    assert combined_r_primitive == some_rg_copy
