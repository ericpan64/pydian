from copy import deepcopy

from result import Err, Ok

import pydian.partials as p
from pydian.validation import RC, RGC, Rule, RuleGroup
from pydian.validation.specific import IsRequired, IsType


def test_rule() -> None:
    # Test an individual rule -- make sure call -> `Result`
    is_str = Rule(lambda x: isinstance(x, str))
    SOME_STR = "Abc"
    SOME_INT = 1
    assert is_str(SOME_STR) == Ok((is_str, SOME_STR, True))
    assert isinstance(is_str(SOME_INT), Err)

    # Test exceptions
    # NOTE: intentionally swapped order of checks below to test exception case
    is_nonempty_list = Rule(lambda x: len(x) > 0 and isinstance(x, list))
    SOME_NONEMPTY_LIST = [True]
    assert is_nonempty_list(SOME_NONEMPTY_LIST) == Ok((is_nonempty_list, SOME_NONEMPTY_LIST, True))
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
    rg_all = RuleGroup(all_rules)
    rg_one = RuleGroup(all_rules, constraint=RGC.AT_LEAST_ONE)
    rg_two = RuleGroup(all_rules, constraint=RGC.AT_LEAST_TWO)
    rg_three = RuleGroup(all_rules, constraint=RGC.AT_LEAST_THREE)

    PASS_ALL_STR = "Abc"
    PASS_ONE_STR = ""
    PASS_TWO_STR = "abc"
    PASS_NONE = 42

    # Test `RuleGroup`
    assert rg_all(PASS_ALL_STR) == Ok((rg_all, PASS_ALL_STR, all_rules))
    assert rg_all(PASS_ONE_STR) == Err(
        (rg_all, PASS_ONE_STR, RuleGroup([is_nonempty, starts_with_upper]))
    )
    assert rg_all(PASS_TWO_STR) == Err((rg_all, PASS_TWO_STR, RuleGroup([starts_with_upper])))
    assert rg_all(PASS_NONE) == Err((rg_all, PASS_NONE, RuleGroup(all_rules)))

    assert rg_one(PASS_ALL_STR) == Ok((rg_one, PASS_ALL_STR, RuleGroup(all_rules)))
    assert rg_one(PASS_ONE_STR) == Ok((rg_one, PASS_ONE_STR, RuleGroup([is_str])))
    assert rg_one(PASS_TWO_STR) == Ok((rg_one, PASS_TWO_STR, RuleGroup([is_str, is_nonempty])))
    assert rg_one(PASS_NONE) == Err((rg_one, PASS_NONE, RuleGroup(all_rules)))

    assert rg_two(PASS_ALL_STR) == Ok((rg_two, PASS_ALL_STR, RuleGroup(all_rules)))
    assert rg_two(PASS_ONE_STR) == Err(
        (rg_two, PASS_ONE_STR, RuleGroup([is_nonempty, starts_with_upper]))
    )
    assert rg_two(PASS_TWO_STR) == Ok((rg_two, PASS_TWO_STR, RuleGroup([is_str, is_nonempty])))
    assert rg_two(PASS_NONE) == Err((rg_two, PASS_NONE, RuleGroup(all_rules)))

    # same as `rg_all`
    assert rg_three(PASS_ALL_STR) == Ok((rg_three, PASS_ALL_STR, RuleGroup(all_rules)))
    assert rg_three(PASS_ONE_STR) == Err(
        (rg_three, PASS_ONE_STR, RuleGroup([is_nonempty, starts_with_upper]))
    )
    assert rg_three(PASS_TWO_STR) == Err((rg_three, PASS_TWO_STR, RuleGroup([starts_with_upper])))
    assert rg_three(PASS_NONE) == Err((rg_three, PASS_NONE, RuleGroup(all_rules)))


def test_rule_at_key() -> None:
    data = {"first": "abc", "second": "Def"}
    is_str = Rule(lambda x: isinstance(x, str), at_key="first")
    is_nonempty = Rule(lambda x: len(x) > 0, at_key="first")
    starts_with_upper = Rule(lambda x: x[0].isupper(), at_key="second")

    rg = RuleGroup([is_str, is_nonempty, starts_with_upper])
    assert rg(data) == Ok((rg, data, RuleGroup([is_str, is_nonempty, starts_with_upper])))
    assert rg({"first": "abc"}) == Err((rg, {"first": "abc"}, RuleGroup([starts_with_upper])))
    assert rg({"second": "Def"}) == Err((rg, {"second": "Def"}, RuleGroup([is_str, is_nonempty])))
    assert rg({}) == Err((rg, {}, RuleGroup([is_str, is_nonempty, starts_with_upper])))


def test_rulegroup_at_key() -> None:
    data = {"some_layer": {"first": "abc", "second": "Def"}}
    is_str = Rule(lambda x: isinstance(x, str), at_key="first")
    is_nonempty = Rule(lambda x: len(x) > 0, at_key="first")
    starts_with_upper = Rule(lambda x: x[0].isupper(), at_key="second")

    rg = RuleGroup([is_str, is_nonempty, starts_with_upper], at_key="some_layer")
    assert rg(data) == Ok((rg, data, RuleGroup([is_str, is_nonempty, starts_with_upper])))
    assert rg({"some_layer": {"first": "abc"}}) == Err(
        (rg, {"some_layer": {"first": "abc"}}, RuleGroup([starts_with_upper]))
    )
    assert rg({"some_layer": {"second": "Def"}}) == Err(
        (rg, {"some_layer": {"second": "Def"}}, RuleGroup([is_str, is_nonempty]))
    )
    assert rg({"first": "abc", "second": "Def"}) == Err(
        (rg, {"first": "abc", "second": "Def"}, RuleGroup([is_str, is_nonempty, starts_with_upper]))
    )


def test_rulegroup_constraint() -> None:
    # Test `RuleGroup` constraints (last rule is changed to required to do this)
    is_str = Rule(lambda x: isinstance(x, str))
    starts_with_upper_required = Rule(lambda x: x[0].isupper(), RC.REQUIRED)  # Marked as REQUIRED
    contains_digit = Rule(lambda x: any(c.isdigit() for c in x) if x else False)

    all_rules = [is_str, contains_digit, starts_with_upper_required]

    PASS_ALL_STR = "Abc123"
    PASS_ONE_STR = ""
    PASS_TWO_STR = "Abc"  # Passes the required rule
    PASS_NONE = False

    rg_all = RuleGroup(all_rules, RGC.ALL)
    assert rg_all(PASS_ALL_STR) == Ok((rg_all, PASS_ALL_STR, RuleGroup(all_rules)))
    assert rg_all(PASS_ONE_STR) == Err(
        (rg_all, PASS_ONE_STR, RuleGroup([contains_digit, starts_with_upper_required]))
    )
    assert rg_all(PASS_TWO_STR) == Err((rg_all, PASS_TWO_STR, RuleGroup([contains_digit])))
    assert rg_all(PASS_NONE) == Err((rg_all, PASS_NONE, RuleGroup(all_rules)))

    rg_all_required = RuleGroup(all_rules, RGC.ALL_REQUIRED_RULES)
    assert rg_all_required(PASS_ALL_STR) == Ok(
        (rg_all_required, PASS_ALL_STR, RuleGroup(all_rules))
    )
    assert rg_all_required(PASS_ONE_STR) == Err(
        (rg_all_required, PASS_ONE_STR, RuleGroup([contains_digit, starts_with_upper_required]))
    )
    assert rg_all_required(PASS_TWO_STR) == Ok(
        (rg_all_required, PASS_TWO_STR, RuleGroup([is_str, starts_with_upper_required]))
    )
    assert rg_all_required(PASS_NONE) == Err((rg_all_required, PASS_NONE, RuleGroup(all_rules)))

    # # NOTE: Required rules will cause the `RuleGroup` to fail if they don't pass
    rg_at_least_one = RuleGroup(all_rules, RGC.AT_LEAST_ONE)
    assert rg_at_least_one(PASS_ALL_STR) == Ok(
        (rg_at_least_one, PASS_ALL_STR, RuleGroup(all_rules))
    )
    assert rg_at_least_one(PASS_ONE_STR) == Err(
        (rg_at_least_one, PASS_ONE_STR, RuleGroup([contains_digit, starts_with_upper_required]))
    )
    assert rg_at_least_one(PASS_TWO_STR) == Ok(
        (rg_at_least_one, PASS_TWO_STR, RuleGroup([is_str, starts_with_upper_required]))
    )
    assert rg_at_least_one(PASS_NONE) == Err((rg_at_least_one, PASS_NONE, RuleGroup(all_rules)))

    rg_at_least_two = RuleGroup(all_rules, RGC.AT_LEAST_TWO)
    assert rg_at_least_two(PASS_ALL_STR) == Ok(
        (rg_at_least_two, PASS_ALL_STR, RuleGroup(all_rules))
    )
    assert rg_at_least_two(PASS_ONE_STR) == Err(
        (rg_at_least_two, PASS_ONE_STR, RuleGroup([contains_digit, starts_with_upper_required]))
    )
    assert rg_at_least_two(PASS_TWO_STR) == Ok(
        (rg_at_least_two, PASS_TWO_STR, RuleGroup([is_str, starts_with_upper_required]))
    )
    assert rg_at_least_two(PASS_NONE) == Err((rg_at_least_two, PASS_NONE, RuleGroup(all_rules)))

    rg_at_least_three = RuleGroup(all_rules, RGC.AT_LEAST_THREE)
    assert rg_at_least_three(PASS_ALL_STR) == Ok(
        (rg_at_least_three, PASS_ALL_STR, RuleGroup(all_rules))
    )
    assert rg_at_least_three(PASS_ONE_STR) == Err(
        (rg_at_least_three, PASS_ONE_STR, RuleGroup([contains_digit, starts_with_upper_required]))
    )
    assert rg_at_least_three(PASS_TWO_STR) == Err(
        (rg_at_least_three, PASS_TWO_STR, RuleGroup([contains_digit]))
    )
    assert rg_at_least_three(PASS_NONE) == Err((rg_at_least_three, PASS_NONE, RuleGroup(all_rules)))

    # Failing the required rule should always result in a fail
    PASS_TWO_STR_FAIL_REQ = "abc123"
    assert rg_all(PASS_TWO_STR_FAIL_REQ) == Err(
        (rg_all, PASS_TWO_STR_FAIL_REQ, RuleGroup([starts_with_upper_required]))
    )
    assert rg_all_required(PASS_TWO_STR_FAIL_REQ) == Err(
        (rg_all_required, PASS_TWO_STR_FAIL_REQ, RuleGroup([starts_with_upper_required]))
    )
    assert rg_at_least_one(PASS_TWO_STR_FAIL_REQ) == Err(
        (rg_at_least_one, PASS_TWO_STR_FAIL_REQ, RuleGroup([starts_with_upper_required]))
    )
    assert rg_at_least_two(PASS_TWO_STR_FAIL_REQ) == Err(
        (rg_at_least_two, PASS_TWO_STR_FAIL_REQ, RuleGroup([starts_with_upper_required]))
    )
    assert rg_at_least_three(PASS_TWO_STR_FAIL_REQ) == Err(
        (rg_at_least_three, PASS_TWO_STR_FAIL_REQ, RuleGroup([starts_with_upper_required]))
    )


def test_rulegroup_constraint_when_data_present() -> None:
    """
    Tests applying a rule at a specific key
    """
    data = {"first": "abc", "second": "Def"}
    is_str = Rule(lambda x: isinstance(x, str), at_key="first")
    starts_with_upper = Rule(lambda x: x[0].isupper(), at_key="second")

    all_rules = [is_str, starts_with_upper]

    # Test key applied at rule level
    rg_when_present = RuleGroup(all_rules, RGC.ALL_WHEN_DATA_PRESENT)
    assert rg_when_present(data) == Ok((rg_when_present, data, rg_when_present))
    assert rg_when_present({"first": "abc"}) == Ok(
        (rg_when_present, {"first": "abc"}, RuleGroup([is_str]))
    )
    assert rg_when_present({"second": "Def"}) == Ok(
        (rg_when_present, {"second": "Def"}, RuleGroup([starts_with_upper]))
    )
    assert rg_when_present({}) == Ok((rg_when_present, {}, RuleGroup([])))

    # Test outer-most key applied at RuleGroup level (with each Rule also having keys specified)
    rg_with_missing_key = RuleGroup(all_rules, RGC.ALL_WHEN_DATA_PRESENT, at_key="third")
    assert rg_with_missing_key(data) == Ok((rg_with_missing_key, data, RuleGroup([])))
    assert rg_with_missing_key({"first": "abc"}) == Ok(
        (rg_with_missing_key, {"first": "abc"}, RuleGroup([]))
    )
    assert rg_with_missing_key({"second": "Def"}) == Ok(
        (rg_with_missing_key, {"second": "Def"}, RuleGroup([]))
    )
    # TODO: This case seems weird -- the first key hits, and then the second two miss due to types
    #       ... right now it'll pass since the data get "misses". Though issue with type check. Hm.
    assert rg_with_missing_key({"third": "Ghi"}) == Ok(
        (rg_with_missing_key, {"third": "Ghi"}, RuleGroup([is_str, starts_with_upper]))
    )
    assert rg_with_missing_key({}) == Ok((rg_with_missing_key, {}, RuleGroup([])))


def test_nested_rulegroup() -> None:
    """
    Test return structure when a `RuleGroup` contains a nested `RuleGroup`

    See the `__call__` docstring on `RuleGroup`
    """
    is_str = Rule(lambda x: isinstance(x, str))
    starts_with_upper = Rule(lambda x: x[0].isupper())
    is_nonempty = Rule(lambda x: len(x) > 0)

    PASS_ALL_STR = "Abc"
    PASS_ONE_STR = "abc"
    PASS_NONE = ""

    # All rules are `RGC.ALL` by default
    rg_str_nonempty = RuleGroup([is_str, is_nonempty])
    rg_notlist_upper = RuleGroup([is_nonempty, starts_with_upper])

    # NOTE: returns groups of `RuleGroup`s within an outer `RuleGroup`.
    nested_rg = RuleGroup([rg_str_nonempty, rg_notlist_upper], RGC.AT_LEAST_ONE)
    expected_err_rg = RuleGroup([RuleGroup([is_nonempty]), rg_notlist_upper], RGC.ALL)

    assert nested_rg(PASS_ALL_STR) == Ok(
        (nested_rg, PASS_ALL_STR, RuleGroup([rg_str_nonempty, rg_notlist_upper]))
    )
    assert nested_rg(PASS_ONE_STR) == Ok((nested_rg, PASS_ONE_STR, RuleGroup([rg_str_nonempty])))
    assert nested_rg(PASS_NONE) == Err((nested_rg, PASS_NONE, expected_err_rg))


def test_rule_constraint() -> None:
    """
    Test `Rule`-level constraint (`RC`)
    """
    is_str_required = Rule(lambda x: isinstance(x, str)) & IsRequired()
    is_nonempty = Rule(lambda x: len(x) > 0)

    # Test `Rule`.constraint
    rg_one = RuleGroup([is_str_required, is_nonempty], RGC.AT_LEAST_ONE)
    PASS_IS_STR_REQ = ""
    PASS_NONEMPTY: list[int] = [1, 2, 3]

    # We pass the RuleGroup since condition is met, and all REQUIRED rules are included
    assert rg_one(PASS_IS_STR_REQ) == Ok((rg_one, PASS_IS_STR_REQ, RuleGroup([is_str_required])))
    # We fail the RuleGroup since a REQUIRED rule does not pass
    assert rg_one(PASS_NONEMPTY) == Err((rg_one, PASS_NONEMPTY, RuleGroup([is_str_required])))


def test_combine_rule() -> None:
    """
    Test the `&` operator
    """
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
            RuleGroup([IsType(str), IsType(bool)], RGC.AT_LEAST_ONE),
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
    assert combined_rg_r == RuleGroup([some_rg_copy, some_other_rule])

    # with RuleGroup
    combined_rg_rg = some_rulegroup & some_other_rulegroup
    assert combined_rg_rg == RuleGroup([some_rulegroup, some_other_rulegroup])

    # with dict
    combined_rg_dict = some_rulegroup & {"A": Rule(p.gt(1)), "B": Rule(p.lt(2))}
    assert combined_rg_dict == RuleGroup(
        [
            some_rulegroup,
            IsType(dict),
            RuleGroup([Rule(p.gt(1), at_key="A"), Rule(p.lt(2), at_key="B")]),
        ]
    )

    # with list
    # NOTE: the list case is a bit weird, we only expect some kind of rules to join with it
    combined_rg_list = some_rulegroup & [str, bool]  # a list[str | bool]
    assert combined_rg_list == RuleGroup(
        [
            some_rulegroup,
            IsType(list),
            RuleGroup([IsType(str), IsType(bool)], RGC.AT_LEAST_ONE),
        ]
    )

    # with callable
    combined_rg_callable = some_rulegroup & int
    some_rg_copy = deepcopy(some_rulegroup)
    assert combined_rg_callable == RuleGroup([some_rg_copy, IsType(int)])

    # with primitive
    combined_r_primitive = some_rulegroup & Rule(p.equals(5))
    some_rg_copy = deepcopy(some_rulegroup)
    assert combined_r_primitive == RuleGroup([some_rg_copy, Rule(p.equals(5))])
