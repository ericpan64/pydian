from result import Err, Ok

import pydian.partials as p
from pydian.validation import RC, Rule, RuleGroup
from pydian.validation.specific import (
    InRange,
    InSet,
    IsRequired,
    IsType,
    MaxCount,
    MinCount,
    NotRequired,
)


def test_is_required() -> None:
    # Base functionality
    r_is_required = IsRequired()
    assert isinstance(r_is_required("something"), Ok)
    assert isinstance(r_is_required(None), Err)

    # Adds the `REQUIRED` constraint
    is_str = Rule(p.isinstance_of(str))
    assert is_str._constraint is not RC.REQUIRED
    is_str_required = IsRequired() & is_str
    assert is_str_required._constraint

    # Test `NotRequired` while we're here!
    is_str_not_required = NotRequired() & is_str_required
    assert is_str_not_required._constraint is None

    # Works for a `RuleGroup`
    is_nonempty_str = RuleGroup([p.isinstance_of(str), p.not_equal("")])
    combined_rg: RuleGroup = IsRequired() & is_nonempty_str  # type: ignore
    assert combined_rg[0] == IsRequired()
    assert combined_rg[1] == is_nonempty_str

    # Works on arbitrary callable
    is_str_req = IsRequired() & str
    assert isinstance(is_str_req, Rule) and is_str_req._constraint is RC.REQUIRED


def test_is_type() -> None:
    is_str = IsType(str)
    assert isinstance(is_str("something"), Ok)
    assert isinstance(is_str(1), Err)

    class CustomType:
        pass

    is_custom = IsType(CustomType)
    assert isinstance(is_custom(CustomType()), Ok)
    assert isinstance(is_custom("abc"), Err)

    class ExtraCustomType(CustomType):
        pass

    is_extra_custom = IsType(ExtraCustomType)
    assert isinstance(is_extra_custom(ExtraCustomType()), Ok)
    assert isinstance(is_extra_custom(CustomType()), Err)
    assert isinstance(is_custom(ExtraCustomType()), Ok)


def test_length_rules() -> None:
    no_items = []  # type: ignore
    one_item = [1]
    two_items = [1, 2]
    ten_items = range(10)

    # MinCount
    min_2 = MinCount(2)
    assert isinstance(min_2(no_items), Err)
    assert isinstance(min_2(one_item), Err)
    assert isinstance(min_2(two_items), Ok)
    assert isinstance(min_2(ten_items), Ok)

    # MaxCount
    max_2 = MaxCount(2)
    assert isinstance(max_2(no_items), Ok)
    assert isinstance(max_2(one_item), Ok)
    assert isinstance(max_2(two_items), Ok)
    assert isinstance(max_2(ten_items), Err)

    # InRange
    in_range_2_4 = InRange(2, 4)
    assert isinstance(in_range_2_4(no_items), Err)
    assert isinstance(in_range_2_4(one_item), Err)
    assert isinstance(in_range_2_4(two_items), Ok)
    assert isinstance(in_range_2_4(range(3)), Ok)
    assert isinstance(in_range_2_4(range(5)), Err)
    assert isinstance(in_range_2_4(ten_items), Err)


def test_container_rules() -> None:
    # InSet
    in_set = InSet({1, 2, 3})
    assert isinstance(in_set(1), Ok)
    assert isinstance(in_set(2), Ok)
    assert isinstance(in_set(3), Ok)
    assert isinstance(in_set(4), Err)
    assert isinstance(in_set("a"), Err)
    assert isinstance(in_set(None), Err)
    assert isinstance(in_set([]), Err)
    assert isinstance(in_set({}), Err)
