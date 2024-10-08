from copy import deepcopy
from typing import Any

import pydian.partials as p


def test_get(simple_data: dict[str, Any]) -> None:
    source = simple_data

    FAIL_DEFAULT_STR = "n/a"

    assert p.get("data.patient.id", apply=str.upper)(source) == str.upper(
        source["data"]["patient"]["id"]
    )
    assert p.get("something_not_there", default=FAIL_DEFAULT_STR, apply=str.upper)(
        source
    ) == str.upper(FAIL_DEFAULT_STR)
    assert p.get("something_not_there", apply=str.upper)(source) is None


def test_do() -> None:
    EXAMPLE_STR = "Some String"
    EXAMPLE_INT = 100

    # Test passing args or kwargs
    def some_function(first: str, second: int) -> str:
        return f"{first}, {second}!"

    kwargs = {"second": EXAMPLE_INT}  # Passes in any order
    str_param_fn_1 = p.do(
        some_function, EXAMPLE_INT
    )  # Partially applies starting at second parameter
    str_param_fn_2 = p.do(some_function, **kwargs)
    assert some_function("Ma", EXAMPLE_INT) == str_param_fn_1("Ma") == str_param_fn_2("Ma")

    # Test passing args and kwargs
    def other_function(
        first: str, second: int, third: bool, fourth: list[str] = [], fifth: set[int] = set()
    ) -> str:
        return f"{first}, {second}, {third}? {fourth}, {fifth}!"

    other_str_fn_1 = p.do(
        other_function, EXAMPLE_INT, True, [EXAMPLE_STR], **{"fifth": {EXAMPLE_INT}}
    )
    other_str_fn_2 = p.do(
        other_function, EXAMPLE_INT, True, **{"fourth": [EXAMPLE_STR], "fifth": {EXAMPLE_INT}}
    )
    other_str_fn_3 = p.do(
        other_function,
        EXAMPLE_INT,
        **{"third": True, "fourth": [EXAMPLE_STR], "fifth": {EXAMPLE_INT}},
    )
    other_str_fn_4 = p.do(other_function, EXAMPLE_INT, **{"third": True, "fifth": {EXAMPLE_INT}})
    other_str_fn_5 = p.do(other_function, EXAMPLE_INT, **{"third": True, "fourth": [EXAMPLE_STR]})
    assert (
        other_function(EXAMPLE_STR, EXAMPLE_INT, True, [EXAMPLE_STR], {EXAMPLE_INT})
        == other_str_fn_1(EXAMPLE_STR)
        == other_str_fn_2(EXAMPLE_STR)
        == other_str_fn_3(EXAMPLE_STR)
    )
    assert other_function(EXAMPLE_STR, EXAMPLE_INT, True, [], {EXAMPLE_INT}) == other_str_fn_4(
        EXAMPLE_STR
    )
    assert other_function(EXAMPLE_STR, EXAMPLE_INT, True, [EXAMPLE_STR], set()) == other_str_fn_5(
        EXAMPLE_STR
    )

    # Test stdlib wrappers
    assert p.do(str.replace, "S", "Z")(EXAMPLE_STR) == EXAMPLE_STR.replace("S", "Z")
    assert p.do(str.startswith, "S")(EXAMPLE_STR) == EXAMPLE_STR.startswith("S")
    assert p.do(str.endswith, "S")(EXAMPLE_STR) == EXAMPLE_STR.endswith("S")


def test_generic_apply_wrappers() -> None:
    n = 100
    assert p.add(1)(n) == n + 1
    assert p.subtract(1)(n) == n - 1
    assert p.subtract(1, before=True)(n) == 1 - n
    assert p.multiply(10)(n) == n * 10
    assert p.divide(10)(n) == n / 10
    assert p.divide(10, before=True)(n) == 10 / n

    l = [1, 2, 3]
    assert p.add([4])(l) == l + [4]
    assert p.add([4], before=True)(l) == [4] + l

    f = 4.2
    assert p.multiply(3)(f) == 3 * f
    assert p.multiply(3, before=True)(f * f) == (f * f) * 3


def test_generic_conditional_wrappers() -> None:
    value = {"a": "b", "c": "d"}
    copied_value = deepcopy(value)
    example_key = "a"

    assert p.equals(copied_value)(value) == (value == copied_value)
    assert p.not_equal(copied_value)(value) == (value != copied_value)
    assert p.equivalent(copied_value)(value) == (value is copied_value)
    assert p.not_equivalent(copied_value)(value) == (value is not copied_value)
    assert p.contains(example_key)(copied_value) == (example_key in value)
    assert p.not_contains(example_key)(copied_value) == (example_key not in value)
    assert p.contained_in(copied_value)(example_key) == (example_key in value)
    assert p.not_contained_in(copied_value)(example_key) == (example_key not in value)
    assert p.isinstance_of(dict)(value) == isinstance(value, dict)
    assert p.isinstance_of(str)(example_key) == isinstance(example_key, str)


def test_iterable_wrappers() -> None:
    supported_iterables = ([1, 2, 3, 4, 5], (1, 2, 3, 4, 5))
    for value in supported_iterables:
        assert p.keep(1)(value) == value[:1]
        assert p.keep(50)(value) == value[:50]
        assert p.index(0)(value) == value[0]
        assert p.index(1)(value) == value[1]
        assert p.index(-1)(value) == value[-1]
        assert p.index(-3)(value) == value[-3]


def test_stdlib_wrappers() -> None:
    EXAMPLE_LIST = ["a", "b", "c"]
    assert p.map_to_list(str.upper)(EXAMPLE_LIST) == ["A", "B", "C"]
    assert p.filter_to_list(p.equals("a"))(EXAMPLE_LIST) == ["a"]
