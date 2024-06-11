from result import Err, Ok

import pydian.partials as p
from pydian.pipelines.pipes import Pipe, Piper, PipeState
from pydian.validation import IsType


def test_pipe() -> None:
    STR_PASS = "Hello!"
    INT_FAIL = 10
    triple_str = Pipe([IsType(str), p.multiply(3), IsType(str)])

    assert triple_str(STR_PASS) == Ok(STR_PASS * 3)
    assert isinstance(triple_str(INT_FAIL), Err)

    # # Example of throwing error at end
    # triple_str_err_end = triple_str & IsType(int)

    # # Example of throwing error at start
    # triple_str_err_start = IsType(int) + triple_str


def test_piper() -> None:
    # Ok. A `Piper` is really a series of `Pipe`s put together into a dict.
    #   And some extra config on runtime.
    #   Expect each `Pipe` to be functional

    STR_PASS = " Hello! "
    INT_FAIL = 10
    triple_str = Pipe([IsType(str), p.multiply(3), IsType(str)])
    clean_str = Pipe([IsType(str), p.do(str.strip)])
    str_pipeline = Piper(
        {
            triple_str: {
                # The value here is some "terminal" callable
                # If we just want to return, `lambda x: x` (or `p.echo`)
                Ok: Ok.unwrap,
                Err: p.echo,
            },
            clean_str: {Ok: p.echo, Err: Err.unwrap_err},
        }
    )

    assert str_pipeline(STR_PASS) == {triple_str: STR_PASS * 3, clean_str: Ok(STR_PASS.strip())}

    assert str_pipeline(INT_FAIL) == {
        triple_str: Err(PipeState(0, IsType(str), {"args": (INT_FAIL,), "kwargs": {}}, None)),
        clean_str: PipeState(0, IsType(str), {"args": (INT_FAIL,), "kwargs": {}}, None),
    }
