from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Collection

from result import Err, Ok

from .rules import Rule, RuleGroup


@dataclass
class PipeState:
    idx: int
    fn: Callable
    fn_input: dict[str, Any]
    fn_res: Any | None


class Pipe(tuple):
    _saved_states: dict[int, Any] | None = None  # For int `idx`, saves state _after_ the run
    _last_idx: int | None = None
    _last_input: Any | None = None

    def __new__(self, items: Collection[Callable | Rule | RuleGroup]):
        """
        Each callable in `items` is expected to return _one_ output and have no "side effects".

        All callables will feed the output into the next callable,
          EXCEPT for `Rule` and `RuleGroup`s: those will take the current input,
          and allow the current input to pass if `Ok` else stop execution on `Err`
        """
        # All things should be callable
        for it in items:
            if not callable(it):
                raise ValueError(f"Error: all items in a Pipe need to be callable, got f{type(it)}")
        return super().__new__(self, items)

    def __init__(self, _items: Collection[Callable | Rule | RuleGroup]):
        self._saved_states = dict()

    def __call__(self, *args: Any, **kwargs: Any) -> Ok[Any] | Err[PipeState]:
        """
        Runs all stored items in-order. On an `Err`, save the last reached state

        Each function is expected to only
        """
        if len(self) == 0:
            raise RuntimeError("Pipe is empty -- nothing to run!")
        # Run using all args/kwargs until first non-Rule/RuleGroup finishes
        idx = 0
        while isinstance(self[idx], Rule) or isinstance(self[idx], RuleGroup):
            try:
                rule_res = self[idx](*args, **kwargs)
                if isinstance(rule_res, Ok):
                    idx += 1
                    continue
                else:
                    raise RuntimeError(f"Expected `Ok`, got: {rule_res}")
            except Exception as e:
                return self._serialize_err(e, self[idx], idx, {"args": args, "kwargs": kwargs})
        # Ok. At this point, should be at the first executable rule
        first_it = True
        res = None
        remaining_items = self[idx:]
        for it in remaining_items:
            if first_it:
                try:
                    res = it(*args, **kwargs)
                    first_it = False
                except Exception as e:
                    return self._serialize_err(e, it, idx, {"res": res})
            else:
                try:
                    if isinstance(res, Ok):
                        res = res.unwrap()
                    # If it's a `Rule`/`RuleGroup`, check the condition, but don't update the result
                    if isinstance(it, Rule) or isinstance(it, RuleGroup):
                        rule_res = it(res)
                        if isinstance(rule_res, Err):
                            raise RuntimeError(f"Expected `Ok`, got: {rule_res}")
                    else:
                        res = it(res)
                except Exception as e:
                    return self._serialize_err(e, it, idx, {"res": res})
            # increment index for debugging
            idx += 1
        # Return current `Ok`/`Err` wrapper if applicable, else go off of truthy/falsy values
        if isinstance(res, Ok) or isinstance(res, Err):
            return res
        elif res:
            return Ok(res)
        else:
            return Err(PipeState(idx, self[-1], {}, res))

    def run(self, kwargs: dict[str, Any], from_step_idx: int = 0) -> Ok[Any] | Err[Any]:
        if from_step_idx == 0:
            return self.__call__(**kwargs)
        else:
            raise RuntimeError("TODO: actually use `_saved_states` to run from last state!")

    def reset(self) -> None:
        self._saved_states = dict()
        self._last_idx = None
        self._last_input = None

    def _serialize_err(
        self, e: Exception, fn: Callable, idx: int, arg_dict: dict[str, Any]
    ) -> Err[PipeState]:
        self._last_idx = idx
        self._last_input = arg_dict
        return Err(PipeState(idx, fn, arg_dict, None))


# Handles combinations
# The `Mapper` equivalent, but for Pipers!
# - Handles multithreading?
# - Runtime state management
# - Stream different inputs? From different inputs
#   ... like an interface engine?
class Piper:
    """
    A class for running a series of `Pipe` objects.

    E.g.
    {
        Pipe(...): {
            Ok: Pipe(...),
            Err: Pipe(...)
        }
    }

    TODO: Add runtime settings -- e.g. streaming, threads, parallelism, state management, etc.
    """

    _runtime_dict: dict | None = None

    # _stream_data: bool = False
    # _n_threads: int = 1
    # _parallelize: bool = False

    def __init__(self, runtime_dict: dict[Pipe, dict[type, Pipe | Callable]]):
        # TODO: validate fields are as expected
        # TODO: Figure-out type hint for extra nested data
        # ... can't use validation lib since keys aren't `str`. That's fine!
        # Since this is recursive in nature: check only
        #   - init layer keys (Pipe | Piper)
        #   - init layer vals (dict)
        #   - next layer keys (i.e. Ok|Err for dict[Ok|Err, ...])
        if runtime_dict:
            self._runtime_dict = runtime_dict
        else:
            self._runtime_dict = dict()

    def __call__(self, *args: Any, **kwargs: Any) -> dict[Pipe | Piper, Ok | Err]:
        # TODO: Go through the runtime dict
        # TODO: figure-out multi-nested runs. Here, just do one nesting!
        res = {}
        for pfn, vdict in self._runtime_dict.items():  # type: ignore
            # Run the key fn, check output, and then run the val fn
            pfn_res = pfn(*args, **kwargs)
            if isinstance(pfn_res, Ok):
                res[pfn] = vdict[Ok](pfn_res)
            elif isinstance(pfn_res, Err):
                res[pfn] = vdict[Err](pfn_res)
        return res
