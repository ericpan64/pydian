import traceback
from typing import Any

from .dicts import drop_keys, impute_enum_values
from .globs import SharedMapperState, _Global_Mapper_State_Dict
from .lib.types import DROP, KEEP, MappingFunc
from .lib.util import encode_stack_trace, get_keys_containing_class, remove_empty_values


class Mapper:
    def __init__(
        self,
        map_fn: MappingFunc,
        remove_empty: bool = True,
        strict: bool = False,
    ) -> None:
        self.map_fn = map_fn
        self.remove_empty = remove_empty
        self.strict = strict
        self.global_mapper_call_id: str | None = None
        self.global_mapper_call_level: int | None = None

    def _register_mapper_call_id(self) -> None:
        """
        A `mapper_call_id` is uniquely identified by the current stack trace.
          This is so subsequence function calls can identify it is part of the mapper.
        """
        # The stack trace, excluding this level and `__call__`
        curr_trace = traceback.format_stack()[:-2]
        self.global_mapper_call_id = encode_stack_trace(curr_trace)
        self.global_mapper_call_level = len(curr_trace)
        # Update global state
        _Global_Mapper_State_Dict[self.global_mapper_call_id] = SharedMapperState(
            _trace_len=self.global_mapper_call_level,
            strict=self.strict,
        )
        return None

    def __call__(self, source: dict[str, Any], **kwargs) -> dict[str, Any]:
        """
        Calls `map_fn` and then performs postprocessing into the result dict.
        """
        # If global mapper ID is not in config dict, then add it + hash stack trace
        if not self.global_mapper_call_id:
            self._register_mapper_call_id()

        res = self.map_fn(source, **kwargs)

        # Handle any DROP-flagged values
        keys_to_drop = get_keys_containing_class(res, DROP)
        if keys_to_drop:
            res = drop_keys(res, keys_to_drop)

        # Remove empty values
        if self.remove_empty:
            res = remove_empty_values(res)

        # Impute KEEP values with corresponding value
        keys_to_impute = get_keys_containing_class(res, KEEP)
        if keys_to_impute:
            res = impute_enum_values(res, keys_to_impute)

        return res
