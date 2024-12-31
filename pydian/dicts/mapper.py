import contextvars
from contextlib import contextmanager
from typing import Any

from ..lib.types import DROP, KEEP, MappingFunc
from ..lib.util import remove_empty_values
from .util import drop_keys, get_keys_containing_class, impute_enum_values

_MapperContextStrict = contextvars.ContextVar("_MapperContextStrict", default=None)


@contextmanager
def mapping_context(strict: bool = False):
    token = _MapperContextStrict.set(strict)  # type: ignore
    yield
    _MapperContextStrict.reset(token)


class Mapper:
    def __init__(
        self,
        map_fn: MappingFunc,
        remove_empty: bool = True,
    ) -> None:
        self.map_fn = map_fn
        self.remove_empty = remove_empty

    def __call__(self, source: dict[str, Any], **kwargs) -> dict[str, Any]:
        """
        Calls `map_fn` and then performs postprocessing into the result dict.
        """
        # Run the function
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
