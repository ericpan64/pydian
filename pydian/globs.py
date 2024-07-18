"""
This module contains _shared_ global state which is managed by the `Mapper` and used by `get`

Some notes:
- Python globals are at the module-level, and persist per interpeted session
- Python currently uses the GIL which means each process has different globals
    - This means all expected `Mapper` config needs to run within a given process
- When developing, naming conventions should be followed
"""

from dataclasses import dataclass
from typing import Generic, TypeVar


@dataclass(frozen=True)
class SharedMapperState:
    _trace_len: int
    strict: bool


K = TypeVar("K")
V = TypeVar("V")


class ImmutableDict(dict, Generic[K, V]):
    """
    Global `dict` that can receive new items, and unioned items are immutable
    """

    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise ValueError(f"Key type needs to be `str`, got: {type(value)} {value}")
        if not isinstance(value, SharedMapperState):
            raise ValueError(
                f"Value type needs to be `SharedMapperState`, got: {type(value)} {value}"
            )
        if key in self:
            raise ValueError(
                f"Key {key} already exists and ImmutableDict doesn't allow updating keys."
            )
        super().__setitem__(key, value)

    def update(self, *args, **kwargs):
        raise ValueError("ImmutableDict doesn't allow updates.")

    def __delitem__(self, key):
        raise ValueError("ImmutableDict doesn't allow deletion of keys.")


_Global_Mapper_State_Dict: ImmutableDict[str, SharedMapperState] = ImmutableDict()
