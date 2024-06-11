from collections.abc import Callable
from typing import Any, TypeAlias

# from result import Err, Ok

ApplyFunc: TypeAlias = Callable[[Any], Any]
ConditionalCheck: TypeAlias = Callable[[Any], bool]
MappingFunc: TypeAlias = Callable[..., dict[str, Any]]


# # TODO - do we need these?
# class Ok_T(Ok):
# """ `Ok` that explicitly evaluates to truthy """
#
#     def __bool__(self) -> bool:
#         return True

# class Err_F(Err):
# """ `Err` that evaluates to falsy """
#     def __bool__(self) -> bool:
#         return False
