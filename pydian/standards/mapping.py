from pydantic import BaseModel

from ..dicts.mapper import Mapper
from ..types import MappingFunc


class DictMapping(dict):
    """
    A class that maps data between established data standards.

    DataMapping == between known input schema, and known output schema

    Each class should only contain one direction, i.e.:
    - One -> One, or
    - Many -> One
    """

    input_schema: BaseModel | tuple[BaseModel]
    output_schema: BaseModel
    forward_fn: MappingFunc | Mapper
    ...
