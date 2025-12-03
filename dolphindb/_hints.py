import sys

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    Set,
    TypeVar,
    Generic,
    Type,
)

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from .settings import ParserType, SqlStd


__all__ = [
    "Any",
    "Callable",
    "Dict",
    "List",
    "Literal",
    "Optional",
    "Tuple",
    "Union",
    "Set",
    "TypeVar",
    "Generic",
    "Type",

    "ParserType",
    "SqlStd",
]
