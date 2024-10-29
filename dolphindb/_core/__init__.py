import os
import sys
from pathlib import Path


class DolphinDBRuntime:
    _inst = None
    _ddbcpp = None

    def __new__(cls):
        if cls._inst is None:
            cls._inst = super().__new__(cls)
        return cls._inst

    def __init__(self) -> None:
        if not DolphinDBRuntime._ddbcpp:
            sys.path.append(str(Path(os.path.dirname(__file__)).parent))
            from .. import _dolphindbcpp as _ddbcpp
            DolphinDBRuntime._ddbcpp = _ddbcpp
            DolphinDBRuntime._ddbcpp.init()


__all__ = [
    DolphinDBRuntime,
]
