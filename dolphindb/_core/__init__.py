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
            DolphinDBRuntime._ddbcpp = __import__("_dolphindbcpp")
            DolphinDBRuntime._ddbcpp.init()


__all__ = [
    DolphinDBRuntime,
]
