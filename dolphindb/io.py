from ._core import DolphinDBRuntime
ddbcpp = DolphinDBRuntime()._ddbcpp

dump = ddbcpp.dump
load = ddbcpp.load
dumps = ddbcpp.dumps
loads = ddbcpp.loads


__all__ = [
    "dump",
    "load",
    "dumps",
    "loads",
]
