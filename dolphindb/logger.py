from ._core import DolphinDBRuntime
ddbcpp = DolphinDBRuntime()._ddbcpp


Level = ddbcpp.Level
LogMessage = ddbcpp.LogMessage
Sink = ddbcpp.Sink
Logger = ddbcpp.Logger
default_logger = ddbcpp.default_logger


__all__ = [
    "Level",
    "LogMessage",
    "Sink",
    "Logger",
    "default_logger",
]
