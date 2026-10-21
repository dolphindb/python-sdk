import platform
from datetime import timedelta

from ._core import DolphinDBRuntime

ddbcpp = DolphinDBRuntime()._ddbcpp

_default_net_timeout = timedelta(seconds=3)


def _get_default_net_timeout() -> timedelta:
    return _default_net_timeout


def enable_job_cancellation() -> None:
    """Enable cancellation of running jobs.

    When this method is enabled, all running jobs (executed with Session.run()) in the process will be canceled immediately. This method can only be executed once for each Session.
    This method is not supported in high availability secenarios.
    This method is not recommended in multithreaded mode. In multithreaded mode, make sure the main process of signal is imported before you call this method.

    """
    if platform.system() == "Linux":
        ddbcpp.sessionimpl.enableJobCancellation()
    else:
        raise RuntimeError("This method is only supported on Linux.")


class tcp:
    @classmethod
    def set_timeout(cls, timeout: float) -> None:
        """Set the default network timeout.

        Args:
            timeout : the global default value of ``net_timeout`` in seconds.
                Used by new connections and DBConnectionPool instances. Defaults to
                3 seconds; zero uses system defaults. Existing connections retain
                their configured timeout.
        """
        if timeout < 0:
            raise ValueError("The parameter timeout must be non-negative.")
        global _default_net_timeout
        _default_net_timeout = timedelta(seconds=timeout)
