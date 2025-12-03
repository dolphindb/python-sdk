import platform

from ._core import DolphinDBRuntime

ddbcpp = DolphinDBRuntime()._ddbcpp


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
        """Set the TCP timeout.

        Args:
            timeout : corresponds to the TCP_USER_TIMEOUT option. Specify the value in units of seconds.
        """
        ddbcpp.sessionimpl.setTimeout(timeout)
