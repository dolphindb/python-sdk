from pandas import DataFrame

from ._core import DolphinDBRuntime
from ._hints import Any, List, Tuple
from .utils import deprecated

ddbcpp = DolphinDBRuntime()._ddbcpp


@deprecated(instead="MultithreadedTableWriter")
class BatchTableWriter(object):
    """Class of batch writer for asynchronous batched writes.

    Support batched writes to in-memory table and distributed table.

    Args:
        host : server address.
        port : port name.
        userid : username. Defaults to None.
        password : password. Defaults to None.
        acquireLock : whether to acquire a lock in the Python API, True (default) means to acquire a lock. Defaults to True.

    Note:
        It's required to acquire the lock for concurrent API calls
    """

    def __init__(
        self,
        host: str,
        port: int,
        userid: str = None,
        password: str = None,
        acquireLock: bool = True,
    ):
        """Constructor of BatchTableWriter"""
        if userid is None:
            userid = ""
        if password is None:
            password = ""
        self.writer = ddbcpp.batchTableWriter(
            host, port, userid, password, acquireLock
        )

    def addTable(
        self,
        dbPath: str = None,
        tableName: str = None,
        partitioned: bool = True,
    ) -> None:
        """Add a table to be written to.

        Args:
            dbPath : the database path for a disk table; leave it empty for an in-memory table. Defaults to None.
            tableName : table name. Defaults to None.
            partitioned : whether is a partitioned table. True indicates a partitioned table. Defaults to True.

        Note:
            If the table is a non-partitioned table on disk, it's required to set partitioned=False.
        """
        if dbPath is None:
            dbPath = ""
        if tableName is None:
            tableName = ""
        self.writer.addTable(dbPath, tableName, partitioned)

    def getStatus(
        self, dbPath: str = None, tableName: str = None
    ) -> Tuple[int, bool, bool]:
        """Get the current write status.

        Args:
            dbPath : database name. Defaults to None.
            tableName : table name. Defaults to None.

        Returns:
            Tuple[int, bool, bool]: indicating the depth of the current writing queue, whether the current table is being removed (True if being removed), and whether the background thread exits due to an error (True if exits due to an error).
        """
        if dbPath is None:
            dbPath = ""
        if tableName is None:
            tableName = ""
        return self.writer.getStatus(dbPath, tableName)

    def getAllStatus(self) -> DataFrame:
        """Get the status of all tables except for the removed ones.

        Returns:
        | DatabaseName | TableName | WriteQueueDepth | sentRows | Removing | Finished |
        | :----------- | :-------- | :-------------- | :------- | :------- | :------- |
        | 0            | tglobal   | 0               | 5        | False    | False    |
        """
        return self.writer.getAllStatus()

    def getUnwrittenData(
        self, dbPath: str = None, tableName: str = None
    ) -> DataFrame:
        """Obtain unwritten data. The method is mainly used to obtain the remaining unwritten data if an error occurs when writing.

        Args:
            dbPath : database name. Defaults to None.
            tableName : table name. Defaults to None.

        Returns:
            DataFrame of unwritten data.
        """
        if dbPath is None:
            dbPath = ""
        if tableName is None:
            tableName = ""
        return self.writer.getUnwrittenData(dbPath, tableName)

    def removeTable(self, dbPath: str = None, tableName: str = None) -> None:
        """Release the resources occupied by the table added by the addTable method. The first time the method is called, it returns if the thread has exited.

        Args:
            dbPath : database name. Defaults to None.
            tableName : table name. Defaults to None.
        """
        if dbPath is None:
            dbPath = ""
        if tableName is None:
            tableName = ""
        self.writer.removeTable(dbPath, tableName)

    def insert(self, dbPath: str = None, tableName: str = None, *args):
        """Insert a single row of data

        Args:
            dbPath : database name. Defaults to None.
            tableName : table name. Defaults to None.
            args : variable-length argument, indicating a row of data to be inserted.
        """
        if dbPath is None:
            dbPath = ""
        if tableName is None:
            tableName = ""
        self.writer.insert(dbPath, tableName, *args)


class ErrorCodeInfo(object):
    """MTW error codes and messages.

    Args:
        errorCode : error code. Defaults to None.
        errorInfo : error information. Defaults to None.
    """

    def __init__(self, errorCode=None, errorInfo=None):
        """Constructor of ErrorCodeInfo."""
        self.errorCode = errorCode
        self.errorInfo = errorInfo

    def __repr__(self):
        errorCodeText = ""
        if self.hasError():
            errorCodeText = self.errorCode
        else:
            errorCodeText = None
        outStr = "errorCode: %s\n" % errorCodeText
        outStr += " errorInfo: %s\n" % self.errorInfo
        outStr += object.__repr__(self)
        return outStr

    def hasError(self) -> bool:
        """Check if an error has occurred.

        Returns:
            True if an error occurred, otherwise False.
        """
        return self.errorCode is not None and len(self.errorCode) > 0

    def succeed(self) -> bool:
        """Check if data has been written successfully.

        Returns:
            True if the write is successful, otherwise False.
        """
        return self.errorCode is None or len(self.errorCode) < 1


class MultithreadedTableWriterThreadStatus(object):
    """Get the status of MTW.

    Args:
        threadId: thread ID.

    Attribute:
        threadId : thread ID.
        sentRows : number of sent rows.
        unsentRows : number of rows to be sent.
        sendFailedRows : number of rows failed to be sent.

    """

    def __init__(self, threadId: int = None):
        """Constructor of MultithreadedTableWriterThreadStatus."""
        self.threadId = threadId
        self.sentRows = None
        self.unsentRows = None
        self.sendFailedRows = None


class MultithreadedTableWriterStatus(ErrorCodeInfo):
    """The status information of MTW.

    Attribute:
        isExiting : whether the threads are exiting.
        sentRows : number of sent rows.
        unsentRows : number of rows to be sent.
        sendFailedRows : number of rows failed to be sent.
        threadStatus : a list of the thread status.
    """

    def __init__(self):
        """Constructor of MultithreadedTableWriterStatus."""
        self.isExiting = None
        self.sentRows = None
        self.unsentRows = None
        self.sendFailedRows = None
        self.threadStatus = []

    def update(self, statusDict):
        threadStatusDict = statusDict["threadStatus"]
        del statusDict["threadStatus"]
        self.__dict__.update(statusDict)
        for oneThreadStatusDict in threadStatusDict:
            oneThreadStatus = MultithreadedTableWriterThreadStatus()
            oneThreadStatus.__dict__.update(oneThreadStatusDict)
            self.threadStatus.append(oneThreadStatus)

    def __repr__(self):
        errorCodeText = ""
        if self.hasError():
            errorCodeText = self.errorCode
        else:
            errorCodeText = None
        outStr = "%-14s: %s\n" % ("errorCode", errorCodeText)
        if self.errorInfo is not None:
            outStr += " %-14s: %s\n" % ("errorInfo", self.errorInfo)
        if self.isExiting is not None:
            outStr += " %-14s: %s\n" % ("isExiting", self.isExiting)
        if self.sentRows is not None:
            outStr += " %-14s: %s\n" % ("sentRows", self.sentRows)
        if self.unsentRows is not None:
            outStr += " %-14s: %s\n" % ("unsentRows", self.unsentRows)
        if self.sendFailedRows is not None:
            outStr += " %-14s: %s\n" % ("sendFailedRows", self.sendFailedRows)
        if self.threadStatus is not None:
            outStr += " %-14s: \n" % "threadStatus"
            outStr += " \tthreadId\tsentRows\tunsentRows\tsendFailedRows\n"
            for thread in self.threadStatus:
                outStr += "\t"
                if thread.threadId is not None:
                    outStr += "%8d" % thread.threadId
                outStr += "\t"
                if thread.sentRows is not None:
                    outStr += "%8d" % thread.sentRows
                outStr += "\t"
                if thread.unsentRows is not None:
                    outStr += "%10d" % thread.unsentRows
                outStr += "\t"
                if thread.sendFailedRows is not None:
                    outStr += "%14d" % thread.sendFailedRows
                outStr += "\n"
        outStr += object.__repr__(self)
        return outStr


class MultithreadedTableWriter(object):
    """A multi-threaded writer is an ungrade of BatchTableWriter with support for multi-threaded concurrent writes.

    Args:
        host : host name.
        port : port number.
        userId : username.
        password : password.
        dbPath : the DFS database path or in-memory table name.
        tableName : the DFS table name. Leave it unspecified for an in-memory table.
        useSSL : whether to enable SSL. Defaults to False.
        enableHighAvailability : whether to enable high availability. Defaults to False.
        highAvailabilitySites : a list of "ip:port" of all available nodes. Defaults to [].
        batchSize : the number of messages in batch processing. Defaults to 1.
        throttle : the waiting time (in seconds) before the server processes the incoming data if the number of data written from the client does not reach batchSize. Defaults to 1.
        threadCount : the number of working threads to be created. Defaults to 1.
        partitionCol : a string indicating the partitioning column, the default is an empty string. The parameter only takes effect when threadCount>1. Defaults to "".
                        For a partitioned table, it must be the partitioning column; for a stream table, it must be a column name; for a dimension table, the parameter does not work.
        compressMethods : a list of the compression methods used for each column. If unspecified, the columns are not compressed. Defaults to [].
                            The compression methods include: "LZ4": LZ4 algorithm; "DELTA": Delta-of-delta encoding.
        mode : the write mode. It can be Append (default) or Upsert. Defaults to "".
        modeOption : the parameters of function upsert!. It only takes effect when mode is Upsert. Defaults to [].
        reconnect : whether to enable reconnection. True means enabled, otherwise False. Defaults to True.
        tryReconnectNums ：the number of reconnection attempts when reconnection is enabled. If high availability mode is on, it will retry tryReconnectNums times for each node. None means unlimited reconnection attempts. Defaults to None.
        enableStreamTableTimestamp: whether to insert data into a stream table with timestamp attached by `setStreamTableTimestamp` function. True means enabled, otherwise False. Defaults to False.
    """

    def __init__(
        self,
        host: str,
        port: int,
        userId: str,
        password: str,
        dbPath: str,
        tableName: str,
        useSSL: bool = False,
        enableHighAvailability: bool = False,
        highAvailabilitySites: List[str] = [],
        batchSize: int = 1,
        throttle: float = 1.0,
        threadCount: int = 1,
        partitionCol: str = "",
        compressMethods: List[str] = [],
        mode: str = "",
        modeOption: List[str] = [],
        *,
        reconnect: bool = True,
        tryReconnectNums: int = -1,
        enableStreamTableTimestamp: bool = False,
        usePublicName: bool = False,
    ):
        """Constructor of MultithreadedTableWriter"""
        self.writer = ddbcpp.multithreadedTableWriter(
            host,
            port,
            userId,
            password,
            dbPath,
            tableName,
            useSSL,
            enableHighAvailability,
            highAvailabilitySites,
            batchSize,
            throttle,
            threadCount,
            partitionCol,
            compressMethods,
            mode,
            modeOption,
            reconnect,
            enableStreamTableTimestamp,
            tryReconnectNums,
            usePublicName,
        )

    def getStatus(self) -> MultithreadedTableWriterStatus:
        """Get the current writer status and return a MultithreadedTableWriterStatus object.

        Returns:
            MultithreadedTableWriterStatus object.
        """
        status = MultithreadedTableWriterStatus()
        status.update(self.writer.getStatus())
        return status

    def getUnwrittenData(self) -> List[List[Any]]:
        """Get unwritten data.

        The obtained data can be passed as an argument to method insertUnwrittenData.

        Returns:
            a nested list where each element is a record.
        """
        return self.writer.getUnwrittenData()

    def insert(self, *args) -> ErrorCodeInfo:
        """Insert a single row of data.

        Args:
            args : variable-length argument, indicating a row of data to be inserted.

        Returns:
            a ErrorCodeInfo object.
        """
        errorCodeInfo = ErrorCodeInfo()
        errorCodeInfo.__dict__.update(self.writer.insert(*args))
        return errorCodeInfo

    def insertUnwrittenData(self, unwrittenData) -> ErrorCodeInfo:
        """Insert unwritten data.

        The result is in the same format as insert.
        The difference is that insertUnwrittenData can insert multiple records at a time.

        Args:
            unwrittenData : the data that has not been written to the server. You can obtain the object with method getUnwrittenData.

        Returns:
            ErrorCodeInfo object
        """
        errorCodeInfo = ErrorCodeInfo()
        errorCodeInfo.__dict__.update(
            self.writer.insertUnwrittenData(unwrittenData)
        )
        return errorCodeInfo

    def waitForThreadCompletion(self) -> None:
        """Wait until all working threads complete their tasks.

        After calling the method, MultithreadedTableWriter will wait until all working threads complete their tasks.

        """
        self.writer.waitForThreadCompletion()
