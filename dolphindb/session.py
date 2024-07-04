"""Session
Package: session package.
        including classes Session, DBConnectionPool, BatchTableWriter, MultithreadedTableWriter
"""


import asyncio
import platform
import re
import warnings
from concurrent.futures import Future
from datetime import date, datetime
from threading import Lock, Thread
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import numpy as np
from dolphindb._core import DolphinDBRuntime
from dolphindb.database import Database
from dolphindb.settings import (DDB_EPSILON, PROTOCOL_ARROW, PROTOCOL_DDB,
                                PROTOCOL_DEFAULT, PROTOCOL_PICKLE)
from dolphindb.table import Table
from dolphindb.utils import _generate_dbname, _generate_tablename, month
from pandas import DataFrame

ddbcpp = DolphinDBRuntime()._ddbcpp


class DBConnectionPool(object):
    """DBConnectionPool is the connection pool object where multiple threads can be created to execute scripts in parallel and improve task efficiency.

    Note:
        To improve efficiency, methods such as run of class DBConnectionPool are packaged into coroutine functions in python.

    Args:
        host : server address. It can be IP address, domain, or LAN hostname, etc.
        port : port name.
        threadNum : number of threads. Defaults to 10.
        userid : username. Defaults to None.
        password : password. Defaults to None.
        loadBalance : whether to enable load balancing. True means enabled (the connections will be distributed evenly across the cluster), otherwise False. Defaults to False.
        highAvailability : whether to enable high availability. True means enabled, otherwise False. Defaults to False.
        compress : whether to enable compression. True means enabled, otherwise False. Defaults to False.
        reConnect : whether to enable reconnection. True means enabled, otherwise False. Defaults to False.
        enablePickle : whether to enable the Pickle protocol. Defaults to False.
        python : whether to enable the Python parser. True means enabled, otherwise False. Defaults to False.
        protocol : the data transmission protocol. Defaults to PROTOCOL_DEFAULT, which is equivalant to PROTOCOL_PICKLE.

    Kwargs:
        show_output : whether to display the output of print statements in the script after execution. Defaults to True.

    Note:
        If the parameter python is True, the script is parsed in Python rather than DolphinDB language.
    """
    def __init__(
        self, host: str, port: int, threadNum: int = 10, userid: str = None, password: str = None,
        loadBalance: bool = False, highAvailability: bool = False, compress: bool = False,
        reConnect: bool = False, python: bool = False, protocol: int = PROTOCOL_DEFAULT,
        *, show_output: bool = True
    ):
        """Constructor of DBConnectionPool, including the number of threads, load balancing, high availability, reconnection, compression and Pickle protocol."""
        userid = userid if userid is not None else ""
        password = password if password is not None else ""
        if protocol == PROTOCOL_DEFAULT:
            protocol = PROTOCOL_PICKLE
        if protocol not in [PROTOCOL_DEFAULT, PROTOCOL_DDB, PROTOCOL_PICKLE, PROTOCOL_ARROW]:
            raise RuntimeError(f"Protocol {protocol} is not supported. ")

        if protocol == PROTOCOL_ARROW and compress:
            raise RuntimeError("The Arrow protocol does not support compression.")

        if protocol == PROTOCOL_ARROW:
            __import__("pyarrow")

        self.mutex = Lock()
        self.pool = ddbcpp.dbConnectionPoolImpl(host, port, threadNum, userid, password, loadBalance, highAvailability, compress, reConnect, python, protocol, show_output)
        self.host = host
        self.port = port
        self.userid = userid
        self.password = password
        self.taskId = 0
        self.loop = None
        self.thread = None
        self.protocol = protocol
        self.shutdown_flag = False

    def __del__(self):
        if hasattr(self, "pool") and self.pool is not None:
            self.shutDown()

    async def run(self, script: str, *args, **kwargs):
        """Coroutine function.

        Pass the script to the connection pool to call the thread execution with the run method.

        Args:
            script : DolphinDB script to be executed.
            args : arguments to be passed to the function.

        Note:
            args is only required when script is the function name.

        Kwargs:
            clearMemory : whether to release variables after queries. True means to release, otherwise False. Defaults to True.
            pickleTableToList : whether to convert table to list or DataFrame. True: to list, False: to DataFrame. Defaults to False.
            priority : a job priority system with 10 priority levels (0 to 9), allocating thread resources to high-priority jobs and using round-robin allocation for jobs of the same priority. Defaults to 4.
            parallelism : parallelism determines the maximum number of threads to execute a job's tasks simultaneously on a data node; the system optimizes resource utilization by allocating all available threads to a job if there's only one job running and multiple local executors are available. Defaults to 2.
            disableDecimal: whether to convert decimal to double in the result table. True: convert to double, False: return as is. Defaults to False.

        Returns:
            execution result.

        Note:
            When setting pickleTableToList=True and enablePickle=True, if the table contains array vectors, it will be converted to a NumPy 2d array.
            If the length of each row is different, the execution fails.
        """
        priority = kwargs.get('priority', 4)
        parallelism = kwargs.get('parallelism', 2)
        if type(priority) is not int or priority > 9 or priority < 0:
            raise RuntimeError("priority must be an integer from 0 to 9")
        if type(parallelism) is not int or parallelism <= 0:
            raise RuntimeError("parallelism must be an integer greater than 0")

        self.mutex.acquire()
        self.taskId = self.taskId + 1
        tid = self.taskId
        self.mutex.release()
        if "clearMemory" not in kwargs.keys():
            kwargs["clearMemory"] = True
        self.pool.run(script, tid, *args, **kwargs)
        while True:
            isFinished = self.pool.isFinished(tid)
            if isFinished == 0:
                await asyncio.sleep(0.01)
            else:
                return self.pool.getData(tid)

    def addTask(self, script: str, taskId: int, *args, **kwargs):
        """Add a task and specify the task ID to execute the script.

        Args:
            script : script to be executed.
            taskId : the task ID.
            args : arguments to be passed to the function.

        Note:
            args is only required when script is the function name.

        Kwargs:
            clearMemory : whether to release variables after queries. True means to release, otherwise False. Defaults to True.

        Returns:
            execution result.
        """
        if "clearMemory" not in kwargs.keys():
            kwargs["clearMemory"] = True
        return self.pool.run(script, taskId, *args, **kwargs)

    def isFinished(self, taskId: int) -> bool:
        """Get the completion status of the specified task.

        Args:
            taskId : the task ID.

        Returns:
            True if the task is finished, otherwise False.
        """
        return self.pool.isFinished(taskId)

    def getData(self, taskId: int):
        """Get data of the specified task.

        Args:
            taskId : the task ID.

        Returns:
            data of the task.

        Note:
            For each task, the data can only be obtained once and will be cleared immediately after the data is obtained.
        """
        return self.pool.getData(taskId)

    def __start_thread_loop(self, loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def startLoop(self):
        """Create and start an event loop.

        Raises:
            the event loop has been created.
        """
        if self.loop is not None:
            raise RuntimeError("Event loop is already started!")
        self.loop = asyncio.new_event_loop()
        self.thread = Thread(target=self.__start_thread_loop, args=(self.loop,))
        self.thread.setDaemon(True)
        self.thread.start()

    async def stopLoop(self):
        """Stop the event loop."""
        await asyncio.sleep(0.01)
        self.loop.stop()

    def runTaskAsync(self, script: str, *args, **kwargs) -> Future:
        """Execute script tasks asynchronously.

        Args:
            script : script to be executed.
            args : arguments to be passed to the function.

        Kwargs:
            clearMemory : whether to release variables after queries. True means to release, otherwise False. Defaults to True.

        Returns:
            concurrent.futures.Future object for receiving data.
        """
        if self.loop is None:
            self.startLoop()
        if "clearMemory" not in kwargs.keys():
            kwargs["clearMemory"] = True
        task = asyncio.run_coroutine_threadsafe(self.run(script, *args, **kwargs), self.loop)
        return task

    def runTaskAsyn(self, script: str, *args, **kwargs) -> Future:
        """Execute script tasks asynchronously.

        Deprecated:
            Please use runTaskAsync instead of runTaskAsyn.

        Args:
            script : script to be executed.
            args : arguments to be passed to the function.

        Kwargs:
            clearMemory : whether to release variables after queries. True means to release, otherwise False. Defaults to True.

        Returns:
            concurrent.futures.Future object for receiving data.
        """
        if "clearMemory" not in kwargs.keys():
            kwargs["clearMemory"] = True
        warnings.warn("Please use runTaskAsync instead of runTaskAsyn.", DeprecationWarning, stacklevel=2)
        return self.runTaskAsync(script, *args, **kwargs)

    def shutDown(self):
        """Close the DBConnectionPool, stop the event loop and terminate all asynchronous tasks."""
        with self.mutex:
            if self.shutdown_flag:
                return
            self.shutdown_flag = True
        self.host = None
        self.port = None
        if self.loop is not None:
            asyncio.run_coroutine_threadsafe(self.stopLoop(), self.loop)
            self.thread.join()
            if self.loop.is_running():
                self.loop.stop()
            else:
                self.loop.close()
        if self.pool is not None:
            self.pool.shutDown()
            self.pool = None
        self.loop = None
        self.thread = None

    def getSessionId(self) -> List[str]:
        """Obtain Session ID of all sessions.

        Returns:
            list of thread ID.
        """
        return self.pool.getSessionId()

    def is_shutdown(self) -> bool:
        with self.mutex:
            return self.shutdown_flag


class streamDeserializer(object):
    """Deserializer stream blob in multistreamingtable reply.

    Args:
        sym2table : a dict object indicating the corresponding relationship between the unique identifiers of the tables and the table objects.
        session : a Session object. Defaults to None.
    """
    def __init__(self, sym2table: Dict[str, Union[List[str], str]], session=None):
        """Constructor of streamDeserializer."""
        self.cpp = ddbcpp.streamDeserializer(sym2table)
        if session is not None:
            self.cpp.setSession(session.cpp)


class Session(object):
    """Session is the connection object to execute scripts.

    Args:
        host : server address. It can be IP address, domain, or LAN hostname, etc. Defaults to None.
        port : port name. Defaults to None.

    Note:
        If neither host nor port is set to None, a connection will be established.

    Args:
        userid : username. Defaults to "", meaning not to log in.
        password : password. Defaults to "", meaning not to log in.
        enableSSL : whether to enable SSL. True means to enable SSL. Defaults to False.
        enableASYNC(enableASYN) : whether to enable asynchronous communication. Defaults to False.

    Deprecated:
        Please use enableASYNC instead of enableASYN.

    Args:
        keepAliveTime : the duration between two keepalive transmissions to detect the TCP connection status. Defaults to 30 (seconds). Set the parameter to release half-open TCP connections timely when the network is unstable.
        enableChunkGranularityConfig : whether to enable chunk granularity configuration. Defaults to False.
        compress : whether to enable compressed communication. Defaults to False.
        enablePickle : whether to enable the Pickle protocol. Defaults to True.
        protocol: the data transmission protocol. Defaults to PROTOCOL_DEFAULT, which is equivalant to PROTOCOL_PICKLE.
        python : whether to enable python parser. Defaults to False.

    Kwargs:
        show_output : whether to display the output of print statements in the script after execution. Defaults to True.

    Note:
        set enableSSL =True to enable encrypted communication. It's also required to configure enableHTTPS =true in the server.

    Note:
        set enableASYNC =True to enable asynchronous mode and the communication with the server can only be done through the session.run method. As there is no return value, it is suitable for asynchronous writes.
    """
    def __init__(
        self, host: Optional[str] = None, port: Optional[int] = None,
        userid: Optional[str] = "", password: Optional[str] = "", enableSSL: bool = False,
        enableASYNC: bool = False, keepAliveTime: int = 30, enableChunkGranularityConfig: bool = False,
        compress: bool = False, enablePickle: bool = None, protocol: int = PROTOCOL_DEFAULT,
        python: bool = False, *, show_output: bool = True, **kwargs
    ):
        """Constructor of Session, inluding OpenSSL encryption, asynchronous mode, TCP detection, block granularity matching, compression, Pickle protocol."""
        if 'enableASYN' in kwargs.keys():
            enableASYNC = kwargs['enableASYN']
            warnings.warn("Please use enableASYNC instead of enableASYN.", DeprecationWarning, stacklevel=2)

        default_protocol = PROTOCOL_PICKLE
        if protocol == PROTOCOL_DEFAULT:
            protocol = PROTOCOL_PICKLE if enablePickle else PROTOCOL_DDB
            if enablePickle is None:
                protocol = default_protocol
        elif enablePickle:
            raise RuntimeError("When enablePickle=true, the parameter protocol must not be specified. ")

        if protocol not in [PROTOCOL_DEFAULT, PROTOCOL_DDB, PROTOCOL_PICKLE, PROTOCOL_ARROW]:
            raise RuntimeError(f"Protocol {protocol} is not supported. ")

        if protocol == PROTOCOL_ARROW and compress:
            raise RuntimeError("The Arrow protocol does not support compression.")

        if protocol == PROTOCOL_ARROW:
            __import__("pyarrow")

        self.cpp = ddbcpp.sessionimpl(enableSSL, enableASYNC, keepAliveTime, compress, python, protocol, show_output)
        self.host = host
        self.port = port
        self.userid = userid
        self.password = password
        self.mutex = Lock()
        self.enableEncryption = True
        self.enableChunkGranularityConfig = enableChunkGranularityConfig
        self.enablePickle = enablePickle
        self.protocol = protocol
        if self.host is not None and self.port is not None:
            self.connect(host, port, userid, password, keepAliveTime=keepAliveTime)

    def __del__(self):
        if hasattr(self, "cpp"):
            self.cpp.close()

    def connect(
        self, host: str, port: int, userid: str = None, password: str = None, startup: str = None,
        highAvailability: bool = False, highAvailabilitySites: Optional[List[str]] = None,
        keepAliveTime: Optional[int] = None, reconnect: bool = False
    ) -> bool:
        """Establish connection, including initialization script, high availability, TCP detection.

        Args:
            host : server address. It can be IP address, domain, or LAN hostname, etc.
            port : port name.
            userid : username. Defaults to None, indicating no login.
            password : password. Defaults to None, indicating no login..
            startup : the startup script to execute the preloaded tasks immediately after the connection is established. Defaults to None.
            highAvailability : whether to enable high availability. True means enabled, otherwise False. Defaults to False.
            highAvailabilitySites : a list of high-availability nodes. Defaults to None.
            keepAliveTime : the duration between two keepalive transmissions to detect the TCP connection status. Defaults to None, meaning keepAliveTime = 30 (seconds). Set the parameter to release half-open TCP connections timely when the network is unstable.
            reconnect : whether to enable reconnection. True means enabled, otherwise False. Defaults to False.

        Returns:
            whether the connection is established. True if established, otherwise False.
        """
        if highAvailabilitySites is None:
            highAvailabilitySites = []
        if keepAliveTime is None:
            keepAliveTime = -1
        if userid is None:
            userid = ""
        if password is None:
            password = ""
        if startup is None:
            startup = ""
        if self.cpp.connect(host, port, userid, password, startup, highAvailability, highAvailabilitySites, keepAliveTime, reconnect):
            self.host = host
            self.port = port
            self.userid = userid
            self.password = password
            return True
        else:
            return False

    def login(self, userid: str, password: str, enableEncryption: bool = True):
        """Manually log in to the server.

        Args:
            userid : username.
            password : password.
            enableEncryption : whether to enable encrypted transmission for username and password. Defaults to True.

        Note:
            Specify the userid and password in the connect() method to log in automatically.
        """
        self.mutex.acquire()
        try:
            self.userid = userid
            self.password = password
            self.enableEncryption = enableEncryption
            self.cpp.login(userid, password, enableEncryption)
        finally:
            self.mutex.release()

    def close(self):
        """Close Session connection."""
        self.host = None
        self.port = None
        self.cpp.close()

    def isClosed(self) -> bool:
        """Check if the current Session has been closed.

        Returns:
            bool: True if closed, otherwise False.
        """
        return self.host is None

    def upload(self, nameObjectDict):
        """Upload Python objects to DolphinDB server.

        Args:
            nameObjectDict : Python dictionary object. The keys of the dictionary are the variable names in DolphinDB and the values are Python objects, which can be numbers, strings, lists, DataFrame, etc.

        Returns:
            the server address of the uploaded object.

        Note:
            A pandas DataFrame corresponds to DolphinDB table.
        """
        return self.cpp.upload(nameObjectDict)

    def run(self, script: str, *args, **kwargs):
        """Execute script.

        Args:
            script : DolphinDB script to be executed.
            args : arguments to be passed to the function.

        Note:
            args is only required when script is the function name.

        Kwargs:
            clearMemory : whether to release variables after queries. True means to release, otherwise False. Defaults to False.
            pickleTableToList : whether to convert table to list or DataFrame. True: to list, False: to DataFrame.  Defaults to False.
            priority : a job priority system with 10 priority levels (0 to 9), allocating thread resources to high-priority jobs and using round-robin allocation for jobs of the same priority. Defaults to 4.
            parallelism : parallelism determines the maximum number of threads to execute a job's tasks simultaneously on a data node; the system optimizes resource utilization by allocating all available threads to a job if there's only one job running and multiple local executors are available. Defaults to 2.
            fetchSize : the size of a block.
            disableDecimal: whether to convert decimal to double in the result table. True: convert to double, False: return as is. Defaults to False.

        Note:
            fetchSize cannot be less than 8192 Bytes.

        Returns:
            execution result. If fetchSize is specified, a BlockReader object will be returned. Each block can be read with the read() method.

        Note:
            When setting pickleTableToList=True and enablePickle=True, if the table contains array vectors, it will be converted to a NumPy 2d array. If the length of each row is different, the execution fails.
        """
        priority = kwargs.get('priority', 4)
        parallelism = kwargs.get('parallelism', 2)
        if type(priority) is not int or priority > 9 or priority < 0:
            raise RuntimeError("priority must be an integer from 0 to 9")
        if type(parallelism) is not int or parallelism <= 0:
            raise RuntimeError("parallelism must be an integer greater than 0")

        if kwargs:
            if "fetchSize" in kwargs.keys():
                return BlockReader(self.cpp.runBlock(script, **kwargs))
        return self.cpp.run(script, *args, **kwargs)

    def runFile(self, filepath: str, *args, **kwargs):
        """Execute script.

        Args:
            filepath : the path of the file on the server to be executed.
            args : arguments to be passed to the function.

        Note:
            args is only required when script is the function name.

        Kwargs:
            clearMemory : whether to release variables after queries. True means to release, otherwise False. Defaults to False.
            pickleTableToList : whether to convert table to list or DataFrame. True: to list, False: to DataFrame.  Defaults to False.

        Returns:
            execution result.

        Note:
            When setting pickleTableToList=True and enablePickle=True, if the table contains array vectors, it will be converted to a NumPy 2d array. If the length of each row is different, the execution fails.
        """
        with open(filepath, "r") as fp:
            script = fp.read()
            return self.run(script, *args, **kwargs)

    def getSessionId(self) -> str:
        """Get the Session ID of the current Session.

        Returns:
            Session ID.
        """
        return self.cpp.getSessionId()

    def enableStreaming(self, port: int = 0, threadCount: int = 1) -> None:
        """Enable streaming.

        Args:
            port : the subscription port for incoming data. The server will connect to the port automatically.
            threadCount : the number of threads. Defaults to 1.

        Note:
            If the server you're connecting to supports transferring subscribed data using the connection initiated by the subsriber, 
            do not specify this port. Otherwise, the port must be specified.

        """
        self.cpp.enableStreaming(port, threadCount)

    def subscribe(
        self, host: str, port: int, handler: Callable, tableName: str, actionName: str = None,
        offset: int = -1, resub: bool = False, filter=None,
        msgAsTable: bool = False, batchSize: int = 0, throttle: float = 1.0,
        userName: str = None, password: str = None, streamDeserializer: Optional["streamDeserializer"] = None,
        backupSites: List[str] = None,
        resubTimeout: int = 100,
        subOnce: bool = False,
    ) -> None:
        """Subscribe to stream tables in DolphinDB.

        Args:
            host : server address. It can be IP address, domain, or LAN hostname, etc.
            port : port name.
            handler : user-defined callback function for processing incoming data.
            tableName : name of the published table.
            actionName : name of the subscription task. Defaults to "".
            offset : an integer indicating the position of the first message where the subscription begins. Defaults to -1.

        Note:
            If offset is -1 or exceeding the number of rows in the stream table, the subscription starts with the next new message. It cannot be other negative values.

        Args:
            resub : True means to keep trying to subscribe to table after the subscription attempt fails. Defaults to False.
            filter : a vector indicating the filtering columns. Only the rows with values of the filtering column in the vector are published to the subscriber. Defaults to None.
            msgAsTable : whether to convert the subscribed data to dataframe. If msgAsTable = True, the subscribed data is ingested into handler as a DataFrame. Defaults to False, which means the subscribed data is ingested into handler as a List of nparrays. This optional parameter has no effect if batchSize is not specified.
            batchSize : an integer indicating the number of unprocessed messages to trigger the handler. If it is positive, the handler does not process messages until the number of unprocessed messages reaches batchSize. If it is unspecified or non-positive, the handler processes incoming messages as soon as they come in. Defaults to 0.
            throttle : a float indicating the maximum waiting time (in seconds) before the handler processes the incoming messages. Defaults to 1. This optional parameter has no effect if batchSize is not specified.
            userName : username. Defaults to None, indicating no login.
            password : password. Defaults to None, indicating no login.
            streamDeserializer : a deserializer of heterogeneous stream table. Defaults to None.
            backupSites : a list of strings indicating the host:port for each backup node, e.g. ["192.168.0.1:8848", "192.168.0.2:8849"]. If specified, the failover mechanism is automatically activated. Defaults to None.
            resubTimeout : an integer representing the timeout for resubscribing (in milliseconds). Defaults to 100.
            subOnce : a boolean, indicating whether to attempt reconnecting to disconnected nodes after each node switch occurs. Defaults to False.
        """
        if not isinstance(msgAsTable, bool):
            raise TypeError("msgAsTable must be a bool.")
        if not isinstance(batchSize, int):
            raise TypeError("batchSize must be a int.")
        if not isinstance(throttle, float) and not isinstance(throttle, int):
            raise TypeError("throttle must be a float or a int.")
        if throttle <= DDB_EPSILON:
            raise ValueError("throttle must be greater than 0.")
        if filter is None:
            filter = np.array([], dtype='int64')
        if actionName is None:
            actionName = ""
        if userName is None:
            userName = ""
        if password is None:
            password = ""
        sd = None
        if streamDeserializer is None:
            sd = ddbcpp.streamDeserializer({})
        else:
            sd = streamDeserializer.cpp
        if backupSites is None:
            backupSites = []
        if not isinstance(backupSites, list):
            raise TypeError("backupSites must be a list of str.")
        for site in backupSites:
            if not isinstance(site, str):
                raise TypeError("backupSites must be a list of str.")
        if not isinstance(resubTimeout, int):
            raise TypeError("resubTimeout must be an int.")
        if not isinstance(subOnce, bool):
            raise TypeError("subOnce must be a bool.")
        if batchSize > 0:
            self.cpp.subscribeBatch(
                host, port, handler, tableName, actionName,
                offset, resub, filter, msgAsTable, batchSize, throttle,
                userName, password, sd, backupSites, resubTimeout, subOnce
            )
        else:
            if msgAsTable:
                raise ValueError("msgAsTable must be False when batchSize is 0")
            self.cpp.subscribe(
                host, port, handler, tableName, actionName,
                offset, resub, filter, userName, password, sd, backupSites, resubTimeout, subOnce
            )

    def unsubscribe(self, host: str, port: int, tableName: str, actionName: str = None) -> None:
        """Unsubscribe.

        Args:
            host : server address. It can be IP address, domain, or LAN hostname, etc.
            port : port name.
            tableName : name of the published table.
            actionName : name of the subscription task. Defaults to None.
        """
        if actionName is None:
            actionName = ""
        self.cpp.unsubscribe(host, port, tableName, actionName)

    def getSubscriptionTopics(self) -> List[str]:
        """Get all subscription topics.

        Returns:
            a list of all subscription topics in the format of "host/port/tableName/actionName".
        """
        return self.cpp.getSubscriptionTopics()

    def getInitScript(self) -> str:
        """Get the init script of the Session.

        Returns:
            string of the init script.
        """
        return self.cpp.getInitScript()

    def setInitScript(self, script: str) -> None:
        """Set up init script of the Session.

        Args:
            string of the init script.
        """
        self.cpp.setInitScript(script)

    def saveTable(self, tbl: Table, dbPath: str) -> bool:
        """Save the table.

        Args:
            tbl : DolphinDB Table object of the in-memory table to be saved.
            dbPath : DolphinDB database path.

        Returns:
            True.
        """
        tblName = tbl.tableName()
        dbName =  _generate_dbname()
        s1 = dbName+"=database('"+dbPath+"')"
        self.run(s1)
        s2 = "saveTable(%s, %s)" % (dbName, tblName)
        self.run(s2)
        return True

    def loadText(self,  remoteFilePath: str, delimiter: str = ",") -> "Table":
        """Import text files into DolphinDB as an in-memory table.

        Args:
            remoteFilePath : the remote file path on the server. Defaults to None.
            delimiter : delimiter, Defaults to ",".

        Returns:
            a DolphinDB in-memory Table object.

        Note:
            The amount of data loaded into the in-memory table must be less than the available memory.
        """
        tableName = _generate_tablename()
        runstr = tableName + '=loadText("' + remoteFilePath + '","' + delimiter + '")'
        self.run(runstr)
        return Table(data=tableName, s=self, isMaterialized=True)

    def loadTextEx(
        self, dbPath: str, tableName: str, partitionColumns: Optional[List[str]] = None,
        remoteFilePath: str = None, delimiter: str = ",", sortColumns: Optional[List[str]] = None,
    ) -> "Table":
        """Import a partitioned in-memory table.

        Args:
            dbPath : database path.
            tableName : table name.
            partitionColumns : list of strings indicating the partitioning columns. Defaults to None.
            remoteFilePath : remote file path. Defaults to None.
            delimiter : delimiter of each column. Defaults to ",".
            sortColumns : list of strings indicating the sort columns. Defaults to None.

        Returns:
            a DolphinDB Table object.
        """
        if partitionColumns is None:
            partitionColumns = []
        if sortColumns is None:
            sortColumns = []
        isDBPath = True
        if "/" in dbPath or "\\" in dbPath or "dfs://" in dbPath:
            dbstr = 'db=database("' + dbPath + '")'
            self.run(dbstr)
            tbl_str = '{tableNameNEW} = loadTextEx(db, "{tableName}", {partitionColumns}, "{remoteFilePath}", {delimiter} {extraParams})'
        else:
            isDBPath = False
            tbl_str = '{tableNameNEW} = loadTextEx("{dbPath}", "{tableName}", {partitionColumns}, "{remoteFilePath}", {delimiter} {extraParams})'
        fmtDict = dict()
        fmtDict['tableNameNEW'] = _generate_tablename()
        fmtDict['dbPath'] = dbPath
        fmtDict['tableName'] = tableName
        fmtDict['partitionColumns'] = '[' + ','.join([f'"{_}"' for _ in partitionColumns]) + ']'
        fmtDict['remoteFilePath'] = remoteFilePath if remoteFilePath is not None else ""
        fmtDict['delimiter'] = delimiter
        if sortColumns:
            extraParams = ", sortColumns=" + '[' + ','.join([f'"{_}"' for _ in sortColumns]) + ']'
        else:
            extraParams = ""
        fmtDict['extraParams'] = extraParams
        tbl_str = re.sub(' +', ' ', tbl_str.format(**fmtDict).strip())
        self.run(tbl_str)
        if isDBPath:
            return Table(data=fmtDict['tableName'], dbPath=dbPath, s=self)
        else:
            return Table(data=fmtDict['tableNameNEW'], s=self)

    def ploadText(self, remoteFilePath: str, delimiter: str = ",") -> "Table":
        """Import text files in parallel into DolphinDB as a partitioned in-memory table, which is faster than method loadText.

        Args:
            remoteFilePath : the remote file path on the server. Defaults to None.
            delimiter : delimiter, Defaults to ",".

        Returns:
            a DolphinDB in-memory Table object.
        """
        tableName = _generate_tablename()
        runstr = tableName + '= ploadText("' + remoteFilePath + '","' + delimiter + '")'
        self.run(runstr)
        return Table(data=tableName, s=self, isMaterialized=True)

    def table(
        self, dbPath: str = None, data=None,  tableAliasName: str = None,
        inMem: bool = False, partitions: Optional[List[str]] = None
    ) -> "Table":
        """Create a DolphinDB table object and upload it to the server.

        Deprecated:
            inMem will be deprecated in a future version.

        Args:
            dbPath : database path. Defaults to None.
            data : data of the table. Defaults to None.
            tableAliasName : alias of the table. Defaults to None.
            inMem : whether to load the table into memory. True: to load the table into memory. Defaults to False.
            partitions : the partitions to be loaded into memory. Defaults to None, which means to load all partitions.

        Returns:
            a DolphinDB table object.
        """
        if partitions is None:
            partitions = []
        return Table(dbPath=dbPath, data=data,  tableAliasName=tableAliasName, inMem=inMem, partitions=partitions, s=self, isMaterialized=True)

    def loadTable(self, tableName: str,  dbPath: Optional[str] = None, partitions=None, memoryMode: bool = False) -> "Table":
        """Load a DolphinDB table.

        Args:
            tableName : the DolphinDB table name.
            dbPath : path to the DolphinDB database. Defaults to None.
            partitions : the partitioning columns of the partitioned table. Defaults to None.
            memoryMode : whether to load the table into memory or just load its column names. True: to load the table into memory. Defaults to False.

        Deprecated:
            memoryMode will be deprecated in a future version.

        Returns:
            a DolphinDB table object.
        """
        def isDate(s):
            try:
                datetime.strptime(s, '%Y.%m.%d')
                return True
            except ValueError:
                return False

        def isMonth(s):
            try:
                datetime.strptime(s, '%Y.%mM')
                return True
            except ValueError:
                return False

        def isDatehour(s):
            try:
                datetime.strptime(s, '%Y.%m.%dT%H')
                return True
            except ValueError:
                return False

        def isTime(s):
            return isDate(s) or isMonth(s) or isDatehour(s)

        def myStr(x):
            if type(x) is str and not isTime(x):
                return "'" + x + "'"
            else:
                return str(x)

        if partitions is None:
            partitions = []
        if dbPath:
            runstr = '{tableName} = loadTable("{dbPath}", "{data}",{partitions},{inMem})'
            fmtDict = dict()
            tbName = _generate_tablename(tableName)
            fmtDict['tableName'] = tbName
            fmtDict['dbPath'] = dbPath
            fmtDict['data'] = tableName
            if type(partitions) is list:
                fmtDict['partitions'] = ('[' + ','.join(myStr(x) for x in partitions) + ']') if len(partitions) else ""
            else:
                fmtDict['partitions'] = myStr(partitions)

            fmtDict['inMem'] = str(memoryMode).lower()
            runstr = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            self.run(runstr)
            return Table(data=tbName, s=self, isMaterialized=True)
        else:
            return Table(data=tableName, s=self, needGC=False, isMaterialized=True)

    def loadTableBySQL(self, tableName: str, dbPath: str, sql: str) -> "Table":
        """Load records that satisfy the filtering conditions in a SQL query as a partitioned in-memory table.

        Args:
            tableName : the DolphinDB table name.
            dbPath : the DolphinDB database where the table is stored.
            sql : SQL statement.

        Returns:
            a Table object.
        """
        # loadTableBySQL
        tmpTableName = _generate_tablename()
        self.run(f"""
            {tableName}=loadTable("{dbPath}", "{tableName}");
            {tmpTableName}=loadTableBySQL(<{sql}>);
        """)
        return Table(data=tmpTableName, s=self, isMaterialized=True)

    def database(
        self, dbName: str = None, partitionType: int = None, partitions=None,
        dbPath: str = None, engine: str = None, atomic: str = None, chunkGranularity: str = None
    ) -> "Database":
        """Create database.

        Args:
            dbName : database name. Defaults to None.
            partitionType : partition type. Defaults to None.
            partitions : describes how the partitions are created. Partitions is usually a list or np.array, with type char/int/np.datetime64. Defaults to None.
            dbPath : database path. Defaults to None.
            engine: storage engine, can be 'OLAP', 'TSDB' or 'PKEY'. Defaults to None, meaning to "OLAP".
            atomic : indicates at which level the atomicity is guaranteed for a write transaction. It can be 'TRANS' or 'CHUNK'. Defaults to None, meaning to "TRANS".
            chunkGranularity : the chunk granularity, can be 'TABLE' or 'DATABASE'. Defaults to None.

        Note:
            The parameter chunkGranularity only takes effect when enableChunkGranularityConfig = true.

        Returns:
            a DolphinDB database object.
        """
        if partitions is not None:
            partition_type = type(partitions[0])
        else:
            partition_type = None

        if partition_type == np.datetime64:
            partition_str = self.convertDatetime64(partitions)

        elif partition_type == Database:
            partition_str = self.convertDatabase(partitions)

        elif type(partitions) == np.ndarray and (partition_type == np.ndarray or partition_type == list):
            dataType = type(partitions[0][0])
            partition_str = '['
            for partition in partitions:
                if dataType == date or dataType == month:
                    partition_str += self.convertDateAndMonth(partition) + ','
                elif dataType == datetime:
                    partition_str += self.convertDatetime(partition) + ','
                elif dataType == Database:
                    partition_str += self.convertDatabase(partition) + ','
                else:
                    partition_str += str(partition) + ','
                    partition_str = partition_str.replace('list', ' ')
                    partition_str = partition_str.replace('(', '')
                    partition_str = partition_str.replace(')', '')
            partition_str = partition_str.rstrip(',')
            partition_str += ']'

        else:
            if partition_type is not None:
                partition_str = str(partitions)
            else:
                partition_str = ""

        if dbName is None:
            dbName = _generate_dbname()

        if partitionType:
            if dbPath:
                dbstr = dbName + '=database("'+dbPath+'",' + str(partitionType) + "," + partition_str
            else:
                dbstr = dbName + '=database("",' + str(partitionType) + "," + partition_str
        else:
            if dbPath:
                dbstr = dbName + '=database("' + dbPath + '"'
            else:
                dbstr = dbName + '=database(""'

        if engine is not None:
            dbstr += ",engine='" + engine + "'"
        if atomic is not None:
            dbstr += ",atomic='" + atomic + "'"
        if self.enableChunkGranularityConfig and chunkGranularity is not None:
            dbstr += ",chunkGranularity='" + chunkGranularity + "'"

        dbstr += ")"

        self.run(dbstr)
        return Database(dbName=dbName, s=self)

    def existsDatabase(self, dbUrl: str) -> bool:
        """Check if the database exists.

        Args:
            dbUrl : database path.

        Returns:
            True if the database exists, otherwise False.
        """
        return self.run("existsDatabase('%s')" % dbUrl)

    def existsTable(self, dbUrl: str, tableName: str) -> bool:
        """Check if the table exists.

        Args:
            dbUrl : database path.
            tableName : table name.

        Returns:
            True if the table exists, otherwise False.
        """
        return self.run("existsTable('%s','%s')" % (dbUrl, tableName))

    def dropDatabase(self, dbPath: str) -> None:
        """Delete a database.

        Args:
            dbPath : database path.
        """
        self.run("dropDatabase('" + dbPath + "')")

    def dropPartition(self, dbPath: str, partitionPaths: Union[str, List[str]], tableName: str = None) -> None:
        """Delete one or more partitions of the database.

        Args:
            dbPath : database path.
            partitionPaths : a string or list of partition paths.

        Note:
            The directory of a partition under the database folder or a list of directories of multiple partitions must start with '/'.

        Args:
            tableName : table name. Defaults to None, indicating all tables under the partitions are deleted.
        """
        db = _generate_dbname()
        self.run(f"""{db} = database("{dbPath}")""")
        if isinstance(partitionPaths, list):
            pths = ','.join(partitionPaths)
        else:
            pths = partitionPaths

        if tableName:
            self.run(f"""dropPartition({db}, [{pths}], "{tableName}")""")
        else:
            self.run(f"""dropPartition({db}, [{pths}])""")
        self.run(f"""undef("{db}")""")

    def dropTable(self, dbPath: str, tableName: str) -> None:
        """Delete a table.

        Args:
            dbPath : database path.
            tableName : table name.
        """
        db = _generate_dbname()
        self.run(db + '=database("' + dbPath + '")')
        self.run(f'dropTable({db}, "{tableName}")')

    def undef(self, varName: str, varType: str) -> None:
        """Release the specified object in the Session.

        Args:
            varName : variable name in DolphinDB.
            varType : variable type in DolphinDB,including "VAR" (variable), "SHARED" (shared variable), "DEF" (function definition).
        """
        undef_str = 'undef("{varName}", {varType})'
        fmtDict = dict()
        fmtDict['varName'] = varName
        fmtDict['varType'] = varType
        self.run(undef_str.format(**fmtDict).strip())

    def undefAll(self) -> None:
        """Release all objects in the Session."""
        self.run("undef all")

    def clearAllCache(self, dfs: bool = False) -> None:
        """Clear all cache.

        Args:
            dfs : True: clear cache of all nodes; False (default): only clear the cache of the connected node. Defaults to False.
        """
        if dfs:
            self.run("pnodeRun(clearAllCache)")
        else:
            self.run("clearAllCache()")

    @classmethod
    def enableJobCancellation(cls) -> None:
        """Enable cancellation of running jobs.

        When this method is enabled, all running jobs (executed with Session.run()) in the process will be canceled immediately. This method can only be executed once for each Session.
        This method is not supported in high availability secenarios.
        This method is not recommended in multithreaded mode. In multithreaded mode, make sure the main process of signal is imported before you call this method.

        """
        if platform.system() == "Linux":
            ddbcpp.sessionimpl.enableJobCancellation()
        else:
            raise RuntimeError("This method is only supported on Linux.")

    @classmethod
    def setTimeout(cls, timeout: float) -> None:
        """Set the TCP timeout.

        Args:
            timeout : corresponds to the TCP_USER_TIMEOUT option. Specify the value in units of seconds. 
        """
        ddbcpp.sessionimpl.setTimeout(timeout)

    def nullValueToZero(self) -> None:
        """Convert all NULL values to 0.

        Deprecated:
            this method will be deleted in a future version.
        """
        self.cpp.nullValueToZero()

    def nullValueToNan(self) -> None:
        """Convert all NULL values to NumPy NaN.

        Deprecated:
            this method will be delete in a future version.
        """
        self.cpp.nullValueToNan()

    def hashBucket(self, obj, nBucket: int):
        """Hash map, which maps DolphinDB objects into hash buckets of size nBucket.

        Args:
            obj : the DolphinDB object to be hashed.
            nBucket : hash bucket size.

        Returns:
            hash of the DolphinDB object.
        """
        if not isinstance(nBucket, int) or nBucket <= 0:
            raise ValueError("nBucket must be a positive integer")
        return self.cpp.hashBucket(obj, nBucket)

    def convertDatetime64(self, datetime64List):
        length = len(str(datetime64List[0]))
        # date and month
        if length == 10 or length == 7:
            listStr = '['
            for dt64 in datetime64List:
                s = str(dt64).replace('-', '.')
                if len(str(dt64)) == 7:
                    s += 'M'
                listStr += s + ','
            listStr = listStr.rstrip(',')
            listStr += ']'
        else:
            listStr = 'datehour(['
            for dt64 in datetime64List:
                s = str(dt64).replace('-', '.').replace('T', ' ')
                ldt = len(str(dt64))
                if ldt == 13:
                    s += ':00:00'
                elif ldt == 16:
                    s += ':00'
                listStr += s + ','
            listStr = listStr.rstrip(',')
            listStr += '])'
        return listStr

    def convertDatabase(self, databaseList):
        listStr = '['
        for db in databaseList:
            listStr += db._getDbName()
            listStr += ','
        listStr = listStr.rstrip(',')
        listStr += ']'
        return listStr

    def _loadPickleFile(self, filePath):
        return self.cpp.loadPickleFile(filePath)

    def _printPerformance(self):
        self.cpp.printPerformance()


class BlockReader(object):
    """Read in blocks.

    Specify the paramter fetchSize for method Session.run and it returns a BlockReader object to read in blocks.

    Args:
        blockReader : dolphindbcpp object.
    """
    def __init__(self, blockReader):
        """Constructor of BlockReader."""
        self.block = blockReader

    def read(self):
        """Read a piece of data.

        Returns:
            execution result of a script.
        """
        return self.block.read()

    def hasNext(self) -> bool:
        """Check if there is data to be read.

        Returns:
            True if there is still data not read, otherwise False.
        """
        return self.block.hasNext()

    def skipAll(self):
        """Skip subsequent data.

        Note:
            When reading data in blocks, if not all blocks are read, please call the skipAll method to abort the reading before executing the subsequent code.
            Otherwise, data will be stuck in the socket buffer and the deserialization of the subsequent data will fail.
        """
        self.block.skipAll()


class PartitionedTableAppender(object):
    """Class for writes to DolphinDB DFS tables.

    Args:
        dbPath : DFS database path. Defaults to None.
        tableName : name of a DFS table. Defaults to None.
        partitionColName : partitioning column. Defaults to None.
        dbConnectionPool : connection pool. Defaults to None.

    Note:
        DolphinDB does not allow multiple writers to write data to the same partition at the same time, so when the client writes data in parallel with multiple threads, please ensure that each thread writes to a different partition.
        The python API provides PartitionedTableAppender, an easy way to automatically split data writes by partition.
    """
    def __init__(
        self, dbPath: str = None, tableName: str = None,
        partitionColName: str = None, dbConnectionPool: DBConnectionPool = None
    ):
        """Constructor of PartitionedTableAppender."""
        if dbPath is None:
            dbPath = ""
        if tableName is None:
            tableName = ""
        if partitionColName is None:
            partitionColName = ""
        if not isinstance(dbConnectionPool, DBConnectionPool):
            raise Exception("dbConnectionPool must be a dolphindb DBConnectionPool!")
        self.appender = ddbcpp.partitionedTableAppender(dbPath, tableName, partitionColName, dbConnectionPool.pool)
        self.pool = dbConnectionPool

    def append(self, table: DataFrame) -> int:
        """Append data.

        Args:
            table : data to be written.

        Returns:
            number of rows written.
        """
        if self.pool.is_shutdown():
            raise RuntimeError("DBConnectionPool has been shut down.")
        return self.appender.append(table)


class TableAppender(object):
    """Class of table appender.

    As the only temporal data type in Python pandas is datetime64, all temporal columns of a DataFrame are converted into nanotimestamp type after uploaded to DolphinDB.
    Each time we use tableInsert or insert into to append a DataFrame with a temporal column to an in-memory table or DFS table, we need to conduct a data type conversion for the time column.
    For automatic data type conversion, Python API offers tableAppender object.

    Args:
        dbPath : the path of a DFS database. Leave it unspecified for in-memory tables. Defaults to None.
        tableName : table name. Defaults to None.
        ddbSession : a Session connected to DolphinDB server. Defaults to None.
        action : the action when appending. Now only supports "fitColumnType", indicating to convert the data type of temporal column. Defaults to "fitColumnType".
    """
    def __init__(self, dbPath: str = None, tableName: str = None, ddbSession: Session = None, action: str = "fitColumnType"):
        """Constructor of tableAppender"""
        if dbPath is None:
            dbPath = ""
        if tableName is None:
            tableName = ""
        if not isinstance(ddbSession, Session):
            raise Exception("ddbSession must be a dolphindb Session!")
        if action == "fitColumnType":
            self.tableappender = ddbcpp.autoFitTableAppender(dbPath, tableName, ddbSession.cpp)
            self.sess = ddbSession
        else:
            raise Exception("other action not supported yet!")

    def append(self, table: DataFrame) -> int:
        """Append data.

        Args:
            table : data to be written.

        Returns:
            number of rows written.
        """
        if self.sess.isClosed():
            raise RuntimeError("Session has been closed.")
        return self.tableappender.append(table)


class TableUpserter(object):
    """Class of table upserter.

    Args:
        dbPath : the path of a DFS database. Leave it unspecified for in-memory tables. Defaults to None.
        tableName : table name. Defaults to None.
        ddbSession : a Session connected to DolphinDB server. Defaults to None.
        ignoreNull : if set to true, for the NULL values in the new data, the correponding elements in the table are not updated. Defaults to False.
        keyColNames : key column names. For a DFS table, the columns specified by this parameter are considrd as key columns. Defaults to [].
        sortColumns : sort column names. All data in the updated partition will be sorted according to the specified column. Defaults to [].
    """
    def __init__(
        self, dbPath: str = None, tableName: str = None,
        ddbSession: Session = None, ignoreNull: bool = False,
        keyColNames: List[str] = [], sortColumns: List[str] = []
    ):
        """Constructor of tableUpsert."""
        if dbPath is None:
            dbPath = ""
        if tableName is None:
            tableName = ""
        if not isinstance(ddbSession, Session):
            raise Exception("ddbSession must be a dolphindb Session!")
        self.tableupserter = ddbcpp.autoFitTableUpsert(dbPath, tableName, ddbSession.cpp, ignoreNull, keyColNames, sortColumns)
        self.sess = ddbSession

    def __del__(self):
        self.tableupserter = None

    def upsert(self, table: DataFrame):
        """upsert data.

        Args:
            table : data to be written.
        """
        if self.sess.isClosed():
            raise RuntimeError("Session has been closed.")
        self.tableupserter.upsert(table)


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
        self, host: str, port: int,
        userid: str = None, password: str = None,
        acquireLock: bool = True
    ):
        """Constructor of BatchTableWriter"""
        if userid is None:
            userid = ""
        if password is None:
            password = ""
        self.writer = ddbcpp.batchTableWriter(host, port, userid, password, acquireLock)

    def addTable(
        self, dbPath: str = None, tableName: str = None,
        partitioned: bool = True
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

    def getStatus(self, dbPath: str = None, tableName: str = None) -> Tuple[int, bool, bool]:
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

    def getUnwrittenData(self, dbPath: str = None, tableName: str = None) -> DataFrame:
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
        mode : The write mode. It can be Append (default) or Upsert. Defaults to "".
        modeOption : The parameters of function upsert!. It only takes effect when mode is Upsert. Defaults to [].
    """
    def __init__(
        self, host: str, port: int, userId: str, password: str,
        dbPath: str, tableName: str, useSSL: bool = False,
        enableHighAvailability: bool = False, highAvailabilitySites: List[str] = [],
        batchSize: int = 1, throttle: float = 1.0, threadCount: int = 1,
        partitionCol: str = "", compressMethods: List[str] = [],
        mode: str = "", modeOption: List[str] = []
    ):
        """Constructor of MultithreadedTableWriter"""
        self.writer = ddbcpp.multithreadedTableWriter(
            host, port, userId, password, dbPath, tableName, useSSL,
            enableHighAvailability, highAvailabilitySites, batchSize, throttle, threadCount,
            partitionCol, compressMethods, mode, modeOption
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
        errorCodeInfo.__dict__.update(self.writer.insertUnwrittenData(unwrittenData))
        return errorCodeInfo

    def waitForThreadCompletion(self) -> None:
        """Wait until all working threads complete their tasks.

        After calling the method, MultithreadedTableWriter will wait until all working threads complete their tasks.

        """
        self.writer.waitForThreadCompletion()
