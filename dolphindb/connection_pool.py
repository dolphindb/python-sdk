import asyncio
import sys
import weakref
import time
import threading
from concurrent.futures import Future
from threading import RLock, Thread, Lock
from pydantic import Field, model_validator

from ._core import DolphinDBRuntime
from ._hints import List, Literal, Optional, Union, Set
from .settings import (
    PROTOCOL_ARROW,
    PROTOCOL_DDB,
    PROTOCOL_DEFAULT,
    PROTOCOL_PICKLE,
    ParserType,
    SqlStd,
    default_protocol,
    DDB_EPSILON,
)
from .utils import _helper_check_parser, standard_deprecated, deprecated
from .connection import DBConnection
from .config import ConnectionConfig, ConnectionSetting, organize_config


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
        python : whether to enable the Python parser. True means enabled, otherwise False. Defaults to False.
        protocol : the data transmission protocol. Defaults to PROTOCOL_DEFAULT, which is equivalant to PROTOCOL_DDB.

    Kwargs:
        show_output : whether to display the output of print statements in the script after execution. Defaults to True.
        sqlStd: an enum specifying the syntax to parse input SQL scripts. Three parsing syntaxes are supported: DolphinDB (default), Oracle, and MySQL.
        tryReconnectNums: the number of reconnection attempts when reconnection is enabled. If high availability mode is on, it will retry tryReconnectNums times for each node. None means unlimited reconnection attempts. Defaults to None.
        parser: the type of parser used by the server when parsing scripts. Available options: "dolphindb", "python", "kdb". Defaults to ParserType.Default, which is equivalant to "dolphindb".

    Note:
        If the parameter python is True, the script is parsed in Python rather than DolphinDB language.
    """
    def __init__(
        self,
        host: str,
        port: int,
        threadNum: int = 10,
        userid: str = None,
        password: str = None,
        loadBalance: bool = False,
        highAvailability: bool = False,
        compress: bool = False,
        reConnect: bool = False,
        python: bool = False,
        protocol: int = PROTOCOL_DEFAULT,
        *,
        show_output: bool = True,
        sqlStd: SqlStd = SqlStd.DolphinDB,
        tryReconnectNums: Optional[int] = None,
        parser: Union[ParserType, Literal["dolphindb", "python", "kdb"]] = ParserType.Default,
        usePublicName: bool = False,
    ):
        """Constructor of DBConnectionPool, including the number of threads, load balancing, high availability, reconnection, compression and Pickle protocol."""
        userid = userid if userid is not None else ""
        password = password if password is not None else ""

        if protocol == PROTOCOL_DEFAULT:
            protocol = default_protocol

        if protocol not in [PROTOCOL_DEFAULT, PROTOCOL_DDB, PROTOCOL_PICKLE, PROTOCOL_ARROW]:
            raise RuntimeError(f"Protocol {protocol} is not supported. ")

        if protocol == PROTOCOL_ARROW and compress:
            raise RuntimeError("The Arrow protocol does not support compression.")

        if protocol == PROTOCOL_ARROW:
            __import__("pyarrow")

        if protocol == PROTOCOL_PICKLE:
            standard_deprecated(
                "The use of PROTOCOL_PICKLE has been deprecated and removed in Python 3.13. "
                "Please migrate to an alternative protocol or serialization method."
            )
            if sys.version_info.minor >= 13:
                protocol = default_protocol

        if tryReconnectNums is None:
            tryReconnectNums = -1

        parser = _helper_check_parser(parser, python)

        self.mutex = Lock()
        self.pool = ddbcpp.dbConnectionPoolImpl(
            host,
            port,
            threadNum,
            userid,
            password,
            loadBalance,
            highAvailability,
            compress,
            reConnect,
            parser.value,
            protocol,
            show_output,
            sqlStd.value,
            tryReconnectNums,
            usePublicName,
        )
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

    async def run(
        self,
        script: str,
        *args,
        clearMemory: Optional[bool] = None,
        pickleTableToList: Optional[bool] = None,
        priority: Optional[int] = None,
        parallelism: Optional[int] = None,
        disableDecimal: Optional[bool] = None,
    ):
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
            parallelism : parallelism determines the maximum number of threads to execute a job's tasks simultaneously on a data node; the system optimizes resource utilization by allocating all available threads to a job if there's only one job running and multiple local executors are available. Defaults to 64.
            disableDecimal: whether to convert decimal to double in the result table. True: convert to double, False: return as is. Defaults to False.

        Returns:
            execution result.

        Note:
            When setting pickleTableToList=True and enablePickle=True, if the table contains array vectors, it will be converted to a NumPy 2d array.
            If the length of each row is different, the execution fails.
        """
        if clearMemory is None:
            clearMemory = True
        if priority is not None:
            if not isinstance(priority, int) or priority > 9 or priority < 0:
                raise RuntimeError("priority must be an integer from 0 to 9")
        if parallelism is not None:
            if not isinstance(parallelism, int) or parallelism <= 0:
                raise RuntimeError("parallelism must be an integer greater than 0")

        self.mutex.acquire()
        self.taskId = self.taskId + 1
        tid = self.taskId
        self.mutex.release()
        self.pool.run(
            script,
            tid,
            *args,
            clearMemory=clearMemory,
            pickleTableToList=pickleTableToList,
            priority=priority,
            parallelism=parallelism,
            disableDecimal=disableDecimal,
        )
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
        self.thread.daemon = True
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
        task = asyncio.run_coroutine_threadsafe(self.run(script, *args, **kwargs), self.loop)
        return task

    @deprecated(instead="runTaskAsync")
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


class SimpleDBConnectionPoolConfig(ConnectionSetting, ConnectionConfig):
    min_pool_size: int = Field(5, gt=0)
    max_pool_size: int = Field(5, gt=0)
    idle_timeout: int = Field(600000, ge=10000)     # in milliseconds
    check_interval: int = Field(60000, ge=1000)    # in milliseconds

    @model_validator(mode='after')
    def validate_pool_sizes(self):
        if self.min_pool_size > self.max_pool_size:
            raise ValueError("min_pool_size cannot be greater than max_pool_size.")
        return self


def check_idle(f):
    def wrapper(self, *args, **kwargs):
        if self._is_idle:
            raise RuntimeError("The connection has not been acquired from the pool and cannot be used.")
        return f(self, *args, **kwargs)
    return wrapper


class PoolEntry(DBConnection):
    def __init__(self, config: SimpleDBConnectionPoolConfig, pool):
        if isinstance(config, dict):
            config = ConnectionSetting(**config)
        super().__init__(config=config)
        re = super().connect(config=config)
        if not re:
            raise RuntimeError("Failed to create a connection for the pool.")
        self.pool = pool
        self._is_idle = True
        self._last_used_time = time.time()

    def __del__(self):
        self._internal_close()

    def connect(self, *args, **kwargs):
        raise RuntimeError(
            "Connection must be created via the pool; direct calls to connect() are not permitted."
        )

    def login(self, *args, **kwargs):
        raise RuntimeError(
            "Login is only allowed when creating the connection via the pool."
        )

    def close(self):
        raise RuntimeError(
            "This connection cannot be closed directly outside the pool; use release() to return it to the pool."
        )

    @property
    def is_idle(self) -> bool:
        return self._is_idle

    @check_idle
    def exec(self, *args, **kwargs):
        return super().exec(*args, **kwargs)

    @check_idle
    def call(self, *args, **kwargs):
        return super().call(*args, **kwargs)

    @check_idle
    def run(self, *args, **kwargs):
        return super().run(*args, **kwargs)

    @check_idle
    def upload(self, *args, **kwargs):
        return super().upload(*args, **kwargs)

    def release(self):
        ref = self.pool()
        if ref:
            ref.release(self)

    def _internal_close(self):
        super().close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class SimpleDBConnectionPool:
    def __init__(self, *, config: SimpleDBConnectionPoolConfig = None, **kwargs):
        config = organize_config(SimpleDBConnectionPoolConfig, config, kwargs)

        self.config = config
        self._mutex = RLock()
        self._condition = threading.Condition(self._mutex)
        self._full = threading.Condition(self._mutex)
        self._idle_conns: Set[PoolEntry] = set()
        self._used_conns: Set[PoolEntry] = set()
        self._is_shutdown = False

        for _ in range(config.min_pool_size):
            self._add_new_connection()

        self._cleanup_thread = Thread(target=self._cleanup_idle_loop, daemon=True)
        self._cleanup_thread.start()

    def __del__(self):
        if not hasattr(self, '_is_shutdown'):
            return
        if not self._is_shutdown:
            self.shutdown()

    def _size(self) -> int:
        return len(self._idle_conns) + len(self._used_conns)

    def _cleanup_idle_internal(self):
        current_time = time.time()
        to_remove: Set[PoolEntry] = set()
        for conn in self._idle_conns:
            if (current_time - conn._last_used_time) * 1000 > self.config.idle_timeout:
                if self._size() - len(to_remove) > self.config.min_pool_size:
                    to_remove.add(conn)
        for conn in to_remove:
            self._idle_conns.remove(conn)
            conn._internal_close()

    def _cleanup_idle_force(self):
        current_time = time.time()
        to_remove: Set[PoolEntry] = set()
        for conn in self._idle_conns:
            if (current_time - conn._last_used_time) * 1000 > self.config.idle_timeout:
                if self._size() - len(to_remove) > self.config.min_pool_size:
                    to_remove.add(conn)
        for conn in to_remove:
            self._idle_conns.remove(conn)
            conn._internal_close()
        to_remove.clear()
        for conn in self._idle_conns:
            if self._size() - len(to_remove) > self.config.min_pool_size:
                to_remove.add(conn)
        for conn in to_remove:
            self._idle_conns.remove(conn)
            conn._internal_close()

    def _cleanup_idle_loop(self):
        while True:
            with self._condition:
                self._condition.wait(timeout=self.config.check_interval / 1000)

                if self._is_shutdown:
                    break
            self._cleanup_idle_internal()

    def _add_new_connection(self):
        if self._size() < self.config.max_pool_size:
            conn = PoolEntry(self.config, pool=weakref.ref(self))
            self._idle_conns.add(conn)

    def acquire(self, timeout: int = None) -> PoolEntry:
        if timeout is not None and timeout < DDB_EPSILON:
            raise ValueError("The parameter timeout must be a positive number or None when acquiring a connection.")
        with self._condition:
            start_time = time.time()
            while not self._is_shutdown:
                if self._idle_conns:
                    conn = self._idle_conns.pop()
                    conn._is_idle = False
                    self._used_conns.add(conn)
                    return conn
                if self._size() < self.config.max_pool_size:
                    self._add_new_connection()
                    continue
                if timeout is None or timeout < DDB_EPSILON:
                    raise RuntimeError("No available connections in the pool.")
                elapsed = (time.time() - start_time) * 1000
                remaining = timeout - elapsed
                if remaining < DDB_EPSILON:
                    raise RuntimeError("Timed out while waiting for a connection.")
                self._full.wait(timeout=remaining / 1000)

    def release(self, conn: PoolEntry):
        if conn.is_busy:
            raise RuntimeError("Cannot release a busy connection.")
        with self._condition:
            if self._is_shutdown:
                return
            if conn not in self._used_conns:
                raise RuntimeError("The connection does not belong to this pool or has been released.")
            self._used_conns.remove(conn)
            conn._is_idle = True
            conn._last_used_time = time.time()
            self._idle_conns.add(conn)
            if len(self._idle_conns) == 1:
                self._full.notify()

    def close_idle(self):
        with self._condition:
            self._cleanup_idle_force()

    @property
    def active_count(self) -> int:
        with self._condition:
            return len(self._used_conns)

    @property
    def idle_count(self) -> int:
        with self._condition:
            return len(self._idle_conns)

    @property
    def total_count(self) -> int:
        with self._condition:
            return self._size()

    @property
    def is_shutdown(self) -> bool:
        return self._is_shutdown

    def shutdown(self):
        with self._condition:
            if self._is_shutdown:
                return
            self._is_shutdown = True
            self._condition.notify_all()
            self._full.notify_all()

        if hasattr(self, '_cleanup_thread'):
            self._cleanup_thread.join()

        with self._condition:
            for conn in self._idle_conns:
                conn._internal_close()
            for conn in self._used_conns:
                conn._internal_close()
            self._idle_conns.clear()
            self._used_conns.clear()
