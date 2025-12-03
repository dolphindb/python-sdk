from threading import RLock
from typing import overload

from ._hints import (
    Dict, List, Literal, Optional, Union, Any
)
from ._core import DolphinDBRuntime
from .config import ConnectionConfig, ConnectionSetting, organize_config

ddbcpp = DolphinDBRuntime()._ddbcpp


def _safe_check(func):
    def wrapper(self, *args, **kwargs):
        with self._mutex:
            self._check_in_block()
            return func(self, *args, **kwargs)
    return wrapper


class DBConnection:
    @overload
    def __init__(
        self,
        *,
        enable_ssl: bool = False,
        enable_async: bool = False,
        compress: bool = False,
        parser: Literal["dolphindb", "python", "kdb"] = "dolphindb",
        protocol: Literal["default", "pickle", "ddb", "arrow"] = "default",
        show_output: bool = True,
        sql_std: Literal["dolphindb", "oracle", "mysql"] = "dolphindb",
    ): ...

    @overload
    def __init__(self, /, config: Union[ConnectionSetting, dict] = None, **kwargs): ...

    def __init__(self, /, config: Union[ConnectionSetting, dict] = None, **kwargs):
        """Initialize a DBConnection object.

        Args:
            config (Union[ConnectionConfig, ConnectionSetting, None]): Configuration for the connection.
        """
        config = organize_config(ConnectionSetting, config, kwargs)

        self._is_closed = True
        self.cpp = ddbcpp.sessionimpl(
            config.enable_ssl,
            config.enable_async,
            30,
            config.compress,
            config.parser.value,
            config.protocol,
            config.show_output,
            config.sql_std.value,
        )

        self._mutex = RLock()    # busy check
        self._is_closed = False
        self._in_block_reading = False

    def __del__(self):
        if hasattr(self, "cpp"):
            self.close()

    @property
    def session_id(self) -> str:
        return self.cpp.session_id

    @property
    def host(self) -> str:
        return self.cpp.host

    @property
    def port(self) -> int:
        return self.cpp.port

    @property
    def startup(self) -> str:
        return self.cpp.startup

    @startup.setter
    def startup(self, script: str):
        self.cpp.startup = script

    @property
    @_safe_check
    def is_alive(self) -> bool:
        return self.cpp.is_alive

    @property
    def is_closed(self) -> bool:
        return self._is_closed

    @property
    def is_busy(self) -> bool:
        """Check if the connection is busy.

        Returns:
            True if the connection is busy, otherwise False.
        """
        if self._mutex.acquire(False):
            if self._in_block_reading:
                self._mutex.release()
                return True
            self._mutex.release()
            return False
        else:
            return True

    @property
    def msg_logger(self):
        return self.cpp.msg_logger

    def _check_in_block(self):
        if self._in_block_reading:
            raise RuntimeError("Cannot execute other commands when fetching data in blocks.")

    def _release_block(self):
        with self._mutex:
            self._in_block_reading = False

    @overload
    def connect(
        self,
        host: str,
        port: int,
        userid: str = None,
        password: str = None,
        *,
        startup: str = None,
        enable_high_availability: bool = False,
        high_availability_sites: Optional[List[str]] = None,
        keep_alive_time: int = 30,
        reconnect: bool = False,
        try_reconnect_nums: Optional[int] = -1,
        read_timeout: Optional[int] = -1,
        write_timeout: Optional[int] = -1,
        use_public_name: bool = False,
    ) -> bool: ...

    @overload
    def connect(
        self,
        *,
        config: ConnectionConfig = None,
        **kwargs,
    ) -> bool: ...

    @_safe_check
    def connect(
        self,
        host: str = None,
        port: int = None,
        userid: str = None,
        password: str = None,
        *,
        config: ConnectionConfig = None,
        **kwargs,
    ) -> bool:
        """Connect to a DolphinDB server.

        Args:
            config (Union[ConnectionConfig, dict, None]): Configuration for the connection.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        if host is not None:
            kwargs['host'] = host
        if port is not None:
            kwargs['port'] = port
        if userid is not None:
            kwargs['userid'] = userid
        if password is not None:
            kwargs['password'] = password
        config = organize_config(ConnectionConfig, config, kwargs)
        res = self.cpp.connect(config.model_dump())
        if res:
            self._is_closed = False
        return res

    @_safe_check
    def login(self, userid: str, password: str, enable_encryption: bool = True):
        """Manually log in to the server.

        Args:
            userid : username.
            password : password.
            enable_encryption : whether to enable encrypted transmission for username and password. Defaults to True.

        Note:
            Specify the userid and password in the connect() method to log in automatically.
        """
        self.cpp.login(userid, password, enable_encryption)

    def close(self) -> None:
        if self._is_closed:
            return
        self.cpp.close()
        self._is_closed = True

    @_safe_check
    def upload(self, objs: Dict[str, Any]) -> str:
        """Upload Python objects to DolphinDB server.

        Args:
            objs : Python dictionary object. The keys of the dictionary are
                the variable names in DolphinDB and the values are Python objects,
                which can be numbers, strings, lists, DataFrame, etc.

        Returns:
            the server address of the uploaded object.

        Note:
            A pandas DataFrame corresponds to DolphinDB table.
        """
        return self.cpp.upload(objs)

    @_safe_check
    def exec(
        self,
        script: str,
        *,
        clear_memory: bool = False,
        table_to_list: bool = False,
        priority: int = 4,
        parallelism: int = 64,
        fetch_size: Optional[int] = None,
        disable_decimal: bool = False,
    ) -> Any:
        """Execute script.

        Args:
            script : DolphinDB script to be executed.

        Kwargs:
            clear_memory : whether to release variables after queries.
                True means to release, otherwise False. Defaults to False.
            table_to_list : whether to convert table to list or DataFrame.
                True: to list, False: to DataFrame.  Defaults to False.
            priority : a job priority system with 10 priority levels (0 to 9),
                allocating thread resources to high-priority jobs and using
                round-robin allocation for jobs of the same priority. Defaults to 4.
            parallelism : parallelism determines the maximum number of threads
                to execute a job's tasks simultaneously on a data node; the
                system optimizes resource utilization by allocating all available
                threads to a job if there's only one job running and multiple
                local executors are available. Defaults to 64.
            fetch_size : the size of a block.
            disable_decimal: whether to convert decimal to double in the result
                table. True: convert to double, False: return as is. Defaults to False.

        Note:
            fetch_size cannot be less than 8192 Bytes.

        Returns:
            execution result. If fetch_size is specified, a BlockReader object
            will be returned. Each block can be read with the read() method.

        Note:
            When setting table_to_list=True and protocol=PROTOCOL_PICKLE, if the
            table contains array vectors, it will be converted to a NumPy 2d array.
            If the length of each row is different, the execution fails.
        """
        if priority is not None:
            if not isinstance(priority, int) or priority > 9 or priority < 0:
                raise RuntimeError("priority must be an integer from 0 to 9")
        if parallelism is not None:
            if not isinstance(parallelism, int) or parallelism <= 0:
                raise RuntimeError("parallelism must be an integer greater than 0")

        if fetch_size is not None:
            self._in_block_reading = True
            return BlockReader(
                self.cpp.runBlock(
                    script,
                    clearMemory=clear_memory,
                    fetchSize=fetch_size,
                    priority=priority,
                    parallelism=parallelism,
                ), self
            )
        return self.cpp.exec(
            script,
            clearMemory=clear_memory,
            pickleTableToList=table_to_list,
            priority=priority,
            parallelism=parallelism,
            disableDecimal=disable_decimal,
        )

    def _exec_with_table_schema(
        self,
        script: str,
        *,
        clear_memory: bool = False,
        table_to_list: bool = False,
        priority: int = 4,
        parallelism: int = 64,
        disable_decimal: bool = False,
    ):
        if priority is not None:
            if not isinstance(priority, int) or priority > 9 or priority < 0:
                raise RuntimeError("priority must be an integer from 0 to 9")
        if parallelism is not None:
            if not isinstance(parallelism, int) or parallelism <= 0:
                raise RuntimeError("parallelism must be an integer greater than 0")

        return self.cpp.exec(
            script,
            clearMemory=clear_memory,
            pickleTableToList=table_to_list,
            priority=priority,
            parallelism=parallelism,
            disableDecimal=disable_decimal,
            withTableSchema=True
        )

    @_safe_check
    def call(
        self,
        func: str,
        *args,
        clear_memory: bool = False,
        table_to_list: bool = False,
        priority: int = 4,
        parallelism: int = 64,
        disable_decimal: bool = False,
    ) -> Any:
        """Execute function.

        Args:
            func : DolphinDB function name to be executed.
            args : arguments to be passed to the function.

        Kwargs:
            clear_memory : whether to release variables after queries.
                True means to release, otherwise False. Defaults to False.
            table_to_list : whether to convert table to list or DataFrame.
                True: to list, False: to DataFrame.  Defaults to False.
            priority : a job priority system with 10 priority levels (0 to 9),
                allocating thread resources to high-priority jobs and using
                round-robin allocation for jobs of the same priority. Defaults to 4.
            parallelism : parallelism determines the maximum number of threads
                to execute a job's tasks simultaneously on a data node; the
                system optimizes resource utilization by allocating all available
                threads to a job if there's only one job running and multiple
                local executors are available. Defaults to 64.
            disable_decimal: whether to convert decimal to double in the result
                table. True: convert to double, False: return as is. Defaults to False.

        Note:
            fetch_size cannot be less than 8192 Bytes.

        Returns:
            execution result. If fetch_size is specified, a BlockReader object
            will be returned. Each block can be read with the read() method.

        Note:
            When setting table_to_list=True and protocol=PROTOCOL_PICKLE, if the
            table contains array vectors, it will be converted to a NumPy 2d array.
            If the length of each row is different, the execution fails.
        """
        if priority is not None:
            if not isinstance(priority, int) or priority > 9 or priority < 0:
                raise RuntimeError("priority must be an integer from 0 to 9")
        if parallelism is not None:
            if not isinstance(parallelism, int) or parallelism <= 0:
                raise RuntimeError("parallelism must be an integer greater than 0")

        return self.cpp.call(
            func,
            *args,
            clearMemory=clear_memory,
            pickleTableToList=table_to_list,
            priority=priority,
            parallelism=parallelism,
            disableDecimal=disable_decimal,
        )

    def _call_with_table_schema(
        self,
        func: str,
        *args,
        clear_memory: bool = False,
        table_to_list: bool = False,
        priority: int = 4,
        parallelism: int = 64,
        disable_decimal: bool = False,
    ):
        if priority is not None:
            if not isinstance(priority, int) or priority > 9 or priority < 0:
                raise RuntimeError("priority must be an integer from 0 to 9")
        if parallelism is not None:
            if not isinstance(parallelism, int) or parallelism <= 0:
                raise RuntimeError("parallelism must be an integer greater than 0")

        return self.cpp.call(
            func,
            *args,
            clearMemory=clear_memory,
            pickleTableToList=table_to_list,
            priority=priority,
            parallelism=parallelism,
            disableDecimal=disable_decimal,
            withTableSchema=True
        )

    def run(self, script, *args, **kwargs):
        if args:
            return self.call(script, *args, **kwargs)
        else:
            return self.exec(script, **kwargs)

    def _run_with_table_schema(self, script, *args, **kwargs):
        if args:
            return self._call_with_table_schema(script, *args, **kwargs)
        else:
            return self._exec_with_table_schema(script, **kwargs)


class BlockReader(object):
    """Read in blocks.

    Specify the paramter fetchSize for method Session.run and it returns a BlockReader object to read in blocks.

    Args:
        blockReader : dolphindbcpp object.
    """
    def __init__(self, blockReader, connection: DBConnection):
        """Constructor of BlockReader."""
        self.block = blockReader
        self.conn = connection

    def __del__(self):
        if self.conn is not None and not self.conn.is_closed:
            self.skip_all()

    def __iter__(self):
        return self

    def __next__(self):
        if self.has_next:
            return self.read()
        else:
            raise StopIteration

    def read(self):
        """Read a piece of data.

        Returns:
            execution result of a script.
        """
        data = self.block.read()
        if not self.has_next:
            if self.conn:
                self.conn._release_block()
                self.conn = None
        return data

    @property
    def has_next(self) -> bool:
        """Check if there is data to be read.

        Returns:
            True if there is still data not read, otherwise False.
        """
        return self.block.hasNext()

    def hasNext(self) -> bool:
        return self.has_next

    def skip_all(self):
        """Skip subsequent data.

        Note:
            When reading data in blocks, if not all blocks are read, please call the skipAll method to abort the reading before executing the subsequent code.
            Otherwise, data will be stuck in the socket buffer and the deserialization of the subsequent data will fail.
        """
        self.block.skipAll()
        if self.conn:
            self.conn._release_block()
            self.conn = None

    skipAll = skip_all
