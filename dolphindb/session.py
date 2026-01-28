import re
from datetime import date, datetime

import numpy as np
from ._hints import (
    Callable, List, Literal, Optional, Union, ParserType, SqlStd
)
from ._core import DolphinDBRuntime
from .database import Database
from .settings import (
    PROTOCOL_DDB,
    PROTOCOL_DEFAULT,
    PROTOCOL_PICKLE,
    default_protocol,
)
from .table import Table
from .utils import (
    _generate_dbname, _generate_tablename, month,
    _helper_check_parser, hash_bucket,
    convert_datetime64, convert_database,
    params_deprecated,
)
from .connection import DBConnection
from .streaming import (
    ThreadedStreamingClientConfig, ThreadedClient,
    ThreadPooledStreamingClientConfig, ThreadPooledClient,
    StreamDeserializer, DEFAULT_ACTION_NAME,
)
from .global_config import enable_job_cancellation, tcp

ddbcpp = DolphinDBRuntime()._ddbcpp


class Session(DBConnection):
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
        protocol: the data transmission protocol. Defaults to PROTOCOL_DEFAULT, which is equivalant to PROTOCOL_DDB.
        python : whether to enable python parser. Defaults to False.

    Kwargs:
        show_output : whether to display the output of print statements in the script after execution. Defaults to True.
        sqlStd: an enum specifying the syntax to parse input SQL scripts. Three parsing syntaxes are supported: DolphinDB (default), Oracle, and MySQL.
        parser: the type of parser used by the server when parsing scripts. Available options: "dolphindb", "python", "kdb". Defaults to ParserType.Default, which is equivalant to "dolphindb".

    Note:
        set enableSSL =True to enable encrypted communication. It's also required to configure enableHTTPS =true in the server.

    Note:
        set enableASYNC =True to enable asynchronous mode and the communication with the server can only be done through the session.run method. As there is no return value, it is suitable for asynchronous writes.
    """
    @params_deprecated(params={"enableASYN": "enableASYNC"})
    def __init__(
        self, host: Optional[str] = None, port: Optional[int] = None,
        userid: Optional[str] = "", password: Optional[str] = "", enableSSL: bool = False,
        enableASYNC: bool = False, keepAliveTime: int = 30, enableChunkGranularityConfig: bool = False,
        compress: bool = False, enablePickle: Optional[bool] = None, protocol: int = PROTOCOL_DEFAULT,
        python: bool = False,
        *,
        enableASYN=None,
        show_output: bool = True,
        sqlStd: SqlStd = SqlStd.DolphinDB,
        parser: Union[ParserType, Literal["dolphindb", "python", "kdb"]] = ParserType.Default,
    ):
        """Constructor of Session, inluding OpenSSL encryption, asynchronous mode, TCP detection, block granularity matching, compression, Pickle protocol."""
        if enableASYN is not None:
            enableASYNC = enableASYN

        if protocol == PROTOCOL_DEFAULT:
            protocol = PROTOCOL_PICKLE if enablePickle else PROTOCOL_DDB
            if enablePickle is None:
                protocol = default_protocol
        elif enablePickle:
            raise RuntimeError("When enablePickle=true, the parameter protocol must not be specified. ")

        super().__init__(config={
            'enable_ssl': enableSSL,
            'enable_async': enableASYNC,
            'compress': compress,
            'parser': _helper_check_parser(parser, python),
            'protocol': protocol,
            'show_output': show_output,
            'sql_std': sqlStd,
        })
        self.keep_alive_time = keepAliveTime
        self.enableChunkGranularityConfig = enableChunkGranularityConfig
        if host is not None and port is not None:
            self.connect(host, port, userid, password, keepAliveTime=keepAliveTime)

    def connect(
        self, host: str, port: int, userid: str = None, password: str = None, startup: str = None,
        highAvailability: bool = False, highAvailabilitySites: Optional[List[str]] = None,
        keepAliveTime: Optional[int] = None, reconnect: bool = False,
        *,
        tryReconnectNums: Optional[int] = None,
        readTimeout: Optional[int] = None,
        writeTimeout: Optional[int] = None,
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

        Kwargs:
            tryReconnectNums ：the number of reconnection attempts when reconnection is enabled. If high availability mode is on, it will retry tryReconnectNums times for each node. None means unlimited reconnection attempts. Defaults to None.
            readTimeout : the read timeout in seconds. If None, no timeout is set. Corresponds to the `SO_RCVTIMEO` socket option. Defaults to None.
            writeTimeout : the write timeout in seconds. If None, no timeout is set. Corresponds to the `SO_SNDTIMEO` socket option. Defaults to None.

        Returns:
            whether the connection is established. True if established, otherwise False.
        """
        if keepAliveTime is None:
            keepAliveTime = self.keep_alive_time
        if userid is None:
            userid = ""
        if password is None:
            password = ""
        if startup is None:
            startup = ""

        return super().connect(config={
            'host': host,
            'port': port,
            'userid': userid,
            'password': password,
            'startup': startup,
            'enable_high_availability': highAvailability,
            'high_availability_sites': highAvailabilitySites,
            'keep_alive_time': keepAliveTime,
            'reconnect': reconnect,
            'try_reconnect_nums': tryReconnectNums,
            'read_timeout': readTimeout,
            'write_timeout': writeTimeout,
        })

    def login(self, userid: str, password: str, enableEncryption: bool = True):
        """Manually log in to the server.

        Args:
            userid : username.
            password : password.
            enableEncryption : whether to enable encrypted transmission for username and password. Defaults to True.

        Note:
            Specify the userid and password in the connect() method to log in automatically.
        """
        super().login(userid, password, enableEncryption)

    def isClosed(self) -> bool:
        """Check if the current Session has been closed.

        Returns:
            bool: True if closed, otherwise False.
        """
        return self.is_closed

    def upload(self, nameObjectDict):
        """Upload Python objects to DolphinDB server.

        Args:
            nameObjectDict : Python dictionary object. The keys of the dictionary are the variable names in DolphinDB and the values are Python objects, which can be numbers, strings, lists, DataFrame, etc.

        Returns:
            the server address of the uploaded object.

        Note:
            A pandas DataFrame corresponds to DolphinDB table.
        """
        return super().upload(nameObjectDict)

    def run(
        self,
        script: str,
        *args,
        clearMemory: Optional[bool] = None,
        pickleTableToList: Optional[bool] = None,
        priority: Optional[int] = None,
        parallelism: Optional[int] = None,
        fetchSize: Optional[int] = None,
        disableDecimal: Optional[bool] = None,
    ):
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
            parallelism : parallelism determines the maximum number of threads to execute a job's tasks simultaneously on a data node; the system optimizes resource utilization by allocating all available threads to a job if there's only one job running and multiple local executors are available. Defaults to 64.
            fetchSize : the size of a block.
            disableDecimal: whether to convert decimal to double in the result table. True: convert to double, False: return as is. Defaults to False.

        Note:
            fetchSize cannot be less than 8192 Bytes.

        Returns:
            execution result. If fetchSize is specified, a BlockReader object will be returned. Each block can be read with the read() method.

        Note:
            When setting pickleTableToList=True and enablePickle=True, if the table contains array vectors, it will be converted to a NumPy 2d array. If the length of each row is different, the execution fails.
        """
        if args:
            return self.call(
                script,
                *args,
                clear_memory=clearMemory,
                table_to_list=pickleTableToList,
                priority=priority,
                parallelism=parallelism,
                disable_decimal=disableDecimal,
            )
        else:
            return self.exec(
                script,
                clear_memory=clearMemory,
                table_to_list=pickleTableToList,
                priority=priority,
                parallelism=parallelism,
                fetch_size=fetchSize,
                disable_decimal=disableDecimal,
            )

    def _run_with_table_schema(
        self,
        script: str,
        *args,
        clearMemory: Optional[bool] = None,
        pickleTableToList: Optional[bool] = None,
        priority: Optional[int] = None,
        parallelism: Optional[int] = None,
        disableDecimal: Optional[bool] = None,
    ):
        if args:
            return self._call_with_table_schema(
                script,
                *args,
                clearMemory=clearMemory,
                table_to_list=pickleTableToList,
                priority=priority,
                parallelism=parallelism,
                disable_decimal=disableDecimal,
            )
        else:
            return self._exec_with_table_schema(
                script,
                *args,
                clear_memory=clearMemory,
                table_to_list=pickleTableToList,
                priority=priority,
                parallelism=parallelism,
                disable_decimal=disableDecimal,
            )

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
        return self.session_id

    def enableStreaming(self, port: int = 0, threadCount: int = 1) -> None:
        """Enable streaming.

        Args:
            port : the subscription port for incoming data. The server will connect to the port automatically.
            threadCount : the number of threads. Defaults to 1.

        Note:
            If the server you're connecting to supports transferring subscribed data using the connection initiated by the subsriber,
            do not specify this port. Otherwise, the port must be specified.

        """
        if hasattr(self, 'client') and self.client is not None:
            raise RuntimeError("streaming is already enabled")
        if threadCount <= 1:
            self.client = ThreadedClient(
                config=ThreadedStreamingClientConfig(port=port),
            )
        else:
            self.client = ThreadPooledClient(
                config=ThreadPooledStreamingClientConfig(port=port, thread_count=threadCount),
            )

    def _check_streaming_enabled(self):
        if not hasattr(self, 'client'):
            raise RuntimeError("streaming is not enabled.")

    def subscribe(
        self, host: str, port: int, handler: Callable, tableName: str, actionName: str = None,
        offset: int = -1, resub: bool = False, filter=None,
        msgAsTable: bool = False, batchSize: int = 0, throttle: float = 1.0,
        userName: str = "", password: str = "", streamDeserializer: Optional["StreamDeserializer"] = None,
        backupSites: List[str] = None, resubscribeInterval: int = 100, subOnce: bool = False,
        *, resubTimeout: Optional[int] = None,
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
            userName : username. Defaults to "", indicating no login.
            password : password. Defaults to "", indicating no login.
            streamDeserializer : a deserializer of heterogeneous stream table. Defaults to None.
            backupSites : a list of strings indicating the host:port for each backup node, e.g. ["192.168.0.1:8848", "192.168.0.2:8849"]. If specified, the failover mechanism is automatically activated. Defaults to None.
            resubscribeInterval : a non-negative integer indicating the wait time (in milliseconds) between resubscription attempts when a disconnection is detected. Defaults to 100.
            subOnce : a boolean, indicating whether to attempt reconnecting to disconnected nodes after each node switch occurs. Defaults to False.

        Note:
            `resubTimeout` has been renamed to `resubscribeInterval`. Please update your code to use `resubscribeInterval` instead.
        """
        if resubTimeout is not None:
            raise ValueError("Please use resubscibeInterval instead of resubTimeout.")
        self._check_streaming_enabled()
        if actionName is None:
            actionName = DEFAULT_ACTION_NAME
        self.client.subscribe(config={
            'host': host,
            'port': port,
            'handler': handler,
            'table_name': tableName,
            'action_name': actionName,
            'offset': offset,
            'resub': resub,
            'filter': filter,
            'msg_as_table': msgAsTable,
            'batch_size': batchSize,
            'throttle': throttle,
            'userid': userName,
            'password': password,
            'stream_deserializer': streamDeserializer,
            'backup_sites': backupSites,
            'resubscribe_interval': resubscribeInterval,
            'sub_once': subOnce,
        })

    def unsubscribe(self, host: str, port: int, tableName: str, actionName: str = None) -> None:
        """Unsubscribe.

        Args:
            host : server address. It can be IP address, domain, or LAN hostname, etc.
            port : port name.
            tableName : name of the published table.
            actionName : name of the subscription task. Defaults to None.
        """
        self._check_streaming_enabled()
        self.client.unsubscribe(host, port, tableName, actionName)

    def getSubscriptionTopics(self) -> List[str]:
        """Get all subscription topics.

        Returns:
            a list of all subscription topics in the format of "host/port/tableName/actionName".
        """
        self._check_streaming_enabled()
        return self.client.topic_strs

    def getInitScript(self) -> str:
        """Get the init script of the Session.

        Returns:
            string of the init script.
        """
        return self.startup

    def setInitScript(self, script: str) -> None:
        """Set up init script of the Session.

        Args:
            string of the init script.
        """
        self.startup = script

    def saveTable(self, tbl: Table, dbPath: str) -> bool:
        """Save the table.

        Args:
            tbl : DolphinDB Table object of the in-memory table to be saved.
            dbPath : DolphinDB database path.

        Returns:
            True.
        """
        tblName = tbl.tableName()
        dbName = _generate_dbname()
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

        elif isinstance(partitions, np.ndarray) and (partition_type == np.ndarray or partition_type == list):
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
        enable_job_cancellation()

    @classmethod
    def setTimeout(cls, timeout: float) -> None:
        """Set the TCP timeout.

        Args:
            timeout : corresponds to the TCP_USER_TIMEOUT option. Specify the value in units of seconds.
        """
        tcp.set_timeout(timeout)

    def hashBucket(self, obj, nBucket: int):
        """Hash map, which maps DolphinDB objects into hash buckets of size nBucket.

        Args:
            obj : the DolphinDB object to be hashed.
            nBucket : hash bucket size.

        Returns:
            hash of the DolphinDB object.
        """
        return hash_bucket(obj, nBucket)

    def convertDatetime64(self, datetime64List):
        return convert_datetime64(datetime64List)

    def convertDatabase(self, databaseList):
        return convert_database(databaseList)
