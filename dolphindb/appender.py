from pandas import DataFrame

from ._core import DolphinDBRuntime
from ._hints import List, Literal
from .connection_pool import DBConnectionPool
from .connection import DBConnection
from .utils import params_deprecated

ddbcpp = DolphinDBRuntime()._ddbcpp


class AutoFitTableAppender:
    """Class of table appender.

    As the only temporal data type in Python pandas is datetime64, all temporal columns of a DataFrame are converted into nanotimestamp type after uploaded to DolphinDB.
    Each time we use tableInsert or insert into to append a DataFrame with a temporal column to an in-memory table or DFS table, we need to conduct a data type conversion for the time column.
    For automatic data type conversion, Python API offers AutoFitTableAppender object.

    Args:
        db_path : the path of a DFS database. Leave it unspecified for in-memory tables. Defaults to None.
        table_name : table name. Defaults to None.
        conn : a DBConnection connected to DolphinDB server. Defaults to None.
        action : the action when appending. Now only supports "fitColumnType", indicating to convert the data type of temporal column. Defaults to "fitColumnType".
    """

    @params_deprecated(
        params={
            'dbPath': "db_path",
            'tableName': "table_name",
            'ddbSession': "conn",
        }
    )
    def __init__(
        self,
        db_path: str = None,
        table_name: str = None,
        conn: DBConnection = None,
        action: Literal["fitColumnType"] = "fitColumnType",
        *,
        dbPath: str = None,
        tableName: str = None,
        ddbSession: DBConnection = None,
    ):
        """Constructor of AutoFitTableAppender"""
        if dbPath is not None:
            db_path = dbPath
        if tableName is not None:
            table_name = tableName
        if ddbSession is not None:
            conn = ddbSession

        if db_path is None:
            db_path = ""
        if table_name is None:
            table_name = ""
        if not isinstance(conn, DBConnection):
            raise ValueError("conn must be a DBConnection.")
        if action == "fitColumnType":
            self.tableappender = ddbcpp.autoFitTableAppender(
                db_path, table_name, conn.cpp
            )
            self.sess = conn
        else:
            raise ValueError(f"Unsupported action type: {action}; only 'fitColumnType' is supported.")

    def __del__(self):
        self.tableappender = None

    def append(self, table: DataFrame) -> int:
        """Append data.

        Args:
            table : data to be written.

        Returns:
            number of rows written.
        """
        if self.sess.is_closed:
            raise RuntimeError("DBConnection has been closed.")
        return self.tableappender.append(table)


TableAppender = AutoFitTableAppender


class AutoFitTableUpserter(object):
    """Class of table upserter.

    Args:
        db_path : the path of a DFS database. Leave it unspecified for in-memory tables. Defaults to None.
        table_name : table name. Defaults to None.
        conn : a Session connected to DolphinDB server. Defaults to None.
        ignore_null : if set to true, for the NULL values in the new data, the correponding elements in the table are not updated. Defaults to False.
        key_cols : key column names. For a DFS table, the columns specified by this parameter are considrd as key columns. Defaults to [].
        sort_cols : sort column names. All data in the updated partition will be sorted according to the specified column. Defaults to [].
    """

    @params_deprecated(
        params={
            'dbPath': "db_path",
            'tableName': "table_name",
            'ddbSession': "conn",
            'ignoreNull': "ignore_null",
            'keyColNames': "key_cols",
            'sortColumns': "sort_cols",
        }
    )
    def __init__(
        self,
        db_path: str = None,
        table_name: str = None,
        conn: DBConnection = None,
        ignore_null: bool = False,
        key_cols: List[str] = None,
        sort_cols: List[str] = None,
        *,
        dbPath: str = None,
        tableName: str = None,
        ddbSession: DBConnection = None,
        ignoreNull: bool = None,
        keyColNames: List[str] = None,
        sortColumns: List[str] = None,
    ):
        """Constructor of tableUpsert."""
        if dbPath is not None:
            db_path = dbPath
        if tableName is not None:
            table_name = tableName
        if ddbSession is not None:
            conn = ddbSession
        if ignoreNull is not None:
            ignore_null = ignoreNull
        if keyColNames is not None:
            key_cols = keyColNames
        if sortColumns is not None:
            sort_cols = sortColumns

        if db_path is None:
            db_path = ""
        if table_name is None:
            table_name = ""
        if not isinstance(conn, DBConnection):
            raise Exception("conn must be a dolphindb DBConnection!")
        if key_cols is None:
            key_cols = []
        if sort_cols is None:
            sort_cols = []
        self.tableupserter = ddbcpp.autoFitTableUpsert(
            db_path,
            table_name,
            conn.cpp,
            ignore_null,
            key_cols,
            sort_cols,
        )
        self.sess = conn

    def __del__(self):
        self.tableupserter = None

    def upsert(self, table: DataFrame):
        """upsert data.

        Args:
            table : data to be written.
        """
        if self.sess.is_closed:
            raise RuntimeError("DBConnection has been closed.")
        return self.tableupserter.upsert(table)


TableUpserter = AutoFitTableUpserter


class PartitionedTableAppender(object):
    """Class for writes to DolphinDB DFS tables.

    Args:
        db_path : DFS database path. Defaults to None.
        table_name : name of a DFS table. Defaults to None.
        partition_col : partitioning column. Defaults to None.
        pool : connection pool. Defaults to None.

    Note:
        DolphinDB does not allow multiple writers to write data to the same partition at the same time, so when the client writes data in parallel with multiple threads, please ensure that each thread writes to a different partition.
        The python API provides PartitionedTableAppender, an easy way to automatically split data writes by partition.
    """

    @params_deprecated(
        params={
            'dbPath': "db_path",
            'tableName': "table_name",
            'partitionColName': "partition_col",
            'dbConnectionPool': "pool",
        }
    )
    def __init__(
        self,
        db_path: str = None,
        table_name: str = None,
        partition_col: str = None,
        pool: DBConnectionPool = None,
        *,
        dbPath: str = None,
        tableName: str = None,
        partitionColName: str = None,
        dbConnectionPool: DBConnectionPool = None,
    ):
        """Constructor of PartitionedTableAppender."""
        if dbPath is not None:
            db_path = dbPath
        if tableName is not None:
            table_name = tableName
        if partitionColName is not None:
            partition_col = partitionColName
        if dbConnectionPool is not None:
            pool = dbConnectionPool

        if db_path is None:
            db_path = ""
        if table_name is None:
            table_name = ""
        if partition_col is None:
            partition_col = ""
        if not isinstance(pool, DBConnectionPool):
            raise Exception("pool must be a dolphindb DBConnectionPool!")
        self.appender = ddbcpp.partitionedTableAppender(
            db_path, table_name, partition_col, pool.pool
        )
        self.pool = pool

    def __del__(self):
        self.pool = None

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
