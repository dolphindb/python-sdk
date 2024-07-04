from typing import Dict, List, Union

from dolphindb.table import Table
from dolphindb.utils import _generate_tablename
from dolphindb.utils import _convertToString, _convertToStringOrStringVector
from dolphindb.utils import _convertToDict, _convertToBool, _convertToConstant


class Database(object):
    """DolphinDB database objects.

    Args:
        dbName : database name. Defaults to None.
        s : connected Session object. Defaults to None.
    """

    def __init__(self, dbName: str, s):
        """Constructor of Database."""
        self.__dbName = dbName
        self.__session = s

    def _getDbName(self):
        return self.__dbName

    def createTable(
        self, table: Table, tableName: str,
        sortColumns: Union[str, List[str]] = None,
        *,
        compressMethods: Dict[str, str] = {},
        primaryKey: Union[str, List[str]] = None,
        keepDuplicates: str = None,
        softDelete: bool = None,
        indexes: Dict[str, Union[str, List[str]]] = None
    ) -> Table:
        """Create an empty dimension table based on the given schema.

        Args:
            table : a table. DolphinDB server will create an empty dimension table based on the schema of the passed table.
            tableName : name of the dimension table.
            sortColumns : str or list of str, indicating the column(s) used to sort the ingested data within each partition. Defaults to None.

        Kwargs:
            compressMethods : a list of the compression methods used for each column. If unspecified, the columns are compressed by LZ4 in default. Defaults to {}.
            primaryKey : str or list of str, specifying the primary key column(s) of the table. If provided, data with the same primary key are logically overwritten. Defaults to None.
            keepDuplicates : how to deal with records with duplicate sortColumns values.
                                  It can have the following values:
                                  | Value   | Meanings                   |
                                  | :----   | :----                      |
                                  | "ALL"   | keep all records           |
                                  | "LAST"  | only keep the last record  |
                                  | "FIRST" | only keep the first record |
            softDelete : bool, used to enable or disable the soft delete feature. Defaults to None, which disables soft delete.
            indexes : dict, indicating indexes for columns in a table. The dict keys are str, dict values are str or list of str. Defaluts to None.

        Returns:
            DolphinDB Table object.
        """
        if not isinstance(table, Table):
            raise RuntimeError("Only DolphinDB Table object is accepted")

        tHandle = table._getTableName()
        ctHandle = _generate_tablename()
        runstr = ctHandle + "=" + self.__dbName + ".createTable(" + tHandle + "," + "`" + tableName

        if sortColumns is not None:
            if isinstance(sortColumns, str):
                runstr += ",sortColumns=`"+sortColumns
            elif isinstance(sortColumns, list):
                runstr += ",sortColumns="
                for key in sortColumns:
                    runstr += "`"+key

        if len(compressMethods) > 0:
            runstr += ",compressMethods={"
            for key, value in compressMethods.items():
                runstr += key+":'"+value+"',"
            runstr = runstr[:-1]+"}"

        errMsg = 'The "primaryKey" parameter must be a str or a list of str.'
        if primaryKey is not None:
            runstr += ",primaryKey=" + _convertToStringOrStringVector(primaryKey, errMsg)

        errMsg = 'The "keepDuplicates" parameter must be a str.'
        if keepDuplicates is not None:
            runstr += ",keepDuplicates=" + _convertToConstant(keepDuplicates)

        errMsg = 'The "softDelete" parameter must be a bool.'
        if softDelete is not None:
            runstr += ",softDelete=" + _convertToBool(softDelete, errMsg)

        errMsg = 'The "indexes" parameter must be a dict with str as keys, and str or list of str as values'
        if indexes is not None:
            runstr += ",indexes=" + _convertToDict(indexes, _convertToString, _convertToStringOrStringVector, errMsg)

        runstr += ");"

        self.__session.run(runstr)
        return self.__session.loadTable(ctHandle)

    def createPartitionedTable(
            self, table: Table, tableName: str, partitionColumns: Union[str, List[str]],
            compressMethods: Dict[str, str] = {}, sortColumns: Union[str, List[str]] = None,
            keepDuplicates: str = None, sortKeyMappingFunction: Union[str, List[str]] = None,
            *,
            primaryKey: Union[str, List[str]] = None,
            softDelete: bool = None,
            indexes: Dict[str, Union[str, List[str]]] = None
            ) -> Table:
        """Create an empty partitioned table based on the given schema.

        Args:
            table : a table. DolphinDB server will create an empty partitioned table based on the schema of the passed table.
            tableName : name of the partitioned table.
            partitionColumns : str or list of str indicating the partitioning column(s). For a compo domain database, it is a list of str; for non-sequential domain, this parameter is required.
            compressMethods : a list of the compression methods used for each column. If unspecified, the columns are compressed by LZ4 in default. Defaults to {}.
            sortColumns : str or list of str, indicating the column(s) used to sort the ingested data within each partition. Defaults to None.
            keepDuplicates : how to deal with records with duplicate sortColumns values.
                                  It can have the following values:
                                  | Value   | Meanings                   |
                                  | :----   | :----                      |
                                  | "ALL"   | keep all records           |
                                  | "LAST"  | only keep the last record  |
                                  | "FIRST" | only keep the first record |
            sortKeyMappingFunction : a vector of functions, which is of the same length as sortKey.
                                     The specified mapping functions are applied to each sort columns (except the temporal column) so as to reduce the dimensionality of sort keys.

        Kwargs:
            primaryKey : str or list of str, specifying the primary key column(s) of the table. If provided, data with the same primary key are logically overwritten. Defaults to None.
            softDelete : bool, used to enable or disable the soft delete feature. Defaults to None, which disables soft delete.
            indexes : dict, indicating indexes for columns in a table. The dict keys are str, dict values are str or list of str. Defaluts to None.

        Returns:
            DolphinDB Table object.
        """
        if not isinstance(table, Table):
            raise RuntimeError("Only DolphinDB Table object is accepted")

        tHandle = table._getTableName()
        cptHandle = _generate_tablename()

        partitionColumns_str = ''
        if isinstance(partitionColumns, str):
            partitionColumns_str = "`" + partitionColumns
        elif isinstance(partitionColumns, list):
            for col in partitionColumns:
                partitionColumns_str += '`'
                partitionColumns_str += col
        else:
            raise RuntimeError("Only String or List of String is accepted for partitionColumns")

        runstr = cptHandle + "=" + self.__dbName + ".createPartitionedTable(" + tHandle + "," + "`" + tableName + "," + partitionColumns_str

        if len(compressMethods) > 0:
            runstr += ",compressMethods={"
            for key, value in compressMethods.items():
                runstr += key+":'"+value+"',"
            runstr = runstr[:-1]+"}"
        if sortColumns is not None:
            if isinstance(sortColumns, str):
                runstr += ",sortColumns=`"+sortColumns
            elif isinstance(sortColumns, list):
                runstr += ",sortColumns="
                for key in sortColumns:
                    runstr += "`"+key

        if keepDuplicates is not None:
            if isinstance(keepDuplicates, str):
                runstr += ",keepDuplicates="+keepDuplicates
            else:
                raise RuntimeError("Only str is accepted for keepDuplicates")

        if sortKeyMappingFunction is not None:
            if isinstance(sortKeyMappingFunction, str):
                runstr += ",sortKeyMappingFunction=["+sortKeyMappingFunction+"]"
            elif isinstance(sortKeyMappingFunction, list):
                runstr += ",sortKeyMappingFunction=["
                for i in range(len(sortKeyMappingFunction)):
                    if i != 0:
                        runstr += ","
                    if isinstance(sortKeyMappingFunction[i], str):
                        runstr += sortKeyMappingFunction[i]
                    else:
                        raise RuntimeError('The "sortKeyMappingFunction" parameter must be a string or a list of strings.')
                runstr += "]"
            else:
                raise RuntimeError('The "sortKeyMappingFunction" parameter must be a string or a list of strings.')

        errMsg = 'The "primaryKey" parameter must be a str or a list of str.'
        if primaryKey is not None:
            runstr += ",primaryKey=" + _convertToStringOrStringVector(primaryKey, errMsg)

        errMsg = 'The "softDelete" parameter must be a bool.'
        if softDelete is not None:
            runstr += ",softDelete=" + _convertToBool(softDelete, errMsg)

        errMsg = 'The "indexes" parameter must be a dict with str as keys, and str or list of str as values'
        if indexes is not None:
            runstr += ",indexes=" + _convertToDict(indexes, _convertToString, _convertToStringOrStringVector, errMsg)

        runstr += ");"
        self.__session.run(runstr)
        return self.__session.loadTable(cptHandle)
