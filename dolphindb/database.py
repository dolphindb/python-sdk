from typing import Dict, List, Union

from dolphindb.table import Table
from dolphindb.utils import _generate_tablename


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
        sortColumns: Union[str, List[str]] = None
    ) -> Table:
        """Create an empty dimension table based on the given schema.

        Args:
            table : a table. DolphinDB server will create an empty dimension table based on the schema of the passed table.
            tableName : name of the dimension table.
            sortColumns : string or a list of strings, indicating the sort column of a table. If specified, data with the same key are stored together in a partition in order. Defaults to None.

        Returns:
            DolphinDB Table object.
        """
        if not isinstance(table, Table):
            raise RuntimeError("Only DolphinDB Table object is accepted")

        tHandle = table._getTableName()
        ctHandle = _generate_tablename()
        runstr = ctHandle + "=" + self.__dbName + ".createTable(" + tHandle + "," + "`" + tableName

        if sortColumns is not None:
            if type(sortColumns) == str:
                runstr += ",sortColumns=`"+sortColumns
            elif type(sortColumns) == list:
                runstr += ",sortColumns="
                for key in sortColumns:
                    runstr += "`"+key

        runstr += ");"

        self.__session.run(runstr)
        return self.__session.loadTable(ctHandle)

    def createPartitionedTable(
            self, table: Table, tableName: str, partitionColumns: Union[str, List[str]],
            compressMethods: Dict[str, str] = {}, sortColumns: Union[str, List[str]] = None,
            keepDuplicates: str = None, sortKeyMappingFunction: Union[str, List[str]] = None
            ) -> Table:
        """Create an empty dimension table based on the given schema.

        Args:
            table : a table. DolphinDB server will create an empty dimension table based on the schema of the passed table.
            tableName : name of the dimension table.
            partitionColumns : string or list of strings indicating the partition columns. For a compo domain database, it is a string; for non-sequential domain, this parameter is required.
            compressMethods : a list of the compression methods used for each column. If unspecified, the columns are not compressed. Defaults to {}.
            sortColumns : string or a list of strings, indicating the sort column of a table. If specified, data with the same key are stored together in a partition in order. Defaults to None.
            keepDuplicates : how to deal with records with duplicate sortColumns values. 
                                  It can have the following values:
                                  | Value   | Meanings                   |
                                  | :----   | :----                      |
                                  | "ALL"   | keep all records           |
                                  | "LAST"  | only keep the last record  |
                                  | "FIRST" | only keep the first record |
            sortKeyMappingFunction : a vector of functions, which is of the same length as sortKey. 
                                     The specified mapping functions are applied to each sort columns (except the temporal column) so as to reduce the dimensionality of sort keys.

        Returns:
            DolphinDB Table object.
        """
        if not isinstance(table, Table):
            raise RuntimeError("Only DolphinDB Table object is accepted")

        tHandle = table._getTableName()
        cptHandle = _generate_tablename()

        partitionColumns_str = ''
        if type(partitionColumns) == str:
            partitionColumns_str = "`" + partitionColumns
        elif type(partitionColumns) == list:
            for col in partitionColumns:
                partitionColumns_str += '`'
                partitionColumns_str += col
        else:
            raise RuntimeError("Only String or List of String is accepted for partitionColumns")

        runstr = cptHandle + "=" + self.__dbName + ".createPartitionedTable(" + tHandle + "," + "`" + tableName + ","  + partitionColumns_str

        if len(compressMethods) > 0:
            runstr += ",compressMethods={"
            for key, value in compressMethods.items():
                runstr += key+":'"+value+"',"
            runstr = runstr[:-1]+"}"
        if sortColumns is not None:
            if type(sortColumns) == str:
                runstr += ",sortColumns=`"+sortColumns
            elif type(sortColumns) == list:
                runstr += ",sortColumns="
                for key in sortColumns:
                    runstr += "`"+key

        if keepDuplicates is not None:
            if isinstance(keepDuplicates, str):
                runstr += ",keepDuplicates="+keepDuplicates
            else:
                raise RuntimeError("Only str is accepted for keepDuplicates")

        if sortKeyMappingFunction is not None:
            if type(sortKeyMappingFunction) == str:
                runstr += ",sortKeyMappingFunction=["+sortKeyMappingFunction+"]"
            elif type(sortKeyMappingFunction) == list:
                runstr += ",sortKeyMappingFunction=["
                for i in range(len(sortKeyMappingFunction)):
                    if i != 0:
                        runstr += ","
                    if type(sortKeyMappingFunction[i]) == str:
                        runstr += sortKeyMappingFunction[i]
                    else:
                        raise RuntimeError('The "sortKeyMappingFunction" parameter must be a string or a list of strings.')
                runstr += "]"
            else:
                raise RuntimeError('The "sortKeyMappingFunction" parameter must be a string or a list of strings.')
        runstr += ");"
        self.__session.run(runstr)
        return self.__session.loadTable(cptHandle)
