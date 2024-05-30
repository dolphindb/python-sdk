import copy
import inspect
import re
import threading
from typing import Any, List, Optional, Tuple, Type, Union

import numpy as np
from dolphindb.settings import get_verbose
from dolphindb.utils import _generate_tablename, _getFuncName
from dolphindb.vector import FilterCond, Vector
from pandas import DataFrame


class Counter(object):
    """Reference counter for internal use when caching table."""
    def __init__(self):
        """Constructor of Counter."""
        self.__lock = threading.Lock()
        self.__value = 1

    def inc(self) -> int:
        """Increment the reference count.

        Returns:
            current reference count.
        """
        with self.__lock:
            self.__value += 1
            return self.__value

    def dec(self) -> int:
        """Decrease the reference count.

        Returns:
            current reference count.
        """
        with self.__lock:
            self.__value -= 1
            return self.__value

    def val(self) -> int:
        """Get the current reference count.

        Returns:
            current reference count.
        """
        with self.__lock:
            return self.__value


class Table(object):
    """DolphinDB Table object.

    Args:
        dbPath : database name. Defaults to None.
        data : data of the table. Defaults to None.
        tableAliasName : table alias. Defaults to None.
        partitions : the partitions to be loaded into memory. Defaults to None, which means to load all partitions.
        inMem : (deprecated) whether to load the table into memory. True: to load the table into memory. Defaults to False.
        schemaInited : whether the table structure has been initialized. True: the table structure has been initialized. Defaults to False.
        s : the connected Session object. Defaults to None.
        needGC : whether to enable garbage collection. True: enable garbage collection. Defaults to True.
        isMaterialized : whether table is materialized True: table has been materialized. Defaults to False.
    """
    def __init__(
        self,
        dbPath: str = None,
        data=None,
        tableAliasName: str = None,
        partitions: Optional[List[str]] = None,
        inMem: bool = False,
        schemaInited: bool = False,
        s=None,
        needGC: bool = True,
        isMaterialized: bool = False
    ):
        """Constructor of Table."""
        if partitions is None:
            partitions = []
        self.__having = None
        self.__top = None
        self.__exec = False
        self.__limit = None
        self.__leftTable = None
        self.__rightTable = None
        self.__merge_for_update = False
        self.__objAddr = None
        self.__columns = None
        self.isMaterialized = isMaterialized
        if tableAliasName is not None:
            self.isMaterialized = True
        if s is None:
            raise RuntimeError("Session must be provided")
        self.__tableName = _generate_tablename() if not isinstance(data, str) else data
        self.__session = s  # type : Session
        self.__schemaInited = schemaInited
        self.__need_gc = needGC
        if self.__need_gc:
            self.__ref = Counter()
        if not isinstance(partitions, list):
            raise RuntimeError(
                'Column names must be passed in as a list')
        if isinstance(data, dict) or isinstance(data, DataFrame):
            df = data if isinstance(data, DataFrame) else DataFrame(data)
            # if  not self.__tableName.startswith("TMP_TBL_"):
            #    self._setTableName(_generate_tablename())
            if tableAliasName is None:
                self._setTableName(_generate_tablename())
            else:
                self._setTableName(tableAliasName)
            self.__objAddr = self.__session.upload({self.__tableName: df})
            self.vecs = {}

            for colName in df.keys():
                self.vecs[colName] = Vector(
                    name=colName, tableName=self.__tableName, s=self.__session
                )
            self._setSelect(list(self.vecs.keys()))
        elif isinstance(data, str):
            if dbPath:
                if tableAliasName:
                    self.__tableName = tableAliasName
                else:
                    self.__tableName = _generate_tablename(data)
                runstr = '{tableName} = loadTable("{dbPath}", "{data}", {partitions}, {inMem})'
                fmtDict = dict()
                fmtDict['tableName'] = self.__tableName
                fmtDict['dbPath'] = dbPath
                fmtDict['data'] = data
                if len(partitions) and type(partitions[0]) is not str:
                    fmtDict['partitions'] = ('[' + ','.join(str(x) for x in partitions) + ']') if len(partitions) else ""
                else:
                    fmtDict['partitions'] = ('["' + '","'.join(partitions) + '"]') if len(partitions) else ""
                fmtDict['inMem'] = str(inMem).lower()
                runstr = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
                self.__session.run(runstr)
                # runstr = '%s = select * from %s' %(self.__tableName, self.__tableName)
                # self.__session.run(runstr)
            else:
                pass
        elif "orca.core.frame.DataFrame" in str(data.__class__):
            df = s.run(data._var_name)
            self.__init__(data=df, s=self.__session)
        else:
            raise RuntimeError("data must be a remote dolphindb table name or dict or DataFrame")
        self._init_schema()

    def __deepcopy__(self, memodict=None):
        if memodict is None:
            memodict = {}
        newTable = Table(data=self.__tableName, schemaInited=True, s=self.__session, needGC=self.__need_gc)
        newTable._setExec(self.isExec)
        newTable.isMaterialized = self.isMaterialized
        try:
            newTable.vecs = copy.deepcopy(self.vecs, memodict)
        except AttributeError:
            pass

        try:
            newTable.__schemaInited = copy.deepcopy(self.__schemaInited, memodict)
        except AttributeError:
            pass

        try:
            newTable.__select = copy.deepcopy(self.__select, memodict)
        except AttributeError:
            pass

        try:
            newTable.__where = copy.deepcopy(self.__where, memodict)
        except AttributeError:
            pass

        try:
            newTable.__groupby = copy.deepcopy(self.__groupby, memodict)
        except AttributeError:
            pass

        try:
            newTable.__contextby = copy.deepcopy(self.__contextby, memodict)
        except AttributeError:
            pass

        try:
            newTable.__having = copy.deepcopy(self.__having, memodict)
        except AttributeError:
            pass

        try:
            newTable.__sort = copy.deepcopy(self.__sort, memodict)
        except AttributeError:
            pass

        try:
            newTable.__top = copy.deepcopy(self.__top, memodict)
        except AttributeError:
            pass

        try:
            newTable.__limit = copy.deepcopy(self.__limit, memodict)
        except AttributeError:
            pass

        try:
            newTable.__csort = copy.deepcopy(self.__csort, memodict)
        except AttributeError:
            pass

        try:
            if self.__need_gc:
                newTable.__ref = self.__ref
                self.__ref.inc()
        except AttributeError:
            pass

        return newTable

    def __copy__(self):
        newTable = Table(data=self.__tableName, schemaInited=True, s=self.__session, needGC=self.__need_gc)
        newTable._setExec(self.isExec)
        newTable.isMaterialized = self.isMaterialized
        try:
            newTable.vecs = copy.copy(self.vecs)
        except AttributeError:
            pass

        try:
            newTable.__schemaInited = copy.copy(self.__schemaInited)
        except AttributeError:
            pass

        try:
            newTable.__select = copy.copy(self.__select)
        except AttributeError:
            pass

        try:
            newTable.__where = copy.copy(self.__where)
        except AttributeError:
            pass

        try:
            newTable.__groupby = copy.copy(self.__groupby)
        except AttributeError:
            pass

        try:
            newTable.__contextby = copy.copy(self.__contextby)
        except AttributeError:
            pass

        try:
            newTable.__having = copy.copy(self.__having)
        except AttributeError:
            pass

        try:
            newTable.__sort = copy.copy(self.__sort)
        except AttributeError:
            pass

        try:
            newTable.__top = copy.copy(self.__top)
        except AttributeError:
            pass

        try:
            newTable.__limit = copy.copy(self.__limit)
        except AttributeError:
            pass

        try:
            newTable.__csort = copy.copy(self.__csort)
        except AttributeError:
            pass

        try:
            if self.__need_gc:
                newTable.__ref = self.__ref
                self.__ref.inc()
                # print("in copy ", self.__tableName, self.__ref.val())
        except AttributeError:
            pass

        return newTable

    def __del__(self):
        # print("try to del ", self.__tableName, self.__ref.val())
        if self.__need_gc:
            if self.__ref.dec() == 0:
                # print('do __del__', self.__tableName)
                try:
                    # this is not a real table name such as join table, no need to undef.
                    if '(' in self.__tableName or ')' in self.__tableName:
                        return
                    # if self.__objAddr is None or self.__objAddr < 0:
                    #     self.__session.run("undef('{}')".format(self.__tableName))
                    # else:
                    #     self.__session.run("undef('{}', VAR, {})".format(self.__tableName, self.__objAddr))
                    if not self.__session.isClosed():
                        if self.__tableName.find("TMP_TBL_") != -1:
                            # print("undef ", self.__tableName)
                            # tmp table, need to undef
                            if self.__objAddr is None or self.__objAddr < 0:
                                self.__session.run("undef('{}')".format(self.__tableName))
                            else:
                                self.__session.run("undef('{}', VAR)".format(self.__tableName))
                except Exception as e:
                    if get_verbose():
                        print("undef table '{}' got an exception: ".format(self.__tableName))
                        print(e)
            # else:
            #     print('__del__', self.__ref.val())

    def _setSelect(self, cols):
        self.__schemaInited = True
        if isinstance(cols, tuple):
            cols = list(cols)
        if isinstance(cols, list) is False:
            cols = [cols]

        self.__select = cols

    def _init_schema(self):
        if self.__schemaInited is True:
            return
        self._init_columns()
        self.vecs = {}
        if self.__columns is not None:
            if isinstance(self.__columns, list):
                for colName in self.__columns:
                    self.vecs[colName] = Vector(name=colName, tableName=self.__tableName, s=self.__session)
                self._setSelect(self.__columns)
            else:
                self._setSelect(self.__columns)

    def __getattr__(self, item):
        vecs = object.__getattribute__(self, "vecs")
        if item not in vecs:
            return object.__getattribute__(self, item)
        else:
            return vecs[item]

    def __getitem__(self, colOrCond):
        if isinstance(colOrCond, FilterCond):
            return self.where(colOrCond)
        else:
            return self.select(colOrCond)

    def tableName(self) -> str:
        """Get table name.

        Returns:
            name of the table in DolphinDB.
        """
        return self.__tableName

    def _setTableName(self, tableName):
        self.__tableName = tableName

    def _getTableName(self):
        return self.__tableName

    def _setLeftTable(self, tableName):
        self.__leftTable = tableName

    def _setRightTable(self, tableName):
        self.__rightTable = tableName

    def getLeftTable(self) -> Type["Table"]:
        """Get the left table of the join.

        Returns:
            the left table of the join.
        """
        return self.__leftTable

    def getRightTable(self) -> Type["Table"]:
        """Get the right table of the join.

        Returns:
            the right table of the join.
        """
        return self.__rightTable

    def session(self):
        """Get the Session where the Table belongs to.

        Returns:
            a Session where this Table belongs to.
        """
        return self.__session

    def _addWhereCond(self, conds):
        try:
            _ = self.__where
        except AttributeError:
            self.__where = []

        if isinstance(conds, list) or isinstance(conds, tuple):
            self.__where.extend([str(x) for x in conds])
        else:
            self.__where.append(str(conds))

    def _setSort(self, bys, ascending=True):
        if isinstance(bys, list) or isinstance(bys, tuple):
            self.__sort = [str(x) for x in bys]
        else:
            self.__sort = [str(bys)]
        if isinstance(ascending, bool):
            if not ascending:
                self.__sort = [x + " desc" for x in self.__sort]
            return
        if (isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(self.__sort) != len(ascending)):
            raise ValueError(f"Length of ascending ({len(ascending)}) != length of by ({len(self.__sort)})")
        self.__sort = [by + " desc" if not asc else by for by, asc in zip(self.__sort, ascending)]

    def _setTop(self, num):
        self.__top = str(num)

    def _setCsort(self, bys: Union[str, Tuple[str, ...], List[str]], ascending: Union[bool, Tuple[bool, ...], List[bool]] = True):
        if isinstance(bys, list) or isinstance(bys, tuple):
            self.__csort = [str(x) for x in bys]
        else:
            self.__csort = [str(bys)]
        if isinstance(ascending, bool):
            if not ascending:
                self.__csort = [x + " desc" for x in self.__csort]
            return
        if (isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(self.__csort) != len(ascending)):
            raise ValueError(f"Length of ascending ({len(ascending)}) != length of by ({len(self.__csort)})")
        self.__csort = [by + " desc" if not asc else by for by, asc in zip(self.__csort, ascending)]

    def _setLimit(self, num):
        if isinstance(num, list) or isinstance(num, tuple):
            self.__limit = [str(x) for x in num]
        else:
            self.__limit = [str(num)]

    def _setWhere(self, where):
        self.__where = where

    def select(self, cols: Union[str, Tuple[str, ...], List[str]]) -> Type["Table"]:
        """Extract the specified columns and return them in a table.

        Args:
            cols : specify the columns to be extracted from table. Can be a string, a tuple of strings or a list of strings.

        Returns:
            a new Table object with the specified columns.
        """
        selectTable = copy.copy(self)
        # print("ref of newTable: ", selectTable.__ref.val(), " self.ref: ", self.__ref.val())
        selectTable._setSelect(cols)
        if not self.isMaterialized:
            selectTable.__tableName = f"({self.showSQL()})"
        selectTable.isMaterialized = False
        return selectTable

    def exec(self, cols: Union[str, Tuple[str, ...], List[str]]) -> Type["Table"]:
        """Generate a Table object containing the specified columns. The execution result is a scalar or vector.

        Args:
            cols : specify the columns. Can be a string, a tuple of strings or a list of strings.

        Returns:
            a new Table object of the specified columns.
        """
        selectTable = copy.copy(self)
        selectTable._setSelect(cols)
        selectTable._setExec(True)
        if not self.isMaterialized:
            selectTable.__tableName = f"({self.showSQL()})"
        selectTable.isMaterialized = False
        return selectTable

    def where(self, conds) -> Type["Table"]:
        """Add query conditions.

        Args:
            conds : query conditions.

        Returns:
            a new Table object with query conditions
        """
        # print("where", self.__tableName, __name__, conds)
        whereTable = copy.copy(self)
        whereTable._addWhereCond(conds)
        return whereTable

    def _setGroupby(self, groupby):
        try:
            _ = self.__contextby
            raise RuntimeError('multiple context/group-by are not allowed ')
        except AttributeError:
            self.__groupby = groupby

    def _setContextby(self, contextby):
        try:
            _ = self.__groupby
            raise RuntimeError('multiple context/group-by are not allowed ')
        except AttributeError:
            self.__contextby = contextby

    def _setHaving(self, having):
        self.__having = having

    def groupby(self, cols: Union[str, Tuple[str, ...], List[str]]) -> Type["TableGroupby"]:
        """Apply group by on Table with specified columns.

        Args:
            cols : name of the grouping columns.

        Returns:
            a new TableGroupby object after adding the groupby.
        """
        groupbyTable = copy.copy(self)
        groupby = TableGroupby(groupbyTable, cols)
        groupbyTable._setGroupby(groupby)
        return groupby

    def sort(self, bys: Union[str, Tuple[str, ...], List[str]], ascending=True) -> Type["Table"]:
        """Specify parameter for sorting.

        Args:
            bys : the column(s) to sort on.
            ascending : the sorting order. True: ascending order. Defaults to True.

        Returns:
            a new Table object after adding sort clause.
        """
        sortTable = copy.copy(self)
        sortTable._setSort(bys, ascending)
        return sortTable

    def top(self, num: int) -> Type["Table"]:
        """Retrieve the top n records from table.

        Args:
            num : the number of records to return. Must be a positive number.

        Returns:
            a new Table object with the top clause.
        """
        topTable = copy.copy(self)
        topTable._setTop(num)
        return topTable

    def csort(self, bys: Union[str, Tuple[str, ...], List[str]], ascending: Union[bool, Tuple[bool, ...], List[bool]] = True) -> Type["Table"]:
        """Specify sorting column(s) for contextby.

        Args:
            bys : the column(s) to sort on.
            ascending : the sorting order. True: ascending order. Defaults to True.

        Returns:
            a new Table object after adding csort clause.
        """
        csortTable = copy.copy(self)
        csortTable._setCsort(bys, ascending)
        return csortTable

    def limit(self, num: Union[int, Tuple[int, ...], List[int]]) -> Type["Table"]:
        """Specify parameters for limit.

        Args:
            num : when used with the contextby clause, the limit clause can use a negative integer to select a limited number of records in the end of each group. 
                  In all other cases the limit clause can only use positive integers.
                  Can pass in [x, y] to select data from row x to row y.

        Returns:
            a new Table object after adding the limit clause
        """
        limitTable = copy.copy(self)
        limitTable._setLimit(num)
        return limitTable

    def execute(self, expr: Union[str, Tuple[str, ...], List[str]]) -> Any:
        """Execute the SQL query.

        Args:
            expr : columns to be executed. Can be a string, tuple, or list.

        Returns:
            SQL query result.
        """
        if expr:
            self._setSelect(expr)
        pattern = re.compile("select", re.IGNORECASE)
        query = pattern.sub('exec', self.showSQL())
        return self.__session.run(query)

    @property
    def rows(self) -> int:
        """Read-only property.

        Get the row count of the Table.

        Returns:
            number of rows in the Table.
        """
        # if 'update' in self.showSQL().lower() or 'insert' in  self.showSQL().lower():
        #     return self.__session.run('exec count(*) from %s'% self.__tableName)
        sql = self.showSQL()
        idx = sql.lower().index("from")
        sql_new = "select count(*) as ct " + sql[idx:]
        df = self.__session.run(sql_new)
        if df.shape[0] > 1:
            return df.shape[0]
        else:
            return df['ct'].iloc[0]

    def _init_columns(self) -> None:
        if not self.__columns:
            schema = self.__session.run("schema(%s)" % self.__tableName)  # type: dict
            columns = schema["colDefs"]["name"].tolist()
            self.__columns = columns

    @property
    def cols(self) -> int:
        """read-only property.

        Get the number of columns in the Table.

        Returns:
            the number of columns in the Table.
        """
        self._init_columns()
        return len(self.__columns)

    @property
    def colNames(self) -> str:
        """read-only property.

        Get all column names of the Table.

        Returns:
            all column names of the Table.
        """
        self._init_columns()
        return self.__columns

    @property
    def schema(self) -> DataFrame:
        """read-only property.

        Get summary information of the Table.

        Returns:
            a DataFrame about summary information of the Table.
        """
        schema = self.__session.run("schema(%s)" % self.__tableName)  # type: dict
        colDefs = schema.get('colDefs')  # type: DataFrame
        return colDefs

    @property
    def isMergeForUpdate(self) -> bool:
        """read-only property.

        Decide whether the Table is to be joined.

        Returns:
            True means Table is to be joined.
        """
        return self.__merge_for_update

    def setMergeForUpdate(self, toUpdate: bool):
        """Mark if a Table is to be joined.

        Args:
            toUpdate : True means to mark the table as to be joined.
        """
        self.__merge_for_update = toUpdate

    @property
    def isExec(self) -> bool:
        """read-only property.

        Whether the SQL statement has an exec clause.

        Returns:
            True means the SQL statement has added an exec clause.
        """
        return self.__exec

    def _setExec(self, isExec):
        self.__exec = isExec

    def pivotby(self, index: str, column: str, value=None, aggFunc=None) -> Type["TablePivotBy"]:
        """Add pivot by clause to rearrange a column (or multiple columns) of a table on two dimensions.

        Args:
            index : the result table has the same number of rows as unique values on this column.
            column : the result table has the same number of columns as unique values on this column.
            value : the parameters of the aggregate function. Defaults to None.
            aggFunc : the specified aggregate function. Defaults to lambda x: x.

        Returns:
            a TablePivotBy object.
        """
        pivotByTable = copy.copy(self)
        return TablePivotBy(pivotByTable, index, column, value, aggFunc)

    def merge(
            self, right, how: str = 'inner', on: Union[str, Tuple[str, ...], List[str]] = None,
            left_on: Union[str, Tuple[str, ...], List[str]] = None,
            right_on: Union[str, Tuple[str, ...], List[str]] = None,
            sort: bool = False, merge_for_update: bool = False
    ) -> Type["Table"]:
        """Join two table objects with ANSI SQL.

        Args:
            right : the right table from local or remote server.
            how : connection type. The options are {"left", "right", "outer", "inner"}. Defaults to 'inner'.
        | How   | Link                                                                 |
        | :---- | :------------------------------------------------------------------- |
        | left  | https://dolphindb.com/help/SQLStatements/TableJoiners/leftjoin.html  |
        | right | https://dolphindb.com/help/SQLStatements/TableJoiners/leftjoin.html  |
        | outer | https://dolphindb.com/help/SQLStatements/TableJoiners/fulljoin.html  |
        | ineer | https://dolphindb.com/help/SQLStatements/TableJoiners/equaljoin.html |   
            on : the columns to join on. If the column names are different in two tables, use the left_on and right_on parameters. Defaults to None.
            left_on : the column to join on from the left table. Defaults to None.
            right_on : the column to join on from the right table. Defaults to None.
            sort : True means to enable sorting. Defaults to False.
            merge_for_update : True means marking tables as to be joined. Defaults to False.

        Note:
            A partitioned table can only outer join a another partitioned table. An in-memory table can only outer join another in-memory table.

        Returns:
            the joined Table object.
        """
        howMap = {
            'inner': 'ej',
            'left': 'lj',
            'right': 'lj',
            'outer': 'fj',
            'left semi': 'lsj'
        }
        joinFunc = howMap[how]
        joinFuncPrefix = '' if sort is False or joinFunc == 'fj' else 's'

        if self.isMaterialized or merge_for_update:
            leftTableName = self.tableName()
        else:
            leftTableName = f"({self.showSQL()}) as {_generate_tablename('TMP_L')}"

        if right.isMaterialized:
            rightTableName = right.tableName() if isinstance(right, Table) else right
        else:
            rightTableName = f"({right.showSQL()}) as {_generate_tablename('TMP_R')}"

        if how == 'right':
            leftTableName, rightTableName = rightTableName, leftTableName
            left_on, right_on = right_on, left_on

        if on is not None and not isinstance(on, list) and not isinstance(on, tuple):
            on = [on]
        if left_on is not None and not isinstance(left_on, list) and not isinstance(left_on, tuple):
            left_on = [left_on]
        if right_on is not None and not isinstance(right_on, list) and not isinstance(right_on, tuple):
            right_on = [right_on]

        if on is not None:
            left_on, right_on = on, on
        elif left_on is None and right_on is None:
            raise Exception('at least one of {\'on\', \'left_on\', \'right_on\'} must be present')
        elif left_on is not None and right_on is not None and len(left_on) != len(right_on):
            raise Exception('\'left_on\' must have the same length as \'right_on\'')

        if left_on is None and right_on is not None:
            left_on = right_on
        if right_on is None and left_on is not None:
            right_on = left_on

        leftColumnNames = ''.join(['`' + x for x in left_on])
        rightColumnNames = ''.join(['`' + x for x in right_on])
        finalTableName = '%s(%s,%s,%s,%s)' % (
            joinFuncPrefix + joinFunc, leftTableName, rightTableName, leftColumnNames, rightColumnNames
        )
        # print(finalTableName)
        self._init_schema()
        right._init_schema()
        joinTable = copy.copy(self)
        # leftAliasPrefix = 'lhs_' if how != 'right' else 'rhs_'
        # rightAliasPrefix = 'rhs_' if how != 'right' else 'lhs_'
        # leftSelectCols = [leftTableName + '.' + col + ' as ' + leftTableName + "_" + col for col in self._getSelect()]
        # rightSelectCols = [rightTableName + '.' + col + ' as ' + rightTableName + "_" + col for col in right._getSelect()]
        # leftSelectCols = self._getSelect()
        # print(leftSelectCols)
        # rightSelectCols = [rightTableName + '.' + col + ' as ' + rightTableName + "_" + col for col in
        #                    right._getSelect() if col in self._getSelect()]
        # print(rightSelectCols)
        joinTable._setLeftTable(self.tableName())
        joinTable._setRightTable(right.tableName())
        joinTable._setTableName(finalTableName)
        # joinTable._setSelect(leftSelectCols + rightSelectCols)
        joinTable._setSelect('*')
        if merge_for_update:
            joinTable.setMergeForUpdate(True)
        joinTable.isMaterialized = False
        return joinTable

    def merge_asof(
        self, right, on: Union[str, Tuple[str, ...], List[str]] = None,
        left_on: Union[str, Tuple[str, ...], List[str]] = None,
        right_on: Union[str, Tuple[str, ...], List[str]] = None
    ) -> Type["Table"]:
        """Use asof join to join two Table objects.

        Refer to https://dolphindb.com/help/SQLStatements/TableJoiners/asofjoin.html.

        Args:
            right : The right table from local or remote server.
            on : the columns to join on. If the column names are different in two tables, use the left_on and right_on parameters. Defaults to None.
            left_on : the column to join on from the left table. Defaults to None.
            right_on : the column to join on from the right table. Defaults to None.

        Returns:
            the joined Table object.
        """
        if self.isMaterialized:
            leftTableName = self.tableName()
        else:
            leftTableName = f"({self.showSQL()}) as {_generate_tablename('TMP_L')}"

        if right.isMaterialized:
            rightTableName = right.tableName() if isinstance(right, Table) else right
        else:
            rightTableName = f"({right.showSQL()}) as {_generate_tablename('TMP_R')}"

        if on is not None and not isinstance(on, list) and not isinstance(on, tuple):
            on = [on]
        if left_on is not None and not isinstance(left_on, list) and not isinstance(left_on, tuple):
            left_on = [left_on]
        if right_on is not None and not isinstance(right_on, list) and not isinstance(right_on, tuple):
            right_on = [right_on]

        if on is not None:
            left_on, right_on = on, on
        elif left_on is None and right_on is None:
            raise Exception('at least one of {\'on\', \'left_on\', \'right_on\'} must be present')
        elif left_on is not None and right_on is not None and len(left_on) != len(right_on):
            raise Exception('\'left_on\' must have the same length as \'right_on\'')

        if left_on is None and right_on is not None:
            left_on = right_on
        if right_on is None and left_on is not None:
            right_on = left_on

        leftColumnNames = ''.join(['`' + x for x in left_on])
        rightColumnNames = ''.join(['`' + x for x in right_on])
        finalTableName = 'aj(%s,%s,%s,%s)' % (
            leftTableName, rightTableName, leftColumnNames, rightColumnNames
        )
        self._init_schema()
        right._init_schema()
        joinTable = copy.copy(self)
        # leftAliasPrefix = 'lhs_'
        # rightAliasPrefix = 'rhs_'
        # leftSelectCols = [leftTableName + '.' + col + ' as ' + leftTableName +"_" + col for col in self._getSelect()]
        # rightSelectCols = [rightTableName + '.' + col + ' as ' + rightTableName + "_" + col for col in right._getSelect()]
        # leftSelectCols = self._getSelect()
        # rightSelectCols = [rightTableName + '.' + col + ' as ' + rightTableName + "_" + col for col in
        #                    right._getSelect() if col in self._getSelect()]
        joinTable._setLeftTable(self.tableName())
        joinTable._setRightTable(right.tableName())
        joinTable._setTableName(finalTableName)
        joinTable._setSelect('*')
        # joinTable._setSelect(leftSelectCols + rightSelectCols)
        joinTable.isMaterialized = False
        return joinTable

    def merge_window(
        self, right, leftBound: int, rightBound: int,
        aggFunctions: Union[str, List[str]], on: Union[str, Tuple[str, ...], List[str]] = None,
        left_on: Union[str, Tuple[str, ...], List[str]] = None,
        right_on: Union[str, Tuple[str, ...], List[str]] = None,
        prevailing: bool = False
    ) -> Type["Table"]:
        """Use window join to join two Table objects.

        Args:
            right : the name of the right table from local or remote server.
            leftBound : left boundary (inclusive) of the window. Defaults to None.
            rightBound : right boundary (inclusive) of the window. Defaults to None.
            aggFunctions : aggregate function. Defaults to None.
            on : the columns to join on. If the column names are different in two tables, use the left_on and right_on parameters. Defaults to None.
            left_on : the column to join on from the left table. Defaults to None.
            right_on : the column to join on from the right table. Defaults to None.
            prevailing : whether to use prevailing window join. True: use prevailing window join. Defaults to False.

        Returns:
            the joined Table object.
        """

        if self.isMaterialized:
            leftTableName = self.tableName()
        else:
            leftTableName = f"({self.showSQL()}) as {_generate_tablename('TMP_L')}"

        if right.isMaterialized:
            rightTableName = right.tableName() if isinstance(right, Table) else right
        else:
            rightTableName = f"({right.showSQL()}) as {_generate_tablename('TMP_R')}"

        ifPrevailing = False

        if prevailing is not None:
            ifPrevailing = prevailing

        if on is not None and not isinstance(on, list) and not isinstance(on, tuple):
            on = [on]
        if left_on is not None and not isinstance(left_on, list) and not isinstance(left_on, tuple):
            left_on = [left_on]
        if right_on is not None and not isinstance(right_on, list) and not isinstance(right_on, tuple):
            right_on = [right_on]

        if on is not None:
            left_on, right_on = on, on
        elif left_on is None and right_on is None:
            raise Exception('at least one of {\'on\', \'left_on\', \'right_on\'} must be present')
        elif left_on is not None and right_on is not None and len(left_on) != len(right_on):
            raise Exception('\'left_on\' must have the same length as \'right_on\'')

        if left_on is None and right_on is not None:
            left_on = right_on
        if right_on is None and left_on is not None:
            right_on = left_on

        leftColumnNames = ''.join(['`' + x for x in left_on])
        rightColumnNames = ''.join(['`' + x for x in right_on])
        if isinstance(aggFunctions, list):
            aggFunctions = '[' + ','.join(aggFunctions) + ']'
        if ifPrevailing:
            finalTableName = 'pwj(%s,%s,%d:%d,%s,%s,%s)' % (
                leftTableName, rightTableName, leftBound, rightBound,
                '<' + aggFunctions + '>', leftColumnNames, rightColumnNames
            )
        else:
            finalTableName = 'wj(%s,%s,%d:%d,%s,%s,%s)' % (
                leftTableName, rightTableName, leftBound, rightBound, '<' + aggFunctions + '>', leftColumnNames,
                rightColumnNames)
        # print(finalTableName)
        self._init_schema()
        right._init_schema()
        joinTable = copy.copy(self)
        joinTable._setLeftTable(self.tableName())
        joinTable._setRightTable(right.tableName())
        joinTable._setTableName(finalTableName)
        joinTable._setSelect('*')
        return joinTable

    def merge_cross(self, right) -> Type["Table"]:
        """Perform a cross join with another table and return their Cartesian product.

        Args:
            right : the name of the right table from local or remote server.

        Returns:
            the joined Table object.
        """
        if self.isMaterialized:
            leftTableName = self.tableName()
        else:
            leftTableName = f"({self.showSQL()}) as {_generate_tablename('TMP_L')}"

        if right.isMaterialized:
            rightTableName = right.tableName() if isinstance(right, Table) else right
        else:
            rightTableName = f"({right.showSQL()}) as {_generate_tablename('TMP_R')}"

        finalTableName = 'cj(%s,%s)' % (leftTableName, rightTableName)
        self._init_schema()
        right._init_schema()
        joinTable = copy.copy(self)
        joinTable._setLeftTable(self.tableName())
        joinTable._setRightTable(right.tableName())
        joinTable._setTableName(finalTableName)
        joinTable._setSelect("*")
        return joinTable

    def _getSelect(self):
        return self.__select

    def _assembleSelect(self):
        try:
            if len(self.__select) and isinstance(self.__select, list):
                return ','.join(self.__select)
            else:
                return '*'
        except AttributeError:
            return '*'
        except ValueError:
            return '*'

    def _assembleWhere(self):
        try:
            if len(self.__where) == 1:
                return 'where ' + self.__where[0]
            else:
                return 'where ' + ' and '.join(["(" + x + ")" for x in self.__where])
        except AttributeError:
            return ''

    def _assembleGroupbyOrContextby(self):
        try:
            return 'group by ' + ','.join(self.__groupby)
        except AttributeError:
            try:
                return 'context by ' + ','.join(self.__contextby)
            except AttributeError:
                return ''

    def _assembleOrderby(self):
        try:
            return 'order by ' + ','.join(self.__sort)
        except AttributeError:
            return ''

    def _assembleCsort(self):
        try:
            return 'csort ' + ','.join(self.__csort)
        except AttributeError:
            return ''

    def _assembleLimit(self):
        try:
            if (self.__limit is None):
                return ''
            if len(self.__limit) and isinstance(self.__limit, list):
                return 'limit ' + ','.join(self.__limit)
            else:
                return self.__limit
        except AttributeError:
            return ''

    def showSQL(self) -> str:
        """View SQL query.

        Returns:
            the SQL query for the current Table.
        """
        import re
        selectOrExec = "exec" if self.isExec else "select"
        queryFmt = selectOrExec + ' {top} {select} from {table} {where} {groupby} {csort} {having} {orderby} {limit}'
        fmtDict = {}
        fmtDict['top'] = ("top " + self.__top) if self.__top else ''
        fmtDict['select'] = self._assembleSelect()
        fmtDict['table'] = self.tableName()
        fmtDict['where'] = self._assembleWhere()
        fmtDict['groupby'] = self._assembleGroupbyOrContextby()
        fmtDict['csort'] = self._assembleCsort()
        fmtDict['having'] = ("having " + self.__having) if self.__having else ''
        fmtDict['orderby'] = self._assembleOrderby()
        fmtDict['limit'] = self._assembleLimit()
        query = re.sub(' +', ' ', queryFmt.format(**fmtDict).strip())
        # print(query)
        # if(self.__tableName.startswith("wj") or self.__tableName.startswith("pwj")):
        #     return self.__tableName

        if get_verbose():
            print(query)
        return query

    def append(self, table: Type["Table"]) -> Type["Table"]:
        """Append data to Table and execute at once.

        Args:
            table : Table object to be appended.

        Returns:
            the appended Table object.

        Note:
            The data in the DolphinDB server will be modified immediately.
        """
        if not isinstance(table, Table):
            raise RuntimeError("Only DolphinDB Table object is accepted")

        runstr = "%s.append!(%s)" % (self.tableName(), table.tableName())
        self.__session.run(runstr)
        return self

    def update(self, cols: List[str], vals: List[str]) -> Type["TableUpdate"]:
        """Update in-memory table. Must be called to be executed.

        Args:
            cols : list of names of columns to be updated.
            vals : a list of values to be updated.

        Returns:
            a TableUpdate object.
        """
        # print("update for ", self.__tableName)
        tmp = copy.copy(self)
        if self.isMergeForUpdate:
            tmp._setLeftTable(self.getLeftTable())
        contextby = self.__contextby if hasattr(self, '__contextby') else None
        having = self.__having if hasattr(self, '__having') else None
        updateTable = TableUpdate(t=tmp, cols=cols, vals=vals, contextby=contextby, having=having)
        updateTable._setMergeForUpdate(self.isMergeForUpdate)
        return updateTable

    def rename(self, newName: str) -> None:
        """Rename table.

        Args:
            newName : new table name.
        """
        self.__session.run(newName + '=' + self.tableName())
        self.__tableName = newName

    def delete(self) -> Type["TableDelete"]:
        """Delete table.

        Returns:
            a TableDelete object.
        """
        tmp = copy.copy(self)
        delTable = TableDelete(t=tmp)
        return delTable

    def drop(self, cols: List[str]) -> Type["Table"]:
        """Delete columns.

        Args:
            cols : list of columns to be deleted. If an empty list is passed in, all columns of the current Table will be deleted.

        Returns:
            a Table object.
        """
        if cols is not None and len(cols) and isinstance(cols, list):
            runstr = '{table}.drop!([{cols}])'
            fmtDict = dict()
            fmtDict['table'] = self.tableName()
            fmtDict['cols'] = '"' + '","'.join(cols) + '"'
            query = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            for col in cols:
                for colName in self.__select:
                    if col.lower() == colName.lower():
                        self.__select.remove(colName)
            self.__session.run(query)
        else:
            runstr = '{table}.drop!([{cols}])'
            fmtDict = dict()
            fmtDict['table'] = self.tableName()
            fmtDict['cols'] = "'" + cols + "'"
            query = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            for colName in self.__select:
                if cols.lower() == colName.lower():
                    self.__select.remove(colName)
            self.__session.run(query)
        return self

    def executeAs(self, newTableName: str) -> Type["Table"]:
        """Save execution result as an in-memory table with a specified name.

        Args:
            newTableName : new table name.

        Returns:
            a new Table object saved with the new table name.
        """
        st = newTableName + "=(" + self.showSQL() + ")"
        # print(st)
        self.__session.run(st)
        return Table(data=newTableName, s=self.__session)

    def contextby(self, cols: Union[str, List[str]]) -> Type["TableContextby"]:
        """Group by specified columns.

        Args:
            cols : name of the columns to context by.

        Returns:
            a TableContextby object after adding context by clause.
        """
        contextbyTable = copy.copy(self) # Table
        contextbyTable.__merge_for_update = self.__merge_for_update
        contextby = TableContextby(contextbyTable, cols)
        contextbyTable._setContextby(contextby)
        contextbyTable._setTableName(self.tableName())
        return contextby

    def ols(self, Y:str, X:Union[str, List[str]], INTERCEPT: bool = True) -> dict:
        """Calculate Least Squares Regression Coefficients.

        Refer to https://dolphindb.com/help/FunctionsandCommands/FunctionReferences/o/ols.html.

        Args:
            Y : dependent variable, column name.
            X : argument, list of column names.
            INTERCEPT : whether to include the intercept in the regression. Defaults to True, meaning that the system automatically adds a column of "1" to X to generate the intercept.

        Returns:
            a dictionary with ANOVA, RegressionStat, Cofficient and Residual.
        """
        myY = ""
        myX = []

        if isinstance(Y, str):
            myY = Y
        else:
            raise ValueError("Y must be a column name")
        if isinstance(X, str):
            myX = [X]
        elif isinstance(X, list):
            myX = X
        else:
            raise ValueError("X must be a column name or a list of column names")
        if not len(myY) or not len(myX):
            raise ValueError("Invalid Input data")
        schema = self.__session.run("schema(%s)" % self.__tableName)
        if 'partitionColumnName' in schema and schema['partitionColumnName']:
            dsstr = "sqlDS(<SQLSQL>)".replace('SQLSQL', self.showSQL()).replace('select select', 'select')
            runstr = "olsEx({ds},{Y},{X},{INTERCEPT},2)"
            fmtDict = dict()
            fmtDict['table'] = self.tableName()
            fmtDict['Y'] = '"' + myY + '"'
            fmtDict['X'] = str(myX)
            fmtDict['ds'] = dsstr
            fmtDict['INTERCEPT'] = str(INTERCEPT).lower()
            query = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            # print(query)
            return self.__session.run(query)
        else:
            runstr = "z=exec ols({Y},{X},{INTERCEPT},2) from {table}"
            fmtDict = dict()
            fmtDict['table'] = self.tableName()
            fmtDict['Y'] = myY
            fmtDict['X'] = str(myX)
            fmtDict['INTERCEPT'] = str(INTERCEPT).lower()
            query = re.sub(' +', ' ', runstr.format(**fmtDict).strip())
            # print(query)
            return self.__session.run(query)

    def toDF(self) -> DataFrame:
        """Execute SQL statement and return a DataFrame object.

        Returns:
            data queried by SQL in DataFrame form.
        """
        self._init_schema()
        query = self.showSQL()
        df = self.__session.run(query)  # type: DataFrame
        return df

    def toList(self) -> list:
        """Execute SQL statement and return a List object.

        Returns:
            list: the query result in the form of a list. The length of the list is same as the rows of the Table.

        Note:
            If the table contains columns of array vectors, the system will try to convert them to NumPy 2D arrays. 
            The conversion will fail if the rows in the array vector are different.
        """
        self._init_schema()
        query = self.showSQL()
        list = self.__session.run(query, pickleTableToList=True)  # type: DataFrame
        return list

    toDataFrame = toDF


class TableDelete(object):
    """Table object to be deleted.

    Args:
        t : Table object to be deleted.
    """
    def __init__(self, t):
        """Constructor of TableDelete."""
        self.__t = t
        # print("constructor of ", __class__, self.__t.__dict__["_Table__tableName"])

    def _assembleWhere(self):
        try:
            if len(self.__where) == 1:
                return 'where ' + self.__where[0]
            else:
                return 'where ' + ' and '.join(["(" + x + ")" for x in self.__where])
        except AttributeError:
            return ''

    def _addWhereCond(self, conds):
        try:
            _ = self.__where
        except AttributeError:
            self.__where = []

        if isinstance(conds, list) or isinstance(conds, tuple):
            self.__where.extend([str(x) for x in conds])
        else:
            self.__where.append(str(conds))

    def where(self, conds) -> Type["TableDelete"]:
        """Add query conditions.

        Args:
            conds : query conditions.

        Returns:
            a TableDelete object with query conditions.
        """
        # print("where", self.__t.__dict__["_Table__tableName"], __name__, conds)
        self._addWhereCond(conds)
        return self

    def __executesql(self) -> str:
        return self.showSQL()

    def showSQL(self) -> str:
        """View SQL query.

        Returns:
            the SQL statement for the current TableDelete object.
        """
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller = calframe[1][3]
        if caller != 'execute' and caller != 'print' and caller != "str" and caller != '<module>' and caller != '__executesql':
            return self.__t.showSQL()
        queryFmt = 'delete from {table} {where}'
        fmtDict = {}
        fmtDict['table'] = self.__t.tableName()
        fmtDict['where'] = self._assembleWhere()
        query = re.sub(' +', ' ', queryFmt.format(**fmtDict).strip())
        return query

    def execute(self) -> Type["Table"]:
        """Execute the SQL query.

        Returns:
            Table: SQL query result as Table.
        """
        query = self.showSQL()
        self.__t.session().run(query)
        return self.__t

    def toDF(self) -> DataFrame:
        """Execute SQL statement and return a DataFrame object.

        Returns:
            data queried by SQL in DataFrame form.
        """
        query = self.showSQL()

        df = self.__t.session().run(query)  # type: DataFrame

        return df


class TableUpdate(object):
    """Table object to be updated.

    Args:
        t : Table object to be updated.
        cols : list of names of columns to be updated.
        vals : a list of values corresponding to the columns to be updated.
        contextby : whether SQL query contains a contextby clause. True means there is. Defaults to None.
        having : the content contained in a having clause. Defaults to None.
    """
    def __init__(self, t:Type["Table"], cols: List[str], vals: List[str], contextby=None, having: str = None):
        """Constructor of TableUpdate."""
        self.__t = t
        self.__cols = cols
        self.__vals = vals
        self.__contextby = contextby
        self.__having = having
        self.__merge_for_update = False
        # print("constructor of ", __class__, self.__t.__dict__["_Table__tableName"])

    def _setMergeForUpdate(self, toMerge):
        self.__merge_for_update = toMerge

    def _assembleUpdate(self):
        query = ""
        for col, val in zip(self.__cols, self.__vals):
            query += col + "=" + val + ","
        return query[:-1]

    def _assembleWhere(self):
        try:
            if len(self.__where) == 1:
                return 'where ' + self.__where[0]
            else:
                return 'where ' + ' and '.join(["(" + x + ")" for x in self.__where])
        except AttributeError:
            return ''

    def _addWhereCond(self, conds):
        try:
            _ = self.__where
        except AttributeError:
            self.__where = []

        if isinstance(conds, list) or isinstance(conds, tuple):
            self.__where.extend([str(x) for x in conds])
        else:
            self.__where.append(str(conds))

    def where(self, conds) -> Type["TableUpdate"]:
        """Add query conditions.

        Args:
            conds : query conditions.

        Returns:
            a TableUpdate object with query conditions.
        """
        # print("where", self.__t.__dict__["_Table__tableName"], __name__, conds)
        self._addWhereCond(conds)
        return self

    def __executesql(self) -> str:
        return self.showSQL()

    def showSQL(self) -> str:
        """View SQL query.

        Returns:
            the SQL statement for the current TableUpdate object.
        """
        curframe = inspect.currentframe()
        calframe = inspect.getouterframes(curframe, 2)
        caller = calframe[1][3]
        if caller != 'execute' and caller != 'print' and caller != "str" and caller != '<module>' and caller != '__executesql':
            return self.__t.showSQL()
        if not self.__merge_for_update:
            queryFmt = 'update {table} set {update} {where} {contextby} {having}'
            fmtDict = {}
            fmtDict['update'] = self._assembleUpdate()
            fmtDict['table'] = self.__t.tableName()
            fmtDict['where'] = self._assembleWhere()
            if self.__contextby:
                fmtDict['contextby'] = 'context by ' + ','.join(self.__contextby)
            else:
                fmtDict['contextby'] = ""
            if self.__having:
                fmtDict['having'] = ' having ' + self.__having
            else:
                fmtDict['having'] = ""
            query = re.sub(' +', ' ', queryFmt.format(**fmtDict).strip())
            if get_verbose():
                print(query)
            return query
        else:
            if self.__t.getLeftTable() is None:
                raise Exception("Join for update missing left table!")
            queryFmt = 'update {table} set {update} from {joinTable} {where} {contextby} {having}'
            fmtDict = {}
            fmtDict['update'] = self._assembleUpdate()
            fmtDict['table'] = self.__t.getLeftTable()
            fmtDict['joinTable'] = self.__t.tableName()
            fmtDict['where'] = self.__t._assembleWhere()
            if self.__contextby:
                fmtDict['contextby'] = 'context by ' + ','.join(self.__t.__contextby)
            else:
                fmtDict['contextby'] = ""
            if self.__having:
                fmtDict['having'] = ' having ' + self.__having
            else:
                fmtDict['having'] = ""
            query = re.sub(' +', ' ', queryFmt.format(**fmtDict).strip())
            self.__t.setMergeForUpdate(False)
            if get_verbose():
                print(query)
            return query

    def execute(self) -> Type["Table"]:
        """Execute the SQL query.

        Returns:
            SQL query result as Table.
        """
        query = self.showSQL()
        # print(query)
        self.__t.session().run(query)
        t = Table(data=self.__t.tableName(), s=self.__t.session(), needGC=self.__t.__dict__["_Table__need_gc"])
        if self.__t.__dict__["_Table__need_gc"]:
            t.__dict__["_Table__ref"] = self.__t.__dict__["_Table__ref"]
            t.__dict__["_Table__ref"].inc()
        return t

    def toDF(self) -> DataFrame:
        """Execute SQL statement and return a DataFrame object.

        Returns:
            data queried by SQL in DataFrame form.
        """
        query = self.showSQL()
        df = self.__t.session().run(query)  # type: DataFrame
        return df


class TablePivotBy(object):
    """Add pivot by clause to rearrange a column (or multiple columns) of a table on two dimensions.

    Args:
        t : Table object
        index : the result table has the same number of rows as unique values on this column.
        column : the result table has the same number of columns as unique values on this column.
        value : the parameters of the aggregate function. Defaults to None.
        agg : the specified aggregate function. Defaults to lambda x: x.
    """
    def __init__(self, t: Type["Table"], index: str, column: str, value=None, agg=None):
        """Constructor of TablePivotBy."""
        self.__row = index
        self.__column = column
        self.__val = value
        self.__t = t
        self.__agg = agg
        # print("constructor of ", __class__, self.__t.__dict__["_Table__tableName"])

    def toDF(self) -> DataFrame:
        """Execute SQL statement and return a DataFrame object.

        Returns:
            data queried by SQL in DataFrame form.
        """
        query = self.showSQL()
        if get_verbose():
            print(query)
        df = self.__t.session().run(query)  # type: DataFrame

        return df

    toDataFrame = toDF

    def _assembleSelect(self):
        if self.__val is not None:
            return self.__val if self.__agg is None else _getFuncName(self.__agg) + '(' + self.__val + ')'
        return None

    def _assembleTableSelect(self):
        return self.__t._assembleSelect()

    def _assembleWhere(self):
        return self.__t._assembleWhere()

    def _assemblePivotBy(self):
        return 'pivot by ' + self.__row + ',' + self.__column

    def executeAs(self, newTableName: str) -> Type["Table"]:
        """Save execution result as an in-memory table with a specified name.

        Args:
            newTableName : new table name.

        Returns:
            a new Table object saved with the new table name.
        """
        self.__t.session().run(newTableName + "=" + self.showSQL())
        return Table(data=newTableName, s=self.__t.session())

    def showSQL(self) -> str:
        """View SQL query.

        Returns:
            the SQL query for the current Table.
        """
        import re
        selectOrExec = "exec" if self.__t.isExec else "select"
        queryFmt = selectOrExec + ' {select} from {table} {where} {pivotby}'
        fmtDict = {}
        select = self._assembleSelect()
        if select is not None:
            fmtDict['select'] = select
        else:
            fmtDict['select'] = self._assembleTableSelect()
        fmtDict['table'] = self.__t.tableName()
        fmtDict['where'] = self._assembleWhere()
        fmtDict['pivotby'] = self._assemblePivotBy()
        query = re.sub(' +', ' ', queryFmt.format(**fmtDict).strip())
        return query

    def selectAsVector(self, colName: Union[str, Tuple[str, ...], List[str]]) -> np.array:
        """Execute query and return the specified columns.

        Args:
            colName : a string indicating the column name.

        Returns:
            data queried by SQL in numpy.array form.
        """
        if colName:
            self.__t._setSelect(colName)
        pattern = re.compile("select", re.IGNORECASE)
        query = pattern.sub('exec', self.showSQL())
        return self.__t.session().run(query)


class TableGroupby(object):
    """The table object to which the group by clause is to be added.

    Args:
        t : Table object to be added the group by clause.
        groupBys : grouping column(s).
        having : the content contained in a having clause. Defaults to None.
    """
    def __init__(self, t: Type["Table"], groupBys: Union[str, List[str]], having: str = None):
        """Constructor of TableGroupby."""
        if isinstance(groupBys, list):
            self.__groupBys = groupBys
        else:
            self.__groupBys = [groupBys]
        self.__having = having
        self.__t = t  # type: Table
        # print("constructor of ", __class__, self.__t.__dict__["_Table__tableName"])

    def sort(self, bys: Union[str, Tuple[str, ...], List[str]], ascending: bool = True) -> Type["TableGroupby"]:
        """Specify parameter for sorting.

        Args:
            bys : the column(s) to sort on.
            ascending : the sorting order. True: ascending order. Defaults to True.

        Returns:
            a TableGroupby object after adding sort clause.
        """
        sortTable = copy.copy(self.__t)
        sortTable._setSort(bys, ascending)
        return TableGroupby(sortTable, self.__groupBys, self.__having)

    def csort(self, bys: Union[str, Tuple[str, ...], List[str]], ascending: Union[bool, Tuple[bool, ...], List[bool]] = True) -> Type["TableGroupby"]:
        """Specify sorting parameters for context by.

        Args:
            bys : the column to sort on.
            ascending : the sorting order. True: ascending order. Defaults to True.

        Returns:
            a TableGroupby object after adding csort clause.
        """
        csortTable = copy.copy(self.__t)
        csortTable._setCsort(bys, ascending)
        return TableGroupby(csortTable, self.__groupBys, self.__having)

    def executeAs(self, newTableName: str) -> Type["Table"]:
        """Save execution result as an in-memory table with a specified name.

        Args:
            newTableName : new table name.

        Returns:
            a new Table object saved with the new table name.
        """
        st = newTableName + "=" + self.showSQL()
        # print(st)
        self.__t.session().run(st)
        return Table(data=newTableName, s=self.__t.session())

    def __getitem__(self, item):
        selectTable = self.__t.select(item)
        return TableGroupby(selectTable, groupBys=self.__groupBys, having=self.__having)

    def __iter__(self):
        self.__groupBysIdx = 0
        return self

    def next(self) -> str:
        """Get the name of the next group by column.

        Returns:
            column name.
        """
        try:
            result = self.__groupBys[self.__groupBysIdx]
        except IndexError:
            raise StopIteration
        self.__groupBysIdx += 1
        return result

    def __next__(self):
        return self.next()

    def having(self, expr: str) -> Type["Table"]:
        """Add having clause.

        Args:
            expr : expression for the having clause.

        Returns:
            a Table object after adding the having clause.
        """
        havingTable = copy.copy(self.__t)
        self.__having = expr
        havingTable._setHaving(self.__having)
        return havingTable

    def ols(self, Y: str, X: List[str], INTERCEPT: bool = True) -> dict:
        """Calculate Least Squares Regression Coefficients.

        Refer to https://dolphindb.com/help/FunctionsandCommands/FunctionReferences/o/ols.html.

        Args:
            Y : dependent variable, column name.
            X : argument, list of column names.
            INTERCEPT : whether to include the intercept in the regression. Defaults to True, meaning that the system automatically adds a column of "1" to X to generate the intercept.

        Returns:
            a dictionary with ANOVA, RegressionStat, Cofficient and Residual.
        """
        return self.__t.ols(Y=Y, X=X, INTERCEPT=INTERCEPT)

    def selectAsVector(self, colName: str) -> np.array:
        """Execute query and return the specified column.

        Args:
            colName : a string indicating the column name.

        Returns:
            data queried by SQL in numpy.array form.
        """
        if colName:
            self.__t._setSelect(colName)
        pattern = re.compile("select", re.IGNORECASE)
        query = pattern.sub('exec', self.showSQL())
        return self.__t.session().run(query)

    def showSQL(self) -> str:
        """View SQL query.

        Returns:
            return the SQL query for the current Table.
        """
        return self.__t.showSQL()

    def agg(self, func) -> Type["Table"]:
        """Apply group by on all columns.

        Args:
            func : can be an aggregate function, a list of aggregate functions, or a dict where each key indicates a column and the corresponding value indicates the aggregate function to be applied to this column.

        Returns:
            a Table object after aggregation.
        """
        selectCols = self.__t._getSelect()
        if isinstance(func, list):
            selectCols = [_getFuncName(f) + '(' + x + ')' for x in selectCols for f in
                          func]  # if x not in self.__groupBys
        elif isinstance(func, dict):
            funcDict = {}
            for colName, f in func.items():
                funcDict[colName] = f if isinstance(f, list) else [f]
            selectCols = [_getFuncName(f) + '(' + x + ')' for x, funcs in funcDict.items() for f in
                          funcs]  # if x not in self.__groupBys
        elif isinstance(func, str):
            selectCols = [_getFuncName(func) + '(' + x + ')' for x in selectCols]  # if x not in self.__groupBys
        else:
            raise RuntimeError(
                'invalid func format, func: aggregate function name or a list of aggregate function names'
                ' or a dict of column label/expression->func')
        aggTable = copy.copy(self.__t)
        aggTable._setSelect(selectCols)
        aggTable.isMaterialized = False
        return aggTable

    def sum(self) -> Type["Table"]:
        """Execute the aggregate function sum.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('sum')

    def avg(self) -> Type["Table"]:
        """Execute the aggregate function avg.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('avg')

    def count(self) -> Type["Table"]:
        """Execute the aggregate function count.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('count')

    def max(self) -> Type["Table"]:
        """Execute the aggregate function max.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('max')

    def min(self) -> Type["Table"]:
        """Execute the aggregate function min.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('min')

    def first(self) -> Type["Table"]:
        """Execute the aggregate function first.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('first')

    def last(self) -> Type["Table"]:
        """Execute the aggregate function last.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('last')

    def size(self) -> Type["Table"]:
        """Execute aggregate function size.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('size')

    def sum2(self) -> Type["Table"]:
        """Execute the aggregate function sum2.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('sum2')

    def std(self) -> Type["Table"]:
        """Execute aggregate function std.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('std')

    def var(self) -> Type["Table"]:
        """Execute aggregate function var.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('var')

    def prod(self) -> Type["Table"]:
        """Execute the aggregate function prod.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('prod')

    def agg2(self, func, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Perform aggregate function(s) on specified column(s).

        Args:
            func : string or list of strings indicating the aggregate function(s).
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object.
        """
        if isinstance(cols, list) is False:
            cols = [cols]
        if isinstance(func, list) is False:
            func = [func]
        if np.sum([1 for x in cols if isinstance(x, tuple) is False or len(x) != 2]):
            raise RuntimeError('agg2 only accepts (x,y) pair or a list of (x,y) pair as cols')
        funcName = [_getFuncName(f) + '(' + x + ',' + y + ')' for f in func for x, y in cols]
        selects = funcName if funcName else self.__t._getSelect()
        agg2Table = copy.copy(self.__t)
        agg2Table._setSelect(selects)
        agg2Table.isMaterialized = False
        return agg2Table

    def wavg(self, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Perform aggregate function wavg on specified column(s).

        Args:
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object.
        """
        return self.agg2('wavg', cols)

    def wsum(self, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Perform aggregate function wsum on specified column(s).

        Args:
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object.
        """
        return self.agg2('wsum', cols)

    def covar(self, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Perform aggregate function covar on specified column(s).

        Args:
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object.
        """
        return self.agg2('covar', cols)

    def corr(self, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Perform aggregate function corr on specified column(s).

        Args:
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object.
        """
        return self.agg2('corr', cols)

    def toDF(self) -> DataFrame:
        """Execute SQL statement and return a DataFrame object.

        Returns:
            data queried by SQL in DataFrame form.
        """
        query = self.showSQL()
        # print(query)
        df = self.__t.session().run(query)  # type: DataFrame
        return df


class TableContextby(object):
    """The table object to add the context by clause.

    Args:
        t : Table object to add the context by clause.
        contextBys : names of columns to be grouped for aggregation.
        having : the content contained in a having clause. Defaults to None.
    """
    def __init__(self, t: Type["Table"], contextBys: Union[str, List[str]], having: str = None):
        """Constructor of TableContextby."""
        if isinstance(contextBys, list):
            self.__contextBys = contextBys
        else:
            self.__contextBys = [contextBys]
        self.__t = t  # type: Table
        self.__having = having
        # print("constructor of ", __class__, self.__t.__dict__["_Table__tableName"])

    def sort(self, bys: Union[str, Tuple[str, ...], List[str]], ascending: bool = True) -> Type["TableContextby"]:
        """Specify parameter for sorting.

        Args:
            bys : the column(s) to sort on.
            ascending : the sorting order. True: ascending order. Defaults to True.

        Returns:
            a TableContextby object after adding sort clause.
        """
        sortTable = copy.copy(self.__t)
        sortTable._setSort(bys, ascending)
        return TableContextby(sortTable, self.__contextBys)

    def csort(self, bys: Union[str, Tuple[str, ...], List[str]], ascending: Union[bool, Tuple[bool, ...], List[bool]] = True) -> Type["TableContextby"]:
        """Specify sorting column(s) for contextby.

        Args:
            bys : the column(s) to sort on.
            ascending : the sorting order. True: ascending order. Defaults to True.

        Returns:
            a TableContextby object after adding csort clause.
        """
        csortTable = copy.copy(self.__t)
        csortTable._setCsort(bys, ascending)
        return TableContextby(csortTable, self.__contextBys)

    def having(self, expr: str) -> Type["Table"]:
        """Set having clause.

        Args:
            expr : expression for the having clause.

        Returns:
            a new Table object after adding the having clause.
        """
        havingTable = copy.copy(self.__t)
        self.__having = expr
        havingTable._setHaving(self.__having)
        return havingTable

    def __getitem__(self, item):
        selectTable = self.__t.select(item)
        return TableContextby(selectTable, contextBys=self.__contextBys)

    def __iter__(self):
        self.__contextBysIdx = 0
        return self

    def next(self) -> str:
        """Get the name of the next grouping column for context by.

        Returns:
            column name.
        """
        try:
            result = self.__contextBys[self.__contextBysIdx]
        except IndexError:
            raise StopIteration
        self.__contextBysIdx += 1
        return result

    def __next__(self):
        return self.next()

    def selectAsVector(self, colName: str) -> np.array:
        """Execute query and return the specified column.

        Args:
            colName : a string indicating the column name.

        Returns:
            data queried by SQL in numpy.array form.
        """
        if colName:
            self.__t._setSelect(colName)
        pattern = re.compile("select", re.IGNORECASE)
        query = pattern.sub('exec', self.showSQL())
        return self.__t.session().run(query)

    def top(self, num: int) -> Type["Table"]:
        """Set the top clause to retrieve the first n records from a Table.

        Args:
            num : the number of records to return. Must be a positive number.

        Returns:
            a Table object with the top clause.
        """
        return self.__t.top(num=num)

    def limit(self, num: Union[int, Tuple[int, ...], List[int]]) -> Type["Table"]:
        """Specify parameters for limit.

        Args:
            num : when used with the contextby clause, the limit clause can use a negative integer to select a limited number of records in the end of each group. 
                  In all other cases the limit clause can only use positive integers.
                  Can pass in [x, y] to select data from row x to row y.

        Returns:
            a Table object after adding the limit clause.
        """
        return self.__t.limit(num=num)

    def executeAs(self, newTableName: str) -> Type["Table"]:
        """Save execution result as an in-memory table with a specified name.

        Args:
            newTableName : new table name.

        Returns:
            a new Table object saved with the new table name.
        """
        st = newTableName + "=" + self.showSQL()
        # print(st)
        self.__t.session().run(st)
        return Table(data=newTableName, s=self.__t.session())

    def showSQL(self) -> str:
        """View SQL query.

        Returns:
            the SQL query for the current Table.
        """
        return self.__t.showSQL()

    def agg(self, func) -> Type["Table"]:
        """Apply context by on all columns except the grouping columns.

        Args:
            func : string or list of strings indicating the aggregate function(s).

        Returns:
            a Table object after aggregation.
        """
        selectCols = self.__t._getSelect()
        if isinstance(func, list):
            selectCols = [_getFuncName(f) + '(' + x + ')' for x in selectCols for f in func if
                          x not in self.__contextBys]
        elif isinstance(func, dict):
            funcDict = {}
            for colName, f in func.items():
                funcDict[colName] = f if isinstance(f, list) else [f]
            selectCols = [_getFuncName(f) + '(' + x + ')' for x, funcs in funcDict.items() for f in funcs if
                          x not in self.__contextBys]
        elif isinstance(func, str):
            selectCols = [_getFuncName(func) + '(' + x + ')' for x in selectCols if x not in self.__contextBys]
        else:
            raise RuntimeError(
                'invalid func format, func: aggregate function name or a list of aggregate function names'
                ' or a dict of column label/expression->func')
        columns = self.__contextBys[:]
        columns.extend(selectCols)
        aggTable = copy.copy(self.__t)
        aggTable._setSelect(columns)
        aggTable.isMaterialized = False
        return aggTable

    def agg2(self, func, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Perform aggregate function(s) on specified column(s).

        Args:
            func : string or list of strings indicating the aggregate function(s).
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object.
        """
        if isinstance(cols, list) is False:
            cols = [cols]
        if isinstance(func, list) is False:
            func = [func]
        if np.sum([1 for x in cols if isinstance(x, tuple) is False or len(x) != 2]):
            raise RuntimeError('agg2 only accepts (x,y) pair or a list of (x,y) pair as cols')
        funcName = [_getFuncName(f) + '(' + x + ',' + y + ')' for f in func for x, y in cols]
        selects = self.__t._getSelect()
        if funcName:
            selects.extend(funcName)
        agg2Table = copy.copy(self.__t)
        agg2Table._setSelect(selects)
        agg2Table.isMaterialized = False
        return agg2Table

    def update(self, cols: List[str], vals: List[str]) -> Type["TableUpdate"]:
        """Update in-memory table. Must be called to be executed.

        Args:
            cols : list of names of columns to be updated.
            vals : a list of values to be updated.

        Returns:
            a TableUpdate object.
        """
        updateTable = TableUpdate(t=self.__t, cols=cols, vals=vals, contextby=self.__contextBys, having=self.__having)
        return updateTable

    def sum(self) -> Type["Table"]:
        """Execute the aggregate function sum.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('sum')

    def avg(self) -> Type["Table"]:
        """Execute the aggregate function avg.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('avg')

    def count(self) -> Type["Table"]:
        """Execute the aggregate function count.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('count')

    def max(self) -> Type["Table"]:
        """Execute the aggregate function max.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('max')

    def min(self) -> Type["Table"]:
        """Execute the aggregate function min.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('min')

    def first(self) -> Type["Table"]:
        """Execute the aggregate function first.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('first')

    def last(self) -> Type["Table"]:
        """Execute the aggregate function last.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('last')

    def size(self) -> Type["Table"]:
        """Execute aggregate function size.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('size')

    def sum2(self) -> Type["Table"]:
        """Execute the aggregate function sum2.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('sum2')

    def std(self) -> Type["Table"]:
        """Execute aggregate function std.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('std')

    def var(self) -> Type["Table"]:
        """Execute aggregate function var.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('var')

    def prod(self) -> Type["Table"]:
        """Execute the aggregate function prod.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('prod')

    def cumsum(self) -> Type["Table"]:
        """Execute the aggregate function cumsum.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('cumsum')

    def cummax(self) -> Type["Table"]:
        """Execute the aggregate function cummax.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('cummax')

    def cumprod(self) -> Type["Table"]:
        """Execute the aggregate function cumprod.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('cumprod')

    def cummin(self) -> Type["Table"]:
        """Execute the aggregate function cummin.

        Returns:
            a Table object after aggregation.
        """
        return self.agg('cummin')

    def wavg(self, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Execute aggregate function wavg.

        Args:
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object after aggregation.
        """
        return self.agg2('wavg', cols)

    def wsum(self, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Execute the aggregate function wsum.

        Args:
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object after aggregation.
        """
        return self.agg2('wsum', cols)

    def covar(self, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Execute the aggregate function covar.

        Args:
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object after aggregation.
        """
        return self.agg2('covar', cols)

    def corr(self, cols: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Execute the aggregate function corr.

        Args:
            cols : ( x, y ) tuple or list of ( x, y ) tuples, where x and y are column labels or column expressions.

        Returns:
            a Table object after aggregation.
        """
        return self.agg2('corr', cols)

    def eachPre(self, args: Union[Tuple[str, str], List[Tuple[str, str]]]) -> Type["Table"]:
        """Execute the aggregate function eachPre.

        Reference link: https://dolphindb.com/help/Functionalprogramming/TemplateFunctions/eachPre.html.

        Args:
            args : the function and parameters of eachPre.

        Note:
            args [0]: the function. args [1]: a vector, matrix or table. Apply args [0] over all pairs of consecutive elements of args [1].

        Returns:
            a Table object after aggregation.
        """
        return self.agg2('eachPre', args)

    def toDF(self) -> DataFrame:
        """Execute the SQL statement and return a DataFrame object.

        Returns:
            data queried by SQL in DataFrame form.
        """
        query = self.showSQL()
        # print(query)
        df = self.__t.session().run(query)  # type : DataFrame
        return df


wavg = TableGroupby.wavg
wsum = TableGroupby.wsum
covar = TableGroupby.covar
corr = TableGroupby.corr
count = TableGroupby.count
max = TableGroupby.max
min = TableGroupby.min
sum = TableGroupby.sum
sum2 = TableGroupby.sum2
size = TableGroupby.size
avg = TableGroupby.avg
std = TableGroupby.std
prod = TableGroupby.prod
var = TableGroupby.var
first = TableGroupby.first
last = TableGroupby.last
eachPre = TableContextby.eachPre
cumsum = TableContextby.cumsum
cumprod = TableContextby.cumprod
cummax = TableContextby.cummax
cummin = TableContextby.cummin
