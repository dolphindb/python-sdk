import copy
import inspect
from locale import atoi

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import pytest
import statsmodels.api as sm
from numpy.testing import assert_equal, assert_array_almost_equal, assert_array_equal, assert_almost_equal
from pandas._testing import assert_frame_equal, assert_series_equal

from basic_testing.utils import equalPlus
from setup.prepare import generate_uuid, get_TableData, get_scripts, eval_table_with_data, eval_sql_with_data
from setup.settings import HOST, PORT, USER, PASSWD, DATA_DIR

conn_fixture = ddb.session(HOST, PORT, USER, PASSWD)

keys.set_verbose(True)

NormalTable = None
SharedTable = None
StreamTable = None
ShareStreamTable = None
IndexedTable = None
ShareIndexedTable = None
KeyedTable = None
ShareKeyedTable = None
PartitionedTable = None
loadText = None
loadText2 = None
ploadText = None
ploadText2 = None
loadTable = None
loadPTable = None
loadTableBySQL = None
loadTextEx = None
loadTextEx2 = None
df_python = None
dict_python = None
dict2_python = None
csv_df = None


def get_combinations(data_list):
    from itertools import combinations
    res = []
    for i in range(len(data_list)):
        for x in combinations(data_list, i + 1):
            res.append(x)
    return res


@pytest.fixture(scope="function", autouse=True)
def flushed_table_n100(request):
    case_name = request.node.name.replace("[", "_").replace("]", "_").replace("-", "_")
    (scripts, names), pythons = get_TableData(names="AllTrade", dbPath=f"dfs://{case_name}", shareName=case_name)
    conn_fixture.run(scripts)
    global NormalTable, SharedTable, StreamTable, ShareStreamTable, IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable, PartitionedTable, df_python, dict_python, dict2_python, NormalTable, SharedTable, StreamTable, ShareStreamTable, IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable, PartitionedTable, loadText, loadText2, ploadText, ploadText2, loadTable, loadPTable, loadTableBySQL, loadTextEx, loadTextEx2, df_python, dict_python, dict2_python, csv_df
    NormalTable = names["NormalTable"]
    SharedTable = names["SharedTable"]
    StreamTable = names["StreamTable"]
    ShareStreamTable = names["ShareStreamTable"]
    IndexedTable = names["IndexedTable"]
    ShareIndexedTable = names["ShareIndexedTable"]
    KeyedTable = names["KeyedTable"]
    ShareKeyedTable = names["ShareKeyedTable"]
    PartitionedTable = names["PartitionedTable"]
    df_python = pythons["DataFrame"]
    dict_python = pythons["dict"]
    dict2_python = pythons["dict_list"]
    NormalTable = names["NormalTable"]
    SharedTable = names["SharedTable"]
    StreamTable = names["StreamTable"]
    ShareStreamTable = names["ShareStreamTable"]
    IndexedTable = names["IndexedTable"]
    ShareIndexedTable = names["ShareIndexedTable"]
    KeyedTable = names["KeyedTable"]
    ShareKeyedTable = names["ShareKeyedTable"]
    PartitionedTable = names["PartitionedTable"]
    loadText = names["loadText"]
    loadText2 = names["loadText$"]
    ploadText = names["ploadText"]
    ploadText2 = names["ploadText$"]
    loadTable = names["loadTable"]
    loadPTable = names["loadPTable"]
    loadTableBySQL = names["loadTableBySQL"]
    loadTextEx = names["loadTextEx"]
    loadTextEx2 = names["loadTextEx$"]
    df_python = pythons["DataFrame"]
    dict_python = pythons["dict"]
    dict2_python = pythons["dict_list"]
    csv_df = pythons["csv_df"]
    yield
    for i in (f"{case_name}_ALLTradeStream", f"{case_name}_ALLQuoteStream", f"{case_name}_ALLShareTrade",
              f"{case_name}_ALLShareQuote", f"{case_name}_ALLTradeIndex", f"{case_name}_ALLQuoteIndex",
              f"{case_name}_ALLTradeKey", f"{case_name}_ALLQuoteKey"):
        conn_fixture.run(f"try{{undef(`{i},SHARED)}}catch(ex){{}}")


def get_global(name):
    if name == "DF":
        return df_python
    elif name == "DICT":
        return dict_python
    elif name == "DICT_List":
        return dict2_python
    elif name == "Table":
        return NormalTable
    elif name == "STable":
        return SharedTable
    elif name == "StreamTable":
        return StreamTable
    elif name == "SStreamTable":
        return ShareStreamTable
    elif name == "indexTable":
        return IndexedTable
    elif name == "SindexTable":
        return ShareIndexedTable
    elif name == "keyTable":
        return KeyedTable
    elif name == "SkeyTable":
        return ShareKeyedTable
    elif name == "PTable":
        return PartitionedTable
    elif name == "csv_comma":
        return loadText
    elif name == "csv_dollar":
        return loadText2
    elif name == "csv_comma_p":
        return ploadText
    elif name == "csv_dollar_p":
        return ploadText2
    elif name == "csv_comma_ex":
        return loadTextEx
    elif name == "csv_dollar_ex2":
        return loadTextEx2
    else:
        raise RuntimeError(f"{name}不存在")


class TestVector:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize("python_data", ["DF", "DICT", "DICT_List"])
    def test_Vector_init_bytable_bydataframe_bydict(self, python_data):
        python_data = get_global(python_data)
        for data in python_data:
            tmp = self.conn.table(data=data)
            if isinstance(data, pd.DataFrame):
                for vi, pi in zip(tmp.vecs, data):
                    assert tmp.vecs[vi]._Vector__name == pi
                    assert tmp.vecs[vi]._Vector__tableName == tmp.tableName()
                    assert not tmp.vecs[vi]._Vector__vec
                    assert tmp.vecs[vi]._Vector__session is self.conn
            elif isinstance(data, dict):
                for vi, pi in zip(tmp.vecs, data):
                    assert tmp.vecs[vi]._Vector__name == pi
                    assert tmp.vecs[vi]._Vector__tableName == tmp.tableName()
                    assert not tmp.vecs[vi]._Vector__vec
                    assert tmp.vecs[vi]._Vector__session is self.conn

    @pytest.mark.parametrize('python_data', ["DF", "DICT", "DICT_List", "STable"])
    def test_Vector_init(self, python_data):
        python_data = get_global(python_data)
        for data, name in zip(python_data, SharedTable):
            colNames = self.conn.run(get_scripts("colNames").format(tableName=name))
            for col in colNames:
                if isinstance(data, str):
                    vectmp = ddb.Vector(name=col, data=data, s=self.conn, tableName=name)
                    assert vectmp.name() == col
                    assert vectmp.tableName() == name
                    assert vectmp._Vector__session is self.conn
                    assert vectmp._Vector__vec is None
                elif isinstance(data, pd.DataFrame):
                    vectmp = ddb.Vector(name=col, data=data[col], s=self.conn, tableName=name)
                    assert vectmp.name() == col
                    assert vectmp.tableName() == name
                    assert vectmp._Vector__session is self.conn
                    assert_series_equal(vectmp._Vector__vec, pd.Series(data[col]))
                elif isinstance(data, dict):
                    vectmp = ddb.Vector(name=col, data=data[col], s=self.conn, tableName=name)
                    assert vectmp.name() == col
                    assert vectmp.tableName() == name
                    assert vectmp._Vector__session is self.conn
                    assert_series_equal(vectmp._Vector__vec, pd.Series(data[col]))

    @pytest.mark.parametrize('python_data', ["DF", "DICT", "DICT_List", "STable"])
    def test_Vector_name_tableName_str(self, python_data):
        python_data = get_global(python_data)
        for data, name in zip(python_data, SharedTable):
            colNames = self.conn.run(get_scripts("colNames").format(tableName=name))
            for col in colNames:
                vectmp = ddb.Vector(name=col, data=data, s=self.conn, tableName=name)
                assert vectmp.name() == vectmp._Vector__name
                assert vectmp.tableName() == vectmp._Vector__tableName
                assert str(vectmp) == col

    @pytest.mark.parametrize('python_data', ["DF", "DICT", "DICT_List", "STable"])
    def test_Vector_as_series(self, python_data):
        python_data = get_global(python_data)
        for useCache in [True, False, None]:
            self.conn.run(NormalTable[0])
            if len(python_data) == 3:
                python_data = python_data[1:]
            for data, name in zip(python_data, NormalTable[1:]):
                colNames = self.conn.run(get_scripts("colNames").format(tableName=name))
                for col in colNames:
                    vectmp = ddb.Vector(name=col, data=data, s=self.conn, tableName=name)
                    res = vectmp.as_series(useCache)
                    if isinstance(data, str):
                        assert res.name is None
                        res.name = col
                        assert_series_equal(res, self.conn.run("select * from {}".format(data))[col])
                    elif isinstance(data, dict):
                        assert_series_equal(res, pd.Series(data[col]))
                    elif isinstance(data, pd.DataFrame):
                        assert res.name is None
                        res.name = col
                        assert_series_equal(res, pd.Series(data[col]))

    @pytest.mark.parametrize('python_data', ["DF", "DICT", "DICT_List", "STable"])
    def test_Vector_op(self, python_data):
        python_data = get_global(python_data)
        self.conn.run(NormalTable[0])
        if len(python_data) == 3:
            python_data = python_data[1:]
        for data, name in zip(python_data, NormalTable[1:]):
            colNames = self.conn.run(get_scripts("colNames").format(tableName=name))
            for col in colNames:
                ddb.Vector(name=col, data=data, s=self.conn, tableName=name)


class TestTable:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('python_data', ["DF", "DICT", "DICT_List"])
    @pytest.mark.parametrize('tableName', ["Table", "STable", "StreamTable", "SStreamTable",
                                           "indexTable", "SindexTable", "keyTable", "SkeyTable"])
    def test_table_data_bydataframe_bydict(self, python_data, tableName):
        python_data = get_global(python_data)
        tableName = get_global(tableName)
        if len(tableName) == 3:
            self.conn.run(tableName[0])
            tableName = tableName[1:]
        for data, name in zip(python_data, tableName):
            tmp = self.conn.table(data=data)
            alias = tmp._getTableName()
            res = self.conn.run(get_scripts("checkEqual").format(tableA=alias, tableB=name))
            assert_equal(res, [True, True, True, True, True])
            # if type(data) == pd.DataFrame:
            assert_frame_equal(tmp.toDF(), pd.DataFrame(data))
            assert_frame_equal(self.conn.run("select * from {}".format(alias)), pd.DataFrame(data))

    @pytest.mark.parametrize('tableName', ["Table", "STable", "StreamTable", "SStreamTable",
                                           "indexTable", "SindexTable", "keyTable", "SkeyTable"])
    def test_table_data_bystr(self, tableName):
        tableName = get_global(tableName)
        if len(tableName) == 3:
            self.conn.run(tableName[0])
            tableName = tableName[1:]
        for data, name in zip(df_python, tableName):
            tmp = self.conn.table(data=name)
            assert_frame_equal(tmp.toDF(), data)

    def test_table_dbPath_data(self):
        for name in PartitionedTable[3:]:
            tmp = self.conn.table(PartitionedTable[0], name)
            res = self.conn.run('select * from loadTable("{}", `{})'.format(PartitionedTable[0], name))
            assert_frame_equal(tmp.toDF(), res)

    @pytest.mark.parametrize('python_data', ["DF", "DICT", "DICT_List"])
    def test_table_tableAliasName_data_bydataframe_bydict(self, python_data):
        python_data = get_global(python_data)
        for data, name in zip(python_data, SharedTable):
            alias = generate_uuid("tmp_")
            tmp = self.conn.table(data=data, tableAliasName=alias)
            res = self.conn.run(get_scripts("checkEqual").format(tableA=alias, tableB=name))
            assert_equal(res, [True, True, True, True, True])
            if isinstance(data, pd.DataFrame):
                assert_frame_equal(tmp.toDF(), data)
                assert_frame_equal(self.conn.run("{}".format(alias)), data)

    @pytest.mark.parametrize('python_data', ["DF"])
    @pytest.mark.parametrize('tableName', ["Table", "STable", "StreamTable", "SStreamTable",
                                           "indexTable", "SindexTable", "keyTable", "SkeyTable"])
    def test_table_tableAliasName_data_bystr(self, python_data, tableName):
        python_data = get_global(python_data)
        tableName = get_global(tableName)
        if len(tableName) == 3:
            self.conn.run(tableName[0])
            tableName = tableName[1:]
        for data, name in zip(python_data, tableName):
            alias = generate_uuid("tmp_")
            tmp = self.conn.table(data=name, tableAliasName=alias)
            assert_frame_equal(tmp.toDF(), data)
            with pytest.raises(RuntimeError):
                assert "Syntax Error" in self.conn.run("{}".format(alias))

    def test_table_tableAliasName_dbPath_data(self):
        for name in PartitionedTable[3:]:
            alias = generate_uuid("tmp_")
            tmp = self.conn.table(PartitionedTable[0], name, tableAliasName=alias)
            res = self.conn.run('select * from loadTable("{}", `{})'.format(PartitionedTable[0], name))
            assert_frame_equal(tmp.toDF(), res)

    @pytest.mark.parametrize('file', ["csv_comma", "csv_dollar"])
    def test_table_loadText(self, file):
        file = get_global(file)
        for i, name in enumerate(file[2:]):
            tbtmp = self.conn.loadText(DATA_DIR + "{}".format(name), file[1])
            re = tbtmp.toDF()
            ex = csv_df[i]
            assert_frame_equal(re, ex)

    @pytest.mark.parametrize('file', ["csv_comma_p", "csv_dollar_p"])
    def test_table_ploadText(self, file):
        file = get_global(file)
        for i, name in enumerate(file[2:]):
            tbtmp = self.conn.ploadText(DATA_DIR + "{}".format(name), file[1])
            re = tbtmp.toDF()
            ex = csv_df[i]
            assert_frame_equal(re, ex)

    @pytest.mark.parametrize('file', ["csv_comma_ex", "csv_dollar_ex2"])
    def test_table_loadTextEx(self, file):
        file = get_global(file)
        self.conn.run(file[1])
        for id, name in enumerate(file[4:6]):
            re = self.conn.loadTextEx(file[2], file[id + 7], [file[6]], DATA_DIR + "{}".format(name), file[3])
            re = re.toDF().sort_values("index", ascending=True).reset_index(drop=True)
            ex = csv_df[id]
            assert_frame_equal(re, ex)

    def test_table_loadTable_bystr(self):
        self.conn.run(loadTable[1])
        for id, name in enumerate(loadTable[2:4]):
            re = self.conn.loadTable(name).toDF()
            ex = df_python[id]
            assert_frame_equal(re, ex)

    def test_table_loadTable_dbPath(self):
        for id, name in enumerate(loadPTable[2:4]):
            re = self.conn.loadTable(name, loadPTable[1]).toDF()
            re = re.sort_values("index", ascending=True).reset_index(drop=True)
            ex = df_python[id]
            assert_frame_equal(re, ex)

    def test_table_loadTableBySQL(self):
        for id, name in enumerate(loadTableBySQL[2:4]):
            re = self.conn.loadTableBySQL(name, loadTableBySQL[1], "select * from {}".format(name)).toDF()
            re = re.sort_values("index", ascending=True).reset_index(drop=True)
            ex = df_python[id]
            assert_frame_equal(re, ex)

    @pytest.mark.parametrize('python_data', ["DF"])
    def test_table_init_needGC(self, python_data):
        python_data = get_global(python_data)
        for needGC in [True, False]:
            for data in python_data:
                tmp = ddb.Table(data=data, needGC=needGC, s=self.conn)
                tableName = tmp._getTableName()
                del tmp
                res = self.conn.run(get_scripts("objExist").format(tableName=tableName))
                assert res != needGC

    @pytest.mark.parametrize('python_data', ["DF"])
    def test_table_copy(self, python_data):
        python_data = get_global(python_data)
        for data in python_data:
            tmpa = self.conn.table(data=data)
            assert tmpa._Table__ref.val() == 1
            tmpb = copy.copy(tmpa)
            assert_frame_equal(tmpa.toDF(), tmpb.toDF())
            assert tmpa._getTableName() == tmpb._getTableName()
            assert tmpa._Table__ref.val() == 2
            assert tmpb._Table__ref.val() == 2

    @pytest.mark.parametrize('python_data', ["DF"])
    def test_table_del(self, python_data):
        python_data = get_global(python_data)
        for data in python_data:
            tmpa = self.conn.table(data=data)
            stra = tmpa.tableName()
            assert self.conn.run(get_scripts("objCounts").format(name=stra)) == 1

            tmpb = self.conn.table(data=data)
            strb = tmpb.tableName()
            assert self.conn.run(get_scripts("objCounts").format(name=strb)) == 1
            del tmpb
            assert self.conn.run(get_scripts("objCounts").format(name=strb)) == 0

    def test_table_del_bigdata(self):
        func_name = inspect.currentframe().f_code.co_name
        (_, _), pythons_tmp = get_TableData(names="AllTrade", n=300000, dbPath=f"dfs://{func_name}",
                                            shareName=func_name)
        pd_left_tmp, pd_right_tmp = pythons_tmp["DataFrame"]
        tmpa = self.conn.table(data=pd_left_tmp)
        stra = tmpa.tableName()
        assert self.conn.run(get_scripts("objCounts").format(name=stra)) == 1
        tmpb = self.conn.table(data=pd_right_tmp)
        strb = tmpb.tableName()
        assert self.conn.run(get_scripts("objCounts").format(name=strb)) == 1
        del tmpb
        assert self.conn.run(get_scripts("objCounts").format(name=strb)) == 0

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_getattr(self, data):
        data = get_global(data)

        def test_getattr(conntmp: ddb.session, tabletmp: ddb.Table):
            cols = conntmp.run(get_scripts("colNames").format(tableName=tabletmp.tableName()))
            for col in cols:
                # res = compile("tabletmp.{}".format(col), "", 'eval')
                assert getattr(tabletmp, col) is tabletmp.vecs[col]
            assert getattr(tabletmp, "tableName") == tabletmp.tableName

        eval_table_with_data(self.conn, data, test_getattr)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_getitem(self, data):
        data = get_global(data)

        def test_getitem(conntmp: ddb.session, tabletmp: ddb.Table):
            cols = conntmp.run(get_scripts("colNames").format(tableName=tabletmp.tableName()))
            for col in cols:
                res1 = tabletmp[col]
                res2 = tabletmp.select(col)
                assert_frame_equal(res1.toDF(), res2.toDF())
            res1 = tabletmp[ddb.FilterCond("index", "<", 5)]
            res2 = tabletmp.where("index < 5")
            assert_frame_equal(res1.toDF(), res2.toDF())

        eval_table_with_data(self.conn, data, test_getitem)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_property(self, data):
        data = get_global(data)

        def test_get_proprety(conntmp: ddb.session, tabletmp: ddb.Table):
            # tableName
            assert tabletmp.tableName() == tabletmp.tableName()
            assert tabletmp._getTableName() == tabletmp.tableName()
            # LeftTable and RightTable
            assert tabletmp.getLeftTable() == tabletmp._Table__leftTable
            assert tabletmp.getRightTable() == tabletmp._Table__rightTable
            # session
            assert tabletmp.session() == tabletmp._Table__session
            # rows and cols
            assert tabletmp.rows == len(tabletmp.toDF())
            assert tabletmp.cols == len(tabletmp.toDF().columns)

            assert tabletmp.colNames == [str(x) for x in tabletmp.toDF().columns]
            assert_frame_equal(tabletmp.schema, conntmp.run("schema({})".format(tabletmp.tableName()))['colDefs'])

            assert tabletmp.isMergeForUpdate == tabletmp._Table__merge_for_update
            assert tabletmp.isExec == tabletmp._Table__exec

        eval_table_with_data(self.conn, data, test_get_proprety)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_toDataFrame(self, data):
        data = get_global(data)

        def test_toDataFrame(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            assert_frame_equal(tb1.toDataFrame(), tb1.toDF())
            assert tb1.toDataFrame.__code__.co_code == tb1.toDF.__code__.co_code

        eval_sql_with_data(self.conn, data, test_toDataFrame)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_setMergeForUpdate(self, data):
        data = get_global(data)

        def test_setMergeForUpdate(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            assert not tb1._Table__merge_for_update

            tb1.setMergeForUpdate(True)
            assert tb1._Table__merge_for_update

            tb1.setMergeForUpdate(False)
            assert not tb1._Table__merge_for_update

        eval_sql_with_data(self.conn, data, test_setMergeForUpdate)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_sql_select_where(self, data):
        data = get_global(data)

        def test_select_where(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tmp1 = copy.copy(tb1)
            re = tmp1.select(["index", "price"])
            re.showSQL()
            df = df1.copy()[["index", "price"]].reset_index(drop=True)
            if partitioned:
                re = re.toDF().sort_values(by="index").reset_index(drop=True)
                df = df.sort_values(by="index").reset_index(drop=True)
                assert_frame_equal(re, df)
            else:
                re = re.toDF().reset_index(drop=True)
                assert_frame_equal(re, df)

            # select * from t where symbol==`X
            tmp1 = copy.copy(tb1)
            re = tmp1.where("symbol == `X")
            sql = re.showSQL()
            assert sql == f"select index,time,symbol,price,size from {tb1.tableName()} where symbol == `X"
            df = df1.copy()[df1["symbol"] == 'X'].reset_index(drop=True)
            if partitioned:
                re = re.toDF().sort_values(by="index").reset_index(drop=True)
                df = df.sort_values(by="index").reset_index(drop=True)
                assert_frame_equal(re, df)
            else:
                re = re.toDF().reset_index(drop=True)
                assert_frame_equal(re, df)

            # select index from t where symbol==`X
            tmp1 = copy.copy(tb1)
            re = tmp1.select("index").where("symbol == `X")
            sql = re.showSQL()
            assert "where symbol == `X" in sql
            df = df1.copy()[["index"]][df1["symbol"] == 'X'].reset_index(drop=True)
            if partitioned:
                re = re.toDF().sort_values(by="index").reset_index(drop=True)
                df = df.sort_values(by="index").reset_index(drop=True)
                assert_frame_equal(re, df)
            else:
                re = re.toDF().reset_index(drop=True)
                assert_frame_equal(re, df)

            # select index time from t where size>=50
            tmp1 = copy.copy(tb1)
            re = tmp1.select(["index", "time"]).where(tmp1.size >= 50)
            sql = re.showSQL()
            assert "where (size >= 50)" in sql
            df = df1.copy()[["index", "time"]][df1["size"] >= 50].reset_index(drop=True)
            if partitioned:
                re = re.toDF().sort_values(by="index").reset_index(drop=True)
                df = df.sort_values(by="index").reset_index(drop=True)
                assert_frame_equal(re, df)
            else:
                re = re.toDF().reset_index(drop=True)
                assert_frame_equal(re, df)

            # select index,symbol,price from t where time <= 1970.01.01 10:01:03 order by index desc
            tmp1 = copy.copy(tb1)
            re = tmp1.select(["index", "symbol", "price"])
            re = re.where(ddb.FilterCond("time", "<=", "1970.01.01 10:01:03"))
            re = re.sort("index", ascending=False)
            sql = re.showSQL()
            assert "where (time <= 1970.01.01 10:01:03) order by index desc" in sql

        eval_sql_with_data(self.conn, data, test_select_where)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_select(self, data):
        data = get_global(data)

        def test_select(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                        select_data, partitioned: bool = False):
            # select index from t
            re = tb1.select(select_data).toDF()
            if isinstance(select_data, tuple):
                select_data = list(select_data)
            if isinstance(select_data, str):
                select_data = [select_data]
            df = df1[select_data].reset_index(drop=True)
            re = re.reset_index(drop=True)
            assert_frame_equal(re, df)

        for select_data in ["index", "index", ("index", "symbol"), ["index"], ["index", "symbol"]]:
            eval_sql_with_data(self.conn, data, test_select, select_data)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_where(self, data):
        data = get_global(data)

        def test_where_str1(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                            partitioned: bool = False):
            # select index from t
            re = tb1.where("index % 3 == 0")
            sql = re.showSQL()
            assert sql == f"select index,time,symbol,price,size from {tb1.tableName()} where index % 3 == 0"
            df = df1[df1["index"] % 3 == 0].reset_index(drop=True)
            re = re.toDF().reset_index(drop=True)
            assert_frame_equal(re, df)

        def test_where_str2(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                            partitioned: bool = False):
            # select index from t
            re = tb1.where("index % 3 == 0 and symbol == `X")
            sql = re.showSQL()
            assert sql == f"select index,time,symbol,price,size from {tb1.tableName()} where index % 3 == 0 and symbol == `X"
            df = df1[(df1["index"] % 3 == 0) & (df1["symbol"] == "X")].reset_index(drop=True)
            re = re.toDF().reset_index(drop=True)
            assert_frame_equal(re, df)

        def test_where_tuple(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            # select index from t
            re = tb1.where(("index % 3 == 0", "symbol == `X"))
            sql = re.showSQL()
            assert sql == f"select index,time,symbol,price,size from {tb1.tableName()} where (index % 3 == 0) and (symbol == `X)"
            df = df1[(df1["index"] % 3 == 0) & (df1["symbol"] == "X")].reset_index(drop=True)
            re = re.toDF().reset_index(drop=True)
            assert_frame_equal(re, df)

        def test_where_list(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                            partitioned: bool = False):
            # select index from t
            re = tb1.where(["index % 3 == 0", "symbol == `X"])
            sql = re.showSQL()
            assert sql == f"select index,time,symbol,price,size from {tb1.tableName()} where (index % 3 == 0) and (symbol == `X)"
            df = df1[(df1["index"] % 3 == 0) & (df1["symbol"] == "X")].reset_index(drop=True)
            re = re.toDF().reset_index(drop=True)
            assert_frame_equal(re, df)

        def test_where_cond1(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            # select index from t
            re = tb1.where((ddb.FilterCond("index", "%", 3) == 0) & ddb.FilterCond("symbol", "==", "`X"))
            sql = re.showSQL()
            assert sql == f"select index,time,symbol,price,size from {tb1.tableName()} where (((index % 3) == 0) and (symbol == `X))"
            df = df1[(df1["index"] % 3 == 0) & (df1["symbol"] == "X")].reset_index(drop=True)
            re = re.toDF().reset_index(drop=True)
            assert_frame_equal(re, df)

        def test_where_cond2(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            # select index from t
            re = tb1.where(((ddb.FilterCond("index", "%", 3) == 0), ddb.FilterCond("symbol", "==", "`X")))
            sql = re.showSQL()
            assert sql == f"select index,time,symbol,price,size from {tb1.tableName()} where (((index % 3) == 0)) and ((symbol == `X))"
            df = df1[(df1["index"] % 3 == 0) & (df1["symbol"] == "X")].reset_index(drop=True)
            re = re.toDF().reset_index(drop=True)
            assert_frame_equal(re, df)

        eval_sql_with_data(self.conn, data, test_where_str1)
        eval_sql_with_data(self.conn, data, test_where_str2)
        eval_sql_with_data(self.conn, data, test_where_tuple)
        eval_sql_with_data(self.conn, data, test_where_list)
        eval_sql_with_data(self.conn, data, test_where_cond1)
        eval_sql_with_data(self.conn, data, test_where_cond2)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_top(self, data):
        data = get_global(data)

        def test_top_num(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            # select index from t
            re = tb1.top(5).toDF()
            df = df1[:5].reset_index(drop=True)
            re = re.reset_index(drop=True)
            assert_frame_equal(re, df)

        def test_top_str1(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                          partitioned: bool = False):
            # select index from t
            re = tb1.top("5").toDF()
            df = df1[:5].reset_index(drop=True)
            re = re.reset_index(drop=True)
            assert_frame_equal(re, df)

        def test_top_str2(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                          partitioned: bool = False):
            # select index from t
            re = tb1.top("5:19").toDF()
            df = df1[5:19].reset_index(drop=True)
            re = re.reset_index(drop=True)
            assert_frame_equal(re, df)

        eval_sql_with_data(self.conn, data, test_top_num)
        eval_sql_with_data(self.conn, data, test_top_str1)
        eval_sql_with_data(self.conn, data, test_top_str2)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_limit(self, data):
        data = get_global(data)

        def test_limit(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                       partitioned: bool = False):
            # select index from t
            re = tb1.limit(limit).toDF()
            if isinstance(limit, list) or isinstance(limit, tuple):
                if len(limit) == 2:
                    df = df1.copy()[atoi(str(limit[0])):atoi(str(limit[0])) + atoi(str(limit[1]))].reset_index(
                        drop=True)
                else:
                    df = df1.copy()[:atoi(str(limit[0]))].reset_index(drop=True)
            else:
                df = df1.copy()[:atoi(str(limit))].reset_index(drop=True)
            re = re.reset_index(drop=True)
            assert_frame_equal(re, df)

        for limit in [5, 5, (5, 18), [5], [5, 18], "5", "5", ("5", "18"), ["5"], ["5", "18"]]:
            eval_sql_with_data(self.conn, data, test_limit)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_sort(self, data):
        data = get_global(data)

        def test_sort(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame, bys,
                      ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError):
                    tb1.sort(bys, ascending=ascending)
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError):
                    tb1.sort(bys, ascending=ascending)
                return
            re = tb1.sort(bys, ascending=ascending).toDF()
            if isinstance(bys, tuple):
                bys = [str(x) for x in bys]
            if isinstance(bys, str):
                bys = [bys]
            if isinstance(ascending, tuple):
                ascending = [x for x in ascending]
            if isinstance(ascending, bool):
                ascending = [ascending for _ in range(len(bys))]
            df = df1.sort_values(bys, ascending=ascending).reset_index(drop=True)
            re = re.reset_index(drop=True)
            assert_frame_equal(re, df)

        for ascending in [True, False, [True], [False], (True, False), [True, False]]:
            for bys in ["index", "index", ["index"], ("size", "price"), ["size", "price"], ("price", "size"),
                        ["price", "size"]]:
                eval_sql_with_data(self.conn, data, test_sort, bys, ascending)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_csort_contextby(self, data):
        data = get_global(data)

        def test_csort_contextby(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, bys, ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError):
                    tb1.top(2).csort(bys, ascending=ascending).contextby("symbol")
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError):
                    tb1.top(2).csort(bys, ascending=ascending).contextby("symbol")
                return
            tbtmp1 = tb1.top(2).csort(bys, ascending=ascending).contextby("symbol")
            if isinstance(bys, tuple):
                bys = [str(x) for x in bys]
            if isinstance(bys, str):
                bys = [bys]
            if isinstance(ascending, tuple):
                ascending = [x for x in ascending]
            if isinstance(ascending, bool):
                ascending = [ascending for _ in range(len(bys))]
            _csort = [by + " desc" if not asc else by for by, asc in zip(bys, ascending)]
            ex = conntmp.run(
                "select top 2 * from {} context by symbol csort {}".format(tb1.tableName(), ",".join(_csort)))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            if partitioned:
                assert_frame_equal(re.sort_values("index", ascending=True).reset_index(drop=True),
                                   ex.sort_values("index", ascending=True).reset_index(drop=True))
            else:
                assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            if partitioned:
                assert_frame_equal(re.sort_values("index", ascending=True).reset_index(drop=True),
                                   ex.sort_values("index", ascending=True).reset_index(drop=True))
            else:
                assert_frame_equal(re, ex)

        for ascending in [True, False, [True], [False], (True, False), [True, False]]:
            for bys in ["index", "index", ["index"], ("size", "price"), ["size", "price"], ("price", "size"),
                        ["price", "size"]]:
                eval_sql_with_data(self.conn, data, test_csort_contextby, bys, ascending)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_merge_without_select(self, data):
        data = get_global(data)

        def test_merge_without_select(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                      df2: pd.DataFrame, on, how, partitioned: bool = False):
            on_0, on_1, on_2 = on
            if on[0] is not None and not isinstance(on_0, list) and not isinstance(on_0, tuple):
                on_0 = [on_0]
            on_0_str = "`" + "`".join(on_0) if on[0] else ""
            if on[1] is not None and not isinstance(on_1, list) and not isinstance(on_1, tuple):
                on_1 = [on_1]
            on_1_str = "`" + "`".join(on_1) if on[1] else ""
            if on[2] is not None and not isinstance(on_2, list) and not isinstance(on_2, tuple):
                on_2 = [on_2]
            on_2_str = "`" + "`".join(on_2) if on[2] else ""

            if on[0] is None and on[1] is None and on[2] is None:
                with pytest.raises(Exception):
                    copy.copy(tb1).merge(tb2, how=how[0], on=on[0], left_on=on[1], right_on=on[2])
                return
            if on_1 is not None and on_2 is not None and len(on_1) != len(on_2):
                with pytest.raises(Exception):
                    copy.copy(tb1).merge(tb2, how=how[0], on=on[0], left_on=on[1], right_on=on[2])
                return

            re = copy.copy(tb1).merge(tb2, how=how[0], on=on[0], left_on=on[1], right_on=on[2]).toDF()
            if on[0] is not None:
                if how[0] == "right":
                    df = conntmp.run(
                        'select * from {}({}, {}, {})'.format(how[1], tb2.tableName(), tb1.tableName(), on_0_str))
                else:
                    df = conntmp.run(
                        'select * from {}({}, {}, {})'.format(how[1], tb1.tableName(), tb2.tableName(), on_0_str))
            else:
                if how[0] == "right":
                    df = conntmp.run(
                        'select * from {}({}, {}, {}, {})'.format(how[1], tb2.tableName(), tb1.tableName(), on_2_str,
                                                                  on_1_str))
                else:
                    df = conntmp.run(
                        'select * from {}({}, {}, {}, {})'.format(how[1], tb1.tableName(), tb2.tableName(), on_1_str,
                                                                  on_2_str))
            re = re.sort_values(by="index").reset_index(drop=True)
            df = df.sort_values(by="index").reset_index(drop=True)
            re.columns = [str(i) for i, k in enumerate(re.columns)]
            df.columns = [str(i) for i, k in enumerate(df.columns)]
            assert_frame_equal(re, df)

        for how in [("left", "lj"), ("right", "lj"), ("inner", "ej"), ("outer", "fj"), ("left semi", "lsj")]:
            for on in [["symbol", None, None],
                       [["symbol"], None, None],
                       [["symbol", "time"], None, None],
                       [None, ["symbol", "size"], ["symbol", "ask"]],
                       [None, ("symbol", "size"), ("symbol", "ask")],
                       [None, None, None],
                       [None, ["symbol"], ["symbol", "ask"]],
                       ]:
                eval_sql_with_data(self.conn, data, test_merge_without_select, on, how)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_merge_asof_without_select(self, data):
        data = get_global(data)

        def test_merge_asof_without_select(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                           df2: pd.DataFrame, on, partitioned: bool = False):
            on_0, on_1, on_2 = on
            if on[0] is not None and not isinstance(on_0, list) and not isinstance(on_0, tuple):
                on_0 = [on_0]
            on_0_str = "`" + "`".join(on_0) if on[0] else ""
            if on[1] is not None and not isinstance(on_1, list) and not isinstance(on_1, tuple):
                on_1 = [on_1]
            on_1_str = "`" + "`".join(on_1) if on[1] else ""
            if on[2] is not None and not isinstance(on_2, list) and not isinstance(on_2, tuple):
                on_2 = [on_2]
            on_2_str = "`" + "`".join(on_2) if on[2] else ""

            if partitioned and on_0 is not None and len(on_0) == 1:
                pytest.skip(reason="For distributed aj, the number of matching columns must be larger than 1!")

            if on[0] is None and on[1] is None and on[2] is None:
                with pytest.raises(Exception):
                    copy.copy(tb1).merge_asof(tb2, on=on[0], left_on=on[1], right_on=on[2])
                return
            if on_1 is not None and on_2 is not None and len(on_1) != len(on_2):
                with pytest.raises(Exception):
                    copy.copy(tb1).merge_asof(tb2, on=on[0], left_on=on[1], right_on=on[2])
                return

            re = copy.copy(tb1).merge_asof(tb2, on=on[0], left_on=on[1], right_on=on[2]).toDF()
            if on[0] is not None:
                df = conntmp.run('select * from aj({}, {}, {})'.format(tb1.tableName(), tb2.tableName(), on_0_str))
            else:
                df = conntmp.run(
                    'select * from aj({}, {}, {}, {})'.format(tb1.tableName(), tb2.tableName(), on_1_str, on_2_str))
            re = re.sort_values(by="index").reset_index(drop=True)
            df = df.sort_values(by="index").reset_index(drop=True)
            re.columns = [str(i) for i, k in enumerate(re.columns)]
            df.columns = [str(i) for i, k in enumerate(df.columns)]
            assert_frame_equal(re, df)

        for on in [["time", None, None],
                   [["time"], None, None],
                   [["symbol", "time"], None, None],
                   [None, ["symbol", "size"], ["symbol", "ask"]],
                   [None, ("symbol", "size"), ("symbol", "ask")],
                   [None, None, None],
                   [None, ["symbol"], ["symbol", "ask"]],
                   ]:
            eval_sql_with_data(self.conn, data, test_merge_asof_without_select, on)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_merge_window_without_select(self, data):
        data = get_global(data)

        def test_merge_window_without_select(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                             df2: pd.DataFrame, on, bound, func, partitioned: bool = False):
            on_0, on_1, on_2 = on
            if on[0] is not None and not isinstance(on_0, list) and not isinstance(on_0, tuple):
                on_0 = [on_0]
            on_0_str = "`" + "`".join(on_0) if on[0] else ""
            if on[1] is not None and not isinstance(on_1, list) and not isinstance(on_1, tuple):
                on_1 = [on_1]
            on_1_str = "`" + "`".join(on_1) if on[1] else ""
            if on[2] is not None and not isinstance(on_2, list) and not isinstance(on_2, tuple):
                on_2 = [on_2]
            on_2_str = "`" + "`".join(on_2) if on[2] else ""
            func_ = func
            if func is not None and not isinstance(func, list) and not isinstance(func, tuple):
                func_ = [func_]
            func_str = "<[" + ",".join(func_) + "]>"

            if partitioned and on_0 is not None and len(on_0) == 1:
                pytest.skip(
                    reason="When joining two partitioned tables, the matching columns must include all partitioning columns.")

            if on[0] is None and on[1] is None and on[2] is None:
                with pytest.raises(Exception):
                    copy.copy(tb1).merge_window(tb2, bound[0], bound[1], func, on=on[0], left_on=on[1],
                                                right_on=on[2])
                return
            if on_1 is not None and on_2 is not None and len(on_1) != len(on_2):
                with pytest.raises(Exception):
                    copy.copy(tb1).merge_window(tb2, bound[0], bound[1], func, on=on[0], left_on=on[1],
                                                right_on=on[2])
                return

            re = copy.copy(tb1).merge_window(tb2, bound[0], bound[1], func, on=on[0], left_on=on[1],
                                             right_on=on[2]).toDF()
            if on[0] is not None:
                df = conntmp.run(
                    'select * from wj({}, {},{}:{},{}, {})'.format(tb1.tableName(), tb2.tableName(), bound[0], bound[1],
                                                                   func_str, on_0_str))
            else:
                df = conntmp.run(
                    'select * from wj({}, {},{}:{},{}, {}, {})'.format(tb1.tableName(), tb2.tableName(), bound[0],
                                                                       bound[1], func_str, on_1_str, on_2_str))
            re = re.sort_values(by="index").reset_index(drop=True)
            df = df.sort_values(by="index").reset_index(drop=True)
            re.columns = [str(i) for i, k in enumerate(re.columns)]
            df.columns = [str(i) for i, k in enumerate(df.columns)]
            assert_frame_equal(re, df)

        for on in [["time", None, None], [["time"], None, None], [["symbol", "time"], None, None],
                   [None, ["symbol", "size"], ["symbol", "ask"]], [None, ("symbol", "size"), ("symbol", "ask")],
                   [None, None, None], [None, ["symbol"], ["symbol", "ask"]]]:
            for bound in [(0, 0), (-15, 0), (0, 15), (-15, 15), (-20, -10), (10, 20)]:
                for func in ["avg(bid)", ["avg(bid)", "wavg(ask, bid)"]]:
                    eval_sql_with_data(self.conn, data, test_merge_window_without_select, on, bound, func)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_merge_cross_without_select(self, data):
        data = get_global(data)

        def test_merge_cross_without_select(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                            df2: pd.DataFrame, partitioned: bool = False):
            if partitioned:
                pytest.skip(
                    reason="When joining two partitioned tables, the matching columns must include all partitioning columns.")

            re = copy.copy(tb1).merge_cross(tb2).toDF()
            df = conntmp.run('select * from cj({}, {})'.format(tb1.tableName(), tb2.tableName()))
            re = re.sort_values(by="index").reset_index(drop=True)
            df = df.sort_values(by="index").reset_index(drop=True)
            re.columns = [str(i) for i, k in enumerate(re.columns)]
            df.columns = [str(i) for i, k in enumerate(df.columns)]
            assert_frame_equal(re, df)

        eval_sql_with_data(self.conn, data, test_merge_cross_without_select)

    def test_table_ols(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        self.conn.run(get_scripts("clearDatabase").format(dbPath=db_name))
        self.conn.run(f"""
                mydb = database("{db_name}", VALUE, `AMZN`NFLX`NVDA)
                loadTextEx(mydb, `trade, `TICKER, "{DATA_DIR}example.csv")
        """)
        trade = self.conn.loadTable(tableName="trade", dbPath=db_name)
        z = trade.ols(Y='PRC', X=['BID'])
        re = z["Coefficient"]
        prc_tmp = trade.toDF().PRC
        bid_tmp = trade.toDF().PRC
        model = sm.OLS(prc_tmp, bid_tmp)
        ex = model.fit().params
        assert_almost_equal(re.iloc[1, 1], ex[0], decimal=4)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_rename(self, data):
        data = get_global(data)

        def test_rename(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                        partitioned: bool = False):
            new_name = generate_uuid("rename_")
            tb1.rename(new_name)
            assert tb1.tableName() == new_name
            assert tb1.tableName() == new_name
            re = tb1.toDF()
            assert len(re) == len(df1)
            assert_frame_equal(re, df1)

        eval_sql_with_data(self.conn, data, test_rename)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_exec(self, data):
        data = get_global(data)

        def test_exec(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame, cols,
                      partitioned: bool = False):
            re = tb1.exec(cols).toDF()
            isDF = True

            if isinstance(cols, str):
                if cols == "*":
                    df = df1
                else:
                    df = df1[cols]
                    isDF = False
            elif isinstance(cols, list) or isinstance(cols, tuple):
                if len(cols) == 1:
                    df = df1[cols[0]]
                    isDF = False
                else:
                    df = df1[list(cols)]
            else:
                raise RuntimeError("error param cols!")

            if isDF:
                assert_frame_equal(re, df)
            else:
                assert_array_equal(re, df.values)

        for cols in ["*", "index", ["time"], ["index", "time"], "time", ("index", "time")]:
            eval_sql_with_data(self.conn, data, test_exec, cols)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_execute(self, data):
        data = get_global(data)

        def test_execute(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         expr, partitioned: bool = False):
            # TODO: execute must have param expr
            re = tb1.execute(expr)
            isDF = True
            if expr is None or expr == "*":
                df = df1
            elif isinstance(expr, str):
                df = df1[expr]
                isDF = False
            elif isinstance(expr, list) or isinstance(expr, tuple):
                if len(expr) == 1:
                    df = df1[expr[0]]
                    isDF = False
                else:
                    df = df1[list(expr)]
            else:
                raise RuntimeError("error param expr!")

            if isDF:
                assert_frame_equal(re, df)
            else:
                assert_array_equal(re, df.values)

        for expr in [None, "*", "index", ["time"], ["index", "time"], "time", ("index", "time")]:
            eval_sql_with_data(self.conn, data, test_execute, expr)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_executeAs(self, data):
        data = get_global(data)

        def test_executeAs(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                           partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            re = conntmp.run(get_scripts("objExist").format(tableName=new_name))
            assert not re

            tbtmp = tb1.executeAs(new_name)

            re = conntmp.run(get_scripts("objExist").format(tableName=new_name))
            assert re
            assert tbtmp is not tb1
            assert tbtmp.tableName() == new_name
            assert tb1.tableName() != new_name
            assert_frame_equal(tbtmp.toDF(), tb1.toDF())

        eval_sql_with_data(self.conn, data, test_executeAs)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List"])
    def test_table_drop(self, data):
        data = get_global(data)

        def test_drop(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame, cols,
                      partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            conntmp.run("""
                {} = {}
            """.format(new_name, tb1.tableName()))
            tbtmp = conntmp.table(data=new_name)

            if cols is None:
                with pytest.raises(Exception):
                    tbtmp.drop(cols)
                return

            if isinstance(cols, str):
                tbtmp.drop(cols)
                re = conntmp.run(get_scripts("tableCols").format(tableName=new_name))
                ex = conntmp.run(get_scripts("tableCols").format(tableName=tb1.tableName())) - 1
                assert re == ex
            elif isinstance(cols, list):
                if len(cols):
                    tbtmp.drop(cols)
                    re = conntmp.run(get_scripts("tableCols").format(tableName=new_name))
                    ex = conntmp.run(get_scripts("tableCols").format(tableName=tb1.tableName())) - len(cols)
                    assert re == ex
                else:
                    tbtmp.drop(cols)
                    return
            else:
                raise RuntimeError("no valid data for param: cols.")

        for cols in [None, "time", ["time"], ["time", "symbol"], []]:
            eval_sql_with_data(self.conn, data, test_drop, cols)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_append(self, data):
        data = get_global(data)

        def test_append(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                        partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            conntmp.run("""
                {} = {}
            """.format(new_name, tb1.tableName()))
            tbtmp = conntmp.table(data=new_name)

            data_name = generate_uuid("tmp_")
            conntmp.run("""
                {} = select * from {}
            """.format(data_name, tb1.tableName()))
            tbdata = conntmp.table(data=data_name)

            assert len(tbtmp.toDF()) == len(tbdata.toDF())
            length = len(tbdata.toDF())

            with pytest.raises(RuntimeError):
                tbtmp.append(df1)

            tbtmp.append(tbdata)
            assert len(tbtmp.toDF()) == 2 * length

        eval_sql_with_data(self.conn, data, test_append)

    @pytest.mark.parametrize('data', ["indexTable", "SindexTable", "keyTable", "SkeyTable"])
    def test_table_append_duplicate_removal(self, data):
        data = get_global(data)

        def test_append(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                        partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            conntmp.run("""
                {} = {}
            """.format(new_name, tb1.tableName()))
            tbtmp = conntmp.table(data=new_name)

            data_name = generate_uuid("tmp_")
            conntmp.run("""
                {} = select * from {}
            """.format(data_name, tb1.tableName()))
            tbdata = conntmp.table(data=data_name)

            assert len(tbtmp.toDF()) == len(tbdata.toDF())
            length = len(tbdata.toDF())

            with pytest.raises(RuntimeError):
                tbtmp.append(df1)

            tbtmp.append(tbdata)
            assert len(tbtmp.toDF()) == length

        eval_sql_with_data(self.conn, data, test_append)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_with_many_where(self, data):
        data = get_global(data)

        def select_with_many_where(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):

            # select * from t where symbol =`X and price = 3.0
            tmp1 = copy.copy(tb1)
            re = tmp1.where("symbol = `X").where("price = 3.0")
            sql = re.showSQL()
            assert "where (symbol = `X) and (price = 3.0)" in sql
            df = df1.copy().query("symbol =='X' and price == 3.0").reset_index(drop=True)
            # print(df)
            if partitioned:
                df = df.sort_values(by="index").reset_index(drop=True)
                re = re.toDF().sort_values(by="index").reset_index(drop=True)
                assert_frame_equal(re, df)
            else:
                re = re.toDF().reset_index(drop=True)
                assert_frame_equal(re, df)

            # select * from t where symbol = `X or price = 3.0
            tmp1 = copy.copy(tb1)
            re = tmp1.where("symbol = `X or price = 3.0")
            sql = re.showSQL()
            assert "where symbol = `X or price = 3.0" in sql
            df = df1.copy().query("symbol =='X' or price == 3.0").reset_index(drop=True)
            # print(df)
            if partitioned:
                df = df.sort_values(by="index").reset_index(drop=True)
                re = re.toDF().sort_values(by="index").reset_index(drop=True)
                assert_frame_equal(re, df)
            else:
                re = re.toDF().reset_index(drop=True)
                assert_frame_equal(re, df)

            # select * from pt1 where (index%3 == 0) and (price = 3.0 or price = 3.1)
            tmp1 = copy.copy(tb1)
            re: ddb.Table = tmp1.where("index%3 == 0").where("price = 3.0 or price = 3.1")
            sql = re.showSQL()
            assert "where (index%3 == 0) and (price = 3.0 or price = 3.1)" in sql
            df = df1.copy().query("(index%3 == 0) and (price == 3.0 or price == 3.1)").sort_values(
                by="index").reset_index(drop=True)
            # print(df)
            if partitioned:
                df = df.sort_values(by="index").reset_index(drop=True)
                re = re.toDF().sort_values(by="index").reset_index(drop=True)
                assert_frame_equal(re, df)
            else:
                re = re.toDF().reset_index(drop=True)
                assert_frame_equal(re, df)

            # select * from t where (index%3 == 0) and (price > 3.0 and index > 30)
            tmp1 = copy.copy(tb1)
            re: ddb.Table = tmp1.where("index%3 == 0").where("price > 3.0 and index > 30")
            sql = re.showSQL()
            assert "where (index%3 == 0) and (price > 3.0 and index > 30)" in sql
            df = df1.copy().query("(index%3 == 0) and (price > 3.0 and index > 30)").sort_values(
                by="index").reset_index(drop=True)
            # print(df)
            if partitioned:
                df = df.sort_values(by="index").reset_index(drop=True)
                re = re.toDF().sort_values(by="index").reset_index(drop=True)
                assert_frame_equal(re, df)
            else:
                re = re.toDF().reset_index(drop=True)
                assert_frame_equal(re, df)

        eval_sql_with_data(self.conn, data, select_with_many_where)


class TestTableDelete:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_delete_init(self, data):
        data = get_global(data)

        def test_delete_init(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            assert tb1._Table__ref.val() == 1
            tbtmp = tb1.delete()
            assert tbtmp._TableDelete__t.tableName() == tb1.tableName()
            assert tb1._Table__ref.val() == 2
            assert tbtmp._TableDelete__t._Table__ref.val() == 2
            assert tb1._Table__ref is tbtmp._TableDelete__t._Table__ref

        eval_sql_with_data(self.conn, data, test_delete_init)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_delete(self, data):
        data = get_global(data)
        n = 5

        def test_delete(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                        partitioned: bool = False):
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1.tableName())) == n
            tb1.delete()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1.tableName())) == n
            tb1.delete().execute()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1.tableName())) == 0

            assert conntmp.run(get_scripts("tableRows").format(tableName=tb2.tableName())) == n
            tbtmp = tb2.delete()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp._TableDelete__t.tableName())) == n
            tbtmp = tb2.delete().execute()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp.tableName())) == 0

        eval_sql_with_data(self.conn, data, test_delete)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_delete_showSQL(self, data):
        data = get_global(data)

        def test_showSQL(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp = tb1.delete()
            assert tbtmp.showSQL() == tbtmp._TableDelete__t.showSQL()

            def execute(tabletmp):
                assert tabletmp.showSQL() == "delete from {table}".format(
                    table=tabletmp._TableDelete__t.tableName())

            tbtmp = tb1.delete()
            execute(tbtmp)

            def print(tabletmp):
                assert tabletmp.showSQL() == "delete from {table}".format(
                    table=tabletmp._TableDelete__t.tableName())

            tbtmp = tb1.delete()
            print(tbtmp)

            def str(tabletmp):
                assert tabletmp.showSQL() == "delete from {table}".format(
                    table=tabletmp._TableDelete__t.tableName())

            tbtmp = tb1.delete()
            str(tbtmp)

        eval_sql_with_data(self.conn, data, test_showSQL)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_delete_where(self, data):
        data = get_global(data)
        n = 5

        def test_delete_where(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1.tableName())) == n
            tbtmp1 = tb1.delete().where("index < 5")
            sql = tbtmp1._TableDelete__executesql()
            assert "where index < 5" in sql
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1._TableDelete__t.tableName())) == n
            tbtmp1 = tb1.delete().where("index < 5").execute()
            re = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1.tableName()))
            assert re == n - 4
            tbtmp1_df = conntmp.table(data=df1)
            conntmp.run("delete from {} where index < 5".format(tbtmp1_df.tableName()))
            ex = conntmp.run("{}".format(tbtmp1_df.tableName()))
            assert_frame_equal(tbtmp1.toDF(), ex)

            assert conntmp.run(get_scripts("tableRows").format(tableName=tb2.tableName())) == n
            tbtmp2 = tb2.delete().where("index > 50").where("symbol==`X").where("ask <= 100").execute()
            sql = tb2.delete().where("index > 50").where("symbol==`X").where("ask <= 100")._TableDelete__executesql()
            assert "where (index > 50) and (symbol==`X) and (ask <= 100)" in sql
            re = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp2.tableName()))
            tbtmp2_df = conntmp.table(data=df2)
            conntmp.run(
                "delete from {} where index > 50 and symbol == `X and ask <= 100".format(tbtmp2_df.tableName()))
            ex = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp2_df.tableName()))
            assert re == ex

        eval_sql_with_data(self.conn, data, test_delete_where)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_delete_many_where(self, data):
        data = get_global(data)
        n = 5

        def test_delete_where(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1.tableName())) == n
            tbtmp1 = tb1.delete().where("index < 5").where("price > 3")
            sql = tbtmp1._TableDelete__executesql()
            assert "where (index < 5) and (price > 3)" in sql
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1._TableDelete__t.tableName())) == n
            tbtmp1 = tb1.delete().where("index < 5").where("price > 3").execute()
            re = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1.tableName()))
            assert re == n - 3
            tbtmp1_df = conntmp.table(data=df1)
            conntmp.run("delete from {} where (index < 5) and (price > 3)".format(tbtmp1_df.tableName()))
            ex = conntmp.run("{}".format(tbtmp1_df.tableName()))
            assert_frame_equal(tbtmp1.toDF(), ex)

            assert conntmp.run(get_scripts("tableRows").format(tableName=tb2.tableName())) == n
            tbtmp2 = tb2.delete().where("index > 50").where("symbol==`X or symbol==`Y").where(
                "ask <= 100 and ask > 50").execute()
            sql = tb2.delete().where("index > 50").where("symbol==`X or symbol==`Y").where(
                "ask <= 100 and ask > 50")._TableDelete__executesql()
            assert "where (index > 50) and (symbol==`X or symbol==`Y) and (ask <= 100 and ask > 50)" in sql
            re = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp2.tableName()))
            tbtmp2_df = conntmp.table(data=df2)
            conntmp.run(
                "delete from {} where (index > 50) and (symbol==`X or symbol == `Y) and (ask <= 100 and ask > 50)".format(
                    tbtmp2_df.tableName()))
            ex = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp2_df.tableName()))
            assert re == ex

        eval_sql_with_data(self.conn, data, test_delete_where)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_delete_toDF(self, data):
        data = get_global(data)
        n = 5

        def test_toDF(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                      partitioned: bool = False):
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1.tableName())) == n
            tbtmp1 = tb1.delete().where("index < 5")
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1._TableDelete__t.tableName())) == n
            tb1.delete().where("index < 5").toDF()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1.tableName())) == n

        eval_sql_with_data(self.conn, data, test_toDF)


class TestTableUpdate:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_update_without_merge(self, data):
        data = get_global(data)

        def test_update_without_merge(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                      df2: pd.DataFrame, partitioned: bool = False):
            assert tb1._Table__ref.val() == 1
            tbtmp1 = tb1.update(["price"], ["price * 10"])
            assert tb1._Table__ref.val() == 2
            assert tbtmp1._TableUpdate__t._Table__ref.val() == 2
            re = tbtmp1.execute().toDF()
            new_name = generate_uuid("tmp_")
            conntmp.upload({new_name: df1})
            conntmp.run("update {} set price=price*10".format(new_name))
            ex = conntmp.run("select * from {}".format(new_name))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_update_without_merge)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_update_merge(self, data):
        data = get_global(data)

        def test_update_merge(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, on, how, partitioned: bool = False):

            if partitioned:
                pytest.skip("merge PTable unsupported")

            tb1_df = conntmp.table(data=df1)
            tb2_df = conntmp.table(data=df2)

            on_0, on_1, on_2 = on
            if on[0] is not None and not isinstance(on_0, list) and not isinstance(on_0, tuple):
                on_0 = [on_0]
            on_0_str = "`" + "`".join(on_0) if on[0] else ""
            if on[1] is not None and not isinstance(on_1, list) and not isinstance(on_1, tuple):
                on_1 = [on_1]
            on_1_str = "`" + "`".join(on_1) if on[1] else ""
            if on[2] is not None and not isinstance(on_2, list) and not isinstance(on_2, tuple):
                on_2 = [on_2]
            on_2_str = "`" + "`".join(on_2) if on[2] else ""

            tb1.merge(tb2, how=how[0], on=on[0], left_on=on[1], right_on=on[2], merge_for_update=True).update(["price"],
                                                                                                              [
                                                                                                                  "price*ask"]).execute()
            if on[0] is not None:
                if how[0] == "right":
                    conntmp.run(
                        'update {} set price=price*ask from {}({}, {}, {})'.format(tb1_df.tableName(), how[1],
                                                                                   tb2_df.tableName(),
                                                                                   tb1_df.tableName(), on_0_str))
                else:
                    conntmp.run(
                        'update {} set price=price*ask from {}({}, {}, {})'.format(tb1_df.tableName(), how[1],
                                                                                   tb1_df.tableName(),
                                                                                   tb2_df.tableName(), on_0_str))
            else:
                if how[0] == "right":
                    conntmp.run(
                        'update {} set price=price*ask from {}({}, {}, {}, {})'.format(tb1_df.tableName(), how[1],
                                                                                       tb2_df.tableName(),
                                                                                       tb1_df.tableName(), on_2_str,
                                                                                       on_1_str))
                else:
                    conntmp.run(
                        'update {} set price=price*ask from {}({}, {}, {}, {})'.format(tb1_df.tableName(), how[1],
                                                                                       tb1_df.tableName(),
                                                                                       tb2_df.tableName(), on_1_str,
                                                                                       on_2_str))
            re = tb1.toDF().sort_values(by="index").reset_index(drop=True)
            df = conntmp.run("select * from {}".format(tb1_df.tableName())).sort_values(by="index").reset_index(
                drop=True)
            re.columns = [str(i) for i, k in enumerate(re.columns)]
            df.columns = [str(i) for i, k in enumerate(df.columns)]
            assert_frame_equal(re, df)

        for on in [
            ["symbol", None, None],
            [["symbol"], None, None],
            [["symbol", "time"], None, None],
            [None, ["symbol", "size"], ["symbol", "ask"]],
            [None, ("symbol", "size"), ("symbol", "ask")],
        ]:
            for how in [("left", "lj"), ("inner", "ej"), ("left semi", "lsj")]:
                eval_sql_with_data(self.conn, data, test_update_merge, on, how)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_update_where(self, data):
        data = get_global(data)

        def test_update_where(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tb1.update(["price"], ["price*10"]).where("symbol=`X").execute()
            sql = tb1.update(["price"], ["price*10"]).where("symbol=`X")._TableUpdate__executesql()
            assert sql == f"update {tb1.tableName()} set price=price*10 where symbol=`X"
            re = tb1.toDF()
            new_name = generate_uuid("tmp_")
            conntmp.upload({new_name: df1})
            conntmp.run("update {} set price=price*10 where symbol=`X".format(new_name))
            ex = conntmp.run("select * from {}".format(new_name))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_update_where)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_update_many_where(self, data):
        data = get_global(data)

        def test_update_where(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tb1.update(["price"], ["price*10"]).where("symbol=`X or symbol =`Z").where(
                "price > 50 and price <= 100").where("index%10=0").execute()
            sql = tb1.update(["price"], ["price*10"]).where("symbol=`X or symbol=`Z").where(
                "price > 50 and price <= 100").where("index%10=0")._TableUpdate__executesql()
            assert sql == f"update {tb1.tableName()} set price=price*10 where (symbol=`X or symbol=`Z) and (price > 50 and price <= 100) and (index%10=0)"
            re = tb1.toDF()
            new_name = generate_uuid("tmp_")
            conntmp.upload({new_name: df1})
            conntmp.run(
                "update {} set price=price*10 where (symbol=`X or symbol=`Z) and (price > 50 and price <= 100) and (index%10 =0)".format(
                    new_name))
            ex = conntmp.run("select * from {}".format(new_name))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_update_where)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_update_showSQL(self, data):
        data = get_global(data)

        def test_showSQL(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp = tb1.update(["price"], ["price+5"])
            assert tbtmp.showSQL() == tbtmp._TableUpdate__t.showSQL()

            def execute(tabletmp):
                assert tabletmp.showSQL() == "update {table} set price=price+5".format(
                    table=tabletmp._TableUpdate__t.tableName())

            tbtmp = tb1.update(["price"], ["price+5"])
            execute(tbtmp)

            def print(tabletmp):
                assert tabletmp.showSQL() == "update {table} set price=price+5".format(
                    table=tabletmp._TableUpdate__t.tableName())

            tbtmp = tb1.update(["price"], ["price+5"])
            print(tbtmp)

            def str(tabletmp):
                assert tabletmp.showSQL() == "update {table} set price=price+5".format(
                    table=tabletmp._TableUpdate__t.tableName())

            tbtmp = tb1.update(["price"], ["price+5"])
            str(tbtmp)

        eval_sql_with_data(self.conn, data, test_showSQL)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_update_toDF(self, data):
        data = get_global(data)

        def test_toDF(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                      partitioned: bool = False):
            assert_frame_equal(tb1.toDF(), df1)

            tbtmp1 = tb1.update(["price"], ["price+10"])
            re = conntmp.run("select * from {}".format(tbtmp1._TableUpdate__t.tableName()))
            assert_frame_equal(re, df1)

            re = tb1.update(["price"], ["price+10"]).toDF()
            assert_frame_equal(re, df1)

        eval_sql_with_data(self.conn, data, test_toDF)


class TestTablePivotBy:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_pivotby(self, data):
        data = get_global(data)

        def test_pivotby(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp = tb1.pivotby("index", "symbol", value="size", aggFunc=sum)
            assert tbtmp._TablePivotBy__row == "index"
            assert tbtmp._TablePivotBy__column == "symbol"
            assert tbtmp._TablePivotBy__val == "size"
            assert tbtmp._TablePivotBy__agg is sum
            assert tbtmp._TablePivotBy__t.tableName() == tb1.tableName()

        eval_sql_with_data(self.conn, data, test_pivotby)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_pivotby_isExec(self, data):
        data = get_global(data)

        def test_pivotby_isExec(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.exec(["size"]).pivotby("price", "symbol")
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("exec size from (select * from {}) pivot by price,symbol".format(tb1.tableName()))
            assert_array_almost_equal(re[0], ex[0])
            assert_array_equal(re[1], ex[1])
            assert_array_equal(re[2], ex[2])

            re = tbtmp1.toDF()
            ex = conntmp.run("exec size from (select * from {}) pivot by price,symbol".format(tb1.tableName()))
            assert_array_almost_equal(re[0], ex[0])
            assert_array_equal(re[1], ex[1])
            assert_array_equal(re[2], ex[2])

        eval_sql_with_data(self.conn, data, test_pivotby_isExec)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_pivotby_value_aggFunc(self, data):
        data = get_global(data)

        def test_pivotby_value_aggFunc(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                       df2: pd.DataFrame, partitioned: bool = False):
            tbtmp = copy.copy(tb1).select(["index", "symbol", "price"]).pivotby("size", "symbol")
            sqlstr = tbtmp.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select index,symbol,price from {} pivot by size,symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp.toDF()
            assert_frame_equal(re, ex)

            tbtmp = copy.copy(tb1).select(["index", "symbol", "price"]).pivotby("size", "symbol", value="price")
            sqlstr = tbtmp.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select price from (select * from {}) pivot by size,symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp.toDF()
            assert_frame_equal(re, ex)

            tbtmp = copy.copy(tb1).select(["index", "symbol", "price"]).pivotby("size", "symbol", value="price",
                                                                                aggFunc="sum")
            sqlstr = tbtmp.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select sum(price) from (select * from {}) pivot by size,symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp.toDF()
            assert_frame_equal(re, ex)

            tbtmp = copy.copy(tb1).select(["index", "symbol", "price"]).pivotby("size", "symbol", value="price",
                                                                                aggFunc=sum)
            sqlstr = tbtmp.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select sum(price) from (select * from {}) pivot by size,symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_pivotby_value_aggFunc)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_pivotby_executeAs(self, data):
        data = get_global(data)

        def test_pivotby_executeAs(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            tb1.pivotby("price", "symbol", "index,time,size").executeAs(new_name)
            re = conntmp.run("select * from {}".format(new_name))
            ex = conntmp.run("select index,time,size from {} pivot by price, symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_pivotby_executeAs)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_pivotby_s_electAsVector(self, data):
        data = get_global(data)

        def test_pivotby_selectAsVector(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                        df2: pd.DataFrame, partitioned: bool = False):
            re = tb1.pivotby("price", "symbol").selectAsVector("index")
            ex = conntmp.run("exec index from {} pivot by price, symbol".format(tb1.tableName()))
            assert_array_equal(re[0], ex[0])
            assert_array_equal(re[1], ex[1])
            assert_array_equal(re[2], ex[2])

            re = tb1.pivotby("price", "symbol").selectAsVector("index")
            ex = conntmp.run("exec index from {} pivot by price, symbol".format(tb1.tableName()))
            assert_array_equal(re[0], ex[0])
            assert_array_equal(re[1], ex[1])
            assert_array_equal(re[2], ex[2])

            re = tb1.pivotby("price", "symbol").selectAsVector(["index"])
            ex = conntmp.run("exec index from {} pivot by price, symbol".format(tb1.tableName()))
            assert_array_equal(re[0], ex[0])
            assert_array_equal(re[1], ex[1])
            assert_array_equal(re[2], ex[2])

        eval_sql_with_data(self.conn, data, test_pivotby_selectAsVector)


class TestTableGroupby:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby(self, data):
        data = get_global(data)

        def test_groupby(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp = copy.copy(tb1).groupby(["symbol"])
            assert tbtmp._TableGroupby__groupBys == ["symbol"]
            assert tbtmp._TableGroupby__having is None
            assert tbtmp._TableGroupby__t.tableName() == tb1.tableName()

            tbtmp = copy.copy(tb1).groupby("symbol")
            assert tbtmp._TableGroupby__groupBys == ["symbol"]
            assert tbtmp._TableGroupby__having is None
            assert tbtmp._TableGroupby__t.tableName() == tb1.tableName()

            tbtmp = copy.copy(tb1).groupby(["symbol", "size"])
            assert tbtmp._TableGroupby__groupBys == ["symbol", "size"]
            assert tbtmp._TableGroupby__having is None
            assert tbtmp._TableGroupby__t.tableName() == tb1.tableName()

        eval_sql_with_data(self.conn, data, test_groupby)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_executeAs(self, data):
        data = get_global(data)

        def test_groupby_executeAs(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            tb1.select(["max(size)"]).groupby(["symbol"]).executeAs(new_name)
            re = conntmp.run("select * from {}".format(new_name))
            ex = conntmp.run("select max(size) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_executeAs)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_s_electAsVector(self, data):
        data = get_global(data)

        def test_groupby_selectAsVector(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                        df2: pd.DataFrame, partitioned: bool = False):
            re = tb1.groupby(["symbol"]).selectAsVector("max(index)")
            ex = conntmp.run("exec max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

            re = tb1.groupby(["symbol"]).selectAsVector("max(index)")
            ex = conntmp.run("exec max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

            re = tb1.groupby(["symbol"]).selectAsVector(["max(index)"])
            ex = conntmp.run("exec max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_selectAsVector)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby___getitem__(self, data):
        data = get_global(data)

        def test_groupby___getitem__(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                     df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.groupby(["symbol"])["max(index)"]
            assert tbtmp1._TableGroupby__t._Table__select == ["max(index)"]
            re = tbtmp1.toDF()
            ex = conntmp.run("select max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby___getitem__)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby___next__(self, data):
        data = get_global(data)

        def test_groupby___next__(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                  df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.groupby(["symbol"])
            re = [x for x in tbtmp1]
            ex = ["symbol"]
            assert re == ex

            tbtmp1 = tb1.groupby(["symbol", "size"])["max(index)"]
            re = [x for x in tbtmp1]
            ex = ["symbol", "size"]
            assert re == ex

            tbtmp1 = tb1.groupby(["symbol", "size"])["max(index)"]
            re = (x for x in tbtmp1)
            re = [x for x in re]
            ex = ["symbol", "size"]
            assert re == ex

        eval_sql_with_data(self.conn, data, test_groupby___next__)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_having(self, data):
        data = get_global(data)

        def test_groupby_having(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select("max(index)").groupby(["symbol"]).having("first(size)>90")
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select max(index) from {} group by symbol having first(size)>90".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            ex = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_having)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_agg(self, data):
        data = get_global(data)

        def test_groupby_agg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select([aggcol]).groupby("symbol").agg(aggfunc)
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select {} from {} group by symbol".format(aggscript, tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

            tbtmp1 = tb1.groupby("symbol").agg({
                'price': ["max"],
                'time': "first",
                'size': ["avg", "sum"],
            })
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run(
                "select max(price), first(time), avg(size), sum(size) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

            with pytest.raises(RuntimeError) as e:
                tb1.select(["index"]).groupby("symbol").agg(("sum", "max"))
            assert str(e).find("invalid func format")

        for aggcol, aggfunc, aggscript in [
            ['index', ['sum', 'max'], 'sum(index), max(index)'],
            ['price', ['max'], 'max(price)'],
            ['time', ['first'], 'first(time)'],
            ['size', ['avg', 'sum'], 'avg(size), sum(size)'],
        ]:
            eval_sql_with_data(self.conn, data, test_groupby_agg)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_sum(self, data):
        data = get_global(data)

        def test_groupby_sum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").sum()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select sum(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_sum)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_avg(self, data):
        data = get_global(data)

        def test_groupby_avg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").avg()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select avg(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_avg)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_count(self, data):
        data = get_global(data)

        def test_groupby_count(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").count()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select count(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_count)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_max(self, data):
        data = get_global(data)

        def test_groupby_max(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").max()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_max)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_min(self, data):
        data = get_global(data)

        def test_groupby_min(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").min()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select min(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_min)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_first(self, data):
        data = get_global(data)

        def test_groupby_first(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["time"]).groupby("symbol").first()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select first(time) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_first)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_last(self, data):
        data = get_global(data)

        def test_groupby_last(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["time"]).groupby("symbol").last()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select last(time) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_last)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_size(self, data):
        data = get_global(data)

        def test_groupby_size(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").size()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select size(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_size)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_sum2(self, data):
        data = get_global(data)

        def test_groupby_sum2(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").sum2()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select sum2(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_sum2)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_std(self, data):
        data = get_global(data)

        def test_groupby_std(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").std()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select std(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_std)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_var(self, data):
        data = get_global(data)

        def test_groupby_var(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").var()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select var(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_var)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_prod(self, data):
        data = get_global(data)

        def test_groupby_prod(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").prod()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select prod(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_prod)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_agg2(self, data):
        data = get_global(data)

        def test_groupby_agg2(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.groupby("symbol").agg2(*aggfunc)
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select {} from {} group by symbol".format(aggscript, tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

            with pytest.raises(RuntimeError):
                tb2.groupby("symbol").agg2(ddb.wsum, [["ask", "bid"]])

            with pytest.raises(RuntimeError):
                tb2.groupby("symbol").agg2(ddb.wsum, [["ask", "bid", "index"]])

            with pytest.raises(RuntimeError):
                tb2.groupby("symbol").agg2(ddb.wsum, [("ask", "bid"), ["ask", "index"]])

            tbtmp2 = tb2.select([aggscript]).groupby("symbol").agg2([], [("ask", "bid")])
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select {} from {} group by symbol".format(aggscript, tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        for aggfunc, aggscript in [
            [[ddb.corr, ('ask', 'bid')], "corr(ask, bid)"],
            [['atImax', ('ask', 'bid')], "atImax(ask, bid)"],
            [['contextSum', ('ask', 'bid')], "contextSum(ask, bid)"],
            [[ddb.covar, ('ask', 'bid')], "covar(ask, bid)"],
            [[ddb.wavg, ('ask', 'bid')], "wavg(ask, bid)"],
            [[ddb.wsum, ('ask', 'bid')], "wsum(ask, bid)"],
            [[ddb.wsum, [('ask', 'bid'), ('bid', 'ask')]], "wsum(ask, bid), wsum(bid, ask)"],
        ]:
            eval_sql_with_data(self.conn, data, test_groupby_agg2)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_wavg(self, data):
        data = get_global(data)

        def test_groupby_wavg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.groupby("symbol").wavg(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select wavg(ask, bid) from {} group by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_wavg)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_wsum(self, data):
        data = get_global(data)

        def test_groupby_wsum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.groupby("symbol").wsum(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select wsum(ask, bid) from {} group by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_wsum)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_covar(self, data):
        data = get_global(data)

        def test_groupby_covar(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.groupby("symbol").covar(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select covar(ask, bid) from {} group by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_covar)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_groupby_corr(self, data):
        data = get_global(data)

        def test_groupby_corr(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.groupby("symbol").corr(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select corr(ask, bid) from {} group by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_corr)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_groupby_sort(self, data):
        data = get_global(data)

        def test_groupby_sort(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, bys, ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError):
                    tb1.select(["sum(price)"]).groupby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError):
                    tb1.select(["sum(price)"]).groupby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
                return
            tbtmp1 = tb1.select(["sum(price)"]).groupby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
            if isinstance(bys, tuple):
                bys = [str(x) for x in bys]
            if isinstance(bys, str):
                bys = [bys]
            if isinstance(ascending, tuple):
                ascending = [x for x in ascending]
            if isinstance(ascending, bool):
                ascending = [ascending for _ in range(len(bys))]
            _sort = [by + " desc" if not asc else by for by, asc in zip(bys, ascending)]
            ex = conntmp.run(
                "select sum(price) from {} group by symbol, size, price order by {}".format(tb1.tableName(),
                                                                                            ",".join(_sort)))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        for bys in ["price", "price", ["price"], ("size", "price"), ["size", "price"], ("price", "size"),
                    ["price", "size"]]:
            for ascending in [True, False, [True], [False], (True, False), [True, False]]:
                eval_sql_with_data(self.conn, data, test_groupby_sort, bys, ascending)


class TestTableContextby:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby(self, data):
        data = get_global(data)

        def test_contextby(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                           partitioned: bool = False):
            tbtmp = copy.copy(tb1).contextby(["symbol"])
            assert tbtmp._TableContextby__contextBys == ["symbol"]
            assert tbtmp._TableContextby__having is None
            assert tbtmp._TableContextby__t.tableName() == tb1.tableName()

            tbtmp = copy.copy(tb1).contextby("symbol")
            assert tbtmp._TableContextby__contextBys == ["symbol"]
            assert tbtmp._TableContextby__having is None
            assert tbtmp._TableContextby__t.tableName() == tb1.tableName()

            tbtmp = copy.copy(tb1).contextby(["symbol", "size"])
            assert tbtmp._TableContextby__contextBys == ["symbol", "size"]
            assert tbtmp._TableContextby__having is None
            assert tbtmp._TableContextby__t.tableName() == tb1.tableName()

        eval_sql_with_data(self.conn, data, test_contextby)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_executeAs(self, data):
        data = get_global(data)

        def test_contextby_executeAs(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                     df2: pd.DataFrame, partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            tb1.contextby(["symbol"]).executeAs(new_name)
            re = conntmp.run("select * from {}".format(new_name))
            ex = conntmp.run("select * from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_executeAs)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_s_electAsVector(self, data):
        data = get_global(data)

        def test_contextby_selectAsVector(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                          df2: pd.DataFrame, partitioned: bool = False):
            re = tb1.contextby(["symbol"]).selectAsVector("max(index)")
            ex = conntmp.run("exec max(index) from {} context by symbol".format(tb1.tableName()))
            assert_array_equal(re, ex)

            re = tb1.contextby(["symbol"]).selectAsVector("max(index)")
            ex = conntmp.run("exec max(index) from {} context by symbol".format(tb1.tableName()))
            assert_array_equal(re, ex)

            re = tb1.contextby(["symbol"]).selectAsVector(["max(index)"])
            ex = conntmp.run("exec max(index) from {} context by symbol".format(tb1.tableName()))
            assert_array_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_selectAsVector)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby___getitem__(self, data):
        data = get_global(data)

        def test_contextby___getitem__(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                       df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.contextby(["symbol"])["index"]
            assert tbtmp1._TableContextby__t._Table__select == ["index"]
            re = tbtmp1.toDF()
            ex = conntmp.run("select index from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby___getitem__)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby___next__(self, data):
        data = get_global(data)

        def test_contextby___next__(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                    df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.groupby(["symbol"])
            re = [x for x in tbtmp1]
            ex = ["symbol"]
            assert re == ex

            tbtmp1 = tb1.groupby(["symbol", "size"])["index"]
            re = [x for x in tbtmp1]
            ex = ["symbol", "size"]
            assert re == ex

            tbtmp1 = tb1.groupby(["symbol", "size"])["index"]
            re = (x for x in tbtmp1)
            re = [x for x in re]
            ex = ["symbol", "size"]
            assert re == ex

        eval_sql_with_data(self.conn, data, test_contextby___next__)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_having(self, data):
        data = get_global(data)

        def test_contextby_having(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                  df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select("index").contextby(["symbol"]).having("first(size)>90")
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select index from {} context by symbol having first(size)>90".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            ex = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_having)

    @pytest.mark.parametrize('data', [
        "Table",
        "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_agg_list(self, data):
        data = get_global(data)

        def test_contextby_agg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select([aggcol]).contextby("symbol").agg(aggfunc)
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol,{} from {} context by symbol".format(aggscript, tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        for aggcol, aggfunc, aggscript in [
            ['index', ['sum', 'max'], 'sum(index), max(index)'],
            ['price', ['max'], 'max(price)'],
            ['time', ['first'], 'first(time)'],
            ['size', ['avg', 'sum'], 'avg(size), sum(size)'],
        ]:
            eval_sql_with_data(self.conn, data, test_contextby_agg)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_agg_tuple(self, data):
        data = get_global(data)

        def test_contextby_agg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            with pytest.raises(RuntimeError) as e:
                tb1.select(["index"]).contextby("symbol").agg(("sum", "max"))
            assert str(e).find("invalid func format")

        eval_sql_with_data(self.conn, data, test_contextby_agg)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_agg_str(self, data):
        data = get_global(data)

        def test_contextby_agg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select([aggcol]).contextby("symbol").agg(aggfunc)
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, {} from {} context by symbol".format(aggscript, tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        for aggcol, aggfunc, aggscript in [
            ['index', ['sum', 'max'], 'sum(index), max(index)'],
            ['price', ['max'], 'max(price)'],
            ['time', ['first'], 'first(time)'],
            ['size', ['avg', 'sum'], 'avg(size), sum(size)'],
        ]:
            eval_sql_with_data(self.conn, data, test_contextby_agg)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_agg_dict(self, data):
        data = get_global(data)

        def test_contextby_agg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.contextby("symbol").agg({
                'price': ["max"],
                'time': "first",
                'size': ["avg", "sum"],
            })
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run(
                "select symbol, max(price), first(time), avg(size), sum(size) from {} context by symbol".format(
                    tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_agg)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_sum(self, data):
        data = get_global(data)

        def test_contextby_sum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").sum()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, sum(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_sum)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_avg(self, data):
        data = get_global(data)

        def test_contextby_avg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").avg()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, avg(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_avg)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_count(self, data):
        data = get_global(data)

        def test_contextby_count(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").count()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, count(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_count)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_max(self, data):
        data = get_global(data)

        def test_contextby_max(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").max()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, max(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_max)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_min(self, data):
        data = get_global(data)

        def test_contextby_min(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").min()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, min(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_min)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_first(self, data):
        data = get_global(data)

        def test_contextby_first(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["time"]).contextby("symbol").first()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, first(time) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_first)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_last(self, data):
        data = get_global(data)

        def test_contextby_last(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["time"]).contextby("symbol").last()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, last(time) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_last)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_size(self, data):
        data = get_global(data)

        def test_contextby_size(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").size()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, size(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_size)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_sum2(self, data):
        data = get_global(data)

        def test_contextby_sum2(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").sum2()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, sum2(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_sum2)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_std(self, data):
        data = get_global(data)

        def test_contextby_std(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").std()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, std(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_std)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_var(self, data):
        data = get_global(data)

        def test_contextby_var(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").var()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, var(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_var)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_prod(self, data):
        data = get_global(data)

        def test_contextby_prod(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").prod()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, prod(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_prod)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_cumsum(self, data):
        data = get_global(data)

        def test_contextby_cumsum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                  df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").cumsum()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, cumsum(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_cumsum)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_cummax(self, data):
        data = get_global(data)

        def test_contextby_cummax(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                  df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").cummax()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, cummax(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_cummax)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_cumprod(self, data):
        data = get_global(data)

        def test_contextby_cumprod(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["price"]).contextby("symbol").cumprod()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, cumprod(price) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_cumprod)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_cummin(self, data):
        data = get_global(data)

        def test_contextby_cummin(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                  df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").cummin()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, cummin(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_cummin)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_agg2(self, data):
        data = get_global(data)

        def test_contextby_agg2(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.contextby("symbol").agg2(*aggfunc)
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run(
                "select index,time,symbol,ask,bid,{} from {} context by symbol".format(aggscript, tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

            with pytest.raises(RuntimeError):
                tb2.contextby("symbol").agg2(ddb.wsum, [["ask", "bid"]])

            with pytest.raises(RuntimeError):
                tb2.contextby("symbol").agg2(ddb.wsum, [["ask", "bid", "index"]])

            with pytest.raises(RuntimeError):
                tb2.contextby("symbol").agg2(ddb.wsum, [("ask", "bid"), ["ask", "index"]])

            tbtmp2 = tb2.select([aggscript]).contextby("symbol").agg2([], [("ask", "bid")])
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select {} from {} context by symbol".format(aggscript, tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        for aggfunc, aggscript in [
            [[ddb.corr, ('ask', 'bid')], "corr(ask, bid)"],
            [['atImax', ('ask', 'bid')], "atImax(ask, bid)"],
            [['contextSum', ('ask', 'bid')], "contextSum(ask, bid)"],
            [[ddb.covar, ('ask', 'bid')], "covar(ask, bid)"],
            [[ddb.wavg, ('ask', 'bid')], "wavg(ask, bid)"],
            [[ddb.wsum, ('ask', 'bid')], "wsum(ask, bid)"],
            [[ddb.wsum, [('ask', 'bid'), ('bid', 'ask')]], "wsum(ask, bid), wsum(bid, ask)"],
        ]:
            eval_sql_with_data(self.conn, data, test_contextby_agg2)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_wavg(self, data):
        data = get_global(data)

        def test_contextby_wavg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.contextby("symbol").wavg(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select *, wavg(ask, bid) from {} context by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_wavg)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_wsum(self, data):
        data = get_global(data)

        def test_contextby_wsum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.contextby("symbol").wsum(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select *, wsum(ask, bid) from {} context by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_wsum)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_covar(self, data):
        data = get_global(data)

        def test_contextby_covar(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.contextby("symbol").covar(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select *, covar(ask, bid) from {} context by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_covar)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_corr(self, data):
        data = get_global(data)

        def test_contextby_corr(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.contextby("symbol").corr(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run(
                "select index,time,symbol,ask,bid,corr(ask, bid) from {} context by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_corr)

    @pytest.mark.parametrize('data', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ])
    def test_table_contextby_eachPre(self, data):
        data = get_global(data)

        def test_contextby_eachPre(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.contextby("symbol").eachPre(("\\", "price"))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select *,symbol, eachPre(\\, price) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_eachPre)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_contextby_sort(self, data):
        data = get_global(data)

        def test_contextby_sort(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, bys, ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError):
                    tb1.contextby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError):
                    tb1.contextby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
                return
            tbtmp1 = tb1.contextby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
            if isinstance(bys, tuple):
                bys = [str(x) for x in bys]
            if isinstance(bys, str):
                bys = [bys]
            if isinstance(ascending, tuple):
                ascending = [x for x in ascending]
            if isinstance(ascending, bool):
                ascending = [ascending for _ in range(len(bys))]
            _csort = [by + " desc" if not asc else by for by, asc in zip(bys, ascending)]
            ex = conntmp.run(
                "select * from {} context by symbol, size, price order by {}".format(tb1.tableName(), ",".join(_csort)))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        for bys in ["price", ["price"], ("size", "price"), ["size", "price"], ("price", "size"),
                    ["price", "size"]]:
            for ascending in [True, False, [True], [False], (True, False), [True, False]]:
                eval_sql_with_data(self.conn, data, test_contextby_sort, bys, ascending)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_contextby_csort(self, data):
        data = get_global(data)

        def test_contextby_csort(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, bys, ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError):
                    tb1.top(2).contextby("symbol").csort(bys, ascending=ascending)
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError):
                    tb1.top(2).contextby("symbol").csort(bys, ascending=ascending)
                return
            tbtmp1 = tb1.top(2).contextby("symbol").csort(bys, ascending=ascending)
            if isinstance(bys, tuple):
                bys = [str(x) for x in bys]
            if isinstance(bys, str):
                bys = [bys]
            if isinstance(ascending, tuple):
                ascending = [x for x in ascending]
            if isinstance(ascending, bool):
                ascending = [ascending for _ in range(len(bys))]
            _csort = [by + " desc" if not asc else by for by, asc in zip(bys, ascending)]
            ex = conntmp.run(
                "select top 2 * from {} context by symbol csort {}".format(tb1.tableName(), ",".join(_csort)))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            if partitioned:
                assert_frame_equal(re.sort_values("index", ascending=True).reset_index(drop=True),
                                   ex.sort_values("index", ascending=True).reset_index(drop=True))
            else:
                assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            if partitioned:
                assert_frame_equal(re.sort_values("index", ascending=True).reset_index(drop=True),
                                   ex.sort_values("index", ascending=True).reset_index(drop=True))
            else:
                assert_frame_equal(re, ex)

        for bys in ["index", "index", ["index"], ("size", "price"), ["size", "price"], ("price", "size"),
                    ["price", "size"]]:
            for ascending in [True, False, [True], [False], (True, False), [True, False]]:
                eval_sql_with_data(self.conn, data, test_contextby_csort, bys, ascending)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_contextby_top(self, data):
        data = get_global(data)

        def test_top_num(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp1 = tb1.contextby("symbol").top(5)
            ex = conntmp.run("select top 5 * from {} context by symbol".format(tb1.tableName()))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        def test_top_str(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp1 = tb1.contextby("symbol").top("5")
            ex = conntmp.run("select top 5 * from {} context by symbol".format(tb1.tableName()))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_top_num)
        eval_sql_with_data(self.conn, data, test_top_str)

    @pytest.mark.parametrize('data', ["Table", "STable", "StreamTable", "SStreamTable",
                                      "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                      "DF", "DICT", "DICT_List", "PTable"])
    def test_table_contextby_limit(self, data):
        data = get_global(data)

        def test_limit(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                       partitioned: bool = False):
            tbtmp1 = tb1.contextby("symbol").limit(limit)
            if isinstance(limit, list) or isinstance(limit, tuple):
                if len(limit) == 2:
                    ex = conntmp.run(
                        "select * from {} context by symbol limit {}, {}".format(tb1.tableName(), limit[0], limit[1]))
                else:
                    ex = conntmp.run("select * from {} context by symbol limit {}".format(tb1.tableName(), limit[0]))
            else:
                ex = conntmp.run("select * from {} context by symbol limit {}".format(tb1.tableName(), limit))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        for limit in [5, [5], "5", ["5"]]:
            eval_sql_with_data(self.conn, data, test_limit)

    def test_table_contextby_illegal_columns(self):
        data = pd.DataFrame({
            '2017.1': [1, 2, 3],
            '2017.1.1': [1, 2, 3]
        })
        self.conn.upload({
            'a': data
        })
        tb = self.conn.table(data='a')
        # colNames
        assert tb.colNames == ['2017.1', '2017.1.1']
        # select
        assert equalPlus(tb.select('_"2017.1"').toDF(), pd.DataFrame({'2017.1': [1, 2, 3]}))
        # drop
        assert equalPlus(tb.drop('2017.1.1').toDF(), pd.DataFrame({'2017.1': [1, 2, 3]}))
        # update
        assert equalPlus(tb.update(['_"2017.1"'], ['1']).where('_"2017.1"=1').execute().toDF(),
                         pd.DataFrame({'2017.1': [1, 2, 3]}))
        # append
        assert equalPlus(tb.append(tb).toDF(), pd.DataFrame({'2017.1': [1, 2, 3, 1, 2, 3]}))
        # toList
        assert equalPlus(tb.toList(), [np.array([1, 2, 3, 1, 2, 3], dtype=np.int64)])
        # delete
        assert equalPlus(tb.delete().where('_"2017.1"=1').execute().toDF(), pd.DataFrame({'2017.1': [2, 3, 2, 3]}))
        # context by
        assert equalPlus(tb.contextby('_"2017.1"').toDF(), pd.DataFrame({'2017.1': [2, 3, 2, 3]}))
        # group by
        assert equalPlus(tb.groupby('_"2017.1"').toDF(), pd.DataFrame({'2017.1': [2, 3]}))
        # order by
        assert equalPlus(tb.sort('_"2017.1"').toDF(), pd.DataFrame({'2017.1': [2, 2, 3, 3]}))
