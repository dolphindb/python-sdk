import copy
from locale import atoi
from operator import add, and_, eq, floordiv, ge, gt, le, lshift, lt, mul, ne, or_, rshift, sub, truediv, mod

import dolphindb as ddb
import dolphindb.settings as keys
import pandas as pd
import pytest
import statsmodels.api as sm
from numpy.testing import *
from pandas.testing import *

from setup.prepare import generate_uuid, get_TableData, get_scripts, eval_table_with_data, eval_sql_with_data
from setup.settings import *
from setup.utils import get_pid

# pytestmark = pytest.mark.skip("有很多error和failure待修复")

keys.set_verbose(True)

(scripts, names), pythons = get_TableData(names="AllTrade", n=100)

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


def get_combinations(data_list):
    from itertools import combinations
    res = []
    for i in range(len(data_list)):
        for x in combinations(data_list, i + 1):
            res.append(x)
    return res


@pytest.fixture(scope="function")
def flushed_table_n100(request):
    conn_fixture = ddb.session(HOST, PORT, USER, PASSWD)
    conn_fixture.run(get_scripts("undefShare"))

    (scripts, names), pythons = get_TableData(names="AllTrade", n=100)
    conn_fixture.run(scripts)
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

    tabledict = {
        "Table": NormalTable,
        "STable": SharedTable,
        "streamTable": StreamTable,
        "SStreamTable": ShareStreamTable,
        "indexTable": IndexedTable,
        "SindexTable": ShareIndexedTable,
        "keyTable": KeyedTable,
        "SkeyTable": ShareKeyedTable,
        "DF": df_python,
        "DICT": dict_python,
        "DICT_List": dict2_python,
        "PTable": PartitionedTable,
    }

    yield tabledict[request.param]


operators = {
    "or": or_,
    "and": and_,
    "<": lt,
    "<=": le,
    ">": gt,
    ">=": ge,
    "==": eq,
    "!=": ne,
    "+": add,
    "-": sub,
    "*": mul,
    "/": truediv,
    r"%": mod,
    "<<": lshift,
    ">>": rshift,
    "//": floordiv,
}

corresponds = {
    "<": ">",
    ">": "<",
    ">=": "<=",
    "<=": ">=",
    "!=": "!=",
    "==": "==",
}


class TestFilterCond:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            self.conn.run(get_scripts("undefShare"))
            self.conn.run(scripts)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    @pytest.mark.parametrize('right', ["rightName"] + [ddb.FilterCond("tmpc", x, "tmpd") for x in operators],
                             ids=["rightName"] + ["R_" + str(x) for x in operators])
    @pytest.mark.parametrize('op', operators.keys(), ids=operators.keys())
    @pytest.mark.parametrize('left', ["leftName"] + [ddb.FilterCond("tmpa", x, "tmpb") for x in operators],
                             ids=["leftName"] + ["L_" + str(x) for x in operators])
    def test_TestFilterCond_init_str(self, left, right, op):
        FilterCondtmp = ddb.FilterCond(left, op, right)
        assert FilterCondtmp._FilterCond__lhs == left
        assert FilterCondtmp._FilterCond__rhs == right
        assert FilterCondtmp._FilterCond__op == op
        assert str(FilterCondtmp) == "({} {} {})".format(str(left), str(op), str(right))

    @pytest.mark.parametrize('right', ["rightName"] + [ddb.FilterCond("tmpc", x, "tmpd") for x in operators],
                             ids=["rightName"] + ["R_" + str(x) for x in operators])
    @pytest.mark.parametrize('op', operators.keys(), ids=operators.keys())
    @pytest.mark.parametrize('left', ["leftName"] + [ddb.FilterCond("tmpa", x, "tmpb") for x in operators],
                             ids=["leftName"] + ["L_" + str(x) for x in operators])
    def test_TestFilterCond_op(self, left, right, op):
        if op == '%' and str(left) == 'leftName':
            pytest.skip("this op is not expected.")
        if (type(left) == str) and (type(right) == str):
            pytest.skip()
        func = operators[op]
        res = func(left, right)
        if type(left) == str and op in corresponds.keys():
            assert str(res) == "({} {} {})".format(str(right), str(corresponds[op]), str(left))
        else:
            assert str(res) == "({} {} {})".format(str(left), str(op), str(right))


class TestVector:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            self.conn.run(get_scripts("undefShare"))
            self.conn.run(scripts)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.run(get_scripts("undefShare"))
        cls.conn.run("""
            if(existsDatabase('{}'))
                dropDatabase('{}')
        """.format(PartitionedTable[0], PartitionedTable[0]))
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    @pytest.mark.parametrize('datas', [df_python, dict_python, dict2_python],
                             ids=["DF", "DICT", "DICT_List"])
    def test_Vector_init_bytable_bydataframe_bydict(self, datas):

        for data in datas:
            tmp = self.conn.table(data=data)
            if isinstance(data, pd.DataFrame):
                for vi, pi in zip(tmp.vecs, data):
                    # print(type(tmp.vecs[vi]))
                    assert tmp.vecs[vi]._Vector__name == pi
                    assert tmp.vecs[vi]._Vector__tableName == tmp.tableName()
                    assert not tmp.vecs[vi]._Vector__vec
                    assert tmp.vecs[vi]._Vector__session is self.conn
            elif isinstance(data, dict):
                for vi, pi in zip(tmp.vecs, data):
                    # print(type(tmp.vecs[vi]))
                    assert tmp.vecs[vi]._Vector__name == pi
                    assert tmp.vecs[vi]._Vector__tableName == tmp.tableName()
                    assert not tmp.vecs[vi]._Vector__vec
                    assert tmp.vecs[vi]._Vector__session is self.conn

    @pytest.mark.parametrize('python_data', [df_python, dict_python, dict2_python, SharedTable],
                             ids=["DF", "DICT", "DICT_List", "NAME"])
    def test_Vector_init(self, python_data):

        for data, name in zip(python_data, SharedTable):
            colNames = self.conn.run(get_scripts("colNames").format(tableName=name))
            for col in colNames:
                if isinstance(data, str):
                    vectmp = ddb.Vector(name=col, data=data, s=self.conn, tableName=name)
                    assert vectmp._Vector__name == col
                    assert vectmp._Vector__tableName == name
                    assert vectmp._Vector__session is self.conn
                    assert vectmp._Vector__vec == None
                elif isinstance(data, pd.DataFrame):
                    vectmp = ddb.Vector(name=col, data=data[col], s=self.conn, tableName=name)
                    assert vectmp._Vector__name == col
                    assert vectmp._Vector__tableName == name
                    assert vectmp._Vector__session is self.conn
                    assert_series_equal(vectmp._Vector__vec, pd.Series(data[col]))
                elif isinstance(data, dict):
                    vectmp = ddb.Vector(name=col, data=data[col], s=self.conn, tableName=name)
                    assert vectmp._Vector__name == col
                    assert vectmp._Vector__tableName == name
                    assert vectmp._Vector__session is self.conn
                    assert_series_equal(vectmp._Vector__vec, pd.Series(data[col]))

    @pytest.mark.parametrize('python_data', [df_python, dict_python, dict2_python, SharedTable],
                             ids=["DF", "DICT", "DICT_List", "NAME"])
    def test_Vector_name_tableName_str(self, python_data):

        for data, name in zip(python_data, SharedTable):
            colNames = self.conn.run(get_scripts("colNames").format(tableName=name))
            for col in colNames:
                vectmp = ddb.Vector(name=col, data=data, s=self.conn, tableName=name)
                assert vectmp.name() == vectmp._Vector__name
                assert vectmp.tableName() == vectmp._Vector__tableName
                assert str(vectmp) == col

    @pytest.mark.parametrize('python_data', [df_python, dict_python, dict2_python, NormalTable],
                             ids=["DF", "DICT", "DICT_List", "NAME"])
    @pytest.mark.parametrize('useCache', [True, False, None], ids=["T_Cache", "F_Cache", "N_Cache"])
    def test_Vector_as_series(self, python_data, useCache):
        self.conn.run(NormalTable[0])
        if len(python_data) == 3:
            python_data = python_data[1:]
        for data, name in zip(python_data, NormalTable[1:]):
            colNames = self.conn.run(get_scripts("colNames").format(tableName=name))
            for col in colNames:
                vectmp = ddb.Vector(name=col, data=data, s=self.conn, tableName=name)
                res = vectmp.as_series(useCache)
                if isinstance(data, str):
                    assert res.name == None
                    res.name = col
                    assert_series_equal(res, self.conn.run("select * from {}".format(data))[col])
                elif isinstance(data, dict):
                    assert_series_equal(res, pd.Series(data[col]))
                elif isinstance(data, pd.DataFrame):
                    assert res.name == None
                    res.name = col
                    assert_series_equal(res, pd.Series(data[col]))

    @pytest.mark.parametrize('python_data', [df_python, dict_python, dict2_python, NormalTable],
                             ids=["DF", "DICT", "DICT_List", "NAME"])
    def test_Vector_op(self, python_data):

        self.conn.run(NormalTable[0])
        if len(python_data) == 3:
            python_data = python_data[1:]
        for data, name in zip(python_data, NormalTable[1:]):
            colNames = self.conn.run(get_scripts("colNames").format(tableName=name))
            for col in colNames:
                vectmp = ddb.Vector(name=col, data=data, s=self.conn, tableName=name)


class TestCounter:
    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def test_Counter_init(self):
        counter = ddb.Counter()
        assert counter._Counter__value == 1

    def test_Counter_inc(self):
        counter1 = ddb.Counter()
        assert counter1._Counter__value == 1
        counter1.inc()
        assert counter1._Counter__value == 2
        for i in range(100):
            counter1.inc()
        assert counter1._Counter__value == 102

        counter2 = ddb.Counter()
        assert counter2._Counter__value == 1

    def test_Counter_dec(self):
        counter1 = ddb.Counter()
        assert counter1._Counter__value == 1
        counter1.dec()
        assert counter1._Counter__value == 0
        for i in range(50):
            counter1.dec()
        assert counter1._Counter__value == -50

    def test_Counter_val(self):
        counter1 = ddb.Counter()
        assert counter1.val() == counter1._Counter__value
        assert counter1.val() == 1
        counter1.inc()
        counter1.inc()
        assert counter1.val() == 3
        counter1.dec()
        assert counter1.val() == 2


class TestTable:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            self.conn.run(get_scripts("undefShare"))
            self.conn.run(scripts)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.run(get_scripts("undefShare"))
        cls.conn.run("""
            if(existsDatabase('{}'))
                dropDatabase('{}')
        """.format(PartitionedTable[0], PartitionedTable[0]))
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    @pytest.mark.parametrize('python_data', [df_python, dict_python, dict2_python], ids=["DF", "DICT", "DICT_List"])
    @pytest.mark.parametrize('tableName', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                           IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable"])
    def test_session_table_data_bydataframe_bydict(self, python_data, tableName):

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

    @pytest.mark.parametrize('tableName', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                           IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable"])
    def test_session_table_data_bystr(self, tableName):

        if len(tableName) == 3:
            self.conn.run(tableName[0])
            tableName = tableName[1:]
        for data, name in zip(df_python, tableName):
            tmp = self.conn.table(data=name)
            assert_frame_equal(tmp.toDF(), data)

    @pytest.mark.parametrize('dbPath', [PartitionedTable[0]], ids=["T_Path"])
    def test_session_table_dbPath_data(self, dbPath):

        for name in PartitionedTable[3:]:
            tmp = self.conn.table(dbPath, name)
            res = self.conn.run('select * from loadTable("{}", `{})'.format(dbPath, name))
            assert_frame_equal(tmp.toDF(), res)

    @pytest.mark.parametrize('python_data', [df_python, dict_python, dict2_python], ids=["DF", "DICT", "DICT_List"])
    def test_session_table_tableAliasName_data_bydataframe_bydict(self, python_data):

        for data, name in zip(python_data, SharedTable):
            alias = generate_uuid("tmp_")
            tmp = self.conn.table(data=data, tableAliasName=alias)
            res = self.conn.run(get_scripts("checkEqual").format(tableA=alias, tableB=name))
            assert_equal(res, [True, True, True, True, True])
            if type(data) == pd.DataFrame:
                assert_frame_equal(tmp.toDF(), data)
                assert_frame_equal(self.conn.run("{}".format(alias)), data)

    @pytest.mark.parametrize('python_data', [df_python], ids=["DF"])
    @pytest.mark.parametrize('tableName', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                           IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable"])
    def test_session_table_tableAliasName_data_bystr(self, python_data, tableName):

        if len(tableName) == 3:
            self.conn.run(tableName[0])
            tableName = tableName[1:]
        for data, name in zip(python_data, tableName):
            alias = generate_uuid("tmp_")
            tmp = self.conn.table(data=name, tableAliasName=alias)
            assert_frame_equal(tmp.toDF(), data)
            with pytest.raises(RuntimeError) as e:
                assert "Syntax Error" in self.conn.run("{}".format(alias))

    def test_session_table_tableAliasName_dbPath_data(self):

        for name in PartitionedTable[3:]:
            alias = generate_uuid("tmp_")
            tmp = self.conn.table(PartitionedTable[0], name, tableAliasName=alias)
            res = self.conn.run('select * from loadTable("{}", `{})'.format(PartitionedTable[0], name))
            assert_frame_equal(tmp.toDF(), res)

    # TODO: need to raise DeprecationWarning
    @pytest.mark.skip(reason="inMem will be deprecated in a future version.")
    def test_session_table_inMem(self):
        pass

    # TODO: need to raise DeprecationWarning because Local Disk Database will be deprecated in the future
    @pytest.mark.skip(reason="partitions will be deprecated in a future version.")
    def test_session_table_partitions(self, colName):
        pass

    @pytest.mark.parametrize('file', [loadText, loadText2, ], ids=["csv_,", "csv_$"])
    def test_session_loadText(self, file):

        for i, name in enumerate(file[2:]):
            tbtmp = self.conn.loadText(DATA_DIR + "/{}".format(name), file[1])
            re = tbtmp.toDF()
            ex = csv_df[i]
            assert_frame_equal(re, ex)

    @pytest.mark.parametrize('file', [ploadText, ploadText2, ], ids=["csv_,", "csv_$"])
    def test_session_ploadText(self, file):

        for i, name in enumerate(file[2:]):
            tbtmp = self.conn.ploadText(DATA_DIR + "/{}".format(name), file[1])
            re = tbtmp.toDF()
            ex = csv_df[i]
            assert_frame_equal(re, ex)

    @pytest.mark.parametrize('file', [loadTextEx, loadTextEx2, ], ids=["csv_,", "csv_$"])
    def test_session_loadTextEx(self, file):

        self.conn.run(file[1])
        for id, name in enumerate(file[4:6]):
            re = self.conn.loadTextEx(file[2], file[id + 7], [file[6]], DATA_DIR + "/{}".format(name), file[3])
            re = re.toDF().sort_values("index", ascending=True).reset_index(drop=True)
            ex = csv_df[id]
            assert_frame_equal(re, ex)

    # @pytest.mark.NOW
    def test_session_loadTable_bystr(self):

        self.conn.run(loadTable[1])
        for id, name in enumerate(loadTable[2:4]):
            re = self.conn.loadTable(name).toDF()
            ex = df_python[id]
            assert_frame_equal(re, ex)

    # @pytest.mark.NOW
    def test_session_loadTable_dbPath(self):

        for id, name in enumerate(loadPTable[2:4]):
            re = self.conn.loadTable(name, loadPTable[1]).toDF()
            re = re.sort_values("index", ascending=True).reset_index(drop=True)
            ex = df_python[id]
            assert_frame_equal(re, ex)

    # @pytest.mark.NOW
    def test_session_loadTableBySQL(self):

        for id, name in enumerate(loadTableBySQL[2:4]):
            re = self.conn.loadTableBySQL(name, loadTableBySQL[1], "select * from {}".format(name)).toDF()
            re = re.sort_values("index", ascending=True).reset_index(drop=True)
            ex = df_python[id]
            assert_frame_equal(re, ex)

    @pytest.mark.parametrize('python_data', [df_python], ids=["DF"])
    @pytest.mark.parametrize('needGC', [True, False], ids=["T_GC", "F_GC"])
    def test_table_init_needGC(self, python_data, needGC):

        for data in python_data:
            tmp = ddb.Table(data=data, needGC=needGC, s=self.conn)
            tableName = tmp._getTableName()
            del tmp
            res = self.conn.run(get_scripts("objExist").format(tableName=tableName))
            assert res != needGC

    @pytest.mark.skip(reason="this param is not used for user")
    def test_table_init_schemaInited(self):
        pass

    # TODO: bug: copy.deepcopy(t)
    @pytest.mark.skip(reason="this method run error")
    def test_table_deepcopy(self):
        pass

    @pytest.mark.parametrize('python_data', [df_python], ids=["DF"])
    def test_table_copy(self, python_data):

        for data in python_data:
            tmpa = self.conn.table(data=data)
            assert tmpa._Table__ref.val() == 1
            tmpb = copy.copy(tmpa)
            assert_frame_equal(tmpa.toDF(), tmpb.toDF())
            assert tmpa._getTableName() == tmpb._getTableName()
            assert tmpa._Table__ref.val() == 2
            assert tmpb._Table__ref.val() == 2

    @pytest.mark.parametrize('python_data', [df_python], ids=["DF"])
    def test_table_del(self, python_data):

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

        (_, _), pythons_tmp = get_TableData(names="AllTrade", n=300000)
        pd_left_tmp, pd_right_tmp = pythons_tmp["DataFrame"]

        tmpa = self.conn.table(data=pd_left_tmp)
        stra = tmpa.tableName()
        assert self.conn.run(get_scripts("objCounts").format(name=stra)) == 1

        tmpb = self.conn.table(data=pd_right_tmp)
        strb = tmpb.tableName()
        assert self.conn.run(get_scripts("objCounts").format(name=strb)) == 1
        del tmpb
        assert self.conn.run(get_scripts("objCounts").format(name=strb)) == 0

    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_getattr(self, data):

        def test_getattr(conntmp: ddb.session, tabletmp: ddb.Table):
            cols = conntmp.run(get_scripts("colNames").format(tableName=tabletmp.tableName()))
            for col in cols:
                # res = compile("tabletmp.{}".format(col), "", 'eval')
                assert getattr(tabletmp, col) is tabletmp.vecs[col]
            assert getattr(tabletmp, "tableName") == tabletmp.tableName

        eval_table_with_data(self.conn, data, test_getattr)

    # TODO: this fucntion is not easy to test
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_getitem(self, data):

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

    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_property(self, data):

        def test_get_proprety(conntmp: ddb.session, tabletmp: ddb.Table):
            # tableName
            assert tabletmp.tableName() == tabletmp._Table__tableName
            assert tabletmp._getTableName() == tabletmp._Table__tableName
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

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_toDataFrame(self, data):

        def test_toDataFrame(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            assert_frame_equal(tb1.toDataFrame(), tb1.toDF())
            assert tb1.toDataFrame.__code__.co_code == tb1.toDF.__code__.co_code

        eval_sql_with_data(self.conn, data, test_toDataFrame)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_setMergeForUpdate(self, data):

        def test_setMergeForUpdate(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            assert tb1._Table__merge_for_update == False

            tb1.setMergeForUpdate(True)
            assert tb1._Table__merge_for_update == True

            tb1.setMergeForUpdate(False)
            assert tb1._Table__merge_for_update == False

        eval_sql_with_data(self.conn, data, test_setMergeForUpdate)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_sql_select_where(self, data):

        def test_select_where(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            # select index,ask from t
            tmp1 = copy.copy(tb1)
            re = tmp1.select(["index", "price"])
            sql = re.showSQL()
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

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('select_data', ["index", ("index"), ("index", "symbol"), ["index"], ["index", "symbol"]])
    def test_table_select(self, data, select_data):

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

        eval_sql_with_data(self.conn, data, test_select, select_data)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_where(self, data):

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

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_top(self, data):

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

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('limit', [5, (5), (5, 18), [5], [5, 18], "5", ("5"), ("5", "18"), ["5"], ["5", "18"]])
    def test_table_limit(self, data, limit):

        def test_limit(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                       partitioned: bool = False):
            # select index from t
            re = tb1.limit(limit).toDF()
            if (isinstance(limit, list) or isinstance(limit, tuple)):
                if len(limit) == 2:
                    df = df1.copy()[atoi(str(limit[0])):atoi(str(limit[0])) + atoi(str(limit[1]))].reset_index(
                        drop=True)
                else:
                    df = df1.copy()[:atoi(str(limit[0]))].reset_index(drop=True)
            else:
                df = df1.copy()[:atoi(str(limit))].reset_index(drop=True)
            re = re.reset_index(drop=True)
            assert_frame_equal(re, df)

        eval_sql_with_data(self.conn, data, test_limit)

    # @pytest.mark.NOW
    # @pytest.mark.skip(reason="This case has many bugs.[300 failed]")
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('bys',
                             ["index", ("index"), ["index"], ("size", "price"), ["size", "price"], ("price", "size"),
                              ["price", "size"]])
    @pytest.mark.parametrize('ascending', [True, False, (True), (False), [True], [False], (True, False), [True, False]])
    def test_table_sort(self, data, bys, ascending):

        def test_sort(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame, bys,
                      ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError) as e:
                    re = tb1.sort(bys, ascending=ascending)
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError) as e:
                    re = tb1.sort(bys, ascending=ascending)
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

        eval_sql_with_data(self.conn, data, test_sort, bys, ascending)

    # @pytest.mark.NOW
    # @pytest.mark.skip(reason="This case has many bugs.[300 failed]")
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('bys',
                             ["index", ("index"), ["index"], ("size", "price"), ["size", "price"], ("price", "size"),
                              ["price", "size"]])
    @pytest.mark.parametrize('ascending', [True, False, (True), (False), [True], [False], (True, False), [True, False]])
    def test_table_csort_contextby(self, data, bys, ascending):

        def test_csort_contextby(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, bys, ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError) as e:
                    re = tb1.top(2).csort(bys, ascending=ascending).contextby("symbol")
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError) as e:
                    re = tb1.top(2).csort(bys, ascending=ascending).contextby("symbol")
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
            _csort = [by + " desc" if asc == False else by for by, asc in zip(bys, ascending)]
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

        eval_sql_with_data(self.conn, data, test_csort_contextby, bys, ascending)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('on', [["symbol", None, None],
                                    [["symbol"], None, None],
                                    [["symbol", "time"], None, None],
                                    [None, ["symbol", "size"], ["symbol", "ask"]],
                                    [None, ("symbol", "size"), ("symbol", "ask")],
                                    [None, None, None],
                                    [None, ["symbol"], ["symbol", "ask"]],
                                    ])
    @pytest.mark.parametrize('how',
                             [("left", "lj"), ("right", "lj"), ("inner", "ej"), ("outer", "fj"), ("left semi", "lsj")],
                             ids=["left", "right", "inner", "outer", "left semi"])
    def test_table_merge_without_select(self, data, on, how):

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
                    re = copy.copy(tb1).merge(tb2, how=how[0], on=on[0], left_on=on[1], right_on=on[2])
                return
            if on_1 is not None and on_2 is not None and len(on_1) != len(on_2):
                with pytest.raises(Exception):
                    re = copy.copy(tb1).merge(tb2, how=how[0], on=on[0], left_on=on[1], right_on=on[2])
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

        eval_sql_with_data(self.conn, data, test_merge_without_select, on, how)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('on', [["time", None, None],
                                    [["time"], None, None],
                                    [["symbol", "time"], None, None],
                                    [None, ["symbol", "size"], ["symbol", "ask"]],
                                    [None, ("symbol", "size"), ("symbol", "ask")],
                                    [None, None, None],
                                    [None, ["symbol"], ["symbol", "ask"]],
                                    ])
    def test_table_merge_asof_without_select(self, data, on):

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
                    re = copy.copy(tb1).merge_asof(tb2, on=on[0], left_on=on[1], right_on=on[2])
                return
            if on_1 is not None and on_2 is not None and len(on_1) != len(on_2):
                with pytest.raises(Exception):
                    re = copy.copy(tb1).merge_asof(tb2, on=on[0], left_on=on[1], right_on=on[2])
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

        eval_sql_with_data(self.conn, data, test_merge_asof_without_select, on)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('on', [["time", None, None],
                                    [["time"], None, None],
                                    [["symbol", "time"], None, None],
                                    [None, ["symbol", "size"], ["symbol", "ask"]],
                                    [None, ("symbol", "size"), ("symbol", "ask")],
                                    [None, None, None],
                                    [None, ["symbol"], ["symbol", "ask"]],
                                    ])
    @pytest.mark.parametrize('bound', [(0, 0), (-15, 0), (0, 15), (-15, 15), (-20, -10), (10, 20)])
    @pytest.mark.parametrize('func', [
        "avg(bid)", ["avg(bid)", "wavg(ask, bid)"]
    ])
    def test_table_merge_window_without_select(self, data, on, bound, func):

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
                    re = copy.copy(tb1).merge_window(tb2, bound[0], bound[1], func, on=on[0], left_on=on[1],
                                                     right_on=on[2])
                return
            if on_1 is not None and on_2 is not None and len(on_1) != len(on_2):
                with pytest.raises(Exception):
                    re = copy.copy(tb1).merge_window(tb2, bound[0], bound[1], func, on=on[0], left_on=on[1],
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

        eval_sql_with_data(self.conn, data, test_merge_window_without_select, on, bound, func)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_merge_cross_without_select(self, data):

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

    # TODO: add more case
    # @pytest.mark.NOW
    def test_table_ols(self):

        self.conn.run(get_scripts("clearDatabase").format(dbPath="dfs://test_table_ols"))
        self.conn.run("""
                mydb = database("dfs://valuedb", VALUE, `AMZN`NFLX`NVDA)
                loadTextEx(mydb, `trade, `TICKER, "{}/example.csv")
        """.format(DATA_DIR))

        trade = self.conn.loadTable(tableName="trade", dbPath="dfs://valuedb")
        z = trade.ols(Y='PRC', X=['BID'], INTERCEPT=True)
        re = z["Coefficient"]
        prc_tmp = trade.toDF().PRC
        bid_tmp = trade.toDF().PRC
        model = sm.OLS(prc_tmp, bid_tmp)
        ex = model.fit().params
        assert_almost_equal(re.iloc[1, 1], ex[0], decimal=4)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_rename(self, data):

        def test_rename(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                        partitioned: bool = False):
            new_name = generate_uuid("rename_")
            tb1.rename(new_name)
            assert tb1._Table__tableName == new_name
            assert tb1.tableName() == new_name
            re = tb1.toDF()
            assert len(re) == len(df1)
            assert_frame_equal(re, df1)

        eval_sql_with_data(self.conn, data, test_rename)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('cols', [
        "*", "index", ["time"], ["index", "time"], ("time"), ("index", "time"),
    ])
    def test_table_exec(self, data, cols):

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

        eval_sql_with_data(self.conn, data, test_exec, cols)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('expr', [
        None, "*", "index", ["time"], ["index", "time"], ("time"), ("index", "time"),
    ])
    def test_table_execute(self, data, expr):

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

        eval_sql_with_data(self.conn, data, test_execute, expr)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_executeAs(self, data):

        def test_executeAs(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                           partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            re = conntmp.run(get_scripts("objExist").format(tableName=new_name))
            assert re == False

            tbtmp = tb1.executeAs(new_name)

            re = conntmp.run(get_scripts("objExist").format(tableName=new_name))
            assert re == True
            assert tbtmp is not tb1
            assert tbtmp._Table__tableName == new_name
            assert tb1._Table__tableName != new_name
            assert_frame_equal(tbtmp.toDF(), tb1.toDF())

        eval_sql_with_data(self.conn, data, test_executeAs)

    # @pytest.mark.NOW
    # require separate testing drop PartitionedTable cols
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List"])
    @pytest.mark.parametrize('cols', [
        None, "time", ["time"], ["time", "symbol"], []
    ])
    def test_table_drop(self, data, cols):

        def test_drop(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame, cols,
                      partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            conntmp.run("""
                {} = {}
            """.format(new_name, tb1._Table__tableName))
            tbtmp = conntmp.table(data=new_name)

            if cols is None:
                with pytest.raises(Exception) as e:
                    tbtmp.drop(cols)
                return

            if isinstance(cols, str):
                tbtmp.drop(cols)
                re = conntmp.run(get_scripts("tableCols").format(tableName=new_name))
                ex = conntmp.run(get_scripts("tableCols").format(tableName=tb1._Table__tableName)) - 1
                assert re == ex
            elif isinstance(cols, list):
                if len(cols):
                    tbtmp.drop(cols)
                    re = conntmp.run(get_scripts("tableCols").format(tableName=new_name))
                    ex = conntmp.run(get_scripts("tableCols").format(tableName=tb1._Table__tableName)) - len(cols)
                    assert re == ex
                else:
                    tbtmp.drop(cols)
                    return
            else:
                raise RuntimeError("no valid data for param: cols.")

        eval_sql_with_data(self.conn, data, test_drop, cols)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_append(self, data):

        def test_append(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                        partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            conntmp.run("""
                {} = {}
            """.format(new_name, tb1._Table__tableName))
            tbtmp = conntmp.table(data=new_name)

            data_name = generate_uuid("tmp_")
            conntmp.run("""
                {} = select * from {}
            """.format(data_name, tb1._Table__tableName))
            tbdata = conntmp.table(data=data_name)

            assert len(tbtmp.toDF()) == len(tbdata.toDF())
            length = len(tbdata.toDF())

            with pytest.raises(RuntimeError) as e:
                tbtmp.append(df1)

            tbtmp.append(tbdata)
            assert len(tbtmp.toDF()) == 2 * length

        eval_sql_with_data(self.conn, data, test_append)

    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_with_many_where(self, data):
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
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            self.conn.run(get_scripts("undefShare"))
            self.conn.run(scripts)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.run(get_scripts("undefShare"))
        cls.conn.run("""
            if(existsDatabase('{}'))
                dropDatabase('{}')
        """.format(PartitionedTable[0], PartitionedTable[0]))
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_delete_init(self, flushed_table_n100):

        n = 100

        def test_delete_init(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            assert tb1._Table__ref.val() == 1
            tbtmp = tb1.delete()
            assert tbtmp._TableDelete__t._Table__tableName == tb1._Table__tableName
            assert tb1._Table__ref.val() == 2
            assert tbtmp._TableDelete__t._Table__ref.val() == 2
            assert tb1._Table__ref is tbtmp._TableDelete__t._Table__ref

        eval_sql_with_data(self.conn, flushed_table_n100, test_delete_init)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_delete(self, flushed_table_n100):

        n = 100

        def test_delete(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                        partitioned: bool = False):
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1._Table__tableName)) == n
            tb1.delete()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1._Table__tableName)) == n
            tb1.delete().execute()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1._Table__tableName)) == 0

            assert conntmp.run(get_scripts("tableRows").format(tableName=tb2._Table__tableName)) == n
            tbtmp = tb2.delete()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp._TableDelete__t._Table__tableName)) == n
            tbtmp = tb2.delete().execute()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp._Table__tableName)) == 0

        eval_sql_with_data(self.conn, flushed_table_n100, test_delete)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_showSQL(self, flushed_table_n100):

        n = 100

        def test_showSQL(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp = tb1.delete()
            assert tbtmp.showSQL() == tbtmp._TableDelete__t.showSQL()

            def execute(tabletmp):
                assert tabletmp.showSQL() == "delete from {table}".format(
                    table=tabletmp._TableDelete__t._Table__tableName)

            tbtmp = tb1.delete()
            execute(tbtmp)

            def print(tabletmp):
                assert tabletmp.showSQL() == "delete from {table}".format(
                    table=tabletmp._TableDelete__t._Table__tableName)

            tbtmp = tb1.delete()
            print(tbtmp)

            def str(tabletmp):
                assert tabletmp.showSQL() == "delete from {table}".format(
                    table=tabletmp._TableDelete__t._Table__tableName)

            tbtmp = tb1.delete()
            str(tbtmp)

            # TODO: showSQL in '<module>'

        eval_sql_with_data(self.conn, flushed_table_n100, test_showSQL)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_delete_where(self, flushed_table_n100):

        n = 100

        def test_delete_where(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1._Table__tableName)) == n
            tbtmp1 = tb1.delete().where("index < 5")
            sql = tbtmp1._TableDelete__executesql()
            assert "where index < 5" in sql
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1._TableDelete__t._Table__tableName)) == n
            tbtmp1 = tb1.delete().where("index < 5").execute()
            re = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1._Table__tableName))
            assert re == n - 4
            tbtmp1_df = conntmp.table(data=df1)
            conntmp.run("delete from {} where index < 5".format(tbtmp1_df._Table__tableName))
            ex = conntmp.run("{}".format(tbtmp1_df._Table__tableName))
            assert_frame_equal(tbtmp1.toDF(), ex)

            assert conntmp.run(get_scripts("tableRows").format(tableName=tb2._Table__tableName)) == n
            tbtmp2 = tb2.delete().where("index > 50").where("symbol==`X").where("ask <= 100").execute()
            sql = tb2.delete().where("index > 50").where("symbol==`X").where("ask <= 100")._TableDelete__executesql()
            assert "where (index > 50) and (symbol==`X) and (ask <= 100)" in sql
            re = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp2._Table__tableName))
            tbtmp2_df = conntmp.table(data=df2)
            conntmp.run(
                "delete from {} where index > 50 and symbol == `X and ask <= 100".format(tbtmp2_df._Table__tableName))
            ex = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp2_df._Table__tableName))
            assert re == ex

        eval_sql_with_data(self.conn, flushed_table_n100, test_delete_where)

    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_delete_many_where(self, flushed_table_n100):

        n = 100

        def test_delete_where(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1._Table__tableName)) == n
            tbtmp1 = tb1.delete().where("index < 5").where("price > 3")
            sql = tbtmp1._TableDelete__executesql()
            assert "where (index < 5) and (price > 3)" in sql
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1._TableDelete__t._Table__tableName)) == n
            tbtmp1 = tb1.delete().where("index < 5").where("price > 3").execute()
            re = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1._Table__tableName))
            assert re == n - 3
            tbtmp1_df = conntmp.table(data=df1)
            conntmp.run("delete from {} where (index < 5) and (price > 3)".format(tbtmp1_df._Table__tableName))
            ex = conntmp.run("{}".format(tbtmp1_df._Table__tableName))
            assert_frame_equal(tbtmp1.toDF(), ex)

            assert conntmp.run(get_scripts("tableRows").format(tableName=tb2._Table__tableName)) == n
            tbtmp2 = tb2.delete().where("index > 50").where("symbol==`X or symbol==`Y").where(
                "ask <= 100 and ask > 50").execute()
            sql = tb2.delete().where("index > 50").where("symbol==`X or symbol==`Y").where(
                "ask <= 100 and ask > 50")._TableDelete__executesql()
            assert "where (index > 50) and (symbol==`X or symbol==`Y) and (ask <= 100 and ask > 50)" in sql
            re = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp2._Table__tableName))
            tbtmp2_df = conntmp.table(data=df2)
            conntmp.run(
                "delete from {} where (index > 50) and (symbol==`X or symbol == `Y) and (ask <= 100 and ask > 50)".format(
                    tbtmp2_df._Table__tableName))
            ex = conntmp.run(get_scripts("tableRows").format(tableName=tbtmp2_df._Table__tableName))
            assert re == ex

        eval_sql_with_data(self.conn, flushed_table_n100, test_delete_where)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_toDF(self, flushed_table_n100):

        n = 100

        def test_toDF(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                      partitioned: bool = False):
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1._Table__tableName)) == n
            tbtmp1 = tb1.delete().where("index < 5")
            assert conntmp.run(get_scripts("tableRows").format(tableName=tbtmp1._TableDelete__t._Table__tableName)) == n
            re = tb1.delete().where("index < 5").toDF()
            assert conntmp.run(get_scripts("tableRows").format(tableName=tb1._Table__tableName)) == n

        eval_sql_with_data(self.conn, flushed_table_n100, test_toDF)


class TestTableUpdate:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            self.conn.run(get_scripts("undefShare"))
            self.conn.run(scripts)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.run(get_scripts("undefShare"))
        cls.conn.run("""
            if(existsDatabase('{}'))
                dropDatabase('{}')
        """.format(PartitionedTable[0], PartitionedTable[0]))
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_update_without_merge(self, flushed_table_n100):

        n = 100

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

        eval_sql_with_data(self.conn, flushed_table_n100, test_update_without_merge)

    # @pytest.mark.NOW
    # TODO: bug fix: not copy leftTableName
    # @pytest.mark.skip(reason="Too many bugs for this case.")
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    @pytest.mark.parametrize('on', [
        ["symbol", None, None],
        [["symbol"], None, None],
        [["symbol", "time"], None, None],
        [None, ["symbol", "size"], ["symbol", "ask"]],
        [None, ("symbol", "size"), ("symbol", "ask")],
    ])
    @pytest.mark.parametrize('how',
                             [("left", "lj"), ("right", "lj"), ("inner", "ej"), ("outer", "fj"), ("left semi", "lsj")],
                             ids=["left", "right", "inner", "outer", "left semi"])
    def test_table_update_merge(self, flushed_table_n100, on, how):

        if how[0] in ['right', 'outer']:
            pytest.skip("unsupported right or outer join")

        n = 100

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
                    df = conntmp.run(
                        'update {} set price=price*ask from {}({}, {}, {})'.format(tb1_df.tableName(), how[1],
                                                                                   tb2_df.tableName(),
                                                                                   tb1_df.tableName(), on_0_str))
                else:
                    df = conntmp.run(
                        'update {} set price=price*ask from {}({}, {}, {})'.format(tb1_df.tableName(), how[1],
                                                                                   tb1_df.tableName(),
                                                                                   tb2_df.tableName(), on_0_str))
            else:
                if how[0] == "right":
                    df = conntmp.run(
                        'update {} set price=price*ask from {}({}, {}, {}, {})'.format(tb1_df.tableName(), how[1],
                                                                                       tb2_df.tableName(),
                                                                                       tb1_df.tableName(), on_2_str,
                                                                                       on_1_str))
                else:
                    df = conntmp.run(
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

        eval_sql_with_data(self.conn, flushed_table_n100, test_update_merge, on, how)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_update_where(self, flushed_table_n100):

        n = 100

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

        eval_sql_with_data(self.conn, flushed_table_n100, test_update_where)

    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_update_many_where(self, flushed_table_n100):
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

        eval_sql_with_data(self.conn, flushed_table_n100, test_update_where)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_showSQL(self, flushed_table_n100):

        n = 100

        def test_showSQL(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp = tb1.update(["price"], ["price+5"])
            assert tbtmp.showSQL() == tbtmp._TableUpdate__t.showSQL()

            def execute(tabletmp):
                assert tabletmp.showSQL() == "update {table} set price=price+5".format(
                    table=tabletmp._TableUpdate__t._Table__tableName)

            tbtmp = tb1.update(["price"], ["price+5"])
            execute(tbtmp)

            def print(tabletmp):
                assert tabletmp.showSQL() == "update {table} set price=price+5".format(
                    table=tabletmp._TableUpdate__t._Table__tableName)

            tbtmp = tb1.update(["price"], ["price+5"])
            print(tbtmp)

            def str(tabletmp):
                assert tabletmp.showSQL() == "update {table} set price=price+5".format(
                    table=tabletmp._TableUpdate__t._Table__tableName)

            tbtmp = tb1.update(["price"], ["price+5"])
            str(tbtmp)

            # TODO: showSQL in '<module>'

        eval_sql_with_data(self.conn, flushed_table_n100, test_showSQL)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_toDF(self, flushed_table_n100):

        n = 100

        def test_toDF(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                      partitioned: bool = False):
            assert_frame_equal(tb1.toDF(), df1)

            tbtmp1 = tb1.update(["price"], ["price+10"])
            re = conntmp.run("select * from {}".format(tbtmp1._TableUpdate__t._Table__tableName))
            assert_frame_equal(re, df1)

            re = tb1.update(["price"], ["price+10"]).toDF()
            assert_frame_equal(re, df1)

        eval_sql_with_data(self.conn, flushed_table_n100, test_toDF)


class TestTablePivotBy:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            self.conn.run(get_scripts("undefShare"))
            self.conn.run(scripts)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.run(get_scripts("undefShare"))
        cls.conn.run("""
            if(existsDatabase('{}'))
                dropDatabase('{}')
        """.format(PartitionedTable[0], PartitionedTable[0]))
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_pivotby(self, flushed_table_n100):

        n = 100

        def test_pivotby(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp = tb1.pivotby("index", "symbol", value="size", aggFunc=sum)
            assert tbtmp._TablePivotBy__row == "index"
            assert tbtmp._TablePivotBy__column == "symbol"
            assert tbtmp._TablePivotBy__val == "size"
            assert tbtmp._TablePivotBy__agg is sum
            assert tbtmp._TablePivotBy__t._Table__tableName == tb1._Table__tableName

        eval_sql_with_data(self.conn, flushed_table_n100, test_pivotby)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_pivotby_isExec(self, flushed_table_n100):

        n = 100

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

        eval_sql_with_data(self.conn, flushed_table_n100, test_pivotby_isExec)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_pivotby_value_aggFunc(self, flushed_table_n100):

        n = 100

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

        eval_sql_with_data(self.conn, flushed_table_n100, test_pivotby_value_aggFunc)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_pivotby_executeAs(self, flushed_table_n100):

        n = 100

        def test_pivotby_executeAs(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            tbtmp1 = tb1.pivotby("price", "symbol", "index,time,size").executeAs(new_name)
            re = conntmp.run("select * from {}".format(new_name))
            ex = conntmp.run("select index,time,size from {} pivot by price, symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_pivotby_executeAs)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_pivotby_selectAsVector(self, flushed_table_n100):

        n = 100

        def test_pivotby_selectAsVector(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                        df2: pd.DataFrame, partitioned: bool = False):
            re = tb1.pivotby("price", "symbol").selectAsVector("index")
            ex = conntmp.run("exec index from {} pivot by price, symbol".format(tb1.tableName()))
            assert_array_equal(re[0], ex[0])
            assert_array_equal(re[1], ex[1])
            assert_array_equal(re[2], ex[2])

            re = tb1.pivotby("price", "symbol").selectAsVector(("index"))
            ex = conntmp.run("exec index from {} pivot by price, symbol".format(tb1.tableName()))
            assert_array_equal(re[0], ex[0])
            assert_array_equal(re[1], ex[1])
            assert_array_equal(re[2], ex[2])

            re = tb1.pivotby("price", "symbol").selectAsVector(["index"])
            ex = conntmp.run("exec index from {} pivot by price, symbol".format(tb1.tableName()))
            assert_array_equal(re[0], ex[0])
            assert_array_equal(re[1], ex[1])
            assert_array_equal(re[2], ex[2])

        eval_sql_with_data(self.conn, flushed_table_n100, test_pivotby_selectAsVector)


class TestTableGroupby:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            self.conn.run(get_scripts("undefShare"))
            self.conn.run(scripts)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.run(get_scripts("undefShare"))
        cls.conn.run("""
            if(existsDatabase('{}'))
                dropDatabase('{}')
        """.format(PartitionedTable[0], PartitionedTable[0]))
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby(self, flushed_table_n100):

        n = 100

        def test_groupby(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                         partitioned: bool = False):
            tbtmp = copy.copy(tb1).groupby(["symbol"])
            assert tbtmp._TableGroupby__groupBys == ["symbol"]
            assert tbtmp._TableGroupby__having == None
            assert tbtmp._TableGroupby__t._Table__tableName == tb1._Table__tableName

            tbtmp = copy.copy(tb1).groupby("symbol")
            assert tbtmp._TableGroupby__groupBys == ["symbol"]
            assert tbtmp._TableGroupby__having == None
            assert tbtmp._TableGroupby__t._Table__tableName == tb1._Table__tableName

            tbtmp = copy.copy(tb1).groupby(["symbol", "size"])
            assert tbtmp._TableGroupby__groupBys == ["symbol", "size"]
            assert tbtmp._TableGroupby__having == None
            assert tbtmp._TableGroupby__t._Table__tableName == tb1._Table__tableName

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_executeAs(self, flushed_table_n100):

        n = 100

        def test_groupby_executeAs(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            tb1.select(["max(size)"]).groupby(["symbol"]).executeAs(new_name)
            re = conntmp.run("select * from {}".format(new_name))
            ex = conntmp.run("select max(size) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_executeAs)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_selectAsVector(self, flushed_table_n100):

        n = 100

        def test_groupby_selectAsVector(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                        df2: pd.DataFrame, partitioned: bool = False):
            re = tb1.groupby(["symbol"]).selectAsVector("max(index)")
            ex = conntmp.run("exec max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

            re = tb1.groupby(["symbol"]).selectAsVector(("max(index)"))
            ex = conntmp.run("exec max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

            re = tb1.groupby(["symbol"]).selectAsVector(["max(index)"])
            ex = conntmp.run("exec max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_selectAsVector)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby___getitem__(self, flushed_table_n100):

        n = 100

        def test_groupby___getitem__(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                     df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.groupby(["symbol"])["max(index)"]
            assert tbtmp1._TableGroupby__t._Table__select == ["max(index)"]
            re = tbtmp1.toDF()
            ex = conntmp.run("select max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby___getitem__)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby___next__(self, flushed_table_n100):

        n = 100

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

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby___next__)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_having(self, flushed_table_n100):

        n = 100

        def test_groupby_having(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select("max(index)").groupby(["symbol"]).having("first(size)>90")
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select max(index) from {} group by symbol having first(size)>90".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            ex = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_having)

    # @pytest.mark.NOW
    @pytest.mark.skip(reason="delete this func")
    def test_table_groupby_ols(self):
        pass

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    @pytest.mark.parametrize('aggcol, aggfunc, aggscript', [
        ['index', ['sum', 'max'], 'sum(index), max(index)'],
        ['price', ['max'], 'max(price)'],
        ['time', ['first'], 'first(time)'],
        ['size', ['avg', 'sum'], 'avg(size), sum(size)'],
    ])
    def test_table_groupby_agg(self, flushed_table_n100, aggcol, aggfunc, aggscript):
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
                tbtmp1 = tb1.select(["index"]).groupby("symbol").agg(("sum", "max"))
            assert str(e).find("invalid func format")

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_agg)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_sum(self, flushed_table_n100):

        n = 100

        def test_groupby_sum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").sum()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select sum(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_sum)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_avg(self, flushed_table_n100):

        n = 100

        def test_groupby_avg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").avg()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select avg(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_avg)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_count(self, flushed_table_n100):

        n = 100

        def test_groupby_count(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").count()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select count(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_count)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_max(self, flushed_table_n100):

        n = 100

        def test_groupby_max(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").max()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select max(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_max)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_min(self, flushed_table_n100):

        n = 100

        def test_groupby_min(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").min()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select min(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_min)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_first(self, flushed_table_n100):

        n = 100

        def test_groupby_first(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["time"]).groupby("symbol").first()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select first(time) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_first)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_last(self, flushed_table_n100):

        n = 100

        def test_groupby_last(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["time"]).groupby("symbol").last()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select last(time) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_last)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_size(self, flushed_table_n100):

        n = 100

        def test_groupby_size(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").size()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select size(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_size)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_sum2(self, flushed_table_n100):

        n = 100

        def test_groupby_sum2(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").sum2()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select sum2(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_sum2)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_std(self, flushed_table_n100):

        n = 100

        def test_groupby_std(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").std()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select std(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_std)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_var(self, flushed_table_n100):

        n = 100

        def test_groupby_var(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                             partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").var()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select var(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_var)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_prod(self, flushed_table_n100):

        n = 100

        def test_groupby_prod(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).groupby("symbol").prod()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select prod(index) from {} group by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_prod)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    @pytest.mark.parametrize('aggfunc, aggscript', [
        [[ddb.corr, ('ask', 'bid')], "corr(ask, bid)"],
        [['atImax', ('ask', 'bid')], "atImax(ask, bid)"],
        [['contextSum', ('ask', 'bid')], "contextSum(ask, bid)"],
        [[ddb.covar, ('ask', 'bid')], "covar(ask, bid)"],
        [[ddb.wavg, ('ask', 'bid')], "wavg(ask, bid)"],
        [[ddb.wsum, ('ask', 'bid')], "wsum(ask, bid)"],
        [[ddb.wsum, [('ask', 'bid'), ('bid', 'ask')]], "wsum(ask, bid), wsum(bid, ask)"],
    ])
    def test_table_groupby_agg2(self, flushed_table_n100, aggfunc, aggscript):

        n = 100

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
                tbtmp2 = tb2.groupby("symbol").agg2(ddb.wsum, [["ask", "bid"]])

            with pytest.raises(RuntimeError):
                tbtmp2 = tb2.groupby("symbol").agg2(ddb.wsum, [["ask", "bid", "index"]])

            with pytest.raises(RuntimeError):
                tbtmp2 = tb2.groupby("symbol").agg2(ddb.wsum, [("ask", "bid"), ["ask", "index"]])

            tbtmp2 = tb2.select([aggscript]).groupby("symbol").agg2([], [("ask", "bid")])
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select {} from {} group by symbol".format(aggscript, tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_agg2)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_wavg(self, flushed_table_n100):

        n = 100

        def test_groupby_wavg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.groupby("symbol").wavg(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select wavg(ask, bid) from {} group by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_wavg)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_wsum(self, flushed_table_n100):

        n = 100

        def test_groupby_wsum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.groupby("symbol").wsum(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select wsum(ask, bid) from {} group by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_wsum)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_covar(self, flushed_table_n100):

        n = 100

        def test_groupby_covar(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.groupby("symbol").covar(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select covar(ask, bid) from {} group by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_covar)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_groupby_corr(self, flushed_table_n100):

        n = 100

        def test_groupby_corr(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.groupby("symbol").corr(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select corr(ask, bid) from {} group by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_groupby_corr)

    # @pytest.mark.NOW
    # @pytest.mark.skip(reason="This case has many bugs.[300 failed]")
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('bys',
                             ["price", ("price"), ["price"], ("size", "price"), ["size", "price"], ("price", "size"),
                              ["price", "size"]])
    @pytest.mark.parametrize('ascending', [True, False, (True), (False), [True], [False], (True, False), [True, False]])
    def test_table_groupby_sort(self, data, bys, ascending):

        def test_groupby_sort(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                              df2: pd.DataFrame, bys, ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError) as e:
                    re = tb1.select(["sum(price)"]).groupby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError) as e:
                    re = tb1.select(["sum(price)"]).groupby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
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
            _sort = [by + " desc" if asc == False else by for by, asc in zip(bys, ascending)]
            ex = conntmp.run(
                "select sum(price) from {} group by symbol, size, price order by {}".format(tb1.tableName(),
                                                                                            ",".join(_sort)))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_groupby_sort, bys, ascending)

    @pytest.mark.skip(reason="DolphinDB not support group by + csort, this function will be deleted.")
    def test_table_groupby_csort(self, data, bys, ascending):
        pass


class TestTableContextby:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            self.conn.run(get_scripts("undefShare"))
            self.conn.run(scripts)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.run(get_scripts("undefShare"))
        cls.conn.run("""
            if(existsDatabase('{}'))
                dropDatabase('{}')
        """.format(PartitionedTable[0], PartitionedTable[0]))
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby(self, flushed_table_n100):

        n = 100

        def test_contextby(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame, df2: pd.DataFrame,
                           partitioned: bool = False):
            tbtmp = copy.copy(tb1).contextby(["symbol"])
            assert tbtmp._TableContextby__contextBys == ["symbol"]
            assert tbtmp._TableContextby__having == None
            assert tbtmp._TableContextby__t._Table__tableName == tb1._Table__tableName

            tbtmp = copy.copy(tb1).contextby("symbol")
            assert tbtmp._TableContextby__contextBys == ["symbol"]
            assert tbtmp._TableContextby__having == None
            assert tbtmp._TableContextby__t._Table__tableName == tb1._Table__tableName

            tbtmp = copy.copy(tb1).contextby(["symbol", "size"])
            assert tbtmp._TableContextby__contextBys == ["symbol", "size"]
            assert tbtmp._TableContextby__having == None
            assert tbtmp._TableContextby__t._Table__tableName == tb1._Table__tableName

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_executeAs(self, flushed_table_n100):

        n = 100

        def test_contextby_executeAs(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                     df2: pd.DataFrame, partitioned: bool = False):
            new_name = generate_uuid("tmp_")
            tb1.contextby(["symbol"]).executeAs(new_name)
            re = conntmp.run("select * from {}".format(new_name))
            ex = conntmp.run("select * from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_executeAs)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_selectAsVector(self, flushed_table_n100):

        n = 100

        def test_contextby_selectAsVector(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                          df2: pd.DataFrame, partitioned: bool = False):
            re = tb1.contextby(["symbol"]).selectAsVector("max(index)")
            ex = conntmp.run("exec max(index) from {} context by symbol".format(tb1.tableName()))
            assert_array_equal(re, ex)

            re = tb1.contextby(["symbol"]).selectAsVector(("max(index)"))
            ex = conntmp.run("exec max(index) from {} context by symbol".format(tb1.tableName()))
            assert_array_equal(re, ex)

            re = tb1.contextby(["symbol"]).selectAsVector(["max(index)"])
            ex = conntmp.run("exec max(index) from {} context by symbol".format(tb1.tableName()))
            assert_array_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_selectAsVector)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby___getitem__(self, flushed_table_n100):

        n = 100

        def test_contextby___getitem__(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                       df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.contextby(["symbol"])["index"]
            assert tbtmp1._TableContextby__t._Table__select == ["index"]
            re = tbtmp1.toDF()
            ex = conntmp.run("select index from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby___getitem__)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby___next__(self, flushed_table_n100):

        n = 100

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

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby___next__)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_having(self, flushed_table_n100):

        n = 100

        def test_contextby_having(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                  df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select("index").contextby(["symbol"]).having("first(size)>90")
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select index from {} context by symbol having first(size)>90".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            ex = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_having)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table",
        "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    @pytest.mark.parametrize('aggcol, aggfunc, aggscript', [
        ['index', ['sum', 'max'], 'sum(index), max(index)'],
        ['price', ['max'], 'max(price)'],
        ['time', ['first'], 'first(time)'],
        ['size', ['avg', 'sum'], 'avg(size), sum(size)'],
    ])
    def test_table_contextby_agg_list(self, flushed_table_n100, aggcol, aggfunc, aggscript):
        def test_contextby_agg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select([aggcol]).contextby("symbol").agg(aggfunc)
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol,{} from {} context by symbol".format(aggscript, tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_agg)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_agg_tuple(self, flushed_table_n100):

        n = 100

        def test_contextby_agg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            with pytest.raises(RuntimeError) as e:
                tbtmp1 = tb1.select(["index"]).contextby("symbol").agg(("sum", "max"))
            assert str(e).find("invalid func format")

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_agg)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    @pytest.mark.parametrize('aggcol, aggfunc, aggscript', [
        ['index', ['sum', 'max'], 'sum(index), max(index)'],
        ['price', ['max'], 'max(price)'],
        ['time', ['first'], 'first(time)'],
        ['size', ['avg', 'sum'], 'avg(size), sum(size)'],
    ])
    def test_table_contextby_agg_str(self, flushed_table_n100, aggcol, aggfunc, aggscript):
        def test_contextby_agg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select([aggcol]).contextby("symbol").agg(aggfunc)
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, {} from {} context by symbol".format(aggscript, tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_agg)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_agg_dict(self, flushed_table_n100):

        n = 100

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

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_agg)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_sum(self, flushed_table_n100):

        n = 100

        def test_contextby_sum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").sum()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, sum(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_sum)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_avg(self, flushed_table_n100):

        n = 100

        def test_contextby_avg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").avg()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, avg(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_avg)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_count(self, flushed_table_n100):

        n = 100

        def test_contextby_count(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").count()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, count(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_count)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_max(self, flushed_table_n100):

        n = 100

        def test_contextby_max(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").max()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, max(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_max)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_min(self, flushed_table_n100):

        n = 100

        def test_contextby_min(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").min()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, min(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_min)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_first(self, flushed_table_n100):

        n = 100

        def test_contextby_first(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["time"]).contextby("symbol").first()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, first(time) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_first)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_last(self, flushed_table_n100):

        n = 100

        def test_contextby_last(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["time"]).contextby("symbol").last()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, last(time) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_last)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_size(self, flushed_table_n100):

        n = 100

        def test_contextby_size(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").size()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, size(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_size)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_sum2(self, flushed_table_n100):

        n = 100

        def test_contextby_sum2(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").sum2()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, sum2(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_sum2)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_std(self, flushed_table_n100):

        n = 100

        def test_contextby_std(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").std()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, std(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_std)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_var(self, flushed_table_n100):

        n = 100

        def test_contextby_var(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                               df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").var()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, var(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_var)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_prod(self, flushed_table_n100):

        n = 100

        def test_contextby_prod(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").prod()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, prod(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_prod)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_cumsum(self, flushed_table_n100):

        n = 100

        def test_contextby_cumsum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                  df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").cumsum()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, cumsum(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_cumsum)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_cummax(self, flushed_table_n100):

        n = 100

        def test_contextby_cummax(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                  df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").cummax()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, cummax(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_cummax)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_cumprod(self, flushed_table_n100):

        n = 100

        def test_contextby_cumprod(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["price"]).contextby("symbol").cumprod()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, cumprod(price) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_cumprod)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_cummin(self, flushed_table_n100):

        n = 100

        def test_contextby_cummin(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                  df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.select(["index"]).contextby("symbol").cummin()
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select symbol, cummin(index) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_cummin)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    @pytest.mark.parametrize('aggfunc, aggscript', [
        [[ddb.corr, ('ask', 'bid')], "corr(ask, bid)"],
        [['atImax', ('ask', 'bid')], "atImax(ask, bid)"],
        [['contextSum', ('ask', 'bid')], "contextSum(ask, bid)"],
        [[ddb.covar, ('ask', 'bid')], "covar(ask, bid)"],
        [[ddb.wavg, ('ask', 'bid')], "wavg(ask, bid)"],
        [[ddb.wsum, ('ask', 'bid')], "wsum(ask, bid)"],
        [[ddb.wsum, [('ask', 'bid'), ('bid', 'ask')]], "wsum(ask, bid), wsum(bid, ask)"],
    ])
    def test_table_contextby_agg2(self, flushed_table_n100, aggfunc, aggscript):
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

            with pytest.raises(RuntimeError) as e:
                tbtmp2 = tb2.contextby("symbol").agg2(ddb.wsum, [["ask", "bid"]])

            with pytest.raises(RuntimeError) as e:
                tbtmp2 = tb2.contextby("symbol").agg2(ddb.wsum, [["ask", "bid", "index"]])

            with pytest.raises(RuntimeError) as e:
                tbtmp2 = tb2.contextby("symbol").agg2(ddb.wsum, [("ask", "bid"), ["ask", "index"]])

            tbtmp2 = tb2.select([aggscript]).contextby("symbol").agg2([], [("ask", "bid")])
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select {} from {} context by symbol".format(aggscript, tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_agg2)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_wavg(self, flushed_table_n100):

        n = 100

        def test_contextby_wavg(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.contextby("symbol").wavg(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select *, wavg(ask, bid) from {} context by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_wavg)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_wsum(self, flushed_table_n100):

        n = 100

        def test_contextby_wsum(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.contextby("symbol").wsum(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select *, wsum(ask, bid) from {} context by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_wsum)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_covar(self, flushed_table_n100):

        n = 100

        def test_contextby_covar(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, partitioned: bool = False):
            tbtmp2 = tb2.contextby("symbol").covar(("ask", "bid"))
            sqlstr = tbtmp2.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select *, covar(ask, bid) from {} context by symbol".format(tb2.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp2.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_covar)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_corr(self, flushed_table_n100):

        n = 100

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

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_corr)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('flushed_table_n100', [
        "Table", "STable", "indexTable", "SindexTable", "keyTable", "SkeyTable",
        "DF", "DICT", "DICT_List", "PTable",
    ], indirect=True)
    def test_table_contextby_eachPre(self, flushed_table_n100):

        n = 100

        def test_contextby_eachPre(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                   df2: pd.DataFrame, partitioned: bool = False):
            tbtmp1 = tb1.contextby("symbol").eachPre(("\\", "price"))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            ex = conntmp.run("select *,symbol, eachPre(\\, price) from {} context by symbol".format(tb1.tableName()))
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, flushed_table_n100, test_contextby_eachPre)

    # @pytest.mark.NOW
    # @pytest.mark.skip(reason="This case has many bugs.[300 failed]")
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('bys',
                             ["price", ("price"), ["price"], ("size", "price"), ["size", "price"], ("price", "size"),
                              ["price", "size"]])
    @pytest.mark.parametrize('ascending', [True, False, (True), (False), [True], [False], (True, False), [True, False]])
    def test_table_contextby_sort(self, data, bys, ascending):

        def test_contextby_sort(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                df2: pd.DataFrame, bys, ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError) as e:
                    re = tb1.contextby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError) as e:
                    re = tb1.contextby(["symbol", "size", "price"]).sort(bys, ascending=ascending)
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
            _csort = [by + " desc" if asc == False else by for by, asc in zip(bys, ascending)]
            ex = conntmp.run(
                "select * from {} context by symbol, size, price order by {}".format(tb1.tableName(), ",".join(_csort)))
            sqlstr = tbtmp1.showSQL()
            re = conntmp.run(sqlstr)
            assert_frame_equal(re, ex)
            re = tbtmp1.toDF()
            assert_frame_equal(re, ex)

        eval_sql_with_data(self.conn, data, test_contextby_sort, bys, ascending)

    # @pytest.mark.NOW
    # @pytest.mark.skip(reason="This case has many bugs.[300 failed]")
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('bys',
                             ["index", ("index"), ["index"], ("size", "price"), ["size", "price"], ("price", "size"),
                              ["price", "size"]])
    @pytest.mark.parametrize('ascending', [True, False, (True), (False), [True], [False], (True, False), [True, False]])
    def test_table_contextby_csort(self, data, bys, ascending):

        def test_contextby_csort(conntmp: ddb.session, tb1: ddb.Table, tb2: ddb.Table, df1: pd.DataFrame,
                                 df2: pd.DataFrame, bys, ascending, partitioned: bool = False):
            if (isinstance(bys, list) or isinstance(bys, tuple)) and (
                    isinstance(ascending, list) or isinstance(ascending, tuple)) and (len(bys) != len(ascending)):
                with pytest.raises(ValueError) as e:
                    re = tb1.top(2).contextby("symbol").csort(bys, ascending=ascending)
                return
            if isinstance(bys, str) and (isinstance(ascending, tuple) or isinstance(ascending, list)) and len(
                    ascending) != 1:
                with pytest.raises(ValueError) as e:
                    re = tb1.top(2).contextby("symbol").csort(bys, ascending=ascending)
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
            _csort = [by + " desc" if asc == False else by for by, asc in zip(bys, ascending)]
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

        eval_sql_with_data(self.conn, data, test_contextby_csort, bys, ascending)

    # @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    def test_table_contextby_top(self, data):

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

    @pytest.mark.NOW
    @pytest.mark.parametrize('data', [NormalTable, SharedTable, StreamTable, ShareStreamTable,
                                      IndexedTable, ShareIndexedTable, KeyedTable, ShareKeyedTable,
                                      df_python, dict_python, dict2_python, PartitionedTable],
                             ids=["Table", "STable", "streamTable", "SStreamTable",
                                  "indexTable", "SindexTable", "keyTable", "SkeyTable",
                                  "DF", "DICT", "DICT_List", "PTable"])
    @pytest.mark.parametrize('limit', [
        5, (5), [5], "5", ("5"), ["5"],
    ])
    def test_table_contextby_limit(self, data, limit):

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

        eval_sql_with_data(self.conn, data, test_limit)
