import pytest
from setup.prepare import get_Scalar, DATATYPE, TIMETYPE, generate_uuid, get_Vector, get_Matrix, get_Set, get_Dictionary, get_Pair, get_Table, get_ArrayVector, get_Table_arrayVetcor,get_PartitionedTable_Append_Upsert
from setup.settings import *
from setup.utils import get_pid

import dolphindb as ddb
import numpy as np
import pandas as pd
from numpy.testing import *
from pandas.testing import *
import dolphindb.settings as keys
import decimal
import random
PANDAS_VERSION=tuple(int(i) for i in pd.__version__.split('.'))

test_type = []
for x in DATATYPE:
    test_type.append({'type': x})

class TestUploadBasicDataTypes:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() +'\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    #TODO: some failed is about upload VOID [12 failed]
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Scalar(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Scalar(types=data_type, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_s_')
            conn.upload({upstr: p})
            res = conn.run("{} == ".format(upstr) + s)
            assert res == True
            res_type = conn.run("type({}) == ".format(upstr) + 'type(' + s + ')' )
            assert res_type == True
            res_typestr = conn.run("typestr({}) == ".format(upstr) + 'typestr(' + s + ')' )
            assert res_typestr == True
        conn.undefAll()
        conn.close()

    #TODO: failed is about EnableCompress [not stable]
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Vector(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Vector(types=data_type, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_v_')
            conn.upload({upstr: p})
            res = conn.run("eqObj({}, {})".format(upstr, s))
            assert res == True
            res_type = conn.run("type({}) == ".format(upstr) + 'type(' + s + ')' )
            assert res_type == True
            res_typestr = conn.run("typestr({}) == ".format(upstr) + 'typestr(' + s + ')' )
            assert res_typestr == True
        conn.undefAll()
        conn.close()

    #TODO: failed is about EnableCompress [not stable]
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Matrix(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Matrix(types=data_type, names="upload", r=10, c=10)
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_m_')
            conn.upload({upstr: p})
            res_value = conn.run("eqObj({}, ".format(upstr) + s + ')')
            assert res_value == True
            res_type = conn.run("type({}) == ".format(upstr) + 'type(' + s + ')' )
            assert res_type == True
            res_typestr = conn.run("typestr({}) == ".format(upstr) + 'typestr(' + s + ')' )
            assert res_typestr == True
        conn.undefAll()
        conn.close()

    #TODO: failed is about EnableCompress [not stable]
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Set(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Set(types=data_type, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_set_')
            conn.upload({upstr: p})
            res_value = conn.run("{} == ".format(upstr) + s)
            assert res_value == True
            res_type = conn.run("type({}) == ".format(upstr) + 'type(' + s + ')' )
            assert res_type == True
            res_typestr = conn.run("typestr({}) == ".format(upstr) + 'typestr(' + s + ')' )
            assert res_typestr == True
        conn.undefAll()
        conn.close()

    #TODO: failed is about EnableCompress [12 failed]
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_Dictionary_upload(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Dictionary(types=data_type, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_d_')
            conn.upload({upstr: p})
            conn.run('print '+s)
            conn.run('print ' + upstr)
            re1 = conn.run(f"""
                x=[]
                for (i in {upstr}.keys()){{ x.append!({upstr}[i]=={s}[i])}}
                for (i in {s}.keys()){{ x.append!({upstr}[i]=={s}[i])}};
                all(x)
            """)
            assert re1 == True
            res_type = conn.run("type({}) == ".format(upstr) + 'type(' + s + ')' )
            assert res_type == True
            res_typestr = conn.run("typestr({}) == ".format(upstr) + 'typestr(' + s + ')' )
            assert res_typestr == True
        conn.undefAll()
        conn.close()

    @pytest.mark.skip(reason="No way to upload Pair.")
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Pair(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Pair(types=data_type, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_s_')
            conn.upload({upstr: p})
            res_value = conn.run("{} == ".format(upstr) + s)
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('isShare', [False], ids=['unshare']) 
    @pytest.mark.parametrize('tabletype', ['table'], ids=['table'])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Table(self, tabletype, data_type, pickle, compress, isShare):
        tmp_s, tmp_p = get_Table(typeTable=tabletype, types=data_type, ishare = isShare, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_t_')
            conn.upload({upstr: p})
            res_value = conn.run("each(eqObj, {}.values(), {}.values())".format(upstr, s))
            conn.run('print '+upstr)
            conn.run('print ' + s)
            if data_type in [DATATYPE.DT_DATE, DATATYPE.DT_DATEHOUR,  DATATYPE.DT_MONTH] and p is not None:
                assert res_value.all() == False
            elif data_type in [DATATYPE.DT_DATETIME,DATATYPE.DT_TIMESTAMP] and p is not None and PANDAS_VERSION<(2,0,0):
                assert res_value.all() == False
            else:
                assert res_value.all() == True

            res_type = conn.run("type({}) == type({})".format(upstr, s))
            assert res_type == True

            res_typestr = conn.run("typestr({}) == typestr({})".format(upstr, s))
            assert res_typestr == True
            res_schema = conn.run("each(eqObj, schema({})['colDefs'].values(), schema({})['colDefs'].values())".format(upstr, s))
            if data_type in [ DATATYPE.DT_DATE, DATATYPE.DT_DATEHOUR, DATATYPE.DT_MONTH] and p is not None:
                assert res_schema.all() == False
            elif data_type in [DATATYPE.DT_DATETIME, DATATYPE.DT_TIMESTAMP] and p is not None and PANDAS_VERSION < (2, 0, 0):
                assert res_value.all() == False
            else:
                assert res_schema.all() == True
            #因为在上传时会把以上的数据类型都转换成nonetimestamp，会导致比较失败。
            #具体上传的方法请见链接https://dolphindb1.atlassian.net/browse/APY-436
        conn.undefAll()
        conn.close()

    #TODO: 96 faileds about DT_MONTH, which will be fixed at 1.30.20.1
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('isShare', [True, False], ids=["enshare", "unshare"])  
    @pytest.mark.parametrize('table_type', ["table", "streamTable", "indexedTable", "keyedTable"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_tableAppender_Table(self, table_type, data_type, pickle, compress, isShare):
        tmp_s, tmp_p = get_Table(typeTable=table_type, types=data_type, isShare = isShare, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = "test_" + s
            res_len_before = conn.run("size(select * from {}) == 0".format(upstr))
            assert res_len_before == True
            appender = ddb.tableAppender(tableName=upstr, ddbSession=conn)
            appender.append(p)
            res_value = conn.run("each(eqObj, (select * from {}).values(), (select * from {}).values())".format(upstr, s))
            assert res_value.all() == True
            res_len_after = conn.run("size(select * from {}) == size(select * from {})".format(upstr, s))
            assert res_len_after == True
            #因为在上传时会把以上的数据类型都转换成nonetimestamp，会导致比较失败。
            #具体上传的方法请见链接https://dolphindb1.atlassian.net/browse/APY-436
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('isShare', [True, False], ids=["enshare", "unshare"])  
    @pytest.mark.parametrize('table_type', ["indexedTable", "keyedTable"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_tableUpsert_Table(self, table_type, data_type, pickle, compress, isShare):
        tmp_s, tmp_p = get_Table(typeTable=table_type, types=data_type, isShare = isShare, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = "test_" + s 
            res_len_before = conn.run("size(select * from {}) == 0".format(upstr))
            assert res_len_before == True
            upsert = ddb.tableUpsert(tableName=upstr, ddbSession=conn)
            upsert.upsert(p)
            res_value = conn.run("each(eqObj, (select * from {}).values(), (select * from {}).values())".format(upstr, s))
            assert res_value.all() == True
            res_len_after = conn.run("size(select * from {}) == size(select * from {})".format(upstr, s))
            assert res_len_after == True
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_partitionedTable_TableAppender(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_PartitionedTable_Append_Upsert(types=data_type, names="upload",script_type=data_type)
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = "test_" + s 
            dbpath = 'dfs://' + s.split('_')[1]
            appender = ddb.TableAppender(dbPath=dbpath, tableName=upstr, ddbSession=conn)
            appender.append(p)
            res_value = conn.run("eqObj((select * from {}).values(), (select * from {}).values())".format(upstr, s))
            res_type = conn.run("type({}) == ".format(upstr) + 'type(' + s + ')' )
            if data_type in [DATATYPE.DT_TIMESTAMP, DATATYPE.DT_DATE, DATATYPE.DT_DATEHOUR, DATATYPE.DT_DATETIME, DATATYPE.DT_MONTH] and p is not None:
                continue
            #因为在上传时会把以上的数据类型都转换成nonetimestamp，会导致比较失败。
            #具体上传的方法请见链接https://dolphindb1.atlassian.net/browse/APY-436
            else:
                assert res_value == True
                assert res_type == True 
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('ignoreNull', [True, False], ids=['enignoreNull', 'unignoreNull'])  
    def test_partitionedTable_TableUpserter(self, data_type, pickle, compress, ignoreNull):
        tmp_s, tmp_p = get_PartitionedTable_Append_Upsert(types=data_type, names="upload",script_type=data_type)
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = "test_" + s 
            dbpath = 'dfs://' + s.split('_')[1]
            upsert = ddb.TableUpserter(dbPath=dbpath, tableName=upstr, ddbSession=conn, ignoreNull=ignoreNull,keyColNames=["index"])
            upsert.upsert(p)
            res_value = conn.run("eqObj((select * from {}).values(), (select * from {}).values())".format(upstr, s))
            res_type = conn.run("type({}) == ".format(upstr) + 'type(' + s + ')' )
            if data_type in [DATATYPE.DT_TIMESTAMP, DATATYPE.DT_DATE, DATATYPE.DT_DATEHOUR, DATATYPE.DT_DATETIME, DATATYPE.DT_MONTH] and p is not None:
                continue
            #因为在上传时会把以上的数据类型都转换成nonetimestamp，会导致比较失败。
            #具体上传的方法请见链接https://dolphindb1.atlassian.net/browse/APY-436
            else:
                assert res_value == True
                assert res_type == True 
        conn.undefAll()
        conn.close()

    test_dataArray = [
        ['DATE',{'a': np.array([0, None, 1000], dtype="datetime64[D]")}],
        ['MONTH',{'a': np.array([0, None, 1000], dtype="datetime64[M]")}],
        ['TIME',{'a': np.array([0, None, 1000], dtype="datetime64[ms]")}],
        ['MINUTE',{'a': np.array([0, None, 1000], dtype="datetime64[m]")}],
        ['SECOND',{'a': np.array([0, None, 1000], dtype="datetime64[s]")}],
        ['DATETIME',{'a': np.array([0, None, 1000], dtype="datetime64[s]")}],
        ['TIMESTAMP',{'a': np.array([0, None, 1000], dtype="datetime64[ms]")}],
        ['NANOTIME',{'a': np.array([0, None, 1000], dtype="datetime64[ns]")}],
        ['NANOTIMESTAMP',{'a': np.array([0, None, 1000], dtype="datetime64[ns]")}],
        ['DATEHOUR',{'a': np.array([0, None, 1000], dtype="datetime64[h]")}]
    ]

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type, np_dict', test_dataArray, ids=[x.name for x in TIMETYPE])
    def test_upload_numpyTimeTypesArray_Vector(self, pickle, data_type, np_dict, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, pickle = pickle, compress = compress)
        if data_type == 'TIME':
            data_type = 'TIMESTAMP'
        elif data_type == 'MINUTE' or data_type == 'SECOND':
            data_type = 'DATETIME'
        elif data_type == 'NANOTIME':
            data_type = 'NANOTIMESTAMP'
        if data_type == 'MONTH':
            ex_s = "v1 = array(MONTH, 0);v1.append!(1970.01M NULL 2053.05M)"""
        else:
            ex_s = f"""v1 = array({data_type}, 0);v1.append!(0 NULL 1000)"""
        conn.upload(np_dict)
        conn.run(ex_s)
        assert conn.run("eqObj(v1, a)")
        conn.undefAll()
        conn.close()

    test_dataArray = [
        [[None, None, None],[None, None, None], [None, None, None]],
        [[pd.NaT, None, None],['', None, None], [decimal.Decimal('NaN'),None,None]],
        [[np.nan, None, None],["", None, None], [np.nan,None,None]],
        [[None, pd.NaT, None],[None, '', None], [None, decimal.Decimal('NaN'), None]],
        [[None, np.nan, None],[None, "", None], [None, np.nan, None]],
        [[None, None, pd.NaT],[None, None, ''], [None, None, decimal.Decimal('NaN')]],
        [[None, None, np.nan],[None, None, ""], [None, None, np.nan]],
        [[None, np.nan, pd.NaT],[None, "", ''], [None, np.nan, pd.NaT]],
        [[None, pd.NaT, np.nan],[None, '', ""], [None, pd.NaT, np.nan]],
        [[pd.NaT, np.nan, None],['', '', None], [pd.NaT,np.nan,None]],
        [[np.nan, pd.NaT, None],["", "", None], [np.nan,pd.NaT,None]],
        [[pd.NaT, pd.NaT, pd.NaT],['', '', ''], [pd.NaT,pd.NaT,pd.NaT]],
        [[np.nan, np.nan, np.nan],["", "", ""], [np.nan,np.nan,np.nan]],
    ]

    @pytest.mark.parametrize('val, valstr, valdecimal', test_dataArray, ids=[str(x[0]) for x in test_dataArray])
    def test_upload_allNone_tables_with_numpyArray(self, val, valstr, valdecimal):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        df = pd.DataFrame({
                           'cbool': np.array(val, dtype='object'),
                           'cchar': np.array(val, dtype='object'),
                           'cshort': np.array(val, dtype='object'),
                           'cint': np.array(val, dtype='object'),
                           'clong': np.array(val, dtype='object'),
                           'cdate': np.array(val, dtype='object'),
                           'cmonth': np.array(val, dtype='object'),
                           'ctime': np.array(val, dtype='object'),
                           'cminute': np.array(val, dtype='object'),
                           'csecond': np.array(val, dtype='object'),
                           'cdatetime': np.array(val, dtype='object'),
                           'ctimestamp': np.array(val, dtype='object'),
                           'cnanotime': np.array(val, dtype='object'),
                           'cnanotimestamp': np.array(val, dtype='object'),
                           'cfloat': np.array(val, dtype='object'),
                           'cdouble': np.array(val, dtype='object'),
                           'csymbol': np.array(valstr, dtype='object'),
                           'cstring': np.array(valstr, dtype='object'),
                           'cipaddr': np.array(valstr, dtype='object'),
                           'cuuid': np.array(valstr, dtype='object'),
                           'cint128': np.array(valstr, dtype='object'),
                           },dtype='object')
        df.__DolphinDB_Type__ = {
            'cbool': keys.DT_BOOL,
            'cchar': keys.DT_CHAR,
            'cshort': keys.DT_SHORT,
            'cint': keys.DT_INT,
            'clong': keys.DT_LONG,
            'cdate': keys.DT_DATE,
            'cmonth': keys.DT_MONTH,
            'ctime': keys.DT_TIME,
            'cminute': keys.DT_MINUTE,
            'csecond': keys.DT_SECOND,
            'cdatetime': keys.DT_DATETIME,
            'ctimestamp': keys.DT_TIMESTAMP,
            'cnanotime': keys.DT_NANOTIME,
            'cnanotimestamp': keys.DT_NANOTIMESTAMP,
            'cfloat': keys.DT_FLOAT,
            'cdouble': keys.DT_DOUBLE,
            'csymbol': keys.DT_SYMBOL,
            'cstring': keys.DT_STRING,
            'cipaddr': keys.DT_IPADDR,
            'cuuid': keys.DT_UUID,
            'cint128': keys.DT_INT128,
        }
        conn.upload({"tab":df})
        assert conn.run(r"""res = bool([]);for(i in 0:tab.columns()){res.append!(tab.column(i).isNull())};all(res)""")
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE', 
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT', 
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(schema, ex_types)
        conn.undefAll()
        conn.close()

    test_dataArray = [
        [[1, None, None], ['1.1.1.1', '', None], ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '', None], ['e1671797c52e15f763380b45e841ec32', '', None], [decimal.Decimal('1.00000'), None, None], [False, True, True]],
        [[1, None, pd.NaT], ['1.1.1.1', '', pd.NaT], ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '', None], ['e1671797c52e15f763380b45e841ec32', '', None], [decimal.Decimal('1.00000'), pd.NaT, pd.NaT], [False, True, True]],
        [[1, None, np.nan], ['1.1.1.1', '', np.nan], ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '', None], ['e1671797c52e15f763380b45e841ec32', '', None], [decimal.Decimal('1.00000'), np.nan, np.nan], [False, True, True]],
        [[pd.NaT, 1, np.nan], ['', '1.1.1.1', np.nan], ['', '5d212a78-cc48-e3b1-4235-b4d91473ee87', None], ['', 'e1671797c52e15f763380b45e841ec32', None], [np.nan, decimal.Decimal('1.00000'), decimal.Decimal('NaN')], [True, False, True]],
        [[np.nan, 1, pd.NaT], ['', '1.1.1.1', pd.NaT], ['', '5d212a78-cc48-e3b1-4235-b4d91473ee87', None], ['', 'e1671797c52e15f763380b45e841ec32', None], [decimal.Decimal('NaN'), decimal.Decimal('1.00000'), pd.NaT], [True, False, True]],
        [[None, 1, None], ['', '1.1.1.1', None], ['', '5d212a78-cc48-e3b1-4235-b4d91473ee87', None], ['', 'e1671797c52e15f763380b45e841ec32', None], [decimal.Decimal('NaN'), decimal.Decimal('1.00000'), None], [True, False, True]],
        [[pd.NaT, None, 1], ['', '', '1.1.1.1'], ['', '', '5d212a78-cc48-e3b1-4235-b4d91473ee87'], ['', '', 'e1671797c52e15f763380b45e841ec32'], [None, pd.NaT, decimal.Decimal('1.00000')], [True, True, False]],
        [[np.nan, None, 1], ['', '', '1.1.1.1'], ['', '', '5d212a78-cc48-e3b1-4235-b4d91473ee87'], ['', '', 'e1671797c52e15f763380b45e841ec32'], [None, np.nan, decimal.Decimal('1.00000')], [True, True, False]],
        [[None, None, 1], ['', '', '1.1.1.1'], ['', '', '5d212a78-cc48-e3b1-4235-b4d91473ee87'], ['', '', 'e1671797c52e15f763380b45e841ec32'], [None, decimal.Decimal('NaN'), decimal.Decimal('1.00000')], [True, True, False]],
        [[3, 2, 1], ['1.1.1.1', '1.1.1.1', '1.1.1.1'], ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87'], ['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32'], [decimal.Decimal('-2.34567'), decimal.Decimal('-3.1200'), decimal.Decimal('0')], [False, False, False]],
        [[3, 2, 1], ['1.1.1.1', '1.1.1.1', '1.1.1.1'], ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87'], ['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32'], [1, 2, 3], [False, False, False]],
        [[3, 2, 1], ['1.1.1.1', '1.1.1.1', '1.1.1.1'], ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87'], ['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32'], [1.1, 2.1, 3.1], [False, False, False]],
        [[None, None, None], [None, None, None],[None, None, None],[None, None, None],[None, None, None],[True, True, True]],
    ]

    test_dataArray = [
        [[None, None, None],[None, None, None], [None, None, None]],
        [[pd.NaT, None, None],['', None, None], [decimal.Decimal('NaN'),None,None]],
        [[np.nan, None, None],["", None, None], [np.nan,None,None]],
        [[None, pd.NaT, None],[None, '', None], [None, decimal.Decimal('NaN'), None]],
        [[None, np.nan, None],[None, "", None], [None, np.nan, None]],
        [[None, None, pd.NaT],[None, None, ''], [None, None, decimal.Decimal('NaN')]],
        [[None, None, np.nan],[None, None, ""], [None, None, np.nan]],
        [[None, np.nan, pd.NaT],[None, "", ''], [None, np.nan, pd.NaT]],
        [[None, pd.NaT, np.nan],[None, '', ""], [None, pd.NaT, np.nan]],
        [[pd.NaT, np.nan, None],['', '', None], [pd.NaT,np.nan,None]],
        [[np.nan, pd.NaT, None],["", "", None], [np.nan,pd.NaT,None]],
        [[pd.NaT, pd.NaT, pd.NaT],['', '', ''], [pd.NaT,pd.NaT,pd.NaT]],
        [[np.nan, np.nan, np.nan],["", "", ""], [np.nan,np.nan,np.nan]],
    ]

    @pytest.mark.parametrize('val, valstr, valdecimal', test_dataArray, ids=[str(x[0]) for x in test_dataArray])
    def test_upload_allNone_tables_with_pythonList(self, val, valstr, valdecimal):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        df = pd.DataFrame({
                           'cbool': val,
                           'cchar': val,
                           'cshort': val,
                           'cint': val,
                           'clong': val,
                           'cdate': val,
                           'cmonth': val,
                           'ctime': val,
                           'cminute': val,
                           'csecond': val,
                           'cdatetime': val,
                           'ctimestamp': val,
                           'cnanotime': val,
                           'cnanotimestamp': val,
                           'cfloat': val,
                           'cdouble': val,
                           'csymbol': valstr,
                           'cstring': valstr,
                           'cipaddr': valstr,
                           'cuuid': valstr,
                           'cint128': valstr,
                           },dtype='object')
        df.__DolphinDB_Type__ = {
            'cbool': keys.DT_BOOL,
            'cchar': keys.DT_CHAR,
            'cshort': keys.DT_SHORT,
            'cint': keys.DT_INT,
            'clong': keys.DT_LONG,
            'cdate': keys.DT_DATE,
            'cmonth': keys.DT_MONTH,
            'ctime': keys.DT_TIME,
            'cminute': keys.DT_MINUTE,
            'csecond': keys.DT_SECOND,
            'cdatetime': keys.DT_DATETIME,
            'ctimestamp': keys.DT_TIMESTAMP,
            'cnanotime': keys.DT_NANOTIME,
            'cnanotimestamp': keys.DT_NANOTIMESTAMP,
            'cfloat': keys.DT_FLOAT,
            'cdouble': keys.DT_DOUBLE,
            'csymbol': keys.DT_SYMBOL,
            'cstring': keys.DT_STRING,
            'cipaddr': keys.DT_IPADDR,
            'cuuid': keys.DT_UUID,
            'cint128': keys.DT_INT128,
        }
        conn.upload({"tab":df})
        assert conn.run(r"""res = bool([]);for(i in 0:tab.columns()){res.append!(tab.column(i).isNull())};all(res)""")
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE', 
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT', 
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(schema, ex_types)
        conn.undefAll()
        conn.close()

    def test_upload_exceptions(self):
        df = pd.DataFrame({
                1: [1, 2, 3],
                "2": [2, 3, 4],
            })
        with pytest.raises(RuntimeError, match=r"Column names must be strings indicating a valid variable name"):
            self.conn.upload({'tab':df})
        class myDataFrame(pd.DataFrame):
            def __init__(self, data) -> None:
                super().__init__(data)
                self.data = data
            def __getitem__(self, index):
                return self.data[index][0]
        df = myDataFrame({
            "a": [4, 5, 6],
            "b": [1, 2, 3],
        })
        with pytest.raises(RuntimeError, match=r"Table columns must be vectors \(np.array, series, tuple, or list\) for upload"):
            self.conn.upload({'a': df})

    # https://dolphindb1.atlassian.net/browse/APY-653
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_upload_dataframe_with_numpy_order(self,_compress,_order,_python_list):
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=False)
        data = []
        for i in range(10):
            row_data = [i, False, i,i,i,i, 
                        np.datetime64(i, "D").astype("datetime64[ns]"),
                        np.datetime64(i, "M").astype("datetime64[ns]"),
                        np.datetime64(i, "ms").astype("datetime64[ns]"),
                        np.datetime64(i, "m").astype("datetime64[ns]"),
                        np.datetime64(i, "s").astype("datetime64[ns]"),
                        np.datetime64(i, "s").astype("datetime64[ns]"),
                        np.datetime64(i, "ms").astype("datetime64[ns]"),
                        np.datetime64(i, "ns").astype("datetime64[ns]"),
                        np.datetime64(i, "ns").astype("datetime64[ns]"),
                        np.datetime64(i, "h").astype("datetime64[ns]"),
                        i,i, 'sym','str', "1.1.1.1", "5d212a78-cc48-e3b1-4235-b4d91473ee87",
                        "e1671797c52e15f763380b45e841ec32"]
            data.append(row_data)
        if _python_list:
            df = pd.DataFrame(data,columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate', 
                            'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime', 'cnanotimestamp', 
                            'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cipaddr', 'cuuid', 'cint128'])
        else:
            df = pd.DataFrame(np.array(data,dtype='object',order=_order),columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate', 
                            'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime', 'cnanotimestamp', 
                            'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cipaddr', 'cuuid', 'cint128'])
        df.__DolphinDB_Type__ = {
            'cbool': keys.DT_BOOL,
            'cchar': keys.DT_CHAR,
            'cshort': keys.DT_SHORT,
            'cint': keys.DT_INT,
            'clong': keys.DT_LONG,
            'cdate': keys.DT_DATE,
            'cmonth': keys.DT_MONTH,
            'ctime': keys.DT_TIME,
            'cminute': keys.DT_MINUTE,
            'csecond': keys.DT_SECOND,
            'cdatetime': keys.DT_DATETIME,
            'ctimestamp': keys.DT_TIMESTAMP,
            'cnanotime': keys.DT_NANOTIME,
            'cnanotimestamp': keys.DT_NANOTIMESTAMP,
            'cdatehour': keys.DT_DATEHOUR,
            'cfloat': keys.DT_FLOAT,
            'cdouble': keys.DT_DOUBLE,
            'csymbol': keys.DT_SYMBOL,
            'cstring': keys.DT_STRING,
            'cipaddr': keys.DT_IPADDR,
            'cuuid': keys.DT_UUID,
            'cint128': keys.DT_INT128,
        }
        conn1.upload({'t':df})
        conn1.run("""
            t1 = t; t1.clear!();go;
            for(i in 0:10){
                tableInsert(t1, i, false, i,i,i,i,i,i+23640,i,i,i,i,i,i,i,i,i,i, 'sym','str', ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"))
            }
        """)
        res = conn1.run("""ex = select * from t1;
                           res = select * from t;
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        tys = conn1.run("schema(t).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE', 
                            'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT', 
                            'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(tys, ex_types)
        conn1.close()

    # https://dolphindb1.atlassian.net/browse/APY-653
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_upload_null_dataframe_with_numpy_order(self,_compress,_order,_python_list):
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress)
        data = []
        origin_nulls = [None,np.nan,pd.NaT]
        for i in range(7):
            row_data = random.choices(origin_nulls, k=22)
            print(f'row {i}:', row_data)
            data.append([i, *row_data])
        data.append([7]+[None]*22)
        data.append([8]+[pd.NaT]*22)
        data.append([9]+[np.nan]*22)
        if _python_list:
            df = pd.DataFrame(data,columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                            'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime', 'cnanotimestamp',
                            'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cipaddr', 'cuuid', 'cint128'],dtype='object')
        else:
            df = pd.DataFrame(np.array(data,dtype='object',order=_order),columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                            'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime', 'cnanotimestamp',
                            'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cipaddr', 'cuuid', 'cint128'],dtype='object')
        df.__DolphinDB_Type__ = {
            'cbool': keys.DT_BOOL,
            'cchar': keys.DT_CHAR,
            'cshort': keys.DT_SHORT,
            'cint': keys.DT_INT,
            'clong': keys.DT_LONG,
            'cdate': keys.DT_DATE,
            'cmonth': keys.DT_MONTH,
            'ctime': keys.DT_TIME,
            'cminute': keys.DT_MINUTE,
            'csecond': keys.DT_SECOND,
            'cdatetime': keys.DT_DATETIME,
            'ctimestamp': keys.DT_TIMESTAMP,
            'cnanotime': keys.DT_NANOTIME,
            'cnanotimestamp': keys.DT_NANOTIMESTAMP,
            'cdatehour': keys.DT_DATEHOUR,
            'cfloat': keys.DT_FLOAT,
            'cdouble': keys.DT_DOUBLE,
            'csymbol': keys.DT_SYMBOL,
            'cstring': keys.DT_STRING,
            'cipaddr': keys.DT_IPADDR,
            'cuuid': keys.DT_UUID,
            'cint128': keys.DT_INT128,
        }
        conn1.upload({'t':df})
        conn1.run("""
            t1 = t; t1.clear!();go;
            for(i in 0:10){
                tableInsert(t1, i,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = conn1.run("""ex = select * from t1;
                           res = select * from t;
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        tys = conn1.run("schema(t).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                            'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                            'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(tys, ex_types)
        conn1.close()

    @pytest.mark.parametrize('data_type',['string'],ids=['string'])
    def test_upload_scalar_over_length(self,data_type):
        if data_type in ('string',):
            data={
                f'test_scalar_{data_type}':'1'*4*64*1024
            }
        conn=ddb.session(HOST,PORT,USER,PASSWD)
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['string'], ids=['string'])
    def test_upload_vector_over_length(self,data_type):
        if data_type in ('string',):
            data={
                f'test_vector_{data_type}':np.array(['1'*4*64*1024, ""], dtype="str")
            }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['string'], ids=['string'])
    def test_upload_set_over_length(self,data_type):
        if data_type in ('string',):
            data={
                f'test_set_{data_type}':{'1'*4*64*1024, ""}
            }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['string'], ids=['string'])
    def test_upload_dicionary_over_length(self,data_type):
        if data_type in ('string',):
            data={
                f'test_dicionary_{data_type}':{'a':'1'*4*64*1024}
            }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['string'], ids=['string'])
    def test_upload_table_over_length(self,data_type):
        if data_type in ('string',):
            data={
                f'test_table_{data_type}':pd.DataFrame({'a': [1, 2, 3], 'b': ['1'*256 * 1024, '2', '3']})
            }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            conn.upload(data)
        conn.close()

if __name__ == '__main__':
    pytest.main(["-s", "test/test_upload.py"])

    
