import decimal
import inspect
import random

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal

from basic_testing.prepare import PANDAS_VERSION, random_string
from setup.prepare import get_Scalar, DATATYPE, TIMETYPE, generate_uuid, get_Vector, get_Matrix, get_Set, \
    get_Dictionary, get_Table, get_Table_arrayVetcor, get_PartitionedTable_Append_Upsert
from setup.settings import HOST, PORT, USER, PASSWD

test_type = []
for x in DATATYPE:
    test_type.append({'type': x})


class TestUpload:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Scalar(self, data_type):
        tmp_s, tmp_p = get_Scalar(types=data_type, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_s_')
            conn.upload({upstr: p})
            res = conn.run(f"eqObj({upstr},{s})")
            assert res
            res_type = conn.run(f"type({upstr})==type({s})")
            assert res_type
            res_typestr = conn.run(f"typestr({upstr})==typestr({s})")
            assert res_typestr

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Vector(self, data_type):
        tmp_s, tmp_p = get_Vector(types=data_type, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_v_')
            conn.upload({upstr: p})
            res = conn.run(f"eqObj({upstr},{s})")
            assert res
            res_type = conn.run(f"type({upstr})==type({s})")
            assert res_type
            res_typestr = conn.run(f"typestr({upstr})==typestr({s})")
            assert res_typestr

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Matrix(self, data_type):
        tmp_s, tmp_p = get_Matrix(types=data_type, names="upload", r=10, c=10)
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_m_')
            conn.upload({upstr: p})
            res_value = conn.run(f"eqObj({upstr},{s})")
            assert res_value
            res_type = conn.run(f"type({upstr})==type({s})")
            assert res_type
            res_typestr = conn.run(f"typestr({upstr})==typestr({s})")
            assert res_typestr

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Set(self, data_type):
        tmp_s, tmp_p = get_Set(types=data_type, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_set_')
            conn.upload({upstr: p})
            res_value = conn.run(f"{upstr}=={s}")
            assert res_value
            res_type = conn.run(f"type({upstr})==type({s})")
            assert res_type
            res_typestr = conn.run(f"typestr({upstr})==typestr({s})")
            assert res_typestr

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Dictionary(self, data_type):
        tmp_s, tmp_p = get_Dictionary(types=data_type, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_d_')
            conn.upload({upstr: p})
            re1 = conn.run(f"""
                x=[]
                for (i in {upstr}.keys()){{ x.append!({upstr}[i]=={s}[i])}}
                for (i in {s}.keys()){{ x.append!({upstr}[i]=={s}[i])}};
                all(x)
            """)
            assert re1
            res_type = conn.run(f"type({upstr})==type({s})")
            assert res_type
            res_typestr = conn.run(f"typestr({upstr})==typestr({s})")
            assert res_typestr

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('isShare', [False], ids=['unshare'])
    @pytest.mark.parametrize('tabletype', ['table'], ids=['table'])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_Table(self, tabletype, data_type, compress, isShare):
        tmp_s, tmp_p = get_Table(typeTable=tabletype, types=data_type, ishare=isShare, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_t_')
            conn.upload({upstr: p})
            res_value = conn.run(f"each(eqObj, {upstr}.values(), {s}.values())")
            if data_type in [DATATYPE.DT_DATE, DATATYPE.DT_DATEHOUR, DATATYPE.DT_MONTH] and p is not None:
                assert not res_value.all()
            elif data_type in [DATATYPE.DT_DATETIME, DATATYPE.DT_TIMESTAMP] and p is not None and PANDAS_VERSION < (
                    2, 0, 0):
                assert not res_value.all()
            else:
                assert res_value.all()
            res_type = conn.run(f"type({upstr})==type({s})")
            assert res_type
            res_typestr = conn.run(f"typestr({upstr})==typestr({s})")
            assert res_typestr
            res_schema = conn.run(f"each(eqObj, schema({upstr})['colDefs'].values(), schema({s})['colDefs'].values())")
            if data_type in [DATATYPE.DT_DATE, DATATYPE.DT_DATEHOUR, DATATYPE.DT_MONTH] and p is not None:
                assert not res_schema.all()
            elif data_type in [DATATYPE.DT_DATETIME, DATATYPE.DT_TIMESTAMP] and p is not None and PANDAS_VERSION < (
                    2, 0, 0):
                assert not res_value.all()
            else:
                assert res_schema.all()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('isShare', [False], ids=['unshare'])
    @pytest.mark.parametrize('table_type', ['table'], ids=['table'])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_TableArrayVector(self, data_type, compress, table_type, isShare):
        tmp_s, tmp_p = get_Table_arrayVetcor(types=data_type, typeTable=table_type, isShare=isShare,
                                             names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = generate_uuid('tmp_at_')
            conn.upload({upstr: p})
            res_value = conn.run(f"each(eqObj, {upstr}.values(), {s}.values())")
            assert res_value.all()
            res_type = conn.run(f"type({upstr})==type({s})")
            assert res_type
            res_typestr = conn.run(f"typestr({upstr})==typestr({s})")
            assert res_typestr
            res_schema = conn.run(f"each(eqObj, schema({upstr})['colDefs'].values(), schema({s})['colDefs'].values())")
            assert res_schema.all()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('isShare', [False], ids=["unshare"])
    @pytest.mark.parametrize('table_type', ["table", "streamTable", "indexedTable", "keyedTable"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_tableAppender_Table(self, table_type, data_type, compress, isShare):
        tmp_s, tmp_p = get_Table(typeTable=table_type, types=data_type, isShare=isShare, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = "test_" + s
            res_len_before = conn.run(f"size(select * from {upstr}) == 0")
            assert res_len_before
            appender = ddb.tableAppender(tableName=upstr, ddbSession=conn)
            appender.append(p)
            res_value = conn.run(f"each(eqObj, (select * from {upstr}).values(), (select * from {s}).values())")
            assert res_value.all()
            res_len_after = conn.run(f"size(select * from {upstr}) == size(select * from {s})")
            assert res_len_after

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('isShare', [False], ids=["unshare"])
    @pytest.mark.parametrize('table_type', ["indexedTable", "keyedTable"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_upload_tableUpsert_Table(self, table_type, data_type, compress, isShare):
        tmp_s, tmp_p = get_Table(typeTable=table_type, types=data_type, isShare=isShare, names="upload")
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = "test_" + s
            res_len_before = conn.run(f"size(select * from {upstr}) == 0")
            assert res_len_before
            upsert = ddb.tableUpsert(tableName=upstr, ddbSession=conn)
            upsert.upsert(p)
            res_value = conn.run(f"each(eqObj, (select * from {upstr}).values(), (select * from {s}).values())")
            assert res_value.all()
            res_len_after = conn.run(f"size(select * from {upstr}) == size(select * from {s})")
            assert res_len_after

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_upload_partitionedTable_TableAppender(self, data_type, compress):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        db_name = f"dfs://{func_name}"
        tmp_s, tmp_p = get_PartitionedTable_Append_Upsert(types=data_type, names="upload", db_name=db_name,
                                                          script_type=data_type)
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = "test_" + s
            dbpath = db_name + '_' + s.split('_')[1]
            appender = ddb.TableAppender(dbPath=dbpath, tableName=upstr, ddbSession=conn)
            appender.append(p)
            res_value = conn.run(f"eqObj((select * from {upstr}).values(), (select * from {s}).values())")
            res_type = conn.run(f"type({upstr})==type({s})")
            if data_type in [DATATYPE.DT_TIMESTAMP, DATATYPE.DT_DATE, DATATYPE.DT_DATEHOUR, DATATYPE.DT_DATETIME,
                             DATATYPE.DT_MONTH] and p is not None:
                continue
            else:
                assert res_value
                assert res_type

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('ignoreNull', [True, False], ids=['enignoreNull', 'unignoreNull'])
    def test_upload_partitionedTable_TableUpserter(self, data_type, compress, ignoreNull):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        db_name = f"dfs://{func_name}"
        tmp_s, tmp_p = get_PartitionedTable_Append_Upsert(types=data_type, names="upload", db_name=db_name,
                                                          script_type=data_type)
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            upstr = "test_" + s
            dbpath = db_name + '_' + s.split('_')[1]
            upsert = ddb.TableUpserter(dbPath=dbpath, tableName=upstr, ddbSession=conn, ignoreNull=ignoreNull,
                                       keyColNames=["index"])
            upsert.upsert(p)
            res_value = conn.run(f"eqObj((select * from {upstr}).values(), (select * from {s}).values())")
            res_type = conn.run(f"type({upstr})==type({s})")
            if data_type in [DATATYPE.DT_TIMESTAMP, DATATYPE.DT_DATE, DATATYPE.DT_DATEHOUR, DATATYPE.DT_DATETIME,
                             DATATYPE.DT_MONTH] and p is not None:
                continue
            else:
                assert res_value
                assert res_type
        conn.close()

    test_dataArray = [
        ['DATE', {'a': np.array([0, None, 1000], dtype="datetime64[D]")}],
        ['MONTH', {'a': np.array([0, None, 1000], dtype="datetime64[M]")}],
        ['TIME', {'a': np.array([0, None, 1000], dtype="datetime64[ms]")}],
        ['MINUTE', {'a': np.array([0, None, 1000], dtype="datetime64[m]")}],
        ['SECOND', {'a': np.array([0, None, 1000], dtype="datetime64[s]")}],
        ['DATETIME', {'a': np.array([0, None, 1000], dtype="datetime64[s]")}],
        ['TIMESTAMP', {'a': np.array([0, None, 1000], dtype="datetime64[ms]")}],
        ['NANOTIME', {'a': np.array([0, None, 1000], dtype="datetime64[ns]")}],
        ['NANOTIMESTAMP', {'a': np.array([0, None, 1000], dtype="datetime64[ns]")}],
        ['DATEHOUR', {'a': np.array([0, None, 1000], dtype="datetime64[h]")}]
    ]

    @pytest.mark.parametrize('data_type, np_dict', test_dataArray, ids=[x.name for x in TIMETYPE])
    def test_upload_numpyTimeTypesArray_Vector(self, data_type, np_dict):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        if data_type == 'TIME':
            _data_type = 'TIMESTAMP'
        elif data_type == 'MINUTE' or data_type == 'SECOND':
            _data_type = 'DATETIME'
        elif data_type == 'NANOTIME':
            _data_type = 'NANOTIMESTAMP'
        else:
            _data_type = data_type
        if data_type == 'MONTH':
            ex_s = "v1 = array(MONTH, 0);v1.append!(1970.01M NULL 2053.05M)"
        elif data_type == 'MINUTE':
            ex_s = "v1 = array(DATETIME, 0);v1.append!(0 NULL 60000)"
        else:
            ex_s = f"v1 = array({_data_type}, 0);v1.append!(0 NULL 1000)"
        conn.upload(np_dict)
        conn.run(ex_s)
        assert conn.run("eqObj(v1, a)")
        conn.close()

    test_dataArray = [
        [[None, None, None], [None, None, None], [None, None, None]],
        [[pd.NaT, None, None], ['', None, None], [decimal.Decimal('NaN'), None, None]],
        [[np.nan, None, None], ["", None, None], [np.nan, None, None]],
        [[None, pd.NaT, None], [None, '', None], [None, decimal.Decimal('NaN'), None]],
        [[None, np.nan, None], [None, "", None], [None, np.nan, None]],
        [[None, None, pd.NaT], [None, None, ''], [None, None, decimal.Decimal('NaN')]],
        [[None, None, np.nan], [None, None, ""], [None, None, np.nan]],
        [[None, np.nan, pd.NaT], [None, "", ''], [None, np.nan, pd.NaT]],
        [[None, pd.NaT, np.nan], [None, '', ""], [None, pd.NaT, np.nan]],
        [[pd.NaT, np.nan, None], ['', '', None], [pd.NaT, np.nan, None]],
        [[np.nan, pd.NaT, None], ["", "", None], [np.nan, pd.NaT, None]],
        [[pd.NaT, pd.NaT, pd.NaT], ['', '', ''], [pd.NaT, pd.NaT, pd.NaT]],
        [[np.nan, np.nan, np.nan], ["", "", ""], [np.nan, np.nan, np.nan]],
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
            'cblob': np.array(valstr, dtype='object'),
            'cdecimal32': np.array(valdecimal, dtype='object'),
            'cdecimal64': np.array(valdecimal, dtype='object'),
            'cdecimal128': np.array(valdecimal, dtype='object'),
        }, dtype='object')
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
            'cblob': keys.DT_BLOB,
            'cdecimal32': keys.DT_DECIMAL32,
            'cdecimal64': keys.DT_DECIMAL64,
            'cdecimal128': keys.DT_DECIMAL128
        }
        conn.upload({"tab": df})
        assert conn.run(r"res = bool([]);for(i in 0:tab.columns()){res.append!(tab.column(i).isNull())};all(res)")
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128', 'BLOB', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(schema, ex_types)
        conn.close()

    test_dataArray = [
        [[1, None, None], ['1.1.1.1', '', None], ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '', None],
         ['e1671797c52e15f763380b45e841ec32', '', None], [decimal.Decimal('1.00000'), None, None], [False, True, True]],
        [[1, None, pd.NaT], ['1.1.1.1', '', pd.NaT], ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '', None],
         ['e1671797c52e15f763380b45e841ec32', '', None], [decimal.Decimal('1.00000'), pd.NaT, pd.NaT],
         [False, True, True]],
        [[1, None, np.nan], ['1.1.1.1', '', np.nan], ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '', None],
         ['e1671797c52e15f763380b45e841ec32', '', None], [decimal.Decimal('1.00000'), np.nan, np.nan],
         [False, True, True]],
        [[pd.NaT, 1, np.nan], ['', '1.1.1.1', np.nan], ['', '5d212a78-cc48-e3b1-4235-b4d91473ee87', None],
         ['', 'e1671797c52e15f763380b45e841ec32', None], [np.nan, decimal.Decimal('1.00000'), decimal.Decimal('NaN')],
         [True, False, True]],
        [[np.nan, 1, pd.NaT], ['', '1.1.1.1', pd.NaT], ['', '5d212a78-cc48-e3b1-4235-b4d91473ee87', None],
         ['', 'e1671797c52e15f763380b45e841ec32', None], [decimal.Decimal('NaN'), decimal.Decimal('1.00000'), pd.NaT],
         [True, False, True]],
        [[None, 1, None], ['', '1.1.1.1', None], ['', '5d212a78-cc48-e3b1-4235-b4d91473ee87', None],
         ['', 'e1671797c52e15f763380b45e841ec32', None], [decimal.Decimal('NaN'), decimal.Decimal('1.00000'), None],
         [True, False, True]],
        [[pd.NaT, None, 1], ['', '', '1.1.1.1'], ['', '', '5d212a78-cc48-e3b1-4235-b4d91473ee87'],
         ['', '', 'e1671797c52e15f763380b45e841ec32'], [None, pd.NaT, decimal.Decimal('1.00000')], [True, True, False]],
        [[np.nan, None, 1], ['', '', '1.1.1.1'], ['', '', '5d212a78-cc48-e3b1-4235-b4d91473ee87'],
         ['', '', 'e1671797c52e15f763380b45e841ec32'], [None, np.nan, decimal.Decimal('1.00000')], [True, True, False]],
        [[None, None, 1], ['', '', '1.1.1.1'], ['', '', '5d212a78-cc48-e3b1-4235-b4d91473ee87'],
         ['', '', 'e1671797c52e15f763380b45e841ec32'], [None, decimal.Decimal('NaN'), decimal.Decimal('1.00000')],
         [True, True, False]],
        [[3, 2, 1], ['1.1.1.1', '1.1.1.1', '1.1.1.1'],
         ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87',
          '5d212a78-cc48-e3b1-4235-b4d91473ee87'],
         ['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32'],
         [decimal.Decimal('-2.34567'), decimal.Decimal('-3.1200'), decimal.Decimal('0')], [False, False, False]],
        [[3, 2, 1], ['1.1.1.1', '1.1.1.1', '1.1.1.1'],
         ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87',
          '5d212a78-cc48-e3b1-4235-b4d91473ee87'],
         ['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32'],
         [1, 2, 3], [False, False, False]],
        [[3, 2, 1], ['1.1.1.1', '1.1.1.1', '1.1.1.1'],
         ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87',
          '5d212a78-cc48-e3b1-4235-b4d91473ee87'],
         ['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32'],
         [1.1, 2.1, 3.1], [False, False, False]],
        [[None, None, None], [None, None, None], [None, None, None], [None, None, None], [None, None, None],
         [True, True, True]],
    ]

    @pytest.mark.parametrize('val, valip, valuuid, valint128, valdecimal, ex', test_dataArray,
                             ids=[str(x[0]) for x in test_dataArray])
    def test_upload_tables_arrayVector(self, val, valip, valuuid, valint128, valdecimal, ex):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        df = pd.DataFrame(
            [
                [val, val, val, val, val, val, val, val, val, val, val, val, val, val, val, val, valip, valuuid,
                 valint128, valdecimal, valdecimal, valdecimal],
                [val, val, val, val, val, val, val, val, val, val, val, val, val, val, val, val, valip, valuuid,
                 valint128, valdecimal, valdecimal, valdecimal],
                [val, val, val, val, val, val, val, val, val, val, val, val, val, val, val, val, valip, valuuid,
                 valint128, valdecimal, valdecimal, valdecimal]
            ],
            columns=[
                'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate', 'cmonth', 'ctime', 'cminute', 'csecond',
                'cdatetime', 'ctimestamp', 'cnanotime', 'cnanotimestamp', 'cfloat', 'cdouble',
                'cipaddr', 'cuuid', 'cint128', 'cdecimal32', 'cdecimal64', 'cdecimal128'
            ], dtype='object'
        )
        df.__DolphinDB_Type__ = {
            'cbool': keys.DT_BOOL_ARRAY,
            'cchar': keys.DT_CHAR_ARRAY,
            'cshort': keys.DT_SHORT_ARRAY,
            'cint': keys.DT_INT_ARRAY,
            'clong': keys.DT_LONG_ARRAY,
            'cdate': keys.DT_DATE_ARRAY,
            'cmonth': keys.DT_MONTH_ARRAY,
            'ctime': keys.DT_TIME_ARRAY,
            'cminute': keys.DT_MINUTE_ARRAY,
            'csecond': keys.DT_SECOND_ARRAY,
            'cdatetime': keys.DT_DATETIME_ARRAY,
            'ctimestamp': keys.DT_TIMESTAMP_ARRAY,
            'cnanotime': keys.DT_NANOTIME_ARRAY,
            'cnanotimestamp': keys.DT_NANOTIMESTAMP_ARRAY,
            'cfloat': keys.DT_FLOAT_ARRAY,
            'cdouble': keys.DT_DOUBLE_ARRAY,
            'cipaddr': keys.DT_IPADDR_ARRAY,
            'cuuid': keys.DT_UUID_ARRAY,
            'cint128': keys.DT_INT128_ARRAY,
            'cdecimal32': keys.DT_DECIMAL32_ARRAY,
            'cdecimal64': keys.DT_DECIMAL64_ARRAY,
            'cdecimal128': keys.DT_DECIMAL128_ARRAY
        }
        if valdecimal in [[1, 2, 3], [1.1, 2.1, 3.1]]:
            with pytest.raises(RuntimeError, match=r'Cannot convert object with ANY type to an ArrayVector'):
                conn.upload({"tab": df})
            return
        conn.upload({"tab": df})
        res = conn.run("res = array(BOOL[]);for(i in 0:tab.columns()){res.append!(tab.column(i).isNull())};res")
        for row_res in res:
            assert_array_equal(row_res, ex)
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]', 'DECIMAL32(5)[]', 'DECIMAL64(5)[]', 'DECIMAL128(5)[]']
        if val == [None, None, None]:
            ex_types[-3] = 'DECIMAL32(0)[]'
            ex_types[-2] = 'DECIMAL64(0)[]'
            ex_types[-1] = 'DECIMAL128(0)[]'
        assert_array_equal(schema, ex_types)
        conn.close()

    test_dataArray = [
        [[None, None, None], [None, None, None], [None, None, None]],
        [[pd.NaT, None, None], ['', None, None], [decimal.Decimal('NaN'), None, None]],
        [[np.nan, None, None], ["", None, None], [np.nan, None, None]],
        [[None, pd.NaT, None], [None, '', None], [None, decimal.Decimal('NaN'), None]],
        [[None, np.nan, None], [None, "", None], [None, np.nan, None]],
        [[None, None, pd.NaT], [None, None, ''], [None, None, decimal.Decimal('NaN')]],
        [[None, None, np.nan], [None, None, ""], [None, None, np.nan]],
        [[None, np.nan, pd.NaT], [None, "", ''], [None, np.nan, pd.NaT]],
        [[None, pd.NaT, np.nan], [None, '', ""], [None, pd.NaT, np.nan]],
        [[pd.NaT, np.nan, None], ['', '', None], [pd.NaT, np.nan, None]],
        [[np.nan, pd.NaT, None], ["", "", None], [np.nan, pd.NaT, None]],
        [[pd.NaT, pd.NaT, pd.NaT], ['', '', ''], [pd.NaT, pd.NaT, pd.NaT]],
        [[np.nan, np.nan, np.nan], ["", "", ""], [np.nan, np.nan, np.nan]],
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
            'cblob': valstr,
            'cdecimal32': valdecimal,
            'cdecimal64': valdecimal,
            'cdecimal128': valdecimal,
        }, dtype='object')
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
            'cblob': keys.DT_BLOB,
            'cdecimal32': keys.DT_DECIMAL32,
            'cdecimal64': keys.DT_DECIMAL64,
            'cdecimal128': keys.DT_DECIMAL128
        }
        conn.upload({"tab": df})
        assert conn.run(r"res = bool([]);for(i in 0:tab.columns()){res.append!(tab.column(i).isNull())};all(res)")
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128', 'BLOB', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(schema, ex_types)
        conn.close()

    def test_upload_exceptions(self):
        df = pd.DataFrame({
            1: [1, 2, 3],
            "2": [2, 3, 4],
        })
        with pytest.raises(RuntimeError, match=r"DataFrame column names must be strings"):
            self.conn.upload({'tab': df})

        class MyDataFrame(pd.DataFrame):

            def __init__(self, data) -> None:
                super().__init__(data)
                self.data = data

            def __getitem__(self, index):
                return self.data[index][0]

        df = MyDataFrame({
            "a": [4, 5, 6],
            "b": [1, 2, 3],
        })
        with pytest.raises(RuntimeError,
                           match=r"Table columns must be vectors"):
            self.conn.upload({'a': df})
        df.__DolphinDB_Type__ = {
            'a': keys.DT_INT
        }
        with pytest.raises(RuntimeError,
                           match=r"Table columns must be vectors"):
            self.conn.upload({'a': df})

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_upload_dataframe_with_numpy_order(self, _compress, _order, _python_list):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress, enablePickle=False)
        data = []
        for i in range(10):
            row_data = [i, False, i, i, i, i,
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
                        i, i, 'sym', 'str', 'blob', "1.1.1.1", "5d212a78-cc48-e3b1-4235-b4d91473ee87",
                        "e1671797c52e15f763380b45e841ec32", decimal.Decimal('-2.11'), decimal.Decimal('0.00000000000'),
                        decimal.Decimal('-1.100000000000000000000000000000000000')]
            data.append(row_data)
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cblob', 'cipaddr',
                                             'cuuid', 'cint128', 'cdecimal32', 'cdecimal64', 'cdecimal128'])
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cblob', 'cipaddr',
                                       'cuuid', 'cint128', 'cdecimal32', 'cdecimal64', 'cdecimal128'])
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
            'cblob': keys.DT_BLOB,
            'cdecimal32': keys.DT_DECIMAL32,
            'cdecimal64': keys.DT_DECIMAL64,
            'cdecimal128': keys.DT_DECIMAL128
        }
        conn1.upload({'t': df})
        conn1.run("""
            t1 = t; t1.clear!();go;
            for(i in 0:10){
                tableInsert(t1, i, false, i,i,i,i,i,i+23640,i,i,i,i,i,i,i,i,i,i, 'sym','str', 'blob', ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"), decimal32('-2.11', 2), decimal64('0.0', 11), decimal128('-1.1', 36))
            }
        """)
        assert conn1.run("""
            ex = select * from t1;
            res = select * from t;
            all(each(eqObj, ex.values(), res.values()))
        """)
        tys = conn1.run("schema(t).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(2)', 'DECIMAL64(11)',
                    'DECIMAL128(36)']
        assert_array_equal(tys, ex_types)
        conn1.close()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_upload_dataframe_array_vector_with_numpy_order(self, _compress, _order, _python_list):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        for i in range(10):
            row_data = [i, [False], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i],
                        ["1.1.1.1"], ["5d212a78-cc48-e3b1-4235-b4d91473ee87"],
                        ["e1671797c52e15f763380b45e841ec32"], [decimal.Decimal('-2.11')],
                        [decimal.Decimal('0.00000000000')],
                        [decimal.Decimal('-1.100000000000000000000000000000000000')]]
            data.append(row_data)
        data.append(
            [10, [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None],
             [None], [None], [None], [None], [None], [None], [None], [None], [None], [None]])
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'cipaddr', 'cuuid', 'cint128',
                                             'cdecimal32', 'cdecimal64', 'cdecimal128'])
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'cipaddr', 'cuuid', 'cint128', 'cdecimal32',
                                       'cdecimal64', 'cdecimal128'])
        df.__DolphinDB_Type__ = {
            'cbool': keys.DT_BOOL_ARRAY,
            'cchar': keys.DT_CHAR_ARRAY,
            'cshort': keys.DT_SHORT_ARRAY,
            'cint': keys.DT_INT_ARRAY,
            'clong': keys.DT_LONG_ARRAY,
            'cdate': keys.DT_DATE_ARRAY,
            'cmonth': keys.DT_MONTH_ARRAY,
            'ctime': keys.DT_TIME_ARRAY,
            'cminute': keys.DT_MINUTE_ARRAY,
            'csecond': keys.DT_SECOND_ARRAY,
            'cdatetime': keys.DT_DATETIME_ARRAY,
            'ctimestamp': keys.DT_TIMESTAMP_ARRAY,
            'cnanotime': keys.DT_NANOTIME_ARRAY,
            'cnanotimestamp': keys.DT_NANOTIMESTAMP_ARRAY,
            'cdatehour': keys.DT_DATEHOUR_ARRAY,
            'cfloat': keys.DT_FLOAT_ARRAY,
            'cdouble': keys.DT_DOUBLE_ARRAY,
            'cipaddr': keys.DT_IPADDR_ARRAY,
            'cuuid': keys.DT_UUID_ARRAY,
            'cint128': keys.DT_INT128_ARRAY,
            'cdecimal32': keys.DT_DECIMAL32_ARRAY,
            'cdecimal64': keys.DT_DECIMAL64_ARRAY,
            'cdecimal128': keys.DT_DECIMAL128_ARRAY
        }
        conn1.upload({'t': df})
        conn1.run("""
            t1 = t; t1.clear!();go;
            for(i in 0:10){
                tableInsert(t1, i, 0, i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i, ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"), decimal32('-2.11', 2), decimal64('0.0', 11), decimal128('-1.1', 36))
            }
            tableInsert(t1, 10, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
        """)
        res = conn1.run("""
            ex = select * from t1;
            res = select * from t;
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        tys = conn1.run("schema(t).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]', 'DECIMAL32(2)[]', 'DECIMAL64(11)[]',
                    'DECIMAL128(36)[]']
        assert_array_equal(tys, ex_types)
        conn1.close()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_upload_null_dataframe_with_numpy_order(self, _compress, _order, _python_list):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        origin_nulls = [None, np.nan, pd.NaT]
        for i in range(7):
            row_data = random.choices(origin_nulls, k=26)
            data.append([i, *row_data])
        data.append([7] + [None] * 26)
        data.append([8] + [pd.NaT] * 26)
        data.append([9] + [np.nan] * 26)
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cblob', 'cipaddr',
                                             'cuuid', 'cint128', 'cdecimal32', 'cdecimal64', 'cdecimal128'],
                              dtype='object')
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cblob', 'cipaddr',
                                       'cuuid', 'cint128', 'cdecimal32', 'cdecimal64', 'cdecimal128'], dtype='object')
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
            'cblob': keys.DT_BLOB,
            'cdecimal32': keys.DT_DECIMAL32,
            'cdecimal64': keys.DT_DECIMAL64,
            'cdecimal128': keys.DT_DECIMAL128
        }
        conn1.upload({'t': df})
        conn1.run("""
            t1 = t; t1.clear!();go;
            for(i in 0:10){
                tableInsert(t1, i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = conn1.run("""
            ex = select * from t1;
            res = select * from t;
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        tys = conn1.run("schema(t).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(tys, ex_types)
        conn1.close()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_upload_null_dataframe_array_vector_with_numpy_order(self, _compress, _order, _python_list):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        origin_nulls = [[None], [np.nan], [pd.NaT]]
        for i in range(7):
            row_data = random.choices(origin_nulls, k=23)
            data.append([i, *row_data])
        data.append([7] + [[None]] * 23)
        data.append([8] + [[pd.NaT]] * 23)
        data.append([9] + [[np.nan]] * 23)
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'cipaddr', 'cuuid', 'cint128',
                                             'cdecimal32', 'cdecimal64', 'cdecimal128'], dtype='object')
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'cipaddr', 'cuuid', 'cint128', 'cdecimal32',
                                       'cdecimal64', 'cdecimal128'], dtype='object')
        df.__DolphinDB_Type__ = {
            'cbool': keys.DT_BOOL_ARRAY,
            'cchar': keys.DT_CHAR_ARRAY,
            'cshort': keys.DT_SHORT_ARRAY,
            'cint': keys.DT_INT_ARRAY,
            'clong': keys.DT_LONG_ARRAY,
            'cdate': keys.DT_DATE_ARRAY,
            'cmonth': keys.DT_MONTH_ARRAY,
            'ctime': keys.DT_TIME_ARRAY,
            'cminute': keys.DT_MINUTE_ARRAY,
            'csecond': keys.DT_SECOND_ARRAY,
            'cdatetime': keys.DT_DATETIME_ARRAY,
            'ctimestamp': keys.DT_TIMESTAMP_ARRAY,
            'cnanotime': keys.DT_NANOTIME_ARRAY,
            'cnanotimestamp': keys.DT_NANOTIMESTAMP_ARRAY,
            'cdatehour': keys.DT_DATEHOUR_ARRAY,
            'cfloat': keys.DT_FLOAT_ARRAY,
            'cdouble': keys.DT_DOUBLE_ARRAY,
            'cipaddr': keys.DT_IPADDR_ARRAY,
            'cuuid': keys.DT_UUID_ARRAY,
            'cint128': keys.DT_INT128_ARRAY,
            'cdecimal32': keys.DT_DECIMAL32_ARRAY,
            'cdecimal64': keys.DT_DECIMAL64_ARRAY,
            'cdecimal128': keys.DT_DECIMAL128_ARRAY
        }
        conn1.upload({'t': df})
        conn1.run("""
            t1 = t; t1.clear!();go;
            for(i in 0:10){
                tableInsert(t1, i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = conn1.run("""
            ex = select * from t1;
            res = select * from t;
            all(each(eqObj, ex.values(), res.values()))
       """)
        assert res
        tys = conn1.run("schema(t).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]', 'DECIMAL32(0)[]', 'DECIMAL64(0)[]', 'DECIMAL128(0)[]']
        assert_array_equal(tys, ex_types)

        conn1.close()

    @pytest.mark.parametrize('data_type', ['string'], ids=['string'])
    def test_upload_scalar_over_length(self, data_type):
        data = {
            f'test_scalar_{data_type}': '1' * 256 * 1024
        }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['string'], ids=['string'])
    def test_upload_vector_over_length(self, data_type):
        data = {
            f'test_vector_{data_type}': np.array(['1' * 256 * 1024, ""], dtype="str")
        }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['string'], ids=['string'])
    def test_upload_set_over_length(self, data_type):
        data = {
            f'test_set_{data_type}': {'1' * 256 * 1024, ""}
        }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['string'], ids=['string'])
    def test_upload_dictionary_over_length(self, data_type):
        data = {
            f'test_dictionary_{data_type}': {'a': '1' * 256 * 1024}
        }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['string'], ids=['string'])
    def test_upload_table_over_length_string(self, data_type):
        data = {
            f'test_table_{data_type}': pd.DataFrame({'a': [1, 2, 3], 'b': ['1' * 256 * 1024, '2', '3']})
        }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['symbol'], ids=['symbol'])
    def test_upload_table_over_length_symbol(self, data_type):
        df = pd.DataFrame({'a': [1, 2, 3], 'b': ['1' * 256 * 1024, '2', '3']})
        df.__DolphinDB_Type__ = {
            'b': keys.DT_SYMBOL
        }
        data = {
            f'test_table_{data_type}': df
        }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            conn.upload(data)
        conn.close()

    @pytest.mark.parametrize('data_type', ['blob'], ids=['blob'])
    def test_upload_table_over_length_blob(self, data_type):
        df = pd.DataFrame({'a': [1, 2, 3], 'b': ['1' * 256 * 1024, '2', '3']})
        df.__DolphinDB_Type__ = {
            'b': keys.DT_BLOB
        }
        data = {
            f'test_table_{data_type}': df
        }
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.upload(data)
        conn.close()
