import decimal
import inspect
import random

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from pandas._testing import assert_frame_equal

from basic_testing.prepare import random_string
from setup.settings import HOST, PORT, USER, PASSWD


class TestTableUpsert:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    def test_tableUpsert_error(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run("t=keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn)
        with pytest.raises(RuntimeError, match='table must be a DataFrame!'):
            upsert.upsert(object())

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_date(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64[ns]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == 10
        script = '''
            tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_dateToMonth(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, MONTH, INT])")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64[ns]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == 10
        script = '''
            tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01M, NULL, 1965.07M, NULL, 2020.12M, 1970.01M, NULL, NULL, NULL, 2009.08M] as date, 1..10 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_month(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`month`qty, [SYMBOL, MONTH, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        month = np.array(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'month': month, 'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == 5
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, [1965.08M, NULL, 2012.02M, 2012.03M, NULL] as month, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_monthToDate(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`month`qty, [SYMBOL, DATE, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        month = np.array(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'month': month, 'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == 5
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, [1965.08.01, NULL, 2012.02.01, 2012.03.01, NULL] as month, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_time(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, TIME, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999] as time, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_minute(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, MINUTE, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00m, 05:12m, NULL, NULL, 23:59m] as time, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_second(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, SECOND, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00, 05:12:48, NULL, NULL, 23:59:59] as time, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_datetime(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, DATETIME, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59] as time, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_timestamp(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, TIMESTAMP, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.008', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00.000, 2015.08.26T05:12:48.008, NULL, NULL, 2015.06.09T23:59:59.999] as time, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_nanotime(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, NANOTIME, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                         '2015-06-09T23:59:59.999008007'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007] as time, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_nanotimestamp(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, NANOTIMESTAMP, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                         '2015-06-09T23:59:59.999008007'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007] as time, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_date_null(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(np.repeat('Nat', 10), dtype="datetime64[D]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, take(date(),10) as date, 1..10 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_nanotimestamp_null(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, NANOTIMESTAMP, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(np.repeat('Nat', 5), dtype="datetime64[ns]")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table(`A1`A2`A3`A4`A5 as sym, take(nanotimestamp(),5) as time, 1..5 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_all_time_type(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(
            "t=keyedTable(`qty,1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = list(map(str, np.arange(100, 600)))
        date = np.array(np.tile(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            50), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '2015-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        qty = np.arange(100, 600)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month': month, 'time': time, 'minute': time, 'second': second,
                             'datetime': second, 'timestamp': time, 'nanotime': nanotime, 'nanotimestamp': nanotime,
                             'qty': qty})
        upsert.upsert(data)
        script = '''
            n = 500
            tmp=table(string(100..599) as sym, take([2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05],n) as date,take([1965.08M, NULL, 2012.02M, 2012.03M, NULL],n) as month,
            take([00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999],n) as time, take([00:00m, 05:12m, NULL, NULL, 23:59m],n) as minute, take([00:00:00, 05:12:48, NULL, NULL, 23:59:59],n) as second,take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59],n) as datetime,
            take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 2015.06.09T23:59:59.999],n) as timestamp,take([00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007],n) as nanotime,take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007],n) as nanotimestamp,
            100..599 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True, True, True, True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_all_time_type(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(
            "t=indexedTable(`qty,1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = list(map(str, np.arange(100, 600)))
        date = np.array(np.tile(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            50), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '2015-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        qty = np.arange(100, 600)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month': month, 'time': time, 'minute': time, 'second': second,
                             'datetime': second, 'timestamp': time, 'nanotime': nanotime, 'nanotimestamp': nanotime,
                             'qty': qty})
        upsert.upsert(data)
        script = '''
            n = 500
            tmp=table(string(100..599) as sym, take([2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05],n) as date,take([1965.08M, NULL, 2012.02M, 2012.03M, NULL],n) as month,
            take([00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999],n) as time, take([00:00m, 05:12m, NULL, NULL, 23:59m],n) as minute, take([00:00:00, 05:12:48, NULL, NULL, 23:59:59],n) as second,take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59],n) as datetime,
            take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 2015.06.09T23:59:59.999],n) as timestamp,take([00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007],n) as nanotime,take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007],n) as nanotimestamp,
            100..599 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True, True, True, True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_all_time_types(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT])
            db=database(dbPath,RANGE,100 200 300 400 601)
            pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        upsert = ddb.tableUpsert(db_name, "pt", conn, keyColNames=["qty"])
        sym = list(map(str, np.arange(100, 600)))
        date = np.array(np.tile(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            50), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '2015-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        qty = np.arange(100, 600)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month': month, 'time': time, 'minute': time, 'second': second,
                             'datetime': second, 'timestamp': time, 'nanotime': nanotime, 'nanotimestamp': nanotime,
                             'qty': qty})
        upsert.upsert(data)
        script = f'''
            n = 500
            tmp=table(string(100..599) as sym, take([2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05],n) as date,take([1965.08M, NULL, 2012.02M, 2012.03M, NULL],n) as month,
            take([00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999],n) as time, take([00:00m, 05:12m, NULL, NULL, 23:59m],n) as minute, take([00:00:00, 05:12:48, NULL, NULL, 23:59:59],n) as second,take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59],n) as datetime,
            take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 2015.06.09T23:59:59.999],n) as timestamp,take([00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007],n) as nanotime,take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007],n) as nanotimestamp,
            100..599 as qty)
            re = select * from loadTable("{db_name}",`pt)
            each(eqObj, tmp.values(), re.values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True, True, True, True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_all_time_type_early_1970(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(
            "t=keyedTable(`qty,1000:0, `date`month`datetime `timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        n = 500
        date = np.array(np.repeat('1960-01-01', n), dtype="datetime64[D]")
        month = np.array(np.repeat('1960-01', n), dtype="datetime64")
        datetime = np.array(np.repeat('1960-01-01T13:30:10', n), dtype="datetime64")
        timestamp = np.array(np.repeat('1960-01-01T13:30:10.008', n), dtype="datetime64")
        nanotimestamp = np.array(np.repeat('1960-01-01 13:30:10.008007006', n), dtype="datetime64")
        qty = np.arange(100, 600)
        data = pd.DataFrame(
            {'date': date, 'month': month, 'datetime': datetime, 'timestamp': timestamp, 'nanotimestamp': nanotimestamp,
             'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == n
        script = '''
            n = 500
            tmp=table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01T13:30:10,n) as datetime,
            take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as nanotimestamp,
            100..599 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_all_time_type_early_1970(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(
            "t=indexedTable(`qty,1000:0, `date`month`datetime `timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])")
        upsert = ddb.tableUpsert("", "t", conn)
        n = 500
        date = np.array(np.repeat('1960-01-01', n), dtype="datetime64[D]")
        month = np.array(np.repeat('1960-01', n), dtype="datetime64")
        datetime = np.array(np.repeat('1960-01-01T13:30:10', n), dtype="datetime64")
        timestamp = np.array(np.repeat('1960-01-01T13:30:10.008', n), dtype="datetime64")
        nanotimestamp = np.array(np.repeat('1960-01-01 13:30:10.008007006', n), dtype="datetime64")
        qty = np.arange(100, 600)
        data = pd.DataFrame(
            {'date': date, 'month': month, 'datetime': datetime, 'timestamp': timestamp, 'nanotimestamp': nanotimestamp,
             'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == n
        script = '''
            n = 500
            tmp=table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01T13:30:10,n) as datetime,
            take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as nanotimestamp,
            100..599 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_all_time_types_early_1970(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(1000:0, `date`month`datetime`timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])
            db=database(dbPath,RANGE,100 200 300 400 601)
            pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        upsert = ddb.tableUpsert(db_name, "pt", conn, keyColNames=["qty"])
        n = 500
        date = np.array(np.repeat('1960-01-01', n), dtype="datetime64[D]")
        month = np.array(np.repeat('1960-01', n), dtype="datetime64")
        datetime = np.array(np.repeat('1960-01-01T13:30:10', n), dtype="datetime64")
        timestamp = np.array(np.repeat('1960-01-01T13:30:10.008', n), dtype="datetime64")
        nanotimestamp = np.array(np.repeat('1960-01-01 13:30:10.008007006', n), dtype="datetime64")
        qty = np.arange(100, 600)
        data = pd.DataFrame(
            {'date': date, 'month': month, 'datetime': datetime, 'timestamp': timestamp, 'nanotimestamp': nanotimestamp,
             'qty': qty})
        upsert.upsert(data)
        script = f'''
            n = 500
            ex = table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01T13:30:10,n) as datetime,
            take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as nanotimestamp,
            100..599 as qty)
            re = select * from loadTable("{db_name}",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_datehour(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(
            "t=keyedTable(`qty,'A1' 'A2' as sym,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty)")
        upsert = ddb.tableUpsert(tableName="t", ddbSession=conn)
        sym = ['A3', 'A4', 'A5', 'A6', 'A7']
        time = np.array(['2021-01-01T03', '2021-01-01T04', 'NaT', 'NaT', '1960-01-01T03'], dtype="datetime64")
        qty = np.arange(3, 8)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table("A"+string(1..7) as sym, datehour([2021.01.01T01:01:01,2021.01.01T02:01:01,2021.01.01T03:01:01,2021.01.01T04:01:01,NULL,NULL,1960.01.01T03:01:01])  as time, 1..7 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_datehour_null(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty)")
        upsert = ddb.tableUpsert(tableName="t", ddbSession=conn)
        n = 100
        data = pd.DataFrame({'time': np.array(np.repeat('Nat', n), dtype="datetime64[ns]"),
                             'qty': np.arange(3, n + 3)})
        upsert.upsert(data)
        script = '''
            n = 100
            tmp=table(datehour([2021.01.01T01:01:01,2021.01.01T02:01:01].join(take(datehour(),n)))  as time, 1..(n+2) as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_datehour(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(
            "t=indexedTable(`qty,'A1' 'A2' as sym,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty)")
        upsert = ddb.tableUpsert(tableName="t", ddbSession=conn)
        sym = ['A3', 'A4', 'A5', 'A6', 'A7']
        time = np.array(['2021-01-01T03', '2021-01-01T04', 'NaT', 'NaT', '1960-01-01T03'], dtype="datetime64")
        qty = np.arange(3, 8)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
            tmp=table("A"+string(1..7) as sym, datehour([2021.01.01T01:01:01,2021.01.01T02:01:01,2021.01.01T03:01:01,2021.01.01T04:01:01,NULL,NULL,1960.01.01T03:01:01])  as time, 1..7 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_datehour_null(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=indexedTable(`qty,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty)")
        upsert = ddb.tableUpsert(tableName="t", ddbSession=conn)
        n = 100
        data = pd.DataFrame({'time': np.array(np.repeat('Nat', n), dtype="datetime64[ns]"),
                             'qty': np.arange(3, n + 3)})
        upsert.upsert(data)
        script = '''
            n = 100
            tmp=table(datehour([2021.01.01T01:01:01,2021.01.01T02:01:01].join(take(datehour(),n)))  as time, 1..(n+2) as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_datehour(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(datehour(2020.01.01T01:01:01) as time, 1 as qty)
            db=database(dbPath,RANGE,0 100 200 300 400 601)
            pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        upsert = ddb.tableUpsert(db_name, "pt", conn, keyColNames=["qty"])
        n = 500
        time = pd.date_range(start='2020-01-01T01', periods=n, freq='h')
        qty = np.arange(1, n + 1)
        data = pd.DataFrame({'time': time, 'qty': qty})
        upsert.upsert(data)
        script = f'''
            n = 500
            ex = table((datehour(2020.01.01T00:01:01)+1..n) as time,1..n as qty)
            re = select * from loadTable("{db_name}",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_key_exist_ignoreNull_False(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64")
        qty = np.arange(1, 11)
        qty1 = np.arange(2, 12)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        data1 = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty1})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == 10
        upsert.upsert(data1)
        num = conn.table(data="t")
        assert num.rows == 11
        script = '''
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_key_exist_ignoreNull_True(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn, ignoreNull=True)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64")
        qty = np.arange(1, 11)
        qty1 = np.arange(2, 12)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        data1 = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty1})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == 10
        upsert.upsert(data1)
        num = conn.table(data="t")
        assert num.rows == 11
        script = '''
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_key_exist_ignoreNull_False(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=indexedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64")
        qty = np.arange(1, 11)
        qty1 = np.arange(2, 12)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        data1 = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty1})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == 10
        upsert.upsert(data1)
        num = conn.table(data="t")
        assert num.rows == 11
        script = '''
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_key_exist_ignoreNull_True(self, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run("t=indexedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn, ignoreNull=True)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64")
        qty = np.arange(1, 11)
        qty1 = np.arange(2, 12)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        data1 = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty1})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert num.rows == 10
        upsert.upsert(data1)
        num = conn.table(data="t")
        assert num.rows == 11
        script = '''
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_DFS_key_exist_ignoreNull_False(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        script = f"""
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
            db=database(dbPath,RANGE,1 6 12,engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `qty,sortColumns=`qty)
            pt.append!(t)
        """
        conn.run(script)
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn,
                                 keyColNames=["qty"])
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64")
        qty1 = np.arange(2, 12)
        data1 = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty1})
        upsert.upsert(data1)
        num = conn.table(data="pt")
        assert num.rows == 11
        script = f'''
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), (select * from loadTable('{db_name}','pt')).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_DFS_key_exist_ignoreNull_True(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        script = f"""
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
            db=database(dbPath,RANGE,1 6 12,engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `qty,sortColumns=`qty)
            pt.append!(t)
        """
        conn.run(script)
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["qty"])
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64")
        qty1 = np.arange(2, 12)
        data1 = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty1})
        upsert.upsert(data1)
        num = conn.table(data="pt")
        assert num.rows == 11
        script = f'''
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), (select * from loadTable('{db_name}','pt')).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_alltype(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["int"])
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([10, 1000], dtype=np.int32),
            'long': np.array([-100000000, 10000000000], dtype=np.int64),
            'date': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
            'time': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                             dtype="datetime64[ms]"),
            'minute': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[m]"),
            'second': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[s]"),
            'datetime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[s]"),
            'datehour': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[h]"),
            'timestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                  dtype="datetime64[ms]"),
            'nanotime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[ns]"),
            'nanotimestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                      dtype="datetime64[ns]"),
            'float': np.array([2.2134500, np.nan], dtype='float32'),
            'double': np.array([3.214, np.nan], dtype='float64'),
            'symbol': np.array(['sym1', 'sym2'], dtype='object'),
            'string': np.array(['str1', 'str2'], dtype='object'),
            'ipaddr': np.array(["192.168.1.1", "0.0.0.0"], dtype='object'),
            'uuid': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                             dtype='object'),
            'int128': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"],
                               dtype='object'),
            'blob': np.array(['blob1', 'blob2'], dtype='object')
        })
        upsert.upsert(df)
        script = f"""
            symbolV = symbol[`sym1,'sym2']
            ipV = ipaddr["192.168.1.1", "0.0.0.0"]
            uuidV = uuid["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"]
            int128V = int128["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"]
            blobV = blob['blob1', 'blob2']
            t=table([bool(1),bool(0)] as bool,
            [char(1),char(-1)] as char,
            [short(-10),short(1000)] as short,
            [int(10),int(1000)] as int,
            [long(-100000000),long(10000000000)] as long,
            [date(2012.02.03T01:02:03.456789123), date(2013.04.02T02:05:06.123456789)] as date,
            [time(2012.02.03T01:02:03.456789123), time(2013.04.02T02:05:06.123456789)] as time,
            [minute(2012.02.03T01:02:03.456789123), minute(2013.04.02T02:05:06.123456789)] as minute,
            [second(2012.02.03T01:02:03.456789123), second(2013.04.02T02:05:06.123456789)] as second,
            [datetime(2012.02.03T01:02:03.456789123), datetime(2013.04.02T02:05:06.123456789)] as datetime,
            [datehour(2012.02.03T01:02:03.456789123), datehour(2013.04.02T02:05:06.123456789)] as datehour,
            [timestamp(2012.02.03T01:02:03.456789123), timestamp(2013.04.02T02:05:06.123456789)] as timestamp,
            [nanotime(2012.02.03T01:02:03.456789123), nanotime(2013.04.02T02:05:06.123456789)] as nanotime,
            [nanotimestamp(2012.02.03T01:02:03.456789123), nanotimestamp(2013.04.02T02:05:06.123456789)] as nanotimestamp,
            [float(2.2134500),float(NULL)] as float,
            [double(3.214),double(NULL)] as double,
            ['sym1','sym2' ] as sym,
            [`str1,'str2'] as str,
            ipV as ipaddr,
            uuidV as uuid,
            int128V as int128,
            blobV as blob)
            re = select * from loadTable("{db_name}",`pt)
            each(eqObj,t.values(),re.values())
        """
        re = conn.run(script)
        assert_array_equal(re, [True for _ in range(22)])

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_alltype_arrayvector(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
            db=database(dbPath,VALUE,1 10000,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["id"])
        df = pd.DataFrame({
            'id': np.array([1, 10000], dtype="int32"),
            'bool': [np.array([True, False], dtype=np.bool8), np.array([True, False], dtype=np.bool8)],
            'char': [np.array([1, -1], dtype=np.int8), np.array([1, -1], dtype=np.int8)],
            'short': [np.array([-10, 1000], dtype=np.int16), np.array([-10, 1000], dtype=np.int16)],
            'int': [np.array([10, 1000], dtype=np.int32), np.array([10, 1000], dtype=np.int32)],
            'long': [np.array([-100000000, 10000000000], dtype=np.int64),
                     np.array([-100000000, 10000000000], dtype=np.int64)],
            'date': [
                np.array(["1970-01-01T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]")],
            'time': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ms]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ms]")],
            'minute': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[m]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[m]")],
            'second': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[s]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[s]")],
            'datetime': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]")],
            'datehour': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[h]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[h]")],
            'timestamp': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]")],
            'nanotime': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ns]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ns]")],
            'nanotimestamp': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]")],
            'float': [np.array([2.2134500, np.nan], dtype='float32'), np.array([2.2134500, np.nan], dtype='float32')],
            'double': [np.array([3.214, np.nan], dtype='float64'), np.array([3.214, np.nan], dtype='float64')],
            'ipaddr': [np.array(["192.168.1.1", "0.0.0.0"], dtype='object'),
                       np.array(["192.168.1.1", "0.0.0.0"], dtype='object')],
            'uuid': [np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                              dtype='object'),
                     np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                              dtype='object')],
            'int128': [
                np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object'),
                np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object')]
        })
        upsert.upsert(df)
        assert_frame_equal(df, conn.run("select * from pt"))

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_1(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["int"])
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([10, 1000], dtype=np.int32),
            'long': np.array([-100000000, 10000000000], dtype=np.int64),
            'date': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object'),
            'time': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                             dtype="datetime64[ms]"),
            'minute': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[m]"),
            'second': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[s]"),
            'datetime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[s]"),
            'datehour': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[h]"),
            'timestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                  dtype="datetime64[ms]"),
            'nanotime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[ns]"),
            'nanotimestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                      dtype="datetime64[ns]"),
            'float': np.array([2.2134500, np.nan], dtype='float32'),
            'double': np.array([3.214, np.nan], dtype='float64'),
            'symbol': np.array(['sym1', 'sym2'], dtype='object'),
            'string': np.array(['str1', 'str2'], dtype='object'),
            'ipaddr': np.array(["192.168.1.1", "0.0.0.0"], dtype='object'),
            'uuid': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                             dtype='object'),
            'int128': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"],
                               dtype='object'),
            'blob': np.array([b'blob1', b'blob2'], dtype='object')
        })
        try:
            upsert.upsert(df)
        except Exception as e:
            assert "The value e1671797c52e15f763380b45e841ec32 (column \"date\", row 0) must be of DATE type." in str(e)

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_2(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["int"])
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([10, 1000], dtype=np.int32),
            'long': np.array(['str1', 'str2'], dtype='object'),
            'date': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
            'time': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                             dtype="datetime64[ms]"),
            'minute': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[m]"),
            'second': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[s]"),
            'datetime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[s]"),
            'datehour': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[h]"),
            'timestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                  dtype="datetime64[ms]"),
            'nanotime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[ns]"),
            'nanotimestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                      dtype="datetime64[ns]"),
            'float': np.array([2.2134500, np.nan], dtype='float32'),
            'double': np.array([3.214, np.nan], dtype='float64'),
            'symbol': np.array(['sym1', 'sym2'], dtype='object'),
            'string': np.array(['str1', 'str2'], dtype='object'),
            'ipaddr': np.array(["192.168.1.1", "0.0.0.0"], dtype='object'),
            'uuid': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                             dtype='object'),
            'int128': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"],
                               dtype='object'),
            'blob': np.array([b'blob1', b'blob2'], dtype='object')
        })
        try:
            upsert.upsert(df)
        except Exception as e:
            assert "The value str1 (column \"long\", row 0) must be of LONG type." in str(e)

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_3(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["int"])
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([10, 1000], dtype=np.int32),
            'long': np.array([-100000000, 10000000000], dtype=np.int64),
            'date': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
            'time': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                             dtype="datetime64[ms]"),
            'minute': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[m]"),
            'second': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[s]"),
            'datetime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[s]"),
            'datehour': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[h]"),
            'timestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                  dtype="datetime64[ms]"),
            'nanotime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[ns]"),
            'nanotimestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                      dtype="datetime64[ns]"),
            'float': np.array([2.2134500, np.nan], dtype='float32'),
            'double': np.array([3.214, np.nan], dtype='float64'),
            'symbol': np.array(['sym1', 'sym2'], dtype='object'),
            'string': np.array(['str1', 'str2'], dtype='object'),
            'ipaddr': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                               dtype='object'),
            'uuid': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                             dtype='object'),
            'int128': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"],
                               dtype='object'),
            'blob': np.array([b'blob1', b'blob2'], dtype='object')
        })
        try:
            upsert.upsert(df)
        except Exception as e:
            assert "The value <NULL> (column \"ipaddr\", row 2) must be of IPADDR type." in str(e)

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_4(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["int"])
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([10, 1000], dtype=np.int32),
            'long': np.array([-100000000, 10000000000], dtype=np.int64),
            'date': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
            'time': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                             dtype="datetime64[ms]"),
            'minute': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[m]"),
            'second': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[s]"),
            'datetime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[s]"),
            'datehour': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[h]"),
            'timestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                  dtype="datetime64[ms]"),
            'nanotime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[ns]"),
            'nanotimestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                      dtype="datetime64[ns]"),
            'float': np.array([2.2134500, np.nan], dtype='float32'),
            'double': np.array([3.214, np.nan], dtype='float64'),
            'symbol': np.array(['sym1', 'sym2'], dtype='object'),
            'string': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[s]"),
            'ipaddr': np.array(["192.168.1.1", "0.0.0.0"], dtype='object'),
            'uuid': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                             dtype='object'),
            'int128': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"],
                               dtype='object'),
            'blob': np.array([b'blob1', b'blob2'], dtype='object')
        })
        try:
            upsert.upsert(df)
        except Exception as e:
            assert "The value <NULL> (column \"int128\", row 2) must be of INT128 type." in str(e)

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_5(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["int"])
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([10, 1000], dtype=np.int32),
            'long': np.array([-100000000, 10000000000], dtype=np.int64),
            'date': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
            'time': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                             dtype="datetime64[ms]"),
            'minute': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[m]"),
            'second': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[s]"),
            'datetime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[s]"),
            'datehour': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[h]"),
            'timestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                  dtype="datetime64[ms]"),
            'nanotime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[ns]"),
            'nanotimestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                      dtype="datetime64[ns]"),
            'float': np.array([2.2134500, np.nan], dtype='float32'),
            'double': np.array([3.214, np.nan], dtype='float64'),
            'symbol': np.array(['sym1', 'sym2'], dtype='object'),
            'string': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[s]"),
            'ipaddr': np.array(["192.168.1.1", "0.0.0.0"], dtype='object'),
            'uuid': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                             dtype='object'),
            'int128': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                               dtype='object'),
            'blob': np.array([b'blob1', b'blob2'], dtype='object')
        })
        try:
            upsert.upsert(df)
        except Exception as e:
            assert "The value <NULL> (column \"int128\", row 2) must be of INT128 type." in str(e)

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_array_vector_not_match_1(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
            db=database(dbPath,VALUE,1 10000,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["id"])
        df = pd.DataFrame({
            'id': np.array([1, 10000], dtype="int32"),
            'bool': [np.array([True, False], dtype=np.bool8), np.array([True, False], dtype=np.bool8)],
            'char': [np.array([1, -1], dtype=np.int8), np.array([1, -1], dtype=np.int8)],
            'short': [np.array([-10, 1000], dtype=np.int16), np.array([-10, 1000], dtype=np.int16)],
            'int': [np.array([10, 1000], dtype=np.int32), np.array([10, 1000], dtype=np.int32)],
            'long': [np.array([-100000000, 10000000000], dtype=np.int64),
                     np.array([-100000000, 10000000000], dtype=np.int64)],
            'date': [
                np.array(["1970-01-01T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]")],
            'time': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ms]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ms]")],
            'minute': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[m]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[m]")],
            'second': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[s]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[s]")],
            'datetime': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]")],
            'datehour': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[h]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[h]")],
            'timestamp': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]")],
            'nanotime': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ns]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ns]")],
            'nanotimestamp': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]")],
            'float': [np.array([2.2134500, np.nan], dtype='float32'), np.array([2.2134500, np.nan], dtype='float32')],
            'double': [np.array([3.214, np.nan], dtype='float64'), np.array([3.214, np.nan], dtype='float64')],
            'ipaddr': [np.array([-100000000, 10000000000], dtype=np.int64),
                       np.array([-100000000, 10000000000], dtype=np.int64)],
            'uuid': [np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                              dtype='object'),
                     np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                              dtype='object')],
            'int128': [
                np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object'),
                np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object')]
        })
        try:
            upsert.upsert(df)
        except Exception as e:
            result = "The value [ -100000000 10000000000] (column \"ipaddr\", row 0) must be of IPADDR[] type" in str(e)
            assert result

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_array_vector_not_match_2(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
            db=database(dbPath,VALUE,1 10000,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["id"])
        df = pd.DataFrame({
            'id': np.array([1, 10000], dtype="int32"),
            'bool': [np.array([True, False], dtype=np.bool8), np.array([True, False], dtype=np.bool8)],
            'char': [np.array([1, -1], dtype=np.int8), np.array([1, -1], dtype=np.int8)],
            'short': [np.array([-10, 1000], dtype=np.int16), np.array([-10, 1000], dtype=np.int16)],
            'int': [np.array([10, 1000], dtype=np.int32), np.array([10, 1000], dtype=np.int32)],
            'long': [np.array([10, 1000], dtype=np.int64), np.array([10, 1000], dtype=np.int64)],
            'date': [
                np.array(["1970-01-01T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]")],
            'time': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ms]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ms]")],
            'minute': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[m]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[m]")],
            'second': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[s]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[s]")],
            'datetime': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]")],
            'datehour': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[h]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[h]")],
            'timestamp': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]")],
            'nanotime': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ns]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ns]")],
            'nanotimestamp': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]")],
            'float': [np.array([2.2134500, np.nan], dtype='float32'), np.array([2.2134500, np.nan], dtype='float32')],
            'double': [np.array([3.214, np.nan], dtype='float64'), np.array([3.214, np.nan], dtype='float64')],
            'ipaddr': [np.array(["192.168.1.1", "0.0.0.0"], dtype='object'),
                       np.array(["192.168.1.1", "0.0.0.0"], dtype='object')],
            'uuid': [np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                              dtype='object'),
                     np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                              dtype='object')],
            'int128': [
                np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object'),
                np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object')]
        })
        upsert.upsert(df)
        res = conn.run(f'select * from loadTable("{db_name}",`pt)')
        assert_frame_equal(res, df)

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_array_vector_not_match_3(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
            db=database(dbPath,VALUE,1 10000,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        upsert = ddb.tableUpsert(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["id"])
        df = pd.DataFrame({
            'id': np.array([1, 10000], dtype="int32"),
            'bool': [np.array([True, False], dtype=np.bool8), np.array([True, False], dtype=np.bool8)],
            'char': [np.array([1, -1], dtype=np.int8), np.array([1, -1], dtype=np.int8)],
            'short': [np.array([-10, 1000], dtype=np.int16), np.array([-10, 1000], dtype=np.int16)],
            'int': [np.array([10, 1000], dtype=np.int32), np.array([10, 1000], dtype=np.int32)],
            'long': [np.array([-100000000, 10000000000], dtype=np.int64),
                     np.array([-100000000, 10000000000], dtype=np.int64)],
            'date': [
                np.array(["1970-01-01T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]")],
            'time': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ms]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ms]")],
            'minute': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[m]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[m]")],
            'second': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[s]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[s]")],
            'datetime': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]")],
            'datehour': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[h]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[h]")],
            'timestamp': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]")],
            'nanotime': [
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ns]"),
                np.array(["1970-01-01T01:02:03.456789123", "1970-01-01T02:05:06.123456789"], dtype="datetime64[ns]")],
            'nanotimestamp': [
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]"),
                np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]")],
            'float': [np.array([2.2134500, np.nan], dtype='float32'), np.array([2.2134500, np.nan], dtype='float32')],
            'double': [np.array([3.214, np.nan], dtype='float64'), np.array([3.214, np.nan], dtype='float64')],
            'ipaddr': [np.array([True, False], dtype=np.bool8), np.array([True, False], dtype=np.bool8)],
            'uuid': [np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                              dtype='object'),
                     np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                              dtype='object')],
            'int128': [
                np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object'),
                np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object')]
        })
        try:
            upsert.upsert(df)
        except Exception as e:
            result = "The value [ True False] (column \"ipaddr\", row 0) must be of IPADDR[] type" in str(e)
            assert result

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableUpsert_dfs_table_alltype(self, compress):
        func_name = inspect.currentframe().f_code.co_name + f"_compress_{compress}"
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD, compress=compress)
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        upsert = ddb.TableUpserter(dbPath=db_name, tableName="pt", ddbSession=conn, ignoreNull=True,
                                   keyColNames=["int"])
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([10, 1000], dtype=np.int32),
            'long': np.array([-100000000, 10000000000], dtype=np.int64),
            'date': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
            'time': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                             dtype="datetime64[ms]"),
            'minute': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[m]"),
            'second': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                               dtype="datetime64[s]"),
            'datetime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[s]"),
            'datehour': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[h]"),
            'timestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                  dtype="datetime64[ms]"),
            'nanotime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                 dtype="datetime64[ns]"),
            'nanotimestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"],
                                      dtype="datetime64[ns]"),
            'float': np.array([2.2134500, np.nan], dtype='float32'),
            'double': np.array([3.214, np.nan], dtype='float64'),
            'symbol': np.array(['sym1', 'sym2'], dtype='object'),
            'string': np.array(['str1', 'str2'], dtype='object'),
            'ipaddr': np.array(["192.168.1.1", "0.0.0.0"], dtype='object'),
            'uuid': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"],
                             dtype='object'),
            'int128': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"],
                               dtype='object'),
            'blob': np.array(['blob1', 'blob2'], dtype='object')
        })
        upsert.upsert(df)
        script = f"""
            symbolV = symbol[`sym1,'sym2']
            ipV = ipaddr["192.168.1.1", "0.0.0.0"]
            uuidV = uuid["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"]
            int128V = int128["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"]
            blobV = blob['blob1', 'blob2']
            t=table([bool(1),bool(0)] as bool,
            [char(1),char(-1)] as char,
            [short(-10),short(1000)] as short,
            [int(10),int(1000)] as int,
            [long(-100000000),long(10000000000)] as long,
            [date(2012.02.03T01:02:03.456789123), date(2013.04.02T02:05:06.123456789)] as date,
            [time(2012.02.03T01:02:03.456789123), time(2013.04.02T02:05:06.123456789)] as time,
            [minute(2012.02.03T01:02:03.456789123), minute(2013.04.02T02:05:06.123456789)] as minute,
            [second(2012.02.03T01:02:03.456789123), second(2013.04.02T02:05:06.123456789)] as second,
            [datetime(2012.02.03T01:02:03.456789123), datetime(2013.04.02T02:05:06.123456789)] as datetime,
            [datehour(2012.02.03T01:02:03.456789123), datehour(2013.04.02T02:05:06.123456789)] as datehour,
            [timestamp(2012.02.03T01:02:03.456789123), timestamp(2013.04.02T02:05:06.123456789)] as timestamp,
            [nanotime(2012.02.03T01:02:03.456789123), nanotime(2013.04.02T02:05:06.123456789)] as nanotime,
            [nanotimestamp(2012.02.03T01:02:03.456789123), nanotimestamp(2013.04.02T02:05:06.123456789)] as nanotimestamp,
            [float(2.2134500),float(NULL)] as float,
            [double(3.214),double(NULL)] as double,
            ['sym1','sym2' ] as sym,
            [`str1,'str2'] as str,
            ipV as ipaddr,
            uuidV as uuid,
            int128V as int128,
            blobV as blob)
            re = select * from loadTable("{db_name}",`pt)
            each(eqObj,t.values(),re.values())
        """
        re = conn.run(script)
        assert_array_equal(re, [True for _ in range(22)])

    def test_TableUpsert_no_pandasWarning(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run("""
            tmp = table(1..2 as a, ["192.168.1.113", "192.168.1.123"] as b)
            t=keyedTable(`a, tmp)
        """)
        df = conn.run("t")
        import warnings
        warnings.filterwarnings('error')
        try:
            pt = ddb.tableUpsert(tableName="t", ddbSession=conn)
            pt.upsert(df)
            del pt
        except Warning:
            assert 0, "expect no warning, but catched"

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
    def test_TableUpsert_allNone_tables_with_numpyArray(self, val, valstr, valdecimal):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        df = pd.DataFrame({
            'ckeycol': np.array([1, 2, 3], dtype='int32'),
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
            'ckeycol': keys.DT_INT,
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
        conn.upload({'tab': df})
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,1],,'TSDB')
            pt = db.createPartitionedTable(tab, `pt, `ckeycol,,`ckeycol)
        ''')
        upserter = ddb.TableUpserter(db_name, 'pt', conn, keyColNames=['ckeycol'])
        upserter.upsert(df)
        assert conn.run(f"rows = exec count(*) from loadTable('{db_name}', 'pt');rows") == 3
        assert conn.run(f"""
            ex_tab = select * from loadTable("{db_name}", "pt");
            res = bool([]);
            for(i in 1:tab.columns()){{res.append!(ex_tab.column(i).isNull())}};
            all(res)
        """)
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['INT', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128', 'BLOB', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(schema, ex_types)

    @pytest.mark.parametrize('val, valstr, valdecimal', test_dataArray, ids=[str(x[0]) for x in test_dataArray])
    def test_TableUpsert_allNone_tables_with_pythonList(self, val, valstr, valdecimal):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        db_name = f"dfs://{func_name}"
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        df = pd.DataFrame({
            'ckeycol': np.array([1, 2, 3], dtype='int32'),
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
            'ckeycol': keys.DT_INT,
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
        conn.upload({'tab': df})
        conn.run(f'''
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,1],,'TSDB')
            pt = db.createPartitionedTable(tab, `pt, `ckeycol,,`ckeycol)
        ''')
        upserter = ddb.TableUpserter(db_name, 'pt', conn, keyColNames=['ckeycol'])
        upserter.upsert(df)
        assert conn.run(f"rows = exec count(*) from loadTable('{db_name}', 'pt');rows") == 3
        assert conn.run(f"""
            ex_tab = select * from loadTable("{db_name}", "pt");
            res = bool([]);
            for(i in 1:tab.columns()){{res.append!(ex_tab.column(i).isNull())}};
            all(res)
        """)
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['INT', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128', 'BLOB', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(schema, ex_types)

    def test_TableUpsert_upsert_after_session_deconstructed(self):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(f"""
            t = table(1..2 as a, ["192.168.1.113", "192.168.1.123"] as b)
            kt = keyedTable(`a,t)
            share kt as {func_name}_share_t
        """)
        df = pd.DataFrame({'a': [3, 4], 'b': ['1.1.1.1', '2.2.2.2']})
        try:
            tu = ddb.TableUpserter(tableName=f"{func_name}_share_t", ddbSession=conn, keyColNames=['a'])
            conn.close()
            tu.upsert(df)
        except RuntimeError as e:
            assert "Session has been closed." in str(e)
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        tu = ddb.TableUpserter(tableName=f"{func_name}_share_t", ddbSession=conn, keyColNames=['a'])
        del conn
        tu.upsert(df)
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        assert conn.run(f"""
            res = select * from {func_name}_share_t;
            ex = table(1 2 3 4 as a, ['192.168.1.113','192.168.1.123', '1.1.1.1', '2.2.2.2'] as b);
            each(eqObj, res.values(), ex.values())
        """).all()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableUpsert_upsert_dataframe_with_numpy_order(self, _python_list, _compress, _order):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        for _i in range(10):
            row_data = [_i, False, _i, _i, _i, _i,
                        np.datetime64(_i, "D").astype("datetime64[ns]"),
                        np.datetime64(_i, "M").astype("datetime64[ns]"),
                        np.datetime64(_i, "ms").astype("datetime64[ns]"),
                        np.datetime64(_i, "m").astype("datetime64[ns]"),
                        np.datetime64(_i, "s").astype("datetime64[ns]"),
                        np.datetime64(_i, "s").astype("datetime64[ns]"),
                        np.datetime64(_i, "ms").astype("datetime64[ns]"),
                        np.datetime64(_i, "ns").astype("datetime64[ns]"),
                        np.datetime64(_i, "ns").astype("datetime64[ns]"),
                        np.datetime64(_i, "h").astype("datetime64[ns]"),
                        _i, _i, 'sym', 'str', 'blob', "1.1.1.1", "5d212a78-cc48-e3b1-4235-b4d91473ee87",
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
        conn1.run(f"""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
            colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(2), DECIMAL64(11), DECIMAL128(36)];
            t=table(1:0, colName,colType)
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        up = ddb.TableUpserter(dbPath=db_name, tableName='pt', ddbSession=conn1, keyColNames=['index'])
        up.upsert(df)
        conn1.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, false, i,i,i,i,i,i+23640,i,i,i,i,i,i,i,i,i,i, 'sym','str', 'blob', ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"), decimal32('-2.11', 2), decimal64('0.0', 11), decimal128('-1.1', 36))
            }
        """)
        res = conn1.run(f"""
            ex = select * from objByName(`t);
            res = select * from loadTable("{db_name}", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        tys = conn1.run(f"schema(loadTable('{db_name}', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(2)', 'DECIMAL64(11)',
                    'DECIMAL128(36)']
        assert_array_equal(tys, ex_types)

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableUpsert_upsert_dataframe_array_vector_with_numpy_order(self, _compress, _order, _python_list):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        for _i in range(10):
            row_data = [_i, [False], [_i], [_i], [_i], [_i], [_i], [_i], [_i], [_i], [_i], [_i], [_i], [_i], [_i], [_i],
                        [_i], [_i],
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
        conn1.run(f"""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
            colType = [LONG, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[], DECIMAL32(2)[], DECIMAL64(11)[], DECIMAL128(36)[]];
            t=table(1:0, colName,colType)
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        up = ddb.TableUpserter(dbPath=db_name, tableName='pt', ddbSession=conn1, keyColNames=['index'])
        up.upsert(df)
        conn1.run("""
            for(i in 0:10){
                tableInsert(t, i, 0, i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i, ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"), decimal32('-2.11', 2), decimal64('0.0', 11), decimal128('-1.1', 36))
            }
            tableInsert(objByName(`t), 10, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
        """)
        res = conn1.run(f"""
            ex = select * from objByName(`t);
            res = select * from loadTable("{db_name}", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        tys = conn1.run(f"schema(loadTable('{db_name}', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]', 'DECIMAL32(2)[]', 'DECIMAL64(11)[]',
                    'DECIMAL128(36)[]']
        assert_array_equal(tys, ex_types)

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableUpsert_upsert_null_dataframe_with_numpy_order(self, _compress, _order, _python_list):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        origin_nulls = [None, np.nan, pd.NaT]
        for _i in range(7):
            row_data = random.choices(origin_nulls, k=26)
            data.append([_i, *row_data])
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
        conn1.run(f"""
        colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
        colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(2), DECIMAL64(11), DECIMAL128(36)];
        t=table(1:0, colName,colType)
        dbPath = "{db_name}"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[LONG,1],,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        up = ddb.TableUpserter(dbPath=db_name, tableName='pt', ddbSession=conn1, keyColNames=['index'])
        up.upsert(df)
        conn1.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = conn1.run(f"""
            ex = select * from objByName(`t);
            res = select * from loadTable("{db_name}", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        tys = conn1.run(f"schema(loadTable('{db_name}', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(2)', 'DECIMAL64(11)',
                    'DECIMAL128(36)']
        assert_array_equal(tys, ex_types)

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableUpsert_upsert_null_dataframe_array_vector_with_numpy_order(self, _compress, _order, _python_list):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        origin_nulls = [[None], [np.nan], [pd.NaT]]
        for _i in range(7):
            row_data = random.choices(origin_nulls, k=23)
            data.append([_i, *row_data])
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
        conn1.run(f"""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
            colType = [LONG, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[], DECIMAL32(2)[], DECIMAL64(11)[], DECIMAL128(36)[]];
            t=table(1:0, colName,colType)
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        up = ddb.TableUpserter(dbPath=db_name, tableName='pt', ddbSession=conn1, keyColNames=['index'])
        up.upsert(df)
        conn1.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = conn1.run(f"""
            ex = select * from objByName(`t);
            res = select * from loadTable("{db_name}", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        tys = conn1.run(f"schema(loadTable('{db_name}', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]', 'DECIMAL32(2)[]', 'DECIMAL64(11)[]',
                    'DECIMAL128(36)[]']
        assert_array_equal(tys, ex_types)

    def test_TableUpsert_over_length(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * 256 * 1024
        tbname = 't_' + random_string(5)
        dbPath = f"dfs://{func_name}"
        conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[STRING,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        upserter = ddb.tableUpsert(dbPath=dbPath, tableName=tbname, ddbSession=conn, keyColNames=['test'])
        df = pd.DataFrame({'id': [data], 'test': '0'})
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            upserter.upsert(df)
        conn.close()

    def test_TableUpsert_max_length_string_dfs_table(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * (256 * 1024 - 1)
        tbname = 't_' + random_string(5)
        dbPath = f"dfs://{func_name}"
        conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[STRING,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        upserter = ddb.tableUpsert(dbPath=dbPath, tableName=tbname, ddbSession=conn, keyColNames=['test'])
        df = pd.DataFrame({'id': [data], 'test': '0'})
        upserter.upsert(df)
        assert conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 64 * 1024 - 1
        conn.close()

    def test_TableUpsert_max_length_symbol_dfs_table(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * 255
        tbname = 't_' + random_string(5)
        dbPath = f"dfs://{func_name}"
        conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[SYMBOL,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        upserter = ddb.tableUpsert(dbPath=dbPath, tableName=tbname, ddbSession=conn, keyColNames=['test'])
        df = pd.DataFrame({'id': [data], 'test': '0'})
        upserter.upsert(df)
        assert conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 255
        conn.close()

    def test_TableUpsert_max_length_blob_dfs_table(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * 256 * 1024
        tbname = 't_' + random_string(5)
        dbPath = f"dfs://{func_name}"
        conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[BLOB,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        upserter = ddb.tableUpsert(dbPath=dbPath, tableName=tbname, ddbSession=conn, keyColNames=['test'])
        df = pd.DataFrame({'id': [data], 'test': '0'})
        upserter.upsert(df)
        assert conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 256 * 1024
        conn.close()
