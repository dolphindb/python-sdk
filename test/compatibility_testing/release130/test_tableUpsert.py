import decimal
import random

import pytest
from numpy.testing import *

from setup.prepare import *
from setup.settings import *
from setup.utils import get_pid, random_string


class TesttableUpsert:
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
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_date(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64[ns]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert (num.rows == 10)
        script = '''
        tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_dateToMonth(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, MONTH, INT]) as t")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64[ns]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert (num.rows == 10)
        script = '''
        tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01M, NULL, 1965.07M, NULL, 2020.12M, 1970.01M, NULL, NULL, NULL, 2009.08M] as date, 1..10 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_month(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`month`qty, [SYMBOL, MONTH, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        month = np.array(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'month': month, 'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert (num.rows == 5)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [1965.08M, NULL, 2012.02M, 2012.03M, NULL] as month, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_monthToDate(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`month`qty, [SYMBOL, DATE, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        month = np.array(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'month': month, 'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert (num.rows == 5)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [1965.08.01, NULL, 2012.02.01, 2012.03.01, NULL] as month, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_time(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, TIME, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_minute(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, MINUTE, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00m, 05:12m, NULL, NULL, 23:59m] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_second(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, SECOND, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00, 05:12:48, NULL, NULL, 23:59:59] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_datetime(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, DATETIME, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_timestamp(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, TIMESTAMP, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.008', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00.000, 2015.08.26T05:12:48.008, NULL, NULL, 2015.06.09T23:59:59.999] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_nanotime(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, NANOTIME, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                         '2015-06-09T23:59:59.999008007'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_nanotimestamp(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, NANOTIMESTAMP, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                         '2015-06-09T23:59:59.999008007'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_date_null(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(np.repeat('Nat', 10), dtype="datetime64[D]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, take(date(),10) as date, 1..10 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_nanotimestamp_null(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, NANOTIMESTAMP, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(np.repeat('Nat', 5), dtype="datetime64[ns]")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, take(nanotimestamp(),5) as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_all_time_type(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "share keyedTable(`qty,1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = list(map(str, np.arange(100000, 600000)))
        date = np.array(np.tile(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            50000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100000), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                    100000), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'], 100000),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '2015-06-09T23:59:59.999008007'], 100000), dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month': month, 'time': time, 'minute': time, 'second': second,
                             'datetime': second, 'timestamp': time, 'nanotime': nanotime, 'nanotimestamp': nanotime,
                             'qty': qty})
        print(data)
        upsert.upsert(data)
        print("-------------")
        script = '''
        n = 500000
        tmp=table(string(100000..599999) as sym, take([2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05],n) as date,take([1965.08M, NULL, 2012.02M, 2012.03M, NULL],n) as month,
        take([00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999],n) as time, take([00:00m, 05:12m, NULL, NULL, 23:59m],n) as minute, take([00:00:00, 05:12:48, NULL, NULL, 23:59:59],n) as second,take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59],n) as datetime,
        take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 2015.06.09T23:59:59.999],n) as timestamp,take([00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007],n) as nanotime,take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007],n) as nanotimestamp,
        100000..599999 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        print(script)
        assert_array_equal(re, [True, True, True, True, True, True, True, True, True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_all_time_type(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "share indexedTable(`qty,1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        sym = list(map(str, np.arange(100000, 600000)))
        date = np.array(np.tile(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            50000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100000), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                    100000), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'], 100000),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '2015-06-09T23:59:59.999008007'], 100000), dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month': month, 'time': time, 'minute': time, 'second': second,
                             'datetime': second, 'timestamp': time, 'nanotime': nanotime, 'nanotimestamp': nanotime,
                             'qty': qty})
        upsert.upsert(data)
        script = '''
        n = 500000
        tmp=table(string(100000..599999) as sym, take([2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05],n) as date,take([1965.08M, NULL, 2012.02M, 2012.03M, NULL],n) as month,
        take([00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999],n) as time, take([00:00m, 05:12m, NULL, NULL, 23:59m],n) as minute, take([00:00:00, 05:12:48, NULL, NULL, 23:59:59],n) as second,take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59],n) as datetime,
        take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 2015.06.09T23:59:59.999],n) as timestamp,take([00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007],n) as nanotime,take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007],n) as nanotimestamp,
        100000..599999 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True, True, True, True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_all_time_types(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT])
        db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        upsert = ddb.tableUpsert("dfs://tableUpsert_test", "pt", conn, False, ["qty"])
        sym = list(map(str, np.arange(100000, 600000)))
        date = np.array(np.tile(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            50000), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100000), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                    100000), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'], 100000),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '2015-06-09T23:59:59.999008007'], 100000), dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month': month, 'time': time, 'minute': time, 'second': second,
                             'datetime': second, 'timestamp': time, 'nanotime': nanotime, 'nanotimestamp': nanotime,
                             'qty': qty})
        num = upsert.upsert(data)
        script = '''
        n = 500000
        tmp=table(string(100000..599999) as sym, take([2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05],n) as date,take([1965.08M, NULL, 2012.02M, 2012.03M, NULL],n) as month,
        take([00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999],n) as time, take([00:00m, 05:12m, NULL, NULL, 23:59m],n) as minute, take([00:00:00, 05:12:48, NULL, NULL, 23:59:59],n) as second,take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59],n) as datetime,
        take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 2015.06.09T23:59:59.999],n) as timestamp,take([00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007],n) as nanotime,take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007],n) as nanotimestamp,
        100000..599999 as qty)
        re = select * from loadTable("dfs://tableUpsert_test",`pt)
        each(eqObj, tmp.values(), re.values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True, True, True, True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_all_time_type_early_1970(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "share keyedTable(`qty,1000:0, `date`month`datetime `timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        n = 500000
        date = np.array(np.repeat('1960-01-01', n), dtype="datetime64[D]")
        month = np.array(np.repeat('1960-01', n), dtype="datetime64")
        datetime = np.array(np.repeat('1960-01-01T13:30:10', n), dtype="datetime64")
        timestamp = np.array(np.repeat('1960-01-01T13:30:10.008', n), dtype="datetime64")
        nanotimestamp = np.array(np.repeat('1960-01-01 13:30:10.008007006', n), dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame(
            {'date': date, 'month': month, 'datetime': datetime, 'timestamp': timestamp, 'nanotimestamp': nanotimestamp,
             'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert (num.rows == n)
        script = '''
        n = 500000
        tmp=table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01T13:30:10,n) as datetime,
        take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as nanotimestamp,
        100000..599999 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_all_time_type_early_1970(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "share indexedTable(`qty,1000:0, `date`month`datetime `timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT]) as t")
        upsert = ddb.tableUpsert("", "t", conn)
        n = 500000
        date = np.array(np.repeat('1960-01-01', n), dtype="datetime64[D]")
        month = np.array(np.repeat('1960-01', n), dtype="datetime64")
        datetime = np.array(np.repeat('1960-01-01T13:30:10', n), dtype="datetime64")
        timestamp = np.array(np.repeat('1960-01-01T13:30:10.008', n), dtype="datetime64")
        nanotimestamp = np.array(np.repeat('1960-01-01 13:30:10.008007006', n), dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame(
            {'date': date, 'month': month, 'datetime': datetime, 'timestamp': timestamp, 'nanotimestamp': nanotimestamp,
             'qty': qty})
        upsert.upsert(data)
        num = conn.table(data="t")
        assert (num.rows == n)
        script = '''
        n = 500000
        tmp=table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01T13:30:10,n) as datetime,
        take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as nanotimestamp,
        100000..599999 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_all_time_types_early_1970(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(1000:0, `date`month`datetime`timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])
        db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        upsert = ddb.tableUpsert("dfs://tableUpsert_test", "pt", conn, False, ["qty"])
        n = 500000
        date = np.array(np.repeat('1960-01-01', n), dtype="datetime64[D]")
        month = np.array(np.repeat('1960-01', n), dtype="datetime64")
        datetime = np.array(np.repeat('1960-01-01T13:30:10', n), dtype="datetime64")
        timestamp = np.array(np.repeat('1960-01-01T13:30:10.008', n), dtype="datetime64")
        nanotimestamp = np.array(np.repeat('1960-01-01 13:30:10.008007006', n), dtype="datetime64")
        qty = np.arange(100000, 600000)
        data = pd.DataFrame(
            {'date': date, 'month': month, 'datetime': datetime, 'timestamp': timestamp, 'nanotimestamp': nanotimestamp,
             'qty': qty})
        num = upsert.upsert(data)
        script = '''
        n = 500000
        ex = table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01T13:30:10,n) as datetime,
        take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as nanotimestamp,
        100000..599999 as qty)
        re = select * from loadTable("dfs://tableUpsert_test",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_datehour(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "try{undef(`t);undef(`t, SHARED)}catch(ex){};share keyedTable(`qty,'A1' 'A2' as sym,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty) as t")
        upsert = ddb.tableUpsert(tableName="t", ddbSession=conn)
        sym = ['A3', 'A4', 'A5', 'A6', 'A7']
        time = np.array(['2021-01-01T03', '2021-01-01T04', 'NaT', 'NaT', '1960-01-01T03'], dtype="datetime64")
        qty = np.arange(3, 8)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table("A"+string(1..7) as sym, datehour([2021.01.01T01:01:01,2021.01.01T02:01:01,2021.01.01T03:01:01,2021.01.01T04:01:01,NULL,NULL,1960.01.01T03:01:01])  as time, 1..7 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_datehour_null(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "try{undef(`t);undef(`t, SHARED)}catch(ex){};share keyedTable(`qty,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty) as t")
        upsert = ddb.tableUpsert(tableName="t", ddbSession=conn)
        n = 100000
        time = np.array(np.repeat('Nat', n), dtype="datetime64[ns]")
        qty = np.arange(3, n + 3)
        data = pd.DataFrame({'time': np.array(np.repeat('Nat', n), dtype="datetime64[ns]"),
                             'qty': np.arange(3, n + 3)})
        num = upsert.upsert(data)
        script = '''
        n = 100000
        tmp=table(datehour([2021.01.01T01:01:01,2021.01.01T02:01:01].join(take(datehour(),n)))  as time, 1..(n+2) as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_datehour(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "try{undef(`t);undef(`t, SHARED)}catch(ex){};share indexedTable(`qty,'A1' 'A2' as sym,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty) as t")
        upsert = ddb.tableUpsert(tableName="t", ddbSession=conn)
        sym = ['A3', 'A4', 'A5', 'A6', 'A7']
        time = np.array(['2021-01-01T03', '2021-01-01T04', 'NaT', 'NaT', '1960-01-01T03'], dtype="datetime64")
        qty = np.arange(3, 8)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        num = upsert.upsert(data)
        script = '''
        tmp=table("A"+string(1..7) as sym, datehour([2021.01.01T01:01:01,2021.01.01T02:01:01,2021.01.01T03:01:01,2021.01.01T04:01:01,NULL,NULL,1960.01.01T03:01:01])  as time, 1..7 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_datehour_null(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "try{undef(`t);undef(`t, SHARED)}catch(ex){};share indexedTable(`qty,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty) as t")
        upsert = ddb.tableUpsert(tableName="t", ddbSession=conn)
        n = 100000
        time = np.array(np.repeat('Nat', n), dtype="datetime64[ns]")
        qty = np.arange(3, n + 3)
        data = pd.DataFrame({'time': np.array(np.repeat('Nat', n), dtype="datetime64[ns]"),
                             'qty': np.arange(3, n + 3)})
        num = upsert.upsert(data)
        script = '''
        n = 100000
        tmp=table(datehour([2021.01.01T01:01:01,2021.01.01T02:01:01].join(take(datehour(),n)))  as time, 1..(n+2) as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_datehour(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(datehour(2020.01.01T01:01:01) as time, 1 as qty)
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        upsert = ddb.tableUpsert("dfs://tableUpsert_test", "pt", conn, False, ["qty"])
        n = 500000
        time = pd.date_range(start='2020-01-01T01', periods=n, freq='h')
        qty = np.arange(1, n + 1)
        data = pd.DataFrame({'time': time, 'qty': qty})
        upsert.upsert(data)
        script = '''
        n = 500000
        ex = table((datehour(2020.01.01T00:01:01)+1..n) as time,1..n as qty)
        re = select * from loadTable("dfs://tableUpsert_test",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_key_exist_ignoreNull_False(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn, ignoreNull=False)
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
        assert (num.rows == 10)
        upsert.upsert(data1)
        num = conn.table(data="t")
        assert (num.rows == 11)
        script = '''
        tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_keyedTable_key_exist_ignoreNull_True(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
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
        assert (num.rows == 10)
        upsert.upsert(data1)
        num = conn.table(data="t")
        assert (num.rows == 11)
        script = '''
        tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_key_exist_ignoreNull_False(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share indexedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
        upsert = ddb.tableUpsert(dbPath="", tableName="t", ddbSession=conn, ignoreNull=False)
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
        assert (num.rows == 10)
        upsert.upsert(data1)
        num = conn.table(data="t")
        assert (num.rows == 11)
        script = '''
        tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_indexedTable_key_exist_ignoreNull_True(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share indexedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
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
        assert (num.rows == 10)
        upsert.upsert(data1)
        num = conn.table(data="t")
        assert (num.rows == 11)
        script = '''
        tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        re = conn.run("t")
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_DFS_key_exist_ignoreNull_False(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        script = """
            dbPath = "dfs://tableUpsert_test"
            if(existsDatabase(dbPath))
            dropDatabase(dbPath)
            t = table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
            db=database(dbPath,RANGE,1 6 12,engine='OLAP')
            pt = db.createPartitionedTable(t, `pt, `qty)
            pt.append!(t)
        """
        conn.run(script)
        upsert = ddb.tableUpsert(dbPath="dfs://tableUpsert_test", tableName="pt", ddbSession=conn, ignoreNull=False,
                                 keyColNames=["qty"])
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64")
        qty = np.arange(1, 11)
        qty1 = np.arange(2, 12)
        data1 = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty1})
        upsert.upsert(data1)
        num = conn.table(data="pt")
        assert (num.rows == 11)
        script = '''
        tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
        each(eqObj, tmp.values(), (select * from loadTable('dfs://tableUpsert_test','pt')).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_DFS_key_exist_ignoreNull_True(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        script = """
            dbPath = "dfs://tableUpsert_test"
            if(existsDatabase(dbPath))
            dropDatabase(dbPath)
            t = table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
            db=database(dbPath,RANGE,1 6 12,engine='OLAP')
            pt = db.createPartitionedTable(t, `pt, `qty)
            pt.append!(t)
        """
        conn.run(script)
        upsert = ddb.tableUpsert(dbPath="dfs://tableUpsert_test", tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["qty"])
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64")
        qty = np.arange(1, 11)
        qty1 = np.arange(2, 12)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        data1 = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty1})
        upsert.upsert(data1)
        num = conn.table(data="pt")
        assert (num.rows == 11)
        script = '''
        tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
        each(eqObj, tmp.values(), (select * from loadTable('dfs://tableUpsert_test','pt')).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_alltype(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `int)
        ''')
        upsert = ddb.tableUpsert(dbPath="dfs://tableUpsert_test", tableName="pt", ddbSession=conn, ignoreNull=True,
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
        })
        num1 = upsert.upsert(df)
        script = """
            symbolV = symbol[`sym1,'sym2'] 
            ipV = ipaddr["192.168.1.1", "0.0.0.0"]
            uuidV = uuid["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"] 
            int128V = int128["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"]
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
            int128V as int128)
            re = select * from loadTable("dfs://tableUpsert_test",`pt)
            each(eqObj,t.values(),re.values())
        """
        re = conn.run(script)
        assert_array_equal(re, [True for _ in range(21)])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_1(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `int)
        ''')
        upsert = ddb.tableUpsert(dbPath="dfs://tableUpsert_test", tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["int"])
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([10, 1000], dtype=np.int32),
            'long': np.array([-100000000, 10000000000], dtype=np.int64),
            # 'long': np.array(['str1', 'str2'], dtype='object'),
            'date': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object'),
            # 'date': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
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
        })
        try:
            num1 = upsert.upsert(df)
        except Exception as e:
            print(str(e))
            assert "The value e1671797c52e15f763380b45e841ec32 (column \"date\", row 0) must be of DATE type" in str(e)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_2(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `int)
        ''')
        upsert = ddb.tableUpsert(dbPath="dfs://tableUpsert_test", tableName="pt", ddbSession=conn, ignoreNull=True,
                                 keyColNames=["int"])
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([10, 1000], dtype=np.int32),
            # 'long': np.array([-100000000, 10000000000], dtype=np.int64),
            'long': np.array(['str1', 'str2'], dtype='object'),
            # 'date': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"], dtype='object'),
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
        })
        try:
            num1 = upsert.upsert(df)
        except Exception as e:
            print(str(e))
            assert "The value str1 (column \"long\", row 0) must be of LONG type" in str(e)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_3(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `int)
        ''')
        upsert = ddb.tableUpsert(dbPath="dfs://tableUpsert_test", tableName="pt", ddbSession=conn, ignoreNull=True,
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
        })
        try:
            num1 = upsert.upsert(df)
        except Exception as e:
            result = "The value 5d212a78-cc48-e3b1-4235-b4d91473ee87 (column \"ipaddr\", row 0) must be of IPADDR type" in str(
                e)
            print(result)
            assert (True == result)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_4(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `int)
        ''')
        upsert = ddb.tableUpsert(dbPath="dfs://tableUpsert_test", tableName="pt", ddbSession=conn, ignoreNull=True,
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
        })
        try:
            num1 = upsert.upsert(df)
        except Exception as e:
            print(str(e))
            assert "(column \"string\", row 0) must be of STRING type" in str(e)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_tableUpsert_dfs_table_column_dateType_not_match_5(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `int)
        ''')
        upsert = ddb.tableUpsert(dbPath="dfs://tableUpsert_test", tableName="pt", ddbSession=conn, ignoreNull=True,
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
        })
        try:
            num1 = upsert.upsert(df)
        except Exception as e:
            print(str(e))
            assert "(column \"string\", row 0) must be of STRING type" in str(e)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableUpserter_dfs_table_alltype(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://tableUpsert_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `int)
        ''')
        upsert = ddb.TableUpserter(dbPath="dfs://tableUpsert_test", tableName="pt", ddbSession=conn, ignoreNull=True,
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
        })
        num1 = upsert.upsert(df)
        script = """
            symbolV = symbol[`sym1,'sym2'] 
            ipV = ipaddr["192.168.1.1", "0.0.0.0"]
            uuidV = uuid["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d914731111"] 
            int128V = int128["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e8411112"]
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
            int128V as int128)
            re = select * from loadTable("dfs://tableUpsert_test",`pt)
            each(eqObj,t.values(),re.values())
        """
        re = conn.run(script)
        assert_array_equal(re, [True for _ in range(21)])

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
        }
        conn.upload({'tab': df})
        conn.run('''
            dbPath = "dfs://test_dfs1"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,1],,'OLAP')
            pt = db.createPartitionedTable(tab, `pt, `ckeycol)
        ''')
        upsert = ddb.TableUpserter("dfs://test_dfs1", 'pt', conn, keyColNames=['ckeycol'])
        upsert.upsert(df)
        assert conn.run(r"""rows = exec count(*) from loadTable("dfs://test_dfs1", "pt");rows""") == 3
        assert conn.run(r"""ex_tab = select * from loadTable("dfs://test_dfs1", "pt");
                            res = bool([]);
                            for(i in 1:tab.columns()){res.append!(ex_tab.column(i).isNull())};
                            all(res)""")
        schema = conn.run("schema(tab).colDefs[`typeString]")
        # print(schema)
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(schema, ex_types)
        conn.dropDatabase("dfs://test_dfs1")
        conn.close()

    @pytest.mark.parametrize('val, valstr, valdecimal', test_dataArray, ids=[str(x[0]) for x in test_dataArray])
    def test_TableUpsert_allNone_tables_with_pythonList(self, val, valstr, valdecimal):
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
        }
        conn.upload({'tab': df})
        conn.run('''
            dbPath = "dfs://test_dfs1"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,1],,'OLAP')
            pt = db.createPartitionedTable(tab, `pt, `ckeycol)
        ''')
        upsert = ddb.TableUpserter("dfs://test_dfs1", 'pt', conn, keyColNames=['ckeycol'])
        upsert.upsert(df)
        assert conn.run(r"""rows = exec count(*) from loadTable("dfs://test_dfs1", "pt");rows""") == 3
        assert conn.run(r"""ex_tab = select * from loadTable("dfs://test_dfs1", "pt");
                            res = bool([]);
                            for(i in 1:tab.columns()){res.append!(ex_tab.column(i).isNull())};
                            all(res)""")
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(schema, ex_types)
        conn.dropDatabase("dfs://test_dfs1")
        conn.close()

    def test_TableUpsert_upsert_after_session_deconstructed(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run("""
            t = table(1..2 as a, ["192.168.1.113", "192.168.1.123"] as b)
            kt = keyedTable(`a,t)
            share kt as share_t
        """)
        df = pd.DataFrame({'a': [3, 4], 'b': ['1.1.1.1', '2.2.2.2']})
        # 主动关闭掉连接，抛异常正常
        try:
            tu = ddb.TableUpserter(tableName="share_t", ddbSession=conn, keyColNames=['a'])
            conn.close()
            tu.upsert(df)
        except RuntimeError as e:
            assert "Session has been closed." in str(e)
        # 析构掉session引用，可继续append数据
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        tu = ddb.TableUpserter(tableName="share_t", ddbSession=conn, keyColNames=['a'])
        del conn
        tu.upsert(df)
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        assert conn.run("""res = select * from share_t;
                        ex = table(1 2 3 4 as a, ['192.168.1.113','192.168.1.123', '1.1.1.1', '2.2.2.2'] as b);
                        each(eqObj, res.values(), ex.values())""").all()
        conn.undef('share_t', 'SHARED')
        conn.close()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableUpserter_append_dataframe_with_numpy_order(self, _compress, _order, _python_list):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
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
                        i, i, 'sym', 'str', "1.1.1.1", "5d212a78-cc48-e3b1-4235-b4d91473ee87",
                        "e1671797c52e15f763380b45e841ec32"]
            data.append(row_data)
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cipaddr', 'cuuid',
                                             'cint128'])
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cipaddr', 'cuuid',
                                       'cint128'])
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
        conn1.run("""
        colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cipaddr`cuuid`cint128;        
        colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, UUID, INT128];        
        t=table(1:0, colName,colType)
        dbPath = "dfs://test_dfs1"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[LONG,1],,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `index)
        """)
        upsert = ddb.TableUpserter(dbPath="dfs://test_dfs1", tableName='pt', ddbSession=conn1, keyColNames=['index'])
        upsert.upsert(df)
        conn1.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, false, i,i,i,i,i,i+23640,i,i,i,i,i,i,i,i,i,i, 'sym','str', ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"))
            }
        """)
        res = conn1.run("""ex = select * from objByName(`t);
                           res = select * from loadTable("dfs://test_dfs1", `pt);
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        tys = conn1.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(tys, ex_types)
        conn1.dropDatabase("dfs://test_dfs1")
        conn1.close()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableUpserter_append_null_dataframe_with_numpy_order(self, _compress, _order, _python_list):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        origin_nulls = [None, np.nan, pd.NaT]
        for i in range(7):
            row_data = random.choices(origin_nulls, k=22)
            print(f'row {i}:', row_data)
            data.append([i, *row_data])
        data.append([7] + [None] * 22)
        data.append([8] + [pd.NaT] * 22)
        data.append([9] + [np.nan] * 22)
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cipaddr', 'cuuid',
                                             'cint128'], dtype='object')
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cipaddr', 'cuuid',
                                       'cint128'], dtype='object')
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
        # print(df)
        conn1.run("""
        colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cipaddr`cuuid`cint128;
        colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, UUID, INT128];
        t=table(1:0, colName,colType)
        dbPath = "dfs://test_dfs1"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[LONG,1],,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `index)
        """)
        upsert = ddb.TableUpserter(dbPath="dfs://test_dfs1", tableName='pt', ddbSession=conn1, keyColNames=['index'])
        upsert.upsert(df)
        conn1.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = conn1.run("""ex = select * from objByName(`t);
                           res = select * from loadTable("dfs://test_dfs1", `pt);
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        tys = conn1.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(tys, ex_types)
        conn1.dropDatabase("dfs://test_dfs1")
        conn1.close()

    def test_TableUpserter_over_length(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        tbname = 't_' + random_string(5)
        conn.run(f"""
                share table([`a] as `a) as {tbname}
        """)
        upserter = ddb.tableUpsert(tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'a': ['1' * 4 * 64 * 1024]})
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            upserter.upsert(df)
        conn.run(f'undef `{tbname},SHARED')
        conn.close()


if __name__ == '__main__':
    pytest.main(["-s", "test/test_tableUpsert.py"])
