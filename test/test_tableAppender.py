import decimal
import random
import pytest
from numpy.testing import *
from pandas.testing import *
from setup.prepare import *
from setup.settings import *
from setup.utils import get_pid, random_string


class TestAutoFitTableAppender:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except Exception:
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
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def test_TableAppender_error(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run("share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        with pytest.raises(RuntimeError, match='table must be a DataFrame!'):
            appender.append(object())

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_date(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64[ns]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        appender.append(data)
        num = conn.table(data="t")
        assert (num.rows == 10)
        script = '''
        tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_dateToMonth(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, MONTH, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64[ns]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        appender.append(data)
        num = conn.table(data="t")
        assert (num.rows == 10)
        script = '''
        tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01M, NULL, 1965.07M, NULL, 2020.12M, 1970.01M, NULL, NULL, NULL, 2009.08M] as date, 1..10 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_month(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`month`qty, [SYMBOL, MONTH, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        month = np.array(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'month': month, 'qty': qty})
        appender.append(data)
        num = conn.table(data="t")
        assert (num.rows == 5)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [1965.08M, NULL, 2012.02M, 2012.03M, NULL] as month, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_monthToDate(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`month`qty, [SYMBOL, DATE, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        month = np.array(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'month': month, 'qty': qty})
        appender.append(data)
        num = conn.table(data="t")
        assert (num.rows == 5)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [1965.08.01, NULL, 2012.02.01, 2012.03.01, NULL] as month, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_time(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, TIME, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_minute(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, MINUTE, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00m, 05:12m, NULL, NULL, 23:59m] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_second(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, SECOND, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00, 05:12:48, NULL, NULL, 23:59:59] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_datetime(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, DATETIME, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_timestamp(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, TIMESTAMP, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.008', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                        dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00.000, 2015.08.26T05:12:48.008, NULL, NULL, 2015.06.09T23:59:59.999] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_nanotime(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, NANOTIME, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                         '2015-06-09T23:59:59.999008007'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_nanotimestamp(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                         '2015-06-09T23:59:59.999008007'], dtype="datetime64")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, [2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007] as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_date_null(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = np.repeat(['AAPL', 'GOOG', 'MSFT', 'IBM', 'YHOO'], 2, axis=0)
        date = np.array(np.repeat('Nat', 10), dtype="datetime64[D]")
        qty = np.arange(1, 11)
        data = pd.DataFrame({'sym': sym, 'date': date, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, take(date(),10) as date, 1..10 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_nanotimestamp_null(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("share keyedTable(`qty,1000:0, `sym`time`qty, [SYMBOL, NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        time = np.array(np.repeat('Nat', 5), dtype="datetime64[ns]")
        qty = np.arange(1, 6)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table(`A1`A2`A3`A4`A5 as sym, take(nanotimestamp(),5) as time, 1..5 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])
        conn.run("undef(`t, SHARED)")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_all_time_type(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "share keyedTable(`qty,1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
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
        appender.append(data)
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
    def test_TableAppender_indexedTable_all_time_type(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "share indexedTable(`qty,1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
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
        appender.append(data)
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
    def test_TableAppender_dfs_table_all_time_types(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP, INT])
        db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
        appender.append(data)
        script = '''
        n = 500000
        tmp=table(string(100000..599999) as sym, take([2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05],n) as date,take([1965.08M, NULL, 2012.02M, 2012.03M, NULL],n) as month,
        take([00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999],n) as time, take([00:00m, 05:12m, NULL, NULL, 23:59m],n) as minute, take([00:00:00, 05:12:48, NULL, NULL, 23:59:59],n) as second,take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59],n) as datetime,
        take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 2015.06.09T23:59:59.999],n) as timestamp,take([00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007],n) as nanotime,take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007],n) as nanotimestamp,
        100000..599999 as qty)
        re = select * from loadTable("dfs://AutoFitTableAppender_test",`pt)
        each(eqObj, tmp.values(), re.values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True, True, True, True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_all_time_type_early_1970(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "share keyedTable(`qty,1000:0, `date`month`datetime `timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
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
        appender.append(data)
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
    def test_TableAppender_indexedTable_all_time_type_early_1970(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "share indexedTable(`qty,1000:0, `date`month`datetime `timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT]) as t")
        appender = ddb.tableAppender("", "t", conn)
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
        appender.append(data)
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
    def test_TableAppender_dfs_table_all_time_types_early_1970(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(1000:0, `date`month`datetime`timestamp`nanotimestamp`qty, [DATE,MONTH,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])
        db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
        appender.append(data)
        script = '''
        n = 500000
        ex = table(take(1960.01.01,n) as date,take(1960.01M,n) as month,take(1960.01.01T13:30:10,n) as datetime,
        take(1960.01.01T13:30:10.008,n) as timestamp,take(1960.01.01 13:30:10.008007006,n) as nanotimestamp,
        100000..599999 as qty)
        re = select * from loadTable("dfs://AutoFitTableAppender_test",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_datehour(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "try{undef(`t);undef(`t, SHARED)}catch(ex){};share keyedTable(`qty,'A1' 'A2' as sym,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A3', 'A4', 'A5', 'A6', 'A7']
        time = np.array(['2021-01-01T03', '2021-01-01T04', 'NaT', 'NaT', '1960-01-01T03'], dtype="datetime64")
        qty = np.arange(3, 8)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table("A"+string(1..7) as sym, datehour([2021.01.01T01:01:01,2021.01.01T02:01:01,2021.01.01T03:01:01,2021.01.01T04:01:01,NULL,NULL,1960.01.01T03:01:01])  as time, 1..7 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_keyedTable_datehour_null(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "try{undef(`t);undef(`t, SHARED)}catch(ex){};share keyedTable(`qty,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty) as t")
        appender = ddb.tableAppender("", "t", conn)
        n = 100000
        data = pd.DataFrame({'time': np.array(np.repeat('Nat', n), dtype="datetime64[ns]"),
                             'qty': np.arange(3, n + 3)})
        appender.append(data)
        script = '''
        n = 100000
        tmp=table(datehour([2021.01.01T01:01:01,2021.01.01T02:01:01].join(take(datehour(),n)))  as time, 1..(n+2) as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_indexedTable_datehour(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "try{undef(`t);undef(`t, SHARED)}catch(ex){};share indexedTable(`qty,'A1' 'A2' as sym,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty) as t")
        appender = ddb.tableAppender("", "t", conn)
        sym = ['A3', 'A4', 'A5', 'A6', 'A7']
        time = np.array(['2021-01-01T03', '2021-01-01T04', 'NaT', 'NaT', '1960-01-01T03'], dtype="datetime64")
        qty = np.arange(3, 8)
        data = pd.DataFrame({'sym': sym, 'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        tmp=table("A"+string(1..7) as sym, datehour([2021.01.01T01:01:01,2021.01.01T02:01:01,2021.01.01T03:01:01,2021.01.01T04:01:01,NULL,NULL,1960.01.01T03:01:01])  as time, 1..7 as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_indexedTable_datehour_null(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(
            "try{undef(`t);undef(`t, SHARED)}catch(ex){};share indexedTable(`qty,datehour([2021.01.01T01:01:01,2021.01.01T02:01:01])  as time,1 2 as qty) as t")
        appender = ddb.tableAppender("", "t", conn)
        n = 100000
        data = pd.DataFrame({'time': np.array(np.repeat('Nat', n), dtype="datetime64[ns]"),
                             'qty': np.arange(3, n + 3)})
        appender.append(data)
        script = '''
        n = 100000
        tmp=table(datehour([2021.01.01T01:01:01,2021.01.01T02:01:01].join(take(datehour(),n)))  as time, 1..(n+2) as qty)
        each(eqObj, tmp.values(), (select * from t).values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_datehour(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(datehour(2020.01.01T01:01:01) as time, 1 as qty)
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001)
        pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
        n = 500000
        time = pd.date_range(start='2020-01-01T01', periods=n, freq='h')
        qty = np.arange(1, n + 1)
        data = pd.DataFrame({'time': time, 'qty': qty})
        appender.append(data)
        script = '''
        n = 500000
        ex = table((datehour(2020.01.01T00:01:01)+1..n) as time,1..n as qty)
        re = select * from loadTable("dfs://AutoFitTableAppender_test",`pt)
        each(eqObj, re.values(), ex.values())
        '''
        re = conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_alltype(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
        appender.append(df)
        script = """
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
            re = select * from loadTable("dfs://AutoFitTableAppender_test",`pt)
            each(eqObj,t.values(),re.values())
        """
        re = conn.run(script)
        assert_array_equal(re, [True for _ in range(22)])

    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_alltype_arrayvector(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
        db=database(dbPath,VALUE,1 10000,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
        appender.append(df)
        assert_frame_equal(df, conn.run("select * from pt"))

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_column_dateType_not_match_1(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
            'blob': np.array(['blob1', 'blob2'], dtype='object')
        })
        try:
            appender.append(df)
        except Exception as e:
            assert "The value e1671797c52e15f763380b45e841ec32 (column \"date\", row 0) must be of DATE type" in str(e)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_column_dateType_not_match_2(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
            'blob': np.array(['blob1', 'blob2'], dtype='object')
        })
        try:
            appender.append(df)
        except Exception as e:
            print(str(e))
            assert "The value str1 (column \"long\", row 0) must be of LONG type" in str(e)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_column_dateType_not_match_3(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
            'blob': np.array(['blob1', 'blob2'], dtype='object')
        })
        try:
            appender.append(df)
        except Exception as e:
            result = "The value 5d212a78-cc48-e3b1-4235-b4d91473ee87 (column \"ipaddr\", row 0) must be of IPADDR type" in str(
                e)
            assert (True == result)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_column_dateType_not_match_4(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
            'blob': np.array(['blob1', 'blob2'], dtype='object')
        })
        try:
            appender.append(df)
        except Exception as e:
            assert "(column \"string\", row 0) must be of STRING type" in str(e)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_column_dateType_not_match_5(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
            'blob': np.array(['blob1', 'blob2'], dtype='object')
        })
        try:
            appender.append(df)
        except Exception as e:
            assert "(column \"string\", row 0) must be of STRING type" in str(e)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_arrayvector_not_match_1(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
        db=database(dbPath,VALUE,1 10000,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)

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
            appender.append(df)
        except Exception as e:
            result = "The value [ -100000000 10000000000] (column \"ipaddr\", row 0) must be of IPADDR[] type" in str(e)
            assert (True == result)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_arrayvector_not_match_2(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
        db=database(dbPath,VALUE,1 10000,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
        # The insertion can be successful, the time type is converted to the long type, and the time difference from the original date is calculated
        assert appender.append(df) == 2
        res = conn.run('''select * from loadTable("dfs://AutoFitTableAppender_test",`pt)''')
        assert_frame_equal(res, df)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_arrayvector_not_match_3(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
        db=database(dbPath,VALUE,1 10000,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
            appender.append(df)
        except Exception as e:
            result = "The value [ True False] (column \"ipaddr\", row 0) must be of IPADDR[] type" in str(e)
            assert (True == result)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_arrayvector_not_match_4(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
        db=database(dbPath,VALUE,1 10000,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
        appender.append(df)

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_dfs_table_alltype(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run('''
        dbPath = "dfs://AutoFitTableAppender_test"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
        db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.tableAppender("dfs://AutoFitTableAppender_test", "pt", conn)
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
        appender.append(df)
        script = """
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
            re = select * from loadTable("dfs://AutoFitTableAppender_test",`pt)
            each(eqObj,t.values(),re.values())
        """
        re = conn.run(script)
        assert_array_equal(re, [True for _ in range(22)])

    @pytest.mark.v130221
    def test_TableAppender_no_pandasWarning(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run("""
            t = table(1..2 as a, ["192.168.1.113", "192.168.1.123"] as b)
        """)
        df = conn.run("t")
        import warnings
        warnings.filterwarnings('error')
        try:
            pt = ddb.tableAppender(tableName="t", ddbSession=conn)
            pt.append(df)
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
    def test_TableAppender_allNone_tables_with_numpyArray(self, val, valstr, valdecimal):
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
        conn.run('''
            dbPath = "dfs://test_dfs1"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,1],,'TSDB')
            pt = db.createPartitionedTable(tab, `pt, `ckeycol,,`ckeycol)
        ''')
        appender = ddb.TableAppender("dfs://test_dfs1", 'pt', conn)
        appender.append(df)
        assert conn.run(r"""rows = exec count(*) from loadTable("dfs://test_dfs1", "pt");rows""") == 3
        assert conn.run(r"""ex_tab = select * from loadTable("dfs://test_dfs1", "pt");
                            res = bool([]);
                            for(i in 1:tab.columns()){res.append!(ex_tab.column(i).isNull())};
                            all(res)""")
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['INT', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128', 'BLOB', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(schema, ex_types)
        conn.dropDatabase("dfs://test_dfs1")
        conn.close()

    @pytest.mark.parametrize('val, valstr, valdecimal', test_dataArray, ids=[str(x[0]) for x in test_dataArray])
    def test_TableAppender_allNone_tables_with_pythonList(self, val, valstr, valdecimal):
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
        conn.run('''
            dbPath = "dfs://test_dfs1"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,1],,'TSDB')
            pt = db.createPartitionedTable(tab, `pt, `ckeycol,,`ckeycol)
        ''')
        appender = ddb.TableAppender("dfs://test_dfs1", 'pt', conn)
        appender.append(df)
        assert conn.run(r"""rows = exec count(*) from loadTable("dfs://test_dfs1", "pt");rows""") == 3
        assert conn.run(r"""ex_tab = select * from loadTable("dfs://test_dfs1", "pt");
                            res = bool([]);
                            for(i in 1:tab.columns()){res.append!(ex_tab.column(i).isNull())};
                            all(res)""")
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['INT', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128', 'BLOB', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(schema, ex_types)
        conn.dropDatabase("dfs://test_dfs1")
        conn.close()

    @pytest.mark.v130221
    def test_TableAppender_append_after_session_deconstructed(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run("""
            t = table(1..2 as a, ["192.168.1.113", "192.168.1.123"] as b)
            share t as share_t
        """)
        df = conn.run("share_t")
        # 主动关闭掉连接，抛异常正常
        try:
            pt = ddb.tableAppender(tableName="share_t", ddbSession=conn)
            conn.close()
            pt.append(df)
        except RuntimeError as e:
            assert "Session has been closed." in str(e)
        # 析构掉session引用，可继续append数据
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        pt = ddb.tableAppender(tableName="share_t", ddbSession=conn)
        del conn
        assert pt.append(df) == 2
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        assert conn.run("""res = select * from share_t;
                        ex = table(take(1 2, 4) as a, take(['192.168.1.113','192.168.1.123'],4) as b);
                        each(eqObj, res.values(), ex.values())""").all()
        conn.undef('share_t', 'SHARED')
        conn.close()

    # @pytest.mark.v130221
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableAppender_append_dataframe_with_numpy_order(self, _python_list, _compress, _order):
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
                        i, i, 'sym', 'str', 'blob', "1.1.1.1", "5d212a78-cc48-e3b1-4235-b4d91473ee87",
                        "e1671797c52e15f763380b45e841ec32", decimal.Decimal('-2.11'), decimal.Decimal('0.00000000000'),
                        decimal.Decimal('-1.100000000000000000000000000000000000')]
            data.append(row_data)
        # data.append([10, None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None,None])
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
        conn1.run("""
        colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
        colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(2), DECIMAL64(11), DECIMAL128(36)];
        t=table(1:0, colName,colType)
        dbPath = "dfs://test_dfs1"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[LONG,1],,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        append = ddb.TableAppender(dbPath="dfs://test_dfs1", tableName='pt', ddbSession=conn1)
        append.append(df)
        conn1.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, false, i,i,i,i,i,i+23640,i,i,i,i,i,i,i,i,i,i, 'sym','str', 'blob', ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"), decimal32('-2.11', 2), decimal64('0.0', 11), decimal128('-1.1', 36))
            }
        """)
        res = conn1.run("""ex = select * from objByName(`t);
                           res = select * from loadTable("dfs://test_dfs1", `pt);
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        tys = conn1.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(2)', 'DECIMAL64(11)',
                    'DECIMAL128(36)']
        assert_array_equal(tys, ex_types)
        conn1.dropDatabase("dfs://test_dfs1")
        conn1.close()

    # https://dolphindb1.atlassian.net/browse/APY-653
    @pytest.mark.v130221
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableAppender_append_dataframe_arrayVector_with_numpy_order(self, _compress, _order, _python_list):
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
        conn1.run("""
        colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
        colType = [LONG, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[], DECIMAL32(2)[], DECIMAL64(11)[], DECIMAL128(36)[]];
        t=table(1:0, colName,colType)
        dbPath = "dfs://test_dfs1"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[LONG,1],,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        append = ddb.TableAppender(dbPath="dfs://test_dfs1", tableName='pt', ddbSession=conn1)
        append.append(df)
        conn1.run("""
            for(i in 0:10){
                tableInsert(t, i, 0, i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i, ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"), decimal32('-2.11', 2), decimal64('0.0', 11), decimal128('-1.1', 36))
            }
            tableInsert(objByName(`t), 10, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
        """)
        res = conn1.run("""ex = select * from objByName(`t);
                           res = select * from loadTable("dfs://test_dfs1", `pt);
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        tys = conn1.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]', 'DECIMAL32(2)[]', 'DECIMAL64(11)[]',
                    'DECIMAL128(36)[]']
        assert_array_equal(tys, ex_types)
        conn1.dropDatabase("dfs://test_dfs1")
        conn1.close()

    # https://dolphindb1.atlassian.net/browse/APY-653
    @pytest.mark.v130221
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableAppender_append_null_dataframe_with_numpy_order(self, _compress, _order, _python_list):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        origin_nulls = [None, np.nan, pd.NaT]
        for i in range(7):
            row_data = random.choices(origin_nulls, k=26)
            print(f'row {i}:', row_data)
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
        conn1.run("""
        colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
        colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(2), DECIMAL64(11), DECIMAL128(36)];
        t=table(1:0, colName,colType)
        dbPath = "dfs://test_dfs1"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[LONG,1],,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        append = ddb.TableAppender(dbPath="dfs://test_dfs1", tableName='pt', ddbSession=conn1)
        append.append(df)
        conn1.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)

        res = conn1.run("""ex = select * from objByName(`t);
                           res = select * from loadTable("dfs://test_dfs1", `pt);
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        tys = conn1.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(2)', 'DECIMAL64(11)',
                    'DECIMAL128(36)']
        assert_array_equal(tys, ex_types)
        conn1.dropDatabase("dfs://test_dfs1")
        conn1.close()

    # https://dolphindb1.atlassian.net/browse/APY-653
    @pytest.mark.v130221
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_TableAppender_append_null_dataframe_arrayVector_with_numpy_order(self, _compress, _order, _python_list):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress)
        data = []
        origin_nulls = [[None], [np.nan], [pd.NaT]]
        for i in range(7):
            row_data = random.choices(origin_nulls, k=23)
            print(f'row {i}:', row_data)
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
        conn1.run("""
        colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
        colType = [LONG, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[], DECIMAL32(2)[], DECIMAL64(11)[], DECIMAL128(36)[]];
        t=table(1:0, colName,colType)
        dbPath = "dfs://test_dfs1"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[LONG,1],,'TSDB')
        pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        append = ddb.TableAppender(dbPath="dfs://test_dfs1", tableName='pt', ddbSession=conn1)
        append.append(df)

        conn1.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)

        res = conn1.run("""ex = select * from objByName(`t);
                           res = select * from loadTable("dfs://test_dfs1", `pt);
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res

        tys = conn1.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]', 'DECIMAL32(2)[]', 'DECIMAL64(11)[]',
                    'DECIMAL128(36)[]']
        assert_array_equal(tys, ex_types)
        conn1.dropDatabase("dfs://test_dfs1")
        conn1.close()

    def test_TableAppender_decimal_1(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        conn1.run("""
            login(`admin,`123456)
            dbName="dfs://test_decimal128"
            if(existsDatabase(dbName)){
                dropDatabase(dbName)
            }
            db = database(dbName, VALUE, 2022.12.01..2022.12.31, engine="TSDB");
            temp_table = table(10:0,`YDM_BAT_DT `TXN_ID `IS_OLD_TXN `OLD_TXN_ID `ENTY_TYPE_CD `RISK_TYPE_CD `TRADE_TABLE_CD `BUSI_CATEG_CD `ACCOUNT_TYPE_CD `BUSI_TYPE_CD `BUSI_DTL_DESC `TRADER_ID `AFFILIATE_CD `TXN_ORG_CD `TXN_DEPT_CD `PORFOLIO_ID `TXN_STAT_CD `TXN_STAT_DTL `TXN_DT `ST_INT_DT `END_DT `TXN_PERD `REMAIN_PERD `POSTE `BOND_CD `BOND_DESC `BOND_ENG_NAME `BOND_CN_NAME `BOND_TYPE `TXN_CURR `TXN_CLEAN_PRICE `NOW_CLEAN_PRICE `BS6_PRICE `CT_FULL_PRICE `FULL_PRICE_CHECK `NOMINAL_AMT `ACCOUNT_AMT `TXN_CNT `BAL_CNT `TXN_PURPOSE `TXN_SETT_AMT `TXN_ACCRUED_INT `CT_ST_INT_DT `CT_END_INT_DT `CT_PAY_INT_DT `CT_REPRICING_DT `REPRICING_FREQ `RATE_TYPE `CT_RATE `PAR_RATE `PAR_VALUE `BASIS `FLO_RATE_CD `MARGIN `PAY_INT_FREQ `ACCRUED_INT `ACCRUED_INT_TB `ACCRUED_INT_ADJ `AMORT_AMT `CAPITAL_GAINS `BOND_ASSET_TYPE `REAL_INT `SELL_INCOME `SELL_INTEREST `SELL_AMORT_AMT `AMORT_DP_AMT `DISC_PRIM_AMT `UN_AMORT_DP_AMT `MTM `TXN_FEE_AMT `SETT_METH_IND `TXN_LOG_DT `TXN_LOG_TM `AUTH_DTTM `AUTH_PTY_NAME `HEDGE_PTFLO_ID `HEDGE_TYPE `HEDGE_ST_DT `HEDGE_END_DT `HEDGE_VALID_AMT `HEDGE_INVALID_AMT `HEDGE_VALID_AMORT `HEDGE_VALID_UN_AMORT `HEDGE_DATE_NPV `HEDGE_DAY_NPV `HEDGE_ACR_BEFORE `HEDGE_ACR `HEDGE_AMOR_BEFORE `HEDGE_AMOR `BOND_ISSUE_DT `BOND_ISSUE_PRICE `ABS_PROMOTER `ISSUER_CD `CN_CREDIT_LEV_ORG `CN_CREDIT_LEV `CN_MAIN_LEV_ORG `CN_MAIN_LEV `INTER_CREDIT_LEV_ORG `INTER_CREDIT_LEV `INTER_MAIN_LEV_ORG `INTER_MAIN_LEV `SRC_SYS_CD `SRC_TXN_ID `PRODUCT_CD `ITEM_CP_ID `IS_OVERSEA_IND `CPARTY_ID `CPARTY_CD `IS_WAVE_TEST `IS_MARKET_TEST `BOND_GRADE `EXTERNAL_GRADE `SOLD_OUT_DT `IS_REPO_BOND `PRINCIPAL_ITEM `ACCRUED_ITEM `PAID_INT_ITEM `MTM_ITEM `DISCOUNT_INT_ITEM `PREMIUM_INT_ITEM `AMORTIZ_INT_ITEM `TXN_CANCEL_IND `TXN_CANCEL_DT `SETT_AMT_ITEM `END_YIELD `IS_POSITION `CN_CREDIT_LEV_2 `CN_CREDIT_LEV_ORG_2 `RATE_FLOAT_PERIOD `REAL_END_DT `CURRENT_RATE `SELL_PRE_DIS `SELL_INTERET `SELL_AMORED `SELL_ST `INTERET_SELL_Z `END_INTERET_SELL `END_INTERET_SELL_Z `END_PREMIUM_DIS `END_PREMIUM_DIS_Z `SETT_AMT `NOMINAL_AMOUNT `CNT_ORG `ROOT_CONTRACT_ID `CONTRACT_ID `BUY_SELL `TXN_SETT_AMT_MX `END_YIELD_MX `TRADER_NAME `ORG_PKG_ID `PACKAGE_ID `TP_DTEFLWL `RESIDUAL_AMOUNT `IS_DOM_WAVE_TEST `TP_NOMINAL `MASTER_AGREEMENT `RATE `SI_CNY,[DATE ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,DATE ,DATE ,DATE ,LONG ,LONG ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,LONG ,LONG ,STRING ,DECIMAL128(6) ,DECIMAL128(6) ,DATE ,DATE ,DATE ,DATE ,STRING ,STRING ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,STRING ,STRING ,DECIMAL128(6) ,STRING ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,STRING ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,STRING ,DATE ,STRING ,STRING ,STRING ,STRING ,STRING ,DATE ,DATE ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DATE ,DECIMAL128(6) ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,DATE ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,STRING ,DATE ,STRING ,DECIMAL128(6) ,STRING ,STRING ,STRING ,LONG ,DATE ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,LONG ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,DECIMAL128(6) ,LONG ,LONG ,LONG ,STRING ,DECIMAL128(6) ,DECIMAL128(6) ,STRING ,LONG ,LONG ,DATE ,DECIMAL128(6) ,STRING ,DECIMAL128(6) ,STRING ,DECIMAL128(6) ,STRING]);
            tb = db.createPartitionedTable(temp_table,`T5_BOND_TXN,`YDM_BAT_DT, sortColumns="YDM_BAT_DT");
        """)
        schemas = conn1.run("schema(temp_table).colDefs[`typeInt]")
        names = conn1.run("schema(temp_table).colDefs[`name]")
        ds = dict()
        for i in range(len(schemas)):
            if schemas[i] == 39:
                ds[names[i]] = str
        df = pd.read_csv(LOCAL_DATA_DIR + '/decimal.csv', dtype=ds)
        for i in range(df.shape[1]):
            if schemas[i] == 39:
                df.iloc[:, i] = df.iloc[:, i].apply(lambda x: decimal.Decimal(x).quantize(decimal.Decimal("0.000000")))
            elif schemas[i] == 6:
                df.iloc[:, i] = df.iloc[:, i].apply(lambda x: np.datetime64(x.replace('.', '-')))
            elif schemas[i] == 5:
                df.iloc[:, i] = df.iloc[:, i].apply(lambda x: np.int64(x))
        df.iloc[:, 1] = df.iloc[:, 1].apply(lambda x: str(x))
        append = ddb.TableAppender(dbPath="dfs://test_decimal128", tableName='T5_BOND_TXN', ddbSession=conn1)
        assert append.append(df) == 41499
        conn1.run(f'temp_table=loadText("{DATA_DIR + "/decimal.csv"}",schema=table(`YDM_BAT_DT `TXN_ID `IS_OLD_TXN `OLD_TXN_ID `ENTY_TYPE_CD `RISK_TYPE_CD `TRADE_TABLE_CD `BUSI_CATEG_CD `ACCOUNT_TYPE_CD `BUSI_TYPE_CD `BUSI_DTL_DESC `TRADER_ID `AFFILIATE_CD `TXN_ORG_CD `TXN_DEPT_CD `PORFOLIO_ID `TXN_STAT_CD `TXN_STAT_DTL `TXN_DT `ST_INT_DT `END_DT `TXN_PERD `REMAIN_PERD `POSTE `BOND_CD `BOND_DESC `BOND_ENG_NAME `BOND_CN_NAME `BOND_TYPE `TXN_CURR `TXN_CLEAN_PRICE `NOW_CLEAN_PRICE `BS6_PRICE `CT_FULL_PRICE `FULL_PRICE_CHECK `NOMINAL_AMT `ACCOUNT_AMT `TXN_CNT `BAL_CNT `TXN_PURPOSE `TXN_SETT_AMT `TXN_ACCRUED_INT `CT_ST_INT_DT `CT_END_INT_DT `CT_PAY_INT_DT `CT_REPRICING_DT `REPRICING_FREQ `RATE_TYPE `CT_RATE `PAR_RATE `PAR_VALUE `BASIS `FLO_RATE_CD `MARGIN `PAY_INT_FREQ `ACCRUED_INT `ACCRUED_INT_TB `ACCRUED_INT_ADJ `AMORT_AMT `CAPITAL_GAINS `BOND_ASSET_TYPE `REAL_INT `SELL_INCOME `SELL_INTEREST `SELL_AMORT_AMT `AMORT_DP_AMT `DISC_PRIM_AMT `UN_AMORT_DP_AMT `MTM `TXN_FEE_AMT `SETT_METH_IND `TXN_LOG_DT `TXN_LOG_TM `AUTH_DTTM `AUTH_PTY_NAME `HEDGE_PTFLO_ID `HEDGE_TYPE `HEDGE_ST_DT `HEDGE_END_DT `HEDGE_VALID_AMT `HEDGE_INVALID_AMT `HEDGE_VALID_AMORT `HEDGE_VALID_UN_AMORT `HEDGE_DATE_NPV `HEDGE_DAY_NPV `HEDGE_ACR_BEFORE `HEDGE_ACR `HEDGE_AMOR_BEFORE `HEDGE_AMOR `BOND_ISSUE_DT `BOND_ISSUE_PRICE `ABS_PROMOTER `ISSUER_CD `CN_CREDIT_LEV_ORG `CN_CREDIT_LEV `CN_MAIN_LEV_ORG `CN_MAIN_LEV `INTER_CREDIT_LEV_ORG `INTER_CREDIT_LEV `INTER_MAIN_LEV_ORG `INTER_MAIN_LEV `SRC_SYS_CD `SRC_TXN_ID `PRODUCT_CD `ITEM_CP_ID `IS_OVERSEA_IND `CPARTY_ID `CPARTY_CD `IS_WAVE_TEST `IS_MARKET_TEST `BOND_GRADE `EXTERNAL_GRADE `SOLD_OUT_DT `IS_REPO_BOND `PRINCIPAL_ITEM `ACCRUED_ITEM `PAID_INT_ITEM `MTM_ITEM `DISCOUNT_INT_ITEM `PREMIUM_INT_ITEM `AMORTIZ_INT_ITEM `TXN_CANCEL_IND `TXN_CANCEL_DT `SETT_AMT_ITEM `END_YIELD `IS_POSITION `CN_CREDIT_LEV_2 `CN_CREDIT_LEV_ORG_2 `RATE_FLOAT_PERIOD `REAL_END_DT `CURRENT_RATE `SELL_PRE_DIS `SELL_INTERET `SELL_AMORED `SELL_ST `INTERET_SELL_Z `END_INTERET_SELL `END_INTERET_SELL_Z `END_PREMIUM_DIS `END_PREMIUM_DIS_Z `SETT_AMT `NOMINAL_AMOUNT `CNT_ORG `ROOT_CONTRACT_ID `CONTRACT_ID `BUY_SELL `TXN_SETT_AMT_MX `END_YIELD_MX `TRADER_NAME `ORG_PKG_ID `PACKAGE_ID `TP_DTEFLWL `RESIDUAL_AMOUNT `IS_DOM_WAVE_TEST `TP_NOMINAL `MASTER_AGREEMENT `RATE `SI_CNY as `name,["DATE","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","DATE","DATE","DATE","LONG","LONG","STRING","STRING","STRING","STRING","STRING","STRING","STRING","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","LONG","LONG","STRING","DECIMAL128(6)","DECIMAL128(6)","DATE","DATE","DATE","DATE","STRING","STRING","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","STRING","STRING","DECIMAL128(6)","STRING","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","STRING","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","STRING","DATE","STRING","STRING","STRING","STRING","STRING","DATE","DATE","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DATE","DECIMAL128(6)","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","DATE","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","STRING","DATE","STRING","DECIMAL128(6)","STRING","STRING","STRING","LONG","DATE","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","LONG","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","DECIMAL128(6)","LONG","LONG","LONG","STRING","DECIMAL128(6)","DECIMAL128(6)","STRING","LONG","LONG","DATE","DECIMAL128(6)","STRING","DECIMAL128(6)","STRING","DECIMAL128(6)","STRING"] as `type))')
        conn1.run("res = select * from loadTable('dfs://test_decimal128', `T5_BOND_TXN);")
        assert conn1.run(
            "each(eqObj, temp_table.sortBy!(temp_table.colNames()).values(), res.sortBy!(res.colNames()).values())").all()

    # todo:scale error
    def test_TableAppender_decimal_2(self):
        df = pd.DataFrame({'date': np.array(["2012-02-03T01:02:03.456789123"] * 10000, dtype='datetime64[D]'),
                           'decimal128_26_1': [decimal.Decimal('0')] * 10000,
                           'decimal128_26_2': [decimal.Decimal('NaN')] * 10000,
                           'decimal128_26_3': [decimal.Decimal('-1.12312312312312312312312312')] * 10000,
                           'decimal128_26_4': [decimal.Decimal('123123123123.0')] * 10000,
                           'decimal128_26_5': [None] * 10000,
                           })
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        conn1.run("""
            login(`admin,`123456)
            dbName="dfs://test_decimal128"
            if(existsDatabase(dbName)){
                dropDatabase(dbName)
            }
            db = database(dbName, VALUE, 2022.12.01..2022.12.31, engine="TSDB");
            schema_t = table(10:0, `date`decimal128_26_1`decimal128_26_2`decimal128_26_3`decimal128_26_4`decimal128_26_5, [DATE, DECIMAL128(26), DECIMAL128(26), DECIMAL128(26), DECIMAL128(26), DECIMAL128(26)])
            pt = createPartitionedTable(db, schema_t, `pt_1, `date, sortColumns=`date)

        """)
        append = ddb.TableAppender(dbPath="dfs://test_decimal128", tableName='pt_1', ddbSession=conn1)
        append.append(df)
        conn1.run("""
            ex = table(take(2012.02.03,10000) as c1,
                        take(decimal128(0,26), 10000) as c2,
                        take(decimal128(NULL,26), 10000) as c3,
                        take(decimal128("-1.12312312312312312312312312",26), 10000) as c4,
                        take(decimal128("123123123123.0",26), 10000) as c5,
                        take(decimal128(NULL,26), 10000) as c6);
            res = select * from loadTable('dfs://test_decimal128', `pt_1);
        """)
        conn1.run("for(i in 1:ex.columns()){assert all(eqObj(ex.values()[i], res.values()[i]))}")

    def test_TableAppender_decimal_3(self):
        context = decimal.Context(prec=39)
        df = pd.DataFrame({'date': np.array(
            ["2012-02-01", "2012-02-02", "2012-02-03", "2012-02-04", "2012-02-05", "2012-02-06"],
            dtype='datetime64[D]'),
                           'decimal128_0': [decimal.Decimal('-1'), decimal.Decimal('123456789'), decimal.Decimal('0'),
                                            None, pd.NaT, np.nan],
                           'decimal128_6': [None, pd.NaT, np.nan, decimal.Decimal('NaN'), decimal.Decimal('-1.123123'),
                                            decimal.Decimal('1.123456')],
                           'decimal128_16': [pd.NaT, np.nan, None, decimal.Decimal('-1.1231231231231231'),
                                             decimal.Decimal('NaN'), decimal.Decimal('1')],
                           'decimal128_27': [np.nan, pd.NaT, None, decimal.Decimal('1.123123123123123123123123123'),
                                             decimal.Decimal('NaN'), decimal.Decimal('-1')],
                           'decimal128_37': [decimal.Decimal('NaN'), np.nan, pd.NaT, None,
                                             decimal.Decimal('0.1231231231231231231231231231231231231'),
                                             decimal.Decimal('-1')],
                           'decimal128_38': [decimal.Decimal('NaN'), np.nan, pd.NaT, None,
                                             decimal.Decimal('0.12312312312312312312312312312312312312'),
                                             decimal.Decimal('0')],
                           })
        df['decimal128_0'] = df['decimal128_0'].apply(
            lambda x: x.quantize(decimal.Decimal('0'), context=context) if x not in [None, pd.NaT, np.nan,
                                                                                     decimal.Decimal('NaN')] else x)
        df['decimal128_6'] = df['decimal128_6'].apply(
            lambda x: x.quantize(decimal.Decimal('0.000000'), context=context) if x not in [None, pd.NaT, np.nan,
                                                                                            decimal.Decimal(
                                                                                                'NaN')] else x)
        df['decimal128_16'] = df['decimal128_16'].apply(
            lambda x: x.quantize(decimal.Decimal('0.0000000000000000'), context=context) if x not in [None, pd.NaT,
                                                                                                      np.nan,
                                                                                                      decimal.Decimal(
                                                                                                          'NaN')] else x)
        df['decimal128_27'] = df['decimal128_27'].apply(
            lambda x: x.quantize(decimal.Decimal('0.000000000000000000000000000'), context=context) if x not in [None,
                                                                                                                pd.NaT,
                                                                                                                np.nan,
                                                                                                                decimal.Decimal(
                                                                                                                    'NaN')] else x)
        df['decimal128_37'] = df['decimal128_37'].apply(
            lambda x: x.quantize(decimal.Decimal('0.0000000000000000000000000000000000000'),
                                 context=context) if x not in [None, pd.NaT, np.nan, decimal.Decimal('NaN')] else x)
        df['decimal128_38'] = df['decimal128_38'].apply(
            lambda x: x.quantize(decimal.Decimal('0.00000000000000000000000000000000000000'),
                                 context=context) if x not in [None, pd.NaT, np.nan, decimal.Decimal('NaN')] else x)
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        conn1.run("""
            login(`admin,`123456)
            dbName="dfs://test_decimal128"
            if(existsDatabase(dbName)){
                dropDatabase(dbName)
            }
            db = database(dbName, VALUE, 2022.12.01..2022.12.31, engine="TSDB");
            schema_t = table(10:0, `date`decimal128_0`decimal128_6`decimal128_16`decimal128_27`decimal128_37`decimal128_38,
                                [DATE, DECIMAL128(0),DECIMAL128(6),DECIMAL128(16),DECIMAL128(27),DECIMAL128(37),DECIMAL128(38)])
            pt = createPartitionedTable(db, schema_t, `pt_2, `date, sortColumns=`date)
        """)
        append = ddb.TableAppender(dbPath="dfs://test_decimal128", tableName='pt_2', ddbSession=conn1)
        append.append(df)
        conn1.run("""
            ex = table([2012.02.01,2012.02.02,2012.02.03,2012.02.04,2012.02.05,2012.02.06] as c1,
                        decimal128([-1, 123456789, 0, NULL, NULL, NULL],0) as c2,
                        [NULL, NULL, NULL, NULL, decimal128('-1.123123',6), decimal128('1.123456',6)] as c3,
                        [NULL, NULL, NULL,decimal128('-1.1231231231231231',16), NULL, decimal128("1",16)] as c4,
                        [NULL, NULL, NULL,decimal128('1.123123123123123123123123123',27), NULL, decimal128("-1",27)] as c5,
                        [NULL, NULL, NULL, NULL,decimal128('0.1231231231231231231231231231231231231',37), decimal128("-1",37)] as c6,
                        [NULL, NULL, NULL, NULL,decimal128('0.12312312312312312312312312312312312312',38), decimal128(`0,38)] as c7);
            res = select * from loadTable('dfs://test_decimal128', `pt_2);
        """)
        conn1.run("for(i in 1:ex.columns()){assert all(eqObj(ex.values()[i], res.values()[i]))}")

    def test_TableAppender_decimal_4(self):
        df = pd.DataFrame({'date': np.array(
            ["2012-02-01", "2012-02-02", "2012-02-03", "2012-02-04", "2012-02-05", "2012-02-06"],
            dtype='datetime64[D]'),
                           'float': [1.2314, -1, None, np.nan, pd.NaT, 0],
                           'double': [None, pd.NaT, np.nan, 1, -1.2345, 0],
                           'string': [pd.NaT, np.nan, None, '', 'str1', 'str2'],
                           'decimal128_6': [np.nan, pd.NaT, None, decimal.Decimal('1.123123'), decimal.Decimal('NaN'),
                                            decimal.Decimal('-1').quantize(decimal.Decimal('0.000000'))],
                           'decimal128_27': [decimal.Decimal('NaN'), np.nan, pd.NaT, None,
                                             decimal.Decimal('0.123123123123123123123123123'),
                                             decimal.Decimal('-1').quantize(
                                                 decimal.Decimal('0.123123123123123123123123123'))],
                           'long': np.array([1, 2, 3, 4, 5, 6], dtype=np.int64),
                           })
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        conn1.run("""
            login(`admin,`123456)
            dbName="dfs://test_decimal128"
            if(existsDatabase(dbName)){
                dropDatabase(dbName)
            }
            db = database(dbName, VALUE, 2022.12.01..2022.12.31, engine="TSDB");
            schema_t = table(10:0, `date`float`double`string`decimal128_6`decimal128_27`long,
                                [DATE, FLOAT, DOUBLE, STRING, DECIMAL128(6), DECIMAL128(27), LONG])
            pt = createPartitionedTable(db, schema_t, `pt_3, `date, sortColumns=`date)
        """)
        append = ddb.TableAppender(dbPath="dfs://test_decimal128", tableName='pt_3', ddbSession=conn1)
        append.append(df)
        conn1.run("""
            ex = table([2012.02.01,2012.02.02,2012.02.03,2012.02.04,2012.02.05,2012.02.06] as c1,
                        float([1.2314, -1, NULL, NULL, NULL, 0]) as c2,
                        double([NULL, NULL, NULL, 1, -1.2345, 0]) as c3,
                        [NULL, NULL, NULL, "", "str1", "str2"] as c4,
                        [NULL, NULL, NULL,decimal128('1.123123123', 6), NULL, decimal128("-1",6)] as c5,
                        [NULL, NULL, NULL, NULL,decimal128('0.123123123123123123123123123',27), decimal128("-1",27)] as c6,
                        long(1 2 3 4 5 6) as c7);
            res = select * from loadTable('dfs://test_decimal128', `pt_3);
            share ex as t1;
            share res as t2;
        """)
        conn1.run("for(i in 1:ex.columns()){assert all(eqObj(ex.values()[i], res.values()[i]))}")

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_append_decimal128_over_flow(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("""
        dbName = "dfs://test_decimal128"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db = database(dbName, VALUE, 2012.12.01..2012.12.31, engine='TSDB');
        //pt_1
        schema_t = table(10:0, `date`decimal128_26, [DATE, DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_1, `date, sortColumns=`date)

        //pt_2
        schema_t = table(10:0, `date`decimal128_0`decimal128_6`decimal128_16`decimal128_26`decimal128_37`decimal128_38,
                            [DATE, DECIMAL128(0),DECIMAL128(6),DECIMAL128(16),DECIMAL128(26),DECIMAL128(37),DECIMAL128(38)])
        pt = createPartitionedTable(db, schema_t, `pt_2, `date, sortColumns=`date)

        //pt_3
        schema_t = table(10:0, `date`float`double`string`long`decimal128_6`decimal128_26,
                            [DATE, FLOAT, DOUBLE, STRING, LONG,DECIMAL128(6), DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_3, `date, sortColumns=`date)
        """)
        appender1 = ddb.tableAppender(dbPath="dfs://test_decimal128", tableName="pt_1", ddbSession=conn)
        date = np.array(['2012-12-01T00:00:00'], dtype="datetime64[D]")
        decimal128_26 = np.array([decimal.Decimal("-1." + "0" * 40 + "1")], dtype='object')
        data = pd.DataFrame({"date": date, "decimal128_26": decimal128_26}, dtype='object')
        with pytest.raises(RuntimeError):
            appender1.append(data)

    # todo:scale error
    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_pt_1_append_decimal128_26_one_row(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("""
        dbName = "dfs://test_decimal128"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db = database(dbName, VALUE, 2012.12.01..2012.12.31, engine='TSDB');
        //pt_1
        schema_t = table(10:0, `date`decimal128_26, [DATE, DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_1, `date, sortColumns=`date)

        //pt_2
        schema_t = table(10:0, `date`decimal128_0`decimal128_6`decimal128_16`decimal128_26`decimal128_37`decimal128_38,
                            [DATE, DECIMAL128(0),DECIMAL128(6),DECIMAL128(16),DECIMAL128(26),DECIMAL128(37),DECIMAL128(38)])
        pt = createPartitionedTable(db, schema_t, `pt_2, `date, sortColumns=`date)

        //pt_3
        schema_t = table(10:0, `date`float`double`string`long`decimal128_6`decimal128_26,
                            [DATE, FLOAT, DOUBLE, STRING, LONG,DECIMAL128(6), DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_3, `date, sortColumns=`date)
        """)
        appender1 = ddb.tableAppender(dbPath="dfs://test_decimal128", tableName="pt_1", ddbSession=conn)
        date = np.array(['2012-12-01T00:00:00'], dtype="datetime64[D]")
        decimal128_26 = np.array([decimal.Decimal("-1." + "0" * 26 + "1")], dtype='object')
        data = pd.DataFrame({"date": date, "decimal128_26": decimal128_26})
        appender1.append(data)
        assert conn.run("""
                        ans = select * from loadTable(dbName, `pt_1)
                        ex = table([2012.12.01] as x, [decimal128("-1", 26)] as y)
                        each(eqObj, ans.values(), ex.values())
                        """).all()

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_pt_1_append_decimal128_26_one_row_NULL(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("""
        dbName = "dfs://test_decimal128"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db = database(dbName, VALUE, 2012.12.01..2012.12.31, engine='TSDB');
        //pt_1
        schema_t = table(10:0, `date`decimal128_26, [DATE, DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_1, `date, sortColumns=`date)

        //pt_2
        schema_t = table(10:0, `date`decimal128_0`decimal128_6`decimal128_16`decimal128_26`decimal128_37`decimal128_38,
                            [DATE, DECIMAL128(0),DECIMAL128(6),DECIMAL128(16),DECIMAL128(26),DECIMAL128(37),DECIMAL128(38)])
        pt = createPartitionedTable(db, schema_t, `pt_2, `date, sortColumns=`date)

        //pt_3
        schema_t = table(10:0, `date`float`double`string`long`decimal128_6`decimal128_26,
                            [DATE, FLOAT, DOUBLE, STRING, LONG,DECIMAL128(6), DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_3, `date, sortColumns=`date)
        """)
        appender1 = ddb.tableAppender(dbPath="dfs://test_decimal128", tableName="pt_1", ddbSession=conn)
        date = np.array(['2012-12-01T00:00:00'], dtype="datetime64[D]")
        decimal128_26 = np.array([decimal.Decimal("NaN")], dtype='object')
        data = pd.DataFrame({"date": date, "decimal128_26": decimal128_26})
        appender1.append(data)
        assert conn.run("""
                        ans = select * from loadTable(dbName, `pt_1)
                        ex = table([2012.12.01] as x, [decimal128(NULL, 26)] as y)
                        each(eqObj, ans.values(), ex.values())
                        """).all()

    # todo:scale error
    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_pt_1_append_decimal128_26_multi_row_without_NULL(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("""
        dbName = "dfs://test_decimal128"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db = database(dbName, VALUE, 2012.12.01..2012.12.31, engine='TSDB');
        //pt_1
        schema_t = table(10:0, `date`decimal128_26, [DATE, DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_1, `date, sortColumns=`date)

        //pt_2
        schema_t = table(10:0, `date`decimal128_0`decimal128_6`decimal128_16`decimal128_26`decimal128_37`decimal128_38,
                            [DATE, DECIMAL128(0),DECIMAL128(6),DECIMAL128(16),DECIMAL128(26),DECIMAL128(37),DECIMAL128(38)])
        pt = createPartitionedTable(db, schema_t, `pt_2, `date, sortColumns=`date)

        //pt_3
        schema_t = table(10:0, `date`float`double`string`long`decimal128_6`decimal128_26,
                            [DATE, FLOAT, DOUBLE, STRING, LONG,DECIMAL128(6), DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_3, `date, sortColumns=`date)
        """)
        appender1 = ddb.tableAppender(dbPath="dfs://test_decimal128", tableName="pt_1", ddbSession=conn)
        data = pd.DataFrame({"date": np.array(
            ['2012-12-01T00:00:00', '2012-12-10T00:00:00', '2012-12-20T00:00:00'] * 100, dtype="datetime64[D]"),
                             "decimal128_26": np.array(
                                 [decimal.Decimal("1.00"), decimal.Decimal("2.01"), decimal.Decimal("2.10")] * 100,
                                 dtype='object')})
        appender1.append(data)
        assert conn.run("""
                        ans = select * from loadTable(dbName, `pt_1) order by date
                        ex = table(take([2012.12.01, 2012.12.10, 2012.12.20],300) as x, take(decimal128(["1.00", "2.01", "2.10"], 26),300) as y)
                        ex = select * from ex order by x
                        each(eqObj, ans.values(), ex.values())
                        """).all()

    # todo:scale error
    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_pt_1_append_decimal128_26_multi_row_NULL(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("""
        dbName = "dfs://test_decimal128"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db = database(dbName, VALUE, 2012.12.01..2012.12.31, engine='TSDB');
        //pt_1
        schema_t = table(10:0, `date`decimal128_26, [DATE, DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_1, `date, sortColumns=`date)

        //pt_2
        schema_t = table(10:0, `date`decimal128_0`decimal128_6`decimal128_16`decimal128_26`decimal128_37`decimal128_38,
                            [DATE, DECIMAL128(0),DECIMAL128(6),DECIMAL128(16),DECIMAL128(26),DECIMAL128(37),DECIMAL128(38)])
        pt = createPartitionedTable(db, schema_t, `pt_2, `date, sortColumns=`date)

        //pt_3
        schema_t = table(10:0, `date`float`double`string`long`decimal128_6`decimal128_26,
                            [DATE, FLOAT, DOUBLE, STRING, LONG,DECIMAL128(6), DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_3, `date, sortColumns=`date)
        """)
        appender1 = ddb.tableAppender(dbPath="dfs://test_decimal128", tableName="pt_1", ddbSession=conn)
        data = pd.DataFrame({"date": np.array(
            ['2012-12-01T00:00:00', '2012-12-10T00:00:00', '2012-12-20T00:00:00'] * 110000, dtype="datetime64[D]"),
                             "decimal128_26": np.array([None, pd.NaT, np.nan, decimal.Decimal('NaN'),
                                                        decimal.Decimal("1.001"),
                                                        decimal.Decimal("2.002"),
                                                        decimal.Decimal("3.003"),
                                                        None, pd.NaT, np.nan, decimal.Decimal('NaN')] * 30000,
                                                       dtype='object')})
        appender1.append(data)
        assert conn.run("""
                        ans = select * from loadTable(dbName, `pt_1) order by date
                        ex = table(take([2012.12.01, 2012.12.10, 2012.12.20],330000) as x, take(decimal128([NULL, NULL, NULL, NULL, "1.001", "2.002", "3.003", NULL, NULL, NULL, NULL], 26),330000) as y)
                        ex = select * from ex order by x
                        each(eqObj, ans.values(), ex.values())
                        """).all()

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_PT_2_append_decimal128(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("""
        dbName = "dfs://test_decimal128"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db = database(dbName, VALUE, 2012.12.01..2012.12.31, engine='TSDB');
        //pt_1
        schema_t = table(10:0, `date`decimal128_26, [DATE, DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_1, `date, sortColumns=`date)

        //pt_2
        schema_t = table(10:0, `date`decimal128_0`decimal128_6`decimal128_16`decimal128_26`decimal128_37,
                            [DATE, DECIMAL128(0),DECIMAL128(6),DECIMAL128(16),DECIMAL128(26),DECIMAL128(37)])
        pt = createPartitionedTable(db, schema_t, `pt_2, `date, sortColumns=`date)

        //pt_3
        schema_t = table(10:0, `date`float`double`string`long`decimal128_6`decimal128_26,
                            [DATE, FLOAT, DOUBLE, STRING, LONG,DECIMAL128(6), DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_3, `date, sortColumns=`date)
        """)
        appender1 = ddb.tableAppender(dbPath="dfs://test_decimal128", tableName="pt_2", ddbSession=conn)
        data = pd.DataFrame({"date": np.array(
            ['2012-12-01T00:00:00', '2012-12-10T00:00:00', '2012-12-20T00:00:00'] * 110000, dtype="datetime64[D]"),
                             "decimal128_0": np.array([None, pd.NaT, np.nan, decimal.Decimal('NaN'),
                                                       decimal.Decimal("-1."),
                                                       decimal.Decimal("0."),
                                                       decimal.Decimal("1."),
                                                       None, pd.NaT, np.nan, decimal.Decimal('NaN')] * 30000,
                                                      dtype='object'),
                             "decimal128_6": np.array([None, pd.NaT, np.nan, decimal.Decimal('NaN'),
                                                       decimal.Decimal("-1." + "0" * 5 + "1"),
                                                       decimal.Decimal("0." + "0" * 5 + "1"),
                                                       decimal.Decimal("1." + "0" * 5 + "1"),
                                                       None, pd.NaT, np.nan, decimal.Decimal('NaN')] * 30000,
                                                      dtype='object'),
                             "decimal128_16": np.array([None, pd.NaT, np.nan, decimal.Decimal('NaN'),
                                                        decimal.Decimal("-1." + "0" * 15 + "1"),
                                                        decimal.Decimal("0." + "0" * 15 + "1"),
                                                        decimal.Decimal("1." + "0" * 15 + "1"),
                                                        None, pd.NaT, np.nan, decimal.Decimal('NaN')] * 30000,
                                                       dtype='object'),
                             "decimal128_26": np.array([None, pd.NaT, np.nan, decimal.Decimal('NaN'),
                                                        decimal.Decimal("-1." + "0" * 25 + "1"),
                                                        decimal.Decimal("0." + "0" * 25 + "1"),
                                                        decimal.Decimal("1." + "0" * 25 + "1"),
                                                        None, pd.NaT, np.nan, decimal.Decimal('NaN')] * 30000,
                                                       dtype='object'),
                             "decimal128_37": np.array([None, pd.NaT, np.nan, decimal.Decimal('NaN'),
                                                        decimal.Decimal("-1." + "0" * 36 + "1"),
                                                        decimal.Decimal("0." + "0" * 36 + "1"),
                                                        decimal.Decimal("1." + "0" * 36 + "1"),
                                                        None, pd.NaT, np.nan, decimal.Decimal('NaN')] * 30000,
                                                       dtype='object')
                             })
        appender1.append(data)
        assert conn.run("""
                    	ans = select * from loadTable(dbName, `pt_2) order by date
                        ex = table(take([2012.12.01, 2012.12.10, 2012.12.20],330000) as date,
                                   decimal128(take([NULL, NULL, NULL, NULL,"-1", "0", "1",NULL, NULL, NULL, NULL],330000),0) as decimal128_0,
                                   decimal128(take([NULL, NULL, NULL, NULL,"-1."+repeat("0",5)+"1", "0."+repeat("0",5)+"1", "1."+repeat("0",5)+"1",NULL, NULL, NULL, NULL],330000), 6) as decimal128_6,
                                   decimal128(take([NULL, NULL, NULL, NULL,"-1."+repeat("0",15)+"1", "0."+repeat("0",15)+"1", "1."+repeat("0",15)+"1",NULL, NULL, NULL, NULL],330000), 16) as decimal128_16,
                                   decimal128(take([NULL, NULL, NULL, NULL,"-1."+repeat("0",25)+"1", "0."+repeat("0",25)+"1", "1."+repeat("0",25)+"1",NULL, NULL, NULL, NULL],330000), 26) as decimal128_26,
                                   decimal128(take([NULL, NULL, NULL, NULL,"-1."+repeat("0",10)+"1", "0."+repeat("0",10)+"1", "1."+repeat("0",10)+"1",NULL, NULL, NULL, NULL],330000), 37) as decimal128_37
                                   )
                        ex = select * from ex order by date
                        each(eqObj, ans.values(), ex.values())
                        """).all()

    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_TableAppender_PT_3_append_decimal128_big_data(self, pickle, compress):
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run("""
        dbName = "dfs://test_decimal128"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db = database(dbName, VALUE, 2012.12.01..2012.12.31, engine='TSDB');
        //pt_1
        schema_t = table(10:0, `date`decimal128_26, [DATE, DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_1, `date, sortColumns=`date)

        //pt_2
        schema_t = table(10:0, `date`decimal128_0`decimal128_6`decimal128_16`decimal128_26`decimal128_37`decimal128_38,
                            [DATE, DECIMAL128(0),DECIMAL128(6),DECIMAL128(16),DECIMAL128(26),DECIMAL128(37),DECIMAL128(38)])
        pt = createPartitionedTable(db, schema_t, `pt_2, `date, sortColumns=`date)

        //pt_3
        schema_t = table(10:0, `date`float`double`string`long`decimal128_6`decimal128_26,
                            [DATE, FLOAT, DOUBLE, STRING, LONG,DECIMAL128(6), DECIMAL128(26)])
        pt = createPartitionedTable(db, schema_t, `pt_3, `date, sortColumns=`date)
        """)
        appender1 = ddb.tableAppender(dbPath="dfs://test_decimal128", tableName="pt_3", ddbSession=conn)
        data = pd.DataFrame({"date": np.array(
            ['2012-12-01T00:00:00', '2012-12-10T00:00:00', '2012-12-20T00:00:00'] * 1100000, dtype="datetime64[D]"),
                             "float": np.array([1, 2, 3] * 1100000, dtype='object'),
                             "double": np.array([1, 2, 3] * 1100000, dtype='object'),
                             "string": np.array(["a", "b", "c"] * 1100000, dtype='object'),
                             "long": np.array([1, 2, 3] * 1100000, dtype='object'),
                             "decimal128_6": np.array([None, pd.NaT, np.nan, decimal.Decimal('NaN'),
                                                       decimal.Decimal("-1." + "0" * 5 + "1"),
                                                       decimal.Decimal("0." + "0" * 5 + "1"),
                                                       decimal.Decimal("1." + "0" * 5 + "1"),
                                                       None, pd.NaT, np.nan, decimal.Decimal('NaN')] * 300000,
                                                      dtype='object'),
                             "decimal128_26": np.array([None, pd.NaT, np.nan, decimal.Decimal('NaN'),
                                                        decimal.Decimal("-1." + "0" * 25 + "1"),
                                                        decimal.Decimal("0." + "0" * 25 + "1"),
                                                        decimal.Decimal("1." + "0" * 25 + "1"),
                                                        None, pd.NaT, np.nan, decimal.Decimal('NaN')] * 300000,
                                                       dtype='object'),
                             })
        appender1.append(data)
        assert conn.run("""
                        ans = select * from loadTable(dbName, `pt_3) order by date
                        ex = table(take([2012.12.01, 2012.12.10, 2012.12.20],3300000) as date,
                                   take(float[1, 2, 3],3300000) as float,
                                   take(double[1, 2, 3],3300000) as double,
                                   take(string["a", "b", "c"],3300000) as string,
                                   take(long[1, 2, 3],3300000) as long,
                                   decimal128(take([NULL, NULL, NULL, NULL,"-1."+repeat("0",5)+"1", "0."+repeat("0",5)+"1", "1."+repeat("0",5)+"1",NULL, NULL, NULL, NULL],3300000), 6) as decimal128_6,
                                   decimal128(take([NULL, NULL, NULL, NULL,"-1."+repeat("0",25)+"1", "0."+repeat("0",25)+"1", "1."+repeat("0",25)+"1",NULL, NULL, NULL, NULL],3300000), 26) as decimal128_26)

                        ex = select * from ex order by date
                        each(eqObj, ans.values(), ex.values())
                        """).all()

    def test_tableAppender_overlength(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * 4 * 64 * 1024
        tbname = 't_' + random_string(5)
        dbPath = "dfs://test_tableAppender"
        self.conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[STRING,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        appender = ddb.tableAppender(dbPath=dbPath, tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data], 'test': '0'})
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            appender.append(df)
        conn.close()

    def test_tableAppender_max_length_string_in_memory_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * (4 * 64 * 1024 - 1)
        tbname = 't_' + random_string(5)
        conn.run(f"""
            {tbname}=table(100:0,[`str],[STRING])
        """)
        appender = ddb.tableAppender(tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data]})
        appender.append(df)
        assert conn.run(f'strlen({tbname}[`str][0])==262143')
        conn.close()

    def test_tableAppender_max_length_symbol_in_memory_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * (4 * 64 * 1024 - 1)
        tbname = 't_' + random_string(5)
        conn.run(f"""
            {tbname}=table(100:0,[`str],[SYMBOL])
        """)
        appender = ddb.tableAppender(tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data]})
        appender.append(df)
        assert conn.run(f'strlen({tbname}[`str][0])==262143')
        conn.close()

    def test_tableAppender_max_length_blob_in_memory_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = b'1' * 4 * 64 * 1024
        tbname = 't_' + random_string(5)
        conn.run(f"""
            {tbname}=table(100:0,[`str],[BLOB])
        """)
        appender = ddb.tableAppender(tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data]})
        appender.append(df)
        assert conn.run(f'strlen({tbname}[`str][0])==262144')
        conn.close()

    def test_tableAppender_max_length_string_stream_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * (4 * 64 * 1024 - 1)
        tbname = 't_' + random_string(5)
        conn.run(f"""
            {tbname}=streamTable(100:0,[`str],[STRING])
        """)
        appender = ddb.tableAppender(tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data]})
        appender.append(df)
        assert conn.run(f'strlen({tbname}[`str][0])==262143')
        conn.close()

    def test_tableAppender_max_length_symbol_stream_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * (4 * 64 * 1024 - 1)
        tbname = 't_' + random_string(5)
        conn.run(f"""
            {tbname}=streamTable(100:0,[`str],[SYMBOL])
        """)
        appender = ddb.tableAppender(tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data]})
        appender.append(df)
        assert conn.run(f'strlen({tbname}[`str][0])==262143')
        conn.close()

    def test_tableAppender_max_length_blob_stream_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = b'1' * 4 * 64 * 1024
        tbname = 't_' + random_string(5)
        conn.run(f"""
            {tbname}=streamTable(100:0,[`str],[BLOB])
        """)
        appender = ddb.tableAppender(tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data]})
        appender.append(df)
        assert conn.run(f'strlen({tbname}[`str][0])==262144')
        conn.close()

    def test_tableAppender_max_length_string_dfs_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * (4 * 64 * 1024 - 1)
        tbname = 't_' + random_string(5)
        dbPath = "dfs://tableAppender"
        conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[STRING,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        appender = ddb.tableAppender(dbPath=dbPath, tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data], 'test': '0'})
        appender.append(df)
        assert conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 64 * 1024 - 1
        conn.close()

    def test_tableAppender_max_length_symbol_dfs_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * 255
        tbname = 't_' + random_string(5)
        dbPath = "dfs://tableAppender"
        conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[SYMBOL,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        appender = ddb.tableAppender(dbPath=dbPath, tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data], 'test': '0'})
        appender.append(df)
        assert conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 255
        conn.close()

    def test_tableAppender_max_length_blob_dfs_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        data = '1' * 4 * 64 * 1024
        tbname = 't_' + random_string(5)
        dbPath = "dfs://tableAppender"
        conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[BLOB,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        appender = ddb.tableAppender(dbPath=dbPath, tableName=tbname, ddbSession=conn)
        df = pd.DataFrame({'id': [data], 'test': '0'})
        appender.append(df)
        assert conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 4 * 64 * 1024
        conn.close()


if __name__ == '__main__':
    pytest.main(["-s", "test/test_AutoFitTableAppender.py"])
