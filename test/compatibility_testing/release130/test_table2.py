import pytest
from setup.settings import *

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import statsmodels.api as sm
from numpy.testing import *
from pandas.testing import *
from setup.utils import get_pid
from setup.utils import random_string

class TestTable2:
    pd_left = pd.DataFrame({'time': pd.to_datetime(
        ['1970-01-01T10:01:01', '1970-01-01T10:01:03', '1970-01-01T10:01:05', '1970-01-01T10:01:05']),
        'symbol': ["X", "Z", "X", "Z"],
        'price': [3, 3.3, 3.2, 3.1],
        'size': [100, 200, 50, 10]})

    pdf_right = pd.DataFrame({'time': pd.to_datetime(
        ['1970-01-01T10:01:01', '1970-01-01T10:01:02', '1970-01-01T10:01:02', '1970-01-01T10:01:03']),
        'symbol': ["X", "Z", "X", "Z"],
        'ask': [90, 150, 100, 52],
        'bid': [70, 200, 200, 68]})
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            stript = r'''
                share_tabs = (select name from objs(true) where shared = true)[`name]
                if(not `shareTrade in share_tabs and not `shareQuote in share_tabs){
                    time1=10:01:01 join 10:01:03 join 10:01:05 join 10:01:05
                    symbol1=take(`X`Z,4)
                    price1=3 3.3 3.2 3.1
                    size1=100 200 50 10
                    Trade=table(time1 as time,symbol1 as symbol,price1 as price,size1 as size)

                    time2=10:01:01 join 10:01:02 join 10:01:02 join 10:01:03
                    symbol2=take(`X`Z,4)
                    ask=90 150 100 52
                    bid=70 200 200 68
                    Quote=table(time2 as time,symbol2 as symbol,ask as ask,bid as bid)

                    share Trade as shareTrade
                    share Quote as shareQuote
                }

                if(not existsDatabase("dfs://testmergepart")){
                    db = database("dfs://testmergepart", VALUE, "X" "Z")
                    pt1 = db.createPartitionedTable(Trade,`pt1,`symbol).append!(Trade)
                    pt2 = db.createPartitionedTable(Quote,`pt2,`symbol).append!(Quote)
                }
            '''
            self.conn.run(stript)

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

    def test_create_table_by_python_dictionary(self):
        data = {'state': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada'],
                'year': [2000, 2001, 2002, 2001, 2002],
                'pop': [1.5, 1.7, 3.6, 2.4, 2.9]}
        tmp = self.conn.table(data=data, tableAliasName="tmp")
        re = self.conn.run("tmp")
        df = pd.DataFrame(data)
        assert_frame_equal(tmp.toDF(), df)
        assert_frame_equal(re, df)

    def test_create_table_by_pandas_dataframe(self):
        data = {'state': ['Ohio', 'Ohio', 'Ohio', 'Nevada', 'Nevada'],
                'year': [2000, 2001, 2002, 2001, 2002],
                'pop': [1.5, 1.7, 3.6, 2.4, 2.9]}
        df = pd.DataFrame(data)
        tmp = self.conn.table(data=df, tableAliasName="tmp")
        re = self.conn.run("tmp")
        assert_frame_equal(tmp.toDF(), df)
        assert_frame_equal(re, df)

    def test_table_toDF(self):
        tmp = self.conn.loadText(DATA_DIR + "/USPrices_FIRST.csv")
        df = self.conn.run(
            "select * from loadText('{data}')".format(data=DATA_DIR + "/USPrices_FIRST.csv"))
        assert len(tmp.toDF()) == len(df)
        assert_frame_equal(tmp.toDF(), df)
        tbName = tmp.tableName()
        self.conn.run("undef", tbName)

    def test_table_showSQL(self):
        tmp = self.conn.loadText(DATA_DIR + "/USPrices_FIRST.csv")
        sql = tmp.showSQL()
        tbName = tmp.tableName()
        assert sql == 'select PERMNO,date,SHRCD,TICKER,TRDSTAT,HEXCD,CUSIP,DLSTCD,DLPRC,DLRET,BIDLO,ASKHI,PRC,VOL,RET,BID,ASK,SHROUT,CFACPR,CFACSHR,OPENPRC from {tbName}'.format(
            tbName=tbName)
        self.conn.run("undef", tbName)

    def test_table_sql_select_where(self):
        data = DATA_DIR + "/USPrices_FIRST.csv"
        tmp = self.conn.loadText(data)

        re = tmp.select(['PERMNO', 'date']).where(tmp.date > '2010.01.01')
        df = self.conn.run(
            "select PERMNO,date from loadText('{data}') where date>2010.01.01".format(data=data))
        assert re.rows == 1510
        assert_frame_equal(re.toDF(), df)

        re = tmp.select(['PERMNO', 'date']).where(
            tmp.date > '2010.01.01').sort(['date desc'])
        df = self.conn.run(
            "select PERMNO,date from loadText('{data}') where date>2010.01.01 order by date desc".format(data=data))
        assert re.rows == 1510
        assert_frame_equal(re.toDF(), df)

        re = tmp[tmp.date > '2010.01.01']
        df = self.conn.run(
            "select * from loadText('{data}') where date>2010.01.01".format(data=data))
        assert re.rows == 1510
        assert_frame_equal(re.toDF(), df)

        tbName = tmp.tableName()
        self.conn.run("undef", tbName)

    def test_table_sql_groupby(self):
        data = DATA_DIR + "/USPrices_FIRST.csv"
        tmp = self.conn.loadText(data)
        origin = tmp.toDF()

        re = tmp.groupby('PERMNO').agg({'bid': ['sum']}).toDF()
        df = self.conn.run(
            "select sum(bid) from loadText('{data}') group by PERMNO".format(data=data))
        assert (re['PERMNO'] == 10001).all()
        assert_almost_equal(re['sum_bid'][0], 59684.9775)
        assert_frame_equal(re, df)

        re = tmp.groupby(['PERMNO', 'date']).agg({'bid': ['sum']}).toDF()
        df = self.conn.run(
            "select sum(bid) from loadText('{data}') group by PERMNO,date".format(data=data))
        assert re.shape[1] == 3
        assert len(re) == 6047
        assert (origin['BID'] == re['sum_bid']).all()
        assert_frame_equal(re, df)

        re = tmp.groupby(['PERMNO', 'date']).agg(
            {'bid': ['sum'], 'ask': ['sum']}).toDF()
        df = self.conn.run(
            "select sum(bid),sum(ask) from loadText('{data}') group by PERMNO,date".format(data=data))
        assert re.shape[1] == 4
        assert len(re) == 6047
        assert (origin['BID'] == re['sum_bid']).all()
        assert (origin['ASK'] == re['sum_ask']).all()
        assert_frame_equal(re, df)

        re = tmp.groupby(['PERMNO']).agg2(
            [ddb.wsum, ddb.wavg], [('bid', 'ask')]).toDF()
        df = self.conn.run(
            "select wsum(bid,ask),wavg(bid,ask) from loadText('{data}') group by PERMNO".format(data=data))
        assert_frame_equal(re, df)

    def test_table_sql_contextby(self):
        data = {'sym': ['A', 'B', 'B', 'A', 'A'], 'vol': [
            1, 3, 2, 5, 4], 'price': [16, 31, 28, 19, 22]}
        dt = self.conn.table(data=data, tableAliasName="tmp")

        re = dt.contextby('sym').agg({'price': [ddb.sum]}).toDF()
        df = self.conn.run("select sym,sum(price) from tmp context by sym")
        assert (re['sym'] == ['A', 'A', 'A', 'B', 'B']).all()
        assert (re['sum_price'] == [57, 57, 57, 59, 59]).all()
        assert_frame_equal(re, df)

        re = dt.contextby(['sym', 'vol']).agg({'price': [ddb.sum]}).toDF()
        df = self.conn.run(
            "select sym,vol,sum(price) from tmp context by sym,vol")
        assert (re['sym'] == ['A', 'A', 'A', 'B', 'B']).all()
        assert (re['vol'] == [1, 4, 5, 2, 3]).all()
        assert (re['sum_price'] == [16, 22, 19, 28, 31]).all()
        assert_frame_equal(re, df)

        re = dt.contextby('sym').agg2(
            [ddb.wsum, ddb.wavg], [('price', 'vol')]).toDF()
        df = self.conn.run(
            "select sym,vol,price,wsum(price,vol),wavg(price,vol) from tmp context by sym")
        assert_frame_equal(re, df)

    def test_table_sql_pivotby(self):
        dt = self.conn.table(data={'sym': ['C', 'MS', 'MS', 'MS', 'IBM', 'IBM', 'C', 'C', 'C'],
                                   'price': [49.6, 29.46, 29.52, 30.02, 174.97, 175.23, 50.76, 50.32, 51.29],
                                   'qty': [2200, 1900, 2100, 3200, 6800, 5400, 1300, 2500, 8800],
                                   'timestamp': pd.date_range('2019-06-01', '2019-06-09')}, tableAliasName="tmp")

        re = dt.pivotby(index='timestamp', column='sym', value='price').toDF()
        expected = self.conn.run(
            'select price from tmp pivot by timestamp,sym')
        assert re.equals(expected)
        assert_frame_equal(re, expected)

        re = dt.pivotby(index='timestamp.month()',
                        column='sym', value='last(price)').toDF()
        expected = self.conn.run(
            'select last(price) from tmp pivot by timestamp.month(),sym')
        assert re.equals(expected)
        assert_frame_equal(re, expected)

        re = dt.pivotby(index='timestamp.month()',
                        column='sym', value='count(price)').toDF()
        expected = self.conn.run(
            'select count(price) from tmp pivot by timestamp.month(),sym')
        assert re.equals(expected)
        assert_frame_equal(re, expected)

        tbName = dt.tableName()
        self.conn.run("undef", tbName)

    def test_table_sql_merge(self):
        dt1 = self.conn.table(data={'id': [1, 2, 3, 3], 'value': [
                              7, 4, 5, 0]}, tableAliasName="t1")
        dt2 = self.conn.table(data={'id': [5, 3, 1], 'qty': [
                              300, 500, 800]}, tableAliasName="t2")

        re = dt1.merge(right=dt2, on='id').toDF()
        expected = self.conn.run('select * from ej(t1,t2,"id")')
        assert_frame_equal(re, expected)

        re = dt1.merge(right=dt2, on='id', how='left').toDF()
        expected = self.conn.run('select * from lj(t1,t2,"id")')
        re.fillna(0, inplace=True)
        expected.fillna(0, inplace=True)
        assert_frame_equal(re, expected)

        re = dt1.merge(right=dt2, on='id', how='outer').toDF()
        expected = self.conn.run('select * from fj(t1,t2,"id")')
        re.fillna(0, inplace=True)
        expected.fillna(0, inplace=True)
        assert_frame_equal(re, expected)

        re = dt2.merge(right=dt1, on='id', how='left semi').toDF()
        expected = self.conn.run('select * from lsj(t2,t1,"id")')
        re.fillna(0, inplace=True)
        expected.fillna(0, inplace=True)
        assert_frame_equal(re, expected)

        self.conn.run("undef", dt1.tableName())
        self.conn.run("undef", dt2.tableName())

    def test_table_sql_mergr_asof(self):
        dt1 = self.conn.table(data={'id': ['A', 'A', 'A', 'B', 'B'],
                                    'date': pd.to_datetime(
            ['2017-02-06', '2017-02-08', '2017-02-10', '2017-02-07', '2017-02-09']),
            'price': [22, 23, 20, 100, 102]},
            tableAliasName="t1")
        dt2 = self.conn.table(data={'id': ['A', 'A', 'B', 'B', 'B'],
                                    'date': pd.to_datetime(
            ['2017-02-07', '2017-02-10', '2017-02-07', '2017-02-08', '2017-02-10'])},
            tableAliasName="t2")

        re = dt2.merge_asof(right=dt1, on=['id', 'date']).toDF()
        expected = self.conn.run('select * from aj(t2,t1,`id`date)')
        assert_frame_equal(re, expected)

    def test_table_sql_merge_cross(self):
        dt1 = self.conn.table(
            data={'year': [2010, 2011, 2012]}, tableAliasName="t1")
        dt2 = self.conn.table(
            data={'ticker': ['IBM', 'C', 'AAPL']}, tableAliasName="t2")
        re = dt1.merge_cross(dt2).toDF()
        expected = self.conn.run('select * from cj(t1,t2)')
        assert_frame_equal(re, expected)

    def test_table_sql_merge_window(self):
        dt1 = self.conn.table(data={'sym': ["A", "A", "B"],
                                    'time': [np.datetime64('2012-09-30 09:56:06'), np.datetime64('2012-09-30 09:56:07'),
                                             np.datetime64('2012-09-30 09:56:06')],
                                    'price': [10.6, 10.7, 20.6]},
                              tableAliasName="t1")
        dt2 = self.conn.table(
            data={'sym': ["A", "A", "A", "A", "A", "A", "A", "A", "A", "A", "B", "B", "B", "B", "B", "B", "B", "B", "B", "B"],
                  'time': pd.date_range(start='2012-09-30 09:56:01', end='2012-09-30 09:56:10', freq='s').append(
                pd.date_range(start='2012-09-30 09:56:01', end='2012-09-30 09:56:10', freq='s')),
                'bid': [10.05, 10.15, 10.25, 10.35, 10.45, 10.55, 10.65, 10.75, 10.85, 10.95, 20.05, 20.15, 20.25,
                        20.35, 20.45, 20.55, 20.65, 20.75, 20.85, 20.95],
                'offer': [10.15, 10.25, 10.35, 10.45, 10.55, 10.65, 10.75, 10.85, 10.95, 11.05, 20.15, 20.25, 20.35,
                          20.45, 20.55, 20.65, 20.75, 20.85, 20.95, 21.01],
                'volume': [100, 300, 800, 200, 600, 100, 300, 800, 200, 600, 100, 300, 800, 200, 600, 100, 300, 800,
                           200, 600]},
            tableAliasName="t2")
        re = dt1.merge_window(right=dt2, leftBound=-5, rightBound=0,
                              aggFunctions="avg(bid)", on=['sym', 'time']).toDF()
        expected = self.conn.run(
            'select * from wj(t1,t2,-5:0,<avg(bid)>,`sym`time)')
        assert_frame_equal(re, expected)

        re = dt1.merge_window(right=dt2, leftBound=-5, rightBound=-1,
                              aggFunctions=["wavg(bid,volume)", "wavg(offer,volume)"], on=["sym", "time"]).toDF()
        expected = self.conn.run(
            'select * from wj(t1,t2,-5:-1,<[wavg(bid,volume), wavg(offer,volume)]>,`sym`time)')
        assert_frame_equal(re, expected)

    def test_table_chinese_column_name(self):
        df = pd.DataFrame(
            {'编号': [1, 2, 3, 4, 5], '序号': ['壹', '贰', '叁', '肆', '伍']})
        tmp = self.conn.table(data=df, tableAliasName="chinese_t")
        res = tmp.toDF()
        assert_array_equal(res['编号'], [1, 2, 3, 4, 5])
        assert_array_equal(res['序号'], ['壹', '贰', '叁', '肆', '伍'])

    def test_table_top_with_other_clause(self):
        df = pd.DataFrame({'id': [10, 8, 5, 6, 7, 9, 1, 4, 2, 3], 'date': pd.date_range(
            '2012-01-01', '2012-01-10', freq="D"), 'value': np.arange(0, 10)})
        tmp = self.conn.table(data=df, tableAliasName="top_t")
        re = tmp.top(3).sort("id").toDF()
        assert_array_equal(re['id'], [1, 2, 3])
        assert_array_equal(re['date'], np.array(
            ['2012-01-07', '2012-01-09', '2012-01-10'], dtype="datetime64[D]"))
        assert_array_equal(re['value'], [6, 8, 9])
        re = tmp.top(3).where("id>5").toDF()
        assert_array_equal(re['id'], [10, 8, 6])
        assert_array_equal(re['date'], np.array(
            ['2012-01-01', '2012-01-02', '2012-01-04'], dtype="datetime64[D]"))
        assert_array_equal(re['value'], [0, 1, 3])
        df = pd.DataFrame({'sym': ["C", "MS", "MS", "MS", "IBM", "IBM", "C", "C", "C"],
                           'price': [49.6, 29.46, 29.52, 30.02, 174.97, 175.23, 50.76, 50.32, 51.29],
                           'qty': [2200, 1900, 2100, 3200, 6800, 5400, 1300, 2500, 8800]})
        tmp = self.conn.table(data=df, tableAliasName="t1")
        re = tmp.top(2).contextby("sym").sort("sym").toDF()
        assert_array_equal(re['sym'], ["C", "C", "IBM", "IBM", "MS", "MS"])
        assert_array_almost_equal(
            re['price'], [49.6, 50.76, 174.97, 175.23, 29.46, 29.52])
        assert_array_equal(re['qty'], [2200, 1300, 6800, 5400, 1900, 2100])

    def test_table_sql_update_where(self):
        n = pd.DataFrame({'timestamp': pd.to_datetime(['09:34:07', '09:36:42', '09:36:51', '09:36:59', '09:32:47', '09:35:26', '09:34:16', '09:34:26', '09:38:12']),
                          'sym': ['C', 'MS', 'MS', 'MS', 'IBM', 'IBM', 'C', 'C', 'C'],
                          'price': [49.6, 29.46, 29.52, 30.02, 174.97, 175.23, 50.76, 50.32, 51.29],
                          'qty': [2200, 1900, 2100, 3200, 6800, 5400, 1300, 2500, 8800]})
        dt1 = self.conn.table(data=n, tableAliasName="t1")
        re = dt1.update(["price"], ["price*10"]
                        ).where("sym=`C").execute().toDF()
        assert_array_almost_equal(
            re["price"], [496, 29.46, 29.52, 30.02, 174.97, 175.23, 507.6, 503.2, 512.9])

    def test_table_twice(self):
        data = {'id': [1, 2, 2, 3],
                'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
                'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                'price': [22, 3.5, 21, 26]}
        dt = self.conn.table(data=data, tableAliasName="t1")
        dt = self.conn.table(data=data, tableAliasName="t1")
        re = self.conn.loadTable("t1").toDF()
        assert_array_equal(data['id'], re['id'])
        assert_array_equal(data['date'], re['date'])
        assert_array_equal(data['ticker'], re['ticker'])
        assert_array_equal(data['price'], re['price'])

    def test_table_repeatedly(self):
        data = {'id': [1, 2, 2, 3],
                'date': np.array(['2019-02-04', '2019-02-05', '2019-02-09', '2019-02-13'], dtype='datetime64[D]'),
                'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
                'price': [22, 3.5, 21, 26]}
        for i in range(1, 100):
            dt = self.conn.table(data=data, tableAliasName="t1")
        re = self.conn.loadTable("t1").toDF()
        assert_array_equal(data['id'], re['id'])
        assert_array_equal(data['date'], re['date'])
        assert_array_equal(data['ticker'], re['ticker'])
        assert_array_equal(data['price'], re['price'])

    def test_table_csort(self):
        script = '''
        sym = `C`MS`MS`MS`IBM`IBM`C`C`C$SYMBOL
        price= 49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29
        qty = 2200 1900 2100 3200 6800 5400 1300 2500 8800
        timestamp = [09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12]
        t1 = table(timestamp, sym, qty, price);
        '''
        self.conn.run(script)
        tb = self.conn.loadTable(tableName="t1")
        re = tb.select(["timestamp", "sym", "qty", "price"]).contextby(
            "sym").csort("timestamp").top(2).toDF()
        expected = self.conn.run(
            "select top 2 timestamp, sym, qty, price from t1 context by sym csort timestamp")
        assert_frame_equal(re, expected)

        re = tb.select(["timestamp", "sym", "qty", "price"]).contextby(
            "sym").csort("timestamp").limit(-2).toDF()
        expected = self.conn.run(
            "select timestamp, sym, qty, price from t1 context by sym csort timestamp limit -2")
        assert_frame_equal(re, expected)

        re = tb.select(["timestamp", "sym", "qty", "price"]).contextby(
            "sym").csort(["timestamp", "qty"]).top(2).toDF()
        expected = self.conn.run(
            "select timestamp, sym, qty, price from t1 context by sym csort timestamp, qty limit 2")
        assert_frame_equal(re, expected)

        re = tb.select(["timestamp", "sym", "qty", "price"]).contextby(
            "sym").csort(["timestamp", "qty"], False).top(2).toDF()
        expected = self.conn.run(
            "select top 2 timestamp, sym, qty, price from t1 context by sym csort timestamp desc, qty desc")
        assert_frame_equal(re, expected)

        re = tb.select(["timestamp", "sym", "qty", "price"]).contextby(
            "sym").csort(["timestamp", "qty"], True).top(2).toDF()
        expected = self.conn.run(
            "select top 2 timestamp, sym, qty, price from t1 context by sym csort timestamp asc, qty asc")
        assert_frame_equal(re, expected)

        re = tb.select(["timestamp", "sym", "qty", "price"]).contextby(
            "sym").csort(["timestamp", "qty"], [True, False]).top(2).toDF()
        expected = self.conn.run(
            "select top 2 timestamp, sym, qty, price from t1 context by sym csort timestamp asc, qty desc")
        assert_frame_equal(re, expected)

        re = tb.select(["timestamp", "sym", "qty", "price"]).contextby(
            "sym").csort(["timestamp", "qty"], [False, True]).top(2).toDF()
        expected = self.conn.run(
            "select top 2 timestamp, sym, qty, price from t1 context by sym csort timestamp desc, qty asc")
        assert_frame_equal(re, expected)

        re = tb.select(["timestamp", "sym", "qty", "price"]).contextby(
            "sym").csort(["timestamp", "qty"], [True, True]).top(2).toDF()
        expected = self.conn.run(
            "select top 2 timestamp, sym, qty, price from t1 context by sym csort timestamp asc, qty asc")
        assert_frame_equal(re, expected)

        re = tb.select(["timestamp", "sym", "qty", "price"]).contextby(
            "sym").csort(["timestamp", "qty"], [False, False]).top(2).toDF()
        expected = self.conn.run(
            "select top 2 timestamp, sym, qty, price from t1 context by sym csort timestamp desc, qty desc")
        assert_frame_equal(re, expected)

    def test_dfs_table_csort(self):
        script = '''
        dbName="dfs://test_csort"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db = database(dbName, VALUE, 1..20)
        n=1000000
        t = table(rand(1..20, n) as id, rand(2012.01.01..2012.06.30, n) as date, rand(100, n) as val)
        db.createPartitionedTable(t, `pt, `id).append!(t)
        '''
        self.conn.run(script)
        tb = self.conn.loadTable(tableName="pt", dbPath="dfs://test_csort")
        re = tb.select(["id", "date", "val"]).contextby(
            "id").csort(["date"]).top(50).toDF()
        expected = self.conn.run(
            '''select top 50 * from loadTable("dfs://test_csort", `pt) context by id csort date ''')
        assert_frame_equal(re, expected)

    def test_table_limit(self):
        script = '''
        sym = `C`MS`MS`MS`IBM`IBM`C`C`C$SYMBOL
        price= 49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29
        qty = 2200 1900 2100 3200 6800 5400 1300 2500 8800
        timestamp = [09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12]
        t = table(timestamp, sym, qty, price);
        '''
        self.conn.run(script)
        tb = self.conn.loadTable(tableName="t")
        re = tb.select("*").limit(2).toDF()
        expected = self.conn.run("select * from t limit 2")
        assert_frame_equal(re, expected)
        # re = tb.select("*").limit(2, 5).toDF()
        # expected = self.conn.run("select * from t limit 2, 5")
        # assert_frame_equal(re, expected)

    def test_table_sort_desc(self):
        script = '''
        sym = `C`MS`MS`MS`IBM`IBM`C`C`C$SYMBOL
        price= 49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29
        qty = 2200 1900 2100 3200 6800 5400 1300 2500 8800
        timestamp = [09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12]
        t1 = table(timestamp, sym, qty, price);
        '''
        self.conn.run(script)
        tb = self.conn.loadTable(tableName="t1")
        re = tb.select("*").sort("timestamp").toDF()
        expected = self.conn.run("select * from t1 order by timestamp asc")
        assert_frame_equal(re, expected)

        re = tb.select("*").sort("timestamp", False).toDF()
        expected = self.conn.run("select * from t1 order by timestamp desc")
        assert_frame_equal(re, expected)

        re = tb.select("*").sort("timestamp", True).toDF()
        expected = self.conn.run("select * from t1 order by timestamp asc")
        assert_frame_equal(re, expected)

        re = tb.select("*").sort(["timestamp", "price"], False).toDF()
        expected = self.conn.run(
            "select * from t1 order by timestamp desc, price desc")
        assert_frame_equal(re, expected)

        re = tb.select("*").sort(["timestamp", "price"], True).toDF()
        expected = self.conn.run(
            "select * from t1 order by timestamp asc, price asc")
        assert_frame_equal(re, expected)

        re = tb.select("*").sort(["timestamp", "price"], [True, False]).toDF()
        expected = self.conn.run(
            "select * from t1 order by timestamp asc, price desc")
        assert_frame_equal(re, expected)

        re = tb.select("*").sort(["timestamp", "price"], [False, True]).toDF()
        expected = self.conn.run(
            "select * from t1 order by timestamp desc, price asc")
        assert_frame_equal(re, expected)

    def test_merge_with_other_operation(self):
        s = self.conn
        trade = s.table(data="shareTrade")
        quote = s.table(data="shareQuote")
        # trade = orca.read_shared_table("shareTrade")
        # quote = orca.read_shared_table("shareQuote")
        #
        pd_left = self.pd_left
        pd_right = self.pdf_right

        # with select
        res_temp = trade.merge(right=quote, how='inner', on=['symbol', 'time']).select(
            ["time", "symbol", "price", "size", "ask-bid as diff"])
        pdf = pd.merge(pd_left, pd_right, on=['symbol', 'time'])
        pdf['diff'] = pdf['ask'] - pdf['bid']
        res = pdf[['time', 'symbol', 'price', 'size', 'diff']]
        assert_frame_equal(res_temp.toDF(), res, check_dtype=False)

        # with sort
        odf_res = trade.merge(right=quote, how='inner', on=[
                              'symbol', 'time']).sort(bys='price')
        pdf_res = pd.merge(pd_left, pd_right, on=[
                           'symbol', 'time']).sort_values(by='price')
        assert_frame_equal(pdf_res, odf_res.toDF(), check_dtype=False)

        # right join with sort
        odf_res = trade.merge(right=quote, how='right', on=['symbol', 'time']).select(
            ["time", "symbol", "price", "size", "ask-bid as diff"]).sort(bys='time ,symbol')
        pdf = pd.merge(pd_left, pd_right, how='right', on=[
                       'symbol', 'time']).sort_values(['symbol', 'time'])
        pdf['diff'] = pdf['ask'] - pdf['bid']
        pdf_res = pdf[['time', 'symbol', 'price', 'size', 'diff']]
        # print(odf_res.toDF())
        # print(pdf_res)
        # assert_frame_equal(odf_res.toDF(),pdf_res,check_dtype=False, check_index_type=False)
        assert_array_equal(
            odf_res.toDF()['time'], pdf_res['time'], verbose=True)
        assert_array_equal(
            odf_res.toDF()['symbol'], pdf_res['symbol'], verbose=True)
        assert_array_equal(
            odf_res.toDF()['price'], pdf_res['price'], verbose=True)
        assert_array_equal(
            odf_res.toDF()['diff'], pdf_res['diff'], verbose=True)
        assert_array_equal(
            odf_res.toDF()['size'], pdf_res['size'], verbose=True)

        # left semi join with sort
        dt1 = self.conn.table(data={'id': [1, 2, 3, 3], 'value': [
                              7, 4, 5, 0]}, tableAliasName="t1")
        dt2 = self.conn.table(data={'id': [5, 3, 1], 'qty': [
                              300, 500, 800]}, tableAliasName="t2")
        odf_res = dt2.merge(right=dt1, how='left semi', on='id').select(
            ["id", "value", "qty", "value-qty as diff"]).sort(bys='id').toDF()

        res = self.conn.run(
            'select id, value,qty, value-qty as diff from lsj(t2,t1,"id") order by id')
        res.fillna(0, inplace=True)
        odf_res.fillna(0, inplace=True)
        assert_frame_equal(odf_res, res)

        self.conn.run("undef", dt1.tableName())
        self.conn.run("undef", dt2.tableName())

    def test_merge_with_other_operation_partition(self):
        s = self.conn
        trade = s.loadTable(dbPath="dfs://testmergepart", tableName="pt1")
        quote = s.loadTable(dbPath="dfs://testmergepart", tableName="pt2")
        # trade = orca.read_shared_table("shareTrade")
        # quote = orca.read_shared_table("shareQuote")
        #
        pd_left = self.pd_left
        pd_right = self.pdf_right

        # with select
        res_temp = trade.merge(right=quote, how='inner', on=['symbol', 'time']).select(
            ["time", "symbol", "price", "size", "ask-bid as diff"])
        pdf = pd.merge(pd_left, pd_right, on=['symbol', 'time'])
        pdf['diff'] = pdf['ask'] - pdf['bid']
        res = pdf[['time', 'symbol', 'price', 'size', 'diff']]
        assert_frame_equal(res_temp.toDF(), res, check_dtype=False)

        # with sort
        odf_res = trade.merge(right=quote, how='inner', on=[
                              'symbol', 'time']).sort(bys='price')
        pdf_res = pd.merge(pd_left, pd_right, on=[
                           'symbol', 'time']).sort_values(by='price')
        # print(odf_res.toDF())
        assert_frame_equal(odf_res.toDF(), pdf_res,  check_dtype=False)

        # right join with sort
        odf_res = trade.merge(right=quote, how='right', on=['symbol', 'time']).select(
            ["time", "symbol", "price", "size", "ask-bid as diff"]).sort(bys='time symbol')
        pdf = pd.merge(pd_left, pd_right, how='right', on=[
                       'symbol', 'time']).sort_values(['time', 'symbol'])
        pdf['diff'] = pdf['ask'] - pdf['bid']
        pdf_res = pdf[['time', 'symbol', 'price', 'size', 'diff']]
        # print(np.array(odf_res.toDF()))
        # print(np.array(pdf_res))
        # assert_frame_equal(odf_res.toDF(),pdf_res,check_dtype=False, check_index_type=False)
        assert_array_equal(
            odf_res.toDF()['time'], pdf_res['time'], verbose=True)
        assert_array_equal(
            odf_res.toDF()['symbol'], pdf_res['symbol'], verbose=True)
        assert_array_equal(
            odf_res.toDF()['price'], pdf_res['price'], verbose=True)
        assert_array_equal(
            odf_res.toDF()['diff'], pdf_res['diff'], verbose=True)
        assert_array_equal(
            odf_res.toDF()['size'], pdf_res['size'], verbose=True)

        # left semi join with sort
        odf_res = trade.merge(right=quote, how='left semi', on=['symbol', 'time']).select(
            ["time", "symbol", "price", "size", "ask-bid as diff"]).sort(bys='price')
        res = self.conn.run(
            'select time, symbol, price,size, ask-bid as diff from lsj(loadTable("dfs://testmergepart", `pt1),loadTable("dfs://testmergepart", `pt2),`symbol`time) order by price')
        # print(res)
        # print(odf_res.toDF())
        assert_frame_equal(odf_res.toDF(), res)
        # assert_array_equal(odf_res.toDF()['time'], res['time'], verbose=True)
        # assert_array_equal(odf_res.toDF()['symbol'], res['symbol'], verbose=True)
        # assert_array_equal(odf_res.toDF()['price'], res['price'], verbose=True)
        # assert_array_equal(odf_res.toDF()['diff'], res['diff'], verbose=True)
        # assert_array_equal(odf_res.toDF()['size'], res['size'], verbose=True)

    def test_merge_asof_with_other_operation(self):
        s = self.conn
        trade = s.table(data="shareTrade")
        quote = s.table(data="shareQuote")
        # inter_quote = quote.select(["temporalAdd(time, -1,s) as time","symbol", "ask", "bid"])
        res_temp = trade.merge_asof(right=quote, on=["symbol", "time"]).select(
            ["time", "symbol", "price", "size", "ask-bid as diff"])

        pd_left = self.pd_left
        pd_right = self.pdf_right
        pdf = pd.merge_asof(pd_left, pd_right, on='time', left_by=[
                            'symbol'], right_by=['symbol'])
        pdf['diff'] = pdf['ask'] - pdf['bid']
        res = pdf[['time', 'symbol', 'price', 'size', 'diff']]

        assert_frame_equal(res_temp.toDF(), res, check_dtype=False)
        #

    def test_merge_asof_with_other_operation_partition(self):
        s = self.conn
        trade = s.loadTable(dbPath="dfs://testmergepart", tableName="pt1")
        quote = s.loadTable(dbPath="dfs://testmergepart", tableName="pt2")
        # inter_quote = quote.select(["temporalAdd(time, -1,s) as time","symbol", "ask", "bid"])
        res_temp = trade.merge_asof(right=quote, on=["symbol", "time"]).select(
            ["time", "symbol", "price", "size", "ask-bid as diff"]).sort('time')

        pd_left = self.pd_left
        pd_right = self.pdf_right
        pdf = pd.merge_asof(pd_left, pd_right, on='time', left_by=[
                            'symbol'], right_by=['symbol'])
        pdf['diff'] = pdf['ask'] - pdf['bid']
        res = pdf[['time', 'symbol', 'price',
                   'size', 'diff']].sort_values("time")

        assert_frame_equal(res_temp.toDF(), res, check_dtype=False)

    def test_table_append(self):
        script = """t1 = table(1000:0,`id`date`ticker`price, [INT,DATE,SYMBOL,DOUBLE])
                    ids = take(1..1000,100)
                    dates = take(2021.01.01..2021.10.01,100)
                    tickers = take(`A`B`C`D,100)
                    prices = rand(1000,100)\\10
                    t2 = table(ids as id,dates as date,tickers as ticker,prices as price)
                    t3 = table(ids as id,dates as date,tickers as ticker,prices as price)
                    share t1 as table1
                    share t2 as table2
                    share t3 as table3
                    """
        self.conn.run(script)
        t1 = self.conn.table(data="table1")
        t2 = self.conn.table(data="table2")
        t3 = self.conn.table(data="table3")
        t1.append(t2)
        assert_frame_equal(t1.toDF(), t2.toDF(), check_dtype=False)
        t2.append(t3)
        self.conn.run("table2.append!(table3)")
        t2_after = self.conn.table(data="table2")
        assert_frame_equal(t2.toDF(), t2_after.toDF(), check_dtype=False)

    def test_table_rename(self):
        script = """ids = take(1..1000,100)
                    dates = take(2021.01.01..2021.10.01,100)
                    tickers = take(`A`B`C`D,100)
                    prices = rand(1000,100)\\10
                    t1 = table(ids as id,dates as date,tickers as ticker,prices as price)
                    share t1 as tglobal
                    """
        self.conn.run(script)
        t1 = self.conn.table(data="tglobal")
        old_name = t1.tableName()
        t1.rename('table1')
        new_name = t1.tableName()
        assert old_name == "tglobal"
        assert new_name == "table1"

    def test_table_delete(self):
        script = """ids = take(1..1000,1000)
                    dates = take(2021.01.01..2021.10.01,1000)
                    tickers = take(`A`B`C`D,1000)
                    prices = rand(1000,1000)\\10
                    t1 = table(ids as id,dates as date,tickers as ticker,prices as price)
                    share t1 as table1
                    """
        self.conn.run(script)
        t1 = self.conn.table(data="table1")
        assert t1.rows == 1000
        t1.delete()
        assert t1.rows == 1000
        t1.delete().execute()
        assert t1.rows == 0

    def test_table_delete_where(self):
        script = """ids = take(1..1000,1000)
                    dates = take(2021.01.01..2021.10.01,1000)
                    tickers = take(`A`B`C`D,1000)
                    prices = rand(1000,1000)\\10
                    t1 = table(ids as id,dates as date,tickers as ticker,prices as price)
                    share t1 as table1
                    t2 = table(ids as id,dates as date,tickers as ticker,prices as price)
                    share t2 as table2
                    """
        self.conn.run(script)
        t1 = self.conn.table(data="table1")
        t1.delete().where("id>400").execute()
        self.conn.run("delete from table2 where id>400")
        ex_row = self.conn.run("exec count(*) from table2")
        assert t1.rows == ex_row

        t1.delete().where("id>200").where("ticker = 'A'").where("price>50").execute()
        self.conn.run(
            "delete from table2 where id>200 and ticker == 'A' and price>50")
        ex_row = self.conn.run("exec count(*) from table2")
        assert t1.rows == ex_row

    def test_table_drop(self):
        script = """ids = take(1..1000,1000)
                    dates = take(2021.01.01..2021.10.01,1000)
                    tickers = take(`A`B`C`D,1000)
                    prices = rand(1000,1000)\\10
                    t1 = table(ids as id,dates as date,tickers as ticker,prices as price)
                    share t1 as table1
                    t2 = table(ids as id,dates as date,tickers as ticker,prices as price)
                    share t2 as table2
                    """

        self.conn.run(script)
        t1 = self.conn.table(data="table1")

        with pytest.raises(RuntimeError):
            t1.drop(['id', 'date', 'ticker', "price"])

        with pytest.raises(RuntimeError):
            t1.drop(['ids'])

        t1.drop(["price"])
        self.conn.run("dropColumns!(table2, 'price')")
        t2 = self.conn.table(data="table2")
        assert_frame_equal(t1.toDF(), t2.toDF(), check_dtype=False)

        t1.drop(['date', 'ticker'])
        self.conn.run("dropColumns!(table2, ['date','ticker'])")
        t2 = self.conn.table(data="table2")
        assert_frame_equal(t1.toDF(), t2.toDF(), check_dtype=False)

        with pytest.raises(RuntimeError):
            t1.drop(["id"])

    def test_table_executeAs(self):
        script = """ids = take(1..1000,1000)
                    dates = take(2021.01.01..2021.10.01,1000)
                    tickers = take(`A`B`C`D,1000)
                    prices = rand(1000,1000)\\10
                    t1 = table(ids as id,dates as date,tickers as ticker,prices as price)
                    share t1 as table1
                    """

        self.conn.run(script)
        t1 = self.conn.table(data="table1")
        # t2 = t1.executeAs("tmp")
        # assert_frame_equal(t1.toDF(), t2.toDF(), check_dtype=False)
        t = t1.select(['date', "id"]).executeAs('t5').select(["date"])
        # print(self.conn.run("t11111"))
        res = self.conn.loadTable(tableName="t5")
        print(res.toDF())

    def test_ols_paramete(self):
        if self.conn.existsDatabase("dfs://valuedb"):
            self.conn.dropDatabase("dfs://valuedb")
        self.conn.database(dbName='mydb', partitionType=keys.VALUE, partitions=[
                           "AMZN", "NFLX", "NVDA"], dbPath="dfs://valuedb")
        self.conn.loadTextEx(dbPath="dfs://valuedb", partitionColumns=[
                             "TICKER"], tableName='trade', remoteFilePath=DATA_DIR + "/example.csv")
        trade = self.conn.loadTable(tableName="trade", dbPath="dfs://valuedb")
        z = trade.ols(Y='PRC', X=['BID'], INTERCEPT=True)
        re = z["Coefficient"]
        prc_tmp = trade.toDF().PRC
        bid_tmp = trade.toDF().BID
        model = sm.OLS(prc_tmp, bid_tmp)
        ex = model.fit().params
        assert_almost_equal(re.iloc[1, 1], ex[0], decimal=4)

    def test_table_function_paramete(self):
        t1 = self.conn.table(dbPath=None, data={
                             'sym': ['C', 'MS']}, tableAliasName="tmp", inMem=False, partitions=None)
        t2 = self.conn.table(dbPath=None, data={'sym': ['C', 'MS'], 'price': [1, 2], 'val': [3, 5], 'timestamp': pd.date_range(
            '2019-06-01', '2019-06-02'), 'ask': [3, 5]}, tableAliasName="tmp", inMem=False, partitions=None)
        t1.append(table=t2)
        t1.contextby(cols='sym')
        t1.csort(bys='sym', ascending=True)
        t1.delete()
        t1.exec(cols='sym')
        t1.execute(expr="")
        t1.executeAs(newTableName="test_name")
        t1.groupby(cols='sym')
        t1.limit(num=2)
        t1.merge(right=t2, how='inner', on="sym", left_on='sym',
                 right_on='sym', sort=False, merge_for_update=False)
        t1.merge_asof(right=t2, on='sym', left_on='sym', right_on='sym')
        t1.merge_cross(right=t2)
        t1.merge_window(right=t1, leftBound=0, rightBound=0, aggFunctions=[
                        "first(sym)"], on='sym', left_on='sym', right_on='sym', prevailing=False)
        t2.pivotby(index='timestamp', column='sym',
                   value='last(price)', aggFunc=None).toDF()
        t1.rename(newName="test_name")
        t1.select(cols="sym")
        t1.sort(bys='sym', ascending=True)
        t2.update(cols=['price'], vals=['price*10']).execute()
        t2.where(conds="price = 1")
        t1.drop(cols='sym')

    def test_table_exec(self):
        script = '''
        sym = `C`MS`MS`MS`IBM`IBM`C`C`C$SYMBOL
        price= 49.6 29.46 29.52 30.02 174.97 175.23 50.76 50.32 51.29
        qty = 2200 1900 2100 3200 6800 5400 1300 2500 8800
        timestamp = [09:34:07,09:36:42,09:36:51,09:36:59,09:32:47,09:35:26,09:34:16,09:34:26,09:38:12]
        t1 = table(timestamp, sym, qty, price)
        insert into t1(timestamp,sym,qty) values(09:34:12,`AAPL,1200)
        '''
        self.conn.run(script)
        table = self.conn.table(data="t1")
        res = table.exec("count(price)").toDF()
        assert res == 9

        res = table.exec("count(qty)").toDF()
        assert res == 10

        res = table.exec("price").toDF()
        expected = [49.6, 29.46, 29.52, 30.02, 174.97,
                    175.23, 50.76, 50.32, 51.29, np.nan]
        assert_array_equal(res, expected)

    def test_table_exec_with_other_operation(self):
        script = '''
            meta = table(`XYZM0`XYZU0`XYZZ0`XYZH1`XYZM1`XYZU1`XYZZ1`XYZH2`XYZM2`XYZU2`XYZZ2 as contract,
	        2020.06.15 2020.09.14 2020.12.14 2021.03.16 2021.06.14 2021.09.13 2021.12.13 2022.03.14 2022.06.13 2022.09.19 2022.12.19 as start_date,
	        2020.09.14 2020.12.14 2021.03.16 2021.06.14 2021.09.13 2021.12.13 2022.03.14 2022.06.13 2022.09.19 2022.12.19 2023.03.13 as end_date)

            price = table(2021.01.01T00:00:00 2021.01.01T00:00:00 2021.01.01T00:01:00 2021.01.01T00:01:00 2021.01.02T23:59:00 2021.01.02T23:59:00 as datetime,
	            `XYZH1`XYZM1`XYZH1`XYZM1`XYZH1`XYZM1 as contract,
	            99 98.1 99.5 99.1 99.3 99.2 as price)
        '''
        self.conn.run(script)
        t = self.conn.loadTable("price")
        res1 = t.exec("price").pivotby("datetime.minute()", "contract").toDF()
        expected = self.conn.run(
            '''exec price from price pivot by datetime.minute(), contract''')
        assert_array_equal(res1[0], expected[0])
        assert_array_equal(res1[1], expected[1])
        assert_array_equal(res1[2], expected[2])

        t1 = self.conn.loadTable("meta")
        res1 = t1.exec("*").merge(right=t, how="inner", on="contract").toDF()
        expected = self.conn.run("exec * from ej(meta, price, `contract)")
        assert_array_equal(res1, expected)
        # print(res1)
        # print(expected)

        res1 = t1.exec("*").merge(right=t1, how="left semi",
                                  on='contract').toDF()
        expected = self.conn.run("exec * from lsj(meta, meta as a, `contract)")
        assert_array_equal(res1, expected)

    def test_table_exec_with_pivot_by(self):
        dt = self.conn.table(data={'sym': ['C', 'MS', 'MS', 'MS', 'IBM', 'IBM', 'C', 'C', 'C'],
                                   'price': [49.6, 29.46, 29.52, 30.02, 174.97, 175.23, 50.76, 50.32, 51.29],
                                   'qty': [2200, 1900, 2100, 3200, 6800, 5400, 1300, 2500, 8800],
                                   'timestamp': pd.date_range('2019-06-01', '2019-06-09')}, tableAliasName="tmp")

        re = dt.exec("price").pivotby("timestamp", "sym").toDF()
        # print(re)
        expected = self.conn.run('exec price from tmp pivot by timestamp,sym')
        # assert re.equals(expected), True)
        assert_array_equal(re[0], expected[0])
        assert_array_equal(re[1], expected[1])
        assert_array_equal(re[2], expected[2])

        #
        re = dt.exec("price").pivotby(
            index='timestamp.month()', column='sym').toDF()
        expected = self.conn.run(
            'exec price from tmp pivot by timestamp.month(),sym')
        # assert re.equals(expected), True)
        assert_array_equal(re[0], expected[0])
        assert_array_equal(re[1], expected[1])
        assert_array_equal(re[2], expected[2])

        tbName = dt.tableName()
        self.conn.run("undef", tbName)

    def test_runFile(self):
        file_path = LOCAL_DATA_DIR+"/run_data.txt"
        s = self.conn
        s.runFile(file_path)
        t1 = s.table(data="t1")
        re1 = s.table(data="re1")
        exec1 = s.run('''select stdp(value) from t1''')
        assert_frame_equal(re1.toDF(), exec1)

        re2 = s.table(data="re2")
        exec2 = s.run('''select sum(value) from t1''')
        assert_frame_equal(re2.toDF(), exec2)

        # db = s.database(dbPath="dfs://test")
        pt1 = s.loadTable(dbPath="dfs://test", tableName="pt1")
        pt1.append(t1)
        assert_frame_equal(pt1.sort(bys='time').toDF(),
                           t1.sort(bys="time").toDF())

        t_share = s.table(data="t_share")
        t_share.append(t1)
        output = s.table(data="output1")
        size3 = len(output.toDF())
        assert size3 > 0

        undef = '''
            unsubscribe("t_share","sub_1")
            dropAggregator("test1")
            if(existsDatabase("dfs://test")){
                dropDatabase("dfs://test")
            }
            undef((exec name from objs(true) where shared=1),SHARED)
        '''
        s.run(undef)

    def test_get_float_null_over_1024(self):
        self.conn.run("a = table(1..1025 as a,take(float(),1025) as b)")
        dbvalue = self.conn.run("a")
        assert np.isnan(dbvalue.loc[1024, "b"])

    def test_get_double_null_over_1024(self):
        self.conn.run("a = table(1..1025 as a,take(double(),1025) as b)")
        dbvalue = self.conn.run("a")
        assert np.isnan(dbvalue.loc[1024, "b"])

    def test_get_int_null_over_1024(self):
        self.conn.run("a = table(1..1025 as a,take(int(),1025) as b)")
        dbvalue = self.conn.run("a")
        assert np.isnan(dbvalue.loc[1024, "b"])

    def test_get_string_null_over_1024(self):
        self.conn.run("a = table(1..1025 as a,take(string(),1025) as b)")
        dbvalue = self.conn.run("a")
        assert dbvalue.loc[1024, "b"] == ""

    def test_upload_dataframe_as_table(self):
        df = pd.DataFrame({
            'bool': np.array([True, False], dtype=np.bool8),
            'char': np.array([1, -1], dtype=np.int8),
            'short': np.array([-10, 1000], dtype=np.int16),
            'int': np.array([-10, 1000], dtype=np.int32),
            'long': np.array([-100000000, 10000000000], dtype=np.int64),
            'date': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[D]"),
            'time': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]"),
            'minute': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[m]"),
            'second': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]"),
            'datetime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[s]"),
            'datehour': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[h]"),
            'timestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ms]"),
            'nanotime': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]"),
            'nanotimestamp': np.array(["2012-02-03T01:02:03.456789123", "2013-04-02T02:05:06.123456789"], dtype="datetime64[ns]"),
            'float': np.array([2.2134500, np.nan], dtype='float32'),
            'double': np.array([3.214, np.nan], dtype='float64'),
            'symbol': np.array(['sym1', ''], dtype='object'),
            'string': np.array(['str1', ''], dtype='object'),
            'ipaddr': np.array(["192.168.1.1", "0.0.0.0"], dtype='object'),
            'uuid': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", ""], dtype='object'),
            'int128': np.array(["e1671797c52e15f763380b45e841ec32", ""], dtype='object'),
            'blob': np.array(['blob1', ''], dtype='object')
        })

        df.__DolphinDB_Type__ = {
            'bool': keys.DT_BOOL,
            'char': keys.DT_CHAR,
            'short': keys.DT_SHORT,
            'int': keys.DT_INT,
            'date': keys.DT_DATE,
            'time': keys.DT_TIME,
            'minute': keys.DT_MINUTE,
            'second': keys.DT_SECOND,
            'datetime': keys.DT_DATETIME,
            'datehour': keys.DT_DATEHOUR,
            'timestamp': keys.DT_TIMESTAMP,
            'nanotime': keys.DT_NANOTIME,
            'nanotimestamp': keys.DT_NANOTIMESTAMP,
            'float': keys.DT_FLOAT,
            'double': keys.DT_DOUBLE,
            'symbol': keys.DT_SYMBOL,
            'string': keys.DT_STRING,
            'ipaddr': keys.DT_IPADDR,
            'uuid': keys.DT_UUID,
            'int128': keys.DT_INT128,
            'blob': keys.DT_BLOB,
        }

        self.conn.upload({'a': df})

        self.conn.run("""
                        symbolV = symbol[`sym1,''] 
                        ipV = ipaddr["192.168.1.1", "0.0.0.0"]
                        uuidV = uuid["5d212a78-cc48-e3b1-4235-b4d91473ee87", ""] 
                        int128V = int128["e1671797c52e15f763380b45e841ec32", ""]
                        blobV = blob['blob1', ''] 

                        t=table([bool(1),bool(0)] as bool,
                        [char(1),char(-1)] as char,
                        [short(-10),short(1000)] as short,
                        [int(-10),int(1000)] as int,
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
                        symbolV as sym,
                        [`str1,''] as str,
                        ipV as ipaddr,
                        uuidV as uuid,
                        int128V as int128,
                        blobV as blob)
                    """)
        assert_array_equal(self.conn.run("each(eqObj,t.values(),a.values())"), [
                           True for _ in range(22)])

    def test_table_dropPartition_loadtable_tmp_variable(self):
        self.conn.run("""
                dbName = "dfs://PTA1_test"
                tbName = "pt"
                if(existsDatabase(dbName)) {dropDatabase(dbName)}
                db = database(dbName, VALUE, 1..5)
                model = table(1:0, `ID`Price`Volume`ids, [INT, DOUBLE, DOUBLE, INT])
                createPartitionedTable(db, model, tbName, `ID)

                n =5
                data = table(take(1..5, n) as ID, rand(100.0, n) as Price, rand(100000, n) as Volume, take(0..1, n) as ids)
                PTA1_test = loadTable(dbName, tbName)
                PTA1_test.append!(data)
                """)
        df = pd.DataFrame({
            'ID': [1, 2, 3, 4, 5],
            'Price': [1.1, 2.2, 3.3, 4.4, 5.5],
            'Volume': [9.9, 8.8, 7.7, 6.6, 5.5],
            'ids': [100, 200, 30000, 400000, 50000],
        })
        print(df)
        for i in range(2):
            a = self.conn.run("select count(*) from objs(false)")
            b = self.conn.run("objs(false)")
            del b
            self.conn.dropPartition(
                dbPath="dfs://PTA1_test", partitionPaths="5", tableName="pt")
            factortable = self.conn.loadTable(
                tableName="pt", dbPath="dfs://PTA1_test")
            day_table = self.conn.table(data=df)
            factortable.append(day_table)
            del factortable
            del day_table
            b = self.conn.run("select count(*) from objs(false)")
            assert a["count"][0] == b["count"][0]
        self.conn.dropDatabase("dfs://PTA1_test")

    def test_sql_pattern_match(self):
        dbname = "dfs://test_"+ random_string(5)
        dbname2 = "dfs://test_"+ random_string(5)
        script = '''
        dbName="dfs://test_csort"
        if(existsDatabase(dbName)){
            dropDatabase(dbName)
        }
        db = database(dbName, VALUE, 1..20)
        n=1000000
        t = table(rand(1..20, n) as id, rand(2012.01.01..2012.06.30, n) as date, rand(100, n) as val)
        db.createPartitionedTable(t, `pt, `id).append!(t)
        '''
        self.conn.run(script)
        pt = self.conn.table(data="pt", dbPath="dfs://test_csort")
        db = self.conn.database('db', keys.VALUE, [x for x in range(1, 21)], dbname)
        db2 = self.conn.database('db2', keys.VALUE, [x for x in range(1, 21)], dbname2)
        t = db.createTable(pt, 'pt')
        t2 = db2.createPartitionedTable(pt, 'pt', 'id')

        import re
        sql1 = self.conn.table(data="pt", dbPath="dfs://test_csort").contextby("id").agg("sum").showSQL()
        sql2 = self.conn.loadTable(tableName="pt", dbPath="dfs://test_csort").contextby("id").agg("sum").showSQL()
        sql3 = self.conn.loadTableBySQL(tableName="pt", dbPath="dfs://test_csort", sql="select * from pt").contextby("id").agg("sum").showSQL()
        sql4 = self.conn.loadText(DATA_DIR+'/sql_pattern_test.csv').contextby("id").agg("sum").showSQL()
        sql5 = self.conn.loadTextEx("dfs://test_csort", "pt", ["id"], DATA_DIR+'/sql_pattern_test.csv').contextby("id").agg("sum").showSQL()
        sql6 = t.contextby("id").agg("sum").showSQL()
        sql7 = t2.contextby("id").agg("sum").showSQL()
        sql8 = self.conn.ploadText(DATA_DIR+'/sql_pattern_test.csv').contextby("id").agg("sum").showSQL()

        pattern1 = r"^select id,sum\(date\),sum\(val\) from pt_TMP_TBL_[a-zA-Z0-9]+ context by id$"
        pattern2 = r"^select id,sum\(date\),sum\(val\) from TMP_TBL_[a-zA-Z0-9]+ context by id$"

        assert bool(re.match(pattern1, sql1))
        assert bool(re.match(pattern1, sql2))
        assert bool(re.match(pattern2, sql3))
        assert bool(re.match(pattern2, sql4))
        assert bool(re.match(pattern1, sql5))
        assert bool(re.match(pattern2, sql6))
        assert bool(re.match(pattern2, sql7))
        assert bool(re.match(pattern2, sql8))
        self.conn.dropDatabase("dfs://test_csort")
        self.conn.dropDatabase(dbname)
        self.conn.dropDatabase(dbname2)

if __name__ == '__main__':
    pytest.main(["-s", "test/test_table2.py"])
