import time
import pytest
from setup.utils import get_pid
from setup.prepare import *
from setup.settings import *
from numpy.testing import *
from pandas.testing import *
import decimal
import random


class TestPartitionedTableAppender:
    conn = ddb.session(enablePickle=False)

    def setup_method(self):
        try:
            self.conn.run("1")
        except RuntimeError:
            self.conn.connect(HOST, PORT, USER, PASSWD)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        cls.pool_list = {
            "COMPRESS_OPEN": ddb.DBConnectionPool(HOST, PORT, 4, USER, PASSWD, compress=True),
            "COMPRESS_CLOSE": ddb.DBConnectionPool(HOST, PORT, 4, USER, PASSWD, compress=False),
        }
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        for i in cls.pool_list.values():
            i.shutDown()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def test_PartitionedTableAppender_append_type_error(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        script = '''
            dbPath = "dfs://PartitionedTableAppender"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])
            db=database(dbPath,RANGE,100000 200000 300000 400000 600001)
            pt = db.createPartitionedTable(t, `pt, `qty)
        '''
        self.conn.run(script)
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender", "pt", "qty", pool)
        with pytest.raises(RuntimeError, match="table must be a DataFrame!"):
            appender.append(None)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_append(self, _compress):
        pool = self.pool_list[_compress]
        script = '''
            dbPath = "dfs://PartitionedTableAppender"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIMESTAMP, INT])
            db=database(dbPath,RANGE,100 200 300 400 601)
            pt = db.createPartitionedTable(t, `pt, `qty)
        '''
        self.conn.run(script)
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender", "pt", "qty", pool)
        sym = list(map(str, np.arange(100, 600)))
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23',
                                 '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'], 50), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426',
                                 'NaT', 'NaT', '2015-06-09T23:59:59.999'], 100), dtype="datetime64")
        second = np.array(np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48',
                                   'NaT', 'NaT', '2015-06-09T23:59:59'], 100), dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006',
                                     'NaT', 'NaT', '2015-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        qty = np.arange(100, 600)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month': month, 'time': time, 'minute': time,
                             'second': second, 'datetime': second, 'timestamp': time, 'nanotimestamp': nanotime,
                             'qty': qty})
        num = appender.append(data)
        assert num == self.conn.run('exec count(*) from loadTable("dfs://PartitionedTableAppender", "pt")')

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_range_int(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            db=database(dbPath,RANGE,[1,10001,20001,30001,40001,50001,60001])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.random.randint(1, 2001, 1000)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_range_short(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,SHORT,INT,DOUBLE])
            db=database(dbPath,RANGE,short([1,10001,20001,30001]))
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.random.randint(1, 2001, 1000)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_range_symbol(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            sym_range=cutPoints(symbol(string(10001..60000)), 10)
            db=database(dbPath,RANGE,sym_range)
            pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.random.randint(0, 2001, 1000)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 60001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_range_string(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[STRING,INT,INT,DOUBLE])
            sym_range=cutPoints(string(10001..60000), 10)
            db=database(dbPath,RANGE,sym_range)
            pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.random.randint(0, 2001, 1000)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_value_int(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            db=database(dbPath,VALUE,1..10)
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.repeat(np.arange(1, 11), 100, axis=0)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_value_short(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,SHORT,INT,DOUBLE])
            db=database(dbPath,VALUE,short(1..10))
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.repeat(np.arange(1, 11), 100, axis=0)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_value_symbol(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            db=database(dbPath,VALUE,symbol(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO']))
            pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", pool)
        sym = np.repeat(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO'], 200, axis=0)
        id = np.random.randint(0, 2001, 1000)
        qty = np.random.randint(0, 2001, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_value_string(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[STRING,INT,INT,DOUBLE])
            db=database(dbPath,VALUE,['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO'])
            pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", pool)
        sym = np.repeat(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO'], 200, axis=0)
        id = np.random.randint(0, 60001, 1000)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 60001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_hash_int(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            db=database(dbPath,HASH,[INT, 10])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.repeat(np.arange(1, 1001), 1, axis=0)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_hash_short(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,SHORT,INT,DOUBLE])
            db=database(dbPath,HASH,[SHORT, 10])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.repeat(np.arange(1, 101), 10, axis=0)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_hash_string(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[STRING,INT,INT,DOUBLE])
            db=database(dbPath,HASH,[STRING, 10])
            pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.random.randint(0, 6001, 1000)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 60001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_hash_symbol(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            db=database(dbPath,HASH,[SYMBOL, 10])
            pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.random.randint(0, 2001, 1000)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_list_int(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            db=database(dbPath,LIST,[[1, 3, 5], [2, 4, 6], [7, 8, 9, 10]])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender(
            "dfs://PTA_test", "pt", "id", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.repeat(np.arange(1, 11), 100, axis=0)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 6001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_list_short(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,SHORT,INT,DOUBLE])
            db=database(dbPath,LIST,[[1, 3, 5], [2, 4, 6], [7, 8, 9, 10]])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "id", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        id = np.repeat(np.arange(1, 11), 100, axis=0)
        qty = np.random.randint(0, 101, 1000)
        price = np.random.randint(0, 2001, 1000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 1000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 1000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_list_symbol(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            db=database(dbPath,LIST,[symbol(string(1001..2000)), symbol(string(2001..4000)), symbol(string(4001..6000))])
            pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", pool)
        sym = list(map(str, np.arange(1001, 6001)))
        id = np.random.randint(0, 6001, 5000)
        qty = np.random.randint(0, 101, 5000)
        price = np.random.randint(0, 6001, 5000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 5000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 5000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_list_string(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PTA_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`id`qty`price,[SYMBOL,INT,INT,DOUBLE])
            db=database(dbPath,LIST,[string(1001..2000), string(2001..4000), string(4001..6000)])
            pt = db.createPartitionedTable(t, `pt, `sym)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PTA_test", "pt", "sym", pool)
        sym = list(map(str, np.arange(1001, 6001)))
        id = np.random.randint(0, 6001, 5000)
        qty = np.random.randint(0, 101, 5000)
        price = np.random.randint(0, 60001, 5000) * 0.1
        data = pd.DataFrame({'sym': sym, 'id': id, 'qty': qty, 'price': price})
        num = appender.append(data)
        assert num == 5000
        re = self.conn.run("select * from loadTable('dfs://PTA_test', 'pt') order by id, sym, qty, price")
        expected = data.sort_values(by=['id', 'sym', 'qty', 'price'], ascending=[True, True, True, True])
        expected.set_index(np.arange(0, 5000), inplace=True)
        assert_frame_equal(re, expected, check_dtype=False, check_index_type=False)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_compo_value_list(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath="dfs://db_compoDB_sym"
            if (existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`ticker,[SYMBOL,STRING])
            dbSym = database(,VALUE,`aaa`bbb`ccc`ddd)
            dbTic = database(, LIST, [`IBM`ORCL`MSFT, `GOOG`FB] )
            db = database("dfs://db_compoDB_sym", COMPO, [dbSym, dbTic])
            pt = db.createPartitionedTable(t, `pt, `sym`ticker)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_sym", "pt", "sym", pool)
        n = 1000
        x = np.array(['aaa', 'bbb', 'ccc', 'ddd'])
        y = np.array(['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'])
        data = pd.DataFrame({"sym": np.repeat(x, 250), "ticker": np.repeat(y, 200)})
        re = appender.append(data)
        assert re == n
        re = self.conn.run('select * from loadTable("dfs://db_compoDB_sym",`pt)')
        assert_frame_equal(data, re)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_compo_range_list(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath="dfs://db_compoDB_int"
            if (existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`ticker,[INT,STRING])
            dbId = database(,RANGE,0 40000 80000 120000)
            dbTic = database(, LIST, [`IBM`ORCL`MSFT, `GOOG`FB] )
            db = database("dfs://db_compoDB_int", COMPO, [dbId, dbTic])
            pt = db.createPartitionedTable(t, `pt, `id`ticker)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_int", "pt", "id", pool)
        n = 1000
        y = np.array(['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'])
        data = pd.DataFrame({"id": range(0, n), "ticker": np.repeat(y, 200)})
        data['id'] = data["id"].astype("int32")
        re = appender.append(data)
        assert re == n
        re = self.conn.run('select * from loadTable("dfs://db_compoDB_int",`pt)')
        assert_frame_equal(data, re)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_compo_hash_range(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath="dfs://db_compoDB_int"
            if (existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`ticker,[INT,STRING])
            dbId = database(,HASH,[INT,2])
            sym_range=cutPoints(string(10001..60000), 10)
            dbTic = database(, RANGE, sym_range )
            db = database("dfs://db_compoDB_int", COMPO, [dbId, dbTic])
            pt = db.createPartitionedTable(t, `pt, `id`ticker)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_int", "pt", "id", pool)
        n = 1000
        id = np.repeat(np.arange(1, 1001), 1, axis=0)
        ticker = list(map(str, np.arange(1001, 2001)))
        data = pd.DataFrame({"id": id, "ticker": ticker})
        data['id'] = data["id"].astype("int32")
        re = appender.append(data)
        assert re == n
        re = self.conn.run('select * from loadTable("dfs://db_compoDB_int",`pt) order by id,ticker')
        assert_frame_equal(data, re)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_compo_hash_list(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath="dfs://db_compoDB_sym"
            if (existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`sym`ticker,[SYMBOL,STRING])
            dbSym = database(,HASH,[SYMBOL,2])
            dbTic = database(, LIST,  [`IBM`ORCL`MSFT, `GOOG`FB] )
            db = database("dfs://db_compoDB_sym", COMPO, [dbSym, dbTic])
            pt = db.createPartitionedTable(t, `pt, `sym`ticker)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_sym", "pt", "sym", pool)
        sym = list(map(str, np.arange(1001, 2001)))
        y = np.array(['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'])
        data = pd.DataFrame({"sym": sym, "ticker": np.repeat(y, 200)})
        re = appender.append(data)
        assert re == 1000
        re = self.conn.run('select * from loadTable("dfs://db_compoDB_sym",`pt) order by sym,ticker')
        assert_frame_equal(data, re)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_compo_hash_value(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath="dfs://db_compoDB_str"
            if (existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`str`ticker,[STRING,SYMBOL])
            dbStr = database(,HASH,[STRING,10])
            dbTic = database(, VALUE,  symbol(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO']) )
            db = database("dfs://db_compoDB_str", COMPO, [dbStr, dbTic])
            pt = db.createPartitionedTable(t, `pt, `str`ticker)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_str", "pt", "str", pool)
        n = 1000
        y = list(map(str, np.arange(1001, 2001)))
        ticker = np.repeat(['AAPL', 'MSFT', 'IBM', 'GOOG', 'YHOO'], 200)
        data = pd.DataFrame({"str": y, "ticker": ticker})
        re = appender.append(data)
        assert re == n
        re = self.conn.run('select * from loadTable("dfs://db_compoDB_str",`pt) order by str,ticker')
        assert_frame_equal(data, re)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_compo_value_list_range(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
           dbPath="dfs://db_compoDB_sym"
           if (existsDatabase(dbPath))
               dropDatabase(dbPath)
           t = table(100:100,`sym`ticker`id,[SYMBOL,STRING,INT])
           dbSym = database(,VALUE,`aaa`bbb`ccc`ddd)
           dbTic = database(, LIST, [`IBM`ORCL`MSFT, `GOOG`FB] )
           dbId = database(,RANGE,0 40000 80000 120000)
           db = database("dfs://db_compoDB_sym", COMPO, [dbSym, dbTic,dbId])
           pt = db.createPartitionedTable(t, `pt, `sym`ticker`id)
       ''')
        appender = ddb.PartitionedTableAppender("dfs://db_compoDB_sym", "pt", "sym", pool)
        n = 1000
        x = np.array(['aaa', 'bbb', 'ccc', 'ddd'])
        y = np.array(['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'])
        data = pd.DataFrame({"sym": np.repeat(x, 250), "ticker": np.repeat(y, 200), 'id': range(0, n)})
        data['id'] = data["id"].astype("int32")
        re = appender.append(data)
        assert re == n
        re = self.conn.run('select * from loadTable("dfs://db_compoDB_sym",`pt)')
        assert_frame_equal(data, re)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_all_time_types(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_all_time_types"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(1000:0, `sym`date`month`time`minute`second`datetime`timestamp`nanotime`nanotimestamp`qty, [SYMBOL, DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,INT])
            db=database(dbPath,RANGE,1000 2000 3000 4000 6001)
            pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_all_time_types", "pt", "qty", pool)
        sym = list(map(str, np.arange(1000, 6000)))
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23',
                                 '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'], 500), dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 1000), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '2015-06-09T23:59:59.999'],
                    1000), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '2015-06-09T23:59:59'], 1000),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '2015-06-09T23:59:59.999008007'], 1000), dtype="datetime64")
        qty = np.arange(1000, 6000)
        data = pd.DataFrame({'sym': sym, 'date': date, 'month': month, 'time': time, 'minute': time, 'second': second,
                             'datetime': second, 'timestamp': time, 'nanotime': nanotime, 'nanotimestamp': nanotime,
                             'qty': qty})
        num = appender.append(data)
        assert num == 5000
        script = '''
            n = 5000
            tmp=table(string(1000..5999) as sym, take([2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05],n) as date,take([1965.08M, NULL, 2012.02M, 2012.03M, NULL],n) as month,
            take([00:00:00.000, 05:12:48.426, NULL, NULL, 23:59:59.999],n) as time, take([00:00m, 05:12m, NULL, NULL, 23:59m],n) as minute, take([00:00:00, 05:12:48, NULL, NULL, 23:59:59],n) as second,take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 2015.06.09T23:59:59],n) as datetime,
            take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 2015.06.09T23:59:59.999],n) as timestamp,take([00:00:00.000000000, 05:12:48.008007006, NULL, NULL, 23:59:59.999008007],n) as nanotime,take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006, NULL, NULL, 2015.06.09T23:59:59.999008007],n) as nanotimestamp,
            1000..5999 as qty)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_all_time_types",`pt)
            each(eqObj, tmp.values(), re.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_datehour(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_datehour"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(datehour(2020.01.01T01:01:01) as time, 1 as qty)
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001)
            pt = db.createPartitionedTable(t, `pt, `qty)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_datehour", "pt", "qty", pool)
        n = 5000
        time = pd.date_range(start='2020-01-01T01', periods=n, freq='h')
        qty = np.arange(1, n + 1)
        data = pd.DataFrame({'time': time, 'qty': qty})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 5000
            ex = table((datehour(2020.01.01T00:01:01)+1..n) as time,1..n as qty)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_datehour",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_date(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_date"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`date1`date2`date3`date4`date5,[INT,DATE,DATE,DATE,DATE,DATE])
            db=database(dbPath,RANGE,[1,101,201,301,401,501,601])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_date", "pt", "id", pool)
        n = 500
        id = np.arange(100, 600)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        data = pd.DataFrame(
            {'id': id, 'date1': date, 'date2': month, 'date3': time, 'date4': second, 'date5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500
            ids = 100..599
            time_name = `date
            dates =funcByName(time_name)(take([2012.01.01, NULL,1965.07.25, NULL, 1970.01.01],n))
            months =  funcByName(time_name)(take([1965.08M, NULL,2012.02M, 2012.03M, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as date1,months as date2,times as date3,seconds as date4,nanotimes as date5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_date",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_month(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_month"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`month1`month2`month3`month4`month5,[INT,MONTH,MONTH,MONTH,MONTH,MONTH])
            db=database(dbPath,RANGE,[1,101,201,301,401,501,601])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_month", "pt", "id", pool)
        n = 500
        id = np.arange(100, 600)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        data = pd.DataFrame(
            {'id': id, 'month1': date, 'month2': month, 'month3': time, 'month4': second, 'month5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500
            ids = 100..599
            time_name = `month
            dates =funcByName(time_name)(take([2012.01.01, NULL,1965.07.25, NULL, 1970.01.01],n))
            months =  funcByName(time_name)(take([1965.08M, NULL,2012.02M, 2012.03M, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_month",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_time(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_time"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`time1`time2`time3`time4`time5,[INT,TIME,TIME,TIME,TIME,TIME])
            db=database(dbPath,RANGE,[1,101,201,301,401,501,601])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender(
            "dfs://test_PartitionedTableAppender_to_time", "pt", "id", pool)
        n = 500
        id = np.arange(100, 600)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        data = pd.DataFrame(
            {'id': id, 'time1': date, 'time2': month, 'time3': time, 'time4': second, 'time5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500
            ids = 100..599
            time_name = `time
            dates =funcByName(time_name)(take([0, NULL,0, NULL, 0],n))
            months =  funcByName(time_name)(take([0, NULL,0, 0, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as time1,months as time2,times as time3,seconds as time4,nanotimes as time5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_time",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_minute(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_minute"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`minute1`minute2`minute3`minute4`minute5,[INT,MINUTE,MINUTE,MINUTE,MINUTE,MINUTE])
            db=database(dbPath,RANGE,[1,101,201,301,401,501,601])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_minute", "pt", "id", pool)
        n = 500
        id = np.arange(100, 600)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        data = pd.DataFrame(
            {'id': id, 'minute1': date, 'minute2': month, 'minute3': time, 'minute4': second, 'minute5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500
            ids = 100..599
            time_name = `minute
            dates =funcByName(time_name)(take([0, NULL,0, NULL, 0],n))
            months =  funcByName(time_name)(take([0, NULL,0, 0, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as time1,months as time2,times as time3,seconds as time4,nanotimes as time5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_minute",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_second(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_second"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`second1`second2`second3`second4`second5,[INT,SECOND,SECOND,SECOND,SECOND,SECOND])
            db=database(dbPath,RANGE,[1,101,201,301,401,501,601])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_second", "pt", "id", pool)
        n = 500
        id = np.arange(100, 600)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        data = pd.DataFrame(
            {'id': id, 'second1': date, 'second2': month, 'second3': time, 'second4': second, 'second5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500
            ids = 100..599
            time_name = `second
            dates =funcByName(time_name)(take([0, NULL,0, NULL, 0],n))
            months =  funcByName(time_name)(take([0, NULL,0, 0, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as time1,months as time2,times as time3,seconds as time4,nanotimes as time5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_second",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_datetime(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_datetime"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`datetime1`datetime2`datetime3`datetime4`datetime5,[INT,DATETIME,DATETIME,DATETIME,DATETIME,DATETIME])
            db=database(dbPath,RANGE,[1,101,201,301,401,501,601])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_datetime", "pt", "id", pool)
        n = 500
        id = np.arange(100, 600)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        data = pd.DataFrame({'id': id, 'datetime1': date, 'datetime2': month, 'datetime3': time, 'datetime4': second,
                             'datetime5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500
            ids = 100..599
            time_name = `datetime
            dates =funcByName(time_name)(take([2012.01.01, NULL,1965.07.25, NULL, 1970.01.01],n))
            months =  funcByName(time_name)(take([1965.08.01, NULL,2012.02.01, 2012.03.01, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_datetime",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_timestamp(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_timestamp"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`timestamp1`timestamp2`timestamp3`timestamp4`timestamp5,[INT,TIMESTAMP,TIMESTAMP,TIMESTAMP,TIMESTAMP,TIMESTAMP])
            db=database(dbPath,RANGE,[1,101,201,301,401,501,601])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_timestamp", "pt", "id", pool)
        n = 500
        id = np.arange(100, 600)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        data = pd.DataFrame(
            {'id': id, 'timestamp1': date, 'timestamp2': month, 'timestamp3': time, 'timestamp4': second,
             'timestamp5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500
            ids = 100..599
            time_name = `timestamp
            dates =funcByName(time_name)(take([2012.01.01, NULL,1965.07.25, NULL, 1970.01.01],n))
            months =  funcByName(time_name)(take([1965.08.01, NULL,2012.02.01, 2012.03.01, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_timestamp",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_nanotime(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_nanotime"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`nanotime1`nanotime2`nanotime3`nanotime4`nanotime5,[INT,NANOTIME,NANOTIME,NANOTIME,NANOTIME,NANOTIME])
            db=database(dbPath,RANGE,[1,101,201,301,401,501,601])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_nanotime", "pt", "id", pool)
        n = 500
        id = np.arange(100, 600)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        data = pd.DataFrame({'id': id, 'nanotime1': date, 'nanotime2': month, 'nanotime3': time, 'nanotime4': second,
                             'nanotime5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500
            ids = 100..599
            time_name = `nanotime
            dates =funcByName(time_name)(take([2012.01.01T00:00:00, NULL,1965.07.25T00:00:00, NULL, 1970.01.01T00:00:00],n))
            months =  funcByName(time_name)(take([1965.08.01T00:00:00, NULL,2012.02.01T00:00:00, 2012.03.01T00:00:00, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_nanotime",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_nanotimestamp(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_nanotimestamp"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`nanotimestamp1`nanotimestamp2`nanotimestamp3`nanotimestamp4`nanotimestamp5,[INT,NANOTIMESTAMP,NANOTIMESTAMP,NANOTIMESTAMP,NANOTIMESTAMP,NANOTIMESTAMP])
            db=database(dbPath,RANGE,[1,101,201,301,401,501,601])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_nanotimestamp", "pt", "id",
                                                pool)
        n = 500
        id = np.arange(100, 600)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100), dtype="datetime64")
        data = pd.DataFrame({'id': id, 'nanotimestamp1': date, 'nanotimestamp2': month, 'nanotimestamp3': time,
                             'nanotimestamp4': second, 'nanotimestamp5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500
            ids = 100..599
            time_name = `nanotimestamp
            dates =funcByName(time_name)(take([2012.01.01T00:00:00, NULL,1965.07.25T00:00:00, NULL, 1970.01.01T00:00:00],n))
            months =  funcByName(time_name)(take([1965.08.01T00:00:00, NULL,2012.02.01T00:00:00, 2012.03.01T00:00:00, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_nanotimestamp",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_datehour(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender_to_datehour"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`datehour1`datehour2`datehour3`datehour4`datehour5,[INT,DATEHOUR,DATEHOUR,DATEHOUR,DATEHOUR,DATEHOUR])
            db=database(dbPath,RANGE,[1,100001,200001,300001,400001,500001,600001])
            pt = db.createPartitionedTable(t, `pt, `id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender_to_datehour", "pt", "id", pool)
        n = 500000
        id = np.arange(100000, 600000)
        date = np.array(np.tile(['2012-01-01', 'NaT', '1965-07-25', 'NaT', '1970-01-01'], 100000),
                        dtype="datetime64[D]")
        month = np.array(np.tile(['1965-08', 'NaT', '2012-02', '2012-03', 'NaT'], 100000), dtype="datetime64")
        time = np.array(
            np.tile(['2012-01-01T00:00:00.000', '2015-08-26T05:12:48.426', 'NaT', 'NaT', '1965-06-09T23:59:59.999'],
                    100000), dtype="datetime64")
        second = np.array(
            np.tile(['2012-01-01T00:00:00', '2015-08-26T05:12:48', 'NaT', 'NaT', '1965-06-09T23:59:59'], 100000),
            dtype="datetime64")
        nanotime = np.array(np.tile(['2012-01-01T00:00:00.000000000', '2015-08-26T05:12:48.008007006', 'NaT', 'NaT',
                                     '1965-06-09T23:59:59.999008007'], 100000), dtype="datetime64")
        data = pd.DataFrame({'id': id, 'datehour1': date, 'datehour2': month, 'datehour3': time, 'datehour4': second,
                             'datehour5': nanotime})
        num = appender.append(data)
        assert num == n
        script = '''
            n = 500000
            ids = 100000..599999
            time_name = `datehour
            dates =funcByName(time_name)(take([2012.01.01T00:00:00, NULL,1965.07.25T00:00:00, NULL, 1970.01.01T00:00:00],n))
            months =  funcByName(time_name)(take([1965.08.01T00:00:00, NULL,2012.02.01T00:00:00, 2012.03.01T00:00:00, NULL],n))
            times = funcByName(time_name)(take([2012.01.01T00:00:00.000, 2015.08.26T05:12:48.426, NULL, NULL, 1965.06.09T23:59:59.999],n))
            seconds = funcByName(time_name)(take([2012.01.01T00:00:00, 2015.08.26T05:12:48, NULL, NULL, 1965.06.09T23:59:59],n))
            nanotimes = funcByName(time_name)(take([2012.01.01T00:00:00.000000000, 2015.08.26T05:12:48.008007006,NULL, NULL, 1965.06.09T23:59:59.999008007],n))
            ex = table(ids as id,dates as month1,months as month2,times as month3,seconds as month4,nanotimes as month5)
            re = select * from loadTable("dfs://test_PartitionedTableAppender_to_datehour",`pt)
            each(eqObj, re.values(), ex.values())
        '''
        re = self.conn.run(script)
        assert_array_equal(re, [True, True, True, True, True, True])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_to_date_partition_col_date(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://test_PartitionedTableAppender"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`date,[INT,DATE])
            db=database(dbPath,VALUE,2010.01.01+0..100)
            pt = db.createPartitionedTable(t, `pt, `date)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://test_PartitionedTableAppender", "pt", "date", pool)
        id = np.arange(100, 600)
        time1 = np.array(
            np.tile(['2010-01-01T00:00:00.000', '2010-02-01T05:12:48.426', 'NaT', 'NaT', '2010-03-03T23:59:59.999'],
                    100), dtype="datetime64")
        data = pd.DataFrame({'id': id, 'date': time1})
        with pytest.raises(RuntimeError):
            appender.append(data)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_insert_one_row(self, _compress):
        pool = self.pool_list[_compress]
        script = '''
            dbPath = "dfs://Rangedb"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:100,`id`val1`val2,[INT,DOUBLE,DATE])
            db=database(dbPath,RANGE,  1  100  200  300)
            pt = db.createPartitionedTable(t, `pt, `id)
        '''
        self.conn.run(script)
        appender = ddb.PartitionedTableAppender("dfs://Rangedb", "pt", "id", pool)
        v = np.array('2012-01-01T00:00:00.000', dtype="datetime64")
        data = pd.DataFrame({"id": np.random.randint(1, 300, 1), "val1": np.random.rand(1), "val2": v})
        appender.append(data)

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_insert_all_datatype_arrayVector(self, _compress):
        pool = self.pool_list[_compress]
        scripts = """
            if(existsDatabase("dfs://test"))
                dropDatabase("dfs://test")
            db=database("dfs://test",VALUE,2021.01.01..2022.01.01,engine="TSDB")
            tb=table(1:0,`date`sym`ID`bool_av`char_av`short_av`int_av`long_av`float_av`double_av`date_av`month_av`datetime_av`timestamp_av`nanotimestamp_av,
                [DATE,SYMBOL,INT, BOOL[], CHAR[], SHORT[], INT[], LONG[], FLOAT[], DOUBLE[], DATE[], MONTH[], DATETIME[], TIMESTAMP[], NANOTIMESTAMP[]])
            test=db.createPartitionedTable(tb,`test,`date,sortColumns=`sym`date)
        """
        self.conn.run(scripts)
        date = np.array(['2021-06-12', '2021-06-13', '2021-06-13', '2021-06-14', '2021-06-14'], dtype="datetime64[ns]")
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        ID = np.array([1, 2, 3, 4, 5], dtype="int32")
        bool_av = [[True, False, True, True, False], [True, False, True, True, False], [True, False, True, True, False],
                   [True, False, True, True, False], [True, False, True, True, False]]
        char_av = [np.array([1, 2, 3, 4, 5], dtype=np.int8), np.array([1, 2, 3, 4, 5], dtype=np.int8),
                   np.array([1, 2, 3, 4, 5], dtype=np.int8), np.array([1, 2, 3, 4, 5], dtype=np.int8),
                   np.array([1, 2, 3, 4, 5], dtype=np.int8)]
        short_av = [np.array([1, 2, 3, 4, 5], dtype=np.int16), np.array([1, 2, 3, 4, 5], dtype=np.int16),
                    np.array([1, 2, 3, 4, 5], dtype=np.int16), np.array([1, 2, 3, 4, 5], dtype=np.int16),
                    np.array([1, 2, 3, 4, 5], dtype=np.int16)]
        int_av = [np.array([1, 2, 3, 4, 5], dtype=np.int32), np.array([1, 2, 3, 4, 5], dtype=np.int32),
                  np.array([1, 2, 3, 4, 5], dtype=np.int32), np.array([1, 2, 3, 4, 5], dtype=np.int32),
                  np.array([1, 2, 3, 4, 5], dtype=np.int32)]
        long_av = [np.array([1, 2, 3, 4, 5], dtype=np.int64), np.array([1, 2, 3, 4, 5], dtype=np.int64),
                   np.array([1, 2, 3, 4, 5], dtype=np.int64), np.array([1, 2, 3, 4, 5], dtype=np.int64),
                   np.array([1, 2, 3, 4, 5], dtype=np.int64)]
        float_av = [np.array([1, 2, 3, 4, 5], dtype=np.float32), np.array([1, 2, 3, 4, 5], dtype=np.float32),
                    np.array([1, 2, 3, 4, 5], dtype=np.float32), np.array([1, 2, 3, 4, 5], dtype=np.float32),
                    np.array([1, 2, 3, 4, 5], dtype=np.float32)]
        double_av = [np.array([1, 2, 3, 4, 5], dtype=np.float64), np.array([1, 2, 3, 4, 5], dtype=np.float64),
                     np.array([1, 2, 3, 4, 5], dtype=np.float64), np.array([1, 2, 3, 4, 5], dtype=np.float64),
                     np.array([1, 2, 3, 4, 5], dtype=np.float64)]
        date_av = [np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13", "2013-06-13"],
                            dtype=np.datetime64("2013-06-13")),
                   np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13",
                             "2013-06-13"], dtype='datetime64[ns]'),
                   np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13",
                             "2013-06-13"], dtype='datetime64[ns]'),
                   np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13",
                             "2013-06-13"], dtype='datetime64[ns]'),
                   np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13", "2013-06-13"],
                            dtype=np.datetime64("2013-06-13"))]
        date_av_expect = [np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13", "2013-06-13"],
                                   dtype=np.datetime64("2013-06-13")),
                          np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13",
                                    "2013-06-13"], dtype='datetime64[D]'),
                          np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13",
                                    "2013-06-13"], dtype='datetime64[D]'),
                          np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13",
                                    "2013-06-13"], dtype='datetime64[D]'),
                          np.array(["2013-06-13", "2014-06-13", "2015-06-13", "2017-06-13", "2013-06-13"],
                                   dtype=np.datetime64("2013-06-13"))]
        month_av = [np.array(["2013-06", "2014-06", "2015-06", "2017-06", "2013-06"], dtype=np.datetime64("2013-06")),
                    np.array(["2013-06", "2014-06", "2015-06", "2017-06",
                              "2013-06"], dtype='datetime64[ns]'),
                    np.array(["2013-06", "2014-06", "2015-06", "2017-06",
                              "2013-06"], dtype='datetime64[ns]'),
                    np.array(["2013-06", "2014-06", "2015-06", "2017-06",
                              "2013-06"], dtype='datetime64[ns]'),
                    np.array(["2013-06", "2014-06", "2015-06", "2017-06", "2013-06"], dtype='datetime64[ns]')]
        month_av_expect = [
            np.array(["2013-06", "2014-06", "2015-06", "2017-06", "2013-06"], dtype=np.datetime64("2013-06")),
            np.array(["2013-06", "2014-06", "2015-06", "2017-06",
                      "2013-06"], dtype='datetime64[M]'),
            np.array(["2013-06", "2014-06", "2015-06", "2017-06",
                      "2013-06"], dtype='datetime64[M]'),
            np.array(["2013-06", "2014-06", "2015-06", "2017-06",
                      "2013-06"], dtype='datetime64[M]'),
            np.array(["2013-06", "2014-06", "2015-06", "2017-06", "2013-06"], dtype='datetime64[M]')]
        datetime_av = [np.array(
            ["2012-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10", "2017-06-13T13:30:10",
             "2013-06-13T13:30:10"], dtype=np.datetime64("2013-06-13T13:30:10")),
            np.array(["2013-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10",
                      "2017-06-13T13:30:10", "2013-06-13T13:30:10"], dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10",
                      "2017-06-13T13:30:10", "2013-06-13T13:30:10"], dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10",
                      "2017-06-13T13:30:10", "2013-06-13T13:30:10"], dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10", "2017-06-13T13:30:10",
                      "2013-06-13T13:30:10"], dtype='datetime64[ns]')]
        datetime_av_expect = [np.array(
            ["2012-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10", "2017-06-13T13:30:10",
             "2013-06-13T13:30:10"], dtype=np.datetime64("2013-06-13T13:30:10")),
            np.array(["2013-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10",
                      "2017-06-13T13:30:10", "2013-06-13T13:30:10"], dtype='datetime64[s]'),
            np.array(["2013-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10",
                      "2017-06-13T13:30:10", "2013-06-13T13:30:10"], dtype='datetime64[s]'),
            np.array(["2013-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10",
                      "2017-06-13T13:30:10", "2013-06-13T13:30:10"], dtype='datetime64[s]'), np.array(
                ["2013-06-13T13:30:10", "2014-06-13T13:30:10", "2015-06-13T13:30:10", "2017-06-13T13:30:10",
                 "2013-06-13T13:30:10"], dtype='datetime64[s]')]
        timestamp_av = [np.array(
            ["2012-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008", "2017-06-13T13:30:10.008",
             "2013-06-13T13:30:10.008"], dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008",
                      "2017-06-13T13:30:10.008", "2013-06-13T13:30:10.008"], dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008",
                      "2017-06-13T13:30:10.008", "2013-06-13T13:30:10.008"], dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008",
                      "2017-06-13T13:30:10.008", "2013-06-13T13:30:10.008"], dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008",
                      "2017-06-13T13:30:10.008", "2013-06-13T13:30:10.008"], dtype='datetime64[ns]')]
        timestamp_av_expect = [np.array(
            ["2012-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008", "2017-06-13T13:30:10.008",
             "2013-06-13T13:30:10.008"], dtype='datetime64[ms]'),
            np.array(
                ["2013-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008",
                 "2017-06-13T13:30:10.008", "2013-06-13T13:30:10.008"], dtype='datetime64[ms]'),
            np.array(
                ["2013-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008",
                 "2017-06-13T13:30:10.008", "2013-06-13T13:30:10.008"], dtype='datetime64[ms]'),
            np.array(
                ["2013-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008",
                 "2017-06-13T13:30:10.008", "2013-06-13T13:30:10.008"], dtype='datetime64[ms]'),
            np.array(
                ["2013-06-13T13:30:10.008", "2014-06-13T13:30:10.008", "2015-06-13T13:30:10.008",
                 "2017-06-13T13:30:10.008", "2013-06-13T13:30:10.008"], dtype='datetime64[ms]')]
        nanotimestamp_av = [np.array(
            ["2012-06-13T13:30:10.008007006", "2014-06-13T13:30:10.008007006", "2015-06-13T13:30:10.008007006",
             "2017-06-13T13:30:10.008007006", "2013-06-13T13:30:10.008007006"], dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10.008007006", "2014-06-13T13:30:10.008007006",
                      "2015-06-13T13:30:10.008007006",
                      "2017-06-13T13:30:10.008007006", "2013-06-13T13:30:10.008007006"],
                     dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10.008007006", "2014-06-13T13:30:10.008007006",
                      "2015-06-13T13:30:10.008007006",
                      "2017-06-13T13:30:10.008007006", "2013-06-13T13:30:10.008007006"],
                     dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10.008007006", "2014-06-13T13:30:10.008007006",
                      "2015-06-13T13:30:10.008007006",
                      "2017-06-13T13:30:10.008007006", "2013-06-13T13:30:10.008007006"],
                     dtype='datetime64[ns]'),
            np.array(["2013-06-13T13:30:10.008007006", "2014-06-13T13:30:10.008007006",
                      "2015-06-13T13:30:10.008007006", "2017-06-13T13:30:10.008007006",
                      "2013-06-13T13:30:10.008007006"], dtype='datetime64[ns]')]
        df = pd.DataFrame(
            {'date': date, 'sym': sym, 'ID': ID, "bool_av": bool_av, "char_av": char_av, "short_av": short_av,
             "int_av": int_av,
             "long_av": long_av, "float_av": float_av, "double_av": double_av, "date_av": date_av, "month_av": month_av,
             "datetime_av": datetime_av,
             "timestamp_av": timestamp_av, "nanotimestamp_av": nanotimestamp_av})
        appender = ddb.PartitionedTableAppender("dfs://test", "test", "date", pool)
        appender.append(df)
        time.sleep(2)
        df_expect = pd.DataFrame(
            {'date': date, 'sym': sym, 'ID': ID, "bool_av": bool_av, "char_av": char_av, "short_av": short_av,
             "int_av": int_av,
             "long_av": long_av, "float_av": float_av, "double_av": double_av, "date_av": date_av_expect,
             "month_av": month_av_expect, "datetime_av": datetime_av_expect,
             "timestamp_av": timestamp_av_expect, "nanotimestamp_av": nanotimestamp_av})
        assert_frame_equal(df_expect, self.conn.run("select * from test"))

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_insert_all_datatype_array_vector_contain_None(self, _compress):
        pool = self.pool_list[_compress]
        scripts = """
            if(existsDatabase("dfs://test"))
                dropDatabase("dfs://test")
            db=database("dfs://test",VALUE,2021.01.01..2022.01.01,engine="TSDB")
            tb=table(1:0,`date`sym`ID`bool_av`char_av`short_av`int_av`long_av`float_av`double_av,
                [DATE,SYMBOL,INT, BOOL[], CHAR[], SHORT[], INT[], LONG[], FLOAT[], DOUBLE[]])
            test=db.createPartitionedTable(tb,`test,`date,sortColumns=`sym`date)
        """
        self.conn.run(scripts)
        date = np.array(['2021-06-12', '2021-06-13', '2021-06-13', '2021-06-14', '2021-06-14'], dtype="datetime64[ns]")
        sym = ['A1', 'A2', 'A3', 'A4', 'A5']
        ID = np.array([1, 2, 3, 4, 5], dtype="int32")
        bool_av = [[np.nan, False, None, True, pd.NaT], [np.nan, False, None, True, pd.NaT],
                   [np.nan, False, None, True, pd.NaT], [np.nan, False, None, True, pd.NaT],
                   [np.nan, False, None, True, pd.NaT]]
        char_av = [[np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT],
                   [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT]]
        short_av = [[np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT],
                    [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT]]
        int_av = [[np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT],
                  [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT]]
        long_av = [[np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT],
                   [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT]]
        float_av = [[np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT],
                    [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT]]
        double_av = [[np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT],
                     [np.nan, 2, None, 4, pd.NaT], [np.nan, 2, None, 4, pd.NaT]]
        df = pd.DataFrame(
            {'date': date, 'sym': sym, 'ID': ID, "bool_av": bool_av, "char_av": char_av, "short_av": short_av,
             "int_av": int_av,
             "long_av": long_av, "float_av": float_av, "double_av": double_av})
        appender = ddb.PartitionedTableAppender("dfs://test", "test", "date", pool)
        appender.append(df)
        time.sleep(1)
        assert_frame_equal(df, self.conn.run("select * from test"))

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_alltype(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "int", pool)
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
        num = appender.append(df)
        assert num == 2
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
            re = select * from loadTable("dfs://PartitionedTableAppender_test",`pt)
            each(eqObj,t.values(),re.values())
        """
        re = self.conn.run(script)
        assert_array_equal(re, [True for _ in range(22)])

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_PartitionedTableAppender_dfs_table_alltype_arrayvector(self, _compress):
        pool = self.pool_list[_compress]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
            db=database(dbPath,VALUE,1 10000,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "id", pool)
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
        num = appender.append(df)
        assert num == 2
        assert_frame_equal(df, self.conn.run("select * from pt"))

    def test_PartitionedTableAppender_dfs_table_column_dateType_not_match_1(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "int", pool)
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
            pass

    def test_PartitionedTableAppender_dfs_table_column_dateType_not_match_2(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "int", pool)
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
            pass

    def test_PartitionedTableAppender_dfs_table_column_dateType_not_match_3(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "int", pool)
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
            pass

    def test_PartitionedTableAppender_dfs_table_column_dateType_not_match_4(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "int", pool)
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
            pass

    def test_PartitionedTableAppender_dfs_table_column_dateType_not_match_5(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:5, `bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`symbol`string`ipaddr`uuid`int128`blob, [BOOL,CHAR,SHORT,INT,LONG,DATE,TIME,MINUTE,SECOND,DATETIME,DATEHOUR,TIMESTAMP,NANOTIME,NANOTIMESTAMP,FLOAT,DOUBLE,SYMBOL,STRING,IPADDR,UUID,INT128,BLOB])
            db=database(dbPath,RANGE,0 100000 200000 300000 400000 600001,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,,`int)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "int", pool)
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
            pass

    def test_PartitionedTableAppender_dfs_table_array_vector_not_match_1(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
            db=database(dbPath,VALUE,1 10000,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "id", pool)
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
            pass

    def test_PartitionedTableAppender_dfs_table_array_vector_not_match_2(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
            db=database(dbPath,VALUE,1 10000,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "id", pool)
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
        assert appender.append(df) == 2
        res = self.conn.run('''select * from loadTable("dfs://PartitionedTableAppender_test",`pt)''')
        assert_frame_equal(res, df)

    def test_PartitionedTableAppender_dfs_table_array_vector_not_match_3(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
            db=database(dbPath,VALUE,1 10000,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "id", pool)
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
            assert "The value [ True False] (column \"ipaddr\", row 0) must be of IPADDR[] type" in str(e)

    def test_PartitionedTableAppender_dfs_table_array_vector_not_match_4(self):
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
        self.conn.run('''
            dbPath = "dfs://PartitionedTableAppender_test"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(100:0, `id`bool`char`short`int`long`date`time`minute`second`datetime`datehour`timestamp`nanotime`nanotimestamp`float`double`ipaddr`uuid`int128, [INT,BOOL[],CHAR[],SHORT[],INT[],LONG[],DATE[],TIME[],MINUTE[],SECOND[],DATETIME[],DATEHOUR[],TIMESTAMP[],NANOTIME[],NANOTIMESTAMP[],FLOAT[],DOUBLE[],IPADDR[],UUID[],INT128[]])
            db=database(dbPath,VALUE,1 10000,,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `id,,`id)
        ''')
        appender = ddb.PartitionedTableAppender("dfs://PartitionedTableAppender_test", "pt", "id", pool)
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

    def test_PartitionedTableAppender_no_pandasWarning(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        pool = ddb.DBConnectionPool(HOST, PORT, 4, 'admin', '123456')
        conn.run("""
            tglobal=table(`a`b`c as names,[date(1), date(2), date(3)] as dates,rand(100.0,3) as prices);
            login(`admin,`123456);
            dbPath = "dfs://demodb2";
            if(existsDatabase(dbPath)){dropDatabase(dbPath)};
            db=database(dbPath,VALUE,1970.01.02..1970.01.04);
            db.createPartitionedTable(tglobal,`pt,`dates);
        """)
        df = conn.run("tglobal")
        import warnings
        warnings.filterwarnings('error')
        try:
            pta = ddb.PartitionedTableAppender("dfs://demodb2", "pt", "dates", pool)
            pta.append(df)
            del pta
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
    def test_PartitionedTableAppender_allNone_tables_with_numpyArray(self, val, valstr, valdecimal):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
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
        pta = ddb.PartitionedTableAppender("dfs://test_dfs1", 'pt', 'ckeycol', pool)
        assert pta.append(df) == 3
        assert conn.run(r'rows = exec count(*) from loadTable("dfs://test_dfs1", "pt");rows') == 3
        assert conn.run(r"""
            ex_tab = select * from loadTable("dfs://test_dfs1", "pt");
            res = bool([]);
            for(i in 1:tab.columns()){res.append!(ex_tab.column(i).isNull())};
            all(res)
        """)
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['INT', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128', 'BLOB', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(schema, ex_types)
        conn.dropDatabase("dfs://test_dfs1")
        conn.close()

    @pytest.mark.parametrize('val, valstr, valdecimal', test_dataArray, ids=[str(x[0]) for x in test_dataArray])
    def test_PartitionedTableAppender_allNone_tables_with_pythonList(self, val, valstr, valdecimal):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        pool = self.__class__.pool_list["COMPRESS_CLOSE"]
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
        pta = ddb.PartitionedTableAppender("dfs://test_dfs1", 'pt', 'ckeycol', pool)
        assert pta.append(df) == 3
        assert conn.run(r'rows = exec count(*) from loadTable("dfs://test_dfs1", "pt");rows') == 3
        assert conn.run(r"""
            ex_tab = select * from loadTable("dfs://test_dfs1", "pt");
            res = bool([]);
            for(i in 1:tab.columns()){res.append!(ex_tab.column(i).isNull())};
            all(res)
        """)
        schema = conn.run("schema(tab).colDefs[`typeString]")
        ex_types = ['INT', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128', 'BLOB', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(schema, ex_types)
        conn.dropDatabase("dfs://test_dfs1")
        conn.close()

    def test_PartitionedTableAppender_append_after_pool_deconstructed(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        pool = ddb.DBConnectionPool(HOST, PORT, 1, 'admin', '123456')
        conn.run("""
            dbPath = "dfs://test_dfs1"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,1])
            tab = table(1 2 as c1, `a`b as c2)
            pt = db.createPartitionedTable(tab, `pt, `c1).append!(tab)
        """)
        df = pd.DataFrame({'c1': [3, 4], 'c2': ['c', 'd']})
        try:
            pta = ddb.PartitionedTableAppender(dbPath="dfs://test_dfs1", tableName="pt", partitionColName='c1',
                                               dbConnectionPool=pool)
            pool.shutDown()
            pta.append(df)
        except RuntimeError as e:
            assert "DBConnectionPool has been shut down" in str(e)
        pool = ddb.DBConnectionPool(HOST, PORT, 2, 'admin', '123456')
        pta = ddb.PartitionedTableAppender(dbPath="dfs://test_dfs1", tableName="pt", partitionColName='c1',
                                           dbConnectionPool=pool)
        del pool
        assert pta.append(df) == 2
        assert conn.run("""
            res = select * from loadTable('dfs://test_dfs1', 'pt') order by c1;
            ex = table(1 2 3 4 as c1, string(['a', 'b', 'c', 'd']) as c2);
            each(eqObj, res.values(), ex.values())
        """).all()
        conn.dropDatabase('dfs://test_dfs1')
        conn.close()

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_PartitionedTableAppender_append_dataframe_with_numpy_order(self, _compress, _order, _python_list):
        pool = self.__class__.pool_list[_compress]
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
        self.conn.run("""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
            colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(2), DECIMAL64(11), DECIMAL128(36)];
            t=table(1:0, colName,colType)
            dbPath = "dfs://test_dfs1"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        append = ddb.PartitionedTableAppender(dbPath="dfs://test_dfs1", tableName='pt', partitionColName='index',
                                              dbConnectionPool=pool)
        append.append(df)
        self.conn.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, false, i,i,i,i,i,i+23640,i,i,i,i,i,i,i,i,i,i, 'sym','str', 'blob', ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"), decimal32(-2.11, 2), decimal64(0, 11), decimal128(-1.1, 36))
            }
        """)
        res = self.conn.run("""
            ex = select * from objByName(`t);
            res = select * from loadTable("dfs://test_dfs1", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        tys = self.conn.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(2)', 'DECIMAL64(11)',
                    'DECIMAL128(36)']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase("dfs://test_dfs1")

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_PartitionedTableAppender_append_dataframe_array_vector_with_numpy_order(self, _compress, _order,
                                                                                     _python_list):
        pool = self.pool_list[_compress]
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
        self.conn.run("""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
            colType = [LONG, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[], DECIMAL32(2)[], DECIMAL64(11)[], DECIMAL128(36)[]];
            t=table(1:0, colName,colType)
            share t as pyt;
            dbPath = "dfs://test_dfs1"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        append = ddb.PartitionedTableAppender(dbPath="dfs://test_dfs1", tableName='pt', partitionColName='index',
                                              dbConnectionPool=pool)
        assert append.append(df) == 11
        self.conn.run("""
            for(i in 0:10){
                tableInsert(t, i, 0, i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i, ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"), decimal32('-2.11', 2), decimal64('0.0', 11), decimal128('-1.1', 36))
            }
            tableInsert(objByName(`t), 10, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
        """)
        res = self.conn.run("""
            ex = select * from objByName(`t);
            res = select * from loadTable("dfs://test_dfs1", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        tys = self.conn.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]', 'DECIMAL32(2)[]', 'DECIMAL64(11)[]',
                    'DECIMAL128(36)[]']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase("dfs://test_dfs1")

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_PartitionedTableAppender_append_null_dataframe_with_numpy_order(self, _compress, _order, _python_list):
        pool = self.pool_list[_compress]
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
        self.conn.run("""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
            colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(0), DECIMAL64(0), DECIMAL128(0)];
            t=table(1:0, colName,colType)
            dbPath = "dfs://test_dfs1"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        append = ddb.PartitionedTableAppender(dbPath="dfs://test_dfs1", tableName='pt', partitionColName='index',
                                              dbConnectionPool=pool)
        append.append(df)
        self.conn.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = self.conn.run("""ex = select * from objByName(`t);
                           res = select * from loadTable("dfs://test_dfs1", `pt);
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        tys = self.conn.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(0)', 'DECIMAL64(0)',
                    'DECIMAL128(0)']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase("dfs://test_dfs1")

    @pytest.mark.parametrize('_compress', ["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_PartitionedTableAppender_append_null_dataframe_array_vector_with_numpy_order(self, _compress, _order,
                                                                                          _python_list):
        pool = self.pool_list[_compress]
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
        self.conn.run("""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128`cdecimal32`cdecimal64`cdecimal128;
            colType = [LONG, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[], DECIMAL32(0)[], DECIMAL64(0)[], DECIMAL128(0)[]];
            t=table(1:0, colName,colType)
            dbPath = "dfs://test_dfs1"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        append = ddb.PartitionedTableAppender(dbPath="dfs://test_dfs1", tableName='pt', partitionColName='index',
                                              dbConnectionPool=pool)
        append.append(df)
        self.conn.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = self.conn.run("""
            ex = select * from objByName(`t);
            res = select * from loadTable("dfs://test_dfs1", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        tys = self.conn.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]', 'DECIMAL32(0)[]', 'DECIMAL64(0)[]', 'DECIMAL128(0)[]']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase("dfs://test_dfs1")

    def test_PartitionedTableAppender_over_length(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(f"""
            dbPath = "dfs://valuedb"
            if(existsDatabase(dbPath)){{dropDatabase(dbPath)}}
            tab=table(100:0,[`id,`test],[SYMBOL,STRING])
            db = database(dbPath, VALUE, `APPL`IBM`AMZN)
            pt = db.createPartitionedTable(tab, `pt, `test)
         """)
        pool = self.pool_list["COMPRESS_CLOSE"]
        appender = ddb.PartitionedTableAppender(dbPath="dfs://valuedb", tableName="pt", partitionColName="test",
                                                dbConnectionPool=pool)
        df = pd.DataFrame({
            'id': ['0' * 256 * 1024],
            'test': ['APPL']
        })
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            appender.append(df)
        conn.close()

    def test_PartitionedTableAppender_max_length_symbol_dfs_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(f"""
            dbPath = "dfs://valuedb"
            if(existsDatabase(dbPath)){{dropDatabase(dbPath)}}
            tab=table(100:0,[`id,`test],[SYMBOL,STRING])
            db = database(dbPath, VALUE, `APPL`IBM`AMZN)
            pt = db.createPartitionedTable(tab, `pt, `test)
         """)
        pool = self.pool_list["COMPRESS_CLOSE"]
        appender = ddb.PartitionedTableAppender(dbPath="dfs://valuedb", tableName="pt", partitionColName="test",
                                                dbConnectionPool=pool)
        df = pd.DataFrame({
            'id': ['0' * 255],
            'test': ['APPL']
        })
        appender.append(df)
        assert conn.run(f'select strlen(id) from pt')['strlen_id'][0] == 255
        conn.close()

    def test_PartitionedTableAppender_max_length_string_dfs_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(f"""
            dbPath = "dfs://valuedb"
            if(existsDatabase(dbPath)){{dropDatabase(dbPath)}}
            tab=table(100:0,[`id,`test],[STRING,STRING])
            db = database(dbPath, VALUE, `APPL`IBM`AMZN)
            pt = db.createPartitionedTable(tab, `pt, `test)
         """)
        pool = self.pool_list["COMPRESS_CLOSE"]
        appender = ddb.PartitionedTableAppender(dbPath="dfs://valuedb", tableName="pt", partitionColName="test",
                                                dbConnectionPool=pool)
        df = pd.DataFrame({
            'id': ['0' * (256 * 1024 - 1)],
            'test': ['APPL']
        })
        appender.append(df)
        assert conn.run(f'select strlen(id) from pt')['strlen_id'][0] == 65535
        conn.close()

    def test_PartitionedTableAppender_max_length_blob_dfs_table(self):
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        conn.run(f"""
            dbPath = "dfs://valuedb"
            if(existsDatabase(dbPath)){{dropDatabase(dbPath)}}
            tab=table(100:0,[`id,`test],[BLOB,STRING])
            db = database(dbPath, VALUE, `APPL`IBM`AMZN,engine=`TSDB)
            pt = db.createPartitionedTable(tab, `pt, `test,sortColumns=`test)
         """)
        pool = self.pool_list["COMPRESS_CLOSE"]
        appender = ddb.PartitionedTableAppender(dbPath="dfs://valuedb", tableName="pt", partitionColName="test",
                                                dbConnectionPool=pool)
        df = pd.DataFrame({
            'id': ['0' * 256 * 1024],
            'test': ['APPL']
        })
        appender.append(df)
        assert conn.run(f'select strlen(id) from pt')['strlen_id'][0] == 256 * 1024
        conn.close()
