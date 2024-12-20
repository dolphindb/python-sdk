import asyncio
import platform
import random
import subprocess
import time

import pytest
from numpy.testing import *
from pandas.testing import *

from setup.prepare import *
from setup.settings import *
from setup.utils import get_pid

PYTHON_VERSION = tuple(int(i) for i in platform.python_version().split('.'))


def create_value_db():
    conn = ddb.session()
    conn.connect(HOST, PORT, USER, PASSWD)
    script = """
                dbName="dfs://test_dbConnection"
                tableName="pt"
                if(existsDatabase(dbName)){
                    dropDatabase(dbName)
                }
                db=database(dbName, VALUE, 1..10)
                n=1000000
                t=table(loop(take{, n/10}, 1..10).flatten() as id, 1..1000000 as val)
                pt=db.createPartitionedTable(t, `pt, `id).append!(t)
            """
    conn.run(script)
    conn.close()


async def get_row_count(pool: ddb.DBConnectionPool):
    return await pool.run("exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")


class TestDBConnectionPool:
    conn = ddb.session(enablePickle=False)

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

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('host', [HOST, "255.255.255.255", ""], ids=["T_HOST", "F_HOST", "N_HOST"])
    @pytest.mark.parametrize('port', [PORT, 0, None], ids=["T_PORT", "F_PORT", "N_PORT"])
    def test_DBConnectionPool_host_port(self, host, port):
        loop = asyncio.get_event_loop_policy().new_event_loop()
        if host == HOST and port == PORT:
            pool1 = ddb.DBConnectionPool(
                host, port, 2, USER, PASSWD)
            taska = [loop.create_task(pool1.run("1+1"))]
            loop.run_until_complete(asyncio.wait(taska))
            assert taska[0].result() == 2
            pool1.shutDown()

        elif port == None:
            with pytest.raises(TypeError) as e:
                pool1 = ddb.DBConnectionPool(
                    host, port, 2, USER, PASSWD)
        else:
            with pytest.raises(RuntimeError) as e:
                pool1 = ddb.DBConnectionPool(
                    host, port, 2, USER, PASSWD)
                taska = [loop.create_task(pool1.run("1+1"))]
                loop.run_until_complete(asyncio.wait(taska))
        loop.close()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('user', [USER, "adminxxxx", ""], ids=["T_USER", "F_USER", "N_USER"])
    @pytest.mark.parametrize('passwd', [PASSWD, "123456777888", ""], ids=["T_PASSWD", "F_PASSWD", "N_PASSWD"])
    def test_DBConnectionPool_user_passwd(self, user, passwd):
        loop = asyncio.get_event_loop_policy().new_event_loop()
        if user == USER and passwd == PASSWD:
            pool1 = ddb.DBConnectionPool(
                HOST, PORT, 2, user, passwd)
            taska = [loop.create_task(pool1.run("1+1"))]
            loop.run_until_complete(asyncio.wait(taska))
            assert taska[0].result() == 2
            pool1.shutDown()

        else:
            try:
                pool1 = ddb.DBConnectionPool(
                    HOST, PORT, 2, user, passwd)
                taska = [loop.create_task(pool1.run("1+1"))]
                loop.run_until_complete(asyncio.wait(taska))
                pool1.shutDown()
                assert False
            except:
                assert True
        loop.close()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_read_dfs_table(self, _compress):
        create_value_db()
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, USER, PASSWD, compress=_compress)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        tasks = [
            loop.create_task(get_row_count(pool)) for i in range(10)]

        loop.run_until_complete(asyncio.wait(tasks))
        for task in tasks:
            assert (task.result() == 1000000)
        pool.shutDown()
        loop.close()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_read_dfs_table_runTaskAsync_Unspecified_time(self, _compress):
        create_value_db()
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        pool.startLoop()
        t1 = time.time()
        task1 = pool.runTaskAsync(
            "sleep(1000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")
        task2 = pool.runTaskAsync(
            "sleep(2000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")
        task3 = pool.runTaskAsync(
            "sleep(4000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")
        task4 = pool.runTaskAsync(
            "sleep(1000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")
        t2 = time.time()
        assert (task1.result() == 1000000)
        t3 = time.time()
        assert (task4.result() == 1000000)
        t4 = time.time()
        assert (task2.result() == 1000000)
        t5 = time.time()
        assert (task3.result() == 1000000)
        t6 = time.time()
        assert (t2 - t1 < 1)
        print(t2 - t1)
        print(t3 - t1)
        print(t4 - t1)
        print(t5 - t1)
        print(t6 - t1)
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_read_dfs_table_runTaskAsyn_set_time(self, _compress):
        create_value_db()
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        pool.startLoop()
        t1 = time.time()
        task1 = pool.runTaskAsync(
            "sleep(1000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")
        task2 = pool.runTaskAsync(
            "sleep(2000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")
        task3 = pool.runTaskAsync(
            "sleep(4000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")
        task4 = pool.runTaskAsync(
            "sleep(1000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")
        t2 = time.time()
        assert (task1.result(2) == 1000000)
        t3 = time.time()
        assert (task4.result(2) == 1000000)
        t4 = time.time()
        assert (task2.result(2) == 1000000)
        t5 = time.time()
        assert (task3.result(4) == 1000000)
        t6 = time.time()
        assert (t2 - t1 < 1)
        print(t2 - t1)
        print(t3 - t1)
        print(t4 - t1)
        print(t5 - t1)
        print(t6 - t1)
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_read_dfs_table_runTaskAsyn_unfinished(self, _compress):
        create_value_db()
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        pool.startLoop()
        t1 = time.time()
        task1 = pool.runTaskAsync(
            "sleep(7000);exec count(*) from loadTable('dfs://test_dbConnection', 'pt')")
        t2 = time.time()
        try:
            assert (task1.result(1) == 1000000)
        except:
            assert (task1.result(8) == 1000000)
        assert (t2 - t1 < 1)
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_read_dfs_table_runTaskAsyn_param_Unspecified_time(self, _compress):
        create_value_db()
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        pool.startLoop()
        t1 = time.time()
        task1 = pool.runTaskAsync("sleep", 1000)
        task2 = pool.runTaskAsync("sleep", 2000)
        task3 = pool.runTaskAsync("sleep", 4000)
        task4 = pool.runTaskAsync("sleep", 1000)
        t2 = time.time()
        task1.result()
        t3 = time.time()
        task4.result()
        t4 = time.time()
        task2.result()
        t5 = time.time()
        task3.result()
        t6 = time.time()
        assert (t2 - t1 < 1)
        assert (t3 - t1 > 1)
        assert (t4 - t1 > 1)
        assert (t5 - t1 > 2)
        assert (t6 - t1 > 4)
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_read_dfs_table_runTaskAsyn_param_set_time(self, _compress):
        create_value_db()
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        pool.startLoop()
        t1 = time.time()
        task1 = pool.runTaskAsync("sleep", 1000)
        task2 = pool.runTaskAsync("sleep", 2000)
        task3 = pool.runTaskAsync("sleep", 4000)
        task4 = pool.runTaskAsync("sleep", 1000)
        t2 = time.time()
        task1.result(2)
        t3 = time.time()
        task4.result(2)
        t4 = time.time()
        task2.result(2)
        t5 = time.time()
        task3.result(4)
        t6 = time.time()
        assert (t2 - t1 < 1)
        assert (t3 - t1 > 1)
        assert (t4 - t1 > 1)
        assert (t5 - t1 > 2)
        assert (t6 - t1 > 4)
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_read_dfs_table_runTaskAsyn_param_unfinished(self, _compress):
        create_value_db()
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        pool.startLoop()
        t1 = time.time()
        task1 = pool.runTaskAsync("sleep", 7000)
        t2 = time.time()
        try:
            task1.result(1)
        except:
            task1.result(8)
            t3 = time.time()
        assert (t2 - t1 < 1)
        assert (t3 - t1 > 7)
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_read_dfs_table_runTaskAsyn_param_basic(self, _compress):
        create_value_db()
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        pool.startLoop()
        task1 = pool.runTaskAsync("add", 1, 2)
        task2 = pool.runTaskAsync("sum", [1, 2, 3])
        task3 = pool.runTaskAsync("time", 8)
        assert (task1.result() == 3)
        assert (task2.result() == 6)
        assert (task3.result() == np.datetime64("1970-01-01T00:00:00.008"))
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_insert_tablewithAlltypes(self, _compress):
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        self.conn.run("""undef(`tab,SHARED);
              t=table(100:0,
              ["cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute","csecond","cdatetime","ctimestamp","cnanotime","cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"],
              [BOOL,CHAR,SHORT,INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, BLOB]);
              share t as tab;
              """)
        df = pd.DataFrame({'cbool': np.array([True, True, None]),
                           'cchar': np.array([1, -1, None], dtype=np.float64),
                           'cshort': np.array([-10, 1000, None], dtype=np.float64),
                           'cint': np.array([-10, 1000, None], dtype=np.float64),
                           # np.array(["2012.01.02T01:02:03.123456789", "2012.01.02"], dtype="datetime64[D]")
                           'cdate': np.array(['2020-01-02', '1970-12-21', 'nat'], dtype='datetime64[ns]'),
                           'cmonth': np.array(['2020-01', '1970-12', 'nat'], dtype='datetime64[ns]'),
                           'ctime': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335', 'nat'],
                                             dtype='datetime64[ns]'),
                           'cminute': np.array(['1970-01-01 13:30', '1970-01-01 05:15', 'nat'], dtype='datetime64[ns]'),
                           'csecond': np.array(['1970-01-01 13:30:10', '1970-01-01 05:15:33', 'nat'],
                                               dtype='datetime64[ns]'),
                           'cdatetime': np.array(['1970-01-01 13:30:10', '2022-10-03 05:15:33', 'nat'],
                                                 dtype='datetime64[ns]'),
                           'ctimestamp': np.array(['2012-06-13 13:30:10.008', '2001-04-22 15:18:29.118', 'nat'],
                                                  dtype='datetime64[ns]'),
                           'cnanotime': np.array(
                               ['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                               dtype='datetime64[ns]'),
                           'cnanotimestamp': np.array(
                               ['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                               dtype='datetime64[ns]'),
                           'cfloat': np.array([2.2134500, -5.36411, np.nan], dtype='float32'),
                           'cdouble': np.array([3.214, -47.795324, np.nan], dtype='float64'),
                           'csymbol': np.array(['sym1', 'sym2', ''], dtype='object'),
                           'cstring': np.array(['str1', 'str2', ''], dtype='object'),
                           'cipaddr': np.array(["192.168.1.1", "192.168.1.254", "0.0.0.0"], dtype='object'),
                           'cblob': np.array(['blob1', 'blob2', ''], dtype='object')
                           })
        df.__DolphinDB_Type__ = {
            'cipaddr': keys.DT_IPPADDR,
            'cblob': keys.DT_BLOB,
        }
        # print(df)
        pool.addTask(r"tableInsert{tab}", 1, df)
        while (not pool.isFinished(1)):
            time.sleep(1)
        res = self.conn.run("select * from tab")
        assert (len(res) == 3)

        assert_frame_equal(df, res)
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_addTask(self, _compress):
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        assert (len(pool.getSessionId()) == 10)
        self.conn.run("""undef(`tab,SHARED);
              t=table(100:0,
              ["cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute","csecond","cdatetime","ctimestamp","cnanotime","cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"],
              [BOOL,CHAR,SHORT,INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, BLOB]);
              share t as tab;
              """)
        df = pd.DataFrame({'cbool': np.array([True, True, None]),
                           'cchar': np.array([1, -1, None], dtype=np.float64),
                           'cshort': np.array([-10, 1000, None], dtype=np.float64),
                           'cint': np.array([-10, 1000, None], dtype=np.float64),
                           # np.array(["2012.01.02T01:02:03.123456789", "2012.01.02"], dtype="datetime64[D]")
                           'cdate': np.array(['2020-01-02', '1970-12-21', 'nat'], dtype='datetime64[ns]'),
                           'cmonth': np.array(['2020-01', '1970-12', 'nat'], dtype='datetime64[ns]'),
                           'ctime': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335', 'nat'],
                                             dtype='datetime64[ns]'),
                           'cminute': np.array(['1970-01-01 13:30', '1970-01-01 05:15', 'nat'], dtype='datetime64[ns]'),
                           'csecond': np.array(['1970-01-01 13:30:10', '1970-01-01 05:15:33', 'nat'],
                                               dtype='datetime64[ns]'),
                           'cdatetime': np.array(['1970-01-01 13:30:10', '2022-10-03 05:15:33', 'nat'],
                                                 dtype='datetime64[ns]'),
                           'ctimestamp': np.array(['2012-06-13 13:30:10.008', '2001-04-22 15:18:29.118', 'nat'],
                                                  dtype='datetime64[ns]'),
                           'cnanotime': np.array(
                               ['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                               dtype='datetime64[ns]'),
                           'cnanotimestamp': np.array(
                               ['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                               dtype='datetime64[ns]'),
                           'cfloat': np.array([2.2134500, -5.36411, np.nan], dtype='float32'),
                           'cdouble': np.array([3.214, -47.795324, np.nan], dtype='float64'),
                           'csymbol': np.array(['sym1', 'sym2', ''], dtype='object'),
                           'cstring': np.array(['str1', 'str2', ''], dtype='object'),
                           'cipaddr': np.array(["192.168.1.1", "192.168.1.254", "0.0.0.0"], dtype='object'),
                           'cblob': np.array(['blob1', 'blob2', ''], dtype='object')
                           })
        df.__DolphinDB_Type__ = {
            'cipaddr': keys.DT_IPPADDR,
            'cblob': keys.DT_BLOB,
        }
        # print(df)
        pool.addTask(r"tableInsert{tab}", 1, df)
        while (not pool.isFinished(1)):
            time.sleep(1)
        res = self.conn.run("select * from tab")
        assert (pool.getData(1) == 3)

        # print(df['cblob'], res['cblob'])
        assert_frame_equal(df, res)
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.timeout(10)
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_addTask_1(self, _compress):
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        assert (len(pool.getSessionId()) == 10)
        self.conn.run("""undef(`tab,SHARED);
              t=table(100:0,
              ["cbool","cchar","cshort","cint","clong","cdate","cmonth","ctime","cminute","csecond","cdatetime","ctimestamp","cnanotime","cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"],
              [BOOL,CHAR,SHORT,INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, BLOB]);
              share t as tab;
              """)
        df = pd.DataFrame({'cbool': np.array([True, True, None]),
                           'cchar': np.array([1, -1, 0], dtype=np.int8),
                           'cshort': np.array([-10, 1000, 0], dtype=np.int16),
                           'cint': np.array([-10, 1000, 0], dtype=np.int32),
                           'clong': np.array([-10, 1000, 0], dtype=np.int64),
                           'cdate': np.array(['2020-01-02 13:30:10.008', '1970-12-21 13:30:10.008', 'nat'],
                                             dtype='datetime64[D]'),
                           'cmonth': np.array(['2020-01-02 13:30:10.008', '1970-12-21 13:30:10.008', 'nat'],
                                              dtype='datetime64[M]'),
                           'ctime': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335245880', 'nat'],
                                             dtype='datetime64[ms]'),
                           'cminute': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335245880', 'nat'],
                                               dtype='datetime64[m]'),
                           'csecond': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335245880', 'nat'],
                                               dtype='datetime64[s]'),
                           'cdatetime': np.array(['1970-01-01 13:30:10.008', '2022-10-03 05:15:33.335245880', 'nat'],
                                                 dtype='datetime64[s]'),
                           'ctimestamp': np.array(['2012-06-13 13:30:10.008', '2001-04-22 15:18:29.118325481', 'nat'],
                                                  dtype='datetime64[ms]'),
                           'cnanotime': np.array(
                               ['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                               dtype='datetime64[ns]'),
                           'cnanotimestamp': np.array(
                               ['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                               dtype='datetime64[ns]'),
                           'cfloat': np.array([2.2134500, -5.36411, np.nan], dtype='float32'),
                           'cdouble': np.array([3.214, -47.795324, np.nan], dtype='float64'),
                           'csymbol': np.array(['sym1', 'sym2', ''], dtype='object'),
                           'cstring': np.array(['str1', 'str2', ''], dtype='object'),
                           'cipaddr': np.array(["192.168.1.1", "192.168.1.254", "0.0.0.0"], dtype='object'),
                           'cblob': np.array(['blob1', 'blob2', ''], dtype='object')
                           })
        df.__DolphinDB_Type__ = {
            'clong': keys.DT_LONG,
            'cipaddr': keys.DT_IPPADDR,
            'cblob': keys.DT_BLOB,
        }
        # print(df)
        pool.addTask(r"tableInsert{tab}", 1, df)
        if not pool.isFinished(1):
            assert pool.getData(1) == None
        else:
            assert pool.getData(1) == 3
        pool.shutDown()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.timeout(10)
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_addTask_2(self, _compress):
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        assert (len(pool.getSessionId()) == 10)
        self.conn.run("""undef(`tab,SHARED);
              t=table(100:0,
              ["cbool","cchar","cshort","cint","clong","cdate","cmonth","ctime","cminute","csecond","cdatetime","ctimestamp","cnanotime","cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"],
              [BOOL,CHAR,SHORT,INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, BLOB]);
              share t as tab;
              """)
        df = pd.DataFrame({'cbool': np.array([True, True, None]),
                           'cchar': np.array([1, -1, 0], dtype=np.int8),
                           'cshort': np.array([-10, 1000, 0], dtype=np.int16),
                           'cint': np.array([-10, 1000, 0], dtype=np.int32),
                           'clong': np.array([-10, 1000, 0], dtype=np.int64),
                           'cdate': np.array(['2020-01-02', '1970-12-21', 'nat'], dtype='datetime64[ns]'),
                           'cmonth': np.array(['2020-01', '1970-12', 'nat'], dtype='datetime64[ns]'),
                           'ctime': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335', 'nat'],
                                             dtype='datetime64[ns]'),
                           'cminute': np.array(['1970-01-01 13:30', '1970-01-01 05:15', 'nat'], dtype='datetime64[ns]'),
                           'csecond': np.array(['1970-01-01 13:30:10', '1970-01-01 05:15:33', 'nat'],
                                               dtype='datetime64[ns]'),
                           'cdatetime': np.array(['1970-01-01 13:30:10', '2022-10-03 05:15:33', 'nat'],
                                                 dtype='datetime64[ns]'),
                           'ctimestamp': np.array(['2012-06-13 13:30:10.008', '2001-04-22 15:18:29.118', 'nat'],
                                                  dtype='datetime64[ns]'),
                           'cnanotime': np.array(
                               ['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                               dtype='datetime64[ns]'),
                           'cnanotimestamp': np.array(
                               ['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                               dtype='datetime64[ns]'),
                           'cfloat': np.array([2.2134500, -5.36411, np.nan], dtype='float32'),
                           'cdouble': np.array([3.214, -47.795324, np.nan], dtype='float64'),
                           'csymbol': np.array(['sym1', 'sym2', ''], dtype='object'),
                           'cstring': np.array(['str1', 'str2', ''], dtype='object'),
                           'cipaddr': np.array(["192.168.1.1", "192.168.1.254", "0.0.0.0"], dtype='object'),
                           'cblob': np.array(['blob1', 'blob2', ''], dtype='object')
                           })
        df.__DolphinDB_Type__ = {
            'cbool': keys.DT_BOOL,
            'cchar': keys.DT_CHAR,
            'cshort': keys.DT_SHORT,
            'cint': keys.DT_INT,
            'clong': keys.DT_LONG,
            'cipaddr': keys.DT_IPPADDR,
            'cblob': keys.DT_BLOB,
        }
        # print(df)
        pool.addTask(r"tableInsert{tab}", 1, df)
        pool.shutDown()
        res = self.conn.run("tab")
        assert_frame_equal(df, res)
        self.conn.undef("tab", "SHARED")

    @pytest.mark.CONNECTIONPOOL
    def test_DBConnectionPool_func_runTaskAsyn_warning(self):
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456")
        with pytest.warns(DeprecationWarning):
            task = pool.runTaskAsyn("sleep(2000);1+1")
            while not task.done():
                time.sleep(1)
            assert not task.running()
            assert (task.result() == 2)

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_runTaskAsync(self, _compress):
        pool = ddb.DBConnectionPool(
            HOST, PORT, 10, "admin", "123456", compress=_compress)
        self.conn.run("""
                        login("admin","123456")
                        dbpath="dfs://test_runTaskAsync";
                        if(existsDatabase(dbpath)){dropDatabase(dbpath)};
                        db=database(dbpath, VALUE, `APPL`TESLA`GOOGLE`PDD);
                        t=table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3);
                        db.createPartitionedTable(t,`dfs_tab,`col1).append!(t);
                      """)
        task = pool.runTaskAsync(
            "sleep(2000);exec count(*) from loadTable('dfs://test_runTaskAsync', `dfs_tab)")
        while not task.done():
            time.sleep(1)
        assert not task.running()
        assert (task.result() == 4)

    @pytest.mark.CONNECTIONPOOL
    def test_DBConnectionPool_print_msg_in_console(self):
        script = "a=int(1);\
        b=bool(1);\
        c=char(1);\
        d=NULL;\
        ee=short(1);\
        f=long(1);\
        g=date(1);\
        h=month(1);\
        i=time(1);\
        j=minute(1);\
        k=second(1);\
        l=datetime(1);\
        m=timestamp(1);\
        n=nanotime(1);\
        o=nanotimestamp(1);\
        p=float(1);\
        q=double(1);\
        r=\"1\";\
        s=uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\");\
        ttt=blob(string[1]);\
        u=table(1 2 3 as col1, `a`b`c as col2);\
        print(a,b,c,d,ee,f,g,h,i,j,k,l,m,n,o,p,q,r,s,ttt,u)"
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "import asyncio;"
                                 f"pool=ddb.DBConnectionPool('{HOST}', {PORT}, 10, '{USER}', '{PASSWD}');"
                                 "loop=asyncio.get_event_loop();"
                                 f"task=pool.run(\"\"\"{script}\"\"\");"
                                 "loop.run_until_complete(asyncio.wait([loop.create_task(task)]));"
                                 "loop.close();"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == """1\n1\n1\n1\n1\n1970.01.02\n0000.02M\n00:00:00.001\n00:01m\n00:00:01\n1970.01.01T00:00:01\n1970.01.01T00:00:00.001\n00:00:00.000000001\n1970.01.01T00:00:00.000000001\n1\n1\n1\n5d212a78-cc48-e3b1-4235-b4d91473ee87\n["1"]\ncol1 col2\n---- ----\n1    a   \n2    b   \n3    c   \n\n"""

    @pytest.mark.CONNECTIONPOOL
    def test_DBConnectionPool_disable_print_msg_in_console(self):
        script = "a=int(1);\
        b=bool(1);\
        c=char(1);\
        d=NULL;\
        ee=short(1);\
        f=long(1);\
        g=date(1);\
        h=month(1);\
        i=time(1);\
        j=minute(1);\
        k=second(1);\
        l=datetime(1);\
        m=timestamp(1);\
        n=nanotime(1);\
        o=nanotimestamp(1);\
        p=float(1);\
        q=double(1);\
        r=\"1\";\
        s=uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\");\
        ttt=blob(string[1]);\
        u=table(1 2 3 as col1, `a`b`c as col2);\
        print(a,b,c,d,ee,f,g,h,i,j,k,l,m,n,o,p,q,r,s,ttt,u)"
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "import asyncio;"
                                 f"pool=ddb.DBConnectionPool('{HOST}', {PORT}, 10, '{USER}', '{PASSWD}',show_output=False);"
                                 "loop=asyncio.get_event_loop();"
                                 f"task=pool.run(\"\"\"{script}\"\"\");"
                                 "loop.run_until_complete(asyncio.wait([loop.create_task(task)]));"
                                 "loop.close();"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == ""

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_DBConnectionPool_insert_dataframe_with_numpy_order(self, _compress, _order, _python_list):
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
        self.conn.upload({'t': df})
        self.conn.run("""
        colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cipaddr`cuuid`cint128;
        colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, UUID, INT128];
        t=table(1:0, colName,colType)
        dbPath = "dfs://test_dfs1"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[LONG,1],,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `index,)
        """)

        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD, compress=_compress)

        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(pool.run("tableInsert{loadTable('dfs://test_dfs1',`pt)}", df))
        self.conn.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, false, i,i,i,i,i,i+23640,i,i,i,i,i,i,i,i,i,i, 'sym','str', ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"))
            }
        """)

        res = self.conn.run("""ex = select * from objByName(`t);
                           res = select * from loadTable("dfs://test_dfs1", `pt);
                           print(ex)
                           print(res)
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        pool.shutDown()
        loop.close()
        tys = self.conn.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase("dfs://test_dfs1")

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_DBConnectionPool_insert_null_dataframe_with_numpy_order(self, _compress, _order, _python_list):
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
                                             'cint128', ], dtype='object')
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
        self.conn.upload({'t': df})
        self.conn.run("""
        colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cipaddr`cuuid`cint128;
        colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, UUID, INT128];
        t=table(1:0, colName,colType)
        dbPath = "dfs://test_dfs1"
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[LONG,1],,'OLAP')
        pt = db.createPartitionedTable(t, `pt, `index)
        """)
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD, compress=_compress)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(pool.run("tableInsert{loadTable('dfs://test_dfs1',`pt)}", df))
        self.conn.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = self.conn.run("""ex = select * from objByName(`t);
                           res = select * from loadTable("dfs://test_dfs1", `pt);
                           all(each(eqObj, ex.values(), res.values()))""")
        assert res
        pool.shutDown()
        loop.close()
        tys = self.conn.run("schema(loadTable('dfs://test_dfs1', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE',
                    'SECOND', 'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT',
                    'DOUBLE', 'SYMBOL', 'STRING', 'IPADDR', 'UUID', 'INT128']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase("dfs://test_dfs1")

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_priority', [-1, 'a', 1.1, dict(), list(), tuple(), set(), None, 10])
    def test_run_with_para_priority_exception(self, _priority):
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        with pytest.raises(Exception, match="priority must be an integer from 0 to 9"):
            loop.run_until_complete(pool.run("objs();sleep(5000)", priority=_priority))
        pool.shutDown()
        loop.close()

    @pytest.mark.CONNECTIONPOOL
    @pytest.mark.parametrize('_parallelism', [-1, 'a', 1.1, dict(), list(), tuple(), set(), None])
    def test_run_with_para_parallelism_exception(self, _parallelism):
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        with pytest.raises(Exception, match="parallelism must be an integer greater than 0"):
            loop.run_until_complete(pool.run("objs();sleep(5000)", parallelism=_parallelism))
        pool.shutDown()
        loop.close()

    @pytest.mark.CONNECTIONPOOL
    def test_run_with_para_priority_parallelism(self):
        pool1 = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        tasks = [
            loop.create_task(pool1.run(f"""
            sessionid = exec sessionid from getSessionMemoryStat() where userId=`{USER};
            priority = exec priority from getConsoleJobs() where sessionId=sessionid;
            parallelism = exec parallelism from getConsoleJobs() where sessionId=sessionid;
            [priority[0], parallelism[0]]
            """, priority=0, parallelism=10)),
            loop.create_task(pool1.run(f"""
            sessionid = exec sessionid from getSessionMemoryStat() where userId=`{USER};
            priority = exec priority from getConsoleJobs() where sessionId=sessionid;
            parallelism = exec parallelism from getConsoleJobs() where sessionId=sessionid;
            [priority[0], parallelism[0]]
            """)),
            loop.create_task(pool1.run(f"""
            sessionid = exec sessionid from getSessionMemoryStat() where userId=`{USER};
            priority = exec priority from getConsoleJobs() where sessionId=sessionid;
            parallelism = exec parallelism from getConsoleJobs() where sessionId=sessionid;
            [priority[0], parallelism[0]]
            """, priority=9, parallelism=1))

        ]
        loop.run_until_complete(asyncio.wait(tasks))
        expect = [[0, 10], [4, 64], [8, 1]]
        for ind, task in enumerate(tasks):
            assert_array_equal(task.result(), expect[ind])
        pool1.shutDown()
        loop.close()

    def test_DBConnectionPool_scalar_overlenth(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            loop.run_until_complete(asyncio.gather(*[pool.run('print', '0' * 4 * 64 * 1024)]))
        pool.shutDown()

    def test_DBConnectionPool_vector_overlenth(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            loop.run_until_complete(asyncio.gather(*[pool.run('print', ['0' * 4 * 64 * 1024])]))
        pool.shutDown()

    def test_DBConnectionPool_set_overlenth(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            loop.run_until_complete(asyncio.gather(*[pool.run('print', {'0' * 256 * 1024})]))
        pool.shutDown()

    def test_DBConnectionPool_dicionary_overlenth(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            loop.run_until_complete(asyncio.gather(*[pool.run('print', {'a': '0' * 256 * 1024})]))
        pool.shutDown()

    def test_DBConnectionPool_table_overlenth(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes") as e:
            loop.run_until_complete(
                asyncio.gather(*[pool.run('print', pd.DataFrame({'a': [1, 2, 3], 'b': ['1' * 256 * 1024, '2', '3']}))]))
        pool.shutDown()


if __name__ == '__main__':
    pytest.main(["-s", "test/test_DBConnectionPool.py"])
