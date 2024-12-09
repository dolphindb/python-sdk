import asyncio
import decimal
import inspect
import random
import subprocess
import sys
import time

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from pandas._testing import assert_frame_equal

from setup.settings import HOST, PORT, USER, PASSWD, HOST_CLUSTER, PORT_DNODE1, PORT_CONTROLLER, USER_CLUSTER, \
    PASSWD_CLUSTER, PORT_DNODE2, PORT_DNODE3


class TestDBConnectionPool:
    conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=False)

    def test_DBConnectionPool_protocol_error(self):
        with pytest.raises(RuntimeError, match="Protocol 4 is not supported. "):
            ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD, protocol=4)
        with pytest.raises(RuntimeError, match="The Arrow protocol does not support compression."):
            ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD, protocol=keys.PROTOCOL_ARROW, compress=True)

    @pytest.mark.parametrize('host', [HOST, "255.255.255.255", ""], ids=["T_HOST", "F_HOST", "N_HOST"])
    @pytest.mark.parametrize('port', [PORT, 0, None], ids=["T_PORT", "F_PORT", "N_PORT"])
    def test_DBConnectionPool_host_port(self, host, port):
        loop = asyncio.get_event_loop_policy().new_event_loop()
        if host == HOST and port == PORT:
            pool1 = ddb.DBConnectionPool(host, port, 2, USER, PASSWD)
            taska = [loop.create_task(pool1.run("1+1"))]
            loop.run_until_complete(asyncio.wait(taska))
            assert taska[0].result() == 2
            pool1.shutDown()
        elif port is None:
            with pytest.raises(TypeError):
                ddb.DBConnectionPool(host, port, 2, USER, PASSWD)
        else:
            with pytest.raises(RuntimeError):
                pool1 = ddb.DBConnectionPool(host, port, 2, USER, PASSWD)
                taska = [loop.create_task(pool1.run("1+1"))]
                loop.run_until_complete(asyncio.wait(taska))
        loop.close()

    @pytest.mark.parametrize('user', [USER, "adminxxxx"], ids=["T_USER", "F_USER"])
    @pytest.mark.parametrize('passwd', [PASSWD, "123456777888", ""], ids=["T_PASSWD", "F_PASSWD", "N_PASSWD"])
    def test_DBConnectionPool_user_passwd(self, user, passwd):
        loop = asyncio.get_event_loop_policy().new_event_loop()
        if user == USER and passwd == PASSWD:
            pool1 = ddb.DBConnectionPool(HOST, PORT, 2, user, passwd)
            taska = [loop.create_task(pool1.run("1+1"))]
            loop.run_until_complete(asyncio.wait(taska))
            assert taska[0].result() == 2
            pool1.shutDown()
        else:
            try:
                pool1 = ddb.DBConnectionPool(HOST, PORT, 2, user, passwd)
                taska = [loop.create_task(pool1.run("1+1"))]
                loop.run_until_complete(asyncio.wait(taska))
                pool1.shutDown()
            except RuntimeError:
                assert True
        loop.close()

    def test_DBConnectionPool_read_dfs_table(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 4, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        pool.runTaskAsync(f"""
            dbName="dfs://{func_name}"
            tableName="pt"
            if(existsDatabase(dbName)){{
                dropDatabase(dbName)
            }}
            db=database(dbName, VALUE, 1..10)
            n=1000
            t=table(loop(take{{, n/10}}, 1..10).flatten() as id, 1..1000 as val)
            pt=db.createPartitionedTable(t, `pt, `id).append!(t)
        """).result()

        async def get_row_count():
            return await pool.run(f"exec count(*) from loadTable('dfs://{func_name}', 'pt')")

        loop = asyncio.get_event_loop_policy().new_event_loop()
        tasks = [loop.create_task(get_row_count()) for _ in range(10)]
        loop.run_until_complete(asyncio.wait(tasks))
        for task in tasks:
            assert task.result() == 1000
        pool.shutDown()
        loop.close()

    def test_DBConnectionPool_read_dfs_table_runTaskAsync_Unspecified_time(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 4, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        pool.startLoop()
        pool.runTaskAsync(f"""
            dbName="dfs://{func_name}"
            tableName="pt"
            if(existsDatabase(dbName)){{
                dropDatabase(dbName)
            }}
            db=database(dbName, VALUE, 1..10)
            n=1000
            t=table(loop(take{{, n/10}}, 1..10).flatten() as id, 1..1000 as val)
            pt=db.createPartitionedTable(t, `pt, `id).append!(t)
        """).result()
        task1 = pool.runTaskAsync(f"sleep(1000);exec count(*) from loadTable('dfs://{func_name}', 'pt')")
        assert task1.result() == 1000
        pool.shutDown()

    def test_DBConnectionPool_read_dfs_table_runTaskAsync_unfinished(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 4, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        pool.startLoop()
        pool.runTaskAsync(f"""
            dbName="dfs://{func_name}"
            tableName="pt"
            if(existsDatabase(dbName)){{
                dropDatabase(dbName)
            }}
            db=database(dbName, VALUE, 1..10)
            n=1000
            t=table(loop(take{{, n/10}}, 1..10).flatten() as id, 1..1000 as val)
            pt=db.createPartitionedTable(t, `pt, `id).append!(t)
        """).result()
        task1 = pool.runTaskAsync(f"sleep(7000);exec count(*) from loadTable('dfs://{func_name}', 'pt')")
        try:
            assert task1.result(1) == 1000
        except:
            assert task1.result(8) == 1000
        pool.shutDown()

    def test_DBConnectionPool_read_dfs_table_runTaskAsync_param_basic(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 4, USER, PASSWD)
        pool.startLoop()
        task1 = pool.runTaskAsync("add", 1, 2)
        task2 = pool.runTaskAsync("sum", [1, 2, 3])
        task3 = pool.runTaskAsync("time", 8)
        assert task1.result() == 3
        assert task2.result() == 6
        assert task3.result() == np.datetime64("1970-01-01T00:00:00.008")
        pool.shutDown()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_insert_table_with_All_types(self, _compress):
        pool = ddb.DBConnectionPool(HOST, PORT, 8, USER, PASSWD, compress=_compress)
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}'
        pool.runTaskAsync(f"""
            undef(`{func_name}_tab,SHARED);
            t=table(100:0,
            ["cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute","csecond","cdatetime","ctimestamp","cnanotime","cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"],
            [BOOL,CHAR,SHORT,INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, BLOB]);
            share t as `{func_name}_tab;
        """).result()
        df = pd.DataFrame({
            'cbool': np.array([True, True, None]),
            'cchar': np.array([1, -1, None], dtype=np.float64),
            'cshort': np.array([-10, 1000, None], dtype=np.float64),
            'cint': np.array([-10, 1000, None], dtype=np.float64),
            'cdate': np.array(['2020-01-02', '1970-12-21', 'nat'], dtype='datetime64[ns]'),
            'cmonth': np.array(['2020-01', '1970-12', 'nat'], dtype='datetime64[ns]'),
            'ctime': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335', 'nat'], dtype='datetime64[ns]'),
            'cminute': np.array(['1970-01-01 13:30', '1970-01-01 05:15', 'nat'], dtype='datetime64[ns]'),
            'csecond': np.array(['1970-01-01 13:30:10', '1970-01-01 05:15:33', 'nat'], dtype='datetime64[ns]'),
            'cdatetime': np.array(['1970-01-01 13:30:10', '2022-10-03 05:15:33', 'nat'], dtype='datetime64[ns]'),
            'ctimestamp': np.array(['2012-06-13 13:30:10.008', '2001-04-22 15:18:29.118', 'nat'],
                                   dtype='datetime64[ns]'),
            'cnanotime': np.array(['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                                  dtype='datetime64[ns]'),
            'cnanotimestamp': np.array(['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                                       dtype='datetime64[ns]'),
            'cfloat': np.array([2.2134500, -5.36411, np.nan], dtype='float32'),
            'cdouble': np.array([3.214, -47.795324, np.nan], dtype='float64'),
            'csymbol': np.array(['sym1', 'sym2', ''], dtype='object'),
            'cstring': np.array(['str1', 'str2', ''], dtype='object'),
            'cipaddr': np.array(["192.168.1.1", "192.168.1.254", "0.0.0.0"], dtype='object'),
            'cblob': np.array([b'blob1', b'blob2', b''], dtype='object')
        })
        df.__DolphinDB_Type__ = {
            'cipaddr': keys.DT_IPPADDR,
            'cblob': keys.DT_BLOB,
        }
        pool.addTask(f"tableInsert{{{func_name}_tab}}", 1, df)
        while not pool.isFinished(1):
            time.sleep(1)
        res = self.conn.run(f"select * from {func_name}_tab")
        assert len(res) == 3
        assert_frame_equal(df, res)
        pool.shutDown()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_run(self, _compress):
        loop = asyncio.get_event_loop_policy().new_event_loop()
        pool = ddb.DBConnectionPool(HOST, PORT, 4, USER, PASSWD, compress=_compress)
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}'
        task1 = pool.run(f'''
            login("{USER}","{PASSWD}")
            dbpath="dfs://{func_name}";
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};
            db=database(dbpath, VALUE, `APPL`TESLA`GOOGLE`PDD);
            t=table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3);
            db.createPartitionedTable(t,`dfs_tab,`col1).append!(t);
            tab = select * from loadTable(dbpath, `dfs_tab)
            tab
        ''', clearMemory=False, pickleTableToList=True)
        task2 = pool.run(f'''
            login("{USER}","{PASSWD}")
            dbpath="dfs://{func_name}_2";
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};
            db=database(dbpath, VALUE, `APPL`TESLA`GOOGLE`PDD);
            t=table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3);
            db.createPartitionedTable(t,`dfs_tab,`col1).append!(t);
            tab = select * from loadTable(dbpath, `dfs_tab)
            tab
        ''', clearMemory=False, pickleTableToList=False)
        task_list = [
            loop.create_task(task1),
            loop.create_task(task2),
        ]
        loop.run_until_complete(asyncio.wait(task_list))
        tab = task_list[0].result()
        tab1 = task_list[1].result()
        assert isinstance(tab, list)
        assert isinstance(tab1, pd.DataFrame)
        expect_df = self.conn.run(f'select * from loadTable("dfs://{func_name}", `dfs_tab)')
        expect_tab = self.conn.table(data=expect_df)
        assert_array_equal(tab, expect_tab.toList())
        expect_df2 = self.conn.run(f'select * from loadTable("dfs://{func_name}_2", `dfs_tab)')
        expect_tab2 = self.conn.table(data=expect_df2)
        assert_frame_equal(tab1, expect_tab2.toDF())
        pool.shutDown()
        loop.close()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_run_clearMemory(self, _compress):
        loop = asyncio.get_event_loop_policy().new_event_loop()
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD, compress=_compress)
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}'
        task1 = pool.run(f'''
            login("{USER}","{PASSWD}")
            dbpath="dfs://{func_name}";
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};
            db=database(dbpath, VALUE, `APPL`TESLA`GOOGLE`PDD);
            t=table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3);
            db.createPartitionedTable(t,`dfs_tab,`col1).append!(t);
            asdeee = select * from loadTable(dbpath, `dfs_tab)
            asdeee
        ''', clearMemory=True, pickleTableToList=True)
        task2 = pool.run(f'''
            login("{USER}","{PASSWD}")
            dbpath="dfs://{func_name}_1";
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};
            db=database(dbpath, VALUE, `APPL`TESLA`GOOGLE`PDD);
            t=table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3);
            db.createPartitionedTable(t,`dfs_tab,`col1).append!(t);
            zxcfff = select * from loadTable(dbpath, `dfs_tab)
            zxcfff
        ''', clearMemory=True, pickleTableToList=False)
        task_list = [
            loop.create_task(task1),
            loop.create_task(task2),
        ]
        loop.run_until_complete(asyncio.wait(task_list))
        tab = task_list[0].result()
        tab1 = task_list[1].result()
        assert isinstance(tab, list)
        assert isinstance(tab1, pd.DataFrame)
        expect_df = self.conn.run(f'select * from loadTable("dfs://{func_name}", `dfs_tab)')
        expect_tab = self.conn.table(data=expect_df)
        assert_array_equal(tab, expect_tab.toList())
        expect_df2 = self.conn.run(f'select * from loadTable("dfs://{func_name}_1", `dfs_tab)')
        expect_tab2 = self.conn.table(data=expect_df2)
        assert_frame_equal(tab1, expect_tab2.toDF())
        try:
            loop.run_until_complete(pool.run("asdeee"))
            assert False
        except Exception as e:
            assert "Cannot recognize the token asdeee" in str(e)
        pool.shutDown()
        loop.close()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_addTask(self, _compress):
        pool = ddb.DBConnectionPool(HOST, PORT, 8, USER, PASSWD, compress=_compress)
        assert len(pool.getSessionId()) == 8
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}'
        pool.runTaskAsync(f"""
            undef(`{func_name}_tab,SHARED);
            t=table(100:0,
            ["cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute","csecond","cdatetime","ctimestamp","cnanotime","cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"],
            [BOOL,CHAR,SHORT,INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, BLOB]);
            share t as `{func_name}_tab;
        """).result()
        df = pd.DataFrame({
            'cbool': np.array([True, True, None]),
            'cchar': np.array([1, -1, None], dtype=np.float64),
            'cshort': np.array([-10, 1000, None], dtype=np.float64),
            'cint': np.array([-10, 1000, None], dtype=np.float64),
            'cdate': np.array(['2020-01-02', '1970-12-21', 'nat'], dtype='datetime64[ns]'),
            'cmonth': np.array(['2020-01', '1970-12', 'nat'], dtype='datetime64[ns]'),
            'ctime': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335', 'nat'], dtype='datetime64[ns]'),
            'cminute': np.array(['1970-01-01 13:30', '1970-01-01 05:15', 'nat'], dtype='datetime64[ns]'),
            'csecond': np.array(['1970-01-01 13:30:10', '1970-01-01 05:15:33', 'nat'], dtype='datetime64[ns]'),
            'cdatetime': np.array(['1970-01-01 13:30:10', '2022-10-03 05:15:33', 'nat'], dtype='datetime64[ns]'),
            'ctimestamp': np.array(['2012-06-13 13:30:10.008', '2001-04-22 15:18:29.118', 'nat'],
                                   dtype='datetime64[ns]'),
            'cnanotime': np.array(
                ['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'], dtype='datetime64[ns]'),
            'cnanotimestamp': np.array(['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                                       dtype='datetime64[ns]'),
            'cfloat': np.array([2.2134500, -5.36411, np.nan], dtype='float32'),
            'cdouble': np.array([3.214, -47.795324, np.nan], dtype='float64'),
            'csymbol': np.array(['sym1', 'sym2', ''], dtype='object'),
            'cstring': np.array(['str1', 'str2', ''], dtype='object'),
            'cipaddr': np.array(["192.168.1.1", "192.168.1.254", "0.0.0.0"], dtype='object'),
            'cblob': np.array([b'blob1', b'blob2', b''], dtype='object')
        })
        df.__DolphinDB_Type__ = {
            'cipaddr': keys.DT_IPPADDR,
            'cblob': keys.DT_BLOB,
        }
        pool.addTask(f"tableInsert{{{func_name}_tab}}", 1, df)
        while not pool.isFinished(1):
            time.sleep(1)
        res = self.conn.run(f"select * from {func_name}_tab")
        assert pool.getData(1) == 3
        assert_frame_equal(df, res)
        pool.shutDown()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_addTask_1(self, _compress):
        pool = ddb.DBConnectionPool(HOST, PORT, 8, USER, PASSWD, compress=_compress)
        assert len(pool.getSessionId()) == 8
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}'
        pool.runTaskAsync(f"""
            undef(`{func_name}_tab,SHARED);
            t=table(100:0,
            ["cbool","cchar","cshort","cint","clong","cdate","cmonth","ctime","cminute","csecond","cdatetime","ctimestamp","cnanotime","cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"],
            [BOOL,CHAR,SHORT,INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, BLOB]);
            share t as `{func_name}_tab;
        """).result()
        df = pd.DataFrame({
            'cbool': np.array([True, True, None]),
            'cchar': np.array([1, -1, 0], dtype=np.int8),
            'cshort': np.array([-10, 1000, 0], dtype=np.int16),
            'cint': np.array([-10, 1000, 0], dtype=np.int32),
            'clong': np.array([-10, 1000, 0], dtype=np.int64),
            'cdate': np.array(['2020-01-02 13:30:10.008', '1970-12-21 13:30:10.008', 'nat'], dtype='datetime64[D]'),
            'cmonth': np.array(['2020-01-02 13:30:10.008', '1970-12-21 13:30:10.008', 'nat'], dtype='datetime64[M]'),
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
            'cnanotime': np.array(['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                                  dtype='datetime64[ns]'),
            'cnanotimestamp': np.array(['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
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
        pool.addTask(f"tableInsert{{{func_name}_tab}}", 1, df)
        if not pool.isFinished(1):
            assert pool.getData(1) is None
        else:
            assert pool.getData(1) == 3
        pool.shutDown()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_addTask_2(self, _compress):
        pool = ddb.DBConnectionPool(HOST, PORT, 8, USER, PASSWD, compress=_compress)
        assert len(pool.getSessionId()) == 8
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}'
        pool.runTaskAsync(f"""
            undef(`{func_name}_tab,SHARED);
            t=table(100:0,
            ["cbool","cchar","cshort","cint","clong","cdate","cmonth","ctime","cminute","csecond","cdatetime","ctimestamp","cnanotime","cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob"],
            [BOOL,CHAR,SHORT,INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, IPADDR, BLOB]);
            share t as `{func_name}_tab;
        """).result()
        df = pd.DataFrame({
            'cbool': np.array([True, True, None]),
            'cchar': np.array([1, -1, 0], dtype=np.int8),
            'cshort': np.array([-10, 1000, 0], dtype=np.int16),
            'cint': np.array([-10, 1000, 0], dtype=np.int32),
            'clong': np.array([-10, 1000, 0], dtype=np.int64),
            'cdate': np.array(['2020-01-02', '1970-12-21', 'nat'], dtype='datetime64[ns]'),
            'cmonth': np.array(['2020-01', '1970-12', 'nat'], dtype='datetime64[ns]'),
            'ctime': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335', 'nat'], dtype='datetime64[ns]'),
            'cminute': np.array(['1970-01-01 13:30', '1970-01-01 05:15', 'nat'], dtype='datetime64[ns]'),
            'csecond': np.array(['1970-01-01 13:30:10', '1970-01-01 05:15:33', 'nat'], dtype='datetime64[ns]'),
            'cdatetime': np.array(['1970-01-01 13:30:10', '2022-10-03 05:15:33', 'nat'], dtype='datetime64[ns]'),
            'ctimestamp': np.array(['2012-06-13 13:30:10.008', '2001-04-22 15:18:29.118', 'nat'],
                                   dtype='datetime64[ns]'),
            'cnanotime': np.array(['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                                  dtype='datetime64[ns]'),
            'cnanotimestamp': np.array(['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                                       dtype='datetime64[ns]'),
            'cfloat': np.array([2.2134500, -5.36411, np.nan], dtype='float32'),
            'cdouble': np.array([3.214, -47.795324, np.nan], dtype='float64'),
            'csymbol': np.array(['sym1', 'sym2', ''], dtype='object'),
            'cstring': np.array(['str1', 'str2', ''], dtype='object'),
            'cipaddr': np.array(["192.168.1.1", "192.168.1.254", "0.0.0.0"], dtype='object'),
            'cblob': np.array([b'blob1', b'blob2', b''], dtype='object')
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
        pool.addTask(f"tableInsert{{{func_name}_tab}}", 1, df)
        pool.shutDown()
        res = self.conn.run(f"{func_name}_tab")
        assert_frame_equal(df, res)
        self.conn.undef(f"{func_name}_tab", "SHARED")

    def test_DBConnectionPool_func_runTaskAsyn_warning(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD, )
        with pytest.warns(DeprecationWarning):
            task = pool.runTaskAsyn("sleep(2000);1+1")
            while not task.done():
                time.sleep(1)
            assert not task.running()
            assert task.result() == 2

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    def test_DBConnectionPool_func_runTaskAsync(self, _compress):
        pool = ddb.DBConnectionPool(HOST, PORT, 8, USER, PASSWD, compress=_compress)
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}'
        self.conn.run(f"""
            dbpath="dfs://{func_name}";
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};
            db=database(dbpath, VALUE, `APPL`TESLA`GOOGLE`PDD);
            t=table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3);
            db.createPartitionedTable(t,`dfs_tab,`col1).append!(t);
        """)
        task = pool.runTaskAsync(f"sleep(2000);exec count(*) from loadTable('dfs://{func_name}', `dfs_tab)")
        while not task.done():
            time.sleep(1)
        assert not task.running()
        assert task.result() == 4

    def test_DBConnectionPool_print_msg_in_console(self):
        script = "\
            a=int(1);\
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
            v=arrayVector(1 2 3 , 9 9 9);\
            print(a,b,c,d,ee,f,g,h,i,j,k,l,m,n,o,p,q,r,s,ttt,u,v)"
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "import asyncio;"
                                 f"pool=ddb.DBConnectionPool('{HOST}', {PORT}, 10, '{USER}', '{PASSWD}');"
                                 "loop=asyncio.get_event_loop();"
                                 f"task=pool.run(\"\"\"{script}\"\"\");"
                                 "loop.run_until_complete(asyncio.wait([loop.create_task(task)]));"
                                 "loop.close();"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == """1\n1\n1\n1\n1\n1970.01.02\n0000.02M\n00:00:00.001\n00:01m\n00:00:01\n1970.01.01T00:00:01\n1970.01.01T00:00:00.001\n00:00:00.000000001\n1970.01.01T00:00:00.000000001\n1\n1\n1\n5d212a78-cc48-e3b1-4235-b4d91473ee87\n["1"]\ncol1 col2\n---- ----\n1    a   \n2    b   \n3    c   \n\n[[9],[9],[9]]\n"""

    def test_DBConnectionPool_disable_print_msg_in_console(self):
        script = "\
            a=int(1);\
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
            v=arrayVector(1 2 3 , 9 9 9);\
            print(a,b,c,d,ee,f,g,h,i,j,k,l,m,n,o,p,q,r,s,ttt,u,v)"
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
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}' + f'order_{_order}' + f'python_list_{_python_list}'
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
                        "e1671797c52e15f763380b45e841ec32", decimal.Decimal('-2.11'), decimal.Decimal('0.00000000000')]
            data.append(row_data)
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cblob', 'cipaddr',
                                             'cuuid', 'cint128', 'cdecimal32', 'cdecimal64'])
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cblob', 'cipaddr',
                                       'cuuid', 'cint128', 'cdecimal32', 'cdecimal64'])
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
            'cdecimal64': keys.DT_DECIMAL64
        }
        self.conn.upload({'t': df})
        self.conn.run(f"""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64;
            colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(2), DECIMAL64(11)];
            t=table(1:0, colName,colType)
            dbPath = "dfs://{func_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD, compress=_compress)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(pool.run(f"tableInsert{{loadTable('dfs://{func_name}',`pt)}}", df))
        self.conn.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, false, i,i,i,i,i,i+23640,i,i,i,i,i,i,i,i,i,i, 'sym','str', 'blob', ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"), decimal32(-2.11, 2), decimal64(0, 11))
            }
        """)
        res = self.conn.run(f"""
            ex = select * from objByName(`t);
            res = select * from loadTable("dfs://{func_name}", `pt);
            print(ex)
            print(res)
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        pool.shutDown()
        loop.close()
        tys = self.conn.run(f"schema(loadTable('dfs://{func_name}', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE', 'SECOND',
                    'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT', 'DOUBLE', 'SYMBOL',
                    'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(2)', 'DECIMAL64(11)']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase(f"dfs://{func_name}")

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_DBConnectionPool_insert_dataframe_array_vector_with_numpy_order(self, _compress, _order, _python_list):
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}' + f'order_{_order}' + f'python_list_{_python_list}'
        data = []
        for i in range(10):
            row_data = [i, [False], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i], [i],
                        ["1.1.1.1"], ["5d212a78-cc48-e3b1-4235-b4d91473ee87"],
                        ["e1671797c52e15f763380b45e841ec32"]]
            data.append(row_data)
        data.append(
            [10, [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None], [None],
             [None], [None], [None], [None], [None], [None], [None]])
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'cipaddr', 'cuuid', 'cint128'])
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'cipaddr', 'cuuid', 'cint128'])
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
            'cint128': keys.DT_INT128_ARRAY
        }
        self.conn.upload({'t': df})
        self.conn.run(f"""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128;
            colType = [LONG, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[]];
            t=table(1:0, colName,colType)
            dbPath = "dfs://{func_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD, compress=_compress)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(pool.run(f"tableInsert{{loadTable('dfs://{func_name}',`pt)}}", df))
        self.conn.run("""
            for(i in 0:10){
                tableInsert(t, i, 0, i,i,i,i,i,i,i,i,i,i,i,i,i,i,i,i, ipaddr("1.1.1.1"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),int128("e1671797c52e15f763380b45e841ec32"))
            }
            tableInsert(objByName(`t), 10, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
        """)
        res = self.conn.run(f"""
            ex = select * from objByName(`t);
            res = select * from loadTable("dfs://{func_name}", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        pool.shutDown()
        loop.close()
        tys = self.conn.run(f"schema(loadTable('dfs://{func_name}', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase(f"dfs://{func_name}")

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_DBConnectionPool_insert_null_dataframe_with_numpy_order(self, _compress, _order, _python_list):
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}' + f'order_{_order}' + f'python_list_{_python_list}'
        data = []
        origin_nulls = [None, np.nan, pd.NaT]
        for i in range(7):
            row_data = random.choices(origin_nulls, k=25)
            data.append([i, *row_data])
        data.append([7] + [None] * 25)
        data.append([8] + [pd.NaT] * 25)
        data.append([9] + [np.nan] * 25)
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cblob', 'cipaddr',
                                             'cuuid', 'cint128', 'cdecimal32', 'cdecimal64'], dtype='object')
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'csymbol', 'cstring', 'cblob', 'cipaddr',
                                       'cuuid', 'cint128', 'cdecimal32', 'cdecimal64'], dtype='object')
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
            'cdecimal64': keys.DT_DECIMAL64
        }
        self.conn.upload({'t': df})
        self.conn.run(f"""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64;
            colType = [LONG, BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, DATEHOUR, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(9), DECIMAL64(18)];
            t=table(1:0, colName,colType)
            dbPath = "dfs://{func_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD, compress=_compress)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(pool.run(f"tableInsert{{loadTable('dfs://{func_name}',`pt)}}", df))
        self.conn.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = self.conn.run(f"""
            ex = select * from objByName(`t);
            res = select * from loadTable("dfs://{func_name}", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        pool.shutDown()
        loop.close()
        tys = self.conn.run(f"schema(loadTable('dfs://{func_name}', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL', 'CHAR', 'SHORT', 'INT', 'LONG', 'DATE', 'MONTH', 'TIME', 'MINUTE', 'SECOND',
                    'DATETIME', 'TIMESTAMP', 'NANOTIME', 'NANOTIMESTAMP', 'DATEHOUR', 'FLOAT', 'DOUBLE', 'SYMBOL',
                    'STRING', 'BLOB', 'IPADDR', 'UUID', 'INT128', 'DECIMAL32(9)', 'DECIMAL64(18)']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase(f"dfs://{func_name}")

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_order', ['F', 'C'], ids=["F_ORDER", "C_ORDER"])
    @pytest.mark.parametrize('_python_list', [True, False], ids=["PYTHON_LIST", "NUMPY_ARRAY"])
    def test_DBConnectionPool_insert_null_dataframe_array_vector_with_numpy_order(self, _compress, _order,
                                                                                  _python_list):
        func_name = inspect.currentframe().f_code.co_name + f'compress_{_compress}' + f'order_{_order}' + f'python_list_{_python_list}'
        data = []
        origin_nulls = [[None], [np.nan], [pd.NaT]]
        for i in range(7):
            row_data = random.choices(origin_nulls, k=20)
            data.append([i, *row_data])
        data.append([7] + [[None]] * 20)
        data.append([8] + [[pd.NaT]] * 20)
        data.append([9] + [[np.nan]] * 20)
        if _python_list:
            df = pd.DataFrame(data, columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                             'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp',
                                             'cnanotime', 'cnanotimestamp',
                                             'cdatehour', 'cfloat', 'cdouble', 'cipaddr', 'cuuid', 'cint128'])
        else:
            df = pd.DataFrame(np.array(data, dtype='object', order=_order),
                              columns=['index', 'cbool', 'cchar', 'cshort', 'cint', 'clong', 'cdate',
                                       'cmonth', 'ctime', 'cminute', 'csecond', 'cdatetime', 'ctimestamp', 'cnanotime',
                                       'cnanotimestamp',
                                       'cdatehour', 'cfloat', 'cdouble', 'cipaddr', 'cuuid', 'cint128'])
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
            'cint128': keys.DT_INT128_ARRAY
        }
        self.conn.upload({'t': df})
        self.conn.run(f"""
            colName =  `index`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cdatehour`cfloat`cdouble`cipaddr`cuuid`cint128;
            colType = [LONG, BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], DATEHOUR[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[]];
            t=table(1:0, colName,colType)
            dbPath = "dfs://{func_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[LONG,1],,'TSDB')
            pt = db.createPartitionedTable(t, `pt, `index,,`index)
        """)
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD, compress=_compress)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        loop.run_until_complete(pool.run(f"tableInsert{{loadTable('dfs://{func_name}',`pt)}}", df))
        self.conn.run("""
            for(i in 0:10){
                tableInsert(objByName(`t), i, NULL, NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
            }
        """)
        res = self.conn.run(f"""
            ex = select * from objByName(`t);
            res = select * from loadTable("dfs://{func_name}", `pt);
            all(each(eqObj, ex.values(), res.values()))
        """)
        assert res
        pool.shutDown()
        loop.close()
        tys = self.conn.run(f"schema(loadTable('dfs://{func_name}', `pt)).colDefs[`typeString]")
        ex_types = ['LONG', 'BOOL[]', 'CHAR[]', 'SHORT[]', 'INT[]', 'LONG[]', 'DATE[]', 'MONTH[]', 'TIME[]', 'MINUTE[]',
                    'SECOND[]', 'DATETIME[]', 'TIMESTAMP[]', 'NANOTIME[]', 'NANOTIMESTAMP[]', 'DATEHOUR[]', 'FLOAT[]',
                    'DOUBLE[]', 'IPADDR[]', 'UUID[]', 'INT128[]']
        assert_array_equal(tys, ex_types)
        self.conn.dropDatabase(f"dfs://{func_name}")

    @pytest.mark.parametrize('_priority', [-1, 'a', 1.1, dict(), list(), tuple(), set(), 10])
    def test_DBConnectionPool_run_with_para_priority_exception(self, _priority):
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        with pytest.raises(Exception, match="priority must be an integer from 0 to 9"):
            loop.run_until_complete(pool.run("objs();sleep(5000)", priority=_priority))
        pool.shutDown()
        loop.close()

    @pytest.mark.parametrize('_parallelism', [-1, 'a', 1.1, dict(), list(), tuple(), set()])
    def test_DBConnectionPool_run_with_para_parallelism_exception(self, _parallelism):
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        with pytest.raises(Exception, match="parallelism must be an integer greater than 0"):
            loop.run_until_complete(pool.run("objs();sleep(5000)", parallelism=_parallelism))
        pool.shutDown()
        loop.close()

    def test_DBConnectionPool_run_with_para_priority_parallelism_0_10(self):
        pool1 = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        tasks = [
            loop.create_task(pool1.run(f"""
                select priority,parallelism from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]
            """, priority=0, parallelism=10))
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        assert tasks[0].result()['priority'].iat[0] == 0
        assert tasks[0].result()['parallelism'].iat[0] == 10
        pool1.shutDown()
        loop.close()

    def test_DBConnectionPool_run_with_para_priority_parallelism_9_1(self):
        pool1 = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        tasks = [
            loop.create_task(pool1.run(f"""
                select priority,parallelism from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]
            """, priority=9, parallelism=1))
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        assert tasks[0].result()['priority'].iat[0] == 8
        assert tasks[0].result()['parallelism'].iat[0] == 1
        pool1.shutDown()
        loop.close()

    def test_DBConnectionPool_run_with_para_priority_parallelism(self):
        pool1 = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop_policy().new_event_loop()
        tasks = [
            loop.create_task(pool1.run(f"""
                select priority,parallelism from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]
            """))
        ]
        loop.run_until_complete(asyncio.wait(tasks))
        assert tasks[0].result()['priority'].iat[0] == 4
        assert tasks[0].result()['parallelism'].iat[0] == 64
        pool1.shutDown()
        loop.close()

    def test_DBConnectionPool_scalar_over_length(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            loop.run_until_complete(asyncio.gather(*[pool.run('print', '0' * 256 * 1024)]))
        pool.shutDown()

    def test_DBConnectionPool_vector_over_length(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            loop.run_until_complete(asyncio.gather(*[pool.run('print', ['0' * 256 * 1024])]))
        pool.shutDown()

    def test_DBConnectionPool_set_over_length(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            loop.run_until_complete(asyncio.gather(*[pool.run('print', {'0' * 256 * 1024})]))
        pool.shutDown()

    def test_DBConnectionPool_dictionary_over_length(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            loop.run_until_complete(asyncio.gather(*[pool.run('print', {'a': '0' * 256 * 1024})]))
        pool.shutDown()

    def test_DBConnectionPool_table_over_length(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER_CLUSTER, PASSWD_CLUSTER)
        loop = asyncio.get_event_loop()
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            loop.run_until_complete(asyncio.gather(*[pool.run('print', pd.DataFrame({'a': ['1' * 256 * 1024]}))]))
        with pytest.raises(RuntimeError,
                           match="String too long, Serialization failed, length must be less than 256K bytes"):
            data = pd.DataFrame({'a': ['1' * 256 * 1024]})
            data.__DolphinDB_Type__ = {
                'a': keys.DT_SYMBOL
            }
            loop.run_until_complete(asyncio.gather(*[pool.run('print', data)]))
        pool.shutDown()

    @pytest.mark.xdist_group(name='cluster_test')
    def test_DBConnectionPool_loadBalance(self):
        pool = ddb.DBConnectionPool(HOST_CLUSTER, PORT_DNODE1, 60, loadBalance=True)
        loadDf = pool.runTaskAsync(
            "select (connectionNum + workerNum + executorNum)/3.0 as load from rpc(getControllerAlias(), getClusterPerf) where mode=0").result()
        print(loadDf)
        assert all(((loadDf - loadDf.mean()).abs() < 0.4)['load'])
        pool.shutDown()

    @pytest.mark.xdist_group(name='cluster_test')
    def test_DBConnectionPool_reconnect(self):
        pool = ddb.DBConnectionPool(HOST_CLUSTER, PORT_DNODE1, 8, reConnect=True)
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn.run(f"stopDataNode(['dnode{PORT_DNODE1}'])")
        time.sleep(3)
        conn.run(f"startDataNode(['dnode{PORT_DNODE1}'])")
        time.sleep(3)
        assert pool.runTaskAsync("1+1").result() == 2
        pool.shutDown()
        conn.close()

    @pytest.mark.xdist_group(name='cluster_test')
    def test_DBConnectionPool_highAvailability(self):
        pool = ddb.DBConnectionPool(HOST_CLUSTER, PORT_DNODE1, 30, highAvailability=True)
        time.sleep(10)
        loadDf = pool.runTaskAsync(
            "select (connectionNum + workerNum + executorNum)/3.0 as load from rpc(getControllerAlias(), getClusterPerf) where mode=0").result()
        assert not all(((loadDf - loadDf.mean()).abs() < 0.4)['load'])
        pool.shutDown()

    def test_DBConnectionPool_sqlStd(self):
        pool_ddb = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD, sqlStd=keys.SqlStd.DolphinDB)
        pool_oracle = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD, sqlStd=keys.SqlStd.Oracle)
        pool_mysql = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD, sqlStd=keys.SqlStd.MySQL)
        with pytest.raises(RuntimeError):
            pool_ddb.runTaskAsync("sysdate()").result()
        pool_oracle.runTaskAsync("sysdate()").result()
        pool_mysql.runTaskAsync("sysdate()").result()

    def test_DBConnectionPool_tryReconnectNums_conn(self):
        n = 3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"pool=ddb.DBConnectionPool('{HOST}', 56789, 3, reConnect=True, tryReconnectNums={n});"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout.count("Failed") == n
        assert f"Connect to {HOST}:56789 failed after {n} reconnect attempts." in result.stdout

    @pytest.mark.xdist_group(name='cluster_test')
    def test_DBConnectionPool_tryReconnectNums_run(self):
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        n = 3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST_CLUSTER}',{PORT_CONTROLLER},'{USER_CLUSTER}','{PASSWD_CLUSTER}');"
                                 f"pool=ddb.DBConnectionPool('{HOST_CLUSTER}', {PORT_DNODE1}, 3, reConnect=True, tryReconnectNums={n});"
                                 f"""conn.run("stopDataNode(['dnode{PORT_DNODE1}'])");"""
                                 "pool.runTaskAsync('1+1').result();"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        conn.run(f"startDataNode(['dnode{PORT_DNODE1}'])")
        assert result.stdout.count("Failed") == n
        assert f"Connect to {HOST_CLUSTER}:{PORT_DNODE1} failed after {n} reconnect attempts." in result.stdout

    @pytest.mark.xdist_group(name='cluster_test')
    def test_DBConnectionPool_highAvailability_tryReconnectNums(self):
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        n = 3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST_CLUSTER}',{PORT_CONTROLLER},'{USER_CLUSTER}','{PASSWD_CLUSTER}');"
                                 f"pool=ddb.DBConnectionPool('{HOST}', {PORT_DNODE1}, 1, reConnect=True, highAvailability=True, tryReconnectNums={n});"
                                 f"""conn.run("stopDataNode(['dnode{PORT_DNODE1}','dnode{PORT_DNODE2}','dnode{PORT_DNODE3}'])");"""
                                 "pool.runTaskAsync('1+1').result();"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        conn.run(f"startDataNode(['dnode{PORT_DNODE1}','dnode{PORT_DNODE2}','dnode{PORT_DNODE3}'])")
        assert result.stdout.count("Failed") == n * 4
        assert f"Connect to nodes failed after {n} reconnect attempts." in result.stdout
