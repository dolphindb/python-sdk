import asyncio
import datetime
import inspect
import random
import threading
import time
from threading import Event

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from pandas._testing import assert_frame_equal

from setup.settings import HOST, PORT, USER, PASSWD, DATA_DIR


class MsgCount:
    def __init__(self):
        self.count = 0

    def setMsg(self):
        self.count += 1

    def getTotalMsg(self):
        return self.count


class TestReadmeCn:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    def test_readme_QuickStart_SimpleDemo_1(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        s.run(f"""
            n=1000000
            ID=rand(10, n)
            x=rand(1.0, n)
            t=table(ID, x)
            if(existsDatabase("{db_name}")){{dropDatabase("{db_name}")}};go;
            db=database(directory="{db_name}", partitionType=HASH, partitionScheme=[INT, 2])
            pt = db.createPartitionedTable(t, `pt, `ID)
            pt.append!(t);
        """)
        re = s.run("select count(x) from pt;")
        ex = pd.DataFrame({'count_x': [1000000]})
        assert_frame_equal(re, ex)
        s.close()

    def test_readme_QuickStart_SimpleDemo_2(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        n = 1000000
        df = pd.DataFrame({
            'ID': np.random.randint(0, 10, n),
            'x': np.random.rand(n),
        })
        s.run("schema_t = table(100000:0, `ID`x,[INT, DOUBLE])")
        schema_t = s.table(data="schema_t")
        if s.existsDatabase(db_name):
            s.dropDatabase(db_name)
        db = s.database(dbPath=db_name, partitionType=keys.HASH, partitions=[keys.DT_INT, 2])
        pt: ddb.Table = db.createPartitionedTable(table=schema_t, tableName="pt", partitionColumns=["ID"])
        data = s.table(data=df)
        pt.append(data)
        assert pt.toDF().shape[0] == 1000000
        assert pt.toDF().shape[1] == 2
        assert pt.toDF().columns.to_list() == ['ID', 'x']
        s.close()

    def test_readme_QuickStart_CommonOperations_1(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        n = 1000
        df = pd.DataFrame({
            'ID': np.random.randint(0, 3, n).astype("int32"),
            'x': np.random.rand(n),
        })
        if s.existsDatabase(db_name):
            s.dropDatabase(db_name)
        db: ddb.Database = s.database(dbPath=db_name, partitionType=keys.VALUE, partitions=[0, 1, 2])
        t = s.table(data=df)
        if not s.existsTable(db_name, "pt"):
            pt = db.createPartitionedTable(t, "pt", partitionColumns="ID")
        else:
            raise RuntimeError(f"{db_name} has table pt.")
        pt.append(t)
        res1 = pt.toDF().shape
        assert res1[0] == 1000
        assert res1[1] == 2
        s.dropPartition(db_name, 0, "pt")
        res2 = pt.toDF().shape
        assert res2[0] < 1000
        assert res2[1] == 2
        s.close()

    def test_readme_QuickStart_CommonOperations_2(self):
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        df = pd.DataFrame({
            'ID': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            'x': ['a', 'b', 'b', 'c', 'a', 'c', 'a', 'b', 'b', 'a'],
        })
        t = s.table(data=df)
        res = t.select(["ID", "x"]).where("ID>=5").executeAs("res")
        ex = pd.DataFrame({'ID': [5, 6, 7, 8, 9, 10], 'x': ['a', 'c', 'a', 'b', 'b', 'a']})
        assert_frame_equal(res.toDF(), ex)

    def test_readme_BasicOperation_Session_ParametersForConstructingSession(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session(HOST, PORT, USER, PASSWD, enableChunkGranularityConfig=True)
        if s.existsDatabase(db_name):
            s.dropDatabase(db_name)
        s.database("db", partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=db_name,
                   chunkGranularity="DATABASE")
        assert s.run("schema(db)")["chunkGranularity"] == 'DATABASE'
        if s.existsDatabase(db_name):
            s.dropDatabase(db_name)
        s.database("db", partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=db_name,
                   chunkGranularity="TABLE")
        assert s.run("schema(db)")["chunkGranularity"] == 'TABLE'
        s.close()

    def test_readme_BasicOperation_DBConnectionPool_Coprocess_1(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 8, USER, PASSWD)

        async def test_run(i):
            try:
                return await pool.run(f"sleep(2000);1+{i}")
            except Exception as e:
                print(e)

        tasks = [
            asyncio.ensure_future(test_run(1)),
            asyncio.ensure_future(test_run(3)),
            asyncio.ensure_future(test_run(5)),
            asyncio.ensure_future(test_run(7)),
        ]

        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(asyncio.wait(tasks))
        except Exception as e:
            print(e)
        for i in range(len(tasks)):
            assert tasks[i].result() == (i + 1) * 2
        pool.shutDown()

    def test_readme_BasicOperation_DBConnectionPool_Coprocess_2(self):

        class DolphinDBHelper(object):
            pool = ddb.DBConnectionPool(HOST, PORT, 8)

            @classmethod
            async def test_run(cls, script):
                return await cls.pool.run(script)

            @classmethod
            async def runTest(cls, script):
                task = loop.create_task(cls.test_run(script))
                result = await asyncio.gather(task)
                return result

        def start_thread_loop(loop):
            asyncio.set_event_loop(loop)
            loop.run_forever()

        async def stop_thread_loop(loop):
            await asyncio.sleep(10)
            loop.stop()

        loop = asyncio.get_event_loop()
        t = threading.Thread(target=start_thread_loop, args=(loop,))
        t.start()
        asyncio.run_coroutine_threadsafe(DolphinDBHelper.runTest("sleep(1000);1+1"), loop)
        asyncio.run_coroutine_threadsafe(DolphinDBHelper.runTest("sleep(3000);1+2"), loop)
        asyncio.run_coroutine_threadsafe(DolphinDBHelper.runTest("sleep(5000);1+3"), loop)
        asyncio.run_coroutine_threadsafe(DolphinDBHelper.runTest("sleep(1000);1+4"), loop)
        loop.create_task(stop_thread_loop(loop))

    def test_readme_BasicOperation_DBConnectionPool_Coprocess_3(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 8, USER, PASSWD)
        taskid = 12
        pool.addTask("sleep(1500);1+2", taskId=taskid)
        while True:
            if pool.isFinished(taskId=taskid):
                break
            time.sleep(0.01)
        res = pool.getData(taskId=taskid)
        assert res == 3
        pool.shutDown()

    def test_readme_BasicOperation_AutoFitTableAppender_TableAppender_1(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        s.run(f"share table(1000:0, `sym`timestamp`qty, [SYMBOL, TIMESTAMP, INT]) as {func_name}")
        appender = ddb.TableAppender(tableName=func_name, ddbSession=s)
        data = pd.DataFrame({
            'sym': ['A1', 'A2', 'A3', 'A4', 'A5'],
            'timestamp': np.array(
                ['2012-06-13 13:30:10.008', 'NaT', '2012-06-13 13:30:10.008', '2012-06-13 15:30:10.008', 'NaT'],
                dtype="datetime64[ns]"),
            'qty': np.arange(1, 6).astype("int32"),
        })
        num = appender.append(data)
        assert num == 5
        t = s.run(func_name)
        assert_frame_equal(t, data)
        schema = s.run(f"schema({func_name})")
        assert_array_equal(schema["colDefs"]['name'], ['sym', 'timestamp', 'qty'])
        assert_array_equal(schema["colDefs"]['typeString'], ['SYMBOL', 'TIMESTAMP', 'INT'])
        assert_array_equal(schema["colDefs"]['typeInt'], [17, 12, 4])
        assert_array_equal(schema["colDefs"]['extra'], [np.nan, np.nan, np.nan])
        assert_array_equal(schema["colDefs"]['comment'], ['', '', ''])
        s.close()

    def test_readme_BasicOperation_AutoFitTableAppender_TableAppender_2(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session(protocol=keys.PROTOCOL_DDB)
        s.connect(HOST, PORT, USER, PASSWD)
        s.run(f"share table(1000:0, `sym`uuid`int128`ipaddr`blob, [SYMBOL, UUID, INT128, IPADDR, BLOB]) as {func_name}")
        appender = ddb.TableAppender(tableName=func_name, ddbSession=s)
        data = pd.DataFrame({
            'sym': ["A1", "A2", "A3"],
            'uuid': ["5d212a78-cc48-e3b1-4235-b4d91473ee87", "b93b8253-8d5e-c609-260a-86522b99864e",
                     "00000000-0000-0000-0000-000000000000"],
            'int128': ['00000000000000000000000000000000', "073dc3bc505dd1643d11a4ac4271d2f2",
                       "e60c84f21b6149959bcf0bd6b509ff6a"],
            'ipaddr': ["2c24:d056:2f77:62c0:c48d:6782:e50:6ad2", "0.0.0.0", "192.168.1.0"],
            'blob': [b"testBLOB1", b"testBLOB2", b"testBLOB3"],
        })
        appender.append(data)
        t = s.run(func_name)
        assert_frame_equal(t, data)
        s.close()

    def test_readme_BasicOperation_AutoFitTableAppender_TableUpsert_1(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        script_KEYEDTABLE = f"""
            testtable=keyedTable(`id,1000:0,`date`text`id,[DATETIME,STRING,LONG])
            share testtable as {func_name}
        """
        s.run(script_KEYEDTABLE)
        upserter = ddb.TableUpserter(tableName=func_name, ddbSession=s)
        dates = []
        texts = []
        ids = []
        for i in range(1000):
            dates.append(np.datetime64('2012-06-13 13:30:10.008', 's'))
            texts.append(f"test_i_{i}")
            ids.append(i % 10)
        df = pd.DataFrame({
            'date': dates,
            'text': texts,
            'id': ids,
        })
        upserter.upsert(df)
        keyed_t = s.run(func_name)
        assert_frame_equal(df.iloc[990:].reset_index(drop=True), keyed_t)
        s.close()

    def test_readme_BasicOperation_AutoFitTableAppender_TableUpsert_2(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        script_DFS_VALUE = f"""
            if(existsDatabase('{db_name}')){{
                dropDatabase('{db_name}')
            }}
            db = database("{db_name}", VALUE, 0..9)
            t = table(1000:0, `date`text`id`flag, [DATETIME, STRING, LONG, INT])
            p_table = db.createPartitionedTable(t, `pt, `flag)
        """
        s.run(script_DFS_VALUE)
        upserter = ddb.TableUpserter(dbPath=db_name, tableName="pt", ddbSession=s, keyColNames=["id"])
        for i in range(10):
            dates = [np.datetime64(datetime.datetime.now(), 's') for _ in range(100)]
            texts = [f"test_{i}_{_}" for _ in range(100)]
            ids = [_ % 10 for _ in range(100)]
            flags = [_ % 10 for _ in range(100)]
            df = pd.DataFrame({
                'date': dates,
                'text': texts,
                'id': ids,
                'flag': flags,
            })
            upserter.upsert(df)
        p_table = s.run("select * from p_table order by text")
        assert_array_equal(p_table['text'].to_list(),
                           [f"test_0_{_}" for _ in range(10, 100)] + [f"test_9_{_}" for _ in range(90, 100)])

    def test_readme_BasicOperation_AutoFitTableAppender_PartitionedTableAppender(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        script = f"""
            dbPath = "{db_name}"
            if(existsDatabase(dbPath)){{
                dropDatabase(dbPath)
            }}
            t = table(100:0, `id`date`vol, [SYMBOL, DATE, LONG])
            db = database(dbPath, VALUE, `APPL`IBM`AMZN)
            pt = db.createPartitionedTable(t, `pt, `id)
        """
        s.run(script)
        pool = ddb.DBConnectionPool(HOST, PORT, 3, USER, PASSWD)
        appender = ddb.PartitionedTableAppender(dbPath=db_name, tableName="pt", partitionColName="id",
                                                dbConnectionPool=pool)
        n = 100
        dates = []
        for i in range(n):
            dates.append(
                np.datetime64(f"201{random.randint(0, 9):d}-0{random.randint(1, 9):1d}-{random.randint(10, 28):2d}"))
        data = pd.DataFrame({
            "id": np.random.choice(['AMZN', 'IBM', 'APPL'], n),
            "date": dates,
            "vol": pd.Series(np.random.randint(100, size=n), dtype='int64')
        })
        re = appender.append(data)
        assert re == 100
        res = s.run(f"pt = loadTable('{db_name}', 'pt'); select * from pt order by date,id;")
        assert_frame_equal(res, data.sort_values(by=['date', 'id'], ascending=True).reset_index(drop=True))

    def test_readme_BasicOperation_StreamingSubscription(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0, 1..10)
        """
        s = ddb.Session(HOST, PORT, USER, PASSWD)
        s.run(script)
        s.enableStreaming(0)
        msg_count = MsgCount()

        def tmp_handler(mc: MsgCount):
            def handler(lst):
                mc.setMsg()

            return handler

        s.subscribe(HOST, PORT, tmp_handler(msg_count), func_name, "action", offset=-1, filter=np.array(["000905"]),
                    userName=USER, password=PASSWD)
        s.run(
            f"insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0, 1..10)")
        Event().wait(timeout=5)
        assert msg_count.getTotalMsg() == 2
        s.unsubscribe(HOST, PORT, func_name, "action")

    def test_readme_BasicOperation_AsynchronousWrite_SessionAsynchronousSubmission_1(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session(enableASYNC=True)
        s.connect(HOST, PORT, USER, PASSWD)
        tbName = "tb1"
        script = f"""
            dbPath="{db_name}"
            tbName=`tb1
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath, VALUE, ["AAPL", "AMZN", "A"])
            testDictSchema=table(5:0, `id`ticker`price, [INT,SYMBOL,DOUBLE])
            tb1=db.createPartitionedTable(testDictSchema, tbName, `ticker)
        """
        s.run(script)
        tb = pd.DataFrame({
            'id': np.array([1, 2, 2, 3], dtype="int32"),
            'ticker': ['AAPL', 'AMZN', 'AMZN', 'A'],
            'price': [22, 3.5, 21, 26],
        })
        time.sleep(2)
        s.run(f"append!{{loadTable('{db_name}', `{tbName})}}", tb)
        time.sleep(3)
        s.close()
        s = ddb.Session(enableASYNC=False)
        s.connect(HOST, PORT, USER, PASSWD)
        res = s.run(f"select * from loadTable('{db_name}', `{tbName}) order by price")
        assert_frame_equal(tb.sort_values(by='price').reset_index(drop=True), res, False)
        s.close()

    def test_readme_BasicOperation_AsynchronousWrite_SessionAsynchronousSubmission_2(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session(enableASYNC=True)
        s.connect(HOST, PORT, USER, PASSWD)
        n = 100
        script = f"share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as `{func_name}"
        s.run(script)
        time.sleep(10)
        time_list = [np.datetime64(datetime.date(2020, random.randint(
            1, 12), random.randint(1, 20)), 'ms') for _ in range(n)]
        sym_list = np.random.choice(['IBN', 'GTYU', 'FHU', 'DGT', 'FHU', 'YUG', 'EE', 'ZD', 'FYU'], n)
        price_list = [round(np.random.uniform(1, 100), 1) for _ in range(n)]
        id_list = np.random.choice([1, 2, 3, 4, 5], n)
        tb = pd.DataFrame({
            'time': time_list,
            'sym': sym_list,
            'price': price_list,
            'id': id_list,
        })
        for _ in range(1000):
            s.run(f"tableInsert{{{func_name}}}", tb)
        time.sleep(10)
        s.close()
        s = ddb.Session(enableASYNC=False)
        s.connect(HOST, PORT, USER, PASSWD)
        res = s.run(f"exec count(*) from {func_name} order by price")
        assert res == 1000 * 100
        s.close()

    def test_readme_BasicOperation_AsynchronousWrite_SessionAsynchronousSubmission_3(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session(enableASYNC=True)
        s.connect(HOST, PORT, USER, PASSWD)
        s.run(f"""
            dropFunctionView(`appendStreamingData)
            go
            share streamTable(10000:0,`time`sym`price`id, [DATE,SYMBOL,DOUBLE,INT]) as {func_name}
            def appendStreamingData(mutable data){{
                tableInsert({func_name}, data.replaceColumn!(`time, date(data.time)))
            }}
            addFunctionView(appendStreamingData)
        """)
        time.sleep(2)
        n = 10
        time_list = [np.datetime64(datetime.date(2020, random.randint(1, 12), random.randint(1, 20))) for _ in range(n)]
        sym_list = np.random.choice(['IBN', 'GTYU', 'FHU', 'DGT', 'FHU', 'YUG', 'EE', 'ZD', 'FYU'], n)
        price_list = [round(np.random.uniform(1, 100), 1) for _ in range(n)]
        id_list = np.random.choice([1, 2, 3, 4, 5], n)
        tb = pd.DataFrame({
            'time': time_list,
            'sym': sym_list,
            'price': price_list,
            'id': id_list,
        })
        for _ in range(10):
            s.run("appendStreamingData", tb)
        s.close()
        s = ddb.Session(enableASYNC=False)
        s.connect(HOST, PORT, USER, PASSWD)
        res = s.run(f"exec count(*) from {func_name} order by price")
        ind = 0
        while res != 10 * 10 and ind < 20:
            res = s.run(f"exec count(*) from {func_name} order by price")
            time.sleep(1)
            ind += 1
        assert ind < 20
        s.close()

    def test_readme_BasicOperation_AsynchronousWrite_MTW_1(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        script = f"""
            t = table(1000:0, `date`ticker`price, [DATE,SYMBOL,LONG]);
            share t as {func_name}
        """
        s.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, dbPath="", tableName=func_name,
            batchSize=10, throttle=1, threadCount=5, partitionCol="date"
        )
        fail_rows = 0
        for i in range(10):
            if i == 3:
                res = writer.insert(np.datetime64(
                    f'2022-03-2{i % 6}'), random.randint(1, 10000))
            else:
                res = writer.insert(np.datetime64(
                    f'2022-03-2{i % 6}'), "AAAA", random.randint(1, 10000))
            if res.hasError():
                fail_rows += 1
        writer.waitForThreadCompletion()
        success_rows = s.run(f"exec count(*) from {func_name}")
        assert fail_rows == 1
        assert success_rows == 10 - fail_rows

    def test_readme_BasicOperation_AsynchronousWrite_MTW_2(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        script = f"""
            dbName = '{db_name}';
            if(exists(dbName)){{
                dropDatabase(dbName);
            }}
            datetest=table(1000:0,`date`symbol`id,[DATE,SYMBOL,LONG]);
            db = database(directory=dbName, partitionType=HASH, partitionScheme=[INT, 10]);
            pt=db.createPartitionedTable(datetest,'pdatetest','id');
        """
        s.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, dbPath=db_name, tableName="pdatetest",
            batchSize=10000, throttle=1, threadCount=5, partitionCol="id", compressMethods=["LZ4", "LZ4", "DELTA"]
        )
        try:
            for i in range(100):
                res = writer.insert(random.randint(
                    1, 10000), "AAAAAAAB", random.randint(1, 10000))
        except Exception as ex:
            pass
        writer.waitForThreadCompletion()
        writeStatus = writer.getStatus()
        assert writeStatus.sentRows == 100
        assert writeStatus.sendFailedRows == 0
        assert s.run("exec count(*) from pt") == 100
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, dbPath=db_name, tableName="pdatetest",
            batchSize=10000, throttle=1, threadCount=5, partitionCol="id", compressMethods=["LZ4", "LZ4", "DELTA"]
        )
        try:
            for i in range(100):
                writer.insert(np.datetime64('2022-03-23'), "AAAAAAAB", random.randint(1, 10000))
            for i in range(10):
                res = writer.insert(np.datetime64('2022-03-23'), 222, random.randint(1, 10000))
            res = writer.insert(np.datetime64('2022-03-23'), "AAAAAAAB")
            time.sleep(1)
            writer.insert(np.datetime64('2022-03-23'),
                          "AAAAAAAB", random.randint(1, 10000))
        except Exception as ex:
            pass
        writer.waitForThreadCompletion()
        writeStatus = writer.getStatus()
        assert writeStatus.sentRows == 0
        assert writeStatus.sendFailedRows + writeStatus.unsentRows == 110
        assert s.run("exec count(*) from pt") == 100
        if writeStatus.hasError():
            unwrittendata = writer.getUnwrittenData()
            newwriter = ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, dbPath=db_name, tableName="pdatetest",
                batchSize=10000, throttle=1, threadCount=5, partitionCol="id", compressMethods=["LZ4", "LZ4", "DELTA"]
            )
            try:
                for row in unwrittendata:
                    row[1] = "aaaaa"
                res = newwriter.insertUnwrittenData(unwrittendata)
            except Exception as ex:
                pass
            finally:
                newwriter.waitForThreadCompletion()
                writeStatus = newwriter.getStatus()
        assert s.run("exec count(*) from pt") == 210
        s.close()

    def test_readme_BasicOperation_AsynchronousWrite_MTW_3(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        script = f"""
            dbName = '{db_name}';
            if(exists(dbName)){{
                dropDatabase(dbName);
            }}
            datetest=table(1000:0,`date`symbol`id,[DATE,SYMBOL,LONG]);
            db = database(directory=dbName, partitionType=HASH, partitionScheme=[INT, 10]);
            pt=db.createPartitionedTable(datetest,'pdatetest','id');
        """
        s.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, dbPath=db_name, tableName="pdatetest",
            batchSize=10000, throttle=1, threadCount=5, partitionCol="id", compressMethods=["LZ4", "LZ4", "DELTA"]
        )

        def insert_MTW(writer):
            try:
                for i in range(100):
                    writer.insert(random.randint(
                        1, 10000), "AAAAAAAB", random.randint(1, 10000))
            except Exception as ex:
                pass

        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=insert_MTW, args=(writer,)))
        for thread in threads:
            thread.start()
        time.sleep(10)
        for thread in threads:
            thread.join()
        writer.waitForThreadCompletion()
        writeStatus = writer.getStatus()
        assert writeStatus.sentRows == 1000
        assert s.run("exec count(*) from pt") == 1000

    @pytest.mark.parametrize('partitioned', [True, False])
    def test_readme_BasicOperation_AsynchronousWrite_BTW(self, partitioned):
        func_name = inspect.currentframe().f_code.co_name + f"partitioned_{partitioned}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        script = f"""
            t = table(1000:0,`id`date`ticker`price, [INT,DATE,SYMBOL,DOUBLE]);
            share t as {func_name}
        """
        s.run(script)
        writer = ddb.BatchTableWriter(HOST, PORT, USER, PASSWD)
        writer.addTable(tableName=func_name, partitioned=partitioned)
        writer.insert("", func_name, 1, np.datetime64("2019-01-01"), 'AAPL', 5.6)
        writer.insert("", func_name, 2, np.datetime64("2019-01-01"), 'GOOG', 8.3)
        writer.insert("", func_name, 3, np.datetime64("2019-01-02"), 'GOOG', 4.2)
        writer.insert("", func_name, 4, np.datetime64("2019-01-03"), 'AMZN', 1.4)
        writer.insert("", func_name, 5, np.datetime64("2019-01-05"), 'AAPL', 6.9)
        writer.getUnwrittenData(dbPath="", tableName=func_name)
        writer.getStatus(tableName=func_name)
        writer.getAllStatus()
        writer.removeTable()
        s.run(f"select * from {func_name}")

    def test_readme_AdvancedOperation_TypeConversion_DDB(self):
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        df1 = pd.DataFrame({
            'int_v': [1, 2, 3],
            'long_v': np.array([None, 3, np.int64(3)], dtype="object"),
            'float_v': np.array([np.nan, 1.2, 3.3], dtype="float32")
        })
        s.upload({'df1': df1})
        ex1 = pd.DataFrame({
            'name': ['int_v', 'long_v', 'float_v'],
            'typeString': ['LONG', 'LONG', 'FLOAT'],
            'typeInt': np.array([5, 5, 15], dtype='int32'),
            'extra': np.array(['NaN', 'NaN', 'NaN'], dtype='float64'),
            'comment': ['', '', '']
        })
        assert_frame_equal(s.run("schema(df1)")['colDefs'], ex1)
        df2 = pd.DataFrame({
            'day_v': [np.datetime64("2012-01-02", 'D'), np.datetime64("2022-02-05", 'D')],
            'month_v': [np.datetime64("2012-01", "M"), pd.NaT],
        }, dtype='object')
        s.upload({'df2': df2})
        ex2 = pd.DataFrame({
            'name': ['day_v', 'month_v'],
            'typeString': ['DATE', 'MONTH'],
            'typeInt': np.array([6, 7], dtype='int32'),
            'extra': np.array(['NaN', 'NaN'], dtype='float64'),
            'comment': ['', '']
        })
        assert_frame_equal(s.run("schema(df2)")['colDefs'], ex2)
        df2.__DolphinDB_Type__ = {
            "day_v": keys.DT_DATE,
            "month_v": keys.DT_MONTH,
        }
        s.upload({'df2': df2})
        ex2_2 = pd.DataFrame({
            'name': ['day_v', 'month_v'],
            'typeString': ['DATE', 'MONTH'],
            'typeInt': np.array([6, 7], dtype='int32'),
            'extra': np.array(['NaN', 'NaN'], dtype='float64'),
            'comment': ['', '']
        })
        assert_frame_equal(s.run("schema(df2)")['colDefs'], ex2_2)
        df3 = pd.DataFrame({
            'long_av': [[1, None], [3]],
            'double_av': np.array([[1.1], [np.nan, 3.3]], dtype="object")
        })
        s.upload({'df3': df3})
        ex3 = pd.DataFrame({
            'name': ['long_av', 'double_av'],
            'typeString': ['LONG[]', 'DOUBLE[]'],
            'typeInt': np.array([69, 80], dtype='int32'),
            'extra': np.array(['NaN', 'NaN'], dtype='float64'),
            'comment': ['', '']
        })
        assert_frame_equal(s.run("schema(df3)")['colDefs'], ex3)

    def test_readme_AdvancedOperation_TypeConversion_ForcedTypeConversion(self):
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        df = pd.DataFrame({
            'cint': [1, 2, 3],
            'csymbol': ["aaa", "bbb", "aaa"],
            'cblob': ["a1", "a2", "a3"],
        })
        s.upload({"df_wrong": df})
        ex1 = pd.DataFrame({
            'name': ['cint', 'csymbol', 'cblob'],
            'typeString': ['LONG', 'STRING', 'STRING'],
            'typeInt': np.array([5, 18, 18], dtype='int32'),
            'extra': np.array(['NaN', 'NaN', 'NaN'], dtype='float64'),
            'comment': ['', '', '']
        })
        assert_frame_equal(s.run("schema(df_wrong)")['colDefs'], ex1)
        df.__DolphinDB_Type__ = {
            'cint': keys.DT_INT,
            'csymbol': keys.DT_SYMBOL,
            'cblob': keys.DT_BLOB,
        }
        s.upload({"df_true": df})
        ex2 = pd.DataFrame({
            'name': ['cint', 'csymbol', 'cblob'],
            'typeString': ['INT', 'SYMBOL', 'BLOB'],
            'typeInt': np.array([4, 17, 32], dtype='int32'),
            'extra': np.array(['NaN', 'NaN', 'NaN'], dtype='float64'),
            'comment': ['', '', '']
        })
        assert_frame_equal(s.run("schema(df_true)")['colDefs'], ex2)

    def test_readme_AdvancedOperation_StreamingSubscription_1(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        s.run(f"share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as `{func_name}")
        s.enableStreaming()
        msg_count = MsgCount()

        def tmp_handler(mc: MsgCount):
            def handler(lst):
                mc.setMsg()

            return handler

        s.subscribe(HOST, PORT, tmp_handler(msg_count), func_name, "SingleMode", offset=-1, userName=USER,
                    password=PASSWD)
        s.run(
            f"insert into {func_name} values(take(now(), 6), take(`000905`600001`300201`000908`600002, 6), rand(1000,6)/10.0, 1..6)")
        time.sleep(5)
        s.unsubscribe(HOST, PORT, func_name, "SingleMode")
        assert msg_count.getTotalMsg() == 6
        s.close()

    def test_readme_AdvancedOperation_StreamingSubscription_2(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        s.run(f"share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as `{func_name}")
        s.enableStreaming()
        msg_count = MsgCount()

        def tmp_handler(mc: MsgCount):
            def handler(lst):
                for _ in range(len(lst)):
                    mc.setMsg()

            return handler

        s.subscribe(HOST, PORT, tmp_handler(msg_count), func_name, "MultiMode1",
                    offset=-1, batchSize=2, throttle=0.1, msgAsTable=False, userName=USER, password=PASSWD)
        s.run(
            f"insert into {func_name} values(take(now(), 6), take(`000905`600001`300201`000908`600002, 6), rand(1000,6)/10.0, 1..6)")
        time.sleep(5)
        s.unsubscribe(HOST, PORT, func_name, "MultiMode1")
        assert msg_count.getTotalMsg() == 6
        s.close()

    def test_readme_AdvancedOperation_StreamingSubscription_3(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        s.run(f"share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as `{func_name}")
        s.enableStreaming()
        msg_count = MsgCount()

        def tmp_handler(mc: MsgCount):
            def handler(lst):
                for _ in range(lst.shape[0]):
                    mc.setMsg()

            return handler

        s.subscribe(HOST, PORT, tmp_handler(msg_count), func_name, "MultiMode2",
                    offset=-1, batchSize=1000, throttle=0.1, msgAsTable=True, userName=USER, password=PASSWD)
        s.run(
            f"n=1500;insert into {func_name} values(take(now(), n), take(`000905`600001`300201`000908`600002, n), rand(1000,n)/10.0, 1..n)")
        time.sleep(5)
        s.unsubscribe(HOST, PORT, func_name, "MultiMode2")
        assert msg_count.getTotalMsg() == 1500
        s.close()

    def test_readme_AdvancedOperation_StreamingSubscription_4(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        s.run(f"""
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as `{func_name}
            n = 6;
            dbName = '{db_name}'
            if(existsDatabase(dbName)){{
                dropDB(dbName)}}
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
            pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1)
            pt2 = db.createPartitionedTable(table2,'pt2',`datetimev).append!(table2)
            re1 = replayDS(sqlObj=<select * from pt1>, dateColumn=`datetimev, timeColumn=`timestampv)
            re2 = replayDS(sqlObj=<select * from pt2>, dateColumn=`datetimev, timeColumn=`timestampv)
            d = dict(['msg1', 'msg2'], [re1, re2])
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv)
        """)
        msg1_count = MsgCount()
        msg2_count = MsgCount()

        def tmp_handler(mc1: MsgCount, mc2: MsgCount):
            def streamDeserializer_handler(lst):
                if lst[-1] == "msg1":
                    mc1.setMsg()
                elif lst[-1] == 'msg2':
                    mc2.setMsg()

            return streamDeserializer_handler

        s.enableStreaming()
        sd = ddb.streamDeserializer({
            'msg1': [db_name, "pt1"],
            'msg2': [db_name, "pt2"],
        }, session=s)
        s.subscribe(HOST, PORT, handler=tmp_handler(msg1_count, msg2_count), tableName=func_name, actionName="action",
                    offset=0, resub=False, msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        Event().wait(5)
        assert msg1_count.getTotalMsg() == 6
        assert msg2_count.getTotalMsg() == 6
        s.unsubscribe(HOST, PORT, func_name, 'action')
        s.close()

    def test_readme_AdvancedOperation_StreamingSubscription_5(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        s.run(f"""
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as `{func_name}
            n = 6;
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
            share table1 as {func_name}_pt1
            share table2 as {func_name}_pt2
            d = dict(['msg1', 'msg2'], [{func_name}_pt1, {func_name}_pt2])
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv)        
        """)
        msg1_count = MsgCount()
        msg2_count = MsgCount()

        def tmp_handler(mc1: MsgCount, mc2: MsgCount):
            def streamDeserializer_handler(msgs):
                for msg in msgs:
                    if msg[-1] == "msg1":
                        mc1.setMsg()
                    elif msg[-1] == 'msg2':
                        mc2.setMsg()

            return streamDeserializer_handler

        s.enableStreaming()
        sd = ddb.streamDeserializer({
            'msg1': f"{func_name}_pt1",
            'msg2': f"{func_name}_pt2",
        }, session=s)
        s.subscribe(HOST, PORT, handler=tmp_handler(msg1_count, msg2_count), tableName=func_name, actionName="action",
                    offset=0, resub=False, batchSize=4,
                    msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        Event().wait(5)
        assert msg1_count.getTotalMsg() == 6
        assert msg2_count.getTotalMsg() == 6
        s.unsubscribe(HOST, PORT, func_name, 'action')
        s.close()

    def test_readme_AdvancedOperation_ObjectorientedOperationOnDatabase_Table_1(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        data1 = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6],
        })
        t1 = s.table(data=data1)
        assert t1.tableName().startswith('TMP_TBL')
        assert isinstance(t1, ddb.Table)
        data2 = {
            'a': ['a', 'b', 'c'],
            'b': [1, 2, 3],
        }
        t2 = s.table(data=data2)
        assert t2.tableName().startswith('TMP_TBL')
        assert isinstance(t2, ddb.Table)
        dbPath = f"dfs://{func_name}_1"
        if s.existsDatabase(dbPath):
            s.dropDatabase(dbPath)
        db = s.database(partitionType=keys.VALUE, partitions=[
            1, 2, 3], dbPath=dbPath, engine="TSDB")
        s.run("schema_t = table(100:0, `ctime`csymbol`price`qty, [TIMESTAMP, SYMBOL, DOUBLE, INT])")
        schema_t = s.table(data="schema_t")
        db.createTable(schema_t, "t", ["csymbol"])
        pt = s.table(dbPath=dbPath, data="t", tableAliasName='pt')
        assert pt.tableName() == 'pt'
        assert isinstance(pt, ddb.Table)
        assert pt.toDF().empty
        s.run("test_t = table(100:0, `ctime`csymbol`price`qty, [TIMESTAMP, SYMBOL, DOUBLE, INT])")
        t = s.table(data="test_t")
        assert t.tableName() == 'test_t'
        assert isinstance(t, ddb.Table)
        assert t.toDF().empty
        data1 = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6],
        })
        t1 = s.table(data=data1, tableAliasName="data1")
        assert t1.tableName() == 'data1'
        assert isinstance(t1, ddb.Table)
        data2 = {
            'a': ['a', 'b', 'c'],
            'b': [1, 2, 3],
        }
        t2 = s.table(data=data2, tableAliasName="data2")
        assert t2.tableName() == 'data2'
        assert isinstance(t2, ddb.Table)
        dbPath = f"dfs://{func_name}_2"
        if s.existsDatabase(dbPath):
            s.dropDatabase(dbPath)
        db = s.database(partitionType=keys.VALUE, partitions=[
            1, 2, 3], dbPath=dbPath, engine="TSDB")
        s.run("schema_t = table(100:0, `ctime`csymbol`price`qty, [TIMESTAMP, SYMBOL, DOUBLE, INT])")
        schema_t = s.table(data="schema_t")
        db.createTable(schema_t, "pt", ["csymbol"])
        pt = s.table(dbPath=dbPath, data="pt", tableAliasName="tmp_pt")
        assert pt.tableName() == 'tmp_pt'
        assert pt.toDF().empty
        s.run(
            "test_t = table(100:0, `ctime`csymbol`price`qty, [TIMESTAMP, SYMBOL, DOUBLE, INT])")
        t = s.table(data="test_t", tableAliasName="test_t2")
        assert t.tableName() == 'test_t'
        assert t.toDF().empty
        s.close()

    def test_readme_AdvancedOperation_ObjectorientedOperationOnDatabase_Table_2(self):
        func_name = inspect.currentframe().f_code.co_name
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        s.run("test_t = table(100:0, `ctime`csymbol`price`qty, [TIMESTAMP, SYMBOL, DOUBLE, INT])")
        t = s.loadTable("test_t")
        assert t.tableName() == 'test_t'
        assert t.toDF().empty
        dbPath = f"dfs://{func_name}"
        if s.existsDatabase(dbPath):
            s.dropDatabase(dbPath)
        db = s.database(partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=dbPath, engine="TSDB")
        s.run("schema_t = table(100:0, `ctime`csymbol`price`qty, [TIMESTAMP, SYMBOL, DOUBLE, INT])")
        schema_t = s.table(data="schema_t")
        db.createTable(schema_t, "pt", ["csymbol"])
        pt = s.loadTable("pt", dbPath=dbPath)
        assert pt.tableName().startswith('pt_TMP_TBL')
        assert pt.toDF().empty

    def test_readme_AdvancedOperation_ObjectorientedOperationOnDatabase_Table_3(self):
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        trade = s.loadText(DATA_DIR + "example.csv")
        trade.update(["VOL"], ["999999"]).where("TICKER=`AMZN").where(["date=2015.12.16"])
        t2 = trade.where("ticker=`AMZN").where("date=2015.12.16")
        assert t2.toDF()['VOL'][0] == 3964470
        trade.update(["VOL"], ["999999"]).where("TICKER=`AMZN").where(["date=2015.12.16"]).execute()
        assert t2.toDF()['VOL'][0] == 999999
        assert trade.rows == 13136
        trade.delete().where('date<2013.01.01').execute()
        assert trade.rows == 3024
        s.close()

    def test_readme_AdvancedOperation_ObjectorientedOperationOnDatabase_Table_4(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        if s.existsDatabase(db_name):
            s.dropDatabase(db_name)
        s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN", "NFLX", "NVDA"], dbPath=db_name)
        trade = s.loadTextEx(dbPath=db_name, partitionColumns=["TICKER"], tableName='trade',
                             remoteFilePath=DATA_DIR + "example.csv")
        res1 = trade.select(['sum(vol)', 'sum(prc)']).groupby(['ticker']).toDF()
        ex1 = s.run(
            f"select sum(vol),sum(prc) from (select TICKER,date,VOL,PRC,BID,ASK from loadTable('{db_name}', `trade)) group by ticker")
        assert_frame_equal(res1, ex1)
        res3 = trade.contextby('ticker').top(3).toDF()
        ex3 = s.run(f"select * from loadTable('{db_name}', `trade) context by ticker limit 3")
        assert_frame_equal(res3, ex3)
        res4 = trade.select("TICKER, month(date) as month, cumsum(VOL)").contextby("TICKER,month(date)").toDF()
        ex4 = s.run(
            f"select TICKER, month(date) as month, cumsum(VOL) from loadTable('{db_name}', `trade) context by TICKER,month(date)")
        assert_frame_equal(res4, ex4)
        res5 = trade.contextby('ticker').having("sum(VOL)>40000000000").toDF()
        ex5 = s.run(f"select * from loadTable('{db_name}', `trade) context by ticker having sum(VOL)>40000000000")
        assert_frame_equal(res5, ex5)
        s.close()

    def test_readme_AdvancedOperation_ObjectorientedOperationOnDatabase_Table_5(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        if s.existsDatabase(db_name):
            s.dropDatabase(db_name)
        s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN", "NFLX", "NVDA"], dbPath=db_name)
        trade = s.loadTextEx(dbPath=db_name, partitionColumns=["TICKER"], tableName='trade',
                             remoteFilePath=DATA_DIR + "example.csv")
        res1 = trade.select("VOL").pivotby("TICKER", "date").toDF()
        ex1 = s.run(
            f"select VOL from (select TICKER,date,VOL,PRC,BID,ASK from loadTable('{db_name}', `trade)) pivot by TICKER,date")
        assert_frame_equal(res1, ex1)
        res2 = trade.exec("VOL").pivotby("TICKER", "date").toDF()
        ex2 = s.run(f"exec VOL from loadTable('{db_name}', `trade) pivot by TICKER, date")
        for i in range(len(res2)):
            assert_array_equal(res2[i], ex2[i])
        trade = s.loadTable("trade", db_name)
        res3 = trade.contextby('ticker').csort('date desc').toDF()
        ex3 = s.run(f"select * from loadTable('{db_name}', `trade) context by ticker csort date desc")
        assert_frame_equal(res3, ex3)
        res4 = trade.select("*").contextby('ticker').csort(["TICKER", "VOL"], True).limit(5).toDF()
        ex4 = s.run(f"select * from loadTable('{db_name}', `trade) context by ticker csort TICKER,VOL asc limit 5")
        assert_frame_equal(res4, ex4)
        res5 = trade.select("*").contextby('ticker').csort(["TICKER", "VOL"], [True, False]).limit(5).toDF()
        ex5 = s.run(f"select * from loadTable('{db_name}', `trade) context by ticker csort TICKER,VOL desc limit 5")
        assert_frame_equal(res5, ex5)
        s.close()

    def test_readme_AdvancedOperation_ObjectorientedOperationOnDatabase_Table_6(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        if s.existsDatabase(db_name):
            s.dropDatabase(db_name)
        s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN", "NFLX", "NVDA"], dbPath=db_name)
        trade = s.loadTextEx(dbPath=db_name, partitionColumns=["TICKER"], tableName='trade',
                             remoteFilePath=DATA_DIR + "example.csv")
        res1 = trade.top(5).toDF()
        ex1 = s.run(f"select top 5 * from loadTable('{db_name}', `trade)")
        assert_frame_equal(res1, ex1)
        res2 = trade.select("*").contextby('ticker').limit(-2).toDF()
        ex2 = s.run(
            f"select * from (select TICKER,date,VOL,PRC,BID,ASK from loadTable('{db_name}', `trade)) context by ticker limit -2")
        assert_frame_equal(res2, ex2)
        res3 = trade.select("*").limit([2, 5]).toDF()
        ex3 = s.run(f"select * from loadTable('{db_name}', `trade) limit 2,5")
        assert_frame_equal(res3, ex3)
        s.close()

    def test_readme_AdvancedOperation_ObjectorientedOperationOnDatabase_Table_7(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        if s.existsDatabase(db_name):
            s.dropDatabase(db_name)
        s.database(dbName='mydb', partitionType=keys.VALUE, partitions=["AMZN", "NFLX", "NVDA"], dbPath=db_name)
        trade = s.loadTextEx(dbPath=db_name, partitionColumns=["TICKER"], tableName='trade',
                             remoteFilePath=DATA_DIR + "example.csv")
        t1 = s.table(data={
            'TICKER': ['AMZN', 'AMZN', 'AMZN'],
            'date': np.array(['2015-12-31', '2015-12-30', '2015-12-29'], dtype='datetime64[D]'),
            'open': [695, 685, 674],
        })
        t1 = t1.select("TICKER, date(date) as date, open")
        res1 = trade.merge(t1, on=["TICKER", "date"]).toDF()
        s.upload({'t1': t1.toDF()})
        ex1 = s.run(
            f"select * from ej((select * from loadTable('{db_name}', `trade)) as t_left,(select TICKER, date(date) as date, open from (select * from t1)) as t_right,`TICKER`date,`TICKER`date)")
        assert_frame_equal(res1, ex1)
        t2 = t1.select("TICKER as TICKER1, date(date) as date1, open")
        s.upload({'t2': t2.toDF()})
        res2 = trade.merge(t2, left_on=["TICKER", "date"], right_on=["TICKER1", "date1"]).toDF()
        ex2 = s.run(
            f"select * from ej((select * from loadTable('{db_name}', `trade)) as left_t,(select TICKER as TICKER1, date(date) as date1, open from (select * from t1)) as right_t,`TICKER`date,`TICKER1`date1)")
        assert_frame_equal(res2, ex2)
        t3 = t1.select("TICKER, date(date) as date, open")
        s.upload({'t3': t3.toDF()})
        res3 = trade.merge(t3, how="left", on=["TICKER", "date"]).where('TICKER=`AMZN').where(
            '2015.12.23<=date<=2015.12.31').toDF()
        ex3 = s.run(
            f"select * from lj((select * from loadTable('{db_name}', `trade)) as left_t,(select TICKER, date(date) as date, open from (select TICKER, date(date) as date, open from (select * from t1))) as right_t,`TICKER`date,`TICKER`date) where (TICKER=`AMZN) and (2015.12.23<=date<=2015.12.31)")
        assert_frame_equal(res3, ex3)
        t4_1 = s.table(data={'TICKER': ['AMZN', 'AMZN', 'NFLX'], 'date': ['2015.12.29', '2015.12.30', '2015.12.31'],
                             'open': [674, 685, 942]})
        t4_2 = s.table(data={'TICKER': ['AMZN', 'NFLX', 'NFLX'], 'date': ['2015.12.29', '2015.12.30', '2015.12.31'],
                             'close': [690, 936, 951]})
        s.upload({'t4_1': t4_1.toDF(), 't4_2': t4_2.toDF()})
        res4 = t4_1.merge(t4_2, how="outer", on=["TICKER", "date"]).toDF()
        ex4 = s.run(
            "select * from fj((select * from t4_1) as lt,(select TICKER,date,close from t4_2) as rt,`TICKER`date,`TICKER`date)")
        for i in range(ex4.shape[1]):
            assert_array_equal(res4.iloc[:, i].to_list(), ex4.iloc[:, i].to_list())
        s.close()

    def test_readme_AdvancedOperation_ObjectorientedOperationOnDatabase_Table_8(self):
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        if s.existsDatabase(db_name):
            s.dropDatabase(db_name)
        s.database(partitionType=keys.VALUE, partitions=["AAPL", "FB"], dbPath=db_name)
        trades = s.loadTextEx(db_name, tableName='trades', partitionColumns=["Symbol"],
                              remoteFilePath=DATA_DIR + "trades.csv")
        quotes = s.loadTextEx(db_name, tableName='quotes', partitionColumns=["Symbol"],
                              remoteFilePath=DATA_DIR + "quotes.csv")
        res1 = trades.top(5).toDF()
        ex1 = s.run(f"select top 5 * from loadTable('{db_name}', `trades)")
        assert_frame_equal(res1, ex1)
        res2 = quotes.where("second(Time)>=09:29:59").top(5).toDF()
        ex2 = s.run(f"select top 5 * from loadTable('{db_name}', `quotes) where second(Time)>=09:29:59")
        assert_frame_equal(res2, ex2)
        res3 = trades.merge_asof(quotes, on=["Symbol", "Time"]).select(
            ["Symbol", "Time", "Trade_Volume", "Trade_Price", "Bid_Price", "Bid_Size", "Offer_Price",
             "Offer_Size"]).top(5).toDF()
        ex3 = s.run(
            f"select top 5 Symbol,Time,Trade_Volume,Trade_Price,Bid_Price,Bid_Size,Offer_Price,Offer_Size from (select * from aj((select * from loadTable('{db_name}', `trades) as lt),(select * from loadTable('{db_name}', `quotes) as rt),`Symbol`Time,`Symbol`Time))")
        assert_frame_equal(res3, ex3)
        res4 = trades.merge_window(quotes, -5000000000, 0, aggFunctions=["avg(Bid_Price)", "avg(Offer_Price)"],
                                   on=["Symbol", "Time"]).where("Time>=07:59:59").top(10).toDF()
        ex4 = s.run(
            f"select top 10 * from wj((select * from loadTable('{db_name}', `trades)) as lt,(select * from loadTable('{db_name}', `quotes) as rt),-5000000000:0,<[avg(Bid_Price),avg(Offer_Price)]>,`Symbol`Time,`Symbol`Time) where (Time>=07:59:59)")
        assert_frame_equal(res4, ex4)
        s.close()

    def test_readme_AdvancedOperation_ObjectorientedOperationOnDatabase_Table_9(self):
        s = ddb.Session()
        s.connect(HOST, PORT, USER, PASSWD)
        s.run("""
            t1 = table(2010 2011 2012 as year);
            t2 = table(`IBM`C`AAPL as Ticker);
        """)
        t1 = s.table(data="t1")
        t2 = s.table(data="t2")
        res = t1.merge_cross(t2).toDF()
        ex = s.run("select * from cj((select year from t1) as lt,(select Ticker from t2) as rt)")
        assert_frame_equal(res, ex)
        s.close()
