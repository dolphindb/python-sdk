import asyncio
import inspect
import threading

import dolphindb as ddb

from setup.settings import HOST, PORT, USER, PASSWD


def insert_job(tablename, sleep_time, conn: ddb.Session = None):
    needClose = False
    if conn is None:
        conn = ddb.session(HOST, PORT, USER, PASSWD)
        needClose = True
    s = f"""
        tableInsert({tablename}, 1, `d, 500);
        sleep({sleep_time});
    """
    conn.run(s)
    if needClose:
        conn.close()
        assert conn.isClosed()


class TestConcurrent:
    conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=False)

    def test_concurrent_session_insert_datas(self):
        func_name = inspect.currentframe().f_code.co_name
        self.conn.run(f"share table(1:0, `c1`c2`c3, [INT, SYMBOL, TIMESTAMP]) as {func_name}_test_cur_share")
        thds = []
        for _ in range(100):
            thds.append(threading.Thread(target=insert_job, args=(f'{func_name}_test_cur_share', 500)))
        for thd in thds:
            thd.start()
        for thd in thds:
            thd.join()
        assert self.conn.run(f"exec count(*) from {func_name}_test_cur_share") == 100

    def test_concurrent_session_insert_datas_single(self):
        func_name = inspect.currentframe().f_code.co_name
        self.conn.run(f"share table(1:0, `c1`c2`c3, [INT, SYMBOL, TIMESTAMP]) as {func_name}_test_cur_share")
        thds = []
        for _ in range(100):
            thds.append(threading.Thread(target=insert_job, args=(f'{func_name}_test_cur_share', 500, self.conn)))
        for thd in thds:
            thd.start()
        for thd in thds:
            thd.join()
        assert self.conn.run(f"exec count(*) from {func_name}_test_cur_share") == 100

    def test_concurrent_DBConnectionPool_concurrent_insert_datas(self):
        dbpath = "dfs://test_concurrent"
        tab = 'pt'
        self.conn.run(f"""
            dbpath = "{dbpath}"
            if(existsDatabase(dbpath))
                dropDatabase(dbpath)
            t=table(1:0, `c1`c2`c3, [INT, SYMBOL, TIMESTAMP])
            db=database(dbpath, VALUE, 1..100, engine='TSDB')
            pt=db.createPartitionedTable(t,'pt','c1', sortColumns=`c1`c3, keepDuplicates=ALL)
        """)

        pool = ddb.DBConnectionPool(HOST, PORT, 8, USER, PASSWD, True)
        s = f"""
            t=table(rand(1..100,1) as c1, rand(`a`b`c`d`e`f`g, 1) as c2, rand(timestamp(10001..10100), 1) as c3)
            loadTable('{dbpath}', `{tab}).append!(t)
            go
            exec count(*) from loadTable('{dbpath}', `{tab})
        """

        async def insert_task():
            while True:
                try:
                    return await pool.run(s)
                except RuntimeError:
                    await asyncio.sleep(1)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*[insert_task() for _ in range(100)]))
        pool.shutDown()
        assert self.conn.run("exec count(*) from loadTable('dfs://test_concurrent', `pt)") == 100
