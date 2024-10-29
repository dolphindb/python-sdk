import asyncio
import threading

import dolphindb as ddb
import pytest

from setup.settings import *
from setup.utils import get_pid


def insert_job(tablename, sleep_time):
    conn = ddb.session(HOST, PORT, USER, PASSWD)
    s = f"""
        tableInsert({tablename}, 1, `d, 500);
        sleep({sleep_time});
        go
    """
    conn.run(s)
    print('now total rows: ', conn.run(f"exec count(*) from {tablename}"))
    conn.close()
    assert conn.isClosed()


class TestConcurrent:
    conn = ddb.session(enablePickle=False)  # pickle=True时，不支持decimal

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
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def test_session_concurrent_insert_datas(self):
        self.conn.run("share table(1:0, `c1`c2`c3, [INT, SYMBOL, TIMESTAMP]) as test_cur_share")

        thds = []
        for _ in range(100):
            thds.append(threading.Thread(target=insert_job, args=('test_cur_share', 500)))

        for thd in thds:
            thd.start()

        for thd in thds:
            thd.join()

        assert self.conn.run("exec count(*) from test_cur_share") == 100

    def test_connectionPool_concurrent_insert_datas(self):
        dbpath = "dfs://test_concurrent"
        tab = 'pt'
        self.conn.run(f"""
            dbpath = "{dbpath}"
            if(existsDatabase(dbpath))
                dropDatabase(dbpath)
            t=table(1:0, `c1`c2`c3, [INT, SYMBOL, TIMESTAMP])
            db=database(dbpath, VALUE, 1..100, engine='OLAP')
            pt=db.createPartitionedTable(t,'pt','c1')
        """)
        pool = ddb.DBConnectionPool(HOST, PORT, 10, USER, PASSWD, True, show_output=True)
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
                except RuntimeError as e:
                    await asyncio.sleep(1)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*[insert_task() for i in range(100)]))
        pool.shutDown()

        assert self.conn.run("exec count(*) from loadTable('dfs://test_concurrent', `pt)") == 100


if __name__ == '__main__':
    pytest.main(['-s', 'test/test_concurrent.py'])
