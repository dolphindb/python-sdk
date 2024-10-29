import pytest
from pandas.testing import *

from setup.prepare import *
from setup.settings import *
from setup.utils import get_pid


class TestPermission:
    conn = ddb.session()
    TEST_USER_NAMES = ['table_write_user', 'table_insert_user', 'table_update_user', 'db_write_user', 'db_insert_user',
                       'db_update_user', 'db_owner_user']
    TEST_USER_PERMISSIONS = ['TABLE_WRITE', 'TABLE_INSERT', 'TABLE_UPDATE', 'DB_WRITE', 'DB_INSERT', 'DB_UPDATE',
                             'DB_OWNER']
    TEST_DB = 'dfs://test_permission_db'
    TEST_TABLE = 'test_permission_table'

    def setup_method(self):
        try:
            self.conn.run("1")
        except RuntimeError:
            self.conn.connect(HOST, PORT, USER, PASSWD)
        if not self.conn.existsDatabase(self.TEST_DB):
            db = self.conn.database(dbName="db_value", partitionType=keys.VALUE, partitions=[1, 2, 3, 4],
                                    dbPath=self.TEST_DB, engine="OLAP")
            df = self.conn.run("table(1 2 3 as c1, `a`b`c as c2, 2023.07.01..2023.07.03 as c3)")
            t = self.conn.table(data=df)
            db.createPartitionedTable(t, self.TEST_TABLE, 'c1')
        self.conn.run(f"delete from loadTable('{self.TEST_DB}','{self.TEST_TABLE}')")
        activeUsers = self.conn.run("getUserList()")
        if not all(user in activeUsers for user in self.TEST_USER_NAMES):
            for user in self.TEST_USER_NAMES:
                self.conn.run(f"createUser('{user}', `{PASSWD});go")
        for i in range(len(self.TEST_USER_PERMISSIONS)):
            if 'table' in self.TEST_USER_NAMES[i]:
                s = f"""
                    grant("{self.TEST_USER_NAMES[i]}", DB_READ, '{self.TEST_DB}');go
                    grant("{self.TEST_USER_NAMES[i]}", {self.TEST_USER_PERMISSIONS[i]}, '{self.TEST_DB}/{self.TEST_TABLE}');go
                """
            elif 'db_owner' in self.TEST_USER_NAMES[i]:
                s = f"""
                    grant("{self.TEST_USER_NAMES[i]}", {self.TEST_USER_PERMISSIONS[i]}, "{self.TEST_DB}*");go
                    grant("{self.TEST_USER_NAMES[i]}", TABLE_READ, '{self.TEST_DB}/{self.TEST_TABLE}');go
                """
            else:
                s = f"""
                    grant("{self.TEST_USER_NAMES[i]}", {self.TEST_USER_PERMISSIONS[i]}, "{self.TEST_DB}");go
                    grant("{self.TEST_USER_NAMES[i]}", TABLE_READ, '{self.TEST_DB}/{self.TEST_TABLE}');go
                """
            self.conn.run(s)

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

    def test_tableAppender_permission_table_write(self):
        conn1 = ddb.session(HOST, PORT, "table_write_user", PASSWD)
        appender = ddb.tableAppender(self.TEST_DB, self.TEST_TABLE, conn1)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        assert appender.append(df) == 3
        res = conn1.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_tableAppender_permission_table_insert(self):
        conn1 = ddb.session(HOST, PORT, "table_insert_user", PASSWD)
        appender = ddb.tableAppender(self.TEST_DB, self.TEST_TABLE, conn1)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        assert appender.append(df) == 3
        res = conn1.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_tableAppender_permission_table_update(self):
        conn1 = ddb.session(HOST, PORT, "table_update_user", PASSWD)
        appender = ddb.tableAppender(self.TEST_DB, self.TEST_TABLE, conn1)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        with pytest.raises(RuntimeError, match="<NoPrivilege>Not granted to write data to table"):
            appender.append(df)

    def test_tableAppender_permission_db_write(self):
        conn1 = ddb.session(HOST, PORT, "db_write_user", PASSWD)
        appender = ddb.tableAppender(self.TEST_DB, self.TEST_TABLE, conn1)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        assert appender.append(df) == 3
        res = conn1.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_tableAppender_permission_db_insert(self):
        conn1 = ddb.session(HOST, PORT, "db_insert_user", PASSWD)
        appender = ddb.tableAppender(self.TEST_DB, self.TEST_TABLE, conn1)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        assert appender.append(df) == 3
        res = conn1.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_tableAppender_permission_db_update(self):
        conn1 = ddb.session(HOST, PORT, "db_update_user", PASSWD)
        appender = ddb.tableAppender(self.TEST_DB, self.TEST_TABLE, conn1)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        with pytest.raises(RuntimeError, match="<NoPrivilege>Not granted to write data to table"):
            appender.append(df)

    def test_tableAppender_permission_db_owner(self):
        conn1 = ddb.session(HOST, PORT, "db_owner_user", PASSWD)
        appender = ddb.tableAppender(self.TEST_DB, self.TEST_TABLE, conn1)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        with pytest.raises(RuntimeError, match="<NoPrivilege>Not granted to write data to table"):
            appender.append(df)

    def test_tableUpsert_permission_table_write(self):
        conn1 = ddb.session(HOST, PORT, "table_write_user", PASSWD)
        upserter = ddb.tableUpsert(self.TEST_DB, self.TEST_TABLE, conn1, keyColNames=['c1'])
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        upserter.upsert(df)
        res = conn1.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_tableUpsert_permission_table_insert(self):
        conn1 = ddb.session(HOST, PORT, "table_insert_user", PASSWD)
        upserter = ddb.tableUpsert(self.TEST_DB, self.TEST_TABLE, conn1, keyColNames=['c1'])
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        with pytest.raises(RuntimeError, match="<NoPrivilege>Not granted to write data to table"):
            upserter.upsert(df)

    def test_tableUpsert_permission_table_update(self):
        conn1 = ddb.session(HOST, PORT, "table_update_user", PASSWD)
        upserter = ddb.tableUpsert(self.TEST_DB, self.TEST_TABLE, conn1, keyColNames=['c1'])
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        upserter.upsert(df)
        res = conn1.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_tableUpsert_permission_db_write(self):
        conn1 = ddb.session(HOST, PORT, "db_write_user", PASSWD)
        upserter = ddb.tableUpsert(self.TEST_DB, self.TEST_TABLE, conn1, keyColNames=['c1'])
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        upserter.upsert(df)
        res = conn1.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_tableUpsert_permission_db_insert(self):
        conn1 = ddb.session(HOST, PORT, "db_insert_user", PASSWD)
        upserter = ddb.tableUpsert(self.TEST_DB, self.TEST_TABLE, conn1, keyColNames=['c1'])
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        with pytest.raises(RuntimeError, match="<NoPrivilege>Not granted to write data to table"):
            upserter.upsert(df)

    def test_tableUpsert_permission_db_update(self):
        conn1 = ddb.session(HOST, PORT, "db_update_user", PASSWD)
        upserter = ddb.tableUpsert(self.TEST_DB, self.TEST_TABLE, conn1, keyColNames=['c1'])
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        upserter.upsert(df)
        res = conn1.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_tableUpsert_permission_db_owner(self):
        conn1 = ddb.session(HOST, PORT, "db_owner_user", PASSWD)
        upserter = ddb.tableUpsert(self.TEST_DB, self.TEST_TABLE, conn1, keyColNames=['c1'])
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        with pytest.raises(RuntimeError, match="<NoPrivilege>Not granted to write data to table"):
            upserter.upsert(df)

    def test_PartitionedTableAppender_permission_table_write(self):
        connpool = ddb.DBConnectionPool(HOST, PORT, 3, "table_write_user", PASSWD)
        appender = ddb.PartitionedTableAppender(self.TEST_DB, self.TEST_TABLE, 'c1', connpool)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        assert appender.append(df) == 3
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)
        connpool.shutDown()

    def test_PartitionedTableAppender_permission_table_insert(self):
        connpool = ddb.DBConnectionPool(HOST, PORT, 3, "table_insert_user", PASSWD)
        appender = ddb.PartitionedTableAppender(self.TEST_DB, self.TEST_TABLE, 'c1', connpool)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        assert appender.append(df) == 3
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)
        connpool.shutDown()

    def test_PartitionedTableAppender_permission_table_update(self):
        connpool = ddb.DBConnectionPool(HOST, PORT, 3, "table_update_user", PASSWD)
        appender = ddb.PartitionedTableAppender(self.TEST_DB, self.TEST_TABLE, 'c1', connpool)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        with pytest.raises(RuntimeError, match="<NoPrivilege>Not granted to write data to table"):
            appender.append(df)
        connpool.shutDown()

    def test_PartitionedTableAppender_permission_db_write(self):
        connpool = ddb.DBConnectionPool(HOST, PORT, 3, "db_write_user", PASSWD)
        appender = ddb.PartitionedTableAppender(self.TEST_DB, self.TEST_TABLE, 'c1', connpool)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        assert appender.append(df) == 3
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)
        connpool.shutDown()

    def test_PartitionedTableAppender_permission_db_insert(self):
        connpool = ddb.DBConnectionPool(HOST, PORT, 3, "db_insert_user", PASSWD)
        appender = ddb.PartitionedTableAppender(self.TEST_DB, self.TEST_TABLE, 'c1', connpool)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        assert appender.append(df) == 3
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)
        connpool.shutDown()

    def test_PartitionedTableAppender_permission_db_update(self):
        connpool = ddb.DBConnectionPool(HOST, PORT, 3, "db_update_user", PASSWD)
        appender = ddb.PartitionedTableAppender(self.TEST_DB, self.TEST_TABLE, 'c1', connpool)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        with pytest.raises(RuntimeError, match="<NoPrivilege>Not granted to write data to table"):
            appender.append(df)
        connpool.shutDown()

    def test_PartitionedTableAppender_permission_db_owner(self):
        connpool = ddb.DBConnectionPool(HOST, PORT, 3, "db_owner_user", PASSWD)
        appender = ddb.PartitionedTableAppender(self.TEST_DB, self.TEST_TABLE, 'c1', connpool)
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        with pytest.raises(RuntimeError, match="<NoPrivilege>Not granted to write data to table"):
            appender.append(df)
        connpool.shutDown()

    def test_MultithreadedTableWriter_permission_table_write(self):
        mtwriter = ddb.MultithreadedTableWriter(HOST, PORT, "table_write_user", PASSWD, self.TEST_DB, self.TEST_TABLE,
                                                False, False, [], 1, 1, 3, 'c1')
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        for i in range(df.shape[0]):
            mtwriter.insert(df.iloc[i][0], df.iloc[i][1], df.iloc[i][2].to_numpy().astype('datetime64'))
        mtwriter.waitForThreadCompletion()
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_MultithreadedTableWriter_permission_table_insert(self):
        mtwriter = ddb.MultithreadedTableWriter(HOST, PORT, "table_insert_user", PASSWD, self.TEST_DB, self.TEST_TABLE,
                                                False, False, [], 1, 1, 3, 'c1')
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        for i in range(df.shape[0]):
            mtwriter.insert(df.iloc[i][0], df.iloc[i][1], df.iloc[i][2].to_numpy().astype('datetime64'))
        mtwriter.waitForThreadCompletion()
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_MultithreadedTableWriter_permission_table_update(self):
        mtwriter = ddb.MultithreadedTableWriter(HOST, PORT, "table_update_user", PASSWD, self.TEST_DB, self.TEST_TABLE,
                                                False, False, [], 1, 1, 3, 'c1')
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        for i in range(df.shape[0]):
            mtwriter.insert(df.iloc[i][0], df.iloc[i][1], df.iloc[i][2].to_numpy().astype('datetime64'))
        mtwriter.waitForThreadCompletion()
        status = mtwriter.getStatus()
        assert status.hasError()
        assert "<NoPrivilege>Not granted to write data to table" in status.errorInfo
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert res.shape[0] == 0

    def test_MultithreadedTableWriter_permission_db_write(self):
        mtwriter = ddb.MultithreadedTableWriter(HOST, PORT, "db_write_user", PASSWD, self.TEST_DB, self.TEST_TABLE,
                                                False, False, [], 1, 1, 3, 'c1')
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        for i in range(df.shape[0]):
            mtwriter.insert(df.iloc[i][0], df.iloc[i][1], df.iloc[i][2].to_numpy().astype('datetime64'))
        mtwriter.waitForThreadCompletion()
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_MultithreadedTableWriter_permission_db_insert(self):
        mtwriter = ddb.MultithreadedTableWriter(HOST, PORT, "db_insert_user", PASSWD, self.TEST_DB, self.TEST_TABLE,
                                                False, False, [], 1, 1, 3, 'c1')
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[ns]'),
        })
        for i in range(df.shape[0]):
            mtwriter.insert(df.iloc[i][0], df.iloc[i][1], df.iloc[i][2].to_numpy().astype('datetime64'))
        mtwriter.waitForThreadCompletion()
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert_frame_equal(df, res)

    def test_MultithreadedTableWriter_permission_db_update(self):
        mtwriter = ddb.MultithreadedTableWriter(HOST, PORT, "db_update_user", PASSWD, self.TEST_DB, self.TEST_TABLE,
                                                False, False, [], 1, 1, 3, 'c1')
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        for i in range(df.shape[0]):
            mtwriter.insert(df.iloc[i][0], df.iloc[i][1], df.iloc[i][2].to_numpy().astype('datetime64'))
        mtwriter.waitForThreadCompletion()
        status = mtwriter.getStatus()
        assert status.hasError()
        assert "<NoPrivilege>Not granted to write data to table" in status.errorInfo

        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert res.shape[0] == 0

    def test_MultithreadedTableWriter_permission_db_owner(self):
        mtwriter = ddb.MultithreadedTableWriter(HOST, PORT, "db_owner_user", PASSWD, self.TEST_DB, self.TEST_TABLE,
                                                False, False, [], 1, 1, 3, 'c1')
        df = pd.DataFrame({
            'c1': np.array([1, 2, 3], dtype=np.int32),
            'c2': ['a', 'b', 'c'],
            'c3': np.array(["2023-07-01", "2023-07-02", "2023-07-03"], dtype='datetime64[D]'),
        })
        for i in range(df.shape[0]):
            mtwriter.insert(df.iloc[i][0], df.iloc[i][1], df.iloc[i][2].to_numpy().astype('datetime64'))
        mtwriter.waitForThreadCompletion()
        status = mtwriter.getStatus()
        assert status.hasError()
        assert "<NoPrivilege>Not granted to write data to table" in status.errorInfo
        res = self.conn.run(f"select * from loadTable('{self.TEST_DB}', '{self.TEST_TABLE}')")
        assert res.shape[0] == 0
