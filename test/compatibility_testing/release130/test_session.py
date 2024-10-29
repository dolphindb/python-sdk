import platform
import subprocess
import threading
import time
import warnings

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import pytest
from numpy.testing import *
from pandas.testing import *

from setup.settings import *
from setup.utils import get_pid, random_string

py_version = int(platform.python_version().split('.')[1])


@pytest.mark.timeout(120)
class TestSession:
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
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    @pytest.mark.parametrize('_protocol', [keys.PROTOCOL_ARROW, keys.PROTOCOL_DEFAULT,
                                           keys.PROTOCOL_DDB, keys.PROTOCOL_PICKLE, None],
                             ids=["ARROW", "DEFAULT", "DDB", "PICKLE", "UNSOUPPORTED"])
    def test_session_parameter_enablePickle_protocol(self, _pickle, _protocol):
        if _pickle and _protocol != keys.PROTOCOL_DEFAULT:
            with pytest.raises(RuntimeError,
                               match='When enablePickle=true, the parameter protocol must not be specified. '):
                conn1 = ddb.session(HOST, PORT, USER, PASSWD,
                                    enablePickle=_pickle, protocol=_protocol)
        elif _protocol == None:
            with pytest.raises(RuntimeError, match='Protocol None is not supported. '):
                conn1 = ddb.session(HOST, PORT, USER, PASSWD,
                                    enablePickle=_pickle, protocol=_protocol)
        else:
            # TODO: 需要抓包对协议类型进行断言
            conn1 = ddb.session(HOST, PORT, USER, PASSWD,
                                enablePickle=_pickle, protocol=_protocol)
            assert conn1.run("1+1") == 2

    @pytest.mark.SESSION
    @pytest.mark.parametrize('user', [USER, "adminxxxx", ""], ids=["T_USER", "F_USER", "N_USER"])
    @pytest.mark.parametrize('passwd', [PASSWD, "123456777888", ""], ids=["T_PASSWD", "F_PASSWD", "N_PASSWD"])
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_user_passwd(self, user, passwd, _compress, _pickle):
        if user == USER and passwd == PASSWD:
            conn1 = ddb.session(HOST, PORT, user, passwd,
                                compress=_compress, enablePickle=_pickle)
            res = conn1.run("1+1")
            assert res == 2
        elif user == "":
            conn1 = ddb.session(HOST, PORT, user, passwd,
                                compress=_compress, enablePickle=_pickle)
            try:
                res = conn1.run("1+1")
            except:
                assert True
        else:
            result = subprocess.run([sys.executable, '-c',
                                     "import dolphindb as ddb;"
                                     f"conn=ddb.Session('{HOST}', {PORT}, '{user}', '{passwd}',compress={_compress},enablePickle={_pickle});"
                                     ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
            assert "The user name or password is incorrect" in result.stderr

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_enableASYNC(self, _compress, _pickle):
        self.conn.run("""
            t=table(`1`2`3 as col1, 1 2 3 as col2);
            share t as tab;
        """)
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableASYNC=True,
                            compress=_compress, enablePickle=_pickle)
        conn1.run(r"""
            for(i in 1..5){tableInsert(tab, string(i), i);sleep(1000)};
        """)
        time.sleep(2)
        curRows = int(self.conn.run("exec count(*) from tab"))
        assert curRows >= 4 and curRows < 8

        time.sleep(6)
        curRows = int(self.conn.run("exec count(*) from tab"))
        assert curRows == 8

        conn1.run("undef(`tab,SHARED)")
        conn1.close()

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_enableChunkGranularityConfig(self, _compress, _pickle):
        num = 10
        dbpath = "dfs://test_" + random_string(5)
        tablename = "dfs_tab"
        if self.conn.run("bool(getConfig()[`enableChunkGranularityConfig])") == "0":
            pytest.skip(
                reason="skip this case when server's 'enableChunkGranularityConfig' is False")
        conn1 = ddb.session(HOST, PORT, "admin", "123456",
                            enableChunkGranularityConfig=True, compress=_compress, enablePickle=_pickle)
        dates = np.array(pd.date_range(start='2000-01-01',
                                       end='2000-12-30', freq="D"), dtype="datetime64[D]")
        conn1.run(
            """if(existsDatabase("{}")){{dropDatabase("{}")}};""".format(dbpath, dbpath))
        db = conn1.database(dbName="test_dfs", partitionType=keys.VALUE, partitions=dates,
                            dbPath=dbpath, engine="OLAP", atomic="TRANS", chunkGranularity="DATABASE")
        t = conn1.table(data=conn1.run(
            "table(100:0, `col1`col2`col3, [INT,INT,DATE])"))
        db.createPartitionedTable(
            table=t, tableName=tablename, partitionColumns="col3")
        threads = []

        def insert_job(host: str, port: int, dbpath: str, tableName: str):
            conn = ddb.session()
            conn.connect(host, port, "admin", "123456")
            try:
                for _ in range(10):
                    conn.run("""t=table(100:0, `col1`col2`col3, [INT,INT,DATE]);
                            tableInsert(t,rand(1000,1), rand(1000,1), 2000.01.01);
                            loadTable('{}',`{}).append!(t);""".format(dbpath, tableName))
            except:
                pass
            conn.close()

        for _ in range(num):
            threads.append(threading.Thread(target=insert_job,
                                            args=(HOST, PORT, dbpath, tablename)))
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        assert (int(self.conn.run(
            "exec count(*) from loadTable('{}',`{})".format(dbpath, tablename))) > 0)
        assert (int(self.conn.run(
            "exec count(*) from loadTable('{}',`{})".format(dbpath, tablename))) < num * 10)
        conn1.dropDatabase(dbpath)
        conn1.close()

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_python(self, _compress, _pickle):
        if self.conn.run("version()")[:3] != "2.10":
            pytest.skip(reason="server version not matched, skip this case")
        else:
            conn1 = ddb.session(HOST, PORT, USER, PASSWD, python=True,
                                compress=_compress, enablePickle=_pickle)
            conn1.run("import dolphindb as ddb\n\
                        a=[1,2,3]\n\
                        b={1,2,3}\n\
                        c={1:1,2:2}\n\
                        d=(12,3,4)")
            assert conn1.run("type(a)") == "list"
            assert conn1.run("type(b)") == "set"
            assert conn1.run("type(c)") == "dict"
            assert conn1.run("type(d)") == "tuple"

    # TODO: no warning is catched
    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_enableASYN_Warning(self, _compress, _pickle):
        warnings.filterwarnings('error')
        try:
            ddb.session(HOST, PORT, USER, PASSWD, enableASYN=True,
                        compress=_compress, enablePickle=_pickle)
        except Warning as war:
            assert "Please use enableASYNC instead of enableASYN" in str(war)

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_enableSSL(self, _compress, _pickle):
        connSSL = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True,
                              compress=_compress, enablePickle=_pickle)
        assert connSSL.run("getConfig()[`enableHTTPS]")

    @pytest.mark.SESSION
    @pytest.mark.parametrize('user', [USER, "adminxxxx", ""], ids=["T_USER", "F_USER", "N_USER"])
    @pytest.mark.parametrize('passwd', [PASSWD, "123456777888", ""], ids=["T_PASSWD", "F_PASSWD", "N_PASSWD"])
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_connect_user_passwd(self, user, passwd, _compress, _pickle):
        conn1 = ddb.session(compress=_compress, enablePickle=_pickle)
        if user == USER and passwd == PASSWD:
            conn1.connect(HOST, PORT, user, passwd)
            res = conn1.run("1+1")
            assert res == 2
        elif user == "":
            conn1.connect(HOST, PORT, user, passwd)
            try:
                res = conn1.run("1+1")
            except:
                assert True
        else:
            result = subprocess.run([sys.executable, '-c',
                                     "import dolphindb as ddb;"
                                     f"conn=ddb.Session('{HOST}', {PORT}, '{user}', '{passwd}');"
                                     ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
            assert "The user name or password is incorrect" in result.stderr

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_connect_startup(self, _compress, _pickle):
        conn1 = ddb.session(compress=_compress, enablePickle=_pickle)
        startup_script = """
            tab=table(`1`2`3 as col1,4 5 6 as col2);
            share tab as startup_tab;
        """
        conn1.connect(HOST, PORT, USER, PASSWD, startup_script)
        res = all(conn1.run(
            "tab1=table(`1`2`3 as col1,4 5 6 as col2);each(eqObj,tab1.values(),startup_tab.values())"))
        conn1.run('undef `startup_tab,SHARED')
        assert res

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_connect_close(self, _compress, _pickle):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD,
                            compress=_compress, enablePickle=_pickle)
        ans = conn1.run("1+2")
        assert (ans == 3)
        c3 = conn1.isClosed()
        assert (c3 == False)
        conn1.close()
        with pytest.raises(Exception):
            conn1.run("1+2")
        c4 = conn1.isClosed()
        assert (c4 == True)

    # TODO: NO PARAM python
    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_connect_python(self, _compress, _pickle):
        if self.conn.run("version()")[:3] != "2.10":
            pytest.skip(reason="server version not matched, skip this case")
        else:
            conn1 = ddb.session()
            conn1.connect(HOST, PORT, USER, PASSWD, python=True,
                          compress=_compress, enablePickle=_pickle)
            conn1.run("import dolphindb as ddb\n\
                        a=[1,2,3]\n\
                        b={1,2,3}\n\
                        c={1:1,2:2}\n\
                        d=(12,3,4)")
            assert conn1.run("type(a)") == "list"
            assert conn1.run("type(b)") == "set"
            assert conn1.run("type(c)") == "dict"
            assert conn1.run("type(d)") == "tuple"

    @pytest.mark.SESSION
    @pytest.mark.skipif(sys.platform.startswith('win32'), reason="Windows server, skip this case")
    def test_session_connect_reconnect(self):
        def startCurNodes(cur_node: str) -> bool:
            connCtl = ddb.session()
            connCtl.connect(HOST, CTL_PORT, USER, PASSWD)
            connCtl.run("try{startDataNode(`" + cur_node + r")}catch(ex){}")
            while connCtl.run("(exec state from getClusterPerf() where name = `{})[0]".format(cur_node)) != 1:
                time.sleep(1)
                connCtl.run("try{startDataNode(`" + cur_node + r")}catch(ex){}")

            print("restart successfully")
            connCtl.close()
            return True

        def stopCurNodes(cur_node: str) -> bool:
            connCtl = ddb.session()
            connCtl.connect(HOST, CTL_PORT, USER, PASSWD)
            connCtl.run("try{stopDataNode(`" + cur_node + r")}catch(ex){}")
            time.sleep(1)
            while connCtl.run("(exec state from getClusterPerf() where name = `{})[0]".format(cur_node)) != 0:
                time.sleep(1)
                connCtl.run("try{stopDataNode(`" + cur_node + r")}catch(ex){}")
            print("stop successfully")
            connCtl.close()
            return True

        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD, reconnect=True)

        cur_node = conn1.run("getNodeAlias()")
        print("cur_node is " + cur_node)

        assert (stopCurNodes(cur_node))
        time.sleep(1)

        conn2 = ddb.session()
        print("check if unconnected...")
        res = conn2.connect(HOST, PORT, USER, PASSWD)
        assert not res

        time.sleep(1)
        assert (startCurNodes(cur_node))

        print('check if reconnected...')
        assert conn1.run("1+1") == 2

        conn1.close()
        conn2.close()

    @pytest.mark.SESSION
    @pytest.mark.parametrize('user', [USER, "adminxxxx"], ids=["T_USER", "F_USER"])
    @pytest.mark.parametrize('passwd', [PASSWD, "123456777888"], ids=["T_PASSWD", "F_PASSWD"])
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_login_user_passwd(self, user, passwd, _compress, _pickle):
        conn1 = ddb.session(HOST, PORT, compress=_compress,
                            enablePickle=_pickle)
        if user == USER and passwd == PASSWD:
            conn1.login(user, passwd)
            res = conn1.run("1+1")
            assert res == 2
        elif user == "" and passwd == "":
            conn1.login(user, passwd)
            with pytest.raises(Exception) as e:
                res = conn1.run("1+1")
        else:
            with pytest.raises(RuntimeError) as e:
                conn1.login(user, passwd)

    @pytest.mark.SESSION
    def test_session_func_loadTableBySQL_when_define_a_para_called_db(self):
        dbPath = "dfs://test_func_loadtablebysql"
        tableName = "tmp"
        if self.conn.existsDatabase(dbPath):
            self.conn.dropDatabase(dbPath)
        db = self.conn.database(dbName='mydb', partitionType=keys.VALUE, partitions=[
            1, 2, 3], dbPath=dbPath)
        df = pd.DataFrame({'c1': np.array([1, 2, 3], dtype='int32'), 'c2': ['a', 'b', 'c']})

        t = self.conn.table(data=df)
        db.createPartitionedTable(
            t, tableName, partitionColumns='c1').append(t)
        self.conn.run("db = 'str_tmp'")
        res = self.conn.loadTableBySQL(
            tableName, dbPath, sql=f"select * from {tableName} where c1 =3").toDF()
        assert_frame_equal(res, pd.DataFrame({'c1': np.array([3], dtype='int32'), 'c2': ['c']}))

        assert self.conn.run("db") == 'str_tmp'

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_Session_connect_close(self, _compress, _pickle):
        conn1 = ddb.Session(HOST, PORT, USER, PASSWD,
                            compress=_compress, enablePickle=_pickle)
        ans = conn1.run("1+2")
        assert (ans == 3)
        c3 = conn1.isClosed()
        assert (c3 == False)
        conn1.close()
        with pytest.raises(Exception):
            conn1.run("1+2")
        c4 = conn1.isClosed()
        assert (c4 == True)

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_print_msg_in_console(self, _compress, _pickle):
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
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}',compress={_compress},enablePickle={_pickle});"
                                 f"conn.run(\"\"\"{script}\"\"\")"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == """1\n1\n1\n1\n1\n1970.01.02\n0000.02M\n00:00:00.001\n00:01m\n00:00:01\n1970.01.01T00:00:01\n1970.01.01T00:00:00.001\n00:00:00.000000001\n1970.01.01T00:00:00.000000001\n1\n1\n1\n5d212a78-cc48-e3b1-4235-b4d91473ee87\n["1"]\ncol1 col2\n---- ----\n1    a   \n2    b   \n3    c   \n\n"""

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_disable_print_msg_in_console(self, _compress, _pickle):
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
        v=arrayVector(1 2 3 , 9 9 9);\
        print(a,b,c,d,ee,f,g,h,i,j,k,l,m,n,o,p,q,r,s,ttt,u,v)"
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "import asyncio;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}',compress={_compress},enablePickle={_pickle},show_output=False);"
                                 f"conn.run(\"\"\"{script}\"\"\")"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == ""

    @pytest.mark.SESSION
    def test_session_enablessl_download_huge_table(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True)
        s = """
            login(`admin,`123456)

            db_name = take("/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8", 1000000)
            table_name = take("tablexxxx", 1000000)
            record_amount = rand(100, 1000000)
            disk_usage = rand(100, 1000000)

            share table(db_name,table_name,record_amount,disk_usage) as tab
        """
        conn1.run(s)
        df = conn1.run("select * from tab")
        conn1.run('undef `tab,SHARED')
        assert df.shape[0] == 1000000
        assert_array_equal(df['db_name'].to_list(), ["/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8"] * 1000000)

    @pytest.mark.SESSION
    def test_session_enablessl_download_huge_array(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True)
        vec = conn1.run("take('/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8', 3000000)")
        assert_array_equal(vec, ["/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8"] * 3000000)

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_priority', [-1, 'a', 1.1, dict(), list(), tuple(), set(), None])
    def test_run_with_para_priority_exception(self, _priority):
        with pytest.raises(Exception, match="priority must be an integer from 0 to 9"):
            self.conn.run("objs();sleep(2000)", priority=_priority)

    @pytest.mark.SESSION
    @pytest.mark.parametrize('_parallelism', [-1, 'a', 1.1, dict(), list(), tuple(), set(), None])
    def test_run_with_para_parallelism_exception(self, _parallelism):
        with pytest.raises(Exception, match="parallelism must be an integer greater than 0"):
            self.conn.run("objs();sleep(2000)", parallelism=_parallelism)

    @pytest.mark.SESSION
    def test_run_with_para_priority_parallelism(self):
        def run_and_assert(conn: ddb.session, expect, priority=None, parallelism=None):
            s = f"""
                sessionid = exec sessionid from getSessionMemoryStat() where userId=`{USER};
                priority = exec priority from getConsoleJobs() where sessionId=sessionid;
                parallelism = exec parallelism from getConsoleJobs() where sessionId=sessionid;
                [priority[0], parallelism[0]]
                """
            if priority is None and parallelism is None:
                res = conn.run(s)
            else:
                res = conn.run(s, priority=priority, parallelism=parallelism)
            assert_array_equal(res, expect)

        run_and_assert(self.conn, [0, 10], 0, 10)
        run_and_assert(self.conn, [4, 64], None, None)
        run_and_assert(self.conn, [8, 1], 9, 1)


if __name__ == '__main__':
    pytest.main(["-s", "test_session.py"])
