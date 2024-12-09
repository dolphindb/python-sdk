import inspect
import subprocess
import sys
import threading
import time
import warnings

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from pandas._testing import assert_frame_equal

from basic_testing.prepare import random_string
from setup.settings import HOST, PORT, USER, PASSWD, HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER, \
    PORT_DNODE1, PORT_DNODE2, PORT_DNODE3


class TestSession:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    def test_session_protocol_error(self):
        with pytest.raises(RuntimeError, match="Protocol 4 is not supported. "):
            ddb.Session(HOST, PORT, USER, PASSWD, protocol=4)
        with pytest.raises(RuntimeError, match="The Arrow protocol does not support compression."):
            ddb.Session(HOST, PORT, USER, PASSWD, protocol=keys.PROTOCOL_ARROW, compress=True)

    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    @pytest.mark.parametrize('_protocol', [keys.PROTOCOL_ARROW, keys.PROTOCOL_DEFAULT,
                                           keys.PROTOCOL_DDB, keys.PROTOCOL_PICKLE, None],
                             ids=["ARROW", "DEFAULT", "DDB", "PICKLE", "UNSOUPPORTED"])
    def test_session_parameter_enablePickle_protocol(self, _pickle, _protocol):
        if _pickle and _protocol != keys.PROTOCOL_DEFAULT:
            with pytest.raises(RuntimeError,
                               match='When enablePickle=true, the parameter protocol must not be specified. '):
                ddb.session(HOST, PORT, USER, PASSWD,
                            enablePickle=_pickle, protocol=_protocol)
        elif _protocol is None:
            with pytest.raises(RuntimeError, match='Protocol None is not supported. '):
                ddb.session(HOST, PORT, USER, PASSWD,
                            enablePickle=_pickle, protocol=_protocol)
        else:
            conn1 = ddb.session(HOST, PORT, USER, PASSWD,
                                enablePickle=_pickle, protocol=_protocol)
            assert conn1.run("1+1") == 2

    @pytest.mark.parametrize('user', [USER, "adminxxxx", ""], ids=["T_USER", "F_USER", "N_USER"])
    @pytest.mark.parametrize('passwd', [PASSWD, "123456777888", ""], ids=["T_PASSWD", "F_PASSWD", "N_PASSWD"])
    def test_session_init_user_passwd(self, user, passwd):
        if user == USER and passwd == PASSWD:
            conn1 = ddb.session(HOST, PORT, user, passwd)
            res = conn1.run("1+1")
            assert res == 2
        elif user == "":
            conn1 = ddb.session(HOST, PORT, user, passwd)
            try:
                conn1.run("1+1")
            except RuntimeError:
                assert True
        else:
            result = subprocess.run([sys.executable, '-c',
                                     "import dolphindb as ddb;"
                                     f"conn=ddb.Session('{HOST}', {PORT}, '{user}', '{passwd}');"
                                     ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
            assert "The user name or password is incorrect" in result.stdout

    def test_session_init_enableASYNC(self):
        func_name = inspect.currentframe().f_code.co_name
        self.conn.run(f"""
            t=table(`1`2`3 as col1, 1 2 3 as col2);
            share t as `{func_name};
        """)
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableASYNC=True)
        conn1.run(f"for(i in 1..5){{tableInsert({func_name}, string(i), i);sleep(1000)}}")
        time.sleep(2)
        curRows = int(self.conn.run(f"exec count(*) from {func_name}"))
        assert 4 <= curRows < 8
        time.sleep(6)
        curRows = int(self.conn.run(f"exec count(*) from {func_name}"))
        assert curRows == 8
        conn1.close()

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_enableChunkGranularityConfig(self, _compress, _pickle):
        num = 10
        dbpath = "dfs://test_" + random_string(5)
        tablename = "dfs_tab"
        if self.conn.run("bool(getConfig()[`enableChunkGranularityConfig])") == "0":
            pytest.skip(reason="skip this case when server's 'enableChunkGranularityConfig' is False")
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableChunkGranularityConfig=True, compress=_compress,
                            enablePickle=_pickle)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-12-30', freq="D"), dtype="datetime64[D]")
        conn1.run(f"if(existsDatabase('{dbpath}')){{dropDatabase('{dbpath}')}}")
        db = conn1.database(dbName="test_dfs", partitionType=keys.VALUE, partitions=dates,
                            dbPath=dbpath, engine="TSDB", atomic="TRANS", chunkGranularity="DATABASE")
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [INT,INT,DATE])"))
        db.createPartitionedTable(table=t, tableName=tablename, partitionColumns="col3", sortColumns="col2")
        threads = []

        def insert_job(host: str, port: int, dbpath: str, tableName: str):
            conn = ddb.session()
            conn.connect(host, port, USER, PASSWD)
            try:
                for _ in range(10):
                    conn.run(f"""
                        t=table(100:0, `col1`col2`col3, [INT,INT,DATE]);
                        tableInsert(t,rand(1000,1), rand(1000,1), 2000.01.01);
                        loadTable('{dbpath}',`{tableName}).append!(t);
                    """)
            except RuntimeError:
                pass
            conn.close()

        for _ in range(num):
            threads.append(threading.Thread(target=insert_job, args=(HOST, PORT, dbpath, tablename)))
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        assert (int(self.conn.run(f"exec count(*) from loadTable('{dbpath}',`{tablename})")) > 0)
        assert (int(self.conn.run(f"exec count(*) from loadTable('{dbpath}',`{tablename})")) < num * 10)
        conn1.dropDatabase(dbpath)
        conn1.close()

    def test_session_init_python(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, python=True)
        conn1.run("""
            import dolphindb as ddb
            a=[1,2,3]
            b={1,2,3}
            c={1:1,2:2}
            d=(12,3,4)
        """)
        assert conn1.run("type(a)") == "list"
        assert conn1.run("type(b)") == "set"
        assert conn1.run("type(c)") == "dict"
        assert conn1.run("type(d)") == "tuple"

    def test_session_init_enableASYN_Warning(self):
        warnings.filterwarnings('error')
        try:
            ddb.session(HOST, PORT, USER, PASSWD, enableASYN=True)
        except Warning as war:
            assert "Please use enableASYNC instead of enableASYN" in str(war)

    def test_session_init_enableSSL(self):
        connSSL = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True)
        assert connSSL.run("getConfig()[`enableHTTPS]")
        a = [1, 2, 3]
        b = ['AAPL', 'AMZN', 'GOOG']
        c = [1.2, 2.3, 3.4]
        d = np.array(['2012-06-13', '2012-06-14', '2012-06-15'], dtype='datetime64[ns]')
        connSSL.upload({'a': a, 'b': b, 'c': c, 'd': d})
        t = connSSL.run("t=table(a as col1, b as col2, c as col3, d as col4);t")
        ex_df = pd.DataFrame({'col1': a, 'col2': b, 'col3': c, 'col4': d})
        assert_frame_equal(t, ex_df)

    @pytest.mark.parametrize('user', [USER, "adminxxxx", ""], ids=["T_USER", "F_USER", "N_USER"])
    @pytest.mark.parametrize('passwd', [PASSWD, "123456777888", ""], ids=["T_PASSWD", "F_PASSWD", "N_PASSWD"])
    def test_session_connect_user_passwd(self, user, passwd):
        conn1 = ddb.session()
        if user == USER and passwd == PASSWD:
            conn1.connect(HOST, PORT, user, passwd)
            res = conn1.run("1+1")
            assert res == 2
        elif user == "":
            conn1.connect(HOST, PORT, user, passwd)
            try:
                conn1.run("1+1")
            except RuntimeError:
                assert True
        else:
            result = subprocess.run([sys.executable, '-c',
                                     "import dolphindb as ddb;"
                                     "conn=ddb.Session();"
                                     f"conn.connect('{HOST}', {PORT}, '{user}', '{passwd}')"
                                     ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
            assert "The user name or password is incorrect" in result.stdout

    def test_session_connect_startup(self):
        conn1 = ddb.session()
        startup_script = "tab=table(`1`2`3 as col1,4 5 6 as col2)"
        conn1.connect(HOST, PORT, USER, PASSWD, startup_script)
        res = all(conn1.run("tab1=table(`1`2`3 as col1,4 5 6 as col2);each(eqObj,tab1.values(),tab.values())"))
        assert res

    def test_session_connect_close(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        ans = conn1.run("1+2")
        assert ans == 3
        c3 = conn1.isClosed()
        assert not c3
        conn1.close()
        with pytest.raises(Exception):
            conn1.run("1+2")
        c4 = conn1.isClosed()
        assert c4

    @pytest.mark.xdist_group(name='cluster_test')
    def test_session_connect_reconnect(self):

        def startCurNodes(cur_node: str) -> bool:
            connCtl = ddb.session()
            connCtl.connect(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
            connCtl.run(f"try{{startDataNode(`{cur_node})}}catch(ex){{}}")
            while connCtl.run(f"(exec state from getClusterPerf() where name = `{cur_node})[0]") != 1:
                time.sleep(1)
                connCtl.run(f"try{{startDataNode(`{cur_node})}}catch(ex){{}}")
            connCtl.close()
            return True

        def stopCurNodes(cur_node: str) -> bool:
            connCtl = ddb.session()
            connCtl.connect(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
            connCtl.run(f"try{{stopDataNode(`{cur_node})}}catch(ex){{}}")
            time.sleep(1)
            while connCtl.run(f"(exec state from getClusterPerf() where name = `{cur_node})[0]") != 0:
                time.sleep(1)
                connCtl.run(f"try{{stopDataNode(`{cur_node})}}catch(ex){{}}")
            connCtl.close()
            return True

        conn1 = ddb.session()
        conn1.connect(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, reconnect=True)
        cur_node = conn1.run("getNodeAlias()")
        assert stopCurNodes(cur_node)
        time.sleep(1)
        conn2 = ddb.session()
        res = conn2.connect(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER)
        assert not res
        time.sleep(1)
        assert startCurNodes(cur_node)
        assert conn1.run("1+1") == 2
        conn1.close()
        conn2.close()

    @pytest.mark.parametrize('user', [USER, "adminxxxx"], ids=["T_USER", "F_USER"])
    @pytest.mark.parametrize('passwd', [PASSWD, "123456777888"], ids=["T_PASSWD", "F_PASSWD"])
    def test_session_login_user_passwd(self, user, passwd):
        conn1 = ddb.session(HOST, PORT)
        if user == USER and passwd == PASSWD:
            conn1.login(user, passwd)
            res = conn1.run("1+1")
            assert res == 2
        elif user == "" and passwd == "":
            conn1.login(user, passwd)
            with pytest.raises(Exception):
                conn1.run("1+1")
        else:
            with pytest.raises(RuntimeError):
                conn1.login(user, passwd)

    def test_session_func_loadTableBySQL_when_define_a_para_called_db(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        tableName = "tmp"
        if self.conn.existsDatabase(db_name):
            self.conn.dropDatabase(db_name)
        db = self.conn.database(dbName='mydb', partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=db_name)
        df = pd.DataFrame({'c1': np.array([1, 2, 3], dtype='int32'), 'c2': ['a', 'b', 'c']})
        t = self.conn.table(data=df)
        db.createPartitionedTable(t, tableName, partitionColumns='c1').append(t)
        self.conn.run("db = 'str_tmp'")
        res = self.conn.loadTableBySQL(tableName, db_name, sql=f"select * from {tableName} where c1 =3").toDF()
        assert_frame_equal(res, pd.DataFrame({'c1': np.array([3], dtype='int32'), 'c2': ['c']}))
        assert self.conn.run("db") == 'str_tmp'

    def test_session_print_msg_in_console(self):
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
            print(a,b,c,d,ee,f,g,h,i,j,k,l,m,n,o,p,q,r,s,ttt,u,v)\
        "
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 f"conn.run(\"\"\"{script}\"\"\")"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == """1\n1\n1\n1\n1\n1970.01.02\n0000.02M\n00:00:00.001\n00:01m\n00:00:01\n1970.01.01T00:00:01\n1970.01.01T00:00:00.001\n00:00:00.000000001\n1970.01.01T00:00:00.000000001\n1\n1\n1\n5d212a78-cc48-e3b1-4235-b4d91473ee87\n["1"]\ncol1 col2\n---- ----\n1    a   \n2    b   \n3    c   \n\n[[9],[9],[9]]\n"""

    def test_session_disable_print_msg_in_console(self):
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
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}',show_output=False);"
                                 f"conn.run(\"\"\"{script}\"\"\")"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == ""

    def test_session_enable_ssl_download_huge_table(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True)
        s = f"""
            db_name = take("/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8", 1000000)
            table_name = take("tablexxxx", 1000000)
            record_amount = rand(100, 1000000)
            disk_usage = rand(100, 1000000)
            share table(db_name,table_name,record_amount,disk_usage) as {func_name}
        """
        conn1.run(s)
        df = conn1.run(f"select * from {func_name}")
        assert df.shape[0] == 1000000
        assert_array_equal(df['db_name'].to_list(), ["/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8"] * 1000000)

    def test_session_enable_ssl_download_huge_array(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True)
        vec = conn1.run("take('/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8', 3000000)")
        assert_array_equal(vec, ["/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8"] * 3000000)

    def test_session_enable_ssl_download_huge_arrayVector(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True)
        av = conn1.run("""
            val = take(123456, 3000000)
            ind = 1..3000000
            arrayVector(ind, val)
        """)
        for v in av:
            assert v == [123456]

    @pytest.mark.parametrize('_priority', [-1, 'a', 1.1, dict(), list(), tuple(), set()])
    def test_session_run_with_para_priority_exception(self, _priority):
        with pytest.raises(Exception, match="priority must be an integer from 0 to 9"):
            self.conn.run("objs();sleep(2000)", priority=_priority)

    @pytest.mark.parametrize('_parallelism', [-1, 'a', 1.1, dict(), list(), tuple(), set()])
    def test_session_run_with_para_parallelism_exception(self, _parallelism):
        with pytest.raises(Exception, match="parallelism must be an integer greater than 0"):
            self.conn.run("objs();sleep(2000)", parallelism=_parallelism)

    def test_session_run_with_para_priority_parallelism(self):
        def run_and_assert(conn: ddb.session, expect, priority=None, parallelism=None):
            s = f"""
                select priority,parallelism from getConsoleJobs() where sessionId=getCurrentSessionAndUser()[0]
            """
            res = conn.run(s, priority=priority, parallelism=parallelism)
            assert res['priority'].iat[0] == expect[0]
            assert res['parallelism'].iat[0] == expect[1]

        run_and_assert(self.conn, [0, 10], 0, 10)
        run_and_assert(self.conn, [4, 64], None, None)
        run_and_assert(self.conn, [8, 1], 9, 1)

    def test_session_run_fetchSize(self):
        fetchSize = 10000
        blockReader = self.conn.run(f"table(take(1,{fetchSize * 10}) as a)", fetchSize=fetchSize)
        total = 0
        while blockReader.hasNext():
            total += len(blockReader.read())
        else:
            blockReader.skipAll()
        assert total == fetchSize * 10

    def test_session_init_script(self):
        script = "sleep(1)"
        self.conn.setInitScript(script)
        assert self.conn.getInitScript() == script

    def test_session_get_session_id(self):
        id, user = self.conn.run('getCurrentSessionAndUser()')[:2]
        assert str(id) == self.conn.getSessionId()
        assert user == USER

    @pytest.mark.xdist_group(name='cluster_test')
    def test_session_highAvailability_load_balance(self):
        sites = [f"{HOST_CLUSTER}:{port}" for port in (PORT_DNODE1, PORT_DNODE2, PORT_DNODE3)]
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        connList = [ddb.Session() for i in range(60)]
        for i in connList:
            time.sleep(0.5)
            i.connect(host=HOST_CLUSTER, port=PORT_CONTROLLER, userid=USER_CLUSTER, password=PASSWD_CLUSTER,
                      highAvailability=True,
                      highAvailabilitySites=sites)
        loadDf = conn.run(
            "select (connectionNum + workerNum + executorNum)/3.0 as load from rpc(getControllerAlias(), getClusterPerf) where mode=0")
        assert all(((loadDf - loadDf.mean()).abs() < 0.4)['load'])
        conn.run(f"stopDataNode(['dnode{PORT_DNODE1}'])")
        for i in connList:
            time.sleep(0.5)
            assert i.run("true")
        conn.run(f"startDataNode(['dnode{PORT_DNODE1}'])")
        # print(conn.run("select site,(connectionNum + workerNum + executorNum)/3.0 as load,connectionNum,workerNum,executorNum from rpc(getControllerAlias(), getClusterPerf) where mode=0"))
        # conn.run(f"stopDataNode(['dnode{PORT_DNODE2}'])")
        # for i in connList:
        #     time.sleep(1)
        #     assert i.run("true")
        # conn.run(f"startDataNode(['dnode{PORT_DNODE2}'])")
        # time.sleep(3)
        # print(conn.run("select site,(connectionNum + workerNum + executorNum)/3.0 as load,connectionNum,workerNum,executorNum from rpc(getControllerAlias(), getClusterPerf) where mode=0"))
        #
        # conn.run(f"stopDataNode(['dnode{PORT_DNODE3}'])")
        # for i in connList:
        #     time.sleep(1)
        #     assert i.run("true")
        # conn.run(f"startDataNode(['dnode{PORT_DNODE3}'])")
        # time.sleep(3)
        # print(conn.run("select site,(connectionNum + workerNum + executorNum)/3.0 as load,connectionNum,workerNum,executorNum from rpc(getControllerAlias(), getClusterPerf) where mode=0"))
        for i in connList:
            i.close()

    def test_session_sqlStd(self):
        conn_ddb = ddb.Session(HOST, PORT, USER, PASSWD, sqlStd=keys.SqlStd.DolphinDB)
        conn_oracle = ddb.Session(HOST, PORT, USER, PASSWD, sqlStd=keys.SqlStd.Oracle)
        conn_mysql = ddb.Session(HOST, PORT, USER, PASSWD, sqlStd=keys.SqlStd.MySQL)
        with pytest.raises(RuntimeError):
            conn_ddb.run("sysdate()")
        conn_oracle.run("sysdate()")
        conn_mysql.run("sysdate()")

    def test_session_tryReconnectNums_conn(self):
        n = 3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "conn=ddb.Session();"
                                 f"res=conn.connect('{HOST}', 56789, '{USER}', '{PASSWD}', reconnect=True, tryReconnectNums={n});"
                                 f"assert not res;"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout.count("Failed") == n
        assert f"Connect to {HOST}:56789 failed after {n} reconnect attempts." in result.stdout

    def test_session_tryReconnectNums_run(self):
        n = 3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "conn=ddb.Session();"
                                 f"res=conn.connect('{HOST}', 56789, '{USER}', '{PASSWD}', reconnect=True, tryReconnectNums={n});"
                                 "assert not res;"
                                 "conn.run('1+1');"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout.count("Failed") == n * 2
        assert f"Connect to {HOST}:56789 failed after {n} reconnect attempts." in result.stdout

    @pytest.mark.xdist_group(name='cluster_test')
    def test_session_highAvailability_tryReconnectNums_conn(self):
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn.run(f"stopDataNode(['dnode{PORT_DNODE1}','dnode{PORT_DNODE2}','dnode{PORT_DNODE3}'])")
        n = 3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "conn_=ddb.Session();"
                                 f"conn_.connect('{HOST_CLUSTER}',{PORT_DNODE1},'{USER_CLUSTER}','{PASSWD_CLUSTER}', highAvailability=True, highAvailabilitySites=['{HOST_CLUSTER}:{PORT_DNODE1}','{HOST_CLUSTER}:{PORT_DNODE2}','{HOST_CLUSTER}:{PORT_DNODE3}'], tryReconnectNums={n});"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        conn.run(f"startDataNode(['dnode{PORT_DNODE1}','dnode{PORT_DNODE2}','dnode{PORT_DNODE3}'])")
        assert result.stdout.count("Failed") == n * 3
        assert f"Connect failed after {n} reconnect attempts for every node in high availability sites." in result.stdout

    @pytest.mark.xdist_group(name='cluster_test')
    def test_session_highAvailability_tryReconnectNums_run(self):
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        n = 3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST_CLUSTER}',{PORT_CONTROLLER},'{USER_CLUSTER}','{PASSWD_CLUSTER}');"
                                 "conn_=ddb.Session();"
                                 f"conn_.connect('{HOST_CLUSTER}',{PORT_DNODE1},'{USER_CLUSTER}','{PASSWD_CLUSTER}', highAvailability=True, highAvailabilitySites=['{HOST_CLUSTER}:{PORT_DNODE1}','{HOST_CLUSTER}:{PORT_DNODE2}','{HOST_CLUSTER}:{PORT_DNODE3}'], tryReconnectNums={n});"
                                 f"""conn.run("stopDataNode(['dnode{PORT_DNODE1}','dnode{PORT_DNODE2}','dnode{PORT_DNODE3}'])");"""
                                 "conn_.run('true');"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        conn.run(f"startDataNode(['dnode{PORT_DNODE1}','dnode{PORT_DNODE2}','dnode{PORT_DNODE3}'])")
        assert result.stdout.count("Failed") == n * 3
        assert f"Connect to nodes failed after {n} reconnect attempts." in result.stderr

    def test_session_readTimeout(self):
        conn = ddb.Session()
        conn.connect(HOST, PORT, USER, PASSWD, readTimeout=3)
        assert conn.run('true')
        with pytest.raises(RuntimeError):
            conn.run('sleep(4000);true')

    def test_session_writeTimeout(self):
        conn = ddb.Session()
        conn.connect(HOST, PORT, USER, PASSWD, writeTimeout=3)
        conn.upload({'a': True})
        assert conn.run('a')
        # set packet loss to test
