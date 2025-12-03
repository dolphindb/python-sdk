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
from pydantic import ValidationError

from basic_testing.prepare import random_string
from basic_testing.utils import operateNodes
from setup.settings import HOST, PORT, USER, PASSWD, HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER, \
    PORT_DNODE1, PORT_DNODE2, PORT_DNODE3


class TestSession:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    def test_session_protocol_error(self):
        with pytest.raises(Exception, match="Protocol 4 is not supported. "):
            ddb.Session(HOST, PORT, USER, PASSWD, protocol=4)
        with pytest.raises(Exception, match="The Arrow protocol does not support compression."):
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
            with pytest.raises(ValidationError):
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
        assert 4 <= curRows <= 8
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
            a=[1,2,3]
            b={1,2,3}
            c={1:1,2:2}
            d=(12,3,4)
        """)
        assert conn1.run("type(a)") == "list"
        assert conn1.run("type(b)") == "set"
        assert conn1.run("type(c)") == "dict"
        assert conn1.run("type(d)") == "tuple"

    def test_session_init_parser_default(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        conn1.run("""
            a=[1,2,3]
            b=(12,3,4)
        """)
        assert conn1.run("type(a)") == 4  # FAST INT VECTOR
        assert conn1.run("type(b)") == 25  # ANY VECTOR

    def test_session_init_parser_dolphindb(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, parser="dolphindb")
        conn1.run("""
            a=[1,2,3]
            b=(12,3,4)
        """)
        assert conn1.run("type(a)") == 4  # FAST INT VECTOR
        assert conn1.run("type(b)") == 25  # ANY VECTOR

    def test_session_init_parser_python(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, parser="python")
        conn1.run("""
            a=[1,2,3]
            b={1,2,3}
            c={1:1,2:2}
            d=(12,3,4)
        """)
        assert conn1.run("type(a)") == "list"
        assert conn1.run("type(b)") == "set"
        assert conn1.run("type(c)") == "dict"
        assert conn1.run("type(d)") == "tuple"

    def test_session_init_parser_kdb(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, parser="kdb")
        conn1.run("""
            t1: 12 2;
            t2: `a`b`c!1 2 3;
            t:([] name:`tom`dick`harry`jack`jill;sex:`m`m`m`m`f;eye:`blue`green`blue`blue`gray);
            select name,eye by sex from t;
        """)
        assert conn1.run("type(t1)") == 7  # kdb整数列表
        assert conn1.run("type(t2)") == 99  # kdb字典

    def test_session_init_python_false_parser_int(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, python=False, parser=1)
        conn1.run("""
                    a=[1,2,3]
                    b=(12,3,4)
                """)
        assert conn1.run("type(a)") == 4  # FAST INT VECTOR
        assert conn1.run("type(b)") == 25  # ANY VECTOR

    def test_session_init_python_true_parser_kdb(self):
        with pytest.raises(RuntimeError, match="The parameter parser must not be specified when python=true"):
            conn1 = ddb.session(HOST, PORT, USER, PASSWD, python=True, parser="kdb")

    def test_session_init_group_by_sex_with_name_and_eye_arrays(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        conn1.run("""
            t= table(`tom`dickh`arry`jack`jill as name,  `m`m`m`m`f as sex, `blue`green`blue`blue`gray as eye);
            re = select toArray(name),toArray(eye) from t group by sex ; 
        """)
        result = conn1.run("re")
        expected = pd.DataFrame({
            "sex": ["f", "m"],
            "toArray_name": [["jill"], ["tom", "dickh", "arry", "jack"]],
            "toArray_eye": [["gray"], ["blue", "green", "blue", "blue"]]
        })
        assert_frame_equal(result, expected)

    def test_session_init_python_true_parser_Nonenum_value(self):
        # with pytest.raises(RuntimeError,match="The parameter parser must not be specified when python=true"):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, python=False, parser="Nonenum_value")
        conn1.run("""
            a=[1,2,3]
            b=(12,3,4)
        """)
        assert conn1.run("type(a)") == 4  # FAST INT VECTOR
        assert conn1.run("type(b)") == 25  # ANY VECTOR

    def test_session_init_enableASYN_Warning(self):
        warnings.filterwarnings('error')
        with pytest.raises(Warning):
            ddb.session(HOST, PORT, USER, PASSWD, enableASYN=True)

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
        while blockReader.has_next:
            total += len(blockReader.read())
        else:
            blockReader.skip_all()
        assert total == fetchSize * 10

    def test_session_init_script(self):
        script = "sleep(1)"
        self.conn.setInitScript(script)
        assert self.conn.getInitScript() == script

    def test_session_get_session_id(self):
        id, user = self.conn.run('getCurrentSessionAndUser()')[:2]
        assert str(id) == self.conn.getSessionId()
        assert user == USER

    @pytest.mark.CLUSTER
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
        assert all(((loadDf - loadDf.mean()).abs() < 0.7)['load'])
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "STOP")
        for i in connList:
            time.sleep(0.5)
            assert i.run("true")
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "START")
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
        assert result.stdout.count("Failed to connect") == n
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
        assert result.stdout.count("Failed to connect") == n * 2
        assert f"Connect to {HOST}:56789 failed after {n} reconnect attempts." in result.stdout

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_session_highAvailability_tryReconnectNums_conn(self):
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}', f'dnode{PORT_DNODE3}'], "STOP")
        n = 3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "conn_=ddb.Session();"
                                 f"conn_.connect('{HOST_CLUSTER}',{PORT_DNODE1},'{USER_CLUSTER}','{PASSWD_CLUSTER}', highAvailability=True, highAvailabilitySites=['{HOST_CLUSTER}:{PORT_DNODE1}','{HOST_CLUSTER}:{PORT_DNODE2}','{HOST_CLUSTER}:{PORT_DNODE3}'], tryReconnectNums={n});"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}', f'dnode{PORT_DNODE3}'], "START")
        assert result.stdout.count("Failed to connect") + result.stdout.count("DataNodeNotAvail") == n * 3
        assert f"Connect failed after {n} reconnect attempts for every node in high availability sites." in result.stdout

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_session_highAvailability_tryReconnectNums_run(self):
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        n = 3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "from basic_testing.utils import operateNodes;"
                                 f"conn=ddb.Session('{HOST_CLUSTER}',{PORT_CONTROLLER},'{USER_CLUSTER}','{PASSWD_CLUSTER}');"
                                 "conn_=ddb.Session();"
                                 f"conn_.connect('{HOST_CLUSTER}',{PORT_DNODE1},'{USER_CLUSTER}','{PASSWD_CLUSTER}', highAvailability=True, highAvailabilitySites=['{HOST_CLUSTER}:{PORT_DNODE1}','{HOST_CLUSTER}:{PORT_DNODE2}','{HOST_CLUSTER}:{PORT_DNODE3}'], tryReconnectNums={n});"
                                 f"""operateNodes(conn,['dnode{PORT_DNODE1}','dnode{PORT_DNODE2}','dnode{PORT_DNODE3}'],'STOP');"""
                                 "conn_.run('true');"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}', f'dnode{PORT_DNODE3}'], "START")
        assert result.stdout.count("Failed to connect") + result.stdout.count("DataNodeNotAvail") == n * 3
        assert f"Connect to nodes failed after {n} reconnect attempts." in result.stderr

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_session_highAvailability_tryReconnectSuccess(self):
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        n = 1
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "from basic_testing.utils import operateNodes;"
                                 f"conn=ddb.Session('{HOST_CLUSTER}',{PORT_CONTROLLER},'{USER_CLUSTER}','{PASSWD_CLUSTER}');"
                                 "conn_=ddb.Session();"
                                 f"conn_.connect('{HOST_CLUSTER}',{PORT_DNODE1},'{USER_CLUSTER}','{PASSWD_CLUSTER}', highAvailability=True, highAvailabilitySites=['{HOST_CLUSTER}:{PORT_DNODE1}','{HOST_CLUSTER}:{PORT_DNODE2}','{HOST_CLUSTER}:{PORT_DNODE3}'], tryReconnectNums={n});"
                                 f"""operateNodes(conn,['dnode{PORT_DNODE1}','dnode{PORT_DNODE2}'],'STOP');"""
                                 "conn_.run('true');"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}', f'dnode{PORT_DNODE3}'], "START")
        assert result.stdout.count("Failed to connect") + result.stdout.count("DataNodeNotAvail") == n
        assert f"Warn: Reconnect to {HOST_CLUSTER} : {PORT_DNODE3} with session id:" in result.stdout

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

    @pytest.mark.parametrize("script", ["1", "[1]", "1..6$2:3", "set(1..3)", "dict(1..3,1..3)", "x=1"],
                             ids=["scalar", "vector", "matrix", "set", "dictionary", "no_return"])
    def test_session_run_with_table_schema_not_table(self, script):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        result = conn._run_with_table_schema(script)
        assert result[1] is None

    def test_session_run_with_table_schema_table(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        result = conn._run_with_table_schema("""
            table(
                array(BOOL) as bool,
                array(CHAR) as char,
                array(SHORT) as short,
                array(INT) as int,
                array(LONG) as long,
                array(DATE) as date,
                array(MONTH) as month,
                array(TIME) as time,
                array(MINUTE) as minute,
                array(SECOND) as second,
                array(DATETIME) as datetime,
                array(TIMESTAMP) as timestamp,
                array(NANOTIME) as nanotime,
                array(NANOTIMESTAMP) as nanotimestamp,
                array(FLOAT) as float,
                array(DOUBLE) as double,
                array(STRING) as string,
                array(UUID) as uuid,
                array(DATEHOUR) as datehour,
                array(IPADDR) as ipaddr,
                array(INT128) as int128,
                array(BLOB) as blob,
                array(DECIMAL32(2)) as decimal32,
                array(DECIMAL64(3)) as decimal64,
                array(DECIMAL128(4)) as decimal128,
                array(ANY) as any,
                array(BOOL[]) as bool_av,
                array(CHAR[]) as char_av,
                array(SHORT[]) as short_av,
                array(INT[]) as int_av,
                array(LONG[]) as long_av,
                array(DATE[]) as date_av,
                array(MONTH[]) as month_av,
                array(TIME[]) as time_av,
                array(MINUTE[]) as minute_av,
                array(SECOND[]) as second_av,
                array(DATETIME[]) as datetime_av,
                array(TIMESTAMP[]) as timestamp_av,
                array(NANOTIME[]) as nanotime_av,
                array(NANOTIMESTAMP[]) as nanotimestamp_av,
                array(FLOAT[]) as float_av,
                array(DOUBLE[]) as double_av,
                array(UUID[]) as uuid_av,
                array(DATEHOUR[]) as datehour_av,
                array(IPADDR[]) as ipaddr_av,
                array(INT128[]) as int128_av,
                array(DECIMAL32(2)[]) as decimal32_av,
                array(DECIMAL64(3)[]) as decimal64_av,
                array(DECIMAL128(4)[]) as decimal128_av
            )
        """)
        assert result[-1]['bool'] == ('BOOL', None)
        assert result[-1]['char'] == ('CHAR', None)
        assert result[-1]['short'] == ('SHORT', None)
        assert result[-1]['int'] == ('INT', None)
        assert result[-1]['long'] == ('LONG', None)
        assert result[-1]['date'] == ('DATE', None)
        assert result[-1]['month'] == ('MONTH', None)
        assert result[-1]['time'] == ('TIME', None)
        assert result[-1]['minute'] == ('MINUTE', None)
        assert result[-1]['second'] == ('SECOND', None)
        assert result[-1]['datetime'] == ('DATETIME', None)
        assert result[-1]['timestamp'] == ('TIMESTAMP', None)
        assert result[-1]['nanotime'] == ('NANOTIME', None)
        assert result[-1]['nanotimestamp'] == ('NANOTIMESTAMP', None)
        assert result[-1]['float'] == ('FLOAT', None)
        assert result[-1]['double'] == ('DOUBLE', None)
        assert result[-1]['string'] == ('STRING', None)
        assert result[-1]['uuid'] == ('UUID', None)
        assert result[-1]['datehour'] == ('DATEHOUR', None)
        assert result[-1]['ipaddr'] == ('IPADDR', None)
        assert result[-1]['int128'] == ('INT128', None)
        assert result[-1]['blob'] == ('BLOB', None)
        assert result[-1]['decimal32'] == ('DECIMAL32', 2)
        assert result[-1]['decimal64'] == ('DECIMAL64', 3)
        assert result[-1]['decimal128'] == ('DECIMAL128', 4)
        assert result[-1]['any'] == ('ANY', None)
        assert result[-1]['bool_av'] == ('BOOL[]', None)
        assert result[-1]['char_av'] == ('CHAR[]', None)
        assert result[-1]['short_av'] == ('SHORT[]', None)
        assert result[-1]['int_av'] == ('INT[]', None)
        assert result[-1]['long_av'] == ('LONG[]', None)
        assert result[-1]['date_av'] == ('DATE[]', None)
        assert result[-1]['month_av'] == ('MONTH[]', None)
        assert result[-1]['time_av'] == ('TIME[]', None)
        assert result[-1]['minute_av'] == ('MINUTE[]', None)
        assert result[-1]['second_av'] == ('SECOND[]', None)
        assert result[-1]['datetime_av'] == ('DATETIME[]', None)
        assert result[-1]['timestamp_av'] == ('TIMESTAMP[]', None)
        assert result[-1]['nanotime_av'] == ('NANOTIME[]', None)
        assert result[-1]['nanotimestamp_av'] == ('NANOTIMESTAMP[]', None)
        assert result[-1]['float_av'] == ('FLOAT[]', None)
        assert result[-1]['double_av'] == ('DOUBLE[]', None)
        assert result[-1]['uuid_av'] == ('UUID[]', None)
        assert result[-1]['datehour_av'] == ('DATEHOUR[]', None)
        assert result[-1]['ipaddr_av'] == ('IPADDR[]', None)
        assert result[-1]['int128_av'] == ('INT128[]', None)
        assert result[-1]['decimal32_av'] == ('DECIMAL32[]', 2)
        assert result[-1]['decimal64_av'] == ('DECIMAL64[]', 3)
        assert result[-1]['decimal128_av'] == ('DECIMAL128[]', 4)

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_session_highAvailability_tryReconnectNums_1(self):
        n = 1
        conn = ddb.Session()
        haSites = [f"{HOST_CLUSTER}:{PORT_DNODE3}"]
        result = conn.connect(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, "",
                              highAvailability=True, highAvailabilitySites=haSites, tryReconnectNums=n)
        assert result

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_session_change_conn(self):
        conn_ctl = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn = ddb.Session()
        conn.connect(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, highAvailability=True,
                     highAvailabilitySites=[f'{HOST_CLUSTER}:{PORT_DNODE1}', f'{HOST_CLUSTER}:{PORT_DNODE2}',
                                            f'{HOST_CLUSTER}:{PORT_DNODE3}'])
        assert conn.session_id == str(conn.call("getCurrentSessionAndUser")[0])
        assert conn.host == HOST_CLUSTER
        assert conn.port == conn.call("getNodePort")
        operateNodes(conn_ctl, [f"{conn.host}:{conn.port}"], "STOP")
        assert conn.run("true")
        assert conn.session_id == str(conn.call("getCurrentSessionAndUser")[0])
        assert conn.host == HOST_CLUSTER
        assert conn.port == conn.call("getNodePort")
        operateNodes(conn_ctl, [f"{conn.host}:{conn.port}"], "STOP")
        assert conn.run("true")
        assert conn.session_id == str(conn.call("getCurrentSessionAndUser")[0])
        assert conn.host == HOST_CLUSTER
        assert conn.port == conn.call("getNodePort")
        operateNodes(conn_ctl, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}', f'dnode{PORT_DNODE3}'], "START")
