from importlib.util import find_spec
import subprocess
import warnings
import pytest
from setup.settings import *
from setup.utils import get_pid, random_string
import dolphindb as ddb
import numpy as np
import pandas as pd
from numpy.testing import *
from pandas.testing import *
import dolphindb.settings as keys
import time
import sys
import threading
import platform
import datetime
import decimal

if find_spec("pyarrow") is not None:
    import pyarrow

py_version = int(platform.python_version().split('.')[1])


def loadPlugin():
    conn = ddb.session(HOST, PORT, USER, PASSWD)
    try:
        conn.run(f"loadPlugin('{ARROW_DIR}')")
    except Exception as e:
        print(e)


@pytest.mark.timeout(120)
class TestSession:
    conn = ddb.session()

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
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

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
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_user_passwd(self, user, passwd, _compress, _pickle):
        if user == USER and passwd == PASSWD:
            conn1 = ddb.session(HOST, PORT, user, passwd, compress=_compress, enablePickle=_pickle)
            res = conn1.run("1+1")
            assert res == 2
        elif user == "":
            conn1 = ddb.session(HOST, PORT, user, passwd, compress=_compress, enablePickle=_pickle)
            try:
                conn1.run("1+1")
            except RuntimeError:
                assert True
        else:
            result = subprocess.run([sys.executable, '-c',
                                     "import dolphindb as ddb;"
                                     f"conn=ddb.Session('{HOST}', {PORT}, '{user}', '{passwd}',compress={_compress},enablePickle={_pickle});"
                                     ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
            assert "The user name or password is incorrect" in result.stderr

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_enableASYNC(self, _compress, _pickle):
        self.conn.run("""
            t=table(`1`2`3 as col1, 1 2 3 as col2);
            share t as tab;
        """)
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableASYNC=True,
                            compress=_compress, enablePickle=_pickle)
        conn1.run("for(i in 1..5){tableInsert(tab, string(i), i);sleep(1000)}")
        time.sleep(2)
        curRows = int(self.conn.run("exec count(*) from tab"))
        assert 4 <= curRows < 8
        time.sleep(6)
        curRows = int(self.conn.run("exec count(*) from tab"))
        assert curRows == 8
        conn1.run("undef(`tab,SHARED)")
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

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_python(self, _compress, _pickle):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, python=True, compress=_compress, enablePickle=_pickle)
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

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_enableASYN_Warning(self, _compress, _pickle):
        warnings.filterwarnings('error')
        try:
            ddb.session(HOST, PORT, USER, PASSWD, enableASYN=True, compress=_compress, enablePickle=_pickle)
        except Warning as war:
            assert "Please use enableASYNC instead of enableASYN" in str(war)

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_init_enableSSL(self, _compress, _pickle):
        connSSL = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True, compress=_compress, enablePickle=_pickle)
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
                conn1.run("1+1")
            except RuntimeError:
                assert True
        else:
            result = subprocess.run([sys.executable, '-c',
                                     "import dolphindb as ddb;"
                                     f"conn=ddb.Session('{HOST}', {PORT}, '{user}', '{passwd}');"
                                     ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
            assert "The user name or password is incorrect" in result.stderr

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_connect_startup(self, _compress, _pickle):
        conn1 = ddb.session(compress=_compress, enablePickle=_pickle)
        startup_script = """
            tab=table(`1`2`3 as col1,4 5 6 as col2);
            share tab as startup_tab;
        """
        conn1.connect(HOST, PORT, USER, PASSWD, startup_script)
        res = all(conn1.run("tab1=table(`1`2`3 as col1,4 5 6 as col2);each(eqObj,tab1.values(),startup_tab.values())"))
        conn1.run('undef `startup_tab,SHARED')
        assert res

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_connect_close(self, _compress, _pickle):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress, enablePickle=_pickle)
        ans = conn1.run("1+2")
        assert ans == 3
        c3 = conn1.isClosed()
        assert not c3
        conn1.close()
        with pytest.raises(Exception):
            conn1.run("1+2")
        c4 = conn1.isClosed()
        assert c4

    @pytest.mark.skipif(sys.platform.startswith('win32'), reason="Windows server, skip this case")
    def test_session_connect_reconnect(self):
        def startCurNodes(cur_node: str) -> bool:
            connCtl = ddb.session()
            connCtl.connect(HOST, CTL_PORT, USER, PASSWD)
            connCtl.run(f"try{{startDataNode(`{cur_node})}}catch(ex){{}}")
            while connCtl.run(f"(exec state from getClusterPerf() where name = `{cur_node})[0]") != 1:
                time.sleep(1)
                connCtl.run(f"try{{startDataNode(`{cur_node})}}catch(ex){{}}")
            connCtl.close()
            return True

        def stopCurNodes(cur_node: str) -> bool:
            connCtl = ddb.session()
            connCtl.connect(HOST, CTL_PORT, USER, PASSWD)
            connCtl.run(f"try{{stopDataNode(`{cur_node})}}catch(ex){{}}")
            time.sleep(1)
            while connCtl.run(f"(exec state from getClusterPerf() where name = `{cur_node})[0]") != 0:
                time.sleep(1)
                connCtl.run(f"try{{stopDataNode(`{cur_node})}}catch(ex){{}}")
            connCtl.close()
            return True

        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD, reconnect=True)
        cur_node = conn1.run("getNodeAlias()")
        assert stopCurNodes(cur_node)
        time.sleep(1)
        conn2 = ddb.session()
        res = conn2.connect(HOST, PORT, USER, PASSWD)
        assert not res
        time.sleep(1)
        assert startCurNodes(cur_node)
        assert conn1.run("1+1") == 2
        conn1.close()
        conn2.close()

    @pytest.mark.parametrize('user', [USER, "adminxxxx"], ids=["T_USER", "F_USER"])
    @pytest.mark.parametrize('passwd', [PASSWD, "123456777888"], ids=["T_PASSWD", "F_PASSWD"])
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_login_user_passwd(self, user, passwd, _compress, _pickle):
        conn1 = ddb.session(HOST, PORT, compress=_compress, enablePickle=_pickle)
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

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_session_runFile(self, _compress, _pickle):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, compress=_compress, enablePickle=_pickle)
        undef = '''
            try{unsubscribeTable(,"t_share","sub_1");}catch(ex){}
            try{dropAggregator("test1");}catch(ex){}
            if(existsDatabase("dfs://test")){
                dropDatabase("dfs://test")
            }
            try{undef((exec name from objs(true) where shared=1),SHARED);}catch(ex){}
        '''
        conn1.run(undef)
        file_dir = LOCAL_DATA_DIR + "/session_runFile.txt"
        tab = conn1.runFile(file_dir, clearMemory=False, pickleTableToList=True)
        assert isinstance(tab, list)
        expect_df = conn1.run("select * from loadTable(dbpath, `dfs_tab)")
        expect_tab = conn1.table(data=expect_df)
        assert_array_equal(tab, expect_tab.toList())
        tab2 = conn1.runFile(file_dir, clearMemory=False, pickleTableToList=False)
        assert isinstance(tab2, pd.DataFrame)
        expect_df2 = conn1.run("select * from loadTable(dbpath, `dfs_tab)")
        expect_tab2 = conn1.table(data=expect_df2)
        assert_frame_equal(tab2, expect_tab2.toDF())
        file_path = LOCAL_DATA_DIR + "/run_data.txt"
        conn1.runFile(file_path)
        time.sleep(3)
        t1 = conn1.table(data="t1")
        re1 = conn1.table(data="re1")
        exec1 = conn1.run('select stdp(value) from t1')
        assert_frame_equal(re1.toDF(), exec1)
        re2 = conn1.table(data="re2")
        exec2 = conn1.run('select sum(value) from t1')
        assert_frame_equal(re2.toDF(), exec2)
        pt1 = conn1.loadTable(dbPath="dfs://test", tableName="pt1")
        pt1.append(t1)
        assert_frame_equal(pt1.sort(bys='time').toDF(), t1.sort(bys="time").toDF())
        t_share = conn1.table(data="t_share")
        t_share.append(t1)
        output = conn1.table(data="output1")
        size3 = len(output.toDF())
        assert size3 > 0
        conn1.runFile(file_dir, clearMemory=True)
        assert len(conn1.run("objs()")) == 0
        conn1.run(undef)

    if find_spec("pyarrow") is not None:
        @pytest.mark.ARROW
        def test_session_download_table_enableArrow_with_supportedTypes(self):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            df = pd.DataFrame({
                'cbool': np.array([True, False, None]),
                'cchar': np.array([1, -1, 0], dtype=np.byte),
                'cshort': np.array([-10, 1000, 0], dtype=np.int16),
                'cint': np.array([-10, 1000, 0], dtype=np.int32),
                'clong': np.array([-10, 1000, 0], dtype=np.int64),
                'ctime': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335245880', 'nat'],
                                  dtype='datetime64[ms]'),
                'cdate': np.array(['1970-01-21 13:30:10.008', '1970-01-01 00:00:00.000000000', 'nat'],
                                  dtype='datetime64[D]'),
                'cmonth': np.array(['2020-01-02 13:30:10.008', '1970-12-21 13:30:10.008', 'nat'],
                                   dtype='datetime64[M]'),
                'cminute': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335245880', 'nat'],
                                    dtype='datetime64[m]'),
                'csecond': np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335245880', 'nat'],
                                    dtype='datetime64[s]'),
                'cdatetime': np.array(['1970-01-01 13:30:10.008', '2022-10-03 05:15:33.335245880', 'nat'],
                                      dtype='datetime64[s]'),
                'ctimestamp': np.array(['2012-06-13 13:30:10.008', '2001-04-22 15:18:29.118325481', 'nat'],
                                       dtype='datetime64[ms]'),
                'cnanotimestamp': np.array(['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                                           dtype='datetime64[ns]'),
                'cfloat': np.array([2.2134500, -5.36411, np.nan], dtype='float32'),
                'cdouble': np.array([3.214, -47.795324, np.nan], dtype='float64'),
                'csymbol': np.array(['sym1', 'sym2', ''], dtype='object'),
                'cstring': np.array(['str1', 'str2', ''], dtype='object'),
                'cblob': np.array(['blob1', 'blob2', ''], dtype='object'),
                'cipaddr': np.array(["192.168.1.1", "192.168.1.254", "0.0.0.0"], dtype='object'),
                'cuuid': np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee99", ""],
                                  dtype='object'),
                'cint128': np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec79", ""],
                                    dtype='object'),
                'cdecimal32': np.array([decimal.Decimal("-1").quantize(decimal.Decimal("0.000")), None, decimal.Decimal(
                    "0").quantize(decimal.Decimal("0.000"))], dtype='object'),
                'cdecimal64': np.array(
                    [decimal.Decimal("-1").quantize(decimal.Decimal("0.0000000000")), None, decimal.Decimal(
                        "0").quantize(decimal.Decimal("0.0000000000"))], dtype='object'),
                'cdatehour': np.array(['1970-01-01 13:30:10.008', '2022-10-03 05:15:33.335245880', 'nat'],
                                      dtype='datetime64[h]'),
            })
            df.__DolphinDB_Type__ = {
                'ctime': keys.DT_TIME,
                'cdate': keys.DT_DATE,
                'cmonth': keys.DT_MONTH,
                'cminute': keys.DT_MINUTE,
                'csecond': keys.DT_SECOND,
                'cdatetime': keys.DT_DATETIME,
                'ctimestamp': keys.DT_TIMESTAMP,
                'cnanotimestamp': keys.DT_NANOTIMESTAMP,
                'csymbol': keys.DT_SYMBOL,
                'cblob': keys.DT_BLOB,
                'cipaddr': keys.DT_IPPADDR,
                'cuuid': keys.DT_UUID,
                'cint128': keys.DT_INT128,
                'cdecimal32': keys.DT_DECIMAL32,
                'cdecimal64': keys.DT_DECIMAL64,
                'cdatehour': keys.DT_DATEHOUR,
            }
            conn1.upload({"tab": df})
            tab = conn1.run("tab")
            ex_tab = pyarrow.table({
                'cbool': np.array([True, False, None]),
                'cchar': np.array([1, -1, 0], dtype=np.int8),
                'cshort': np.array([-10, 1000, 0], dtype=np.int16),
                'cint': np.array([-10, 1000, 0], dtype=np.int32),
                'clong': np.array([-10, 1000, 0], dtype=np.int64),
                'ctime': pyarrow.array(
                    np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335245880', 'nat'],
                             dtype='datetime64[ms]'),
                    type=pyarrow.time32('ms')),
                'cdate': pyarrow.array([datetime.datetime(1970, 1, 21), datetime.datetime(1970, 1, 1), None],
                                       type=pyarrow.date32()),
                'cmonth': pyarrow.array([datetime.datetime(2020, 1, 1), datetime.datetime(1970, 12, 1), None],
                                        type=pyarrow.date32()),
                'cminute': pyarrow.array(
                    [datetime.time(hour=13, minute=30, second=00), datetime.time(hour=5, minute=15, second=00), None],
                    type=pyarrow.time32('s')),
                'csecond': pyarrow.array(
                    np.array(['1970-01-01 13:30:10.008', '1970-01-01 05:15:33.335245880', 'nat'],
                             dtype='datetime64[s]'),
                    type=pyarrow.time32('s')),
                'cdatetime': pyarrow.array(
                    np.array(['1970-01-01 13:30:10.008', '2022-10-03 05:15:33.335245880', 'nat'],
                             dtype='datetime64[s]'),
                    type=pyarrow.timestamp('s')),
                'ctimestamp': pyarrow.array(
                    np.array(['2012-06-13 13:30:10.008', '2001-04-22 15:18:29.118325481', 'nat'],
                             dtype='datetime64[ms]'),
                    type=pyarrow.timestamp('ms')),
                'cnanotimestamp': pyarrow.array(
                    np.array(['1970-01-01 13:30:10.008007006', '1970-01-01 21:08:02.008007006', 'nat'],
                             dtype='datetime64[ns]'), type=pyarrow.timestamp('ns')),
                'cfloat': pyarrow.array([2.2134500, -5.36411, None], type=pyarrow.float32()),
                'cdouble': pyarrow.array([3.214, -47.795324, None], type=pyarrow.float64()),
                'csymbol': pyarrow.DictionaryArray.from_arrays(pyarrow.array([1, 2, None], type=pyarrow.int32()),
                                                               pyarrow.array(['', 'sym1', 'sym2'])),
                'cstring': np.array(['str1', 'str2', None], dtype='object'),
                'cblob': pyarrow.array([b'blob1', b'blob2', None], type=pyarrow.large_binary()),
                'cipaddr': np.array(["192.168.1.1", "192.168.1.254", None], dtype='object'),
                'cuuid': pyarrow.array(
                    [b']!*x\xccH\xe3\xb1B5\xb4\xd9\x14s\xee\x87', b']!*x\xccH\xe3\xb1B5\xb4\xd9\x14s\xee\x99', None],
                    type=pyarrow.binary(16)),
                'cint128': pyarrow.array(
                    [b'\xe1g\x17\x97\xc5.\x15\xf7c8\x0bE\xe8A\xec2', b'\xe1g\x17\x97\xc5.\x15\xf7c8\x0bE\xe8A\xecy',
                     None],
                    type=pyarrow.binary(16)),
                'cdecimal32': pyarrow.array([-1, None, 0], type=pyarrow.decimal128(38, 3)),
                'cdecimal64': pyarrow.array([-1, None, 0], type=pyarrow.decimal128(38, 10)),
                'cdatehour': pyarrow.array(
                    np.array(['1970-01-01 13:00:00', '2022-10-03 05:00:00', 'nat'], dtype='datetime64[s]'),
                    type=pyarrow.timestamp('s')),
            })
            assert tab.equals(ex_tab)

        @pytest.mark.ARROW
        @pytest.mark.parametrize('_coltype, _patype', [('bool', None), ('char', pyarrow.int8()),
                                                       ('int', pyarrow.int32()), ('short', pyarrow.int16()),
                                                       ('long', pyarrow.int64())],
                                 ids=['bool', 'char', 'int', 'short', 'long'])
        def test_session_download_table_enableArrow_with_array_vector_1(self, _coltype, _patype):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            t = conn1.run(f"""
                ind = 1 3 4;
                col = arrayVector(ind, {_coltype}([-2, NULL, 0, 7]));
                t=table(col);
                t
            """)
            assert isinstance(t, pyarrow.Table)
            ex_t = pyarrow.table({
                'col': conn1.run('t[`col]') if _coltype == 'bool' else
                pyarrow.array([[-2], [None, 0], [7]], type=pyarrow.list_(_patype))
            })
            assert t.equals(ex_t)

        @pytest.mark.ARROW
        @pytest.mark.parametrize('_coltype, _patype', [('float', pyarrow.float32()), ('double', pyarrow.float64())],
                                 ids=['float', 'double'])
        def test_session_download_table_enableArrow_with_array_vector_2(self, _coltype, _patype):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            t = conn1.run(f"""
                ind = 1 3 4;
                col = arrayVector(ind, {_coltype}([-2.578921, NULL, 0.00, 390520312.123]));
                t=table(col);
                t
            """)
            assert isinstance(t, pyarrow.Table)
            ex_t = pyarrow.table({
                'col': pyarrow.array([[-2.578921], [None, 0.00], [390520312.123]], type=pyarrow.list_(_patype))
            })
            assert t.equals(ex_t)

        @pytest.mark.ARROW
        @pytest.mark.parametrize('_coltype, _patype', [('time', pyarrow.time32('ms')), ('minute', pyarrow.time32('s')),
                                                       ('second', pyarrow.time32('s'))],
                                 ids=['time', 'minute', 'second'])
        def test_session_download_table_enableArrow_with_array_vector_3(self, _coltype, _patype):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            t = conn1.run(f"""
                ind = 1 3;
                col = arrayVector(ind, {_coltype}([0, 1, NULL]));
                t=table(col);
                t
            """)
            assert isinstance(t, pyarrow.Table)
            ex_t = pyarrow.table({
                'col': pyarrow.array([[0], [60, None]], type=pyarrow.list_(_patype)) if _coltype == 'minute'
                else pyarrow.array([[0], [1, None]], type=pyarrow.list_(_patype))
            })
            assert t.equals(ex_t)

        @pytest.mark.ARROW
        @pytest.mark.parametrize('_coltype, _patype', [('date', pyarrow.date32()), ('datetime', pyarrow.timestamp('s')),
                                                       ('datehour', pyarrow.timestamp('s')),
                                                       ('timestamp', pyarrow.timestamp('ms')),
                                                       ('nanotime', pyarrow.time64('ns')),
                                                       ('nanotimestamp', pyarrow.timestamp('ns'))],
                                 ids=['date', 'datetime', 'datehour', 'timestamp', 'nanotime', 'nanotimestamp'])
        def test_session_download_table_enableArrow_with_array_vector_4(self, _coltype, _patype):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            t = conn1.run(f"""
                ind = 1 3;
                col = arrayVector(ind, {_coltype}([0,1,NULL]));
                t=table(col);
                t
            """)
            assert isinstance(t, pyarrow.Table)
            ex_t = pyarrow.table({
                'col': pyarrow.array([[0], [3600, None]], type=pyarrow.list_(_patype)) if _coltype == 'datehour'
                else pyarrow.array([[0], [1, None]], type=pyarrow.list_(_patype))
            })
            assert t.equals(ex_t)

        @pytest.mark.ARROW
        def test_session_download_table_enableArrow_with_array_vector_5(self):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            t = conn1.run("""
                ind = 1 3;
                col4 = arrayVector(ind, ipaddr(['', '192.168.1.13', '1:0:0:0:0:0:0:0']));
                col5 = arrayVector(ind, uuid(['', '5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87']));
                col6 = arrayVector(ind, int128(['', 'e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32']));
                t=table(col4,col5,col6);
                t
            """)
            assert isinstance(t, pyarrow.Table)
            ex_t = pyarrow.table({
                'col4': pyarrow.array([[None], ['192.168.1.13', '1::0']], type=pyarrow.list_(pyarrow.string())),
                'col5': pyarrow.array(
                    [[None],
                     [b']!*x\xccH\xe3\xb1B5\xb4\xd9\x14s\xee\x87', b']!*x\xccH\xe3\xb1B5\xb4\xd9\x14s\xee\x87']],
                    type=pyarrow.list_(pyarrow.binary(16))),
                'col6': pyarrow.array([[None], [b'\xe1g\x17\x97\xc5.\x15\xf7c8\x0bE\xe8A\xec2',
                                                b'\xe1g\x17\x97\xc5.\x15\xf7c8\x0bE\xe8A\xec2']],
                                      type=pyarrow.list_(pyarrow.binary(16))),
            })
            assert t.equals(ex_t)

        @pytest.mark.ARROW
        def test_session_download_table_enableArrow_with_array_vector_month(self):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            scripts = "t1 = table(month([0, 1, 1970*12+1, 1970*12+100, NULL]) as cmonth)"
            conn1.run(scripts)
            re: pyarrow.Table = conn1.run("t1")
            conn2 = ddb.session(HOST, PORT, USER, PASSWD)
            conn2.run(scripts)
            col1: np.ndarray = conn2.run('t1["cmonth"]')
            col1 = [x.astype("datetime64[D]").astype("int32")
                    if str(x) != "NaT" else None for x in col1]
            ex = pyarrow.table({
                'cmonth': pyarrow.array(col1, type=pyarrow.date32())
            })
            assert re.equals(ex)

        @pytest.mark.ARROW
        def test_session_download_table_enableArrow_with_unsupported_types(self):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            with pytest.raises(RuntimeError, match="Arrow Not support Decimal ArrayVector"):
                conn1.run("index=1 3 5;val=decimal32(1 2 3 4 5,2);av=arrayVector(index, val);table(av as col1)")
            with pytest.raises(RuntimeError, match="Arrow Not support Decimal ArrayVector"):
                conn1.run("index=1 3 5;val=decimal64(1 2 3 4 5,10);av=arrayVector(index, val);table(av as col1)")

        @pytest.mark.ARROW
        @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
        def test_session_upload_table_enableArrow(self, compress):
            if compress:
                with pytest.raises(RuntimeError, match="The Arrow protocol does not support compression."):
                    ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW, compress=compress)
            else:
                conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW, compress=compress)
                tab = conn1.run("tab = table(1 2 3 as c1, `a`b`c as c2, date(1 5 9) as c3);tab")
                c1 = tab['c1'].to_pylist()
                conn1.upload({'ex_c1': c1})
                assert conn1.run('`tab in objs().name')

        @pytest.mark.ARROW
        def test_upload_download_other_dataforms_with_enableArrow(self):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            ar = conn1.run('ar = [1,`2,NULL,decimal32(2.3,3)];ar')
            arV = conn1.run('arV = arrayVector(1 3, [1,2,3]);arV')
            par = conn1.run('par = pair(1, 3);par')
            matrx = conn1.run('matrx = matrix(1 2 3, 4 5 6);matrx')
            st = conn1.run('st = set(4 5 6);st')
            dit = conn1.run("dit = {'1': 1};dit")
            assert ar == [1, '2', None, decimal.Decimal('2.300')]
            exV = np.array([np.array([1], dtype=np.int32), np.array(
                [2, 3], dtype=np.int32)], dtype='object')
            for j, k in zip(arV, exV):
                assert_array_equal(j, k)
            assert par == [1, 3]
            exM = np.array([[1, 4], [2, 5], [3, 6]], dtype=np.int32)
            assert_array_equal(matrx[0], exM)
            assert st == {4, 5, 6}
            assert dit == {'1': 1}
            conn1.upload({
                "ar2": ar,
                'arV2': arV,
                'par2': par,
                'matrx2': matrx,
                'st2': st,
                'dit2': dit
            })
            assert conn1.run("st2 == st")
            assert conn1.run("dit2 == dit")
            assert conn1.run("each(eqObj, [ar2,arV2,par2,matrx2],[ar,arV,par,matrx])").any()

        @pytest.mark.ARROW
        def test_session_download_table_enableArrow_with_supportedTypes_gt_8192rows(self):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            table1 = conn1.run("""
                cbool = take(false,10000);
                cchar = char(take(3,10000));
                cshort = short(take(3,10000));
                cint = int(take(3,10000));
                clong = long(take(3,10000));
                cdate = date(take(0,10000));
                cmonth = month(take(12,10000));
                ctime = time(take(1,10000));
                cminute = minute(take(1,10000));
                csecond = second(take(1,10000));
                cdatetime = datetime(take(0,10000));
                ctimestamp = timestamp(take(0,10000));
                cnanotime = nanotime(take(0,10000));
                cnanotimestamp= nanotimestamp(take(0,10000));
                cfloat = float(take(3.12437,10000));
                cdouble =  double(take(-3.12437,10000));
                csymbol =symbol(take(`ddvb,10000));
                cstring =take(`ddvb,10000);
                cblob =blob(take(`ddvb,10000));
                cipaddr =  take(ipaddr("192.168.1.13"),10000);
                cuuid = take(uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),10000);
                cint128 = take(int128("e1671797c52e15f763380b45e841ec32"),10000);
                cdecimal32=take(decimal32(0,3),10000);
                cdecimal64=take(decimal64(0,11),10000);
                //cdecimal128=take(decimal128(0,26),10000);
                table1=table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,csymbol,cstring,cblob,cipaddr,cuuid,cint128,cdecimal32,cdecimal64);
                table1
            """)
            assert isinstance(table1, pyarrow.Table)
            assert len(table1) == 10000
            assert table1['cbool'].to_pylist() == [False for _ in range(10000)]
            assert table1['cchar'].to_pylist() == [3 for _ in range(10000)]
            assert table1['cshort'].to_pylist() == [3 for _ in range(10000)]
            assert table1['cint'].to_pylist() == [3 for _ in range(10000)]
            assert table1['clong'].to_pylist() == [3 for _ in range(10000)]
            assert table1['cdate'].to_pylist() == [datetime.date(1970, 1, 1)
                                                   for _ in range(10000)]
            assert table1['cmonth'].to_pylist() == [datetime.date(1, 1, 1)
                                                    for _ in range(10000)]
            assert table1['ctime'].to_pylist() == [datetime.time(0, 0, 0, 1000)
                                                   for _ in range(10000)]
            assert table1['cminute'].to_pylist() == [datetime.time(0, 1)
                                                     for _ in range(10000)]
            assert table1['csecond'].to_pylist() == [datetime.time(0, 0, 1)
                                                     for _ in range(10000)]
            assert table1['cdatetime'].to_pylist() == [datetime.datetime(
                1970, 1, 1, 0, 0) for _ in range(10000)]
            assert table1['ctimestamp'].to_pylist() == [datetime.datetime(
                1970, 1, 1, 0, 0) for _ in range(10000)]
            temp = pyarrow.table({
                'cnanotime': pyarrow.array([0 for _ in range(10000)], type=pyarrow.time64('ns')),
                'cnanotimestamp': pyarrow.array([0 for _ in range(10000)], type=pyarrow.timestamp('ns')),
                'csymbol': pyarrow.chunked_array(
                    pyarrow.DictionaryArray.from_arrays(pyarrow.array([1 for _ in range(10000)],
                                                                      type=pyarrow.int32()),
                                                        pyarrow.array(['', 'ddvb']))),
                'cstring': np.array(['ddvb' for _ in range(10000)], dtype='object'),
                'cblob': pyarrow.array([b'ddvb' for _ in range(10000)], type=pyarrow.large_binary()),
                'cipaddr': np.array(["192.168.1.13" for _ in range(10000)], dtype='object'),
                'cuuid': pyarrow.array([b']!*x\xccH\xe3\xb1B5\xb4\xd9\x14s\xee\x87' for _ in range(10000)],
                                       type=pyarrow.binary(16)),
                'cint128': pyarrow.array([b'\xe1g\x17\x97\xc5.\x15\xf7c8\x0bE\xe8A\xec2' for _ in range(10000)],
                                         type=pyarrow.binary(16)),
                'cdecimal32': pyarrow.array([0 for _ in range(10000)], type=pyarrow.decimal128(38, 3)),
                'cdecimal64': pyarrow.array([0 for _ in range(10000)], type=pyarrow.decimal128(38, 11)),
                # 'cdecimal128': pyarrow.array([0 for _ in range(10000)], type=pyarrow.decimal128(38, 26)),
            })
            assert table1['cnanotime'].equals(temp['cnanotime'])
            assert table1['cnanotimestamp'].equals(temp['cnanotimestamp'])
            assert_array_almost_equal(table1['cfloat'].to_pylist(), [3.12437 for _ in range(10000)])
            assert_array_almost_equal(table1['cdouble'].to_pylist(), [-3.12437 for _ in range(10000)])
            assert table1['csymbol'].equals(temp['csymbol'])
            assert table1['cstring'].equals(temp['cstring'])
            assert table1['cblob'].equals(temp['cblob'])
            assert table1['cipaddr'].equals(temp['cipaddr'])
            assert table1['cuuid'].equals(temp['cuuid'])
            assert table1['cint128'].equals(temp['cint128'])
            assert table1['cdecimal32'].equals(temp['cdecimal32'])
            assert table1['cdecimal64'].equals(temp['cdecimal64'])
            # assert table1['cdecimal128'].equals(temp['cdecimal128'])

        @pytest.mark.ARROW
        def test_session_download_array_vector_table_enableArrow_with_supportedTypes_8192rows(self):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            table1 = conn1.run("""
                cbool = array(BOOL[],0,10000).append!(cut(take(false,30000),3));
                cchar = array(CHAR[],0,10000).append!(cut(take(3c,30000),3));
                cshort = array(SHORT[],0,10000).append!(cut(take(3h,30000),3));
                cint = array(INT[],0,10000).append!(cut(take(3i,30000),3));
                clong = array(LONG[],0,10000).append!(cut(take(3l,30000),3));
                cdate = array(DATE[],0,10000).append!(cut(take(1970.01.01d,30000),3));
                cmonth = array(MONTH[],0,10000).append!(cut(take(1970.01M,30000),3));
                ctime = array(TIME[],0,10000).append!(cut(take(13:30:10.008t,30000),3));
                cminute = array(MINUTE[],0,10000).append!(cut(take(13:30m,30000),3));
                csecond = array(SECOND[],0,10000).append!(cut(take(13:30:10s,30000),3));
                cdatetime = array(DATETIME[],0,10000).append!(cut(take(2012.06.13T13:30:10D,30000),3));
                ctimestamp = array(TIMESTAMP[],0,10000).append!(cut(take(2012.06.13T13:30:10.008T,30000),3));
                cnanotime = array(NANOTIME[],0,10000).append!(cut(take(0,30000),3));
                cnanotimestamp= array(NANOTIMESTAMP[],0,10000).append!(cut(take(0,30000),3));
                cfloat = array(FLOAT[],0,10000).append!(cut(take(2.1f,30000),3));
                cdouble =  array(DOUBLE[],0,10000).append!(cut(take(-2.1F,30000),3));
                cipaddr =  array(IPADDR[],0,10000).append!(cut(take(ipaddr("192.168.1.13"),30000),3));
                cuuid = array(UUID[],0,10000).append!(cut(take(uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),30000),3));
                cint128 = array(INT128[],0,10000).append!(cut(take(int128("e1671797c52e15f763380b45e841ec32"),30000),3));
                table1=table(cbool,cchar,cshort,cint,clong,cdate,cmonth,ctime,cminute,csecond,cdatetime,ctimestamp,cnanotime,cnanotimestamp,cfloat,cdouble,cipaddr,cuuid,cint128);
                table1
            """)
            assert isinstance(table1, pyarrow.Table)
            assert len(table1) == 10000
            assert table1['cbool'].to_pylist() == [[False, False, False] for i in range(10000)]
            assert table1['cchar'].to_pylist() == [[3, 3, 3] for i in range(10000)]
            assert table1['cshort'].to_pylist() == [[3, 3, 3] for i in range(10000)]
            assert table1['cint'].to_pylist() == [[3, 3, 3] for i in range(10000)]
            assert table1['clong'].to_pylist() == [[3, 3, 3] for i in range(10000)]
            _ = datetime.date(1970, 1, 1)
            assert table1['cdate'].to_pylist() == [[_, _, _] for i in range(10000)]
            assert table1['cmonth'].to_pylist() == [[_, _, _] for i in range(10000)]
            _ = datetime.time(13, 30, 10, 8000)
            assert table1['ctime'].to_pylist() == [[_, _, _] for i in range(10000)]
            _ = datetime.time(13, 30)
            assert table1['cminute'].to_pylist() == [[_, _, _] for i in range(10000)]
            _ = datetime.time(13, 30, 10)
            assert table1['csecond'].to_pylist() == [[_, _, _] for i in range(10000)]
            _ = datetime.datetime(2012, 6, 13, 13, 30, 10)
            assert table1['cdatetime'].to_pylist() == [[_, _, _] for i in range(10000)]
            _ = datetime.datetime(2012, 6, 13, 13, 30, 10, 8000)
            assert table1['ctimestamp'].to_pylist() == [[_, _, _] for i in range(10000)]
            temp = pyarrow.table({
                'cnanotime': pyarrow.array([[0, 0, 0] for _ in range(10000)], type=pyarrow.list_(pyarrow.time64('ns'))),
                'cnanotimestamp': pyarrow.array([[0, 0, 0] for _ in range(10000)],
                                                type=pyarrow.list_(pyarrow.timestamp('ns'))),
                'cipaddr': pyarrow.array([["192.168.1.13", "192.168.1.13", "192.168.1.13"] for _ in range(10000)],
                                         type=pyarrow.list_(pyarrow.utf8())),
                'cuuid': pyarrow.array([[b']!*x\xccH\xe3\xb1B5\xb4\xd9\x14s\xee\x87',
                                         b']!*x\xccH\xe3\xb1B5\xb4\xd9\x14s\xee\x87',
                                         b']!*x\xccH\xe3\xb1B5\xb4\xd9\x14s\xee\x87'] for _ in range(10000)],
                                       type=pyarrow.list_(pyarrow.binary(16))),
                'cint128': pyarrow.array([[b'\xe1g\x17\x97\xc5.\x15\xf7c8\x0bE\xe8A\xec2',
                                           b'\xe1g\x17\x97\xc5.\x15\xf7c8\x0bE\xe8A\xec2',
                                           b'\xe1g\x17\x97\xc5.\x15\xf7c8\x0bE\xe8A\xec2'] for _ in range(10000)],
                                         type=pyarrow.list_(pyarrow.binary(16))),
            })
            assert table1['cnanotime'].equals(temp['cnanotime'])
            assert table1['cnanotimestamp'].equals(temp['cnanotimestamp'])
            assert_array_almost_equal(table1['cfloat'].to_pylist(), [[2.1, 2.1, 2.1] for _ in range(10000)])
            assert_array_almost_equal(
                table1['cdouble'].to_pylist(), [[-2.1, -2.1, -2.1] for _ in range(10000)])
            assert table1['cipaddr'].equals(temp['cipaddr'])
            assert table1['cuuid'].equals(temp['cuuid'])
            assert table1['cint128'].equals(temp['cint128'])

        @pytest.mark.ARROW
        def test_session_enableArrow_with_type_not_support(self):
            loadPlugin()
            conn1 = ddb.session(HOST, PORT, protocol=keys.PROTOCOL_ARROW)
            with pytest.raises(RuntimeError):
                conn1.run("table(complex([2,3],[4,5]) as `complex)")
            with pytest.raises(RuntimeError):
                conn1.run("table(array(COMPLEX[]).append!([complex([2,3],[4,5]),complex([2,3],[4,5])]) as `complex)")
            with pytest.raises(RuntimeError):
                conn1.run("table(array(COMPLEX) as `complex)")
            with pytest.raises(RuntimeError):
                conn1.run("table(array(COMPLEX[]) as `complex)")

    def test_session_func_loadTableBySQL_when_define_a_para_called_db(self):
        dbPath = "dfs://test_func_loadtablebysql"
        tableName = "tmp"
        if self.conn.existsDatabase(dbPath):
            self.conn.dropDatabase(dbPath)
        db = self.conn.database(dbName='mydb', partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=dbPath)
        df = pd.DataFrame({'c1': np.array([1, 2, 3], dtype='int32'), 'c2': ['a', 'b', 'c']})
        t = self.conn.table(data=df)
        db.createPartitionedTable(t, tableName, partitionColumns='c1').append(t)
        self.conn.run("db = 'str_tmp'")
        res = self.conn.loadTableBySQL(
            tableName, dbPath, sql=f"select * from {tableName} where c1 =3").toDF()
        assert_frame_equal(res, pd.DataFrame({'c1': np.array([3], dtype='int32'), 'c2': ['c']}))
        assert self.conn.run("db") == 'str_tmp'

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_Session_connect_close(self, _compress, _pickle):
        conn1 = ddb.Session(HOST, PORT, USER, PASSWD, compress=_compress, enablePickle=_pickle)
        ans = conn1.run("1+2")
        assert ans == 3
        c3 = conn1.isClosed()
        assert not c3
        conn1.close()
        with pytest.raises(Exception):
            conn1.run("1+2")
        c4 = conn1.isClosed()
        assert c4

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_print_msg_in_console(self, _compress, _pickle):
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
                                 "import asyncio;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}',compress={_compress},enablePickle={_pickle});"
                                 f"conn.run(\"\"\"{script}\"\"\")"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == """1\n1\n1\n1\n1\n1970.01.02\n0000.02M\n00:00:00.001\n00:01m\n00:00:01\n1970.01.01T00:00:01\n1970.01.01T00:00:00.001\n00:00:00.000000001\n1970.01.01T00:00:00.000000001\n1\n1\n1\n5d212a78-cc48-e3b1-4235-b4d91473ee87\n["1"]\ncol1 col2\n---- ----\n1    a   \n2    b   \n3    c   \n\n[[9],[9],[9]]\n"""

    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_disable_print_msg_in_console(self, _compress, _pickle):
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
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}',compress={_compress},enablePickle={_pickle},show_output=False);"
                                 f"conn.run(\"\"\"{script}\"\"\")"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout == ""

    def test_session_enablessl_download_huge_table(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True)
        s = """
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

    def test_session_enablessl_download_huge_array(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True)
        vec = conn1.run("take('/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8', 3000000)")
        assert_array_equal(vec, ["/DBA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA.DBAA/Key1/8"] * 3000000)

    def test_session_enablessl_download_huge_arrayVector(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableSSL=True)
        av = conn1.run("""
            val = take(123456, 3000000)
            ind = 1..3000000
            arrayVector(ind, val)
        """)
        for v in av:
            assert v == [123456]

    @pytest.mark.parametrize('_priority', [-1, 'a', 1.1, dict(), list(), tuple(), set()])
    def test_run_with_para_priority_exception(self, _priority):
        with pytest.raises(Exception, match="priority must be an integer from 0 to 9"):
            self.conn.run("objs();sleep(2000)", priority=_priority)

    @pytest.mark.parametrize('_parallelism', [-1, 'a', 1.1, dict(), list(), tuple(), set()])
    def test_run_with_para_parallelism_exception(self, _parallelism):
        with pytest.raises(Exception, match="parallelism must be an integer greater than 0"):
            self.conn.run("objs();sleep(2000)", parallelism=_parallelism)

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

    def test_run_fetchSize(self):
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

    @pytest.mark.skipif(AUTO_TESTING, reason="auto test not support")
    def test_session_highAvailability_load_balance(self):
        host = "192.168.0.54"
        port0 = 9900
        port1 = 9902
        port2 = 9903
        port3 = 9904
        user = "admin"
        passwd = "123456"
        sites = [f"{host}:{port}" for port in (port1, port2, port3)]
        conn = ddb.Session(host, port0, user, passwd)
        connList = [ddb.Session() for i in range(30)]
        for i in connList:
            time.sleep(1)
            i.connect(host=host, port=port0, userid=user, password=passwd, highAvailability=True,
                      highAvailabilitySites=sites)
        loadDf = conn.run(
            "select (connectionNum + workerNum + executorNum)/3.0 as load from rpc(getControllerAlias(), getClusterPerf) where mode=0")
        assert all(((loadDf - loadDf.mean()).abs() < 0.4)['load'])
        print(conn.run(
            "select site,(connectionNum + workerNum + executorNum)/3.0 as load,connectionNum,workerNum,executorNum from rpc(getControllerAlias(), getClusterPerf) where mode=0"))
        conn.run(f"stopDataNode(['{host}:{port1}'])")
        for i in connList:
            time.sleep(1)
            assert i.run("true")
        conn.run(f"startDataNode(['{host}:{port1}'])")
        time.sleep(3)
        # print(conn.run("select site,(connectionNum + workerNum + executorNum)/3.0 as load,connectionNum,workerNum,executorNum from rpc(getControllerAlias(), getClusterPerf) where mode=0"))
        # conn.run(f"stopDataNode(['{host}:{port2}'])")
        # for i in connList:
        #     time.sleep(1)
        #     assert i.run("true")
        # conn.run(f"startDataNode(['{host}:{port2}'])")
        # time.sleep(3)
        # print(conn.run("select site,(connectionNum + workerNum + executorNum)/3.0 as load,connectionNum,workerNum,executorNum from rpc(getControllerAlias(), getClusterPerf) where mode=0"))
        #
        # conn.run(f"stopDataNode(['{host}:{port3}'])")
        # for i in connList:
        #     time.sleep(1)
        #     assert i.run("true")
        # conn.run(f"startDataNode(['{host}:{port3}'])")
        # time.sleep(3)
        # print(conn.run("select site,(connectionNum + workerNum + executorNum)/3.0 as load,connectionNum,workerNum,executorNum from rpc(getControllerAlias(), getClusterPerf) where mode=0"))
        for i in connList:
            i.close()

    def test_session_sqlStd(self):
        conn_ddb=ddb.Session(HOST,PORT,USER,PASSWD,sqlStd=keys.SqlStd.DolphinDB)
        conn_oracle=ddb.Session(HOST,PORT,USER,PASSWD,sqlStd=keys.SqlStd.Oracle)
        conn_myqsql=ddb.Session(HOST,PORT,USER,PASSWD,sqlStd=keys.SqlStd.MySQL)
        with pytest.raises(RuntimeError):
            conn_ddb.run("sysdate()")
        conn_oracle.run("sysdate()")
        conn_myqsql.run("sysdate()")

    def test_session_tryReconnectNums_conn(self):
        n=3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "conn=ddb.Session();"
                                 f"res=conn.connect('{HOST}', 56789, '{USER}', '{PASSWD}', reconnect=True, tryReconnectNums={n});"
                                 f"assert not res;"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout.count("Failed")==n
        assert result.stderr==f"Connect to {HOST}:56789 failed after {n} reconnect attempts.\n"

    def test_session_tryReconnectNums_run(self):
        n=3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "conn=ddb.Session();"
                                 f"res=conn.connect('{HOST}', 56789, '{USER}', '{PASSWD}', reconnect=True, tryReconnectNums={n});"
                                 "assert not res;"
                                 "conn.run('1+1');"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert result.stdout.count("Failed")==n*2
        assert f"Connect to {HOST}:56789 failed after {n} reconnect attempts." in result.stderr
        print(result.stderr)

    @pytest.mark.skipif(AUTO_TESTING, reason="auto test not support")
    def test_session_highAvailability_tryReconnectNums_conn(self):
        host = "192.168.0.54"
        port0 = 9900
        port1 = 9902
        port2 = 9903
        port3 = 9904
        user = "admin"
        passwd = "123456"
        conn = ddb.Session(host, port0, user, passwd)
        conn.run(f"stopDataNode(['{host}:{port1}','{host}:{port2}','{host}:{port3}'])")
        n=3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "conn_=ddb.Session();"
                                 f"conn_.connect('{host}',{port1},'{user}','{passwd}', highAvailability=True, highAvailabilitySites=['{host}:{port1}','{host}:{port2}','{host}:{port3}'], tryReconnectNums={n});"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        conn.run(f"startDataNode(['{host}:{port1}','{host}:{port2}','{host}:{port3}'])")
        assert result.stdout.count("Failed")==n*3
        assert f"Connect failed after {n} reconnect attempts for every node in high availability sites." in result.stderr

    @pytest.mark.skipif(AUTO_TESTING, reason="auto test not support")
    def test_session_highAvailability_tryReconnectNums_run(self):
        host = "192.168.0.54"
        port0 = 9900
        port1 = 9902
        port2 = 9903
        port3 = 9904
        user = "admin"
        passwd = "123456"
        conn = ddb.Session(host, port0, user, passwd)
        n=3
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{host}',{port0},'{user}','{passwd}');"
                                 "conn_=ddb.Session();"
                                 f"conn_.connect('{host}',{port1},'{user}','{passwd}', highAvailability=True, highAvailabilitySites=['{host}:{port1}','{host}:{port2}','{host}:{port3}'], tryReconnectNums={n});"
                                 f"""conn.run("stopDataNode(['{host}:{port1}','{host}:{port2}','{host}:{port3}'])");"""
                                 "conn_.run('true');"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        conn.run(f"startDataNode(['{host}:{port1}','{host}:{port2}','{host}:{port3}'])")
        assert result.stdout.count("Failed")==n*3
        assert f"Connect to nodes failed after {n} reconnect attempts." in result.stderr

    def test_session_readTimeout(self):
        conn = ddb.Session()
        conn.connect(HOST,PORT,USER,PASSWD,readTimeout=3)
        assert conn.run('true')
        with pytest.raises(RuntimeError):
            conn.run('sleep(4000);true')

    def test_session_writeTimeout(self):
        conn = ddb.Session()
        conn.connect(HOST,PORT,USER,PASSWD,writeTimeout=3)
        conn.upload({'a':True})
        assert conn.run('a')
        # set packet loss to test
