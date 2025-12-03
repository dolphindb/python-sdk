#!/usr/bin/python3
# -*- coding:utf-8 -*-
""""
@Author: shijianbo
@Time: 2024/1/23 11:27
@Note: 
"""
import socket
import sys
import time
from importlib.util import find_spec
from threading import Thread

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest
from dolphindb.utils import get_types_from_schema

from basic_testing.prepare import DataUtils
from basic_testing.prepare import PANDAS_VERSION
from basic_testing.utils import equalPlus
from setup.settings import HOST, PORT, USER, PASSWD

if find_spec("pyarrow") is not None:
    import pyarrow as pa
from dolphindb._core import DolphinDBRuntime

ddbcpp = DolphinDBRuntime()._ddbcpp

ddbcpp.init()


def http_request(host, port, path="/", method="GET"):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    request = f"{method} {path} HTTP/1.1\r\nHost: {host}\r\n\r\n"
    client_socket.sendall(request.encode())
    response = client_socket.recv(1024)
    client_socket.close()
    return response.decode()


class TestOther:
    conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=False)

    def test_other_DBConnectionPool_cov(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        # run+taskid
        pool.pool.run('print 123', pool.taskId)
        while True:
            isFinished = pool.pool.isFinished(pool.taskId)
            if isFinished == 0:
                time.sleep(0.1)
            else:
                pool.taskId += 1
                break
        # run+taskid+args
        pool.pool.run('print', pool.taskId, 1)
        while True:
            isFinished = pool.pool.isFinished(pool.taskId)
            if isFinished == 0:
                time.sleep(0.1)
            else:
                pool.taskId += 1
                break
        # run+taskid+kwargs,not contain clearMemory
        pool.pool.run('print 1', pool.taskId, pickleTableToList=True)
        while True:
            isFinished = pool.pool.isFinished(pool.taskId)
            if isFinished == 0:
                time.sleep(0.1)
            else:
                pool.taskId += 1
                break
        # run+taskid+kwargs,not contain clearMemory
        pool.pool.run('print', pool.taskId, 1, pickleTableToList=True, priority=4, parallelism=2)
        while True:
            isFinished = pool.pool.isFinished(pool.taskId)
            if isFinished == 0:
                time.sleep(0.1)
            else:
                pool.taskId += 1
                break

    @pytest.mark.skipif(not sys.platform.lower().startswith('linux'), reason="only support in linux")
    def test_other_Session_cov(self):

        conn = ddb.Session(HOST, PORT, USER, PASSWD)

        conn.upload({
            b'a': 1
        })

        # setTimeout
        conn.setTimeout(10)

        # nullValueToZero
        # conn.nullValueToZero()

        # nullValueToNan
        # conn.nullValueToNan()

        with pytest.raises(RuntimeError, match='non-string key in upload dictionary is not allowed'):
            conn.upload({
                None: 1,
            })

        conn_async = ddb.Session(HOST, PORT, USER, PASSWD, enableASYNC=True)
        addr = conn_async.upload({
            'a': 1
        })
        assert addr == -1

        # run+args+kwargs
        conn.run('print', 1, priority=4, parallelism=2)

        # runBlock
        with pytest.raises(RuntimeError, match='fetchSize must be greater than 8192'):
            conn.cpp.runBlock('table(take(1,10000) as a)', clearMemory=True)

        # hashBucket
        assert conn.hashBucket(1, 2) == 1
        conn.hashBucket(np.datetime64(0, 'ns'), 2)
        conn.hashBucket("1", 2)
        with pytest.raises(RuntimeError, match='Key must be integer, date/time, or string.'):
            conn.hashBucket(1.1, 2)
        print(conn.hashBucket([1, 2, 3], 4))

        # printPerformance
        # conn._printPerformance()

        # enableJobCancellation
        ddb.Session.enableJobCancellation()
        with pytest.raises(RuntimeError, match='Job cancellation is already enabled'):
            ddb.Session.enableJobCancellation()

        def inner_func():
            conn.cpp.run('sleep(1)')
            conn.cpp.run('sleep', 1)
            conn.cpp.run('sleep(1)', clearMemory=True)
            conn.cpp.run('sleep', 1, clearMemory=True, pickleTableToList=True)

        _ = [Thread(target=inner_func) for _ in range(100)]
        for i in _:
            i.start()
        for i in _:
            i.join()

    def test_other_BTW_cov(self):
        with pytest.raises(RuntimeError, match='Failed to connect to server'):
            writer = ddb.BatchTableWriter("127.0.0.1", 0, USER, PASSWD)
            writer.addTable(tableName="tglobal")
        writer = ddb.BatchTableWriter(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,
                           match='Failed to get unwritten data. Please use addTable to add infomation of database and table first.'):
            writer.getUnwrittenData(tableName="tglobal")

    def test_other_DdbPythonUtil_cov(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        # tuple,series
        conn.upload({
            'a': (1, 2, 3),
            'b': pd.Series([1, 2, 3]),
            'c': pd.Index([1, 2, 3]),
            'd': np.array(["1970-01-01"], dtype="datetime64")
        })
        with pytest.raises(RuntimeError, match='Conversion failed'):
            df = pd.DataFrame({'a': [0]})
            df.__DolphinDB_Type__ = {
                'a': (1,)
            }
            conn.upload({
                'e': df
            })
        with pytest.raises(RuntimeError, match='Conversion failed'):
            df = pd.DataFrame({'a': [0]})
            df.__DolphinDB_Type__ = {
                'a': []
            }
            conn.upload({
                'e': df
            })
        with pytest.raises(RuntimeError, match='Conversion failed'):
            df = pd.DataFrame({'a': [0]})
            df.__DolphinDB_Type__ = {
                'a': [1, 2, 3]
            }
            conn.upload({
                'e': df
            })
        conn.upload({
            'ee': np.datetime64(None)
        })
        assert conn.run("isNull(ee)")
        with pytest.raises(RuntimeError, match="Cannot convert"):
            conn.upload({
                'f': np.datetime64('2020-01-01', 'ps')
            })
        conn.upload({
            'f': np.str_("123")
        })
        assert conn.run("f=='123'")
        with pytest.raises(RuntimeError):
            conn.upload({
                'g': np.complex64(0)
            })
        with pytest.raises(RuntimeError,
                           match='Cannot create a Matrix from the given numpy.ndarray with dimension greater than 2.'):
            conn.upload({
                'g': np.array([[[1, 2, 3]]])
            })
        conn.upload({
            'g': np.array([[1, 2]], dtype='object'),
            'h': np.matrix([[1, 2]])
        })
        # conn.upload({
        #     'i':pd.Series([1,2,3],dtype=pd.Int32Dtype())
        # })
        with pytest.raises(RuntimeError):
            conn.upload({
                'i': pd.Series([1, 2, 3], dtype='complex128')
            })
        with pytest.raises(RuntimeError, match="Cannot create a Set with mixing incompatible types."):
            conn.upload({
                'i': {'1', 1.1}
            })
        # with pytest.raises(RuntimeError, match="can not create all None vector in dictionary"):
        #     conn.upload({
        #         'i': dict()
        #     })
        # with pytest.raises(RuntimeError, match="can not create all None vector in dictionary"):
        #     conn.upload({
        #         'i': {1: None}
        #     })
        if PANDAS_VERSION >= (2, 0, 0) and find_spec("pyarrow") is not None:
            from basic_testing.prepare import PYARROW_VERSION
            if PYARROW_VERSION >= (10, 0, 1):
                with pytest.raises(RuntimeError, match="Cannot convert"):
                    conn.upload({
                        'i': pd.DataFrame({'a': [1, 2, 3]}, dtype=pd.ArrowDtype(pa.decimal256(3, 2)))
                    })
        assert equalPlus(conn.run("symbol(`a`b`c)"), np.array(['a', 'b', 'c'], dtype='object'))
        with pytest.raises(RuntimeError,
                           match="Only dictionary with string, symbol or integral keys can be converted to dict"):
            conn.run('dict(FLOAT,INT)')
        assert conn.run('dict(SYMBOL,INT)') == {}

    def test_other_setType_cov(self):
        for k, v in DataUtils.DATA_UPLOAD.items():
            for i in range(41):
                x_dtype = pd.DataFrame({'a': [v['value'], v['value'], v['value']]}, dtype=v['dtype'])
                x_object = pd.DataFrame({'a': [v['value'], v['value'], v['value']]}, dtype='object')
                x_dtype.__DolphinDB_Type__ = {
                    'a': i
                }
                x_object.__DolphinDB_Type__ = {
                    'a': i
                }
                try:
                    self.__class__.conn.upload({'x_dtype': x_dtype, 'x_object': x_object})
                except Exception:
                    pass
        for k, v in DataUtils.DATA_UPLOAD.items():
            for i in range(64, 104):
                x_object = pd.DataFrame({'a': [v['value'], v['value'], v['value']]}, dtype='object')
                x_object.__DolphinDB_Type__ = {
                    'a': i
                }
                try:
                    self.__class__.conn.upload({'x_object': x_object})
                except Exception:
                    pass
        if hasattr(DataUtils, "DATA_UPLOAD_ARROW") and PANDAS_VERSION >= (2, 0, 0) and find_spec("pyarrow") is not None:
            for k, v in DataUtils.DATA_UPLOAD_ARROW.items():
                for i in range(41):
                    x_dtype = pd.DataFrame({'a': [v['value'], v['value'], v['value']]},
                                           dtype=pd.ArrowDtype(v['dtype_arrow']))
                    x_dtype.__DolphinDB_Type__ = {
                        'a': i
                    }
                    print(i, k)
                    try:
                        self.__class__.conn.upload({'x_dtype': x_dtype})
                    except Exception:
                        pass
            for k, v in DataUtils.DATA_UPLOAD_ARROW.items():
                for i in range(64, 104):
                    x_dtype = pd.DataFrame({'a': [[v['value'], v['value'], v['value']]]},
                                           dtype=pd.ArrowDtype(pa.list_(v['dtype_arrow'])))
                    x_dtype.__DolphinDB_Type__ = {
                        'a': i
                    }
                    print(i, k)
                    try:
                        self.__class__.conn.upload({'x_dtype': x_dtype})
                    except Exception:
                        pass

    def test_other_secure_get_any_file(self):
        assert "can't access file not in web dir." in http_request(HOST, PORT, "/../dolphindb.lic\x00.html")
        assert "can't access file not in web dir." not in http_request(HOST, PORT, "/version.json")

    def test_other_upload_and_download(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.upload({'x': [None, np.int32(1), np.int64(2)]})
        x = conn.run("x")
        expect = np.array([np.nan, 1., 2.], dtype='float64')
        assert equalPlus(x, expect)

    def test_other_get_types_from_schema(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        schema = conn.run("""
            schema(
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
            )
        """)
        result = get_types_from_schema(schema['colDefs'])
        assert result['bool'] == ['BOOL', None]
        assert result['char'] == ['CHAR', None]
        assert result['short'] == ['SHORT', None]
        assert result['int'] == ['INT', None]
        assert result['long'] == ['LONG', None]
        assert result['date'] == ['DATE', None]
        assert result['month'] == ['MONTH', None]
        assert result['time'] == ['TIME', None]
        assert result['minute'] == ['MINUTE', None]
        assert result['second'] == ['SECOND', None]
        assert result['datetime'] == ['DATETIME', None]
        assert result['timestamp'] == ['TIMESTAMP', None]
        assert result['nanotime'] == ['NANOTIME', None]
        assert result['nanotimestamp'] == ['NANOTIMESTAMP', None]
        assert result['float'] == ['FLOAT', None]
        assert result['double'] == ['DOUBLE', None]
        assert result['string'] == ['STRING', None]
        assert result['uuid'] == ['UUID', None]
        assert result['datehour'] == ['DATEHOUR', None]
        assert result['ipaddr'] == ['IPADDR', None]
        assert result['int128'] == ['INT128', None]
        assert result['blob'] == ['BLOB', None]
        assert result['decimal32'] == ['DECIMAL32', 2]
        assert result['decimal64'] == ['DECIMAL64', 3]
        assert result['decimal128'] == ['DECIMAL128', 4]
        assert result['any'] == ['ANY', None]
        assert result['bool_av'] == ['BOOL[]', None]
        assert result['char_av'] == ['CHAR[]', None]
        assert result['short_av'] == ['SHORT[]', None]
        assert result['int_av'] == ['INT[]', None]
        assert result['long_av'] == ['LONG[]', None]
        assert result['date_av'] == ['DATE[]', None]
        assert result['month_av'] == ['MONTH[]', None]
        assert result['time_av'] == ['TIME[]', None]
        assert result['minute_av'] == ['MINUTE[]', None]
        assert result['second_av'] == ['SECOND[]', None]
        assert result['datetime_av'] == ['DATETIME[]', None]
        assert result['timestamp_av'] == ['TIMESTAMP[]', None]
        assert result['nanotime_av'] == ['NANOTIME[]', None]
        assert result['nanotimestamp_av'] == ['NANOTIMESTAMP[]', None]
        assert result['float_av'] == ['FLOAT[]', None]
        assert result['double_av'] == ['DOUBLE[]', None]
        assert result['uuid_av'] == ['UUID[]', None]
        assert result['datehour_av'] == ['DATEHOUR[]', None]
        assert result['ipaddr_av'] == ['IPADDR[]', None]
        assert result['int128_av'] == ['INT128[]', None]
        assert result['decimal32_av'] == ['DECIMAL32[]', 2]
        assert result['decimal64_av'] == ['DECIMAL64[]', 3]
        assert result['decimal128_av'] == ['DECIMAL128[]', 4]

    def test_other_get_types_from_schema_no_extra(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        schema = conn.run("""
            schema(
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
                    array(INT128[]) as int128_av
                )
            )
        """)
        result = get_types_from_schema(schema['colDefs'].drop(columns=["extra"]))
        assert result['bool'] == ['BOOL', None]
        assert result['char'] == ['CHAR', None]
        assert result['short'] == ['SHORT', None]
        assert result['int'] == ['INT', None]
        assert result['long'] == ['LONG', None]
        assert result['date'] == ['DATE', None]
        assert result['month'] == ['MONTH', None]
        assert result['time'] == ['TIME', None]
        assert result['minute'] == ['MINUTE', None]
        assert result['second'] == ['SECOND', None]
        assert result['datetime'] == ['DATETIME', None]
        assert result['timestamp'] == ['TIMESTAMP', None]
        assert result['nanotime'] == ['NANOTIME', None]
        assert result['nanotimestamp'] == ['NANOTIMESTAMP', None]
        assert result['float'] == ['FLOAT', None]
        assert result['double'] == ['DOUBLE', None]
        assert result['string'] == ['STRING', None]
        assert result['uuid'] == ['UUID', None]
        assert result['datehour'] == ['DATEHOUR', None]
        assert result['ipaddr'] == ['IPADDR', None]
        assert result['int128'] == ['INT128', None]
        assert result['blob'] == ['BLOB', None]
        assert result['any'] == ['ANY', None]
        assert result['bool_av'] == ['BOOL[]', None]
        assert result['char_av'] == ['CHAR[]', None]
        assert result['short_av'] == ['SHORT[]', None]
        assert result['int_av'] == ['INT[]', None]
        assert result['long_av'] == ['LONG[]', None]
        assert result['date_av'] == ['DATE[]', None]
        assert result['month_av'] == ['MONTH[]', None]
        assert result['time_av'] == ['TIME[]', None]
        assert result['minute_av'] == ['MINUTE[]', None]
        assert result['second_av'] == ['SECOND[]', None]
        assert result['datetime_av'] == ['DATETIME[]', None]
        assert result['timestamp_av'] == ['TIMESTAMP[]', None]
        assert result['nanotime_av'] == ['NANOTIME[]', None]
        assert result['nanotimestamp_av'] == ['NANOTIMESTAMP[]', None]
        assert result['float_av'] == ['FLOAT[]', None]
        assert result['double_av'] == ['DOUBLE[]', None]
        assert result['uuid_av'] == ['UUID[]', None]
        assert result['datehour_av'] == ['DATEHOUR[]', None]
        assert result['ipaddr_av'] == ['IPADDR[]', None]
        assert result['int128_av'] == ['INT128[]', None]
