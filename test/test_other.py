#!/usr/bin/python3
# -*- coding:utf-8 -*-
""""
@Author: shijianbo
@Time: 2024/1/23 11:27
@Note: 
"""
import time
from threading import Thread
import pytest
from pandas2_testing.prepare import PANDAS_VERSION
from pandas2_testing.utils import equalPlus
from setup.prepare import *
from setup.settings import *
from setup.utils import get_pid
import sys
from dolphindb.session import ddbcpp
import pyarrow as pa

ddbcpp.init()

class TestOther():
    conn = ddb.session(enablePickle=False)

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
                f.write(cls.__name__ + ' start, pid: ' + get_pid() +'\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def test_DBConnectionPool_cov(self):
        pool = ddb.DBConnectionPool(HOST, PORT, 1, USER, PASSWD)
        # run+taskid
        pool.pool.run('print 123',pool.taskId)
        while True:
            isFinished = pool.pool.isFinished(pool.taskId)
            if isFinished == 0:
                time.sleep(0.1)
            else:
                pool.taskId+=1
                break
        # run+taskid+args
        pool.pool.run('print',pool.taskId,1)
        while True:
            isFinished = pool.pool.isFinished(pool.taskId)
            if isFinished == 0:
                time.sleep(0.1)
            else:
                pool.taskId+=1
                break
        # run+taskid+kwargs,not contain clearMemory
        pool.pool.run('print 1',pool.taskId,test=True)
        while True:
            isFinished = pool.pool.isFinished(pool.taskId)
            if isFinished == 0:
                time.sleep(0.1)
            else:
                pool.taskId+=1
                break
        # run+taskid+kwargs,not contain clearMemory
        pool.pool.run('print',pool.taskId,1,pickleTableToList=True,priority=4,parallelism=2)
        while True:
            isFinished = pool.pool.isFinished(pool.taskId)
            if isFinished == 0:
                time.sleep(0.1)
            else:
                pool.taskId+=1
                break


    @pytest.mark.skipif(not sys.platform.lower().startswith('linux'),reason="only support in linux")
    def test_Session_cov(self):

        conn=ddb.Session(HOST, PORT, USER, PASSWD)

        conn.upload({
            b'a':1
        })

        # setTimeout
        conn.setTimeout(10)

        # nullValueToZero
        conn.nullValueToZero()

        # nullValueToNan
        conn.nullValueToNan()

        with pytest.raises(RuntimeError,match='non-string key in upload dictionary is not allowed'):
            conn.upload({
                None:1,
            })

        conn_async=ddb.Session(HOST, PORT, USER, PASSWD,enableASYNC=True)
        addr=conn_async.upload({
            'a':1
        })
        assert addr==-1

        # run+args+kwargs
        conn.run('print',1,priority=4,parallelism=2)

        # runBlock
        with pytest.raises(RuntimeError,match='<Exception> in run: fectchSize must be greater than 8192'):
            conn.cpp.runBlock('table(take(1,10000) as a)',clearMemory=True)

        # hashBucket
        assert conn.hashBucket(1,2)==1
        conn.hashBucket(np.datetime64(0,'ns'), 2)
        conn.hashBucket("1", 2)
        with pytest.raises(RuntimeError,match='Key must be integer, date/time, or string.'):
            conn.hashBucket(1.1,2)
        print(conn.hashBucket([1,2,3],4))

        # printPerformance
        conn._printPerformance()

        # enableJobCancellation
        ddb.Session.enableJobCancellation()
        with pytest.raises(RuntimeError,match='Job cancellation is already enabled'):
            ddb.Session.enableJobCancellation()

        def inner_func():
            conn.cpp.run('sleep(1)')
            conn.cpp.run('sleep',1)
            conn.cpp.run('sleep(1)',clearMemory=True)
            conn.cpp.run('sleep',1, clearMemory=True,pickleTableToList=True)
        _=[Thread(target=inner_func) for i in range(100)]
        for i in _:
            i.start()
        for i in _:
            i.join()

    def test_BTW_cov(self):
        with pytest.raises(RuntimeError,match='<Exception> in addTable: Failed to connect to server.'):
            writer = ddb.BatchTableWriter("127.0.0.1", 0, USER, PASSWD)
            writer.addTable(tableName="tglobal")
        writer = ddb.BatchTableWriter(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError,match='<Exception> in getUnwrittenData: Failed to get unwritten data. Please use addTable to add infomation of database and table first.'):
            writer.getUnwrittenData(tableName="tglobal")

    def test_DdbPythonUtil_cov(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        # tuple,series
        conn.upload({
            'a':(1,2,3),
            'b':pd.Series([1,2,3]),
            'c':pd.Index([1,2,3]),
            'd':np.array(["1970-01-01"],dtype="datetime64")
        })
        with pytest.raises(RuntimeError,match='Error Form of TableChecker.'):
            df=pd.DataFrame({'a':[0]})
            df.__DolphinDB_Type__={
                'a':(1,)
            }
            conn.upload({
                'e':df
            })
        with pytest.raises(RuntimeError,match='Error Form of TableChecker.'):
            df=pd.DataFrame({'a':[0]})
            df.__DolphinDB_Type__={
                'a':[]
            }
            conn.upload({
                'e':df
            })
        with pytest.raises(RuntimeError,match='Error Form of TableChecker.'):
            df=pd.DataFrame({'a':[0]})
            df.__DolphinDB_Type__={
                'a':[1,2,3]
            }
            conn.upload({
                'e':df
            })
        conn.upload({
            'ee':  np.datetime64(None)
        })
        assert conn.run("isNull(ee)")
        with pytest.raises(RuntimeError,match="unsupported datetime type 'datetime64\[ps\]'"):
            conn.upload({
                'f': np.datetime64('2020-01-01','ps')
            })
        conn.upload({
            'f':np.str_("123")
        })
        assert conn.run("f=='123'")
        with pytest.raises(RuntimeError):
            conn.upload({
                'g':np.complex64(0)
            })
        with pytest.raises(RuntimeError,match='numpy.ndarray with dimension > 2 is not supported'):
            conn.upload({
                'g':np.array([[[1,2,3]]])
            })
        conn.upload({
            'g':np.array([[1,2]],dtype='object'),
            'h':np.matrix([[1,2]])
        })
        # conn.upload({
        #     'i':pd.Series([1,2,3],dtype=pd.Int32Dtype())
        # })
        with pytest.raises(RuntimeError):
            conn.upload({
                'i':pd.Series([1,2,3],dtype='complex128')
            })
        with pytest.raises(RuntimeError,match="set in DolphinDB doesn't support multiple types"):
            conn.upload({
                'i':{1,1.1}
            })
        with pytest.raises(RuntimeError, match="can not create all None vector in dictionary"):
            conn.upload({
                'i':dict()
            })
        with pytest.raises(RuntimeError, match="can not create all None vector in dictionary"):
            conn.upload({
                'i':{1:None}
            })
        if PANDAS_VERSION>=(2,0,0):
            with pytest.raises(RuntimeError, match="unsupport pyarrow_dtype"):
                conn.upload({
                    'i':pd.DataFrame({'a':[1,2,3]},dtype=pd.ArrowDtype(pa.decimal256(3,2)))
                })
        assert equalPlus(conn.run("symbol(`a`b`c)"),np.array(['a','b','c'],dtype='object'))
        with pytest.raises(RuntimeError, match="currently only string, symbol or integral key is supported in dictionary"):
            conn.run('dict(FLOAT,INT)')
        assert conn.run('dict(SYMBOL,INT)')=={}
    # def test_util_cov(self):
    #     conn = ddb.Session(HOST, PORT, USER, PASSWD)
    #     data=pd.DataFrame({'a':["abc"]})
    #     data.__DolphinDB_Type__={
    #         'a':keys.DT_UUID
    #     }
    #     conn.upload({'a':data})
    #     conn.run('print a')
