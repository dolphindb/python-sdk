import inspect
import random
import subprocess
import sys
from itertools import chain
from time import sleep

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest
from pandas._testing import assert_frame_equal

from basic_testing.prepare import random_string
from basic_testing.utils import operateNodes
from setup.prepare import CountBatchDownLatch
from setup.settings import HOST, PORT, USER, PASSWD, HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, \
    PORT_CONTROLLER, PORT_DNODE2, PORT_DNODE3


def gethandler(df, counter):
    def handler(lst):
        index = len(df)
        df.loc[index] = lst
        counter.countDown(1)

    return handler


def gethandler_multi_row(df, counter):
    print("get msg")

    def handler(lst):
        for row in lst:
            index = len(df)
            df.loc[index] = row
        counter.countDown(1)

    return handler


def streamDSgethandler(df1, df2, counter):
    def streamDeserializer_handler(lst):
        if lst[-1] == "msg1":
            index_1 = len(df1)
            df1.loc[index_1] = lst
        else:
            index_2 = len(df2)
            df2.loc[index_2] = lst
        counter.countDown(1)

    return streamDeserializer_handler


def getListenPort():
    return random.randint(30001, 39999)


class TestSubscribe:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    def handler(self, lst):
        index = len(self.df)
        self.df.loc[index] = lst

    def handler_df(self, counter):
        def handler(lst):
            self.df = lst
            counter.countDown(1)

        return handler

    def test_subscribe_error(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError, match='streaming is not enabled'):
            conn.subscribe(HOST, PORT, self.handler, "trades1", None, 0, False, userName=USER, password=PASSWD)
        with pytest.raises(RuntimeError, match='streaming is not enabled'):
            conn.subscribe(HOST, PORT, self.handler, "trades1", None, 0, False, batchSize=2, userName=USER,
                           password=PASSWD)
        with pytest.raises(RuntimeError, match='streaming is not enabled'):
            conn.unsubscribe(HOST, PORT, "trades1", None)
        conn.enableStreaming(0, 2)
        with pytest.raises(RuntimeError, match='streaming is already enabled'):
            conn.enableStreaming()
        with pytest.raises(RuntimeError, match="Thread pool streaming doesn't support batch subscribe"):
            conn.subscribe(HOST, PORT, self.handler, "trades1", None, 0, False, batchSize=2, userName=USER,
                           password=PASSWD)
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.enableStreaming()
        with pytest.raises(RuntimeError, match='streaming is already enabled'):
            conn.enableStreaming()

    def test_subscribe_error_host_int(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(1, PORT, self.handler, 'test', None, 0, False, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_host_fail_connect(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort, 2)
        with pytest.raises(RuntimeError):
            conn1.subscribe("255.255.255.255", PORT, self.handler, "test", "action", 0, False, userName=USER,
                            password=PASSWD)
        conn1.close()

    def test_subscribe_error_port_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(HOST, "dsf", self.handler, "test", "action", 0, False, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_port_fail_connect(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(RuntimeError):
            conn1.subscribe(HOST, -1, self.handler, "test", "action", 0, False, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_tableName_int(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(HOST, PORT, self.handler, 1, "action", 0, False, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_tableName_not_exist(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(RuntimeError):
            conn1.subscribe(HOST, PORT, self.handler, "skdfls", "action", 0, False, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_actionName_int(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(HOST, PORT, self.handler, "test", 1, 0, False, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_actionName_same_name(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(RuntimeError):
            conn1.subscribe(HOST, PORT, self.handler, func_name, "action", 0, False, userName=USER, password=PASSWD)
            conn1.subscribe(HOST, PORT, self.handler, func_name, "action", 0, False, userName=USER, password=PASSWD)
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_error_offset_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(HOST, PORT, self.handler, "test", "action", "fsd", False, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_resub_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(HOST, PORT, self.handler, "test", "action", 0, "fsd", userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_msgAsTable_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(HOST, PORT, self.handler, "test", "action", 0, False, np.array(["000905"]), "11",
                            userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_msgAsTable_True_batchSize_zero(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(ValueError):
            conn1.subscribe(HOST, PORT, self.handler, "test", "action", 0, False, np.array(["000905"]), True,
                            userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_batchSize_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(HOST, PORT, self.handler, "trades15", "action", 0, False, np.array(["000905"]), False,
                            "fdsf", userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_batchSize_float(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(HOST, PORT, self.handler, "trades15", "action", 0, False, np.array(["000905"]), False, 1.1,
                            userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_throttle_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            conn1.subscribe(HOST, PORT, self.handler, "trades15", "action", 0, False, np.array(["000905"]), False, -1,
                            "sdfse", userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_throttle_float(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(ValueError):
            conn1.subscribe(HOST, PORT, self.handler, "trades15", "action", 0, False, np.array(["000905"]), False, -1,
                            -1, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_throttle_lt_zero(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(ValueError):
            conn1.subscribe(HOST, PORT, self.handler, "trades15",
                            "action", 0, False, np.array(["000905"]), False, -1, -1, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_offset_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_offset_lt_zero_gt(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        df = pd.DataFrame(columns=["time", "sym", "price"])
        counter.reset(5)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", -1, False, userName=USER,
                        password=PASSWD)
        script = f"""
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, rand(1000,5)/10.0 as price)
            {func_name}.append!(insert_table)
        """
        conn1.run(script)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from insert_table"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_resub_True(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, True, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
        """
        conn1.run(script)
        sleep(1)
        script = f"""
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, rand(1000,5)/10.0 as price)
            {func_name}.append!(insert_table)
        """
        counter.reset(5)
        conn1.run(script)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_resub_False(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
        """
        conn1.run(script)
        sleep(1)
        script = f"""
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, rand(1000,5)/10.0 as price)
            {func_name}.append!(insert_table)
        """
        conn1.run(script)
        sleep(3)
        assert len(df) == 10
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_filter(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price", "id"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE, INT]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0, int(1..10))
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(2)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, np.array(["000905"]),
                        userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        df["id"] = df["id"].astype(np.int32)
        assert_frame_equal(df, conn1.run(f"select * from {func_name} where sym=`000905"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_filter_lambda(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price", "id"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE, INT]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0, int(1..10))
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(2)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False,
                        "msg -> select * from msg where sym=`000905",
                        userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        df["id"] = df["id"].astype(np.int32)
        assert_frame_equal(df, conn1.run(f"select * from {func_name} where sym=`000905"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_double_array_vector_msgAsTable_False(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["symbolv", "doublev"])
        script = f'''
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`symbolv`doublev, [SYMBOL, DOUBLE[]]) as {func_name}
            n = 10
            exTable = table(n:0, `symbolv`doublev, [SYMBOL, DOUBLE[]])
            symbol_vector=take(`A`B`C`D`E`F`G, n)
            double_vector=cut(take(double([36,98,95,69,41,60,78,92,78,21]), 100),n)
            exTable.tableInsert(symbol_vector, double_vector)
            {func_name}.append!(exTable)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, msgAsTable=False,
                        userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_double_array_vector_msgAsTable_True(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        self.df = pd.DataFrame(columns=["symbolv", "doublev"])
        script = f'''
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`symbolv`doublev, [SYMBOL, DOUBLE]) as {func_name}
            n = 10
            exTable = table(n:0, `symbolv`doublev, [SYMBOL, DOUBLE])
            symbol_vector=take(`A`B`C`D`E`F`G, n)
            double_vector=take(double([36,98,95,69,41,60,78,92,78,21]), n)
            exTable.tableInsert(symbol_vector, double_vector)
            {func_name}.append!(exTable)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(1)
        conn1.subscribe(HOST, PORT, self.handler_df(counter), func_name,
                        "action", 0, False, msgAsTable=True, batchSize=1000, throttle=1, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")

    def test_subscribe_batchSize_lt_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["symbolv", "doublev"])
        script = f'''
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`trades_doublev)}}catch(ex){{}}
            share streamTable(10000:0,`symbolv`doublev, [SYMBOL, DOUBLE]) as {func_name}
            n = 10
            exTable = table(n:0, `symbolv`doublev, [SYMBOL, DOUBLE])
            symbol_vector=take(`A`B`C`D`E`F`G, n)
            double_vector=take(double([36,98,95,69,41,60,78,92,78,21]), n)
            exTable.tableInsert(symbol_vector, double_vector)
            {func_name}.append!(exTable)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, msgAsTable=False,
                        batchSize=-1, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_msgAsTable_False_batchSize_gt_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["symbolv", "doublev"])
        script = f'''
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`symbolv`doublev, [SYMBOL, DOUBLE]) as {func_name}
            n = 10
            exTable = table(n:0, `symbolv`doublev, [SYMBOL, DOUBLE])
            symbol_vector=take(`A`B`C`D`E`F`G, n)
            double_vector=take(double([36,98,95,69,41,60,78,92,78,21]), n)
            exTable.tableInsert(symbol_vector, double_vector)
            {func_name}.append!(exTable)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(5)
        conn1.subscribe(HOST, PORT, gethandler_multi_row(
            df, counter), func_name, "action", 0, False, msgAsTable=False, batchSize=2, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_throttle_gt_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f'''
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(1)
        conn1.subscribe(HOST, PORT, self.handler_df(counter), func_name,
                        "action", 0, False, msgAsTable=True, batchSize=1000, throttle=10.1, userName=USER,
                        password=PASSWD)
        assert not counter.wait_s(10)
        assert len(self.df) == 0
        counter.reset(1)
        assert counter.wait_s(20)
        assert len(self.df) == 10
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_streamDeserializer_error_sym2table_None(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            ddb.streamDeserializer(None, conn1)
        conn1.close()

    def test_subscribe_streamDeserializer_error_sym2table_scalar(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            ddb.streamDeserializer(1, conn1)
        conn1.close()

    def test_subscribe_streamDeserializer_error_sym2table_list(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            ddb.streamDeserializer(["a", "b"], conn1)
        conn1.close()

    def test_subscribe_streamDeserializer_error_sym2table_dict_value_list_size_not_2(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(RuntimeError):
            ddb.streamDeserializer({"msg1": ["table1"], "msg2": ["table2"]}, conn1)
        conn1.close()

    def test_subscribe_streamDeserializer_error_sym2table_dict_value_tuple_size_not_2(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(RuntimeError):
            ddb.streamDeserializer({"msg1": ("table1",), "msg2": ("table2",)}, conn1)
        conn1.close()

    def test_subscribe_streamDeserializer_error_session_scalar(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            ddb.streamDeserializer(["a", "b"], 1)
        conn1.close()

    def test_subscribe_streamDeserializer_error_session_list(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(TypeError):
            ddb.streamDeserializer(["a", "b"], [conn1, conn1])
        conn1.close()

    def test_subscribe_streamDeserializer_error_msgAsTable_True(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            go
            n = 10
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
            d = dict(['msg1','msg2'], [table1, table2]);
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        sd = ddb.streamDeserializer({"msg1": "table1", "msg2": "table2"}, conn1)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2), func_name,
                            "action", 0, False, msgAsTable=True, streamDeserializer=sd, batchSize=5, userName=USER,
                            password=PASSWD)
        conn1.close()

    def test_subscribe_streamDeserializer_memory_table_session_None(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            go
            n = 10
            t1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            t2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
            share t1 as {func_name}_1
            share t2 as {func_name}_2
            tableInsert({func_name}_1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
            tableInsert({func_name}_2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
            d = dict(['msg1','msg2'], [{func_name}_1, {func_name}_2]);
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(20)
        sd = ddb.streamDeserializer({"msg1": f"{func_name}_1", "msg2": f"{func_name}_2"}, None)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter),
                        func_name, "action", 0, False, msgAsTable=False, streamDeserializer=sd, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run(f"select * from {func_name}_1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run(f"select * from {func_name}_2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_streamDeserializer_PartitionedTable_table_session_None(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            n = 10;
            dbName = '{db_name}'
            if(existsDatabase(dbName))
                dropDatabase(dbName)
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
            pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1)
            pt2 = db.createPartitionedTable(table2,'pt2',`datetimev).append!(table2)
            d = dict(['msg1','msg2'], [table1, table2])
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(20)
        sd = ddb.streamDeserializer({"msg1": [db_name, "pt1"], "msg2": [db_name, "pt2"]}, None)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), func_name, "action",
                        0, False, msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from pt1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from pt2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_streamDeserializer_memory_table(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            go
            n = 10
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
            d = dict(['msg1','msg2'], [table1, table2]);
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(20)
        sd = ddb.streamDeserializer({"msg1": "table1", "msg2": "table2"}, conn1)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter),
                        func_name, "action", 0, False, msgAsTable=False, streamDeserializer=sd, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from table1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from table2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_streamDeserializer_memory_table_10000_rows(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            go
            n = 5000
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
            d = dict(['msg1','msg2'], [table1, table2]);
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10000)
        sd = ddb.streamDeserializer({"msg1": "table1", "msg2": "table2"}, conn1)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter),
                        func_name, "action", 0, False, msgAsTable=False, streamDeserializer=sd, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(200)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from table1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from table2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_streamDeserializer_PartitionedTable_table(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            n = 10;
            dbName = '{db_name}'
            if(existsDatabase(dbName))
                dropDatabase(dbName)
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
            pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1)
            pt2 = db.createPartitionedTable(table2,'pt2',`datetimev).append!(table2)
            d = dict(['msg1','msg2'], [table1, table2])
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(20)
        sd = ddb.streamDeserializer({"msg1": [db_name, "pt1"], "msg2": [db_name, "pt2"]}, conn1)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), func_name, "action",
                        0, False, msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from pt1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from pt2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_streamDeserializer_PartitionedTable_table_10000_rows(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            n = 5000;
            dbName = '{db_name}'
            if(existsDatabase(dbName))
                dropDatabase(dbName)
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
            pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1)
            pt2 = db.createPartitionedTable(table2,'pt2',`datetimev).append!(table2)
            d = dict(['msg1','msg2'], [table1, table2])
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10000)
        sd = ddb.streamDeserializer({"msg1": [db_name, "pt1"], "msg2": [db_name, "pt2"]}, conn1)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), func_name, "action",
                        0, False, msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        assert counter.wait_s(200)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from pt1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from pt2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    @pytest.mark.parametrize(argnames="error_host", argvalues=[None, "255.255.255.255"],
                             ids=["host_none", "host_error"])
    def test_subscribe_unsubscribe_error_host(self, error_host):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        with pytest.raises(Exception):
            conn1.unsubscribe(error_host, PORT, func_name, "action")
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    @pytest.mark.parametrize(argnames="error_port", argvalues=[None, "sdjfk", -1],
                             ids=["port_none", "port_str", "port_error"])
    def test_subscribe_unsubscribe_error_port(self, error_port):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        with pytest.raises(Exception):
            conn1.unsubscribe(HOST, error_port, func_name, "action")
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    @pytest.mark.parametrize(argnames="error_tableName", argvalues=[None, 12, "fddksj"],
                             ids=["tableName_none", "tableName_str", "tableName_error"])
    def test_subscribe_unsubscribe_error_tableName(self, error_tableName):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        with pytest.raises(Exception):
            conn1.unsubscribe(HOST, PORT, error_tableName, "action")
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    @pytest.mark.parametrize(argnames="error_actionName", argvalues=[None, 12, "fddksj"],
                             ids=["actionName_none", "actionName_str", "actionName_error"])
    def test_subscribe_unsubscribe_error_actionName(self, error_actionName):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        with pytest.raises(Exception):
            conn1.unsubscribe(HOST, PORT, func_name, error_actionName)
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_enableStreaming_error_port(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        with pytest.raises(Exception):
            conn1.enableStreaming("adc")
        with pytest.raises(Exception):
            conn1.enableStreaming(-1)
        with pytest.raises(Exception):
            conn1.enableStreaming(None)

    def test_subscribe_getSubscriptionTopics(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        ans = conn1.getSubscriptionTopics()
        get_host = ans[0].split("/")[0]
        get_port = ans[0].split("/")[1]
        get_tableName = ans[0].split("/")[2]
        get_actionName = ans[0].split("/")[3]
        assert get_host == HOST
        assert int(get_port) == PORT
        assert get_tableName == func_name
        assert get_actionName == "action"
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        ans = conn1.getSubscriptionTopics()
        assert len(ans) == 0

    def test_subscribe_many_tables(self):
        func_name = inspect.currentframe().f_code.co_name
        self.conn.run(f"""
            share streamTable(12000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}
            insert into {func_name} values(take(now(), 2000), take(`000905`600001`300201`000908`600002, 2000), rand(1000,2000)/10.0, 1..2000)

            share streamTable(120000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}2
            insert into {func_name}2 values(take(now(), 2000), take(`000905`600001`300201`000908`600002, 2000), rand(1000,2000)/10.0, 1..2000)

            share streamTable(120000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}3
            insert into {func_name}3 values(take(now(), 2000), take(`000905`600001`300201`000908`600002, 2000), rand(1000,2000)/10.0, 1..2000)

            share streamTable(120000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}4
            insert into {func_name}4 values(take(now(), 2000), take(`000905`600001`300201`000908`600002, 2000), rand(1000,2000)/10.0, 1..2000)

            share streamTable(120000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}5
            insert into {func_name}5 values(take(now(), 2000), take(`000905`600001`300201`000908`600002, 2000), rand(1000,2000)/10.0, 1..2000)
        """)
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        counter = CountBatchDownLatch(2000)
        counter2 = CountBatchDownLatch(2000)
        counter3 = CountBatchDownLatch(2000)
        counter4 = CountBatchDownLatch(2000)
        counter5 = CountBatchDownLatch(2000)
        counters = [counter, counter2, counter3, counter4, counter5]
        results = [[], [], [], [], []]
        tables = [func_name, f'{func_name}2', f'{func_name}3', f'{func_name}4', f'{func_name}5']

        def tmp_handle(ndlist: list, counter):
            def handler(lst):
                ndlist.append(lst)
                counter.countDown(len(lst))

            return handler

        for ind, tab in enumerate(tables):
            co = counters[ind]
            conn1.subscribe(
                host=HOST,
                port=PORT,
                handler=tmp_handle(results[ind], co),
                tableName=tab, actionName="action", offset=0,
                resub=True,
                filter=None,
                batchSize=100,
                throttle=1, userName=USER, password=PASSWD
            )

        for ind, co in enumerate(counters):
            assert co.wait_s(200)
            ex_df = self.conn.run(f"select * from {tables[ind]}")
            res1 = list(chain.from_iterable(results[ind]))
            for i in range(len(res1)):
                assert res1[i] == list(ex_df.iloc[i])
        for tab in tables:
            conn1.unsubscribe(HOST, PORT, tab, "action")
        conn1.close()

    def test_subscribe_exception_in_handler(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            colName=["time","x"]
            colType=["timestamp","int"]
            t = streamTable(100:0, colName, colType);
            share t as {func_name};go
            insert into st values(now(), rand(100.00,1))
        """
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "from time import sleep;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 "conn.enableStreaming();"
                                 f"conn.run(\"\"\"{script}\"\"\");"
                                 f"conn.subscribe('{HOST}',{PORT},lambda :raise RuntimeError('this should be catched'),'{func_name}','test',0,True, userName='{USER}', password='{PASSWD}');"
                                 f"conn.run('insert into {func_name} values(now(), rand(100.00,1))');"
                                 "sleep(3);"
                                 f"conn.unsubscribe('{HOST}',{PORT},'{func_name}','test');"
                                 "conn.close();"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert "this should be catched" in result.stderr

    def test_subscribe_keyededStreamTable(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming(getListenPort())
        script = f"""
            colName=["time","sym", "p1", "p2", "ind"]
            colType=["timestamp","symbol","double","double","int"]
            t = keyedStreamTable(`time, 100:0, colName, colType);
            share t as {func_name};go
            for(i in 0:100){{
                insert into {func_name} values(now(), rand(`APPL`GOOG`TESLA`TX`BABA`AMAZON,1), rand(200.00, 1), rand(200.00, 1), rand(1000, 1))
            }}
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        res = []

        def tmp_handle(array, co):
            def myhandler(lst):
                array.append(lst)
                co.countDown(1)

            return myhandler

        conn1.subscribe(HOST, PORT, tmp_handle(res, counter), func_name, "test", 0, True, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(10)
        conn1.unsubscribe(HOST, PORT, func_name, "test")
        ex_df = conn1.run(f'select * from {func_name} order by time')
        for i in range(len(res)):
            assert res[i] == list(ex_df.iloc[i])
        conn1.close()

    @pytest.mark.xdist_group(name='cluster_test')
    def test_subscribe_backupSites(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn.enableStreaming()
        conn1 = ddb.Session()
        conn1.connect(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, reconnect=True)
        conn2 = ddb.Session(HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER)
        conn2.connect(HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, reconnect=True)
        conn3 = ddb.Session(HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER)
        conn3.connect(HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER, reconnect=True)
        script = f"""
            t=streamTable(100:0,[`a,`b],[INT,STRING])
            share t as `{func_name}
        """
        conn1.run(script)
        conn2.run(script)
        conn3.run(script)

        def handler(df: pd.DataFrame):
            def inner(data):
                index = len(df)
                df.loc[index] = data

            return inner

        df = pd.DataFrame(columns=['a', 'b'])

        def wait_until(count):
            while True:
                if len(df) < count:
                    sleep(0.5)
                else:
                    break

        conn.subscribe(HOST_CLUSTER, PORT_DNODE1, handler(df), func_name, "test", resub=True,
                       backupSites=[f"{HOST_CLUSTER}:{PORT_DNODE2}", f"{HOST_CLUSTER}:{PORT_DNODE3}"],
                       resubscribeInterval=20, userName=USER, password=PASSWD)
        conn1_insert = [f'insert into {func_name} values({i},"DataNode1")' for i in range(1, 11)]
        conn2_insert = [f'insert into {func_name} values({i},"DataNode2")' for i in range(1, 21)]
        conn3_insert = [f'insert into {func_name} values({i},"DataNode3")' for i in range(1, 31)]
        for sql in conn1_insert:
            conn1.run(sql)
        for sql in conn2_insert:
            conn2.run(sql)
        for sql in conn3_insert:
            conn3.run(sql)
        wait_until(10)
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "STOP")
        wait_until(20)
        operateNodes(conn, [f'dnode{PORT_DNODE2}'], "STOP")
        wait_until(30)
        operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}'], "START")
        df_expect = pd.DataFrame({
            'a': [i for i in range(1, 31)],
            'b': ["DataNode1"] * 10 + ["DataNode2"] * 10 + ["DataNode3"] * 10,
        })
        assert_frame_equal(df, df_expect, check_dtype=False)
        conn1.run(script)
        conn1_insert = [f'insert into {func_name} values({i},"DataNode1")' for i in range(1, 41)]
        for sql in conn1_insert:
            conn1.run(sql)
        operateNodes(conn, [f'dnode{PORT_DNODE3}'], "STOP")
        wait_until(40)
        operateNodes(conn, [f'dnode{PORT_DNODE3}'], "START")
        conn2.run(script)
        conn2_insert = [f'insert into {func_name} values({i},"DataNode2")' for i in range(1, 51)]
        for sql in conn2_insert:
            conn2.run(sql)
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "STOP")
        wait_until(50)
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "START")
        conn.unsubscribe(HOST_CLUSTER, PORT_DNODE1, func_name, "test")
        df_expect = pd.DataFrame({
            'a': [i for i in range(1, 51)],
            'b': ["DataNode1"] * 10 + ["DataNode2"] * 10 + ["DataNode3"] * 10 + ["DataNode1"] * 10 + ["DataNode2"] * 10,
        })
        assert_frame_equal(df, df_expect, check_dtype=False)

    @pytest.mark.xdist_group(name='cluster_test')
    def test_subscribe_backupSites_subOnce(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn.enableStreaming()
        conn1 = ddb.Session()
        conn1.connect(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, reconnect=True)
        conn2 = ddb.Session(HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER)
        conn2.connect(HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER, reconnect=True)
        conn3 = ddb.Session(HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER)
        conn3.connect(HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER, reconnect=True)
        script = f"""
            t=streamTable(100:0,[`a,`b],[INT,STRING])
            share t as `{func_name}
        """
        conn1.run(script)
        conn2.run(script)
        conn3.run(script)

        def handler(df: pd.DataFrame):
            def inner(data):
                index = len(df)
                df.loc[index] = data

            return inner

        df = pd.DataFrame(columns=['a', 'b'])

        def wait_until(count):
            while True:
                if len(df) < count:
                    sleep(0.5)
                else:
                    break

        conn.subscribe(HOST_CLUSTER, PORT_DNODE1, handler(df), func_name, "test", resub=True,
                       backupSites=[f"{HOST_CLUSTER}:{PORT_DNODE2}", f"{HOST_CLUSTER}:{PORT_DNODE3}"],
                       resubscribeInterval=20,
                       subOnce=True, userName=USER, password=PASSWD)
        conn1_insert = [f'insert into {func_name} values({i},"DataNode1")' for i in range(1, 11)]
        conn2_insert = [f'insert into {func_name} values({i},"DataNode2")' for i in range(1, 21)]
        conn3_insert = [f'insert into {func_name} values({i},"DataNode3")' for i in range(1, 31)]
        for sql in conn1_insert:
            conn1.run(sql)
        for sql in conn2_insert:
            conn2.run(sql)
        for sql in conn3_insert:
            conn3.run(sql)
        wait_until(10)
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "STOP")
        wait_until(20)
        operateNodes(conn, [f'dnode{PORT_DNODE2}'], "STOP")
        wait_until(30)
        operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}'], "START")
        conn1.run(script)
        conn2.run(script)
        df_expect = pd.DataFrame({
            'a': [i for i in range(1, 31)],
            'b': ["DataNode1"] * 10 + ["DataNode2"] * 10 + ["DataNode3"] * 10,
        })
        assert_frame_equal(df, df_expect, check_dtype=False)
        conn1_insert = [f'insert into {func_name} values({i},"DataNode1")' for i in range(1, 41)]
        for sql in conn1_insert:
            conn1.run(sql)
        operateNodes(conn, [f'dnode{PORT_DNODE3}'], "STOP")
        wait_until(30)
        operateNodes(conn, [f'dnode{PORT_DNODE3}'], "START")
        conn3.run(script)
        conn2_insert = [f'insert into {func_name} values({i},"DataNode2")' for i in range(1, 51)]
        for sql in conn2_insert:
            conn2.run(sql)
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "STOP")
        wait_until(30)
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "START")
        conn.unsubscribe(HOST_CLUSTER, PORT_DNODE1, func_name, "test")
        assert_frame_equal(df, df_expect, check_dtype=False)

    @pytest.mark.xdist_group(name='cluster_test')
    def test_subscribe_backupSites_already_stopped(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn.enableStreaming()
        conn2 = ddb.Session(HOST_CLUSTER, PORT_DNODE2, USER_CLUSTER, PASSWD_CLUSTER)
        conn3 = ddb.Session(HOST_CLUSTER, PORT_DNODE3, USER_CLUSTER, PASSWD_CLUSTER)
        script = f"""
            t=streamTable(100:0,[`a,`b],[INT,STRING])
            share t as `{func_name}
        """
        conn2.run(script)
        conn3.run(script)

        def handler(df: pd.DataFrame):
            def inner(data):
                index = len(df)
                df.loc[index] = data

            return inner

        def wait_until(count):
            while True:
                if len(df) < count:
                    sleep(0.5)
                else:
                    break

        df = pd.DataFrame(columns=['a', 'b'])
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "STOP")
        conn.subscribe(HOST_CLUSTER, PORT_DNODE1, handler(df), func_name, "test", resub=True,
                       backupSites=[f"{HOST_CLUSTER}:{PORT_DNODE2}", f"{HOST_CLUSTER}:{PORT_DNODE3}"], userName=USER,
                       password=PASSWD)
        sleep(3)
        conn2_insert = [f'insert into {func_name} values({i},"DataNode2")' for i in range(1, 11)]
        conn3_insert = [f'insert into {func_name} values({i},"DataNode3")' for i in range(1, 21)]
        for sql in conn2_insert:
            conn2.run(sql)
        for sql in conn3_insert:
            conn3.run(sql)
        wait_until(10)
        operateNodes(conn, [f'dnode{PORT_DNODE2}'], "STOP")
        wait_until(20)
        operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}'], "START")
        conn.unsubscribe(HOST_CLUSTER, PORT_DNODE1, func_name, "test")
        df_expect = pd.DataFrame({
            'a': [i for i in range(1, 21)],
            'b': ["DataNode2"] * 10 + ["DataNode3"] * 10,
        })
        assert_frame_equal(df, df_expect, check_dtype=False)

    def test_subscribe_backupSites_type_error(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.enableStreaming()
        script = f"""
            t=streamTable(100:0,[`a,`b],[INT,STRING])
            share t as `{func_name}
        """
        conn.run(script)

        def handler(df: pd.DataFrame):
            def inner(data):
                index = len(df)
                df.loc[index] = data

            return inner

        df = pd.DataFrame(columns=['a', 'b'])
        with pytest.raises(TypeError, match="backupSites must be a list of str."):
            conn.subscribe(HOST_CLUSTER, PORT, handler(df), func_name, "test", backupSites="127.0.0.1:8848",
                           userName=USER, password=PASSWD)
        with pytest.raises(TypeError, match="backupSites must be a list of str."):
            conn.subscribe(HOST_CLUSTER, PORT, handler(df), func_name, "test", backupSites=[1], userName=USER,
                           password=PASSWD)

    @pytest.mark.xdist_group(name='cluster_test')
    def test_subscribe_backupSites_format_error(self):
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn.enableStreaming(35555)

        def handler(df: pd.DataFrame):
            def inner(data):
                index = len(df)
                df.loc[index] = data

            return inner

        df = pd.DataFrame(columns=['a', 'b'])
        with pytest.raises(RuntimeError):
            conn.subscribe(HOST_CLUSTER, PORT_DNODE1, handler(df), "st", "test",
                           backupSites=[f"{HOST_CLUSTER}{PORT_DNODE2}"], userName=USER, password=PASSWD)

    @pytest.mark.xdist_group(name='cluster_test')
    def test_subscribe_backupSites_port_gt_65535(self):
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn.enableStreaming(35555)

        def handler(df: pd.DataFrame):
            def inner(data):
                index = len(df)
                df.loc[index] = data

            return inner

        df = pd.DataFrame(columns=['a', 'b'])
        with pytest.raises(RuntimeError,
                           match="Incorrect input .* for backupSite\. The port number must be a positive integer no greater than 65535"):
            conn.subscribe(HOST_CLUSTER, PORT_DNODE1, handler(df), "st", "test", backupSites=[f"{HOST_CLUSTER}:65536"],
                           userName=USER, password=PASSWD)

    def test_subscribe_resubTimeout_error(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        with pytest.raises(ValueError, match="Please use resubscibeInterval instead of resubTimeout"):
            conn.subscribe(HOST, PORT, print, "test", "test", resubTimeout=20, userName=USER, password=PASSWD)
