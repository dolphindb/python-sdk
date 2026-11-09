import inspect
import json
import random
import re
import subprocess
import sys
import threading
import time
from itertools import chain
from time import sleep

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest
from dolphindb.logger import Sink
from pandas._testing import assert_frame_equal
from pydantic import ValidationError

from basic_testing.prepare import random_string
from basic_testing.utils import get_dolphindb_version
from basic_testing.utils import operateNodes
from setup.prepare import CountBatchDownLatch
from setup.settings import HOST, PORT, USER, PASSWD, HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, \
    PORT_CONTROLLER, PORT_DNODE2, PORT_DNODE3


class ResubscribeRecorder(Sink):
    def __init__(self, keyword="Failed to resubscribe"):
        self.name_ = f"apy2063_resubscribe_recorder_{id(self)}"
        super().__init__(self.name_)
        self._keyword = keyword
        self._times = []
        self._lock = threading.Lock()

    def handle(self, msg):
        if self._keyword in msg.log:
            with self._lock:
                self._times.append(time.monotonic())

    def times(self):
        with self._lock:
            return list(self._times)


def resubscribe_intervals_ms(times):
    return [(times[i + 1] - times[i]) * 1000 for i in range(len(times) - 1)]


def get_cluster_data_nodes():
    for entry_port in [PORT_CONTROLLER, PORT_DNODE1, PORT_DNODE2, PORT_DNODE3]:
        conn = ddb.Session()
        try:
            conn.connect(HOST_CLUSTER, entry_port, USER_CLUSTER, PASSWD_CLUSTER)
            perf = conn.run("rpc(getControllerAlias(), getClusterPerf)")
        except Exception as e:
            print(f"connect {HOST_CLUSTER}:{entry_port} as controller entry failed: {e}")
            continue
        finally:
            try:
                conn.close()
            except Exception:
                pass
        nodes = []
        for _, row in perf.iterrows():
            if int(row["mode"]) != 0:
                continue
            nodes.append((row["host"], int(row["port"])))
        print(f"cluster {HOST_CLUSTER}:{entry_port} data nodes: {nodes}")
        if len(nodes) >= 3:
            return nodes
    return []


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

    def execute_orca_stream_graph_script(self, session, graph_name='orca', table_name="trades"):
        version_str = session.run("version()")
        match = re.search(r'(\d+)\.(\d+)\.(\d+)', version_str)
        if match:
            major, minor, patch = map(int, match.groups())
            is_new_version = (major > 3) or (major == 3 and minor > 0) or (major == 3 and minor == 0 and patch >= 4)
        else:
            is_new_version = True

        if is_new_version:
            source_call = 'g.source("trades", ["time","sym","volume"], [TIMESTAMP, SYMBOL, INT])'
        else:
            source_call = 'g.source("trades", 1000:0, ["time","sym","volume"], [TIMESTAMP, SYMBOL, INT])'

        script = f"""

        subscribers = select * from getStreamingStat().pubTables where tableName = "{graph_name}.orca_table.output";
        for(subscriber in subscribers){{
            ip_port = subscriber.subscriber.split(":");
            stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
        }}
        if (existsCatalog("{graph_name}")) {{
            dropCatalog("{graph_name}")
        }}
        go
        createCatalog("{graph_name}")
        go
        use catalog {graph_name}

        g = createStreamGraph('engine')
        {source_call}.timeSeriesEngine(
            windowSize=60000, 
            step=60000, 
            metrics=<[sum(volume)]>, 
            timeColumn="time", 
            useSystemTime=false, 
            keyColumn="sym", 
            useWindowStartTime=false
        ).sink("output")
        g.submit()
        go

        times = [2018.10.08T01:01:01.785, 2018.10.08T01:01:02.125, 2018.10.08T01:01:10.263, 2018.10.08T01:01:12.457, 2018.10.08T01:02:10.789, 2018.10.08T01:02:12.005, 2018.10.08T01:02:30.021, 2018.10.08T01:04:02.236, 2018.10.08T01:04:04.412, 2018.10.08T01:04:05.152]
        syms = [`A, `B, `B, `A, `A, `B, `A, `A, `B, `B]
        volumes = [10, 26, 14, 28, 15, 9, 10, 29, 32, 23]
        tmp = table(times as time, syms as sym, volumes as volume)
        appendOrcaStreamTable("{table_name}", tmp)
        """

        return session.run(script)

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
        with pytest.raises(ValidationError, match='Input should be a valid string'):
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
        with pytest.raises(ValidationError, match='Input should be a valid integer'):
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
        with pytest.raises(ValidationError, match='Input should be a valid string'):
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
        with pytest.raises(ValidationError, match='Input should be a valid string'):
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
        with pytest.raises(ValidationError, match='Input should be a valid integer'):
            conn1.subscribe(HOST, PORT, self.handler, "test", "action", "fsd", False, userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_resub_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(ValidationError, match='Input should be a valid boolean'):
            conn1.subscribe(HOST, PORT, self.handler, "test", "action", 0, "fsd", userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_msgAsTable_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(ValidationError, match='Input should be a valid boolean'):
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
        with pytest.raises(ValidationError, match='Input should be a valid integer'):
            conn1.subscribe(HOST, PORT, self.handler, "trades15", "action", 0, False, np.array(["000905"]), False,
                            "fdsf", userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_batchSize_float(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(ValidationError, match='Input should be a valid integer'):
            conn1.subscribe(HOST, PORT, self.handler, "trades15", "action", 0, False, np.array(["000905"]), False, 1.1,
                            userName=USER, password=PASSWD)
        conn1.close()

    def test_subscribe_error_throttle_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        listenPort = getListenPort()
        conn1.enableStreaming(listenPort)
        with pytest.raises(ValidationError, match='Input should be a valid number'):
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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

    def test_subscribe_offset_lt_zero(self):
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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

    def test_subscribe_userName_password_default(self):
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        with pytest.raises(RuntimeError, match="Login is required for script execution with client authentication enabled"):
            conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False)

    def test_subscribe_unsubscribe_resubscribe(self):
        func_name = inspect.currentframe().f_code.co_name
        for i in range(10):
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
                share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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
            share streamTable(10000:0,`time`sym`price`id, [NANOTIMESTAMP,SYMBOL,DOUBLE, INT]) as {func_name}
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
            share streamTable(10000:0,`time`sym`price`id, [NANOTIMESTAMP,SYMBOL,DOUBLE, INT]) as {func_name}
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
            subscribers = select * from getStreamingStat().pubTables where tableName =`{func_name};
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
        expect = conn1.run(f"select * from {func_name}")
        expect["symbolv"] = expect["symbolv"].astype("object")
        assert_frame_equal(df, expect)
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
            share streamTable(100:0, `timestampv`sym`blob`price1,[NANOTIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            go
            n = 10
            t1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            t2 = table(100:0, `datetimev`timestampv`sym`price1, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE]);
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
            share streamTable(100:0, `timestampv`sym`blob`price1,[NANOTIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            n = 10;
            dbName = '{db_name}'
            if(existsDatabase(dbName))
                dropDatabase(dbName)
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE])
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

    def test_subscribe_orca_streamDeserializer_memory_table_session_None(self):
        version = get_dolphindb_version(self.conn)
        if version < (3, 0, 5):
            pytest.skip(f"Orca streamDeserializer test requires DolphinDB 300.5+, current version: {version}")
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
            if (existsCatalog("orca")) {{
                dropCatalog("orca")
            }}
            go
            createCatalog("orca")
            go
            use catalog orca
            g = createStreamGraph('engine')
            g.source("orca.orca_table.{func_name}", `timestampv`sym`blob`price1,[NANOTIMESTAMP,SYMBOL,BLOB,DOUBLE]).sink("output")
            g.submit()
            go
            n = 10
            t1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            t2 = table(100:0, `datetimev`timestampv`sym`price1, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE]);
            share t1 as {func_name}_1
            share t2 as {func_name}_2
            tableInsert({func_name}_1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
            tableInsert({func_name}_2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
            d = dict(['msg1','msg2'], [{func_name}_1, {func_name}_2]);
            replay(inputTables=d, outputTables="orca.orca_table.{func_name}", dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(20)
        sd = ddb.streamDeserializer({"msg1": f"{func_name}_1", "msg2": f"{func_name}_2"}, None)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter),
                        f"orca.orca_table.{func_name}", "action", 0, False, msgAsTable=False, streamDeserializer=sd,
                        userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run(f"select * from {func_name}_1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run(f"select * from {func_name}_2"))
        conn1.unsubscribe(HOST, PORT, f"orca.orca_table.{func_name}", "action")
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
            share streamTable(100:0, `timestampv`sym`blob`price1,[NANOTIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            go
            n = 10
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE]);
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
            share streamTable(100:0, `timestampv`sym`blob`price1,[NANOTIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            go
            n = 5000
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE]);
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
            share streamTable(100:0, `timestampv`sym`blob`price1,[NANOTIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            n = 10;
            dbName = '{db_name}'
            if(existsDatabase(dbName))
                dropDatabase(dbName)
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE])
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
            share streamTable(100:0, `timestampv`sym`blob`price1,[NANOTIMESTAMP,SYMBOL,BLOB,DOUBLE]) as {func_name}
            n = 5000;
            dbName = '{db_name}'
            if(existsDatabase(dbName))
                dropDatabase(dbName)
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [NANOTIMESTAMP, NANOTIMESTAMP, SYMBOL, DOUBLE])
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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
            share streamTable(10000:0,`time`sym`price, [NANOTIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
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
            share streamTable(12000:0,`time`sym`price`id, [NANOTIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}
            insert into {func_name} values(take(now(), 2000), take(`000905`600001`300201`000908`600002, 2000), rand(1000,2000)/10.0, 1..2000)

            share streamTable(120000:0,`time`sym`price`id, [NANOTIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}2
            insert into {func_name}2 values(take(now(), 2000), take(`000905`600001`300201`000908`600002, 2000), rand(1000,2000)/10.0, 1..2000)

            share streamTable(120000:0,`time`sym`price`id, [NANOTIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}3
            insert into {func_name}3 values(take(now(), 2000), take(`000905`600001`300201`000908`600002, 2000), rand(1000,2000)/10.0, 1..2000)

            share streamTable(120000:0,`time`sym`price`id, [NANOTIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}4
            insert into {func_name}4 values(take(now(), 2000), take(`000905`600001`300201`000908`600002, 2000), rand(1000,2000)/10.0, 1..2000)

            share streamTable(120000:0,`time`sym`price`id, [NANOTIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}5
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

    @pytest.mark.CLUSTER
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
        try:
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
        finally:
            operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}', f'dnode{PORT_DNODE3}'], "START")
        conn.unsubscribe(HOST_CLUSTER, PORT_DNODE1, func_name, "test")
        df_expect = pd.DataFrame({
            'a': [i for i in range(1, 51)],
            'b': ["DataNode1"] * 10 + ["DataNode2"] * 10 + ["DataNode3"] * 10 + ["DataNode1"] * 10 + ["DataNode2"] * 10,
        })
        assert_frame_equal(df, df_expect, check_dtype=False)

    @pytest.mark.CLUSTER
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
        try:
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
        finally:
            operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}', f'dnode{PORT_DNODE3}'], "START")
        conn.unsubscribe(HOST_CLUSTER, PORT_DNODE1, func_name, "test")
        assert_frame_equal(df, df_expect, check_dtype=False)

    @pytest.mark.CLUSTER
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
        try:
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
        finally:
            operateNodes(conn, [f'dnode{PORT_DNODE1}', f'dnode{PORT_DNODE2}', f'dnode{PORT_DNODE3}'], "START")
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
        with pytest.raises(ValidationError, match="Input should be a valid list"):
            conn.subscribe(HOST_CLUSTER, PORT, handler(df), func_name, "test", backupSites="127.0.0.1:8848",
                           userName=USER, password=PASSWD)
        with pytest.raises(ValidationError, match="Input should be a valid string"):
            conn.subscribe(HOST_CLUSTER, PORT, handler(df), func_name, "test", backupSites=[1], userName=USER,
                           password=PASSWD)

    @pytest.mark.CLUSTER
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

    @pytest.mark.CLUSTER
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
                           match="Incorrect input .* for backupSite. The port number must be a positive integer no greater than 65535"):
            conn.subscribe(HOST_CLUSTER, PORT_DNODE1, handler(df), "st", "test", backupSites=[f"{HOST_CLUSTER}:65536"],
                           userName=USER, password=PASSWD)

    def test_subscribe_resubTimeout_error(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        with pytest.raises(ValueError, match="Please use resubscibeInterval instead of resubTimeout"):
            conn.subscribe(HOST, PORT, print, "test", "test", resubTimeout=20, userName=USER, password=PASSWD)

    def test_subscribe_resubscribeInterval_table_not_exist(self):
        table_name = "non_existent_table_2063"
        action_name = "test_2063_resub"

        script = f"""
import json, time
import dolphindb as ddb
from dolphindb.logger import Sink, LogMessage

records = []

class ResubSink(Sink):
    def handle(self, msg: LogMessage):
        if "Failed to resubscribe" in msg.log:
            records.append(time.time())

ddb.logger.default_logger.add_sink(ResubSink("ResubSink"))

s = ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}')
s.enableStreaming()
s.subscribe(
    host='{HOST}', port={PORT}, handler=lambda msg: None,
    tableName='{table_name}', actionName='{action_name}',
    offset=0, resub=True, resubscribeInterval=3000,
)
time.sleep(9)
try:
    s.unsubscribe('{HOST}', {PORT}, '{table_name}', '{action_name}')
except Exception:
    pass
s.close()
print(json.dumps(records))
"""

        result = subprocess.run(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
        )
        assert result.returncode == 0, (
            f"子进程执行失败，stderr: {result.stderr}"
        )

        lines = [ln for ln in result.stdout.strip().splitlines() if ln.strip()]
        assert lines, f"子进程无 stdout 输出，stderr: {result.stderr}"
        try:
            timestamps = json.loads(lines[-1])
        except json.JSONDecodeError:
            raise AssertionError(
                f"无法解析子进程输出为 JSON: {lines[-1]!r}，stderr: {result.stderr}"
            )

        assert len(timestamps) <= 4, (
            f"9s 内 Failed to resubscribe 出现 {len(timestamps)} 次，"
            f"远超预期（<=4），resubscribeInterval=3000 未生效。"
        )

        assert len(timestamps) >= 2, (
            f"9s 内仅 {len(timestamps)} 次记录，不足以判断间隔。"
        )

        intervals_ms = [
            (timestamps[i + 1] - timestamps[i]) * 1000
            for i in range(len(timestamps) - 1)
        ]
        min_interval = min(intervals_ms)
        assert min_interval >= 3000, (
            f"最小相邻重试间隔 {min_interval:.0f}ms，小于 resubscribeInterval=3000ms。"
            f"完整间隔(ms): {[round(x, 1) for x in intervals_ms]}"
        )

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_subscribe_resubscribeInterval_backupSites_table_not_exist(self):
        table_name = "non_existent_table_2063_backup"
        action_name = "test_2063_backup"
        interval_ms = 3000
        observe_seconds = 9

        nodes = get_cluster_data_nodes()
        assert len(nodes) >= 3, (
            f"集群 {HOST_CLUSTER} 上可用的数据节点不足 3 个（实际 {nodes}），无法构造 backupSites 场景"
        )
        primary, backup_sites = nodes[0], nodes[1:3]

        for host, port in nodes[:3]:
            node_conn = ddb.Session(host, port, USER_CLUSTER, PASSWD_CLUSTER)
            try:
                node_conn.run(
                    f"if(`{table_name} in objs(true)['name']){{undef(`{table_name}, SHARED)}}")
            finally:
                node_conn.close()

        recorder = ResubscribeRecorder()
        ddb.logger.default_logger.add_sink(recorder)

        sub_conn = ddb.Session(primary[0], primary[1], USER_CLUSTER, PASSWD_CLUSTER)
        sub_conn.enableStreaming()
        try:
            sub_conn.subscribe(
                host=primary[0], port=primary[1],
                handler=lambda msg: None,
                tableName=table_name, actionName=action_name,
                resub=True,
                backupSites=[f"{h}:{p}" for h, p in backup_sites],
                resubscribeInterval=interval_ms,
                userName=USER_CLUSTER, password=PASSWD_CLUSTER,
            )
            time.sleep(observe_seconds)
            try:
                sub_conn.unsubscribe(primary[0], primary[1], table_name, action_name)
            except Exception:
                pass
        finally:
            sub_conn.close()
            ddb.logger.default_logger.remove_sink(recorder.name_)

        times = recorder.times()
        intervals_ms = resubscribe_intervals_ms(times)

        assert 3 <= len(times) <= 12, (
            f"{observe_seconds}s 内重试 {len(times)} 次，超出预期区间 [3, 12]。"
            f"次数过多说明 resubscribeInterval={interval_ms} 未生效（缺陷态约 10ms/次）；"
            f"次数过少说明备份站点没有被重试到。"
        )

        assert min(intervals_ms) <= interval_ms / 2, (
            f"同一轮内相邻重试的最小间隔为 {min(intervals_ms):.0f}ms，"
            f"说明备份站点切换也被 resubscribeInterval={interval_ms} 拖慢了。"
            f"完整间隔(ms): {[round(x) for x in intervals_ms]}"
        )

        assert max(intervals_ms) >= interval_ms - 500, (
            f"相邻重试的最大间隔仅 {max(intervals_ms):.0f}ms，小于 resubscribeInterval={interval_ms}ms，"
            f"说明一轮（主站 + 全部备份站）失败后没有按 interval 等待。"
            f"完整间隔(ms): {[round(x) for x in intervals_ms]}"
        )

    def test_subscribe_resubscribeInterval_table_recreated(self):
        table_name = "table_recreated_2063"
        action_name = "test_recreated"

        admin_conn = ddb.Session(HOST, PORT, USER, PASSWD)
        try:
            admin_conn.run(
                f"if(`{table_name} in objs(true)['name']){{undef(`{table_name}, SHARED)}}")

            received = []
            lock = threading.Lock()

            def handler(data):
                with lock:
                    received.append(data)

            sub_conn = ddb.Session(HOST, PORT, USER, PASSWD)
            sub_conn.enableStreaming()
            try:
                sub_conn.subscribe(
                    host=HOST, port=PORT, handler=handler,
                    tableName=table_name, actionName=action_name,
                    offset=0, resub=True, resubscribeInterval=3000,
                    userName=USER, password=PASSWD,
                )
                time.sleep(3)
                with lock:
                    assert len(received) == 0, f"表尚不存在却收到了数据: {received}"

                admin_conn.run(f"""
                    t = streamTable(100:0, [`a, `b], [INT, STRING])
                    share t as `{table_name}
                    insert into {table_name} values(1, "row1")
                    insert into {table_name} values(2, "row2")
                    insert into {table_name} values(3, "row3")
                """)

                deadline = time.time() + 10
                while time.time() < deadline:
                    with lock:
                        if len(received) >= 3:
                            break
                    time.sleep(0.2)

                with lock:
                    rows = list(received)
                assert len(rows) >= 3, (
                    f"表恢复后未在一个重试周期内收到数据，实际收到: {rows}"
                )
                values = [row[0] for row in rows if isinstance(row, (list, tuple)) and row]
                assert values[:3] == [1, 2, 3], f"收到的数据不符合预期: {rows}"
            finally:
                try:
                    sub_conn.unsubscribe(HOST, PORT, table_name, action_name)
                except Exception:
                    pass
                sub_conn.close()
        finally:
            try:
                admin_conn.run(
                    f"if(`{table_name} in objs(true)['name']){{undef(`{table_name}, SHARED)}}")
            finally:
                admin_conn.close()

    def test_unsubscribe_during_init_retry(self):
        table_name = "table_for_unsubscribe_2063"
        action_name = "test_unsub"

        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.enableStreaming()

        try:
            conn.subscribe(
                host=HOST, port=PORT, handler=lambda x: None,
                tableName=table_name, actionName=action_name,
                resub=True, resubscribeInterval=30000
            )

            time.sleep(2)

            start_time = time.time()
            conn.unsubscribe(HOST, PORT, table_name, action_name)
            cost = time.time() - start_time
            assert cost < 2.0, f"unsubscribe 耗时 {cost:.2f}s，说明重试等待期调用 unsubscribe 发生了阻塞或死锁"

        finally:
            conn.close()