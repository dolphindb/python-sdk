import subprocess
import time
from itertools import chain

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest
from pandas.testing import *

from setup.settings import *
from setup.utils import CountBatchDownLatch
from setup.utils import get_pid


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
    import random
    return random.randint(30001, 39999)


class TestSubscribe:
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
        self.conn.run("""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
                stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
        """)

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
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def handler(self, lst):
        index = len(self.df)
        self.df.loc[index] = lst

    def handler_df(self, counter):
        def handler(lst):
            self.df = lst
            counter.countDown(1)

        return handler

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_host_int(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades1) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades1
            setStreamTableFilterColumn(trades1, `sym)
            insert into trades1 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(1, PORT, self.handler,
                            "trades1", "action", 0, False)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_host_fail_connect(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades2) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades2
            setStreamTableFilterColumn(trades2, `sym)
            insert into trades2 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe("999999:9999:9999:9999", PORT,
                            self.handler, "trades2", "action", 0, False)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_port_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades3) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades3
            setStreamTableFilterColumn(trades3, `sym)
            insert into trades3 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, "dsf", self.handler,
                            "trades3", "action", 0, False)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_port_fail_connect(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades4) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades4
            setStreamTableFilterColumn(trades4, `sym)
            insert into trades4 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, -1, self.handler,
                            "trades4", "action", 0, False)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_tableName_int(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades5) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades5
            setStreamTableFilterColumn(trades5, `sym)
            insert into trades5 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, 1, "action", 0, False)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_tableName_not_exist(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades6) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades6
            setStreamTableFilterColumn(trades6, `sym)
            insert into trades6 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler,
                            "skdfls", "action", 0, False)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_actionName_int(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades7) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades7
            setStreamTableFilterColumn(trades7, `sym)
            insert into trades7 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, "trades7", 1, 0, False)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_actionName_same_name(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades8) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades8
            setStreamTableFilterColumn(trades8, `sym)
            insert into trades8 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler,
                            "trades8", "action", 0, False)
            conn1.subscribe(HOST, PORT, self.handler,
                            "trades8", "action", 0, False)
        conn1.unsubscribe(HOST, PORT, "trades8", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_offset_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades9) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades9
            setStreamTableFilterColumn(trades9, `sym)
            insert into trades9 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler,
                            "trades9", "action", "fsd", False)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_resub_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades10) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades10
            setStreamTableFilterColumn(trades10, `sym)
            insert into trades10 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler,
                            "trades10", "action", 0, "fsd")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_filter(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades15) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades15
            setStreamTableFilterColumn(trades15, `sym)
            insert into trades15 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler,
                            "trades15", "action", 0, False, "dfs")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_msgAsTable_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades15) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades15
            setStreamTableFilterColumn(trades15, `sym)
            insert into trades15 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, "trades15",
                            "action", 0, False, np.array(["000905"]), "11")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_msgAsTable_True_batchSize_zero(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades15) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades15
            setStreamTableFilterColumn(trades15, `sym)
            insert into trades15 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, "trades15",
                            "action", 0, False, np.array(["000905"]), True)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_batchSize_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades15) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades15
            setStreamTableFilterColumn(trades15, `sym)
            insert into trades15 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, "trades15",
                            "action", 0, False, np.array(["000905"]), False, "fdsf")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_batchSize_float(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades15) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades15
            setStreamTableFilterColumn(trades15, `sym)
            insert into trades15 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, "trades15",
                            "action", 0, False, np.array(["000905"]), False, 1.1)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_throttle_string(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades15) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades15
            setStreamTableFilterColumn(trades15, `sym)
            insert into trades15 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, "trades15", "action",
                            0, False, np.array(["000905"]), False, -1, "sdfse")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_throttle_float(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades15) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades15
            setStreamTableFilterColumn(trades15, `sym)
            insert into trades15 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, "trades15",
                            "action", 0, False, np.array(["000905"]), False, -1, -1)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_error_throttle_lt_zero(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades15) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades15
            setStreamTableFilterColumn(trades15, `sym)
            insert into trades15 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, "trades15",
                            "action", 0, False, np.array(["000905"]), False, -1, -1)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_offset_zero(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades11) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades11
            setStreamTableFilterColumn(trades11, `sym)
            insert into trades11 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades11", "action", 0, False)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades11"))
        print(df)
        conn1.unsubscribe(HOST, PORT, "trades11", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_offset_lt_zero_gt(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades12) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades12
            setStreamTableFilterColumn(trades12, `sym)
            insert into trades12 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades12", "action", 0, False)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades12"))
        conn1.unsubscribe(HOST, PORT, "trades12", "action")

        df = pd.DataFrame(columns=["time", "sym", "price"])
        counter.reset(5)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades12", "action", -1, False)
        script = """
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, rand(1000,5)/10.0 as price)
            trades12.append!(insert_table)
        """
        conn1.run(script)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from insert_table"))
        conn1.unsubscribe(HOST, PORT, "trades12", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_resub_True(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades13) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades13
            setStreamTableFilterColumn(trades13, `sym)
            insert into trades13 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades13", "action", 0, True)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades13"))
        script = """
            stopPublishTable("{}", {}, `trades13, `action)
        """.format(CLIENT_HOST, SUBPORT)
        conn1.run(script)
        time.sleep(1)
        script = """
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, rand(1000,5)/10.0 as price)
            trades13.append!(insert_table)
        """
        counter.reset(5)
        conn1.run(script)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades13"))
        conn1.unsubscribe(HOST, PORT, "trades13", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_resub_False(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades14) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades14
            setStreamTableFilterColumn(trades14, `sym)
            insert into trades14 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades14", "action", 0, False)
        assert counter.wait_s(20)
        script = """
            stopPublishTable("{}", {}, `trades14, `action)
        """.format(CLIENT_HOST, SUBPORT)
        time.sleep(3)
        conn1.run(script)
        script = """
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, rand(1000,5)/10.0 as price)
            trades14.append!(insert_table)
        """
        conn1.run(script)
        time.sleep(3)
        assert len(df) == 10
        conn1.unsubscribe(HOST, PORT, "trades14", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_filter(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price", "id"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades16) }catch(ex){}
            share streamTable(10000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE, INT]) as trades16
            setStreamTableFilterColumn(trades16, `sym)
            insert into trades16 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0, int(1..10))
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(2)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades16", "action", 0, False, np.array(["000905"], ))
        assert counter.wait_s(20)
        df["id"] = df["id"].astype(np.int32)
        assert_frame_equal(df, conn1.run(
            "select * from trades16 where sym=`000905"))
        conn1.unsubscribe(HOST, PORT, "trades16", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_batchSize_lt_zero(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["symbolv", "doublev"])
        script = '''
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }   
            try{ dropStreamTable(`trades_doublev) }catch(ex){}
            share streamTable(10000:0,`symbolv`doublev, [SYMBOL, DOUBLE]) as trades_doublev
            n = 10
            exTable = table(n:0, `symbolv`doublev, [SYMBOL, DOUBLE])
            symbol_vector=take(`A`B`C`D`E`F`G, n)
            double_vector=take(double([36,98,95,69,41,60,78,92,78,21]), n)
            exTable.tableInsert(symbol_vector, double_vector)
            trades_doublev.append!(exTable)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(
            df, counter), "trades_doublev", "action", 0, False, msgAsTable=False, batchSize=-1)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades_doublev"))
        conn1.unsubscribe(HOST, PORT, "trades_doublev", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_subscribe_msgAsTable_False_batchSize_gt_zero(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["symbolv", "doublev"])
        script = '''
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            } 
            try{ dropStreamTable(`trades_doublev) }catch(ex){}
            share streamTable(10000:0,`symbolv`doublev, [SYMBOL, DOUBLE]) as trades_doublev
            n = 10
            exTable = table(n:0, `symbolv`doublev, [SYMBOL, DOUBLE])
            symbol_vector=take(`A`B`C`D`E`F`G, n)
            double_vector=take(double([36,98,95,69,41,60,78,92,78,21]), n)
            exTable.tableInsert(symbol_vector, double_vector)
            trades_doublev.append!(exTable)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(5)
        conn1.subscribe(HOST, PORT, gethandler_multi_row(
            df, counter), "trades_doublev", "action", 0, False, msgAsTable=False, batchSize=2)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades_doublev"))
        conn1.unsubscribe(HOST, PORT, "trades_doublev", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_throttle_gt_zero(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = '''
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades14) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades14
            setStreamTableFilterColumn(trades14, `sym)
            insert into trades14 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(1)
        conn1.subscribe(HOST, PORT, self.handler_df(counter), "trades14",
                        "action", 0, False, msgAsTable=True, batchSize=1000, throttle=10.1)
        assert not counter.wait_s(10)
        assert len(self.df) == 0
        counter.reset(1)
        assert counter.wait_s(20)
        assert len(self.df) == 10
        assert_frame_equal(self.df, conn1.run("select * from trades14"))
        conn1.unsubscribe(HOST, PORT, "trades14", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_error_sym2table_None(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        with pytest.raises(Exception):
            sd = ddb.streamDeserializer(None, conn1)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_error_sym2table_scalar(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        with pytest.raises(Exception):
            sd = ddb.streamDeserializer(1, conn1)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_error_sym2table_list(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        with pytest.raises(Exception):
            sd = ddb.streamDeserializer(["a", "b"], conn1)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_error_sym2table_dict_value_list_size_not_2(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        with pytest.raises(Exception):
            sd = ddb.streamDeserializer(
                {"msg1": ["table1"], "msg2": ["table2"]}, conn1)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_error_sym2table_dict_value_tuple_size_not_2(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        with pytest.raises(Exception):
            sd = ddb.streamDeserializer(
                {"msg1": ("table1",), "msg2": ("table2",)}, conn1)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_error_session_scalar(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        with pytest.raises(Exception):
            sd = ddb.streamDeserializer(["a", "b"], 1)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_error_session_list(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        with pytest.raises(Exception):
            sd = ddb.streamDeserializer(["a", "b"], [conn1, conn1])
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_error_msgAsTable_True(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df1 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`outTables7) }catch(ex){}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables7
            go
            n = 10
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
            d = dict(['msg1','msg2'], [table1, table2]);
            replay(inputTables=d, outputTables=`outTables7, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        sd = ddb.streamDeserializer(
            {"msg1": "table1", "msg2": "table2"}, conn1)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2), "outTables7",
                            "action", 0, False, msgAsTable=True, streamDeserializer=sd, batchSize=5)
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_memory_talbe_session_None(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df1 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            undef all
            try{ dropStreamTable(`outTables6) }catch(ex){}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables6
            go
            n = 10
            t1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            t2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
            share t1 as table1
            share t2 as table2
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
            d = dict(['msg1','msg2'], [table1, table2]);
            replay(inputTables=d, outputTables=`outTables6, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(20)
        sd = ddb.streamDeserializer({"msg1": "table1", "msg2": "table2"}, None)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter),
                        "outTables6", "action", 0, False, msgAsTable=False, streamDeserializer=sd)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"],
                           conn1.run("select * from table1"))
        assert_frame_equal(df2.loc[:, :"price1"],
                           conn1.run("select * from table2"))
        conn1.unsubscribe(HOST, PORT, "outTables6", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_PartitionedTable_table_session_None(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df1 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{dropStreamTable(`outTables5)}catch(ex){}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables5
            n = 10;
            dbName = 'dfs://test_StreamDeserializer_pair'
            if(existsDatabase(dbName)){
                dropDB(dbName)}
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
            pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1)
            pt2 = db.createPartitionedTable(table2,'pt2',`datetimev).append!(table2)
            d = dict(['msg1','msg2'], [table1, table2])
            replay(inputTables=d, outputTables=`outTables5, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(20)
        sd = ddb.streamDeserializer({"msg1": ['dfs://test_StreamDeserializer_pair', "pt1"], "msg2": [
            'dfs://test_StreamDeserializer_pair', "pt2"]}, None)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), "outTables5", "action",
                        0, False, msgAsTable=False, streamDeserializer=sd, userName="admin", password="123456")
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"],
                           conn1.run("select * from pt1"))
        assert_frame_equal(df2.loc[:, :"price1"],
                           conn1.run("select * from pt2"))
        conn1.unsubscribe(HOST, PORT, "outTables5", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_memory_table(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df1 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`outTables4) }catch(ex){}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables4
            go
            n = 10
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
            d = dict(['msg1','msg2'], [table1, table2]);
            replay(inputTables=d, outputTables=`outTables4, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(20)
        sd = ddb.streamDeserializer(
            {"msg1": "table1", "msg2": "table2"}, conn1)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter),
                        "outTables4", "action", 0, False, msgAsTable=False, streamDeserializer=sd)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"],
                           conn1.run("select * from table1"))
        assert_frame_equal(df2.loc[:, :"price1"],
                           conn1.run("select * from table2"))
        conn1.unsubscribe(HOST, PORT, "outTables4", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_memory_table_10000_rows(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df1 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`outTables3) }catch(ex){}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables3
            go
            n = 5000
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE]);
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n));
            d = dict(['msg1','msg2'], [table1, table2]);
            replay(inputTables=d, outputTables=`outTables3, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10000)
        sd = ddb.streamDeserializer(
            {"msg1": "table1", "msg2": "table2"}, conn1)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter),
                        "outTables3", "action", 0, False, msgAsTable=False, streamDeserializer=sd)
        assert counter.wait_s(200)
        assert_frame_equal(df1.loc[:, :"price2"],
                           conn1.run("select * from table1"))
        assert_frame_equal(df2.loc[:, :"price1"],
                           conn1.run("select * from table2"))
        conn1.unsubscribe(HOST, PORT, "outTables3", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_PartitionedTable_table(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df1 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{dropStreamTable(`outTables2)}catch(ex){}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables2
            n = 10;
            dbName = 'dfs://test_StreamDeserializer_pair'
            if(existsDatabase(dbName)){
                dropDB(dbName)}
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
            pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1)
            pt2 = db.createPartitionedTable(table2,'pt2',`datetimev).append!(table2)
            d = dict(['msg1','msg2'], [table1, table2])
            replay(inputTables=d, outputTables=`outTables2, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(20)
        sd = ddb.streamDeserializer({"msg1": ['dfs://test_StreamDeserializer_pair', "pt1"], "msg2": [
            'dfs://test_StreamDeserializer_pair', "pt2"]}, conn1)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), "outTables2", "action",
                        0, False, msgAsTable=False, streamDeserializer=sd, userName="admin", password="123456")
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"],
                           conn1.run("select * from pt1"))
        assert_frame_equal(df2.loc[:, :"price1"],
                           conn1.run("select * from pt2"))
        conn1.unsubscribe(HOST, PORT, "outTables2", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enalbeStreaming_streamDeserializer_PartitionedTable_table_10000_rows(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df1 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(
            columns=["datetimev", "timestampv", "sym", "price1", "table"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{dropStreamTable(`outTables1)}catch(ex){}
            share streamTable(100:0, `timestampv`sym`blob`price1,[TIMESTAMP,SYMBOL,BLOB,DOUBLE]) as outTables1
            n = 5000;
            dbName = 'dfs://test_StreamDeserializer_pair'
            if(existsDatabase(dbName)){
                dropDB(dbName)}
            db = database(dbName,RANGE,2012.01.01 2013.01.01 2014.01.01 2015.01.01 2016.01.01 2017.01.01 2018.01.01 2019.01.01)
            table1 = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])
            table2 = table(100:0, `datetimev`timestampv`sym`price1, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE])
            tableInsert(table1, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n))
            tableInsert(table2, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n))
            pt1 = db.createPartitionedTable(table1,'pt1',`datetimev).append!(table1)
            pt2 = db.createPartitionedTable(table2,'pt2',`datetimev).append!(table2)
            d = dict(['msg1','msg2'], [table1, table2])
            replay(inputTables=d, outputTables=`outTables1, dateColumn=`timestampv, timeColumn=`timestampv)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10000)
        sd = ddb.streamDeserializer({"msg1": ['dfs://test_StreamDeserializer_pair', "pt1"], "msg2": [
            'dfs://test_StreamDeserializer_pair', "pt2"]}, conn1)
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), "outTables1", "action",
                        0, False, msgAsTable=False, streamDeserializer=sd, userName="admin", password="123456")
        assert counter.wait_s(200)
        assert_frame_equal(df1.loc[:, :"price2"],
                           conn1.run("select * from pt1"))
        assert_frame_equal(df2.loc[:, :"price1"],
                           conn1.run("select * from pt2"))
        conn1.unsubscribe(HOST, PORT, "outTables1", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    @pytest.mark.parametrize(argnames="error_host", argvalues=[None, "sdjfk", "999.999.999.9"],
                             ids=["host_none", "host_str", "host_error"])
    def test_enableStreaming_unsubscribe_error_host(self, error_host):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades11) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades11
            setStreamTableFilterColumn(trades11, `sym)
            insert into trades11 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades11", "action", 0, False)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades11"))
        with pytest.raises(Exception):
            conn1.unsubscribe(error_host, PORT, "trades11", "action")
        conn1.unsubscribe(HOST, PORT, "trades11", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    @pytest.mark.parametrize(argnames="error_port", argvalues=[None, "sdjfk", -1],
                             ids=["port_none", "port_str", "port_error"])
    def test_enableStreaming_unsubscribe_error_port(self, error_port):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades11) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades11
            setStreamTableFilterColumn(trades11, `sym)
            insert into trades11 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades11", "action", 0, False)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades11"))
        with pytest.raises(Exception):
            conn1.unsubscribe(HOST, error_port, "trades11", "action")
        conn1.unsubscribe(HOST, PORT, "trades11", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    @pytest.mark.parametrize(argnames="error_tableName", argvalues=[None, 12, "fddksj"],
                             ids=["tableName_none", "tableName_str", "tableName_error"])
    def test_enableStreaming_unsubscribe_error_tableName(self, error_tableName):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades11) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades11
            setStreamTableFilterColumn(trades11, `sym)
            insert into trades11 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades11", "action", 0, False)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades11"))
        with pytest.raises(Exception):
            conn1.unsubscribe(HOST, PORT, error_tableName, "action")
        conn1.unsubscribe(HOST, PORT, "trades11", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    @pytest.mark.parametrize(argnames="error_actionName", argvalues=[None, 12, "fddksj"],
                             ids=["actionName_none", "actionName_str", "actionName_error"])
    def test_enableStreaming_unsubscribe_error_actionName(self, error_actionName):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades11) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades11
            setStreamTableFilterColumn(trades11, `sym)
            insert into trades11 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades11", "action", 0, False)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades11"))
        with pytest.raises(Exception):
            conn1.unsubscribe(HOST, PORT, "trades11", error_actionName)
        conn1.unsubscribe(HOST, PORT, "trades11", "action")
        conn1.close()

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_enableStreaming_error_port(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        with pytest.raises(Exception):
            conn1.enableStreaming("adc")

        with pytest.raises(Exception):
            conn1.enableStreaming(-1)

        with pytest.raises(Exception):
            conn1.enableStreaming(None)

    @pytest.mark.SUBSCRIBE
    def test_enableStreaming_getSubscriptionTopics(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        script = """
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){
	            stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
            }
            try{ dropStreamTable(`trades11) }catch(ex){}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as trades11
            setStreamTableFilterColumn(trades11, `sym)
            insert into trades11 values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter),
                        "trades11", "action", 0, False)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run("select * from trades11"))
        ans = conn1.getSubscriptionTopics()
        get_host = ans[0].split("/")[0]
        get_port = ans[0].split("/")[1]
        get_tableName = ans[0].split("/")[2]
        get_actionName = ans[0].split("/")[3]
        assert get_host == HOST, "1"
        assert int(get_port) == PORT, "2"
        assert get_tableName == "trades11", "3"
        assert get_actionName == "action", "4"
        conn1.unsubscribe(HOST, PORT, "trades11", "action")
        ans = conn1.getSubscriptionTopics()
        assert len(ans) == 0, "5"

    @pytest.mark.SUBSCRIBE
    @pytest.mark.timeout(1200)
    def test_enableStreaming_subscribe_many_tables(self):
        self.conn.run("""
            share streamTable(120000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades
            insert into trades values(take(now(), 200000), take(`000905`600001`300201`000908`600002, 200000), rand(1000,200000)/10.0, 1..200000)

            share streamTable(120000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades2
            insert into trades2 values(take(now(), 200000), take(`000905`600001`300201`000908`600002, 200000), rand(1000,200000)/10.0, 1..200000)

            share streamTable(120000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades3
            insert into trades3 values(take(now(), 200000), take(`000905`600001`300201`000908`600002, 200000), rand(1000,200000)/10.0, 1..200000)

            share streamTable(120000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades4
            insert into trades4 values(take(now(), 200000), take(`000905`600001`300201`000908`600002, 200000), rand(1000,200000)/10.0, 1..200000)

            share streamTable(120000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as trades5
            insert into trades5 values(take(now(), 200000), take(`000905`600001`300201`000908`600002, 200000), rand(1000,200000)/10.0, 1..200000)
        """)

        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)
        counter = CountBatchDownLatch(200000)
        counter2 = CountBatchDownLatch(200000)
        counter3 = CountBatchDownLatch(200000)
        counter4 = CountBatchDownLatch(200000)
        counter5 = CountBatchDownLatch(200000)
        counters = [counter, counter2, counter3, counter4, counter5]
        results = [[], [], [], [], []]
        tables = ['trades', 'trades2', 'trades3', 'trades4', 'trades5']

        def tmp_handle(ndlist: list, counter):
            print("get msg")

            def handler(lst):
                ndlist.append(lst)
                counter.countDown(len(lst))

            return handler

        for ind, tab in enumerate(tables):
            co = counters[ind]
            conn1.subscribe(host=HOST,
                            port=PORT,
                            handler=tmp_handle(results[ind], co),
                            tableName=tab, actionName="action", offset=0,
                            resub=True,
                            filter=None,
                            batchSize=100,
                            throttle=1
                            )

        for ind, co in enumerate(counters):
            assert co.wait_s(200)
            ex_df = self.conn.run(f"select * from {tables[ind]}")

            for res in results:
                print(len(res))
            res1 = list(chain.from_iterable(results[ind]))
            for i in range(len(res1)):
                # print( "\n\n==============================", res1[i], list(ex_df.iloc[i]), "\n\n==============================")
                assert res1[i] == list(ex_df.iloc[i])
            print(f"all datas from table {tables[ind]} assert pass")

        for tab in tables:
            conn1.unsubscribe(HOST, PORT, tab, "action")
        conn1.close()
        self.conn.run(
            "undef(`trades,SHARED);undef(`trades2,SHARED);undef(`trades3,SHARED);undef(`trades4,SHARED);undef(`trades5,SHARED);go")

    def test_enalbeStreaming_exception_in_handler(self):
        script = """
            colName=["time","x"]
            colType=["timestamp","int"]
            t = streamTable(100:0, colName, colType);
            share t as st;go
            insert into st values(now(), rand(100.00,1))
        """
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "from time import sleep;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 f"conn.enableStreaming({SUBPORT});"
                                 f"conn.run(\"\"\"{script}\"\"\");"
                                 f"conn.subscribe('{HOST}',{PORT},lambda :raise RuntimeError('this should be catched'),'st','test',0,True);"
                                 "conn.run('insert into st values(now(), rand(100.00,1))');"
                                 "sleep(3);"
                                 "conn.unsubscribe('{HOST}',{PORT},'st','test');"
                                 "conn.run('undef(`st,SHARED)');"
                                 "conn.close();"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert "this should be catched" in result.stdout

    def test_enalbeStreaming_subscribe_keyededStreamTable(self):
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, "admin", "123456")
        conn1.enableStreaming(SUBPORT)

        script = """
            colName=["time","sym", "p1", "p2", "ind"]
            colType=["timestamp","symbol","double","double","int"]
            t = keyedStreamTable(`time, 100:0, colName, colType);
            share t as st;go
            for(i in 0:100){
                insert into st values(now(), rand(`APPL`GOOG`TESLA`TX`BABA`AMAZON,1), rand(200.00, 1), rand(200.00, 1), rand(1000, 1))
            }
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        res = []

        def tmp_handle(array, co):
            def myhandler(lst):
                array.append(lst)
                co.countDown(1)

            return myhandler

        conn1.subscribe(HOST, PORT, tmp_handle(res, counter), "st", "test", 0, True)
        assert counter.wait_s(10)
        conn1.unsubscribe(HOST, PORT, "st", "test")
        ex_df = conn1.run('select * from st order by time')
        for i in range(len(res)):
            assert res[i] == list(ex_df.iloc[i])

        conn1.undef('st', 'SHARED')
        conn1.close()


if __name__ == '__main__':
    pytest.main(["-s", "test/test_subscribe.py"])
