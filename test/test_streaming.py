import inspect
import random
from decimal import Decimal
from threading import Lock
from time import sleep

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest
from dolphindb import SubscriptionConfig, ThreadedStreamingClientConfig, ThreadPooledStreamingClientConfig, \
    ThreadedClient, ThreadPooledClient
from dolphindb.streaming import SubscribeInfo

from basic_testing.utils import equalPlus
from setup.settings import HOST, PORT, USER, PASSWD, HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, \
    HA_STREAM_GROUP_ID, PORT_CNODE1, PORT_DNODE2, PORT_DNODE3


class TestSubscribeInfo(object):

    def test_subscribe_info_str(self):
        assert str(SubscribeInfo("127.0.0.1", 8848, "table", "test")) == "127.0.0.1/8848/table/test"

    def test_subscribe_info_parse(self):
        assert str(SubscribeInfo.parse("127.0.0.1/8848/table/")) == "127.0.0.1/8848/table/"

    def test_subscribe_info_action_none(self):
        assert str(SubscribeInfo("127.0.0.1", 8848, "table", None)) == "127.0.0.1/8848/table/"

    def test_subscribe_info_action_default(self):
        assert str(SubscribeInfo("127.0.0.1", 8848, "table")) == "127.0.0.1/8848/table/"


class TestStreamConfig(object):

    def test_stream_config_subscription_config_default(self):
        config = SubscriptionConfig(host="127.0.0.1", port=8848, handler=print, table_name="table")
        assert config.host == "127.0.0.1"
        assert config.port == 8848
        assert config.handler == print
        assert config.action_name == ""
        assert config.offset == -1
        assert config.resub == False
        assert equalPlus(config.filter, np.array([], dtype='int64'))
        assert config.msg_as_table == False
        assert config.batch_size == 0
        assert config.throttle == 1.0
        assert config.userid == ""
        assert config.password == ""
        assert config.stream_deserializer is None
        assert config.backup_sites == []
        assert config.resubscribe_interval == 100
        assert config.sub_once == False

    def test_stream_config_subscription_config_throttle_check(self):
        with pytest.raises(ValueError, match=r"throttle must be greater than 0\."):
            SubscriptionConfig(host="127.0.0.1", port=8848, handler=print, table_name="table", throttle=0)

    def test_stream_config_threaded_streaming_client_config_default(self):
        config = ThreadedStreamingClientConfig()
        assert config.port == 0

    def test_stream_config_threaded_streaming_client_config_port_less_than_0(self):
        with pytest.raises(Exception):
            ThreadedStreamingClientConfig(port=-1)

    def test_stream_config_thread_pooled_streaming_client_config_default(self):
        config = ThreadPooledStreamingClientConfig()
        assert config.thread_count == 1


class TestThreadedClient(object):

    def test_threaded_client_create_topic_from_params(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        topic = ThreadedClient().subscribe(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                                           userid=USER, password=PASSWD)
        assert str(topic) == f"{HOST}/{PORT}/{func_name}_stream_table/"

    def test_threaded_client_create_topic_from_config_class(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        topic = ThreadedClient().subscribe(
            config=SubscriptionConfig(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                                      userid=USER, password=PASSWD)
        )
        assert str(topic) == f"{HOST}/{PORT}/{func_name}_stream_table/"

    def test_threaded_client_create_topic_from_config_dict(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        topic = ThreadedClient().subscribe(
            config=dict(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table", userid=USER,
                        password=PASSWD)
        )
        assert str(topic) == f"{HOST}/{PORT}/{func_name}_stream_table/"

    def test_threaded_client_error_host(self):
        func_name = inspect.currentframe().f_code.co_name
        with pytest.raises(RuntimeError):
            ThreadedClient().subscribe(host="255.255.255.255", port=PORT, handler=print,
                                       table_name=f"{func_name}_stream_table", userid=USER, password=PASSWD)

    def test_threaded_client_error_port(self):
        func_name = inspect.currentframe().f_code.co_name
        with pytest.raises(RuntimeError):
            ThreadedClient().subscribe(host=HOST, port=-1, handler=print, table_name=f"{func_name}_stream_table",
                                       userid=USER, password=PASSWD)

    def test_threaded_client_error_table_name(self):
        with pytest.raises(RuntimeError):
            ThreadedClient().subscribe(host=HOST, port=PORT, handler=print, table_name=f"error", userid=USER,
                                       password=PASSWD)

    def test_threaded_client_need_login(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        client = ThreadedClient()
        with pytest.raises(RuntimeError, match="Login is required for script execution with client authentication enabled"):
            client.subscribe(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                             action_name="same_name")

    def test_threaded_client_same_action(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        client = ThreadedClient()
        topic1 = client.subscribe(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                                  action_name="same_name", userid=USER, password=PASSWD)
        with pytest.raises(RuntimeError):
            client.subscribe(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                             action_name="same_name", userid=USER, password=PASSWD)
        client.unsubscribe(subscribe_info=topic1)

    def test_threaded_client_batch_size_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        with pytest.raises(ValueError):
            ThreadedClient().subscribe(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                                       batch_size=0, msg_as_table=True, userid=USER, password=PASSWD)

    def test_threaded_client_throttle_lt_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        with pytest.raises(ValueError):
            ThreadedClient().subscribe(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                                       throttle=-1, userid=USER, password=PASSWD)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_offset_zero(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(config=dict(port=port))
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                                 password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df, conn.run(func_name))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_offset_lt_zero(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(config=ThreadedStreamingClientConfig(port=port))
        topic1 = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                                  password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df, conn.run(func_name))
        client.unsubscribe(subscribe_info=topic1)
        topic2 = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=-1, userid=USER,
                                  password=PASSWD)
        conn.run(f"""
                insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, 11..15 as price)
                {func_name}.append!(insert_table)
        """)
        assert wait_until(15)
        assert equalPlus(df, conn.run(func_name))
        client.unsubscribe(subscribe_info=topic2)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_offset_gt_zero(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic1 = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=5, userid=USER,
                                  password=PASSWD)
        assert wait_until(5)
        assert equalPlus(df, conn.run(f"select * from {func_name} limit 5,5"))
        client.unsubscribe(subscribe_info=topic1)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_resub_True(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, resub=True,
                                 userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df, conn.run(func_name))
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
        """)
        conn.run(f"""
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, 11..15 as price)
            {func_name}.append!(insert_table)
        """)
        assert wait_until(15)
        assert equalPlus(df, conn.run(func_name))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_resub_False(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, resub=False,
                                 userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df, conn.run(func_name))
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
        """)
        conn.run(f"""
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, 11..15 as price)
            {func_name}.append!(insert_table)
        """)
        sleep(3)
        assert len(df) == 10
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_filter(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 filter=np.array(["000905"]), userid=USER, password=PASSWD)
        assert wait_until(2)
        assert equalPlus(df, conn.run(f"select * from {func_name} where sym=`000905"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_filter_lambda(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 filter="msg -> select * from msg where sym=`000905", userid=USER, password=PASSWD)
        assert wait_until(2)
        assert equalPlus(df, conn.run(f"select * from {func_name} where sym=`000905"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_double_array_vector_msgAsTable_False(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f'''
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
        ''')
        df = pd.DataFrame(columns=["symbolv", "doublev"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 msg_as_table=False, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df, conn.run(func_name))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_double_array_vector_msgAsTable_True(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f'''
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
        ''')
        df = pd.DataFrame(columns=["symbolv", "doublev"])

        def handler(data):
            nonlocal df
            df = pd.concat([df, data], ignore_index=True)

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 msg_as_table=True, batch_size=1000, throttle=1, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df, conn.run(func_name))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_double_throttle_gt_zero(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df = pd.concat([df, data], ignore_index=True)

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 msg_as_table=True, batch_size=1000,
                                 throttle=7, userid=USER, password=PASSWD)
        assert not wait_until(10)
        assert len(df) == 0
        assert wait_until(10)
        assert equalPlus(df, conn.run(func_name))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_streamDeserializer_memory_table_session_None(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
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
        """)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        sd = ddb.streamDeserializer({"msg1": f"{func_name}_1", "msg2": f"{func_name}_2"}, None)

        def handler(data):
            nonlocal df1
            nonlocal df2
            if data[-1] == "msg1":
                df1.loc[len(df1)] = data
            else:
                df2.loc[len(df2)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df1) >= len_ and len(df2) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 stream_deserializer=sd, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df1.loc[:, :"price2"], conn.run(f"{func_name}_1"))
        assert equalPlus(df2.loc[:, :"price1"], conn.run(f"{func_name}_2"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_streamDeserializer_PartitionedTable_session_None(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        db_name = f"dfs://{func_name}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
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
        """)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        sd = ddb.streamDeserializer({"msg1": [db_name, "pt1"], "msg2": [db_name, "pt2"]}, None)

        def handler(data):
            nonlocal df1
            nonlocal df2
            if data[-1] == "msg1":
                df1.loc[len(df1)] = data
            else:
                df2.loc[len(df2)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df1) >= len_ and len(df2) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 stream_deserializer=sd, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df1.loc[:, :"price2"], conn.run("select * from pt1"))
        assert equalPlus(df2.loc[:, :"price1"], conn.run("select * from pt2"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_streamDeserializer_memory_table_session(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
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
        """)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        sd = ddb.streamDeserializer({"msg1": f"{func_name}_1", "msg2": f"{func_name}_2"}, conn)

        def handler(data):
            nonlocal df1
            nonlocal df2
            if data[-1] == "msg1":
                df1.loc[len(df1)] = data
            else:
                df2.loc[len(df2)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df1) >= len_ and len(df2) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 stream_deserializer=sd, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df1.loc[:, :"price2"], conn.run(f"{func_name}_1"))
        assert equalPlus(df2.loc[:, :"price1"], conn.run(f"{func_name}_2"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_streamDeserializer_PartitionedTable_session(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        db_name = f"dfs://{func_name}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
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
        """)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        sd = ddb.streamDeserializer({"msg1": [db_name, "pt1"], "msg2": [db_name, "pt2"]}, conn)

        def handler(data):
            nonlocal df1
            nonlocal df2
            if data[-1] == "msg1":
                df1.loc[len(df1)] = data
            else:
                df2.loc[len(df2)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df1) >= len_ and len(df2) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 stream_deserializer=sd, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df1.loc[:, :"price2"], conn.run("select * from pt1"))
        assert equalPlus(df2.loc[:, :"price1"], conn.run("select * from pt2"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_unsubscribe_auto(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                                 password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df, conn.run(func_name))

    @pytest.mark.parametrize("port", [0, -1])
    def test_threaded_client_unsubscribe_by_host(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                         password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df, conn.run(func_name))
        client.unsubscribe(HOST, PORT, func_name)

    def test_threaded_client_topics(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        client = ThreadedClient()
        assert len(client.topics) == 0
        assert len(client.topic_strs) == 0
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                                 password=PASSWD)
        assert len(client.topics) == 1
        assert len(client.topic_strs) == 1
        assert str(client.topics[0]) == f"{HOST}/{PORT}/{func_name}/"
        assert client.topic_strs[0] == f"{HOST}/{PORT}/{func_name}/"
        client.unsubscribe(topic)
        assert len(client.topics) == 0
        assert len(client.topic_strs) == 0

    def test_threaded_client_all_type(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(
                [true,false,00b] as c_bool,
                array(BOOL[]).append!(cut(take(true false 00b,9),3)) as av_bool,
                [1c,0c,00c] as c_char,
                array(CHAR[]).append!(cut(take(1c 0c 00c,9),3)) as av_char,
                [1h,0h,00h] as c_short,
                array(SHORT[]).append!(cut(take(1h 0h 00h,9),3)) as av_short,
                [1i,0i,00i] as c_int,
                array(INT[]).append!(cut(take(1i 0i 00i,9),3)) as av_int,
                [1l,0l,00l] as c_long,
                array(LONG[]).append!(cut(take(1l 0l 00l,9),3)) as av_long,
                [2013.06.13d,1970.01.01d,00d] as c_date,
                array(DATE[]).append!(cut(take(2024.01.10d 1970.01.01d 00d,9),3)) as av_date,
                [2013.06M,1970.01M,00M] as c_month,
                array(MONTH[]).append!(cut(take(2024.01M 1970.01M 00M,9),3)) as av_month,
                [13:30:10.008t,00:00:00.000t,00t] as c_time,
                array(TIME[]).append!(cut(take(12:00:00.000t 00:00:00.000t 00t,9),3)) as av_time,
                [13:30m,00:00m,00m] as c_minute,
                array(MINUTE[]).append!(cut(take(12:00m 00:00m 00m,9),3)) as av_minute,
                [13:30:00s,00:00:00s,00s] as c_seconde,
                array(SECOND[]).append!(cut(take(12:00:00s 00:00:00s 00s,9),3)) as av_second,
                [2012.06.13T13:30:10,1970.01.01T00:00:00D,00D] as c_datetime,
                array(DATETIME[]).append!(cut(take(2024.01.01T12:00:00D 1970.01.01T00:00:00D 00D,9),3)) as av_datetime,
                [2012.06.13T13:30:10.008T,1970.01.01T00:00:00.000T,00T] as c_timestamp,
                array(TIMESTAMP[]).append!(cut(take(2024.01.01T12:00:00.000T 1970.01.01T00:00:00.000T 00T,9),3)) as av_timestamp,
                [13:30:10.008007006n,00:00:00.000000000n,00n] as c_nanotime,
                array(NANOTIME[]).append!(cut(take(12:00:00.000000000n 00:00:00.000000000n 00n,9),3)) as av_nanotime,
                [2012.06.13T13:30:10.008007006N,1970.01.01T00:00:00.000000000N,00N] as c_nanotimestamp,
                array(NANOTIMESTAMP[]).append!(cut(take(2024.01.01T12:00:00.000000000N 1970.01.01T00:00:00.000000000N 00N,9),3)) as av_nanotimestamp,
                [datehour("2012.06.13T13"),datehour("1970.01.01T00"),datehour(null)] as c_datehour,
                array(DATEHOUR[]).append!(cut(take(datehour("2024.01.01T12" "1970.01.01T00" null),9),3)) as av_datehour,
                [float('nan'),3.14f,00f] as c_float,
                array(FLOAT[]).append!(cut(take([0.0f,1.0f,00f],9),3)) as av_float,
                [double('nan'),3.14F,00F] as c_double,
                array(DOUBLE[]).append!(cut(take([0.0F,1.0F,00F],9),3)) as av_double,
                ["123","a",""] as c_string,
                blob(["123","a",""]) as c_blob,
                symbol(["123","a",""]) as c_symbol,
                uuid(["5d212a78-cc48-e3b1-4235-b4d91473ee87","5d212a78-cc48-e3b1-4235-b4d91473ee87","00000000-0000-0000-0000-000000000000"]) as c_uuid,
                array(UUID[]).append!(cut(take(uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87" "5d212a78-cc48-e3b1-4235-b4d91473ee87" "00000000-0000-0000-0000-000000000000"),9),3)) as av_uuid,
                ipaddr(["127.0.0.1","192.168.0.1","0.0.0.0"]) as c_ipaddr,
                array(IPADDR[]).append!(cut(take(ipaddr("127.0.0.1" "192.168.0.1" "0.0.0.0"),9),3)) as av_ipaddr,
                int128(["e1671797c52e15f763380b45e841ec32","e1671797c52e15f763380b45e841ec32","00000000000000000000000000000000"]) as c_int128,
                array(INT128[]).append!(cut(take(int128("5d212a78cc48e3b14235b4d91473ee87" "5d212a78cc48e3b14235b4d91473ee87" "00000000000000000000000000000000"),9),3)) as av_int128,
                decimal32(["3.14","0","nan"],2) as c_decimal32,
                array(DECIMAL32(2)[]).append!(cut(take(decimal32("3.14" "0" "nan",2),9),3)) as av_decimal32,
                decimal64(["3.14","0","nan"],6) as c_decimal64,
                array(DECIMAL64(6)[]).append!(cut(take(decimal64("3.14" "0" "nan",6),9),3)) as av_decimal64,
                decimal128(["3.14","0","nan"],8) as c_decimal128,
                array(DECIMAL128(8)[]).append!(cut(take(decimal128("3.14" "0" "nan",8),9),3)) as av_decimal128
            ) as {func_name}
        """)
        df = conn.run(f"select * from {func_name} where 1==0")

        def handler(data):
            nonlocal df
            df = pd.concat([df, data], ignore_index=True)

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient()
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 msg_as_table=True, batch_size=3, userid=USER, password=PASSWD)
        assert wait_until(3)
        assert equalPlus(df, conn.run(func_name))
        client.unsubscribe(subscribe_info=topic)

    def test_threaded_client_type_not_support(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            share streamTable([complex(1,2)] as data) as {func_name}
        """)

        client = ThreadedClient()
        topic = client.subscribe(host=HOST, port=PORT, handler=print, table_name=func_name, offset=0,
                                 msg_as_table=True, batch_size=3, userid=USER, password=PASSWD)
        sleep(3)
        client.unsubscribe(subscribe_info=topic)

    def test_threaded_client_orca_streaming_subscribe_on_leader(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.DBConnection()
        conn.connect(HOST, PORT, USER, PASSWD)
        conn.exec(f"""
            if (existsCatalog("{func_name}"))
                dropCatalog("{func_name}")
            createCatalog("{func_name}")
            go
            use catalog {func_name}
            g=createStreamGraph("engine")
            g.source("trades", ["time","sym","volume"], [TIMESTAMP, SYMBOL, INT])
            .timeSeriesEngine(windowSize=60000, step=60000, metrics=<[sum(volume)]>, timeColumn="time", useSystemTime=false, keyColumn="sym", useWindowStartTime=false)
            .sink("output")
            g.submit()
            times = [2018.10.08T01:01:01.785, 2018.10.08T01:01:02.125, 2018.10.08T01:01:10.263, 2018.10.08T01:01:12.457, 2018.10.08T01:02:10.789, 2018.10.08T01:02:12.005, 2018.10.08T01:02:30.021, 2018.10.08T01:04:02.236, 2018.10.08T01:04:04.412, 2018.10.08T01:04:05.152]
            syms = [`A, `B, `B, `A, `A, `B, `A, `A, `B, `B]
            volumes = [10, 26, 14, 28, 15, 9, 10, 29, 32, 23]
            tmp = table(times as time, syms as sym, volumes as volume)
            appendOrcaStreamTable("trades", tmp)
        """)
        df = conn.run(f"select * from {func_name}.orca_table.output where 1==0")

        def handler(data):
            nonlocal df
            df = pd.concat([df, data], ignore_index=True)

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient()
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=f"{func_name}.orca_table.output", msg_as_table=True, batch_size=4, offset=0, userid=USER, password=PASSWD)
        wait_until(4)
        client.unsubscribe(subscribe_info=topic)
        assert equalPlus(df, conn.run(f"select * from {func_name}.orca_table.output"))

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_threaded_client_orca_streaming_subscribe_on_follower(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.DBConnection()
        conn.connect(HOST_CLUSTER, PORT_CNODE1, USER_CLUSTER, PASSWD_CLUSTER)
        conn.exec(f"""
            if (existsCatalog("{func_name}"))
                dropCatalog("{func_name}")
            createCatalog("{func_name}")
            go
            use catalog {func_name}
            g=createStreamGraph("engine")
            g.source("trades", ["time","sym","volume"], [TIMESTAMP, SYMBOL, INT])
            .timeSeriesEngine(windowSize=60000, step=60000, metrics=<[sum(volume)]>, timeColumn="time", useSystemTime=false, keyColumn="sym", useWindowStartTime=false)
            .sink("output")
            g.submit()
            times = [2018.10.08T01:01:01.785, 2018.10.08T01:01:02.125, 2018.10.08T01:01:10.263, 2018.10.08T01:01:12.457, 2018.10.08T01:02:10.789, 2018.10.08T01:02:12.005, 2018.10.08T01:02:30.021, 2018.10.08T01:04:02.236, 2018.10.08T01:04:04.412, 2018.10.08T01:04:05.152]
            syms = [`A, `B, `B, `A, `A, `B, `A, `A, `B, `B]
            volumes = [10, 26, 14, 28, 15, 9, 10, 29, 32, 23]
            tmp = table(times as time, syms as sym, volumes as volume)
            appendOrcaStreamTable("trades", tmp)
        """)
        df = conn.run(f"select * from {func_name}.orca_table.output where 1==0")

        def handler(data):
            nonlocal df
            df = pd.concat([df, data], ignore_index=True)

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        port = conn.run(f"(exec port from getClusterPerf() where name != (exec site from getOrcaStreamTableMeta() where fqn = \"{func_name}.orca_table.output\") and mode = 0 limit 1)[0]")
        client = ThreadedClient()
        topic = client.subscribe(host=HOST_CLUSTER, port=port, handler=handler, table_name=f"{func_name}.orca_table.output", msg_as_table=True, batch_size=4, offset=0, userid=USER, password=PASSWD)
        wait_until(4)
        client.unsubscribe(subscribe_info=topic)
        assert equalPlus(df, conn.run(f"select * from {func_name}.orca_table.output"))


class TestThreadPooledClient(object):

    def test_thread_pooled_client_create_topic_from_params(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        topic = ThreadPooledClient(thread_count=2).subscribe(host=HOST, port=PORT, handler=print,
                                                             table_name=f"{func_name}_stream_table", userid=USER,
                                                             password=PASSWD)
        assert str(topic) == f"{HOST}/{PORT}/{func_name}_stream_table/"

    def test_thread_pooled_client_create_topic_from_config_class(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        topic = ThreadPooledClient(thread_count=2).subscribe(
            config=SubscriptionConfig(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                                      userid=USER, password=PASSWD)
        )
        assert str(topic) == f"{HOST}/{PORT}/{func_name}_stream_table/"

    def test_thread_pooled_client_create_topic_from_config_dict(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        topic = ThreadPooledClient(thread_count=2).subscribe(
            config=dict(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table", userid=USER,
                        password=PASSWD)
        )
        assert str(topic) == f"{HOST}/{PORT}/{func_name}_stream_table/"

    def test_thread_pooled_client_error_host(self):
        func_name = inspect.currentframe().f_code.co_name
        with pytest.raises(RuntimeError):
            ThreadPooledClient(thread_count=2).subscribe(host="255.255.255.255", port=PORT, handler=print,
                                                         table_name=f"{func_name}_stream_table", userid=USER,
                                                         password=PASSWD)

    def test_thread_pooled_client_error_port(self):
        func_name = inspect.currentframe().f_code.co_name
        with pytest.raises(RuntimeError):
            ThreadPooledClient(thread_count=2).subscribe(host=HOST, port=-1, handler=print,
                                                         table_name=f"{func_name}_stream_table", userid=USER,
                                                         password=PASSWD)

    def test_thread_pooled_client_error_table_name(self):
        with pytest.raises(RuntimeError):
            ThreadPooledClient(thread_count=2).subscribe(host=HOST, port=PORT, handler=print, table_name=f"error",
                                                         userid=USER, password=PASSWD)

    def test_thread_pooled_client_same_action(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        client = ThreadPooledClient(thread_count=2)
        topic1 = client.subscribe(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                                  action_name="same_name", userid=USER, password=PASSWD)
        with pytest.raises(RuntimeError):
            client.subscribe(host=HOST, port=PORT, handler=print, table_name=f"{func_name}_stream_table",
                             action_name="same_name", userid=USER, password=PASSWD)
        client.unsubscribe(subscribe_info=topic1)

    def test_thread_pooled_client_batch_size_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        with pytest.raises(ValueError):
            ThreadPooledClient(thread_count=2).subscribe(host=HOST, port=PORT, handler=print,
                                                         table_name=f"{func_name}_stream_table", batch_size=0,
                                                         msg_as_table=True, userid=USER, password=PASSWD)

    def test_thread_pooled_client_throttle_lt_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"share streamTable(1..3 as id) as {func_name}_stream_table")
        with pytest.raises(ValueError):
            ThreadPooledClient(thread_count=2).subscribe(host=HOST, port=PORT, handler=print,
                                                         table_name=f"{func_name}_stream_table", throttle=-1,
                                                         userid=USER, password=PASSWD)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_offset_zero(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                                 password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by price asc"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_offset_lt_zero(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic1 = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                                  password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by price asc"))
        client.unsubscribe(subscribe_info=topic1)
        topic2 = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=-1, userid=USER,
                                  password=PASSWD)
        conn.run(f"""
                insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, 11..15 as price)
                {func_name}.append!(insert_table)
        """)
        assert wait_until(15)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by price asc"))
        client.unsubscribe(subscribe_info=topic2)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_offset_gt_zero(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])

        def handler(data):
            nonlocal df
            df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadedClient(port=port)
        topic1 = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=5, userid=USER,
                                  password=PASSWD)
        assert wait_until(5)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by price asc limit 5,5"))
        client.unsubscribe(subscribe_info=topic1)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_resub_True(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, resub=True,
                                 userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by price asc"))
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
        """)
        conn.run(f"""
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, 11..15 as price)
            {func_name}.append!(insert_table)
        """)
        assert wait_until(15)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by price asc"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_resub_False(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, resub=False,
                                 userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by price asc"))
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
        """)
        conn.run(f"""
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, 11..15 as price)
            {func_name}.append!(insert_table)
        """)
        sleep(3)
        assert len(df) == 10
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_filter(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 filter=np.array(["000905"]), userid=USER, password=PASSWD)
        assert wait_until(2)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} where sym=`000905 order by price asc"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_filter_lambda(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 filter="msg -> select * from msg where sym=`000905", userid=USER, password=PASSWD)
        assert wait_until(2)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} where sym=`000905 order by price asc"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_double_array_vector_msgAsTable_False(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f'''
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
        ''')
        df = pd.DataFrame(columns=["symbolv", "doublev"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 msg_as_table=False, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df.sort_values("symbolv", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by symbolv asc"))
        client.unsubscribe(subscribe_info=topic)

    def test_thread_pooled_client_double_array_vector_msgAsTable_True(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f'''
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
        ''')
        df = pd.DataFrame(columns=["symbolv", "doublev"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df = pd.concat([df, data], ignore_index=True)

        client = ThreadPooledClient(thread_count=2)
        with pytest.raises(RuntimeError):
            client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, msg_as_table=True,
                             batch_size=1000, throttle=1, userid=USER, password=PASSWD)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_streamDeserializer_memory_table_session_None(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
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
        """)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        sd = ddb.streamDeserializer({"msg1": f"{func_name}_1", "msg2": f"{func_name}_2"}, None)
        lock = Lock()

        def handler(data):
            nonlocal df1
            nonlocal df2
            with lock:
                if data[-1] == "msg1":
                    df1.loc[len(df1)] = data
                else:
                    df2.loc[len(df2)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df1) >= len_ and len(df2) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 stream_deserializer=sd, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df1.loc[:, :"price2"].sort_values("timestampv", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name}_1 order by timestampv asc"))
        assert equalPlus(df2.loc[:, :"price1"].sort_values("timestampv", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name}_2 order by timestampv asc"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_streamDeserializer_PartitionedTable_session_None(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        db_name = f"dfs://{func_name}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
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
        """)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        sd = ddb.streamDeserializer({"msg1": [db_name, "pt1"], "msg2": [db_name, "pt2"]}, None)
        lock = Lock()

        def handler(data):
            nonlocal df1
            nonlocal df2
            with lock:
                if data[-1] == "msg1":
                    df1.loc[len(df1)] = data
                else:
                    df2.loc[len(df2)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df1) >= len_ and len(df2) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 stream_deserializer=sd, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df1.loc[:, :"price2"].sort_values("timestampv", ascending=True).reset_index(drop=True),
                         conn.run("select * from pt1 order by timestampv asc"))
        assert equalPlus(df2.loc[:, :"price1"].sort_values("timestampv", ascending=True).reset_index(drop=True),
                         conn.run("select * from pt2 order by timestampv asc"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_streamDeserializer_memory_table_session(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
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
        """)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        sd = ddb.streamDeserializer({"msg1": f"{func_name}_1", "msg2": f"{func_name}_2"}, conn)
        lock = Lock()

        def handler(data):
            nonlocal df1
            nonlocal df2
            with lock:
                if data[-1] == "msg1":
                    df1.loc[len(df1)] = data
                else:
                    df2.loc[len(df2)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df1) >= len_ and len(df2) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 stream_deserializer=sd, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df1.loc[:, :"price2"].sort_values("timestampv", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name}_1 order by timestampv asc"))
        assert equalPlus(df2.loc[:, :"price1"].sort_values("timestampv", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name}_2 order by timestampv asc"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_streamDeserializer_PartitionedTable_session(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        db_name = f"dfs://{func_name}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
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
        """)
        df1 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "price2", "table"])
        df2 = pd.DataFrame(columns=["datetimev", "timestampv", "sym", "price1", "table"])
        sd = ddb.streamDeserializer({"msg1": [db_name, "pt1"], "msg2": [db_name, "pt2"]}, conn)
        lock = Lock()

        def handler(data):
            nonlocal df1
            nonlocal df2
            with lock:
                if data[-1] == "msg1":
                    df1.loc[len(df1)] = data
                else:
                    df2.loc[len(df2)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df1) >= len_ and len(df2) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0,
                                 stream_deserializer=sd, userid=USER, password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df1.loc[:, :"price2"].sort_values("timestampv", ascending=True).reset_index(drop=True),
                         conn.run("select * from pt1 order by timestampv asc"))
        assert equalPlus(df2.loc[:, :"price1"].sort_values("timestampv", ascending=True).reset_index(drop=True),
                         conn.run("select * from pt2 order by timestampv asc"))
        client.unsubscribe(subscribe_info=topic)

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_unsubscribe_auto(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                         password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by price asc"))

    @pytest.mark.parametrize("port", [0, -1])
    def test_thread_pooled_client_unsubscribe_by_host(self, port):
        if port == -1:
            port = random.randint(30001, 39999)
        func_name = inspect.currentframe().f_code.co_name + f"_{port}"
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), 1..10)
        """)
        df = pd.DataFrame(columns=["time", "sym", "price"])
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(port=port, thread_count=2)
        client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                         password=PASSWD)
        assert wait_until(10)
        assert equalPlus(df.sort_values("price", ascending=True).reset_index(drop=True),
                         conn.run(f"select * from {func_name} order by price asc"))
        client.unsubscribe(HOST, PORT, func_name)

    def test_thread_pooled_client_all_type(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(
                [true,false,00b] as c_bool,
                array(BOOL[]).append!(cut(take(true false 00b,9),3)) as av_bool,
                [1c,0c,00c] as c_char,
                array(CHAR[]).append!(cut(take(1c 0c 00c,9),3)) as av_char,
                [1h,0h,00h] as c_short,
                array(SHORT[]).append!(cut(take(1h 0h 00h,9),3)) as av_short,
                [1i,0i,00i] as c_int,
                array(INT[]).append!(cut(take(1i 0i 00i,9),3)) as av_int,
                [1l,0l,00l] as c_long,
                array(LONG[]).append!(cut(take(1l 0l 00l,9),3)) as av_long,
                [2013.06.13d,1970.01.01d,00d] as c_date,
                array(DATE[]).append!(cut(take(2024.01.10d 1970.01.01d 00d,9),3)) as av_date,
                [2013.06M,1970.01M,00M] as c_month,
                array(MONTH[]).append!(cut(take(2024.01M 1970.01M 00M,9),3)) as av_month,
                [13:30:10.008t,00:00:00.000t,00t] as c_time,
                array(TIME[]).append!(cut(take(12:00:00.000t 00:00:00.000t 00t,9),3)) as av_time,
                [13:30m,00:00m,00m] as c_minute,
                array(MINUTE[]).append!(cut(take(12:00m 00:00m 00m,9),3)) as av_minute,
                [13:30:00s,00:00:00s,00s] as c_seconde,
                array(SECOND[]).append!(cut(take(12:00:00s 00:00:00s 00s,9),3)) as av_second,
                [2012.06.13T13:30:10,1970.01.01T00:00:00D,00D] as c_datetime,
                array(DATETIME[]).append!(cut(take(2024.01.01T12:00:00D 1970.01.01T00:00:00D 00D,9),3)) as av_datetime,
                [2012.06.13T13:30:10.008T,1970.01.01T00:00:00.000T,00T] as c_timestamp,
                array(TIMESTAMP[]).append!(cut(take(2024.01.01T12:00:00.000T 1970.01.01T00:00:00.000T 00T,9),3)) as av_timestamp,
                [13:30:10.008007006n,00:00:00.000000000n,00n] as c_nanotime,
                array(NANOTIME[]).append!(cut(take(12:00:00.000000000n 00:00:00.000000000n 00n,9),3)) as av_nanotime,
                [2012.06.13T13:30:10.008007006N,1970.01.01T00:00:00.000000000N,00N] as c_nanotimestamp,
                array(NANOTIMESTAMP[]).append!(cut(take(2024.01.01T12:00:00.000000000N 1970.01.01T00:00:00.000000000N 00N,9),3)) as av_nanotimestamp,
                [datehour("2012.06.13T13"),datehour("1970.01.01T00"),datehour(null)] as c_datehour,
                array(DATEHOUR[]).append!(cut(take(datehour("2024.01.01T12" "1970.01.01T00" null),9),3)) as av_datehour,
                [float('nan'),3.14f,00f] as c_float,
                array(FLOAT[]).append!(cut(take([0.0f,1.0f,00f],9),3)) as av_float,
                [double('nan'),3.14F,00F] as c_double,
                array(DOUBLE[]).append!(cut(take([0.0F,1.0F,00F],9),3)) as av_double,
                ["123","a",""] as c_string,
                blob(["123","a",""]) as c_blob,
                symbol(["123","a",""]) as c_symbol,
                uuid(["5d212a78-cc48-e3b1-4235-b4d91473ee87","5d212a78-cc48-e3b1-4235-b4d91473ee87","00000000-0000-0000-0000-000000000000"]) as c_uuid,
                array(UUID[]).append!(cut(take(uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87" "5d212a78-cc48-e3b1-4235-b4d91473ee87" "00000000-0000-0000-0000-000000000000"),9),3)) as av_uuid,
                ipaddr(["127.0.0.1","192.168.0.1","0.0.0.0"]) as c_ipaddr,
                array(IPADDR[]).append!(cut(take(ipaddr("127.0.0.1" "192.168.0.1" "0.0.0.0"),9),3)) as av_ipaddr,
                int128(["e1671797c52e15f763380b45e841ec32","e1671797c52e15f763380b45e841ec32","00000000000000000000000000000000"]) as c_int128,
                array(INT128[]).append!(cut(take(int128("e1671797c52e15f763380b45e841ec32" "e1671797c52e15f763380b45e841ec32" "00000000000000000000000000000000"),9),3)) as av_int128,
                decimal32(["3.14","0","nan"],2) as c_decimal32,
                array(DECIMAL32(2)[]).append!(cut(take(decimal32("3.14" "0" "nan",2),9),3)) as av_decimal32,
                decimal64(["3.14","0","nan"],6) as c_decimal64,
                array(DECIMAL64(6)[]).append!(cut(take(decimal64("3.14" "0" "nan",6),9),3)) as av_decimal64,
                decimal128(["3.14","0","nan"],8) as c_decimal128,
                array(DECIMAL128(8)[]).append!(cut(take(decimal128("3.14" "0" "nan",8),9),3)) as av_decimal128
            ) as {func_name}
        """)
        df = conn.run(f"select * from {func_name} where 1==0")
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=func_name, offset=0, userid=USER,
                                 password=PASSWD)
        assert wait_until(3)
        df_expect = pd.DataFrame({
            "c_bool": [True, False, None],
            "av_bool": [np.array([True, False, None], dtype="object") for i in range(3)],
            "c_char": np.array([1, 0, None], dtype='object'),
            "av_char": [np.array([1, 0, None], dtype='float64') for i in range(3)],
            "c_short": np.array([1, 0, None], dtype='object'),
            "av_short": [np.array([1, 0, None], dtype='float64') for i in range(3)],
            "c_int": np.array([1, 0, None], dtype='object'),
            "av_int": [np.array([1, 0, None], dtype='float64') for i in range(3)],
            "c_long": np.array([1, 0, None], dtype='object'),
            "av_long": [np.array([1, 0, None], dtype='float64') for i in range(3)],
            "c_date": np.array([np.datetime64("2013-06-13", "D"), np.datetime64("1970-01-01", "D"), None],
                               dtype="datetime64[ns]"),
            "av_date": [np.array([np.datetime64("2024-01-10", "D"), np.datetime64("1970-01-01", "D"), None],
                                 dtype="datetime64[D]") for i in range(3)],
            "c_month": np.array([np.datetime64("2013-06-01", "D"), np.datetime64("1970-01-01", "D"), None],
                                dtype="datetime64[ns]"),
            "av_month": [
                np.array([np.datetime64("2024-01", "M"), np.datetime64("1970-01", "M"), None], dtype="datetime64[M]")
                for i in range(3)],
            "c_time": np.array(
                [np.datetime64("1970-01-01 13:30:10.008", "ns"), np.datetime64("1970-01-01", "ns"), None],
                dtype="datetime64[ns]"),
            "av_time": [np.array([np.datetime64("1970-01-01T12", "ms"), np.datetime64("1970-01-01", "ms"), None],
                                 dtype="datetime64[ms]") for i in range(3)],
            "c_minute": np.array([np.datetime64("1970-01-01 13:30:00", "ns"), np.datetime64("1970-01-01", "ns"), None],
                                 dtype="datetime64[ns]"),
            "av_minute": [np.array([np.datetime64("1970-01-01T12", "m"), np.datetime64("1970-01-01", "m"), None],
                                   dtype="datetime64[m]") for i in range(3)],
            "c_seconde": np.array([np.datetime64("1970-01-01 13:30:00", "ns"), np.datetime64("1970-01-01", "ns"), None],
                                  dtype="datetime64[ns]"),
            "av_second": [np.array([np.datetime64("1970-01-01T12", "s"), np.datetime64("1970-01-01", "s"), None],
                                   dtype="datetime64[s]") for i in range(3)],
            "c_datetime": np.array(
                [np.datetime64("2012-06-13 13:30:10", "ns"), np.datetime64("1970-01-01", "ns"), None],
                dtype="datetime64[ns]"),
            "av_datetime": [np.array([np.datetime64("2024-01-01T12", "s"), np.datetime64("1970-01-01", "s"), None],
                                     dtype="datetime64[s]") for i in range(3)],
            "c_timestamp": np.array(
                [np.datetime64("2012-06-13 13:30:10.008", "ns"), np.datetime64("1970-01-01", "ns"), None],
                dtype="datetime64[ns]"),
            "av_timestamp": [np.array([np.datetime64("2024-01-01T12", "ms"), np.datetime64("1970-01-01", "ms"), None],
                                      dtype="datetime64[ms]") for i in range(3)],
            "c_nanotime": np.array(
                [np.datetime64("1970-01-01 13:30:10.008007006", "ns"), np.datetime64("1970-01-01", "ns"), None],
                dtype="datetime64[ns]"),
            "av_nanotime": [np.array([np.datetime64("1970-01-01T12", "ns"), np.datetime64("1970-01-01", "ns"), None],
                                     dtype="datetime64[ns]") for i in range(3)],
            "c_nanotimestamp": np.array(
                [np.datetime64("2012-06-13 13:30:10.008007006", "ns"), np.datetime64("1970-01-01", "ns"), None],
                dtype="datetime64[ns]"),
            "av_nanotimestamp": [
                np.array([np.datetime64("2024-01-01T12", "ns"), np.datetime64("1970-01-01", "ns"), None],
                         dtype="datetime64[ns]") for i in range(3)],
            "c_datehour": np.array(
                [np.datetime64("2012-06-13 13:00:00", "ns"), np.datetime64("1970-01-01", "ns"), None],
                dtype="datetime64[ns]"),
            "av_datehour": [np.array([np.datetime64("2024-01-01T12", "h"), np.datetime64("1970-01-01", "h"), None],
                                     dtype="datetime64[h]") for i in range(3)],
            "c_float": [np.nan, 3.14, np.nan],
            "av_float": [np.array([0, 1, np.nan], dtype="float32") for i in range(3)],
            "c_double": [np.nan, 3.14, np.nan],
            "av_double": [np.array([0, 1, np.nan], dtype="float64") for i in range(3)],
            "c_string": ["123", "a", None],
            "c_blob": [b"123", b"a", None],
            "c_symbol": ["123", "a", None],
            "c_uuid": ["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee87", None],
            "av_uuid": [np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "5d212a78-cc48-e3b1-4235-b4d91473ee87",
                                  "00000000-0000-0000-0000-000000000000"], dtype="object") for i in range(3)],
            "c_ipaddr": ["127.0.0.1", "192.168.0.1", None],
            "av_ipaddr": [np.array(["127.0.0.1", "192.168.0.1", "0.0.0.0"], dtype="object") for i in range(3)],
            "c_int128": ["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec32", None],
            "av_int128": [np.array(["e1671797c52e15f763380b45e841ec32", "e1671797c52e15f763380b45e841ec32",
                                    "00000000000000000000000000000000"], dtype="object") for i in range(3)],
            "c_decimal32": [Decimal("3.14"), Decimal("0.00"), None],
            "av_decimal32": [np.array([Decimal("3.14"), Decimal("0.00"), None], dtype="object") for i in range(3)],
            "c_decimal64": [Decimal("3.140000"), Decimal("0.000000"), None],
            "av_decimal64": [np.array([Decimal("3.140000"), Decimal("0.000000"), None], dtype="object") for i in
                             range(3)],
            "c_decimal128": [Decimal("3.14000000"), Decimal("0.00000000"), None],
            "av_decimal128": [np.array([Decimal("3.14000000"), Decimal("0.00000000"), None], dtype="object") for i in
                              range(3)],
        })
        assert equalPlus(df.sort_values("c_char", ascending=True).reset_index(drop=True),
                         df_expect.sort_values("c_char", ascending=True).reset_index(drop=True))
        client.unsubscribe(subscribe_info=topic)

    def test_thread_pooled_client_type_not_support(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.Session(host=HOST, port=PORT, userid=USER, password=PASSWD)
        conn.run(f"""
            share streamTable([complex(1,2)] as data) as {func_name}
        """)

        client = ThreadPooledClient(thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=print, table_name=func_name, offset=0,
                                 userid=USER, password=PASSWD)
        sleep(3)
        client.unsubscribe(subscribe_info=topic)

    def test_thread_pooled_client_orca_streaming_subscribe_on_leader(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.DBConnection()
        conn.connect(HOST, PORT, USER, PASSWD)
        conn.exec(f"""
            if (existsCatalog("{func_name}"))
                dropCatalog("{func_name}")
            createCatalog("{func_name}")
            go
            use catalog {func_name}
            g=createStreamGraph("engine")
            g.source("trades", ["time","sym","volume"], [TIMESTAMP, SYMBOL, INT])
            .timeSeriesEngine(windowSize=60000, step=60000, metrics=<[sum(volume)]>, timeColumn="time", useSystemTime=false, keyColumn="sym", useWindowStartTime=false)
            .sink("output")
            g.submit()
            times = [2018.10.08T01:01:01.785, 2018.10.08T01:01:02.125, 2018.10.08T01:01:10.263, 2018.10.08T01:01:12.457, 2018.10.08T01:02:10.789, 2018.10.08T01:02:12.005, 2018.10.08T01:02:30.021, 2018.10.08T01:04:02.236, 2018.10.08T01:04:04.412, 2018.10.08T01:04:05.152]
            syms = [`A, `B, `B, `A, `A, `B, `A, `A, `B, `B]
            volumes = [10, 26, 14, 28, 15, 9, 10, 29, 32, 23]
            tmp = table(times as time, syms as sym, volumes as volume)
            appendOrcaStreamTable("trades", tmp)
        """)
        df = conn.run(f"select * from {func_name}.orca_table.output where 1==0")
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        client = ThreadPooledClient(thread_count=2)
        topic = client.subscribe(host=HOST, port=PORT, handler=handler, table_name=f"{func_name}.orca_table.output", offset=0, userid=USER,
                                 password=PASSWD)
        wait_until(4)
        client.unsubscribe(subscribe_info=topic)
        assert equalPlus(df.sort_values("time", ascending=True).sort_values("sym", ascending=True).reset_index(drop=True), conn.run(f"select * from {func_name}.orca_table.output").sort_values("time", ascending=True).sort_values("sym", ascending=True).reset_index(drop=True))

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_thread_pooled_client_orca_streaming_subscribe_on_follower(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.DBConnection()
        conn.connect(HOST_CLUSTER, PORT_CNODE1, USER_CLUSTER, PASSWD_CLUSTER)
        conn.exec(f"""
            if (existsCatalog("{func_name}"))
                dropCatalog("{func_name}")
            createCatalog("{func_name}")
            go
            use catalog {func_name}
            g=createStreamGraph("engine")
            g.source("trades", ["time","sym","volume"], [TIMESTAMP, SYMBOL, INT])
            .timeSeriesEngine(windowSize=60000, step=60000, metrics=<[sum(volume)]>, timeColumn="time", useSystemTime=false, keyColumn="sym", useWindowStartTime=false)
            .sink("output")
            g.submit()
            times = [2018.10.08T01:01:01.785, 2018.10.08T01:01:02.125, 2018.10.08T01:01:10.263, 2018.10.08T01:01:12.457, 2018.10.08T01:02:10.789, 2018.10.08T01:02:12.005, 2018.10.08T01:02:30.021, 2018.10.08T01:04:02.236, 2018.10.08T01:04:04.412, 2018.10.08T01:04:05.152]
            syms = [`A, `B, `B, `A, `A, `B, `A, `A, `B, `B]
            volumes = [10, 26, 14, 28, 15, 9, 10, 29, 32, 23]
            tmp = table(times as time, syms as sym, volumes as volume)
            appendOrcaStreamTable("trades", tmp)
        """)
        df = conn.run(f"select * from {func_name}.orca_table.output where 1==0")
        lock = Lock()

        def handler(data):
            nonlocal df
            with lock:
                df.loc[len(df)] = data

        def wait_until(len_, timeout=5):
            for i in range(timeout):
                if len(df) >= len_:
                    break
                sleep(1)
            else:
                return False
            return True

        port = conn.run(f"(exec port from getClusterPerf() where name != (exec site from getOrcaStreamTableMeta() where fqn = \"{func_name}.orca_table.output\") and mode = 0 limit 1)[0]")
        client = ThreadPooledClient(thread_count=2)
        topic = client.subscribe(host=HOST_CLUSTER, port=port, handler=handler, table_name=f"{func_name}.orca_table.output", offset=0, userid=USER,
                                 password=PASSWD)
        wait_until(4)
        client.unsubscribe(subscribe_info=topic)
        assert equalPlus(df.sort_values("time", ascending=True).sort_values("sym", ascending=True).reset_index(drop=True), conn.run(f"select * from {func_name}.orca_table.output").sort_values("time", ascending=True).sort_values("sym", ascending=True).reset_index(drop=True))

class TestHaStreaming(object):

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_ha_streaming_subscribe_haStreamTable_on_leader(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.DBConnection()
        conn.connect(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER)
        leader = conn.exec(f"""
            rtn=""
            for (i in 1:20){{
                try{{
                    rtn=getStreamingLeader({HA_STREAM_GROUP_ID})
                }}catch(ex){{
                    sleep(500)
                    continue
                }}
                break
            }}
            rtn
        """)
        leader_port = conn.exec(f"exec port from rpc(getControllerAlias(), getClusterPerf) where name=\"{leader}\"")[0]
        conn_leader = ddb.DBConnection()
        conn_leader.connect(HOST_CLUSTER, leader_port, USER_CLUSTER, PASSWD_CLUSTER)
        conn_leader.run(f"""
            try{{dropStreamTable("{func_name}")}}catch(ex){{}};
            {func_name}_result = table(1000000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);
            haStreamTable({HA_STREAM_GROUP_ID}, {func_name}_result, "{func_name}", 100000);
            go;
            batch = 1000;
            tmp = table(batch:batch, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);
            tmp[`permno] = take(1..100, batch);
            tmp[`timestamp] = take(now(), batch);
            tmp[`ticker] = rand(\"A\" + string(1..100), batch);
            tmp[`price1] = rand(100, batch);
            tmp[`price2] = rand(100, batch);
            tmp[`price3] = rand(100, batch);
            tmp[`price4] = rand(100, batch);
            tmp[`price5] = rand(100, batch);
            tmp[`vol1] = rand(100, batch);
            tmp[`vol2] = rand(100, batch);
            tmp[`vol3] = rand(100, batch);
            tmp[`vol4] = rand(100, batch);
            tmp[`vol5] = rand(100, batch);
            {func_name}.append!(tmp); 
        """)

        def my_handler(data):
            conn_leader.call(f"tableInsert{{{func_name}_result}}",*data)

        client = ThreadedClient()
        topic = client.subscribe(host=HOST_CLUSTER, port=leader_port, handler=my_handler, table_name=func_name, offset=0, userid=USER, password=PASSWD)
        while conn_leader.exec(f"exec count(*) from {func_name}_result") < 1000:
            sleep(1)
        assert conn_leader.exec(f"each(eqObj, {func_name}.values(), {func_name}_result.values()).all()")
        client.unsubscribe(subscribe_info=topic)
        conn_leader.exec(f"dropStreamTable(\"{func_name}\")")

    @pytest.mark.CLUSTER
    @pytest.mark.xdist_group(name='cluster_test')
    def test_ha_streaming_subscribe_haStreamTable_on_follower(self):
        func_name = inspect.currentframe().f_code.co_name
        conn = ddb.DBConnection()
        conn.connect(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER)
        leader = conn.exec(f"""
            rtn=""
            for (i in 1:20){{
                try{{
                    rtn=getStreamingLeader({HA_STREAM_GROUP_ID})
                }}catch(ex){{
                    sleep(500)
                    continue
                }}
                break
            }}
            rtn
        """)
        follower_port = conn.exec(f"exec port from rpc(getControllerAlias(), getClusterPerf) where site in (exec sites[0] from getStreamingRaftGroups() where id=={HA_STREAM_GROUP_ID}).split(\",\") and name!=\"{leader}\"")[0]
        conn_follower = ddb.DBConnection()
        conn_follower.connect(HOST_CLUSTER, follower_port, USER_CLUSTER, PASSWD_CLUSTER, enable_high_availability=True, high_availability_sites=[f"{HOST_CLUSTER}:{PORT_DNODE1}",f"{HOST_CLUSTER}:{PORT_DNODE2}",f"{HOST_CLUSTER}:{PORT_DNODE3}"])
        conn_follower.run(f"""
            try{{dropStreamTable("{func_name}")}}catch(ex){{}};
            {func_name}_result = table(1000:0, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);
            haStreamTable({HA_STREAM_GROUP_ID}, {func_name}_result, "{func_name}", 1000);
            go;
            do {{
                sleep(500)
            }} while((exec count(*) from objs(true) where name="{func_name}")<1)
            batch = 1000;
            tmp = table(batch:batch, `permno`timestamp`ticker`price1`price2`price3`price4`price5`vol1`vol2`vol3`vol4`vol5, [INT, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, INT, INT, INT, INT, INT]);
            tmp[`permno] = take(1..100, batch);
            tmp[`timestamp] = take(now(), batch);
            tmp[`ticker] = rand("A" + string(1..100), batch);
            tmp[`price1] = rand(100, batch);
            tmp[`price2] = rand(100, batch);
            tmp[`price3] = rand(100, batch);
            tmp[`price4] = rand(100, batch);
            tmp[`price5] = rand(100, batch);
            tmp[`vol1] = rand(100, batch);
            tmp[`vol2] = rand(100, batch);
            tmp[`vol3] = rand(100, batch);
            tmp[`vol4] = rand(100, batch);
            tmp[`vol5] = rand(100, batch);
            objByName("{func_name}",true).append!(tmp);
        """)

        def my_handler(data):
            conn_follower.call(f"tableInsert{{{func_name}_result}}",*data)

        client = ThreadedClient()
        topic = client.subscribe(host=HOST_CLUSTER, port=follower_port, handler=my_handler, table_name=func_name, offset=0, userid=USER, password=PASSWD)
        while conn_follower.exec(f"exec count(*) from {func_name}_result") < 1000:
            sleep(1)
        assert conn_follower.exec(f"each(eqObj, {func_name}.values(), {func_name}_result.values()).all()")
        client.unsubscribe(subscribe_info=topic)
        conn_follower.exec(f"dropStreamTable(\"{func_name}\")")
