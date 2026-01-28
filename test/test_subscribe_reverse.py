import inspect
import subprocess
import sys
import time
from itertools import chain

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from pandas._testing import assert_frame_equal

from setup.prepare import CountBatchDownLatch
from setup.settings import HOST, PORT, USER, PASSWD


def gethandler(df, counter):
    def handler(lst):
        index = len(df)
        df.loc[index] = lst
        counter.countDown(1)

    return handler


def gethandler_multi_row(df, counter):
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


class TestSubscribeReverse:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    def handler(self, lst):
        index = len(self.df)
        self.df.loc[index] = lst

    def handler_df(self, counter):
        def handler(lst):
            self.df = lst
            counter.countDown(1)

        return handler

    def handler_batch_df(self, counter):
        def handler(lst):
            self.batchNum += 1
            self.num += len(lst)
            if self.df is None:
                self.df = lst
            else:
                self.df = pd.concat([self.df, lst], ignore_index=True)
            counter.countDown(len(lst))

        return handler

    def handler_batch_df_sleep_10(self, counter):
        def handler(lst):
            time.sleep(10)
            self.batchNum += 1
            self.num += len(lst)
            self.df = pd.concat([self.df, lst], ignore_index=True)
            counter.countDown(len(lst))

        return handler

    def handler_batch_list(self, counter):
        def handler(lst):
            self.batchNum += 1
            self.num += len(lst)
            self.lst.extend(lst)
            counter.countDown(len(lst))

        return handler

    def handler_batch_list_sleep_10(self, counter):
        def handler(lst):
            time.sleep(10)
            self.batchNum += 1
            self.num += len(lst)
            self.lst.extend(lst)
            counter.countDown(len(lst))

        return handler

    def test_subscribe_reverse_error_enableStreaming_gt_0(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`ti`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 "import pandas as pd;"
                                 "import time;"
                                 f"conn=ddb.Session('{HOST}',{PORT},'{USER}','{PASSWD}');"
                                 "conn.enableStreaming(28852);"
                                 "df=pd.DataFrame(columns=['time', 'sym', 'price']);"
                                 f"conn.run(\"\"\"{script}\"\"\");"
                                 f"conn.subscribe('{HOST}',{PORT},print,'{func_name}','action',0,False, userName='{USER}', password='{PASSWD}');"
                                 "time.sleep(3);" # APY-1920
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        ex = "The server only supports transferring subscribed data using the connection initiated by the subsriber. The specified port will not take effect."
        assert ex in result.stdout

    def test_subscribe_reverse_error_actionName_same_name(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming(0)
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        script = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                }}break
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 10), take(`000905`600001`300201`000908`600002, 10), rand(1000,10)/10.0)
        """
        conn1.run(script)
        with pytest.raises(Exception):
            conn1.subscribe(HOST, PORT, self.handler, func_name, "action", 0, False, userName=USER, password=PASSWD)
            conn1.subscribe(HOST, PORT, self.handler, func_name, "action", 0, False, userName=USER, password=PASSWD)
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_double_vector_msgAsTable_False(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
            double_vector=take([double([36,98,95,69,41,60,78,92,78,21])], n)
            exTable.tableInsert(symbol_vector, double_vector)
            {func_name}.append!(exTable)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, userName=USER,
                        password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_double_vector_msgAsTable_True(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        self.df = pd.DataFrame(columns=["symbolv", "doublev"])
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
            double_vector=take([double([36,98,95,69,41,60,78,92,78,21])], n)
            exTable.tableInsert(symbol_vector, double_vector)
            {func_name}.append!(exTable)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(1)
        conn1.subscribe(HOST, PORT, self.handler_df(counter), func_name, "action", 0, False, msgAsTable=True,
                        batchSize=1000, throttle=1.0, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_false_all_datatype(self):
        func_name = inspect.currentframe().f_code.co_name
        self.num = 0
        self.batchNum = 0
        self.lst = []
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`boolv`charv`shortv`intv`longv`datev`monthv`timev`minutev`secondv`datetimev`nanotimev`timestampv`nanotimestampv`stringv`doublev`boolav`charav`shortav`intav`longav`dateav`monthav`timeav`minuteav`secondav`datetimeav`nanotimeav`timestampav`nanotimestampav`doubleav, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, NANOTIME, TIMESTAMP, NANOTIMESTAMP, STRING, DOUBLE, BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], NANOTIME[], TIMESTAMP[], NANOTIMESTAMP[], DOUBLE[]]) as {func_name}
            setStreamTableFilterColumn({func_name}, `charv)
            boolav = array(BOOL[], 0, 10).append!(take([[true],[true, NULL, false]], 10))
            charav = array(CHAR[], 0, 10).append!(take([[1.0,2.0],[2,3, NULL]], 10))
            shortav = array(SHORT[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            intav = array(INT[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            longav = array(LONG[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            dateav = array(DATE[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            monthav = array(MONTH[], 0, 10).append!(take([[2012.12.03],[2011.01.03, 1977.01.03, NULL]], 10))
            timeav = array(TIME[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            minuteav = array(MINUTE[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            secondav = array(SECOND[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            datetimeav = array(DATETIME[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            nanotimeav = array(NANOTIME[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            timestampav = array(TIMESTAMP[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            nanotimestampav = array(NANOTIMESTAMP[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            doubleav = array(DOUBLE[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            insert into {func_name} values(
                take([true, NULL, false], 10), 
                take(char([1, NULL, 2]), 10),
                take(short([1, NULL, 2]), 10),
                take(int([1, NULL, 2]), 10),
                take(long([1, NULL, 2]), 10),
                take(date([1, NULL, 2]), 10),
                take(month([2012.12.03, NULL, 2012.12.03]), 10),
                take(time([1, NULL, 2]), 10),
                take(minute([1, NULL, 2]), 10),
                take(second([1, NULL, 2]), 10),
                take(datetime([1, NULL, 2]), 10),
                take(nanotime([1, NULL, 2]), 10),
                take(timestamp([1, NULL, 2]), 10),
                take(nanotimestamp([1, NULL, 2]), 10),
                take(string([1, 2, 2]), 10),
                take(double([1, NULL, 2]), 10),
                boolav,charav,shortav,intav,longav,dateav,monthav,timeav,minuteav,secondav,datetimeav,nanotimeav,timestampav,nanotimestampav,doubleav)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, self.handler_batch_list(counter), func_name, "action", 0, False,
                        msgAsTable=False, batchSize=5, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert self.num == 10, "1"
        df = pd.DataFrame(self.lst,
                          columns=["boolv", "charv", "shortv", "intv", "longv", "datev", "monthv", "timev", "minutev",
                                   "secondv", "datetimev", "nanotimev", "timestampv", "nanotimestampv", "stringv",
                                   "doublev", "boolav", "charav", "shortav", "intav", "longav", "dateav", "monthav",
                                   "timeav", "minuteav", "secondav", "datetimeav", "nanotimeav", "timestampav",
                                   "nanotimestampav", "doubleav"])
        columns = ["boolv", "charv", "shortv", "intv", "longv", "datev", "monthv", "timev", "minutev", "secondv",
                   "datetimev", "nanotimev", "timestampv", "nanotimestampv", "stringv", "doublev", "boolav", "charav",
                   "shortav", "intav", "longav", "dateav", "monthav", "timeav", "minuteav", "secondav", "datetimeav",
                   "nanotimeav", "timestampav", "nanotimestampav", "doubleav"]
        for col in columns:
            ans = self.conn.run(f"exec {col} from {func_name}")
            ex = np.array(df[col])
            assert len(ans) == len(ex), "1"
            for i in range(len(ans)):
                assert_array_equal(ans[i], ex[i])
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_false_all_datatype_one_row(self):
        func_name = inspect.currentframe().f_code.co_name
        self.num = 0
        self.batchNum = 0
        self.lst = []
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`boolv`charv`shortv`intv`longv`datev`monthv`timev`minutev`secondv`datetimev`nanotimev`timestampv`nanotimestampv`stringv`doublev`boolav`charav`shortav`intav`longav`dateav`monthav`timeav`minuteav`secondav`datetimeav`nanotimeav`timestampav`nanotimestampav`doubleav, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, NANOTIME, TIMESTAMP, NANOTIMESTAMP, STRING, DOUBLE, BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], NANOTIME[], TIMESTAMP[], NANOTIMESTAMP[], DOUBLE[]]) as {func_name}
            setStreamTableFilterColumn({func_name}, `charv)
            boolav = array(BOOL[], 0, 10).append!(take([[true],[true, NULL, false]], 1))
            charav = array(CHAR[], 0, 10).append!(take([[1.0,2.0],[2,3, NULL]], 1))
            shortav = array(SHORT[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            intav = array(INT[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            longav = array(LONG[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            dateav = array(DATE[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            monthav = array(MONTH[], 0, 10).append!(take([[2012.12.03],[2011.01.03, 1977.01.03, NULL]], 1))
            timeav = array(TIME[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            minuteav = array(MINUTE[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            secondav = array(SECOND[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            datetimeav = array(DATETIME[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            nanotimeav = array(NANOTIME[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            timestampav = array(TIMESTAMP[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            nanotimestampav = array(NANOTIMESTAMP[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            doubleav = array(DOUBLE[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            insert into {func_name} values(
                take([true, NULL, false], 1), 
                take(char([1, NULL, 2]), 1),
                take(short([1, NULL, 2]), 1),
                take(int([1, NULL, 2]), 1),
                take(long([1, NULL, 2]), 1),
                take(date([1, NULL, 2]), 1),
                take(month([2012.12.03, NULL, 2012.12.03]), 1),
                take(time([1, NULL, 2]), 1),
                take(minute([1, NULL, 2]), 1),
                take(second([1, NULL, 2]), 1),
                take(datetime([1, NULL, 2]), 1),
                take(nanotime([1, NULL, 2]), 1),
                take(timestamp([1, NULL, 2]), 1),
                take(nanotimestamp([1, NULL, 2]), 1),
                take(string([1, 2, 2]), 1),
                take(double([1, NULL, 2]), 1),
                boolav,charav,shortav,intav,longav,dateav,monthav,timeav,minuteav,secondav,datetimeav,nanotimeav,timestampav,nanotimestampav,doubleav)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(1)
        conn1.subscribe(HOST, PORT, self.handler_batch_list(counter), func_name, "action", 0, False,
                        msgAsTable=False, batchSize=5, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert self.num == 1
        ans = [True, 1, 1, 1, 1, np.datetime64('1970-01-02'), np.datetime64('2012-12'),
               np.datetime64('1970-01-01T00:00:00.001'), np.datetime64('1970-01-01T00:01'),
               np.datetime64('1970-01-01T00:00:01'), np.datetime64('1970-01-01T00:00:01'),
               np.datetime64('1970-01-01T00:00:00.000000001'), np.datetime64('1970-01-01T00:00:00.001'),
               np.datetime64('1970-01-01T00:00:00.000000001'), '1', 1.0, np.array([True]),
               np.array([1, 2], dtype=np.int8), np.array([1], dtype=np.int16), np.array([1], dtype=np.int32),
               np.array([1]), np.array(['1970-01-02'], dtype='datetime64[D]'),
               np.array(['2012-12'], dtype='datetime64[M]'),
               np.array(['1970-01-01T00:00:00.001'], dtype='datetime64[ms]'),
               np.array(['1970-01-01T00:01'], dtype='datetime64[m]'),
               np.array(['1970-01-01T00:00:01'], dtype='datetime64[s]'),
               np.array(['1970-01-01T00:00:01'], dtype='datetime64[s]'),
               np.array(['1970-01-01T00:00:00.000000001'], dtype='datetime64[ns]'),
               np.array(['1970-01-01T00:00:00.001'], dtype='datetime64[ms]'),
               np.array(['1970-01-01T00:00:00.000000001'], dtype='datetime64[ns]'), np.array([1.])]
        columns = ["boolv", "charv", "shortv", "intv", "longv", "datev", "monthv", "timev", "minutev", "secondv",
                   "datetimev", "nanotimev", "timestampv", "nanotimestampv", "stringv", "doublev", "boolav", "charav",
                   "shortav", "intav", "longav", "dateav", "monthav", "timeav", "minuteav", "secondav", "datetimeav",
                   "nanotimeav", "timestampav", "nanotimestampav", "doubleav"]
        for i in range(len(ans)):
            if i in range(16):
                assert ans[i] == self.lst[0][i]
            else:
                assert (ans[i] == self.lst[0][i]).all()
        conn1.unsubscribe(HOST, PORT, func_name, "action")

    def test_subscribe_reverse_msgAsTable_false_batchSize_positive_number(self):
        func_name = inspect.currentframe().f_code.co_name
        self.num = 0
        self.batchNum = 0
        self.lst = []
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, self.handler_batch_list(counter), func_name, "action", 0, False,
                        msgAsTable=False, batchSize=5, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert self.num == 10, "1"
        df = pd.DataFrame(self.lst, columns=["time", "sym", "price"])
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_false_rows_gt_65536_positive_number(self):
        func_name = inspect.currentframe().f_code.co_name
        self.num = 0
        self.batchNum = 0
        self.lst = []
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 70005), take(`000905`600001`300201`000908`600002, 70005), rand(1000,70005)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(70005)
        conn1.subscribe(HOST, PORT, self.handler_batch_list(counter), func_name, "action", 0, False,
                        msgAsTable=False, batchSize=10000, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert self.num == 70005
        df = pd.DataFrame(self.lst, columns=["time", "sym", "price"])
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_true_all_datatype(self):
        func_name = inspect.currentframe().f_code.co_name
        self.num = 0
        self.batchNum = 0
        self.df = None
        conn1 = ddb.session(enablePickle=False)
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`boolv`charv`shortv`intv`longv`datev`monthv`timev`minutev`secondv`datetimev`nanotimev`timestampv`nanotimestampv`stringv`doublev`boolav`charav`shortav`intav`longav`dateav`monthav`timeav`minuteav`secondav`datetimeav`nanotimeav`timestampav`nanotimestampav`doubleav, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, NANOTIME, TIMESTAMP, NANOTIMESTAMP, STRING, DOUBLE, BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], NANOTIME[], TIMESTAMP[], NANOTIMESTAMP[], DOUBLE[]]) as {func_name}
            setStreamTableFilterColumn({func_name}, `charv)
            boolav = array(BOOL[], 0, 10).append!(take([[true],[true, NULL, false]], 10))
            charav = array(CHAR[], 0, 10).append!(take([[1.0,2.0],[2,3, NULL]], 10))
            shortav = array(SHORT[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            intav = array(INT[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            longav = array(LONG[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            dateav = array(DATE[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            monthav = array(MONTH[], 0, 10).append!(take([[2012.12.03],[2011.01.03, 1977.01.03, NULL]], 10))
            timeav = array(TIME[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            minuteav = array(MINUTE[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            secondav = array(SECOND[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            datetimeav = array(DATETIME[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            nanotimeav = array(NANOTIME[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            timestampav = array(TIMESTAMP[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            nanotimestampav = array(NANOTIMESTAMP[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            doubleav = array(DOUBLE[], 0, 10).append!(take([[1],[2,3, NULL]], 10))
            insert into {func_name} values(
                take([true, NULL, false], 10), 
                take(char([1, NULL, 2]), 10),
                take(short([1, NULL, 2]), 10),
                take(int([1, NULL, 2]), 10),
                take(long([1, NULL, 2]), 10),
                take(date([1, NULL, 2]), 10),
                take(month([2012.12.03, NULL, 2012.12.03]), 10),
                take(time([1, NULL, 2]), 10),
                take(minute([1, NULL, 2]), 10),
                take(second([1, NULL, 2]), 10),
                take(datetime([1, NULL, 2]), 10),
                take(nanotime([1, NULL, 2]), 10),
                take(timestamp([1, NULL, 2]), 10),
                take(nanotimestamp([1, NULL, 2]), 10),
                take(string([1, 2, 2]), 10),
                take(double([1, NULL, 2]), 10),
                boolav,charav,shortav,intav,longav,dateav,monthav,timeav,minuteav,secondav,datetimeav,nanotimeav,timestampav,nanotimestampav,doubleav)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(10)
        conn1.subscribe(HOST, PORT, self.handler_batch_df(counter), func_name, "action", 0, False, msgAsTable=True,
                        batchSize=5, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert self.num == 10, "1"
        ex = conn1.run(f"select * from {func_name}")
        ex["dateav"] = ex["dateav"].apply(lambda x: x.astype("datetime64[D]"))
        ex["timeav"] = ex["timeav"].apply(lambda x: x.astype("datetime64[ms]"))
        ex["minuteav"] = ex["minuteav"].apply(lambda x: x.astype("datetime64[m]"))
        ex["secondav"] = ex["secondav"].apply(lambda x: x.astype("datetime64[s]"))
        ex["datetimeav"] = ex["datetimeav"].apply(lambda x: x.astype("datetime64[s]"))
        ex["timestampav"] = ex["timestampav"].apply(lambda x: x.astype("datetime64[ms]"))
        assert_frame_equal(self.df, ex)
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_true_all_datatype_one_row(self):
        func_name = inspect.currentframe().f_code.co_name
        self.num = 0
        self.batchNum = 0
        self.df = None
        conn1 = ddb.session(enablePickle=False)
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`boolv`charv`shortv`intv`longv`datev`monthv`timev`minutev`secondv`datetimev`nanotimev`timestampv`nanotimestampv`stringv`doublev`boolav`charav`shortav`intav`longav`dateav`monthav`timeav`minuteav`secondav`datetimeav`nanotimeav`timestampav`nanotimestampav`doubleav, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, NANOTIME, TIMESTAMP, NANOTIMESTAMP, STRING, DOUBLE, BOOL[], CHAR[], SHORT[], INT[], LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], NANOTIME[], TIMESTAMP[], NANOTIMESTAMP[], DOUBLE[]]) as {func_name}
            setStreamTableFilterColumn({func_name}, `charv)
            boolav = array(BOOL[], 0, 10).append!(take([[true],[true, NULL, false]], 1))
            charav = array(CHAR[], 0, 10).append!(take([[1.0,2.0],[2,3, NULL]], 1))
            shortav = array(SHORT[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            intav = array(INT[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            longav = array(LONG[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            dateav = array(DATE[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            monthav = array(MONTH[], 0, 10).append!(take([[2012.12.03],[2011.01.03, 1977.01.03, NULL]], 1))
            timeav = array(TIME[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            minuteav = array(MINUTE[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            secondav = array(SECOND[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            datetimeav = array(DATETIME[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            nanotimeav = array(NANOTIME[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            timestampav = array(TIMESTAMP[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            nanotimestampav = array(NANOTIMESTAMP[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            doubleav = array(DOUBLE[], 0, 10).append!(take([[1],[2,3, NULL]], 1))
            insert into {func_name} values(
                take([true, NULL, false], 1), 
                take(char([1, NULL, 2]), 1),
                take(short([1, NULL, 2]), 1),
                take(int([1, NULL, 2]), 1),
                take(long([1, NULL, 2]), 1),
                take(date([1, NULL, 2]), 1),
                take(month([2012.12.03, NULL, 2012.12.03]), 1),
                take(time([1, NULL, 2]), 1),
                take(minute([1, NULL, 2]), 1),
                take(second([1, NULL, 2]), 1),
                take(datetime([1, NULL, 2]), 1),
                take(nanotime([1, NULL, 2]), 1),
                take(timestamp([1, NULL, 2]), 1),
                take(nanotimestamp([1, NULL, 2]), 1),
                take(string([1, 2, 2]), 1),
                take(double([1, NULL, 2]), 1),
                boolav,charav,shortav,intav,longav,dateav,monthav,timeav,minuteav,secondav,datetimeav,nanotimeav,timestampav,nanotimestampav,doubleav)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(1)
        conn1.subscribe(HOST, PORT, self.handler_batch_df(counter), func_name, "action", 0, False, msgAsTable=True,
                        batchSize=5, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert self.num == 1, "1"
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")

    def test_subscribe_reverse_msgAsTable_true_batchSize_lt_1024_positive_number(self):
        func_name = inspect.currentframe().f_code.co_name
        self.num = 0
        self.batchNum = 0
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, self.handler_batch_df(counter), func_name, "action", 0, False, msgAsTable=True,
                        batchSize=5, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert self.num == 10
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"), False)
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_true_rows_gt_65536_positive_number(self):
        func_name = inspect.currentframe().f_code.co_name
        self.num = 0
        self.batchNum = 0
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 70005), take(`000905`600001`300201`000908`600002, 70005), rand(1000,70005)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(70005)
        conn1.subscribe(HOST, PORT, self.handler_batch_df(counter), func_name, "action", 0, False, msgAsTable=True,
                        batchSize=10000, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert self.num == 70005, "1"
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"), False)
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_true_cost_gt_create(self):
        func_name = inspect.currentframe().f_code.co_name
        self.batchNum = 0
        self.num = 0
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 20), take(`000905`600001`300201`000908`600002, 20), rand(1000,20)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(2020)
        conn1.subscribe(HOST, PORT, self.handler_batch_df(counter), func_name, "action", 0, False, msgAsTable=True,
                        batchSize=1200, throttle=5, userName=USER, password=PASSWD)
        time.sleep(5.5)
        script = f"""
            for(i in 1..100){{
                insert into {func_name} values(take(now(), 20), take(`000905`600001`300201`000908`600002, 20), rand(1000,20)/10.0)
            }}
        """
        conn1.run(script)
        assert counter.wait_s(20)
        assert self.num == 2020
        assert self.batchNum == 3
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"), False)
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_true_cost_lt_create(self):
        func_name = inspect.currentframe().f_code.co_name
        self.batchNum = 0
        self.num = 0
        self.df = pd.DataFrame(columns=["time", "sym", "price"])
        conn1 = ddb.session(enablePickle=False)
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 20), take(`000905`600001`300201`000908`600002, 20), rand(1000,20)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(2020)
        conn1.subscribe(HOST, PORT, self.handler_batch_df_sleep_10(counter), func_name, "action", 0, False,
                        msgAsTable=True, batchSize=1200, throttle=2, userName=USER, password=PASSWD)
        time.sleep(1)
        script = f"""
            for(i in 1..100){{
                insert into {func_name} values(take(now(), 20), take(`000905`600001`300201`000908`600002, 20), rand(1000,20)/10.0)
            }}
        """
        conn1.run(script)
        assert counter.wait_s(30)
        assert self.num == 2020
        assert self.batchNum == 2
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"), False)
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_false_cost_gt_create(self):
        func_name = inspect.currentframe().f_code.co_name
        self.batchNum = 0
        self.num = 0
        self.lst = []
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 20), take(`000905`600001`300201`000908`600002, 20), rand(1000,20)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(2020)
        conn1.subscribe(HOST, PORT, self.handler_batch_list(counter), func_name, "action", 0, False,
                        msgAsTable=False, batchSize=1200, throttle=5.5, userName=USER, password=PASSWD)
        time.sleep(10)
        script = f"""
            for(i in 1..100){{
                insert into {func_name} values(take(now(), 20), take(`000905`600001`300201`000908`600002, 20), rand(1000,20)/10.0)
            }}
        """
        conn1.run(script)
        assert counter.wait_s(20)
        assert self.num == 2020, "1"
        df = pd.DataFrame(self.lst, columns=["time", "sym", "price"])
        assert self.batchNum == 3, "2"
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")

    def test_subscribe_reverse_msgAsTable_false_cost_lt_create(self):
        func_name = inspect.currentframe().f_code.co_name
        self.batchNum = 0
        self.num = 0
        self.lst = []
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            subscribers = select * from getStreamingStat().pubTables where tableName=`{func_name};
            for(subscriber in subscribers){{
                ip_port = subscriber.subscriber.split(":");
                stopPublishTable(ip_port[0],int(ip_port[1]),subscriber.tableName,subscriber.actions);
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(10000:0,`time`sym`price, [TIMESTAMP,SYMBOL,DOUBLE]) as {func_name}
            setStreamTableFilterColumn({func_name}, `sym)
            insert into {func_name} values(take(now(), 20), take(`000905`600001`300201`000908`600002, 20), rand(1000,20)/10.0)
        """
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(2020)
        conn1.subscribe(HOST, PORT, self.handler_batch_list_sleep_10(counter), func_name, "action", 0, False,
                        msgAsTable=False, batchSize=1200, throttle=5.5, userName=USER, password=PASSWD)
        script = f"""
            for(i in 1..100){{
                insert into {func_name} values(take(now(), 20), take(`000905`600001`300201`000908`600002, 20), rand(1000,20)/10.0)
            }}
        """
        conn1.run(script)
        assert counter.wait_s(30)
        assert self.num == 2020
        df = pd.DataFrame(self.lst, columns=["time", "sym", "price"])
        assert self.batchNum == 2
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_offset_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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

    def test_subscribe_reverse_offset_lt_zero_gt(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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

    def test_subscribe_reverse_resub_True(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        time.sleep(1)
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

    def test_subscribe_reverse_resub_False(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        time.sleep(1)
        script = f"""
            insert_table = table(take(now(), 5) as time, take(`000905`600001`300201`000908`600002, 5) as sym, rand(1000,5)/10.0 as price)
            {func_name}.append!(insert_table)
        """
        conn1.run(script)
        time.sleep(3)
        assert len(df) == 10
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_filter(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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

    def test_subscribe_reverse_double_array_vector_msgAsTable_False(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
            double_vector=take([double([36,98,95,69,41,60,78,92,78,21])], n)
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

    def test_subscribe_reverse_double_array_vector_msgAsTable_True(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        self.df = pd.DataFrame(columns=["symbolv", "doublev"])
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
            double_vector=take([double([36,98,95,69,41,60,78,92,78,21])], n)
            exTable.tableInsert(symbol_vector, double_vector)
            {func_name}.append!(exTable)
        '''
        conn1.run(script)
        counter = CountBatchDownLatch(1)
        counter.reset(1)
        conn1.subscribe(HOST, PORT, self.handler_df(counter), func_name, "action", 0, False, msgAsTable=True,
                        batchSize=1000, throttle=1, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(self.df, conn1.run(func_name))
        conn1.unsubscribe(HOST, PORT, func_name, "action")

    def test_subscribe_reverse_batchSize_lt_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        counter.reset(10)
        conn1.subscribe(HOST, PORT, gethandler(df, counter), func_name, "action", 0, False, msgAsTable=False,
                        batchSize=-1, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_False_batchSize_gt_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, gethandler_multi_row(df, counter), func_name, "action", 0, False,
                        msgAsTable=False, batchSize=2, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_msgAsTable_True_batchSize_gt_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, self.handler_df(counter), func_name, "action", 0, False, msgAsTable=True,
                        batchSize=5, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_throttle_gt_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, self.handler_df(counter), func_name, "action", 0, False, msgAsTable=True,
                        batchSize=1000, throttle=10.1, userName=USER, password=PASSWD)
        assert not counter.wait_s(10)
        assert len(self.df) == 0
        assert counter.wait_s(20)
        assert len(self.df) == 10
        assert_frame_equal(self.df, conn1.run(f"select * from {func_name}"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_streamDeserializer_memory_table_session_None(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), func_name, "action", 0, False,
                        msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run(f"select * from {func_name}_1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run(f"select * from {func_name}_2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_streamDeserializer_PartitionedTable_table_session_None(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), func_name, "action", 0, False,
                        msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from pt1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from pt2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_streamDeserializer_memory_table(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), func_name, "action", 0, False,
                        msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from table1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from table2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_streamDeserializer_memory_table_10000_rows(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), func_name, "action", 0, False,
                        msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        assert counter.wait_s(200)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from table1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from table2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_streamDeserializer_PartitionedTable_table(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), func_name, "action", 0, False,
                        msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        assert counter.wait_s(20)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from pt1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from pt2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_streamDeserializer_PartitionedTable_table_10000_rows(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
                dropDB(dbName)
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
        conn1.subscribe(HOST, PORT, streamDSgethandler(df1, df2, counter), func_name, "action", 0, False,
                        msgAsTable=False, streamDeserializer=sd, userName=USER, password=PASSWD)
        assert counter.wait_s(200)
        assert_frame_equal(df1.loc[:, :"price2"], conn1.run("select * from pt1"))
        assert_frame_equal(df2.loc[:, :"price1"], conn1.run("select * from pt2"))
        conn1.unsubscribe(HOST, PORT, func_name, "action")
        conn1.close()

    def test_subscribe_reverse_getSubscriptionTopics(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
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
        assert len(ans) == 0, "5"

    def test_subscribe_reverse_many_tables(self):
        func_name = inspect.currentframe().f_code.co_name
        self.conn.run(f"""
            share streamTable(1000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}
            insert into {func_name} values(take(now(), 1000), take(`000905`600001`300201`000908`600002, 1000), rand(1000,1000)/10.0, 1..1000)
            share streamTable(1000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}2
            insert into {func_name}2 values(take(now(), 1000), take(`000905`600001`300201`000908`600002, 1000), rand(1000,1000)/10.0, 1..1000)
            share streamTable(1000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}3
            insert into {func_name}3 values(take(now(), 1000), take(`000905`600001`300201`000908`600002, 1000), rand(1000,1000)/10.0, 1..1000)
            share streamTable(1000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}4
            insert into {func_name}4 values(take(now(), 1000), take(`000905`600001`300201`000908`600002, 1000), rand(1000,1000)/10.0, 1..1000)
            share streamTable(1000:0,`time`sym`price`id, [TIMESTAMP,SYMBOL,DOUBLE,INT]) as {func_name}5
            insert into {func_name}5 values(take(now(), 1000), take(`000905`600001`300201`000908`600002, 1000), rand(1000,1000)/10.0, 1..1000)
        """)
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming(0)
        counter = CountBatchDownLatch(1000)
        counter2 = CountBatchDownLatch(1000)
        counter3 = CountBatchDownLatch(1000)
        counter4 = CountBatchDownLatch(1000)
        counter5 = CountBatchDownLatch(1000)
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
            assert co.wait_s(5)
            ex_df = self.conn.run(f"select * from {tables[ind]}")
            res1 = list(chain.from_iterable(results[ind]))
            for i in range(len(res1)):
                assert res1[i] == list(ex_df.iloc[i])
        for tab in tables:
            conn1.unsubscribe(HOST, PORT, tab, "action")
        conn1.close()

    def test_subscribe_reverse_exception_in_handler(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            colName=["time","x"]
            colType=["timestamp","int"]
            t = streamTable(100:0, colName, colType);
            share t as {func_name};go
            insert into {func_name} values(now(), rand(100.00,1))
        """
        result = subprocess.run([sys.executable, '-c',
                                 "import dolphindb as ddb;"
                                 f"conn=ddb.Session('{HOST}', {PORT}, '{USER}', '{PASSWD}');"
                                 "conn.enableStreaming();"
                                 f"conn.run(\"\"\"{script}\"\"\");"
                                 f"conn.subscribe('{HOST}',{PORT},lambda :raise RuntimeError('this should be catched'),'st','test',0,True,userName='{USER}',password='{PASSWD}');"
                                 f"conn.run('insert into {func_name} values(now(), rand(100.00,1))');"
                                 "sleep(3);"
                                 f"conn.unsubscribe('{HOST}',{PORT},'{func_name}','test');"
                                 "conn.close();"
                                 ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
        assert "this should be catched" in result.stderr

    def test_subscribe_reverse_subscribe_keyededStreamTable(self):
        func_name = inspect.currentframe().f_code.co_name
        conn1 = ddb.session()
        conn1.connect(HOST, PORT, USER, PASSWD)
        conn1.enableStreaming()
        script = f"""
            colName=["time","sym", "p1", "p2", "ind"]
            colType=["timestamp","symbol","double","double","int"]
            t = keyedStreamTable(`time, 100:0, colName, colType);
            share t as {func_name};go
            for(i in 0:100)
                insert into {func_name} values(now(), rand(`APPL`GOOG`TESLA`TX`BABA`AMAZON,1), rand(200.00, 1), rand(200.00, 1), rand(1000, 1))
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
