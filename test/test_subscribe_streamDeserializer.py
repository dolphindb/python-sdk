#!/usr/bin/python3
# -*- coding:utf-8 -*-
""""
@Author: shijianbo
@Time: 2024/1/10 10:51
@Note: 
"""
import inspect
from time import sleep

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest

from basic_testing.prepare import PANDAS_VERSION
from basic_testing.utils import equalPlus
from setup.settings import HOST, PORT, USER, PASSWD


def handlerDataframe(df1: pd.DataFrame, df2: pd.DataFrame):
    def handler(msg):
        if msg[-1] == "msg1":
            df1.loc[len(df1)] = pd.Series(msg[:-1], dtype='object', index=df1.columns)
        else:
            df2.loc[len(df2)] = pd.Series(msg[:-1], dtype='object', index=df2.columns)

    return handler


class TestSubscribeStreamDeserializer(object):
    conn = ddb.Session(HOST, PORT, USER, PASSWD, enablePickle=False)

    @classmethod
    def setup_class(cls):
        cls.conn.enableStreaming()

    def test_streamDeserializer_error(self):
        with pytest.raises(RuntimeError, match='<Exception> in StreamDeserializer: tuple size must be 2'):
            ddb.streamDeserializer({
                "msg1": ("", "pub_t1", ""),
                "msg2": ("", "pub_t2"),
            }, session=self.conn)
        with pytest.raises(RuntimeError,
                           match='<Exception> in StreamDeserializer: unsupported type in dict, support string, list and tuple.'):
            ddb.streamDeserializer({
                "msg1": {"", "pub_t1"},
                "msg2": {"", "pub_t2"},
            }, session=self.conn)

    @pytest.mark.parametrize('dataType',
                             ["BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH", "TIME", "MINUTE", "SECOND",
                              "DATETIME", "TIMESTAMP", "NANOTIME", "NANOTIMESTAMP", "FLOAT", "DOUBLE", "STRING",
                              "UUID", "DATEHOUR", "IPADDR", "INT128", "BLOB", "DECIMAL32", "DECIMAL64", "DECIMAL128"])
    def test_streamDeserializer_single(self, dataType):
        if dataType in ("STRING", "UUID", "IPADDR", "INT128", "BLOB") and PANDAS_VERSION < (1, 4, 0):
            pytest.skip("pandas bug")
        func_name = inspect.currentframe().f_code.co_name + f'_{dataType}'
        dtype = {
            'BOOL': 'object',
            'CHAR': np.float64,
            'SHORT': np.float64,
            'INT': np.float64,
            'LONG': np.float64,
            'DATE': 'datetime64[ns]',
            'MONTH': 'datetime64[ns]',
            'TIME': 'datetime64[ns]',
            'MINUTE': 'datetime64[ns]',
            'SECOND': 'datetime64[ns]',
            'DATETIME': 'datetime64[ns]',
            'TIMESTAMP': 'datetime64[ns]',
            'NANOTIME': 'datetime64[ns]',
            'NANOTIMESTAMP': 'datetime64[ns]',
            'FLOAT': np.float32,
            'DOUBLE': np.float64,
            'STRING': 'object',
            'UUID': 'object',
            'DATEHOUR': 'datetime64[ns]',
            'IPADDR': 'object',
            'INT128': 'object',
            'BLOB': 'object',
            'DECIMAL32': 'object',
            'DECIMAL64': 'object',
            'DECIMAL128': 'object',
        }
        script = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(1000:0,`time`sym`blob`dataType,[TIMESTAMP,SYMBOL,BLOB,{dataType}{"(2)" if "DECIMAL" in dataType else ""}]) as `{func_name};
            timestampv=2024.01.10T12:00:00.000+1..100;
            dataType_BOOL=take(true false 00b,100);
            dataType_CHAR=take(1c 0c 00c,100);
            dataType_SHORT=take(1h 0h 00h,100);
            dataType_INT=take(1i 0i 00i,100);
            dataType_LONG=take(1l 0l 00l,100);
            dataType_DATE=take(2024.01.10d 1970.01.01d 00d,100);
            dataType_MONTH=take(2024.01M 1970.01M 00M,100);
            dataType_TIME=take(12:00:00.000t 00:00:00.000t 00t,100);
            dataType_MINUTE=take(12:00m 00:00m 00m,100);
            dataType_SECOND=take(12:00:00s 00:00:00s 00s,100);
            dataType_DATETIME=take(2024.01.01T12:00:00D 1970.01.01T00:00:00D 00D,100);
            dataType_TIMESTAMP=take(2024.01.01T12:00:00.000T 1970.01.01T00:00:00.000T 00T,100);
            dataType_NANOTIME=take(12:00:00.000000000n 00:00:00.000000000n 00n,100);
            dataType_NANOTIMESTAMP=take(2024.01.01T12:00:00.000000000N 1970.01.01T00:00:00.000000000N 00N,100);
            dataType_FLOAT=take([0.0f,1.0f,00f,float('nan'),float('inf')],100);
            dataType_DOUBLE=take([0.0F,1.0F,00F,double('nan'),double('inf')],100);
            dataType_STRING=take("123" "",100);
            dataType_UUID=take(uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87" "00000000-0000-0000-0000-000000000000"),100);
            dataType_DATEHOUR=take(datehour("2024.01.01T12" "1970.01.01T00" null),100);
            dataType_IPADDR=take(ipaddr("127.0.0.1" "0.0.0.0"),100);
            dataType_INT128=take(int128("5d212a78cc48e3b14235b4d91473ee87" "00000000000000000000000000000000"),100);
            dataType_BLOB=take(blob("123" ""),100);
            dataType_DECIMAL32=take(decimal32("3.14" "0" "nan",2),100);
            dataType_DECIMAL64=take(decimal64("3.14" "0" "nan",2),100);
            dataType_DECIMAL128=take(decimal128("3.14" "0" "nan",2),100);
            share table(timestampv as `timestampv,dataType_{dataType} as dataType) as `{func_name}_1;
            share table(timestampv as `timestampv,dataType_{dataType} as dataType) as `{func_name}_2;
            d=dict(`msg1`msg2,[{func_name}_1,{func_name}_2]);
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv);
        """
        self.conn.run(script)
        sd = ddb.streamDeserializer({
            "msg1": ("", f"{func_name}_1"),
            "msg2": ("", f"{func_name}_2"),
        })
        df1 = pd.DataFrame(columns=['timestampv', 'dataType'], dtype='object')
        df2 = pd.DataFrame(columns=['timestampv', 'dataType'], dtype='object')
        self.conn.subscribe(host=HOST, port=PORT, handler=handlerDataframe(df1, df2), tableName=func_name,
                            actionName="action", offset=0, streamDeserializer=sd,
                            userName=USER, password=PASSWD)
        sleep(1)
        self.conn.unsubscribe(HOST, PORT, func_name, "action")
        df_expect = self.conn.run(f'{func_name}_1')
        df1['timestampv'] = df1['timestampv'].astype('datetime64[ns]')
        df2['timestampv'] = df2['timestampv'].astype('datetime64[ns]')
        df1['dataType'] = df1['dataType'].astype(dtype[dataType])
        df2['dataType'] = df2['dataType'].astype(dtype[dataType])
        if dataType == 'STRING':
            df_expect = df_expect.replace("", None)
        elif dataType == 'UUID':
            df_expect = df_expect.replace("00000000-0000-0000-0000-000000000000", None)
        elif dataType == 'IPADDR':
            df_expect = df_expect.replace("0.0.0.0", None)
        elif dataType == 'INT128':
            df_expect = df_expect.replace("00000000000000000000000000000000", None)
        elif dataType == "BLOB":
            df_expect = df_expect.replace(b"", None)
        assert equalPlus(df1, df_expect)
        assert equalPlus(df2, df_expect)

    @pytest.mark.parametrize('dataType',
                             ["BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH", "TIME", "MINUTE", "SECOND",
                              "DATETIME", "TIMESTAMP", "NANOTIME", "NANOTIMESTAMP", "FLOAT", "DOUBLE",
                              "UUID", "DATEHOUR", "IPADDR", "INT128", "DECIMAL32", "DECIMAL64", "DECIMAL128"])
    def test_streamDeserializer_array_vector(self, dataType):
        func_name = inspect.currentframe().f_code.co_name + f'_{dataType}'
        script = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamTable(`{func_name})}}catch(ex){{}}
            share streamTable(1000:0,`time`sym`blob`dataType,[TIMESTAMP,SYMBOL,BLOB,{dataType}{"(2)" if "DECIMAL" in dataType else ""}[]]) as `{func_name};
            timestampv=2024.01.10T12:00:00.000+1..34;
            dataType_BOOL=array(BOOL[]).append!(cut(take(true false 00b,100),3));
            dataType_CHAR=array(CHAR[]).append!(cut(take(1c 0c 00c,100),3));
            dataType_SHORT=array(SHORT[]).append!(cut(take(1h 0h 00h,100),3));
            dataType_INT=array(INT[]).append!(cut(take(1i 0i 00i,100),3));
            dataType_LONG=array(LONG[]).append!(cut(take(1l 0l 00l,100),3));
            dataType_DATE=array(DATE[]).append!(cut(take(2024.01.10d 1970.01.01d 00d,100),3));
            dataType_MONTH=array(MONTH[]).append!(cut(take(2024.01M 1970.01M 00M,100),3));
            dataType_TIME=array(TIME[]).append!(cut(take(12:00:00.000t 00:00:00.000t 00t,100),3));
            dataType_MINUTE=array(MINUTE[]).append!(cut(take(12:00m 00:00m 00m,100),3));
            dataType_SECOND=array(SECOND[]).append!(cut(take(12:00:00s 00:00:00s 00s,100),3));
            dataType_DATETIME=array(DATETIME[]).append!(cut(take(2024.01.01T12:00:00D 1970.01.01T00:00:00D 00D,100),3));
            dataType_TIMESTAMP=array(TIMESTAMP[]).append!(cut(take(2024.01.01T12:00:00.000T 1970.01.01T00:00:00.000T 00T,100),3));
            dataType_NANOTIME=array(NANOTIME[]).append!(cut(take(12:00:00.000000000n 00:00:00.000000000n 00n,100),3));
            dataType_NANOTIMESTAMP=array(NANOTIMESTAMP[]).append!(cut(take(2024.01.01T12:00:00.000000000N 1970.01.01T00:00:00.000000000N 00N,100),3));
            dataType_FLOAT=array(FLOAT[]).append!(cut(take([0.0f,1.0f,00f,float('nan'),float('inf')],100),3));
            dataType_DOUBLE=array(DOUBLE[]).append!(cut(take([0.0F,1.0F,00F,double('nan'),double('inf')],100),3));
            dataType_UUID=array(UUID[]).append!(cut(take(uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87" "00000000-0000-0000-0000-000000000000"),100),3));
            dataType_DATEHOUR=array(DATEHOUR[]).append!(cut(take(datehour("2024.01.01T12" "1970.01.01T00" null),100),3));
            dataType_IPADDR=array(IPADDR[]).append!(cut(take(ipaddr("127.0.0.1" "0.0.0.0"),100),3));
            dataType_INT128=array(INT128[]).append!(cut(take(int128("5d212a78cc48e3b14235b4d91473ee87" "00000000000000000000000000000000"),100),3));
            dataType_DECIMAL32=array(DECIMAL32(2)[]).append!(cut(take(decimal32("3.14" "0" "nan",2),100),3));
            dataType_DECIMAL64=array(DECIMAL64(2)[]).append!(cut(take(decimal64("3.14" "0" "nan",2),100),3));
            dataType_DECIMAL128=array(DECIMAL128(2)[]).append!(cut(take(decimal128("3.14" "0" "nan",2),100),3));
            share table(timestampv as `timestampv,dataType_{dataType} as dataType) as `{func_name}_1;
            share table(timestampv as `timestampv,dataType_{dataType} as dataType) as `{func_name}_2;
            d=dict(`msg1`msg2,[{func_name}_1,{func_name}_2]);
            replay(inputTables=d, outputTables=`{func_name}, dateColumn=`timestampv, timeColumn=`timestampv);
        """
        self.conn.run(script)
        sd = ddb.streamDeserializer({
            "msg1": ["", f"{func_name}_1"],
            "msg2": ["", f"{func_name}_2"],
        }, session=self.conn)
        df1 = pd.DataFrame(columns=['timestampv', 'dataType'], dtype='object')
        df2 = pd.DataFrame(columns=['timestampv', 'dataType'], dtype='object')
        self.conn.subscribe(host=HOST, port=PORT, handler=handlerDataframe(df1, df2), tableName=func_name,
                            actionName="action", offset=0, streamDeserializer=sd,
                            userName=USER, password=PASSWD)
        sleep(1)
        self.conn.unsubscribe(HOST, PORT, func_name, "action")
        df_expect = self.conn.run(f"{func_name}_1")
        df1['timestampv'] = df1['timestampv'].astype('datetime64[ns]')
        df2['timestampv'] = df2['timestampv'].astype('datetime64[ns]')
        assert equalPlus(df1["timestampv"], df_expect["timestampv"])
        assert equalPlus(df2["timestampv"], df_expect["timestampv"])
        # arrayvector bug:APY-891
        for index, i in enumerate(df1["dataType"]):
            assert equalPlus(df1["dataType"][index][0], df_expect["dataType"][index])
            assert equalPlus(df2["dataType"][index][0], df_expect["dataType"][index])
