#!/usr/bin/python3
# -*- coding:utf-8 -*-
""""
@Author: shijianbo
@Time: 2024/3/22 10:38
@Note: 
"""
import inspect
import math
from decimal import Decimal
from importlib.util import find_spec
from time import sleep

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import pytest
from dolphindb.cep import Event, EventSender, EventClient
from dolphindb.typing import Scalar, Vector
from pandas._testing import assert_almost_equal

from basic_testing.prepare import PANDAS_VERSION
from basic_testing.utils import equalPlus
from setup.settings import HOST, PORT, USER, PASSWD


class TestCEP(object):
    conn: ddb.Session = ddb.Session(HOST, PORT, USER, PASSWD, enablePickle=False)

    def test_CEP_scalar_bool(self):
        class EventScalarBool(Event):
            _event_name = "EventScalarBool"
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarBool):
                    if self.s_bool == other.s_bool and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
        
            share table(100:0,`s_bool`eventTime,[BOOL,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarBool{{
                s_bool::BOOL
                eventTime::TIMESTAMP
                def EventScalarBool(s_bool_){{
                    s_bool=s_bool_
                    eventTime=now()
                }}
            }}
            class EventScalarBoolMonitor{{
                def EventScalarBoolMonitor(){{}}
                def updateEventScalarBool(event){{
                    insert into {func_name}_eventTest values(event.s_bool,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarBool,'EventScalarBool',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(BOOL, 0) as s_bool)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(BOOL, 0) as s_bool) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(BOOL, 0) as s_bool) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_bool,eventTime"
            fieldType="BOOL,TIMESTAMP"
            fieldTypeId=[[BOOL,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarBool", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_bool")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarBoolMonitor()>, dummy, [EventScalarBool], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarBool],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_bool"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarBool], eventTimeFields=["eventTime"], commonFields=["s_bool"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((True, False, np.bool_(True), np.bool_(False), None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarBool(
                s_bool=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_bool'],[true,false,true,false,00b])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_bool': np.array([True, False, True, False, None], dtype='object'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(5)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarBool(s_bool=True, eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarBool(s_bool=False, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarBool(s_bool=True, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarBool(s_bool=False, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventScalarBool(s_bool=None, eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_char(self):
        class EventScalarChar(Event):
            _event_name = "EventScalarChar"
            s_char: Scalar[keys.DT_CHAR]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarChar):
                    if self.s_char == other.s_char and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
        
            share table(100:0,`s_char`eventTime,[CHAR,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarChar{{
                s_char::CHAR
                eventTime::TIMESTAMP
                def EventScalarChar(s_char_){{
                    s_char=s_char_
                    eventTime=now()
                }}
            }}
            class EventScalarCharMonitor{{
                def EventScalarCharMonitor(){{}}
                def updateEventScalarChar(event){{
                    insert into {func_name}_eventTest values(event.s_char,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarChar,'EventScalarChar',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(CHAR, 0) as s_char)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(CHAR, 0) as s_char) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(CHAR, 0) as s_char) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_char,eventTime"
            fieldType="CHAR,TIMESTAMP"
            fieldTypeId=[[CHAR,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarChar", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_char")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarCharMonitor()>, dummy, [EventScalarChar], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarChar],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_char"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarChar], eventTimeFields=["eventTime"], commonFields=["s_char"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate(
                (0, np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7), None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarChar(
                s_char=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_char'],[0c,0c,127c,-127c,00c,00c])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_char': np.array([0, 0, 127, -127, None, None], dtype='float64'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(6)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarChar(s_char=0, eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarChar(s_char=0, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarChar(s_char=127, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarChar(s_char=-127, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventScalarChar(s_char=None, eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventScalarChar(s_char=None, eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_short(self):
        class EventScalarShort(Event):
            _event_name = "EventScalarShort"
            s_short: Scalar[keys.DT_SHORT]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarShort):
                    if self.s_short == other.s_short and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
        
            share table(100:0,`s_short`eventTime,[SHORT,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarShort{{
                s_short::SHORT
                eventTime::TIMESTAMP
                def EventScalarShort(s_short_){{
                    s_short=s_short_
                    eventTime=now()
                }}
            }}
            class EventScalarShortMonitor{{
                def EventScalarShortMonitor(){{}}
                def updateEventScalarShort(event){{
                    insert into {func_name}_eventTest values(event.s_short,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarShort,'EventScalarShort',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SHORT, 0) as s_short)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SHORT, 0) as s_short) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SHORT, 0) as s_short) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_short,eventTime"
            fieldType="SHORT,TIMESTAMP"
            fieldTypeId=[[SHORT,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarShort", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_short")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarShortMonitor()>, dummy, [EventScalarShort], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarShort],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_short"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarShort], eventTimeFields=["eventTime"], commonFields=["s_short"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate(
                (0, np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15), None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarShort(
                s_short=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_short'],[0h,0h,32767h,-32767h,00h,00h])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_short': np.array([0, 0, 2 ** 15 - 1, -2 ** 15 + 1, None, None], dtype='float64'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(6)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarShort(s_short=0, eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarShort(s_short=0, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarShort(s_short=2 ** 15 - 1, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarShort(s_short=-2 ** 15 + 1, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventScalarShort(s_short=None, eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventScalarShort(s_short=None, eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_int(self):
        class EventScalarInt(Event):
            _event_name = "EventScalarInt"
            s_int: Scalar[keys.DT_INT]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarInt):
                    if self.s_int == other.s_int and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_int`eventTime,[INT,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarInt{{
                s_int::INT
                eventTime::TIMESTAMP
                def EventScalarInt(s_int_){{
                    s_int=s_int_
                    eventTime=now()
                }}
            }}
            class EventScalarIntMonitor{{
                def EventScalarIntMonitor(){{}}
                def updateEventScalarInt(event){{
                    insert into {func_name}_eventTest values(event.s_int,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarInt,'EventScalarInt',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT, 0) as s_int)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT, 0) as s_int) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT, 0) as s_int) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_int,eventTime"
            fieldType="INT,TIMESTAMP"
            fieldTypeId=[[INT,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarInt", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_int")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarIntMonitor()>, dummy, [EventScalarInt], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarInt], eventTimeFields=["eventTime"],
                             commonFields=["s_int"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarInt], eventTimeFields=["eventTime"], commonFields=["s_int"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate(
                (0, np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31), None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarInt(
                s_int=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_int'],[0i,0i,2147483647i,-2147483647i,00i,00i])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_int': np.array([0, 0, 2 ** 31 - 1, -2 ** 31 + 1, None, None], dtype='float64'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(6)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarInt(s_int=0, eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarInt(s_int=0, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarInt(s_int=2 ** 31 - 1, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarInt(s_int=-2 ** 31 + 1, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventScalarInt(s_int=None, eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventScalarInt(s_int=None, eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_long(self):
        class EventScalarLong(Event):
            _event_name = "EventScalarLong"
            s_long: Scalar[keys.DT_LONG]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarLong):
                    if self.s_long == other.s_long and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
        
            share table(100:0,`s_long`eventTime,[LONG,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarLong{{
                s_long::LONG
                eventTime::TIMESTAMP
                def EventScalarLong(s_long_){{
                    s_long=s_long_
                    eventTime=now()
                }}
            }}
            class EventScalarLongMonitor{{
                def EventScalarLongMonitor(){{}}
                def updateEventScalarLong(event){{
                    insert into {func_name}_eventTest values(event.s_long,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarLong,'EventScalarLong',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(LONG, 0) as s_long)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(LONG, 0) as s_long) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(LONG, 0) as s_long) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_long,eventTime"
            fieldType="LONG,TIMESTAMP"
            fieldTypeId=[[LONG,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarLong", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_long")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarLongMonitor()>, dummy, [EventScalarLong], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarLong],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_long"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarLong], eventTimeFields=["eventTime"], commonFields=["s_long"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate(
                (0, np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63), None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarLong(
                s_long=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_long'],[0l,0l,9223372036854775807l,-9223372036854775807l,00l,00l])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_long': np.array([0, 0, 2 ** 63 - 1, -2 ** 63 + 1, None, None], dtype='float64'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(6)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarLong(s_long=0, eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarLong(s_long=0, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarLong(s_long=2 ** 63 - 1, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarLong(s_long=-2 ** 63 + 1, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventScalarLong(s_long=None, eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventScalarLong(s_long=None, eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_date(self):
        class EventScalarDate(Event):
            _event_name = "EventScalarDate"
            s_date: Scalar[keys.DT_DATE]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarDate):
                    if self.s_date == other.s_date and self.eventTime == other.eventTime:
                        if hasattr(self.s_date, 'dtype') and hasattr(other.s_date, 'dtype'):
                            if self.s_date.dtype != other.s_date.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
        
            share table(100:0,`s_date`eventTime,[DATE,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarDate{{
                s_date::DATE
                eventTime::TIMESTAMP
                def EventScalarDate(s_date_){{
                    s_date=s_date_
                    eventTime=now()
                }}
            }}
            class EventScalarDateMonitor{{
                def EventScalarDateMonitor(){{}}
                def updateEventScalarDate(event){{
                    insert into {func_name}_eventTest values(event.s_date,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarDate,'EventScalarDate',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATE, 0) as s_date)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATE, 0) as s_date) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATE, 0) as s_date) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_date,eventTime"
            fieldType="DATE,TIMESTAMP"
            fieldTypeId=[[DATE,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarDate", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_date")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarDateMonitor()>, dummy, [EventScalarDate], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarDate],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_date"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarDate], eventTimeFields=["eventTime"], commonFields=["s_date"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 'D'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarDate(
                s_date=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_date'],[1970.01.01d,00d,00d])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_date': np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarDate(s_date=np.datetime64(0, 'D'), eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarDate(s_date=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarDate(s_date=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_month(self):
        class EventScalarMonth(Event):
            _event_name = "EventScalarMonth"
            s_month: Scalar[keys.DT_MONTH]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarMonth):
                    if self.s_month == other.s_month and self.eventTime == other.eventTime:
                        if hasattr(self.s_month, 'dtype') and hasattr(other.s_month, 'dtype'):
                            if self.s_month.dtype != other.s_month.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_month`eventTime,[MONTH,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarMonth{{
                s_month::MONTH
                eventTime::TIMESTAMP
                def EventScalarMonth(s_month_){{
                    s_month=s_month_
                    eventTime=now()
                }}
            }}
            class EventScalarMonthMonitor{{
                def EventScalarMonthMonitor(){{}}
                def updateEventScalarMonth(event){{
                    insert into {func_name}_eventTest values(event.s_month,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarMonth,'EventScalarMonth',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MONTH, 0) as s_month)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MONTH, 0) as s_month) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MONTH, 0) as s_month) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_month,eventTime"
            fieldType="MONTH,TIMESTAMP"
            fieldTypeId=[[MONTH,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarMonth", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_month")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarMonthMonitor()>, dummy, [EventScalarMonth], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarMonth],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_month"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarMonth], eventTimeFields=["eventTime"], commonFields=["s_month"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 'M'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarMonth(
                s_month=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_month'],[1970.01M,00M,00M])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_month': np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarMonth(s_month=np.datetime64(0, 'M'), eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarMonth(s_month=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarMonth(s_month=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_time(self):
        class EventScalarTime(Event):
            _event_name = "EventScalarTime"
            s_time: Scalar[keys.DT_TIME]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarTime):
                    if self.s_time == other.s_time and self.eventTime == other.eventTime:
                        if hasattr(self.s_time, 'dtype') and hasattr(other.s_time, 'dtype'):
                            if self.s_time.dtype != other.s_time.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_time`eventTime,[TIME,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarTime{{
                s_time::TIME
                eventTime::TIMESTAMP
                def EventScalarTime(s_time_){{
                    s_time=s_time_
                    eventTime=now()
                }}
            }}
            class EventScalarTimeMonitor{{
                def EventScalarTimeMonitor(){{}}
                def updateEventScalarTime(event){{
                    insert into {func_name}_eventTest values(event.s_time,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarTime,'EventScalarTime',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIME, 0) as s_time)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIME, 0) as s_time) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIME, 0) as s_time) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_time,eventTime"
            fieldType="TIME,TIMESTAMP"
            fieldTypeId=[[TIME,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarTime", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_time")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarTimeMonitor()>, dummy, [EventScalarTime], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarTime],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_time"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarTime], eventTimeFields=["eventTime"], commonFields=["s_time"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 'ms'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarTime(
                s_time=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_time'],[00:00:00.000t,00t,00t])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_time': np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarTime(s_time=np.datetime64(0, 'ms'), eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarTime(s_time=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarTime(s_time=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_minute(self):
        class EventScalarMinute(Event):
            _event_name = "EventScalarMinute"
            s_minute: Scalar[keys.DT_MINUTE]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarMinute):
                    if self.s_minute == other.s_minute and self.eventTime == other.eventTime:
                        if hasattr(self.s_minute, 'dtype') and hasattr(other.s_minute, 'dtype'):
                            if self.s_minute.dtype != other.s_minute.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_minute`eventTime,[MINUTE,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarMinute{{
                s_minute::MINUTE
                eventTime::TIMESTAMP
                def EventScalarMinute(s_minute_){{
                    s_minute=s_minute_
                    eventTime=now()
                }}
            }}
            class EventScalarMinuteMonitor{{
                def EventScalarMinuteMonitor(){{}}
                def updateEventScalarMinute(event){{
                    insert into {func_name}_eventTest values(event.s_minute,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarMinute,'EventScalarMinute',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MINUTE, 0) as s_minute)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MINUTE, 0) as s_minute) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MINUTE, 0) as s_minute) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_minute,eventTime"
            fieldType="MINUTE,TIMESTAMP"
            fieldTypeId=[[MINUTE,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarMinute", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_minute")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarMinuteMonitor()>, dummy, [EventScalarMinute], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarMinute],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_minute"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarMinute], eventTimeFields=["eventTime"], commonFields=["s_minute"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 'm'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarMinute(
                s_minute=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_minute'],[00:00m,00m,00m])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_minute': np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarMinute(s_minute=np.datetime64(0, 'm'), eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarMinute(s_minute=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarMinute(s_minute=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_second(self):
        class EventScalarSecond(Event):
            _event_name = "EventScalarSecond"
            s_second: Scalar[keys.DT_SECOND]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarSecond):
                    if self.s_second == other.s_second and self.eventTime == other.eventTime:
                        if hasattr(self.s_second, 'dtype') and hasattr(other.s_second, 'dtype'):
                            if self.s_second.dtype != other.s_second.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_second`eventTime,[SECOND,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarSecond{{
                s_second::SECOND
                eventTime::TIMESTAMP
                def EventScalarSecond(s_second_){{
                    s_second=s_second_
                    eventTime=now()
                }}
            }}
            class EventScalarSecondMonitor{{
                def EventScalarSecondMonitor(){{}}
                def updateEventScalarSecond(event){{
                    insert into {func_name}_eventTest values(event.s_second,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarSecond,'EventScalarSecond',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SECOND, 0) as s_second)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SECOND, 0) as s_second) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SECOND, 0) as s_second) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_second,eventTime"
            fieldType="SECOND,TIMESTAMP"
            fieldTypeId=[[SECOND,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarSecond", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_second")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarSecondMonitor()>, dummy, [EventScalarSecond], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarSecond],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_second"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarSecond], eventTimeFields=["eventTime"], commonFields=["s_second"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 's'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarSecond(
                s_second=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_second'],[00:00:00s,00s,00s])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_second': np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarSecond(s_second=np.datetime64(0, 's'), eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarSecond(s_second=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarSecond(s_second=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_datetime(self):
        class EventScalarDatetime(Event):
            _event_name = "EventScalarDatetime"
            s_datetime: Scalar[keys.DT_DATETIME]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarDatetime):
                    if self.s_datetime == other.s_datetime and self.eventTime == other.eventTime:
                        if hasattr(self.s_datetime, 'dtype') and hasattr(other.s_datetime, 'dtype'):
                            if self.s_datetime.dtype != other.s_datetime.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_datetime`eventTime,[DATETIME,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarDatetime{{
                s_datetime::DATETIME
                eventTime::TIMESTAMP
                def EventScalarDatetime(s_datetime_){{
                    s_datetime=s_datetime_
                    eventTime=now()
                }}
            }}
            class EventScalarDatetimeMonitor{{
                def EventScalarDatetimeMonitor(){{}}
                def updateEventScalarDatetime(event){{
                    insert into {func_name}_eventTest values(event.s_datetime,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarDatetime,'EventScalarDatetime',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATETIME, 0) as s_datetime)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATETIME, 0) as s_datetime) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATETIME, 0) as s_datetime) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_datetime,eventTime"
            fieldType="DATETIME,TIMESTAMP"
            fieldTypeId=[[DATETIME,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarDatetime", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_datetime")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarDatetimeMonitor()>, dummy, [EventScalarDatetime], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarDatetime],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_datetime"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarDatetime], eventTimeFields=["eventTime"], commonFields=["s_datetime"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 's'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarDatetime(
                s_datetime=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_datetime'],[1970.01.01T00:00:00D,00D,00D])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_datetime': np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarDatetime(s_datetime=np.datetime64(0, 's'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarDatetime(s_datetime=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarDatetime(s_datetime=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_timestamp(self):
        class EventScalarTimestamp(Event):
            _event_name = "EventScalarTimestamp"
            s_timestamp: Scalar[keys.DT_TIMESTAMP]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarTimestamp):
                    if self.s_timestamp == other.s_timestamp and self.eventTime == other.eventTime:
                        if hasattr(self.s_timestamp, 'dtype') and hasattr(other.s_timestamp, 'dtype'):
                            if self.s_timestamp.dtype != other.s_timestamp.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_timestamp`eventTime,[TIMESTAMP,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarTimestamp{{
                s_timestamp::TIMESTAMP
                eventTime::TIMESTAMP
                def EventScalarTimestamp(s_timestamp_){{
                    s_timestamp=s_timestamp_
                    eventTime=now()
                }}
            }}
            class EventScalarTimestampMonitor{{
                def EventScalarTimestampMonitor(){{}}
                def updateEventScalarTimestamp(event){{
                    insert into {func_name}_eventTest values(event.s_timestamp,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarTimestamp,'EventScalarTimestamp',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIMESTAMP, 0) as s_timestamp)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIMESTAMP, 0) as s_timestamp) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIMESTAMP, 0) as s_timestamp) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_timestamp,eventTime"
            fieldType="TIMESTAMP,TIMESTAMP"
            fieldTypeId=[[TIMESTAMP,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarTimestamp", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_timestamp")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarTimestampMonitor()>, dummy, [EventScalarTimestamp], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarTimestamp],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_timestamp"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarTimestamp], eventTimeFields=["eventTime"], commonFields=["s_timestamp"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 'ms'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarTimestamp(
                s_timestamp=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_timestamp'],[1970.01.01T00:00:00.000T,00T,00T])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_timestamp': np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarTimestamp(s_timestamp=np.datetime64(0, 'ms'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarTimestamp(s_timestamp=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarTimestamp(s_timestamp=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_nanotime(self):
        class EventScalarNanotime(Event):
            _event_name = "EventScalarNanotime"
            s_nanotime: Scalar[keys.DT_NANOTIME]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarNanotime):
                    if self.s_nanotime == other.s_nanotime and self.eventTime == other.eventTime:
                        if hasattr(self.s_nanotime, 'dtype') and hasattr(other.s_nanotime, 'dtype'):
                            if self.s_nanotime.dtype != other.s_nanotime.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_nanotime`eventTime,[NANOTIME,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarNanotime{{
                s_nanotime::NANOTIME
                eventTime::TIMESTAMP
                def EventScalarNanotime(s_nanotime_){{
                    s_nanotime=s_nanotime_
                    eventTime=now()
                }}
            }}
            class EventScalarNanotimeMonitor{{
                def EventScalarNanotimeMonitor(){{}}
                def updateEventScalarNanotime(event){{
                    insert into {func_name}_eventTest values(event.s_nanotime,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarNanotime,'EventScalarNanotime',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIME, 0) as s_nanotime)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIME, 0) as s_nanotime) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIME, 0) as s_nanotime) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_nanotime,eventTime"
            fieldType="NANOTIME,TIMESTAMP"
            fieldTypeId=[[NANOTIME,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarNanotime", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_nanotime")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarNanotimeMonitor()>, dummy, [EventScalarNanotime], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarNanotime],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_nanotime"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarNanotime], eventTimeFields=["eventTime"], commonFields=["s_nanotime"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 'ns'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarNanotime(
                s_nanotime=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_nanotime'],[00:00:00.000000000n,00n,00n])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_nanotime': np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarNanotime(s_nanotime=np.datetime64(0, 'ns'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarNanotime(s_nanotime=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarNanotime(s_nanotime=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_nanotimestamp(self):
        class EventScalarNanotimestamp(Event):
            _event_name = "EventScalarNanotimestamp"
            s_nanotimestamp: Scalar[keys.DT_NANOTIMESTAMP]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarNanotimestamp):
                    if self.s_nanotimestamp == other.s_nanotimestamp and self.eventTime == other.eventTime:
                        if hasattr(self.s_nanotimestamp, 'dtype') and hasattr(other.s_nanotimestamp, 'dtype'):
                            if self.s_nanotimestamp.dtype != other.s_nanotimestamp.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
        
            share table(100:0,`s_nanotimestamp`eventTime,[NANOTIMESTAMP,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarNanotimestamp{{
                s_nanotimestamp::NANOTIMESTAMP
                eventTime::TIMESTAMP
                def EventScalarNanotimestamp(s_nanotimestamp_){{
                    s_nanotimestamp=s_nanotimestamp_
                    eventTime=now()
                }}
            }}
            class EventScalarNanotimestampMonitor{{
                def EventScalarNanotimestampMonitor(){{}}
                def updateEventScalarNanotimestamp(event){{
                    insert into {func_name}_eventTest values(event.s_nanotimestamp,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarNanotimestamp,'EventScalarNanotimestamp',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIMESTAMP, 0) as s_nanotimestamp)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIMESTAMP, 0) as s_nanotimestamp) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIMESTAMP, 0) as s_nanotimestamp) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_nanotimestamp,eventTime"
            fieldType="NANOTIMESTAMP,TIMESTAMP"
            fieldTypeId=[[NANOTIMESTAMP,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarNanotimestamp", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_nanotimestamp")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarNanotimestampMonitor()>, dummy, [EventScalarNanotimestamp], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarNanotimestamp],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_nanotimestamp"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarNanotimestamp], eventTimeFields=["eventTime"],
                             commonFields=["s_nanotimestamp"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 'ns'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarNanotimestamp(
                s_nanotimestamp=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_nanotimestamp'],[1970.01.01T00:00:00.000000000N,00N,00N])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_nanotimestamp': np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarNanotimestamp(s_nanotimestamp=np.datetime64(0, 'ns'),
                                     eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarNanotimestamp(s_nanotimestamp=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarNanotimestamp(s_nanotimestamp=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_datehour(self):
        class EventScalarDatehour(Event):
            _event_name = "EventScalarDatehour"
            s_datehour: Scalar[keys.DT_DATEHOUR]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarDatehour):
                    if self.s_datehour == other.s_datehour and self.eventTime == other.eventTime:
                        if hasattr(self.s_datehour, 'dtype') and hasattr(other.s_datehour, 'dtype'):
                            if self.s_datehour.dtype != other.s_datehour.dtype:
                                return False
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
        
            share table(100:0,`s_datehour`eventTime,[DATEHOUR,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarDatehour{{
                s_datehour::DATEHOUR
                eventTime::TIMESTAMP
                def EventScalarDatehour(s_datehour_){{
                    s_datehour=s_datehour_
                    eventTime=now()
                }}
            }}
            class EventScalarDatehourMonitor{{
                def EventScalarDatehourMonitor(){{}}
                def updateEventScalarDatehour(event){{
                    insert into {func_name}_eventTest values(event.s_datehour,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarDatehour,'EventScalarDatehour',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATEHOUR, 0) as s_datehour)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATEHOUR, 0) as s_datehour) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATEHOUR, 0) as s_datehour) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_datehour,eventTime"
            fieldType="DATEHOUR,TIMESTAMP"
            fieldTypeId=[[DATEHOUR,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarDatehour", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_datehour")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarDatehourMonitor()>, dummy, [EventScalarDatehour], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarDatehour],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_datehour"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarDatehour], eventTimeFields=["eventTime"], commonFields=["s_datehour"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((np.datetime64(0, 'h'), pd.NaT, None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarDatehour(
                s_datehour=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(f"eqObj({func_name}_output['s_datehour'],datehour([0,null,null]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_datehour': np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[ns]'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarDatehour(s_datehour=np.datetime64(0, 'h'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarDatehour(s_datehour=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarDatehour(s_datehour=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_float(self):
        class EventScalarFloat(Event):
            _event_name = "EventScalarFloat"
            s_float: Scalar[keys.DT_FLOAT]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarFloat):
                    if ((other.s_float is None and self.s_float is None) or math.isclose(self.s_float, other.s_float,
                                                                                         rel_tol=1e-6)) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_float`eventTime,[FLOAT,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarFloat{{
                s_float::FLOAT
                eventTime::TIMESTAMP
                def EventScalarFloat(s_float_){{
                    s_float=s_float_
                    eventTime=now()
                }}
            }}
            class EventScalarFloatMonitor{{
                def EventScalarFloatMonitor(){{}}
                def updateEventScalarFloat(event){{
                    insert into {func_name}_eventTest values(event.s_float,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarFloat,'EventScalarFloat',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(FLOAT, 0) as s_float)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(FLOAT, 0) as s_float) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(FLOAT, 0) as s_float) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_float,eventTime"
            fieldType="FLOAT,TIMESTAMP"
            fieldTypeId=[[FLOAT,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarFloat", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_float")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarFloatMonitor()>, dummy, [EventScalarFloat], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarFloat],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_float"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarFloat], eventTimeFields=["eventTime"], commonFields=["s_float"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38),
                                      np.float32(-3.4028235e+38), np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0],
                                      None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarFloat(
                s_float=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        # todo
        # assert self.__class__.conn.run("eqFloat(input['s_float'],[3.14f,00f,0f,340282346638528860000000000000000000000f,00f,00f,00f])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_float': np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float32'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(7)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert_almost_equal(df, df_expect)
        expect = [
            EventScalarFloat(s_float=3.14, eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarFloat(s_float=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarFloat(s_float=0.0, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarFloat(s_float=3.4028235e+38, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventScalarFloat(s_float=None, eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventScalarFloat(s_float=None, eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventScalarFloat(s_float=None, eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_double(self):
        class EventScalarDouble(Event):
            _event_name = "EventScalarDouble"
            s_double: Scalar[keys.DT_DOUBLE]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarDouble):
                    if ((other.s_double is None and self.s_double is None) or math.isclose(self.s_double,
                                                                                           other.s_double,
                                                                                           rel_tol=1e-6)) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_double`eventTime,[DOUBLE,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarDouble{{
                s_double::DOUBLE
                eventTime::TIMESTAMP
                def EventScalarDouble(s_double_){{
                    s_double=s_double_
                    eventTime=now()
                }}
            }}
            class EventScalarDoubleMonitor{{
                def EventScalarDoubleMonitor(){{}}
                def updateEventScalarDouble(event){{
                    insert into {func_name}_eventTest values(event.s_double,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarDouble,'EventScalarDouble',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DOUBLE, 0) as s_double)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DOUBLE, 0) as s_double) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DOUBLE, 0) as s_double) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_double,eventTime"
            fieldType="DOUBLE,TIMESTAMP"
            fieldTypeId=[[DOUBLE,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarDouble", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_double")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarDoubleMonitor()>, dummy, [EventScalarDouble], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarDouble],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_double"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarDouble], eventTimeFields=["eventTime"], commonFields=["s_double"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308),
                                      np.float64(-1.7976931348623157e+308),
                                      np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarDouble(
                s_double=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        # todo
        # assert self.__class__.conn.run("eqObj(output['s_bool'],[true,false,true,false,00b])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_double': np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                                  np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float64'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(7)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert_almost_equal(df, df_expect)
        expect = [
            EventScalarDouble(s_double=3.14, eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarDouble(s_double=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarDouble(s_double=0.0, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarDouble(s_double=1.7976931348623157e+308,
                              eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventScalarDouble(s_double=None, eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventScalarDouble(s_double=None, eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventScalarDouble(s_double=None, eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_string(self):
        class EventScalarString(Event):
            _event_name = "EventScalarString"
            s_string: Scalar[keys.DT_STRING]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarString):
                    if self.s_string == other.s_string and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_string`eventTime,[STRING,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarString{{
                s_string::STRING
                eventTime::TIMESTAMP
                def EventScalarString(s_string_){{
                    s_string=s_string_
                    eventTime=now()
                }}
            }}
            class EventScalarStringMonitor{{
                def EventScalarStringMonitor(){{}}
                def updateEventScalarString(event){{
                    insert into {func_name}_eventTest values(event.s_string,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarString,'EventScalarString',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(STRING, 0) as s_string)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(STRING, 0) as s_string) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(STRING, 0) as s_string) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_string,eventTime"
            fieldType="STRING,TIMESTAMP"
            fieldTypeId=[[STRING,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarString", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_string")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarStringMonitor()>, dummy, [EventScalarString], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarString],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_string"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarString], eventTimeFields=["eventTime"], commonFields=["s_string"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate(("abc!@#中文 123", np.str_("abc!@#中文 123"), "", None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarString(
                s_string=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_string'],[\"abc!@#中文 123\", \"abc!@#中文 123\", \"\", \"\"])")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_string': np.array(["abc!@#中文 123", "abc!@#中文 123", "", ""], dtype='object'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(4)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarString(s_string="abc!@#中文 123", eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarString(s_string="abc!@#中文 123", eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarString(s_string=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarString(s_string=None, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_blob(self):
        class EventScalarBlob(Event):
            _event_name = "EventScalarBlob"
            s_blob: Scalar[keys.DT_BLOB]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarBlob):
                    if self.s_blob == other.s_blob and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_blob`eventTime,[BLOB,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarBlob{{
                s_blob::BLOB
                eventTime::TIMESTAMP
                def EventScalarBlob(s_blob_){{
                    s_blob=s_blob_
                    eventTime=now()
                }}
            }}
            class EventScalarBlobMonitor{{
                def EventScalarBlobMonitor(){{}}
                def updateEventScalarBlob(event){{
                    insert into {func_name}_eventTest values(event.s_blob,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarBlob,'EventScalarBlob',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(BLOB, 0) as s_blob)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(BLOB, 0) as s_blob) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(BLOB, 0) as s_blob) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_blob,eventTime"
            fieldType="BLOB,TIMESTAMP"
            fieldTypeId=[[BLOB,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarBlob", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_blob")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarBlobMonitor()>, dummy, [EventScalarBlob], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarBlob],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_blob"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarBlob], eventTimeFields=["eventTime"], commonFields=["s_blob"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate(("abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'),
                                      np.bytes_("abc!@#中文 123".encode()), np.bytes_("abc!@#中文 123".encode('gbk')),
                                      b"", None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarBlob(
                s_blob=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_blob'],blob(['abc!@#中文 123',fromUTF8('abc!@#中文 123','gbk'),'abc!@#中文 123',fromUTF8('abc!@#中文 123','gbk'),\"\",\"\"]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_blob': np.array(["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), "abc!@#中文 123".encode(),
                                "abc!@#中文 123".encode('gbk'), b"", b""],
                               dtype='object'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(6)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarBlob(s_blob="abc!@#中文 123".encode(), eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarBlob(s_blob="abc!@#中文 123".encode('gbk'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarBlob(s_blob="abc!@#中文 123".encode(), eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarBlob(s_blob="abc!@#中文 123".encode('gbk'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventScalarBlob(s_blob=None, eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventScalarBlob(s_blob=None, eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_int128(self):
        class EventScalarInt128(Event):
            _event_name = "EventScalarInt128"
            s_int128: Scalar[keys.DT_INT128]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarInt128):
                    if self.s_int128 == other.s_int128 and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_int128`eventTime,[INT128,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarInt128{{
                s_int128::INT128
                eventTime::TIMESTAMP
                def EventScalarInt128(s_int128_){{
                    s_int128=s_int128_
                    eventTime=now()
                }}
            }}
            class EventScalarInt128Monitor{{
                def EventScalarInt128Monitor(){{}}
                def updateEventScalarInt128(event){{
                    insert into {func_name}_eventTest values(event.s_int128,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarInt128,'EventScalarInt128',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT128, 0) as s_int128)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT128, 0) as s_int128) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT128, 0) as s_int128) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_int128,eventTime"
            fieldType="INT128,TIMESTAMP"
            fieldTypeId=[[INT128,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarInt128", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_int128")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarInt128Monitor()>, dummy, [EventScalarInt128], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarInt128],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_int128"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarInt128], eventTimeFields=["eventTime"], commonFields=["s_int128"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate(('e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarInt128(
                s_int128=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_int128'],int128(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', '00000000000000000000000000000000']))")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_int128': np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                                  '00000000000000000000000000000000'], dtype='object'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarInt128(s_int128='e1671797c52e15f763380b45e841ec32',
                              eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarInt128(s_int128=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarInt128(s_int128=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_uuid(self):
        class EventScalarUuid(Event):
            _event_name = "EventScalarUuid"
            s_uuid: Scalar[keys.DT_UUID]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarUuid):
                    if self.s_uuid == other.s_uuid and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
        
            share table(100:0,`s_uuid`eventTime,[UUID,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarUuid{{
                s_uuid::UUID
                eventTime::TIMESTAMP
                def EventScalarUuid(s_uuid_){{
                    s_uuid=s_uuid_
                    eventTime=now()
                }}
            }}
            class EventScalarUuidMonitor{{
                def EventScalarUuidMonitor(){{}}
                def updateEventScalarUuid(event){{
                    insert into {func_name}_eventTest values(event.s_uuid,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarUuid,'EventScalarUuid',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(UUID, 0) as s_uuid)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(UUID, 0) as s_uuid) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(UUID, 0) as s_uuid) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_uuid,eventTime"
            fieldType="UUID,TIMESTAMP"
            fieldTypeId=[[UUID,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarUuid", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_uuid")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarUuidMonitor()>, dummy, [EventScalarUuid], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarUuid],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_uuid"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarUuid], eventTimeFields=["eventTime"], commonFields=["s_uuid"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate(
                ('5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarUuid(
                s_uuid=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_uuid'],uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000']))")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_uuid': np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                                '00000000-0000-0000-0000-000000000000'], dtype='object'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarUuid(s_uuid='5d212a78-cc48-e3b1-4235-b4d91473ee87',
                            eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarUuid(s_uuid=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarUuid(s_uuid=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_ipaddr(self):
        class EventScalarIpaddr(Event):
            _event_name = "EventScalarIpaddr"
            s_ipaddr: Scalar[keys.DT_IPADDR]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarIpaddr):
                    if self.s_ipaddr == other.s_ipaddr and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_ipaddr`eventTime,[IPADDR,TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarIpaddr{{
                s_ipaddr::IPADDR
                eventTime::TIMESTAMP
                def EventScalarIpaddr(s_ipaddr_){{
                    s_ipaddr=s_ipaddr_
                    eventTime=now()
                }}
            }}
            class EventScalarIpaddrMonitor{{
                def EventScalarIpaddrMonitor(){{}}
                def updateEventScalarIpaddr(event){{
                    insert into {func_name}_eventTest values(event.s_ipaddr,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarIpaddr,'EventScalarIpaddr',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(IPADDR, 0) as s_ipaddr)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(IPADDR, 0) as s_ipaddr) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(IPADDR, 0) as s_ipaddr) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_ipaddr,eventTime"
            fieldType="IPADDR,TIMESTAMP"
            fieldTypeId=[[IPADDR,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarIpaddr", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_ipaddr")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarIpaddrMonitor()>, dummy, [EventScalarIpaddr], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarIpaddr],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_ipaddr"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarIpaddr], eventTimeFields=["eventTime"], commonFields=["s_ipaddr"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate(('127.0.0.1', '0.0.0.0', None)):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarIpaddr(
                s_ipaddr=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_ipaddr'],ipaddr(['127.0.0.1', '0.0.0.0','0.0.0.0']))")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_ipaddr': np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(3)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarIpaddr(s_ipaddr='127.0.0.1', eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarIpaddr(s_ipaddr=None, eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarIpaddr(s_ipaddr=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_decimal32_4(self):
        class EventScalarDecimal32(Event):
            _event_name = "EventScalarDecimal32"
            s_decimal32_4: Scalar[keys.DT_DECIMAL32, 4]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarDecimal32):
                    if self.s_decimal32_4 == other.s_decimal32_4 and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}

            share table(100:0,`s_decimal32_4`eventTime,[DECIMAL32(4),TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarDecimal32{{
                s_decimal32_4::DECIMAL32(4)
                eventTime::TIMESTAMP
                def EventScalarDecimal32(s_decimal32_4_){{
                    s_decimal32_4=s_decimal32_4_
                    eventTime=now()
                }}
            }}
            class EventScalarDecimal32Monitor{{
                def EventScalarDecimal32Monitor(){{}}
                def updateEventScalarDecimal32(event){{
                    insert into {func_name}_eventTest values(event.s_decimal32_4,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarDecimal32,'EventScalarDecimal32',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL32(4), 0) as s_decimal32_4)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL32(4), 0) as s_decimal32_4) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL32(4), 0) as s_decimal32_4) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_decimal32_4,eventTime"
            fieldType="DECIMAL32(4),TIMESTAMP"
            fieldTypeId=[[DECIMAL32(4),TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarDecimal32", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_decimal32_4")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarDecimal32Monitor()>, dummy, [EventScalarDecimal32], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarDecimal32],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_decimal32_4"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarDecimal32], eventTimeFields=["eventTime"], commonFields=["s_decimal32_4"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((Decimal('0.0000'), Decimal('3.1415'), None, Decimal("nan"))):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarDecimal32(
                s_decimal32_4=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_decimal32_4'],decimal32(['0.0000','3.1415',null,null],4))")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_decimal32_4': np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(4)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarDecimal32(s_decimal32_4=Decimal('0.0000'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarDecimal32(s_decimal32_4=Decimal('3.1415'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarDecimal32(s_decimal32_4=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarDecimal32(s_decimal32_4=None, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_decimal64_12(self):
        class EventScalarDecimal64(Event):
            _event_name = "EventScalarDecimal64"
            s_decimal64_12: Scalar[keys.DT_DECIMAL64, 12]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarDecimal64):
                    if self.s_decimal64_12 == other.s_decimal64_12 and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_decimal64_12`eventTime,[DECIMAL64(12),TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarDecimal64{{
                s_decimal64_12::DECIMAL64(12)
                eventTime::TIMESTAMP
                def EventScalarDecimal64(s_decimal64_12_){{
                    s_decimal64_12=s_decimal64_12_
                    eventTime=now()
                }}
            }}
            class EventScalarDecimal64Monitor{{
                def EventScalarDecimal64Monitor(){{}}
                def updateEventScalarDecimal64(event){{
                    insert into {func_name}_eventTest values(event.s_decimal64_12,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarDecimal64,'EventScalarDecimal64',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL64(12), 0) as s_decimal64_12)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL64(12), 0) as s_decimal64_12) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL64(12), 0) as s_decimal64_12) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_decimal64_12,eventTime"
            fieldType="DECIMAL64(12),TIMESTAMP"
            fieldTypeId=[[DECIMAL64(12),TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarDecimal64", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_decimal64_12")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarDecimal64Monitor()>, dummy, [EventScalarDecimal64], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarDecimal64],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_decimal64_12"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarDecimal64], eventTimeFields=["eventTime"], commonFields=["s_decimal64_12"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((Decimal('0.000000000000'), Decimal('3.141592653589'), None, Decimal("nan"))):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarDecimal64(
                s_decimal64_12=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_decimal64_12'],decimal64(['0.000000000000','3.141592653589',null,null],12))")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_decimal64_12': np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None],
                                       dtype='object'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(4)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarDecimal64(s_decimal64_12=Decimal('0.000000000000'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarDecimal64(s_decimal64_12=Decimal('3.141592653589'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarDecimal64(s_decimal64_12=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarDecimal64(s_decimal64_12=None, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_scalar_decimal128_26(self):
        class EventScalarDecimal128(Event):
            _event_name = "EventScalarDecimal128"
            s_decimal128_26: Scalar[keys.DT_DECIMAL128, 26]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventScalarDecimal128):
                    if self.s_decimal128_26 == other.s_decimal128_26 and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`s_decimal128_26`eventTime,[DECIMAL128(26),TIMESTAMP]) as `{func_name}_eventTest
            class EventScalarDecimal128{{
                s_decimal128_26::DECIMAL128(26)
                eventTime::TIMESTAMP
                def EventScalarDecimal128(s_decimal128_26_){{
                    s_decimal128_26=s_decimal128_26_
                    eventTime=now()
                }}
            }}
            class EventScalarDecimal128Monitor{{
                def EventScalarDecimal128Monitor(){{}}
                def updateEventScalarDecimal128(event){{
                    insert into {func_name}_eventTest values(event.s_decimal128_26,event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventScalarDecimal128,'EventScalarDecimal128',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL128(26), 0) as s_decimal128_26)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL128(26), 0) as s_decimal128_26) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL128(26), 0) as s_decimal128_26) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_decimal128_26,eventTime"
            fieldType="DECIMAL128(26),TIMESTAMP"
            fieldTypeId=[[DECIMAL128(26),TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("EventScalarDecimal128", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="s_decimal128_26")
            engine = createCEPEngine('{func_name}_cep1', <EventScalarDecimal128Monitor()>, dummy, [EventScalarDecimal128], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventScalarDecimal128],
                             eventTimeFields=["eventTime"],
                             commonFields=["s_decimal128_26"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventScalarDecimal128], eventTimeFields=["eventTime"], commonFields=["s_decimal128_26"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'),
                                      None, Decimal("nan"))):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventScalarDecimal128(
                s_decimal128_26=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['s_decimal128_26'],decimal128(['0.00000000000000000000000000','3.14159265358979323846264338',null,null],26))")
        # check server deserialize
        df_expect = pd.DataFrame({
            's_decimal128_26': np.array(
                [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                dtype='object'),
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(4)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventScalarDecimal128(s_decimal128_26=Decimal('0.00000000000000000000000000'),
                                  eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventScalarDecimal128(s_decimal128_26=Decimal('3.14159265358979323846264338'),
                                  eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventScalarDecimal128(s_decimal128_26=None, eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventScalarDecimal128(s_decimal128_26=None, eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_bool(self):
        class EventVectorBool(Event):
            _event_name = "EventVectorBool"
            v_bool: Vector[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorBool):
                    if equalPlus(self.v_bool, other.v_bool) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_bool`eventTime,[BOOL[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorBool{{
                v_bool::BOOL VECTOR
                eventTime::TIMESTAMP
                def EventVectorBool(v_bool_){{
                    v_bool=v_bool_
                    eventTime=now()
                }}
            }}
            class EventVectorBoolMonitor{{
                def EventVectorBoolMonitor(){{}}
                def updateEventVectorBool(event){{
                    insert into {func_name}_eventTest values([event.v_bool],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorBool,'EventVectorBool',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(BOOL[], 0) as v_bool)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(BOOL[], 0) as v_bool) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(BOOL[], 0) as v_bool) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_bool,eventTime"
            fieldType="BOOL,TIMESTAMP"
            fieldTypeId=[[BOOL,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorBool", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_bool")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorBoolMonitor()>, dummy, [EventVectorBool], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorBool],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_bool"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorBool], eventTimeFields=["eventTime"], commonFields=["v_bool"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, True, False, np.bool_(True), np.bool_(False)],
                [True, False, None, np.bool_(True), np.bool_(False)],
                [True, False, np.bool_(True), np.bool_(False), None],
                np.array([None, True, False, np.bool_(True), np.bool_(False)], dtype='object'),
                np.array([True, False, None, np.bool_(True), np.bool_(False)], dtype='object'),
                np.array([True, False, np.bool_(True), np.bool_(False), None], dtype='object'),
                np.array([True, False, np.bool_(True), np.bool_(False)], dtype=np.bool_),
                pd.Series([None, True, False, np.bool_(True), np.bool_(False)], dtype='object'),
                pd.Series([True, False, None, np.bool_(True), np.bool_(False)], dtype='object'),
                pd.Series([True, False, np.bool_(True), np.bool_(False), None], dtype='object'),
                pd.Series([True, False, np.bool_(True), np.bool_(False)], dtype=np.bool_),
                pd.Series([True, False, True, False], dtype=pd.BooleanDtype()),
                [],
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorBool(
                v_bool=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][0,],array(BOOL[]).append!([[00b,true,false,true,false]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][1,],array(BOOL[]).append!([[true,false,00b,true,false]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][2,],array(BOOL[]).append!([[true,false,true,false,00b]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][3,],array(BOOL[]).append!([[00b,true,false,true,false]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][4,],array(BOOL[]).append!([[true,false,00b,true,false]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][5,],array(BOOL[]).append!([[true,false,true,false,00b]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][6,],array(BOOL[]).append!([[true,false,true,false]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][7,],array(BOOL[]).append!([[00b,true,false,true,false]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][8,],array(BOOL[]).append!([[true,false,00b,true,false]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][9,],array(BOOL[]).append!([[true,false,true,false,00b]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][10,],array(BOOL[]).append!([[true,false,true,false]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_bool'][11,],array(BOOL[]).append!([[true,false,true,false]]))")
        assert self.__class__.conn.run(f"eqObj({func_name}_output['v_bool'][12,],array(BOOL[]).append!([[00b]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_bool': [
                np.array([None, True, False, True, False], dtype='object'),
                np.array([True, False, None, True, False], dtype='object'),
                np.array([True, False, True, False, None], dtype='object'),
                np.array([None, True, False, True, False], dtype='object'),
                np.array([True, False, None, True, False], dtype='object'),
                np.array([True, False, True, False, None], dtype='object'),
                np.array([True, False, True, False], dtype=np.bool_),
                np.array([None, True, False, True, False], dtype='object'),
                np.array([True, False, None, True, False], dtype='object'),
                np.array([True, False, True, False, None], dtype='object'),
                np.array([True, False, True, False], dtype=np.bool_),
                np.array([True, False, True, False], dtype=np.bool_),
                np.array([None], dtype='object'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(13)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorBool(v_bool=np.array([None, True, False, True, False], dtype='object'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorBool(v_bool=np.array([True, False, None, True, False], dtype='object'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorBool(v_bool=np.array([True, False, True, False, None], dtype='object'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorBool(v_bool=np.array([None, True, False, True, False], dtype='object'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorBool(v_bool=np.array([True, False, None, True, False], dtype='object'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorBool(v_bool=np.array([True, False, True, False, None], dtype='object'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorBool(v_bool=np.array([True, False, True, False], dtype=np.bool_),
                            eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorBool(v_bool=np.array([None, True, False, True, False], dtype='object'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorBool(v_bool=np.array([True, False, None, True, False], dtype='object'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorBool(v_bool=np.array([True, False, True, False, None], dtype='object'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorBool(v_bool=np.array([True, False, True, False], dtype=np.bool_),
                            eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
            EventVectorBool(v_bool=np.array([True, False, True, False], dtype=np.bool_),
                            eventTime=np.datetime64('2024-03-25T12:30:05.011', 'ms')),
            EventVectorBool(v_bool=np.array([], dtype=np.bool_),
                            eventTime=np.datetime64('2024-03-25T12:30:05.012', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_char(self):
        class EventVectorChar(Event):
            _event_name = "EventVectorChar"
            v_char: Vector[keys.DT_CHAR]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorChar):
                    if equalPlus(self.v_char, other.v_char) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_char`eventTime,[CHAR[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorChar{{
                v_char::CHAR VECTOR
                eventTime::TIMESTAMP
                def EventVectorChar(v_char_){{
                    v_char=v_char_
                    eventTime=now()
                }}
            }}
            class EventVectorCharMonitor{{
                def EventVectorCharMonitor(){{}}
                def updateEventVectorChar(event){{
                    insert into {func_name}_eventTest values([event.v_char],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorChar,'EventVectorChar',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(CHAR[], 0) as v_char)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(CHAR[], 0) as v_char) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(CHAR[], 0) as v_char) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_char,eventTime"
            fieldType="CHAR,TIMESTAMP"
            fieldTypeId=[[CHAR,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorChar", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_char")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorCharMonitor()>, dummy, [EventVectorChar], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorChar],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_char"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorChar], eventTimeFields=["eventTime"], commonFields=["v_char"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7)],
                [np.int8(0), np.int8(2 ** 7 - 1), None, np.int8(-2 ** 7 + 1), np.int8(-2 ** 7)],
                [np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7), None],
                np.array([None, np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7)],
                         dtype='object'),
                np.array([np.int8(0), np.int8(2 ** 7 - 1), None, np.int8(-2 ** 7 + 1), np.int8(-2 ** 7)],
                         dtype='object'),
                np.array([np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7), None],
                         dtype='object'),
                np.array([np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7)], dtype=np.int8),
                pd.Series([None, np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7)],
                          dtype='object'),
                pd.Series([np.int8(0), np.int8(2 ** 7 - 1), None, np.int8(-2 ** 7 + 1), np.int8(-2 ** 7)],
                          dtype='object'),
                pd.Series([np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7), None],
                          dtype='object'),
                pd.Series([np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7)], dtype=np.int8),
                pd.Series([np.int8(0), np.int8(2 ** 7 - 1), np.int8(-2 ** 7 + 1), np.int8(-2 ** 7)],
                          dtype=pd.Int8Dtype()),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorChar(
                v_char=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][0,],array(CHAR[]).append!([[00c,0c,127c,-127c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][1,],array(CHAR[]).append!([[0c,127c,00c,-127c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][2,],array(CHAR[]).append!([[0c,127c,-127c,00c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][3,],array(CHAR[]).append!([[00c,0c,127c,-127c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][4,],array(CHAR[]).append!([[0c,127c,00c,-127c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][5,],array(CHAR[]).append!([[0c,127c,-127c,00c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][6,],array(CHAR[]).append!([[0c,127c,-127c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][7,],array(CHAR[]).append!([[00c,0c,127c,-127c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][8,],array(CHAR[]).append!([[0c,127c,00c,-127c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][9,],array(CHAR[]).append!([[0c,127c,-127c,00c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][10,],array(CHAR[]).append!([[0c,127c,-127c,00c]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_char'][11,],array(CHAR[]).append!([[0c,127c,-127c,00c]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_char': [
                np.array([None, 0, 127, -127, None], dtype='float64'),
                np.array([0, 127, None, -127, None], dtype='float64'),
                np.array([0, 127, -127, None, None], dtype='float64'),
                np.array([None, 0, 127, -127, None], dtype='float64'),
                np.array([0, 127, None, -127, None], dtype='float64'),
                np.array([0, 127, -127, None, None], dtype='float64'),
                np.array([0, 127, -127, None], dtype='float64'),
                np.array([None, 0, 127, -127, None], dtype='float64'),
                np.array([0, 127, None, -127, None], dtype='float64'),
                np.array([0, 127, -127, None, None], dtype='float64'),
                np.array([0, 127, -127, None], dtype='float64'),
                np.array([0, 127, -127, None], dtype='float64'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(12)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorChar(v_char=np.array([None, 0, 127, -127, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorChar(v_char=np.array([0, 127, None, -127, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorChar(v_char=np.array([0, 127, -127, None, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorChar(v_char=np.array([None, 0, 127, -127, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorChar(v_char=np.array([0, 127, None, -127, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorChar(v_char=np.array([0, 127, -127, None, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorChar(v_char=np.array([0, 127, -127, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorChar(v_char=np.array([None, 0, 127, -127, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorChar(v_char=np.array([0, 127, None, -127, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorChar(v_char=np.array([0, 127, -127, None, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorChar(v_char=np.array([0, 127, -127, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
            EventVectorChar(v_char=np.array([0, 127, -127, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.011', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_short(self):
        class EventVectorShort(Event):
            _event_name = "EventVectorShort"
            v_short: Vector[keys.DT_SHORT]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorShort):
                    if equalPlus(self.v_short, other.v_short) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_short`eventTime,[SHORT[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorShort{{
                v_short::SHORT VECTOR
                eventTime::TIMESTAMP
                def EventVectorShort(v_short_){{
                    v_short=v_short_
                    eventTime=now()
                }}
            }}
            class EventVectorShortMonitor{{
                def EventVectorShortMonitor(){{}}
                def updateEventVectorShort(event){{
                    insert into {func_name}_eventTest values([event.v_short],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorShort,'EventVectorShort',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SHORT[], 0) as v_short)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SHORT[], 0) as v_short) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SHORT[], 0) as v_short) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_short,eventTime"
            fieldType="SHORT,TIMESTAMP"
            fieldTypeId=[[SHORT,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorShort", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_short")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorShortMonitor()>, dummy, [EventVectorShort], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorShort],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_short"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorShort], eventTimeFields=["eventTime"], commonFields=["v_short"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15)],
                [np.int16(0), np.int16(2 ** 15 - 1), None, np.int16(-2 ** 15 + 1), np.int16(-2 ** 15)],
                [np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15), None],
                np.array([None, np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15)],
                         dtype='object'),
                np.array([np.int16(0), np.int16(2 ** 15 - 1), None, np.int16(-2 ** 15 + 1), np.int16(-2 ** 15)],
                         dtype='object'),
                np.array([np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15), None],
                         dtype='object'),
                np.array([np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15)],
                         dtype=np.int16),
                pd.Series([None, np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15)],
                          dtype='object'),
                pd.Series([np.int16(0), np.int16(2 ** 15 - 1), None, np.int16(-2 ** 15 + 1), np.int16(-2 ** 15)],
                          dtype='object'),
                pd.Series([np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15), None],
                          dtype='object'),
                pd.Series([np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15)],
                          dtype=np.int16),
                pd.Series([np.int16(0), np.int16(2 ** 15 - 1), np.int16(-2 ** 15 + 1), np.int16(-2 ** 15)],
                          dtype=pd.Int16Dtype()),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorShort(
                v_short=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][0,],array(SHORT[]).append!([[00h,0h,32767h,-32767h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][1,],array(SHORT[]).append!([[0h,32767h,00h,-32767h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][2,],array(SHORT[]).append!([[0h,32767h,-32767h,00h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][3,],array(SHORT[]).append!([[00h,0h,32767h,-32767h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][4,],array(SHORT[]).append!([[0h,32767h,00h,-32767h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][5,],array(SHORT[]).append!([[0h,32767h,-32767h,00h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][6,],array(SHORT[]).append!([[0h,32767h,-32767h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][7,],array(SHORT[]).append!([[00h,0h,32767h,-32767h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][8,],array(SHORT[]).append!([[0h,32767h,00h,-32767h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][9,],array(SHORT[]).append!([[0h,32767h,-32767h,00h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][10,],array(SHORT[]).append!([[0h,32767h,-32767h,00h]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_short'][11,],array(SHORT[]).append!([[0h,32767h,-32767h,00h]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_short': [
                np.array([None, 0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                np.array([0, 2 ** 15 - 1, None, -2 ** 15 + 1, None], dtype='float64'),
                np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None, None], dtype='float64'),
                np.array([None, 0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                np.array([0, 2 ** 15 - 1, None, -2 ** 15 + 1, None], dtype='float64'),
                np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None, None], dtype='float64'),
                np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                np.array([None, 0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                np.array([0, 2 ** 15 - 1, None, -2 ** 15 + 1, None], dtype='float64'),
                np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None, None], dtype='float64'),
                np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(12)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorShort(v_short=np.array([None, 0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorShort(v_short=np.array([0, 2 ** 15 - 1, None, -2 ** 15 + 1, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorShort(v_short=np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorShort(v_short=np.array([None, 0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorShort(v_short=np.array([0, 2 ** 15 - 1, None, -2 ** 15 + 1, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorShort(v_short=np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorShort(v_short=np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorShort(v_short=np.array([None, 0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorShort(v_short=np.array([0, 2 ** 15 - 1, None, -2 ** 15 + 1, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorShort(v_short=np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorShort(v_short=np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
            EventVectorShort(v_short=np.array([0, 2 ** 15 - 1, -2 ** 15 + 1, None], dtype='float64'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.011', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_int(self):
        class EventVectorInt(Event):
            _event_name = "EventVectorInt"
            v_int: Vector[keys.DT_INT]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorInt):
                    if equalPlus(self.v_int, other.v_int) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_int`eventTime,[INT[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorInt{{
                v_int::INT VECTOR
                eventTime::TIMESTAMP
                def EventVectorInt(v_int_){{
                    v_int=v_int_
                    eventTime=now()
                }}
            }}
            class EventVectorIntMonitor{{
                def EventVectorIntMonitor(){{}}
                def updateEventVectorInt(event){{
                    insert into {func_name}_eventTest values([event.v_int],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorInt,'EventVectorInt',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT[], 0) as v_int)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT[], 0) as v_int) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT[], 0) as v_int) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_int,eventTime"
            fieldType="INT,TIMESTAMP"
            fieldTypeId=[[INT,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorInt", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_int")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorIntMonitor()>, dummy, [EventVectorInt], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorInt], eventTimeFields=["eventTime"],
                             commonFields=["v_int"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorInt], eventTimeFields=["eventTime"], commonFields=["v_int"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31)],
                [np.int32(0), np.int32(2 ** 31 - 1), None, np.int32(-2 ** 31 + 1), np.int32(-2 ** 31)],
                [np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31), None],
                np.array([None, np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31)],
                         dtype='object'),
                np.array([np.int32(0), np.int32(2 ** 31 - 1), None, np.int32(-2 ** 31 + 1), np.int32(-2 ** 31)],
                         dtype='object'),
                np.array([np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31), None],
                         dtype='object'),
                np.array([np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31)],
                         dtype=np.int32),
                pd.Series([None, np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31)],
                          dtype='object'),
                pd.Series([np.int32(0), np.int32(2 ** 31 - 1), None, np.int32(-2 ** 31 + 1), np.int32(-2 ** 31)],
                          dtype='object'),
                pd.Series([np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31), None],
                          dtype='object'),
                pd.Series([np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31)],
                          dtype=np.int32),
                pd.Series([np.int32(0), np.int32(2 ** 31 - 1), np.int32(-2 ** 31 + 1), np.int32(-2 ** 31)],
                          dtype=pd.Int32Dtype()),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorInt(
                v_int=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][0,],array(INT[]).append!([[00i,0i,2147483647i,-2147483647i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][1,],array(INT[]).append!([[0i,2147483647i,00i,-2147483647i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][2,],array(INT[]).append!([[0i,2147483647i,-2147483647i,00i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][3,],array(INT[]).append!([[00i,0i,2147483647i,-2147483647i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][4,],array(INT[]).append!([[0i,2147483647i,00i,-2147483647i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][5,],array(INT[]).append!([[0i,2147483647i,-2147483647i,00i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][6,],array(INT[]).append!([[0i,2147483647i,-2147483647i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][7,],array(INT[]).append!([[00i,0i,2147483647i,-2147483647i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][8,],array(INT[]).append!([[0i,2147483647i,00i,-2147483647i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][9,],array(INT[]).append!([[0i,2147483647i,-2147483647i,00i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][10,],array(INT[]).append!([[0i,2147483647i,-2147483647i,00i]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][11,],array(INT[]).append!([[0i,2147483647i,-2147483647i,00i]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_int': [
                np.array([None, 0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                np.array([0, 2 ** 31 - 1, None, -2 ** 31 + 1, None], dtype='float64'),
                np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None, None], dtype='float64'),
                np.array([None, 0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                np.array([0, 2 ** 31 - 1, None, -2 ** 31 + 1, None], dtype='float64'),
                np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None, None], dtype='float64'),
                np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                np.array([None, 0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                np.array([0, 2 ** 31 - 1, None, -2 ** 31 + 1, None], dtype='float64'),
                np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None, None], dtype='float64'),
                np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(12)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorInt(v_int=np.array([None, 0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorInt(v_int=np.array([0, 2 ** 31 - 1, None, -2 ** 31 + 1, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorInt(v_int=np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorInt(v_int=np.array([None, 0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorInt(v_int=np.array([0, 2 ** 31 - 1, None, -2 ** 31 + 1, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorInt(v_int=np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorInt(v_int=np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorInt(v_int=np.array([None, 0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorInt(v_int=np.array([0, 2 ** 31 - 1, None, -2 ** 31 + 1, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorInt(v_int=np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorInt(v_int=np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
            EventVectorInt(v_int=np.array([0, 2 ** 31 - 1, -2 ** 31 + 1, None], dtype='float64'),
                           eventTime=np.datetime64('2024-03-25T12:30:05.011', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_long(self):
        class EventVectorLong(Event):
            _event_name = "EventVectorLong"
            v_long: Vector[keys.DT_LONG]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorLong):
                    if equalPlus(self.v_long, other.v_long) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_long`eventTime,[LONG[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorLong{{
                v_long::LONG VECTOR
                eventTime::TIMESTAMP
                def EventVectorLong(v_long_){{
                    v_long=v_long_
                    eventTime=now()
                }}
            }}
            class EventVectorLongMonitor{{
                def EventVectorLongMonitor(){{}}
                def updateEventVectorLong(event){{
                    insert into {func_name}_eventTest values([event.v_long],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorLong,'EventVectorLong',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(LONG[], 0) as v_long)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(LONG[], 0) as v_long) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(LONG[], 0) as v_long) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_long,eventTime"
            fieldType="LONG,TIMESTAMP"
            fieldTypeId=[[LONG,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorLong", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_long")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorLongMonitor()>, dummy, [EventVectorLong], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorLong],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_long"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorLong], eventTimeFields=["eventTime"], commonFields=["v_long"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63)],
                [np.int64(0), np.int64(2 ** 63 - 1), None, np.int64(-2 ** 63 + 1), np.int64(-2 ** 63)],
                [np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63), None],
                np.array([None, np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63)],
                         dtype='object'),
                np.array([np.int64(0), np.int64(2 ** 63 - 1), None, np.int64(-2 ** 63 + 1), np.int64(-2 ** 63)],
                         dtype='object'),
                np.array([np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63), None],
                         dtype='object'),
                np.array([np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63)],
                         dtype='object'),
                pd.Series([None, np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63)],
                          dtype='object'),
                pd.Series([np.int64(0), np.int64(2 ** 63 - 1), None, np.int64(-2 ** 63 + 1), np.int64(-2 ** 63)],
                          dtype='object'),
                pd.Series([np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63), None],
                          dtype='object'),
                pd.Series([np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63)],
                          dtype=np.int64),
                pd.Series([np.int64(0), np.int64(2 ** 63 - 1), np.int64(-2 ** 63 + 1), np.int64(-2 ** 63)],
                          dtype=pd.Int64Dtype()),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorLong(
                v_long=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][0,],array(LONG[]).append!([[00l,0l,9223372036854775807l,-9223372036854775807l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][1,],array(LONG[]).append!([[0l,9223372036854775807l,00l,-9223372036854775807l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][2,],array(LONG[]).append!([[0l,9223372036854775807l,-9223372036854775807l,00l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][3,],array(LONG[]).append!([[00l,0l,9223372036854775807l,-9223372036854775807l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][4,],array(LONG[]).append!([[0l,9223372036854775807l,00l,-9223372036854775807l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][5,],array(LONG[]).append!([[0l,9223372036854775807l,-9223372036854775807l,00l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][6,],array(LONG[]).append!([[0l,9223372036854775807l,-9223372036854775807l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][7,],array(LONG[]).append!([[00l,0l,9223372036854775807l,-9223372036854775807l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][8,],array(LONG[]).append!([[0l,9223372036854775807l,00l,-9223372036854775807l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][9,],array(LONG[]).append!([[0l,9223372036854775807l,-9223372036854775807l,00l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][10,],array(LONG[]).append!([[0l,9223372036854775807l,-9223372036854775807l,00l]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_long'][10,],array(LONG[]).append!([[0l,9223372036854775807l,-9223372036854775807l,00l]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_long': [
                np.array([None, 0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                np.array([0, 2 ** 63 - 1, None, -2 ** 63 + 1, None], dtype='float64'),
                np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None, None], dtype='float64'),
                np.array([None, 0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                np.array([0, 2 ** 63 - 1, None, -2 ** 63 + 1, None], dtype='float64'),
                np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None, None], dtype='float64'),
                np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                np.array([None, 0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                np.array([0, 2 ** 63 - 1, None, -2 ** 63 + 1, None], dtype='float64'),
                np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None, None], dtype='float64'),
                np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(12)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorLong(v_long=np.array([None, 0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorLong(v_long=np.array([0, 2 ** 63 - 1, None, -2 ** 63 + 1, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorLong(v_long=np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorLong(v_long=np.array([None, 0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorLong(v_long=np.array([0, 2 ** 63 - 1, None, -2 ** 63 + 1, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorLong(v_long=np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorLong(v_long=np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorLong(v_long=np.array([None, 0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorLong(v_long=np.array([0, 2 ** 63 - 1, None, -2 ** 63 + 1, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorLong(v_long=np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorLong(v_long=np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
            EventVectorLong(v_long=np.array([0, 2 ** 63 - 1, -2 ** 63 + 1, None], dtype='float64'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.011', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_date(self):
        class EventVectorDate(Event):
            _event_name = "EventVectorDate"
            v_date: Vector[keys.DT_DATE]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorDate):
                    if equalPlus(self.v_date, other.v_date) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_date`eventTime,[DATE[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorDate{{
                v_date::DATE VECTOR
                eventTime::TIMESTAMP
                def EventVectorDate(v_date_){{
                    v_date=v_date_
                    eventTime=now()
                }}
            }}
            class EventVectorDateMonitor{{
                def EventVectorDateMonitor(){{}}
                def updateEventVectorDate(event){{
                    insert into {func_name}_eventTest values([event.v_date],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorDate,'EventVectorDate',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATE[], 0) as v_date)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATE[], 0) as v_date) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATE[], 0) as v_date) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_date,eventTime"
            fieldType="DATE,TIMESTAMP"
            fieldTypeId=[[DATE,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorDate", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_date")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorDateMonitor()>, dummy, [EventVectorDate], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorDate],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_date"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorDate], eventTimeFields=["eventTime"], commonFields=["v_date"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 'D'), pd.NaT],
                [np.datetime64(0, 'D'), None, pd.NaT],
                [np.datetime64(0, 'D'), pd.NaT, None],
                np.array([None, np.datetime64(0, 'D'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'D'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'D'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 'D'), None], dtype='datetime64[D]'),
                pd.Series([None, np.datetime64(0, 'D'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'D'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'D'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 'D'), pd.NaT, None], dtype='datetime64[ns]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorDate(
                v_date=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][0,],array(DATE[]).append!([[00d,1970.01.01d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][1,],array(DATE[]).append!([[1970.01.01d,00d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][2,],array(DATE[]).append!([[1970.01.01d,00d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][3,],array(DATE[]).append!([[00d,1970.01.01d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][4,],array(DATE[]).append!([[1970.01.01d,00d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][5,],array(DATE[]).append!([[1970.01.01d,00d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][6,],array(DATE[]).append!([[1970.01.01d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][7,],array(DATE[]).append!([[00d,1970.01.01d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][8,],array(DATE[]).append!([[1970.01.01d,00d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][9,],array(DATE[]).append!([[1970.01.01d,00d,00d]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_date'][10,],array(DATE[]).append!([[1970.01.01d,00d,00d]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_date': [
                np.array([None, np.datetime64(0, 'D'), None], dtype='datetime64[D]'),
                np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                np.array([None, np.datetime64(0, 'D'), None], dtype='datetime64[D]'),
                np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                np.array([np.datetime64(0, 'D'), None], dtype='datetime64[D]'),
                np.array([None, np.datetime64(0, 'D'), None], dtype='datetime64[D]'),
                np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorDate(v_date=np.array([None, np.datetime64(0, 'D'), None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorDate(v_date=np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorDate(v_date=np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorDate(v_date=np.array([None, np.datetime64(0, 'D'), None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorDate(v_date=np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorDate(v_date=np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorDate(v_date=np.array([np.datetime64(0, 'D'), None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorDate(v_date=np.array([None, np.datetime64(0, 'D'), None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorDate(v_date=np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorDate(v_date=np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorDate(v_date=np.array([np.datetime64(0, 'D'), None, None], dtype='datetime64[D]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_month(self):
        class EventVectorMonth(Event):
            _event_name = "EventVectorMonth"
            v_month: Vector[keys.DT_MONTH]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorMonth):
                    if equalPlus(self.v_month, other.v_month) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_month`eventTime,[MONTH[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorMonth{{
                v_month::MONTH VECTOR
                eventTime::TIMESTAMP
                def EventVectorMonth(v_month_){{
                    v_month=v_month_
                    eventTime=now()
                }}
            }}
            class EventVectorMonthMonitor{{
                def EventVectorMonthMonitor(){{}}
                def updateEventVectorMonth(event){{
                    insert into {func_name}_eventTest values([event.v_month],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorMonth,'EventVectorMonth',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MONTH[], 0) as v_month)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MONTH[], 0) as v_month) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MONTH[], 0) as v_month) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_month,eventTime"
            fieldType="MONTH,TIMESTAMP"
            fieldTypeId=[[MONTH,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorMonth", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_month")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorMonthMonitor()>, dummy, [EventVectorMonth], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorMonth],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_month"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorMonth], eventTimeFields=["eventTime"], commonFields=["v_month"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 'M'), pd.NaT],
                [np.datetime64(0, 'M'), None, pd.NaT],
                [np.datetime64(0, 'M'), pd.NaT, None],
                np.array([None, np.datetime64(0, 'M'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'M'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'M'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 'M'), None], dtype='datetime64[M]'),
                pd.Series([None, np.datetime64(0, 'M'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'M'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'M'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 'M'), pd.NaT, None], dtype='datetime64[ns]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorMonth(
                v_month=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][0,],array(MONTH[]).append!([[00M,1970.01M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][1,],array(MONTH[]).append!([[1970.01M,00M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][2,],array(MONTH[]).append!([[1970.01M,00M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][3,],array(MONTH[]).append!([[00M,1970.01M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][4,],array(MONTH[]).append!([[1970.01M,00M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][5,],array(MONTH[]).append!([[1970.01M,00M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][6,],array(MONTH[]).append!([[1970.01M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][7,],array(MONTH[]).append!([[00M,1970.01M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][8,],array(MONTH[]).append!([[1970.01M,00M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][9,],array(MONTH[]).append!([[1970.01M,00M,00M]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_month'][10,],array(MONTH[]).append!([[1970.01M,00M,00M]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_month': [
                np.array([None, np.datetime64(0, 'M'), None], dtype='datetime64[M]'),
                np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                np.array([None, np.datetime64(0, 'M'), None], dtype='datetime64[M]'),
                np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                np.array([np.datetime64(0, 'M'), None], dtype='datetime64[M]'),
                np.array([None, np.datetime64(0, 'M'), None], dtype='datetime64[M]'),
                np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorMonth(v_month=np.array([None, np.datetime64(0, 'M'), None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorMonth(v_month=np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorMonth(v_month=np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorMonth(v_month=np.array([None, np.datetime64(0, 'M'), None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorMonth(v_month=np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorMonth(v_month=np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorMonth(v_month=np.array([np.datetime64(0, 'M'), None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorMonth(v_month=np.array([None, np.datetime64(0, 'M'), None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorMonth(v_month=np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorMonth(v_month=np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorMonth(v_month=np.array([np.datetime64(0, 'M'), None, None], dtype='datetime64[M]'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_time(self):
        class EventVectorTime(Event):
            _event_name = "EventVectorTime"
            v_time: Vector[keys.DT_TIME]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorTime):
                    if equalPlus(self.v_time, other.v_time) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_time`eventTime,[TIME[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorTime{{
                v_time::TIME VECTOR
                eventTime::TIMESTAMP
                def EventVectorTime(v_time_){{
                    v_time=v_time_
                    eventTime=now()
                }}
            }}
            class EventVectorTimeMonitor{{
                def EventVectorTimeMonitor(){{}}
                def updateEventVectorTime(event){{
                    insert into {func_name}_eventTest values([event.v_time],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorTime,'EventVectorTime',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIME[], 0) as v_time)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIME[], 0) as v_time) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIME[], 0) as v_time) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_time,eventTime"
            fieldType="TIME,TIMESTAMP"
            fieldTypeId=[[TIME,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorTime", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_time")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorTimeMonitor()>, dummy, [EventVectorTime], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorTime],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_time"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorTime], eventTimeFields=["eventTime"], commonFields=["v_time"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 'ms'), pd.NaT],
                [np.datetime64(0, 'ms'), None, pd.NaT],
                [np.datetime64(0, 'ms'), pd.NaT, None],
                np.array([None, np.datetime64(0, 'ms'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'ms'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'ms'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                pd.Series([None, np.datetime64(0, 'ms'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'ms'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'ms'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 'ms'), pd.NaT, None], dtype='datetime64[ms]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorTime(
                v_time=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][0,],array(TIME[]).append!([[00t,00:00:00.000t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][1,],array(TIME[]).append!([[00:00:00.000t,00t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][2,],array(TIME[]).append!([[00:00:00.000t,00t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][3,],array(TIME[]).append!([[00t,00:00:00.000t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][4,],array(TIME[]).append!([[00:00:00.000t,00t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][5,],array(TIME[]).append!([[00:00:00.000t,00t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][6,],array(TIME[]).append!([[00:00:00.000t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][7,],array(TIME[]).append!([[00t,00:00:00.000t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][8,],array(TIME[]).append!([[00:00:00.000t,00t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][9,],array(TIME[]).append!([[00:00:00.000t,00t,00t]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_time'][10,],array(TIME[]).append!([[00:00:00.000t,00t,00t]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_time': [
                np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorTime(v_time=np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorTime(v_time=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorTime(v_time=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorTime(v_time=np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorTime(v_time=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorTime(v_time=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorTime(v_time=np.array([np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorTime(v_time=np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorTime(v_time=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorTime(v_time=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorTime(v_time=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                            eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_minute(self):
        class EventVectorMinute(Event):
            _event_name = "EventVectorMinute"
            v_minute: Vector[keys.DT_MINUTE]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorMinute):
                    if equalPlus(self.v_minute, other.v_minute) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_minute`eventTime,[MINUTE[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorMinute{{
                v_minute::MINUTE VECTOR
                eventTime::TIMESTAMP
                def EventVectorMinute(v_minute_){{
                    v_minute=v_minute_
                    eventTime=now()
                }}
            }}
            class EventVectorMinuteMonitor{{
                def EventVectorMinuteMonitor(){{}}
                def updateEventVectorMinute(event){{
                    insert into {func_name}_eventTest values([event.v_minute],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorMinute,'EventVectorMinute',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MINUTE[], 0) as v_minute)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MINUTE[], 0) as v_minute) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(MINUTE[], 0) as v_minute) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_minute,eventTime"
            fieldType="MINUTE,TIMESTAMP"
            fieldTypeId=[[MINUTE,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorMinute", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_minute")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorMinuteMonitor()>, dummy, [EventVectorMinute], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorMinute],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_minute"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorMinute], eventTimeFields=["eventTime"], commonFields=["v_minute"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 'm'), pd.NaT],
                [np.datetime64(0, 'm'), None, pd.NaT],
                [np.datetime64(0, 'm'), pd.NaT, None],
                np.array([None, np.datetime64(0, 'm'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'm'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'm'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 'm'), None], dtype='datetime64[m]'),
                pd.Series([None, np.datetime64(0, 'm'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'm'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'm'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 'm'), pd.NaT, None], dtype='datetime64[ns]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorMinute(
                v_minute=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][0,],array(MINUTE[]).append!([[00m,00:00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][1,],array(MINUTE[]).append!([[00:00m,00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][2,],array(MINUTE[]).append!([[00:00m,00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][3,],array(MINUTE[]).append!([[00m,00:00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][4,],array(MINUTE[]).append!([[00:00m,00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][5,],array(MINUTE[]).append!([[00:00m,00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][6,],array(MINUTE[]).append!([[00:00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][7,],array(MINUTE[]).append!([[00m,00:00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][8,],array(MINUTE[]).append!([[00:00m,00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][9,],array(MINUTE[]).append!([[00:00m,00m,00m]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_minute'][10,],array(MINUTE[]).append!([[00:00m,00m,00m]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_minute': [
                np.array([None, np.datetime64(0, 'm'), None], dtype='datetime64[m]'),
                np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                np.array([None, np.datetime64(0, 'm'), None], dtype='datetime64[m]'),
                np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                np.array([np.datetime64(0, 'm'), None], dtype='datetime64[m]'),
                np.array([None, np.datetime64(0, 'm'), None], dtype='datetime64[m]'),
                np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorMinute(v_minute=np.array([None, np.datetime64(0, 'm'), None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorMinute(v_minute=np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorMinute(v_minute=np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorMinute(v_minute=np.array([None, np.datetime64(0, 'm'), None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorMinute(v_minute=np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorMinute(v_minute=np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorMinute(v_minute=np.array([np.datetime64(0, 'm'), None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorMinute(v_minute=np.array([None, np.datetime64(0, 'm'), None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorMinute(v_minute=np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorMinute(v_minute=np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorMinute(v_minute=np.array([np.datetime64(0, 'm'), None, None], dtype='datetime64[m]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_second(self):
        class EventVectorSecond(Event):
            _event_name = "EventVectorSecond"
            v_second: Vector[keys.DT_SECOND]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorSecond):
                    if equalPlus(self.v_second, other.v_second) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_second`eventTime,[SECOND[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorSecond{{
                v_second::SECOND VECTOR
                eventTime::TIMESTAMP
                def EventVectorSecond(v_second_){{
                    v_second=v_second_
                    eventTime=now()
                }}
            }}
            class EventVectorSecondMonitor{{
                def EventVectorSecondMonitor(){{}}
                def updateEventVectorSecond(event){{
                    insert into {func_name}_eventTest values([event.v_second],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorSecond,'EventVectorSecond',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SECOND[], 0) as v_second)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SECOND[], 0) as v_second) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(SECOND[], 0) as v_second) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_second,eventTime"
            fieldType="SECOND,TIMESTAMP"
            fieldTypeId=[[SECOND,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorSecond", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_second")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorSecondMonitor()>, dummy, [EventVectorSecond], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorSecond],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_second"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorSecond], eventTimeFields=["eventTime"], commonFields=["v_second"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 's'), pd.NaT],
                [np.datetime64(0, 's'), None, pd.NaT],
                [np.datetime64(0, 's'), pd.NaT, None],
                np.array([None, np.datetime64(0, 's'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 's'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 's'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                pd.Series([None, np.datetime64(0, 's'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 's'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 's'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 's'), pd.NaT, None], dtype='datetime64[s]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorSecond(
                v_second=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][0,],array(SECOND[]).append!([[00s,00:00:00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][1,],array(SECOND[]).append!([[00:00:00s,00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][2,],array(SECOND[]).append!([[00:00:00s,00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][3,],array(SECOND[]).append!([[00s,00:00:00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][4,],array(SECOND[]).append!([[00:00:00s,00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][5,],array(SECOND[]).append!([[00:00:00s,00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][6,],array(SECOND[]).append!([[00:00:00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][7,],array(SECOND[]).append!([[00s,00:00:00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][8,],array(SECOND[]).append!([[00:00:00s,00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][9,],array(SECOND[]).append!([[00:00:00s,00s,00s]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_second'][10,],array(SECOND[]).append!([[00:00:00s,00s,00s]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_second': [
                np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorSecond(v_second=np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorSecond(v_second=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorSecond(v_second=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorSecond(v_second=np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorSecond(v_second=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorSecond(v_second=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorSecond(v_second=np.array([np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorSecond(v_second=np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorSecond(v_second=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorSecond(v_second=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorSecond(v_second=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_datetime(self):
        class EventVectorDatetime(Event):
            _event_name = "EventVectorDatetime"
            v_datetime: Vector[keys.DT_DATETIME]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorDatetime):
                    if equalPlus(self.v_datetime, other.v_datetime) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_datetime`eventTime,[DATETIME[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorDatetime{{
                v_datetime::DATETIME VECTOR
                eventTime::TIMESTAMP
                def EventVectorDatetime(v_datetime_){{
                    v_datetime=v_datetime_
                    eventTime=now()
                }}
            }}
            class EventVectorDatetimeMonitor{{
                def EventVectorDatetimeMonitor(){{}}
                def updateEventVectorDatetime(event){{
                    insert into {func_name}_eventTest values([event.v_datetime],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorDatetime,'EventVectorDatetime',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATETIME[], 0) as v_datetime)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATETIME[], 0) as v_datetime) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATETIME[], 0) as v_datetime) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_datetime,eventTime"
            fieldType="DATETIME,TIMESTAMP"
            fieldTypeId=[[DATETIME,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorDatetime", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_datetime")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorDatetimeMonitor()>, dummy, [EventVectorDatetime], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorDatetime],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_datetime"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorDatetime], eventTimeFields=["eventTime"], commonFields=["v_datetime"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 's'), pd.NaT],
                [np.datetime64(0, 's'), None, pd.NaT],
                [np.datetime64(0, 's'), pd.NaT, None],
                np.array([None, np.datetime64(0, 's'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 's'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 's'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                pd.Series([None, np.datetime64(0, 's'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 's'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 's'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 's'), pd.NaT, None], dtype='datetime64[s]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorDatetime(
                v_datetime=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][0,],array(DATETIME[]).append!([[00D,1970.01.01T00:00:00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][1,],array(DATETIME[]).append!([[1970.01.01T00:00:00D,00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][2,],array(DATETIME[]).append!([[1970.01.01T00:00:00D,00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][3,],array(DATETIME[]).append!([[00D,1970.01.01T00:00:00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][4,],array(DATETIME[]).append!([[1970.01.01T00:00:00D,00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][5,],array(DATETIME[]).append!([[1970.01.01T00:00:00D,00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][6,],array(DATETIME[]).append!([[1970.01.01T00:00:00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][7,],array(DATETIME[]).append!([[00D,1970.01.01T00:00:00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][8,],array(DATETIME[]).append!([[1970.01.01T00:00:00D,00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][9,],array(DATETIME[]).append!([[1970.01.01T00:00:00D,00D,00D]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datetime'][10,],array(DATETIME[]).append!([[1970.01.01T00:00:00D,00D,00D]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_datetime': [
                np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorDatetime(v_datetime=np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorDatetime(v_datetime=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorDatetime(v_datetime=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorDatetime(v_datetime=np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorDatetime(v_datetime=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorDatetime(v_datetime=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorDatetime(v_datetime=np.array([np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorDatetime(v_datetime=np.array([None, np.datetime64(0, 's'), None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorDatetime(v_datetime=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorDatetime(v_datetime=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorDatetime(v_datetime=np.array([np.datetime64(0, 's'), None, None], dtype='datetime64[s]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_timestamp(self):
        class EventVectorTimestamp(Event):
            _event_name = "EventVectorTimestamp"
            v_timestamp: Vector[keys.DT_TIMESTAMP]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorTimestamp):
                    if equalPlus(self.v_timestamp, other.v_timestamp) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_timestamp`eventTime,[TIMESTAMP[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorTimestamp{{
                v_timestamp::TIMESTAMP VECTOR
                eventTime::TIMESTAMP
                def EventVectorTimestamp(v_timestamp_){{
                    v_timestamp=v_timestamp_
                    eventTime=now()
                }}
            }}
            class EventVectorTimestampMonitor{{
                def EventVectorTimestampMonitor(){{}}
                def updateEventVectorTimestamp(event){{
                    insert into {func_name}_eventTest values([event.v_timestamp],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorTimestamp,'EventVectorTimestamp',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIMESTAMP[], 0) as v_timestamp)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIMESTAMP[], 0) as v_timestamp) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(TIMESTAMP[], 0) as v_timestamp) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_timestamp,eventTime"
            fieldType="TIMESTAMP,TIMESTAMP"
            fieldTypeId=[[TIMESTAMP,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorTimestamp", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_timestamp")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorTimestampMonitor()>, dummy, [EventVectorTimestamp], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorTimestamp],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_timestamp"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorTimestamp], eventTimeFields=["eventTime"], commonFields=["v_timestamp"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 'ms'), pd.NaT],
                [np.datetime64(0, 'ms'), None, pd.NaT],
                [np.datetime64(0, 'ms'), pd.NaT, None],
                np.array([None, np.datetime64(0, 'ms'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'ms'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'ms'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                pd.Series([None, np.datetime64(0, 'ms'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'ms'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'ms'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 'ms'), pd.NaT, None], dtype='datetime64[ms]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorTimestamp(
                v_timestamp=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][0,],array(TIMESTAMP[]).append!([[00T,1970.01.01T00:00:00.000T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][1,],array(TIMESTAMP[]).append!([[1970.01.01T00:00:00.000T,00T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][2,],array(TIMESTAMP[]).append!([[1970.01.01T00:00:00.000T,00T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][3,],array(TIMESTAMP[]).append!([[00T,1970.01.01T00:00:00.000T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][4,],array(TIMESTAMP[]).append!([[1970.01.01T00:00:00.000T,00T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][5,],array(TIMESTAMP[]).append!([[1970.01.01T00:00:00.000T,00T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][6,],array(TIMESTAMP[]).append!([[1970.01.01T00:00:00.000T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][7,],array(TIMESTAMP[]).append!([[00T,1970.01.01T00:00:00.000T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][8,],array(TIMESTAMP[]).append!([[1970.01.01T00:00:00.000T,00T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][9,],array(TIMESTAMP[]).append!([[1970.01.01T00:00:00.000T,00T,00T]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_timestamp'][10,],array(TIMESTAMP[]).append!([[1970.01.01T00:00:00.000T,00T,00T]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_timestamp': [
                np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorTimestamp(v_timestamp=np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([None, np.datetime64(0, 'ms'), None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorTimestamp(v_timestamp=np.array([np.datetime64(0, 'ms'), None, None], dtype='datetime64[ms]'),
                                 eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_nanotime(self):
        class EventVectorNanotime(Event):
            _event_name = "EventVectorNanotime"
            v_nanotime: Vector[keys.DT_NANOTIME]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorNanotime):
                    if equalPlus(self.v_nanotime, other.v_nanotime) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_nanotime`eventTime,[NANOTIME[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorNanotime{{
                v_nanotime::NANOTIME VECTOR
                eventTime::TIMESTAMP
                def EventVectorNanotime(v_nanotime_){{
                    v_nanotime=v_nanotime_
                    eventTime=now()
                }}
            }}
            class EventVectorNanotimeMonitor{{
                def EventVectorNanotimeMonitor(){{}}
                def updateEventVectorNanotime(event){{
                    insert into {func_name}_eventTest values([event.v_nanotime],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorNanotime,'EventVectorNanotime',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIME[], 0) as v_nanotime)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIME[], 0) as v_nanotime) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIME[], 0) as v_nanotime) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_nanotime,eventTime"
            fieldType="NANOTIME,TIMESTAMP"
            fieldTypeId=[[NANOTIME,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorNanotime", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_nanotime")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorNanotimeMonitor()>, dummy, [EventVectorNanotime], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorNanotime],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_nanotime"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorNanotime], eventTimeFields=["eventTime"], commonFields=["v_nanotime"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 'ns'), pd.NaT],
                [np.datetime64(0, 'ns'), None, pd.NaT],
                [np.datetime64(0, 'ns'), pd.NaT, None],
                np.array([None, np.datetime64(0, 'ns'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'ns'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'ns'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                pd.Series([None, np.datetime64(0, 'ns'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'ns'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'ns'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 'ns'), pd.NaT, None], dtype='datetime64[ns]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorNanotime(
                v_nanotime=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][0,],array(NANOTIME[]).append!([[00n,00:00:00.000000000n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][1,],array(NANOTIME[]).append!([[00:00:00.000000000n,00n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][2,],array(NANOTIME[]).append!([[00:00:00.000000000n,00n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][3,],array(NANOTIME[]).append!([[00n,00:00:00.000000000n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][4,],array(NANOTIME[]).append!([[00:00:00.000000000n,00n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][5,],array(NANOTIME[]).append!([[00:00:00.000000000n,00n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][6,],array(NANOTIME[]).append!([[00:00:00.000000000n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][7,],array(NANOTIME[]).append!([[00n,00:00:00.000000000n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][8,],array(NANOTIME[]).append!([[00:00:00.000000000n,00n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][9,],array(NANOTIME[]).append!([[00:00:00.000000000n,00n,00n]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotime'][10,],array(NANOTIME[]).append!([[00:00:00.000000000n,00n,00n]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_nanotime': [
                np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorNanotime(v_nanotime=np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorNanotime(v_nanotime=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_nanotimestamp(self):
        class EventVectorNanotimestamp(Event):
            _event_name = "EventVectorNanotimestamp"
            v_nanotimestamp: Vector[keys.DT_NANOTIMESTAMP]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorNanotimestamp):
                    if equalPlus(self.v_nanotimestamp, other.v_nanotimestamp) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}

            share table(100:0,`v_nanotimestamp`eventTime,[NANOTIMESTAMP[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorNanotimestamp{{
                v_nanotimestamp::NANOTIMESTAMP VECTOR
                eventTime::TIMESTAMP
                def EventVectorNanotimestamp(v_nanotimestamp_){{
                    v_nanotimestamp=v_nanotimestamp_
                    eventTime=now()
                }}
            }}
            class EventVectorNanotimestampMonitor{{
                def EventVectorNanotimestampMonitor(){{}}
                def updateEventVectorNanotimestamp(event){{
                    insert into {func_name}_eventTest values([event.v_nanotimestamp],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorNanotimestamp,'EventVectorNanotimestamp',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIMESTAMP[], 0) as v_nanotimestamp)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIMESTAMP[], 0) as v_nanotimestamp) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(NANOTIMESTAMP[], 0) as v_nanotimestamp) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_nanotimestamp,eventTime"
            fieldType="NANOTIMESTAMP,TIMESTAMP"
            fieldTypeId=[[NANOTIMESTAMP,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorNanotimestamp", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_nanotimestamp")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorNanotimestampMonitor()>, dummy, [EventVectorNanotimestamp], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorNanotimestamp],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_nanotimestamp"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorNanotimestamp], eventTimeFields=["eventTime"],
                             commonFields=["v_nanotimestamp"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 'ns'), pd.NaT],
                [np.datetime64(0, 'ns'), None, pd.NaT],
                [np.datetime64(0, 'ns'), pd.NaT, None],
                np.array([None, np.datetime64(0, 'ns'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'ns'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'ns'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                pd.Series([None, np.datetime64(0, 'ns'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'ns'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'ns'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 'ns'), pd.NaT, None], dtype='datetime64[ns]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorNanotimestamp(
                v_nanotimestamp=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][0,],array(NANOTIMESTAMP[]).append!([[00N,1970.01.01T00:00:00.000000000N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][1,],array(NANOTIMESTAMP[]).append!([[1970.01.01T00:00:00.000000000N,00N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][2,],array(NANOTIMESTAMP[]).append!([[1970.01.01T00:00:00.000000000N,00N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][3,],array(NANOTIMESTAMP[]).append!([[00N,1970.01.01T00:00:00.000000000N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][4,],array(NANOTIMESTAMP[]).append!([[1970.01.01T00:00:00.000000000N,00N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][5,],array(NANOTIMESTAMP[]).append!([[1970.01.01T00:00:00.000000000N,00N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][6,],array(NANOTIMESTAMP[]).append!([[1970.01.01T00:00:00.000000000N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][7,],array(NANOTIMESTAMP[]).append!([[00N,1970.01.01T00:00:00.000000000N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][8,],array(NANOTIMESTAMP[]).append!([[1970.01.01T00:00:00.000000000N,00N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][9,],array(NANOTIMESTAMP[]).append!([[1970.01.01T00:00:00.000000000N,00N,00N]]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_nanotimestamp'][10,],array(NANOTIMESTAMP[]).append!([[1970.01.01T00:00:00.000000000N,00N,00N]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_nanotimestamp': [
                np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([None, np.datetime64(0, 'ns'), None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorNanotimestamp(
                v_nanotimestamp=np.array([np.datetime64(0, 'ns'), None, None], dtype='datetime64[ns]'),
                eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_datehour(self):
        class EventVectorDatehour(Event):
            _event_name = "EventVectorDatehour"
            v_datehour: Vector[keys.DT_DATEHOUR]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorDatehour):
                    if equalPlus(self.v_datehour, other.v_datehour) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_datehour`eventTime,[DATEHOUR[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorDatehour{{
                v_datehour::DATEHOUR VECTOR
                eventTime::TIMESTAMP
                def EventVectorDatehour(v_datehour_){{
                    v_datehour=v_datehour_
                    eventTime=now()
                }}
            }}
            class EventVectorDatehourMonitor{{
                def EventVectorDatehourMonitor(){{}}
                def updateEventVectorDatehour(event){{
                    insert into {func_name}_eventTest values([event.v_datehour],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorDatehour,'EventVectorDatehour',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATEHOUR[], 0) as v_datehour)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATEHOUR[], 0) as v_datehour) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DATEHOUR[], 0) as v_datehour) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_datehour,eventTime"
            fieldType="DATEHOUR,TIMESTAMP"
            fieldTypeId=[[DATEHOUR,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorDatehour", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_datehour")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorDatehourMonitor()>, dummy, [EventVectorDatehour], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorDatehour],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_datehour"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorDatehour], eventTimeFields=["eventTime"], commonFields=["v_datehour"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, np.datetime64(0, 'h'), pd.NaT],
                [np.datetime64(0, 'h'), None, pd.NaT],
                [np.datetime64(0, 'h'), pd.NaT, None],
                np.array([None, np.datetime64(0, 'h'), pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'h'), None, pd.NaT], dtype='object'),
                np.array([np.datetime64(0, 'h'), pd.NaT, None], dtype='object'),
                np.array([np.datetime64(0, 'h'), None], dtype='datetime64[h]'),
                pd.Series([None, np.datetime64(0, 'h'), pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'h'), None, pd.NaT], dtype='object'),
                pd.Series([np.datetime64(0, 'h'), pd.NaT, None], dtype='object'),
                pd.Series([np.datetime64(0, 'h'), pd.NaT, None], dtype='datetime64[ns]'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorDatehour(
                v_datehour=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][0,],array(DATEHOUR[]).append!([datehour([null,'1970.01.01T00',null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][1,],array(DATEHOUR[]).append!([datehour(['1970.01.01T00',null,null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][2,],array(DATEHOUR[]).append!([datehour(['1970.01.01T00',null,null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][3,],array(DATEHOUR[]).append!([datehour([null,'1970.01.01T00',null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][4,],array(DATEHOUR[]).append!([datehour(['1970.01.01T00',null,null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][5,],array(DATEHOUR[]).append!([datehour(['1970.01.01T00',null,null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][6,],array(DATEHOUR[]).append!([datehour(['1970.01.01T00',null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][7,],array(DATEHOUR[]).append!([datehour([null,'1970.01.01T00',null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][8,],array(DATEHOUR[]).append!([datehour(['1970.01.01T00',null,null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][9,],array(DATEHOUR[]).append!([datehour(['1970.01.01T00',null,null])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_datehour'][10,],array(DATEHOUR[]).append!([datehour(['1970.01.01T00',null,null])]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_datehour': [
                np.array([None, np.datetime64(0, 'h'), None], dtype='datetime64[h]'),
                np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                np.array([None, np.datetime64(0, 'h'), None], dtype='datetime64[h]'),
                np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                np.array([np.datetime64(0, 'h'), None], dtype='datetime64[h]'),
                np.array([None, np.datetime64(0, 'h'), None], dtype='datetime64[h]'),
                np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorDatehour(v_datehour=np.array([None, np.datetime64(0, 'h'), None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorDatehour(v_datehour=np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorDatehour(v_datehour=np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorDatehour(v_datehour=np.array([None, np.datetime64(0, 'h'), None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorDatehour(v_datehour=np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorDatehour(v_datehour=np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorDatehour(v_datehour=np.array([np.datetime64(0, 'h'), None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorDatehour(v_datehour=np.array([None, np.datetime64(0, 'h'), None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorDatehour(v_datehour=np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorDatehour(v_datehour=np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorDatehour(v_datehour=np.array([np.datetime64(0, 'h'), None, None], dtype='datetime64[h]'),
                                eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_float(self):
        class EventVectorFloat(Event):
            _event_name = "EventVectorFloat"
            v_float: Vector[keys.DT_FLOAT]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorFloat):
                    if equalPlus(self.v_float, other.v_float) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_float`eventTime,[FLOAT[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorFloat{{
                v_float::FLOAT VECTOR
                eventTime::TIMESTAMP
                def EventVectorFloat(v_float_){{
                    v_float=v_float_
                    eventTime=now()
                }}
            }}
            class EventVectorFloatMonitor{{
                def EventVectorFloatMonitor(){{}}
                def updateEventVectorFloat(event){{
                    insert into {func_name}_eventTest values([event.v_float],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorFloat,'EventVectorFloat',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(FLOAT[], 0) as v_float)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(FLOAT[], 0) as v_float) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(FLOAT[], 0) as v_float) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_float,eventTime"
            fieldType="FLOAT,TIMESTAMP"
            fieldTypeId=[[FLOAT,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorFloat", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_float")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorFloatMonitor()>, dummy, [EventVectorFloat], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorFloat],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_float"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorFloat], eventTimeFields=["eventTime"], commonFields=["v_float"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        data = [
            [None, 3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), np.float32(-3.4028235e+38),
             np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]],
            [3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None, np.float32(-3.4028235e+38),
             np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]],
            [3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), np.float32(-3.4028235e+38),
             np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
            np.array([None, 3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), np.float32(-3.4028235e+38),
                      np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='object'),
            np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None, np.float32(-3.4028235e+38),
                      np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='object'),
            np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), np.float32(-3.4028235e+38),
                      np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='object'),
            np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), np.float32(-3.4028235e+38),
                      np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype=np.float32),
            pd.Series([None, 3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), np.float32(-3.4028235e+38),
                       np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='object'),
            pd.Series([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None, np.float32(-3.4028235e+38),
                       np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='object'),
            pd.Series([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), np.float32(-3.4028235e+38),
                       np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='object'),
            pd.Series([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), np.float32(-3.4028235e+38),
                       np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype=np.float32),
        ]
        if PANDAS_VERSION >= (1, 2, 0):
            data.append(
                pd.Series([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), np.float32(-3.4028235e+38),
                           np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype=pd.Float32Dtype()),
            )
        for index, data in enumerate(data):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorFloat(
                v_float=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        # todo
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][0,],array(FLOAT[]).append!([[00f,]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][1,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][2,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][3,],array(FLOAT[]).append!([[00d,1970.01.01d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][4,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][5,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][6,],array(FLOAT[]).append!([[1970.01.01d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][7,],array(FLOAT[]).append!([[00d,1970.01.01d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][8,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][9,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][10,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_float': [
                np.array([None, 3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None, None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float32'),
                np.array([None, 3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None, None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float32'),
                np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float32'),
                np.array([None, 3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None, None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float32'),
                np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float32'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        if PANDAS_VERSION >= (1, 2, 0):
            df_expect.loc[11] = [
                np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float32'),
                np.datetime64('2024-03-25T12:30:05.011', "ms")
            ]
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorFloat(v_float=np.array([None, 3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorFloat(v_float=np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None, None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorFloat(v_float=np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                              dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorFloat(v_float=np.array([None, 3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorFloat(v_float=np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None, None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorFloat(v_float=np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                              dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorFloat(v_float=np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                              dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorFloat(v_float=np.array([None, 3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorFloat(v_float=np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None, None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorFloat(v_float=np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                              dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorFloat(v_float=np.array([3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                                               np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                              dtype='float32'),
                             eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        if PANDAS_VERSION >= (1, 2, 0):
            expect.append(EventVectorFloat(v_float=np.array(
                [3.14, float('NaN'), np.float32(0), np.float32(3.4028235e+38), None,
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float32'),
                eventTime=np.datetime64('2024-03-25T12:30:05.011', 'ms')))
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_double(self):
        class EventVectorDouble(Event):
            _event_name = "EventVectorDouble"
            v_double: Vector[keys.DT_DOUBLE]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorDouble):
                    if equalPlus(self.v_double, other.v_double) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_double`eventTime,[DOUBLE[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorDouble{{
                v_double::DOUBLE VECTOR
                eventTime::TIMESTAMP
                def EventVectorDouble(v_double_){{
                    v_double=v_double_
                    eventTime=now()
                }}
            }}
            class EventVectorDoubleMonitor{{
                def EventVectorDoubleMonitor(){{}}
                def updateEventVectorDouble(event){{
                    insert into {func_name}_eventTest values([event.v_double],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorDouble,'EventVectorDouble',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DOUBLE[], 0) as v_double)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DOUBLE[], 0) as v_double) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DOUBLE[], 0) as v_double) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_double,eventTime"
            fieldType="DOUBLE,TIMESTAMP"
            fieldTypeId=[[DOUBLE,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorDouble", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_double")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorDoubleMonitor()>, dummy, [EventVectorDouble], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorDouble],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_double"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorDouble], eventTimeFields=["eventTime"], commonFields=["v_double"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        data = [
            [None, 3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308),
             np.float64(-1.7976931348623157e+308), np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]],
            [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
             np.float64(-1.7976931348623157e+308), np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]],
            [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), np.float64(-1.7976931348623157e+308),
             np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
            np.array([None, 3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308),
                      np.float64(-1.7976931348623157e+308), np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]],
                     dtype='object'),
            np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                      np.float64(-1.7976931348623157e+308), np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]],
                     dtype='object'),
            np.array(
                [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), np.float64(-1.7976931348623157e+308),
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='object'),
            np.array(
                [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), np.float64(-1.7976931348623157e+308),
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype=np.float64),
            pd.Series([None, 3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308),
                       np.float64(-1.7976931348623157e+308), np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]],
                      dtype='object'),
            pd.Series([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                       np.float64(-1.7976931348623157e+308), np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]],
                      dtype='object'),
            pd.Series(
                [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), np.float64(-1.7976931348623157e+308),
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='object'),
            pd.Series(
                [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), np.float64(-1.7976931348623157e+308),
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype=np.float64),
        ]
        if PANDAS_VERSION >= (1, 2, 0):
            data.append(pd.Series(
                [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), np.float64(-1.7976931348623157e+308),
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype=pd.Float64Dtype()), )
        for index, data in enumerate(data):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorDouble(
                v_double=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        # todo
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][0,],array(FLOAT[]).append!([[00f,]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][1,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][2,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][3,],array(FLOAT[]).append!([[00d,1970.01.01d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][4,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][5,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][6,],array(FLOAT[]).append!([[1970.01.01d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][7,],array(FLOAT[]).append!([[00d,1970.01.01d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][8,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][9,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # assert self.__class__.conn.run(f"eqObj({func_name}_output['v_float'][10,],array(FLOAT[]).append!([[1970.01.01d,00d,00d]]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_double': [
                np.array([None, 3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None, None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float64'),
                np.array([None, 3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None, None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float64'),
                np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float64'),
                np.array([None, 3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None, None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float64'),
                np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float64'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(11)], dtype='datetime64[ns]')
        })
        if PANDAS_VERSION >= (1, 2, 0):
            df_expect.loc[11] = [
                np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                          np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float64'),
                np.datetime64('2024-03-25T12:30:05.011', "ms")
            ]
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorDouble(v_double=np.array(
                [None, 3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorDouble(v_double=np.array(
                [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None, None,
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorDouble(v_double=np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                                                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                                dtype='float64'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorDouble(v_double=np.array(
                [None, 3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorDouble(v_double=np.array(
                [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None, None,
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorDouble(v_double=np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                                                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                                dtype='float64'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorDouble(v_double=np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                                                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                                dtype='float64'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorDouble(v_double=np.array(
                [None, 3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorDouble(v_double=np.array(
                [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None, None,
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0]], dtype='float64'),
                eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorDouble(v_double=np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                                                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                                dtype='float64'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorDouble(v_double=np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                                                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None],
                                                dtype='float64'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        if PANDAS_VERSION >= (1, 2, 0):
            expect.append(EventVectorDouble(v_double=np.array(
                [3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,
                 np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0], None], dtype='float64'),
                eventTime=np.datetime64('2024-03-25T12:30:05.011', 'ms')))
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_string(self):
        class EventVectorString(Event):
            _event_name = "EventVectorString"
            v_string: Vector[keys.DT_STRING]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorString):
                    if equalPlus(self.v_string, other.v_string) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            //share table(100:0,`v_string`eventTime,[STRING[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorString{{
                v_string::STRING VECTOR
                eventTime::TIMESTAMP
                def EventVectorString(v_string_){{
                    v_string=v_string_
                    eventTime=now()
                }}
            }}
            class EventVectorStringMonitor{{
                def EventVectorStringMonitor(){{}}
                def updateEventVectorString(event){{
                    //insert into {func_name}_eventTest values([event.v_string],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorString,'EventVectorString',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_string,eventTime"
            fieldType="STRING,TIMESTAMP"
            fieldTypeId=[[STRING,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorString", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorStringMonitor()>, dummy, [EventVectorString], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorString],
                             eventTimeFields=["eventTime"],
                             commonFields=[])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorString], eventTimeFields=["eventTime"], commonFields=[])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        data = [
            [None, "abc!@#中文 123", np.str_("abc!@#中文 123"), ""],
            ["abc!@#中文 123", None, np.str_("abc!@#中文 123"), ""],
            ["abc!@#中文 123", np.str_("abc!@#中文 123"), "", None],
            np.array([None, "abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype='object'),
            np.array(["abc!@#中文 123", None, np.str_("abc!@#中文 123"), ""], dtype='object'),
            np.array(["abc!@#中文 123", np.str_("abc!@#中文 123"), "", None], dtype='object'),
            np.array(["abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype=np.str_),
            pd.Series([None, "abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype='object'),
            pd.Series(["abc!@#中文 123", None, np.str_("abc!@#中文 123"), ""], dtype='object'),
            pd.Series(["abc!@#中文 123", np.str_("abc!@#中文 123"), "", None], dtype='object'),
            pd.Series(["abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype=np.str_),
            pd.Series(["abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype=pd.StringDtype()),
        ]
        if find_spec("pyarrow") is not None:
            from basic_testing.prepare import PYARROW_VERSION
            if PYARROW_VERSION >= (10, 0, 1):
                if PANDAS_VERSION >= (1, 3, 0):
                    data.append(
                        pd.Series(["abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype=pd.StringDtype("pyarrow")))
                    data.append(
                        pd.Series(["abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype=pd.StringDtype("python")))
                if PANDAS_VERSION >= (2, 1, 0):
                    data.append(
                        pd.Series(["abc!@#中文 123", np.str_("abc!@#中文 123"), ""],
                                  dtype=pd.StringDtype("pyarrow_numpy")))
        for index, data in enumerate(data):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorString(
                v_string=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # df_expect=pd.DataFrame({
        #     'v_string':[np.array(["abc!@#中文 123",np.str_("abc!@#中文 123"),"",None],dtype='object')],
        #     'eventTime':np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(1)],dtype='datetime64[ns]')
        # })
        # df=self.__class__.conn.run(f"{func_name}_eventTest")
        # assert equalPlus(df,df_expect)
        expect = [
            EventVectorString(v_string=np.array(["", "abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorString(v_string=np.array(["abc!@#中文 123", "", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorString(v_string=np.array(["abc!@#中文 123", "abc!@#中文 123", "", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorString(v_string=np.array(["", "abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorString(v_string=np.array(["abc!@#中文 123", "", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorString(v_string=np.array(["abc!@#中文 123", "abc!@#中文 123", "", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorString(v_string=np.array(["abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorString(v_string=np.array(["", "abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorString(v_string=np.array(["abc!@#中文 123", "", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorString(v_string=np.array(["abc!@#中文 123", "abc!@#中文 123", "", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorString(v_string=np.array(["abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
            EventVectorString(v_string=np.array(["abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.011', 'ms')),
        ]
        if find_spec("pyarrow") is not None:
            from basic_testing.prepare import PYARROW_VERSION
            if PYARROW_VERSION >= (10, 0, 1):
                if PANDAS_VERSION >= (1, 3, 0):
                    expect.append(
                        EventVectorString(v_string=np.array(["abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                                          eventTime=np.datetime64('2024-03-25T12:30:05.012', 'ms')))
                    expect.append(
                        EventVectorString(v_string=np.array(["abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                                          eventTime=np.datetime64('2024-03-25T12:30:05.013', 'ms')))
                if PANDAS_VERSION >= (2, 1, 0):
                    expect.append(
                        EventVectorString(v_string=np.array(["abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                                          eventTime=np.datetime64('2024-03-25T12:30:05.014', 'ms')))
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_blob(self):
        class EventVectorBlob(Event):
            _event_name = "EventVectorBlob"
            v_blob: Vector[keys.DT_BLOB]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorBlob):
                    if equalPlus(self.v_blob, other.v_blob) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_blob`eventTime,[BLOB,TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorBlob{{
                v_blob::BLOB VECTOR
                eventTime::TIMESTAMP
                def EventVectorBlob(v_blob_){{
                    v_blob=v_blob_
                    eventTime=now()
                }}
            }}
            class EventVectorBlobMonitor{{
                def EventVectorBlobMonitor(){{}}
                def updateEventVectorBlob(event){{
                    insert into {func_name}_eventTest values(event.v_blob[0],event.eventTime)
                    insert into {func_name}_eventTest values(event.v_blob[1],event.eventTime)
                    insert into {func_name}_eventTest values(event.v_blob[2],event.eventTime)
                    insert into {func_name}_eventTest values(event.v_blob[3],event.eventTime)
                    insert into {func_name}_eventTest values(event.v_blob[4],event.eventTime)
                    insert into {func_name}_eventTest values(event.v_blob[5],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorBlob,'EventVectorBlob',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_blob,eventTime"
            fieldType="BLOB,TIMESTAMP"
            fieldTypeId=[[BLOB,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorBlob", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorBlobMonitor()>, dummy, [EventVectorBlob], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorBlob],
                             eventTimeFields=["eventTime"],
                             commonFields=[])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorBlob], eventTimeFields=["eventTime"], commonFields=[])
        client.subscribe(HOST, PORT, handler, f"{func_name}_input", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, "abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), np.bytes_("abc!@#中文 123".encode()),
                 np.bytes_("abc!@#中文 123".encode('gbk')), b""],
                ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), None, np.bytes_("abc!@#中文 123".encode()),
                 np.bytes_("abc!@#中文 123".encode('gbk')), b""],
                ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), np.bytes_("abc!@#中文 123".encode()),
                 np.bytes_("abc!@#中文 123".encode('gbk')), b"", None],
                np.array([None, "abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'),
                          np.bytes_("abc!@#中文 123".encode()), np.bytes_("abc!@#中文 123".encode('gbk')), b""],
                         dtype='object'),
                np.array(["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), None,
                          np.bytes_("abc!@#中文 123".encode()), np.bytes_("abc!@#中文 123".encode('gbk')), b""],
                         dtype='object'),
                np.array(
                    ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), np.bytes_("abc!@#中文 123".encode()),
                     np.bytes_("abc!@#中文 123".encode('gbk')), b"", None], dtype='object'),
                pd.Series([None, "abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'),
                           np.bytes_("abc!@#中文 123".encode()), np.bytes_("abc!@#中文 123".encode('gbk')), b""],
                          dtype='object'),
                pd.Series(["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), None,
                           np.bytes_("abc!@#中文 123".encode()), np.bytes_("abc!@#中文 123".encode('gbk')), b""],
                          dtype='object'),
                pd.Series(
                    ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), np.bytes_("abc!@#中文 123".encode()),
                     np.bytes_("abc!@#中文 123".encode('gbk')), b"", None], dtype='object'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorBlob(
                v_blob=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            print(event)
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_input", "ttt")
        # df_expect=pd.DataFrame({
        #     'v_blob':[np.array([3.14, np.nan, np.float64(0), np.float64(1.7976931348623157e+308), None,np.frombuffer(b'\xff\xff\xff\xff\xff\xff\xff\x7f')[0],None],dtype='float64')],
        #     'eventTime':np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(1)],dtype='datetime64[ns]')
        # })
        # df=self.__class__.conn.run("eventTest")
        # assert equalPlus(df,df_expect)
        expect = [
            EventVectorBlob(v_blob=np.array(
                [b"", "abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), "abc!@#中文 123".encode(),
                 "abc!@#中文 123".encode('gbk'), b""],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorBlob(v_blob=np.array(
                ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), b"", "abc!@#中文 123".encode(),
                 "abc!@#中文 123".encode('gbk'), b""],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorBlob(v_blob=np.array(
                ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), "abc!@#中文 123".encode(),
                 "abc!@#中文 123".encode('gbk'), b"", b""],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorBlob(v_blob=np.array(
                [b"", "abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), "abc!@#中文 123".encode(),
                 "abc!@#中文 123".encode('gbk'), b""],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorBlob(v_blob=np.array(
                ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), b"", "abc!@#中文 123".encode(),
                 "abc!@#中文 123".encode('gbk'), b""],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorBlob(v_blob=np.array(
                ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), "abc!@#中文 123".encode(),
                 "abc!@#中文 123".encode('gbk'), b"", b""],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorBlob(v_blob=np.array(
                [b"", "abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), "abc!@#中文 123".encode(),
                 "abc!@#中文 123".encode('gbk'), b""],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorBlob(v_blob=np.array(
                ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), b"", "abc!@#中文 123".encode(),
                 "abc!@#中文 123".encode('gbk'), b""],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorBlob(v_blob=np.array(
                ["abc!@#中文 123".encode(), "abc!@#中文 123".encode('gbk'), "abc!@#中文 123".encode(),
                 "abc!@#中文 123".encode('gbk'), b"", b""],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_symbol(self):
        class EventVectorSymbol(Event):
            _event_name = "EventVectorSymbol"
            v_symbol: Vector[keys.DT_SYMBOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorSymbol):
                    if equalPlus(self.v_symbol, other.v_symbol) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,[`v_symbol],[STRING]) as `{func_name}_eventTest
            class EventVectorSymbol{{
                v_symbol::SYMBOL VECTOR
                eventTime::TIMESTAMP
                def EventVectorSymbol(v_symbol_){{
                    v_symbol=v_symbol_
                    eventTime=now()
                }}
            }}
            class EventVectorSymbolMonitor{{
                def EventVectorSymbolMonitor(){{}}
                def updateEventVectorSymbol(event){{
                    insert into {func_name}_eventTest values(typestr(event.v_symbol))
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorSymbol,'EventVectorSymbol',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_symbol,eventTime"
            fieldType="SYMBOL,TIMESTAMP"
            fieldTypeId=[[SYMBOL,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorSymbol", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorSymbolMonitor()>, dummy, [EventVectorSymbol], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorSymbol],
                             eventTimeFields=["eventTime"],
                             commonFields=[])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorSymbol], eventTimeFields=["eventTime"], commonFields=[])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, "abc!@#中文 123", np.str_("abc!@#中文 123"), ""],
                ["abc!@#中文 123", None, np.str_("abc!@#中文 123"), ""],
                ["abc!@#中文 123", np.str_("abc!@#中文 123"), "", None],
                np.array([None, "abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype='object'),
                np.array(["abc!@#中文 123", None, np.str_("abc!@#中文 123"), ""], dtype='object'),
                np.array(["abc!@#中文 123", np.str_("abc!@#中文 123"), "", None], dtype='object'),
                np.array(["abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype=np.str_),
                pd.Series([None, "abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype='object'),
                pd.Series(["abc!@#中文 123", None, np.str_("abc!@#中文 123"), ""], dtype='object'),
                pd.Series(["abc!@#中文 123", np.str_("abc!@#中文 123"), "", None], dtype='object'),
                pd.Series(["abc!@#中文 123", np.str_("abc!@#中文 123"), ""], dtype=np.str_),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorSymbol(
                v_symbol=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        # assert self.__class__.conn.run("eqObj(output['v_float'][0,],array(FLOAT[]).append!([[00f,]]))")
        # check server deserialize
        # df_expect = pd.DataFrame({
        #     'v_symbol': [
        #         np.array(["a","b"], dtype='object'),
        #     ],
        #     'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(1)], dtype='datetime64[ns]')
        # })
        # df = self.__class__.conn.run("eventTest")
        # assert equalPlus(df, df_expect)
        expect = [
            EventVectorSymbol(v_symbol=np.array(["", "abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["abc!@#中文 123", "", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["abc!@#中文 123", "abc!@#中文 123", "", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["", "abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["abc!@#中文 123", "", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["abc!@#中文 123", "abc!@#中文 123", "", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["", "abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["abc!@#中文 123", "", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["abc!@#中文 123", "abc!@#中文 123", "", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.009', 'ms')),
            EventVectorSymbol(v_symbol=np.array(["abc!@#中文 123", "abc!@#中文 123", ""], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.010', 'ms')),
        ]
        # assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_int128(self):
        class EventVectorInt128(Event):
            _event_name = "EventVectorInt128"
            v_int128: Vector[keys.DT_INT128]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorInt128):
                    if equalPlus(self.v_int128, other.v_int128) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_int128`eventTime,[INT128[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorInt128{{
                v_int128::INT128 VECTOR
                eventTime::TIMESTAMP
                def EventVectorInt128(v_int128_){{
                    v_int128=v_int128_
                    eventTime=now()
                }}
            }}
            class EventVectorInt128Monitor{{
                def EventVectorInt128Monitor(){{}}
                def updateEventVectorInt128(event){{
                    insert into {func_name}_eventTest values([event.v_int128],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorInt128,'EventVectorInt128',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT128[], 0) as v_int128)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT128[], 0) as v_int128) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT128[], 0) as v_int128) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_int128,eventTime"
            fieldType="INT128,TIMESTAMP"
            fieldTypeId=[[INT128,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorInt128", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_int128")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorInt128Monitor()>, dummy, [EventVectorInt128], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorInt128],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_int128"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorInt128], eventTimeFields=["eventTime"], commonFields=["v_int128"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, 'e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000'],
                ['e1671797c52e15f763380b45e841ec32', None, '00000000000000000000000000000000'],
                ['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', None],
                np.array([None, 'e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000'],
                         dtype='object'),
                np.array(['e1671797c52e15f763380b45e841ec32', None, '00000000000000000000000000000000'],
                         dtype='object'),
                np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', None],
                         dtype='object'),
                pd.Series([None, 'e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000'],
                          dtype='object'),
                pd.Series(['e1671797c52e15f763380b45e841ec32', None, '00000000000000000000000000000000'],
                          dtype='object'),
                pd.Series(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', None],
                          dtype='object'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorInt128(
                v_int128=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int128'][0,],array(INT128[]).append!([int128(['00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int128'][1,],array(INT128[]).append!([int128(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', '00000000000000000000000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int128'][2,],array(INT128[]).append!([int128(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', '00000000000000000000000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int128'][3,],array(INT128[]).append!([int128(['00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int128'][4,],array(INT128[]).append!([int128(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', '00000000000000000000000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int128'][5,],array(INT128[]).append!([int128(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', '00000000000000000000000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int128'][6,],array(INT128[]).append!([int128(['00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int128'][7,],array(INT128[]).append!([int128(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', '00000000000000000000000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int128'][8,],array(INT128[]).append!([int128(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000', '00000000000000000000000000000000'])]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_int128': [
                np.array(['00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec32',
                          '00000000000000000000000000000000'], dtype='object'),
                np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                          '00000000000000000000000000000000'], dtype='object'),
                np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                          '00000000000000000000000000000000'], dtype='object'),
                np.array(['00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec32',
                          '00000000000000000000000000000000'], dtype='object'),
                np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                          '00000000000000000000000000000000'], dtype='object'),
                np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                          '00000000000000000000000000000000'], dtype='object'),
                np.array(['00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec32',
                          '00000000000000000000000000000000'], dtype='object'),
                np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                          '00000000000000000000000000000000'], dtype='object'),
                np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                          '00000000000000000000000000000000'], dtype='object'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(9)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorInt128(v_int128=np.array(['00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec32',
                                                 '00000000000000000000000000000000'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorInt128(v_int128=np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                                                 '00000000000000000000000000000000'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorInt128(v_int128=np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                                                 '00000000000000000000000000000000'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorInt128(v_int128=np.array(['00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec32',
                                                 '00000000000000000000000000000000'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorInt128(v_int128=np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                                                 '00000000000000000000000000000000'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorInt128(v_int128=np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                                                 '00000000000000000000000000000000'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorInt128(v_int128=np.array(['00000000000000000000000000000000', 'e1671797c52e15f763380b45e841ec32',
                                                 '00000000000000000000000000000000'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorInt128(v_int128=np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                                                 '00000000000000000000000000000000'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorInt128(v_int128=np.array(['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000',
                                                 '00000000000000000000000000000000'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_uuid(self):
        class EventVectorUuid(Event):
            _event_name = "EventVectorUuid"
            v_uuid: Vector[keys.DT_UUID]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorUuid):
                    if equalPlus(self.v_uuid, other.v_uuid) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_uuid`eventTime,[UUID[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorUuid{{
                v_uuid::UUID VECTOR
                eventTime::TIMESTAMP
                def EventVectorUuid(v_uuid_){{
                    v_uuid=v_uuid_
                    eventTime=now()
                }}
            }}
            class EventVectorUuidMonitor{{
                def EventVectorUuidMonitor(){{}}
                def updateEventVectorUuid(event){{
                    insert into {func_name}_eventTest values([event.v_uuid],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorUuid,'EventVectorUuid',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(UUID[], 0) as v_uuid)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(UUID[], 0) as v_uuid) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(UUID[], 0) as v_uuid) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_uuid,eventTime"
            fieldType="UUID,TIMESTAMP"
            fieldTypeId=[[UUID,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorUuid", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_uuid")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorUuidMonitor()>, dummy, [EventVectorUuid], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorUuid],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_uuid"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorUuid], eventTimeFields=["eventTime"], commonFields=["v_uuid"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, '5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000'],
                ['5d212a78-cc48-e3b1-4235-b4d91473ee87', None, '00000000-0000-0000-0000-000000000000'],
                ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', None],
                np.array([None, '5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000'],
                         dtype='object'),
                np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', None, '00000000-0000-0000-0000-000000000000'],
                         dtype='object'),
                np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', None],
                         dtype='object'),
                pd.Series([None, '5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000'],
                          dtype='object'),
                pd.Series(['5d212a78-cc48-e3b1-4235-b4d91473ee87', None, '00000000-0000-0000-0000-000000000000'],
                          dtype='object'),
                pd.Series(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', None],
                          dtype='object'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorUuid(
                v_uuid=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_uuid'][0,],array(UUID[]).append!([uuid(['00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_uuid'][1,],array(UUID[]).append!([uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_uuid'][2,],array(UUID[]).append!([uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_uuid'][3,],array(UUID[]).append!([uuid(['00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_uuid'][4,],array(UUID[]).append!([uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_uuid'][5,],array(UUID[]).append!([uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_uuid'][6,],array(UUID[]).append!([uuid(['00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_uuid'][7,],array(UUID[]).append!([uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_uuid'][8,],array(UUID[]).append!([uuid(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-000000000000'])]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_uuid': [
                np.array(['00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee87',
                          '00000000-0000-0000-0000-000000000000'], dtype='object'),
                np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                          '00000000-0000-0000-0000-000000000000'], dtype='object'),
                np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                          '00000000-0000-0000-0000-000000000000'], dtype='object'),
                np.array(['00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee87',
                          '00000000-0000-0000-0000-000000000000'], dtype='object'),
                np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                          '00000000-0000-0000-0000-000000000000'], dtype='object'),
                np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                          '00000000-0000-0000-0000-000000000000'], dtype='object'),
                np.array(['00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee87',
                          '00000000-0000-0000-0000-000000000000'], dtype='object'),
                np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                          '00000000-0000-0000-0000-000000000000'], dtype='object'),
                np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                          '00000000-0000-0000-0000-000000000000'], dtype='object'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(9)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorUuid(v_uuid=np.array(
                ['00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee87',
                 '00000000-0000-0000-0000-000000000000'], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorUuid(v_uuid=np.array(
                ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                 '00000000-0000-0000-0000-000000000000'], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorUuid(v_uuid=np.array(
                ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                 '00000000-0000-0000-0000-000000000000'], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorUuid(v_uuid=np.array(
                ['00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee87',
                 '00000000-0000-0000-0000-000000000000'], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorUuid(v_uuid=np.array(
                ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                 '00000000-0000-0000-0000-000000000000'], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorUuid(v_uuid=np.array(
                ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                 '00000000-0000-0000-0000-000000000000'], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorUuid(v_uuid=np.array(
                ['00000000-0000-0000-0000-000000000000', '5d212a78-cc48-e3b1-4235-b4d91473ee87',
                 '00000000-0000-0000-0000-000000000000'], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorUuid(v_uuid=np.array(
                ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                 '00000000-0000-0000-0000-000000000000'], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorUuid(v_uuid=np.array(
                ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000',
                 '00000000-0000-0000-0000-000000000000'], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_ipaddr(self):
        class EventVectorIpaddr(Event):
            _event_name = "EventVectorIpaddr"
            v_ipaddr: Vector[keys.DT_IPADDR]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorIpaddr):
                    if equalPlus(self.v_ipaddr, other.v_ipaddr) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_ipaddr`eventTime,[IPADDR[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorIpaddr{{
                v_ipaddr::IPADDR VECTOR
                eventTime::TIMESTAMP
                def EventVectorIpaddr(v_ipaddr_){{
                    v_ipaddr=v_ipaddr_
                    eventTime=now()
                }}
            }}
            class EventVectorIpaddrMonitor{{
                def EventVectorIpaddrMonitor(){{}}
                def updateEventVectorIpaddr(event){{
                    insert into {func_name}_eventTest values([event.v_ipaddr],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorIpaddr,'EventVectorIpaddr',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(IPADDR[], 0) as v_ipaddr)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(IPADDR[], 0) as v_ipaddr) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(IPADDR[], 0) as v_ipaddr) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_ipaddr,eventTime"
            fieldType="IPADDR,TIMESTAMP"
            fieldTypeId=[[IPADDR,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorIpaddr", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_ipaddr")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorIpaddrMonitor()>, dummy, [EventVectorIpaddr], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorIpaddr],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_ipaddr"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorIpaddr], eventTimeFields=["eventTime"], commonFields=["v_ipaddr"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, '127.0.0.1', '0.0.0.0'],
                ['127.0.0.1', None, '0.0.0.0'],
                ['127.0.0.1', '0.0.0.0', None],
                np.array([None, '127.0.0.1', '0.0.0.0'], dtype='object'),
                np.array(['127.0.0.1', None, '0.0.0.0'], dtype='object'),
                np.array(['127.0.0.1', '0.0.0.0', None], dtype='object'),
                pd.Series([None, '127.0.0.1', '0.0.0.0'], dtype='object'),
                pd.Series(['127.0.0.1', None, '0.0.0.0'], dtype='object'),
                pd.Series(['127.0.0.1', '0.0.0.0', None], dtype='object'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorIpaddr(
                v_ipaddr=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            print(event)
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_ipaddr'][0,],array(IPADDR[]).append!([ipaddr(['0.0.0.0', '127.0.0.1', '0.0.0.0'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_ipaddr'][1,],array(IPADDR[]).append!([ipaddr(['127.0.0.1', '0.0.0.0', '0.0.0.0'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_ipaddr'][2,],array(IPADDR[]).append!([ipaddr(['127.0.0.1', '0.0.0.0', '0.0.0.0'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_ipaddr'][3,],array(IPADDR[]).append!([ipaddr(['0.0.0.0', '127.0.0.1', '0.0.0.0'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_ipaddr'][4,],array(IPADDR[]).append!([ipaddr(['127.0.0.1', '0.0.0.0', '0.0.0.0'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_ipaddr'][5,],array(IPADDR[]).append!([ipaddr(['127.0.0.1', '0.0.0.0', '0.0.0.0'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_ipaddr'][6,],array(IPADDR[]).append!([ipaddr(['0.0.0.0', '127.0.0.1', '0.0.0.0'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_ipaddr'][7,],array(IPADDR[]).append!([ipaddr(['127.0.0.1', '0.0.0.0', '0.0.0.0'])]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_ipaddr'][8,],array(IPADDR[]).append!([ipaddr(['127.0.0.1', '0.0.0.0', '0.0.0.0'])]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_ipaddr': [
                np.array(['0.0.0.0', '127.0.0.1', '0.0.0.0'], dtype='object'),
                np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                np.array(['0.0.0.0', '127.0.0.1', '0.0.0.0'], dtype='object'),
                np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                np.array(['0.0.0.0', '127.0.0.1', '0.0.0.0'], dtype='object'),
                np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(9)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorIpaddr(v_ipaddr=np.array(['0.0.0.0', '127.0.0.1', '0.0.0.0'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorIpaddr(v_ipaddr=np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorIpaddr(v_ipaddr=np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorIpaddr(v_ipaddr=np.array(['0.0.0.0', '127.0.0.1', '0.0.0.0'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorIpaddr(v_ipaddr=np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorIpaddr(v_ipaddr=np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorIpaddr(v_ipaddr=np.array(['0.0.0.0', '127.0.0.1', '0.0.0.0'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorIpaddr(v_ipaddr=np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorIpaddr(v_ipaddr=np.array(['127.0.0.1', '0.0.0.0', '0.0.0.0'], dtype='object'),
                              eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_decimal32_4(self):
        class EventVectorDecimal32(Event):
            _event_name = "EventVectorDecimal32"
            v_decimal32_4: Vector[keys.DT_DECIMAL32, 4]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorDecimal32):
                    if equalPlus(self.v_decimal32_4, other.v_decimal32_4) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_decimal32_4`eventTime,[DECIMAL32(4)[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorDecimal32{{
                v_decimal32_4::DECIMAL32(4) VECTOR
                eventTime::TIMESTAMP
                def EventVectorDecimal32(v_decimal32_4_){{
                    v_decimal32_4=v_decimal32_4_
                    eventTime=now()
                }}
            }}
            class EventVectorDecimal32Monitor{{
                def EventVectorDecimal32Monitor(){{}}
                def updateEventVectorDecimal32(event){{
                    insert into {func_name}_eventTest values([event.v_decimal32_4],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorDecimal32,'EventVectorDecimal32',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL32(4)[], 0) as v_decimal32_4)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL32(4)[], 0) as v_decimal32_4) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL32(4)[], 0) as v_decimal32_4) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_decimal32_4,eventTime"
            fieldType="DECIMAL32(4),TIMESTAMP"
            fieldTypeId=[[DECIMAL32(4),TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorDecimal32", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_decimal32_4")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorDecimal32Monitor()>, dummy, [EventVectorDecimal32], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorDecimal32],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_decimal32_4"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorDecimal32], eventTimeFields=["eventTime"], commonFields=["v_decimal32_4"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, Decimal('0.0000'), Decimal('3.1415'), Decimal("nan")],
                [Decimal('0.0000'), Decimal('3.1415'), None, Decimal("nan")],
                [Decimal('0.0000'), Decimal('3.1415'), Decimal("nan"), None],
                np.array([None, Decimal('0.0000'), Decimal('3.1415'), Decimal("nan")], dtype='object'),
                np.array([Decimal('0.0000'), Decimal('3.1415'), None, Decimal("nan")], dtype='object'),
                np.array([Decimal('0.0000'), Decimal('3.1415'), Decimal("nan"), None], dtype='object'),
                pd.Series([None, Decimal('0.0000'), Decimal('3.1415'), Decimal("nan")], dtype='object'),
                pd.Series([Decimal('0.0000'), Decimal('3.1415'), None, Decimal("nan")], dtype='object'),
                pd.Series([Decimal('0.0000'), Decimal('3.1415'), Decimal("nan"), None], dtype='object'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorDecimal32(
                v_decimal32_4=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal32_4'][0,],array(DECIMAL32(4)[]).append!([decimal32([null,'0.0000','3.1415',null],4)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal32_4'][1,],array(DECIMAL32(4)[]).append!([decimal32(['0.0000','3.1415',null,null],4)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal32_4'][2,],array(DECIMAL32(4)[]).append!([decimal32(['0.0000','3.1415',null,null],4)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal32_4'][3,],array(DECIMAL32(4)[]).append!([decimal32([null,'0.0000','3.1415',null],4)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal32_4'][4,],array(DECIMAL32(4)[]).append!([decimal32(['0.0000','3.1415',null,null],4)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal32_4'][5,],array(DECIMAL32(4)[]).append!([decimal32(['0.0000','3.1415',null,null],4)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal32_4'][6,],array(DECIMAL32(4)[]).append!([decimal32([null,'0.0000','3.1415',null],4)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal32_4'][7,],array(DECIMAL32(4)[]).append!([decimal32(['0.0000','3.1415',null,null],4)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal32_4'][8,],array(DECIMAL32(4)[]).append!([decimal32(['0.0000','3.1415',null,null],4)]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_decimal32_4': [
                np.array([None, Decimal('0.0000'), Decimal('3.1415'), None], dtype='object'),
                np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                np.array([None, Decimal('0.0000'), Decimal('3.1415'), None], dtype='object'),
                np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                np.array([None, Decimal('0.0000'), Decimal('3.1415'), None], dtype='object'),
                np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(9)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorDecimal32(
                v_decimal32_4=np.array([None, Decimal('0.0000'), Decimal('3.1415'), None], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorDecimal32(
                v_decimal32_4=np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorDecimal32(
                v_decimal32_4=np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorDecimal32(
                v_decimal32_4=np.array([None, Decimal('0.0000'), Decimal('3.1415'), None], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorDecimal32(
                v_decimal32_4=np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorDecimal32(
                v_decimal32_4=np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorDecimal32(
                v_decimal32_4=np.array([None, Decimal('0.0000'), Decimal('3.1415'), None], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorDecimal32(
                v_decimal32_4=np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorDecimal32(
                v_decimal32_4=np.array([Decimal('0.0000'), Decimal('3.1415'), None, None], dtype='object'),
                eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_decimal64_12(self):
        class EventVectorDecimal64(Event):
            _event_name = "EventVectorDecimal64"
            v_decimal64_12: Vector[keys.DT_DECIMAL64, 12]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorDecimal64):
                    if equalPlus(self.v_decimal64_12, other.v_decimal64_12) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_decimal64_12`eventTime,[DECIMAL64(12)[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorDecimal64{{
                v_decimal64_12::DECIMAL64(12) VECTOR
                eventTime::TIMESTAMP
                def EventVectorDecimal64(v_decimal64_12_){{
                    v_decimal64_12=v_decimal64_12_
                    eventTime=now()
                }}
            }}
            class EventVectorDecimal64Monitor{{
                def EventVectorDecimal64Monitor(){{}}
                def updateEventVectorDecimal64(event){{
                    insert into {func_name}_eventTest values([event.v_decimal64_12],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorDecimal64,'EventVectorDecimal64',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL64(12)[], 0) as v_decimal64_12)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL64(12)[], 0) as v_decimal64_12) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL64(12)[], 0) as v_decimal64_12) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_decimal64_12,eventTime"
            fieldType="DECIMAL64(12),TIMESTAMP"
            fieldTypeId=[[DECIMAL64(12),TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorDecimal64", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_decimal64_12")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorDecimal64Monitor()>, dummy, [EventVectorDecimal64], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorDecimal64],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_decimal64_12"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorDecimal64], eventTimeFields=["eventTime"], commonFields=["v_decimal64_12"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, Decimal('0.000000000000'), Decimal('3.141592653589'), Decimal("nan")],
                [Decimal('0.000000000000'), Decimal('3.141592653589'), None, Decimal("nan")],
                [Decimal('0.000000000000'), Decimal('3.141592653589'), Decimal("nan"), None],
                np.array([None, Decimal('0.000000000000'), Decimal('3.141592653589'), Decimal("nan")], dtype='object'),
                np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, Decimal("nan")], dtype='object'),
                np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), Decimal("nan"), None], dtype='object'),
                pd.Series([None, Decimal('0.000000000000'), Decimal('3.141592653589'), Decimal("nan")], dtype='object'),
                pd.Series([Decimal('0.000000000000'), Decimal('3.141592653589'), None, Decimal("nan")], dtype='object'),
                pd.Series([Decimal('0.000000000000'), Decimal('3.141592653589'), Decimal("nan"), None], dtype='object'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorDecimal64(
                v_decimal64_12=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal64_12'][0,],array(DECIMAL64(12)[]).append!([decimal64([null,'0.000000000000','3.141592653589',null],12)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal64_12'][1,],array(DECIMAL64(12)[]).append!([decimal64(['0.000000000000','3.141592653589',null,null],12)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal64_12'][2,],array(DECIMAL64(12)[]).append!([decimal64(['0.000000000000','3.141592653589',null,null],12)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal64_12'][3,],array(DECIMAL64(12)[]).append!([decimal64([null,'0.000000000000','3.141592653589',null],12)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal64_12'][4,],array(DECIMAL64(12)[]).append!([decimal64(['0.000000000000','3.141592653589',null,null],12)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal64_12'][5,],array(DECIMAL64(12)[]).append!([decimal64(['0.000000000000','3.141592653589',null,null],12)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal64_12'][6,],array(DECIMAL64(12)[]).append!([decimal64([null,'0.000000000000','3.141592653589',null],12)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal64_12'][7,],array(DECIMAL64(12)[]).append!([decimal64(['0.000000000000','3.141592653589',null,null],12)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal64_12'][8,],array(DECIMAL64(12)[]).append!([decimal64(['0.000000000000','3.141592653589',null,null],12)]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_decimal64_12': [
                np.array([None, Decimal('0.000000000000'), Decimal('3.141592653589'), None], dtype='object'),
                np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None], dtype='object'),
                np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None], dtype='object'),
                np.array([None, Decimal('0.000000000000'), Decimal('3.141592653589'), None], dtype='object'),
                np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None], dtype='object'),
                np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None], dtype='object'),
                np.array([None, Decimal('0.000000000000'), Decimal('3.141592653589'), None], dtype='object'),
                np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None], dtype='object'),
                np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None], dtype='object'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(9)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorDecimal64(
                v_decimal64_12=np.array([None, Decimal('0.000000000000'), Decimal('3.141592653589'), None],
                                        dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorDecimal64(
                v_decimal64_12=np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None],
                                        dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorDecimal64(
                v_decimal64_12=np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None],
                                        dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorDecimal64(
                v_decimal64_12=np.array([None, Decimal('0.000000000000'), Decimal('3.141592653589'), None],
                                        dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorDecimal64(
                v_decimal64_12=np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None],
                                        dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorDecimal64(
                v_decimal64_12=np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None],
                                        dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorDecimal64(
                v_decimal64_12=np.array([None, Decimal('0.000000000000'), Decimal('3.141592653589'), None],
                                        dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorDecimal64(
                v_decimal64_12=np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None],
                                        dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorDecimal64(
                v_decimal64_12=np.array([Decimal('0.000000000000'), Decimal('3.141592653589'), None, None],
                                        dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_decimal128_26(self):
        class EventVectorDecimal128(Event):
            _event_name = "EventVectorDecimal128"
            v_decimal128_26: Vector[keys.DT_DECIMAL128, 26]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorDecimal128):
                    if equalPlus(self.v_decimal128_26, other.v_decimal128_26) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_decimal128_26`eventTime,[DECIMAL128(26)[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorDecimal128{{
                v_decimal128_26::DECIMAL128(26) VECTOR
                eventTime::TIMESTAMP
                def EventVectorDecimal128(v_decimal128_26_){{
                    v_decimal128_26=v_decimal128_26_
                    eventTime=now()
                }}
            }}
            class EventVectorDecimal128Monitor{{
                def EventVectorDecimal128Monitor(){{}}
                def updateEventVectorDecimal128(event){{
                    insert into {func_name}_eventTest values([event.v_decimal128_26],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorDecimal128,'EventVectorDecimal128',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL128(26)[], 0) as v_decimal128_26)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL128(26)[], 0) as v_decimal128_26) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(DECIMAL128(26)[], 0) as v_decimal128_26) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_decimal128_26,eventTime"
            fieldType="DECIMAL128(26),TIMESTAMP"
            fieldTypeId=[[DECIMAL128(26),TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorDecimal128", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_decimal128_26")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorDecimal128Monitor()>, dummy, [EventVectorDecimal128], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorDecimal128],
                             eventTimeFields=["eventTime"],
                             commonFields=["v_decimal128_26"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorDecimal128], eventTimeFields=["eventTime"], commonFields=["v_decimal128_26"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [None, Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'),
                 Decimal("nan")],
                [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None,
                 Decimal("nan")],
                [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), Decimal("nan"),
                 None],
                np.array([None, Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'),
                          Decimal("nan")], dtype='object'),
                np.array([Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None,
                          Decimal("nan")], dtype='object'),
                np.array(
                    [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), Decimal("nan"),
                     None], dtype='object'),
                pd.Series([None, Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'),
                           Decimal("nan")], dtype='object'),
                pd.Series([Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None,
                           Decimal("nan")], dtype='object'),
                pd.Series(
                    [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), Decimal("nan"),
                     None], dtype='object'),
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorDecimal128(
                v_decimal128_26=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal128_26'][0,],array(DECIMAL128(26)[]).append!([decimal128([null,'0.00000000000000000000000000','3.14159265358979323846264338',null],26)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal128_26'][1,],array(DECIMAL128(26)[]).append!([decimal128(['0.00000000000000000000000000','3.14159265358979323846264338',null,null],26)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal128_26'][2,],array(DECIMAL128(26)[]).append!([decimal128(['0.00000000000000000000000000','3.14159265358979323846264338',null,null],26)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal128_26'][3,],array(DECIMAL128(26)[]).append!([decimal128([null,'0.00000000000000000000000000','3.14159265358979323846264338',null],26)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal128_26'][4,],array(DECIMAL128(26)[]).append!([decimal128(['0.00000000000000000000000000','3.14159265358979323846264338',null,null],26)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal128_26'][5,],array(DECIMAL128(26)[]).append!([decimal128(['0.00000000000000000000000000','3.14159265358979323846264338',null,null],26)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal128_26'][6,],array(DECIMAL128(26)[]).append!([decimal128([null,'0.00000000000000000000000000','3.14159265358979323846264338',null],26)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal128_26'][7,],array(DECIMAL128(26)[]).append!([decimal128(['0.00000000000000000000000000','3.14159265358979323846264338',null,null],26)]))")
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_decimal128_26'][8,],array(DECIMAL128(26)[]).append!([decimal128(['0.00000000000000000000000000','3.14159265358979323846264338',null,null],26)]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_decimal128_26': [
                np.array([None, Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None],
                         dtype='object'),
                np.array([Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                         dtype='object'),
                np.array([Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                         dtype='object'),
                np.array([None, Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None],
                         dtype='object'),
                np.array([Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                         dtype='object'),
                np.array([Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                         dtype='object'),
                np.array([None, Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None],
                         dtype='object'),
                np.array([Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                         dtype='object'),
                np.array([Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                         dtype='object'),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.{i:03}' for i in range(9)], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorDecimal128(v_decimal128_26=np.array(
                [None, Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
            EventVectorDecimal128(v_decimal128_26=np.array(
                [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.001', 'ms')),
            EventVectorDecimal128(v_decimal128_26=np.array(
                [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.002', 'ms')),
            EventVectorDecimal128(v_decimal128_26=np.array(
                [None, Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.003', 'ms')),
            EventVectorDecimal128(v_decimal128_26=np.array(
                [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.004', 'ms')),
            EventVectorDecimal128(v_decimal128_26=np.array(
                [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.005', 'ms')),
            EventVectorDecimal128(v_decimal128_26=np.array(
                [None, Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.006', 'ms')),
            EventVectorDecimal128(v_decimal128_26=np.array(
                [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.007', 'ms')),
            EventVectorDecimal128(v_decimal128_26=np.array(
                [Decimal('0.00000000000000000000000000'), Decimal('3.14159265358979323846264338'), None, None],
                dtype='object'), eventTime=np.datetime64('2024-03-25T12:30:05.008', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_event_all_type(self):
        class EventAllType(Event):
            s_bool: Scalar[keys.DT_BOOL]
            s_char: Scalar[keys.DT_CHAR]
            s_short: Scalar[keys.DT_SHORT]
            s_int: Scalar[keys.DT_INT]
            s_long: Scalar[keys.DT_LONG]
            s_date: Scalar[keys.DT_DATE]
            s_month: Scalar[keys.DT_MONTH]
            s_time: Scalar[keys.DT_TIME]
            s_minute: Scalar[keys.DT_MINUTE]
            s_second: Scalar[keys.DT_SECOND]
            s_datetime: Scalar[keys.DT_DATETIME]
            s_timestamp: Scalar[keys.DT_TIMESTAMP]
            s_nanotime: Scalar[keys.DT_NANOTIME]
            s_nanotimestamp: Scalar[keys.DT_NANOTIMESTAMP]
            s_datehour: Scalar[keys.DT_DATEHOUR]
            s_float: Scalar[keys.DT_FLOAT]
            s_double: Scalar[keys.DT_DOUBLE]
            s_string: Scalar[keys.DT_STRING]
            s_blob: Scalar[keys.DT_BLOB]
            s_int128: Scalar[keys.DT_INT128]
            s_uuid: Scalar[keys.DT_UUID]
            s_ipaddr: Scalar[keys.DT_IPADDR]
            # s_any:Scalar[keys.DT_ANY]
            s_decimal32_4: Scalar[keys.DT_DECIMAL32, 4]
            s_decimal64_12: Scalar[keys.DT_DECIMAL64, 12]
            s_decimal128_26: Scalar[keys.DT_DECIMAL128, 26]
            v_bool: Vector[keys.DT_BOOL]
            v_char: Vector[keys.DT_CHAR]
            v_short: Vector[keys.DT_SHORT]
            v_int: Vector[keys.DT_INT]
            v_long: Vector[keys.DT_LONG]
            v_date: Vector[keys.DT_DATE]
            v_month: Vector[keys.DT_MONTH]
            v_time: Vector[keys.DT_TIME]
            v_minute: Vector[keys.DT_MINUTE]
            v_second: Vector[keys.DT_SECOND]
            v_datetime: Vector[keys.DT_DATETIME]
            v_timestamp: Vector[keys.DT_TIMESTAMP]
            v_nanotime: Vector[keys.DT_NANOTIME]
            v_nanotimestamp: Vector[keys.DT_NANOTIMESTAMP]
            v_datehour: Vector[keys.DT_DATEHOUR]
            v_float: Vector[keys.DT_FLOAT]
            v_double: Vector[keys.DT_DOUBLE]
            v_string: Vector[keys.DT_STRING]
            v_blob: Vector[keys.DT_BLOB]
            v_symbol: Vector[keys.DT_SYMBOL]
            v_int128: Vector[keys.DT_INT128]
            v_uuid: Vector[keys.DT_UUID]
            v_ipaddr: Vector[keys.DT_IPADDR]
            # v_any:Vector[keys.DT_ANY]
            v_decimal32_4: Vector[keys.DT_DECIMAL32, 4]
            v_decimal64_12: Vector[keys.DT_DECIMAL64, 12]
            v_decimal128_26: Vector[keys.DT_DECIMAL128, 26]

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            class EventAllType{{
                s_bool::BOOL
                s_char::CHAR
                s_short::SHORT
                s_int::INT
                s_long::LONG
                s_date::DATE
                s_month::MONTH
                s_time::TIME
                s_minute::MINUTE
                s_second::SECOND
                s_datetime::DATETIME
                s_timestamp::TIMESTAMP
                s_nanotime::NANOTIME
                s_nanotimestamp::NANOTIMESTAMP
                s_datehour::DATEHOUR
                s_float::FLOAT
                s_double::DOUBLE
                s_string::STRING
                s_blob::BLOB
                s_int128::INT128
                s_uuid::UUID
                s_ipaddr::IPADDR
                // s_any::ANY
                s_decimal32_4::DECIMAL32(4)
                s_decimal64_12::DECIMAL64(12)
                s_decimal128_26::DECIMAL128(26)
                v_bool::BOOL VECTOR
                v_char::CHAR VECTOR
                v_short::SHORT VECTOR
                v_int::INT VECTOR
                v_long::LONG VECTOR
                v_date::DATE VECTOR
                v_month::MONTH VECTOR
                v_time::TIME VECTOR
                v_minute::MINUTE VECTOR
                v_second::SECOND VECTOR
                v_datetime::DATETIME VECTOR
                v_timestamp::TIMESTAMP VECTOR
                v_nanotime::NANOTIME VECTOR
                v_nanotimestamp::NANOTIMESTAMP VECTOR
                v_datehour::DATEHOUR VECTOR
                v_float::FLOAT VECTOR
                v_double::DOUBLE VECTOR
                v_string::STRING VECTOR
                v_blob::BLOB VECTOR
                v_symbol::SYMBOL VECTOR
                v_int128::INT128 VECTOR
                v_uuid::UUID VECTOR
                v_ipaddr::IPADDR VECTOR
                // v_any::ANY VECTOR
                v_decimal32_4::DECIMAL32(4) VECTOR
                v_decimal64_12::DECIMAL64(12) VECTOR
                v_decimal128_26::DECIMAL128(26) VECTOR

                def EventAllType(s_bool_,s_char_,s_short_,s_int_,s_long_,s_date_,s_month_,s_time_,s_minute_,s_second_,s_datetime_,s_timestamp_,s_nanotime_,s_nanotimestamp_,s_datehour_,s_float_,s_double_,s_string_,s_blob_,s_int128_,s_uuid_,s_ipaddr_,s_decimal32_4_,s_decimal64_12_,s_decimal128_26_,v_bool_,v_char_,v_short_,v_int_,v_long_,v_date_,v_month_,v_time_,v_minute_,v_second_,v_datetime_,v_timestamp_,v_nanotime_,v_nanotimestamp_,v_datehour_,v_float_,v_double_,v_string_,v_blob_,v_symbol_,v_int128_,v_uuid_,v_ipaddr_,v_decimal32_4_,v_decimal64_12_,v_decimal128_26_){{
                    s_bool=s_bool_
                    s_char=s_char_
                    s_short=s_short_
                    s_int=s_int_
                    s_long=s_long_
                    s_date=s_date_
                    s_month=s_month_
                    s_time=s_time_
                    s_minute=s_minute_
                    s_second=s_second_
                    s_datetime=s_datetime_
                    s_timestamp=s_timestamp_
                    s_nanotime=s_nanotime_
                    s_nanotimestamp=s_nanotimestamp_
                    s_datehour=s_datehour_
                    s_float=s_float_
                    s_double=s_double_
                    s_string=s_string_
                    s_blob=s_blob_
                    s_int128=s_int128_
                    s_uuid=s_uuid_
                    s_ipaddr=s_ipaddr_
                    s_decimal32_4=s_decimal32_4_
                    s_decimal64_12=s_decimal64_12_
                    s_decimal128_26=s_decimal128_26_
                    v_bool=v_bool_
                    v_char=v_char_
                    v_short=v_short_
                    v_int=v_int_
                    v_long=v_long_
                    v_date=v_date_
                    v_month=v_month_
                    v_time=v_time_
                    v_minute=v_minute_
                    v_second=v_second_
                    v_datetime=v_datetime_
                    v_timestamp=v_timestamp_
                    v_nanotime=v_nanotime_
                    v_nanotimestamp=v_nanotimestamp_
                    v_datehour=v_datehour_
                    v_float=v_float_
                    v_double=v_double_
                    v_string=v_string_
                    v_blob=v_blob_
                    v_symbol=v_symbol_
                    v_int128=v_int128_
                    v_uuid=v_uuid_
                    v_ipaddr=v_ipaddr_
                    v_decimal32_4=v_decimal32_4_
                    v_decimal64_12=v_decimal64_12_
                    v_decimal128_26=v_decimal128_26_
                }}
            }}

            class EventAllTypeMonitor{{
                def EventAllTypeMonitor(){{}}
                def updateEventAllType(event){{
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventAllType,'EventAllType',,'all')
                }}
            }}

            dummy = table(array(STRING, 0) as eventType, array(BLOB, 0) as blobs)
            share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input
            share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])

            eventField="s_bool,s_char,s_short,s_int,s_long,s_date,s_month,s_time,s_minute,s_second,s_datetime,s_timestamp,s_nanotime,s_nanotimestamp,s_datehour,s_float,s_double,s_string,s_blob,s_int128,s_uuid,s_ipaddr,s_decimal32_4,s_decimal64_12,s_decimal128_26,v_bool,v_char,v_short,v_int,v_long,v_date,v_month,v_time,v_minute,v_second,v_datetime,v_timestamp,v_nanotime,v_nanotimestamp,v_datehour,v_float,v_double,v_string,v_blob,v_symbol,v_int128,v_uuid,v_ipaddr,v_decimal32_4,v_decimal64_12,v_decimal128_26"
            fieldType="BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR,FLOAT,DOUBLE,STRING,BLOB,INT128,UUID,IPADDR,DECIMAL32(4),DECIMAL64(12),DECIMAL128(26),BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR,FLOAT,DOUBLE,STRING,BLOB,SYMBOL,INT128,UUID,IPADDR,DECIMAL32(4),DECIMAL64(12),DECIMAL128(26)"
            fieldTypeId=[[BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR,FLOAT,DOUBLE,STRING,BLOB,INT128,UUID,IPADDR,DECIMAL32(4),DECIMAL64(12),DECIMAL128(26),BOOL,CHAR,SHORT,INT,LONG,DATE,MONTH,TIME,MINUTE,SECOND,DATETIME,TIMESTAMP,NANOTIME,NANOTIMESTAMP,DATEHOUR,FLOAT,DOUBLE,STRING,BLOB,SYMBOL,INT128,UUID,IPADDR,DECIMAL32(4),DECIMAL64(12),DECIMAL128(26)]]
            fieldFormId=[[SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,SCALAR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR,VECTOR]]
            insert into schema values("EventAllType", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output)
            engine = createCEPEngine('{func_name}_cep1', <EventAllTypeMonitor()>, dummy, [EventAllType], 1,, 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventAllType])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventAllType])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        event = EventAllType(s_bool=True, s_char=1, s_short=2, s_int=3, s_long=4, s_date=0, s_month=0, s_time=0,
                             s_minute=0, s_second=0, s_datetime=0, s_timestamp=0, s_nanotime=0, s_nanotimestamp=0,
                             s_datehour=0, s_float=3.14, s_double=3.14, s_string="123", s_blob="132",
                             s_int128='e1671797c52e15f763380b45e841ec32', s_uuid='5d212a78-cc48-e3b1-4235-b4d91473ee87',
                             s_ipaddr='127.0.0.1', s_decimal32_4=Decimal("3.1415"),
                             s_decimal64_12=Decimal('3.141592653589'),
                             s_decimal128_26=Decimal('3.14159265358979323846264338'), v_bool=[True, False],
                             v_char=[1, 2], v_short=[1, 2], v_int=[1, 2], v_long=[1, 2], v_date=[0, 0], v_month=[0, 0],
                             v_time=[0, 0], v_minute=[0, 0], v_second=[0, 0], v_datetime=[0, 0], v_timestamp=[0, 0],
                             v_nanotime=[0, 0], v_nanotimestamp=[0, 0], v_datehour=[0, 0], v_float=[0, 0],
                             v_double=[0, 0], v_string=["1", "2"], v_blob=["1", "2"], v_symbol=["1", "2"],
                             v_int128=['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec32'],
                             v_uuid=['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee87'],
                             v_ipaddr=['127.0.0.1', '127.0.0.1'], v_decimal32_4=[Decimal("3.1415")],
                             v_decimal64_12=[Decimal('3.141592653589')],
                             v_decimal128_26=[Decimal('3.14159265358979323846264338')])
        sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")

    def test_CEP_double_events(self):
        class Event1(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime1: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, Event1):
                    if equalPlus(self.s_bool, other.s_bool) and self.eventTime1 == other.eventTime1:
                        return True
                    else:
                        return False
                else:
                    return False

        class Event2(Event):
            s_char: Scalar[keys.DT_CHAR]
            eventTime2: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, Event2):
                    if equalPlus(self.s_char, other.s_char) and self.eventTime2 == other.eventTime2:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            class Event1{{
                s_bool::BOOL
                eventTime1::TIMESTAMP
                def Event1(s_bool_){{
                    s_bool=s_bool_
                    eventTime1=now()
                }}
            }}
            class Event2{{
                s_char::CHAR
                eventTime2::TIMESTAMP
                def Event2(s_char_){{
                    s_char=s_char_
                    eventTime2=now()
                }}
            }}
            class EventMonitor{{
                def EventMonitor(){{}}
                def updateEvent(event){{
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEvent,'Event1',,'all')
                    addEventListener(updateEvent,'Event2',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_bool,eventTime1"
            eventField_="s_char,eventTime2"
            fieldType="BOOL,TIMESTAMP"
            fieldType_="CHAR,TIMESTAMP"
            fieldTypeId=[[BOOL,TIMESTAMP]]
            fieldTypeId_=[[CHAR,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR]]
            insert into schema values("Event1", eventField, fieldType,fieldTypeId,fieldFormId)
            insert into schema values("Event2", eventField_, fieldType_,fieldTypeId_,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = ["eventTime1","eventTime2"])
            engine = createCEPEngine('{func_name}_cep1', <EventMonitor()>, dummy, [Event1,Event2], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [Event1, Event2],
                             eventTimeFields=["eventTime1", "eventTime2"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([Event1, Event2], eventTimeFields=["eventTime1", "eventTime2"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        event1 = Event1(True, 0)
        event2 = Event2(0, 1)
        sender.sendEvent(event1)
        sender.sendEvent(event2)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        expect = [
            Event1(s_bool=True, eventTime1=np.datetime64('1970-01-01T00:00:00.000', 'ms')),
            Event2(s_char=0, eventTime2=np.datetime64('1970-01-01T00:00:00.001', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_events_double_commonFields(self):
        class EventTest(Event):
            s_bool1: Scalar[keys.DT_BOOL]
            s_bool2: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventTest):
                    if equalPlus(self.s_bool1, other.s_bool1) and equalPlus(self.s_bool2,
                                                                            other.s_bool2) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            class EventTest{{
                s_bool1::BOOL
                s_bool2::BOOL
                eventTime::TIMESTAMP
                def EventTest(s_bool1_,s_bool2_){{
                    s_bool1=s_bool1_
                    s_bool2=s_bool2_
                    eventTime=now()
                }}
            }}
            
            class EventTestMonitor{{
                def EventTestMonitor(){{}}
                def updateEventTest(event){{
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventTest,'EventTest',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime,array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(BOOL,0) as s_bool1,array(BOOL,0) as s_bool2)
            share streamTable(array(TIMESTAMP, 0) as eventTime,array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(BOOL,0) as s_bool1,array(BOOL,0) as s_bool2) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime,array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(BOOL,0) as s_bool1,array(BOOL,0) as s_bool2) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="s_bool1,s_bool2,eventTime"
            fieldType="BOOL,BOOL,TIMESTAMP"
            fieldTypeId=[[BOOL,BOOL,TIMESTAMP]]
            fieldFormId=[[SCALAR,SCALAR,SCALAR]]
            insert into schema values("EventTest", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output,eventTimeField="eventTime",commonField=["s_bool1","s_bool2"])
            engine = createCEPEngine('{func_name}_cep1', <EventTestMonitor()>, dummy, [EventTest], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventTest], eventTimeFields=["eventTime"],
                             commonFields=["s_bool1", "s_bool2"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventTest], eventTimeFields=["eventTime"], commonFields=["s_bool1", "s_bool2"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        event = EventTest(True, False, 0)
        sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        expect = [
            EventTest(s_bool1=True, s_bool2=False, eventTime=np.datetime64('1970-01-01T00:00:00.000', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e

    def test_CEP_vector_65535(self):
        class EventVectorInt(Event):
            _event_name = "EventVectorInt"
            v_int: Vector[keys.DT_INT]
            eventTime: Scalar[keys.DT_TIMESTAMP]

            def __eq__(self, other):
                if isinstance(other, EventVectorInt):
                    if equalPlus(self.v_int, other.v_int) and self.eventTime == other.eventTime:
                        return True
                    else:
                        return False
                else:
                    return False

        func_name = inspect.currentframe().f_code.co_name
        scripts = f"""
            all_pubTables = getStreamingStat().pubTables
            for(pubTables in all_pubTables){{
                if (pubTables.tableName==`{func_name}_input){{
                    stopPublishTable(pubTables.subscriber.split(":")[0],int(pubTables.subscriber.split(":")[1]),pubTables.tableName,pubTables.actions)
                    break
                }}
            }}
            try{{dropStreamEngine(`{func_name}_serOutput)}} catch(ex) {{}}
            try{{dropStreamEngine(`{func_name}_cep1)}} catch(ex) {{}}
            try{{unsubscribeTable(,`{func_name}_input, `{func_name}_subopt)}} catch(ex){{}}
            
            share table(100:0,`v_int`eventTime,[INT[],TIMESTAMP]) as `{func_name}_eventTest
            class EventVectorInt{{
                v_int::INT VECTOR
                eventTime::TIMESTAMP
                def EventVectorInt(v_int_){{
                    v_int=v_int_
                    eventTime=now()
                }}
            }}
            class EventVectorIntMonitor{{
                def EventVectorIntMonitor(){{}}
                def updateEventVectorInt(event){{
                    insert into {func_name}_eventTest values([event.v_int],event.eventTime)
                    emitEvent(event)
                }}
                def onload(){{
                    addEventListener(updateEventVectorInt,'EventVectorInt',,'all')
                }}
            }}
            dummy = table(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT[], 0) as v_int)
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT[], 0) as v_int) as {func_name}_input
            share streamTable(array(TIMESTAMP, 0) as eventTime, array(STRING, 0) as eventType, array(BLOB, 0) as blobs, array(INT[], 0) as v_int) as {func_name}_output
            schema = table(1:0, `eventType`eventField`fieldType`fieldTypeId`fieldFormId, [STRING, STRING, STRING, INT[], INT[]])
            eventField="v_int,eventTime"
            fieldType="INT,TIMESTAMP"
            fieldTypeId=[[INT,TIMESTAMP]]
            fieldFormId=[[VECTOR,SCALAR]]
            insert into schema values("EventVectorInt", eventField, fieldType,fieldTypeId,fieldFormId)
            outputSerializer = streamEventSerializer(name=`{func_name}_serOutput, eventSchema=schema, outputTable={func_name}_output, eventTimeField = "eventTime", commonField="v_int")
            engine = createCEPEngine('{func_name}_cep1', <EventVectorIntMonitor()>, dummy, [EventVectorInt], 1, 'eventTime', 10000, outputSerializer)
            subscribeTable(,`{func_name}_input, `{func_name}_subopt, 0, getStreamEngine('{func_name}_cep1'),true)
        """
        self.__class__.conn.run(scripts)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventVectorInt], eventTimeFields=["eventTime"],
                             commonFields=["v_int"])
        result = []

        def handler(_event):
            print(_event)
            result.append(_event)

        client = EventClient([EventVectorInt], eventTimeFields=["eventTime"], commonFields=["v_int"])
        client.subscribe(HOST, PORT, handler, f"{func_name}_output", "ttt", offset=0, userName=USER,
                         password=PASSWD)
        for index, data in enumerate((
                [i for i in range(65535)],
        )):
            eventTime = f'2024-03-25T12:30:05.{index:03}'
            event = EventVectorInt(
                v_int=data,
                eventTime=np.datetime64(eventTime, 'ms')
            )
            sender.sendEvent(event)
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_output", "ttt")
        # check CommonFields
        assert self.__class__.conn.run(
            f"eqObj({func_name}_output['v_int'][0,],array(INT[]).append!([0..65534]))")
        # check server deserialize
        df_expect = pd.DataFrame({
            'v_int': [
                np.array([i for i in range(65535)], dtype=np.int32),
            ],
            'eventTime': np.array([f'2024-03-25T12:30:05.000'], dtype='datetime64[ns]')
        })
        df = self.__class__.conn.run(f"{func_name}_eventTest")
        assert equalPlus(df, df_expect)
        expect = [
            EventVectorInt(v_int=np.array([i for i in range(65535)], dtype=np.int32),
                           eventTime=np.datetime64('2024-03-25T12:30:05.000', 'ms')),
        ]
        assert all(self.__class__.conn.run(f"each(eqObj, {func_name}_input.values(), {func_name}_output.values())"))
        # check api deserialize
        for r, e in zip(result, expect):
            assert r == e


class TestEventSender(object):
    conn: ddb.Session = ddb.Session(HOST, PORT, USER, PASSWD, enablePickle=False)

    def test_EventSender_eventSchema_type_error(self):
        with pytest.raises(TypeError, match="eventSchema must be a list of child class of Event"):
            EventSender(self.__class__.conn, "input", 1)
        with pytest.raises(ValueError, match="The event defined must be a child class of Event"):
            EventSender(self.__class__.conn, "input", [1])

    def test_EventSender_eventSchema_empty(self):
        with pytest.raises(RuntimeError, match='eventSchema must be non-null and non-empty'):
            EventSender(self.__class__.conn, "input", [])

    def test_EventSender_eventSchema_event_empty(self):
        class EventEmpty(Event):
            pass

        with pytest.raises(RuntimeError, match="The fieldNames in eventSchema must be a non-empty string"):
            EventSender(self.__class__.conn, "input", [EventEmpty])

    def test_EventSender_eventSchema_event_error_extraParam(self):
        class EventErrorExtraParam(Event):
            s_bool: Scalar[keys.DT_BOOL, 4]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventErrorExtraParam])
        sender.sendEvent(EventErrorExtraParam(True))

    def test_EventSender_send_event_not_in_eventSchema(self):
        class EventSchema(Event):
            s_bool: Scalar[keys.DT_BOOL]

        class EventSend(Event):
            test: Scalar[keys.DT_BOOL]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventSchema])
        with pytest.raises(RuntimeError, match="Unknown eventType EventSend"):
            sender.sendEvent(EventSend(True))

    def test_EventSender_schema_mismatch(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        with pytest.raises(RuntimeError, match="Schema mismatch: Output table contains 2 cols, expected 3 cols."):
            EventSender(self.__class__.conn, f"{func_name}_input", [EventTest], eventTimeFields="eventTime")

    def test_EventSender_eventTimeFields_not_time(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_BLOB]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(TIMESTAMP, 0) as eventTime,array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventTest], eventTimeFields="eventTime")
        with pytest.raises(RuntimeError, match="Failed to append data to column 'eventTime' with error"):
            sender.sendEvent(EventTest(True, b""))

    def test_EventSender_commonFields_absent(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(TIMESTAMP, 0) as eventTime,array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(INT,0) as a) as {func_name}_input")
        with pytest.raises(RuntimeError, match="Event EventTest doesn't contain commonField a"):
            EventSender(self.__class__.conn, f"{func_name}_input", [EventTest], eventTimeFields="eventTime",
                        commonFields=["a"])

    def test_EventSender_connect_closed(self):
        class EventSchema(Event):
            s_bool: Scalar[keys.DT_BOOL]

        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.close()
        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        with pytest.raises(RuntimeError,
                           match="Couldn't send script/function to the remote host because the connection has been closed"):
            EventSender(conn, f"{func_name}_input", [EventSchema])

    def test_EventSender_connect_table_absent(self):
        class EventSchema(Event):
            s_bool: Scalar[keys.DT_BOOL]

        with pytest.raises(RuntimeError, match=r"Can\'t find the object with name absent"):
            EventSender(self.__class__.conn, "absent", [EventSchema])


class TestEventClient(object):
    conn: ddb.Session = ddb.Session(HOST, PORT, USER, PASSWD, enablePickle=False)

    def test_EventClient_eventSchema_type_error(self):
        with pytest.raises(TypeError, match="eventSchema must be a list of child class of Event"):
            EventClient(1)
        with pytest.raises(ValueError, match="The event defined must be a child class of Event"):
            EventClient([1])

    def test_EventClient_eventSchema_empty(self):
        with pytest.raises(RuntimeError, match='eventSchema must be non-null and non-empty'):
            EventClient([])

    def test_EventClient_eventSchema_event_empty(self):
        class EventEmpty(Event):
            pass

        with pytest.raises(RuntimeError, match="The fieldNames in eventSchema must be a non-empty string"):
            EventClient([EventEmpty])

    def test_EventClient_eventSchema_event_error_extraParam(self):
        class EventErrorExtraParam(Event):
            s_bool: Scalar[keys.DT_BOOL, 4]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventErrorExtraParam])
        client = EventClient([EventErrorExtraParam])
        client.subscribe(HOST, PORT, print, f"{func_name}_input", "ttt")
        sender.sendEvent(EventErrorExtraParam(True))
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_input", "ttt")

    def test_EventClient_receive_event_not_in_eventSchema(self):
        class EventSchema(Event):
            s_bool: Scalar[keys.DT_BOOL]

        class EventReceive(Event):
            test: Scalar[keys.DT_BOOL]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventSchema])
        client = EventClient([EventReceive])
        client.subscribe(HOST, PORT, print, f"{func_name}_input", "ttt")
        sender.sendEvent(EventSchema(True))
        sleep(1)
        # todo:check
        client.unsubscribe(HOST, PORT, f"{func_name}_input", "ttt")

    def test_EventClient_schema_mismatch(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        client = EventClient([EventTest], "eventTime")
        with pytest.raises(RuntimeError, match="Schema mismatch"):
            client.subscribe(HOST, PORT, print, f"{func_name}_input")

    def test_EventClient_eventTimeFields_not_time(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_BLOB]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs,array(BLOB, 0) as eventTime) as {func_name}_input")
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventTest], commonFields=["eventTime"])
        client = EventClient([EventTest], commonFields=["eventTime"])
        client.subscribe(HOST, PORT, print, f"{func_name}_input")
        sender.sendEvent(EventTest(True, b""))
        sleep(1)
        client.unsubscribe(HOST, PORT, f"{func_name}_input")

    def test_EventSender_commonFields_Event_absent(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        with pytest.raises(RuntimeError, match="Event EventTest doesn't contain commonField a"):
            EventClient([EventTest], eventTimeFields="eventTime", commonFields=["a"])

    def test_EventSender_commonFields_table_absent(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        class EventTestClass(Event):
            _event_name = "EventTest"
            a: Scalar[keys.DT_BOOL]
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(TIMESTAMP, 0) as eventTime,array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        client = EventClient([EventTestClass], eventTimeFields="eventTime", commonFields=["a"])
        with pytest.raises(RuntimeError, match="Schema mismatch"):
            client.subscribe(HOST, PORT, print, f"{func_name}_input")

    def test_EventClient_subscribe_host_error(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        client = EventClient([EventTest])
        with pytest.raises(RuntimeError, match="Subscribe Fail"):
            client.subscribe("192.168.0.0", 8848, print, "input")

    def test_EventClient_subscribe_port_error(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        client = EventClient([EventTest])
        with pytest.raises(RuntimeError, match="Subscribe Fail"):
            client.subscribe("192.168.0.54", 8888, print, "input")

    def test_EventClient_subscribe_table_absent(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        client = EventClient([EventTest])
        with pytest.raises(RuntimeError, match=r"Can\'t find the object with name absent\'"):
            client.subscribe(HOST, PORT, print, "absent")

    def test_EventClient_subscribe_action_existed(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]
            eventTime: Scalar[keys.DT_TIMESTAMP]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(
            f"share streamTable(array(TIMESTAMP, 0) as eventTime,array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input")
        client = EventClient([EventTest], eventTimeFields="eventTime")
        client.subscribe(HOST, PORT, print, f"{func_name}_input", "existed")
        with pytest.raises(RuntimeError, match="already exists"):
            client.subscribe(HOST, PORT, print, f"{func_name}_input", "existed")

    def test_EventClient_subscribe_twice(self):
        class EventTest1(Event):
            s_bool: Scalar[keys.DT_BOOL]

        class EventTest2(Event):
            v_bool: Vector[keys.DT_BOOL]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(f"""
            share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input1
            share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input2
        """)
        sender1 = EventSender(self.__class__.conn, f"{func_name}_input1", [EventTest1])
        sender2 = EventSender(self.__class__.conn, f"{func_name}_input2", [EventTest2])
        client = EventClient([EventTest1, EventTest2])
        client.subscribe(HOST, PORT, print, f"{func_name}_input1", f"{func_name}_input1")
        client.subscribe(HOST, PORT, print, f"{func_name}_input2", f"{func_name}_input2")
        sender1.sendEvent(EventTest1(True))
        sender2.sendEvent(EventTest2([True]))
        sleep(1)
        # todo:check
        client.unsubscribe(HOST, PORT, f"{func_name}_input1", f"{func_name}_input1")
        client.unsubscribe(HOST, PORT, f"{func_name}_input2", f"{func_name}_input2")

    def test_EventClient_subscribe_offset_minus_two(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(f"""
            share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input
        """)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventTest])
        client = EventClient([EventTest])
        client.subscribe(HOST, PORT, print, f"{func_name}_input", offset=-2)
        sender.sendEvent(EventTest(True))
        sender.sendEvent(EventTest(False))
        sleep(1)
        # todo:check
        client.unsubscribe(HOST, PORT, f"{func_name}_input")

    def test_EventClient_subscribe_offset_one(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(f"""
            share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input
        """)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventTest])
        client = EventClient([EventTest])
        sender.sendEvent(EventTest(True))
        client.subscribe(HOST, PORT, print, f"{func_name}_input", offset=1)
        sender.sendEvent(EventTest(False))
        sleep(1)
        # todo:check
        client.unsubscribe(HOST, PORT, f"{func_name}_input")

    def test_EventClient_unsubscribe_absent(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]

        client = EventClient([EventTest])
        with pytest.raises(RuntimeError, match='not exists'):
            client.unsubscribe(HOST, PORT, "absent")

    def test_EventClient_getSubscriptionTopics(self):
        class EventTest(Event):
            s_bool: Scalar[keys.DT_BOOL]

        func_name = inspect.currentframe().f_code.co_name
        self.__class__.conn.run(f"""
            share streamTable(array(STRING, 0) as eventType, array(BLOB, 0) as blobs) as {func_name}_input
        """)
        sender = EventSender(self.__class__.conn, f"{func_name}_input", [EventTest])
        client = EventClient([EventTest])
        sender.sendEvent(EventTest(True))
        client.subscribe(HOST, PORT, print, f"{func_name}_input")
        sender.sendEvent(EventTest(False))
        assert client.getSubscriptionTopics() == [HOST + "/" + str(PORT) + f"/{func_name}_input/"]
        sleep(1)
        # todo:check
        client.unsubscribe(HOST, PORT, f"{func_name}_input")


class TestEvent(object):
    conn: ddb.Session = ddb.Session(HOST, PORT, USER, PASSWD, enablePickle=False)

    def setup_method(self):
        try:
            self.__class__.conn.run("1")
        except RuntimeError:
            self.__class__.conn.connect(HOST, PORT, USER, PASSWD)

    def test_Event_array_vector(self):
        with pytest.raises(ValueError, match="ArrayVector is not supported"):
            class EventArrayVector(Event):
                test: Vector[keys.DT_BOOL_ARRAY]

    def test_Event_error_form(self):
        with pytest.raises(ValueError, match="Invalid data form"):
            class EventErrorType(Event):
                test: str

    def test_Event_scalar_type_not_support(self):
        with pytest.raises(ValueError, match="Invalid data type"):
            class EventScalarTypeNotSupport(Event):
                test: Scalar[keys.DT_ANY]

    def test_Event_vector_type_not_support(self):
        with pytest.raises(ValueError, match="Invalid data type"):
            class EventVectorTypeNotSupport(Event):
                test: Vector[keys.DT_ANY]

    def test_Event_scalar_decimal_miss_precision(self):
        with pytest.raises(ValueError, match="Must specify exparam for DECIMAL"):
            class Decimal32MissPrecision(Event):
                s_decimal32: Scalar[keys.DT_DECIMAL32]
        with pytest.raises(ValueError, match="Must specify exparam for DECIMAL"):
            class Decimal64MissPrecision(Event):
                s_decimal64: Scalar[keys.DT_DECIMAL64]
        with pytest.raises(ValueError, match="Must specify exparam for DECIMAL"):
            class Decimal128MissPrecision(Event):
                s_decimal128: Scalar[keys.DT_DECIMAL128]

    def test_Event_vector_decimal_miss_precision(self):
        with pytest.raises(ValueError, match="Must specify exparam for DECIMAL"):
            class Decimal32MissPrecision(Event):
                v_decimal32: Vector[keys.DT_DECIMAL32]
        with pytest.raises(ValueError, match="Must specify exparam for DECIMAL"):
            class Decimal64MissPrecision(Event):
                v_decimal64: Vector[keys.DT_DECIMAL64]
        with pytest.raises(ValueError, match="Must specify exparam for DECIMAL"):
            class Decimal128MissPrecision(Event):
                v_decimal128: Vector[keys.DT_DECIMAL128]
