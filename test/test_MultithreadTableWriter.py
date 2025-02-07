import decimal
import inspect
import re
import threading
import time

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from pandas._testing import assert_frame_equal

from basic_testing.prepare import random_string
from basic_testing.utils import operateNodes
from setup.settings import HOST, PORT, USER, PASSWD, HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER, \
    PORT_DNODE1


class TestMultithreadTableWriter:
    conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=False)

    def insert_grant(self, writer, id):
        for _i in range(10):
            writer.insert(np.datetime64("2016-01-12"), _i)

    def insert_different_data_type(self, writer, id):
        for _i in range(10):
            res = writer.insert("djflsjfdlk", _i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_HASH(self, writer, id):
        for _i in range(10):
            res = writer.insert(np.datetime64("2016-01-12"), _i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(np.datetime64("2016-02-12"), _i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_COMPO(self, writer, id):
        for _i in range(80):
            res = writer.insert(_i, "a", "y")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(_i, "a", "z")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_HASH_Huge(self, writer, id):
        for i in range(150):
            res = writer.insert(np.datetime64("2016-01-12"), i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(np.datetime64("2016-02-12"), i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_VALUE_Huge(self, writer, id):
        for i in range(150):
            res = writer.insert(i, "a")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(i, "b")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(i, "c")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_RANGE_Huge(self, writer, id):
        for i in range(150):
            res = writer.insert(i, 1)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(i, 7)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_LIST_Huge(self, writer, id):
        for i in range(150):
            res = writer.insert(i, "a")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(i, "d")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_COMPO_Huge(self, writer, id):
        for i in range(150):
            res = writer.insert(i, "a", "y")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(i, "a", "z")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_Dimensional(self, writer, id):
        for i in range(90):
            res = writer.insert(i, "a")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_Memory_Table(self, writer, id):
        for i in range(110):
            res = writer.insert(i, 1)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(i, 2)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_Keyed_Table(self, writer, id):
        for i in range(130):
            res = writer.insert(i, i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_Stream_Table(self, writer, id):
        for i in range(130):
            res = writer.insert(i, i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_TSDB_Huge(self, writer, id):
        for i in range(300):
            res = writer.insert(i, "a")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_data_size_between_1024_1048576(self, writer, id):
        for i in range(2000):
            res = writer.insert(i, i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_data_size_larger_than_1048576(self, writer, id):
        for i in range(150000):
            res = writer.insert(i, i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_more_than_200_cols(self, writer, id):
        for i in range(100):
            add_data = []
            for j in range(300):
                add_data.append(i)
            res = writer.insert(*add_data)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_date(self, writer, id):
        for i in range(300):
            col_datetime = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[D]")
            j = i % 2
            res = writer.insert(j, col_datetime[j])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_datetime(self, writer, id):
        for i in range(300):
            col_datetime = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[s]")
            j = i % 2
            res = writer.insert(j, col_datetime[j])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_timestamp(self, writer, id):
        for i in range(300):
            col_timestamp = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ms]")
            j = i % 2
            res = writer.insert(j, col_timestamp[j])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_nanotimestamp(self, writer, id):
        for i in range(300):
            col_nanotimestamp = np.array(["2012-06-15T15:30:10.008007006", "2013-06-15T17:30:10.008007006"],
                                         dtype="datetime64[ns]")
            j = i % 2
            res = writer.insert(j, col_nanotimestamp[j])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_all_data_type(self, writer, id):
        col_bool = [True, False, True, False, True, False]
        col_char = np.array([1, 2, 3, 4, 5, 6], dtype=np.int8)
        col_short = np.array([1, 2, 3, 4, 5, 6], dtype=np.int16)
        col_int = np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)
        col_long = np.array([1, 2, 3, 4, 5, 6], dtype=np.int64)
        col_date = np.array(["2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13"],
                            dtype="datetime64[D]")
        col_month = np.array(["2012-06", "2012-06", "2012-06", "2012-06", "2012-06", "2012-06"], dtype="datetime64[M]")
        col_time = np.array(
            ["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
             "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"], dtype="datetime64[ms]")
        col_second = np.array(
            ["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
             "2012-06-13T13:30:10", "2012-06-13T13:30:10"], dtype="datetime64[s]")
        col_minute = np.array(
            ["1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30",
             "1970-01-01T13:30"], dtype="datetime64[m]")
        col_datehour = np.array(
            ["2012-06-13T13:30", "2012-06-13T13:30", "2012-06-13T13:30", "2012-06-13T13:30", "2012-06-13T13:30",
             "2012-06-13T13:30"], dtype="datetime64[h]")
        col_datetime = np.array(
            ["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
             "2012-06-13T13:30:10", "2012-06-13T13:30:10"], dtype="datetime64[s]")
        col_timestamp = np.array(
            ["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
             "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"], dtype="datetime64[ms]")
        col_nanotime = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_nanotimestamp = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_float = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        col_double = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
        col_string = ["1", "2", "3", "4", "5", "6"]
        col_uuid = ["88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976",
                    "88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976",
                    "88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976"]
        col_int128 = ["af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484"]
        col_ipaddr = ["3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6"]
        col_decimal32 = np.array(
            [decimal.Decimal("NaN"), decimal.Decimal('0.000000'), decimal.Decimal('-1'), decimal.Decimal('2.000000'),
             decimal.Decimal('3.123123'), decimal.Decimal('4.0')], dtype='object')
        col_decimal64 = np.array(
            [decimal.Decimal("NaN"), decimal.Decimal('0.0000000000000000'), decimal.Decimal('-1.0000000000000000'),
             decimal.Decimal('2.0000000000000000'), decimal.Decimal('3.123123123123123'), decimal.Decimal('4.0')],
            dtype='object')
        col_decimal128 = np.array([decimal.Decimal("NaN"), decimal.Decimal('0.000000000000000000000000'),
                                   decimal.Decimal('-1.000000000000000000000000'), decimal.Decimal('2.000000'),
                                   decimal.Decimal('3.123123123123123123123123'), decimal.Decimal('4.0')],
                                  dtype='object')
        for i in range(6):
            res = writer.insert(col_bool[i], col_char[i], col_short[i], col_int[i], col_long[i], col_date[i],
                                col_month[i], col_time[i], col_second[i], col_minute[i], col_datehour[i],
                                col_datetime[i], col_timestamp[i], col_nanotime[i], col_nanotimestamp[i], col_float[i],
                                col_double[i], col_string[i], col_uuid[i], col_int128[i], col_ipaddr[i],
                                col_decimal32[i], col_decimal64[i], col_decimal128[i])
            if res.hasError():
                print("error:", res.errorInfo)
                break

    def insert_all_data_type_basic(self, writer, id):
        col_bool = [True, False, True, False, True, False]
        col_char = np.array([1, 2, 3, 4, 5, 6], dtype=np.int8)
        col_short = np.array([1, 2, 3, 4, 5, 6], dtype=np.int16)
        col_int = np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)
        col_long = np.array([1, 2, 3, 4, 5, 6], dtype=np.int64)
        col_date = np.array(["2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13"],
                            dtype="datetime64[D]")
        col_month = np.array(["2012-06", "2012-06", "2012-06", "2012-06", "2012-06", "2012-06"], dtype="datetime64[M]")
        col_time = np.array(
            ["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
             "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"], dtype="datetime64[ms]")
        col_second = np.array(
            ["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
             "2012-06-13T13:30:10", "2012-06-13T13:30:10"], dtype="datetime64[s]")
        col_minute = np.array(
            ["1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30",
             "1970-01-01T13:30"], dtype="datetime64[m]")
        col_datehour = np.array(
            ["2012-06-13T13:30", "2012-06-13T13:30", "2012-06-13T13:30", "2012-06-13T13:30", "2012-06-13T13:30",
             "2012-06-13T13:30"], dtype="datetime64[h]")
        col_datetime = np.array(
            ["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
             "2012-06-13T13:30:10", "2012-06-13T13:30:10"], dtype="datetime64[s]")
        col_timestamp = np.array(
            ["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
             "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"], dtype="datetime64[ms]")
        col_nanotime = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_nanotimestamp = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_float = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        col_double = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
        col_string = ["1", "2", "3", "4", "5", "6"]
        for i in range(6):
            res = writer.insert(col_bool[i], col_char[i], col_short[i], col_int[i], col_long[i], col_date[i],
                                col_month[i], col_time[i], col_second[i], col_minute[i], col_datehour[i],
                                col_datetime[i], col_timestamp[i], col_nanotime[i], col_nanotimestamp[i], col_float[i],
                                col_double[i], col_string[i])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_all_type_compress(self, writer, id):
        print("\ninsert_all_type_compress {}".format(id))
        col_bool = [True, False, True, False, True, False]
        col_char = np.array([1, 2, 3, 4, 5, 6], dtype=np.int8)
        col_short = np.array([1, 2, 3, 4, 5, 6], dtype=np.int16)
        col_int = np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)
        col_long = np.array([1, 2, 3, 4, 5, 6], dtype=np.int64)
        col_date = np.array(["2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13"],
                            dtype="datetime64[D]")
        col_month = np.array(["2012-06", "2012-06", "2012-06", "2012-06", "2012-06", "2012-06"], dtype="datetime64[M]")
        col_time = np.array(
            ["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
             "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"], dtype="datetime64[ms]")
        col_second = np.array(
            ["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
             "2012-06-13T13:30:10", "2012-06-13T13:30:10"], dtype="datetime64[s]")
        col_minute = np.array(
            ["1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30",
             "1970-01-01T13:30"], dtype="datetime64[m]")
        col_datehour = np.array(
            ["2012-06-13T13:30", "2012-06-13T13:30", "2012-06-13T13:30", "2012-06-13T13:30", "2012-06-13T13:30",
             "2012-06-13T13:30"], dtype="datetime64[h]")
        col_datetime = np.array(
            ["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
             "2012-06-13T13:30:10", "2012-06-13T13:30:10"], dtype="datetime64[s]")
        col_timestamp = np.array(
            ["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
             "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"], dtype="datetime64[ms]")
        col_nanotime = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_nanotimestamp = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_float = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        col_double = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
        col_string = ["1", "2", "3", "4", "5", "6"]
        col_arrayVector = [[1, 2], [1, 2], [1, 2], [1, 2], [1, 2], [1, 2]]
        for i in range(6):
            res = writer.insert(col_bool[i], col_char[i], col_short[i], col_int[i], col_long[i], col_date[i],
                                col_month[i], col_time[i], col_second[i], col_minute[i], col_datehour[i],
                                col_datetime[i], col_timestamp[i], col_nanotime[i], col_nanotimestamp[i], col_float[i],
                                col_double[i], col_string[i], col_arrayVector[i])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_NULL2(self, writer, id):
        sym = ['AAPL', 'AAPL', 'GOOG', 'GOOG', 'MSFT', 'MSFT', 'IBM', 'IBM', 'YHOO', 'YHOO']
        date = np.array(
            ['2012-01-01', 'NaT', '1965-07-25', 'NaT', '2020-12-23', '1970-01-01', 'NaT', 'NaT', 'NaT', '2009-08-05'],
            dtype="datetime64")
        qty1 = np.array([2, 3, 4, 5, 6, 7, 8, 9, 10, 11], dtype=np.int64)
        for i in range(10):
            res = writer.insert(sym[i], date[i], qty1[i])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def test_multithreadTableWriterTest_error_mode(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        with pytest.raises(RuntimeError, match='Unsupported mtw mode TEST'):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 100, 0.1,
                                         10, "date", mode="test")

    def test_multithreadTableWriterTest_error_compressMethods(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        with pytest.raises(RuntimeError, match='Unsupported compression method TEST'):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 100, 0.1,
                                         10, "date", compressMethods=["test"])

    def test_multithreadTableWriterTest_error_hostName(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter("1.1", PORT, USER, PASSWD, db_name, func_name, False, False, [], 100,
                                         0.1, 10, "date")

    def test_multithreadTableWriterTest_error_port(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, -5, USER, PASSWD, db_name, func_name, False, False, [], 100, 0.1,
                                         10, "date")

    def test_multithreadTableWriterTest_error_userID(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, func_name, PASSWD, db_name, func_name, False, False, [], 100,
                                         0.1, 10, "date")

    def test_multithreadTableWriterTest_error_password(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, "123", db_name, func_name, False, False, [], 100, 0.1,
                                         10, "date")

    def test_multithreadTableWriterTest_error_dbName(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 100, 0.1,
                                         10, "date")

    def test_multithreadTableWriterTest_error_tableName(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, "pd", False, False, [], 100, 0.1, 10,
                                         "date")
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_userid_no_grant_write(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        user = func_name[30:]
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        script_user_no_grant_write = f"""
            def test_user(){{
                try{{createUser("{user}", "123456")}}catch(ex){{}};go;
                grant("{user}", TABLE_READ, "*")
            }}
            rpc(getControllerAlias(),  test_user)
        """
        self.conn.run(script_user_no_grant_write)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, user, PASSWD, db_name, func_name, False, False, [],
                                              10, 0.1, 1, "date")
        self.insert_grant(writer, 1)
        writer.waitForThreadCompletion()
        assert writer.getStatus().errorCode == 'A5'
        pattern = re.compile(".*Failed to save the inserted data.*")
        errorInfo = writer.getStatus().errorInfo
        flag = pattern.match(errorInfo)
        assert flag is not None
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','{func_name}')")
        assert last["count"][0] == 0
        self.conn.dropDatabase(db_name)
        self.conn.run(f'rpc(getControllerAlias(),  deleteUser,  "{user}")')

    def test_multithreadTableWriterTest_batchSize_negative_number(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], -1, 0.1,
                                         10, "date")
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_batchSize_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 0, 0.1,
                                         10, "date")
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_throttle_negative_number(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 100, -10,
                                         10, "date")
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_threadCount_negative_number(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 100, 0.1,
                                         -10, "date")
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_threadCount_zero(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 100, 0.1,
                                         0, "date")
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_Memory_Table_mutilthread_unspecified_partitioncol(self):
        func_name = inspect.currentframe().f_code.co_name
        script_Memory_Table = f"""
            t = table(1000:0, `id`x, [LONG, INT])
            share t as `{func_name}
        """
        self.conn.run(script_Memory_Table)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, func_name, "", False, False, [], 100, 0.1, 3, "")

    def test_multithreadTableWriterTest_DFS_Table_mutilthread_specified_not_partitioncol(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 100, 0.1,
                                         10, "id")
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_insert_different_data_type(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 1, 0.1, 1, "date")
        threads = []
        for i in range(1):
            threads.append(threading.Thread(
                target=self.insert_different_data_type, args=(writer, i,)))
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        time.sleep(2)
        assert writer.getStatus().errorCode == 'A1'
        assert writer.getStatus().errorInfo == 'Data conversion error: Cannot convert str to DATE.'
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','{func_name}')")
        assert last["count"][0] == 0
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_Memory_Table_dbName_empty(self):
        func_name = inspect.currentframe().f_code.co_name
        script_Memory_Table = f"""
            t = table(1000:0, `id`x, [LONG, INT])
            share t as `{func_name}
        """
        self.conn.run(script_Memory_Table)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, func_name, "", False, False, [], 100, 0.1, 3, "id")

    def test_multithreadTableWriterTest_function_getUnwrittenData(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            share t as `{func_name}
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, batchSize=100, threadCount=5, partitionCol="date")
        for i in range(10):
            res = writer.insert(np.datetime64(f'2022-03-2{i}'), 999999999999999999 + i, "str" + str(i),
                                "sym" + str(i)) if i == 5 \
                else writer.insert(np.datetime64(f'2022-03-2{i}'), 1, "str" + str(i), "sym" + str(i))
            if res.hasError():
                print("insert error: ", res.errorInfo)
        writer.waitForThreadCompletion()
        status = writer.getStatus()
        assert status.sendFailedRows + status.unsentRows == 10
        datas = writer.getUnwrittenData()
        assert datas[5][0] == np.datetime64('2022-03-25')
        assert datas[5][1] == 1000000000000000004
        assert datas[5][2] == "str5"
        assert datas[5][3] == "sym5"

    def test_multithreadTableWriterTest_function_getUnwrittenData_1(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            share t as `{func_name}
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, batchSize=100, threadCount=5, partitionCol="date")
        for i in range(10):
            res = writer.insert(np.datetime64(f'2022-03-2{i}'), -1.356468, "str" + str(i), "sym" + str(i)) if i == 5 \
                else writer.insert(np.datetime64(f'2022-03-2{i}'), 1, "str" + str(i), "sym" + str(i))
            if res.hasError():
                print("insert error: ", res.errorInfo)
        writer.waitForThreadCompletion()
        status2 = writer.getStatus()
        assert status2.sendFailedRows + status2.unsentRows == 10
        datas2 = writer.getUnwrittenData()
        assert datas2[5][0] == np.datetime64('2022-03-25')
        assert datas2[5][1] == -1.356468
        assert datas2[5][2] == "str5"
        assert datas2[5][3] == "sym5"

    def test_multithreadTableWriterTest_function_getUnwrittenData_2(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            t = table(100:0, [`date, `int, `string, `ipaddr], [DATE, INT, STRING, IPADDR]);
            share t as `{func_name}
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, batchSize=100, threadCount=5, partitionCol="date")
        for i in range(10):
            res = writer.insert(np.datetime64(f'2022-03-2{i}'), i, "str" + str(i), "abcd") if i == 5 \
                else writer.insert(np.datetime64(f'2022-03-2{i}'), 1, "str" + str(i), f"192.168.0.{i}")
            if res.hasError():
                print("insert error: ", res.errorInfo)
        writer.waitForThreadCompletion()
        status2 = writer.getStatus()
        assert status2.sendFailedRows + status2.unsentRows == 10
        datas2 = writer.getUnwrittenData()
        assert datas2[5][0] == np.datetime64('2022-03-25')
        assert datas2[5][1] == 5
        assert datas2[5][2] == "str5"
        assert datas2[5][3] == "abcd"

    def test_multithreadTableWriterTest_function_getUnwrittenData_no_wrong_data(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            share t as `{func_name}
        """
        self.conn.run(script)

        def insert_task(rows, thread_count):
            writer = ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, "", func_name, batchSize=100, threadCount=thread_count, partitionCol="date")
            for i in range(rows):
                res = writer.insert(np.datetime64(f'2022-03-2{i % 10}'), 1, "str" + str(i), "sym" + str(i))
                if res.hasError():
                    print("insert error: ", res.errorInfo)
            writer.waitForThreadCompletion()
            status = writer.getStatus()
            assert status.sendFailedRows + status.unsentRows == 0
            assert status.sentRows == rows
            datas = writer.getUnwrittenData()
            assert len(datas) == 0

        tests = [[10000, 1], [100000, 10], [1000000, 15], [2000000, 20]]
        for cp in tests:
            row = cp[0]
            th_count = cp[1]
            insert_task(row, th_count)

    def test_multithreadTableWriterTest_function_getUnwrittenData_dfs(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script = f"""
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            dbpath = "{db_name}"
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};go
            db=database(dbpath,VALUE,2022.03.20..2022.03.29);
            tmp=db.createPartitionedTable(t,`tmp,`date);go
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "tmp", batchSize=1000, threadCount=10, partitionCol="date")
        for i in range(100):
            res = writer.insert(np.datetime64(f'2022-03-2{i % 10}'), 999999999999999999 + i, "str" + str(i),
                                "sym" + str(i)) if i == 5 \
                else writer.insert(np.datetime64(f'2022-03-2{i % 10}'), 1, "str" + str(i), "sym" + str(i))
            if res.hasError():
                print("insert error: ", res.errorInfo)
        writer.waitForThreadCompletion()
        status = writer.getStatus()
        assert status.sendFailedRows + status.unsentRows == 100
        datas = writer.getUnwrittenData()
        assert len(datas) == 100
        assert datas[5][0] == np.datetime64('2022-03-25')
        assert datas[5][1] == 1000000000000000004
        assert datas[5][2] == "str5"
        assert datas[5][3] == "sym5"
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_function_getUnwrittenData_dfs_1(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script = f"""
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            dbpath = "{db_name}"
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};go
            db=database(dbpath,VALUE,2022.03.20..2022.03.29,,'TSDB');
            db.createPartitionedTable(t,`mtw,`date,,`int);go
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "mtw", batchSize=1000, threadCount=10, partitionCol="date")
        for i in range(100):
            res = writer.insert(np.datetime64(f'2022-03-2{i % 10}'), -1.356468, "str" + str(i),
                                "sym" + str(i)) if i == 5 \
                else writer.insert(np.datetime64(f'2022-03-2{i % 10}'), 1, "str" + str(i), "sym" + str(i))
            if res.hasError():
                print("insert error: ", res.errorInfo)
        writer.waitForThreadCompletion()
        status2 = writer.getStatus()
        assert status2.sendFailedRows + status2.unsentRows == 100
        datas2 = writer.getUnwrittenData()
        assert datas2[5][0] == np.datetime64('2022-03-25')
        assert datas2[5][1] == -1.356468
        assert datas2[5][2] == "str5"
        assert datas2[5][3] == "sym5"

    def test_multithreadTableWriterTest_function_getUnwrittenData_dfs_2(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script = f"""
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            dbpath = "{db_name}"
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};go
            db=database(dbpath,VALUE,2022.03.20..2022.03.29,,'TSDB');
            db.createPartitionedTable(t,`mtw,`date,,`int);go
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, "mtw", batchSize=1000,
                                              threadCount=10, partitionCol="date", mode="APPEND")
        for i in range(100):
            res = writer.insert(np.datetime64(f'2022-03-2{i % 10}'), str(i), "str" + str(i),
                                "abcd") if i == 5 else writer.insert(np.datetime64(f'2022-03-2{i % 10}'), 1,
                                                                     "str" + str(i), f"192.168.0.{i}")
            if res.hasError():
                print("insert error: ", res.errorInfo)
        writer.waitForThreadCompletion()
        status2 = writer.getStatus()
        assert status2.sendFailedRows + status2.unsentRows == 100
        datas2 = writer.getUnwrittenData()
        assert datas2[5][0] == np.datetime64('2022-03-25')
        assert datas2[5][1] == "5"
        assert datas2[5][2] == "str5"
        assert datas2[5][3] == "abcd"

    def test_multithreadTableWriterTest_object_MultithreadedTableWriterThreadStatus(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            t = table(100:0, [`int, `str], [INT, STRING]);
            share t as `{func_name}
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name)
        col_int = [1, 2, 3]
        col_str = ['dolphindb', 'apple', 'google']
        for i in range(3):
            writer.insert(col_int[i], col_int[i])
        writer.waitForThreadCompletion()
        status = writer.getStatus()
        assert status.hasError()
        assert status.sentRows == 0
        assert status.sendFailedRows + status.unsentRows == 3
        assert status.errorCode != ''
        assert status.errorInfo != ''
        writer2 = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name)
        for i in range(3):
            writer2.insert(col_int[i], col_str[i])
        writer2.waitForThreadCompletion()
        status2 = writer2.getStatus()
        assert not status2.hasError()
        assert status2.sentRows == 3
        assert status2.sendFailedRows == 0
        assert status2.errorCode == ''
        assert status2.errorInfo == ''

    def test_multithreadTableWriterTest_all_data_type(self):
        func_name = inspect.currentframe().f_code.co_name
        script_all_data_type = f"""
            t = table(1000:0,
            `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`csecond`cminute`cdatehour`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`cstring`cuuid`cint128`cip`cdecimal32`cdecimal64`cdecimal128,
            [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, INT128, IPADDR, DECIMAL32(6), DECIMAL64(16), DECIMAL128(24)])
            share t as `{func_name}
        """
        self.conn.run(script_all_data_type)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 100, 0.1, 1, "")
        self.conn.run(f"delete from {func_name}")
        first = self.conn.run(f"select count(*) from {func_name}")
        self.insert_all_data_type(writer, 1)
        writer.waitForThreadCompletion()
        st = writer.getStatus()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run(f"select * from {func_name}")
        col_bool = [True, False, True, False, True, False]
        col_char = np.array([1, 2, 3, 4, 5, 6], dtype=np.int8)
        col_short = np.array([1, 2, 3, 4, 5, 6], dtype=np.int16)
        col_int = np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)
        col_long = np.array([1, 2, 3, 4, 5, 6], dtype=np.int64)
        col_date = np.array(["2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13"],
                            dtype="datetime64[ns]")
        col_month = np.array(["2012-06", "2012-06", "2012-06", "2012-06", "2012-06", "2012-06"], dtype="datetime64[ns]")
        col_time = np.array(
            ["1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008",
             "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008"], dtype="datetime64[ns]")
        col_second = np.array(
            ["1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10",
             "1970-01-01T13:30:10", "1970-01-01T13:30:10"], dtype="datetime64[ns]")
        col_minute = np.array(
            ["1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30",
             "1970-01-01T13:30"], dtype="datetime64[ns]")
        col_datehour = np.array(
            ["2012-06-13T13", "2012-06-13T13", "2012-06-13T13", "2012-06-13T13", "2012-06-13T13", "2012-06-13T13"],
            dtype="datetime64[ns]")
        col_datetime = np.array(
            ["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
             "2012-06-13T13:30:10", "2012-06-13T13:30:10"], dtype="datetime64[ns]")
        col_timestamp = np.array(
            ["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
             "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"], dtype="datetime64[ns]")
        col_nanotime = np.array(
            ["1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006",
             "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_nanotimestamp = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_float = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        col_double = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
        col_string = ["1", "2", "3", "4", "5", "6"]
        col_uuid = ["88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976",
                    "88b4ac61-1a43-94ca-1352-4da53cda28bd",
                    "9e495846-1e79-2ca1-bb9b-cf62c3556976", "88b4ac61-1a43-94ca-1352-4da53cda28bd",
                    "9e495846-1e79-2ca1-bb9b-cf62c3556976"]
        col_int128 = ["af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484"]
        col_ipaddr = ["3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6"]
        col_decimal32 = np.array([None, decimal.Decimal('0.000000'), decimal.Decimal('-1.000000'),
                                  decimal.Decimal('2.000000'), decimal.Decimal('3.123123'),
                                  decimal.Decimal('4.000000')], dtype='object')
        col_decimal64 = np.array([None, decimal.Decimal('0.0000000000000000'), decimal.Decimal('-1.0000000000000000'),
                                  decimal.Decimal('2.0000000000000000'), decimal.Decimal('3.123123123123123'),
                                  decimal.Decimal('4.0')], dtype='object')
        col_decimal128 = np.array(
            [None, decimal.Decimal('0.000000000000000000000000'), decimal.Decimal('-1.000000000000000000000000'),
             decimal.Decimal('2.000000'), decimal.Decimal('3.123123123123123123123123'), decimal.Decimal('4.0')],
            dtype='object')
        ex = pd.DataFrame({
            "cbool": col_bool,
            "cchar": col_char,
            "cshort": col_short,
            "cint": col_int,
            "clong": col_long,
            "cdate": col_date,
            "cmonth": col_month,
            "ctime": col_time,
            "csecond": col_second,
            "cminute": col_minute,
            "cdatehour": col_datehour,
            "cdatetime": col_datetime,
            "ctimestamp": col_timestamp,
            "cnanotime": col_nanotime,
            "cnanotimestamp": col_nanotimestamp,
            "cfloat": col_float,
            "cdouble": col_double,
            "cstring": col_string,
            "cuuid": col_uuid,
            "cint128": col_int128,
            "cip": col_ipaddr,
            "cdecimal32": col_decimal32,
            "cdecimal64": col_decimal64,
            "cdecimal128": col_decimal128
        })
        assert_frame_equal(re, ex)

    def test_multithreadTableWriterTest_keyedTable_upsert_all_data_type(self):
        func_name = inspect.currentframe().f_code.co_name
        script_all_data_type = f"""
            t1 = keyedTable(`int,1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`uuid`int128`ip`decimal32`decimal64`decimal128, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, INT128, IPADDR,DECIMAL32(2),DECIMAL64(2),DECIMAL128(2)])
            share t1 as `{func_name}
        """
        self.conn.run(script_all_data_type)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 1, 0.1,
                                              1, "int", mode="upsert",
                                              modeOption=["ignoreNull=false", "keyColNames=`int"])
        first = self.conn.run(f"select count(*) from {func_name}")
        self.insert_all_data_type(writer, 1)
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run(f"select * from {func_name}")
        col_bool = [True, False, True, False, True, False]
        col_char = np.array([1, 2, 3, 4, 5, 6], dtype=np.int8)
        col_short = np.array([1, 2, 3, 4, 5, 6], dtype=np.int16)
        col_int = np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)
        col_long = np.array([1, 2, 3, 4, 5, 6], dtype=np.int64)
        col_date = np.array(["2013-06-13", "2013-06-13", "2013-06-13",
                             "2013-06-13", "2013-06-13", "2013-06-13"], dtype="datetime64[ns]")
        col_month = np.array(["2012-06", "2012-06", "2012-06",
                              "2012-06", "2012-06", "2012-06"], dtype="datetime64[ns]")
        col_time = np.array(["1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008",
                             "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008"],
                            dtype="datetime64[ns]")
        col_second = np.array(["1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10",
                               "1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10"],
                              dtype="datetime64[ns]")
        col_minute = np.array(["1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30",
                               "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30"], dtype="datetime64[ns]")
        col_datehour = np.array(["2012-06-13T13", "2012-06-13T13", "2012-06-13T13",
                                 "2012-06-13T13", "2012-06-13T13", "2012-06-13T13"], dtype="datetime64[ns]")
        col_datetime = np.array(["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
                                 "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10"],
                                dtype="datetime64[ns]")
        col_timestamp = np.array(["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
                                  "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"],
                                 dtype="datetime64[ns]")
        col_nanotime = np.array(
            ["1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006",
             "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_nanotimestamp = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_float = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        col_double = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
        col_string = ["1", "2", "3", "4", "5", "6"]
        col_uuid = ["88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976",
                    "88b4ac61-1a43-94ca-1352-4da53cda28bd",
                    "9e495846-1e79-2ca1-bb9b-cf62c3556976", "88b4ac61-1a43-94ca-1352-4da53cda28bd",
                    "9e495846-1e79-2ca1-bb9b-cf62c3556976"]
        col_int128 = ["af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484"]
        col_ipaddr = ["3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6"]
        col_decimal32 = np.array([None, decimal.Decimal('0.00'), decimal.Decimal('-1.00'),
                                  decimal.Decimal('2.00'), decimal.Decimal('3.12'), decimal.Decimal('4.00')],
                                 dtype='object')
        col_decimal64 = np.array([None, decimal.Decimal('0.00'), decimal.Decimal('-1.00'),
                                  decimal.Decimal('2.00'), decimal.Decimal('3.12'), decimal.Decimal('4.00')],
                                 dtype='object')
        col_decimal128 = np.array([None, decimal.Decimal('0.00'), decimal.Decimal('-1.00'),
                                   decimal.Decimal('2.00'), decimal.Decimal('3.12'), decimal.Decimal('4.00')],
                                  dtype='object')
        ex = pd.DataFrame({
            "bool": col_bool,
            "char": col_char,
            "short": col_short,
            "int": col_int,
            "long": col_long,
            "date": col_date,
            "month": col_month,
            "time": col_time,
            "second": col_second,
            "minute": col_minute,
            "datehour": col_datehour,
            "datetime": col_datetime,
            "timestamp": col_timestamp,
            "nanotime": col_nanotime,
            "nanotimestamp": col_nanotimestamp,
            "float": col_float,
            "double": col_double,
            "string": col_string,
            "uuid": col_uuid,
            "int128": col_int128,
            "ip": col_ipaddr,
            'decimal32': col_decimal32,
            'decimal64': col_decimal64,
            'decimal128': col_decimal128
        })
        assert_frame_equal(re, ex)

    def test_multithreadTableWriterTest_indexedTable_upsert_all_data_type(self):
        func_name = inspect.currentframe().f_code.co_name
        script_all_data_type = f"""
            t1 = indexedTable(`int,1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`uuid`int128`ip`decimal32`decimal64`decimal128, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, INT128, IPADDR,DECIMAL32(2),DECIMAL64(2),DECIMAL128(2)])
            share t1 as `{func_name}
        """
        self.conn.run(script_all_data_type)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 1, 0.1,
                                              1, "int", mode="upsert",
                                              modeOption=["ignoreNull=false", "keyColNames=`int"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_all_data_type, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run(f"select * from {func_name}")
        col_bool = [True, False, True, False, True, False]
        col_char = np.array([1, 2, 3, 4, 5, 6], dtype=np.int8)
        col_short = np.array([1, 2, 3, 4, 5, 6], dtype=np.int16)
        col_int = np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)
        col_long = np.array([1, 2, 3, 4, 5, 6], dtype=np.int64)
        col_date = np.array(["2013-06-13", "2013-06-13", "2013-06-13",
                             "2013-06-13", "2013-06-13", "2013-06-13"], dtype="datetime64[ns]")
        col_month = np.array(["2012-06", "2012-06", "2012-06",
                              "2012-06", "2012-06", "2012-06"], dtype="datetime64[ns]")
        col_time = np.array(["1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008",
                             "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008"],
                            dtype="datetime64[ns]")
        col_second = np.array(["1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10",
                               "1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10"],
                              dtype="datetime64[ns]")
        col_minute = np.array(["1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30",
                               "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30"], dtype="datetime64[ns]")
        col_datehour = np.array(["2012-06-13T13", "2012-06-13T13", "2012-06-13T13",
                                 "2012-06-13T13", "2012-06-13T13", "2012-06-13T13"], dtype="datetime64[ns]")
        col_datetime = np.array(["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
                                 "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10"],
                                dtype="datetime64[ns]")
        col_timestamp = np.array(["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
                                  "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"],
                                 dtype="datetime64[ns]")
        col_nanotime = np.array(
            ["1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006",
             "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_nanotimestamp = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_float = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        col_double = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
        col_string = ["1", "2", "3", "4", "5", "6"]
        col_uuid = ["88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976",
                    "88b4ac61-1a43-94ca-1352-4da53cda28bd",
                    "9e495846-1e79-2ca1-bb9b-cf62c3556976", "88b4ac61-1a43-94ca-1352-4da53cda28bd",
                    "9e495846-1e79-2ca1-bb9b-cf62c3556976"]
        col_int128 = ["af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484"]
        col_ipaddr = ["3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6"]
        col_decimal32 = np.array([None, decimal.Decimal('0.00'), decimal.Decimal('-1.00'),
                                  decimal.Decimal('2.00'), decimal.Decimal('3.12'), decimal.Decimal('4.00')],
                                 dtype='object')
        col_decimal64 = np.array([None, decimal.Decimal('0.00'), decimal.Decimal('-1.00'),
                                  decimal.Decimal('2.00'), decimal.Decimal('3.12'), decimal.Decimal('4.00')],
                                 dtype='object')
        col_decimal128 = np.array([None, decimal.Decimal('0.00'), decimal.Decimal('-1.00'),
                                   decimal.Decimal('2.00'), decimal.Decimal('3.12'), decimal.Decimal('4.00')],
                                  dtype='object')
        ex = pd.DataFrame({
            "bool": col_bool,
            "char": col_char,
            "short": col_short,
            "int": col_int,
            "long": col_long,
            "date": col_date,
            "month": col_month,
            "time": col_time,
            "second": col_second,
            "minute": col_minute,
            "datehour": col_datehour,
            "datetime": col_datetime,
            "timestamp": col_timestamp,
            "nanotime": col_nanotime,
            "nanotimestamp": col_nanotimestamp,
            "float": col_float,
            "double": col_double,
            "string": col_string,
            "uuid": col_uuid,
            "int128": col_int128,
            "ip": col_ipaddr,
            'decimal32': col_decimal32,
            'decimal64': col_decimal64,
            'decimal128': col_decimal128
        })
        assert_frame_equal(re, ex)

    def test_multithreadTableWriterTest_dfs_upsert_all_data_type(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_all_data_type = f"""
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`uuid`int128`ip`decimal32`decimal64`decimal128, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, INT128, IPADDR,DECIMAL32(2),DECIMAL64(2),DECIMAL128(2)])
            db=database(dbPath,RANGE,1 3 7,engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,sortColumns=`int)
        """
        self.conn.run(script_all_data_type)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 1, 0.1, 1,
                                              "int", mode="upsert", modeOption=["ignoreNull=false", "keyColNames=`int"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_all_data_type, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        col_bool = [True, False, True, False, True, False]
        col_char = np.array([1, 2, 3, 4, 5, 6], dtype=np.int8)
        col_short = np.array([1, 2, 3, 4, 5, 6], dtype=np.int16)
        col_int = np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)
        col_long = np.array([1, 2, 3, 4, 5, 6], dtype=np.int64)
        col_date = np.array(["2013-06-13", "2013-06-13", "2013-06-13",
                             "2013-06-13", "2013-06-13", "2013-06-13"], dtype="datetime64[ns]")
        col_month = np.array(["2012-06", "2012-06", "2012-06",
                              "2012-06", "2012-06", "2012-06"], dtype="datetime64[ns]")
        col_time = np.array(["1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008",
                             "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008"],
                            dtype="datetime64[ns]")
        col_second = np.array(["1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10",
                               "1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10"],
                              dtype="datetime64[ns]")
        col_minute = np.array(["1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30",
                               "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30"], dtype="datetime64[ns]")
        col_datehour = np.array(["2012-06-13T13", "2012-06-13T13", "2012-06-13T13",
                                 "2012-06-13T13", "2012-06-13T13", "2012-06-13T13"], dtype="datetime64[ns]")
        col_datetime = np.array(["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
                                 "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10"],
                                dtype="datetime64[ns]")
        col_timestamp = np.array(["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
                                  "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"],
                                 dtype="datetime64[ns]")
        col_nanotime = np.array(
            ["1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006",
             "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_nanotimestamp = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_float = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        col_double = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
        col_string = ["1", "2", "3", "4", "5", "6"]
        col_uuid = ["88b4ac61-1a43-94ca-1352-4da53cda28bd", "9e495846-1e79-2ca1-bb9b-cf62c3556976",
                    "88b4ac61-1a43-94ca-1352-4da53cda28bd",
                    "9e495846-1e79-2ca1-bb9b-cf62c3556976", "88b4ac61-1a43-94ca-1352-4da53cda28bd",
                    "9e495846-1e79-2ca1-bb9b-cf62c3556976"]
        col_int128 = ["af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484", "af5cad08c356296a0544b6bf11556484",
                      "af5cad08c356296a0544b6bf11556484"]
        col_ipaddr = ["3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6", "3d5b:14af:b811:c475:5c90:f554:45aa:98a6",
                      "3d5b:14af:b811:c475:5c90:f554:45aa:98a6"]
        col_decimal32 = np.array([None, decimal.Decimal('0.00'), decimal.Decimal('-1.00'),
                                  decimal.Decimal('2.00'), decimal.Decimal('3.12'), decimal.Decimal('4.00')],
                                 dtype='object')
        col_decimal64 = np.array([None, decimal.Decimal('0.00'), decimal.Decimal('-1.00'),
                                  decimal.Decimal('2.00'), decimal.Decimal('3.12'), decimal.Decimal('4.00')],
                                 dtype='object')
        col_decimal128 = np.array([None, decimal.Decimal('0.00'), decimal.Decimal('-1.00'),
                                   decimal.Decimal('2.00'), decimal.Decimal('3.12'), decimal.Decimal('4.00')],
                                  dtype='object')
        ex = pd.DataFrame({
            "bool": col_bool,
            "char": col_char,
            "short": col_short,
            "int": col_int,
            "long": col_long,
            "date": col_date,
            "month": col_month,
            "time": col_time,
            "second": col_second,
            "minute": col_minute,
            "datehour": col_datehour,
            "datetime": col_datetime,
            "timestamp": col_timestamp,
            "nanotime": col_nanotime,
            "nanotimestamp": col_nanotimestamp,
            "float": col_float,
            "double": col_double,
            "string": col_string,
            "uuid": col_uuid,
            "int128": col_int128,
            "ip": col_ipaddr,
            'decimal32': col_decimal32,
            'decimal64': col_decimal64,
            'decimal128': col_decimal128
        })
        assert_frame_equal(re, ex)
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_dfs_upsert_key_exist_ignoreNull_True(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script = f"""
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
            db=database(dbPath,RANGE,1 6 12,engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `qty,sortColumns=`qty)
            pt.append!(t)
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 1, 0.1, 1,
                                              "qty", mode="upsert", modeOption=["ignoreNull=true", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 1
        script = f'''
            re = select * from loadTable('{db_name}','pt')
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_dfs_upsert_key_exist_ignoreNull_False(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script = f"""
            dbPath = "{db_name}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
            db=database(dbPath,RANGE,1 6 12,engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `qty,sortColumns=`qty)
            pt.append!(t)
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 1, 0.1, 1,
                                              "qty", mode="upsert", modeOption=["ignoreNull=false", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(
                target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run(
            f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 1
        script = f'''
            re = select * from loadTable('{db_name}','pt')
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_keyedTable_upsert_key_exist_ignoreNull_True(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            t1 = keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])
            insert into t1 values(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO,[2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05], 1..10 )
            share t1 as `{func_name}
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 1, 0.1, 1,
                                              "qty",
                                              mode="upsert", modeOption=["ignoreNull=true", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(
                target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 1
        script = f'''
            re = select * from {func_name}
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])

    def test_multithreadTableWriterTest_keyedTable_upsert_key_exist_ignoreNull_False(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            t1 = keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])
            insert into t1 values(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO,[2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05], 1..10 )
            share t1 as `{func_name}
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 1, 0.1, 1,
                                              "qty",
                                              mode="upsert", modeOption=["ignoreNull=false", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 1
        script = f'''
            re = select * from {func_name}
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])

    def test_multithreadTableWriterTest_indexedTable_upsert_key_exist_ignoreNull_True(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            t1 = indexedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])
            insert into t1 values(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO,[2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05], 1..10 )
            share t1 as `{func_name}
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 1, 0.1, 1,
                                              "qty",
                                              mode="upsert", modeOption=["ignoreNull=true", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 1
        script = f'''
            re = select * from {func_name}
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])

    def test_multithreadTableWriterTest_indexedTable_upsert_key_exist_ignoreNull_False(self):
        func_name = inspect.currentframe().f_code.co_name
        script = f"""
            t1 = indexedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])
            insert into t1 values(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO,[2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05], 1..10 )
            share t1 as `{func_name}
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 1, 0.1, 1,
                                              "qty",
                                              mode="upsert", modeOption=["ignoreNull=false", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 1
        script = f'''
            re = select * from {func_name}
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])

    def test_multithreadTableWriterTest_batchSize_throttle(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_COMPO_batchSize_throttle = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x`y, [LONG, STRING, STRING])
            dbX = database(, VALUE, `a`b`c)
            dbY = database(, LIST, [`x`y, `z])
            db = database("{db_name}", COMPO, [dbX, dbY])
            pt= db.createPartitionedTable(t, `pt, `x`y)
        """
        self.conn.run(script_DFS_COMPO_batchSize_throttle)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name,
                                              "pt", False, False, [], 2000, 5, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_COMPO, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 0
        while writer.getStatus().unsentRows != 0:
            time.sleep(0.5)
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 1600

    def test_multithreadTableWriterTest_AllData(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_RANGE_ALLDATA = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, INT])
            db = database("{db_name}", RANGE, 0 5 10)
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_DFS_RANGE_ALLDATA)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 1, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_RANGE_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000

    def test_multithreadTableWriterTest_getStatus_waitfor(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 1, 0.1, 1, "date")
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_DFS_HASH, args=(writer, i,)))
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        assert writer.getStatus().isExiting
        assert writer.getStatus().errorCode == ''
        assert writer.getStatus().errorInfo == ''
        assert writer.getStatus().sentRows == 20
        assert writer.getStatus().unsentRows == 0
        assert writer.getStatus().sendFailedRows == 0
        assert len(writer.getStatus().threadStatus) == 2
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_getStatus_time(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 1,
                                              0.1, 1, "date")
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_DFS_HASH, args=(writer, i,)))
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        while writer.getStatus().unsentRows:
            time.sleep(1)
        assert not writer.getStatus().isExiting
        assert writer.getStatus().errorCode == ''
        assert writer.getStatus().errorInfo == ''
        assert writer.getStatus().sentRows == 20
        assert writer.getStatus().unsentRows == 0
        assert writer.getStatus().sendFailedRows == 0
        assert len(writer.getStatus().threadStatus) == 2
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_Memory_Table_single_thread(self):
        func_name = inspect.currentframe().f_code.co_name
        script_Memory_Table = f"""
            t = table(1000:0, `id`x, [LONG, INT])
            share t as `{func_name}
        """
        self.conn.run(script_Memory_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 100, 0.1, 1, "id")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(
                target=self.insert_Memory_Table, args=(writer, i,)))
        self.conn.run(f"delete from {func_name}")
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 2200
        re = self.conn.run(f"select * from {func_name}")
        id = re["id"].to_list()
        id.sort()
        x = re["x"].to_list()
        x.sort()
        ex_id = list(range(110)) * 20
        ex_id.sort()
        ex_x = [1, 2] * 1100
        ex_x.sort()
        assert id == ex_id
        assert x == ex_x

    def test_multithreadTableWriterTest_Memory_Table_mutil_thread(self):
        func_name = inspect.currentframe().f_code.co_name
        script_Memory_Table = f"""
            t = table(1000:0, `id`x, [LONG, INT])
            share t as `{func_name}
        """
        self.conn.run(script_Memory_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 100, 0.1, 10, "id")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(
                target=self.insert_Memory_Table, args=(writer, i,)))
        self.conn.run(f"delete from {func_name}")
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 2200
        re = self.conn.run(f"select * from {func_name}")
        id = re["id"].to_list()
        id.sort()
        x = re["x"].to_list()
        x.sort()
        ex_id = list(range(110)) * 20
        ex_id.sort()
        ex_x = [1, 2] * 1100
        ex_x.sort()
        assert id == ex_id
        assert x == ex_x

    def test_multithreadTableWriterTest_Keyed_Table(self):
        func_name = inspect.currentframe().f_code.co_name
        script_Keyed_Table = f"""
            tmp = table(1000:0, `id`x, [LONG, INT])
            tt = keyedTable(`id, tmp)
            share tt as `{func_name}
        """
        self.conn.run(script_Keyed_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 100, 0.1, 1, "id")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(
                target=self.insert_Keyed_Table, args=(writer, i,)))
        self.conn.run(f"delete from {func_name}")
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 130
        re = self.conn.run(f"select * from {func_name}")
        id = re["id"].to_list()
        id.sort()
        x = re["x"].to_list()
        x.sort()
        ex_id = list(range(130))
        ex_id.sort()
        ex_x = list(range(130))
        ex_x.sort()
        assert id == ex_id
        assert x == ex_x

    def test_multithreadTableWriterTest_stream_Table_single_thread(self):
        func_name = inspect.currentframe().f_code.co_name
        script_Stream_Table = f"""
            tmp = streamTable(1000:0, `id`x, [LONG, INT])
            share tmp as `{func_name}
        """
        self.conn.run(script_Stream_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 100, 0.1, 1, "id")
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_Stream_Table, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 130
        re = self.conn.run(f"select * from {func_name}")
        id = re["id"].to_list()
        id.sort()
        x = re["x"].to_list()
        x.sort()
        ex_id = list(range(130))
        ex_id.sort()
        ex_x = list(range(130))
        ex_x.sort()
        assert id == ex_id
        assert x == ex_x

    def test_multithreadTableWriterTest_stream_Table_multi_thread(self):
        func_name = inspect.currentframe().f_code.co_name
        script_Stream_Table = f"""
            tmp = streamTable(1000:0, `id`x, [LONG, INT])
            share tmp as `{func_name}
        """
        self.conn.run(script_Stream_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 100, 0.1, 10, "id")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_Stream_Table, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 1300
        re = self.conn.run(f"select * from {func_name}")
        id = re["id"].to_list()
        id.sort()
        x = re["x"].to_list()
        x.sort()
        ex_id = list(range(130)) * 10
        ex_id.sort()
        ex_x = list(range(130)) * 10
        ex_x.sort()
        assert id == ex_id
        assert x == ex_x

    def test_multithreadTableWriterTest_DFS_HASH(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_HASH = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{db_name}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'{func_name}','date')
        """
        self.conn.run(script_DFS_HASH)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, func_name, False, False, [], 100,
                                              0.1, 10, "date")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_HASH_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','{func_name}')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','{func_name}')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','{func_name}')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select date from loadTable('{db_name}','{func_name}')")
        date = []
        date.extend([np.datetime64("2016-02-12"),
                     np.datetime64("2016-01-12")] * 1500)
        date.sort()
        ex = pd.DataFrame({
            "date": date,
        })
        assert_frame_equal(re, ex)
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_DFS_VALUE(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_VALUE = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`name, [LONG, STRING])
            db = database("{db_name}", VALUE, `a`b`c)
            pt= db.createPartitionedTable(t, `pt, `name)
        """
        self.conn.run(script_DFS_VALUE)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "name")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_VALUE_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 4500
        re = self.conn.run(f"select name from loadTable('{db_name}','pt')")
        name = ["a", "b", "c"] * 1500
        name.sort()
        ex = pd.DataFrame({
            "name": name
        })
        assert_frame_equal(re, ex)
        self.conn.dropDatabase(db_name)

    def test_multithreadTableWriterTest_DFS_RANGE(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_RANGE = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, INT])
            db = database("{db_name}", RANGE, 0 5 10)
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_DFS_RANGE)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_RANGE_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select x from loadTable('{db_name}','pt')")
        x = [1, 7] * 1500
        x.sort()
        ex = pd.DataFrame({
            "x": np.array(x, dtype=np.int32)
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_LIST(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_LIST = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`name, [LONG, STRING])
            db = database("{db_name}", LIST, [`a`b`c, `d`e])
            pt= db.createPartitionedTable(t, `pt, `name)
        """
        self.conn.run(script_DFS_LIST)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "name")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(
                target=self.insert_DFS_LIST_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select name from loadTable('{db_name}','pt')")
        name = ["a", "d"] * 1500
        name.sort()
        ex = pd.DataFrame({
            "name": name
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_COMPO_first_level(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_COMPO = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x`y, [LONG, STRING, STRING])
            dbX = database(, VALUE, `a`b`c)
            dbY = database(, LIST, [`x`y, `z])
            db = database("{db_name}", COMPO, [dbX, dbY])
            pt= db.createPartitionedTable(t, `pt, `x`y)
        """
        self.conn.run(script_DFS_COMPO)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_COMPO_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select x,y from loadTable('{db_name}','pt')")
        x = ["a"] * 3000
        y = ["y", "z"] * 1500
        y.sort()
        ex = pd.DataFrame({
            "x": x,
            "y": y
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_COMPO_second_level(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_COMPO = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x`y, [LONG, STRING, STRING])
            dbX = database(, VALUE, `a`b`c)
            dbY = database(, LIST, [`x`y, `z])
            db = database("{db_name}", COMPO, [dbX, dbY])
            pt= db.createPartitionedTable(t, `pt, `x`y)
        """
        self.conn.run(script_DFS_COMPO)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "y")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_COMPO_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select x,y from loadTable('{db_name}','pt')")
        x = ["a"] * 3000
        y = ["y", "z"] * 1500
        y.sort()
        ex = pd.DataFrame({
            "x": x,
            "y": y
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_Dimensional(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_Dimensional = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, STRING])
            db = database("{db_name}", VALUE, `a`b`c)
            pt= db.createTable(t, `pt)
        """
        self.conn.run(script_DFS_Dimensional)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 1)
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_Dimensional, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 900
        re = self.conn.run(f"select * from loadTable('{db_name}','pt') order by id")
        id = list(range(90)) * 10
        id.sort()
        x = ["a"] * 900
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_Dimensional_multi_thread_designation_partition(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_Dimensional = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, STRING])
            db = database("{db_name}", VALUE, `a`b`c)
            pt= db.createTable(t, `pt)
        """
        self.conn.run(script_DFS_Dimensional)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 3, "id")
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_useSSL(self):
        if self.conn.run("getConfig(`enableHTTPS)") == '0':
            pytest.skip("https is not true, skip this case")
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_DFS_COMPO_SSL = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x`y, [LONG, STRING, STRING])
            dbX = database(, VALUE, `a`b`c)
            dbY = database(, LIST, [`x`y, `z])
            db = database("{db_name}", COMPO, [dbX, dbY])
            pt= db.createPartitionedTable(t, `pt, `x`y)
        """
        self.conn.run(script_DFS_COMPO_SSL)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", True, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_COMPO, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 1600
        re = self.conn.run(f"select x,y from loadTable('{db_name}','pt')")
        x = ["a"] * 1600
        y = ["y", "z"] * 800
        y.sort()
        ex = pd.DataFrame({
            "x": x,
            "y": y
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_TSDB_single_thread(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_TSDB = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, STRING])
            db = database("{db_name}", VALUE, `a`b`c, engine="TSDB")
            pt= db.createPartitionedTable(t, `pt, `x, sortColumns="id")
        """
        self.conn.run(script_TSDB)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 1, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_TSDB_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = list(range(300)) * 10
        id.sort()
        x = ["a"] * 3000
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_TSDB_multi_thread(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_TSDB = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, STRING])
            db = database("{db_name}", VALUE, `a`b`c, engine="TSDB")
            pt= db.createPartitionedTable(t, `pt, `x, sortColumns="id")
        """
        self.conn.run(script_TSDB)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_TSDB_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = list(range(300)) * 10
        id.sort()
        x = ["a"] * 3000
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_VALUE_datehour_datetime(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_datehour_datetime = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("{db_name}", VALUE, datehour([2012.06.15 15:30:00.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_datehour_datetime)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_datetime, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_VALUE_datehour_timestamp(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_datehour_timestamp = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, TIMESTAMP])
            db = database("{db_name}", VALUE, datehour([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_datehour_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_timestamp, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_VALUE_datehour_nanotimestamp(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_datehour_timestamp = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, NANOTIMESTAMP])
            db = database("{db_name}", VALUE, datehour([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_datehour_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10,
            "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_nanotimestamp, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008007006", "2013-06-15T17:30:10.008007006"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_VALUE_date_datetime(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_date_datetime = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("{db_name}", VALUE, date([2012.06.15 15:30:00.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_datetime)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_datetime, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_VALUE_date_timestamp(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_date_timestamp = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, TIMESTAMP])
            db = database("{db_name}", VALUE, date([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_timestamp, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_VALUE_date_nanotimestamp(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_date_timestamp = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, NANOTIMESTAMP])
            db = database("{db_name}", VALUE, date([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_nanotimestamp, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008007006", "2013-06-15T17:30:10.008007006"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_VALUE_month_datetime(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_month_datetime = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("{db_name}", VALUE, month([2012.06.15 15:30:00.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_month_datetime)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, "pt", False,
                                              False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_datetime, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_VALUE_month_timestamp(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_month_timestamp = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, TIMESTAMP])
            db = database("{db_name}", VALUE, month([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_month_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_timestamp, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_VALUE_month_nanotimestamp(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_month_timestamp = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, NANOTIMESTAMP])
            db = database("{db_name}", VALUE, month([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_month_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_nanotimestamp, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008007006", "2013-06-15T17:30:10.008007006"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_RANGE_date_date(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_date_date = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, DATE])
            db = database("{db_name}", RANGE, date([2010.06.15 15:30:00.158,2012.08.15 17:30:10.008, 2014.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_date)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_date, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15", "2013-06-15"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_RANGE_date_datetime(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_date_datetime = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("{db_name}", RANGE, date([2010.06.15 15:30:00.158,2012.08.15 17:30:10.008, 2014.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_datetime)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, "pt", False,
                                              False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_datetime, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_RANGE_date_timestamp(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_date_timestamp = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, TIMESTAMP])
            db = database("{db_name}", RANGE, date([2010.06.15 15:30:00.158,2012.08.15 17:30:10.008, 2014.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_timestamp, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_DFS_RANGE_date_nanotimestamp(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_date_timestamp = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, NANOTIMESTAMP])
            db = database("{db_name}", RANGE, date([2010.06.15 15:30:00.158,2012.08.15 17:30:10.008, 2014.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_timestamp)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, "pt",
                                              False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_nanotimestamp, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 3000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = [0] * 1500
        id_last = [1] * 1500
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008007006", "2013-06-15T17:30:10.008007006"], dtype="datetime64[ns]")
        x = np.repeat(x, 1500)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_data_size_between_1024_1048576(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_data_size_between_1024_1048576 = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, INT])
            db = database("{db_name}", RANGE, 0 1000 2000)
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_data_size_between_1024_1048576)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_data_size_between_1024_1048576, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 20000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = re["id"].to_list()
        id.sort()
        x = re["x"].to_list()
        x.sort()
        ex_id = list(range(2000)) * 10
        ex_id.sort()
        ex_x = list(range(2000)) * 10
        ex_x.sort()
        assert id == ex_id
        assert x == ex_x
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_data_size_larger_than_1048576(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_data_size_larger_than_1048576 = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, INT])
            db = database("{db_name}", RANGE, 0 1000000 2000000)
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_data_size_larger_than_1048576)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_data_size_larger_than_1048576, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 1500000
        re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
        id = re["id"].to_list()
        id.sort()
        x = re["x"].to_list()
        x.sort()
        ex_id = list(range(150000)) * 10
        ex_id.sort()
        ex_x = list(range(150000)) * 10
        ex_x.sort()
        assert id == ex_id
        assert x == ex_x
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_more_than_200_cols(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_more_than_200_cols = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            col_name = array(STRING, 0)
            for(i in 1..300){{
                col_name_element = "col"+string(i)
                col_name.append!(col_name_element)
            }}
            col_type = take(INT, 300)
            t = table(1000:0, col_name, col_type)
            db = database("{db_name}", RANGE, 0 50 200)
            pt= db.createPartitionedTable(t, `pt, `col1)
        """
        self.conn.run(script_more_than_200_cols)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, "pt", False, False,
                                              [], 100, 0.1, 10, "col1")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_more_than_200_cols, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{db_name}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from loadTable('{db_name}','pt')")
        assert last["count"][0] - first["count"][0] == 1000
        for i in range(300):
            re = self.conn.run(f"select * from loadTable('{db_name}','pt')")
            col = re["col{}".format(i + 1)].to_list()
            col.sort()
            ex_col = list(range(100)) * 10
            ex_col.sort()
            assert col == ex_col
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_all_type_compress_Delta(self):
        func_name = inspect.currentframe().f_code.co_name
        script_all_type_compress_Delta = f"""
            t = table(1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`arrayVector, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, INT[]])
            share t as `{func_name}
        """
        self.conn.run(script_all_type_compress_Delta)
        pattern = re.compile(".*Failed to save the inserted data.*")
        compress = ["delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta",
                    "delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta"]
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", func_name, False, False, [], 100, 0.1, 1, "", compress)
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_all_type_compress, args=(writer, i,)))
        self.conn.run(f"delete from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        errorInfo = writer.getStatus().errorInfo
        flag = pattern.match(errorInfo)
        assert flag is not None

    def test_multithreadTableWriterTest_all_type_compress_LZ4(self):
        func_name = inspect.currentframe().f_code.co_name
        script_all_type_compress_LZ4 = f"""
            t = table(1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`arrayVector, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, INT[]])
            share t as `{func_name}
        """
        self.conn.run(script_all_type_compress_LZ4)
        compress = ["LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4",
                    "LZ4", "LZ4", "LZ4", "LZ4", "LZ4"]
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name, False, False, [],
                                              100, 0.1, 1, "", compress)
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_all_type_compress, args=(writer, i,)))
        self.conn.run(f"delete from {func_name}")
        first = self.conn.run(f"select count(*) from {func_name}")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        last = self.conn.run(f"select count(*) from {func_name}")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run(f"select * from {func_name}")
        col_bool = [True, False, True, False, True, False]
        col_char = np.array([1, 2, 3, 4, 5, 6], dtype=np.int8)
        col_short = np.array([1, 2, 3, 4, 5, 6], dtype=np.int16)
        col_int = np.array([1, 2, 3, 4, 5, 6], dtype=np.int32)
        col_long = np.array([1, 2, 3, 4, 5, 6], dtype=np.int64)
        col_date = np.array(["2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13", "2013-06-13"],
                            dtype="datetime64[ns]")
        col_month = np.array(["2012-06", "2012-06", "2012-06", "2012-06", "2012-06", "2012-06"], dtype="datetime64[ns]")
        col_time = np.array(
            ["1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008",
             "1970-01-01T13:30:10.008", "1970-01-01T13:30:10.008"], dtype="datetime64[ns]")
        col_second = np.array(
            ["1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10", "1970-01-01T13:30:10",
             "1970-01-01T13:30:10", "1970-01-01T13:30:10"], dtype="datetime64[ns]")
        col_minute = np.array(
            ["1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30", "1970-01-01T13:30",
             "1970-01-01T13:30"], dtype="datetime64[ns]")
        col_datehour = np.array(["2012-06-13T13", "2012-06-13T13", "2012-06-13T13",
                                 "2012-06-13T13", "2012-06-13T13", "2012-06-13T13"], dtype="datetime64[ns]")
        col_datetime = np.array(["2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10",
                                 "2012-06-13T13:30:10", "2012-06-13T13:30:10", "2012-06-13T13:30:10"],
                                dtype="datetime64[ns]")
        col_timestamp = np.array(["2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008",
                                  "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008", "2012-06-13T13:30:10.008"],
                                 dtype="datetime64[ns]")
        col_nanotime = np.array(
            ["1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006",
             "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006", "1970-01-01T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_nanotimestamp = np.array(
            ["2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006",
             "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006", "2012-06-13T13:30:10.008007006"],
            dtype="datetime64[ns]")
        col_float = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        col_double = np.array([1, 2, 3, 4, 5, 6], dtype=np.float64)
        col_string = ["1", "2", "3", "4", "5", "6"]
        col_arrayVector = [[1, 2], [1, 2], [1, 2], [1, 2], [1, 2], [1, 2]]
        ex = pd.DataFrame({
            "bool": col_bool,
            "char": col_char,
            "short": col_short,
            "int": col_int,
            "long": col_long,
            "date": col_date,
            "month": col_month,
            "time": col_time,
            "second": col_second,
            "minute": col_minute,
            "datehour": col_datehour,
            "datetime": col_datetime,
            "timestamp": col_timestamp,
            "nanotime": col_nanotime,
            "nanotimestamp": col_nanotimestamp,
            "float": col_float,
            "double": col_double,
            "string": col_string,
            "arrayVector": col_arrayVector
        })
        assert_frame_equal(re, ex)

    def test_multithreadTableWriterTest_DFS_partitionCol_null(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        script_VALUE_datehour_datetime = f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("{db_name}", VALUE, datehour([2012.06.15 15:30:00.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_datehour_datetime)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, db_name, "pt", False, False, [], 100, 0.1, 10, "")
        self.conn.run(f'dropDatabase("{db_name}")')

    def test_multithreadTableWriterTest_one_col_insert_double_and_int(self):
        func_name = inspect.currentframe().f_code.co_name
        tbname = func_name
        scirpt_one_col_insert_double_and_int = f"share table(1 2 as id, double(1 2) as val, nanotimestamp(111111 222222) as time) as {tbname}"
        self.conn.run(scirpt_one_col_insert_double_and_int)
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", tbname)
        mtw.insert(1, 1, np.datetime64("2022-01-02T14:12:12.123456789"))
        mtw.insert(2, 2.2, np.datetime64("2022-01-02T14:12:12.123456789"))
        mtw.insert(3, None, None)
        mtw.waitForThreadCompletion()
        data = {
            "id": np.array([1, 2, 1, 2, 3], dtype=np.int32),
            "val": np.array([1.0, 2.0, 1.0, 2.2, np.nan], dtype=np.float64),
            "time": np.array(
                ["1970-01-01 00:00:00.000111111", "1970-01-01 00:00:00.000222222", "2022-01-02 14:12:12.123456789",
                 "2022-01-02 14:12:12.123456789", None], dtype="datetime64[ns]")
        }
        ex = pd.DataFrame(data)
        ans = self.conn.run(tbname)
        assert_frame_equal(ex, ans)

    def test_multithreadTableWriter_createObject_elapsed_time(self):
        func_name = inspect.currentframe().f_code.co_name
        tbname = func_name
        s = f"share table(1 2 as id, double(1 2) as val, nanotimestamp(111111 222222) as time) as {tbname}"
        self.conn.run(s)
        import random
        random_threadCount = random.randint(10, 20)
        st = time.time()
        ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", tbname, threadCount=random_threadCount, partitionCol='id')
        et = time.time()
        elapsed_time = et - st
        assert elapsed_time < 10

    @pytest.mark.parametrize('data', [[1, 2, 3], {1}, {1: 1}, (1,)], ids=['LIST', 'SET', 'DICT', 'TUPLE'])
    def test_multithreadTableWriter_insert_error_type(self, data):
        func_name = inspect.currentframe().f_code.co_name + random_string(5)
        self.conn.run(f"""
            t=table(100:0,[`int,`str],[INT,STRING])
            share t as `{func_name}
        """)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", func_name)
        for i in range(3):
            writer.insert(data, data)
        writer.waitForThreadCompletion()
        status = writer.getStatus()
        assert status.hasError()
        assert not status.succeed()
        assert status.errorCode == 'A1'
        assert 'Data conversion error' in status.errorInfo

    def test_multithreadTableWriter_over_length(self):
        func_name = inspect.currentframe().f_code.co_name
        data = '1' * 256 * 1024
        tbname = func_name
        self.conn.run(f"share table(100:0,[`str],[STRING]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert status.hasError()
        assert status.errorCode == 'A5'
        assert 'Failed to save the inserted data: String too long, Serialization failed, length must be less than 256K bytes' in status.errorInfo
        assert status.sendFailedRows + status.unsentRows == 1
        del mtw

    def test_multithreadTableWriter_max_length_string_in_memory_table(self):
        func_name = inspect.currentframe().f_code.co_name
        data = '1' * (256 * 1024 - 1)
        tbname = func_name
        self.conn.run(f"share table(100:0,[`str],[STRING]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024 - 1
        del mtw

    def test_multithreadTableWriter_max_length_symbol_in_memory_table(self):
        func_name = inspect.currentframe().f_code.co_name
        data = '1' * (256 * 1024 - 1)
        tbname = func_name
        self.conn.run(f"share table(100:0,[`str],[SYMBOL]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024 - 1
        del mtw

    def test_multithreadTableWriter_max_length_blob_in_memory_table(self):
        func_name = inspect.currentframe().f_code.co_name
        data = '1' * 256 * 1024
        tbname = func_name
        self.conn.run(f"share table(100:0,[`str],[BLOB]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024
        del mtw

    def test_multithreadTableWriter_max_length_string_stream_table(self):
        func_name = inspect.currentframe().f_code.co_name
        data = '1' * (256 * 1024 - 1)
        tbname = func_name
        self.conn.run(f"share streamTable(100:0,[`str],[STRING]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024 - 1
        del mtw

    def test_multithreadTableWriter_max_length_symbol_stream_table(self):
        func_name = inspect.currentframe().f_code.co_name
        data = '1' * (256 * 1024 - 1)
        tbname = func_name
        self.conn.run(f"share streamTable(100:0,[`str],[SYMBOL]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024 - 1
        del mtw

    def test_multithreadTableWriter_max_length_blob_stream_table(self):
        func_name = inspect.currentframe().f_code.co_name
        data = '1' * 256 * 1024
        tbname = func_name
        self.conn.run(f"share streamTable(100:0,[`str],[BLOB]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024
        del mtw

    def test_multithreadTableWriter_max_length_string_dfs_table(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        data = '1' * (256 * 1024 - 1)
        tbname = func_name
        self.conn.run(f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            tab=table(100:0,[`id,`test],[STRING,STRING])
            db=database("{db_name}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, tbname, threadCount=1)
        mtw.insert(data, "0")
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 64 * 1024 - 1
        del mtw

    def test_multithreadTableWriter_max_length_symbol_dfs_table(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        data = '1' * 255
        tbname = func_name
        self.conn.run(f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            tab=table(100:0,[`id,`test],[SYMBOL,STRING])
            db=database("{db_name}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, tbname, threadCount=1)
        mtw.insert(data, "0")
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 255
        del mtw

    def test_multithreadTableWriter_max_length_blob_dfs_table(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        data = '1' * 256 * 1024
        tbname = func_name
        self.conn.run(f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            tab=table(100:0,[`id,`test],[BLOB,STRING])
            db=database("{db_name}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, db_name, tbname, threadCount=1)
        mtw.insert(data, "0")
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 256 * 1024
        del mtw

    @pytest.mark.xdist_group(name='cluster_test')
    def test_multithreadTableWriter_reconnect_false(self):
        func_name = inspect.currentframe().f_code.co_name
        data = "test"
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn_ = ddb.Session(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER)
        conn_.run(f"share streamTable(100:0,[`str],[BLOB]) as {func_name}")
        mtw = ddb.MultithreadedTableWriter(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, '', func_name,
                                           threadCount=1, reconnect=False)
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "STOP")
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "START")
        status = mtw.getStatus()
        assert status.hasError()

    @pytest.mark.xdist_group(name='cluster_test')
    def test_multithreadTableWriter_reconnect_true(self):
        func_name = inspect.currentframe().f_code.co_name
        db_name = f"dfs://{func_name}"
        data = "test"
        conn = ddb.Session(HOST_CLUSTER, PORT_CONTROLLER, USER_CLUSTER, PASSWD_CLUSTER)
        conn_ = ddb.Session(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER)
        conn_.run(f"""
            if(existsDatabase("{db_name}")){{
                dropDatabase("{db_name}")
            }}
            tab=table(100:0,[`id,`test],[BLOB,STRING])
            db=database("{db_name}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{func_name}",`test,sortColumns=`test)
        """)
        mtw = ddb.MultithreadedTableWriter(HOST_CLUSTER, PORT_DNODE1, USER_CLUSTER, PASSWD_CLUSTER, db_name, func_name,
                                           threadCount=1)
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "STOP")
        mtw.insert(data, data)
        operateNodes(conn, [f'dnode{PORT_DNODE1}'], "START")
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
