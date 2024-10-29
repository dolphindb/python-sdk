import decimal
import re
import threading
import time

import pytest
from numpy.testing import *
from pandas.testing import *

from setup.prepare import *
from setup.settings import *
from setup.utils import get_pid
from setup.utils import random_string


class TestMultithreadTableWriter:
    conn = ddb.session(enablePickle=False)

    def setup_method(self):
        try:
            self.conn.run("1")
        except RuntimeError:
            self.conn.connect(HOST, PORT, USER, PASSWD)
        self.dbname = "dfs://test_" + random_string(12)

    def teardown_method(self):
        time.sleep(1)

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

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
        for i in range(1500):
            res = writer.insert(np.datetime64("2016-01-12"), i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(np.datetime64("2016-02-12"), i)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_VALUE_Huge(self, writer, id):
        for i in range(15000):
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
        for i in range(15000):
            res = writer.insert(i, 1)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(i, 7)
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_LIST_Huge(self, writer, id):
        for i in range(15000):
            res = writer.insert(i, "a")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break
            res = writer.insert(i, "d")
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_DFS_COMPO_Huge(self, writer, id):
        for i in range(15000):
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
        for i in range(3000):
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
        for i in range(15000):
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
        for i in range(3000):
            col_datetime = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[D]")
            j = i % 2
            res = writer.insert(j, col_datetime[j])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_datetime(self, writer, id):
        for i in range(3000):
            col_datetime = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[s]")
            j = i % 2
            res = writer.insert(j, col_datetime[j])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_timestamp(self, writer, id):
        for i in range(3000):
            col_timestamp = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ms]")
            j = i % 2
            res = writer.insert(j, col_timestamp[j])
            if res.hasError():
                print("error code ", res.errorCode, res.errorInfo)
                break

    def insert_nanotimestamp(self, writer, id):
        for i in range(3000):
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
        with pytest.raises(RuntimeError, match='Unsupported mtw mode TEST'):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100, 0.1,
                                         10, "date", mode="test")

    def test_multithreadTableWriterTest_error_compressMethods(self):
        with pytest.raises(RuntimeError, match='Unsupported compression method TEST'):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100, 0.1,
                                         10, "date", compressMethods=["test"])

    def test_multithreadTableWriterTest_error_hostName(self):
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter("1.1", PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100,
                                         0.1, 10, "date")

    def test_multithreadTableWriterTest_error_port(self):
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, -5, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100, 0.1,
                                         10, "date")

    def test_multithreadTableWriterTest_error_userID(self):
        self.conn.run("try{deleteUser(`mark)}catch(ex){}")
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, "mark", PASSWD, self.dbname, "pdatetest", False, False, [], 100,
                                         0.1, 10, "date")

    def test_multithreadTableWriterTest_error_password(self):
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, "123", self.dbname, "pdatetest", False, False, [], 100, 0.1,
                                         10, "date")

    def test_multithreadTableWriterTest_error_dbName(self):
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100, 0.1,
                                         10, "date")

    def test_multithreadTableWriterTest_error_tableName(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pd", False, False, [], 100, 0.1, 10,
                                         "date")
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_userid_no_grant_write(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        script_user_no_grant_write = """
            def test_user(){
                try{createUser("mark", "123456")}catch(ex){};go;
                grant("mark", TABLE_READ, "*")
            }
            rpc(getControllerAlias(),  test_user)
        """
        self.conn.run(script_user_no_grant_write)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, "mark", PASSWD, self.dbname, "pdatetest", False, False, [],
                                              10, 0.1, 1, "date")
        self.insert_grant(writer, 1)
        writer.waitForThreadCompletion()
        assert writer.getStatus().errorCode == 'A5'
        pattern = re.compile(".*Failed to save the inserted data.*")
        errorInfo = writer.getStatus().errorInfo
        flag = pattern.match(errorInfo)
        assert flag is not None
        last = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pdatetest')")
        assert last["count"][0] == 0
        self.conn.dropDatabase(self.dbname)
        self.conn.run('rpc(getControllerAlias(),  deleteUser,  "mark")')

    def test_multithreadTableWriterTest_batchSize_negative_number(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], -1, 0.1,
                                         10, "date")
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_batchSize_zero(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 0, 0.1,
                                         10, "date")
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_throttle_negative_number(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100, -10,
                                         10, "date")
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_threadCount_negative_number(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100, 0.1,
                                         -10, "date")
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_threadCount_zero(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100, 0.1,
                                         0, "date")
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_Memory_Table_mutilthread_unspecified_partitioncol(self):
        script_Memory_Table = """
            t = table(1000:0, `id`x, [LONG, INT])
            share t as share_table
        """
        self.conn.run(script_Memory_Table)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, "share_table", "", False, False, [], 100, 0.1, 3, "")
        self.conn.run("undef(`share_table, SHARED)")

    def test_multithreadTableWriterTest_DFS_Table_mutilthread_specified_not_partitioncol(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100, 0.1,
                                         10, "id")
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_insert_differnt_data_type(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 1, 0.1, 1, "date")
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
        assert writer.getStatus().errorInfo == 'Data conversion error: Cannot convert <class \'str\'> to DATE.'
        last = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pdatetest')")
        assert last["count"][0] == 0
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_Memory_Table_dbName_empty(self):
        script_Memory_Table = """
            t = table(1000:0, `id`x, [LONG, INT])
            share t as share_table
        """
        self.conn.run(script_Memory_Table)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, "share_table", "", False, False, [], 100, 0.1, 3, "id")
        self.conn.run("undef(`share_table, SHARED)")

    def test_multithreadTableWriterTest_function_getUnwrittenData(self):
        script = """
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            share t as test_tab;go
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "test_tab", batchSize=100, threadCount=5, partitionCol="date")
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
        script = """
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            share t as test_tab;go
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "test_tab", batchSize=100, threadCount=5, partitionCol="date")
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
        self.conn.run("undef(`test_tab, SHARED)")

    def test_multithreadTableWriterTest_function_getUnwrittenData_2(self):
        script = """
            t = table(100:0, [`date, `int, `string, `ipaddr], [DATE, INT, STRING, IPADDR]);
            share t as test_tab;go
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "test_tab", batchSize=100, threadCount=5, partitionCol="date")
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
        self.conn.run("undef(`test_tab, SHARED)")

    def test_multithreadTableWriterTest_function_getUnwrittenData_no_wrong_data(self):
        script = """
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            share t as test_tab;go
        """
        self.conn.run(script)

        def insert_task(rows, thread_count):
            writer = ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, "", "test_tab", batchSize=100, threadCount=thread_count, partitionCol="date")
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
        self.conn.run("undef(`test_tab,SHARED)")

    def test_multithreadTableWriterTest_function_getUnwrittenData_dfs(self):
        script = f"""
            // try{{undef(test_tab, SHARED);}}catch(ex){{}};
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            dbpath = "{self.dbname}"
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};go
            db=database(dbpath,VALUE,2022.03.20..2022.03.29);
            tmp=db.createPartitionedTable(t,`tmp,`date);go
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, self.dbname, "tmp", batchSize=1000, threadCount=10, partitionCol="date")
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
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_function_getUnwrittenData_dfs_1(self):
        script = f"""
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            dbpath = "{self.dbname}"
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};go
            db=database(dbpath,VALUE,2022.03.20..2022.03.29,,'TSDB');
            tmp=db.createPartitionedTable(t,`tmp,`date,,`int);go
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, self.dbname, "tmp", batchSize=1000, threadCount=10, partitionCol="date")
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
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_function_getUnwrittenData_dfs_2(self):
        script = f"""
            t = table(100:0, [`date, `int, `string, `symbol], [DATE, INT, STRING, SYMBOL]);
            dbpath = "{self.dbname}"
            if(existsDatabase(dbpath)){{dropDatabase(dbpath)}};go
            db=database(dbpath,VALUE,2022.03.20..2022.03.29,,'TSDB');
            tmp=db.createPartitionedTable(t,`tmp,`date,,`int);go
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "tmp", batchSize=1000,
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
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_object_MultithreadedTableWriterThreadStatus(self):
        script = """
            t = table(100:0, [`int, `str], [INT, STRING]);
            share t as test_tab_2;
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "test_tab_2")
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
        writer2 = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "test_tab_2")
        for i in range(3):
            writer2.insert(col_int[i], col_str[i])
        writer2.waitForThreadCompletion()
        status2 = writer2.getStatus()
        assert not status2.hasError()
        assert status2.sentRows == 3
        assert status2.sendFailedRows == 0
        assert status2.errorCode == ''
        assert status2.errorInfo == ''
        self.conn.run("undef(`test_tab_2, SHARED)")

    def test_multithreadTableWriterTest_all_data_type(self):
        script_all_data_type = """
            t = table(1000:0,
            `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`csecond`cminute`cdatehour`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`cstring`cuuid`cint128`cip`cdecimal32`cdecimal64`cdecimal128,
            [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, INT128, IPADDR, DECIMAL32(6), DECIMAL64(16), DECIMAL128(24)])
            share t as all_data_type
        """
        self.conn.run(script_all_data_type)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "all_data_type", False, False, [], 100, 0.1, 1, "")
        self.conn.run("delete from all_data_type")
        first = self.conn.run("select count(*) from all_data_type")
        self.insert_all_data_type(writer, 1)
        writer.waitForThreadCompletion()
        time.sleep(3)
        st = writer.getStatus()
        last = self.conn.run("select count(*) from all_data_type")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run("select * from all_data_type")
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
        self.conn.run("undef(`all_data_type, SHARED)")

    def test_multithreadTableWriterTest_keyedTable_upsert_all_data_type(self):
        script_all_data_type = """
            t1 = keyedTable(`int,1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`uuid`int128`ip`decimal32`decimal64`decimal128, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, INT128, IPADDR,DECIMAL32(2),DECIMAL64(2),DECIMAL128(2)])
            share t1 as all_data_type
        """
        self.conn.run(script_all_data_type)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "all_data_type", False, False, [], 1, 0.1,
                                              1, "int", mode="upsert",
                                              modeOption=["ignoreNull=false", "keyColNames=`int"])
        first = self.conn.run("select count(*) from all_data_type")
        self.insert_all_data_type(writer, 1)
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from all_data_type")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run("select * from all_data_type")
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
        self.conn.run("undef(`all_data_type, SHARED)")

    def test_multithreadTableWriterTest_indexedTable_upsert_all_data_type(self):
        script_all_data_type = """
            t1 = indexedTable(`int,1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`uuid`int128`ip`decimal32`decimal64`decimal128, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, INT128, IPADDR,DECIMAL32(2),DECIMAL64(2),DECIMAL128(2)])
            share t1 as all_data_type
        """
        self.conn.run(script_all_data_type)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "all_data_type", False, False, [], 1, 0.1,
                                              1, "int", mode="upsert",
                                              modeOption=["ignoreNull=false", "keyColNames=`int"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_all_data_type, args=(writer, i,)))
        first = self.conn.run("select count(*) from all_data_type")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from all_data_type")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run("select * from all_data_type")
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
        self.conn.run("undef(`all_data_type, SHARED)")

    def test_multithreadTableWriterTest_dfs_upsert_all_data_type(self):
        script_all_data_type = f"""
            dbPath = "{self.dbname}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`uuid`int128`ip`decimal32`decimal64`decimal128, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, INT128, IPADDR,DECIMAL32(2),DECIMAL64(2),DECIMAL128(2)])
            db=database(dbPath,RANGE,1 3 7,engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `int,sortColumns=`int)
        """
        self.conn.run(script_all_data_type)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pt", False, False, [], 1, 0.1, 1,
                                              "int", mode="upsert", modeOption=["ignoreNull=false", "keyColNames=`int"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_all_data_type, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(6)
        last = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pt')")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run(f"select * from loadTable('{self.dbname}','pt')")
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
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_dfs_upsert_key_exist_ignoreNull_True(self):
        script = f"""
            dbPath = "{self.dbname}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
            db=database(dbPath,RANGE,1 6 12,engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `qty,sortColumns=`qty)
            pt.append!(t)
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pt", False, False, [], 1, 0.1, 1,
                                              "qty", mode="upsert", modeOption=["ignoreNull=true", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(6)
        last = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pt')")
        assert last["count"][0] - first["count"][0] == 1
        script = f'''
            re = select * from loadTable('{self.dbname}','pt')
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_dfs_upsert_key_exist_ignoreNull_False(self):
        script = f"""
            dbPath = "{self.dbname}"
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            t = table(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..10 as qty)
            db=database(dbPath,RANGE,1 6 12,engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `qty,sortColumns=`qty)
            pt.append!(t)
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pt", False, False, [], 1, 0.1, 1,
                                              "qty", mode="upsert", modeOption=["ignoreNull=false", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(
                target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run(
            f"select count(*) from loadTable('{self.dbname}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(6)
        last = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pt')")
        assert last["count"][0] - first["count"][0] == 1
        script = f'''
            re = select * from loadTable('{self.dbname}','pt')
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_keyedTable_upsert_key_exist_ignoreNull_True(self):
        script = """
            t1 = keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])
            insert into t1 values(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO,[2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05], 1..10 )
            share t1 as tt1
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "tt1", False, False, [], 1, 0.1, 1, "qty",
                                              mode="upsert", modeOption=["ignoreNull=true", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(
                target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run("select count(*) from tt1")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(6)
        last = self.conn.run("select count(*) from tt1")
        assert last["count"][0] - first["count"][0] == 1
        script = '''
            re = select * from tt1
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])
        self.conn.run("undef(`tt1, SHARED)")

    def test_multithreadTableWriterTest_keyedTable_upsert_key_exist_ignoreNull_False(self):
        script = """
            t1 = keyedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])
            insert into t1 values(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO,[2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05], 1..10 )
            share t1 as tt1
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "tt1", False, False, [], 1, 0.1, 1, "qty",
                                              mode="upsert", modeOption=["ignoreNull=false", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run("select count(*) from tt1")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(6)
        last = self.conn.run("select count(*) from tt1")
        assert last["count"][0] - first["count"][0] == 1
        script = '''
            re = select * from tt1
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])
        self.conn.run("undef(`tt1, SHARED)")

    def test_multithreadTableWriterTest_indexedTable_upsert_key_exist_ignoreNull_True(self):
        script = """
            t1 = indexedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])
            insert into t1 values(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO,[2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05], 1..10 )
            share t1 as tt1
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "tt1", False, False, [], 1, 0.1, 1, "qty",
                                              mode="upsert", modeOption=["ignoreNull=true", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run("select count(*) from tt1")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(6)
        last = self.conn.run("select count(*) from tt1")
        assert last["count"][0] - first["count"][0] == 1
        script = '''
            re = select * from tt1
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, 1965.07.25, 1965.07.25, 2020.12.23, 2020.12.23, 1970.01.01, NULL, NULL, 2009.08.05, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])
        self.conn.run("undef(`tt1, SHARED)")

    def test_multithreadTableWriterTest_indexedTable_upsert_key_exist_ignoreNull_False(self):
        script = """
            t1 = indexedTable(`qty,1000:0, `sym`date`qty, [SYMBOL, DATE, INT])
            insert into t1 values(`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO,[2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05], 1..10 )
            share t1 as tt1
        """
        self.conn.run(script)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "tt1", False, False, [], 1, 0.1, 1, "qty",
                                              mode="upsert", modeOption=["ignoreNull=false", "keyColNames=`qty"])
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_NULL2, args=(writer, i,)))
        first = self.conn.run("select count(*) from tt1")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(6)
        last = self.conn.run("select count(*) from tt1")
        assert last["count"][0] - first["count"][0] == 1
        script = '''
            re = select * from tt1
            tmp=table(`AAPL`AAPL`AAPL`GOOG`GOOG`MSFT`MSFT`IBM`IBM`YHOO`YHOO as sym, [2012.01.01, 2012.01.01, NULL, 1965.07.25, NULL, 2020.12.23, 1970.01.01, NULL, NULL, NULL, 2009.08.05] as date, 1..11 as qty)
            each(eqObj, tmp.values(), re.values())
        '''
        re1 = self.conn.run(script)
        assert_array_equal(re1, [True, True, True])
        self.conn.run("undef(`tt1, SHARED)")

    def test_multithreadTableWriterTest_batchSize_throttle(self):
        script_DFS_COMPO_batchSize_throttle = """
            if(existsDatabase("dfs://valuedb_DFS_COMPO_batchSize_throttle")){
                dropDatabase("dfs://valuedb_DFS_COMPO_batchSize_throttle")
            }
            t = table(1000:0, `id`x`y, [LONG, STRING, STRING])
            dbX = database(, VALUE, `a`b`c)
            dbY = database(, LIST, [`x`y, `z])
            db = database("dfs://valuedb_DFS_COMPO_batchSize_throttle", COMPO, [dbX, dbY])
            pt= db.createPartitionedTable(t, `pt, `x`y)
        """
        self.conn.run(script_DFS_COMPO_batchSize_throttle)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_COMPO_batchSize_throttle",
                                              "pt", False, False, [], 2000, 10, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_COMPO, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_COMPO_batchSize_throttle','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_COMPO_batchSize_throttle','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        time.sleep(5)
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_COMPO_batchSize_throttle','pt')")
        assert last["count"][0] - first["count"][0] == 0
        time.sleep(10)
        assert writer.getStatus().unsentRows == 0
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_COMPO_batchSize_throttle','pt')")
        assert last["count"][0] - first["count"][0] == 1600
        self.conn.run('dropDatabase("dfs://valuedb_DFS_COMPO_batchSize_throttle")')

    def test_multithreadTableWriterTest_AllData(self):
        script_DFS_RANGE_ALLDATA = """
            if(existsDatabase("dfs://valuedb_DFS_RANGE_AllData")){
                dropDatabase("dfs://valuedb_DFS_RANGE_AllData")
            }
            t = table(1000:0, `id`x, [LONG, INT])
            db = database("dfs://valuedb_DFS_RANGE_AllData", RANGE, 0 5 10)
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_DFS_RANGE_ALLDATA)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_RANGE_AllData", "pt", False, False, [], 1, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_RANGE_Huge, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_RANGE_AllData','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_RANGE_AllData','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        getUnwiteen = writer.getUnwrittenData()
        res = writer.getStatus()
        sentRows = res.sentRows
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_RANGE_AllData','pt')")
        while last["count"][0] - first["count"][0] != sentRows:
            res = writer.getStatus()
            sentRows = res.sentRows
            time.sleep(1)
        assert last["count"][0] - first["count"][0] + len(getUnwiteen) == 300000
        insert_res = writer.insertUnwrittenData(getUnwiteen)
        assert not insert_res.hasError()
        writer.waitForThreadCompletion()
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_RANGE_AllData','pt')")
        assert last["count"][0] - first["count"][0] == 300000
        self.conn.run('dropDatabase("dfs://valuedb_DFS_RANGE_AllData")')

    def test_multithreadTableWriterTest_getStatus_waitfor(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 1, 0.1, 1, "date")
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
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_getStatus_time(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 1,
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
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_Memory_Table_single_thread(self):
        script_Memory_Table = """
            t = table(1000:0, `id`x, [LONG, INT])
            share t as share_table
        """
        self.conn.run(script_Memory_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "share_table", False, False, [], 100, 0.1, 1, "id")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(
                target=self.insert_Memory_Table, args=(writer, i,)))
        self.conn.run("delete from share_table")
        first = self.conn.run("select count(*) from share_table")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from share_table")
        assert last["count"][0] - first["count"][0] == 2200
        re = self.conn.run("select * from share_table")
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
        self.conn.run("undef(`share_table, SHARED)")

    def test_multithreadTableWriterTest_Memory_Table_mutil_thread(self):
        script_Memory_Table = """
            t = table(1000:0, `id`x, [LONG, INT])
            share t as share_table
        """
        self.conn.run(script_Memory_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "share_table", False, False, [], 100, 0.1, 10, "id")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(
                target=self.insert_Memory_Table, args=(writer, i,)))
        self.conn.run("delete from share_table")
        first = self.conn.run("select count(*) from share_table")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from share_table")
        assert last["count"][0] - first["count"][0] == 2200
        re = self.conn.run("select * from share_table")
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
        self.conn.run("undef(`share_table, SHARED)")

    def test_multithreadTableWriterTest_Keyed_Table(self):
        script_Keyed_Table = """
            tmp = table(1000:0, `id`x, [LONG, INT])
            tt = keyedTable(`id, tmp)
            share tt as keyed_table
        """
        self.conn.run(script_Keyed_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "keyed_table", False, False, [], 100, 0.1, 1, "id")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(
                target=self.insert_Keyed_Table, args=(writer, i,)))
        self.conn.run("delete from keyed_table")
        first = self.conn.run("select count(*) from keyed_table")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from keyed_table")
        assert last["count"][0] - first["count"][0] == 130
        re = self.conn.run("select * from keyed_table")
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
        self.conn.run("undef(`keyed_table, SHARED)")

    def test_multithreadTableWriterTest_stream_Table_single_thread(self):
        script_Stream_Table = """
            tmp = streamTable(1000:0, `id`x, [LONG, INT])
            share tmp as stream_table
        """
        self.conn.run(script_Stream_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "stream_table", False, False, [], 100, 0.1, 1, "id")
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_Stream_Table, args=(writer, i,)))
        first = self.conn.run("select count(*) from stream_table")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from stream_table")
        assert last["count"][0] - first["count"][0] == 130
        re = self.conn.run("select * from stream_table")
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
        self.conn.run("undef(`stream_table, SHARED)")

    def test_multithreadTableWriterTest_stream_Table_multi_thread(self):
        script_Stream_Table = """
            tmp = streamTable(1000:0, `id`x, [LONG, INT])
            share tmp as stream_table
        """
        self.conn.run(script_Stream_Table)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "stream_table", False, False, [], 100, 0.1, 10, "id")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_Stream_Table, args=(writer, i,)))
        first = self.conn.run("select count(*) from stream_table")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from stream_table")
        assert last["count"][0] - first["count"][0] == 1300
        re = self.conn.run("select * from stream_table")
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
        self.conn.run("undef(`stream_table, SHARED)")

    def test_multithreadTableWriterTest_DFS_HASH(self):
        script_DFS_HASH = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            datetest=table(1000:0,`date`id,[DATE,LONG])
            db=database("{self.dbname}",HASH, [MONTH,10])
            pt=db.createPartitionedTable(datetest,'pdatetest','date')
        """
        self.conn.run(script_DFS_HASH)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, self.dbname, "pdatetest", False, False, [], 100,
                                              0.1, 10, "date")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_HASH_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{self.dbname}','pdatetest')")
        first = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pdatetest')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pdatetest')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run(f"select date from loadTable('{self.dbname}','pdatetest')")
        date = []
        date.extend([np.datetime64("2016-02-12"),
                     np.datetime64("2016-01-12")] * 15000)
        date.sort()
        ex = pd.DataFrame({
            "date": date,
        })
        assert_frame_equal(re, ex)
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_DFS_VALUE(self):
        script_DFS_VALUE = f"""
            if(existsDatabase("{self.dbname}")){{
                dropDatabase("{self.dbname}")
            }}
            t = table(1000:0, `id`name, [LONG, STRING])
            db = database("{self.dbname}", VALUE, `a`b`c)
            pt= db.createPartitionedTable(t, `pt, `name)
        """
        self.conn.run(script_DFS_VALUE)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, self.dbname, "pt", False, False, [], 100, 0.1, 10, "name")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_VALUE_Huge, args=(writer, i,)))
        self.conn.run(f"delete from loadTable('{self.dbname}','pt')")
        first = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run(f"select count(*) from loadTable('{self.dbname}','pt')")
        assert last["count"][0] - first["count"][0] == 450000
        re = self.conn.run(f"select name from loadTable('{self.dbname}','pt')")
        name = ["a", "b", "c"] * 150000
        name.sort()
        ex = pd.DataFrame({
            "name": name
        })
        assert_frame_equal(re, ex)
        self.conn.dropDatabase(self.dbname)

    def test_multithreadTableWriterTest_DFS_RANGE(self):
        script_DFS_RANGE = """
            if(existsDatabase("dfs://valuedb_DFS_RANGE")){
                dropDatabase("dfs://valuedb_DFS_RANGE")
            }
            t = table(1000:0, `id`x, [LONG, INT])
            db = database("dfs://valuedb_DFS_RANGE", RANGE, 0 5 10)
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_DFS_RANGE)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_RANGE", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_RANGE_Huge, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_RANGE','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_RANGE','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_RANGE','pt')")
        assert last["count"][0] - first["count"][0] == 300000
        re = self.conn.run("select x from loadTable('dfs://valuedb_DFS_RANGE','pt')")
        x = [1, 7] * 150000
        x.sort()
        ex = pd.DataFrame({
            "x": np.array(x, dtype=np.int32)
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://valuedb_DFS_RANGE")')

    def test_multithreadTableWriterTest_DFS_LIST(self):
        script_DFS_LIST = """
            if(existsDatabase("dfs://valuedb_DFS_LIST")){
                dropDatabase("dfs://valuedb_DFS_LIST")
            }
            t = table(1000:0, `id`name, [LONG, STRING])
            db = database("dfs://valuedb_DFS_LIST", LIST, [`a`b`c, `d`e])
            pt= db.createPartitionedTable(t, `pt, `name)
        """
        self.conn.run(script_DFS_LIST)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_LIST", "pt", False, False, [], 100, 0.1, 10, "name")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(
                target=self.insert_DFS_LIST_Huge, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_LIST','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_LIST','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_LIST','pt')")
        assert last["count"][0] - first["count"][0] == 300000
        re = self.conn.run("select name from loadTable('dfs://valuedb_DFS_LIST','pt')")
        name = ["a", "d"] * 150000
        name.sort()
        ex = pd.DataFrame({
            "name": name
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://valuedb_DFS_LIST")')

    def test_multithreadTableWriterTest_DFS_COMPO_first_level(self):
        script_DFS_COMPO = """
            if(existsDatabase("dfs://valuedb_DFS_COMPO")){
                dropDatabase("dfs://valuedb_DFS_COMPO")
            }
            t = table(1000:0, `id`x`y, [LONG, STRING, STRING])
            dbX = database(, VALUE, `a`b`c)
            dbY = database(, LIST, [`x`y, `z])
            db = database("dfs://valuedb_DFS_COMPO", COMPO, [dbX, dbY])
            pt= db.createPartitionedTable(t, `pt, `x`y)
        """
        self.conn.run(script_DFS_COMPO)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_COMPO", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_COMPO_Huge, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_COMPO','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_COMPO','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_COMPO','pt')")
        assert last["count"][0] - first["count"][0] == 300000
        re = self.conn.run("select x,y from loadTable('dfs://valuedb_DFS_COMPO','pt')")
        x = ["a"] * 300000
        y = ["y", "z"] * 150000
        y.sort()
        ex = pd.DataFrame({
            "x": x,
            "y": y
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://valuedb_DFS_COMPO")')

    def test_multithreadTableWriterTest_DFS_COMPO_second_level(self):
        script_DFS_COMPO = """
            if(existsDatabase("dfs://valuedb_DFS_COMPO")){
                dropDatabase("dfs://valuedb_DFS_COMPO")
            }
            t = table(1000:0, `id`x`y, [LONG, STRING, STRING])
            dbX = database(, VALUE, `a`b`c)
            dbY = database(, LIST, [`x`y, `z])
            db = database("dfs://valuedb_DFS_COMPO", COMPO, [dbX, dbY])
            pt= db.createPartitionedTable(t, `pt, `x`y)
        """
        self.conn.run(script_DFS_COMPO)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_COMPO", "pt", False, False, [], 100, 0.1, 10, "y")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_COMPO_Huge, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_COMPO','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_COMPO','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_COMPO','pt')")
        assert last["count"][0] - first["count"][0] == 300000
        re = self.conn.run("select x,y from loadTable('dfs://valuedb_DFS_COMPO','pt')")
        x = ["a"] * 300000
        y = ["y", "z"] * 150000
        y.sort()
        ex = pd.DataFrame({
            "x": x,
            "y": y
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://valuedb_DFS_COMPO")')

    def test_multithreadTableWriterTest_DFS_Dimensional(self):
        script_DFS_Dimensional = """
            if(existsDatabase("dfs://valuedb_DFS_Dimensional")){
                dropDatabase("dfs://valuedb_DFS_Dimensional")
            }
            t = table(1000:0, `id`x, [LONG, STRING])
            db = database("dfs://valuedb_DFS_Dimensional", VALUE, `a`b`c)
            pt= db.createTable(t, `pt)
        """
        self.conn.run(script_DFS_Dimensional)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_Dimensional", "pt", False, False, [], 100, 0.1, 1)
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_Dimensional, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_Dimensional','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_Dimensional','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_Dimensional','pt')")
        assert last["count"][0] - first["count"][0] == 900
        re = self.conn.run("select * from loadTable('dfs://valuedb_DFS_Dimensional','pt') order by id")
        id = list(range(90)) * 10
        id.sort()
        x = ["a"] * 900
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://valuedb_DFS_Dimensional")')

    def test_multithreadTableWriterTest_DFS_Dimensional_multi_thread_designation_partition(self):
        script_DFS_Dimensional = """
            if(existsDatabase("dfs://valuedb_DFS_Dimensional")){
                dropDatabase("dfs://valuedb_DFS_Dimensional")
            }
            t = table(1000:0, `id`x, [LONG, STRING])
            db = database("dfs://valuedb_DFS_Dimensional", VALUE, `a`b`c)
            pt= db.createTable(t, `pt)
        """
        self.conn.run(script_DFS_Dimensional)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_Dimensional", "pt", False, False, [], 100, 0.1, 3, "id")
        self.conn.run('dropDatabase("dfs://valuedb_DFS_Dimensional")')

    def test_multithreadTableWriterTest_useSSL(self):
        if self.conn.run("getConfig(`enableHTTPS)") == '0':
            pytest.skip("https is not true, skip this case")
        script_DFS_COMPO_SSL = """
            if(existsDatabase("dfs://valuedb_DFS_COMPO_SSL")){
                dropDatabase("dfs://valuedb_DFS_COMPO_SSL")
            }
            t = table(1000:0, `id`x`y, [LONG, STRING, STRING])
            dbX = database(, VALUE, `a`b`c)
            dbY = database(, LIST, [`x`y, `z])
            db = database("dfs://valuedb_DFS_COMPO_SSL", COMPO, [dbX, dbY])
            pt= db.createPartitionedTable(t, `pt, `x`y)
        """
        self.conn.run(script_DFS_COMPO_SSL)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_COMPO_SSL", "pt", True, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_DFS_COMPO, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_COMPO_SSL','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_COMPO_SSL','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_COMPO_SSL','pt')")
        assert last["count"][0] - first["count"][0] == 1600
        re = self.conn.run("select x,y from loadTable('dfs://valuedb_DFS_COMPO_SSL','pt')")
        x = ["a"] * 1600
        y = ["y", "z"] * 800
        y.sort()
        ex = pd.DataFrame({
            "x": x,
            "y": y
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://valuedb_DFS_COMPO_SSL")')

    def test_multithreadTableWriterTest_TSDB_single_thread(self):
        script_TSDB = """
            if(existsDatabase("dfs://valuedb_DFS_TSDB")){
                dropDatabase("dfs://valuedb_DFS_TSDB")
            }
            t = table(1000:0, `id`x, [LONG, STRING])
            db = database("dfs://valuedb_DFS_TSDB", VALUE, `a`b`c, engine="TSDB")
            pt= db.createPartitionedTable(t, `pt, `x, sortColumns="id")
        """
        self.conn.run(script_TSDB)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_TSDB", "pt", False, False, [], 100, 0.1, 1, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_TSDB_Huge, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_TSDB','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_TSDB','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_TSDB','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://valuedb_DFS_TSDB','pt')")
        id = list(range(3000)) * 10
        id.sort()
        x = ["a"] * 30000
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://valuedb_DFS_TSDB")')

    def test_multithreadTableWriterTest_TSDB_multi_thread(self):
        script_TSDB = """
            if(existsDatabase("dfs://valuedb_DFS_TSDB")){
                dropDatabase("dfs://valuedb_DFS_TSDB")
            }
            t = table(1000:0, `id`x, [LONG, STRING])
            db = database("dfs://valuedb_DFS_TSDB", VALUE, `a`b`c, engine="TSDB")
            pt= db.createPartitionedTable(t, `pt, `x, sortColumns="id")
        """
        self.conn.run(script_TSDB)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://valuedb_DFS_TSDB", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_TSDB_Huge, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://valuedb_DFS_TSDB','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_TSDB','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://valuedb_DFS_TSDB','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://valuedb_DFS_TSDB','pt')")
        id = list(range(3000)) * 10
        id.sort()
        x = ["a"] * 30000
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://valuedb_DFS_TSDB")')

    def test_multithreadTableWriterTest_DFS_VALUE_datehour_datetime(self):
        script_VALUE_datehour_datetime = """
            if(existsDatabase("dfs://DFS_VALUE_datehour_datetime")){
                dropDatabase("dfs://DFS_VALUE_datehour_datetime")
            }
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("dfs://DFS_VALUE_datehour_datetime", VALUE, datehour([2012.06.15 15:30:00.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_datehour_datetime)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://DFS_VALUE_datehour_datetime", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_datetime, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_VALUE_datehour_datetime','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_datehour_datetime','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_datehour_datetime','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_VALUE_datehour_datetime','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_VALUE_datehour_datetime")')

    def test_multithreadTableWriterTest_DFS_VALUE_datehour_timestamp(self):
        script_VALUE_datehour_timestamp = """
            if(existsDatabase("dfs://DFS_VALUE_datehour_timestamp")){
                dropDatabase("dfs://DFS_VALUE_datehour_timestamp")
            }
            t = table(1000:0, `id`x, [LONG, TIMESTAMP])
            db = database("dfs://DFS_VALUE_datehour_timestamp", VALUE, datehour([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_datehour_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://DFS_VALUE_datehour_timestamp", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_timestamp, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_VALUE_datehour_timestamp','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_datehour_timestamp','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_datehour_timestamp','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_VALUE_datehour_timestamp','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_VALUE_datehour_timestamp")')

    def test_multithreadTableWriterTest_DFS_VALUE_datehour_nanotimestamp(self):
        script_VALUE_datehour_timestamp = """
            if(existsDatabase("dfs://DFS_VALUE_datehour_nanotimestamp")){
                dropDatabase("dfs://DFS_VALUE_datehour_nanotimestamp")
            }
            t = table(1000:0, `id`x, [LONG, NANOTIMESTAMP])
            db = database("dfs://DFS_VALUE_datehour_nanotimestamp", VALUE, datehour([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_datehour_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://DFS_VALUE_datehour_nanotimestamp", "pt", False, False, [], 100, 0.1, 10,
            "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_nanotimestamp, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_VALUE_datehour_nanotimestamp','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_datehour_nanotimestamp','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_datehour_nanotimestamp','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_VALUE_datehour_nanotimestamp','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008007006", "2013-06-15T17:30:10.008007006"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_VALUE_datehour_nanotimestamp")')

    def test_multithreadTableWriterTest_DFS_VALUE_date_datetime(self):
        script_VALUE_date_datetime = """
            if(existsDatabase("dfs://DFS_VALUE_date_datetime")){
                dropDatabase("dfs://DFS_VALUE_date_datetime")
            }
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("dfs://DFS_VALUE_date_datetime", VALUE, date([2012.06.15 15:30:00.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_datetime)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://DFS_VALUE_date_datetime", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_datetime, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_VALUE_date_datetime','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_date_datetime','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_date_datetime','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_VALUE_date_datetime','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_VALUE_date_datetime")')

    def test_multithreadTableWriterTest_DFS_VALUE_date_timestamp(self):
        script_VALUE_date_timestamp = """
            if(existsDatabase("dfs://DFS_VALUE_date_timestamp")){
                dropDatabase("dfs://DFS_VALUE_date_timestamp")
            }
            t = table(1000:0, `id`x, [LONG, TIMESTAMP])
            db = database("dfs://DFS_VALUE_date_timestamp", VALUE, date([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://DFS_VALUE_date_timestamp", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_timestamp, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_VALUE_date_timestamp','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_date_timestamp','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_date_timestamp','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_VALUE_date_timestamp','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_VALUE_date_timestamp")')

    def test_multithreadTableWriterTest_DFS_VALUE_date_nanotimestamp(self):
        script_VALUE_date_timestamp = """
            if(existsDatabase("dfs://DFS_VALUE_date_nanotimestamp")){
                dropDatabase("dfs://DFS_VALUE_date_nanotimestamp")
            }
            t = table(1000:0, `id`x, [LONG, NANOTIMESTAMP])
            db = database("dfs://DFS_VALUE_date_nanotimestamp", VALUE, date([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://DFS_VALUE_date_nanotimestamp", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_nanotimestamp, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_VALUE_date_nanotimestamp','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_date_nanotimestamp','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_date_nanotimestamp','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_VALUE_date_nanotimestamp','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008007006", "2013-06-15T17:30:10.008007006"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_VALUE_date_nanotimestamp")')

    def test_multithreadTableWriterTest_DFS_VALUE_month_datetime(self):
        script_VALUE_month_datetime = """
            if(existsDatabase("dfs://DFS_VALUE_month_datetime")){
                dropDatabase("dfs://DFS_VALUE_month_datetime")
            }
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("dfs://DFS_VALUE_month_datetime", VALUE, month([2012.06.15 15:30:00.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_month_datetime)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://DFS_VALUE_month_datetime", "pt", False,
                                              False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_datetime, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_VALUE_month_datetime','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_month_datetime','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_month_datetime','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_VALUE_month_datetime','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_VALUE_month_datetime")')

    def test_multithreadTableWriterTest_DFS_VALUE_month_timestamp(self):
        script_VALUE_month_timestamp = """
            if(existsDatabase("dfs://_DFS_VALUE_month_timestamp")){
                dropDatabase("dfs://_DFS_VALUE_month_timestamp")
            }
            t = table(1000:0, `id`x, [LONG, TIMESTAMP])
            db = database("dfs://_DFS_VALUE_month_timestamp", VALUE, month([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_month_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://_DFS_VALUE_month_timestamp", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_timestamp, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://_DFS_VALUE_month_timestamp','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://_DFS_VALUE_month_timestamp','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://_DFS_VALUE_month_timestamp','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://_DFS_VALUE_month_timestamp','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://_DFS_VALUE_month_timestamp")')

    def test_multithreadTableWriterTest_DFS_VALUE_month_nanotimestamp(self):
        script_VALUE_month_timestamp = """
            if(existsDatabase("dfs://DFS_VALUE_month_nanotimestamp")){
                dropDatabase("dfs://DFS_VALUE_month_nanotimestamp")
            }
            t = table(1000:0, `id`x, [LONG, NANOTIMESTAMP])
            db = database("dfs://DFS_VALUE_month_nanotimestamp", VALUE, month([2012.06.15 15:32:10.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_month_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://DFS_VALUE_month_nanotimestamp", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_nanotimestamp, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_VALUE_month_nanotimestamp','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_month_nanotimestamp','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_VALUE_month_nanotimestamp','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_VALUE_month_nanotimestamp','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008007006", "2013-06-15T17:30:10.008007006"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_VALUE_month_nanotimestamp")')

    def test_multithreadTableWriterTest_DFS_RANGE_date_date(self):
        script_VALUE_date_date = """
            if(existsDatabase("dfs://DFS_RANGE_date_date")){
                dropDatabase("dfs://DFS_RANGE_date_date")
            }
            t = table(1000:0, `id`x, [LONG, DATE])
            db = database("dfs://DFS_RANGE_date_date", RANGE, date([2010.06.15 15:30:00.158,2012.08.15 17:30:10.008, 2014.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_date)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://DFS_RANGE_date_date", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_date, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_RANGE_date_date','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_RANGE_date_date','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_RANGE_date_date','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_RANGE_date_date','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15", "2013-06-15"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_RANGE_date_date")')

    def test_multithreadTableWriterTest_DFS_RANGE_date_datetime(self):
        script_VALUE_date_datetime = """
            if(existsDatabase("dfs://DFS_RANGE_date_datetime")){
                dropDatabase("dfs://DFS_RANGE_date_datetime")
            }
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("dfs://DFS_RANGE_date_datetime", RANGE, date([2010.06.15 15:30:00.158,2012.08.15 17:30:10.008, 2014.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_datetime)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://DFS_RANGE_date_datetime", "pt", False,
                                              False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_datetime, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_RANGE_date_datetime','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_RANGE_date_datetime','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_RANGE_date_datetime','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_RANGE_date_datetime','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10", "2013-06-15T17:30:10"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_RANGE_date_datetime")')

    def test_multithreadTableWriterTest_DFS_RANGE_date_timestamp(self):
        script_VALUE_date_timestamp = """
            if(existsDatabase("dfs://DFS_RANGE_date_timestamp")){
                dropDatabase("dfs://DFS_RANGE_date_timestamp")
            }
            t = table(1000:0, `id`x, [LONG, TIMESTAMP])
            db = database("dfs://DFS_RANGE_date_timestamp", RANGE, date([2010.06.15 15:30:00.158,2012.08.15 17:30:10.008, 2014.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_timestamp)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://DFS_RANGE_date_timestamp", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_timestamp, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_RANGE_date_timestamp','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_RANGE_date_timestamp','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_RANGE_date_timestamp','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_RANGE_date_timestamp','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008", "2013-06-15T17:30:10.008"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_RANGE_date_timestamp")')

    def test_multithreadTableWriterTest_DFS_RANGE_date_nanotimestamp(self):
        script_VALUE_date_timestamp = """
            if(existsDatabase("dfs://DFS_RANGE_date_nanotimestamp")){
                dropDatabase("dfs://DFS_RANGE_date_nanotimestamp")
            }
            t = table(1000:0, `id`x, [LONG, NANOTIMESTAMP])
            db = database("dfs://DFS_RANGE_date_nanotimestamp", RANGE, date([2010.06.15 15:30:00.158,2012.08.15 17:30:10.008, 2014.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_date_timestamp)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://DFS_RANGE_date_nanotimestamp", "pt",
                                              False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_nanotimestamp, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://DFS_RANGE_date_nanotimestamp','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://DFS_RANGE_date_nanotimestamp','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://DFS_RANGE_date_nanotimestamp','pt')")
        assert last["count"][0] - first["count"][0] == 30000
        re = self.conn.run("select * from loadTable('dfs://DFS_RANGE_date_nanotimestamp','pt')")
        id = [0] * 15000
        id_last = [1] * 15000
        id.extend(id_last)
        x = np.array(["2012-06-15T15:30:10.008007006", "2013-06-15T17:30:10.008007006"], dtype="datetime64[ns]")
        x = np.repeat(x, 15000)
        x.sort()
        ex = pd.DataFrame({
            "id": id,
            "x": x
        })
        assert_frame_equal(re, ex)
        self.conn.run('dropDatabase("dfs://DFS_RANGE_date_nanotimestamp")')

    def test_multithreadTableWriterTest_data_size_between_1024_1048576(self):
        script_data_size_between_1024_1048576 = """
            if(existsDatabase("dfs://data_size_between_1024_1048576")){
                dropDatabase("dfs://data_size_between_1024_1048576")
            }
            t = table(1000:0, `id`x, [LONG, INT])
            db = database("dfs://data_size_between_1024_1048576", RANGE, 0 1000 2000)
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_data_size_between_1024_1048576)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://data_size_between_1024_1048576", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_data_size_between_1024_1048576, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://data_size_between_1024_1048576','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://data_size_between_1024_1048576','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://data_size_between_1024_1048576','pt')")
        assert last["count"][0] - first["count"][0] == 20000
        re = self.conn.run("select * from loadTable('dfs://data_size_between_1024_1048576','pt')")
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
        self.conn.run('dropDatabase("dfs://data_size_between_1024_1048576")')

    def test_multithreadTableWriterTest_data_size_larger_than_1048576(self):
        script_data_size_larger_than_1048576 = """
            if(existsDatabase("dfs://data_size_larger_than_1048576")){
                dropDatabase("dfs://data_size_larger_than_1048576")
            }
            t = table(1000:0, `id`x, [LONG, INT])
            db = database("dfs://data_size_larger_than_1048576", RANGE, 0 1000000 2000000)
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_data_size_larger_than_1048576)
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "dfs://data_size_larger_than_1048576", "pt", False, False, [], 100, 0.1, 10, "x")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_data_size_larger_than_1048576, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://data_size_larger_than_1048576','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://data_size_larger_than_1048576','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://data_size_larger_than_1048576','pt')")
        assert last["count"][0] - first["count"][0] == 150000
        re = self.conn.run("select * from loadTable('dfs://data_size_larger_than_1048576','pt')")
        id = re["id"].to_list()
        id.sort()
        x = re["x"].to_list()
        x.sort()
        ex_id = list(range(15000)) * 10
        ex_id.sort()
        ex_x = list(range(15000)) * 10
        ex_x.sort()
        assert id == ex_id
        assert x == ex_x
        self.conn.run('dropDatabase("dfs://data_size_larger_than_1048576")')

    def test_multithreadTableWriterTest_more_than_200_cols(self):
        script_more_than_200_cols = """
            if(existsDatabase("dfs://more_than_200_cols")){
                dropDatabase("dfs://more_than_200_cols")
            }
            col_name = array(STRING, 0)
            for(i in 1..300){
                col_name_element = "col"+string(i)
                col_name.append!(col_name_element)
            }
            col_type = take(INT, 300)
            t = table(1000:0, col_name, col_type)
            db = database("dfs://more_than_200_cols", RANGE, 0 50 200)
            pt= db.createPartitionedTable(t, `pt, `col1)
        """
        self.conn.run(script_more_than_200_cols)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://more_than_200_cols", "pt", False, False,
                                              [], 100, 0.1, 10, "col1")
        threads = []
        for i in range(10):
            threads.append(threading.Thread(target=self.insert_more_than_200_cols, args=(writer, i,)))
        self.conn.run("delete from loadTable('dfs://more_than_200_cols','pt')")
        first = self.conn.run("select count(*) from loadTable('dfs://more_than_200_cols','pt')")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from loadTable('dfs://more_than_200_cols','pt')")
        assert last["count"][0] - first["count"][0] == 1000
        for i in range(300):
            re = self.conn.run("select * from loadTable('dfs://more_than_200_cols','pt')")
            col = re["col{}".format(i + 1)].to_list()
            col.sort()
            ex_col = list(range(100)) * 10
            ex_col.sort()
            assert col == ex_col
        self.conn.run('dropDatabase("dfs://more_than_200_cols")')

    def test_multithreadTableWriterTest_all_type_compress_Delta(self):
        script_all_type_compress_Delta = """
            t = table(1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`arrayVector, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, INT[]])
            share t as all_type_compress_Delta
        """
        self.conn.run(script_all_type_compress_Delta)
        pattern = re.compile(".*Failed to save the inserted data.*")
        compress = ["delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta",
                    "delta", "delta", "delta", "delta", "delta", "delta", "delta", "delta"]
        writer = ddb.MultithreadedTableWriter(
            HOST, PORT, USER, PASSWD, "", "all_type_compress_Delta", False, False, [], 100, 0.1, 1, "", compress)
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_all_type_compress, args=(writer, i,)))
        self.conn.run("delete from all_type_compress_Delta")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        errorInfo = writer.getStatus().errorInfo
        flag = pattern.match(errorInfo)
        assert flag is not None
        self.conn.run("undef(`all_type_compress_Delta, SHARED)")

    def test_multithreadTableWriterTest_all_type_compress_LZ4(self):
        script_all_type_compress_LZ4 = """
            t = table(1000:0, `bool`char`short`int`long`date`month`time`second`minute`datehour`datetime`timestamp`nanotime`nanotimestamp`float`double`string`arrayVector, [BOOL, CHAR, SHORT, INT, LONG, DATE, MONTH, TIME, SECOND, MINUTE, DATEHOUR, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, INT[]])
            share t as all_type_compress_LZ4
        """
        self.conn.run(script_all_type_compress_LZ4)
        compress = ["LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4", "LZ4",
                    "LZ4", "LZ4", "LZ4", "LZ4", "LZ4"]
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "all_type_compress_LZ4", False, False, [],
                                              100, 0.1, 1, "", compress)
        threads = []
        for i in range(1):
            threads.append(threading.Thread(target=self.insert_all_type_compress, args=(writer, i,)))
        self.conn.run("delete from all_type_compress_LZ4")
        first = self.conn.run("select count(*) from all_type_compress_LZ4")
        for t in threads:
            t.daemon = True
            t.start()
        for t in threads:
            t.join()
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from all_type_compress_LZ4")
        assert last["count"][0] - first["count"][0] == 6
        re = self.conn.run("select * from all_type_compress_LZ4")
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
        self.conn.run("undef(`all_type_compress_LZ4, SHARED)")

    def test_multithreadTableWriterTest_DFS_partitionCol_null(self):
        script_VALUE_datehour_datetime = """
            if(existsDatabase("dfs://DFS_VALUE_datehour_datetime")){
                dropDatabase("dfs://DFS_VALUE_datehour_datetime")
            }
            t = table(1000:0, `id`x, [LONG, DATETIME])
            db = database("dfs://DFS_VALUE_datehour_datetime", VALUE, datehour([2012.06.15 15:30:00.158,2013.06.15 17:30:10.008]))
            pt= db.createPartitionedTable(t, `pt, `x)
        """
        self.conn.run(script_VALUE_datehour_datetime)
        with pytest.raises(RuntimeError):
            ddb.MultithreadedTableWriter(
                HOST, PORT, USER, PASSWD, "dfs://DFS_VALUE_datehour_datetime", "pt", False, False, [], 100, 0.1, 10, "")
        self.conn.run('dropDatabase("dfs://DFS_VALUE_datehour_datetime")')

    def test_multithreadTableWriterTest_one_col_insert_double_and_int(self):
        tbname = 't_' + random_string(5)
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
        tbname = 'tab_' + random_string(5)
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
        self.conn.undef(tbname, 'SHARED')

    @pytest.mark.parametrize('data', [[1, 2, 3], {1}, {1: 1}, (1,)], ids=['LIST', 'SET', 'DICT', 'TUPLE'])
    def test_multithreadTableWriter_insert_error_type(self, data):
        self.conn.run("""
            t=table(100:0,[`int,`str],[INT,STRING])
            share t as `terror_type_test
        """)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "terror_type_test")
        for i in range(3):
            writer.insert(data, data)
        writer.waitForThreadCompletion()
        status = writer.getStatus()
        assert status.hasError()
        assert not status.succeed()
        assert status.errorCode == 'A1'
        assert 'Data conversion error' in status.errorInfo
        self.conn.run('undef(`terror_type_test,SHARED)')
        del writer

    def test_multithreadTableWriter_over_length(self):
        data = '1' * 256 * 1024
        tbname = 't_' + random_string(5)
        self.conn.run(f"share table(100:0,[`str],[STRING]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert status.hasError()
        assert status.errorCode == 'A5'
        assert 'Failed to save the inserted data: String too long, Serialization failed, length must be less than 256K bytes' in status.errorInfo
        assert status.sendFailedRows + status.unsentRows == 1
        self.conn.run(f'undef `{tbname},SHARED')
        del mtw

    def test_multithreadTableWriter_max_length_string_in_memory_table(self):
        data = '1' * (256 * 1024 - 1)
        tbname = 't_' + random_string(5)
        self.conn.run(f"share table(100:0,[`str],[STRING]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024 - 1
        self.conn.run(f'undef `{tbname},SHARED')
        del mtw

    def test_multithreadTableWriter_max_length_symbol_in_memory_table(self):
        data = '1' * (256 * 1024 - 1)
        tbname = 't_' + random_string(5)
        self.conn.run(f"share table(100:0,[`str],[SYMBOL]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024 - 1
        self.conn.run(f'undef `{tbname},SHARED')
        del mtw

    def test_multithreadTableWriter_max_length_blob_in_memory_table(self):
        data = '1' * 256 * 1024
        tbname = 't_' + random_string(5)
        self.conn.run(f"share table(100:0,[`str],[BLOB]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024
        self.conn.run(f'undef `{tbname},SHARED')
        del mtw

    def test_multithreadTableWriter_max_length_string_stream_table(self):
        data = '1' * (256 * 1024 - 1)
        tbname = 't_' + random_string(5)
        self.conn.run(f"share streamTable(100:0,[`str],[STRING]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024 - 1
        self.conn.run(f'undef `{tbname},SHARED')
        del mtw

    def test_multithreadTableWriter_max_length_symbol_stream_table(self):
        data = '1' * (256 * 1024 - 1)
        tbname = 't_' + random_string(5)
        self.conn.run(f"share streamTable(100:0,[`str],[SYMBOL]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024 - 1
        self.conn.run(f'undef `{tbname},SHARED')
        del mtw

    def test_multithreadTableWriter_max_length_blob_stream_table(self):
        data = '1' * 256 * 1024
        tbname = 't_' + random_string(5)
        self.conn.run(f"share streamTable(100:0,[`str],[BLOB]) as {tbname}")
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, '', tbname, threadCount=1)
        mtw.insert(data)
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'strlen({tbname}[`str][0])') == 256 * 1024
        self.conn.run(f'undef `{tbname},SHARED')
        del mtw

    def test_multithreadTableWriter_max_length_string_dfs_table(self):
        data = '1' * (256 * 1024 - 1)
        tbname = 't_' + random_string(5)
        dbPath = "dfs://multithreadTableWriter"
        self.conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[STRING,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, dbPath, tbname, threadCount=1)
        mtw.insert(data, "0")
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 64 * 1024 - 1
        del mtw

    def test_multithreadTableWriter_max_length_symbol_dfs_table(self):
        data = '1' * 255
        tbname = 't_' + random_string(5)
        dbPath = "dfs://multithreadTableWriter"
        self.conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[SYMBOL,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, dbPath, tbname, threadCount=1)
        mtw.insert(data, "0")
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 255
        del mtw

    def test_multithreadTableWriter_max_length_blob_dfs_table(self):
        data = '1' * 256 * 1024
        tbname = 't_' + random_string(5)
        dbPath = "dfs://multithreadTableWriter"
        self.conn.run(f"""
            if(existsDatabase("{dbPath}")){{
                dropDatabase("{dbPath}")
            }}
            tab=table(100:0,[`id,`test],[BLOB,STRING])
            db=database("{dbPath}",VALUE,["0","1","2"],engine=`TSDB)
            tb=db.createPartitionedTable(tab,"{tbname}",`test,sortColumns=`test)
        """)
        mtw = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, dbPath, tbname, threadCount=1)
        mtw.insert(data, "0")
        mtw.waitForThreadCompletion()
        status = mtw.getStatus()
        assert not status.hasError()
        assert self.conn.run(f'select strlen(id) from tb')['strlen_id'][0] == 256 * 1024
        del mtw
