import threading
import uuid
from enum import Enum, unique
from math import ceil

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
from numpy import matrix

from setup.settings import USER, PASSWD


def generate_uuid(prefix=None):
    if prefix is not None:
        return prefix + uuid.uuid4().hex[:8]
    return uuid.uuid4().hex[:8]


class CountBatchDownLatch:
    def __init__(self, count):
        self.count = count
        self.lock = threading.Lock()
        self.semp = threading.Semaphore(0)

    def wait(self):
        self.semp.acquire()

    def wait_s(self, timeout: float):
        return self.semp.acquire(timeout=timeout)

    def countDown(self, count):
        with self.lock:
            self.count -= count
            if self.count == 0:
                self.semp.release()

    def getCount(self):
        with self.lock:
            return self.count

    def reset(self, count):
        with self.lock:
            self.count = count


@unique
class DATAFORM(Enum):
    DF_SCALAR = keys.DF_SCALAR
    DF_VECTOR = keys.DF_VECTOR
    DF_PAIR = keys.DF_PAIR
    DF_MATRIX = keys.DF_MATRIX
    DF_DICTIONARY = keys.DF_DICTIONARY
    DF_TABLE = keys.DF_TABLE


# DF_CHART        = keys.DF_CHART


ARRAY_TYPE_BASE = keys.ARRAY_TYPE_BASE


@unique
class DATATYPE(Enum):
    DT_VOID = keys.DT_VOID
    DT_BOOL = keys.DT_BOOL
    DT_CHAR = keys.DT_BYTE
    DT_SHORT = keys.DT_SHORT
    DT_INT = keys.DT_INT
    DT_LONG = keys.DT_LONG
    DT_DATE = keys.DT_DATE
    DT_MONTH = keys.DT_MONTH
    DT_TIME = keys.DT_TIME
    DT_MINUTE = keys.DT_MINUTE
    DT_SECOND = keys.DT_SECOND
    DT_DATETIME = keys.DT_DATETIME
    DT_TIMESTAMP = keys.DT_TIMESTAMP
    DT_NANOTIME = keys.DT_NANOTIME
    DT_NANOTIMESTAMP = keys.DT_NANOTIMESTAMP
    DT_FLOAT = keys.DT_FLOAT
    DT_DOUBLE = keys.DT_DOUBLE
    DT_SYMBOL = keys.DT_SYMBOL
    DT_STRING = keys.DT_STRING
    DT_UUID = keys.DT_UUID
    DT_DATEHOUR = keys.DT_DATEHOUR
    DT_IPADDR = keys.DT_IPADDR
    DT_INT128 = keys.DT_INT128
    DT_BLOB = keys.DT_BLOB
    DT_DECIMAL32 = keys.DT_DECIMAL32
    DT_DECIMAL64 = keys.DT_DECIMAL64


TIMETYPE = [
    DATATYPE.DT_DATE, DATATYPE.DT_MONTH, DATATYPE.DT_TIME, DATATYPE.DT_MINUTE, DATATYPE.DT_SECOND,
    DATATYPE.DT_DATETIME, DATATYPE.DT_TIMESTAMP, DATATYPE.DT_NANOTIME, DATATYPE.DT_NANOTIMESTAMP,
    DATATYPE.DT_DATEHOUR,
]

COLNAMES = {}

for i in DATATYPE:
    COLNAMES[i.value] = "c" + (i.name[3:]).lower()
for i in DATATYPE:
    COLNAMES[i.value + ARRAY_TYPE_BASE] = "c" + (i.name[3:]).lower() + "_array"


def data_wrapper(func):
    def function(*args, **kwargs):
        s_func, p_func = func(*args, **kwargs)
        scripts = s_func(*args, **kwargs)
        res = p_func(*args, **kwargs)
        return scripts, res

    return function


@data_wrapper
def get_Scalar(*args, only_script=False, **kwargs):
    scripts = """
        s_void_1 = NULL

        s_bool_1 = true
        s_bool_2 = false
        s_bool_n = bool()

        s_char_1 = char(19)
        s_char_2 = char(65)
        s_char_n = char()

        s_short_1 = short(1)
        s_short_2 = short(0)
        s_short_n = short()

        s_int_1 = int(1)
        s_int_2 = int(0)
        s_int_n = int()

        s_long_1 = long(1)
        s_long_2 = long(0)
        s_long_n = long()

        s_date_1 = date(2022.01.01 12:30:56.123456789)
        s_date_2 = date(0)
        s_date_n = date()

        s_month_1 = month(2022.01.01 12:30:56.123456789)
        s_month_2 = month(23640)
        s_month_n = month()

        s_time_1 = time(2022.01.01 12:30:56.123456789)
        s_time_2 = time(0)
        s_time_n = time()

        s_minute_1 = minute(2022.01.01 12:30:56.123456789)
        s_minute_2 = minute(0)
        s_minute_n = minute()

        s_second_1 = second(2022.01.01 12:30:56.123456789)
        s_second_2 = second(0)
        s_second_n = second()

        s_datetime_1 = datetime(2022.01.01 12:30:56.123456789)
        s_datetime_2 = datetime(0)
        s_datetime_n = datetime()

        s_timestamp_1 = timestamp(2022.01.01 12:30:56.123456789)
        s_timestamp_2 = timestamp(0)
        s_timestamp_n = timestamp()

        s_nanotime_1 = nanotime(2022.01.01 12:30:56.123456789)
        s_nanotime_2 = nanotime(0)
        s_nanotime_n = nanotime()

        s_nanotimestamp_1 = nanotimestamp(2022.01.01 12:30:56.123456789)
        s_nanotimestamp_2 = nanotimestamp(0)
        s_nanotimestamp_n = nanotimestamp()

        s_float_1 = float(1.11)
        s_float_2 = float(0.0)
        s_float_n = float()

        s_double_1 = double(1.11)
        s_double_2 = double(0.0)
        s_double_n = double()

        s_string_1 = string("abc123测试")
        s_string_2 = string("")

        s_uuid_1 = uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87")
        s_uuid_2 = uuid("")

        s_datehour_1 = datehour(2022.01.01 12:30:56.123456789)
        s_datehour_2 = datehour(0)
        s_datehour_n = datehour()

        s_ip_1 = ipaddr("192.168.1.135")
        s_ip_2 = ipaddr("")

        s_int128_1 = int128("e1671797c52e15f763380b45e841ec32")
        s_int128_2 = int128("")

        s_blob_1 = blob("hello")
        s_blob_2 = blob("")

        //s_decimal32_1 = decimal32("0.1234567899", 9)
        //s_decimal32_2 = decimal32("0.0", 9)
        //s_decimal32_n = decimal32(NULL, 9)

        //s_decimal64_1 = decimal64("0.1234567899999999999", 18)
        //s_decimal64_2 = decimal64("0.0", 18)
        //s_decimal64_n = decimal64(NULL, 18)
    """
    pythons = {}
    if not only_script:
        pythons[DATATYPE.DT_VOID] = {
            "upload": [
            ],
            "download": [
                ('s_void_1', None),
            ],
        }
        pythons[DATATYPE.DT_BOOL] = {
            "upload": [
                ('s_bool_1', True),
                ('s_bool_2', False),
                ('s_bool_1', np.bool_(True)),
                ('s_bool_2', np.bool_(False)),
            ],
            "download": [
                ('s_bool_1', True),
                ('s_bool_2', False),
                ('s_bool_n', None),
            ],
        }
        pythons[DATATYPE.DT_CHAR] = {
            "upload": [
                ('s_char_1', np.int8(19)),
                ('s_char_2', np.int8(65)),
            ],
            "download": [
                ('s_char_1', 19),
                ('s_char_2', 65),
                ('s_char_n', None),
            ],
        }
        pythons[DATATYPE.DT_SHORT] = {
            "upload": [
                ('s_short_1', np.int16(1)),
                ('s_short_2', np.int16(0)),
            ],
            "download": [
                ('s_short_1', 1),
                ('s_short_2', 0),
                ('s_short_n', None),
            ],
        }
        pythons[DATATYPE.DT_INT] = {
            "upload": [
                ('s_int_1', np.int32(1)),
                ('s_int_2', np.int32(0)),
            ],
            "download": [
                ('s_int_1', 1),
                ('s_int_2', 0),
                ('s_int_n', None),
            ],
        }
        pythons[DATATYPE.DT_LONG] = {
            "upload": [
                ('s_long_1', 1),
                ('s_long_2', 0),
                ('s_long_1', np.int64(1)),
                ('s_long_2', np.int64(0)),
            ],
            "download": [
                ('s_long_1', 1),
                ('s_long_2', 0),
                ('s_long_n', None),
            ],
        }
        pythons[DATATYPE.DT_DATE] = {
            "upload": [
                ('s_date_1', np.datetime64("2022-01-01 12:30:56.123456789", "D")),
                ('s_date_2', np.datetime64(0, "D")),
                ('s_date_n', np.datetime64("", "D"))  # 这样上传是空值             # ??? 存疑 时间类型无法上传空值
            ],
            "download": [
                ('s_date_1', np.datetime64("2022-01-01 12:30:56.123456789", "D")),
                ('s_date_2', np.datetime64(0, "D")),
                ('s_date_n', None),
            ],
        }
        pythons[DATATYPE.DT_MONTH] = {
            "upload": [
                ('s_month_1', np.datetime64("2022-01-01 12:30:56.123456789", "M")),
                ('s_month_2', np.datetime64("1970-01-01 12:30:56.123456789", "M")),
                ('s_month_n', np.datetime64("", "M")),
            ],
            "download": [
                ('s_month_1', np.datetime64("2022-01-01 12:30:56.123456789", "M")),
                ('s_month_2', np.datetime64("1970-01-01 12:30:56.123456789", "M")),
                ('s_month_n', None),
            ],
        }
        pythons[DATATYPE.DT_TIME] = {
            "upload": [
                # No way to upload DT_TIME Scalar
            ],
            "download": [
                ('s_time_1', np.datetime64("1970-01-01 12:30:56.123456789", "ms")),
                ('s_time_2', np.datetime64(0, "ms")),
                ('s_time_n', None),
            ],
        }
        pythons[DATATYPE.DT_MINUTE] = {
            "upload": [
                # No way to upload DT_MINUTE Scalar
            ],
            "download": [
                ('s_minute_1', np.datetime64("1970-01-01 12:30:56.123456789", "m")),
                ('s_minute_2', np.datetime64(0, "m")),
                ('s_minute_n', None),
            ],
        }
        pythons[DATATYPE.DT_SECOND] = {
            "upload": [
                # No way to upload DT_SECOND Scalar
            ],
            "download": [
                ('s_second_1', np.datetime64("1970-01-01 12:30:56.123456789", "s")),
                ('s_second_2', np.datetime64(0, "s")),
                ('s_second_n', None),
            ],
        }
        pythons[DATATYPE.DT_DATETIME] = {
            "upload": [
                ('s_datetime_1', np.datetime64("2022-01-01 12:30:56.123456789", "s")),
                ('s_datetime_2', np.datetime64(0, "s")),
                ('s_datetime_n', np.datetime64("", "s")),
            ],
            "download": [
                ('s_datetime_1', np.datetime64("2022-01-01 12:30:56.123456789", "s")),
                ('s_datetime_2', np.datetime64(0, "s")),
                ('s_datetime_n', None),
            ],
        }
        pythons[DATATYPE.DT_TIMESTAMP] = {
            "upload": [
                ('s_timestamp_1', np.datetime64("2022-01-01 12:30:56.123456789", "ms")),
                ('s_timestamp_2', np.datetime64(0, "ms")),
                ('s_timestamp_n', np.datetime64("", "ms")),
            ],
            "download": [
                ('s_timestamp_1', np.datetime64("2022-01-01 12:30:56.123456789", "ms")),
                ('s_timestamp_2', np.datetime64(0, "ms")),
                ('s_timestamp_n', None),
            ],
        }
        pythons[DATATYPE.DT_NANOTIME] = {
            "upload": [
                # No way to upload DT_NANOTIME Scalar
            ],
            "download": [
                ('s_nanotime_1', np.datetime64("1970-01-01 12:30:56.123456789", "ns")),
                ('s_nanotime_2', np.datetime64(0, "ns")),
                ('s_nanotime_n', None),
            ],
        }
        pythons[DATATYPE.DT_NANOTIMESTAMP] = {
            "upload": [
                ('s_nanotimestamp_1', np.datetime64("2022-01-01 12:30:56.123456789", "ns")),
                ('s_nanotimestamp_2', np.datetime64(0, "ns")),
                ('s_nanotimestamp_n', np.datetime64("", "ns")),
            ],
            "download": [
                ('s_nanotimestamp_1', np.datetime64("2022-01-01 12:30:56.123456789", "ns")),
                ('s_nanotimestamp_2', np.datetime64(0, "ns")),
                ('s_nanotimestamp_n', None),
            ],
        }
        pythons[DATATYPE.DT_FLOAT] = {
            "upload": [
                ('s_float_1', np.float32("1.11")),
                ('s_float_2', np.float32("0.0")),
                # ('s_float_2', np.float32("")), #?? upload None, but server is 0.0
            ],
            "download": [
                ('s_float_1', 1.11),
                ('s_float_2', 0.0),
                ('s_float_n', None),
            ],
        }
        pythons[DATATYPE.DT_DOUBLE] = {
            "upload": [
                ('s_double_1', 1.11),
                ('s_double_2', 0.0),
                ('s_double_1', np.float64("1.11")),
                ('s_double_2', np.float64("0.0")),
            ],
            "download": [
                ('s_double_1', 1.11),
                ('s_double_2', 0.0),
                ('s_double_n', None),
            ],
        }
        # NO SYMBOL Scalar
        pythons[DATATYPE.DT_SYMBOL] = {
            "upload": [

            ],
            "download": [

            ],
        }
        pythons[DATATYPE.DT_STRING] = {
            "upload": [
                ('s_string_1', "abc123测试"),
                ('s_string_2', ""),
            ],
            "download": [
                ('s_string_1', "abc123测试"),
                ('s_string_2', None),
            ],
        }
        pythons[DATATYPE.DT_UUID] = {
            "upload": [
                # No way to upload DT_UUID Scalar
            ],
            "download": [
                ('s_uuid_1', "5d212a78-cc48-e3b1-4235-b4d91473ee87"),
                ('s_uuid_2', None),
            ],
        }
        pythons[DATATYPE.DT_DATEHOUR] = {
            "upload": [
                ('s_datehour_1', np.datetime64("2022-01-01 12:30:56.123456789", "h")),
                ('s_datehour_2', np.datetime64(0, "h")),
                ('s_datehour_n', np.datetime64("", "h")),
            ],
            "download": [
                ('s_datehour_1', np.datetime64("2022-01-01 12:30:56.123456789", "h")),
                ('s_datehour_2', np.datetime64(0, "h")),
                ('s_datehour_n', None),
            ],
        }
        pythons[DATATYPE.DT_IPADDR] = {
            "upload": [
                # No way to upload DT_IPADDR Scalar
            ],
            "download": [
                ('s_ip_1', "192.168.1.135"),
                ('s_ip_2', None),
            ],
        }
        pythons[DATATYPE.DT_INT128] = {
            "upload": [
                # No way to upload DT_INT128 Scalar
            ],
            "download": [
                ('s_int128_1', "e1671797c52e15f763380b45e841ec32"),
                ('s_int128_2', None),
            ],
        }
        pythons[DATATYPE.DT_BLOB] = {
            "upload": [
                # No way to upload DT_BLOB Scalar
            ],
            "download": [
                ('s_blob_1', b"hello"),
                ('s_blob_2', None),
            ],
        }
        # TODO: python decimal32
        pythons[DATATYPE.DT_DECIMAL32] = {
            "upload": [

            ],
            "download": [

            ],
        }
        # TODO: python decimal64
        pythons[DATATYPE.DT_DECIMAL64] = {
            "upload": [

            ],
            "download": [

            ],
        }

    def get_script(*args, **kwargs):
        return scripts

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_Vector(*args, n=100, **kwargs):
    tmp_s, _ = get_Scalar(only_script=True)
    prefix = "v_n = {}\n".format(n)
    prefix += tmp_s

    pythons = {DATATYPE.DT_VOID: {
        # NO way to create a VOID Vector
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_BOOL: {
        "scripts": """
            v_bool_0 = take([s_bool_1, s_bool_2], v_n)
            v_bool_1 = take([s_bool_1, s_bool_2, s_bool_n], v_n)
            v_bool_2 = take([s_bool_2, s_bool_n, s_bool_1], v_n)
            v_bool_3 = take([s_bool_n, s_bool_1, s_bool_2], v_n)
            v_bool_n = take([s_bool_n], v_n)
            """,
        "upload": [
            ("v_bool_0", np.tile(np.array([True, False], dtype="object"), ceil(n / 2))[:n]),
            ("v_bool_1", np.tile(np.array([True, False, None], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_2", np.tile(np.array([False, None, True], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_3", np.tile(np.array([None, True, False], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_1", np.tile(np.array([True, False, np.nan], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_2", np.tile(np.array([False, np.nan, True], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_3", np.tile(np.array([np.nan, True, False], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_1", np.tile(np.array([True, False, pd.NaT], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_2", np.tile(np.array([False, pd.NaT, True], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_3", np.tile(np.array([pd.NaT, True, False], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_0", ([True, False] * ceil(n / 2))[:n]),
            ("v_bool_1", ([True, False, None] * ceil(n / 3))[:n]),
            ("v_bool_2", ([False, None, True] * ceil(n / 3))[:n]),
            ("v_bool_3", ([None, True, False] * ceil(n / 3))[:n]),
            ("v_bool_1", ([True, False, np.nan] * ceil(n / 3))[:n]),
            ("v_bool_2", ([False, np.nan, True] * ceil(n / 3))[:n]),
            ("v_bool_3", ([np.nan, True, False] * ceil(n / 3))[:n]),
            # No way to upload All None Bool Vector
        ],
        "download": [
            ("v_bool_0", np.tile(np.array([True, False], dtype="object"), ceil(n / 2))[:n]),
            ("v_bool_1", np.tile(np.array([True, False, None], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_2", np.tile(np.array([False, None, True], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_3", np.tile(np.array([None, True, False], dtype="object"), ceil(n / 3))[:n]),
            ("v_bool_n", np.tile(np.array([None], dtype="object"), n)),
        ],
    }, DATATYPE.DT_CHAR: {
        "scripts": """
            v_char_0 = take([s_char_1, s_char_2], v_n)
            v_char_1 = take([s_char_1, s_char_2, s_char_n], v_n)
            v_char_2 = take([s_char_2, s_char_n, s_char_1], v_n)
            v_char_3 = take([s_char_n, s_char_1, s_char_2], v_n)
            v_char_n = take([s_char_n], v_n)
        """,
        "upload": [
            ("v_char_0", np.tile(np.array([19, 65], dtype="int8"), ceil(n / 2))[:n]),
            # No way to create a int8 None in numpy
            # ("v_char_1", np.tile(np.array([19, 65, None], dtype="int8"), ceil(n/3))[:n]),
            # ("v_char_2", np.tile(np.array([65, None, 19], dtype="int8"), ceil(n/3))[:n]),
            # ("v_char_3", np.tile(np.array([None, 19, 65], dtype="int8"), ceil(n/3))[:n]),
            # ("v_char_1", np.tile(np.array([19, 65, np.nan], dtype="int8"), ceil(n/3))[:n]),
            # ("v_char_2", np.tile(np.array([65, np.nan, 19], dtype="int8"), ceil(n/3))[:n]),
            # ("v_char_3", np.tile(np.array([np.nan, 19, 65], dtype="int8"), ceil(n/3))[:n]),
            # ("v_char_1", np.tile(np.array([19, 65, pd.NaT], dtype="int8"), ceil(n/3))[:n]),
            # ("v_char_2", np.tile(np.array([65, pd.NaT, 19], dtype="int8"), ceil(n/3))[:n]),
            # ("v_char_3", np.tile(np.array([pd.NaT, 19, 65], dtype="int8"), ceil(n/3))[:n]),
        ],
        "download": [
            ("v_char_0", np.tile(np.array([19, 65], dtype="int8"), ceil(n / 2))[:n]),
            ("v_char_1", np.tile(np.array([19, 65, np.nan]), ceil(n / 3))[:n]),
            ("v_char_2", np.tile(np.array([65, np.nan, 19]), ceil(n / 3))[:n]),
            ("v_char_3", np.tile(np.array([np.nan, 19, 65]), ceil(n / 3))[:n]),
            ("v_char_n", np.tile(np.array([np.nan], dtype="float64"), n)),
        ],
    }, DATATYPE.DT_SHORT: {
        "scripts": """
            v_short_0 = take([s_short_1, s_short_2], v_n)
            v_short_1 = take([s_short_1, s_short_2, s_short_n], v_n)
            v_short_2 = take([s_short_2, s_short_n, s_short_1], v_n)
            v_short_3 = take([s_short_n, s_short_1, s_short_2], v_n)
            v_short_n = take([s_short_n], v_n)
        """,
        "upload": [
            ("v_short_0", np.tile(np.array([1, 0], dtype="int16"), ceil(n / 2))[:n]),
            # No way to create a int16 None in numpy
            # ("v_short_1", np.tile(np.array([1, 0, None], dtype="int16"), ceil(n/3))[:n]),
            # ("v_short_2", np.tile(np.array([0, None, 1], dtype="int16"), ceil(n/3))[:n]),
            # ("v_short_3", np.tile(np.array([None, 1, 0], dtype="int16"), ceil(n/3))[:n]),
            # ("v_short_1", np.tile(np.array([1, 0, np.nan], dtype="int16"), ceil(n/3))[:n]),
            # ("v_short_2", np.tile(np.array([0, np.nan, 1], dtype="int16"), ceil(n/3))[:n]),
            # ("v_short_3", np.tile(np.array([np.nan, 1, 0], dtype="int16"), ceil(n/3))[:n]),
            # ("v_short_1", np.tile(np.array([1, 0, pd.NaT], dtype="int16"), ceil(n/3))[:n]),
            # ("v_short_2", np.tile(np.array([0, pd.NaT, 1], dtype="int16"), ceil(n/3))[:n]),
            # ("v_short_3", np.tile(np.array([pd.NaT, 1, 0], dtype="int16"), ceil(n/3))[:n]),
        ],
        "download": [
            ("v_short_0", np.tile(np.array([1, 0], dtype="int16"), ceil(n / 2))[:n]),
            ("v_short_1", np.tile(np.array([1, 0, np.nan]), ceil(n / 3))[:n]),
            ("v_short_2", np.tile(np.array([0, np.nan, 1]), ceil(n / 3))[:n]),
            ("v_short_3", np.tile(np.array([np.nan, 1, 0]), ceil(n / 3))[:n]),
            ("v_short_n", np.tile(np.array([np.nan], dtype="float64"), n)),
        ],
    }, DATATYPE.DT_INT: {
        "scripts": """
            v_int_0 = take([s_int_1, s_int_2], v_n)
            v_int_1 = take([s_int_1, s_int_2, s_int_n], v_n)
            v_int_2 = take([s_int_2, s_int_n, s_int_1], v_n)
            v_int_3 = take([s_int_n, s_int_1, s_int_2], v_n)
            v_int_n = take([s_int_n], v_n)
        """,
        "upload": [
            ("v_int_0", np.tile(np.array([1, 0], dtype="int32"), ceil(n / 2))[:n]),
            # No way to create a int32 None in numpy
            # ("v_int_1", np.tile(np.array([1, 0, None], dtype="int32"), ceil(n/3))[:n]),
            # ("v_int_2", np.tile(np.array([0, None, 1], dtype="int32"), ceil(n/3))[:n]),
            # ("v_int_3", np.tile(np.array([None, 1, 0], dtype="int32"), ceil(n/3))[:n]),
            # ("v_int_1", np.tile(np.array([1, 0, np.nan], dtype="int32"), ceil(n/3))[:n]),
            # ("v_int_2", np.tile(np.array([0, np.nan, 1], dtype="int32"), ceil(n/3))[:n]),
            # ("v_int_3", np.tile(np.array([np.nan, 1, 0], dtype="int32"), ceil(n/3))[:n]),
            # ("v_int_1", np.tile(np.array([1, 0, pd.NaT], dtype="int32"), ceil(n/3))[:n]),
            # ("v_int_2", np.tile(np.array([0, pd.NaT, 1], dtype="int32"), ceil(n/3))[:n]),
            # ("v_int_3", np.tile(np.array([pd.NaT, 1, 0], dtype="int32"), ceil(n/3))[:n]),
        ],
        "download": [
            ("v_int_0", np.tile(np.array([1, 0], dtype="int32"), ceil(n / 2))[:n]),
            ("v_int_1", np.tile(np.array([1, 0, np.nan]), ceil(n / 3))[:n]),
            ("v_int_2", np.tile(np.array([0, np.nan, 1]), ceil(n / 3))[:n]),
            ("v_int_3", np.tile(np.array([np.nan, 1, 0]), ceil(n / 3))[:n]),
            ("v_int_n", np.tile(np.array([np.nan], dtype="float64"), n)),
        ],
    }, DATATYPE.DT_LONG: {
        "scripts": """
            v_long_0 = take([s_long_1, s_long_2], v_n)
            v_long_1 = take([s_long_1, s_long_2, s_long_n], v_n)
            v_long_2 = take([s_long_2, s_long_n, s_long_1], v_n)
            v_long_3 = take([s_long_n, s_long_1, s_long_2], v_n)
            v_long_n = take([s_long_n], v_n)
        """,
        "upload": [
            ("v_long_0", np.tile(np.array([1, 0], dtype="object"), ceil(n / 2))[:n]),
            # No way to create a int64 None in numpy
            # ("v_long_0", np.tile(np.array([1, 0, None], dtype="object"), ceil(n/3))[:n]),
            # ("v_long_0", np.tile(np.array([0, None, 1], dtype="object"), ceil(n/3))[:n]),
            # ("v_long_0", np.tile(np.array([None, 1, 0], dtype="object"), ceil(n/3))[:n]),
            # ("v_long_0", np.tile(np.array([1, 0, np.nan], dtype="object"), ceil(n/3))[:n]),
            # ("v_long_0", np.tile(np.array([0, np.nan, 1], dtype="object"), ceil(n/3))[:n]),
            # ("v_long_0", np.tile(np.array([np.nan, 1, 0], dtype="object"), ceil(n/3))[:n]),
            # ("v_long_0", np.tile(np.array([1, 0, pd.NaT], dtype="object"), ceil(n/3))[:n]),
            # ("v_long_0", np.tile(np.array([0, pd.NaT, 1], dtype="object"), ceil(n/3))[:n]),
            # ("v_long_0", np.tile(np.array([pd.NaT, 1, 0], dtype="object"), ceil(n/3))[:n]),

            ("v_long_0", np.tile(np.array([1, 0], dtype="int64"), ceil(n / 2))[:n]),
            #
            # ("v_long_1", np.tile(np.array([1, 0, None], dtype="int64"), ceil(n/3))[:n]),
            # ("v_long_2", np.tile(np.array([0, None, 1], dtype="int64"), ceil(n/3))[:n]),
            # ("v_long_3", np.tile(np.array([None, 1, 0], dtype="int64"), ceil(n/3))[:n]),
            # ("v_long_1", np.tile(np.array([1, 0, np.nan], dtype="int64"), ceil(n/3))[:n]),
            # ("v_long_2", np.tile(np.array([0, np.nan, 1], dtype="int64"), ceil(n/3))[:n]),
            # ("v_long_3", np.tile(np.array([np.nan, 1, 0], dtype="int64"), ceil(n/3))[:n]),
            # ("v_long_1", np.tile(np.array([1, 0, pd.NaT], dtype="int64"), ceil(n/3))[:n]),
            # ("v_long_2", np.tile(np.array([0, pd.NaT, 1], dtype="int64"), ceil(n/3))[:n]),
            # ("v_long_3", np.tile(np.array([pd.NaT, 1, 0], dtype="int64"), ceil(n/3))[:n]),

            ("v_long_0", ([1, 0] * ceil(n / 2))[:n]),
            ("v_long_1", ([1, 0, None] * ceil(n / 3))[:n]),
            ("v_long_2", ([0, None, 1] * ceil(n / 3))[:n]),
            ("v_long_3", ([None, 1, 0] * ceil(n / 3))[:n]),
            ("v_long_1", ([1, 0, np.nan] * ceil(n / 3))[:n]),
            ("v_long_2", ([0, np.nan, 1] * ceil(n / 3))[:n]),
            ("v_long_3", ([np.nan, 1, 0] * ceil(n / 3))[:n]),
            ("v_long_1", ([1, 0, pd.NaT] * ceil(n / 3))[:n]),
            ("v_long_2", ([0, pd.NaT, 1] * ceil(n / 3))[:n]),
            ("v_long_3", ([pd.NaT, 1, 0] * ceil(n / 3))[:n]),
        ],
        "download": [
            ("v_long_0", np.tile(np.array([1, 0], dtype="object"), ceil(n / 2))[:n]),
            ("v_long_1", np.tile(np.array([1, 0, np.nan]), ceil(n / 3))[:n]),
            ("v_long_2", np.tile(np.array([0, np.nan, 1]), ceil(n / 3))[:n]),
            ("v_long_3", np.tile(np.array([np.nan, 1, 0]), ceil(n / 3))[:n]),
            ("v_long_n", np.tile(np.array([np.nan], dtype="float64"), n)),
        ],
    }, DATATYPE.DT_DATE: {
        "scripts": """
            v_date_0 = take([s_date_1, s_date_2], v_n)
            v_date_1 = take([s_date_1, s_date_2, s_date_n], v_n)
            v_date_2 = take([s_date_2, s_date_n, s_date_1], v_n)
            v_date_3 = take([s_date_n, s_date_1, s_date_2], v_n)
            v_date_n = take([s_date_n], v_n)
        """,
        "upload": [
            ("v_date_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"), ceil(n / 2))[:n]),
            ("v_date_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[D]"), ceil(n / 3))[:n]),
            ("v_date_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[D]"), ceil(n / 3))[:n]),
            ("v_date_3",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"), ceil(n / 3))[:n]),
            ("v_date_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[D]"), ceil(n / 3))[:n]),
            ("v_date_2",
             np.tile(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[D]"), ceil(n / 3))[:n]),
            ("v_date_3",
             np.tile(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"), ceil(n / 3))[:n]),
            # ?? upload don't allow np.nan or pd.NaT
            # ("v_date_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[D]"), ceil(n/3))[:n]),
            # ("v_date_2", np.tile(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[D]"), ceil(n/3))[:n]),
            # ("v_date_3", np.tile(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"), ceil(n/3))[:n]),
            # ("v_date_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[D]"), ceil(n/3))[:n]),
            # ("v_date_2", np.tile(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[D]"), ceil(n/3))[:n]),
            # ("v_date_3", np.tile(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"), ceil(n/3))[:n]),

            ("v_date_0",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "D"), np.datetime64(0, "D")] * ceil(n / 2))[:n]),
            ("v_date_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "D"), np.datetime64(0, "D"), None] * ceil(n / 3))[:n]),
            ("v_date_2",
             ([np.datetime64(0, "D"), None, np.datetime64("2022-01-01 12:30:56.123456789", "D")] * ceil(n / 3))[:n]),
            ("v_date_3",
             ([None, np.datetime64("2022-01-01 12:30:56.123456789", "D"), np.datetime64(0, "D")] * ceil(n / 3))[:n]),
            ("v_date_1", ([np.datetime64("2022-01-01 12:30:56.123456789", "D"), np.datetime64(0, "D"),
                           np.datetime64("", "D")] * ceil(n / 3))[:n]),
            ("v_date_2", ([np.datetime64(0, "D"), np.datetime64("", "D"),
                           np.datetime64("2022-01-01 12:30:56.123456789", "D")] * ceil(n / 3))[:n]),
            ("v_date_3", ([np.datetime64("", "D"), np.datetime64("2022-01-01 12:30:56.123456789", "D"),
                           np.datetime64(0, "D")] * ceil(n / 3))[:n]),
            ("v_date_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "D"), np.datetime64(0, "D"), np.nan] * ceil(n / 3))[:n]),
            ("v_date_2",
             ([np.datetime64(0, "D"), np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "D")] * ceil(n / 3))[:n]),
            ("v_date_3",
             ([np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "D"), np.datetime64(0, "D")] * ceil(n / 3))[:n]),
            ("v_date_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "D"), np.datetime64(0, "D"), pd.NaT] * ceil(n / 3))[:n]),
            ("v_date_2",
             ([np.datetime64(0, "D"), pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "D")] * ceil(n / 3))[:n]),
            ("v_date_3",
             ([pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "D"), np.datetime64(0, "D")] * ceil(n / 3))[:n]),
        ],
        "download": [
            ("v_date_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"), ceil(n / 2))[:n]),
            ("v_date_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[D]"), ceil(n / 3))[:n]),
            ("v_date_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[D]"), ceil(n / 3))[:n]),
            ("v_date_3",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"), ceil(n / 3))[:n]),
            ("v_date_n", np.tile(np.array([None], dtype="datetime64[D]"), n))
        ],
    }, DATATYPE.DT_MONTH: {
        "scripts": """
            v_month_0 = take([s_month_1, s_month_2], v_n)
            v_month_1 = take([s_month_1, s_month_2, s_month_n], v_n)
            v_month_2 = take([s_month_2, s_month_n, s_month_1], v_n)
            v_month_n = take([s_month_n, s_month_1, s_month_2], v_n)
        """,
        "upload": [
            ("v_month_0", np.tile(
                np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]"),
                ceil(n / 2))[:n]),
            # ?? None upload is 1970-01
            # TODO None upload as 1970-01
            # ("v_month_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789", None], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_2", np.tile(np.array(["1970-01-01 12:30:56.123456789", None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_n", np.tile(np.array([None, "2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789", ""], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_2", np.tile(np.array(["1970-01-01 12:30:56.123456789", "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_n", np.tile(np.array(["", "2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ?? upload don't allow np.nan or pd.NaT
            # ("v_month_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789", np.nan], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_2", np.tile(np.array(["1970-01-01 12:30:56.123456789", np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_n", np.tile(np.array([np.nan, "2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789", pd.NaT], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_2", np.tile(np.array(["1970-01-01 12:30:56.123456789", pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[M]"), ceil(n/3))[:n]),
            # ("v_month_n", np.tile(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]"), ceil(n/3))[:n]),

            ("v_month_0", ([np.datetime64("2022-01-01 12:30:56.123456789", "M"),
                            np.datetime64("1970-01-01 12:30:56.123456789", "M")] * ceil(n / 2))[:n]),
            ("v_month_1", ([np.datetime64("2022-01-01 12:30:56.123456789", "M"),
                            np.datetime64("1970-01-01 12:30:56.123456789", "M"), None] * ceil(n / 3))[:n]),
            ("v_month_2", ([np.datetime64("1970-01-01 12:30:56.123456789", "M"), None,
                            np.datetime64("2022-01-01 12:30:56.123456789", "M")] * ceil(n / 3))[:n]),
            ("v_month_n", ([None, np.datetime64("2022-01-01 12:30:56.123456789", "M"),
                            np.datetime64("1970-01-01 12:30:56.123456789", "M")] * ceil(n / 3))[:n]),
            ("v_month_1", ([np.datetime64("2022-01-01 12:30:56.123456789", "M"),
                            np.datetime64("1970-01-01 12:30:56.123456789", "M"), np.nan] * ceil(n / 3))[:n]),
            ("v_month_2", ([np.datetime64("1970-01-01 12:30:56.123456789", "M"), np.nan,
                            np.datetime64("2022-01-01 12:30:56.123456789", "M")] * ceil(n / 3))[:n]),
            ("v_month_n", ([np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "M"),
                            np.datetime64("1970-01-01 12:30:56.123456789", "M")] * ceil(n / 3))[:n]),
            ("v_month_1", ([np.datetime64("2022-01-01 12:30:56.123456789", "M"),
                            np.datetime64("1970-01-01 12:30:56.123456789", "M"), pd.NaT] * ceil(n / 3))[:n]),
            ("v_month_2", ([np.datetime64("1970-01-01 12:30:56.123456789", "M"), pd.NaT,
                            np.datetime64("2022-01-01 12:30:56.123456789", "M")] * ceil(n / 3))[:n]),
            ("v_month_n", ([pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "M"),
                            np.datetime64("1970-01-01 12:30:56.123456789", "M")] * ceil(n / 3))[:n]),
        ],
        "download": [
            ("v_month_0", np.tile(
                np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]"),
                ceil(n / 2))[:n]),
            ("v_month_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789", None],
                                           dtype="datetime64[M]"), ceil(n / 3))[:n]),
            ("v_month_2", np.tile(np.array(["1970-01-01 12:30:56.123456789", None, "2022-01-01 12:30:56.123456789"],
                                           dtype="datetime64[M]"), ceil(n / 3))[:n]),
            ("v_month_n", np.tile(np.array([None, "2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"],
                                           dtype="datetime64[M]"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_TIME: {
        "scripts": """
            v_time_0 = take([s_time_1, s_time_2], v_n)
            v_time_1 = take([s_time_1, s_time_2, s_time_n], v_n)
            v_time_2 = take([s_time_2, s_time_n, s_time_1], v_n)
            v_time_n = take([s_time_n, s_time_1, s_time_2], v_n)
        """,
        "upload": [
            # No way to upload DT_TIME Vector
        ],
        "download": [
            ("v_time_0",
             np.tile(np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"), ceil(n / 2))[:n]),
            ("v_time_1",
             np.tile(np.array(["1970-01-01 12:30:56.123456789", 0, None], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
            ("v_time_2",
             np.tile(np.array([0, None, "1970-01-01 12:30:56.123456789"], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
            ("v_time_n",
             np.tile(np.array([None, "1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_MINUTE: {
        "scripts": """
            v_minute_0 = take([s_minute_1, s_minute_2], v_n)
            v_minute_1 = take([s_minute_1, s_minute_2, s_minute_n], v_n)
            v_minute_2 = take([s_minute_2, s_minute_n, s_minute_1], v_n)
            v_minute_n = take([s_minute_n, s_minute_1, s_minute_2], v_n)
        """,
        "upload": [
            # No way to upload DT_MINUTE Vector
        ],
        "download": [
            ("v_minute_0",
             np.tile(np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[m]"), ceil(n / 2))[:n]),
            ("v_minute_1",
             np.tile(np.array(["1970-01-01 12:30:56.123456789", 0, None], dtype="datetime64[m]"), ceil(n / 3))[:n]),
            ("v_minute_2",
             np.tile(np.array([0, None, "1970-01-01 12:30:56.123456789"], dtype="datetime64[m]"), ceil(n / 3))[:n]),
            ("v_minute_n",
             np.tile(np.array([None, "1970-01-01 12:30:56.123456789", 0], dtype="datetime64[m]"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_SECOND: {
        "scripts": """
            v_second_0 = take([s_second_1, s_second_2], v_n)
            v_second_1 = take([s_second_1, s_second_2, s_second_n], v_n)
            v_second_2 = take([s_second_2, s_second_n, s_second_1], v_n)
            v_second_n = take([s_second_n, s_second_1, s_second_2], v_n)
        """,
        "upload": [
            # No way to upload DT_second Vector
        ],
        "download": [
            ("v_second_0",
             np.tile(np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"), ceil(n / 2))[:n]),
            ("v_second_1",
             np.tile(np.array(["1970-01-01 12:30:56.123456789", 0, None], dtype="datetime64[s]"), ceil(n / 3))[:n]),
            ("v_second_2",
             np.tile(np.array([0, None, "1970-01-01 12:30:56.123456789"], dtype="datetime64[s]"), ceil(n / 3))[:n]),
            ("v_second_n",
             np.tile(np.array([None, "1970-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_DATETIME: {
        "scripts": """
            v_datetime_0 = take([s_datetime_1, s_datetime_2], v_n)
            v_datetime_1 = take([s_datetime_1, s_datetime_2, s_datetime_n], v_n)
            v_datetime_2 = take([s_datetime_2, s_datetime_n, s_datetime_1], v_n)
            v_datetime_n = take([s_datetime_n, s_datetime_1, s_datetime_2], v_n)
        """,
        "upload": [
            ("v_datetime_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"), ceil(n / 2))[:n]),
            ("v_datetime_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[s]"), ceil(n / 3))[:n]),
            ("v_datetime_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[s]"), ceil(n / 3))[:n]),
            ("v_datetime_n",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"), ceil(n / 3))[:n]),
            ("v_datetime_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[s]"), ceil(n / 3))[:n]),
            ("v_datetime_2",
             np.tile(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[s]"), ceil(n / 3))[:n]),
            ("v_datetime_n",
             np.tile(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"), ceil(n / 3))[:n]),

            # ("v_datetime_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[s]"), ceil(n/3))[:n]),
            # ("v_datetime_2", np.tile(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[s]"), ceil(n/3))[:n]),
            # ("v_datetime_n", np.tile(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"), ceil(n/3))[:n]),
            # ("v_datetime_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[s]"), ceil(n/3))[:n]),
            # ("v_datetime_2", np.tile(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[s]"), ceil(n/3))[:n]),
            # ("v_datetime_n", np.tile(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"), ceil(n/3))[:n]),

            ("v_datetime_0",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "s"), np.datetime64(0, "s")] * ceil(n / 2))[:n]),
            ("v_datetime_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "s"), np.datetime64(0, "s"), None] * ceil(n / 3))[:n]),
            ("v_datetime_2",
             ([np.datetime64(0, "s"), None, np.datetime64("2022-01-01 12:30:56.123456789", "s")] * ceil(n / 3))[:n]),
            ("v_datetime_n",
             ([None, np.datetime64("2022-01-01 12:30:56.123456789", "s"), np.datetime64(0, "s")] * ceil(n / 3))[:n]),
            ("v_datetime_1", ([np.datetime64("2022-01-01 12:30:56.123456789", "s"), np.datetime64(0, "s"),
                               np.datetime64("", "s")] * ceil(n / 3))[:n]),
            ("v_datetime_2", ([np.datetime64(0, "s"), np.datetime64("", "s"),
                               np.datetime64("2022-01-01 12:30:56.123456789", "s")] * ceil(n / 3))[:n]),
            ("v_datetime_n", ([np.datetime64("", "s"), np.datetime64("2022-01-01 12:30:56.123456789", "s"),
                               np.datetime64(0, "s")] * ceil(n / 3))[:n]),
            ("v_datetime_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "s"), np.datetime64(0, "s"), np.nan] * ceil(n / 3))[:n]),
            ("v_datetime_2",
             ([np.datetime64(0, "s"), np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "s")] * ceil(n / 3))[:n]),
            ("v_datetime_n",
             ([np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "s"), np.datetime64(0, "s")] * ceil(n / 3))[:n]),
            ("v_datetime_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "s"), np.datetime64(0, "s"), pd.NaT] * ceil(n / 3))[:n]),
            ("v_datetime_2",
             ([np.datetime64(0, "s"), pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "s")] * ceil(n / 3))[:n]),
            ("v_datetime_n",
             ([pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "s"), np.datetime64(0, "s")] * ceil(n / 3))[:n]),
        ],
        "download": [
            ("v_datetime_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"), ceil(n / 2))[:n]),
            ("v_datetime_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[s]"), ceil(n / 3))[:n]),
            ("v_datetime_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[s]"), ceil(n / 3))[:n]),
            ("v_datetime_n",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_TIMESTAMP: {
        "scripts": """
            v_timestamp_0 = take([s_timestamp_1, s_timestamp_2], v_n)
            v_timestamp_1 = take([s_timestamp_1, s_timestamp_2, s_timestamp_n], v_n)
            v_timestamp_2 = take([s_timestamp_2, s_timestamp_n, s_timestamp_1], v_n)
            v_timestamp_n = take([s_timestamp_n, s_timestamp_1, s_timestamp_2], v_n)
        """,
        "upload": [
            ("v_timestamp_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"), ceil(n / 2))[:n]),
            ("v_timestamp_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
            ("v_timestamp_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
            ("v_timestamp_n",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
            ("v_timestamp_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
            ("v_timestamp_2",
             np.tile(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
            ("v_timestamp_n",
             np.tile(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"), ceil(n / 3))[:n]),

            # ("v_timestamp_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[ms]"), ceil(n/3))[:n]),
            # ("v_timestamp_2", np.tile(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ms]"), ceil(n/3))[:n]),
            # ("v_timestamp_n", np.tile(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"), ceil(n/3))[:n]),
            # ("v_timestamp_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[ms]"), ceil(n/3))[:n]),
            # ("v_timestamp_2", np.tile(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ms]"), ceil(n/3))[:n]),
            # ("v_timestamp_n", np.tile(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"), ceil(n/3))[:n]),

            ("v_timestamp_0",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "ms"), np.datetime64(0, "ms")] * ceil(n / 2))[:n]),
            ("v_timestamp_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "ms"), np.datetime64(0, "ms"), None] * ceil(n / 3))[:n]),
            ("v_timestamp_2",
             ([np.datetime64(0, "ms"), None, np.datetime64("2022-01-01 12:30:56.123456789", "ms")] * ceil(n / 3))[:n]),
            ("v_timestamp_n",
             ([None, np.datetime64("2022-01-01 12:30:56.123456789", "ms"), np.datetime64(0, "ms")] * ceil(n / 3))[:n]),
            ("v_timestamp_1", ([np.datetime64("2022-01-01 12:30:56.123456789", "ms"), np.datetime64(0, "ms"),
                                np.datetime64("", "ms")] * ceil(n / 3))[:n]),
            ("v_timestamp_2", ([np.datetime64(0, "ms"), np.datetime64("", "ms"),
                                np.datetime64("2022-01-01 12:30:56.123456789", "ms")] * ceil(n / 3))[:n]),
            ("v_timestamp_n", ([np.datetime64("", "ms"), np.datetime64("2022-01-01 12:30:56.123456789", "ms"),
                                np.datetime64(0, "ms")] * ceil(n / 3))[:n]),
            ("v_timestamp_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "ms"), np.datetime64(0, "ms"), np.nan] * ceil(n / 3))[
             :n]),
            ("v_timestamp_2",
             ([np.datetime64(0, "ms"), np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "ms")] * ceil(n / 3))[
             :n]),
            ("v_timestamp_n",
             ([np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "ms"), np.datetime64(0, "ms")] * ceil(n / 3))[
             :n]),
            ("v_timestamp_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "ms"), np.datetime64(0, "ms"), pd.NaT] * ceil(n / 3))[
             :n]),
            ("v_timestamp_2",
             ([np.datetime64(0, "ms"), pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "ms")] * ceil(n / 3))[
             :n]),
            ("v_timestamp_n",
             ([pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "ms"), np.datetime64(0, "ms")] * ceil(n / 3))[
             :n]),
        ],
        "download": [
            ("v_timestamp_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"), ceil(n / 2))[:n]),
            ("v_timestamp_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
            ("v_timestamp_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
            ("v_timestamp_n",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_NANOTIME: {
        "scripts": """
            v_nanotime_0 = take([s_nanotime_1, s_nanotime_2], v_n)
            v_nanotime_1 = take([s_nanotime_1, s_nanotime_2, s_nanotime_n], v_n)
            v_nanotime_2 = take([s_nanotime_2, s_nanotime_n, s_nanotime_1], v_n)
            v_nanotime_n = take([s_nanotime_n, s_nanotime_1, s_nanotime_2], v_n)
        """,
        "upload": [
            # No way to upload DT_NANOTIME Vector
        ],
        "download": [
            ("v_nanotime_0",
             np.tile(np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"), ceil(n / 2))[:n]),
            ("v_nanotime_1",
             np.tile(np.array(["1970-01-01 12:30:56.123456789", 0, None], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
            ("v_nanotime_2",
             np.tile(np.array([0, None, "1970-01-01 12:30:56.123456789"], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
            ("v_nanotime_n",
             np.tile(np.array([None, "1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_NANOTIMESTAMP: {
        "scripts": """
            v_nanotimestamp_0 = take([s_nanotimestamp_1, s_nanotimestamp_2], v_n)
            v_nanotimestamp_1 = take([s_nanotimestamp_1, s_nanotimestamp_2, s_nanotimestamp_n], v_n)
            v_nanotimestamp_2 = take([s_nanotimestamp_2, s_nanotimestamp_n, s_nanotimestamp_1], v_n)
            v_nanotimestamp_n = take([s_nanotimestamp_n, s_nanotimestamp_1, s_nanotimestamp_2], v_n)
        """,
        "upload": [
            ("v_nanotimestamp_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"), ceil(n / 2))[:n]),
            ("v_nanotimestamp_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
            ("v_nanotimestamp_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
            ("v_nanotimestamp_n",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
            ("v_nanotimestamp_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
            ("v_nanotimestamp_2",
             np.tile(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
            ("v_nanotimestamp_n",
             np.tile(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"), ceil(n / 3))[:n]),

            # ("v_nanotimestamp_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[ns]"), ceil(n/3))[:n]),
            # ("v_nanotimestamp_2", np.tile(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ns]"), ceil(n/3))[:n]),
            # ("v_nanotimestamp_n", np.tile(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"), ceil(n/3))[:n]),
            # ("v_nanotimestamp_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[ns]"), ceil(n/3))[:n]),
            # ("v_nanotimestamp_2", np.tile(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ns]"), ceil(n/3))[:n]),
            # ("v_nanotimestamp_n", np.tile(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"), ceil(n/3))[:n]),

            ("v_nanotimestamp_0",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "ns"), np.datetime64(0, "ns")] * ceil(n / 2))[:n]),
            ("v_nanotimestamp_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "ns"), np.datetime64(0, "ns"), None] * ceil(n / 3))[:n]),
            ("v_nanotimestamp_2",
             ([np.datetime64(0, "ns"), None, np.datetime64("2022-01-01 12:30:56.123456789", "ns")] * ceil(n / 3))[:n]),
            ("v_nanotimestamp_n",
             ([None, np.datetime64("2022-01-01 12:30:56.123456789", "ns"), np.datetime64(0, "ns")] * ceil(n / 3))[:n]),
            ("v_nanotimestamp_1", ([np.datetime64("2022-01-01 12:30:56.123456789", "ns"), np.datetime64(0, "ns"),
                                    np.datetime64("", "ns")] * ceil(n / 3))[:n]),
            ("v_nanotimestamp_2", ([np.datetime64(0, "ns"), np.datetime64("", "ns"),
                                    np.datetime64("2022-01-01 12:30:56.123456789", "ns")] * ceil(n / 3))[:n]),
            ("v_nanotimestamp_n", ([np.datetime64("", "ns"), np.datetime64("2022-01-01 12:30:56.123456789", "ns"),
                                    np.datetime64(0, "ns")] * ceil(n / 3))[:n]),
            ("v_nanotimestamp_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "ns"), np.datetime64(0, "ns"), np.nan] * ceil(n / 3))[
             :n]),
            ("v_nanotimestamp_2",
             ([np.datetime64(0, "ns"), np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "ns")] * ceil(n / 3))[
             :n]),
            ("v_nanotimestamp_n",
             ([np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "ns"), np.datetime64(0, "ns")] * ceil(n / 3))[
             :n]),
            ("v_nanotimestamp_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "ns"), np.datetime64(0, "ns"), pd.NaT] * ceil(n / 3))[
             :n]),
            ("v_nanotimestamp_2",
             ([np.datetime64(0, "ns"), pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "ns")] * ceil(n / 3))[
             :n]),
            ("v_nanotimestamp_n",
             ([pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "ns"), np.datetime64(0, "ns")] * ceil(n / 3))[
             :n]),

        ],
        "download": [
            ("v_nanotimestamp_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"), ceil(n / 2))[:n]),
            ("v_nanotimestamp_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
            ("v_nanotimestamp_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
            ("v_nanotimestamp_n",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_FLOAT: {
        "scripts": """
            v_float_0 = take([s_float_1, s_float_2], v_n)
            v_float_1 = take([s_float_1, s_float_2, s_float_n], v_n)
            v_float_2 = take([s_float_2, s_float_n, s_float_1], v_n)
            v_float_n = take([s_float_n, s_float_1, s_float_2], v_n)
        """,
        "upload": [
            # ("v_float_0", np.tile(np.array([1.11, 0.0], dtype="float32"), ceil(n/2))[:n]),
            ("v_float_1", np.tile(np.array([1.11, 0.0, None], dtype="float32"), ceil(n / 3))[:n]),
            ("v_float_2", np.tile(np.array([0.0, None, 1.11], dtype="float32"), ceil(n / 3))[:n]),
            ("v_float_n", np.tile(np.array([None, 1.11, 0.0], dtype="float32"), ceil(n / 3))[:n]),
            ("v_float_1", np.tile(np.array([1.11, 0.0, np.nan], dtype="float32"), ceil(n / 3))[:n]),
            ("v_float_2", np.tile(np.array([0.0, np.nan, 1.11], dtype="float32"), ceil(n / 3))[:n]),
            ("v_float_n", np.tile(np.array([np.nan, 1.11, 0.0], dtype="float32"), ceil(n / 3))[:n]),
            # ("v_float_1", np.tile(np.array([1.11, 0.0, pd.NaT], dtype="float32"), ceil(n/3))[:n]),
            # ("v_float_2", np.tile(np.array([0.0, pd.NaT, 1.11], dtype="float32"), ceil(n/3))[:n]),
            # ("v_float_n", np.tile(np.array([pd.NaT, 1.11, 0.0], dtype="float32"), ceil(n/3))[:n]),
        ],
        "download": [
            ("v_float_0", np.tile(np.array([1.11, 0.0], dtype="float32"), ceil(n / 2))[:n]),
            ("v_float_1", np.tile(np.array([1.11, 0.0, np.nan], dtype="float32"), ceil(n / 3))[:n]),
            ("v_float_2", np.tile(np.array([0.0, np.nan, 1.11], dtype="float32"), ceil(n / 3))[:n]),
            ("v_float_n", np.tile(np.array([np.nan, 1.11, 0.0], dtype="float32"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_DOUBLE: {
        "scripts": """
            v_double_0 = take([s_double_1, s_double_2], v_n)
            v_double_1 = take([s_double_1, s_double_2, s_double_n], v_n)
            v_double_2 = take([s_double_2, s_double_n, s_double_1], v_n)
            v_double_n = take([s_double_n, s_double_1, s_double_2], v_n)
        """,
        "upload": [
            ("v_double_0", np.tile(np.array([1.11, 0.0], dtype="object"), ceil(n / 2))[:n]),
            ("v_double_1", np.tile(np.array([1.11, 0.0, None], dtype="object"), ceil(n / 3))[:n]),
            ("v_double_2", np.tile(np.array([0.0, None, 1.11], dtype="object"), ceil(n / 3))[:n]),
            ("v_double_n", np.tile(np.array([None, 1.11, 0.0], dtype="object"), ceil(n / 3))[:n]),
            ("v_double_0", np.tile(np.array([1.11, 0.0], dtype="float64"), ceil(n / 2))[:n]),
            ("v_double_1", np.tile(np.array([1.11, 0.0, None], dtype="float64"), ceil(n / 3))[:n]),
            ("v_double_2", np.tile(np.array([0.0, None, 1.11], dtype="float64"), ceil(n / 3))[:n]),
            ("v_double_n", np.tile(np.array([None, 1.11, 0.0], dtype="float64"), ceil(n / 3))[:n]),
            ("v_double_1", np.tile(np.array([1.11, 0.0, np.nan], dtype="float64"), ceil(n / 3))[:n]),
            ("v_double_2", np.tile(np.array([0.0, np.nan, 1.11], dtype="float64"), ceil(n / 3))[:n]),
            ("v_double_n", np.tile(np.array([np.nan, 1.11, 0.0], dtype="float64"), ceil(n / 3))[:n]),
            # ("v_double_1", np.tile(np.array([1.11, 0.0, pd.NaT], dtype="float64"), ceil(n/3))[:n]),
            # ("v_double_2", np.tile(np.array([0.0, pd.NaT, 1.11], dtype="float64"), ceil(n/3))[:n]),
            # ("v_double_n", np.tile(np.array([pd.NaT, 1.11, 0.0], dtype="float64"), ceil(n/3))[:n]),
            ("v_double_0", ([1.11, 0.0] * ceil(n / 2))[:n]),
            ("v_double_1", ([1.11, 0.0, None] * ceil(n / 3))[:n]),
            ("v_double_2", ([0.0, None, 1.11] * ceil(n / 3))[:n]),
            ("v_double_n", ([None, 1.11, 0.0] * ceil(n / 3))[:n]),
            ("v_double_1", ([1.11, 0.0, np.nan] * ceil(n / 3))[:n]),
            ("v_double_2", ([0.0, np.nan, 1.11] * ceil(n / 3))[:n]),
            ("v_double_n", ([np.nan, 1.11, 0.0] * ceil(n / 3))[:n]),
        ],
        "download": [
            ("v_double_0", np.tile(np.array([1.11, 0.], dtype="float64"), ceil(n / 2))[:n]),
            ("v_double_1", np.tile(np.array([1.11, 0., np.nan], dtype="float64"), ceil(n / 3))[:n]),
            ("v_double_2", np.tile(np.array([0., np.nan, 1.11], dtype="float64"), ceil(n / 3))[:n]),
            ("v_double_n", np.tile(np.array([np.nan, 1.11, 0.], dtype="float64"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_SYMBOL: {
        "scripts": """
            v_symbol_0 = symbol(take(["AAPL", string()], v_n))
            v_symbol_1 = symbol(take([string(), "AAPL"], v_n))
        """,
        "upload": [
            # No way to upload DT_SYMBOL Vector
        ],
        "download": [
            ("v_symbol_0", np.tile(np.array(["AAPL", ""], dtype="str"), ceil(n / 2))[:n]),
            ("v_symbol_1", np.tile(np.array(["", "AAPL"], dtype="str"), ceil(n / 2))[:n]),
        ],
    }, DATATYPE.DT_STRING: {
        "scripts": """
            v_string_0 = take([s_string_1, s_string_2], v_n)
            v_string_1 = take([s_string_2, s_string_1], v_n)
        """,
        "upload": [
            ("v_string_0", np.tile(np.array(["abc123测试", ""], dtype="object"), ceil(n / 2))[:n]),
            ("v_string_1", np.tile(np.array(["", "abc123测试"], dtype="object"), ceil(n / 2))[:n]),
            ("v_string_0", np.tile(np.array(["abc123测试", ""], dtype="str"), ceil(n / 2))[:n]),
            ("v_string_1", np.tile(np.array(["", "abc123测试"], dtype="str"), ceil(n / 2))[:n]),
        ],
        "download": [
            ("v_string_0", np.tile(np.array(["abc123测试", ""], dtype="str"), ceil(n / 2))[:n]),
            ("v_string_1", np.tile(np.array(["", "abc123测试"], dtype="str"), ceil(n / 2))[:n]),
        ],
    }, DATATYPE.DT_UUID: {
        "scripts": """
            v_uuid_0 = take([s_uuid_1, s_uuid_2], v_n)
            v_uuid_1 = take([s_uuid_2, s_uuid_1], v_n)
        """,
        "upload": [
            # No Way to upload DT_UUID Vector
        ],
        "download": [
            ("v_uuid_0", np.tile(
                np.array(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "00000000-0000-0000-0000-000000000000"], dtype="str"),
                ceil(n / 2))[:n]),
            ("v_uuid_1", np.tile(
                np.array(["00000000-0000-0000-0000-000000000000", "5d212a78-cc48-e3b1-4235-b4d91473ee87"], dtype="str"),
                ceil(n / 2))[:n]),
        ],
    }, DATATYPE.DT_DATEHOUR: {
        "scripts": """
            v_datehour_0 = take([s_datehour_1, s_datehour_2], v_n)
            v_datehour_1 = take([s_datehour_1, s_datehour_2, s_datehour_n], v_n)
            v_datehour_2 = take([s_datehour_2, s_datehour_n, s_datehour_1], v_n)
            v_datehour_n = take([s_datehour_n, s_datehour_1, s_datehour_2], v_n)
        """,
        "upload": [
            ("v_datehour_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"), ceil(n / 2))[:n]),
            ("v_datehour_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[h]"), ceil(n / 3))[:n]),
            ("v_datehour_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[h]"), ceil(n / 3))[:n]),
            ("v_datehour_n",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"), ceil(n / 3))[:n]),
            ("v_datehour_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[h]"), ceil(n / 3))[:n]),
            ("v_datehour_2",
             np.tile(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[h]"), ceil(n / 3))[:n]),
            ("v_datehour_n",
             np.tile(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"), ceil(n / 3))[:n]),

            # ("v_datehour_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[h]"), ceil(n/3))[:n]),
            # ("v_datehour_2", np.tile(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[h]"), ceil(n/3))[:n]),
            # ("v_datehour_n", np.tile(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"), ceil(n/3))[:n]),
            # ("v_datehour_1", np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[h]"), ceil(n/3))[:n]),
            # ("v_datehour_2", np.tile(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[h]"), ceil(n/3))[:n]),
            # ("v_datehour_n", np.tile(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"), ceil(n/3))[:n]),

            ("v_datehour_0",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "h"), np.datetime64(0, "h")] * ceil(n / 2))[:n]),
            ("v_datehour_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "h"), np.datetime64(0, "h"), None] * ceil(n / 3))[:n]),
            ("v_datehour_2",
             ([np.datetime64(0, "h"), None, np.datetime64("2022-01-01 12:30:56.123456789", "h")] * ceil(n / 3))[:n]),
            ("v_datehour_n",
             ([None, np.datetime64("2022-01-01 12:30:56.123456789", "h"), np.datetime64(0, "h")] * ceil(n / 3))[:n]),
            ("v_datehour_1", ([np.datetime64("2022-01-01 12:30:56.123456789", "h"), np.datetime64(0, "h"),
                               np.datetime64("", "h")] * ceil(n / 3))[:n]),
            ("v_datehour_2", ([np.datetime64(0, "h"), np.datetime64("", "h"),
                               np.datetime64("2022-01-01 12:30:56.123456789", "h")] * ceil(n / 3))[:n]),
            ("v_datehour_n", ([np.datetime64("", "h"), np.datetime64("2022-01-01 12:30:56.123456789", "h"),
                               np.datetime64(0, "h")] * ceil(n / 3))[:n]),
            ("v_datehour_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "h"), np.datetime64(0, "h"), np.nan] * ceil(n / 3))[:n]),
            ("v_datehour_2",
             ([np.datetime64(0, "h"), np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "h")] * ceil(n / 3))[:n]),
            ("v_datehour_n",
             ([np.nan, np.datetime64("2022-01-01 12:30:56.123456789", "h"), np.datetime64(0, "h")] * ceil(n / 3))[:n]),
            ("v_datehour_1",
             ([np.datetime64("2022-01-01 12:30:56.123456789", "h"), np.datetime64(0, "h"), pd.NaT] * ceil(n / 3))[:n]),
            ("v_datehour_2",
             ([np.datetime64(0, "h"), pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "h")] * ceil(n / 3))[:n]),
            ("v_datehour_n",
             ([pd.NaT, np.datetime64("2022-01-01 12:30:56.123456789", "h"), np.datetime64(0, "h")] * ceil(n / 3))[:n]),

        ],
        "download": [
            ("v_datehour_0",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"), ceil(n / 2))[:n]),
            ("v_datehour_1",
             np.tile(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[h]"), ceil(n / 3))[:n]),
            ("v_datehour_2",
             np.tile(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[h]"), ceil(n / 3))[:n]),
            ("v_datehour_n",
             np.tile(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"), ceil(n / 3))[:n]),
        ],
    }, DATATYPE.DT_IPADDR: {
        "scripts": """
            v_ip_0 = take([s_ip_1, s_ip_2], v_n)
            v_ip_1 = take([s_ip_2, s_ip_1], v_n)
        """,
        "upload": [
            # No Way to upload DT_IPADDR Vector
        ],
        "download": [
            ("v_ip_0", np.tile(np.array(["192.168.1.135", "0.0.0.0"], dtype="str"), ceil(n / 2))[:n]),
            ("v_ip_1", np.tile(np.array(["0.0.0.0", "192.168.1.135"], dtype="str"), ceil(n / 2))[:n]),
        ],
    }, DATATYPE.DT_INT128: {
        "scripts": """
            v_int128_0 = take([s_int128_1, s_int128_2], v_n)
            v_int128_1 = take([s_int128_2, s_int128_1], v_n)
        """,
        "upload": [
            # No Way to upload DT_INT128 Vector
        ],
        "download": [
            ("v_int128_0",
             np.tile(np.array(["e1671797c52e15f763380b45e841ec32", "00000000000000000000000000000000"], dtype="str"),
                     ceil(n / 2))[:n]),
            ("v_int128_1",
             np.tile(np.array(["00000000000000000000000000000000", "e1671797c52e15f763380b45e841ec32"], dtype="str"),
                     ceil(n / 2))[:n]),
        ],
    }, DATATYPE.DT_BLOB: {
        "scripts": """
            v_blob_0 = take([s_blob_1, s_blob_2], v_n)
            v_blob_1 = take([s_blob_2, s_blob_1], v_n)
        """,
        "upload": [
            # No Way to upload DT_BLOB Vector
        ],
        "download": [
            ("v_blob_0", np.tile(np.array([b"hello", b""], dtype="object"), ceil(n / 2))[:n]),
            ("v_blob_1", np.tile(np.array([b"", b"hello"], dtype="object"), ceil(n / 2))[:n]),
        ],
    }, DATATYPE.DT_DECIMAL32: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL64: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }}
    # NO SYMBOL Vector
    # TODO: python decimal32
    # TODO: python decimal64

    for x in DATATYPE:
        prefix += str(pythons[x]["scripts"])

    def get_script(*args, **kwargs):
        return prefix

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_Set(*args, n=20, **kwargs):
    tmp_s, _ = get_Scalar(only_script=True)
    prefix = "v_n = {}\n".format(n)
    prefix += tmp_s

    pythons = {DATATYPE.DT_VOID: {
        # NO way to create a VOID Set
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_BOOL: {
        # NO way to create a BOOL Set
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_CHAR: {
        "scripts": """
            set_char_0 = set([s_char_1,s_char_2])
            set_char_1 = set([s_char_1,s_char_2,s_char_n])
            set_char_2 = set([s_char_2,s_char_n,s_char_1])
            set_char_n = set([s_char_n,s_char_1,s_char_2])
        """,
        "upload": [
            ("set_char_0", set(np.array([19, 65], dtype="int8"))),
            # error
            # ("set_char_1", set(np.array([19, 65, None], dtype="int8"))),
            # ("set_char_2", set(np.array([65, None, 19], dtype="int8"))),
            # ("set_char_n", set(np.array([None, 19, 65], dtype="int8"))),
            # ("set_char_1", set(np.array([19, 65, np.nan], dtype="int8"))),
            # ("set_char_2", set(np.array([65, np.nan, 19], dtype="int8"))),
            # ("set_char_n", set(np.array([np.nan, 19, 65], dtype="int8"))),
            # ("set_char_1", set(np.array([19, 65, pd.NaT], dtype="int8"))),
            # ("set_char_2", set(np.array([65, pd.NaT, 19], dtype="int8"))),
            # ("set_char_n", set(np.array([pd.NaT, 19, 65], dtype="int8"))),
        ],
        "download": [
            ("set_char_0", {19, 65}),
            ("set_char_1", {19, 65, None}),
            ("set_char_2", {65, None, 19}),
            ("set_char_n", {None, 19, 65}),
        ],
    }, DATATYPE.DT_SHORT: {
        "scripts": """
            set_short_0 = set([s_short_1,s_short_2])
            set_short_1 = set([s_short_1,s_short_2,s_short_n])
            set_short_2 = set([s_short_2,s_short_n,s_short_1])
            set_short_n = set([s_short_n,s_short_1,s_short_2])
        """,
        "upload": [
            ("set_short_0", set(np.array([1, 0], dtype="int16"))),
            # error
            # ("set_short_1", set(np.array([1, 0, None], dtype="int16"))),
            # ("set_short_2", set(np.array([0, None, 1], dtype="int16"))),
            # ("set_short_n", set(np.array([None, 1, 0], dtype="int16"))),
            # ("set_short_1", set(np.array([1, 0, np.nan], dtype="int16"))),
            # ("set_short_2", set(np.array([0, np.nan, 1], dtype="int16"))),
            # ("set_short_n", set(np.array([np.nan, 1, 0], dtype="int16"))),
            # ("set_short_1", set(np.array([1, 0, pd.NaT], dtype="int16"))),
            # ("set_short_2", set(np.array([0, pd.NaT, 1], dtype="int16"))),
            # ("set_short_n", set(np.array([pd.NaT, 1, 0], dtype="int16"))),
        ],
        "download": [
            ("set_short_0", {1, 0}),
            ("set_short_1", {1, 0, None}),
            ("set_short_2", {0, None, 1}),
            ("set_short_n", {None, 1, 0}),
        ],
    }, DATATYPE.DT_INT: {
        "scripts": """
            set_int_0 = set([s_int_1,s_int_2])
            set_int_1 = set([s_int_1,s_int_2,s_int_n])
            set_int_2 = set([s_int_2,s_int_n,s_int_1])
            set_int_n = set([s_int_n,s_int_1,s_int_2])
        """,
        "upload": [
            ("set_int_0", set(np.array([1, 0], dtype="int32"))),
            # error
            # ("set_int_1", set(np.array([1, 0, None], dtype="int32"))),
            # ("set_int_2", set(np.array([0, None, 1], dtype="int32"))),
            # ("set_int_n", set(np.array([None, 1, 0], dtype="int32"))),
            # ("set_int_1", set(np.array([1, 0, np.nan], dtype="int32"))),
            # ("set_int_2", set(np.array([0, np.nan, 1], dtype="int32"))),
            # ("set_int_n", set(np.array([np.nan, 1, 0], dtype="int32"))),
            # ("set_int_1", set(np.array([1, 0, pd.NaT], dtype="int32"))),
            # ("set_int_2", set(np.array([0, pd.NaT, 1], dtype="int32"))),
            # ("set_int_n", set(np.array([pd.NaT, 1, 0], dtype="int32"))),
        ],
        "download": [
            ("set_int_0", {1, 0}),
            ("set_int_1", {1, 0, None}),
            ("set_int_2", {0, None, 1}),
            ("set_int_n", {None, 1, 0}),
        ],
    }, DATATYPE.DT_LONG: {
        "scripts": """
            set_long_0 = set([s_long_1,s_long_2])
            set_long_1 = set([s_long_1,s_long_2,s_long_n])
            set_long_2 = set([s_long_2,s_long_n,s_long_1])
            set_long_n = set([s_long_n,s_long_1,s_long_2])
        """,
        "upload": [
            ("set_long_0", {1, 0}),
            ("set_long_1", {1, 0, None}),
            ("set_long_2", {0, None, 1}),
            ("set_long_n", {None, 1, 0}),
            ("set_long_1", {1, 0, np.nan}),
            ("set_long_2", {0, np.nan, 1}),
            ("set_long_n", {np.nan, 1, 0}),
            ("set_long_1", {1, 0, pd.NaT}),
            ("set_long_2", {0, pd.NaT, 1}),
            ("set_long_n", {pd.NaT, 1, 0}),

            ("set_long_0", {1, 0}),
            ("set_long_1", {1, 0, None}),
            ("set_long_2", {0, None, 1}),
            ("set_long_n", {None, 1, 0}),
            ("set_long_1", {1, 0, np.nan}),
            ("set_long_2", {0, np.nan, 1}),
            ("set_long_n", {np.nan, 1, 0}),
            ("set_long_1", {1, 0, pd.NaT}),
            ("set_long_2", {0, pd.NaT, 1}),
            ("set_long_n", {pd.NaT, 1, 0}),

            ("set_long_0", set(np.array([1, 0], dtype="int64"))),
            # error
            # ("set_long_1", set(np.array([1, 0, None], dtype="int64"))),
            # ("set_long_2", set(np.array([0, None, 1], dtype="int64"))),
            # ("set_long_n", set(np.array([None, 1, 0], dtype="int64"))),
            # ("set_long_1", set(np.array([1, 0, np.nan], dtype="int64"))),
            # ("set_long_2", set(np.array([0, np.nan, 1], dtype="int64"))),
            # ("set_long_n", set(np.array([np.nan, 1, 0], dtype="int64"))),
            # ("set_long_1", set(np.array([1, 0, pd.NaT], dtype="int64"))),
            # ("set_long_2", set(np.array([0, pd.NaT, 1], dtype="int64"))),
            # ("set_long_n", set(np.array([pd.NaT, 1, 0], dtype="int64"))),

            ("set_long_0", set(np.array([1, 0], dtype="object"))),
            ("set_long_1", set(np.array([1, 0, None], dtype="object"))),
            ("set_long_2", set(np.array([0, None, 1], dtype="object"))),
            ("set_long_n", set(np.array([None, 1, 0], dtype="object"))),
            ("set_long_1", set(np.array([1, 0, np.nan], dtype="object"))),
            ("set_long_2", set(np.array([0, np.nan, 1], dtype="object"))),
            ("set_long_n", set(np.array([np.nan, 1, 0], dtype="object"))),
            ("set_long_1", set(np.array([1, 0, pd.NaT], dtype="object"))),
            ("set_long_2", set(np.array([0, pd.NaT, 1], dtype="object"))),
            ("set_long_n", set(np.array([pd.NaT, 1, 0], dtype="object"))),
        ],
        "download": [
            ("set_long_0", {1, 0}),
            ("set_long_1", {1, 0, None}),
            ("set_long_2", {0, None, 1}),
            ("set_long_n", {None, 1, 0}),
        ],
    }, DATATYPE.DT_DATE: {
        "scripts": """
            set_date_0 = set([s_date_1,s_date_2])
            set_date_1 = set([s_date_1,s_date_2,s_date_n])
            set_date_2 = set([s_date_2,s_date_n,s_date_1])
            set_date_n = set([s_date_n,s_date_1,s_date_2])
        """,
        "upload": [
            ("set_date_0", set(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"))),
            ("set_date_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[D]"))),
            ("set_date_2", set(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[D]"))),
            ("set_date_n", set(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"))),
            ("set_date_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[D]"))),
            ("set_date_2", set(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[D]"))),
            ("set_date_n", set(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"))),
            # could not convert object to numpy datetime
            # ("set_date_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[D]"))),
            # ("set_date_2", set(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[D]"))),
            # ("set_date_n", set(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"))),
            # ("set_date_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[D]"))),
            # ("set_date_2", set(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[D]"))),
            # ("set_date_n", set(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]"))),
        ],
        "download": [
            ("set_date_0", {np.datetime64('1970-01-01'), np.datetime64('2022-01-01')}),
            ("set_date_1", {np.datetime64('1970-01-01'), np.datetime64('2022-01-01'), None}),
            ("set_date_2", {np.datetime64('1970-01-01'), np.datetime64('2022-01-01'), None}),
            ("set_date_n", {np.datetime64('1970-01-01'), np.datetime64('2022-01-01'), None}),
        ],
    }, DATATYPE.DT_MONTH: {
        "scripts": """
            set_month_0 = set([s_month_1,s_month_2])
            set_month_1 = set([s_month_1,s_month_2,s_month_n])
            set_month_2 = set([s_month_2,s_month_n,s_month_1])
            set_month_n = set([s_month_n,s_month_1,s_month_2])
        """,
        "upload": [
            ("set_month_0",
             set(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]"))),
            ("set_month_1", set(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789", None],
                                         dtype="datetime64[M]"))),
            ("set_month_2", set(np.array(["1970-01-01 12:30:56.123456789", None, "2022-01-01 12:30:56.123456789"],
                                         dtype="datetime64[M]"))),
            ("set_month_n", set(np.array([None, "2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"],
                                         dtype="datetime64[M]"))),
            ("set_month_1", set(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789", ""],
                                         dtype="datetime64[M]"))),
            ("set_month_2", set(np.array(["1970-01-01 12:30:56.123456789", "", "2022-01-01 12:30:56.123456789"],
                                         dtype="datetime64[M]"))),
            ("set_month_n", set(np.array(["", "2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"],
                                         dtype="datetime64[M]"))),

            # ("set_month_1", set(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789", np.nan], dtype="datetime64[M]"))),
            # ("set_month_2", set(np.array(["1970-01-01 12:30:56.123456789", np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[M]"))),
            # ("set_month_n", set(np.array([np.nan, "2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]"))),
            # ("set_month_1", set(np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789", pd.NaT], dtype="datetime64[M]"))),
            # ("set_month_2", set(np.array(["1970-01-01 12:30:56.123456789", pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[M]"))),
            # ("set_month_n", set(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]"))),
        ],
        "download": [
            ("set_month_0", {np.datetime64('2022-01'), np.datetime64('1970-01')}),
            ("set_month_1", {np.datetime64('2022-01'), np.datetime64('1970-01'), None}),
            ("set_month_2", {np.datetime64('2022-01'), np.datetime64('1970-01'), None}),
            ("set_month_n", {np.datetime64('2022-01'), np.datetime64('1970-01'), None}),
        ],
    }, DATATYPE.DT_TIME: {
        "scripts": """
            set_time_0 = set([s_time_1,s_time_2])
            set_time_1 = set([s_time_1,s_time_2,s_time_n])
            set_time_2 = set([s_time_2,s_time_n,s_time_1])
            set_time_n = set([s_time_n,s_time_1,s_time_2])
        """,
        "upload": [
            # No way to upload DT_TIME Set
        ],
        "download": [
            ("set_time_0", {np.datetime64('1970-01-01T00:00:00.000'), np.datetime64('1970-01-01T12:30:56.123')}),
            ("set_time_1", {np.datetime64('1970-01-01T00:00:00.000'), np.datetime64('1970-01-01T12:30:56.123'), None}),
            ("set_time_2", {np.datetime64('1970-01-01T00:00:00.000'), np.datetime64('1970-01-01T12:30:56.123'), None}),
            ("set_time_n", {np.datetime64('1970-01-01T00:00:00.000'), np.datetime64('1970-01-01T12:30:56.123'), None}),
        ],
    }, DATATYPE.DT_MINUTE: {
        "scripts": """
            set_minute_0 = set([s_minute_1,s_minute_2])
            set_minute_1 = set([s_minute_1,s_minute_2,s_minute_n])
            set_minute_2 = set([s_minute_2,s_minute_n,s_minute_1])
            set_minute_n = set([s_minute_n,s_minute_1,s_minute_2])
        """,
        "upload": [
            # No way to upload DT_MINUTE Set
        ],
        "download": [
            ("set_minute_0", {np.datetime64('1970-01-01T00:00'), np.datetime64('1970-01-01T12:30')}),
            ("set_minute_1", {np.datetime64('1970-01-01T00:00'), np.datetime64('1970-01-01T12:30'), None}),
            ("set_minute_2", {np.datetime64('1970-01-01T00:00'), np.datetime64('1970-01-01T12:30'), None}),
            ("set_minute_n", {np.datetime64('1970-01-01T00:00'), np.datetime64('1970-01-01T12:30'), None}),
        ],
    }, DATATYPE.DT_SECOND: {
        "scripts": """
            set_second_0 = set([s_second_1,s_second_2])
            set_second_1 = set([s_second_1,s_second_2,s_second_n])
            set_second_2 = set([s_second_2,s_second_n,s_second_1])
            set_second_n = set([s_second_n,s_second_1,s_second_2])
        """,
        "upload": [
            # No way to upload DT_MINUTE Set
        ],
        "download": [
            ("set_second_0", {np.datetime64('1970-01-01T12:30:56'), np.datetime64('1970-01-01T00:00:00')}),
            ("set_second_1", {np.datetime64('1970-01-01T12:30:56'), np.datetime64('1970-01-01T00:00:00'), None}),
            ("set_second_2", {np.datetime64('1970-01-01T12:30:56'), np.datetime64('1970-01-01T00:00:00'), None}),
            ("set_second_n", {np.datetime64('1970-01-01T12:30:56'), np.datetime64('1970-01-01T00:00:00'), None}),
        ],
    }, DATATYPE.DT_DATETIME: {
        "scripts": """
            set_datetime_0 = set([s_datetime_1,s_datetime_2])
            set_datetime_1 = set([s_datetime_1,s_datetime_2,s_datetime_n])
            set_datetime_2 = set([s_datetime_2,s_datetime_n,s_datetime_1])
            set_datetime_n = set([s_datetime_n,s_datetime_1,s_datetime_2])
        """,
        "upload": [
            ("set_datetime_0", set(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"))),
            ("set_datetime_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[s]"))),
            ("set_datetime_2", set(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[s]"))),
            ("set_datetime_n", set(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"))),
            ("set_datetime_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[s]"))),
            ("set_datetime_2", set(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[s]"))),
            ("set_datetime_n", set(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"))),

            # ("set_datetime_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[s]"))),
            # ("set_datetime_2", set(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[s]"))),
            # ("set_datetime_n", set(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"))),
            # ("set_datetime_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[s]"))),
            # ("set_datetime_2", set(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[s]"))),
            # ("set_datetime_n", set(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]"))),
        ],
        "download": [
            ("set_datetime_0", {np.datetime64('2022-01-01T12:30:56'), np.datetime64('1970-01-01T00:00:00')}),
            ("set_datetime_1", {np.datetime64('2022-01-01T12:30:56'), np.datetime64('1970-01-01T00:00:00'), None}),
            ("set_datetime_2", {np.datetime64('2022-01-01T12:30:56'), np.datetime64('1970-01-01T00:00:00'), None}),
            ("set_datetime_n", {np.datetime64('2022-01-01T12:30:56'), np.datetime64('1970-01-01T00:00:00'), None}),
        ],
    }, DATATYPE.DT_TIMESTAMP: {
        "scripts": """
            set_timestamp_0 = set([s_timestamp_1,s_timestamp_2])
            set_timestamp_1 = set([s_timestamp_1,s_timestamp_2,s_timestamp_n])
            set_timestamp_2 = set([s_timestamp_2,s_timestamp_n,s_timestamp_1])
            set_timestamp_n = set([s_timestamp_n,s_timestamp_1,s_timestamp_2])
        """,
        "upload": [
            ("set_timestamp_0", set(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"))),
            ("set_timestamp_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[ms]"))),
            ("set_timestamp_2", set(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ms]"))),
            ("set_timestamp_n", set(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"))),
            ("set_timestamp_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[ms]"))),
            ("set_timestamp_2", set(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[ms]"))),
            ("set_timestamp_n", set(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"))),

            # ("set_timestamp_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[ms]"))),
            # ("set_timestamp_2", set(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ms]"))),
            # ("set_timestamp_n", set(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"))),
            # ("set_timestamp_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[ms]"))),
            # ("set_timestamp_2", set(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ms]"))),
            # ("set_timestamp_n", set(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]"))),
        ],
        "download": [
            ("set_timestamp_0", {np.datetime64('1970-01-01T00:00:00.000'), np.datetime64('2022-01-01T12:30:56.123')}),
            ("set_timestamp_1",
             {np.datetime64('1970-01-01T00:00:00.000'), np.datetime64('2022-01-01T12:30:56.123'), None}),
            ("set_timestamp_2",
             {np.datetime64('1970-01-01T00:00:00.000'), np.datetime64('2022-01-01T12:30:56.123'), None}),
            ("set_timestamp_n",
             {np.datetime64('1970-01-01T00:00:00.000'), np.datetime64('2022-01-01T12:30:56.123'), None}),
        ],
    }, DATATYPE.DT_NANOTIME: {
        "scripts": """
            set_nanotime_0 = set([s_nanotime_1,s_nanotime_2])
            set_nanotime_1 = set([s_nanotime_1,s_nanotime_2,s_nanotime_n])
            set_nanotime_2 = set([s_nanotime_2,s_nanotime_n,s_nanotime_1])
            set_nanotime_n = set([s_nanotime_n,s_nanotime_1,s_nanotime_2])
        """,
        "upload": [
            # No way to upload DT_NANOTIME Set
        ],
        "download": [
            ("set_nanotime_0",
             {np.datetime64('1970-01-01T00:00:00.000000000'), np.datetime64('1970-01-01T12:30:56.123456789')}),
            ("set_nanotime_1",
             {np.datetime64('1970-01-01T00:00:00.000000000'), np.datetime64('1970-01-01T12:30:56.123456789'), None}),
            ("set_nanotime_2",
             {np.datetime64('1970-01-01T00:00:00.000000000'), np.datetime64('1970-01-01T12:30:56.123456789'), None}),
            ("set_nanotime_n",
             {np.datetime64('1970-01-01T00:00:00.000000000'), np.datetime64('1970-01-01T12:30:56.123456789'), None}),
        ],
    }, DATATYPE.DT_NANOTIMESTAMP: {
        "scripts": """
            set_nanotimestamp_0 = set([s_nanotimestamp_1,s_nanotimestamp_2])
            set_nanotimestamp_1 = set([s_nanotimestamp_1,s_nanotimestamp_2,s_nanotimestamp_n])
            set_nanotimestamp_2 = set([s_nanotimestamp_2,s_nanotimestamp_n,s_nanotimestamp_1])
            set_nanotimestamp_n = set([s_nanotimestamp_n,s_nanotimestamp_1,s_nanotimestamp_2])
        """,
        "upload": [
            ("set_nanotimestamp_0", set(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"))),
            ("set_nanotimestamp_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[ns]"))),
            ("set_nanotimestamp_2", set(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ns]"))),
            ("set_nanotimestamp_n", set(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"))),
            ("set_nanotimestamp_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[ns]"))),
            ("set_nanotimestamp_2", set(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[ns]"))),
            ("set_nanotimestamp_n", set(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"))),

            # ("set_nanotimestamp_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[ns]"))),
            # ("set_nanotimestamp_2", set(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ns]"))),
            # ("set_nanotimestamp_n", set(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"))),
            # ("set_nanotimestamp_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[ns]"))),
            # ("set_nanotimestamp_2", set(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[ns]"))),
            # ("set_nanotimestamp_n", set(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]"))),
        ],
        "download": [
            ("set_nanotimestamp_0",
             {np.datetime64('1970-01-01T00:00:00.000000000'), np.datetime64('2022-01-01T12:30:56.123456789')}),
            ("set_nanotimestamp_1",
             {np.datetime64('1970-01-01T00:00:00.000000000'), np.datetime64('2022-01-01T12:30:56.123456789'), None}),
            ("set_nanotimestamp_2",
             {np.datetime64('1970-01-01T00:00:00.000000000'), np.datetime64('2022-01-01T12:30:56.123456789'), None}),
            ("set_nanotimestamp_n",
             {np.datetime64('1970-01-01T00:00:00.000000000'), np.datetime64('2022-01-01T12:30:56.123456789'), None}),
        ],
    }, DATATYPE.DT_FLOAT: {
        "scripts": """
            set_float_0 = set([s_float_1,s_float_2])
            set_float_1 = set([s_float_1,s_float_2,s_float_n])
            set_float_2 = set([s_float_2,s_float_n,s_float_1])
            set_float_n = set([s_float_n,s_float_1,s_float_2])
        """,
        "upload": [
            ("set_float_0", set(np.array([1.11, 0.0], dtype="float32"))),
            # TODO upload None or np.nan, server is nan couldn't match None in server
            # ("set_float_1", set(np.array([1.11, 0.0, None], dtype="float32"))),
            # ("set_float_2", set(np.array([0.0, None, 1.11], dtype="float32"))),
            # ("set_float_n", set(np.array([None, 1.11, 0.0], dtype="float32"))),
            # ("set_float_1", set(np.array([1.11, 0.0, np.nan], dtype="float32"))),
            # ("set_float_2", set(np.array([0.0, np.nan, 1.11], dtype="float32"))),
            # ("set_float_n", set(np.array([np.nan, 1.11, 0.0], dtype="float32"))),

        ],
        "download": [
            ("set_float_0", {1.11, 0.0}),
            ("set_float_1", {1.11, 0.0, None}),
            ("set_float_2", {0.0, None, 1.11}),
            ("set_float_n", {None, 1.11, 0.0}),
        ],
    }, DATATYPE.DT_DOUBLE: {
        "scripts": """
            set_double_0 = set([s_double_1,s_double_2])
            set_double_1 = set([s_double_1,s_double_2,s_double_n])
            set_double_2 = set([s_double_2,s_double_n,s_double_1])
            set_double_n = set([s_double_n,s_double_1,s_double_2])
        """,
        "upload": [
            ("set_double_0", set(np.array([1.11, 0.0], dtype="float64"))),
            ("set_double_1", set(np.array([1.11, 0.0, None], dtype="float64"))),
            ("set_double_2", set(np.array([0.0, None, 1.11], dtype="float64"))),
            ("set_double_n", set(np.array([None, 1.11, 0.0], dtype="float64"))),
            ("set_double_1", set(np.array([1.11, 0.0, np.nan], dtype="float64"))),
            ("set_double_2", set(np.array([0.0, np.nan, 1.11], dtype="float64"))),
            ("set_double_n", set(np.array([np.nan, 1.11, 0.0], dtype="float64"))),
            # ("set_double_1", set(np.array([1.11, 0.0, pd.NaT], dtype="float64"))),
            # ("set_double_2", set(np.array([0.0, pd.NaT, 1.11], dtype="float64"))),
            # ("set_double_n", set(np.array([pd.NaT, 1.11, 0.0], dtype="float64"))),

            ("set_double_0", set(np.array([1.11, 0.0], dtype="object"))),
            ("set_double_1", set(np.array([1.11, 0.0, None], dtype="object"))),
            ("set_double_2", set(np.array([0.0, None, 1.11], dtype="object"))),
            ("set_double_n", set(np.array([None, 1.11, 0.0], dtype="object"))),
            ("set_double_1", set(np.array([1.11, 0.0, np.nan], dtype="object"))),
            ("set_double_2", set(np.array([0.0, np.nan, 1.11], dtype="object"))),
            ("set_double_n", set(np.array([np.nan, 1.11, 0.0], dtype="object"))),
            ("set_double_1", set(np.array([1.11, 0.0, pd.NaT], dtype="object"))),
            ("set_double_2", set(np.array([0.0, pd.NaT, 1.11], dtype="object"))),
            ("set_double_n", set(np.array([pd.NaT, 1.11, 0.0], dtype="object"))),
        ],
        "download": [
            ("set_double_0", {1.11, 0.0}),
            ("set_double_1", {1.11, 0.0, None}),
            ("set_double_2", {0.0, None, 1.11}),
            ("set_double_n", {None, 1.11, 0.0}),
        ],
    }, DATATYPE.DT_SYMBOL: {
        "scripts": """
            set_symbol_0 = set(symbol(["AAPL", string()]))
            set_symbol_1 = set(symbol([string(), "AAPL"]))
        """,
        "upload": [
            # No way to upload symbol Set
        ],
        "download": [
            ("set_symbol_0", {"AAPL", None}),
            ("set_symbol_1", {None, "AAPL"}),
        ],
    }, DATATYPE.DT_STRING: {
        "scripts": """
            set_string_0 = set([s_string_1, s_string_2])
            set_string_1 = set([s_string_2, s_string_1])
        """,
        "upload": [
            ("set_string_0", set(np.array(["abc123测试", ""], dtype="object"))),
            ("set_string_1", set(np.array(["", "abc123测试"], dtype="object"))),
            ("set_string_0", set(np.array(["abc123测试", None], dtype="object"))),
            ("set_string_1", set(np.array([None, "abc123测试"], dtype="object"))),
            ("set_string_0", set(np.array(["abc123测试", np.nan], dtype="object"))),
            ("set_string_1", set(np.array([np.nan, "abc123测试"], dtype="object"))),
            ("set_string_0", set(np.array(["abc123测试", pd.NaT], dtype="object"))),
            ("set_string_1", set(np.array([pd.NaT, "abc123测试"], dtype="object"))),

        ],
        "download": [
            ("set_string_0", {"abc123测试", None}),
            ("set_string_1", {None, "abc123测试"}),
        ],
    }, DATATYPE.DT_UUID: {
        "scripts": """
            set_uuid_0 = set([s_uuid_1, s_uuid_2])
            set_uuid_1 = set([s_uuid_2, s_uuid_1])
        """,
        "upload": [
            # No Way to upload DT_UUID Set
        ],
        "download": [
            ("set_uuid_0", {"5d212a78-cc48-e3b1-4235-b4d91473ee87", None}),
            ("set_uuid_1", {None, "5d212a78-cc48-e3b1-4235-b4d91473ee87"}),
        ],
    }, DATATYPE.DT_DATEHOUR: {
        "scripts": """
            set_datehour_0 = set([s_datehour_1,s_datehour_2])
            set_datehour_1 = set([s_datehour_1,s_datehour_2,s_datehour_n])
            set_datehour_2 = set([s_datehour_2,s_datehour_n,s_datehour_1])
            set_datehour_n = set([s_datehour_n,s_datehour_1,s_datehour_2])
        """,
        "upload": [
            ("set_datehour_0", set(np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"))),
            ("set_datehour_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, None], dtype="datetime64[h]"))),
            ("set_datehour_2", set(np.array([0, None, "2022-01-01 12:30:56.123456789"], dtype="datetime64[h]"))),
            ("set_datehour_n", set(np.array([None, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"))),
            ("set_datehour_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, ""], dtype="datetime64[h]"))),
            ("set_datehour_2", set(np.array([0, "", "2022-01-01 12:30:56.123456789"], dtype="datetime64[h]"))),
            ("set_datehour_n", set(np.array(["", "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"))),

            # ("set_datehour_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, np.nan], dtype="datetime64[h]"))),
            # ("set_datehour_2", set(np.array([0, np.nan, "2022-01-01 12:30:56.123456789"], dtype="datetime64[h]"))),
            # ("set_datehour_n", set(np.array([np.nan, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"))),
            # ("set_datehour_1", set(np.array(["2022-01-01 12:30:56.123456789", 0, pd.NaT], dtype="datetime64[h]"))),
            # ("set_datehour_2", set(np.array([0, pd.NaT, "2022-01-01 12:30:56.123456789"], dtype="datetime64[h]"))),
            # ("set_datehour_n", set(np.array([pd.NaT, "2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]"))),
        ],
        "download": [
            ("set_datehour_0", {np.datetime64('1970-01-01T00', 'h'), np.datetime64('2022-01-01T12', 'h')}),
            ("set_datehour_1", {np.datetime64('1970-01-01T00', 'h'), np.datetime64('2022-01-01T12', 'h'), None}),
            ("set_datehour_2", {np.datetime64('1970-01-01T00', 'h'), np.datetime64('2022-01-01T12', 'h'), None}),
            ("set_datehour_n", {np.datetime64('1970-01-01T00', 'h'), np.datetime64('2022-01-01T12', 'h'), None}),
        ],
    }, DATATYPE.DT_IPADDR: {
        "scripts": """
            set_ip_0 = set([s_ip_1, s_ip_2])
            set_ip_1 = set([s_ip_2, s_ip_1])
        """,
        "upload": [
            # No Way to upload DT_IPADDR Vector
        ],
        "download": [
            ("set_ip_0", {"192.168.1.135", None}),
            ("set_ip_1", {None, "192.168.1.135"}),
        ],
    }, DATATYPE.DT_INT128: {
        "scripts": """
            set_int128_0 = set([s_int128_1, s_int128_2])
            set_int128_1 = set([s_int128_2, s_int128_1])
        """,
        "upload": [
            # No Way to upload DT_INT128 Vector
        ],
        "download": [
            ("set_int128_0", {"e1671797c52e15f763380b45e841ec32", None}),
            ("set_int128_1", {None, "e1671797c52e15f763380b45e841ec32"}),
        ],
    }, DATATYPE.DT_BLOB: {
        "scripts": """
            set_blob_0 = set([s_blob_1, s_blob_2])
            set_blob_1 = set([s_blob_2, s_blob_1])
        """,
        "upload": [
            # No Way to upload DT_INT128 Vector
        ],
        "download": [
            # ("set_blob_0", {"hello", None}),
            # ("set_blob_1", {None, "hello"}),
        ],
    }, DATATYPE.DT_DECIMAL32: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL64: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }}
    # NO SYMBOL Set
    # TODO: python decimal32
    # TODO: python decimal64

    for x in DATATYPE:
        prefix += str(pythons[x]["scripts"])

    def get_script(*args, **kwargs):
        return prefix

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_Dictionary(*args, **kwargs):
    tmp_s, _ = get_Scalar(only_script=True)
    prefix = ""
    prefix += tmp_s

    key_datatypes = [
        DATATYPE.DT_CHAR, DATATYPE.DT_SHORT, DATATYPE.DT_INT, DATATYPE.DT_LONG,
        DATATYPE.DT_DATE, DATATYPE.DT_MONTH, DATATYPE.DT_TIME, DATATYPE.DT_MINUTE,
        DATATYPE.DT_SECOND, DATATYPE.DT_DATETIME, DATATYPE.DT_TIMESTAMP, DATATYPE.DT_NANOTIME,
        DATATYPE.DT_NANOTIMESTAMP, DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE, DATATYPE.DT_STRING,
        DATATYPE.DT_UUID, DATATYPE.DT_DATEHOUR, DATATYPE.DT_IPADDR, DATATYPE.DT_INT128, DATATYPE.DT_BLOB,
    ]

    value_datatypes = {
        DATATYPE.DT_BOOL: "[s_bool_1, s_bool_2]",
        DATATYPE.DT_CHAR: "[s_char_1, s_char_2]",
        DATATYPE.DT_SHORT: "[s_short_1, s_short_2]",
        DATATYPE.DT_INT: "[s_int_1, s_int_2]",
        DATATYPE.DT_LONG: "[s_long_1, s_long_2]",
        DATATYPE.DT_DATE: "[s_date_1, s_date_2]",
        DATATYPE.DT_MONTH: "[s_month_1, s_month_2]",
        DATATYPE.DT_TIME: "[s_time_1, s_time_2]",
        DATATYPE.DT_MINUTE: "[s_minute_1, s_minute_2]",
        DATATYPE.DT_SECOND: "[s_second_1, s_second_2]",
        DATATYPE.DT_DATETIME: "[s_datetime_1, s_datetime_2]",
        DATATYPE.DT_TIMESTAMP: "[s_timestamp_1, s_timestamp_2]",
        DATATYPE.DT_NANOTIME: "[s_nanotime_1, s_nanotime_2]",
        DATATYPE.DT_NANOTIMESTAMP: "[s_nanotimestamp_1, s_nanotimestamp_2]",
        DATATYPE.DT_FLOAT: "[s_float_1, s_float_2]",
        DATATYPE.DT_DOUBLE: "[s_double_1, s_double_2]",
        DATATYPE.DT_STRING: "[s_string_1, s_string_2]",
        # DATATYPE.DT_UUID:"[s_uuid_1, s_uuid_2]",
        DATATYPE.DT_DATEHOUR: "[s_datehour_1, s_datehour_2]",
        # DATATYPE.DT_IPADDR:"[s_ip_1, s_ip_2]",
        DATATYPE.DT_INT128: "[s_int128_1, s_int128_2]",
        DATATYPE.DT_BLOB: "[s_blob_1, s_blob_2]",
        # DATATYPE.DT_DECIMAL32:"[s_decimal32_1, s_decimal32_2]",
        # DATATYPE.DT_DECIMAL64:"[s_decimal64_1, s_decimal64_2]",
    }

    for key in value_datatypes:
        scripts = "v_{} = {}\n".format(str(key).split('_')[-1].lower(), value_datatypes[key])
        prefix += scripts

    pythons = {DATATYPE.DT_VOID: {
        # void can't be keytype
        "scripts": [
            # NO way to create a VOID Dict
        ],
        "upload": [],
        "download": [],
    }, DATATYPE.DT_BOOL: {
        # bool can't be keytype
        "scripts": """

        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_CHAR: {
        "scripts": """
            dict_char_bool = dict(v_char, v_bool)
            dict_char_char = dict(v_char, v_char)
            dict_char_short = dict(v_char, v_short)
            dict_char_int = dict(v_char, v_int)
            dict_char_long = dict(v_char, v_long)
            dict_char_date = dict(v_char, v_date)
            dict_char_month = dict(v_char, v_month)
            dict_char_time = dict(v_char, v_time)
            dict_char_minute = dict(v_char, v_minute)
            dict_char_second = dict(v_char, v_second)
            dict_char_datetime = dict(v_char, v_datetime)
            dict_char_timestamp = dict(v_char, v_timestamp)
            dict_char_nanotime = dict(v_char, v_nanotime)
            dict_char_nanotimestamp = dict(v_char, v_nanotimestamp)
            dict_char_float = dict(v_char, v_float)
            dict_char_double = dict(v_char, v_double)
            dict_char_string = dict(v_char, v_string)
            dict_char_datehour = dict(v_char, v_datehour)
            dict_char_blob = dict(v_char, v_blob)
        """,
        "upload": [
            ("dict_char_bool", dict(zip(np.array([19, 65], dtype="int8"), np.array([True, False], dtype="bool")))),
            ("dict_char_char", dict(zip(np.array([19, 65], dtype="int8"), np.array([19, 65], dtype="int8")))),
            ("dict_char_short", dict(zip(np.array([19, 65], dtype="int8"), np.array([1, 0], dtype="int16")))),
            ("dict_char_int", dict(zip(np.array([19, 65], dtype="int8"), np.array([1, 0], dtype="int32")))),
            ("dict_char_long", dict(zip(np.array([19, 65], dtype="int8"), np.array([1, 0], dtype="int64")))),
            ("dict_char_date", dict(zip(np.array([19, 65], dtype="int8"),
                                        np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            ("dict_char_month", dict(zip(np.array([19, 65], dtype="int8"),
                                         np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"],
                                                  dtype="datetime64[M]")))),
            ("dict_char_datetime", dict(zip(np.array([19, 65], dtype="int8"),
                                            np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_char_timestamp", dict(zip(np.array([19, 65], dtype="int8"),
                                             np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_char_nanotimestamp", dict(zip(np.array([19, 65], dtype="int8"),
                                                 np.array(["2022-01-01 12:30:56.123456789", 0],
                                                          dtype="datetime64[ns]")))),
            ("dict_char_float", dict(zip(np.array([19, 65], dtype="int8"), np.array([1.11, 0.0], dtype="float32")))),
            ("dict_char_double", dict(zip(np.array([19, 65], dtype="int8"), np.array([1.11, 0.0], dtype="float64")))),
            ("dict_char_string",
             dict(zip(np.array([19, 65], dtype="int8"), np.array(["abc123测试", "", "123abc测试"], dtype="str")))),
            ("dict_char_datehour", dict(zip(np.array([19, 65], dtype="int8"),
                                            np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
        ],
        "download": [
            ("dict_char_bool", dict(zip(np.array([19, 65], dtype="int8"), np.array([True, False], dtype="bool")))),
            ("dict_char_char", dict(zip(np.array([19, 65], dtype="int8"), np.array([19, 65], dtype="int8")))),
            ("dict_char_short", dict(zip(np.array([19, 65], dtype="int8"), np.array([1, 0], dtype="int16")))),
            ("dict_char_int", dict(zip(np.array([19, 65], dtype="int8"), np.array([1, 0], dtype="int32")))),
            ("dict_char_long", dict(zip(np.array([19, 65], dtype="int8"), np.array([1, 0], dtype="int64")))),
            ("dict_char_date", dict(zip(np.array([19, 65], dtype="int8"),
                                        np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            ("dict_char_month", dict(zip(np.array([19, 65], dtype="int8"),
                                         np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"],
                                                  dtype="datetime64[M]")))),
            ("dict_char_time", dict(zip(np.array([19, 65], dtype="int8"),
                                        np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_char_minute", dict(zip(np.array([19, 65], dtype="int8"),
                                          np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[m]")))),
            ("dict_char_second", dict(zip(np.array([19, 65], dtype="int8"),
                                          np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_char_datetime", dict(zip(np.array([19, 65], dtype="int8"),
                                            np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_char_timestamp", dict(zip(np.array([19, 65], dtype="int8"),
                                             np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_char_nanotime", dict(zip(np.array([19, 65], dtype="int8"),
                                            np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]")))),
            ("dict_char_nanotimestamp", dict(zip(np.array([19, 65], dtype="int8"),
                                                 np.array(["2022-01-01 12:30:56.123456789", 0],
                                                          dtype="datetime64[ns]")))),
            ("dict_char_float", dict(zip(np.array([19, 65], dtype="int8"), np.array([1.11, 0.0], dtype="float32")))),
            ("dict_char_double", dict(zip(np.array([19, 65], dtype="int8"), np.array([1.11, 0.0], dtype="float64")))),
            ("dict_char_string",
             dict(zip(np.array([19, 65], dtype="int8"), np.array(["abc123测试", None], dtype="object")))),
            ("dict_char_datehour", dict(zip(np.array([19, 65], dtype="int8"),
                                            np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
            ("dict_char_blob", dict(zip(np.array([19, 65], dtype="int8"), np.array([b"hello", None], dtype="object")))),
        ],
    }, DATATYPE.DT_SHORT: {
        "scripts": """
            dict_short_bool = dict(v_short, v_bool)
            dict_short_char = dict(v_short, v_char)
            dict_short_short = dict(v_short, v_short)
            dict_short_int = dict(v_short, v_int)
            dict_short_long = dict(v_short, v_long)
            dict_short_date = dict(v_short, v_date)
            dict_short_month = dict(v_short, v_month)
            dict_short_time = dict(v_short, v_time)
            dict_short_minute = dict(v_short, v_minute)
            dict_short_second = dict(v_short, v_second)
            dict_short_datetime = dict(v_short, v_datetime)
            dict_short_timestamp = dict(v_short, v_timestamp)
            dict_short_nanotime = dict(v_short, v_nanotime)
            dict_short_nanotimestamp = dict(v_short, v_nanotimestamp)
            dict_short_float = dict(v_short, v_float)
            dict_short_double = dict(v_short, v_double)
            dict_short_string = dict(v_short, v_string)
            dict_short_datehour = dict(v_short, v_datehour)
            dict_short_blob = dict(v_short, v_blob)
        """,
        "upload": [
            # ("dict_short_bool",dict(zip(np.array([1, 0], dtype="int16"), np.array([True, False], dtype="bool")))),
            # ("dict_short_char",dict(zip(np.array([1, 0], dtype="int16"), np.array([19, 65], dtype="int8")))),
            # ("dict_short_short",dict(zip(np.array([1, 0], dtype="int16"), np.array([1, 0], dtype="int16")))),
            # ("dict_short_int",dict(zip(np.array([1, 0], dtype="int16"), np.array([1, 0], dtype="int32")))),
            # ("dict_short_long",dict(zip(np.array([1, 0], dtype="int16"), np.array([1, 0], dtype="int64")))),
            # ("dict_short_date",dict(zip(np.array([1, 0], dtype="int16"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            # ("dict_short_month",dict(zip(np.array([1, 0], dtype="int16"), np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]")))),
            # ("dict_short_datetime",dict(zip(np.array([1, 0], dtype="int16"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            # ("dict_short_timestamp",dict(zip(np.array([1, 0], dtype="int16"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            # ("dict_short_nanotimestamp",dict(zip(np.array([1, 0], dtype="int16"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]")))),
            # ("dict_short_float",dict(zip(np.array([1, 0], dtype="int16"), np.array([1.11, 0.0], dtype="float32")))),
            # ("dict_short_double",dict(zip(np.array([1, 0], dtype="int16"), np.array([1.11, 0.0], dtype="float64")))),
            # ("dict_short_string",dict(zip(np.array([1, 0], dtype="int16"), np.array(["abc123测试", "", "123abc测试"], dtype="str")))),
            # ("dict_short_datehour",dict(zip(np.array([1, 0], dtype="int16"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
        ],
        "download": [
            ("dict_short_bool", dict(zip(np.array([1, 0], dtype="int16"), np.array([True, False], dtype="bool")))),
            ("dict_short_char", dict(zip(np.array([1, 0], dtype="int16"), np.array([19, 65], dtype="int8")))),
            ("dict_short_short", dict(zip(np.array([1, 0], dtype="int16"), np.array([1, 0], dtype="int16")))),
            ("dict_short_int", dict(zip(np.array([1, 0], dtype="int16"), np.array([1, 0], dtype="int32")))),
            ("dict_short_long", dict(zip(np.array([1, 0], dtype="int16"), np.array([1, 0], dtype="int64")))),
            ("dict_short_date", dict(zip(np.array([1, 0], dtype="int16"),
                                         np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            ("dict_short_month", dict(zip(np.array([1, 0], dtype="int16"),
                                          np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"],
                                                   dtype="datetime64[M]")))),
            ("dict_short_time", dict(zip(np.array([1, 0], dtype="int16"),
                                         np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_short_minute", dict(zip(np.array([1, 0], dtype="int16"),
                                           np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[m]")))),
            ("dict_short_second", dict(zip(np.array([1, 0], dtype="int16"),
                                           np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_short_datetime", dict(zip(np.array([1, 0], dtype="int16"),
                                             np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_short_timestamp", dict(zip(np.array([1, 0], dtype="int16"),
                                              np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_short_nanotime", dict(zip(np.array([1, 0], dtype="int16"),
                                             np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]")))),
            ("dict_short_nanotimestamp", dict(zip(np.array([1, 0], dtype="int16"),
                                                  np.array(["2022-01-01 12:30:56.123456789", 0],
                                                           dtype="datetime64[ns]")))),
            ("dict_short_float", dict(zip(np.array([1, 0], dtype="int16"), np.array([1.11, 0.0], dtype="float32")))),
            ("dict_short_double", dict(zip(np.array([1, 0], dtype="int16"), np.array([1.11, 0.0], dtype="float64")))),
            ("dict_short_string",
             dict(zip(np.array([1, 0], dtype="int16"), np.array(["abc123测试", None], dtype="object")))),
            ("dict_short_datehour", dict(zip(np.array([1, 0], dtype="int16"),
                                             np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
            ("dict_short_blob", dict(zip(np.array([1, 0], dtype="int16"), np.array([b"hello", None], dtype="object")))),
        ],
    }, DATATYPE.DT_INT: {
        "scripts": """
            dict_int_bool = dict(v_int, v_bool)
            dict_int_char = dict(v_int, v_char)
            dict_int_short = dict(v_int, v_short)
            dict_int_int = dict(v_int, v_int)
            dict_int_long = dict(v_int, v_long)
            dict_int_date = dict(v_int, v_date)
            dict_int_month = dict(v_int, v_month)
            dict_int_time = dict(v_int, v_time)
            dict_int_minute = dict(v_int, v_minute)
            dict_int_second = dict(v_int, v_second)
            dict_int_datetime = dict(v_int, v_datetime)
            dict_int_timestamp = dict(v_int, v_timestamp)
            dict_int_nanotime = dict(v_int, v_nanotime)
            dict_int_nanotimestamp = dict(v_int, v_nanotimestamp)
            dict_int_float = dict(v_int, v_float)
            dict_int_double = dict(v_int, v_double)
            dict_int_string = dict(v_int, v_string)
            dict_int_datehour = dict(v_int, v_datehour)
            dict_int_blob = dict(v_int, v_blob)
        """,
        "upload": [
            # ("dict_int_bool",dict(zip(np.array([1, 0], dtype="int32"), np.array([True, False], dtype="bool")))),
            # ("dict_int_char",dict(zip(np.array([1, 0], dtype="int32"), np.array([19, 65], dtype="int8")))),
            # ("dict_int_short",dict(zip(np.array([1, 0], dtype="int32"), np.array([1, 0], dtype="int16")))),
            # ("dict_int_int",dict(zip(np.array([1, 0], dtype="int32"), np.array([1, 0], dtype="int32")))),
            # ("dict_int_long",dict(zip(np.array([1, 0], dtype="int32"), np.array([1, 0], dtype="int64")))),
            # ("dict_int_date",dict(zip(np.array([1, 0], dtype="int32"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            # ("dict_int_month",dict(zip(np.array([1, 0], dtype="int32"), np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]")))),
            # ("dict_int_datetime",dict(zip(np.array([1, 0], dtype="int32"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            # ("dict_int_timestamp",dict(zip(np.array([1, 0], dtype="int32"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            # ("dict_int_nanotimestamp",dict(zip(np.array([1, 0], dtype="int32"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]")))),
            # ("dict_int_float",dict(zip(np.array([1, 0], dtype="int32"), np.array([1.11, 0.0], dtype="float32")))),
            # ("dict_int_double",dict(zip(np.array([1, 0], dtype="int32"), np.array([1.11, 0.0], dtype="float64")))),
            # ("dict_int_string",dict(zip(np.array([1, 0], dtype="int32"), np.array(["abc123测试", "", "123abc测试"], dtype="str")))),
            # ("dict_int_datehour",dict(zip(np.array([1, 0], dtype="int32"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
        ],
        "download": [
            ("dict_int_bool", dict(zip(np.array([1, 0], dtype="int32"), np.array([True, False], dtype="bool")))),
            ("dict_int_char", dict(zip(np.array([1, 0], dtype="int32"), np.array([19, 65], dtype="int8")))),
            ("dict_int_short", dict(zip(np.array([1, 0], dtype="int32"), np.array([1, 0], dtype="int16")))),
            ("dict_int_int", dict(zip(np.array([1, 0], dtype="int32"), np.array([1, 0], dtype="int32")))),
            ("dict_int_long", dict(zip(np.array([1, 0], dtype="int32"), np.array([1, 0], dtype="int64")))),
            ("dict_int_date", dict(zip(np.array([1, 0], dtype="int32"),
                                       np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            ("dict_int_month", dict(zip(np.array([1, 0], dtype="int32"),
                                        np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"],
                                                 dtype="datetime64[M]")))),
            ("dict_int_time", dict(zip(np.array([1, 0], dtype="int32"),
                                       np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_int_minute", dict(zip(np.array([1, 0], dtype="int32"),
                                         np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[m]")))),
            ("dict_int_second", dict(zip(np.array([1, 0], dtype="int32"),
                                         np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_int_datetime", dict(zip(np.array([1, 0], dtype="int32"),
                                           np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_int_timestamp", dict(zip(np.array([1, 0], dtype="int32"),
                                            np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_int_nanotime", dict(zip(np.array([1, 0], dtype="int32"),
                                           np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]")))),
            ("dict_int_nanotimestamp", dict(zip(np.array([1, 0], dtype="int32"),
                                                np.array(["2022-01-01 12:30:56.123456789", 0],
                                                         dtype="datetime64[ns]")))),
            ("dict_int_float", dict(zip(np.array([1, 0], dtype="int32"), np.array([1.11, 0.0], dtype="float32")))),
            ("dict_int_double", dict(zip(np.array([1, 0], dtype="int32"), np.array([1.11, 0.0], dtype="float64")))),
            ("dict_int_string",
             dict(zip(np.array([1, 0], dtype="int32"), np.array(["abc123测试", None], dtype="object")))),
            ("dict_int_datehour", dict(zip(np.array([1, 0], dtype="int32"),
                                           np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
            ("dict_int_blob", dict(zip(np.array([1, 0], dtype="int32"), np.array([b"hello", None], dtype="object")))),
        ],
    }, DATATYPE.DT_LONG: {
        "scripts": """
            dict_long_bool = dict(v_long, v_bool)
            dict_long_char = dict(v_long, v_char)
            dict_long_short = dict(v_long, v_short)
            dict_long_long = dict(v_long, v_long)
            dict_long_int = dict(v_long, v_int)
            dict_long_date = dict(v_long, v_date)
            dict_long_month = dict(v_long, v_month)
            dict_long_time = dict(v_long, v_time)
            dict_long_minute = dict(v_long, v_minute)
            dict_long_second = dict(v_long, v_second)
            dict_long_datetime = dict(v_long, v_datetime)
            dict_long_timestamp = dict(v_long, v_timestamp)
            dict_long_nanotime = dict(v_long, v_nanotime)
            dict_long_nanotimestamp = dict(v_long, v_nanotimestamp)
            dict_long_float = dict(v_long, v_float)
            dict_long_double = dict(v_long, v_double)
            dict_long_string = dict(v_long, v_string)
            dict_long_datehour = dict(v_long, v_datehour)
            dict_long_blob = dict(v_long, v_blob)
        """,
        "upload": [
            # ("dict_long_bool",dict(zip(np.array([1, 0], dtype="int64"), np.array([True, False], dtype="bool")))),
            # ("dict_long_char",dict(zip(np.array([1, 0], dtype="int64"), np.array([19, 65], dtype="int8")))),
            # ("dict_long_short",dict(zip(np.array([1, 0], dtype="int64"), np.array([1, 0], dtype="int16")))),
            # ("dict_long_int",dict(zip(np.array([1, 0], dtype="int64"), np.array([1, 0], dtype="int32")))),
            # ("dict_long_long",dict(zip(np.array([1, 0], dtype="int64"), np.array([1, 0], dtype="int64")))),
            # ("dict_long_date",dict(zip(np.array([1, 0], dtype="int64"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            # ("dict_long_month",dict(zip(np.array([1, 0], dtype="int64"), np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]")))),
            # ("dict_long_datetime",dict(zip(np.array([1, 0], dtype="int64"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            # ("dict_long_timestamp",dict(zip(np.array([1, 0], dtype="int64"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            # ("dict_long_nanotimestamp",dict(zip(np.array([1, 0], dtype="int64"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]")))),
            # ("dict_long_float",dict(zip(np.array([1, 0], dtype="int64"), np.array([1.11, 0.0], dtype="float32")))),
            # ("dict_long_double",dict(zip(np.array([1, 0], dtype="int64"), np.array([1.11, 0.0], dtype="float64")))),
            # ("dict_long_string",dict(zip(np.array([1, 0], dtype="int64"), np.array(["abc123测试", "", "123abc测试"], dtype="str")))),
            # ("dict_long_datehour",dict(zip(np.array([1, 0], dtype="int64"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
        ],
        "download": [
            ("dict_long_bool", dict(zip(np.array([1, 0], dtype="int64"), np.array([True, False], dtype="bool")))),
            ("dict_long_char", dict(zip(np.array([1, 0], dtype="int64"), np.array([19, 65], dtype="int8")))),
            ("dict_long_short", dict(zip(np.array([1, 0], dtype="int64"), np.array([1, 0], dtype="int16")))),
            ("dict_long_int", dict(zip(np.array([1, 0], dtype="int64"), np.array([1, 0], dtype="int32")))),
            ("dict_long_long", dict(zip(np.array([1, 0], dtype="int64"), np.array([1, 0], dtype="int64")))),
            ("dict_long_date", dict(zip(np.array([1, 0], dtype="int64"),
                                        np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            ("dict_long_month", dict(zip(np.array([1, 0], dtype="int64"),
                                         np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"],
                                                  dtype="datetime64[M]")))),
            ("dict_long_time", dict(zip(np.array([1, 0], dtype="int64"),
                                        np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_long_minute", dict(zip(np.array([1, 0], dtype="int64"),
                                          np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[m]")))),
            ("dict_long_second", dict(zip(np.array([1, 0], dtype="int64"),
                                          np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_long_datetime", dict(zip(np.array([1, 0], dtype="int64"),
                                            np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_long_timestamp", dict(zip(np.array([1, 0], dtype="int64"),
                                             np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_long_nanotime", dict(zip(np.array([1, 0], dtype="int64"),
                                            np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]")))),
            ("dict_long_nanotimestamp", dict(zip(np.array([1, 0], dtype="int64"),
                                                 np.array(["2022-01-01 12:30:56.123456789", 0],
                                                          dtype="datetime64[ns]")))),
            ("dict_long_float", dict(zip(np.array([1, 0], dtype="int64"), np.array([1.11, 0.0], dtype="float32")))),
            ("dict_long_double", dict(zip(np.array([1, 0], dtype="int64"), np.array([1.11, 0.0], dtype="float64")))),
            ("dict_long_string",
             dict(zip(np.array([1, 0], dtype="int64"), np.array(["abc123测试", None], dtype="object")))),
            ("dict_long_datehour", dict(zip(np.array([1, 0], dtype="int64"),
                                            np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
            ("dict_long_blob", dict(zip(np.array([1, 0], dtype="int64"), np.array([b"hello", None], dtype="object")))),
        ],
    }, DATATYPE.DT_DATE: {
        # TODO date don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_MONTH: {
        # TODO month don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_TIME: {
        # TODO time don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_MINUTE: {
        # TODO minute don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_SECOND: {
        # TODO second don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_DATETIME: {
        # TODO datetime don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_TIMESTAMP: {
        # TODO timestamp don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_NANOTIME: {
        # TODO nanotime don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_NANOTIMESTAMP: {
        # TODO nanotimestamp don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_FLOAT: {
        # TODO float don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_DOUBLE: {
        # TODO double don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_SYMBOL: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_STRING: {
        "scripts": """
            dict_string_bool = dict(v_string, v_bool)
            dict_string_char = dict(v_string, v_char)
            dict_string_short = dict(v_string, v_short)
            dict_string_long = dict(v_string, v_long)
            dict_string_int = dict(v_string, v_int)
            dict_string_date = dict(v_string, v_date)
            dict_string_month = dict(v_string, v_month)
            dict_string_time = dict(v_string, v_time)
            dict_string_minute = dict(v_string, v_minute)
            dict_string_second = dict(v_string, v_second)
            dict_string_datetime = dict(v_string, v_datetime)
            dict_string_timestamp = dict(v_string, v_timestamp)
            dict_string_nanotime = dict(v_string, v_nanotime)
            dict_string_nanotimestamp = dict(v_string, v_nanotimestamp)
            dict_string_float = dict(v_string, v_float)
            dict_string_double = dict(v_string, v_double)
            dict_string_string = dict(v_string, v_string)
            dict_string_datehour = dict(v_string, v_datehour)
            dict_string_blob = dict(v_string, v_blob)
        """,
        "upload": [
            # ("dict_string_bool",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([True, False], dtype="bool")))),
            # ("dict_string_char",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([19, 65], dtype="int8")))),
            # ("dict_string_short",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1, 0], dtype="int16")))),
            # ("dict_string_int",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1, 0], dtype="int32")))),
            # ("dict_string_long",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1, 0], dtype="int64")))),
            # ("dict_string_date",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            # ("dict_string_month",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"], dtype="datetime64[M]")))),
            # ("dict_string_datetime",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            # ("dict_string_timestamp",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            # ("dict_string_nanotimestamp",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]")))),
            # ("dict_string_float",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1.11, 0.0], dtype="float32")))),
            # ("dict_string_double",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1.11, 0.0], dtype="float64")))),
            # ("dict_string_string",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array(["abc123测试", "", "123abc测试"], dtype="str")))),
            # ("dict_string_datehour",dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
        ],
        "download": [
            ("dict_string_bool",
             dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([True, False], dtype="bool")))),
            (
                "dict_string_char",
                dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([19, 65], dtype="int8")))),
            (
                "dict_string_short",
                dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1, 0], dtype="int16")))),
            ("dict_string_int", dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1, 0], dtype="int32")))),
            ("dict_string_long", dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1, 0], dtype="int64")))),
            ("dict_string_date", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                          np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[D]")))),
            ("dict_string_month", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                           np.array(["2022-01-01 12:30:56.123456789", "1970-01-01 12:30:56.123456789"],
                                                    dtype="datetime64[M]")))),
            ("dict_string_time", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                          np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ms]")))),
            ("dict_string_minute", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                            np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[m]")))),
            ("dict_string_second", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                            np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_string_datetime", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                              np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[s]")))),
            ("dict_string_timestamp", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                               np.array(["2022-01-01 12:30:56.123456789", 0],
                                                        dtype="datetime64[ms]")))),
            ("dict_string_nanotime", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                              np.array(["1970-01-01 12:30:56.123456789", 0], dtype="datetime64[ns]")))),
            ("dict_string_nanotimestamp", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                            dtype="datetime64[ns]")))),
            ("dict_string_float",
             dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1.11, 0.0], dtype="float32")))),
            ("dict_string_double",
             dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([1.11, 0.0], dtype="float64")))),
            ("dict_string_string",
             dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array(["abc123测试", None], dtype="object")))),
            ("dict_string_datehour", dict(zip(np.array(["abc123测试", ""], dtype="str"),
                                              np.array(["2022-01-01 12:30:56.123456789", 0], dtype="datetime64[h]")))),
            ("dict_string_blob",
             dict(zip(np.array(["abc123测试", ""], dtype="str"), np.array([b"hello", None], dtype="object")))),
        ],
    }, DATATYPE.DT_UUID: {
        # TODO Dictionary dont supporte uuid
        "scripts": """
        """,
        "upload": [
            # No Way to upload DT_UUID Dictionary
        ],
        "download": [
        ],
    }, DATATYPE.DT_DATEHOUR: {
        # TODO datehour don't supported as keytype temporarily
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_IPADDR: {
        # TODO Dictionary dont supporte ipaddr
        "scripts": """
        """,
        "upload": [
            # No Way to upload DT_UUID Dictionary
        ],
        "download": [
        ],
    }, DATATYPE.DT_INT128: {
        # TODO Dictionary dont supporte int128
        "scripts": """
        """,
        "upload": [
            # No Way to upload DT_UUID Dictionary
        ],
        "download": [
        ],
    }, DATATYPE.DT_BLOB: {
        # TODO Dictionary dont supporte blob
        "scripts": """
        """,
        "upload": [
            # No Way to upload DT_UUID Dictionary
        ],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL32: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL64: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }}
    # NO SYMBOL Dictionary
    # TODO: python decimal32
    # TODO: python decimal64

    for x in DATATYPE:
        prefix += str(pythons[x]["scripts"])

    def get_script(*args, **kwargs):
        return prefix

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_Matrix(*args, types=None, names=None, r=100, c=100, **kwargs):
    pythons = {}
    matrix_row = "m_r = {}\n".format(r)
    matrix_col = "m_c = {}\n".format(c)

    pythons[DATATYPE.DT_VOID] = {
        "scripts": ''''''
        # No way to create a matrix
        # No way to create a matrix
        ,
        "upload": [],
        # No way to create a matrix
        "download": [],
    }

    pythons[DATATYPE.DT_BOOL] = {
        "scripts": '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([true,false],m_c))
            }
            m_bool_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([true, false, bool()],m_c))
            }
            m_bool_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([true, bool(), false],m_c))
            }
            m_bool_2 = matrix(x2)
        
            xn = []
            for(i in 1..m_r){
                xn.append!(take([bool(),true, false],m_c))
            }
            m_bool_n = matrix(xn)

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([true, false],m_c))
            }
            m_bool_3 = matrix(x3)
            m_bool_3.rename!(1..m_c, 1..m_r)
        '''
        ,
        "upload": [
            ("m_bool_0", matrix(np.array([([True, False] * ceil(c / 2))[:c]] * r), dtype="object").T),
            ("m_bool_1", matrix(np.array([([True, False, None] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_bool_2", matrix(np.array([([True, None, False] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_bool_n", matrix(np.array([([None, True, False] * ceil(c / 3))[:c]] * r), dtype="object").T),
            # ("m_bool_1",matrix(np.array([([True, False, np.nan] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_bool_2",matrix(np.array([([True, np.nan, False] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_bool_n",matrix(np.array([([np.nan, True, False] * ceil(c/3))[:c]] *r),dtype="object").T),
            ("m_bool_1", matrix(np.array([([True, False, pd.NaT] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_bool_2", matrix(np.array([([True, pd.NaT, False] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_bool_n", matrix(np.array([([pd.NaT, True, False] * ceil(c / 3))[:c]] * r), dtype="object").T),

            ("m_bool_0", matrix(np.array([([True, False] * ceil(c / 2))[:c]] * r), dtype="bool").T),
            # ("m_bool_1",matrix(np.array([([True, False, None] * ceil(c/3))[:c]] *r),dtype="bool").T),
            # ("m_bool_2",matrix(np.array([([True, None, False] * ceil(c/3))[:c]] *r),dtype="bool").T),
            # ("m_bool_n",matrix(np.array([([None, True, False] * ceil(c/3))[:c]] *r),dtype="bool").T),
            # ("m_bool_1",matrix(np.array([([True, False, np.nan] * ceil(c/3))[:c]] *r),dtype="bool").T),
            # ("m_bool_2",matrix(np.array([([True, np.nan, False] * ceil(c/3))[:c]] *r),dtype="bool").T),
            # ("m_bool_n",matrix(np.array([([np.nan, True, False] * ceil(c/3))[:c]] *r),dtype="bool").T),
            # ("m_bool_1",matrix(np.array([([True, False, pd.NaT] * ceil(c/3))[:c]] *r),dtype="bool").T),
            # ("m_bool_2",matrix(np.array([([True, pd.Nat, False] * ceil(c/3))[:c]] *r),dtype="bool").T),
            # ("m_bool_n",matrix(np.array([([pd.NaT, True, False] * ceil(c/3))[:c]] *r),dtype="bool").T),

            # ("m_bool_1",matrix(np.array([([True, False, pd.NaT, np.nan, None] * ceil(c/5))[:c]] *r),dtype="bool").T),
            # ("m_bool_2",matrix(np.array([([True, pd.NaT, np.nan, None, False] * ceil(c/5))[:c]] *r),dtype="bool").T),
            # ("m_bool_n",matrix(np.array([([pd.NaT, np.nan, None, True, False] * ceil(c/5))[:c]] *r),dtype="bool").T),
            # ("m_bool_0",list(map(list, zip(*[([True, False] * ceil(c/2))[:c]] *r)))),
            # ("m_bool_1",list(map(list, zip(*[([True, False, None] * ceil(c/3))[:c]] *r)))),
            # ("m_bool_2",list(map(list, zip(*[([True, None, False] * ceil(c/3))[:c]] *r)))),
            # ("m_bool_n",list(map(list, zip(*[([None, True, False] * ceil(c/3))[:c]] *r)))),
            # ("m_bool_1",list(map(list, zip(*[([True, False, np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_bool_2",list(map(list, zip(*[([True, np.nan, False] * ceil(c/3))[:c]] *r)))),
            # ("m_bool_n",list(map(list, zip(*[([np.nan, True, False] * ceil(c/3))[:c]] *r)))),
            # ("m_bool_1",list(map(list, zip(*[([True, False, pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_bool_2",list(map(list, zip(*[([True, pd.NaT, False] * ceil(c/3))[:c]] *r)))),
            # ("m_bool_n",list(map(list, zip(*[([pd.NaT, True, False] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_bool_0",
             list([np.transpose(np.array([([True, False] * ceil(c / 2))[:c]] * r, dtype='object')), None, None])),
            ("m_bool_1",
             list([np.transpose(np.array([([True, False, None] * ceil(c / 3))[:c]] * r, dtype='object')), None, None])),
            ("m_bool_2",
             list([np.transpose(np.array([([True, None, False] * ceil(c / 3))[:c]] * r, dtype='object')), None, None])),
            ("m_bool_n",
             list([np.transpose(np.array([([None, True, False] * ceil(c / 3))[:c]] * r, dtype='object')), None, None])),
            ("m_bool_3", list([np.transpose(np.array([([True, False] * ceil(c / 2))[:c]] * r, dtype='object')),
                               np.array([_i for _i in range(1, r + 1)], dtype='int32'),
                               np.array([_i for _i in range(1, c + 1)], dtype='int32')])),
        ],
    }

    pythons[DATATYPE.DT_CHAR] = {
        "scripts": '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([1c, 2c, 'i', '{'],m_c))
            }
            m_char_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([1c, 2c, 'i', '{', char()],m_c))
            }
            m_char_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([1c, 2c, char(), 'i', '{'],m_c))
            }
            m_char_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([char(),1c, 2c, 'i', '{'],m_c))
            }
            m_char_n = matrix(xn)

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([1c, 2c, 'i', '{'],m_c))
            }
            m_char_3 = matrix(x3)
            m_char_3.rename!(1..m_c, 1..m_r)
        '''
        ,
        "upload": [
            ("m_char_0", matrix(np.array([([1, 2, 105, 123] * ceil(c / 4))[:c]] * r), dtype="int8").T),
            # ("m_char_1",matrix(np.array([([1, 2, 105, 123, None] * ceil(c/5))[:c]] *r),dtype="object").T),
            # ("m_char_2",matrix(np.array([([1, 2, None, 105, 123] * ceil(c/5))[:c]] *r),dtype="object").T),
            # ("m_char_n",matrix(np.array([([None, 1, 2, 105, 123] * ceil(c/5))[:c]] *r),dtype="object").T),
            # ("m_char_1",matrix(np.array([([1, 2, 105, 123, np.nan] * ceil(c/5))[:c]] *r),dtype="object").T),
            # ("m_char_2",matrix(np.array([([1, 2, np.nan, 105, 123] * ceil(c/5))[:c]] *r),dtype="object").T),
            # ("m_char_n",matrix(np.array([([np.nan, 1, 2, 105, 123] * ceil(c/5))[:c]] *r),dtype="object").T),
            # ("m_char_1",matrix(np.array([([1, 2, 105, 123, pd.NaT] * ceil(c/5))[:c]] *r),dtype="object").T),
            # ("m_char_2",matrix(np.array([([1, 2, pd.NaT, 105, 123] * ceil(c/5))[:c]] *r),dtype="object").T),
            # ("m_char_n",matrix(np.array([([pd.NaT, 1, 2, 105, 123] * ceil(c/5))[:c]] *r),dtype="object").T),
            ("m_char_0", matrix(np.array([(['1', '2', '105', '123'] * ceil(c / 4))[:c]] * r), dtype="int8").T),

            # ("m_char_0",list(map(list, zip(*[(['1', '2', '105', '123'] * ceil(c/4))[:c]] *r)))),
            # ("m_char_1",list(map(list, zip(*[(['1', '2', '105', '123', None] * ceil(c/5))[:c]] *r)))),
            # ("m_char_2",list(map(list, zip(*[(['1', '2', None, '105', '123'] * ceil(c/5))[:c]] *r)))),
            # ("m_char_n",list(map(list, zip(*[([None, '1', '2', '105', '123'] * ceil(c/5))[:c]] *r)))),
            # ("m_char_1",list(map(list, zip(*[(['1', '2', '105', '123', np.nan] * ceil(c/5))[:c]] *r)))),
            # ("m_char_2",list(map(list, zip(*[(['1', '2', np.nan, '105', '123'] * ceil(c/5))[:c]] *r)))),
            # ("m_char_n",list(map(list, zip(*[([np.nan, '1', '2', '105', '123'] * ceil(c/5))[:c]] *r)))),
            # ("m_char_1",list(map(list, zip(*[(['1', '2', '105', '123', pd.NaT] * ceil(c/5))[:c]] *r)))),
            # ("m_char_2",list(map(list, zip(*[(['1', '2', pd.NaT, '105', '123'] * ceil(c/5))[:c]] *r)))),
            # ("m_char_n",list(map(list, zip(*[([pd.NaT, '1', '2', '105', '123'] * ceil(c/5))[:c]] *r))))
        ],
        "download": [
            ("m_char_0", list([np.transpose(np.array([([1, 2, 105, 123] * ceil(c / 4))[:c]] * r)), None, None])),
            (
                "m_char_1",
                list([np.transpose(np.array([([1, 2, 105, 123, np.nan] * ceil(c / 5))[:c]] * r)), None, None])),
            (
                "m_char_2",
                list([np.transpose(np.array([([1, 2, np.nan, 105, 123] * ceil(c / 5))[:c]] * r)), None, None])),
            (
                "m_char_n",
                list([np.transpose(np.array([([np.nan, 1, 2, 105, 123] * ceil(c / 5))[:c]] * r)), None, None])),
            ("m_char_3", list([np.transpose(np.array([([1, 2, 105, 123] * ceil(c / 4))[:c]] * r)),
                               np.array([_i for _i in range(1, r + 1)], dtype='int32'),
                               np.array([_i for _i in range(1, c + 1)], dtype='int32')])),
        ]
    }

    pythons[DATATYPE.DT_SHORT] = {
        "scripts": ''' 
            x0 = []
            for(i in 1..m_r){
                x0.append!(short(take([1, 2],m_c)))
            }
            m_short_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([1, 2, short()],m_c))
            }
            m_short_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([1, short(), 2],m_c))
            }
            m_short_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([short(), 1, 2],m_c))
            }
            m_short_n = matrix(xn)

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([1, 2],m_c))
            }
            m_short_3 = matrix(x3)
            m_short_3.rename!(1..m_c, 1..m_r)
        '''
        ,
        "upload": [
            # ("m_short_0",matrix(np.array([([1, 2] * ceil(c/2))[:c]] *r),dtype="object").T),
            # ("m_short_1",matrix(np.array([([1, 2, None] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_short_2",matrix(np.array([([1, None, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_short_n",matrix(np.array([([None, 1, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_short_1",matrix(np.array([([1, 2, np.nan] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_short_2",matrix(np.array([([1, np.nan, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_short_n",matrix(np.array([([np.nan, 1, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_short_1",matrix(np.array([([1, 2, pd.NaT] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_short_2",matrix(np.array([([1, pd.NaT, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_short_n",matrix(np.array([([pd.NaT, 1, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            ("m_short_0", matrix(np.array([([1, 2] * ceil(c / 2))[:c]] * r), dtype="int16").T),
            # ("m_short_0",list(map(list, zip(*[([1, 2] * ceil(c/2))[:c]] *r)))),
            # ("m_short_1",list(map(list, zip(*[([1, 2, None] * ceil(c/3))[:c]] *r)))),
            # ("m_short_2",list(map(list, zip(*[([1, None, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_short_n",list(map(list, zip(*[([None, 1, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_short_1",list(map(list, zip(*[([1, 2, np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_short_2",list(map(list, zip(*[([1, np.nan, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_short_n",list(map(list, zip(*[([np.nan, 1, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_short_1",list(map(list, zip(*[([1, 2, pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_short_2",list(map(list, zip(*[([1, pd.NaT, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_short_n",list(map(list, zip(*[([pd.NaT, 1, 2] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_short_0", list([np.transpose(np.array([([1, 2] * ceil(c / 2))[:c]] * r)), None, None])),
            ("m_short_1", list([np.transpose(np.array([([1, 2, np.nan] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_short_2", list([np.transpose(np.array([([1, np.nan, 2] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_short_n", list([np.transpose(np.array([([np.nan, 1, 2] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_short_3", list([np.transpose(np.array([([1, 2] * ceil(c / 2))[:c]] * r)),
                                np.array([i for i in range(1, r + 1)], dtype='int32'),
                                np.array([i for i in range(1, c + 1)], dtype='int32')])), ]
    }

    pythons[DATATYPE.DT_INT] = {
        "scripts": '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([1, 2],m_c))
            }
            m_int_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([1, 2, int()],m_c))
            }
            m_int_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([1, int(), 2],m_c))
            }
            m_int_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([int(), 1, 2],m_c))
            }
            m_int_n = matrix(xn)        

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([1, 2],m_c))
            }
            m_int_3 = matrix(x3)
            m_int_3.rename!(1..m_c, 1..m_r)
        '''
        ,
        "upload": [
            # ("m_int_0",matrix(np.array([([1, 2] * ceil(c/2))[:c]] *r),dtype="object").T),
            # ("m_int_1",matrix(np.array([([1, 2, None] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_int_2",matrix(np.array([([1, None, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_int_n",matrix(np.array([([None, 1, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_int_1",matrix(np.array([([1, 2, np.nan] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_int_2",matrix(np.array([([1, np.nan, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_int_n",matrix(np.array([([np.nan, 1, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_int_1",matrix(np.array([([1, 2, pd.NaT] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_int_2",matrix(np.array([([1, pd.NaT, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_int_n",matrix(np.array([([pd.NaT, 1, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            ("m_int_0", matrix(np.array([([1, 2] * ceil(c / 2))[:c]] * r), dtype="int32").T),
            # ("m_int_0",list(map(list, zip(*[([1, 2] * ceil(c/2))[:c]] *r)))),
            # ("m_int_1",list(map(list, zip(*[([1, 2, None] * ceil(c/3))[:c]] *r)))),
            # ("m_int_2",list(map(list, zip(*[([1, None, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_int_n",list(map(list, zip(*[([None, 1, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_int_1",list(map(list, zip(*[([1, 2, np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_int_2",list(map(list, zip(*[([1, np.nan, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_int_n",list(map(list, zip(*[([np.nan, 1, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_int_1",list(map(list, zip(*[([1, 2, pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_int_2",list(map(list, zip(*[([1, pd.NaT, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_int_n",list(map(list, zip(*[([pd.NaT, 1, 2] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_int_0", list([np.transpose(np.array([([1, 2] * ceil(c / 2))[:c]] * r)), None, None])),
            ("m_int_1", list([np.transpose(np.array([([1, 2, np.nan] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_int_2", list([np.transpose(np.array([([1, np.nan, 2] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_int_n", list([np.transpose(np.array([([np.nan, 1, 2] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_int_3", list([np.transpose(np.array([([1, 2] * ceil(c / 2))[:c]] * r)),
                              np.array([i for i in range(1, r + 1)], dtype='int32'),
                              np.array([i for i in range(1, c + 1)], dtype='int32')]))
        ]
    }

    pythons[DATATYPE.DT_LONG] = {
        "scripts": '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(long(take([1, 2],m_c)))
            }
            m_long_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([1, 2, long()],m_c))
            }
            m_long_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([1, long(), 2],m_c))
            }
            m_long_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([long(), 1, 2],m_c))
            }
            m_long_n = matrix(xn)   

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([1, 2],m_c))
            }
            m_long_3 = matrix(x3)
            m_long_3.rename!(1..m_c, 1..m_r)
        '''
        ,
        "upload": [
            ("m_long_0", matrix(np.array([([1, 2] * ceil(c / 2))[:c]] * r), dtype="object").T),
            # ("m_long_1",matrix(np.array([([1, 2, None] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_long_2",matrix(np.array([([1, None, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_long_n",matrix(np.array([([None, 1, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_long_1",matrix(np.array([([1, 2, np.nan] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_long_2",matrix(np.array([([1, np.nan, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_long_n",matrix(np.array([([np.nan, 1, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_long_1",matrix(np.array([([1, 2, pd.NaT] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_long_2",matrix(np.array([([1, pd.NaT, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_long_n",matrix(np.array([([pd.NaT, 1, 2] * ceil(c/3))[:c]] *r),dtype="object").T),
            ("m_long_0", matrix(np.array([([1, 2] * ceil(c / 2))[:c]] * r), dtype="int64").T),
            # ("m_long_0",list(map(list, zip(*[([1, 2] * ceil(c/2))[:c]] *r)))),
            # ("m_long_1",list(map(list, zip(*[([1, 2, None] * ceil(c/3))[:c]] *r)))),
            # ("m_long_2",list(map(list, zip(*[([1, None, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_long_n",list(map(list, zip(*[([None, 1, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_long_1",list(map(list, zip(*[([1, 2, np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_long_2",list(map(list, zip(*[([1, np.nan, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_long_n",list(map(list, zip(*[([np.nan, 1, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_long_1",list(map(list, zip(*[([1, 2, pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_long_2",list(map(list, zip(*[([1, pd.NaT, 2] * ceil(c/3))[:c]] *r)))),
            # ("m_long_n",list(map(list, zip(*[([pd.NaT, 1, 2] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_long_0", list([np.transpose(np.array([([1, 2] * ceil(c / 2))[:c]] * r)), None, None])),
            ("m_long_1", list([np.transpose(np.array([([1, 2, np.nan] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_long_2", list([np.transpose(np.array([([1, np.nan, 2] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_long_n", list([np.transpose(np.array([([np.nan, 1, 2] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_long_3", list([np.transpose(np.array([([1, 2] * ceil(c / 2))[:c]] * r)),
                               np.array([i for i in range(1, r + 1)], dtype='int32'),
                               np.array([i for i in range(1, c + 1)], dtype='int32')]))
        ]
    }

    pythons[DATATYPE.DT_DATE] = {
        "scripts": '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([2022.01.31, 2001.01.31],m_c))
            }
            m_date_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([2022.01.31, 2001.01.31, date()],m_c))
            }
            m_date_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([2022.01.31, date(), 2001.01.31],m_c))
            }
            m_date_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([date(), 2022.01.31, 2001.01.31],m_c))
            }
            m_date_n = matrix(xn)  

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([2022.01.31, 2001.01.31],m_c))
            }
            m_date_3 = matrix(x3)
            m_date_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            # object error
            # ("m_date_0",matrix(np.array([(['2022-01-31', '2001-01-31'] * ceil(c/2))[:c]] *r),dtype="object").T),
            # ("m_date_1",matrix(np.array([(['2022-01-31', '2001-01-31', None] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_2",matrix(np.array([(['2022-01-31', None, '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_n",matrix(np.array([([None, '2022-01-31', '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_1",matrix(np.array([(['2022-01-31', '2001-01-31', np.nan] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_2",matrix(np.array([(['2022-01-31', np.nan, '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_n",matrix(np.array([([np.nan, '2022-01-31', '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_1",matrix(np.array([(['2022-01-31', '2001-01-31', pd.NaT] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_2",matrix(np.array([(['2022-01-31', pd.NaT, '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_n",matrix(np.array([([pd.NaT, '2022-01-31', '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="object").T),
            ("m_date_0",
             matrix(np.array([(['2022-01-31', '2001-01-31'] * ceil(c / 2))[:c]] * r), dtype="datetime64[D]").T),
            ("m_date_1",
             matrix(np.array([(['2022-01-31', '2001-01-31', None] * ceil(c / 3))[:c]] * r), dtype="datetime64[D]").T),
            ("m_date_2",
             matrix(np.array([(['2022-01-31', None, '2001-01-31'] * ceil(c / 3))[:c]] * r), dtype="datetime64[D]").T),
            ("m_date_n",
             matrix(np.array([([None, '2022-01-31', '2001-01-31'] * ceil(c / 3))[:c]] * r), dtype="datetime64[D]").T),
            # ("m_date_1",matrix(np.array([(['2022-01-31', '2001-01-31', np.nan] * ceil(c/3))[:c]] *r),dtype="datetime64[D]").T),
            # ("m_date_2",matrix(np.array([(['2022-01-31', np.nan, '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="datetime64[D]").T),
            # ("m_date_n",matrix(np.array([([np.nan, '2022-01-31', '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="datetime64[D]").T),
            # ("m_date_1",matrix(np.array([(['2022-01-31', '2001-01-31', pd.NaT] * ceil(c/3))[:c]] *r),dtype="datetime64[D]").T),
            # ("m_date_2",matrix(np.array([(['2022-01-31', pd.NaT, '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="datetime64[D]").T),
            # ("m_date_n",matrix(np.array([([pd.NaT, '2022-01-31', '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="datetime64[D]").T),

            # ("m_date_0",list(map(list, zip(*[(['2022-01-31', '2001-01-31'] * ceil(c/2))[:c]] *r)))),
            # ("m_date_1",list(map(list, zip(*[(['2022-01-31', '2001-01-31', None] * ceil(c/3))[:c]] *r)))),
            # ("m_date_2",list(map(list, zip(*[(['2022-01-31', None, '2001-01-31'] * ceil(c/3))[:c]] *r)))),
            # ("m_date_n",list(map(list, zip(*[([None, '2022-01-31', '2001-01-31'] * ceil(c/3))[:c]] *r)))),
            # ("m_date_1",list(map(list, zip(*[(['2022-01-31', '2001-01-31', np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_date_2",list(map(list, zip(*[(['2022-01-31', np.nan, '2001-01-31'] * ceil(c/3))[:c]] *r)))),
            # ("m_date_n",list(map(list, zip(*[([np.nan, '2022-01-31', '2001-01-31'] * ceil(c/3))[:c]] *r)))),
            # ("m_date_1",list(map(list, zip(*[(['2022-01-31', '2001-01-31', pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_date_2",list(map(list, zip(*[(['2022-01-31', pd.NaT, '2001-01-31'] * ceil(c/3))[:c]] *r)))),
            # ("m_date_n",list(map(list, zip(*[([pd.NaT, '2022-01-31', '2001-01-31'] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            # ("m_date_0",matrix(np.array([(['2022-01-31', '2001-01-31'] * ceil(c/2))[:c]] *r),dtype="object").T),
            # ("m_date_1",matrix(np.array([(['2022-01-31', '2001-01-31', None] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_2",matrix(np.array([(['2022-01-31', None, '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_date_n",matrix(np.array([([None, '2022-01-31', '2001-01-31'] * ceil(c/3))[:c]] *r),dtype="object").T),
            ("m_date_0", list([np.transpose(
                np.array([(['2022-01-31T00:00:00.000000000', '2001-01-31T00:00:00.000000000'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[D]')), None, None])),
            ("m_date_1", list([np.transpose(np.array(
                [(['2022-01-31T00:00:00.000000000', '2001-01-31T00:00:00.000000000', None] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[D]')), None, None])),
            ("m_date_2", list([np.transpose(np.array(
                [(['2022-01-31T00:00:00.000000000', None, '2001-01-31T00:00:00.000000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[D]')), None, None])),
            ("m_date_n", list([np.transpose(np.array(
                [([None, '2022-01-31T00:00:00.000000000', '2001-01-31T00:00:00.000000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[D]')), None, None])),
            ("m_date_3", list([np.transpose(
                np.array([(['2022-01-31T00:00:00.000000000', '2001-01-31T00:00:00.000000000'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[D]')), np.array([i for i in range(1, r + 1)], dtype='int32'),
                np.array([i for i in range(1, c + 1)], dtype='int32')]))
        ]
    }

    pythons[DATATYPE.DT_MONTH] = {
        "scripts": '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([2022.01M, 2001.01M],m_c))
            }
            m_month_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([2022.01M, 2001.01M, month()],m_c))
            }
            m_month_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([2022.01M, month(), 2001.01M],m_c))
            }
            m_month_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([month(), 2022.01M, 2001.01M],m_c))
            }
            m_month_n = matrix(xn)  
            
            x3 = []
            for(i in 1..m_r){
                x3.append!(take([2022.01M, 2001.01M],m_c))
            }
            m_month_3 = matrix(x3)
            m_month_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            # ("m_month_0",matrix(np.array([(['2022-01', '2001-03'] * ceil(c/2))[:c]] *r),dtype="object").T),
            # ("m_month_1",matrix(np.array([(['2022-01', '2001-03', None] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_month_2",matrix(np.array([(['2022-01', None, '2001-03'] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_month_n",matrix(np.array([([None, '2022-01', '2001-03'] * ceil(c/3))[:c]] *r),dtype="object").T),
            ("m_month_0", matrix(np.array([(['2022-01', '2001-01'] * ceil(c / 2))[:c]] * r), dtype="datetime64[M]").T),
            ("m_month_1",
             matrix(np.array([(['2022-01', '2001-01', None] * ceil(c / 3))[:c]] * r), dtype="datetime64[M]").T),
            ("m_month_2",
             matrix(np.array([(['2022-01', None, '2001-01'] * ceil(c / 3))[:c]] * r), dtype="datetime64[M]").T),
            ("m_month_n",
             matrix(np.array([([None, '2022-01', '2001-01'] * ceil(c / 3))[:c]] * r), dtype="datetime64[M]").T),
            # ("m_month_1",matrix(np.array([(['2022-01', '2001-03', np.nan] * ceil(c/3))[:c]] *r),dtype="datetime64[M]").T),
            # ("m_month_2",matrix(np.array([(['2022-01', np.nan, '2001-03'] * ceil(c/3))[:c]] *r),dtype="datetime64[M]").T),
            # ("m_month_n",matrix(np.array([([np.nan, '2022-01', '2001-03'] * ceil(c/3))[:c]] *r),dtype="datetime64[M]").T),
            # ("m_month_1",matrix(np.array([(['2022-01', '2001-03', pd.NaT] * ceil(c/3))[:c]] *r),dtype="datetime64[M]").T),
            # ("m_month_2",matrix(np.array([(['2022-01', pd.NaT, '2001-03'] * ceil(c/3))[:c]] *r),dtype="datetime64[M]").T),
            # ("m_month_n",matrix(np.array([([pd.nat, '2022-01', '2001-03'] * ceil(c/3))[:c]] *r),dtype="datetime64[M]").T),

            # ("m_month_0",list(map(list, zip(*[(['2022-01', '2001-03'] * ceil(c/2))[:c]] *r)))),
            # ("m_month_1",list(map(list, zip(*[(['2022-01', '2001-03', None] * ceil(c/3))[:c]] *r)))),
            # ("m_month_2",list(map(list, zip(*[(['2022-01', None, '2001-03'] * ceil(c/3))[:c]] *r)))),
            # ("m_month_n",list(map(list, zip(*[([None, '2022-01', '2001-03'] * ceil(c/3))[:c]] *r)))),
            # ("m_month_1",list(map(list, zip(*[(['2022-01', '2001-03', np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_month_2",list(map(list, zip(*[(['2022-01', np.nan, '2001-03'] * ceil(c/3))[:c]] *r)))),
            # ("m_month_n",list(map(list, zip(*[([np.nan, '2022-01', '2001-03'] * ceil(c/3))[:c]] *r)))),
            # ("m_month_1",list(map(list, zip(*[(['2022-01', '2001-03', pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_month_2",list(map(list, zip(*[(['2022-01', pd.Nat, '2001-03'] * ceil(c/3))[:c]] *r)))),
            # ("m_month_n",list(map(list, zip(*[([pd.NaT, '2022-01', '2001-03'] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            # ("m_month_0",matrix(np.array([(['2022-01', '2001-03'] * ceil(c/2))[:c]] *r),dtype="object").T),
            # ("m_month_1",matrix(np.array([(['2022-01', '2001-03', None] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_month_2",matrix(np.array([(['2022-01', None, '2001-03'] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_month_n",matrix(np.array([([None, '2022-01', '2001-03'] * ceil(c/3))[:c]] *r),dtype="object").T),
            ("m_month_0", list(
                [np.transpose(np.array([(['2022-01-31', '2001-01-31'] * ceil(c / 2))[:c]] * r, dtype='datetime64[M]')),
                 None, None])),
            ("m_month_1", list([np.transpose(
                np.array([(['2022-01-31', '2001-01-31', None] * ceil(c / 3))[:c]] * r, dtype='datetime64[M]')), None,
                None])),
            ("m_month_2", list([np.transpose(
                np.array([(['2022-01-31', None, '2001-01-31'] * ceil(c / 3))[:c]] * r, dtype='datetime64[M]')), None,
                None])),
            ("m_month_n", list([np.transpose(
                np.array([([None, '2022-01-31', '2001-01-31'] * ceil(c / 3))[:c]] * r, dtype='datetime64[M]')), None,
                None])),
            ("m_month_3", list(
                [np.transpose(np.array([(['2022-01-31', '2001-01-31'] * ceil(c / 2))[:c]] * r, dtype='datetime64[M]')),
                 np.array([i for i in range(1, r + 1)], dtype='int32'),
                 np.array([i for i in range(1, c + 1)], dtype='int32')]))
        ]
    }

    pythons[DATATYPE.DT_TIME] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([13:30:10.008, 17:37:16.058],m_c))
            }
            m_time_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([13:30:10.008, 17:37:16.058, time()],m_c))
            }
            m_time_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([13:30:10.008, time(), 17:37:16.058],m_c))
            }
            m_time_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([time(), 13:30:10.008, 17:37:16.058],m_c))
            }
            m_time_n = matrix(xn)  

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([13:30:10.008, 17:37:16.058],m_c))
            }
            m_time_3 = matrix(x3)
            m_time_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            # No way to upload
        ],
        "download": [
            ("m_time_0", list([np.transpose(
                np.array([(['1970-01-01 13:30:10.008000000', '1970-01-01 17:37:16.058000000'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[ms]')), None, None])),
            ("m_time_1", list([np.transpose(np.array(
                [(['1970-01-01 13:30:10.008000000', '1970-01-01 17:37:16.058000000', None] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ms]')), None, None])),
            ("m_time_2", list([np.transpose(np.array(
                [(['1970-01-01 13:30:10.008000000', None, '1970-01-01 17:37:16.058000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ms]')), None, None])),
            ("m_time_n", list([np.transpose(np.array(
                [([None, '1970-01-01 13:30:10.008000000', '1970-01-01 17:37:16.058000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ms]')), None, None])),
            ("m_time_3", list([np.transpose(
                np.array([(['1970-01-01 13:30:10.008000000', '1970-01-01 17:37:16.058000000'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[ms]')), np.array([i for i in range(1, r + 1)], dtype='int32'),
                np.array([i for i in range(1, c + 1)], dtype='int32')])),
        ],
    }

    pythons[DATATYPE.DT_MINUTE] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([13:30m, 17:37m],m_c))
            }
            m_minute_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([13:30m, 17:37m, minute()],m_c))
            }
            m_minute_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([13:30m, minute(), 17:37m],m_c))
            }
            m_minute_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([minute(), 13:30m, 17:37m],m_c))
            }
            m_minute_n = matrix(xn) 

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([13:30m, 17:37m],m_c))
            }
            m_minute_3 = matrix(x3)
            m_minute_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            # Noway to upload
        ],
        "download": [
            ("m_minute_0", list([np.transpose(
                np.array([(['1970-01-01T13:30:00.000000000', '1970-01-01T17:37:00.000000000'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[m]')), None, None])),
            ("m_minute_1", list([np.transpose(np.array(
                [(['1970-01-01T13:30:00.000000000', '1970-01-01T17:37:00.000000000', None] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[m]')), None, None])),
            ("m_minute_2", list([np.transpose(np.array(
                [(['1970-01-01T13:30:00.000000000', None, '1970-01-01T17:37:00.000000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[m]')), None, None])),
            ("m_minute_n", list([np.transpose(np.array(
                [([None, '1970-01-01T13:30:00.000000000', '1970-01-01T17:37:00.000000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[m]')), None, None])),
            ("m_minute_3", list([np.transpose(
                np.array([(['1970-01-01T13:30:00.000000000', '1970-01-01T17:37:00.000000000'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[m]')), np.array([i for i in range(1, r + 1)], dtype='int32'),
                np.array([i for i in range(1, c + 1)], dtype='int32')]))
        ],
    }

    pythons[DATATYPE.DT_SECOND] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([13:30:10, 17:37:20],m_c))
            }
            m_second_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([13:30:10, 17:37:20, second()],m_c))
            }
            m_second_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([13:30:10, second(), 17:37:20],m_c))
            }
            m_second_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([second(), 13:30:10, 17:37:20],m_c))
            }
            m_second_n = matrix(xn)  

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([13:30:10, 17:37:20],m_c))
            }
            m_second_3 = matrix(x3)
            m_second_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            # No way to upload
        ],
        "download": [
            ("m_second_0", list([np.transpose(
                np.array([(['1970-01-01 13:30:10', '1970-01-01 17:37:20'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[s]')), None, None])),
            ("m_second_1", list([np.transpose(
                np.array([(['1970-01-01 13:30:10', '1970-01-01 17:37:20', None] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[s]')), None, None])),
            ("m_second_2", list([np.transpose(
                np.array([(['1970-01-01 13:30:10', None, '1970-01-01 17:37:20'] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[s]')), None, None])),
            ("m_second_n", list([np.transpose(
                np.array([([None, '1970-01-01 13:30:10', '1970-01-01 17:37:20'] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[s]')), None, None])),
            ("m_second_3", list([np.transpose(
                np.array([(['1970-01-01 13:30:10', '1970-01-01 17:37:20'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[s]')), np.array([i for i in range(1, r + 1)], dtype='int32'),
                np.array([i for i in range(1, c + 1)], dtype='int32')]))
        ],
    }

    pythons[DATATYPE.DT_DATETIME] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([2001.01.31 13:30:10, 2020.01.31T17:37:20],m_c))
            }
            m_datetime_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([2001.01.31 13:30:10, 2020.01.31T17:37:20, datetime()],m_c))
            }
            m_datetime_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([2001.01.31 13:30:10, datetime(), 2020.01.31T17:37:20],m_c))
            }
            m_datetime_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([datetime(), 2001.01.31 13:30:10, 2020.01.31T17:37:20],m_c))
            }
            m_datetime_n = matrix(xn)  

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([2001.01.31 13:30:10, 2020.01.31T17:37:20],m_c))
            }
            m_datetime_3 = matrix(x3)
            m_datetime_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            ("m_datetime_0", matrix(np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r),
                                    dtype="datetime64[s]").T),
            ("m_datetime_1",
             matrix(np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', None] * ceil(c / 3))[:c]] * r),
                    dtype="datetime64[s]").T),
            ("m_datetime_2",
             matrix(np.array([(['2001-01-31 13:30:10', None, '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r),
                    dtype="datetime64[s]").T),
            ("m_datetime_n",
             matrix(np.array([([None, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r),
                    dtype="datetime64[s]").T),
            # ("m_datetime_1",matrix(np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', np.nan] * ceil(c/3))[:c]] *r),dtype="datetime64[s]").T),
            # ("m_datetime_2",matrix(np.array([(['2001-01-31 13:30:10', np.nan, '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r),dtype="datetime64[s]").T),
            # ("m_datetime_n",matrix(np.array([([np.nan, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r),dtype="datetime64[s]").T),
            # ("m_datetime_1",matrix(np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', pd.NaT] * ceil(c/3))[:c]] *r),dtype="datetime64[s]").T),
            # ("m_datetime_2",matrix(np.array([(['2001-01-31 13:30:10.123', pd.NaT, '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r),dtype="datetime64[s]").T),
            # ("m_datetime_n",matrix(np.array([([pd.NaT, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r),dtype="datetime64[s]").T),

            # ("m_datetime_0",list(map(list, zip(*[(['2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c/2))[:c]] *r)))),
            # ("m_datatime_1",list(map(list, zip(*[(['2001-01-31 13:30:10', '2020-01-31 17:37:20', None] * ceil(c/3))[:c]] *r)))),
            # ("m_datetime_2",list(map(list, zip(*[(['2001-01-31 13:30:10', None, '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r)))),
            # ("m_datetime_n",list(map(list, zip(*[([None, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r)))),
            # ("m_datetime_1",list(map(list, zip(*[(['2001-01-31 13:30:10', '2020-01-31 17:37:20', np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_datetime_2",list(map(list, zip(*[(['2001-01-31 13:30:10', np.nan, '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r)))),
            # ("m_datetime_n",list(map(list, zip(*[([np.nan, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r)))),
            # ("m_datetime_1",list(map(list, zip(*[(['2001-01-31 13:30:10', '2020-01-31 17:37:20', pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_datetime_2",list(map(list, zip(*[(['2001-01-31 13:30:10', pd.Nat, '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r)))),
            # ("m_datetime_n",list(map(list, zip(*[([pd.NaT, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_datetime_0", list([np.transpose(
                np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[s]')), None, None])),
            ("m_datetime_1", list([np.transpose(
                np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', None] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[s]')), None, None])),
            ("m_datetime_2", list([np.transpose(
                np.array([(['2001-01-31 13:30:10', None, '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[s]')), None, None])),
            ("m_datetime_n", list([np.transpose(
                np.array([([None, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[s]')), None, None])),
            ("m_datetime_3", list([np.transpose(
                np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[s]')), np.array([i for i in range(1, r + 1)], dtype='int32'),
                np.array([i for i in range(1, c + 1)], dtype='int32')])),
        ]
    }

    pythons[DATATYPE.DT_TIMESTAMP] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([2001.01.31 13:30:10.123, 2020.01.31T17:37:20.123],m_c))
            }
            m_timestamp_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([2001.01.31 13:30:10.123, 2020.01.31T17:37:20.123, timestamp()],m_c))
            }
            m_timestamp_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([2001.01.31 13:30:10.123, timestamp(), 2020.01.31T17:37:20.123],m_c))
            }
            m_timestamp_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([timestamp(), 2001.01.31 13:30:10.123, 2020.01.31T17:37:20.123],m_c))
            }
            m_timestamp_n = matrix(xn)  

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([2001.01.31 13:30:10.123, 2020.01.31T17:37:20.123],m_c))
            }
            m_timestamp_3 = matrix(x3)
            m_timestamp_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            ("m_timestamp_0",
             matrix(np.array([(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c / 2))[:c]] * r),
                    dtype="datetime64[ms]").T),
            ("m_timestamp_1",
             matrix(np.array([(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123', None] * ceil(c / 3))[:c]] * r),
                    dtype="datetime64[ms]").T),
            ("m_timestamp_2",
             matrix(np.array([(['2001-01-31 13:30:10.123', None, '2020-01-31 17:37:20.123'] * ceil(c / 3))[:c]] * r),
                    dtype="datetime64[ms]").T),
            ("m_timestamp_n",
             matrix(np.array([([None, '2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c / 3))[:c]] * r),
                    dtype="datetime64[ms]").T),
            # ("m_timestamp_1",matrix(np.array([(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123', np.nan] * ceil(c/3))[:c]] *r),dtype="datetime64[ms]").T),
            # ("m_timestamp_2",matrix(np.array([(['2001-01-31 13:30:10.123', np.nan, '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r),dtype="datetime64[ms]").T),
            # ("m_timestamp_n",matrix(np.array([([np.nan, '2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r),dtype="datetime64[ms]").T),
            # ("m_timestamp_1",matrix(np.array([(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123', pd.NaT] * ceil(c/3))[:c]] *r),dtype="datetime64[ms]").T),
            # ("m_timestamp_2",matrix(np.array([(['2001-01-31 13:30:10.123', pd.NaT, '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r),dtype="datetime64[ms]").T),
            # ("m_timestamp_n",matrix(np.array([([pd.NaT, '2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r),dtype="datetime64[ms]").T),

            # ("m_timestamp_0",list(map(list, zip(*[(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c/2))[:c]] *r)))),
            # ("m_timestamp_1",list(map(list, zip(*[(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123', None] * ceil(c/3))[:c]] *r)))),
            # ("m_timestamp_2",list(map(list, zip(*[(['2001-01-31 13:30:10.123', None, '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r)))),
            # ("m_timestamp_n",list(map(list, zip(*[([None, '2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r)))),
            # ("m_timestamp_1",list(map(list, zip(*[(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123', np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_timestamp_2",list(map(list, zip(*[(['2001-01-31 13:30:10.123', np.nan, '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r)))),
            # ("m_timestamp_n",list(map(list, zip(*[([np.nan, '2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r)))),
            # ("m_timestamp_1",list(map(list, zip(*[(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123', pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_timestamp_2",list(map(list, zip(*[(['2001-01-31 13:30:10.123', pd.Nat, '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r)))),
            # ("m_timestamp_n",list(map(list, zip(*[([pd.NaT, '2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_timestamp_0", list([np.transpose(
                np.array([(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[ms]')), None, None])),
            ("m_timestamp_1", list([np.transpose(
                np.array([(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123', None] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[ms]')), None, None])),
            ("m_timestamp_2", list([np.transpose(
                np.array([(['2001-01-31 13:30:10.123', None, '2020-01-31 17:37:20.123'] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[ms]')), None, None])),
            ("m_timestamp_n", list([np.transpose(
                np.array([([None, '2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[ms]')), None, None])),
            ("m_timestamp_3", list([np.transpose(
                np.array([(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[ms]')), np.array([i for i in range(1, r + 1)], dtype='int32'),
                np.array([i for i in range(1, c + 1)], dtype='int32')])),
        ],
    }

    pythons[DATATYPE.DT_NANOTIME] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([13:30:10.123456789, 17:37:20.123456789],m_c))
            }
            m_nanotime_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([13:30:10.123456789, 17:37:20.123456789, nanotime()],m_c))
            }
            m_nanotime_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([13:30:10.123456789, nanotime(), 17:37:20.123456789],m_c))
            }
            m_nanotime_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([nanotime(), 13:30:10.123456789, 17:37:20.123456789],m_c))
            }
            m_nanotime_n = matrix(xn)  

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([13:30:10.123456789, 17:37:20.123456789],m_c))
            }
            m_nanotime_3 = matrix(x3)
            m_nanotime_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            # No way to upload
        ],
        "download": [
            ("m_nanotime_0", list([np.transpose(
                np.array([(['1970-01-01 13:30:10.123456789', '1970-01-01 17:37:20.123456789'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[ns]')), None, None])),
            ("m_nanotime_1", list([np.transpose(np.array(
                [(['1970-01-01 13:30:10.123456789', '1970-01-01 17:37:20.123456789', None] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')), None, None])),
            ("m_nanotime_2", list([np.transpose(np.array(
                [(['1970-01-01 13:30:10.123456789', None, '1970-01-01 17:37:20.123456789'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')), None, None])),
            ("m_nanotime_n", list([np.transpose(np.array(
                [([None, '1970-01-01 13:30:10.123456789', '1970-01-01 17:37:20.123456789'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')), None, None])),
            ("m_nanotime_3", list([np.transpose(
                np.array([(['1970-01-01 13:30:10.123456789', '1970-01-01 17:37:20.123456789'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[ns]')), np.array([i for i in range(1, r + 1)], dtype='int32'),
                np.array([i for i in range(1, c + 1)], dtype='int32')])),

        ],
    }

    pythons[DATATYPE.DT_NANOTIMESTAMP] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([2001.01.31 13:30:10.123456789, 2020.01.31T17:37:20.123456789],m_c))
            }
            m_nanotimestamp_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([2001.01.31 13:30:10.123456789, 2020.01.31T17:37:20.123456789, nanotimestamp()],m_c))
            }
            m_nanotimestamp_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([2001.01.31 13:30:10.123456789, nanotimestamp(), 2020.01.31T17:37:20.123456789],m_c))
            }
            m_nanotimestamp_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([nanotimestamp(), 2001.01.31 13:30:10.123456789, 2020.01.31T17:37:20.123456789],m_c))
            }
            m_nanotimestamp_n = matrix(xn)  

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([2001.01.31 13:30:10.123456789, 2020.01.31T17:37:20.123456789],m_c))
            }
            m_nanotimestamp_3 = matrix(x3)
            m_nanotimestamp_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            ("m_nanotimestamp_0", matrix(
                np.array([(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c / 2))[:c]] * r),
                dtype="datetime64[ns]").T),
            ("m_nanotimestamp_1", matrix(np.array(
                [(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789', None] * ceil(c / 3))[:c]] * r),
                dtype="datetime64[ns]").T),
            ("m_nanotimestamp_2", matrix(np.array(
                [(['2001-01-31 13:30:10.123456789', None, '2020-01-31 17:37:20.123456789'] * ceil(c / 3))[:c]] * r),
                dtype="datetime64[ns]").T),
            ("m_nanotimestamp_n", matrix(np.array(
                [([None, '2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c / 3))[:c]] * r),
                dtype="datetime64[ns]").T),
            # ("m_nanotimestamp_1",matrix(np.array([(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789', np.nan] * ceil(c/3))[:c]] *r),dtype="datetime64[ns]").T),
            # ("m_nanotimestamp_2",matrix(np.array([(['2001-01-31 13:30:10.123456789', np.nan, '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r),dtype="datetime64[ns]").T),
            # ("m_nanotimestamp_n",matrix(np.array([([np.nan, '2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r),dtype="datetime64[ns]").T),
            # ("m_nanotimestamp_1",matrix(np.array([(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789', pd.NaT] * ceil(c/3))[:c]] *r),dtype="datetime64[ns]").T),
            # ("m_nanotimestamp_2",matrix(np.array([(['2001-01-31 13:30:10.123456789', pd.NaT, '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r),dtype="datetime64[ns]").T),
            # ("m_nanotimestamp_n",matrix(np.array([([pd.NaT, '2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r),dtype="datetime64[ns]").T),

            # ("m_nanotimestamp_0",list(map(list, zip(*[(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c/2))[:c]] *r)))),
            # ("m_nanotimestamp_1",list(map(list, zip(*[(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789', None] * ceil(c/3))[:c]] *r)))),
            # ("m_nanotimestamp_2",list(map(list, zip(*[(['2001-01-31 13:30:10.123456789', None, '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r)))),
            # ("m_nanotimestamp_n",list(map(list, zip(*[([None, '2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r)))),
            # ("m_nanotimestamp_1",list(map(list, zip(*[(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789', np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_nanotimestamp_2",list(map(list, zip(*[(['2001-01-31 13:30:10.123456789', np.nan, '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r)))),
            # ("m_nanotimestamp_n",list(map(list, zip(*[([np.nan, '2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r)))),
            # ("m_nanotimestamp_1",list(map(list, zip(*[(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789', pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_nanotimestamp_2",list(map(list, zip(*[(['2001-01-31 13:30:10.123456789', pd.Nat, '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r)))),
            # ("m_nanotimestamp_n",list(map(list, zip(*[([pd.NaT, '2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_nanotimestamp_0", list([np.transpose(
                np.array([(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[ns]')), None, None])),
            ("m_nanotimestamp_1", list([np.transpose(np.array(
                [(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789', None] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')), None, None])),
            ("m_nanotimestamp_2", list([np.transpose(np.array(
                [(['2001-01-31 13:30:10.123456789', None, '2020-01-31 17:37:20.123456789'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')), None, None])),
            ("m_nanotimestamp_n", list([np.transpose(np.array(
                [([None, '2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')), None, None])),
            ("m_nanotimestamp_3", list([np.transpose(
                np.array([(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[ns]')), np.array([i for i in range(1, r + 1)], dtype='int32'),
                np.array([i for i in range(1, c + 1)], dtype='int32')])),
        ],
    }

    pythons[DATATYPE.DT_FLOAT] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([1.0f, 2.0f],m_c))
            }
            m_float_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([1.0f, 2.0f, float()],m_c))
            }
            m_float_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([1.0f, float(), 2.0f],m_c))
            }
            m_float_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([float(), 1.0f, 2.0f],m_c))
            }
            m_float_n = matrix(xn)       

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([1.0f, 2.0f],m_c))
            }
            m_float_3 = matrix(x3)
            m_float_3.rename!(1..m_c, 1..m_r) 
        '''
        ,
        "upload": [
            ("m_float_0", matrix(np.array([([1.0, 2.0] * ceil(c / 2))[:c]] * r), dtype="float32").T),
            ("m_float_1", matrix(np.array([([1.0, 2.0, None] * ceil(c / 3))[:c]] * r), dtype="float32").T),
            ("m_float_2", matrix(np.array([([1.0, None, 2.0] * ceil(c / 3))[:c]] * r), dtype="float32").T),
            ("m_float_n", matrix(np.array([([None, 1.0, 2.0] * ceil(c / 3))[:c]] * r), dtype="float32").T),
            ("m_float_1", matrix(np.array([([1.0, 2.0, np.nan] * ceil(c / 3))[:c]] * r), dtype="float32").T),
            ("m_float_2", matrix(np.array([([1.0, np.nan, 2.0] * ceil(c / 3))[:c]] * r), dtype="float32").T),
            ("m_float_n", matrix(np.array([([np.nan, 1.0, 2.0] * ceil(c / 3))[:c]] * r), dtype="float32").T),
            # ("m_float_1",matrix(np.array([([1.0, 2.0, pd.NaT] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_float_2",matrix(np.array([([1.0, pd.NaT, 2.0] * ceil(c/3))[:c]] *r),dtype="object").T),
            # ("m_float_n",matrix(np.array([([pd.NaT, 1.0, 2.0] * ceil(c/3))[:c]] *r),dtype="object").T),

            # ("m_float_1",matrix(np.array([([1.0, 2.0, None] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_2",matrix(np.array([([1.0, None, 2.0] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_n",matrix(np.array([([None, 1.0, 2.0] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_1",matrix(np.array([([1.0, 2.0, np.nan] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_2",matrix(np.array([([1.0, np.nan, 2.0] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_n",matrix(np.array([([np.nan, 1.0, 2.0] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_1",matrix(np.array([([1.0, 2.0, pd.NaT] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_2",matrix(np.array([([1.0, pd.NaT, 2.0] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_n",matrix(np.array([([pd.NaT, 1.0, 2.0] * ceil(c/3))[:c]] *r),dtype="float32").T),

            # ("m_float_0",list(map(list, zip(*[([1.0, 2.0] * ceil(c/2))[:c]] *r)))),
            # ("m_float_1",list(map(list, zip(*[([1.0, 2.0, None] * ceil(c/3))[:c]] *r)))),
            # ("m_float_2",list(map(list, zip(*[([1.0, None, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_float_n",list(map(list, zip(*[([None, 1.0, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_float_1",list(map(list, zip(*[([1.0, 2.0, np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_float_2",list(map(list, zip(*[([1.0, np.nan, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_float_n",list(map(list, zip(*[([np.nan, 1.0, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_float_1",list(map(list, zip(*[([1.0, 2.0, pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_float_2",list(map(list, zip(*[([1.0, pd.NaT, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_float_n",list(map(list, zip(*[([pd.NaT, 1.0, 2.0] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_float_0", list([np.transpose(np.array([([1.0, 2.0] * ceil(c / 2))[:c]] * r)), None, None])),
            ("m_float_1", list([np.transpose(np.array([([1.0, 2.0, np.nan] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_float_2", list([np.transpose(np.array([([1.0, np.nan, 2.0] * ceil(c / 2))[:c]] * r)), None, None])),
            ("m_float_n", list([np.transpose(np.array([([np.nan, 1.0, 2.0] * ceil(c / 2))[:c]] * r)), None, None])),
            ("m_float_3", list([np.transpose(np.array([([1.0, 2.0] * ceil(c / 2))[:c]] * r)),
                                np.array([i for i in range(1, r + 1)], dtype='int32'),
                                np.array([i for i in range(1, c + 1)], dtype='int32')])),
            # ("m_float_0",matrix(np.array([([1.0, 2.0] * ceil(c/2))[:c]] *r),dtype="float32").T),
            # ("m_float_1",matrix(np.array([([1.0, 2.0, None] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_2",matrix(np.array([([1.0, None, 2.0] * ceil(c/3))[:c]] *r),dtype="float32").T),
            # ("m_float_n",matrix(np.array([([None, 1.0, 2.0] * ceil(c/3))[:c]] *r),dtype="float32").T),
        ],
    }

    pythons[DATATYPE.DT_DOUBLE] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take([1.0F, 2.0F],m_c))
            }
            m_double_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take([1.0F, 2.0F, double()],m_c))
            }
            m_double_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take([1.0F, double(), 2.0F],m_c))
            }
            m_double_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take([double(), 1.0F, 2.0F],m_c))
            }
            m_double_n = matrix(xn)

            x3 = []
            for(i in 1..m_r){
                x3.append!(take([1.0F, 2.0F],m_c))
            }
            m_double_3 = matrix(x3)
            m_double_3.rename!(1..m_c, 1..m_r) 
        '''
        ,
        "upload": [
            ("m_double_0", matrix(np.array([([1.0, 2.0] * ceil(c / 2))[:c]] * r), dtype="object").T),
            ("m_double_1", matrix(np.array([([1.0, 2.0, None] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_double_2", matrix(np.array([([1.0, None, 2.0] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_double_n", matrix(np.array([([None, 1.0, 2.0] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_double_1", matrix(np.array([([1.0, 2.0, np.nan] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_double_2", matrix(np.array([([1.0, np.nan, 2.0] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_double_n", matrix(np.array([([np.nan, 1.0, 2.0] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_double_1", matrix(np.array([([1.0, 2.0, pd.NaT] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_double_2", matrix(np.array([([1.0, pd.NaT, 2.0] * ceil(c / 3))[:c]] * r), dtype="object").T),
            ("m_double_n", matrix(np.array([([pd.NaT, 1.0, 2.0] * ceil(c / 3))[:c]] * r), dtype="object").T),

            ("m_double_0", matrix(np.array([([1.0, 2.0] * ceil(c / 2))[:c]] * r), dtype="float64").T),
            # ("m_double_1",matrix(np.array([([1.0, 2.0, None] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_2",matrix(np.array([([1.0, None, 2.0] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_n",matrix(np.array([([None, 1.0, 2.0] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_1",matrix(np.array([([1.0, 2.0, np.nan] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_2",matrix(np.array([([1.0, np.nan, 2.0] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_n",matrix(np.array([([np.nan, 1.0, 2.0] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_1",matrix(np.array([([1.0, 2.0, pd.NaT] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_2",matrix(np.array([([1.0, pd.NaT, 2.0] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_n",matrix(np.array([([pd.NaT, 1.0, 2.0] * ceil(c/3))[:c]] *r),dtype="float64").T),

            # ("m_double_0",list(map(list, zip(*[([1.0, 2.0] * ceil(c/2))[:c]] *r)))),
            # ("m_double_1",list(map(list, zip(*[([1.0, 2.0, None] * ceil(c/3))[:c]] *r)))),
            # ("m_double_2",list(map(list, zip(*[([1.0, None, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_double_n",list(map(list, zip(*[([None, 1.0, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_double_1",list(map(list, zip(*[([1.0, 2.0, np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_double_2",list(map(list, zip(*[([1.0, np.nan, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_double_n",list(map(list, zip(*[([np.nan, 1.0, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_double_1",list(map(list, zip(*[([1.0, 2.0, pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_double_2",list(map(list, zip(*[([1.0, pd.NaT, 2.0] * ceil(c/3))[:c]] *r)))),
            # ("m_double_n",list(map(list, zip(*[([pd.NaT, 1.0, 2.0] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_double_0", list([np.transpose(np.array([([1.0, 2.0] * ceil(c / 2))[:c]] * r)), None, None])),
            ("m_double_1", list([np.transpose(np.array([([1.0, 2.0, np.nan] * ceil(c / 3))[:c]] * r)), None, None])),
            ("m_double_2", list([np.transpose(np.array([([1.0, np.nan, 2.0] * ceil(c / 2))[:c]] * r)), None, None])),
            ("m_double_n", list([np.transpose(np.array([([np.nan, 1.0, 2.0] * ceil(c / 2))[:c]] * r)), None, None])),
            ("m_double_3", list([np.transpose(np.array([([1.0, 2.0] * ceil(c / 2))[:c]] * r)),
                                 np.array([i for i in range(1, r + 1)], dtype='int32'),
                                 np.array([i for i in range(1, c + 1)], dtype='int32')])),
            # ("m_double_0",matrix(np.array([([1.0, 2.0] * ceil(c/2))[:c]] *r),dtype="float64").T),
            # ("m_double_1",matrix(np.array([([1.0, 2.0, None] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_2",matrix(np.array([([1.0, None, 2.0] * ceil(c/3))[:c]] *r),dtype="float64").T),
            # ("m_double_n",matrix(np.array([([None, 1.0, 2.0] * ceil(c/3))[:c]] *r),dtype="float64").T),
        ],
    }

    pythons[DATATYPE.DT_SYMBOL] = {
        "scripts": ''''''
        # can not create a matrix of symbol
        ,
        "upload": [],
        "download": [],
    }

    pythons[DATATYPE.DT_STRING] = {
        "scripts": ''''''
        # can not create a matrix of string
        ,
        "upload": [],
        "download": [],
    }

    pythons[DATATYPE.DT_UUID] = {
        "scripts": ''''''
        # can not create a matrix of uuid
        ,
        "upload": [],
        "download": [],
    }

    pythons[DATATYPE.DT_DATEHOUR] = {
        "scripts":
            '''
            x0 = []
            for(i in 1..m_r){
                x0.append!(take(datehour([2001.01.31 13:30:10, 2020.01.31T17:37:20]),m_c))
            }
            m_datehour_0 = matrix(x0) 

            x1 = []
            for(i in 1..m_r){
                x1.append!(take(datehour([2001.01.31 13:30:10, 2020.01.31T17:37:20, datetime()]),m_c))
            }
            m_datehour_1 = matrix(x1)

            x2 = []
            for(i in 1..m_r){
                x2.append!(take(datehour([2001.01.31 13:30:10, datetime(), 2020.01.31T17:37:20]),m_c))
            }
            m_datehour_2 = matrix(x2)

            xn = []
            for(i in 1..m_r){
                xn.append!(take(datehour([datetime(), 2001.01.31 13:30:10, 2020.01.31T17:37:20]),m_c))
            }
            m_datehour_n = matrix(xn)  

            x3 = []
            for(i in 1..m_r){
                x3.append!(take(datehour([2001.01.31 13:30:10, 2020.01.31T17:37:20]),m_c))
            }
            m_datehour_3 = matrix(x3)
            m_datehour_3.rename!(1..m_c, 1..m_r)
            '''
        ,
        "upload": [
            ("m_datehour_0", matrix(np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r),
                                    dtype="datetime64[h]").T),
            ("m_datehour_1",
             matrix(np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', None] * ceil(c / 3))[:c]] * r),
                    dtype="datetime64[h]").T),
            ("m_datehour_2",
             matrix(np.array([(['2001-01-31 13:30:10', None, '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r),
                    dtype="datetime64[h]").T),
            ("m_datehour_n",
             matrix(np.array([([None, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r),
                    dtype="datetime64[h]").T),
            # ("m_datehour_1",matrix(np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', np.nan] * ceil(c/3))[:c]] *r),dtype="datetime64[h]").T),
            # ("m_datehour_2",matrix(np.array([(['2001-01-31 13:30:10', np.nan, '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r),dtype="datetime64[h]").T),
            # ("m_datehour_n",matrix(np.array([([np.nan, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r),dtype="datetime64[h]").T),
            # ("m_datehour_1",matrix(np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', pd.NaT] * ceil(c/3))[:c]] *r),dtype="datetime64[h]").T),
            # ("m_datehour_2",matrix(np.array([(['2001-01-31 13:30:10.123', pd.NaT, '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r),dtype="datetime64[h]").T),
            # ("m_datehour_n",matrix(np.array([([pd.NaT, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c/3))[:c]] *r),dtype="datetime64[h]").T),

            # ("m_datehour_0",list(map(list, zip(*[(['2001.01.31T13', '2020.01.31T17'] * ceil(c/2))[:c]] *r)))),
            # ("m_datahour_1",list(map(list, zip(*[(['2001.01.31T13', '2020.01.31T17', None] * ceil(c/3))[:c]] *r)))),
            # ("m_datehour_2",list(map(list, zip(*[(['2001.01.31T13', None, '2020.01.31T17'] * ceil(c/3))[:c]] *r)))),
            # ("m_datehour_n",list(map(list, zip(*[([None, '2001.01.31T13', '2020.01.31T17'] * ceil(c/3))[:c]] *r)))),
            # ("m_datehour_1",list(map(list, zip(*[(['2001.01.31T13', '2020.01.31T17', np.nan] * ceil(c/3))[:c]] *r)))),
            # ("m_datehour_2",list(map(list, zip(*[(['2001.01.31T13', np.nan, '2020.01.31T17'] * ceil(c/3))[:c]] *r)))),
            # ("m_datehour_n",list(map(list, zip(*[([np.nan, '2001.01.31T13', '2020.01.31T17'] * ceil(c/3))[:c]] *r)))),
            # ("m_datehour_1",list(map(list, zip(*[(['2001.01.31T13', '2020.01.31T17', pd.NaT] * ceil(c/3))[:c]] *r)))),
            # ("m_datehour_2",list(map(list, zip(*[(['2001.01.31T13', pd.Nat, '2020.01.31T17'] * ceil(c/3))[:c]] *r)))),
            # ("m_datehour_n",list(map(list, zip(*[([pd.NaT, '2001.01.31T13', '2020.01.31T17'] * ceil(c/3))[:c]] *r))))
        ],
        "download": [
            ("m_datehour_0", list([np.transpose(
                np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[h]')), None, None])),
            ("m_datehour_1", list([np.transpose(
                np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', None] * ceil(c / 3))[:c]] * r,
                         dtype='datetime64[h]')), None, None])),
            ("m_datehour_2", list([np.transpose(
                np.array([(['2001-01-31 13:30:10', None, '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[h]')), None, None])),
            ("m_datehour_n", list([np.transpose(
                np.array([([None, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[h]')), None, None])),
            ("m_datehour_3", list([np.transpose(
                np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r,
                         dtype='datetime64[h]')), np.array([i for i in range(1, r + 1)], dtype='int32'),
                np.array([i for i in range(1, c + 1)], dtype='int32')])),
        ],
    }

    pythons[DATATYPE.DT_IPADDR] = {
        "scripts": ''''''
        # can not create a matrix of IPADDR
        ,
        "upload": [],
        "download": [],
    }

    pythons[DATATYPE.DT_INT128] = {
        "scripts": ''''''
        # can not create a matrix of INT128
        ,
        "upload": [],
        "download": [],
    }

    pythons[DATATYPE.DT_BLOB] = {
        "scripts": ''''''
        # can not create a matrix of BLOB
        ,
        "upload": [],
        "download": [],
    }

    pythons[DATATYPE.DT_DECIMAL32] = {
        "scripts": ''''''
        # not support in this version
        ,
        "upload": [],
        "download": [],
    }

    pythons[DATATYPE.DT_DECIMAL64] = {
        "scripts": ''''''
        # not support in this version
        ,
        "upload": [],
        "download": [],
    }

    def get_script(*args, types=None, **kwargs):
        res = pythons[types]["scripts"]
        return matrix_row + matrix_col + res

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_Pair(*args, **kwargs):
    pythons = {DATATYPE.DT_VOID: {
        "scripts":
            '''
            p_void_0 = pair(,)
            '''
        ,
        "upload": [],
        "download": [
            # ('p_void_0', np.array([None, None], dtype = 'object')),
            ('p_void_0', list([None, None])),
        ],
    }, DATATYPE.DT_BOOL: {
        "scripts":
            '''
            p_bool_0 = pair(true,false)
            p_bool_1 = pair(true,bool())
            p_bool_2 = pair(bool(),false)                      
            '''
        ,
        "upload": [],
        "download": [
            # ('p_bool_0', np.array([True, False], dtype = 'object')),
            # ('p_bool_1', np.array([True, None], dtype = 'object')),
            # ('p_bool_2', np.array([None, False], dtype = 'object')),
            # ('p_bool_0', np.array([True, False], dtype = 'bool')),
            # ('p_bool_1', np.array([True, None], dtype = 'bool')),
            # ('p_bool_2', np.array([None, False], dtype = 'bool')),
            ('p_bool_0', list([True, False])),
            ('p_bool_1', list([True, None])),
            ('p_bool_2', list([None, False])),
        ],
    }, DATATYPE.DT_CHAR: {
        "scripts":
            '''
            p_char_0 = pair(1c,2c)
            p_char_1 = pair(1c,char())
            p_char_2 = pair(char(),2c)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_char_0', np.array([1, 2], dtype = 'object')),
            # ('p_char_1', np.array([1, None], dtype = 'object')),
            # ('p_char_2', np.array([None, 2], dtype = 'object')),
            # ('p_char_0', np.array([1, 2], dtype = 'int8')),
            ('p_char_0', list([1, 2])),
            ('p_char_1', list([1, None])),
            ('p_char_2', list([None, 2])),
        ],
    }, DATATYPE.DT_SHORT: {
        "scripts":
            '''
            p_short_0 = pair(1,2)
            p_short_1 = pair(1, short())
            p_short_2 = pair(short(),2)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_short_0', np.array([1, 2], dtype = 'object')),
            # ('p_short_1', np.array([1, None], dtype = 'object')),
            # ('p_short_2', np.array([None, 2], dtype = 'object')),
            # ('p_short_0', np.array([1, 2], dtype = 'int16')),
            ('p_short_0', list([1, 2])),
            ('p_short_1', list([1, None])),
            ('p_short_2', list([None, 2])),
        ],
    }, DATATYPE.DT_INT: {
        "scripts":
            '''
            p_int_0 = pair(1,2)
            p_int_1 = pair(1, int())
            p_int_2 = pair(int(),2)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_int_0', np.array([1, 2], dtype = 'object')),
            # ('p_int_1', np.array([1, None], dtype = 'object')),
            # ('p_int_2', np.array([None, 2], dtype = 'object')),
            # ('p_int_0', np.array([1, 2], dtype = 'int32')),
            ('p_int_0', list([1, 2])),
            ('p_int_1', list([1, None])),
            ('p_int_2', list([None, 2])),
        ],
    }, DATATYPE.DT_LONG: {
        "scripts":
            '''
            p_long_0 = pair(1l,2l)
            p_long_1 = pair(1l, long())
            p_long_2 = pair(long(),2l)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_long_0', np.array([1, 2], dtype = 'object')),
            # ('p_long_1', np.array([1, None], dtype = 'object')),
            # ('p_long_2', np.array([None, 2], dtype = 'object')),
            # ('p_long_0', np.array([1, 2], dtype = 'int64')),
            ('p_long_0', list([1, 2])),
            ('p_long_1', list([1, None])),
            ('p_long_2', list([None, 2])),
        ],
    }, DATATYPE.DT_DATE: {
        "scripts":
            '''
            p_date_0 = pair(2022.11.09, 2001.01.31)
            p_date_1 = pair(2022.11.09, date())
            p_date_2 = pair(date(), 2001.01.31)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_date_0', np.array(['2022-11-09', '2001-01-30'], dtype = 'object')),
            # ('p_date_1', np.array(['2022-11-09', None], dtype = 'object')),
            # ('p_date_2', np.array([None, '2001-01-30'], dtype = 'object')),
            # ('p_date_0', np.array(['2022-11-09', '2001-01-30'], dtype = 'datetime64[D]')),
            # ('p_date_1', np.array(['2022-11-09', None], dtype = 'datetime64[D]')),
            # ('p_date_2', np.array([None, '2001-01-30'], dtype = 'datetime64[D]')),
            ('p_date_0', list([np.datetime64('2022-11-09', 'D'), np.datetime64('2001-01-31', 'D')])),
            ('p_date_1', list([np.datetime64('2022-11-09', 'D'), None])),
            ('p_date_2', list([None, np.datetime64('2001-01-31', 'D')])),
        ],
    }, DATATYPE.DT_MONTH: {
        "scripts":
            '''
            p_month_0 = pair(2022.11M, 2001.01M)
            p_month_1 = pair(2022.11M, month())
            p_month_2 = pair(month(), 2001.01M)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_month_0', np.array(['2022-11', '2001-01'], dtype = 'object')),
            # ('p_month_1', np.array(['2022-11', None], dtype = 'object')),
            # ('p_month_2', np.array([None, '2001-01'], dtype = 'object')),
            # ('p_month_0', np.array(['2022-11-01', '2001-01-01'], dtype = 'datetime64[M]')),
            # ('p_month_1', np.array(['2022-11-01', None], dtype = 'datetime64[M]')),
            # ('p_month_2', np.array([None, '2001-01-01'], dtype = 'datetime64[M]')),
            ('p_month_0', list([np.datetime64('2022-11', 'M'), np.datetime64('2001-01', 'M')])),
            ('p_month_1', list([np.datetime64('2022-11', 'M'), None])),
            ('p_month_2', list([None, np.datetime64('2001-01', 'M')])),
        ],
    }, DATATYPE.DT_TIME: {
        "scripts":
            '''
            p_time_0 = pair(13:10:10.008, 22:25:45.007)
            p_time_1 = pair(13:10:10.008, time())
            p_time_2 = pair(time(), 22:25:45.007)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_time_0', np.array(['1970-01-01T13:10:10.008', '1970-01-01T22:25:45.007'], dtype = 'object')),
            # ('p_time_1', np.array(['1970-01-01T13:10:10.008', None], dtype = 'object')),
            # ('p_time_2', np.array([None, '1970-01-01T22:25:45.007'], dtype = 'object')),
            # ('p_time_0', np.array(['1970-01-01T13:10:10.008', '1970-01-01T22:25:45.007'], dtype = 'datetime64[ms]')),
            # ('p_time_1', np.array(['1970-01-01T13:10:10.008', None], dtype = 'datetime64[ms]')),
            # ('p_time_2', np.array([None, '1970-01-01T22:25:45.007'], dtype = 'datetime64[ms]')),
            ('p_time_0',
             list([np.datetime64('1970-01-01T13:10:10.008', 'ms'), np.datetime64('1970-01-01T22:25:45.007', 'ms')])),
            ('p_time_1', list([np.datetime64('1970-01-01T13:10:10.008', 'ms'), None])),
            ('p_time_2', list([None, np.datetime64('1970-01-01T22:25:45.007', 'ms')])),
        ],
    }, DATATYPE.DT_MINUTE: {
        "scripts":
            '''
            p_minute_0 = pair(13:10m, 22:25m)
            p_minute_1 = pair(13:10m, minute())
            p_minute_2 = pair(minute(), 22:25m)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_minute_0', np.array(['1970-01-01T13:10', '1970-01-01T22:25'], dtype = 'object')),
            # ('p_minute_1', np.array(['1970-01-01T13:10', None], dtype = 'object')),
            # ('p_minute_2', np.array([None, '1970-01-01T22:25'], dtype = 'object')),
            # ('p_minute_0', np.array(['1970-01-01T13:10', '1970-01-01T22:25'], dtype = 'datetime64[m]')),
            # ('p_minute_1', np.array(['1970-01-01T13:10:10.008', None], dtype = 'datetime64[m]')),
            # ('p_minute_2', np.array([None, '1970-01-01T22:25:45.007'], dtype = 'datetime64[m]')),
            ('p_minute_0', list([np.datetime64('1970-01-01T13:10', 'm'), np.datetime64('1970-01-01T22:25', 'm')])),
            ('p_minute_1', list([np.datetime64('1970-01-01T13:10', 'm'), None])),
            ('p_minute_2', list([None, np.datetime64('1970-01-01T22:25', 'm')])),
        ],
    }, DATATYPE.DT_SECOND: {
        "scripts":
            '''
            p_second_0 = pair(13:10:10, 22:25:45)
            p_second_1 = pair(13:10:10, second())
            p_second_2 = pair(second(), 22:25:45)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_second_0', np.array(['1970-01-01T13:10:10', '1970-01-01T22:25:45'], dtype = 'object')),
            # ('p_second_1', np.array(['1970-01-01T13:10:10', None], dtype = 'object')),
            # ('p_second_2', np.array([None, '1970-01-01T22:25:45'], dtype = 'object')),
            # ('p_second_0', np.array(['1970-01-01T13:10:10.008', '1970-01-01T22:25:45.007'], dtype = 'datetime64[s]')),
            # ('p_second_1', np.array(['1970-01-01T13:10:10.008', None], dtype = 'datetime64[s]')),
            # ('p_second_2', np.array([None, '1970-01-01T22:25:45.007'], dtype = 'datetime64[s]')),
            (
                'p_second_0',
                list([np.datetime64('1970-01-01T13:10:10', 's'), np.datetime64('1970-01-01T22:25:45', 's')])),
            ('p_second_1', list([np.datetime64('1970-01-01T13:10:10', 's'), None])),
            ('p_second_2', list([None, np.datetime64('1970-01-01T22:25:45', 's')])),
        ],
    }, DATATYPE.DT_DATETIME: {
        "scripts":
            '''
            p_datetime_0 = pair(2022.11.09T13:30:10, 2001.01.31T15:32:17)
            p_datetime_1 = pair(2022.11.09T13:30:10, datetime())
            p_datetime_2 = pair(datetime(), 2001.01.31T15:32:17)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_datetime_0', np.array(['2022-11-09T13:30:10', '2001-01-31T15:32:17'], dtype = 'object')),
            # ('p_datetime_1', np.array(['2022-11-09T13:30:10', None], dtype = 'object')),
            # ('p_datetime_2', np.array([None, '2001-01-31T15:32:17'], dtype = 'object')),
            # ('p_datetime_0', np.array(['2022-11-09T13:30:10', '2001-01-31T15:32:17'], dtype = 'datetime64[D]')),
            # ('p_datetime_1', np.array(['2022-11-09T13:30:10', None], dtype = 'datetime64[s]')),
            # ('p_datetime_2', np.array([None, '2001-01-31T15:32:17'], dtype = 'datetime64[s]')),
            ('p_datetime_0',
             list([np.datetime64('2022-11-09T13:30:10', 's'), np.datetime64('2001-01-31T15:32:17', 's')])),
            ('p_datetime_1', list([np.datetime64('2022-11-09T13:30:10', 's'), None])),
            ('p_datetime_2', list([None, np.datetime64('2001-01-31T15:32:17', 's')])),
        ],
    }, DATATYPE.DT_TIMESTAMP: {
        "scripts":
            '''
            p_timestamp_0 = pair(2022.11.09T13:30:10.001, 2001.01.31T15:32:17.005)
            p_timestamp_1 = pair(2022.11.09T13:30:10.001, timestamp())
            p_timestamp_2 = pair(timestamp(), 2001.01.31T15:32:17.005)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_timestamp_0', np.array(['2022-11-09T13:30:10.001', '2001-01-31T15:32:17.005'], dtype = 'object')),
            # ('p_timestamp_1', np.array(['2022-11-09T13:30:10.001', None], dtype = 'object')),
            # ('p_timestamp_2', np.array([None, '2001-01-31T15:32:17.005'], dtype = 'object')),
            # ('p_timestamp_0', np.array(['2022-11-09T13:30:10.001', '2001-01-31T15:32:17.005'], dtype = 'datetime64[ms]')),
            # ('p_timestamp_1', np.array(['2022-11-09T13:30:10.001', None], dtype = 'datetime64[ms]')),
            # ('p_timestamp_2', np.array([None, '2001-01-31T15:32:17.005'], dtype = 'datetime64[ms]')),
            ('p_timestamp_0',
             list([np.datetime64('2022-11-09T13:30:10.001', 'ms'), np.datetime64('2001-01-31T15:32:17.005', 'ms')])),
            ('p_timestamp_1', list([np.datetime64('2022-11-09T13:30:10.001', 'ms'), None])),
            ('p_timestamp_2', list([None, np.datetime64('2001-01-31T15:32:17.005', 'ms')])),
        ],
    }, DATATYPE.DT_NANOTIME: {
        "scripts":
            '''
            p_nanotime_0 = pair(13:10:10.123456789, 22:25:45.789456123)
            p_nanotime_1 = pair(13:10:10.123456789, nanotime())
            p_nanotime_2 = pair(nanotime(), 22:25:45.789456123)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_nanotime_0', np.array(['1970-01-01T13:10:10.123456789', '1970-01-01T22:25:45.789456123'], dtype = 'object')),
            # ('p_nanotime_1', np.array(['1970-01-01T13:10:10.123456789', None], dtype = 'object')),
            # ('p_nanotime_2', np.array([None, '1970-01-01T22:25:45.789456123'], dtype = 'object')),
            # ('p_nanotime_0', np.array(['1970-01-01T13:10:10.123456789', '1970-01-01T22:25:45.789456123'], dtype = 'datetime64[ns]')),
            # ('p_nanotime_1', np.array(['1970-01-01T13:10:10.123456789', None], dtype = 'datetime64[ns]')),
            # ('p_nanotime_2', np.array([None, '1970-01-01T22:25:45.789456123'], dtype = 'datetime64[ns]')),
            ('p_nanotime_0', list([np.datetime64('1970-01-01T13:10:10.123456789', 'ns'),
                                   np.datetime64('1970-01-01T22:25:45.789456123', 'ns')])),
            ('p_nanotime_1', list([np.datetime64('1970-01-01T13:10:10.123456789', 'ns'), None])),
            ('p_nanotime_2', list([None, np.datetime64('1970-01-01T22:25:45.789456123', 'ns')])),
        ],
    }, DATATYPE.DT_NANOTIMESTAMP: {
        "scripts":
            '''
            p_nanotimestamp_0 = pair(2022.11.09T13:30:10.123456789, 2001.01.31T15:32:17.789456123)
            p_nanotimestamp_1 = pair(2022.11.09T13:30:10.123456789, nanotimestamp())
            p_nanotimestamp_2 = pair(nanotimestamp(), 2001.01.31T15:32:17.789456123)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_nanotimestamp_0', np.array(['2022-11-09T13:30:10.123456789', '2001-01-31T15:32:17.789456123'], dtype = 'object')),
            # ('p_nanotimestamp_1', np.array(['2022-11-09T13:30:10.123456789', None], dtype = 'object')),
            # ('p_nanotimestamp_2', np.array([None, '2001-01-31T15:32:17.789456123'], dtype = 'object')),
            # ('p_nanotimestamp_0', np.array(['2022-11-09T13:30:10.123456789', '2001-01-31T15:32:17.789456123'], dtype = 'datetime64[ns]')),
            # ('p_nanotimestamp_1', np.array(['2022-11-09T13:30:10.123456789', None], dtype = 'datetime64[ns]')),
            # ('p_nanotimestamp_2', np.array([None, '2001-01-31T15:32:17.789456123'], dtype = 'datetime64[ns]')),
            ('p_nanotimestamp_0', list([np.datetime64('2022-11-09T13:30:10.123456789', 'ns'),
                                        np.datetime64('2001-01-31T15:32:17.789456123', 'ns')])),
            ('p_nanotimestamp_1', list([np.datetime64('2022-11-09T13:30:10.123456789', 'ns'), None])),
            ('p_nanotimestamp_2', list([None, np.datetime64('2001-01-31T15:32:17.789456123', 'ns')])),
        ],
    }, DATATYPE.DT_FLOAT: {
        "scripts":
            '''
            p_float_0 = pair(1f,2f)
            p_float_1 = pair(1f,float())
            p_float_2 = pair(float(),2f)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_float_0', np.array([1.0, 2.0], dtype = 'object')),
            # ('p_float_1', np.array([1.0, None], dtype = 'object')),
            # ('p_float_2', np.array([None, 2.0], dtype = 'object')),
            # ('p_float_0', np.array([1.0, 2.0], dtype = 'float32')),
            ('p_float_0', list([1.0, 2.0])),
            ('p_float_1', list([1.0, None])),
            ('p_float_2', list([None, 2.0])),
        ],
    }, DATATYPE.DT_DOUBLE: {
        "scripts":
            '''
            p_double_0 = pair(1F,2F)
            p_double_1 = pair(1F, double())
            p_double_2 = pair(double(),2F)     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_double_0', np.array([1.0, 2.0], dtype = 'object')),
            # ('p_double_1', np.array([1.0, None], dtype = 'object')),
            # ('p_double_2', np.array([None, 2.0], dtype = 'object')),
            # ('p_double_0', np.array([1.0, 2.0], dtype = 'float64')),
            ('p_double_0', list([1.0, 2.0])),
            ('p_double_1', list([1.0, None])),
            ('p_double_2', list([None, 2.0])),
        ],
    }, DATATYPE.DT_SYMBOL: {
        "scripts": ''''''
        # no way to create a symbol pair
        ,
        "upload": [],
        "download": [
        ],
    }, DATATYPE.DT_STRING: {
        "scripts":
            '''
            p_string_0 = pair("sas","sahska")
            p_string_1 = pair("sas",string())
            p_string_2 = pair(string(),"sahska")
            '''
        ,
        "upload": [],
        "download": [
            # ('p_string_0', np.array(["sas", "sahska"], dtype = 'object')),
            # ('p_string_1', np.array(["sas", None], dtype = 'object')),
            # ('p_string_2', np.array([None, "sahska"], dtype = 'object')),
            ('p_string_0', list(["sas", "sahska"])),
            ('p_string_1', list(["sas", None])),
            ('p_string_2', list([None, "sahska"])),
        ],
    }, DATATYPE.DT_UUID: {
        "scripts":
            '''
            p_uuid_0 = pair(uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"), uuid(""))
            p_uuid_1 = pair(uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"), uuid())
            p_uuid_2 = pair(uuid(), uuid(""))
            '''
        ,
        "upload": [],
        "download": [
            # ('p_uuid_0', np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', ""], dtype = 'object')),
            # ('p_uuid_1', np.array(['5d212a78-cc48-e3b1-4235-b4d91473ee87', None], dtype = 'object')),
            # ('p_uuid_2', np.array([None, ''], dtype = 'object')),
            ('p_uuid_0', list(['5d212a78-cc48-e3b1-4235-b4d91473ee87', None])),
            ('p_uuid_1', list(['5d212a78-cc48-e3b1-4235-b4d91473ee87', None])),
            ('p_uuid_2', list([None, None])),
        ],
    }, DATATYPE.DT_DATEHOUR: {
        "scripts":
            '''
            p_timestamp_0 = pair(datehour(2022.11.09T13:30:10.001), datehour(2001.01.31T15:32:17.005))
            p_timestamp_1 = pair(datehour(2022.11.09T13:30:10.001), datehour())
            p_timestamp_2 = pair(datehour(), datehour(2001.01.31T15:32:17.005))     
            '''
        ,
        "upload": [],
        "download": [
            # ('p_timestamp_0', np.array(['2022-11-09T13', '2001-01-31T15'], dtype = 'object')),
            # ('p_timestamp_1', np.array(['2022-11-09T13', None], dtype = 'object')),
            # ('p_timestamp_2', np.array([None, '2001-01-31T15'], dtype = 'object')),
            # ('p_timestamp_0', np.array(['2022-11-09T13:30:10.001', '2001-01-31T15:32:17.005'], dtype = 'datetime64[h]')),
            # ('p_timestamp_1', np.array(['2022-11-09T13:30:10.001', None], dtype = 'datetime64[h]')),
            # ('p_timestamp_2', np.array([None, '2001-01-31T15:32:17.005'], dtype = 'datetime64[h]')),
            ('p_timestamp_0', list([np.datetime64('2022-11-09T13', 'h'), np.datetime64('2001-01-31T15', 'h')])),
            ('p_timestamp_1', list([np.datetime64('2022-11-09T13', 'h'), None])),
            ('p_timestamp_2', list([None, np.datetime64('2001-01-31T15', 'h')])),
        ],
    }, DATATYPE.DT_IPADDR: {
        "scripts":
            '''
            p_ipaddr_0 = pair(ipaddr("192.168.1.13"), ipaddr(""))
            p_ipaddr_1 = pair(ipaddr("192.168.1.13"), ipaddr())
            p_ipaddr_2 = pair(ipaddr(), ipaddr(""))
            '''
        ,
        "upload": [],
        "download": [
            # ('p_ipaddr_0', np.array(["192.168.1.13", None], dtype = 'object')),
            # ('p_ipaddr_1', np.array(["192.168.1.13", None], dtype = 'object')),
            # ('p_ipaddr_2', np.array([None, ''], dtype = 'object')),
            ('p_ipaddr_0', list(["192.168.1.13", None])),
            ('p_ipaddr_1', list(["192.168.1.13", None])),
            ('p_ipaddr_2', list([None, None])),
        ],
    }, DATATYPE.DT_INT128: {
        "scripts": ''''''
        # can not create a matrix of INT128

        ,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_BLOB: {
        "scripts":
            '''
            p_blob_0 = pair(blob("test"), blob("123456"))
            p_blob_1 = pair(blob("test"), blob(''))
            p_blob_2 = pair(blob(''), blob(""))
            '''
        ,
        "upload": [],
        "download": [
            # ('p_blob_0', np.array(['test', "123456"], dtype = 'object')),
            # ('p_blob_1', np.array(['test', None], dtype = 'object')),
            # ('p_blob_2', np.array([None, "123456"], dtype = 'object')),
            ('p_blob_0', list([b'test', b"123456"])),
            ('p_blob_1', list([b'test', None])),
            ('p_blob_2', list([None, None])),
        ],
    }, DATATYPE.DT_DECIMAL32: {
        "scripts": ''''''
        # not support in this version
        ,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_DECIMAL64: {
        "scripts": ''''''
        # not support in this version
        ,
        "upload": [],
        "download": [],
    }}

    # all type no way to upload

    # TODO some problem in the return value of uuid("")

    # TODO some problem in the return value of ippadr("")

    # TODO some problem in the return value of INT128("")

    def get_script(*args, types=None, **kwargs):
        res = pythons[types]["scripts"]
        return res

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_Table(*args, n=100, typeTable="table", isShare=False, **kwargs):
    """typeTable: [table, streamTable, indexedTable, keyedTable]"""
    prefix, _ = get_Vector(n=n)

    if not isShare:
        testTypeTable = typeTable
    else:
        testTypeTable = "share_" + typeTable

    shareScrips = """
        share table_bool_0 as share_table_bool_0
        share table_bool_1 as share_table_bool_1
        share table_bool_2 as share_table_bool_2
        share table_bool_3 as share_table_bool_3

        share table_char_0 as share_table_char_0
        share table_char_1 as share_table_char_1
        share table_char_2 as share_table_char_2
        share table_char_3 as share_table_char_3

        share table_short_0 as share_table_short_0
        share table_short_1 as share_table_short_1
        share table_short_2 as share_table_short_2
        share table_short_3 as share_table_short_3

        share table_int_0 as share_table_int_0
        share table_int_1 as share_table_int_1
        share table_int_2 as share_table_int_2
        share table_int_3 as share_table_int_3

        share table_long_0 as share_table_long_0
        share table_long_1 as share_table_long_1
        share table_long_2 as share_table_long_2
        share table_long_3 as share_table_long_3

        share table_date_0 as share_table_date_0
        share table_date_1 as share_table_date_1
        share table_date_2 as share_table_date_2
        share table_date_3 as share_table_date_3

        share table_month_0 as share_table_month_0
        share table_month_1 as share_table_month_1
        share table_month_2 as share_table_month_2
        share table_month_3 as share_table_month_3

        share table_time_0 as share_table_time_0
        share table_time_1 as share_table_time_1
        share table_time_2 as share_table_time_2
        share table_time_3 as share_table_time_3

        share table_minute_0 as share_table_minute_0
        share table_minute_1 as share_table_minute_1
        share table_minute_2 as share_table_minute_2
        share table_minute_3 as share_table_minute_3

        share table_second_0 as share_table_second_0
        share table_second_1 as share_table_second_1
        share table_second_2 as share_table_second_2
        share table_second_3 as share_table_second_3

        share table_datetime_0 as share_table_datetime_0
        share table_datetime_1 as share_table_datetime_1
        share table_datetime_2 as share_table_datetime_2
        share table_datetime_3 as share_table_datetime_3

        share table_timestamp_0 as share_table_timestamp_0
        share table_timestamp_1 as share_table_timestamp_1
        share table_timestamp_2 as share_table_timestamp_2
        share table_timestamp_3 as share_table_timestamp_3

        share table_nanotime_0 as share_table_nanotime_0
        share table_nanotime_1 as share_table_nanotime_1
        share table_nanotime_2 as share_table_nanotime_2
        share table_nanotime_3 as share_table_nanotime_3

        share table_nanotimestamp_0 as share_table_nanotimestamp_0
        share table_nanotimestamp_1 as share_table_nanotimestamp_1
        share table_nanotimestamp_2 as share_table_nanotimestamp_2
        share table_nanotimestamp_3 as share_table_nanotimestamp_3

        share table_float_0 as share_table_float_0
        share table_float_1 as share_table_float_1
        share table_float_2 as share_table_float_2
        share table_float_3 as share_table_float_3

        share table_double_0 as share_table_double_0
        share table_double_1 as share_table_double_1
        share table_double_2 as share_table_double_2
        share table_double_3 as share_table_double_3

        share table_symbol_0 as share_table_symbol_0
        share table_symbol_1 as share_table_symbol_1
        share table_symbol_2 as share_table_symbol_2
        share table_symbol_3 as share_table_symbol_3

        share table_string_0 as share_table_string_0
        share table_string_1 as share_table_string_1
        share table_string_2 as share_table_string_2
        share table_string_3 as share_table_string_3

        share table_uuid_0 as share_table_uuid_0
        share table_uuid_1 as share_table_uuid_1
        share table_uuid_2 as share_table_uuid_2
        share table_uuid_3 as share_table_uuid_3

        share table_datehour_0 as share_table_datehour_0
        share table_datehour_1 as share_table_datehour_1
        share table_datehour_2 as share_table_datehour_2
        share table_datehour_3 as share_table_datehour_3

        share table_ip_0 as share_table_ip_0
        share table_ip_1 as share_table_ip_1
        share table_ip_2 as share_table_ip_2
        share table_ip_3 as share_table_ip_3

        share table_int128_0 as share_table_int128_0
        share table_int128_1 as share_table_int128_1
        share table_int128_2 as share_table_int128_2
        share table_int128_3 as share_table_int128_3

        share table_blob_0 as share_table_blob_0
        share table_blob_1 as share_table_blob_1
        share table_blob_2 as share_table_blob_2
        share table_blob_3 as share_table_blob_3
    """.replace("table", typeTable)

    appendScrips = """
        share test_table_bool_0 as test_share_table_bool_0
        share test_table_bool_1 as test_share_table_bool_1
        share test_table_bool_2 as test_share_table_bool_2
        share test_table_bool_3 as test_share_table_bool_3

        share test_table_char_0 as test_share_table_char_0
        share test_table_char_1 as test_share_table_char_1
        share test_table_char_2 as test_share_table_char_2
        share test_table_char_3 as test_share_table_char_3

        share test_table_short_0 as test_share_table_short_0
        share test_table_short_1 as test_share_table_short_1
        share test_table_short_2 as test_share_table_short_2
        share test_table_short_3 as test_share_table_short_3

        share test_table_int_0 as test_share_table_int_0
        share test_table_int_1 as test_share_table_int_1
        share test_table_int_2 as test_share_table_int_2
        share test_table_int_3 as test_share_table_int_3

        share test_table_long_0 as test_share_table_long_0
        share test_table_long_1 as test_share_table_long_1
        share test_table_long_2 as test_share_table_long_2
        share test_table_long_3 as test_share_table_long_3

        share test_table_date_0 as test_share_table_date_0
        share test_table_date_1 as test_share_table_date_1
        share test_table_date_2 as test_share_table_date_2
        share test_table_date_3 as test_share_table_date_3

        share test_table_month_0 as test_share_table_month_0
        share test_table_month_1 as test_share_table_month_1
        share test_table_month_2 as test_share_table_month_2
        share test_table_month_3 as test_share_table_month_3

        share test_table_time_0 as test_share_table_time_0
        share test_table_time_1 as test_share_table_time_1
        share test_table_time_2 as test_share_table_time_2
        share test_table_time_3 as test_share_table_time_3

        share test_table_minute_0 as test_share_table_minute_0
        share test_table_minute_1 as test_share_table_minute_1
        share test_table_minute_2 as test_share_table_minute_2
        share test_table_minute_3 as test_share_table_minute_3

        share test_table_second_0 as test_share_table_second_0
        share test_table_second_1 as test_share_table_second_1
        share test_table_second_2 as test_share_table_second_2
        share test_table_second_3 as test_share_table_second_3

        share test_table_datetime_0 as test_share_table_datetime_0
        share test_table_datetime_1 as test_share_table_datetime_1
        share test_table_datetime_2 as test_share_table_datetime_2
        share test_table_datetime_3 as test_share_table_datetime_3

        share test_table_timestamp_0 as test_share_table_timestamp_0
        share test_table_timestamp_1 as test_share_table_timestamp_1
        share test_table_timestamp_2 as test_share_table_timestamp_2
        share test_table_timestamp_3 as test_share_table_timestamp_3

        share test_table_nanotime_0 as test_share_table_nanotime_0
        share test_table_nanotime_1 as test_share_table_nanotime_1
        share test_table_nanotime_2 as test_share_table_nanotime_2
        share test_table_nanotime_3 as test_share_table_nanotime_3

        share test_table_nanotimestamp_0 as test_share_table_nanotimestamp_0
        share test_table_nanotimestamp_1 as test_share_table_nanotimestamp_1
        share test_table_nanotimestamp_2 as test_share_table_nanotimestamp_2
        share test_table_nanotimestamp_3 as test_share_table_nanotimestamp_3

        share test_table_float_0 as test_share_table_float_0
        share test_table_float_1 as test_share_table_float_1
        share test_table_float_2 as test_share_table_float_2
        share test_table_float_3 as test_share_table_float_3

        share test_table_double_0 as test_share_table_double_0
        share test_table_double_1 as test_share_table_double_1
        share test_table_double_2 as test_share_table_double_2
        share test_table_double_3 as test_share_table_double_3

        share test_table_symbol_0 as test_share_table_symbol_0
        share test_table_symbol_1 as test_share_table_symbol_1
        share test_table_symbol_2 as test_share_table_symbol_2
        share test_table_symbol_3 as test_share_table_symbol_3

        share test_table_string_0 as test_share_table_string_0
        share test_table_string_1 as test_share_table_string_1
        share test_table_string_2 as test_share_table_string_2
        share test_table_string_3 as test_share_table_string_3

        share test_table_uuid_0 as test_share_table_uuid_0
        share test_table_uuid_1 as test_share_table_uuid_1
        share test_table_uuid_2 as test_share_table_uuid_2
        share test_table_uuid_3 as test_share_table_uuid_3

        share test_table_datehour_0 as test_share_table_datehour_0
        share test_table_datehour_1 as test_share_table_datehour_1
        share test_table_datehour_2 as test_share_table_datehour_2
        share test_table_datehour_3 as test_share_table_datehour_3

        share test_table_ip_0 as test_share_table_ip_0
        share test_table_ip_1 as test_share_table_ip_1
        share test_table_ip_2 as test_share_table_ip_2
        share test_table_ip_3 as test_share_table_ip_3

        share test_table_int128_0 as test_share_table_int128_0
        share test_table_int128_1 as test_share_table_int128_1
        share test_table_int128_2 as test_share_table_int128_2
        share test_table_int128_3 as test_share_table_int128_3

        share test_table_blob_0 as test_share_table_blob_0
        share test_table_blob_1 as test_share_table_blob_1
        share test_table_blob_2 as test_share_table_blob_2
        share test_table_blob_3 as test_share_table_blob_3
    """.replace("table", typeTable)

    pythons = {DATATYPE.DT_VOID: {
        # NO way to create a VOID Vector
        "scripts": """
        """,
        "keyScripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_BOOL: {

        # 分类：table_0:正常值， 部分空值， 全部空值 , table_1:只有一列, table_2:只有一行， table_3:空表
        "scripts": """
            table_bool_0 = table(1..v_n as index, v_bool_0 as bool_0, v_bool_1 as bool_1, take([s_bool_n, s_bool_n, s_bool_n], v_n) as bool_2)
            table_bool_1 = table(1..v_n as index, v_bool_0 as bool_0)
            table_bool_2 = table(1 as index, [s_bool_1] as bool_0, [s_bool_2] as bool_1, [s_bool_n] as bool_2)
            table_bool_3 = table(v_n:0, `index`bool_0`bool_1`bool_2, [INT, BOOL, BOOL, BOOL])
            test_table_bool_0 = table(v_n:0, `index`bool_0`bool_1`bool_2, [INT, BOOL, BOOL, BOOL])
            test_table_bool_1 = table(v_n:0, `index`bool_0, [INT, BOOL])
            test_table_bool_2 = table(v_n:0, `index`bool_0`bool_1`bool_2, [INT, BOOL, BOOL, BOOL])
            test_table_bool_3 = table(v_n:0, `index`bool_0`bool_1`bool_2, [INT, BOOL, BOOL, BOOL])
        """.replace("table", typeTable),
        "keyScripts": """
            table_bool_0 = table(`index, 1..v_n as index, v_bool_0 as bool_0, v_bool_1 as bool_1, take([s_bool_n, s_bool_n, s_bool_n], v_n) as bool_2)
            table_bool_1 = table(`index, 1..v_n as index, v_bool_0 as bool_0)
            table_bool_2 = table(`index, 1 as index, [s_bool_1] as bool_0, [s_bool_2] as bool_1, [s_bool_n] as bool_2)
            table_bool_3 = table(`index, v_n:0, `index`bool_0`bool_1`bool_2, [INT, BOOL, BOOL, BOOL])
            test_table_bool_0 = table(`index, v_n:0, `index`bool_0`bool_1`bool_2, [INT, BOOL, BOOL, BOOL])
            test_table_bool_1 = table(`index, v_n:0, `index`bool_0, [INT, BOOL])
            test_table_bool_2 = table(`index, v_n:0, `index`bool_0`bool_1`bool_2, [INT, BOOL, BOOL, BOOL])
            test_table_bool_3 = table(`index, v_n:0, `index`bool_0`bool_1`bool_2, [INT, BOOL, BOOL, BOOL])
        """.replace("table", typeTable),
        "upload": [
            # ("{}_bool_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n+1),
            #                                                  "bool_0": np.tile(np.array([True, False], dtype="bool"), ceil(n/2))[:n],
            #                                                  "bool_1": np.tile(np.array([True, False, None], dtype="object"), ceil(n/3))[:n],
            #                                                  "bool_2": np.tile(np.array([None, None, None], dtype="object"), ceil(n/3))[:n]
            #                                                  })),
            ("{}_bool_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "bool_0": np.tile(np.array([True, False], dtype="bool"),
                                                                                ceil(n / 2))[:n],
                                                              })),
            # ("{}_bool_2".format(testTypeTable),  pd.DataFrame({"index": np.arange(1, 2),
            #                                                   "bool_0": np.array([True], dtype="bool"),
            #                                                   "bool_1": np.array([False], dtype="bool"),
            #                                                   "bool_2": np.array([None], dtype="object")
            #                                                   })),
            ("{}_bool_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                              "bool_0": np.array([], dtype="bool"),
                                                              "bool_1": np.array([], dtype="bool"),
                                                              "bool_2": np.array([], dtype="bool")
                                                              })),
        ],
        "download": [
            ("{}_bool_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "bool_0": np.tile(np.array([True, False], dtype="bool"),
                                                                                ceil(n / 2))[:n],
                                                              "bool_1": np.tile(
                                                                  np.array([True, False, None], dtype="object"),
                                                                  ceil(n / 3))[:n],
                                                              "bool_2": np.tile(
                                                                  np.array([None, None, None], dtype="object"),
                                                                  ceil(n / 3))[:n]
                                                              })),
            ("{}_bool_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "bool_0": np.tile(np.array([True, False], dtype="bool"),
                                                                                ceil(n / 2))[:n],
                                                              })),
            ("{}_bool_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                              "bool_0": np.array([True], dtype="bool"),
                                                              "bool_1": np.array([False], dtype="bool"),
                                                              "bool_2": np.array([None], dtype="object")
                                                              })),
            ("{}_bool_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                              "bool_0": np.array([], dtype="bool"),
                                                              "bool_1": np.array([], dtype="bool"),
                                                              "bool_2": np.array([], dtype="bool")
                                                              })),
        ],
    }, DATATYPE.DT_CHAR: {
        "scripts": """
            table_char_0 = table(1..v_n as index, v_char_0 as char_0, v_char_1 as char_1, take([s_char_n, s_char_n, s_char_n], v_n) as char_2)
            table_char_1 = table(1..v_n as index, v_char_0 as char_0)
            table_char_2 = table(1 as index, [s_char_1] as char_0, [s_char_2] as char_1, [s_char_n] as char_2)
            table_char_3 = table(v_n:0, `index`char_0`char_1`char_2, [INT, CHAR, CHAR, CHAR])
            test_table_char_0 = table(v_n:0, `index`char_0`char_1`char_2, [INT, CHAR, CHAR, CHAR])
            test_table_char_1 = table(v_n:0, `index`char_0, [INT, CHAR])
            test_table_char_2 = table(v_n:0, `index`char_0`char_1`char_2, [INT, CHAR, CHAR, CHAR])
            test_table_char_3 = table(v_n:0, `index`char_0`char_1`char_2, [INT, CHAR, CHAR, CHAR])
        """.replace("table", typeTable),
        "keyScripts": """
            table_char_0 = table(`index, 1..v_n as index, v_char_0 as char_0, v_char_1 as char_1, take([s_char_n, s_char_n, s_char_n], v_n) as char_2)
            table_char_1 = table(`index, 1..v_n as index, v_char_0 as char_0)
            table_char_2 = table(`index, 1 as index, [s_char_1] as char_0, [s_char_2] as char_1, [s_char_n] as char_2)
            table_char_3 = table(`index, v_n:0, `index`char_0`char_1`char_2, [INT, CHAR, CHAR, CHAR])
            test_table_char_0 = table(`index, v_n:0, `index`char_0`char_1`char_2, [INT, CHAR, CHAR, CHAR])
            test_table_char_1 = table(`index, v_n:0, `index`char_0, [INT, CHAR])
            test_table_char_2 = table(`index, v_n:0, `index`char_0`char_1`char_2, [INT, CHAR, CHAR, CHAR])
            test_table_char_3 = table(`index, v_n:0, `index`char_0`char_1`char_2, [INT, CHAR, CHAR, CHAR])
        """.replace("table", typeTable),
        "upload": [
            # No way to create a int8 None in numpy
            # ("{}_char_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n+1),
            #                                                   "char_0": np.tile(np.array([19, 65], dtype="int8"), ceil(n/2))[:n],
            #                                                   "char_1": np.tile(np.array([19, 65, None], dtype="int8"), ceil(n/3))[:n],
            #                                                   "char_2": np.tile(np.array([None, None, None], dtype="int8"), ceil(n/3))[:n]
            #                                                   })),
            ("{}_char_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "char_0": np.tile(np.array([19, 65], dtype="int8"),
                                                                                ceil(n / 2))[:n],
                                                              })),
            # ("{}_char_2".format(testTypeTable),  pd.DataFrame({"index": np.arange(1, 2),
            #                                                    "char_0": np.array([19], dtype="int8"),
            #                                                    "char_1": np.array([65], dtype="int8"),
            #                                                    "char_2": np.array([None], dtype="int8")
            #                                                    })),
            ("{}_char_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                              "char_0": np.array([], dtype="int8"),
                                                              "char_1": np.array([], dtype="int8"),
                                                              "char_2": np.array([], dtype="int8")
                                                              })),
        ],
        "download": [
            ("{}_char_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "char_0": np.tile(np.array([19, 65], dtype=np.int8),
                                                                                ceil(n / 2))[:n],
                                                              "char_1": np.tile(np.array([19, 65, np.nan]),
                                                                                ceil(n / 3))[:n],
                                                              "char_2": np.tile(np.array([np.nan, np.nan, np.nan]),
                                                                                ceil(n / 3))[:n]
                                                              })),
            ("{}_char_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "char_0": np.tile(np.array([19, 65], dtype=np.int8),
                                                                                ceil(n / 2))[:n],
                                                              })),
            ("{}_char_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                              "char_0": np.array([19], dtype=np.int8),
                                                              "char_1": np.array([65], dtype=np.int8),
                                                              "char_2": np.array([np.nan])
                                                              })),
            ("{}_char_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                              "char_0": np.array([], dtype=np.int8),
                                                              "char_1": np.array([], dtype=np.int8),
                                                              "char_2": np.array([], dtype=np.int8)
                                                              })),
        ],
    }, DATATYPE.DT_SHORT: {
        "scripts": """
            table_short_0 = table(1..v_n as index, v_short_0 as short_0, v_short_1 as short_1, take([s_short_n, s_short_n, s_short_n], v_n) as short_2)
            table_short_1 = table(1..v_n as index, v_short_0 as short_0)
            table_short_2 = table(1 as index, [s_short_1] as short_0, [s_short_2] as short_1, [s_short_n] as short_2)
            table_short_3 = table(v_n:0, `index`short_0`short_1`short_2, [INT, SHORT, SHORT, SHORT])
            test_table_short_0 = table(v_n:0, `index`short_0`short_1`short_2, [INT, SHORT, SHORT, SHORT])
            test_table_short_1 = table(v_n:0, `index`short_0, [INT, SHORT])
            test_table_short_2 = table(v_n:0, `index`short_0`short_1`short_2, [INT, SHORT, SHORT, SHORT])
            test_table_short_3 = table(v_n:0, `index`short_0`short_1`short_2, [INT, SHORT, SHORT, SHORT])
        """.replace("table", typeTable),
        "keyScripts": """
            table_short_0 = table(`index, 1..v_n as index, v_short_0 as short_0, v_short_1 as short_1, take([s_short_n, s_short_n, s_short_n], v_n) as short_2)
            table_short_1 = table(`index, 1..v_n as index, v_short_0 as short_0)
            table_short_2 = table(`index, 1 as index, [s_short_1] as short_0, [s_short_2] as short_1, [s_short_n] as short_2)
            table_short_3 = table(`index, v_n:0, `index`short_0`short_1`short_2, [INT, SHORT, SHORT, SHORT])
            test_table_short_0 = table(`index, v_n:0, `index`short_0`short_1`short_2, [INT, SHORT, SHORT, SHORT])
            test_table_short_1 = table(`index, v_n:0, `index`short_0, [INT, SHORT])
            test_table_short_2 = table(`index, v_n:0, `index`short_0`short_1`short_2, [INT, SHORT, SHORT, SHORT])
            test_table_short_3 = table(`index, v_n:0, `index`short_0`short_1`short_2, [INT, SHORT, SHORT, SHORT])
        """.replace("table", typeTable),
        "upload": [
            # No way to create a int16 None in numpy
            # ("{}_short_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n+1),
            #                                                    "short_0": np.tile(np.array([1, 0], dtype="int16"), ceil(n/2))[:n],
            #                                                    'short_1': np.tile(np.array([1, 0, None], dtype="int16"), ceil(n/3))[:n],
            #                                                    'short_2': np.tile(np.array([None, None, None], dtype="int16"), ceil(n/3))[:n]
            #                                                    })),
            ("{}_short_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "short_0": np.tile(np.array([1, 0], dtype="int16"),
                                                                                  ceil(n / 2))[:n],
                                                               })),
            # ("{}_short_2".format(testTypeTable),  pd.DataFrame({"index": np.arange(1, 2),
            #                                                     "short_0": np.array([1], dtype="int16"),
            #                                                     'short_1': np.array([0], dtype="int16"),
            #                                                     'short_2': np.array([None], dtype="int16")
            #                                                     })),
            ("{}_short_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                               "short_0": np.array([], dtype="int16"),
                                                               "short_1": np.array([], dtype="int16"),
                                                               "short_2": np.array([], dtype="int16")
                                                               })),
        ],
        "download": [
            ("{}_short_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "short_0": np.tile(np.array([1, 0], dtype=np.int16),
                                                                                  ceil(n / 2))[:n],
                                                               "short_1": np.tile(np.array([1, 0, np.nan]),
                                                                                  ceil(n / 3))[:n],
                                                               "short_2": np.tile(np.array([np.nan, np.nan, np.nan]),
                                                                                  ceil(n / 3))[:n]
                                                               })),
            ("{}_short_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "short_0": np.tile(np.array([1, 0], dtype=np.int16),
                                                                                  ceil(n / 2))[:n],
                                                               })),
            ("{}_short_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                               "short_0": np.array([1], dtype=np.int16),
                                                               "short_1": np.array([0], dtype=np.int16),
                                                               "short_2": np.array([np.nan])
                                                               })),
            ("{}_short_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                               "short_0": np.array([], dtype=np.int16),
                                                               "short_1": np.array([], dtype=np.int16),
                                                               "short_2": np.array([], dtype=np.int16)
                                                               })),
        ],
    }, DATATYPE.DT_INT: {
        "scripts": """
            table_int_0 = table(1..v_n as index, v_int_0 as int_0, v_int_1 as int_1, take([s_int_n, s_int_n, s_int_n], v_n) as int_2)
            table_int_1 = table(1..v_n as index, v_int_0 as int_0)
            table_int_2 = table(1 as index, [s_int_1] as int_0, [s_int_2] as int_1, [s_int_n] as int_2)
            table_int_3 = table(v_n:0, `index`int_0`int_1`int_2, [INT, INT, INT, INT])
            test_table_int_0 = table(v_n:0, `index`int_0`int_1`int_2, [INT, INT, INT, INT])
            test_table_int_1 = table(v_n:0, `index`int_0, [INT, INT])
            test_table_int_2 = table(v_n:0, `index`int_0`int_1`int_2, [INT, INT, INT, INT])
            test_table_int_3 = table(v_n:0, `index`int_0`int_1`int_2, [INT, INT, INT, INT])
        """.replace("table", typeTable),
        "keyScripts": """
            table_int_0 = table(`index, 1..v_n as index, v_int_0 as int_0, v_int_1 as int_1, take([s_int_n, s_int_n, s_int_n], v_n) as int_2)
            table_int_1 = table(`index, 1..v_n as index, v_int_0 as int_0)
            table_int_2 = table(`index, 1 as index, [s_int_1] as int_0, [s_int_2] as int_1, [s_int_n] as int_2)
            table_int_3 = table(`index, v_n:0, `index`int_0`int_1`int_2, [INT, INT, INT, INT])
            test_table_int_0 = table(`index, v_n:0, `index`int_0`int_1`int_2, [INT, INT, INT, INT])
            test_table_int_1 = table(`index, v_n:0, `index`int_0, [INT, INT])
            test_table_int_2 = table(`index, v_n:0, `index`int_0`int_1`int_2, [INT, INT, INT, INT])
            test_table_int_3 = table(`index, v_n:0, `index`int_0`int_1`int_2, [INT, INT, INT, INT])
        """.replace("table", typeTable),
        "upload": [
            # ("{}_int_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n+1),
            #                                                  "int_0": np.tile(np.array([1, 0], dtype="int32"), ceil(n/2))[:n],
            #                                                  "int_1": np.tile(np.array([1, 0, None], dtype="int32"), ceil(n/3))[:n],
            #                                                  "int_2": np.tile(np.array([None, None, None], dtype="int32"), ceil(n/3))[:n]
            #                                                  })),
            ("{}_int_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                             "int_0": np.tile(np.array([1, 0], dtype="int32"),
                                                                              ceil(n / 2))[:n],
                                                             })),
            # ("{}_int_2".format(testTypeTable),  pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
            #                                                   "int_0": np.array([1], dtype="int32"),
            #                                                   "int_1": np.array([0], dtype="int32"),
            #                                                   "int_2": np.array([None], dtype="int32")
            #                                                   })),
            ("{}_int_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                             "int_0": np.array([], dtype="int32"),
                                                             "int_1": np.array([], dtype="int32"),
                                                             "int_2": np.array([], dtype="int32")
                                                             })),
        ],
        "download": [
            ("{}_int_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                             "int_0": np.tile(np.array([1, 0], dtype=np.int32),
                                                                              ceil(n / 2))[:n],
                                                             "int_1": np.tile(np.array([1, 0, np.nan]), ceil(n / 3))[
                                                                      :n],
                                                             "int_2": np.tile(np.array([np.nan, np.nan, np.nan]),
                                                                              ceil(n / 3))[:n]
                                                             })),
            ("{}_int_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                             "int_0": np.tile(np.array([1, 0], dtype=np.int32),
                                                                              ceil(n / 2))[:n],
                                                             })),
            ("{}_int_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                             "int_0": np.array([1], dtype=np.int32),
                                                             "int_1": np.array([0], dtype=np.int32),
                                                             "int_2": np.array([np.nan])
                                                             })),
            ("{}_int_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                             "int_0": np.array([], dtype=np.int32),
                                                             "int_1": np.array([], dtype=np.int32),
                                                             "int_2": np.array([], dtype=np.int32)
                                                             })),
        ],
    }, DATATYPE.DT_LONG: {
        "scripts": """
            table_long_0 = table(1..v_n as index, v_long_0 as long_0, v_long_1 as long_1, take([s_long_n, s_long_n, s_long_n], v_n) as long_2)
            table_long_1 = table(1..v_n as index, v_long_0 as long_0)
            table_long_2 = table(1 as index, [s_long_1] as long_0, [s_long_2] as long_1, [s_long_n] as long_2)
            table_long_3 = table(v_n:0, `index`long_0`long_1`long_2, [INT, LONG, LONG, LONG])
            test_table_long_0 = table(v_n:0, `index`long_0`long_1`long_2, [INT, LONG, LONG, LONG])
            test_table_long_1 = table(v_n:0, `index`long_0, [INT, LONG])
            test_table_long_2 = table(v_n:0, `index`long_0`long_1`long_2, [INT, LONG, LONG, LONG])
            test_table_long_3 = table(v_n:0, `index`long_0`long_1`long_2, [INT, LONG, LONG, LONG])
        """.replace("table", typeTable),
        "keyScripts": """
            table_long_0 = table(`index, 1..v_n as index, v_long_0 as long_0, v_long_1 as long_1, take([s_long_n, s_long_n, s_long_n], v_n) as long_2)
            table_long_1 = table(`index, 1..v_n as index, v_long_0 as long_0)
            table_long_2 = table(`index, 1 as index, [s_long_1] as long_0, [s_long_2] as long_1, [s_long_n] as long_2)
            table_long_3 = table(`index, v_n:0, `index`long_0`long_1`long_2, [INT, LONG, LONG, LONG])
            test_table_long_0 = table(`index, v_n:0, `index`long_0`long_1`long_2, [INT, LONG, LONG, LONG])
            test_table_long_1 = table(`index, v_n:0, `index`long_0, [INT, LONG])
            test_table_long_2 = table(`index, v_n:0, `index`long_0`long_1`long_2, [INT, LONG, LONG, LONG])
            test_table_long_3 = table(`index, v_n:0, `index`long_0`long_1`long_2, [INT, LONG, LONG, LONG])
        """.replace("table", typeTable),
        "upload": [
            # ("{}_long_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n+1),
            #                                                   "long_0": np.tile(np.array([1, 0], dtype="int64"), ceil(n/2))[:n],
            #                                                   "long_1": np.tile(np.array([1, 0, None], dtype="int64"), ceil(n/3))[:n],
            #                                                   "long_2": np.tile(np.array([None, None, None], dtype="int64"), ceil(n/3))[:n]
            #                                                   })),
            ("{}_long_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "long_0": np.tile(np.array([1, 0], dtype="int64"),
                                                                                ceil(n / 2))[:n],
                                                              })),
            # ("{}_long_2".format(testTypeTable),  pd.DataFrame({"index": np.arange(1, 2),
            #                                                    "long_0": np.array([1], dtype="int64"),
            #                                                    "long_1": np.array([0], dtype="int64"),
            #                                                    "long_2": np.array([None], dtype="int64")
            #                                                    })),
            ("{}_long_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                              "long_0": np.array([], dtype="int64"),
                                                              "long_1": np.array([], dtype="int64"),
                                                              "long_2": np.array([], dtype="int64")
                                                              })),
        ],
        "download": [
            ("{}_long_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "long_0": np.tile(np.array([1, 0], dtype=np.int64),
                                                                                ceil(n / 2))[:n],
                                                              "long_1": np.tile(np.array([1, 0, np.nan]), ceil(n / 3))[
                                                                        :n],
                                                              "long_2": np.tile(np.array([np.nan, np.nan, np.nan]),
                                                                                ceil(n / 3))[:n]
                                                              })),
            ("{}_long_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "long_0": np.tile(np.array([1, 0], dtype=np.int64),
                                                                                ceil(n / 2))[:n],
                                                              })),
            ("{}_long_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                              "long_0": np.array([1], dtype=np.int64),
                                                              "long_1": np.array([0], dtype=np.int64),
                                                              "long_2": np.array([np.nan])
                                                              })),
            ("{}_long_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                              "long_0": np.array([], dtype=np.int64),
                                                              "long_1": np.array([], dtype=np.int64),
                                                              "long_2": np.array([], dtype=np.int64)
                                                              })),
        ],
    }, DATATYPE.DT_DATE: {
        "scripts": """
            table_date_0 = table(1..v_n as index, v_date_0 as date_0, v_date_1 as date_1, take([s_date_n, s_date_n, s_date_n], v_n) as date_2)
            table_date_1 = table(1..v_n as index, v_date_0 as date_0)
            table_date_2 = table(1 as index, [s_date_1] as date_0, [s_date_2] as date_1, [s_date_n] as date_2)
            table_date_3 = table(v_n:0, `index`date_0`date_1`date_2, [INT, DATE, DATE, DATE])
            test_table_date_0 = table(v_n:0, `index`date_0`date_1`date_2, [INT, DATE, DATE, DATE])
            test_table_date_1 = table(v_n:0, `index`date_0, [INT, DATE])
            test_table_date_2 = table(v_n:0, `index`date_0`date_1`date_2, [INT, DATE, DATE, DATE])
            test_table_date_3 = table(v_n:0, `index`date_0`date_1`date_2, [INT, DATE, DATE, DATE])
        """.replace("table", typeTable),
        "keyScripts": """
            table_date_0 = table(`index, 1..v_n as index, v_date_0 as date_0, v_date_1 as date_1, take([s_date_n, s_date_n, s_date_n], v_n) as date_2)
            table_date_1 = table(`index, 1..v_n as index, v_date_0 as date_0)
            table_date_2 = table(`index, 1 as index, [s_date_1] as date_0, [s_date_2] as date_1, [s_date_n] as date_2)
            table_date_3 = table(`index, v_n:0, `index`date_0`date_1`date_2, [INT, DATE, DATE, DATE])
            test_table_date_0 = table(`index, v_n:0, `index`date_0`date_1`date_2, [INT, DATE, DATE, DATE])
            test_table_date_1 = table(`index, v_n:0, `index`date_0, [INT, DATE])
            test_table_date_2 = table(`index, v_n:0, `index`date_0`date_1`date_2, [INT, DATE, DATE, DATE])
            test_table_date_3 = table(`index, v_n:0, `index`date_0`date_1`date_2, [INT, DATE, DATE, DATE])
        """.replace("table", typeTable),
        "upload": [
            ("{}_date_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "date_0": np.tile(
                                                                  np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                           dtype="datetime64[D]"), ceil(n / 2))[:n],
                                                              "date_1": np.tile(
                                                                  np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                           dtype="datetime64[D]"), ceil(n / 3))[:n],
                                                              "date_2": np.tile(
                                                                  np.array([None, None, None], dtype="datetime64[D]"),
                                                                  ceil(n / 3))[:n]
                                                              })),
            ("{}_date_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "date_0": np.tile(
                                                                  np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                           dtype="datetime64[D]"), ceil(n / 2))[:n],
                                                              })),
            ("{}_date_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                              "date_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                                 dtype="datetime64[D]"),
                                                              "date_1": np.array([0], dtype="datetime64[D]"),
                                                              "date_2": np.array([None], dtype="datetime64[D]")
                                                              })),
            ("{}_date_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                              "date_0": np.array([], dtype="datetime64[D]"),
                                                              "date_1": np.array([], dtype="datetime64[D]"),
                                                              "date_2": np.array([], dtype="datetime64[D]")
                                                              })),
        ],
        "download": [
            ("{}_date_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "date_0": np.tile(
                                                                  np.array(["2022-01-01", 0], dtype="datetime64[ns]"),
                                                                  ceil(n / 2))[:n],
                                                              "date_1": np.tile(np.array(["2022-01-01", 0, None],
                                                                                         dtype="datetime64[ns]"),
                                                                                ceil(n / 3))[:n],
                                                              "date_2": np.tile(
                                                                  np.array([None, None, None], dtype="datetime64[ns]"),
                                                                  ceil(n / 3))[:n]
                                                              })),
            ("{}_date_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "date_0": np.tile(
                                                                  np.array(["2022-01-01", 0], dtype="datetime64[ns]"),
                                                                  ceil(n / 2))[:n],
                                                              })),
            ("{}_date_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                              "date_0": np.array(["2022-01-01"],
                                                                                 dtype="datetime64[ns]"),
                                                              "date_1": np.array([0], dtype="datetime64[ns]"),
                                                              "date_2": np.array([None], dtype="datetime64[ns]")
                                                              })),
            ("{}_date_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                              "date_0": np.array([], dtype="datetime64[ns]"),
                                                              "date_1": np.array([], dtype="datetime64[ns]"),
                                                              "date_2": np.array([], dtype="datetime64[ns]")
                                                              })),
        ],
    }, DATATYPE.DT_MONTH: {
        "scripts": """
            table_month_0 = table(1..v_n as index, v_month_0 as month_0, v_month_1 as month_1, take([s_month_n, s_month_n, s_month_n], v_n) as month_2)
            table_month_1 = table(1..v_n as index, v_month_0 as month_0,v_month_0 as month_1,v_month_0 as month_2)
            table_month_2 = table(1 as index, [s_month_1] as month_0, [s_month_2] as month_1, [s_month_n] as month_2)
            table_month_3 = table(v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
            test_table_month_0 = table(v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
            test_table_month_1 = table(v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
            test_table_month_2 = table(v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
            test_table_month_3 = table(v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
        """.replace("table", typeTable),
        "keyScripts": """
            table_month_0 = table(`index, 1..v_n as index, v_month_0 as month_0, v_month_1 as month_1, take([s_month_n, s_month_n, s_month_n], v_n) as month_2)
            table_month_1 = table(`index, 1..v_n as index, v_month_0 as month_0,v_month_0 as month_1,v_month_0 as month_2)
            table_month_2 = table(`index, 1 as index, [s_month_1] as month_0, [s_month_2] as month_1, [s_month_n] as month_2)
            table_month_3 = table(`index, v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
            test_table_month_0 = table(`index, v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
            test_table_month_1 = table(`index, v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
            test_table_month_2 = table(`index, v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
            test_table_month_3 = table(`index, v_n:0, `index`month_0`month_1`month_2, [INT, MONTH, MONTH, MONTH])
        """.replace("table", typeTable),
        "upload": [
            ("{}_month_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "month_0": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[M]"), ceil(n / 2))[:n],
                                                               "month_1": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                            dtype="datetime64[M]"), ceil(n / 3))[:n],
                                                               "month_2": np.tile(
                                                                   np.array([None, None, None], dtype="datetime64[M]"),
                                                                   ceil(n / 3))[:n]
                                                               })),
            ("{}_month_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "month_0": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[M]"), ceil(n / 2))[:n],
                                                               "month_1": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[M]"), ceil(n / 2))[:n],
                                                               "month_2": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[M]"), ceil(n / 2))[:n],
                                                               })),
            ("{}_month_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                               "month_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                                   dtype="datetime64[M]"),
                                                               "month_1": np.array([0], dtype="datetime64[M]"),
                                                               "month_2": np.array([None], dtype="datetime64[M]")
                                                               })),
            ("{}_month_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                               "month_0": np.array([], dtype="datetime64[M]"),
                                                               "month_1": np.array([], dtype="datetime64[M]"),
                                                               "month_2": np.array([], dtype="datetime64[M]")
                                                               })),
        ],
        "download": [
            ("{}_month_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "month_0": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[M]"), ceil(n / 2))[:n],
                                                               "month_1": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                            dtype="datetime64[M]"), ceil(n / 3))[:n],
                                                               "month_2": np.tile(
                                                                   np.array([None, None, None], dtype="datetime64[M]"),
                                                                   ceil(n / 3))[:n]
                                                               })),
            ("{}_month_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "month_0": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[M]"), ceil(n / 2))[:n],
                                                               "month_1": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[M]"), ceil(n / 2))[:n],
                                                               "month_2": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[M]"), ceil(n / 2))[:n],
                                                               })),
            ("{}_month_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                               "month_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                                   dtype="datetime64[M]"),
                                                               "month_1": np.array([0], dtype="datetime64[M]"),
                                                               "month_2": np.array([None], dtype="datetime64[M]")
                                                               })),
            ("{}_month_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                               "month_0": np.array([], dtype="datetime64[M]"),
                                                               "month_1": np.array([], dtype="datetime64[M]"),
                                                               "month_2": np.array([], dtype="datetime64[M]")
                                                               })),
        ],
    }, DATATYPE.DT_TIME: {
        "scripts": """
            table_time_0 = table(1..v_n as index, v_time_0 as time_0, v_time_1 as time_1, take([s_time_n, s_time_n, s_time_n], v_n) as time_2)
            table_time_1 = table(1..v_n as index, v_time_0 as time_0)
            table_time_2 = table(1 as index, [s_time_1] as time_0, [s_time_2] as time_1, [s_time_n] as time_2)
            table_time_3 = table(v_n:0, `index`time_0`time_1`time_2, [INT, TIME, TIME, TIME])
            test_table_time_0 = table(v_n:0, `index`time_0`time_1`time_2, [INT, TIME, TIME, TIME])
            test_table_time_1 = table(v_n:0, `index`time_0, [INT, TIME])
            test_table_time_2 = table(v_n:0, `index`time_0`time_1`time_2, [INT, TIME, TIME, TIME])
            test_table_time_3 = table(v_n:0, `index`time_0`time_1`time_2, [INT, TIME, TIME, TIME])
        """.replace("table", typeTable),
        "keyScripts": """
            table_time_0 = table(`index, 1..v_n as index, v_time_0 as time_0, v_time_1 as time_1, take([s_time_n, s_time_n, s_time_n], v_n) as time_2)
            table_time_1 = table(`index, 1..v_n as index, v_time_0 as time_0)
            table_time_2 = table(`index, 1 as index, [s_time_1] as time_0, [s_time_2] as time_1, [s_time_n] as time_2)
            table_time_3 = table(`index, v_n:0, `index`time_0`time_1`time_2, [INT, TIME, TIME, TIME])
            test_table_time_0 = table(`index, v_n:0, `index`time_0`time_1`time_2, [INT, TIME, TIME, TIME])
            test_table_time_1 = table(`index, v_n:0, `index`time_0, [INT, TIME])
            test_table_time_2 = table(`index, v_n:0, `index`time_0`time_1`time_2, [INT, TIME, TIME, TIME])
            test_table_time_3 = table(`index, v_n:0, `index`time_0`time_1`time_2, [INT, TIME, TIME, TIME])
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_TIME Table
        ],
        "download": [
            ("{}_time_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "time_0": np.tile(np.array(["1970-01-01 12:30:56.123", 0],
                                                                                         dtype="datetime64[ns]"),
                                                                                ceil(n / 2))[:n],
                                                              "time_1": np.tile(
                                                                  np.array(["1970-01-01 12:30:56.123", 0, None],
                                                                           dtype="datetime64[ns]"), ceil(n / 3))[:n],
                                                              "time_2": np.tile(
                                                                  np.array([None, None, None], dtype="datetime64[ns]"),
                                                                  ceil(n / 3))[:n]
                                                              })),
            ("{}_time_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "time_0": np.tile(np.array(["1970-01-01 12:30:56.123", 0],
                                                                                         dtype="datetime64[ns]"),
                                                                                ceil(n / 2))[:n],
                                                              })),
            ("{}_time_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                              "time_0": np.array(["1970-01-01 12:30:56.123"],
                                                                                 dtype="datetime64[ns]"),
                                                              "time_1": np.array([0], dtype="datetime64[ns]"),
                                                              "time_2": np.array([None], dtype="datetime64[ns]")
                                                              })),
            ("{}_time_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                              "time_0": np.array([], dtype="datetime64[ns]"),
                                                              "time_1": np.array([], dtype="datetime64[ns]"),
                                                              "time_2": np.array([], dtype="datetime64[ns]")
                                                              })),
        ],
    }, DATATYPE.DT_MINUTE: {
        "scripts": """
            table_minute_0 = table(1..v_n as index, v_minute_0 as minute_0, v_minute_1 as minute_1, take([s_minute_n, s_minute_n, s_minute_n], v_n) as minute_2)
            table_minute_1 = table(1..v_n as index, v_minute_0 as minute_0)
            table_minute_2 = table(1 as index, [s_minute_1] as minute_0, [s_minute_2] as minute_1, [s_minute_n] as minute_2)
            table_minute_3 = table(v_n:0, `index`minute_0`minute_1`minute_2, [INT, MINUTE, MINUTE, MINUTE])
            test_table_minute_0 = table(v_n:0, `index`minute_0`minute_1`minute_2, [INT, MINUTE, MINUTE, MINUTE])
            test_table_minute_1 = table(v_n:0, `index`minute_0, [INT, MINUTE])
            test_table_minute_2 = table(v_n:0, `index`minute_0`minute_1`minute_2, [INT, MINUTE, MINUTE, MINUTE])
            test_table_minute_3 = table(v_n:0, `index`minute_0`minute_1`minute_2, [INT, MINUTE, MINUTE, MINUTE])
        """.replace("table", typeTable),
        "keyScripts": """
            table_minute_0 = table(`index, 1..v_n as index, v_minute_0 as minute_0, v_minute_1 as minute_1, take([s_minute_n, s_minute_n, s_minute_n], v_n) as minute_2)
            table_minute_1 = table(`index, 1..v_n as index, v_minute_0 as minute_0)
            table_minute_2 = table(`index, 1 as index, [s_minute_1] as minute_0, [s_minute_2] as minute_1, [s_minute_n] as minute_2)
            table_minute_3 = table(`index, v_n:0, `index`minute_0`minute_1`minute_2, [INT, MINUTE, MINUTE, MINUTE])
            test_table_minute_0 = table(`index, v_n:0, `index`minute_0`minute_1`minute_2, [INT, MINUTE, MINUTE, MINUTE])
            test_table_minute_1 = table(`index, v_n:0, `index`minute_0, [INT, MINUTE])
            test_table_minute_2 = table(`index, v_n:0, `index`minute_0`minute_1`minute_2, [INT, MINUTE, MINUTE, MINUTE])
            test_table_minute_3 = table(`index, v_n:0, `index`minute_0`minute_1`minute_2, [INT, MINUTE, MINUTE, MINUTE])
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_MINUTE Table
        ],
        "download": [
            ("{}_minute_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "minute_0": np.tile(np.array(["1970-01-01 12:30", 0],
                                                                                             dtype="datetime64[ns]"),
                                                                                    ceil(n / 2))[:n],
                                                                "minute_1": np.tile(
                                                                    np.array(["1970-01-01 12:30", 0, None],
                                                                             dtype="datetime64[ns]"), ceil(n / 3))[:n],
                                                                "minute_2": np.tile(np.array([None, None, None],
                                                                                             dtype="datetime64[ns]"),
                                                                                    ceil(n / 3))[:n]
                                                                })),
            ("{}_minute_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "minute_0": np.tile(np.array(["1970-01-01 12:30", 0],
                                                                                             dtype="datetime64[ns]"),
                                                                                    ceil(n / 2))[:n],
                                                                })),
            ("{}_minute_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                "minute_0": np.array(["1970-01-01 12:30"],
                                                                                     dtype="datetime64[ns]"),
                                                                "minute_1": np.array([0], dtype="datetime64[ns]"),
                                                                "minute_2": np.array([None], dtype="datetime64[ns]")
                                                                })),
            ("{}_minute_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                "minute_0": np.array([], dtype="datetime64[ns]"),
                                                                "minute_1": np.array([], dtype="datetime64[ns]"),
                                                                "minute_2": np.array([], dtype="datetime64[ns]")
                                                                })),
        ],
    }, DATATYPE.DT_SECOND: {
        "scripts": """
            table_second_0 = table(1..v_n as index, v_second_0 as second_0, v_second_1 as second_1, take([s_second_n, s_second_n, s_second_n], v_n) as second_2)
            table_second_1 = table(1..v_n as index, v_second_0 as second_0)
            table_second_2 = table(1 as index, [s_second_1] as second_0, [s_second_2] as second_1, [s_second_n] as second_2)
            table_second_3 = table(v_n:0, `index`second_0`second_1`second_2, [INT, SECOND, SECOND, SECOND])
            test_table_second_0 = table(v_n:0, `index`second_0`second_1`second_2, [INT, SECOND, SECOND, SECOND])
            test_table_second_1 = table(v_n:0, `index`second_0, [INT, SECOND])
            test_table_second_2 = table(v_n:0, `index`second_0`second_1`second_2, [INT, SECOND, SECOND, SECOND])
            test_table_second_3 = table(v_n:0, `index`second_0`second_1`second_2, [INT, SECOND, SECOND, SECOND])
        """.replace("table", typeTable),
        "keyScripts": """
            table_second_0 = table(`index, 1..v_n as index, v_second_0 as second_0, v_second_1 as second_1, take([s_second_n, s_second_n, s_second_n], v_n) as second_2)
            table_second_1 = table(`index, 1..v_n as index, v_second_0 as second_0)
            table_second_2 = table(`index, 1 as index, [s_second_1] as second_0, [s_second_2] as second_1, [s_second_n] as second_2)
            table_second_3 = table(`index, v_n:0, `index`second_0`second_1`second_2, [INT, SECOND, SECOND, SECOND])
            test_table_second_0 = table(`index, v_n:0, `index`second_0`second_1`second_2, [INT, SECOND, SECOND, SECOND])
            test_table_second_1 = table(`index, v_n:0, `index`second_0, [INT, SECOND])
            test_table_second_2 = table(`index, v_n:0, `index`second_0`second_1`second_2, [INT, SECOND, SECOND, SECOND])
            test_table_second_3 = table(`index, v_n:0, `index`second_0`second_1`second_2, [INT, SECOND, SECOND, SECOND])
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_SECOND Table
        ],
        "download": [
            ("{}_second_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "second_0": np.tile(np.array(["1970-01-01 12:30:56", 0],
                                                                                             dtype="datetime64[ns]"),
                                                                                    ceil(n / 2))[:n],
                                                                "second_1": np.tile(
                                                                    np.array(["1970-01-01 12:30:56", 0, None],
                                                                             dtype="datetime64[ns]"), ceil(n / 3))[:n],
                                                                "second_2": np.tile(np.array([None, None, None],
                                                                                             dtype="datetime64[ns]"),
                                                                                    ceil(n / 3))[:n]
                                                                })),
            ("{}_second_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "second_0": np.tile(np.array(["1970-01-01 12:30:56", 0],
                                                                                             dtype="datetime64[ns]"),
                                                                                    ceil(n / 2))[:n],
                                                                })),
            ("{}_second_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                "second_0": np.array(["1970-01-01 12:30:56"],
                                                                                     dtype="datetime64[ns]"),
                                                                "second_1": np.array([0], dtype="datetime64[ns]"),
                                                                "second_2": np.array([None], dtype="datetime64[ns]")
                                                                })),
            ("{}_second_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                "second_0": np.array([], dtype="datetime64[ns]"),
                                                                "second_1": np.array([], dtype="datetime64[ns]"),
                                                                "second_2": np.array([], dtype="datetime64[ns]")
                                                                })),
        ],
    }, DATATYPE.DT_DATETIME: {
        "scripts": """
            table_datetime_0 = table(1..v_n as index, v_datetime_0 as datetime_0, v_datetime_1 as datetime_1, take([s_datetime_n, s_datetime_n, s_datetime_n], v_n) as datetime_2)
            table_datetime_1 = table(1..v_n as index, v_datetime_0 as datetime_0)
            table_datetime_2 = table(1 as index, [s_datetime_1] as datetime_0, [s_datetime_2] as datetime_1, [s_datetime_n] as datetime_2)
            table_datetime_3 = table(v_n:0, `index`datetime_0`datetime_1`datetime_2, [INT, DATETIME, DATETIME, DATETIME])
            test_table_datetime_0 = table(v_n:0, `index`datetime_0`datetime_1`datetime_2, [INT, datetime, datetime, datetime])
            test_table_datetime_1 = table(v_n:0, `index`datetime_0, [INT, datetime])
            test_table_datetime_2 = table(v_n:0, `index`datetime_0`datetime_1`datetime_2, [INT, datetime, datetime, datetime])
            test_table_datetime_3 = table(v_n:0, `index`datetime_0`datetime_1`datetime_2, [INT, datetime, datetime, datetime])
        """.replace("table", typeTable),
        "keyScripts": """
            table_datetime_0 = table(`index, 1..v_n as index, v_datetime_0 as datetime_0, v_datetime_1 as datetime_1, take([s_datetime_n, s_datetime_n, s_datetime_n], v_n) as datetime_2)
            table_datetime_1 = table(`index, 1..v_n as index, v_datetime_0 as datetime_0)
            table_datetime_2 = table(`index, 1 as index, [s_datetime_1] as datetime_0, [s_datetime_2] as datetime_1, [s_datetime_n] as datetime_2)
            table_datetime_3 = table(`index, v_n:0, `index`datetime_0`datetime_1`datetime_2, [INT, DATETIME, DATETIME, DATETIME])
            test_table_datetime_0 = table(`index, v_n:0, `index`datetime_0`datetime_1`datetime_2, [INT, DATETIME, DATETIME, DATETIME])
            test_table_datetime_1 = table(`index, v_n:0, `index`datetime_0, [INT, DATETIME])
            test_table_datetime_2 = table(`index, v_n:0, `index`datetime_0`datetime_1`datetime_2, [INT, DATETIME, DATETIME, DATETIME])
            test_table_datetime_3 = table(`index, v_n:0, `index`datetime_0`datetime_1`datetime_2, [INT, DATETIME, DATETIME, DATETIME])
        """.replace("table", typeTable),
        "upload": [
            ("{}_datetime_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "datetime_0": np.tile(
                                                                      np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                               dtype="datetime64[s]"), ceil(n / 2))[:n],
                                                                  "datetime_1": np.tile(np.array(
                                                                      ["2022-01-01 12:30:56.123456789", 0, None],
                                                                      dtype="datetime64[s]"), ceil(n / 3))[:n],
                                                                  "datetime_2": np.tile(np.array([None, None, None],
                                                                                                 dtype="datetime64[s]"),
                                                                                        ceil(n / 3))[:n]
                                                                  })),
            ("{}_datetime_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "datetime_0": np.tile(
                                                                      np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                               dtype="datetime64[s]"), ceil(n / 2))[:n],
                                                                  })),
            ("{}_datetime_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                  "datetime_0": np.array(
                                                                      ["2022-01-01 12:30:56.123456789"],
                                                                      dtype="datetime64[s]"),
                                                                  "datetime_1": np.array([0], dtype="datetime64[s]"),
                                                                  "datetime_2": np.array([None], dtype="datetime64[s]")
                                                                  })),
            ("{}_datetime_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                  "datetime_0": np.array([], dtype="datetime64[s]"),
                                                                  "datetime_1": np.array([], dtype="datetime64[s]"),
                                                                  "datetime_2": np.array([], dtype="datetime64[s]")
                                                                  })),
        ],
        "download": [
            ("{}_datetime_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "datetime_0": np.tile(
                                                                      np.array(["2022-01-01 12:30:56", 0],
                                                                               dtype="datetime64[ns]"), ceil(n / 2))[
                                                                                :n],
                                                                  "datetime_1": np.tile(
                                                                      np.array(["2022-01-01 12:30:56", 0, None],
                                                                               dtype="datetime64[ns]"), ceil(n / 3))[
                                                                                :n],
                                                                  "datetime_2": np.tile(np.array([None, None, None],
                                                                                                 dtype="datetime64[ns]"),
                                                                                        ceil(n / 3))[:n]
                                                                  })),
            ("{}_datetime_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "datetime_0": np.tile(
                                                                      np.array(["2022-01-01 12:30:56", 0],
                                                                               dtype="datetime64[ns]"), ceil(n / 2))[
                                                                                :n],
                                                                  })),
            ("{}_datetime_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                  "datetime_0": np.array(["2022-01-01 12:30:56"],
                                                                                         dtype="datetime64[ns]"),
                                                                  "datetime_1": np.array([0], dtype="datetime64[ns]"),
                                                                  "datetime_2": np.array([None], dtype="datetime64[ns]")
                                                                  })),
            ("{}_datetime_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                  "datetime_0": np.array([], dtype="datetime64[ns]"),
                                                                  "datetime_1": np.array([], dtype="datetime64[ns]"),
                                                                  "datetime_2": np.array([], dtype="datetime64[ns]")
                                                                  })),
        ],
    }, DATATYPE.DT_TIMESTAMP: {
        "scripts": """
            table_timestamp_0 = table(1..v_n as index, v_timestamp_0 as timestamp_0, v_timestamp_1 as timestamp_1, take([s_timestamp_n, s_timestamp_n, s_timestamp_n], v_n) as timestamp_2)
            table_timestamp_1 = table(1..v_n as index, v_timestamp_0 as timestamp_0)
            table_timestamp_2 = table(1 as index, [s_timestamp_1] as timestamp_0, [s_timestamp_2] as timestamp_1, [s_timestamp_n] as timestamp_2)
            table_timestamp_3 = table(v_n:0, `index`timestamp_0`timestamp_1`timestamp_2, [INT, TIMESTAMP, TIMESTAMP, TIMESTAMP])
            test_table_timestamp_0 = table(v_n:0, `index`timestamp_0`timestamp_1`timestamp_2, [INT, TIMESTAMP, TIMESTAMP, TIMESTAMP])
            test_table_timestamp_1 = table(v_n:0, `index`timestamp_0, [INT, TIMESTAMP])
            test_table_timestamp_2 = table(v_n:0, `index`timestamp_0`timestamp_1`timestamp_2, [INT, TIMESTAMP, TIMESTAMP, TIMESTAMP])
            test_table_timestamp_3 = table(v_n:0, `index`timestamp_0`timestamp_1`timestamp_2, [INT, TIMESTAMP, TIMESTAMP, TIMESTAMP])
        """.replace("table", typeTable),
        "keyScripts": """
            table_timestamp_0 = table(`index, 1..v_n as index, v_timestamp_0 as timestamp_0, v_timestamp_1 as timestamp_1, take([s_timestamp_n, s_timestamp_n, s_timestamp_n], v_n) as timestamp_2)
            table_timestamp_1 = table(`index, 1..v_n as index, v_timestamp_0 as timestamp_0)
            table_timestamp_2 = table(`index, 1 as index, [s_timestamp_1] as timestamp_0, [s_timestamp_2] as timestamp_1, [s_timestamp_n] as timestamp_2)
            table_timestamp_3 = table(`index, v_n:0, `index`timestamp_0`timestamp_1`timestamp_2, [INT, TIMESTAMP, TIMESTAMP, TIMESTAMP])
            test_table_timestamp_0 = table(`index, v_n:0, `index`timestamp_0`timestamp_1`timestamp_2, [INT, TIMESTAMP, TIMESTAMP, TIMESTAMP])
            test_table_timestamp_1 = table(`index, v_n:0, `index`timestamp_0, [INT, TIMESTAMP])
            test_table_timestamp_2 = table(`index, v_n:0, `index`timestamp_0`timestamp_1`timestamp_2, [INT, TIMESTAMP, TIMESTAMP, TIMESTAMP])
            test_table_timestamp_3 = table(`index, v_n:0, `index`timestamp_0`timestamp_1`timestamp_2, [INT, TIMESTAMP, TIMESTAMP, TIMESTAMP])
        """.replace("table", typeTable),
        "upload": [
            ("{}_timestamp_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                   "timestamp_0": np.tile(
                                                                       np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                dtype="datetime64[ms]"), ceil(n / 2))[
                                                                                  :n],
                                                                   "timestamp_1": np.tile(np.array(
                                                                       ["2022-01-01 12:30:56.123456789", 0, None],
                                                                       dtype="datetime64[ms]"), ceil(n / 3))[:n],
                                                                   "timestamp_2": np.tile(np.array([None, None, None],
                                                                                                   dtype="datetime64[ms]"),
                                                                                          ceil(n / 3))[:n]
                                                                   })),
            ("{}_timestamp_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                   "timestamp_0": np.tile(
                                                                       np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                dtype="datetime64[ms]"), ceil(n / 2))[
                                                                                  :n],
                                                                   })),
            ("{}_timestamp_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                   "timestamp_0": np.array(
                                                                       ["2022-01-01 12:30:56.123456789"],
                                                                       dtype="datetime64[ms]"),
                                                                   "timestamp_1": np.array([0], dtype="datetime64[ms]"),
                                                                   "timestamp_2": np.array([None],
                                                                                           dtype="datetime64[ms]")
                                                                   })),
            ("{}_timestamp_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                   "timestamp_0": np.array([], dtype="datetime64[ms]"),
                                                                   "timestamp_1": np.array([], dtype="datetime64[ms]"),
                                                                   "timestamp_2": np.array([], dtype="datetime64[ms]")
                                                                   })),
        ],
        "download": [
            ("{}_timestamp_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                   "timestamp_0": np.tile(
                                                                       np.array(["2022-01-01 12:30:56.123", 0],
                                                                                dtype="datetime64[ns]"), ceil(n / 2))[
                                                                                  :n],
                                                                   "timestamp_1": np.tile(
                                                                       np.array(["2022-01-01 12:30:56.123", 0, None],
                                                                                dtype="datetime64[ns]"), ceil(n / 3))[
                                                                                  :n],
                                                                   "timestamp_2": np.tile(np.array([None, None, None],
                                                                                                   dtype="datetime64[ns]"),
                                                                                          ceil(n / 3))[:n]
                                                                   })),
            ("{}_timestamp_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                   "timestamp_0": np.tile(
                                                                       np.array(["2022-01-01 12:30:56.123", 0],
                                                                                dtype="datetime64[ns]"), ceil(n / 2))[
                                                                                  :n],
                                                                   })),
            ("{}_timestamp_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                   "timestamp_0": np.array(["2022-01-01 12:30:56.123"],
                                                                                           dtype="datetime64[ns]"),
                                                                   "timestamp_1": np.array([0], dtype="datetime64[ns]"),
                                                                   "timestamp_2": np.array([None],
                                                                                           dtype="datetime64[ns]")
                                                                   })),
            ("{}_timestamp_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                   "timestamp_0": np.array([], dtype="datetime64[ns]"),
                                                                   "timestamp_1": np.array([], dtype="datetime64[ns]"),
                                                                   "timestamp_2": np.array([], dtype="datetime64[ns]")
                                                                   })),
        ],
    }, DATATYPE.DT_NANOTIME: {
        "scripts": """
            table_nanotime_0 = table(1..v_n as index, v_nanotime_0 as nanotime_0, v_nanotime_1 as nanotime_1, take([s_nanotime_n, s_nanotime_n, s_nanotime_n], v_n) as nanotime_2)
            table_nanotime_1 = table(1..v_n as index, v_nanotime_0 as nanotime_0)
            table_nanotime_2 = table(1 as index, [s_nanotime_1] as nanotime_0, [s_nanotime_2] as nanotime_1, [s_nanotime_n] as nanotime_2)
            table_nanotime_3 = table(v_n:0, `index`nanotime_0`nanotime_1`nanotime_2, [INT, NANOTIME, NANOTIME, NANOTIME])
            test_table_nanotime_0 = table(v_n:0, `index`nanotime_0`nanotime_1`nanotime_2, [INT, NANOTIME, NANOTIME, NANOTIME])
            test_table_nanotime_1 = table(v_n:0, `index`nanotime_0, [INT, NANOTIME])
            test_table_nanotime_2 = table(v_n:0, `index`nanotime_0`nanotime_1`nanotime_2, [INT, NANOTIME, NANOTIME, NANOTIME])
            test_table_nanotime_3 = table(v_n:0, `index`nanotime_0`nanotime_1`nanotime_2, [INT, NANOTIME, NANOTIME, NANOTIME])
        """.replace("table", typeTable),
        "keyScripts": """
            table_nanotime_0 = table(`index, 1..v_n as index, v_nanotime_0 as nanotime_0, v_nanotime_1 as nanotime_1, take([s_nanotime_n, s_nanotime_n, s_nanotime_n], v_n) as nanotime_2)
            table_nanotime_1 = table(`index, 1..v_n as index, v_nanotime_0 as nanotime_0)
            table_nanotime_2 = table(`index, 1 as index, [s_nanotime_1] as nanotime_0, [s_nanotime_2] as nanotime_1, [s_nanotime_n] as nanotime_2)
            table_nanotime_3 = table(`index, v_n:0, `index`nanotime_0`nanotime_1`nanotime_2, [INT, NANOTIME, NANOTIME, NANOTIME])
            test_table_nanotime_0 = table(`index, v_n:0, `index`nanotime_0`nanotime_1`nanotime_2, [INT, NANOTIME, NANOTIME, NANOTIME])
            test_table_nanotime_1 = table(`index, v_n:0, `index`nanotime_0, [INT, NANOTIME])
            test_table_nanotime_2 = table(`index, v_n:0, `index`nanotime_0`nanotime_1`nanotime_2, [INT, NANOTIME, NANOTIME, NANOTIME])
            test_table_nanotime_3 = table(`index, v_n:0, `index`nanotime_0`nanotime_1`nanotime_2, [INT, NANOTIME, NANOTIME, NANOTIME])
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_NANOTIME Table
        ],
        "download": [
            ("{}_nanotime_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "nanotime_0": np.tile(
                                                                      np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                               dtype="datetime64[ns]"), ceil(n / 2))[
                                                                                :n],
                                                                  "nanotime_1": np.tile(np.array(
                                                                      ["1970-01-01 12:30:56.123456789", 0, None],
                                                                      dtype="datetime64[ns]"), ceil(n / 3))[:n],
                                                                  "nanotime_2": np.tile(np.array([None, None, None],
                                                                                                 dtype="datetime64[ns]"),
                                                                                        ceil(n / 3))[:n]
                                                                  })),
            ("{}_nanotime_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "nanotime_0": np.tile(
                                                                      np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                               dtype="datetime64[ns]"), ceil(n / 2))[
                                                                                :n],
                                                                  })),
            ("{}_nanotime_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                  "nanotime_0": np.array(
                                                                      ["1970-01-01 12:30:56.123456789"],
                                                                      dtype="datetime64[ns]"),
                                                                  "nanotime_1": np.array([0], dtype="datetime64[ns]"),
                                                                  "nanotime_2": np.array([None], dtype="datetime64[ns]")
                                                                  })),
            ("{}_nanotime_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                  "nanotime_0": np.array([], dtype="datetime64[ns]"),
                                                                  "nanotime_1": np.array([], dtype="datetime64[ns]"),
                                                                  "nanotime_2": np.array([], dtype="datetime64[ns]")
                                                                  })),
        ],
    }, DATATYPE.DT_NANOTIMESTAMP: {
        "scripts": """
            table_nanotimestamp_0 = table(1..v_n as index, v_nanotimestamp_0 as nanotimestamp_0, v_nanotimestamp_1 as nanotimestamp_1, take([s_nanotimestamp_n, s_nanotimestamp_n, s_nanotimestamp_n], v_n) as nanotimestamp_2)
            table_nanotimestamp_1 = table(1..v_n as index, v_nanotimestamp_0 as nanotimestamp_0)
            table_nanotimestamp_2 = table(1 as index, [s_nanotimestamp_1] as nanotimestamp_0, [s_nanotimestamp_2] as nanotimestamp_1, [s_nanotimestamp_n] as nanotimestamp_2)
            table_nanotimestamp_3 = table(v_n:0, `index`nanotimestamp_0`nanotimestamp_1`nanotimestamp_2, [INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])
            test_table_nanotimestamp_0 = table(v_n:0, `index`nanotimestamp_0`nanotimestamp_1`nanotimestamp_2, [INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])
            test_table_nanotimestamp_1 = table(v_n:0, `index`nanotimestamp_0, [INT, NANOTIMESTAMP])
            test_table_nanotimestamp_2 = table(v_n:0, `index`nanotimestamp_0`nanotimestamp_1`nanotimestamp_2, [INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])
            test_table_nanotimestamp_3 = table(v_n:0, `index`nanotimestamp_0`nanotimestamp_1`nanotimestamp_2, [INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])
        """.replace("table", typeTable),
        "keyScripts": """
            table_nanotimestamp_0 = table(`index, 1..v_n as index, v_nanotimestamp_0 as nanotimestamp_0, v_nanotimestamp_1 as nanotimestamp_1, take([s_nanotimestamp_n, s_nanotimestamp_n, s_nanotimestamp_n], v_n) as nanotimestamp_2)
            table_nanotimestamp_1 = table(`index, 1..v_n as index, v_nanotimestamp_0 as nanotimestamp_0)
            table_nanotimestamp_2 = table(`index, 1 as index, [s_nanotimestamp_1] as nanotimestamp_0, [s_nanotimestamp_2] as nanotimestamp_1, [s_nanotimestamp_n] as nanotimestamp_2)
            table_nanotimestamp_3 = table(`index, v_n:0, `index`nanotimestamp_0`nanotimestamp_1`nanotimestamp_2, [INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])
            test_table_nanotimestamp_0 = table(`index, v_n:0, `index`nanotimestamp_0`nanotimestamp_1`nanotimestamp_2, [INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])
            test_table_nanotimestamp_1 = table(`index, v_n:0, `index`nanotimestamp_0, [INT, NANOTIMESTAMP])
            test_table_nanotimestamp_2 = table(`index, v_n:0, `index`nanotimestamp_0`nanotimestamp_1`nanotimestamp_2, [INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])
            test_table_nanotimestamp_3 = table(`index, v_n:0, `index`nanotimestamp_0`nanotimestamp_1`nanotimestamp_2, [INT, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP])
        """.replace("table", typeTable),
        "upload": [
            ("{}_nanotimestamp_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                       "nanotimestamp_0": np.tile(np.array(
                                                                           ["2022-01-01 12:30:56.123456789", 0],
                                                                           dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                                       "nanotimestamp_1": np.tile(np.array(
                                                                           ["2022-01-01 12:30:56.123456789", 0, None],
                                                                           dtype="datetime64[ns]"), ceil(n / 3))[:n],
                                                                       "nanotimestamp_2": np.tile(
                                                                           np.array([None, None, None],
                                                                                    dtype="datetime64[ns]"),
                                                                           ceil(n / 3))[:n]
                                                                       })),
            ("{}_nanotimestamp_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                       "nanotimestamp_0": np.tile(np.array(
                                                                           ["2022-01-01 12:30:56.123456789", 0],
                                                                           dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                                       })),
            ("{}_nanotimestamp_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                       "nanotimestamp_0": np.array(
                                                                           ["2022-01-01 12:30:56.123456789"],
                                                                           dtype="datetime64[ns]"),
                                                                       "nanotimestamp_1": np.array([0],
                                                                                                   dtype="datetime64[ns]"),
                                                                       "nanotimestamp_2": np.array([None],
                                                                                                   dtype="datetime64[ns]")
                                                                       })),
            ("{}_nanotimestamp_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                       "nanotimestamp_0": np.array([],
                                                                                                   dtype="datetime64[ns]"),
                                                                       "nanotimestamp_1": np.array([],
                                                                                                   dtype="datetime64[ns]"),
                                                                       "nanotimestamp_2": np.array([],
                                                                                                   dtype="datetime64[ns]")
                                                                       })),
        ],
        "download": [
            ("{}_nanotimestamp_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                       "nanotimestamp_0": np.tile(np.array(
                                                                           ["2022-01-01 12:30:56.123456789", 0],
                                                                           dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                                       "nanotimestamp_1": np.tile(np.array(
                                                                           ["2022-01-01 12:30:56.123456789", 0, None],
                                                                           dtype="datetime64[ns]"), ceil(n / 3))[:n],
                                                                       "nanotimestamp_2": np.tile(
                                                                           np.array([None, None, None],
                                                                                    dtype="datetime64[ns]"),
                                                                           ceil(n / 3))[:n]
                                                                       })),
            ("{}_nanotimestamp_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                       "nanotimestamp_0": np.tile(np.array(
                                                                           ["2022-01-01 12:30:56.123456789", 0],
                                                                           dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                                       })),
            ("{}_nanotimestamp_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                       "nanotimestamp_0": np.array(
                                                                           ["2022-01-01 12:30:56.123456789"],
                                                                           dtype="datetime64[ns]"),
                                                                       "nanotimestamp_1": np.array([0],
                                                                                                   dtype="datetime64[ns]"),
                                                                       "nanotimestamp_2": np.array([None],
                                                                                                   dtype="datetime64[ns]")
                                                                       })),
            ("{}_nanotimestamp_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                       "nanotimestamp_0": np.array([],
                                                                                                   dtype="datetime64[ns]"),
                                                                       "nanotimestamp_1": np.array([],
                                                                                                   dtype="datetime64[ns]"),
                                                                       "nanotimestamp_2": np.array([],
                                                                                                   dtype="datetime64[ns]")
                                                                       })),
        ],
    }, DATATYPE.DT_FLOAT: {
        "scripts": """
            table_float_0 = table(1..v_n as index, v_float_0 as float_0, v_float_1 as float_1, take([s_float_n, s_float_n, s_float_n], v_n) as float_2)
            table_float_1 = table(1..v_n as index, v_float_0 as float_0)
            table_float_2 = table(1 as index, [s_float_1] as float_0, [s_float_2] as float_1, [s_float_n] as float_2)
            table_float_3 = table(v_n:0, `index`float_0`float_1`float_2, [INT, FLOAT, FLOAT, FLOAT])
            test_table_float_0 = table(v_n:0, `index`float_0`float_1`float_2, [INT, FLOAT, FLOAT, FLOAT])
            test_table_float_1 = table(v_n:0, `index`float_0, [INT, FLOAT])
            test_table_float_2 = table(v_n:0, `index`float_0`float_1`float_2, [INT, FLOAT, FLOAT, FLOAT])
            test_table_float_3 = table(v_n:0, `index`float_0`float_1`float_2, [INT, FLOAT, FLOAT, FLOAT])
        """.replace("table", typeTable),
        "keyScripts": """
            table_float_0 = table(`index, 1..v_n as index, v_float_0 as float_0, v_float_1 as float_1, take([s_float_n, s_float_n, s_float_n], v_n) as float_2)
            table_float_1 = table(`index, 1..v_n as index, v_float_0 as float_0)
            table_float_2 = table(`index, 1 as index, [s_float_1] as float_0, [s_float_2] as float_1, [s_float_n] as float_2)
            table_float_3 = table(`index, v_n:0, `index`float_0`float_1`float_2, [INT, FLOAT, FLOAT, FLOAT])
            test_table_float_0 = table(`index, v_n:0, `index`float_0`float_1`float_2, [INT, FLOAT, FLOAT, FLOAT])
            test_table_float_1 = table(`index, v_n:0, `index`float_0, [INT, FLOAT])
            test_table_float_2 = table(`index, v_n:0, `index`float_0`float_1`float_2, [INT, FLOAT, FLOAT, FLOAT])
            test_table_float_3 = table(`index, v_n:0, `index`float_0`float_1`float_2, [INT, FLOAT, FLOAT, FLOAT])
        """.replace("table", typeTable),
        "upload": [
            ("{}_float_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "float_0": np.tile(
                                                                   np.array([1.11, 0.0], dtype="float32"), ceil(n / 2))[
                                                                          :n],
                                                               "float_1": np.tile(
                                                                   np.array([1.11, 0.0, None], dtype="float32"),
                                                                   ceil(n / 3))[:n],
                                                               "float_2": np.tile(
                                                                   np.array([None, None, None], dtype="float32"),
                                                                   ceil(n / 3))[:n]
                                                               })),
            ("{}_float_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "float_0": np.tile(
                                                                   np.array([1.11, 0.0], dtype="float32"), ceil(n / 2))[
                                                                          :n],
                                                               })),
            ("{}_float_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                               "float_0": np.array([1.11], dtype="float32"),
                                                               "float_1": np.array([0.0], dtype="float32"),
                                                               "float_2": np.array([None], dtype="float32")
                                                               })),
            ("{}_float_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                               "float_0": np.array([], dtype="float32"),
                                                               "float_1": np.array([], dtype="float32"),
                                                               "float_2": np.array([], dtype="float32")
                                                               })),
        ],
        "download": [
            ("{}_float_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "float_0": np.tile(
                                                                   np.array([1.11, 0.0], dtype="float32"), ceil(n / 2))[
                                                                          :n],
                                                               "float_1": np.tile(
                                                                   np.array([1.11, 0.0, None], dtype="float32"),
                                                                   ceil(n / 3))[:n],
                                                               "float_2": np.tile(
                                                                   np.array([None, None, None], dtype="float32"),
                                                                   ceil(n / 3))[:n]
                                                               })),
            ("{}_float_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "float_0": np.tile(
                                                                   np.array([1.11, 0.0], dtype="float32"), ceil(n / 2))[
                                                                          :n],
                                                               })),
            ("{}_float_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                               "float_0": np.array([1.11], dtype="float32"),
                                                               "float_1": np.array([0.0], dtype="float32"),
                                                               "float_2": np.array([None], dtype="float32")
                                                               })),
            ("{}_float_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                               "float_0": np.array([], dtype="float32"),
                                                               "float_1": np.array([], dtype="float32"),
                                                               "float_2": np.array([], dtype="float32")
                                                               })),
        ],
    }, DATATYPE.DT_DOUBLE: {
        "scripts": """
            table_double_0 = table(1..v_n as index, v_double_0 as double_0, v_double_1 as double_1, take([s_double_n, s_double_n, s_double_n], v_n) as double_2)
            table_double_1 = table(1..v_n as index, v_double_0 as double_0)
            table_double_2 = table(1 as index, [s_double_1] as double_0, [s_double_2] as double_1, [s_double_n] as double_2)
            table_double_3 = table(v_n:0, `index`double_0`double_1`double_2, [INT, DOUBLE, DOUBLE, DOUBLE])
            test_table_double_0 = table(v_n:0, `index`double_0`double_1`double_2, [INT, DOUBLE, DOUBLE, DOUBLE])
            test_table_double_1 = table(v_n:0, `index`double_0, [INT, DOUBLE])
            test_table_double_2 = table(v_n:0, `index`double_0`double_1`double_2, [INT, DOUBLE, DOUBLE, DOUBLE])
            test_table_double_3 = table(v_n:0, `index`double_0`double_1`double_2, [INT, DOUBLE, DOUBLE, DOUBLE])
        """.replace("table", typeTable),
        "keyScripts": """
            table_double_0 = table(`index, 1..v_n as index, v_double_0 as double_0, v_double_1 as double_1, take([s_double_n, s_double_n, s_double_n], v_n) as double_2)
            table_double_1 = table(`index, 1..v_n as index, v_double_0 as double_0)
            table_double_2 = table(`index, 1 as index, [s_double_1] as double_0, [s_double_2] as double_1, [s_double_n] as double_2)
            table_double_3 = table(`index, v_n:0, `index`double_0`double_1`double_2, [INT, DOUBLE, DOUBLE, DOUBLE])
            test_table_double_0 = table(`index, v_n:0, `index`double_0`double_1`double_2, [INT, DOUBLE, DOUBLE, DOUBLE])
            test_table_double_1 = table(`index, v_n:0, `index`double_0, [INT, DOUBLE])
            test_table_double_2 = table(`index, v_n:0, `index`double_0`double_1`double_2, [INT, DOUBLE, DOUBLE, DOUBLE])
            test_table_double_3 = table(`index, v_n:0, `index`double_0`double_1`double_2, [INT, DOUBLE, DOUBLE, DOUBLE])
        """.replace("table", typeTable),
        "upload": [
            ("{}_double_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "double_0": np.tile(
                                                                    np.array([1.11, 0.0], dtype="float64"),
                                                                    ceil(n / 2))[:n],
                                                                "double_1": np.tile(
                                                                    np.array([1.11, 0.0, None], dtype="float64"),
                                                                    ceil(n / 3))[:n],
                                                                "double_2": np.tile(
                                                                    np.array([None, None, None], dtype="float64"),
                                                                    ceil(n / 3))[:n]
                                                                })),
            ("{}_double_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "double_0": np.tile(
                                                                    np.array([1.11, 0.0], dtype="float64"),
                                                                    ceil(n / 2))[:n],
                                                                })),
            ("{}_double_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                "double_0": np.array([1.11], dtype="float64"),
                                                                "double_1": np.array([0.0], dtype="float64"),
                                                                "double_2": np.array([None], dtype="float64")
                                                                })),
            ("{}_double_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                "double_0": np.array([], dtype="float64"),
                                                                "double_1": np.array([], dtype="float64"),
                                                                "double_2": np.array([], dtype="float64")
                                                                })),
        ],
        "download": [
            ("{}_double_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "double_0": np.tile(
                                                                    np.array([1.11, 0.0], dtype="float64"),
                                                                    ceil(n / 2))[:n],
                                                                "double_1": np.tile(
                                                                    np.array([1.11, 0.0, None], dtype="float64"),
                                                                    ceil(n / 3))[:n],
                                                                "double_2": np.tile(
                                                                    np.array([None, None, None], dtype="float64"),
                                                                    ceil(n / 3))[:n]
                                                                })),
            ("{}_double_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "double_0": np.tile(
                                                                    np.array([1.11, 0.0], dtype="float64"),
                                                                    ceil(n / 2))[:n],
                                                                })),
            ("{}_double_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                "double_0": np.array([1.11], dtype="float64"),
                                                                "double_1": np.array([0.0], dtype="float64"),
                                                                "double_2": np.array([None], dtype="float64")
                                                                })),
            ("{}_double_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                "double_0": np.array([], dtype="float64"),
                                                                "double_1": np.array([], dtype="float64"),
                                                                "double_2": np.array([], dtype="float64")
                                                                })),
        ],
    }, DATATYPE.DT_SYMBOL: {
        "scripts": """
            table_symbol_0 = table(1..v_n as index, take(symbol(["hello", "world"]), v_n) as symbol_0, take(symbol(["hello", NULL]), v_n) as symbol_1)
            table_symbol_1 = table(1..v_n as index, take(symbol(["hello", "world"]), v_n) as symbol_0)
            table_symbol_2 = table(1 as index, symbol(["hello"]) as symbol_0, symbol(["world"]) as symbol_1)
            table_symbol_3 = table(v_n:0, `index`symbol_0`symbol_1`symbol_2, [INT, SYMBOL, SYMBOL, SYMBOL])
            test_table_symbol_0 = table(v_n:0, `index`symbol_0`symbol_1`symbol_2, [INT, SYMBOL, SYMBOL, SYMBOL])
            test_table_symbol_1 = table(v_n:0, `index`symbol_0, [INT, SYMBOL])
            test_table_symbol_2 = table(v_n:0, `index`symbol_0`symbol_1`symbol_2, [INT, SYMBOL, SYMBOL, SYMBOL])
            test_table_symbol_3 = table(v_n:0, `index`symbol_0`symbol_1`symbol_2, [INT, SYMBOL, SYMBOL, SYMBOL])
        """.replace("table", typeTable),
        "keyScripts": """
            table_symbol_0 = table(`index, 1..v_n as index, take(symbol(["hello", "world"]), v_n) as symbol_0, take(symbol(["hello", NULL]), v_n) as symbol_1)
            table_symbol_1 = table(`index, 1..v_n as index, take(symbol(["hello", "world"]), v_n) as symbol_0)
            table_symbol_2 = table(`index, 1 as index, symbol(["hello"]) as symbol_0, symbol(["world"]) as symbol_1)
            table_symbol_3 = table(`index, v_n:0, `index`symbol_0`symbol_1`symbol_2, [INT, SYMBOL, SYMBOL, SYMBOL])
            test_table_symbol_0 = table(`index, v_n:0, `index`symbol_0`symbol_1`symbol_2, [INT, SYMBOL, SYMBOL, SYMBOL])
            test_table_symbol_1 = table(`index, v_n:0, `index`symbol_0, [INT, SYMBOL])
            test_table_symbol_2 = table(`index, v_n:0, `index`symbol_0`symbol_1`symbol_2, [INT, SYMBOL, SYMBOL, SYMBOL])
            test_table_symbol_3 = table(`index, v_n:0, `index`symbol_0`symbol_1`symbol_2, [INT, SYMBOL, SYMBOL, SYMBOL])
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_SYMBOL Table
        ],
        "download": [
            ("{}_symbol_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "symbol_0": np.tile(
                                                                    np.array(["hello", "world"], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                "symbol_1": np.tile(
                                                                    np.array(["hello", ''], dtype="str"), ceil(n / 2))[
                                                                            :n],
                                                                })),
            ("{}_symbol_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "symbol_0": np.tile(
                                                                    np.array(["hello", "world"], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                })),
            ("{}_symbol_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                "symbol_0": np.array(["hello"], dtype="str"),
                                                                "symbol_1": np.array(["world"], dtype="str"),
                                                                })),
            ("{}_symbol_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                "symbol_0": np.array([], dtype="str"),
                                                                "symbol_1": np.array([], dtype="str"),
                                                                "symbol_2": np.array([], dtype="str")
                                                                })),
        ],
    }, DATATYPE.DT_STRING: {
        "scripts": """
            table_string_0 = table(1..v_n as index, v_string_0 as string_0, v_string_1 as string_1, take([s_string_2, s_string_2], v_n) as string_2)
            table_string_1 = table(1..v_n as index, v_string_0 as string_0)
            table_string_2 = table(1 as index, [s_string_1] as string_0, [s_string_2] as string_1)
            table_string_3 = table(v_n:1, `index`string_0`string_1`string_2, [INT, STRING, STRING, STRING])
            test_table_string_0 = table(v_n:0, `index`string_0`string_1`string_2, [INT, STRING, STRING, STRING])
            test_table_string_1 = table(v_n:0, `index`string_0, [INT, STRING])
            test_table_string_2 = table(v_n:0, `index`string_0`string_1, [INT, STRING, STRING])
            test_table_string_3 = table(v_n:0, `index`string_0`string_1`string_2, [INT, STRING, STRING, STRING])
        """.replace("table", typeTable),
        "keyScripts": """
            table_string_0 = table(`index, 1..v_n as index, v_string_0 as string_0, v_string_1 as string_1, take([s_string_2, s_string_2], v_n) as string_2)
            table_string_1 = table(`index, 1..v_n as index, v_string_0 as string_0)
            table_string_2 = table(`index, 1 as index, [s_string_1] as string_0, [s_string_2] as string_1)
            table_string_3 = table(`index, v_n:1, `index`string_0`string_1`string_2, [INT, STRING, STRING, STRING])
            test_table_string_0 = table(`index, v_n:0, `index`string_0`string_1`string_2, [INT, STRING, STRING, STRING])
            test_table_string_1 = table(`index, v_n:0, `index`string_0, [INT, STRING])
            test_table_string_2 = table(`index, v_n:0, `index`string_0`string_1, [INT, STRING, STRING])
            test_table_string_3 = table(`index, v_n:0, `index`string_0`string_1`string_2, [INT, STRING, STRING, STRING])
        """.replace("table", typeTable),
        "upload": [
            ("{}_string_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "string_0": np.tile(
                                                                    np.array(["abc123测试", ""], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                "string_1": np.tile(
                                                                    np.array(["", "abc123测试"], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                "string_2": np.tile(np.array(["", ""], dtype="str"),
                                                                                    ceil(n / 2))[:n]
                                                                })),
            ("{}_string_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "string_0": np.tile(
                                                                    np.array(["abc123测试", ""], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                })),
            ("{}_string_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                "string_0": np.array(["abc123测试"], dtype="str"),
                                                                "string_1": np.array([""], dtype="str"),
                                                                })),
            ("{}_string_3".format(testTypeTable), pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                                "string_0": np.array([""], dtype="str"),
                                                                "string_1": np.array([""], dtype="str"),
                                                                "string_2": np.array([""], dtype="str")
                                                                })),
        ],
        "download": [
            ("{}_string_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "string_0": np.tile(
                                                                    np.array(["abc123测试", ""], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                "string_1": np.tile(
                                                                    np.array(["", "abc123测试"], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                "string_2": np.tile(np.array(["", ""], dtype="str"),
                                                                                    ceil(n / 2))[:n]
                                                                })),
            ("{}_string_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "string_0": np.tile(
                                                                    np.array(["abc123测试", ""], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                })),
            ("{}_string_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                "string_0": np.array(["abc123测试"], dtype="str"),
                                                                "string_1": np.array([""], dtype="str"),
                                                                })),
            ("{}_string_3".format(testTypeTable), pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                                "string_0": np.array([""], dtype="str"),
                                                                "string_1": np.array([""], dtype="str"),
                                                                "string_2": np.array([""], dtype="str")
                                                                })),
        ],
    }, DATATYPE.DT_UUID: {
        "scripts": """
            table_uuid_0 = table(1..v_n as index, v_uuid_0 as uuid_0, v_uuid_1 as uuid_1, take([s_uuid_2, s_uuid_2], v_n) as uuid_2)
            table_uuid_1 = table(1..v_n as index, v_uuid_0 as uuid_0)
            table_uuid_2 = table(1 as index, [s_uuid_1] as uuid_0, [s_uuid_2] as uuid_1)
            table_uuid_3 = table(v_n:1, `index`uuid_0`uuid_1`uuid_2, [INT, UUID, UUID, UUID])
            test_table_uuid_0 = table(v_n:0, `index`uuid_0`uuid_1`uuid_2, [INT, UUID, UUID, UUID])
            test_table_uuid_1 = table(v_n:0, `index`uuid_0, [INT, UUID])
            test_table_uuid_2 = table(v_n:0, `index`uuid_0`uuid_1, [INT, UUID, UUID])
            test_table_uuid_3 = table(v_n:0, `index`uuid_0`uuid_1`uuid_2, [INT, UUID, UUID, UUID])
        """.replace("table", typeTable),
        "keyScripts": """
            table_uuid_0 = table(`index, 1..v_n as index, v_uuid_0 as uuid_0, v_uuid_1 as uuid_1, take([s_uuid_2, s_uuid_2], v_n) as uuid_2)
            table_uuid_1 = table(`index, 1..v_n as index, v_uuid_0 as uuid_0)
            table_uuid_2 = table(`index, 1 as index, [s_uuid_1] as uuid_0, [s_uuid_2] as uuid_1)
            table_uuid_3 = table(`index, v_n:1, `index`uuid_0`uuid_1`uuid_2, [INT, UUID, UUID, UUID])
            test_table_uuid_0 = table(`index, v_n:0, `index`uuid_0`uuid_1`uuid_2, [INT, UUID, UUID, UUID])
            test_table_uuid_1 = table(`index, v_n:0, `index`uuid_0, [INT, UUID])
            test_table_uuid_2 = table(`index, v_n:0, `index`uuid_0`uuid_1, [INT, UUID, UUID])
            test_table_uuid_3 = table(`index, v_n:0, `index`uuid_0`uuid_1`uuid_2, [INT, UUID, UUID, UUID])
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_UUID Table
        ],
        "download": [
            ("{}_uuid_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "uuid_0": np.tile(np.array(
                                                                  ["5d212a78-cc48-e3b1-4235-b4d91473ee87",
                                                                   '00000000-0000-0000-0000-000000000000'],
                                                                  dtype="str"), ceil(n / 2))[:n],
                                                              "uuid_1": np.tile(np.array(
                                                                  ['00000000-0000-0000-0000-000000000000',
                                                                   "5d212a78-cc48-e3b1-4235-b4d91473ee87"],
                                                                  dtype="str"), ceil(n / 2))[:n],
                                                              "uuid_2": np.tile(np.array(
                                                                  ['00000000-0000-0000-0000-000000000000',
                                                                   '00000000-0000-0000-0000-000000000000'],
                                                                  dtype="str"), ceil(n / 2))[:n]
                                                              })),
            ("{}_uuid_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "uuid_0": np.tile(np.array(
                                                                  ["5d212a78-cc48-e3b1-4235-b4d91473ee87",
                                                                   '00000000-0000-0000-0000-000000000000'],
                                                                  dtype="str"), ceil(n / 2))[:n],
                                                              })),
            ("{}_uuid_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                              "uuid_0": np.array(
                                                                  ["5d212a78-cc48-e3b1-4235-b4d91473ee87"],
                                                                  dtype="str"),
                                                              "uuid_1": np.array(
                                                                  ['00000000-0000-0000-0000-000000000000'],
                                                                  dtype="str"),
                                                              })),
            ("{}_uuid_3".format(testTypeTable), pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                              "uuid_0": np.array(
                                                                  ['00000000-0000-0000-0000-000000000000'],
                                                                  dtype="str"),
                                                              "uuid_1": np.array(
                                                                  ['00000000-0000-0000-0000-000000000000'],
                                                                  dtype="str"),
                                                              "uuid_2": np.array(
                                                                  ['00000000-0000-0000-0000-000000000000'], dtype="str")
                                                              })),
        ],
    }, DATATYPE.DT_DATEHOUR: {
        "scripts": """
            table_datehour_0 = table(1..v_n as index, v_datehour_0 as datehour_0, v_datehour_1 as datehour_1, take([s_datehour_n, s_datehour_n, s_datehour_n], v_n) as datehour_2)
            table_datehour_1 = table(1..v_n as index, v_datehour_0 as datehour_0)
            table_datehour_2 = table(1 as index, [s_datehour_1] as datehour_0, [s_datehour_2] as datehour_1, [s_datehour_n] as datehour_2)
            table_datehour_3 = table(v_n:0, `index`datehour_0`datehour_1`datehour_2, [INT, DATEHOUR, DATEHOUR, DATEHOUR])
            test_table_datehour_0 = table(v_n:0, `index`datehour_0`datehour_1`datehour_2, [INT, DATEHOUR, DATEHOUR, DATEHOUR])
            test_table_datehour_1 = table(v_n:0, `index`datehour_0, [INT, DATEHOUR])
            test_table_datehour_2 = table(v_n:0, `index`datehour_0`datehour_1`datehour_2, [INT, DATEHOUR, DATEHOUR, DATEHOUR])
            test_table_datehour_3 = table(v_n:0, `index`datehour_0`datehour_1`datehour_2, [INT, DATEHOUR, DATEHOUR, DATEHOUR])
        """.replace("table", typeTable),
        "keyScripts": """
            table_datehour_0 = table(`index, 1..v_n as index, v_datehour_0 as datehour_0, v_datehour_1 as datehour_1, take([s_datehour_n, s_datehour_n, s_datehour_n], v_n) as datehour_2)
            table_datehour_1 = table(`index, 1..v_n as index, v_datehour_0 as datehour_0)
            table_datehour_2 = table(`index, 1 as index, [s_datehour_1] as datehour_0, [s_datehour_2] as datehour_1, [s_datehour_n] as datehour_2)
            table_datehour_3 = table(`index, v_n:0, `index`datehour_0`datehour_1`datehour_2, [INT, DATEHOUR, DATEHOUR, DATEHOUR])
            test_table_datehour_0 = table(`index, v_n:0, `index`datehour_0`datehour_1`datehour_2, [INT, DATEHOUR, DATEHOUR, DATEHOUR])
            test_table_datehour_1 = table(`index, v_n:0, `index`datehour_0, [INT, DATEHOUR])
            test_table_datehour_2 = table(`index, v_n:0, `index`datehour_0`datehour_1`datehour_2, [INT, DATEHOUR, DATEHOUR, DATEHOUR])
            test_table_datehour_3 = table(`index, v_n:0, `index`datehour_0`datehour_1`datehour_2, [INT, DATEHOUR, DATEHOUR, DATEHOUR])
        """.replace("table", typeTable),
        "upload": [
            ("{}_datehour_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "datehour_0": np.tile(
                                                                      np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                               dtype="datetime64[h]"), ceil(n / 2))[:n],
                                                                  "datehour_1": np.tile(np.array(
                                                                      ["2022-01-01 12:30:56.123456789", 0, None],
                                                                      dtype="datetime64[h]"), ceil(n / 3))[:n],
                                                                  "datehour_2": np.tile(np.array([None, None, None],
                                                                                                 dtype="datetime64[h]"),
                                                                                        ceil(n / 3))[:n]
                                                                  })),
            ("{}_datehour_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "datehour_0": np.tile(
                                                                      np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                               dtype="datetime64[h]"), ceil(n / 2))[:n],
                                                                  })),
            ("{}_datehour_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                  "datehour_0": np.array(
                                                                      ["2022-01-01 12:30:56.123456789"],
                                                                      dtype="datetime64[h]"),
                                                                  "datehour_1": np.array([0], dtype="datetime64[h]"),
                                                                  "datehour_2": np.array([None], dtype="datetime64[h]")
                                                                  })),
            ("{}_datehour_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                  "datehour_0": np.array([], dtype="datetime64[h]"),
                                                                  "datehour_1": np.array([], dtype="datetime64[h]"),
                                                                  "datehour_2": np.array([], dtype="datetime64[h]")
                                                                  })),
        ],
        "download": [
            ("{}_datehour_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "datehour_0": np.tile(np.array(["2022-01-01 12", 0],
                                                                                                 dtype="datetime64[ns]"),
                                                                                        ceil(n / 2))[:n],
                                                                  "datehour_1": np.tile(
                                                                      np.array(["2022-01-01 12", 0, None],
                                                                               dtype="datetime64[ns]"), ceil(n / 3))[
                                                                                :n],
                                                                  "datehour_2": np.tile(np.array([None, None, None],
                                                                                                 dtype="datetime64[ns]"),
                                                                                        ceil(n / 3))[:n]
                                                                  })),
            ("{}_datehour_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                  "datehour_0": np.tile(np.array(["2022-01-01 12", 0],
                                                                                                 dtype="datetime64[ns]"),
                                                                                        ceil(n / 2))[:n],
                                                                  })),
            ("{}_datehour_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                  "datehour_0": np.array(["2022-01-01 12"],
                                                                                         dtype="datetime64[ns]"),
                                                                  "datehour_1": np.array([0], dtype="datetime64[ns]"),
                                                                  "datehour_2": np.array([None], dtype="datetime64[ns]")
                                                                  })),
            ("{}_datehour_3".format(testTypeTable), pd.DataFrame({"index": np.array([], dtype="int32"),
                                                                  "datehour_0": np.array([], dtype="datetime64[ns]"),
                                                                  "datehour_1": np.array([], dtype="datetime64[ns]"),
                                                                  "datehour_2": np.array([], dtype="datetime64[ns]")
                                                                  })),
        ],
    }, DATATYPE.DT_IPADDR: {
        "scripts": """
            table_ip_0 = table(1..v_n as index, v_ip_0 as ip_0, v_ip_1 as ip_1, take([s_ip_2, s_ip_2], v_n) as ip_2)
            table_ip_1 = table(1..v_n as index, v_ip_0 as ip_0)
            table_ip_2 = table(1 as index, [s_ip_1] as ip_0, [s_ip_2] as ip_1)
            table_ip_3 = table(v_n:1, `index`ip_0`ip_1`ip_2, [INT, IPADDR, IPADDR, IPADDR])
            test_table_ip_0 = table(v_n:0, `index`ip_0`ip_1`ip_2, [INT, IPADDR, IPADDR, IPADDR])
            test_table_ip_1 = table(v_n:0, `index`ip_0, [INT, IPADDR])
            test_table_ip_2 = table(v_n:0, `index`ip_0`ip_1, [INT, IPADDR, IPADDR])
            test_table_ip_3 = table(v_n:1, `index`ip_0`ip_1`ip_2, [INT, IPADDR, IPADDR, IPADDR])
        """.replace("table", typeTable),
        "keyScripts": """
            table_ip_0 = table(`index, 1..v_n as index, v_ip_0 as ip_0, v_ip_1 as ip_1, take([s_ip_2, s_ip_2], v_n) as ip_2)
            table_ip_1 = table(`index, 1..v_n as index, v_ip_0 as ip_0)
            table_ip_2 = table(`index, 1 as index, [s_ip_1] as ip_0, [s_ip_2] as ip_1)
            table_ip_3 = table(`index, v_n:1, `index`ip_0`ip_1`ip_2, [INT, IPADDR, IPADDR, IPADDR])
            test_table_ip_0 = table(`index, v_n:0, `index`ip_0`ip_1`ip_2, [INT, IPADDR, IPADDR, IPADDR])
            test_table_ip_1 = table(`index, v_n:0, `index`ip_0, [INT, IPADDR])
            test_table_ip_2 = table(`index, v_n:0, `index`ip_0`ip_1, [INT, IPADDR, IPADDR])
            test_table_ip_3 = table(`index, v_n:1, `index`ip_0`ip_1`ip_2, [INT, IPADDR, IPADDR, IPADDR])
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_IPADDR Table
        ],
        "download": [
            ("{}_ip_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                            "ip_0": np.tile(
                                                                np.array(["192.168.1.135", '0.0.0.0'], dtype="str"),
                                                                ceil(n / 2))[:n],
                                                            "ip_1": np.tile(
                                                                np.array(['0.0.0.0', "192.168.1.135"], dtype="str"),
                                                                ceil(n / 2))[:n],
                                                            "ip_2": np.tile(
                                                                np.array(['0.0.0.0', '0.0.0.0'], dtype="str"),
                                                                ceil(n / 2))[:n]
                                                            })),
            ("{}_ip_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                            "ip_0": np.tile(
                                                                np.array(["192.168.1.135", '0.0.0.0'], dtype="str"),
                                                                ceil(n / 2))[:n],
                                                            })),
            ("{}_ip_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                            "ip_0": np.array(["192.168.1.135"], dtype="str"),
                                                            "ip_1": np.array(['0.0.0.0'], dtype="str"),
                                                            })),
            ("{}_ip_3".format(testTypeTable), pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                            "ip_0": np.array(['0.0.0.0'], dtype="str"),
                                                            "ip_1": np.array(['0.0.0.0'], dtype="str"),
                                                            "ip_2": np.array(['0.0.0.0'], dtype="str")
                                                            })),
        ],
    }, DATATYPE.DT_INT128: {
        "scripts": """
            table_int128_0 = table(1..v_n as index, v_int128_0 as int128_0, v_int128_1 as int128_1, take([s_int128_2, s_int128_2], v_n) as int128_2)
            table_int128_1 = table(1..v_n as index, v_int128_0 as int128_0)
            table_int128_2 = table(1 as index, [s_int128_1] as int128_0, [s_int128_2] as int128_1)
            table_int128_3 = table(v_n:1, `index`int128_0`int128_1`int128_2, [INT, INT128, INT128, INT128])
            test_table_int128_0 = table(v_n:0, `index`int128_0`int128_1`int128_2, [INT, INT128, INT128, INT128])
            test_table_int128_1 = table(v_n:0, `index`int128_0, [INT, INT128])
            test_table_int128_2 = table(v_n:0, `index`int128_0`int128_1, [INT, INT128, INT128])
            test_table_int128_3 = table(v_n:1, `index`int128_0`int128_1`int128_2, [INT, INT128, INT128, INT128])
        """.replace("table", typeTable),
        "keyScripts": """
            table_int128_0 = table(`index, 1..v_n as index, v_int128_0 as int128_0, v_int128_1 as int128_1, take([s_int128_2, s_int128_2], v_n) as int128_2)
            table_int128_1 = table(`index, 1..v_n as index, v_int128_0 as int128_0)
            table_int128_2 = table(`index, 1 as index, [s_int128_1] as int128_0, [s_int128_2] as int128_1)
            table_int128_3 = table(`index, v_n:1, `index`int128_0`int128_1`int128_2, [INT, INT128, INT128, INT128])
            test_table_int128_0 = table(`index, v_n:0, `index`int128_0`int128_1`int128_2, [INT, INT128, INT128, INT128])
            test_table_int128_1 = table(`index, v_n:0, `index`int128_0, [INT, INT128])
            test_table_int128_2 = table(`index, v_n:0, `index`int128_0`int128_1, [INT, INT128, INT128])
            test_table_int128_3 = table(`index, v_n:1, `index`int128_0`int128_1`int128_2, [INT, INT128, INT128, INT128])
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_IPADDR Table
        ],
        "download": [
            ("{}_int128_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "int128_0": np.tile(np.array(
                                                                    ["e1671797c52e15f763380b45e841ec32",
                                                                     '00000000000000000000000000000000'], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                "int128_1": np.tile(np.array(
                                                                    ['00000000000000000000000000000000',
                                                                     "e1671797c52e15f763380b45e841ec32"], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                "int128_2": np.tile(np.array(
                                                                    ['00000000000000000000000000000000',
                                                                     '00000000000000000000000000000000'], dtype="str"),
                                                                    ceil(n / 2))[:n]
                                                                })),
            ("{}_int128_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                                "int128_0": np.tile(np.array(
                                                                    ["e1671797c52e15f763380b45e841ec32",
                                                                     '00000000000000000000000000000000'], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                                })),
            ("{}_int128_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                                "int128_0": np.array(
                                                                    ["e1671797c52e15f763380b45e841ec32"], dtype="str"),
                                                                'int128_1': np.array(
                                                                    ['00000000000000000000000000000000'], dtype="str"),
                                                                })),
            ("{}_int128_3".format(testTypeTable), pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                                "int128_0": np.array(
                                                                    ['00000000000000000000000000000000'], dtype="str"),
                                                                "int128_1": np.array(
                                                                    ['00000000000000000000000000000000'], dtype="str"),
                                                                "int128_2": np.array(
                                                                    ['00000000000000000000000000000000'], dtype="str")
                                                                })),
        ],
    }, DATATYPE.DT_BLOB: {
        "scripts": """
            table_blob_0 = table(1..v_n as index, v_blob_0 as blob_0, v_blob_1 as blob_1, take([s_blob_2, s_blob_2], v_n) as blob_2)
            table_blob_1 = table(1..v_n as index, v_blob_0 as blob_0)
            table_blob_2 = table(1 as index, [s_blob_1] as blob_0, [s_blob_2] as blob_1)
            table_blob_3 = table(v_n:1, `index`blob_0`blob_1`blob_2, [INT, BLOB, BLOB, BLOB])
            test_table_blob_0 = table(v_n:0, `index`blob_0`blob_1`blob_2, [INT, BLOB, BLOB, BLOB])
            test_table_blob_1 = table(v_n:0, `index`blob_0, [INT, BLOB])
            test_table_blob_2 = table(v_n:0, `index`blob_0`blob_1, [INT, BLOB, BLOB])
            test_table_blob_3 = table(v_n:1, `index`blob_0`blob_1`blob_2, [INT, BLOB, BLOB, BLOB])
        """.replace("table", typeTable),
        "keyScripts": """
            table_blob_0 = table(`index, 1..v_n as index, v_blob_0 as blob_0, v_blob_1 as blob_1, take([s_blob_2, s_blob_2], v_n) as blob_2)
            table_blob_1 = table(`index, 1..v_n as index, v_blob_0 as blob_0)
            table_blob_2 = table(`index, 1 as index, [s_blob_1] as blob_0, [s_blob_2] as blob_1)
            table_blob_3 = table(`index, v_n:1, `index`blob_0`blob_1`blob_2, [INT, BLOB, BLOB, BLOB])
            test_table_blob_0 = table(`index, v_n:0, `index`blob_0`blob_1`blob_2, [INT, BLOB, BLOB, BLOB])
            test_table_blob_1 = table(`index, v_n:0, `index`blob_0, [INT, BLOB])
            test_table_blob_2 = table(`index, v_n:0, `index`blob_0`blob_1, [INT, BLOB, BLOB])
            test_table_blob_3 = table(`index, v_n:1, `index`blob_0`blob_1`blob_2, [INT, BLOB, BLOB, BLOB])
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_BLOB Table
        ],
        "download": [
            ("{}_blob_0".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "blob_0": np.tile(
                                                                  np.array([b"hello", b""], dtype="object"),
                                                                  ceil(n / 2))[:n],
                                                              "blob_1": np.tile(
                                                                  np.array([b"", b"hello"], dtype="object"),
                                                                  ceil(n / 2))[:n],
                                                              "blob_2": np.tile(np.array([b"", b""], dtype="object"),
                                                                                ceil(n / 2))[:n]
                                                              })),
            ("{}_blob_1".format(testTypeTable), pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                              "blob_0": np.tile(
                                                                  np.array([b"hello", b""], dtype="object"),
                                                                  ceil(n / 2))[:n],
                                                              })),
            ("{}_blob_2".format(testTypeTable), pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                              "blob_0": np.array([b"hello"], dtype="object"),
                                                              "blob_1": np.array([b""], dtype="object"),
                                                              })),
            ("{}_blob_3".format(testTypeTable), pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                              "blob_0": np.array([b""], dtype="object"),
                                                              "blob_1": np.array([b""], dtype="object"),
                                                              "blob_2": np.array([b""], dtype="object")
                                                              })),
        ],
    }, DATATYPE.DT_DECIMAL32: {
        "scripts": """
        """,
        "keyScripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL64: {
        "scripts": """
        """,
        "keyScripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }}
    # TODO: python decimal32
    # TODO: python decimal64

    for x in DATATYPE:
        if typeTable in ["keyedTable", "indexedTable"]:
            prefix += str(pythons[x]["keyScripts"])
        else:
            prefix += str(pythons[x]["scripts"])

    if isShare:
        prefix += shareScrips
        prefix += appendScrips

    def get_script(*args, **kwargs):
        return prefix

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_ArrayVector(*args, r=10, c=10, size=1000, **kwargs):
    prefix1 = "a_r = {}\n".format(r)
    prefix2 = "a_c = {}\n".format(c)
    prefix3 = "a_s = {}\n".format(size)
    pythons = {DATATYPE.DT_VOID: {
        # no way to create a void array vector
        "scripts": ''''''
        ,
        "upload": [],
        "download": [
        ],
    }, DATATYPE.DT_BOOL: {
        "scripts":
            '''
            x = array(BOOL[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([true, false], a_c)])}
            a_bool_0 = x        
            x = array(BOOL[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([true, false, bool()], a_c)])}
            a_bool_1 = x
            x = array(BOOL[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([true, bool(), false], a_c)])}
            a_bool_2 = x
            x = array(BOOL[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([bool(), true, false], a_c)])}
            a_bool_3 = x
            x = array(BOOL[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([bool()], a_c)])}
            a_bool_n = x
            '''
        ,
        "upload": [],
        "download": [
            ('a_bool_0', np.array([([True, False] * ceil(c / 2))[:c]] * r)),
            ('a_bool_1', np.array([([True, False, None] * ceil(c / 3))[:c]] * r)),
            ('a_bool_2', np.array([([True, None, False] * ceil(c / 3))[:c]] * r)),
            ('a_bool_3', np.array([([None, True, False] * ceil(c / 3))[:c]] * r)),
            ('a_bool_n', np.array([([None] * ceil(c))[:c]] * r)),

        ],
    }, DATATYPE.DT_CHAR: {
        "scripts":
            '''
            x = array(CHAR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1c, 2c, 'i', '{'], a_c)])}
            a_char_0 = x        
            x = array(CHAR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1c, 2c, 'i', '{', char()], a_c)])}
            a_char_1 = x
            x = array(CHAR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1c, 2c, char(), 'i', '{'], a_c)])}
            a_char_2 = x
            x = array(CHAR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([char(), 1c, 2c, 'i', '{'], a_c)])}
            a_char_3 = x
            x = array(CHAR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([char()], a_c)])}
            a_char_n = x
            '''
        ,
        "upload": [],
        "download": [
            ('a_char_0', np.array([([1, 2, 105, 123] * ceil(c / 4))[:c]] * r)),
            ('a_char_1', np.array([([1, 2, 105, 123, np.nan] * ceil(c / 5))[:c]] * r)),
            ('a_char_2', np.array([([1, 2, np.nan, 105, 123] * ceil(c / 5))[:c]] * r)),
            ('a_char_3', np.array([([np.nan, 1, 2, 105, 123] * ceil(c / 5))[:c]] * r)),
            ('a_char_n', np.array([([np.nan] * ceil(c))[:c]] * r)),
        ],
    }, DATATYPE.DT_SHORT: {
        "scripts":
            '''
            x = array(SHORT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1, 2], a_c)])}
            a_short_0 = x        
            x = array(SHORT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1, 2, short()], a_c)])}
            a_short_1 = x
            x = array(SHORT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1, short(), 2], a_c)])}
            a_short_2 = x
            x = array(SHORT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([short(), 1, 2], a_c)])}
            a_short_3 = x
            x = array(SHORT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([short()], a_c)])}
            a_short_n = x 
            '''
        ,
        "upload": [],
        "download": [
            ('a_short_0', np.array([([1, 2] * ceil(c / 2))[:c]] * r)),
            ('a_short_1', np.array([([1, 2, np.nan] * ceil(c / 3))[:c]] * r)),
            ('a_short_2', np.array([([1, np.nan, 2] * ceil(c / 3))[:c]] * r)),
            ('a_short_3', np.array([([np.nan, 1, 2] * ceil(c / 3))[:c]] * r)),
            ('a_short_n', np.array([([np.nan] * ceil(c))[:c]] * r)),
        ],
    }, DATATYPE.DT_INT: {
        "scripts":
            '''
            x = array(INT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1, 2], a_c)])}
            a_int_0 = x        
            x = array(INT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1, 2, int()], a_c)])}
            a_int_1 = x
            x = array(INT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1, int(), 2], a_c)])}
            a_int_2 = x
            x = array(INT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([int(), 1, 2], a_c)])}
            a_int_3 = x
            x = array(INT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([int()], a_c)])}
            a_int_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_int_0', np.array([([1, 2] * ceil(c / 2))[:c]] * r)),
            ('a_int_1', np.array([([1, 2, np.nan] * ceil(c / 3))[:c]] * r)),
            ('a_int_2', np.array([([1, np.nan, 2] * ceil(c / 3))[:c]] * r)),
            ('a_int_3', np.array([([np.nan, 1, 2] * ceil(c / 3))[:c]] * r)),
            ('a_int_n', np.array([([np.nan] * ceil(c))[:c]] * r)),
        ],
    }, DATATYPE.DT_LONG: {
        "scripts":
            '''
            x = array(LONG[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1, 2], a_c)])}
            a_long_0 = x        
            x = array(LONG[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1, 2, long()], a_c)])}
            a_long_1 = x
            x = array(LONG[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1, long(), 2], a_c)])}
            a_long_2 = x
            x = array(LONG[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([long(), 1, 2], a_c)])}
            a_long_3 = x
            x = array(LONG[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([long()], a_c)])}
            a_long_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_long_0', np.array([([1, 2] * ceil(c / 2))[:c]] * r)),
            ('a_long_1', np.array([([1, 2, np.nan] * ceil(c / 3))[:c]] * r)),
            ('a_long_2', np.array([([1, np.nan, 2] * ceil(c / 3))[:c]] * r)),
            ('a_long_3', np.array([([np.nan, 1, 2] * ceil(c / 3))[:c]] * r)),
            ('a_long_n', np.array([([np.nan] * ceil(c))[:c]] * r)),
        ],
    }, DATATYPE.DT_DATE: {
        "scripts":
            '''
            x = array(DATE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2022.01.31, 2001.01.31], a_c)])}
            a_date_0 = x        
            x = array(DATE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2022.01.31, 2001.01.31, date()], a_c)])}
            a_date_1 = x
            x = array(DATE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2022.01.31, date(), 2001.01.31], a_c)])}
            a_date_2 = x
            x = array(DATE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([date(), 2022.01.31, 2001.01.31], a_c)])}
            a_date_3 = x
            x = array(DATE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([date()], a_c)])}
            a_date_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_date_0', np.array([(['2022-01-31', '2001-01-31'] * ceil(c / 2))[:c]] * r, dtype='datetime64[D]')),
            ('a_date_1', np.array([(['2022-01-31', '2001-01-31', None] * ceil(c / 3))[:c]] * r, dtype='datetime64[D]')),
            ('a_date_2', np.array([(['2022-01-31', None, '2001-01-31'] * ceil(c / 3))[:c]] * r, dtype='datetime64[D]')),
            ('a_date_3', np.array([([None, '2022-01-31', '2001-01-31'] * ceil(c / 3))[:c]] * r, dtype='datetime64[D]')),
            ('a_date_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[D]')),
        ],
    }, DATATYPE.DT_MONTH: {
        "scripts":
            '''
            x = array(MONTH[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2022.01M, 2001.01M], a_c)])}
            a_month_0 = x        
            x = array(MONTH[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2022.01M, 2001.01M, month()], a_c)])}
            a_month_1 = x
            x = array(MONTH[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2022.01M, month(), 2001.01M], a_c)])}
            a_month_2 = x
            x = array(MONTH[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([month(), 2022.01M, 2001.01M], a_c)])}
            a_month_3 = x
            x = array(MONTH[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([month()], a_c)])}
            a_month_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_month_0', np.array([(['2022-01-31', '2001-01-31'] * ceil(c / 2))[:c]] * r, dtype='datetime64[M]')),
            (
                'a_month_1',
                np.array([(['2022-01-31', '2001-01-31', None] * ceil(c / 3))[:c]] * r, dtype='datetime64[M]')),
            (
                'a_month_2',
                np.array([(['2022-01-31', None, '2001-01-31'] * ceil(c / 3))[:c]] * r, dtype='datetime64[M]')),
            (
                'a_month_3',
                np.array([([None, '2022-01-31', '2001-01-31'] * ceil(c / 3))[:c]] * r, dtype='datetime64[M]')),
            ('a_month_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[M]')),
        ],
    }, DATATYPE.DT_TIME: {
        "scripts":
            '''
            x = array(TIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30:10.008, 17:37:16.058], a_c)])}
            a_time_0 = x        
            x = array(TIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30:10.008, 17:37:16.058, time()], a_c)])}
            a_time_1 = x
            x = array(TIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30:10.008, time(), 17:37:16.058], a_c)])}
            a_time_2 = x
            x = array(TIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([time(), 13:30:10.008, 17:37:16.058], a_c)])}
            a_time_3 = x
            x = array(TIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([time()], a_c)])}
            a_time_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_time_0',
             np.array([(['1970-01-01 13:30:10.008000000', '1970-01-01 17:37:16.058000000'] * ceil(c / 2))[:c]] * r,
                      dtype='datetime64[ms]')),
            ('a_time_1', np.array(
                [(['1970-01-01 13:30:10.008000000', '1970-01-01 17:37:16.058000000', None] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ms]')),
            ('a_time_2', np.array(
                [(['1970-01-01 13:30:10.008000000', None, '1970-01-01 17:37:16.058000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ms]')),
            ('a_time_3', np.array(
                [([None, '1970-01-01 13:30:10.008000000', '1970-01-01 17:37:16.058000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ms]')),
            ('a_time_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[ms]')),
        ],
    }, DATATYPE.DT_MINUTE: {
        "scripts":
            '''
            x = array(MINUTE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30m, 17:37m], a_c)])}
            a_minute_0 = x        
            x = array(MINUTE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30m, 17:37m, minute()], a_c)])}
            a_minute_1 = x
            x = array(MINUTE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30m, minute(), 17:37m], a_c)])}
            a_minute_2 = x
            x = array(MINUTE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([minute(), 13:30m, 17:37m], a_c)])}
            a_minute_3 = x
            x = array(MINUTE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([minute()], a_c)])}
            a_minute_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_minute_0',
             np.array([(['1970-01-01T13:30:00.000000000', '1970-01-01T17:37:00.000000000'] * ceil(c / 2))[:c]] * r,
                      dtype='datetime64[m]')),
            ('a_minute_1', np.array(
                [(['1970-01-01T13:30:00.000000000', '1970-01-01T17:37:00.000000000', None] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[m]')),
            ('a_minute_2', np.array(
                [(['1970-01-01T13:30:00.000000000', None, '1970-01-01T17:37:00.000000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[m]')),
            ('a_minute_3', np.array(
                [([None, '1970-01-01T13:30:00.000000000', '1970-01-01T17:37:00.000000000'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[m]')),
            ('a_minute_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[m]')),
        ],
    }, DATATYPE.DT_SECOND: {
        "scripts":
            '''
            x = array(SECOND[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30:10, 17:37:20], a_c)])}
            a_second_0 = x        
            x = array(SECOND[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30:10, 17:37:20, second()], a_c)])}
            a_second_1 = x
            x = array(SECOND[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30:10, second(), 17:37:20], a_c)])}
            a_second_2 = x
            x = array(SECOND[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([second(), 13:30:10, 17:37:20], a_c)])}
            a_second_3 = x
            x = array(SECOND[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([second()], a_c)])}
            a_second_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_second_0',
             np.array([(['1970-01-01 13:30:10', '1970-01-01 17:37:20'] * ceil(c / 2))[:c]] * r, dtype='datetime64[s]')),
            ('a_second_1', np.array([(['1970-01-01 13:30:10', '1970-01-01 17:37:20', None] * ceil(c / 3))[:c]] * r,
                                    dtype='datetime64[s]')),
            ('a_second_2', np.array([(['1970-01-01 13:30:10', None, '1970-01-01 17:37:20'] * ceil(c / 3))[:c]] * r,
                                    dtype='datetime64[s]')),
            ('a_second_3', np.array([([None, '1970-01-01 13:30:10', '1970-01-01 17:37:20'] * ceil(c / 3))[:c]] * r,
                                    dtype='datetime64[s]')),
            ('a_second_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[s]')),
        ],
    }, DATATYPE.DT_DATETIME: {
        "scripts":
            '''
            x = array(DATETIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2001.01.31 13:30:10, 2020.01.31T17:37:20], a_c)])}
            a_datetime_0 = x        
            x = array(DATETIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2001.01.31 13:30:10, 2020.01.31T17:37:20, datetime()], a_c)])}
            a_datetime_1 = x
            x = array(DATETIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2001.01.31 13:30:10, datetime(), 2020.01.31T17:37:20], a_c)])}
            a_datetime_2 = x
            x = array(DATETIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([datetime(), 2001.01.31 13:30:10, 2020.01.31T17:37:20], a_c)])}
            a_datetime_3 = x
            x = array(DATETIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([datetime()], a_c)])}
            a_datetime_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_datetime_0',
             np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r, dtype='datetime64[s]')),
            ('a_datetime_1', np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', None] * ceil(c / 3))[:c]] * r,
                                      dtype='datetime64[s]')),
            ('a_datetime_2', np.array([(['2001-01-31 13:30:10', None, '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r,
                                      dtype='datetime64[s]')),
            ('a_datetime_3', np.array([([None, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r,
                                      dtype='datetime64[s]')),
            ('a_datetime_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[s]')),
        ],
    }, DATATYPE.DT_TIMESTAMP: {
        "scripts":
            '''
            x = array(TIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2001.01.31 13:30:10.123, 2020.01.31T17:37:20.123], a_c)])}
            a_timestamp_0 = x        
            x = array(TIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2001.01.31 13:30:10.123, 2020.01.31T17:37:20.123, timestamp()], a_c)])}
            a_timestamp_1 = x
            x = array(TIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2001.01.31 13:30:10.123, timestamp(), 2020.01.31T17:37:20.123], a_c)])}
            a_timestamp_2 = x
            x = array(TIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([timestamp(), 2001.01.31 13:30:10.123, 2020.01.31T17:37:20.123], a_c)])}
            a_timestamp_3 = x
            x = array(TIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([timestamp()], a_c)])}
            a_timestamp_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_timestamp_0', np.array([(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c / 2))[:c]] * r,
                                       dtype='datetime64[ms]')),
            ('a_timestamp_1',
             np.array([(['2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123', None] * ceil(c / 3))[:c]] * r,
                      dtype='datetime64[ms]')),
            ('a_timestamp_2',
             np.array([(['2001-01-31 13:30:10.123', None, '2020-01-31 17:37:20.123'] * ceil(c / 3))[:c]] * r,
                      dtype='datetime64[ms]')),
            ('a_timestamp_3',
             np.array([([None, '2001-01-31 13:30:10.123', '2020-01-31 17:37:20.123'] * ceil(c / 3))[:c]] * r,
                      dtype='datetime64[ms]')),
            ('a_timestamp_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[ms]')),
        ],
    }, DATATYPE.DT_NANOTIME: {
        "scripts":
            '''
            x = array(NANOTIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30:10.123456789, 17:37:20.123456789], a_c)])}
            a_nanotime_0 = x        
            x = array(NANOTIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30:10.123456789, 17:37:20.123456789, nanotime()], a_c)])}
            a_nanotime_1 = x
            x = array(NANOTIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([13:30:10.123456789, nanotime(), 17:37:20.123456789], a_c)])}
            a_nanotime_2 = x
            x = array(NANOTIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([nanotime(), 13:30:10.123456789, 17:37:20.123456789], a_c)])}
            a_nanotime_3 = x
            x = array(NANOTIME[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([nanotime()], a_c)])}
            a_nanotime_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_nanotime_0',
             np.array([(['1970-01-01 13:30:10.123456789', '1970-01-01 17:37:20.123456789'] * ceil(c / 2))[:c]] * r,
                      dtype='datetime64[ns]')),
            ('a_nanotime_1', np.array(
                [(['1970-01-01 13:30:10.123456789', '1970-01-01 17:37:20.123456789', None] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')),
            ('a_nanotime_2', np.array(
                [(['1970-01-01 13:30:10.123456789', None, '1970-01-01 17:37:20.123456789'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')),
            ('a_nanotime_3', np.array(
                [([None, '1970-01-01 13:30:10.123456789', '1970-01-01 17:37:20.123456789'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')),
            ('a_nanotime_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[ns]')),
        ],
    }, DATATYPE.DT_NANOTIMESTAMP: {
        "scripts":
            '''
            x = array(NANOTIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2001.01.31 13:30:10.123456789, 2020.01.31T17:37:20.123456789], a_c)])}
            a_nanotimestamp_0 = x        
            x = array(NANOTIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2001.01.31 13:30:10.123456789, 2020.01.31T17:37:20.123456789, nanotimestamp()], a_c)])}
            a_nanotimestamp_1 = x
            x = array(NANOTIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([2001.01.31 13:30:10.123456789, nanotimestamp(), 2020.01.31T17:37:20.123456789], a_c)])}
            a_nanotimestamp_2 = x
            x = array(NANOTIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([nanotimestamp(), 2001.01.31 13:30:10.123456789, 2020.01.31T17:37:20.123456789], a_c)])}
            a_nanotimestamp_3 = x
            x = array(NANOTIMESTAMP[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([nanotimestamp()], a_c)])}
            a_nanotimestamp_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_nanotimestamp_0',
             np.array([(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c / 2))[:c]] * r,
                      dtype='datetime64[ns]')),
            ('a_nanotimestamp_1', np.array(
                [(['2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789', None] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')),
            ('a_nanotimestamp_2', np.array(
                [(['2001-01-31 13:30:10.123456789', None, '2020-01-31 17:37:20.123456789'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')),
            ('a_nanotimestamp_3', np.array(
                [([None, '2001-01-31 13:30:10.123456789', '2020-01-31 17:37:20.123456789'] * ceil(c / 3))[:c]] * r,
                dtype='datetime64[ns]')),
            ('a_nanotimestamp_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[ns]')),
        ],
    }, DATATYPE.DT_FLOAT: {
        "scripts":
            '''
            x = array(FLOAT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1.0f, 2.0f], a_c)])}
            a_float_0 = x        
            x = array(FLOAT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1.0f, 2.0f, float()], a_c)])}
            a_float_1 = x
            x = array(FLOAT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1.0f, float(), 2.0f], a_c)])}
            a_float_2 = x
            x = array(FLOAT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([float(), 1.0f, 2.0f], a_c)])}
            a_float_3 = x
            x = array(FLOAT[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([float()], a_c)])}
            a_float_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_float_0', np.array([([1.0, 2.0] * ceil(c / 2))[:c]] * r)),
            ('a_float_1', np.array([([1.0, 2.0, np.nan] * ceil(c / 3))[:c]] * r)),
            ('a_float_2', np.array([([1.0, np.nan, 2.0] * ceil(c / 3))[:c]] * r)),
            ('a_float_3', np.array([([np.nan, 1.0, 2.0] * ceil(c / 3))[:c]] * r)),
            ('a_float_n', np.array([([np.nan] * ceil(c))[:c]] * r)),
        ],
    }, DATATYPE.DT_DOUBLE: {
        "scripts":
            '''
            x = array(DOUBLE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1.0f, 2.0f], a_c)])}
            a_double_0 = x        
            x = array(DOUBLE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1.0f, 2.0f, double()], a_c)])}
            a_double_1 = x
            x = array(DOUBLE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([1.0f, double(), 2.0f], a_c)])}
            a_double_2 = x
            x = array(DOUBLE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([double(), 1.0f, 2.0f], a_c)])}
            a_double_3 = x
            x = array(DOUBLE[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take([double()], a_c)])}
            a_double_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_double_0', np.array([([1.0, 2.0] * ceil(c / 2))[:c]] * r)),
            ('a_double_1', np.array([([1.0, 2.0, np.nan] * ceil(c / 3))[:c]] * r)),
            ('a_double_2', np.array([([1.0, np.nan, 2.0] * ceil(c / 3))[:c]] * r)),
            ('a_double_3', np.array([([np.nan, 1.0, 2.0] * ceil(c / 3))[:c]] * r)),
            ('a_double_n', np.array([([np.nan] * ceil(c))[:c]] * r)),
        ],
    }, DATATYPE.DT_SYMBOL: {
        "scripts":
            '''   
            '''
        ,
        "upload": [],
        "download": [
        ],
    }, DATATYPE.DT_STRING: {
        "scripts":
            '''
            '''
        ,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_UUID: {
        "scripts":
            '''
            x = array(UUID[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([uuid(take(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "6b212a78-cc48-e3b1-4545-b4d91473ee79"], a_c))])}
            a_uuid_0 = x        
            x = array(UUID[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([uuid(take(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "6b212a78-cc48-e3b1-4545-b4d91473ee79", ''], a_c))])}
            a_uuid_1 = x
            x = array(UUID[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([uuid(take(["5d212a78-cc48-e3b1-4235-b4d91473ee87", '', "6b212a78-cc48-e3b1-4545-b4d91473ee79"], a_c))])}
            a_uuid_2 = x
            x = array(UUID[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([uuid(take(['', "5d212a78-cc48-e3b1-4235-b4d91473ee87", "6b212a78-cc48-e3b1-4545-b4d91473ee79"], a_c))])}
            a_uuid_3 = x
            x = array(UUID[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([uuid(take([''], a_c))])}
            a_uuid_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_uuid_0', np.array([(["5d212a78-cc48-e3b1-4235-b4d91473ee87",
                                     "6b212a78-cc48-e3b1-4545-b4d91473ee79"] * ceil(c / 2))[:c]] * r)),
            ('a_uuid_1', np.array([(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "6b212a78-cc48-e3b1-4545-b4d91473ee79",
                                     "00000000-0000-0000-0000-000000000000"] * ceil(c / 3))[:c]] * r)),
            ('a_uuid_2', np.array([(["5d212a78-cc48-e3b1-4235-b4d91473ee87", "00000000-0000-0000-0000-000000000000",
                                     "6b212a78-cc48-e3b1-4545-b4d91473ee79"] * ceil(c / 3))[:c]] * r)),
            ('a_uuid_3', np.array([(["00000000-0000-0000-0000-000000000000", "5d212a78-cc48-e3b1-4235-b4d91473ee87",
                                     "6b212a78-cc48-e3b1-4545-b4d91473ee79"] * ceil(c / 3))[:c]] * r)),
            ('a_uuid_n', np.array([(["00000000-0000-0000-0000-000000000000"] * ceil(c))[:c]] * r)),
        ],
    }, DATATYPE.DT_DATEHOUR: {
        "scripts":
            '''
            x = array(DATEHOUR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take(datehour([2001.01.31 13:30:10, 2020.01.31T17:37:20]),a_c)])}
            a_datehour_0 = x        
            x = array(DATEHOUR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take(datehour([2001.01.31 13:30:10, 2020.01.31T17:37:20, datetime()]),a_c)])}
            a_datehour_1 = x
            x = array(DATEHOUR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take(datehour([2001.01.31 13:30:10, datetime(), 2020.01.31T17:37:20]),a_c)])}
            a_datehour_2 = x
            x = array(DATEHOUR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take(datehour([datetime(), 2001.01.31 13:30:10, 2020.01.31T17:37:20]),a_c)])}
            a_datehour_3 = x
            x = array(DATEHOUR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([take(datehour([datetime()]),a_c)])}
            a_datehour_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_datehour_0',
             np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 2))[:c]] * r, dtype='datetime64[h]')),
            ('a_datehour_1', np.array([(['2001-01-31 13:30:10', '2020-01-31 17:37:20', None] * ceil(c / 3))[:c]] * r,
                                      dtype='datetime64[h]')),
            ('a_datehour_2', np.array([(['2001-01-31 13:30:10', None, '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r,
                                      dtype='datetime64[h]')),
            ('a_datehour_3', np.array([([None, '2001-01-31 13:30:10', '2020-01-31 17:37:20'] * ceil(c / 3))[:c]] * r,
                                      dtype='datetime64[h]')),
            ('a_datehour_n', np.array([([None] * ceil(c))[:c]] * r, dtype='datetime64[h]')),
        ],
    }, DATATYPE.DT_IPADDR: {
        "scripts":
            '''
            x = array(IPADDR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([ipaddr(take(["192.168.1.13", "193.169.0.13"], a_c))])}
            a_ipaddr_0 = x        
            x = array(IPADDR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([ipaddr(take(["192.168.1.13", "193.169.0.13", ""], a_c))])}
            a_ipaddr_1 = x
            x = array(IPADDR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([ipaddr(take(["192.168.1.13", "", "193.169.0.13"], a_c))])}
            a_ipaddr_2 = x
            x = array(IPADDR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([ipaddr(take(["", "192.168.1.13", "193.169.0.13"], a_c))])}
            a_ipaddr_3 = x
            x = array(IPADDR[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([ipaddr(take(["", NULL], a_c))])}
            a_ipaddr_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_ipaddr_0', np.array([(["192.168.1.13", "193.169.0.13"] * ceil(c / 2))[:c]] * r)),
            ('a_ipaddr_1', np.array([(["192.168.1.13", "193.169.0.13", "0.0.0.0"] * ceil(c / 3))[:c]] * r)),
            ('a_ipaddr_2', np.array([(["192.168.1.13", "0.0.0.0", "193.169.0.13"] * ceil(c / 3))[:c]] * r)),
            ('a_ipaddr_3', np.array([(["0.0.0.0", "192.168.1.13", "193.169.0.13"] * ceil(c / 3))[:c]] * r)),
            ('a_ipaddr_n', np.array([(["0.0.0.0"] * ceil(c))[:c]] * r)),
        ],
    }, DATATYPE.DT_INT128: {
        "scripts":
            '''
            x = array(INT128[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([int128(take(["e1671797c52e15f763380b45e841ec32", "e5795797c52e15f763380b45e841ec32"], a_c))])}
            a_int128_0 = x        
            x = array(INT128[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([int128(take(["e1671797c52e15f763380b45e841ec32", "e5795797c52e15f763380b45e841ec32", ""], a_c))])}
            a_int128_1 = x
            x = array(INT128[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([int128(take(["e1671797c52e15f763380b45e841ec32", "", "e5795797c52e15f763380b45e841ec32"], a_c))])}
            a_int128_2 = x
            x = array(INT128[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([int128(take(["", "e1671797c52e15f763380b45e841ec32", "e5795797c52e15f763380b45e841ec32"], a_c))])}
            a_int128_3 = x
            x = array(INT128[], 0, a_s)
            for(i in 1..a_r){
                x = x.append!([int128(take(["", NULL], a_c))])}
            a_int128_n = x   
            '''
        ,
        "upload": [],
        "download": [
            ('a_int128_0', np.array(
                [(["e1671797c52e15f763380b45e841ec32", "e5795797c52e15f763380b45e841ec32"] * ceil(c / 2))[:c]] * r)),
            ('a_int128_1', np.array([(["e1671797c52e15f763380b45e841ec32", "e5795797c52e15f763380b45e841ec32",
                                       "00000000000000000000000000000000"] * ceil(c / 3))[:c]] * r)),
            ('a_int128_2', np.array([(["e1671797c52e15f763380b45e841ec32", "00000000000000000000000000000000",
                                       "e5795797c52e15f763380b45e841ec32"] * ceil(c / 3))[:c]] * r)),
            ('a_int128_3', np.array([(["00000000000000000000000000000000", "e1671797c52e15f763380b45e841ec32",
                                       "e5795797c52e15f763380b45e841ec32"] * ceil(c / 3))[:c]] * r)),
            ('a_int128_n', np.array([(["00000000000000000000000000000000"] * ceil(c))[:c]] * r)),
        ],
    }, DATATYPE.DT_BLOB: {
        "scripts":
            '''
            '''
        ,
        "upload": [],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL32: {
        "scripts": ''''''
        # not support in this version
        ,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_DECIMAL64: {
        "scripts": ''''''
        # not support in this version
        ,
        "upload": [],
        "download": [],
    }}

    # all type no way to upload

    # can not create a array vector of symbol

    # can not create a array vector of string

    # can not create a array vector of BLOB

    def get_script(*args, types=None, **kwargs):
        res = prefix1 + prefix2 + prefix3 + pythons[types]["scripts"]
        return res

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_Table_arrayVetcor(*args, n=100, typeTable="table", isShare=False, **kwargs):
    # tabletype: [table, streamTable]
    prefix = "v_n = {}\n".format(n)

    if not isShare:
        testTypeTable = typeTable
    else:
        testTypeTable = "share_" + typeTable

    shareScrips = """
        share table_bool_0 as share_table_bool_0

        share table_char_0 as share_table_char_0

        share table_short_0 as share_table_short_0

        share table_int_0 as share_table_int_0

        share table_long_0 as share_table_long_0

        share table_date_0 as share_table_date_0

        share table_month_0 as share_table_month_0

        share table_time_0 as share_table_time_0

        share table_minute_0 as share_table_minute_0

        share table_second_0 as share_table_second_0

        share table_datetime_0 as share_table_datetime_0

        share table_timestamp_0 as share_table_timestamp_0

        share table_nanotime_0 as share_table_nanotime_0

        share table_nanotimestamp_0 as share_table_nanotimestamp_0

        share table_float_0 as share_table_float_0

        share table_double_0 as share_table_double_0

        share table_datehour_0 as share_table_datehour_0
    """.replace("table", typeTable)

    pythons = {DATATYPE.DT_VOID: {
        # NO way to create a VOID Table
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_BOOL: {
        "scripts": """
            bool_0 = array(BOOL[], 0, v_n).append!([true false, NULL true false, true NULL false, true false NULL, NULL])
            table_bool_0 = table(bool_0)
        """.replace("table", typeTable),
        "upload": [
            ("{}_bool_0".format(testTypeTable), pd.DataFrame({"bool_0": [np.array([True, False], dtype="object"),
                                                                         np.array([None, True, False], dtype="object"),
                                                                         np.array([True, None, False], dtype="object"),
                                                                         np.array([True, False, None], dtype="object"),
                                                                         np.array([None], dtype="object")]})),
        ],
        "download": [
            ("{}_bool_0".format(testTypeTable), pd.DataFrame({"bool_0": [np.array([True, False]),
                                                                         np.array([None, True, False], dtype="object"),
                                                                         np.array([True, None, False], dtype="object"),
                                                                         np.array([True, False, None], dtype="object"),
                                                                         np.array([None], dtype="object")]})),
        ],
    }, DATATYPE.DT_CHAR: {
        "scripts": """BOOL
            char_0 = array(CHAR[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_char_0 = table(char_0)
        """.replace("table", typeTable),
        "upload": [
            # ("{}_char_0".format(testTypeTable), pd.DataFrame({"char_0": [np.array([1, 0], dtype="int8"), np.array([np.nan, 1, 0], dtype="int8"), np.array([1, np.nan, 0], dtype="int8"), np.array([1, 0, np.nan], dtype="int8"), np.array([np.nan], dtype="int8")]})),
        ],
        "download": [
            ("{}_char_0".format(testTypeTable), pd.DataFrame({"char_0": [np.array([1, 0], dtype="int8"),
                                                                         np.array([np.nan, 1, 0]),
                                                                         np.array([1, np.nan, 0]),
                                                                         np.array([1, 0, np.nan]),
                                                                         np.array([np.nan])]})),
        ],
    }, DATATYPE.DT_SHORT: {
        "scripts": """
            short_0 = array(SHORT[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_short_0 = table(short_0)
        """.replace("table", typeTable),
        "upload": [
            # ("{}_short_0".format(testTypeTable), pd.DataFrame({"short_0": [np.array([1, 0], dtype="int16"), np.array([np.nan, 1, 0], dtype="int16"), np.array([1, np.nan, 0], dtype="int16"), np.array([1, 0, np.nan], dtype="int16"), np.array([np.nan], dtype="int16")]})),
        ],
        "download": [
            ("{}_short_0".format(testTypeTable), pd.DataFrame({"short_0": [np.array([1, 0], dtype="int16"),
                                                                           np.array([np.nan, 1, 0]),
                                                                           np.array([1, np.nan, 0]),
                                                                           np.array([1, 0, np.nan]),
                                                                           np.array([np.nan])]})),
        ],
    }, DATATYPE.DT_INT: {
        "scripts": """
            int_0 = array(INT[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_int_0 = table(int_0)
        """.replace("table", typeTable),
        "upload": [
            # ("{}_int_0".format(testTypeTable), pd.DataFrame({"int_0": [np.array([1, 0], dtype="int32"), np.array([np.nan, 1, 0], dtype="int32"), np.array([1, np.nan, 0], dtype="int32"), np.array([1, 0, np.nan], dtype="int32"), np.array([np.nan], dtype="int32")]})),
        ],
        "download": [
            ("{}_int_0".format(testTypeTable), pd.DataFrame({"int_0": [np.array([1, 0], dtype="int32"),
                                                                       np.array([np.nan, 1, 0]),
                                                                       np.array([1, np.nan, 0]),
                                                                       np.array([1, 0, np.nan]), np.array([np.nan])]})),
        ],
    }, DATATYPE.DT_LONG: {
        "scripts": """
            long_0 = array(LONG[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_long_0 = table(long_0)
        """.replace("table", typeTable),
        "upload": [
            # ("{}_long_0".format(testTypeTable), pd.DataFrame({"long_0": [np.array([1, 0], dtype="int64"), np.array([np.nan, 1, 0], dtype="int64"), np.array([1, np.nan, 0], dtype="int64"), np.array([1, 0, np.nan], dtype="int64"), np.array([np.nan], dtype="int64")]})),
        ],
        "download": [
            ("{}_long_0".format(testTypeTable), pd.DataFrame({"long_0": [np.array([1, 0], dtype="int64"),
                                                                         np.array([np.nan, 1, 0]),
                                                                         np.array([1, np.nan, 0]),
                                                                         np.array([1, 0, np.nan]),
                                                                         np.array([np.nan])]})),
        ],
    }, DATATYPE.DT_DATE: {
        "scripts": """
            date_0 = array(DATE[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_date_0 = table(date_0)
        """.replace("table", typeTable),
        "upload": [
            ("{}_date_0".format(testTypeTable), pd.DataFrame({"date_0": [
                np.array(['1970-01-02', '1970-01-01'], dtype="datetime64[D]"),
                np.array([None, '1970-01-02', '1970-01-01'], dtype="datetime64[D]"),
                np.array(['1970-01-02', None, '1970-01-01'], dtype="datetime64[D]"),
                np.array(['1970-01-02', '1970-01-01', None], dtype="datetime64[D]"),
                np.array([None], dtype="datetime64[D]")]})),
        ],
        "download": [
            ("{}_date_0".format(testTypeTable), pd.DataFrame({"date_0": [
                np.array(['1970-01-02', '1970-01-01'], dtype="datetime64[D]"),
                np.array([None, '1970-01-02', '1970-01-01'], dtype="datetime64[D]"),
                np.array(['1970-01-02', None, '1970-01-01'], dtype="datetime64[D]"),
                np.array(['1970-01-02', '1970-01-01', None], dtype="datetime64[D]"),
                np.array([None], dtype="datetime64[D]")]})),
        ],
    }, DATATYPE.DT_MONTH: {
        "scripts": """
            month_0 = array(MONTH[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_month_0 = table(month_0)
        """.replace("table", typeTable),
        "upload": [
            ("{}_month_0".format(testTypeTable), pd.DataFrame({"month_0": [
                np.array(['0000-02', '0000-01'], dtype="datetime64[M]"),
                np.array([None, '0000-02', '0000-01'], dtype="datetime64[M]"),
                np.array(['0000-02', None, '0000-01'], dtype="datetime64[M]"),
                np.array(['0000-02', '0000-01', None], dtype="datetime64[M]"),
                np.array([None], dtype="datetime64[M]")]})),
        ],
        "download": [
            ("{}_month_0".format(testTypeTable), pd.DataFrame({"month_0": [
                np.array(['0000-02', '0000-01'], dtype="datetime64[M]"),
                np.array([None, '0000-02', '0000-01'], dtype="datetime64[M]"),
                np.array(['0000-02', None, '0000-01'], dtype="datetime64[M]"),
                np.array(['0000-02', '0000-01', None], dtype="datetime64[M]"),
                np.array([None], dtype="datetime64[M]")]})),
        ],
    }, DATATYPE.DT_TIME: {
        "scripts": """
            time_0 = array(TIME[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_time_0 = table(time_0)
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_TIME arrayVector
        ],
        "download": [
            ("{}_time_0".format(testTypeTable), pd.DataFrame({"time_0": [
                np.array(['1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000'], dtype="datetime64[ms]"),
                np.array([None, '1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000'], dtype="datetime64[ms]"),
                np.array(['1970-01-01 00:00:00.001', None, '1970-01-01 00:00:00.000'], dtype="datetime64[ms]"),
                np.array(['1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000', None], dtype="datetime64[ms]"),
                np.array([None], dtype="datetime64[ms]")]})),
        ],
    }, DATATYPE.DT_MINUTE: {
        "scripts": """
            minute_0 = array(MINUTE[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_minute_0 = table(minute_0)
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_TIME arrayVector
        ],
        "download": [
            ("{}_minute_0".format(testTypeTable), pd.DataFrame({"minute_0": [
                np.array(['1970-01-01 00:01', '1970-01-01 00:00'], dtype="datetime64[m]"),
                np.array([None, '1970-01-01 00:01', '1970-01-01 00:00'], dtype="datetime64[m]"),
                np.array(['1970-01-01 00:01', None, '1970-01-01 00:00'], dtype="datetime64[m]"),
                np.array(['1970-01-01 00:01', '1970-01-01 00:00', None], dtype="datetime64[m]"),
                np.array([None], dtype="datetime64[m]")]})),
        ],
    }, DATATYPE.DT_SECOND: {
        "scripts": """
            second_0 = array(SECOND[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_second_0 = table(second_0)
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_SECOND arrayVector
        ],
        "download": [
            ("{}_second_0".format(testTypeTable), pd.DataFrame({"second_0": [
                np.array(['1970-01-01 00:00:01', '1970-01-01 00:00:00'], dtype="datetime64[s]"),
                np.array([None, '1970-01-01 00:00:01', '1970-01-01 00:00:00'], dtype="datetime64[s]"),
                np.array(['1970-01-01 00:00:01', None, '1970-01-01 00:00:00'], dtype="datetime64[s]"),
                np.array(['1970-01-01 00:00:01', '1970-01-01 00:00:00', None], dtype="datetime64[s]"),
                np.array([None], dtype="datetime64[s]")]})),
        ],
    }, DATATYPE.DT_DATETIME: {
        "scripts": """
            datetime_0 = array(DATETIME[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_datetime_0 = table(datetime_0)
        """.replace("table", typeTable),
        "upload": [
            ("{}_datetime_0".format(testTypeTable), pd.DataFrame({"datetime_0": [
                np.array(['1970-01-01 00:00:01', '1970-01-01 00:00:00'], dtype="datetime64[s]"),
                np.array([None, '1970-01-01 00:00:01', '1970-01-01 00:00:00'], dtype="datetime64[s]"),
                np.array(['1970-01-01 00:00:01', None, '1970-01-01 00:00:00'], dtype="datetime64[s]"),
                np.array(['1970-01-01 00:00:01', '1970-01-01 00:00:00', None], dtype="datetime64[s]"),
                np.array([None], dtype="datetime64[s]")]})),
        ],
        "download": [
            ("{}_datetime_0".format(testTypeTable), pd.DataFrame({"datetime_0": [
                np.array(['1970-01-01 00:00:01', '1970-01-01 00:00:00'], dtype="datetime64[s]"),
                np.array([None, '1970-01-01 00:00:01', '1970-01-01 00:00:00'], dtype="datetime64[s]"),
                np.array(['1970-01-01 00:00:01', None, '1970-01-01 00:00:00'], dtype="datetime64[s]"),
                np.array(['1970-01-01 00:00:01', '1970-01-01 00:00:00', None], dtype="datetime64[s]"),
                np.array([None], dtype="datetime64[s]")]})),
        ],
    }, DATATYPE.DT_TIMESTAMP: {
        "scripts": """
            timestamp_0 = array(TIMESTAMP[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_timestamp_0 = table(timestamp_0)
        """.replace("table", typeTable),
        "upload": [
            ("{}_timestamp_0".format(testTypeTable), pd.DataFrame({"timestamp_0": [
                np.array(['1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000'], dtype="datetime64[ms]"),
                np.array([None, '1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000'], dtype="datetime64[ms]"),
                np.array(['1970-01-01 00:00:00.001', None, '1970-01-01 00:00:00.000'], dtype="datetime64[ms]"),
                np.array(['1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000', None], dtype="datetime64[ms]"),
                np.array([None], dtype="datetime64[ms]")]})),
        ],
        "download": [
            ("{}_timestamp_0".format(testTypeTable), pd.DataFrame({"timestamp_0": [
                np.array(['1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000'], dtype="datetime64[ms]"),
                np.array([None, '1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000'], dtype="datetime64[ms]"),
                np.array(['1970-01-01 00:00:00.001', None, '1970-01-01 00:00:00.000'], dtype="datetime64[ms]"),
                np.array(['1970-01-01 00:00:00.001', '1970-01-01 00:00:00.000', None], dtype="datetime64[ms]"),
                np.array([None], dtype="datetime64[ms]")]})),
        ],
    }, DATATYPE.DT_NANOTIME: {
        "scripts": """
            nanotime_0 = array(NANOTIME[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_nanotime_0 = table(nanotime_0)
        """.replace("table", typeTable),
        "upload": [
            # No way to upload DT_SECOND arrayVector
        ],
        "download": [
            ("{}_nanotime_0".format(testTypeTable), pd.DataFrame({"nanotime_0": [
                np.array(['1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.000000000'], dtype="datetime64[ns]"),
                np.array([None, '1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.000000000'],
                         dtype="datetime64[ns]"),
                np.array(['1970-01-01 00:00:00.000000001', None, '1970-01-01 00:00:00.000000000'],
                         dtype="datetime64[ns]"),
                np.array(['1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.000000000', None],
                         dtype="datetime64[ns]"), np.array([None], dtype="datetime64[ns]")]})),
        ],
    }, DATATYPE.DT_NANOTIMESTAMP: {
        "scripts": """
            nanotimestamp_0 = array(NANOTIMESTAMP[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_nanotimestamp_0 = table(nanotimestamp_0)
        """.replace("table", typeTable),
        "upload": [
            ("{}_nanotimestamp_0".format(testTypeTable), pd.DataFrame({"nanotimestamp_0": [
                np.array(['1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.000000000'], dtype="datetime64[ns]"),
                np.array([None, '1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.000000000'],
                         dtype="datetime64[ns]"),
                np.array(['1970-01-01 00:00:00.000000001', None, '1970-01-01 00:00:00.000000000'],
                         dtype="datetime64[ns]"),
                np.array(['1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.000000000', None],
                         dtype="datetime64[ns]"), np.array([None], dtype="datetime64[ns]")]})),
        ],
        "download": [
            ("{}_nanotimestamp_0".format(testTypeTable), pd.DataFrame({"nanotimestamp_0": [
                np.array(['1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.000000000'], dtype="datetime64[ns]"),
                np.array([None, '1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.000000000'],
                         dtype="datetime64[ns]"),
                np.array(['1970-01-01 00:00:00.000000001', None, '1970-01-01 00:00:00.000000000'],
                         dtype="datetime64[ns]"),
                np.array(['1970-01-01 00:00:00.000000001', '1970-01-01 00:00:00.000000000', None],
                         dtype="datetime64[ns]"), np.array([None], dtype="datetime64[ns]")]})),
        ],
    }, DATATYPE.DT_FLOAT: {
        "scripts": """
            float_0 = array(FLOAT[], 0, v_n).append!([1.11 0.0, NULL 1.11 0.0, 1.11 NULL 0.0, 1.11 0.0 NULL, NULL])
            table_float_0 = table(float_0)
        """.replace("table", typeTable),
        "upload": [
            # ("{}_float_0".format(testTypeTable), pd.DataFrame({"float_0": [np.array([1.11, 0.0], dtype="float32"), np.array([np.nan, 1.11, 0.0], dtype="float32"), np.array([1.11, np.nan, 0.0], dtype="float32"), np.array([1.11, 0.0, np.nan], dtype="float32"), np.array([np.nan], dtype="float32")]})),
        ],
        "download": [
            ("{}_float_0".format(testTypeTable), pd.DataFrame({"float_0": [np.array([1.11, 0.0], dtype="float32"),
                                                                           np.array([np.nan, 1.11, 0.0],
                                                                                    dtype="float32"),
                                                                           np.array([1.11, np.nan, 0.0],
                                                                                    dtype="float32"),
                                                                           np.array([1.11, 0.0, np.nan],
                                                                                    dtype="float32"),
                                                                           np.array([np.nan], dtype="float32")]})),
        ],
    }, DATATYPE.DT_DOUBLE: {
        "scripts": """
            double_0 = array(DOUBLE[], 0, v_n).append!([1.11 0.0, NULL 1.11 0.0, 1.11 NULL 0.0, 1.11 0.0 NULL, NULL])
            table_double_0 = table(double_0)
        """.replace("table", typeTable),
        "upload": [
            ("{}_double_0".format(testTypeTable), pd.DataFrame({"double_0": [np.array([1.11, 0.0], dtype="float64"),
                                                                             np.array([np.nan, 1.11, 0.0],
                                                                                      dtype="float64"),
                                                                             np.array([1.11, np.nan, 0.0],
                                                                                      dtype="float64"),
                                                                             np.array([1.11, 0.0, np.nan],
                                                                                      dtype="float64"),
                                                                             np.array([np.nan], dtype="float64")]})),
        ],
        "download": [
            ("{}_double_0".format(testTypeTable), pd.DataFrame({"double_0": [np.array([1.11, 0.0], dtype="float64"),
                                                                             np.array([np.nan, 1.11, 0.0],
                                                                                      dtype="float64"),
                                                                             np.array([1.11, np.nan, 0.0],
                                                                                      dtype="float64"),
                                                                             np.array([1.11, 0.0, np.nan],
                                                                                      dtype="float64"),
                                                                             np.array([np.nan], dtype="float64")]})),
        ],
    }, DATATYPE.DT_SYMBOL: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_STRING: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_UUID: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_DATEHOUR: {
        "scripts": """
            datehour_0 = array(DATEHOUR[], 0, v_n).append!([1 0, NULL 1 0, 1 NULL 0, 1 0 NULL, NULL])
            table_datehour_0 = table(datehour_0)
        """.replace("table", typeTable),
        "upload": [
            ("{}_datehour_0".format(testTypeTable), pd.DataFrame({"datehour_0": [
                np.array(['1970-01-01 01', '1970-01-01 00'], dtype="datetime64[h]"),
                np.array([None, '1970-01-01 01', '1970-01-01 00'], dtype="datetime64[h]"),
                np.array(['1970-01-01 01', None, '1970-01-01 00'], dtype="datetime64[h]"),
                np.array(['1970-01-01 01', '1970-01-01 00', None], dtype="datetime64[h]"),
                np.array([None], dtype="datetime64[h]")]})),
        ],
        "download": [
            ("{}_datehour_0".format(testTypeTable), pd.DataFrame({"datehour_0": [
                np.array(['1970-01-01 01', '1970-01-01 00'], dtype="datetime64[h]"),
                np.array([None, '1970-01-01 01', '1970-01-01 00'], dtype="datetime64[h]"),
                np.array(['1970-01-01 01', None, '1970-01-01 00'], dtype="datetime64[h]"),
                np.array(['1970-01-01 01', '1970-01-01 00', None], dtype="datetime64[h]"),
                np.array([None], dtype="datetime64[h]")]})),
        ],
    }, DATATYPE.DT_IPADDR: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_INT128: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_BLOB: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL32: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL64: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }}
    # NO SYMBOL TABLE
    # NO STRING TABLE
    # NO UUID TABLE
    # NO IPADDR TABLE
    # NO INT128 TABLE
    # NO BLOB TABLE
    # TODO: python decimal32
    # TODO: python decimal64

    for x in DATATYPE:
        prefix += str(pythons[x]["scripts"])

    if isShare:
        prefix += shareScrips

    def get_script(*args, **kwargs):
        return prefix

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_PartitionedTable_Append_Upsert(*args, n=100, db_name="", **kwargs):
    # tabletype: [table, streamTable, indexedTable, keyedTable]

    pythons = {DATATYPE.DT_VOID: {
        # NO way to create a VOID PartitionedTable
        "scripts": """
        """,
        "upload": [],
        "download": [],
    }, DATATYPE.DT_BOOL: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_bool'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_bool_0 = db.createPartitionedTable(table_bool_0,`partitionedTable_bool_0,`index).append!(table_bool_0)
            partitionedTable_bool_1 = db.createPartitionedTable(table_bool_1,`partitionedTable_bool_1,`index).append!(table_bool_1)
            partitionedTable_bool_2 = db.createPartitionedTable(table_bool_2,`partitionedTable_bool_2,`index).append!(table_bool_2)
            partitionedTable_bool_3 = db.createPartitionedTable(table_bool_3,`partitionedTable_bool_3,`index).append!(table_bool_3)

            test_partitionedTable_bool_0 = db.createPartitionedTable(test_table_bool_0,`test_partitionedTable_bool_0,`index).append!(test_table_bool_0)
            test_partitionedTable_bool_1 = db.createPartitionedTable(test_table_bool_1,`test_partitionedTable_bool_1,`index).append!(test_table_bool_1)
            test_partitionedTable_bool_2 = db.createPartitionedTable(test_table_bool_2,`test_partitionedTable_bool_2,`index).append!(test_table_bool_2)
            test_partitionedTable_bool_3 = db.createPartitionedTable(test_table_bool_3,`test_partitionedTable_bool_3,`index).append!(test_table_bool_3)
        """,
        "upload": [
            # ("partitionedTable_bool_0", pd.DataFrame({"index": np.arange(1, n+1),
            #                                "bool_0": np.tile(np.array([True, False], dtype="bool"), ceil(n/2))[:n],
            #                                "bool_1": np.tile(np.array([True, False, None], dtype="object"), ceil(n/3))[:n],
            #                                "bool_2": np.tile(np.array([None, None, None], dtype="object"), ceil(n/3))[:n]
            #                                })),
            ("partitionedTable_bool_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "bool_0": np.tile(np.array([True, False], dtype="bool"),
                                                                        ceil(n / 2))[:n],
                                                      })),
            # ("partitionedTable_bool_2",  pd.DataFrame({"index": np.arange(1, 2),
            #                                 "bool_0": np.array([True], dtype="bool"),
            #                                 "bool_1": np.array([False], dtype="bool"),
            #                                 "bool_2": np.array([None], dtype="object")
            #                                 })),
            ("partitionedTable_bool_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                      "bool_0": np.array([], dtype="bool"),
                                                      "bool_1": np.array([], dtype="bool"),
                                                      "bool_2": np.array([], dtype="bool")
                                                      })),
        ],
        "download": [
            ("partitionedTable_bool_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "bool_0": np.tile(np.array([True, False], dtype="bool"),
                                                                        ceil(n / 2))[:n],
                                                      "bool_1": np.tile(np.array([True, False, None], dtype="object"),
                                                                        ceil(n / 3))[:n],
                                                      "bool_2": np.tile(np.array([None, None, None], dtype="object"),
                                                                        ceil(n / 3))[:n]
                                                      })),
            ("partitionedTable_bool_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "bool_0": np.tile(np.array([True, False], dtype="bool"),
                                                                        ceil(n / 2))[:n],
                                                      })),
            ("partitionedTable_bool_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                      "bool_0": np.array([True], dtype="bool"),
                                                      "bool_1": np.array([False], dtype="bool"),
                                                      "bool_2": np.array([None], dtype="object")
                                                      })),
            ("partitionedTable_bool_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                      "bool_0": np.array([], dtype="bool"),
                                                      "bool_1": np.array([], dtype="bool"),
                                                      "bool_2": np.array([], dtype="bool")
                                                      })),
        ],
    }, DATATYPE.DT_CHAR: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_char'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_char_0 = db.createPartitionedTable(table_char_0,`partitionedTable_char_0,`index).append!(table_char_0)
            partitionedTable_char_1 = db.createPartitionedTable(table_char_1,`partitionedTable_char_1,`index).append!(table_char_1)
            partitionedTable_char_2 = db.createPartitionedTable(table_char_2,`partitionedTable_char_2,`index).append!(table_char_2)
            partitionedTable_char_3 = db.createPartitionedTable(table_char_3,`partitionedTable_char_3,`index).append!(table_char_3)

            test_partitionedTable_char_0 = db.createPartitionedTable(test_table_char_0,`test_partitionedTable_char_0,`index).append!(test_table_char_0)
            test_partitionedTable_char_1 = db.createPartitionedTable(test_table_char_1,`test_partitionedTable_char_1,`index).append!(test_table_char_1)
            test_partitionedTable_char_2 = db.createPartitionedTable(test_table_char_2,`test_partitionedTable_char_2,`index).append!(test_table_char_2)
            test_partitionedTable_char_3 = db.createPartitionedTable(test_table_char_3,`test_partitionedTable_char_3,`index).append!(test_table_char_3)
        """,
        "upload": [
            # No way to create a int8 None in numpy
            # ("partitionedTable_char_0", pd.DataFrame({"index": np.arange(1, n+1),
            #                                                   "char_0": np.tile(np.array([19, 65], dtype="int8"), ceil(n/2))[:n],
            #                                                   "char_1": np.tile(np.array([19, 65, None], dtype="int8"), ceil(n/3))[:n],
            #                                                   "char_2": np.tile(np.array([None, None, None], dtype="int8"), ceil(n/3))[:n]
            #                                                   })),
            ("partitionedTable_char_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "char_0": np.tile(np.array([19, 65], dtype="int8"), ceil(n / 2))[
                                                                :n],
                                                      })),
            # ("partitionedTable_char_2",  pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
            #                                            "char_0": np.array([19], dtype="int8"),
            #                                            "char_1": np.array([65], dtype="int8"),
            #                                            "char_2": np.array([None], dtype="int8")
            #                                            })),
            ("partitionedTable_char_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                      "char_0": np.array([], dtype="int8"),
                                                      "char_1": np.array([], dtype="int8"),
                                                      "char_2": np.array([], dtype="int8")
                                                      })),
        ],
        "download": [
            ("partitionedTable_char_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "char_0": np.tile(np.array([19, 65], dtype=np.int8), ceil(n / 2))[
                                                                :n],
                                                      "char_1": np.tile(np.array([19, 65, np.nan]), ceil(n / 3))[:n],
                                                      "char_2": np.tile(np.array([np.nan, np.nan, np.nan]),
                                                                        ceil(n / 3))[:n]
                                                      })),
            ("partitionedTable_char_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "char_0": np.tile(np.array([19, 65], dtype=np.int8), ceil(n / 2))[
                                                                :n],
                                                      })),
            ("partitionedTable_char_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                      "char_0": np.array([19], dtype=np.int8),
                                                      "char_1": np.array([65], dtype=np.int8),
                                                      "char_2": np.array([np.nan])
                                                      })),
            ("partitionedTable_char_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                      "char_0": np.array([], dtype=np.int8),
                                                      "char_1": np.array([], dtype=np.int8),
                                                      "char_2": np.array([], dtype=np.int8)
                                                      })),
        ],
    }, DATATYPE.DT_SHORT: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_short'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5])
            partitionedTable_short_0 = db.createPartitionedTable(table_short_0,`partitionedTable_short_0,`index).append!(table_short_0)
            partitionedTable_short_1 = db.createPartitionedTable(table_short_1,`partitionedTable_short_1,`index).append!(table_short_1)
            partitionedTable_short_2 = db.createPartitionedTable(table_short_2,`partitionedTable_short_2,`index).append!(table_short_2)
            partitionedTable_short_3 = db.createPartitionedTable(table_short_3,`partitionedTable_short_3,`index).append!(table_short_3)

            test_partitionedTable_short_0 = db.createPartitionedTable(test_table_short_0,`test_partitionedTable_short_0,`index).append!(test_table_short_0)
            test_partitionedTable_short_1 = db.createPartitionedTable(test_table_short_1,`test_partitionedTable_short_1,`index).append!(test_table_short_1)
            test_partitionedTable_short_2 = db.createPartitionedTable(test_table_short_2,`test_partitionedTable_short_2,`index).append!(test_table_short_2)
            test_partitionedTable_short_3 = db.createPartitionedTable(test_table_short_3,`test_partitionedTable_short_3,`index).append!(test_table_short_3)
        """,
        "upload": [
            # No way to create a int16 None in numpy
            # ("partitionedTable_short_0", pd.DataFrame({"index": np.arange(1, n+1),
            #                                            "short_0": np.tile(np.array([1, 0], dtype="int16"), ceil(n/2))[:n],
            #                                            'short_1': np.tile(np.array([1, 0, None], dtype="int16"), ceil(n/3))[:n],
            #                                            'short_2': np.tile(np.array([None, None, None], dtype="int16"), ceil(n/3))[:n]
            #                                            })),
            ("partitionedTable_short_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "short_0": np.tile(np.array([1, 0], dtype="int16"), ceil(n / 2))[
                                                                  :n],
                                                       })),
            # ("partitionedTable_short_2",  pd.DataFrame({"index": np.arange(1, 2),
            #                                             "short_0": np.array([1], dtype="int16"),
            #                                             'short_1': np.array([0], dtype="int16"),
            #                                             'short_2': np.array([None], dtype="int16")
            #                                             })),
            ("partitionedTable_short_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                       "short_0": np.array([], dtype="int16"),
                                                       "short_1": np.array([], dtype="int16"),
                                                       "short_2": np.array([], dtype="int16")
                                                       })),
        ],
        "download": [
            ("partitionedTable_short_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "short_0": np.tile(np.array([1, 0], dtype=np.int16),
                                                                          ceil(n / 2))[:n],
                                                       "short_1": np.tile(np.array([1, 0, np.nan]), ceil(n / 3))[:n],
                                                       "short_2": np.tile(np.array([np.nan, np.nan, np.nan]),
                                                                          ceil(n / 3))[:n]
                                                       })),
            ("partitionedTable_short_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "short_0": np.tile(np.array([1, 0], dtype=np.int16),
                                                                          ceil(n / 2))[:n],
                                                       })),
            ("partitionedTable_short_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                       "short_0": np.array([1], dtype=np.int16),
                                                       "short_1": np.array([0], dtype=np.int16),
                                                       "short_2": np.array([np.nan])
                                                       })),
            ("partitionedTable_short_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                       "short_0": np.array([], dtype=np.int16),
                                                       "short_1": np.array([], dtype=np.int16),
                                                       "short_2": np.array([], dtype=np.int16)
                                                       })),
        ],
    }, DATATYPE.DT_INT: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_int'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5])
            partitionedTable_int_0 = db.createPartitionedTable(table_int_0,`partitionedTable_int_0,`index).append!(table_int_0)
            partitionedTable_int_1 = db.createPartitionedTable(table_int_1,`partitionedTable_int_1,`index).append!(table_int_1)
            partitionedTable_int_2 = db.createPartitionedTable(table_int_2,`partitionedTable_int_2,`index).append!(table_int_2)
            partitionedTable_int_3 = db.createPartitionedTable(table_int_3,`partitionedTable_int_3,`index).append!(table_int_3)

            test_partitionedTable_int_0 = db.createPartitionedTable(test_table_int_0,`test_partitionedTable_int_0,`index).append!(test_table_int_0)
            test_partitionedTable_int_1 = db.createPartitionedTable(test_table_int_1,`test_partitionedTable_int_1,`index).append!(test_table_int_1)
            test_partitionedTable_int_2 = db.createPartitionedTable(test_table_int_2,`test_partitionedTable_int_2,`index).append!(test_table_int_2)
            test_partitionedTable_int_3 = db.createPartitionedTable(test_table_int_3,`test_partitionedTable_int_3,`index).append!(test_table_int_3)
        """,
        "upload": [
            # No way to create a int32 None in numpy
            # ("partitionedTable_int_0", pd.DataFrame({"index": np.arange(1, n+1),
            #                                            "int_0": np.tile(np.array([1, 0], dtype="int32"), ceil(n/2))[:n],
            #                                            'int_1': np.tile(np.array([1, 0, None], dtype="int32"), ceil(n/3))[:n],
            #                                            'int_2': np.tile(np.array([None, None, None], dtype="int32"), ceil(n/3))[:n]
            #                                            })),
            ("partitionedTable_int_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                     "int_0": np.tile(np.array([1, 0], dtype="int32"), ceil(n / 2))[:n],
                                                     })),
            # ("partitionedTable_int_2",  pd.DataFrame({"index": np.arange(1, 2),
            #                                             "int_0": np.array([1], dtype="int32"),
            #                                             'int_1': np.array([0], dtype="int32"),
            #                                             'int_2': np.array([None], dtype="int32")
            #                                             })),
            ("partitionedTable_int_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                     "int_0": np.array([], dtype="int32"),
                                                     "int_1": np.array([], dtype="int32"),
                                                     "int_2": np.array([], dtype="int32")
                                                     })),
        ],
        "download": [
            ("partitionedTable_int_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                     "int_0": np.tile(np.array([1, 0], dtype=np.int32), ceil(n / 2))[
                                                              :n],
                                                     "int_1": np.tile(np.array([1, 0, np.nan]), ceil(n / 3))[:n],
                                                     "int_2": np.tile(np.array([np.nan, np.nan, np.nan]), ceil(n / 3))[
                                                              :n]
                                                     })),
            ("partitionedTable_int_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                     "int_0": np.tile(np.array([1, 0], dtype=np.int32), ceil(n / 2))[
                                                              :n],
                                                     })),
            ("partitionedTable_int_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                     "int_0": np.array([1], dtype=np.int32),
                                                     "int_1": np.array([0], dtype=np.int32),
                                                     "int_2": np.array([np.nan])
                                                     })),
            ("partitionedTable_int_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                     "int_0": np.array([], dtype=np.int32),
                                                     "int_1": np.array([], dtype=np.int32),
                                                     "int_2": np.array([], dtype=np.int32)
                                                     })),

        ],
    }, DATATYPE.DT_LONG: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_long'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5])
            partitionedTable_long_0 = db.createPartitionedTable(table_long_0,`partitionedTable_long_0,`index).append!(table_long_0)
            partitionedTable_long_1 = db.createPartitionedTable(table_long_1,`partitionedTable_long_1,`index).append!(table_long_1)
            partitionedTable_long_2 = db.createPartitionedTable(table_long_2,`partitionedTable_long_2,`index).append!(table_long_2)
            partitionedTable_long_3 = db.createPartitionedTable(table_long_3,`partitionedTable_long_3,`index).append!(table_long_3)

            test_partitionedTable_long_0 = db.createPartitionedTable(test_table_long_0,`test_partitionedTable_long_0,`index).append!(test_table_long_0)
            test_partitionedTable_long_1 = db.createPartitionedTable(test_table_long_1,`test_partitionedTable_long_1,`index).append!(test_table_long_1)
            test_partitionedTable_long_2 = db.createPartitionedTable(test_table_long_2,`test_partitionedTable_long_2,`index).append!(test_table_long_2)
            test_partitionedTable_long_3 = db.createPartitionedTable(test_table_long_3,`test_partitionedTable_long_3,`index).append!(test_table_long_3)
        """,
        "upload": [
            # No way to create a int64 None in numpy
            # ("partitionedTable_long_0", pd.DataFrame({"index": np.arange(1, n+1),
            #                                            "long_0": np.tile(np.array([1, 0], dtype="int64"), ceil(n/2))[:n],
            #                                            'long_1': np.tile(np.array([1, 0, None], dtype="int64"), ceil(n/3))[:n],
            #                                            'long_2': np.tile(np.array([None, None, None], dtype="int64"), ceil(n/3))[:n]
            #                                            })),
            ("partitionedTable_long_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "long_0": np.tile(np.array([1, 0], dtype="int64"), ceil(n / 2))[
                                                                :n],
                                                      })),
            # ("partitionedTable_long_2",  pd.DataFrame({"index": np.arange(1, 2),
            #                                             "long_0": np.array([1], dtype="int64"),
            #                                             'long_1': np.array([0], dtype="int64"),
            #                                             'long_2': np.array([None], dtype="int64")
            #                                             })),
            ("partitionedTable_long_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                      "long_0": np.array([], dtype="int64"),
                                                      "long_1": np.array([], dtype="int64"),
                                                      "long_2": np.array([], dtype="int64")
                                                      })),
        ],
        "download": [
            ("partitionedTable_long_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "long_0": np.tile(np.array([1, 0], dtype=np.int64), ceil(n / 2))[
                                                                :n],
                                                      "long_1": np.tile(np.array([1, 0, np.nan]), ceil(n / 3))[:n],
                                                      "long_2": np.tile(np.array([np.nan, np.nan, np.nan]),
                                                                        ceil(n / 3))[:n]
                                                      })),
            ("partitionedTable_long_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "long_0": np.tile(np.array([1, 0], dtype=np.int64), ceil(n / 2))[
                                                                :n],
                                                      })),
            ("partitionedTable_long_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                      "long_0": np.array([1], dtype="int64"),
                                                      "long_1": np.array([0], dtype="int64"),
                                                      "long_2": np.array([np.nan])
                                                      })),
            ("partitionedTable_long_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                      "long_0": np.array([], dtype="int64"),
                                                      "long_1": np.array([], dtype="int64"),
                                                      "long_2": np.array([], dtype="int64")
                                                      })),
        ],
    }, DATATYPE.DT_DATE: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_date'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5])
            partitionedTable_date_0 = db.createPartitionedTable(table_date_0,`partitionedTable_date_0,`index).append!(table_date_0)
            partitionedTable_date_1 = db.createPartitionedTable(table_date_1,`partitionedTable_date_1,`index).append!(table_date_1)
            partitionedTable_date_2 = db.createPartitionedTable(table_date_2,`partitionedTable_date_2,`index).append!(table_date_2)
            partitionedTable_date_3 = db.createPartitionedTable(table_date_3,`partitionedTable_date_3,`index).append!(table_date_3)

            test_partitionedTable_date_0 = db.createPartitionedTable(test_table_date_0,`test_partitionedTable_date_0,`index).append!(test_table_date_0)
            test_partitionedTable_date_1 = db.createPartitionedTable(test_table_date_1,`test_partitionedTable_date_1,`index).append!(test_table_date_1)
            test_partitionedTable_date_2 = db.createPartitionedTable(test_table_date_2,`test_partitionedTable_date_2,`index).append!(test_table_date_2)
            test_partitionedTable_date_3 = db.createPartitionedTable(test_table_date_3,`test_partitionedTable_date_3,`index).append!(test_table_date_3)
        """,
        "upload": [
            ("partitionedTable_date_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "date_0": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                 dtype="datetime64[D]"), ceil(n / 2))[
                                                                :n],
                                                      "date_1": np.tile(
                                                          np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                   dtype="datetime64[D]"), ceil(n / 3))[:n],
                                                      "date_2": np.tile(
                                                          np.array([None, None, None], dtype="datetime64[D]"),
                                                          ceil(n / 3))[:n]
                                                      })),
            ("partitionedTable_date_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "date_0": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                 dtype="datetime64[D]"), ceil(n / 2))[
                                                                :n],
                                                      })),
            ("partitionedTable_date_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                      "date_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                         dtype="datetime64[D]"),
                                                      "date_1": np.array([0], dtype="datetime64[D]"),
                                                      "date_2": np.array([None], dtype="datetime64[D]")
                                                      })),
            ("partitionedTable_date_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                      "date_0": np.array([], dtype="datetime64[D]"),
                                                      "date_1": np.array([], dtype="datetime64[D]"),
                                                      "date_2": np.array([], dtype="datetime64[D]")
                                                      })),
        ],
        "download": [
            ("partitionedTable_date_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "date_0": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                 dtype="datetime64[D]"), ceil(n / 2))[
                                                                :n],
                                                      "date_1": np.tile(
                                                          np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                   dtype="datetime64[D]"), ceil(n / 3))[:n],
                                                      "date_2": np.tile(
                                                          np.array([None, None, None], dtype="datetime64[D]"),
                                                          ceil(n / 3))[:n]
                                                      })),
            ("partitionedTable_date_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "date_0": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                 dtype="datetime64[D]"), ceil(n / 2))[
                                                                :n],
                                                      })),
            ("partitionedTable_date_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                      "date_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                         dtype="datetime64[D]"),
                                                      "date_1": np.array([0], dtype="datetime64[D]"),
                                                      "date_2": np.array([None], dtype="datetime64[D]")
                                                      })),
            ("partitionedTable_date_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                      "date_0": np.array([], dtype="datetime64[D]"),
                                                      "date_1": np.array([], dtype="datetime64[D]"),
                                                      "date_2": np.array([], dtype="datetime64[D]")
                                                      })),
        ],
    }, DATATYPE.DT_MONTH: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_month'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5])
            partitionedTable_month_0 = db.createPartitionedTable(table_month_0,`partitionedTable_month_0,`index).append!(table_month_0)
            partitionedTable_month_1 = db.createPartitionedTable(table_month_1,`partitionedTable_month_1,`index).append!(table_month_1)
            partitionedTable_month_2 = db.createPartitionedTable(table_month_2,`partitionedTable_month_2,`index).append!(table_month_2)
            partitionedTable_month_3 = db.createPartitionedTable(table_month_3,`partitionedTable_month_3,`index).append!(table_month_3)

            test_partitionedTable_month_0 = db.createPartitionedTable(test_table_month_0,`test_partitionedTable_month_0,`index).append!(test_table_month_0)
            test_partitionedTable_month_1 = db.createPartitionedTable(test_table_month_1,`test_partitionedTable_month_1,`index).append!(test_table_month_1)
            test_partitionedTable_month_2 = db.createPartitionedTable(test_table_month_2,`test_partitionedTable_month_2,`index).append!(test_table_month_2)
            test_partitionedTable_month_3 = db.createPartitionedTable(test_table_month_3,`test_partitionedTable_month_3,`index).append!(test_table_month_3)
        """,
        "upload": [
            ("partitionedTable_month_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "month_0": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                   dtype="datetime64[M]"), ceil(n / 2))[
                                                                  :n],
                                                       "month_1": np.tile(
                                                           np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                    dtype="datetime64[M]"), ceil(n / 3))[:n],
                                                       "month_2": np.tile(
                                                           np.array([None, None, None], dtype="datetime64[M]"),
                                                           ceil(n / 3))[:n]
                                                       })),
            ("partitionedTable_month_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "month_0": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                   dtype="datetime64[M]"), ceil(n / 2))[
                                                                  :n],
                                                       "month_1": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                   dtype="datetime64[M]"), ceil(n / 2))[
                                                                  :n],
                                                       "month_2": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                   dtype="datetime64[M]"), ceil(n / 2))[
                                                                  :n],
                                                       })),
            ("partitionedTable_month_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                       "month_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                           dtype="datetime64[M]"),
                                                       "month_1": np.array([0], dtype="datetime64[M]"),
                                                       "month_2": np.array([None], dtype="datetime64[M]")
                                                       })),
            ("partitionedTable_month_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                       "month_0": np.array([], dtype="datetime64[M]"),
                                                       "month_1": np.array([], dtype="datetime64[M]"),
                                                       "month_2": np.array([], dtype="datetime64[M]")
                                                       })),
        ],
        "download": [
            ("partitionedTable_month_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "month_0": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                   dtype="datetime64[M]"), ceil(n / 2))[
                                                                  :n],
                                                       "month_1": np.tile(
                                                           np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                    dtype="datetime64[M]"), ceil(n / 3))[:n],
                                                       "month_2": np.tile(
                                                           np.array([None, None, None], dtype="datetime64[M]"),
                                                           ceil(n / 3))[:n]
                                                       })),
            ("partitionedTable_month_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "month_0": np.tile(np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                                   dtype="datetime64[M]"), ceil(n / 2))[
                                                                  :n],
                                                       })),
            ("partitionedTable_month_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                       "month_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                           dtype="datetime64[M]"),
                                                       "month_1": np.array([0], dtype="datetime64[M]"),
                                                       "month_2": np.array([None], dtype="datetime64[M]")
                                                       })),
            ("partitionedTable_month_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                       "month_0": np.array([], dtype="datetime64[M]"),
                                                       "month_1": np.array([], dtype="datetime64[M]"),
                                                       "month_2": np.array([], dtype="datetime64[M]")
                                                       })),
        ],
    }, DATATYPE.DT_TIME: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_time'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_time_0 = db.createPartitionedTable(table_time_0,`partitionedTable_time_0,`index).append!(table_time_0)
            partitionedTable_time_1 = db.createPartitionedTable(table_time_1,`partitionedTable_time_1,`index).append!(table_time_1)
            partitionedTable_time_2 = db.createPartitionedTable(table_time_2,`partitionedTable_time_2,`index).append!(table_time_2)
            partitionedTable_time_3 = db.createPartitionedTable(table_time_3,`partitionedTable_time_3,`index).append!(table_time_3)

            test_partitionedTable_time_0 = db.createPartitionedTable(test_table_time_0,`test_partitionedTable_time_0,`index).append!(test_table_time_0)
            test_partitionedTable_time_1 = db.createPartitionedTable(test_table_time_1,`test_partitionedTable_time_1,`index).append!(test_table_time_1)
            test_partitionedTable_time_2 = db.createPartitionedTable(test_table_time_2,`test_partitionedTable_time_2,`index).append!(test_table_time_2)
            test_partitionedTable_time_3 = db.createPartitionedTable(test_table_time_3,`test_partitionedTable_time_3,`index).append!(test_table_time_3)
        """,
        "upload": [
            # no way to upload DT_TIME table
        ],
        "download": [
            ("partitionedTable_time_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "time_0": np.tile(np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                                 dtype="datetime64[ms]"), ceil(n / 2))[
                                                                :n],
                                                      "time_1": np.tile(
                                                          np.array(["1970-01-01 12:30:56.123456789", 0, None],
                                                                   dtype="datetime64[ms]"), ceil(n / 3))[:n],
                                                      "time_2": np.tile(
                                                          np.array([None, None, None], dtype="datetime64[ms]"),
                                                          ceil(n / 3))[:n]
                                                      })),
            ("partitionedTable_time_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "time_0": np.tile(np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                                 dtype="datetime64[ms]"), ceil(n / 2))[
                                                                :n],
                                                      })),
            ("partitionedTable_time_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                      "time_0": np.array(["1970-01-01 12:30:56.123456789"],
                                                                         dtype="datetime64[ms]"),
                                                      "time_1": np.array([0], dtype="datetime64[ms]"),
                                                      "time_2": np.array([None], dtype="datetime64[ms]")
                                                      })),
            ("partitionedTable_time_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                      "time_0": np.array([], dtype="datetime64[ms]"),
                                                      "time_1": np.array([], dtype="datetime64[ms]"),
                                                      "time_2": np.array([], dtype="datetime64[ms]")
                                                      })),
        ],
    }, DATATYPE.DT_MINUTE: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_minute'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_minute_0 = db.createPartitionedTable(table_minute_0,`partitionedTable_minute_0,`index).append!(table_minute_0)
            partitionedTable_minute_1 = db.createPartitionedTable(table_minute_1,`partitionedTable_minute_1,`index).append!(table_minute_1)
            partitionedTable_minute_2 = db.createPartitionedTable(table_minute_2,`partitionedTable_minute_2,`index).append!(table_minute_2)
            partitionedTable_minute_3 = db.createPartitionedTable(table_minute_3,`partitionedTable_minute_3,`index).append!(table_minute_3)

            test_partitionedTable_minute_0 = db.createPartitionedTable(test_table_minute_0,`test_partitionedTable_minute_0,`index).append!(test_table_minute_0)
            test_partitionedTable_minute_1 = db.createPartitionedTable(test_table_minute_1,`test_partitionedTable_minute_1,`index).append!(test_table_minute_1)
            test_partitionedTable_minute_2 = db.createPartitionedTable(test_table_minute_2,`test_partitionedTable_minute_2,`index).append!(test_table_minute_2)
            test_partitionedTable_minute_3 = db.createPartitionedTable(test_table_minute_3,`test_partitionedTable_minute_3,`index).append!(test_table_minute_3)
        """,
        "upload": [
            # no way to upload DT_MINUTE table
        ],
        "download": [
            ("partitionedTable_minute_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "minute_0": np.tile(
                                                            np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                     dtype="datetime64[m]"), ceil(n / 2))[:n],
                                                        "minute_1": np.tile(
                                                            np.array(["1970-01-01 12:30:56.123456789", 0, None],
                                                                     dtype="datetime64[m]"), ceil(n / 3))[:n],
                                                        "minute_2": np.tile(
                                                            np.array([None, None, None], dtype="datetime64[m]"),
                                                            ceil(n / 3))[:n]
                                                        })),
            ("partitionedTable_minute_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "minute_0": np.tile(
                                                            np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                     dtype="datetime64[m]"), ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_minute_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                        "minute_0": np.array(["1970-01-01 12:30:56.123456789"],
                                                                             dtype="datetime64[m]"),
                                                        "minute_1": np.array([0], dtype="datetime64[m]"),
                                                        "minute_2": np.array([None], dtype="datetime64[m]")
                                                        })),
            ("partitionedTable_minute_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                        "minute_0": np.array([], dtype="datetime64[m]"),
                                                        "minute_1": np.array([], dtype="datetime64[m]"),
                                                        "minute_2": np.array([], dtype="datetime64[m]")
                                                        })),
        ],
    }, DATATYPE.DT_SECOND: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_second'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_second_0 = db.createPartitionedTable(table_second_0,`partitionedTable_second_0,`index).append!(table_second_0)
            partitionedTable_second_1 = db.createPartitionedTable(table_second_1,`partitionedTable_second_1,`index).append!(table_second_1)
            partitionedTable_second_2 = db.createPartitionedTable(table_second_2,`partitionedTable_second_2,`index).append!(table_second_2)
            partitionedTable_second_3 = db.createPartitionedTable(table_second_3,`partitionedTable_second_3,`index).append!(table_second_3)

            test_partitionedTable_second_0 = db.createPartitionedTable(test_table_second_0,`test_partitionedTable_second_0,`index).append!(test_table_second_0)
            test_partitionedTable_second_1 = db.createPartitionedTable(test_table_second_1,`test_partitionedTable_second_1,`index).append!(test_table_second_1)
            test_partitionedTable_second_2 = db.createPartitionedTable(test_table_second_2,`test_partitionedTable_second_2,`index).append!(test_table_second_2)
            test_partitionedTable_second_3 = db.createPartitionedTable(test_table_second_3,`test_partitionedTable_second_3,`index).append!(test_table_second_3)
        """,
        "upload": [
            # no way to upload DT_SECOND table
        ],
        "download": [
            ("partitionedTable_second_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "second_0": np.tile(
                                                            np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                     dtype="datetime64[s]"), ceil(n / 2))[:n],
                                                        "second_1": np.tile(
                                                            np.array(["1970-01-01 12:30:56.123456789", 0, None],
                                                                     dtype="datetime64[s]"), ceil(n / 3))[:n],
                                                        "second_2": np.tile(
                                                            np.array([None, None, None], dtype="datetime64[s]"),
                                                            ceil(n / 3))[:n]
                                                        })),
            ("partitionedTable_second_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "second_0": np.tile(
                                                            np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                     dtype="datetime64[s]"), ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_second_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                        "second_0": np.array(["1970-01-01 12:30:56.123456789"],
                                                                             dtype="datetime64[s]"),
                                                        "second_1": np.array([0], dtype="datetime64[s]"),
                                                        "second_2": np.array([None], dtype="datetime64[s]")
                                                        })),
            ("partitionedTable_second_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                        "second_0": np.array([], dtype="datetime64[s]"),
                                                        "second_1": np.array([], dtype="datetime64[s]"),
                                                        "second_2": np.array([], dtype="datetime64[s]")
                                                        })),
        ],
    }, DATATYPE.DT_DATETIME: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_datetime'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_datetime_0 = db.createPartitionedTable(table_datetime_0,`partitionedTable_datetime_0,`index).append!(table_datetime_0)
            partitionedTable_datetime_1 = db.createPartitionedTable(table_datetime_1,`partitionedTable_datetime_1,`index).append!(table_datetime_1)
            partitionedTable_datetime_2 = db.createPartitionedTable(table_datetime_2,`partitionedTable_datetime_2,`index).append!(table_datetime_2)
            partitionedTable_datetime_3 = db.createPartitionedTable(table_datetime_3,`partitionedTable_datetime_3,`index).append!(table_datetime_3)

            test_partitionedTable_datetime_0 = db.createPartitionedTable(test_table_datetime_0,`test_partitionedTable_datetime_0,`index).append!(test_table_datetime_0)
            test_partitionedTable_datetime_1 = db.createPartitionedTable(test_table_datetime_1,`test_partitionedTable_datetime_1,`index).append!(test_table_datetime_1)
            test_partitionedTable_datetime_2 = db.createPartitionedTable(test_table_datetime_2,`test_partitionedTable_datetime_2,`index).append!(test_table_datetime_2)
            test_partitionedTable_datetime_3 = db.createPartitionedTable(test_table_datetime_3,`test_partitionedTable_datetime_3,`index).append!(test_table_datetime_3)
        """,
        "upload": [
            ("partitionedTable_datetime_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "datetime_0": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[s]"), ceil(n / 2))[:n],
                                                          "datetime_1": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                       dtype="datetime64[s]"), ceil(n / 3))[:n],
                                                          "datetime_2": np.tile(
                                                              np.array([None, None, None], dtype="datetime64[s]"),
                                                              ceil(n / 3))[:n]
                                                          })),
            ("partitionedTable_datetime_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "datetime_0": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[s]"), ceil(n / 2))[:n],
                                                          })),
            ("partitionedTable_datetime_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                          "datetime_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                                 dtype="datetime64[s]"),
                                                          "datetime_1": np.array([0], dtype="datetime64[s]"),
                                                          "datetime_2": np.array([None], dtype="datetime64[s]")
                                                          })),
            ("partitionedTable_datetime_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                          "datetime_0": np.array([], dtype="datetime64[s]"),
                                                          "datetime_1": np.array([], dtype="datetime64[s]"),
                                                          "datetime_2": np.array([], dtype="datetime64[s]")
                                                          })),
        ],
        "download": [
            ("partitionedTable_datetime_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "datetime_0": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[s]"), ceil(n / 2))[:n],
                                                          "datetime_1": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                       dtype="datetime64[s]"), ceil(n / 3))[:n],
                                                          "datetime_2": np.tile(
                                                              np.array([None, None, None], dtype="datetime64[s]"),
                                                              ceil(n / 3))[:n]
                                                          })),
            ("partitionedTable_datetime_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "datetime_0": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[s]"), ceil(n / 2))[:n],
                                                          })),
            ("partitionedTable_datetime_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                          "datetime_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                                 dtype="datetime64[s]"),
                                                          "datetime_1": np.array([0], dtype="datetime64[s]"),
                                                          "datetime_2": np.array([None], dtype="datetime64[s]")
                                                          })),
            ("partitionedTable_datetime_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                          "datetime_0": np.array([], dtype="datetime64[s]"),
                                                          "datetime_1": np.array([], dtype="datetime64[s]"),
                                                          "datetime_2": np.array([], dtype="datetime64[s]")
                                                          })),
        ],
    }, DATATYPE.DT_TIMESTAMP: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_timestamp'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_timestamp_0 = db.createPartitionedTable(table_timestamp_0,`partitionedTable_timestamp_0,`index).append!(table_timestamp_0)
            partitionedTable_timestamp_1 = db.createPartitionedTable(table_timestamp_1,`partitionedTable_timestamp_1,`index).append!(table_timestamp_1)
            partitionedTable_timestamp_2 = db.createPartitionedTable(table_timestamp_2,`partitionedTable_timestamp_2,`index).append!(table_timestamp_2)
            partitionedTable_timestamp_3 = db.createPartitionedTable(table_timestamp_3,`partitionedTable_timestamp_3,`index).append!(table_timestamp_3)

            test_partitionedTable_timestamp_0 = db.createPartitionedTable(test_table_timestamp_0,`test_partitionedTable_timestamp_0,`index).append!(test_table_timestamp_0)
            test_partitionedTable_timestamp_1 = db.createPartitionedTable(test_table_timestamp_1,`test_partitionedTable_timestamp_1,`index).append!(test_table_timestamp_1)
            test_partitionedTable_timestamp_2 = db.createPartitionedTable(test_table_timestamp_2,`test_partitionedTable_timestamp_2,`index).append!(test_table_timestamp_2)
            test_partitionedTable_timestamp_3 = db.createPartitionedTable(test_table_timestamp_3,`test_partitionedTable_timestamp_3,`index).append!(test_table_timestamp_3)
        """,
        "upload": [
            ("partitionedTable_timestamp_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                           "timestamp_0": np.tile(
                                                               np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                        dtype="datetime64[ms]"), ceil(n / 2))[:n],
                                                           "timestamp_1": np.tile(
                                                               np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                        dtype="datetime64[ms]"), ceil(n / 3))[:n],
                                                           "timestamp_2": np.tile(
                                                               np.array([None, None, None], dtype="datetime64[ms]"),
                                                               ceil(n / 3))[:n]
                                                           })),
            ("partitionedTable_timestamp_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                           "timestamp_0": np.tile(
                                                               np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                        dtype="datetime64[ms]"), ceil(n / 2))[:n],
                                                           })),
            ("partitionedTable_timestamp_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                           "timestamp_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                                   dtype="datetime64[ms]"),
                                                           "timestamp_1": np.array([0], dtype="datetime64[ms]"),
                                                           "timestamp_2": np.array([None], dtype="datetime64[ms]")
                                                           })),
            ("partitionedTable_timestamp_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                           "timestamp_0": np.array([], dtype="datetime64[ms]"),
                                                           "timestamp_1": np.array([], dtype="datetime64[ms]"),
                                                           "timestamp_2": np.array([], dtype="datetime64[ms]")
                                                           })),
        ],
        "download": [
            ("partitionedTable_timestamp_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                           "timestamp_0": np.tile(
                                                               np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                        dtype="datetime64[ms]"), ceil(n / 2))[:n],
                                                           "timestamp_1": np.tile(
                                                               np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                        dtype="datetime64[ms]"), ceil(n / 3))[:n],
                                                           "timestamp_2": np.tile(
                                                               np.array([None, None, None], dtype="datetime64[ms]"),
                                                               ceil(n / 3))[:n]
                                                           })),
            ("partitionedTable_timestamp_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                           "timestamp_0": np.tile(
                                                               np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                        dtype="datetime64[ms]"), ceil(n / 2))[:n],
                                                           })),
            ("partitionedTable_timestamp_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                           "timestamp_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                                   dtype="datetime64[ms]"),
                                                           "timestamp_1": np.array([0], dtype="datetime64[ms]"),
                                                           "timestamp_2": np.array([None], dtype="datetime64[ms]")
                                                           })),
            ("partitionedTable_timestamp_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                           "timestamp_0": np.array([], dtype="datetime64[ms]"),
                                                           "timestamp_1": np.array([], dtype="datetime64[ms]"),
                                                           "timestamp_2": np.array([], dtype="datetime64[ms]")
                                                           })),
        ],
    }, DATATYPE.DT_NANOTIME: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_nanotime'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_nanotime_0 = db.createPartitionedTable(table_nanotime_0,`partitionedTable_nanotime_0,`index).append!(table_nanotime_0)
            partitionedTable_nanotime_1 = db.createPartitionedTable(table_nanotime_1,`partitionedTable_nanotime_1,`index).append!(table_nanotime_1)
            partitionedTable_nanotime_2 = db.createPartitionedTable(table_nanotime_2,`partitionedTable_nanotime_2,`index).append!(table_nanotime_2)
            partitionedTable_nanotime_3 = db.createPartitionedTable(table_nanotime_3,`partitionedTable_nanotime_3,`index).append!(table_nanotime_3)

            test_partitionedTable_nanotime_0 = db.createPartitionedTable(test_table_nanotime_0,`test_partitionedTable_nanotime_0,`index).append!(test_table_nanotime_0)
            test_partitionedTable_nanotime_1 = db.createPartitionedTable(test_table_nanotime_1,`test_partitionedTable_nanotime_1,`index).append!(test_table_nanotime_1)
            test_partitionedTable_nanotime_2 = db.createPartitionedTable(test_table_nanotime_2,`test_partitionedTable_nanotime_2,`index).append!(test_table_nanotime_2)
            test_partitionedTable_nanotime_3 = db.createPartitionedTable(test_table_nanotime_3,`test_partitionedTable_nanotime_3,`index).append!(test_table_nanotime_3)
        """,
        "upload": [
            # no way to upload DT_NANOTIME table
        ],
        "download": [
            ("partitionedTable_nanotime_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "nanotime_0": np.tile(
                                                              np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                          "nanotime_1": np.tile(
                                                              np.array(["1970-01-01 12:30:56.123456789", 0, None],
                                                                       dtype="datetime64[ns]"), ceil(n / 3))[:n],
                                                          "nanotime_2": np.tile(
                                                              np.array([None, None, None], dtype="datetime64[ns]"),
                                                              ceil(n / 3))[:n]
                                                          })),
            ("partitionedTable_nanotime_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "nanotime_0": np.tile(
                                                              np.array(["1970-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                          })),
            ("partitionedTable_nanotime_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                          "nanotime_0": np.array(["1970-01-01 12:30:56.123456789"],
                                                                                 dtype="datetime64[ns]"),
                                                          "nanotime_1": np.array([0], dtype="datetime64[ns]"),
                                                          "nanotime_2": np.array([None], dtype="datetime64[ns]")
                                                          })),
            ("partitionedTable_nanotime_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                          "nanotime_0": np.array([], dtype="datetime64[ns]"),
                                                          "nanotime_1": np.array([], dtype="datetime64[ns]"),
                                                          "nanotime_2": np.array([], dtype="datetime64[ns]")
                                                          })),
        ],
    }, DATATYPE.DT_NANOTIMESTAMP: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_nanotimestamp'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_nanotimestamp_0 = db.createPartitionedTable(table_nanotimestamp_0,`partitionedTable_nanotimestamp_0,`index).append!(table_nanotimestamp_0)
            partitionedTable_nanotimestamp_1 = db.createPartitionedTable(table_nanotimestamp_1,`partitionedTable_nanotimestamp_1,`index).append!(table_nanotimestamp_1)
            partitionedTable_nanotimestamp_2 = db.createPartitionedTable(table_nanotimestamp_2,`partitionedTable_nanotimestamp_2,`index).append!(table_nanotimestamp_2)
            partitionedTable_nanotimestamp_3 = db.createPartitionedTable(table_nanotimestamp_3,`partitionedTable_nanotimestamp_3,`index).append!(table_nanotimestamp_3)

            test_partitionedTable_nanotimestamp_0 = db.createPartitionedTable(test_table_nanotimestamp_0,`test_partitionedTable_nanotimestamp_0,`index).append!(test_table_nanotimestamp_0)
            test_partitionedTable_nanotimestamp_1 = db.createPartitionedTable(test_table_nanotimestamp_1,`test_partitionedTable_nanotimestamp_1,`index).append!(test_table_nanotimestamp_1)
            test_partitionedTable_nanotimestamp_2 = db.createPartitionedTable(test_table_nanotimestamp_2,`test_partitionedTable_nanotimestamp_2,`index).append!(test_table_nanotimestamp_2)
            test_partitionedTable_nanotimestamp_3 = db.createPartitionedTable(test_table_nanotimestamp_3,`test_partitionedTable_nanotimestamp_3,`index).append!(test_table_nanotimestamp_3)
        """,
        "upload": [
            ("partitionedTable_nanotimestamp_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "nanotimestamp_0": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                               "nanotimestamp_1": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                            dtype="datetime64[ns]"), ceil(n / 3))[:n],
                                                               "nanotimestamp_2": np.tile(
                                                                   np.array([None, None, None], dtype="datetime64[ns]"),
                                                                   ceil(n / 3))[:n]
                                                               })),
            ("partitionedTable_nanotimestamp_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "nanotimestamp_0": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                               })),
            ("partitionedTable_nanotimestamp_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                               "nanotimestamp_0": np.array(
                                                                   ["2022-01-01 12:30:56.123456789"],
                                                                   dtype="datetime64[ns]"),
                                                               "nanotimestamp_1": np.array([0], dtype="datetime64[ns]"),
                                                               "nanotimestamp_2": np.array([None],
                                                                                           dtype="datetime64[ns]")
                                                               })),
            ("partitionedTable_nanotimestamp_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                               "nanotimestamp_0": np.array([], dtype="datetime64[ns]"),
                                                               "nanotimestamp_1": np.array([], dtype="datetime64[ns]"),
                                                               "nanotimestamp_2": np.array([], dtype="datetime64[ns]")
                                                               })),
        ],
        "download": [
            ("partitionedTable_nanotimestamp_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "nanotimestamp_0": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                               "nanotimestamp_1": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                            dtype="datetime64[ns]"), ceil(n / 3))[:n],
                                                               "nanotimestamp_2": np.tile(
                                                                   np.array([None, None, None], dtype="datetime64[ns]"),
                                                                   ceil(n / 3))[:n]
                                                               })),
            ("partitionedTable_nanotimestamp_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                               "nanotimestamp_0": np.tile(
                                                                   np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                            dtype="datetime64[ns]"), ceil(n / 2))[:n],
                                                               })),
            ("partitionedTable_nanotimestamp_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                               "nanotimestamp_0": np.array(
                                                                   ["2022-01-01 12:30:56.123456789"],
                                                                   dtype="datetime64[ns]"),
                                                               "nanotimestamp_1": np.array([0], dtype="datetime64[ns]"),
                                                               "nanotimestamp_2": np.array([None],
                                                                                           dtype="datetime64[ns]")
                                                               })),
            ("partitionedTable_nanotimestamp_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                               "nanotimestamp_0": np.array([], dtype="datetime64[ns]"),
                                                               "nanotimestamp_1": np.array([], dtype="datetime64[ns]"),
                                                               "nanotimestamp_2": np.array([], dtype="datetime64[ns]")
                                                               })),
        ],
    }, DATATYPE.DT_FLOAT: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_float'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_float_0 = db.createPartitionedTable(table_float_0,`partitionedTable_float_0,`index).append!(table_float_0)
            partitionedTable_float_1 = db.createPartitionedTable(table_float_1,`partitionedTable_float_1,`index).append!(table_float_1)
            partitionedTable_float_2 = db.createPartitionedTable(table_float_2,`partitionedTable_float_2,`index).append!(table_float_2)
            partitionedTable_float_3 = db.createPartitionedTable(table_float_3,`partitionedTable_float_3,`index).append!(table_float_3)

            test_partitionedTable_float_0 = db.createPartitionedTable(test_table_float_0,`test_partitionedTable_float_0,`index).append!(test_table_float_0)
            test_partitionedTable_float_1 = db.createPartitionedTable(test_table_float_1,`test_partitionedTable_float_1,`index).append!(test_table_float_1)
            test_partitionedTable_float_2 = db.createPartitionedTable(test_table_float_2,`test_partitionedTable_float_2,`index).append!(test_table_float_2)
            test_partitionedTable_float_3 = db.createPartitionedTable(test_table_float_3,`test_partitionedTable_float_3,`index).append!(test_table_float_3)
        """,
        "upload": [
            ("partitionedTable_float_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "float_0": np.tile(np.array([1.11, 0.0], dtype="float32"),
                                                                          ceil(n / 2))[:n],
                                                       "float_1": np.tile(np.array([1.11, 0.0, None], dtype="float32"),
                                                                          ceil(n / 3))[:n],
                                                       "float_2": np.tile(np.array([None, None, None], dtype="float32"),
                                                                          ceil(n / 3))[:n]
                                                       })),
            ("partitionedTable_float_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "float_0": np.tile(np.array([1.11, 0.0], dtype="float32"),
                                                                          ceil(n / 2))[:n],
                                                       })),
            ("partitionedTable_float_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                       "float_0": np.array([1.11], dtype="float32"),
                                                       "float_1": np.array([0.0], dtype="float32"),
                                                       "float_2": np.array([None], dtype="float32")
                                                       })),
            ("partitionedTable_float_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                       "float_0": np.array([], dtype="float32"),
                                                       "float_1": np.array([], dtype="float32"),
                                                       "float_2": np.array([], dtype="float32")
                                                       })),
        ],
        "download": [
            ("partitionedTable_float_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "float_0": np.tile(np.array([1.11, 0.0], dtype="float32"),
                                                                          ceil(n / 2))[:n],
                                                       "float_1": np.tile(np.array([1.11, 0.0, None], dtype="float32"),
                                                                          ceil(n / 3))[:n],
                                                       "float_2": np.tile(np.array([None, None, None], dtype="float32"),
                                                                          ceil(n / 3))[:n]
                                                       })),
            ("partitionedTable_float_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                       "float_0": np.tile(np.array([1.11, 0.0], dtype="float32"),
                                                                          ceil(n / 2))[:n],
                                                       })),
            ("partitionedTable_float_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                       "float_0": np.array([1.11], dtype="float32"),
                                                       "float_1": np.array([0.0], dtype="float32"),
                                                       "float_2": np.array([None], dtype="float32")
                                                       })),
            ("partitionedTable_float_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                       "float_0": np.array([], dtype="float32"),
                                                       "float_1": np.array([], dtype="float32"),
                                                       "float_2": np.array([], dtype="float32")
                                                       })),
        ],
    }, DATATYPE.DT_DOUBLE: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_double'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_double_0 = db.createPartitionedTable(table_double_0,`partitionedTable_double_0,`index).append!(table_double_0)
            partitionedTable_double_1 = db.createPartitionedTable(table_double_1,`partitionedTable_double_1,`index).append!(table_double_1)
            partitionedTable_double_2 = db.createPartitionedTable(table_double_2,`partitionedTable_double_2,`index).append!(table_double_2)
            partitionedTable_double_3 = db.createPartitionedTable(table_double_3,`partitionedTable_double_3,`index).append!(table_double_3)

            test_partitionedTable_double_0 = db.createPartitionedTable(test_table_double_0,`test_partitionedTable_double_0,`index).append!(test_table_double_0)
            test_partitionedTable_double_1 = db.createPartitionedTable(test_table_double_1,`test_partitionedTable_double_1,`index).append!(test_table_double_1)
            test_partitionedTable_double_2 = db.createPartitionedTable(test_table_double_2,`test_partitionedTable_double_2,`index).append!(test_table_double_2)
            test_partitionedTable_double_3 = db.createPartitionedTable(test_table_double_3,`test_partitionedTable_double_3,`index).append!(test_table_double_3)
        """,
        "upload": [
            ("partitionedTable_double_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "double_0": np.tile(np.array([1.11, 0.0], dtype="float64"),
                                                                            ceil(n / 2))[:n],
                                                        "double_1": np.tile(
                                                            np.array([1.11, 0.0, None], dtype="float64"), ceil(n / 3))[
                                                                    :n],
                                                        "double_2": np.tile(
                                                            np.array([None, None, None], dtype="float64"), ceil(n / 3))[
                                                                    :n]
                                                        })),
            ("partitionedTable_double_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "double_0": np.tile(np.array([1.11, 0.0], dtype="float64"),
                                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_double_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                        "double_0": np.array([1.11], dtype="float64"),
                                                        "double_1": np.array([0.0], dtype="float64"),
                                                        "double_2": np.array([None], dtype="float64")
                                                        })),
            ("partitionedTable_double_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                        "double_0": np.array([], dtype="float64"),
                                                        "double_1": np.array([], dtype="float64"),
                                                        "double_2": np.array([], dtype="float64")
                                                        })),
        ],
        "download": [
            ("partitionedTable_double_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "double_0": np.tile(np.array([1.11, 0.0], dtype="float64"),
                                                                            ceil(n / 2))[:n],
                                                        "double_1": np.tile(
                                                            np.array([1.11, 0.0, None], dtype="float64"), ceil(n / 3))[
                                                                    :n],
                                                        "double_2": np.tile(
                                                            np.array([None, None, None], dtype="float64"), ceil(n / 3))[
                                                                    :n]
                                                        })),
            ("partitionedTable_double_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "double_0": np.tile(np.array([1.11, 0.0], dtype="float64"),
                                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_double_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                        "double_0": np.array([1.11], dtype="float64"),
                                                        "double_1": np.array([0.0], dtype="float64"),
                                                        "double_2": np.array([None], dtype="float64")
                                                        })),
            ("partitionedTable_double_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                        "double_0": np.array([], dtype="float64"),
                                                        "double_1": np.array([], dtype="float64"),
                                                        "double_2": np.array([], dtype="float64")
                                                        })),
        ],
    }, DATATYPE.DT_SYMBOL: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_symbol'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_symbol_0 = db.createPartitionedTable(table_symbol_0,`partitionedTable_symbol_0,`index).append!(table_symbol_0)
            partitionedTable_symbol_1 = db.createPartitionedTable(table_symbol_1,`partitionedTable_symbol_1,`index).append!(table_symbol_1)
            partitionedTable_symbol_2 = db.createPartitionedTable(table_symbol_2,`partitionedTable_symbol_2,`index).append!(table_symbol_2)
            partitionedTable_symbol_3 = db.createPartitionedTable(table_symbol_3,`partitionedTable_symbol_3,`index).append!(table_symbol_3)

            test_partitionedTable_symbol_0 = db.createPartitionedTable(test_table_symbol_0,`test_partitionedTable_symbol_0,`index).append!(test_table_symbol_0)
            test_partitionedTable_symbol_1 = db.createPartitionedTable(test_table_symbol_1,`test_partitionedTable_symbol_1,`index).append!(test_table_symbol_1)
            test_partitionedTable_symbol_2 = db.createPartitionedTable(test_table_symbol_2,`test_partitionedTable_symbol_2,`index).append!(test_table_symbol_2)
            test_partitionedTable_symbol_3 = db.createPartitionedTable(test_table_symbol_3,`test_partitionedTable_symbol_3,`index).append!(test_table_symbol_3)
        """,
        "upload": [
            # No way to upload DT_SYMBOL Table
        ],
        "download": [
            ("partitionedTable_symbol_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "symbol_0": np.tile(np.array(["hello", "world"], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        "symbol_1": np.tile(np.array(["hello", ''], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_symbol_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "symbol_0": np.tile(np.array(["hello", "world"], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_symbol_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                        "symbol_0": np.array(["hello"], dtype="str"),
                                                        "symbol_1": np.array(["world"], dtype="str"),
                                                        })),
            ("partitionedTable_symbol_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                        "symbol_0": np.array([], dtype="str"),
                                                        "symbol_1": np.array([], dtype="str"),
                                                        "symbol_2": np.array([], dtype="str")
                                                        })),
        ],
    }, DATATYPE.DT_STRING: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_string'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_string_0 = db.createPartitionedTable(table_string_0,`partitionedTable_string_0,`index).append!(table_string_0)
            partitionedTable_string_1 = db.createPartitionedTable(table_string_1,`partitionedTable_string_1,`index).append!(table_string_1)
            partitionedTable_string_2 = db.createPartitionedTable(table_string_2,`partitionedTable_string_2,`index).append!(table_string_2)
            partitionedTable_string_3 = db.createPartitionedTable(table_string_3,`partitionedTable_string_3,`index).append!(table_string_3)

            test_partitionedTable_string_0 = db.createPartitionedTable(test_table_string_0,`test_partitionedTable_string_0,`index).append!(test_table_string_0)
            test_partitionedTable_string_1 = db.createPartitionedTable(test_table_string_1,`test_partitionedTable_string_1,`index).append!(test_table_string_1)
            test_partitionedTable_string_2 = db.createPartitionedTable(test_table_string_2,`test_partitionedTable_string_2,`index).append!(test_table_string_2)
            test_partitionedTable_string_3 = db.createPartitionedTable(test_table_string_3,`test_partitionedTable_string_3,`index).append!(test_table_string_3)
        """,
        "upload": [
            ("partitionedTable_string_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "string_0": np.tile(np.array(["abc123测试", ""], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        "string_1": np.tile(np.array(["", "abc123测试"], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        "string_2": np.tile(np.array(["", ""], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_string_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "string_0": np.tile(np.array(["abc123测试", ""], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_string_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                        "string_0": np.array(["abc123测试"], dtype="str"),
                                                        "string_1": np.array([""], dtype="str"),
                                                        })),
            ("partitionedTable_string_3", pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                        "string_0": np.array([""], dtype="str"),
                                                        "string_1": np.array([""], dtype="str"),
                                                        "string_2": np.array([""], dtype="str")
                                                        })),
        ],
        "download": [
            ("partitionedTable_string_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "string_0": np.tile(np.array(["abc123测试", ""], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        "string_1": np.tile(np.array(["", "abc123测试"], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        "string_2": np.tile(np.array(["", ""], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_string_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "string_0": np.tile(np.array(["abc123测试", ""], dtype="str"),
                                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_string_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                        "string_0": np.array(["abc123测试"], dtype="str"),
                                                        "string_1": np.array([""], dtype="str"),
                                                        })),
            ("partitionedTable_string_3", pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                        "string_0": np.array([""], dtype="str"),
                                                        "string_1": np.array([""], dtype="str"),
                                                        "string_2": np.array([""], dtype="str")
                                                        })),
        ],
    }, DATATYPE.DT_UUID: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_uuid'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_uuid_0 = db.createPartitionedTable(table_uuid_0,`partitionedTable_uuid_0,`index).append!(table_uuid_0)
            partitionedTable_uuid_1 = db.createPartitionedTable(table_uuid_1,`partitionedTable_uuid_1,`index).append!(table_uuid_1)
            partitionedTable_uuid_2 = db.createPartitionedTable(table_uuid_2,`partitionedTable_uuid_2,`index).append!(table_uuid_2)
            partitionedTable_uuid_3 = db.createPartitionedTable(table_uuid_3,`partitionedTable_uuid_3,`index).append!(table_uuid_3)

            test_partitionedTable_uuid_0 = db.createPartitionedTable(test_table_uuid_0,`test_partitionedTable_uuid_0,`index).append!(test_table_uuid_0)
            test_partitionedTable_uuid_1 = db.createPartitionedTable(test_table_uuid_1,`test_partitionedTable_uuid_1,`index).append!(test_table_uuid_1)
            test_partitionedTable_uuid_2 = db.createPartitionedTable(test_table_uuid_2,`test_partitionedTable_uuid_2,`index).append!(test_table_uuid_2)
            test_partitionedTable_uuid_3 = db.createPartitionedTable(test_table_uuid_3,`test_partitionedTable_uuid_3,`index).append!(test_table_uuid_3)
        """,
        "upload": [
            # No way to upload DT_UUID Table
        ],
        "download": [
            ("partitionedTable_uuid_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "uuid_0": np.tile(np.array(
                                                          ["5d212a78-cc48-e3b1-4235-b4d91473ee87",
                                                           "00000000-0000-0000-0000-000000000000"], dtype="str"),
                                                          ceil(n / 2))[:n],
                                                      "uuid_1": np.tile(np.array(
                                                          ["00000000-0000-0000-0000-000000000000",
                                                           "5d212a78-cc48-33b1-4235-b4d91473ee87"], dtype="str"),
                                                          ceil(n / 2))[:n],
                                                      "uuid_2": np.tile(np.array(
                                                          ["00000000-0000-0000-0000-000000000000",
                                                           "00000000-0000-0000-0000-000000000000"], dtype="str"),
                                                          ceil(n / 2))[:n],
                                                      })),
            ("partitionedTable_uuid_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                      "uuid_0": np.tile(np.array(
                                                          ["5d212a78-cc48-33b1-4235-b4d91473ee87",
                                                           "00000000-0000-0000-0000-000000000000"], dtype="str"),
                                                          ceil(n / 2))[:n],
                                                      })),
            ("partitionedTable_uuid_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                      "uuid_0": np.array(["5d212a78-cc48-33b1-4235-b4d91473ee87"],
                                                                         dtype="str"),
                                                      "uuid_1": np.array(["00000000-0000-0000-0000-000000000000"],
                                                                         dtype="str"),
                                                      })),
            ("partitionedTable_uuid_3", pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                      "uuid_0": np.array(["00000000-0000-0000-0000-000000000000"],
                                                                         dtype="str"),
                                                      "uuid_1": np.array(["00000000-0000-0000-0000-000000000000"],
                                                                         dtype="str"),
                                                      "uuid_2": np.array(["00000000-0000-0000-0000-000000000000"],
                                                                         dtype="str")
                                                      })),
        ],
    }, DATATYPE.DT_DATEHOUR: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_datehour'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_datehour_0 = db.createPartitionedTable(table_datehour_0,`partitionedTable_datehour_0,`index).append!(table_datehour_0)
            partitionedTable_datehour_1 = db.createPartitionedTable(table_datehour_1,`partitionedTable_datehour_1,`index).append!(table_datehour_1)
            partitionedTable_datehour_2 = db.createPartitionedTable(table_datehour_2,`partitionedTable_datehour_2,`index).append!(table_datehour_2)
            partitionedTable_datehour_3 = db.createPartitionedTable(table_datehour_3,`partitionedTable_datehour_3,`index).append!(table_datehour_3)

            test_partitionedTable_datehour_0 = db.createPartitionedTable(test_table_datehour_0,`test_partitionedTable_datehour_0,`index).append!(test_table_datehour_0)
            test_partitionedTable_datehour_1 = db.createPartitionedTable(test_table_datehour_1,`test_partitionedTable_datehour_1,`index).append!(test_table_datehour_1)
            test_partitionedTable_datehour_2 = db.createPartitionedTable(test_table_datehour_2,`test_partitionedTable_datehour_2,`index).append!(test_table_datehour_2)
            test_partitionedTable_datehour_3 = db.createPartitionedTable(test_table_datehour_3,`test_partitionedTable_datehour_3,`index).append!(test_table_datehour_3)
        """,
        "upload": [
            ("partitionedTable_datehour_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "datehour_0": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[h]"), ceil(n / 2))[:n],
                                                          "datehour_1": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                       dtype="datetime64[h]"), ceil(n / 3))[:n],
                                                          "datehour_2": np.tile(
                                                              np.array([None, None, None], dtype="datetime64[h]"),
                                                              ceil(n / 3))[:n]
                                                          })),
            ("partitionedTable_datehour_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "datehour_0": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[h]"), ceil(n / 2))[:n],
                                                          })),
            ("partitionedTable_datehour_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                          "datehour_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                                 dtype="datetime64[h]"),
                                                          "datehour_1": np.array([0], dtype="datetime64[h]"),
                                                          "datehour_2": np.array([None], dtype="datetime64[h]")
                                                          })),
            ("partitionedTable_datehour_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                          "datehour_0": np.array([], dtype="datetime64[h]"),
                                                          "datehour_1": np.array([], dtype="datetime64[h]"),
                                                          "datehour_2": np.array([], dtype="datetime64[h]")
                                                          })),
        ],
        "download": [
            ("partitionedTable_datehour_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "datehour_0": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[h]"), ceil(n / 2))[:n],
                                                          "datehour_1": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0, None],
                                                                       dtype="datetime64[h]"), ceil(n / 3))[:n],
                                                          "datehour_2": np.tile(
                                                              np.array([None, None, None], dtype="datetime64[h]"),
                                                              ceil(n / 3))[:n]
                                                          })),
            ("partitionedTable_datehour_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                          "datehour_0": np.tile(
                                                              np.array(["2022-01-01 12:30:56.123456789", 0],
                                                                       dtype="datetime64[h]"), ceil(n / 2))[:n],
                                                          })),
            ("partitionedTable_datehour_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                          "datehour_0": np.array(["2022-01-01 12:30:56.123456789"],
                                                                                 dtype="datetime64[h]"),
                                                          "datehour_1": np.array([0], dtype="datetime64[h]"),
                                                          "datehour_2": np.array([None], dtype="datetime64[h]")
                                                          })),
            ("partitionedTable_datehour_3", pd.DataFrame({"index": np.array([], dtype="int32"),
                                                          "datehour_0": np.array([], dtype="datetime64[h]"),
                                                          "datehour_1": np.array([], dtype="datetime64[h]"),
                                                          "datehour_2": np.array([], dtype="datetime64[h]")
                                                          })),
        ],
    }, DATATYPE.DT_IPADDR: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_ip'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_ip_0 = db.createPartitionedTable(table_ip_0,`partitionedTable_ip_0,`index).append!(table_ip_0)
            partitionedTable_ip_1 = db.createPartitionedTable(table_ip_1,`partitionedTable_ip_1,`index).append!(table_ip_1)
            partitionedTable_ip_2 = db.createPartitionedTable(table_ip_2,`partitionedTable_ip_2,`index).append!(table_ip_2)
            partitionedTable_ip_3 = db.createPartitionedTable(table_ip_3,`partitionedTable_ip_3,`index).append!(table_ip_3)

            test_partitionedTable_ip_0 = db.createPartitionedTable(test_table_ip_0,`test_partitionedTable_ip_0,`index).append!(test_table_ip_0)
            test_partitionedTable_ip_1 = db.createPartitionedTable(test_table_ip_1,`test_partitionedTable_ip_1,`index).append!(test_table_ip_1)
            test_partitionedTable_ip_2 = db.createPartitionedTable(test_table_ip_2,`test_partitionedTable_ip_2,`index).append!(test_table_ip_2)
            test_partitionedTable_ip_3 = db.createPartitionedTable(test_table_ip_3,`test_partitionedTable_ip_3,`index).append!(test_table_ip_3)
        """,
        "upload": [
            # No way to upload DT_IPADDR Table
        ],
        "download": [
            ("partitionedTable_ip_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                    "ip_0": np.tile(np.array(["192.168.1.135", "0.0.0.0"], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                    "ip_1": np.tile(np.array(["0.0.0.0", "192.168.1.135"], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                    "ip_2": np.tile(np.array(["0.0.0.0", "0.0.0.0"], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                    })),
            ("partitionedTable_ip_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                    "ip_0": np.tile(np.array(["192.168.1.135", "0.0.0.0"], dtype="str"),
                                                                    ceil(n / 2))[:n],
                                                    })),
            ("partitionedTable_ip_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                    "ip_0": np.array(["192.168.1.135"], dtype="str"),
                                                    "ip_1": np.array(["0.0.0.0"], dtype="str"),
                                                    })),
            ("partitionedTable_ip_3", pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                    "ip_0": np.array(["0.0.0.0"], dtype="str"),
                                                    "ip_1": np.array(["0.0.0.0"], dtype="str"),
                                                    "ip_2": np.array(["0.0.0.0"], dtype="str")
                                                    })),
        ],
    }, DATATYPE.DT_INT128: {
        "scripts": f"""
            login("{USER}", "{PASSWD}")
            dbPath='{db_name}_int128'
            if(existsDatabase(dbPath)) 
               dropDatabase(dbPath)
            db=database(dbPath,HASH, [INT, 5]) 
            partitionedTable_int128_0 = db.createPartitionedTable(table_int128_0,`partitionedTable_int128_0,`index).append!(table_int128_0)
            partitionedTable_int128_1 = db.createPartitionedTable(table_int128_1,`partitionedTable_int128_1,`index).append!(table_int128_1)
            partitionedTable_int128_2 = db.createPartitionedTable(table_int128_2,`partitionedTable_int128_2,`index).append!(table_int128_2)
            partitionedTable_int128_3 = db.createPartitionedTable(table_int128_3,`partitionedTable_int128_3,`index).append!(table_int128_3)

            test_partitionedTable_int128_0 = db.createPartitionedTable(test_table_int128_0,`test_partitionedTable_int128_0,`index).append!(test_table_int128_0)
            test_partitionedTable_int128_1 = db.createPartitionedTable(test_table_int128_1,`test_partitionedTable_int128_1,`index).append!(test_table_int128_1)
            test_partitionedTable_int128_2 = db.createPartitionedTable(test_table_int128_2,`test_partitionedTable_int128_2,`index).append!(test_table_int128_2)
            test_partitionedTable_int128_3 = db.createPartitionedTable(test_table_int128_3,`test_partitionedTable_int128_3,`index).append!(test_table_int128_3)
        """,
        "upload": [
            # No way to upload DT_INT128 Table
        ],
        "download": [
            ("partitionedTable_int128_0", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "int128_0": np.tile(np.array(
                                                            ["e1671797c52e15f763380b45e841ec32",
                                                             "00000000000000000000000000000000"], dtype="str"),
                                                            ceil(n / 2))[:n],
                                                        "int128_1": np.tile(np.array(
                                                            ["00000000000000000000000000000000",
                                                             "e1671797c52e15f763380b45e841ec32"], dtype="str"),
                                                            ceil(n / 2))[:n],
                                                        "int128_2": np.tile(np.array(
                                                            ["00000000000000000000000000000000",
                                                             "00000000000000000000000000000000"], dtype="str"),
                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_int128_1", pd.DataFrame({"index": np.arange(1, n + 1).astype("int32"),
                                                        "int128_0": np.tile(np.array(
                                                            ["e1671797c52e15f763380b45e841ec32",
                                                             "00000000000000000000000000000000"], dtype="str"),
                                                            ceil(n / 2))[:n],
                                                        })),
            ("partitionedTable_int128_2", pd.DataFrame({"index": np.arange(1, 2).astype("int32"),
                                                        "int128_0": np.array(["e1671797c52e15f763380b45e841ec32"],
                                                                             dtype="str"),
                                                        "int128_1": np.array(["00000000000000000000000000000000"],
                                                                             dtype="str"),
                                                        })),
            ("partitionedTable_int128_3", pd.DataFrame({"index": np.array([0], dtype="int32"),
                                                        "int128_0": np.array(["00000000000000000000000000000000"],
                                                                             dtype="str"),
                                                        "int128_1": np.array(["00000000000000000000000000000000"],
                                                                             dtype="str"),
                                                        "int128_2": np.array(["00000000000000000000000000000000"],
                                                                             dtype="str")
                                                        })),
        ],
    }, DATATYPE.DT_BLOB: {
        # OLAP engine doesn't support BLOB or array type.
        "scripts": """
        """,
        "upload": [
            # No way to upload DT_blob Table
        ],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL32: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }, DATATYPE.DT_DECIMAL64: {
        "scripts": """
        """,
        "upload": [
        ],
        "download": [
        ],
    }}

    # TODO: python decimal32
    # TODO: python decimal64

    def get_script(script_type="all", *args, **kwargs):
        prefix, _ = get_Table(n=n, typeTable="table", isShare=False)
        if script_type == "all":
            for x in DATATYPE:
                prefix += str(pythons[x]["scripts"])
            return prefix
        else:
            return prefix + str(pythons[script_type]["scripts"])

    def get_python(*args, types=None, names=None, **kwargs):
        res = pythons
        if types is not None:
            res = pythons[types]
            if names is not None and names in ["upload", "download"]:
                res = pythons[types][names]
        return res

    return get_script, get_python


@data_wrapper
def get_TableData(*args, n=5, m=5, dbPath="dfs://testmergepart", shareName="share", **kwargs):
    n_scripts = "n = {}\n".format(n)
    m_scripts = "m = {}\n".format(m)
    AllTrade_scripts = f"""
        index1=long(1..n)
        time1=take(1970.01.01 10:01:01 1970.01.01 10:01:03 1970.01.01 10:01:05 1970.01.01 10:01:05, n)
        symbol1=symbol(take(`X`Z`Y`Z,n))
        price1=take(3 3.3 3.2 3.1, n)
        size1=long(take(100 200 50 10, n))
        Trade=table(index1 as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,size1 as size)

        index2=long(1..n)
        time2=take(1970.01.01 10:01:01 1970.01.01 10:01:02 1970.01.01 10:01:02 1970.01.01 10:01:03, n)
        symbol2=symbol(take(`X`Z,n))
        ask=long(take(90 150 100 52, n))
        bid=long(take(70 200 200 68, n))
        Quote=table(index2 as index, nanotimestamp(time2) as time,symbol2 as symbol,ask as ask,bid as bid)

        TradeStream = streamTable(index1 as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,size1 as size)
        QuoteStream = streamTable(index2 as index, nanotimestamp(time2) as time,symbol2 as symbol,ask as ask,bid as bid)

        share TradeStream as {shareName}_ALLTradeStream
        share QuoteStream as {shareName}_ALLQuoteStream

        share Trade as {shareName}_ALLShareTrade
        share Quote as {shareName}_ALLShareQuote

        login("{USER}", "{PASSWD}")
        if(existsDatabase("{dbPath}"))
            dropDatabase("{dbPath}")
        db = database("{dbPath}", VALUE, "X" "Z")
        pt1 = db.createPartitionedTable(Trade,`pt1,`symbol).append!(Trade)
        pt2 = db.createPartitionedTable(Quote,`pt2,`symbol).append!(Quote)

        TradeIndex = indexedTable(`index, index1 as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,size1 as size)
        QuoteIndex = indexedTable(`index, index2 as index, nanotimestamp(time2) as time,symbol2 as symbol,ask as ask,bid as bid)

        share TradeIndex as {shareName}_ALLTradeIndex
        share QuoteIndex as {shareName}_ALLQuoteIndex

        TradeKey = keyedTable(`index, index1 as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,size1 as size)
        QuoteKey = keyedTable(`index, index2 as index, nanotimestamp(time2) as time,symbol2 as symbol,ask as ask,bid as bid)

        share TradeKey as {shareName}_ALLTradeKey
        share QuoteKey as {shareName}_ALLQuoteKey
    """
    AllTrade_names = {
        "NormalTable": (
            n_scripts + """
                index1=long(1..n)
                time1=take(1970.01.01 10:01:01 1970.01.01 10:01:03 1970.01.01 10:01:05 1970.01.01 10:01:05, n)
                symbol1=symbol(take(`X`Z`Y`Z,n))
                price1=take(3 3.3 3.2 3.1, n)
                size1=long(take(100 200 50 10, n))
                Trade=table(index1 as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,size1 as size)

                index2=long(1..n)
                time2=take(1970.01.01 10:01:01 1970.01.01 10:01:02 1970.01.01 10:01:02 1970.01.01 10:01:03, n)
                symbol2=symbol(take(`X`Z,n))
                ask=long(take(90 150 100 52, n))
                bid=long(take(70 200 200 68, n))
                Quote=table(index2 as index, nanotimestamp(time2) as time,symbol2 as symbol,ask as ask,bid as bid)
            """,
            "Trade", "Quote",
        ),
        "SharedTable": [
            f"{shareName}_ALLShareTrade", f"{shareName}_ALLShareQuote",
        ],
        "StreamTable": (
            n_scripts + """
                index1=long(1..n)
                time1=take(1970.01.01 10:01:01 1970.01.01 10:01:03 1970.01.01 10:01:05 1970.01.01 10:01:05, n)
                symbol1=symbol(take(`X`Z`Y`Z,n))
                price1=take(3 3.3 3.2 3.1, n)
                size1=long(take(100 200 50 10, n))

                index2=long(1..n)
                time2=take(1970.01.01 10:01:01 1970.01.01 10:01:02 1970.01.01 10:01:02 1970.01.01 10:01:03, n)
                symbol2=symbol(take(`X`Z,n))
                ask=long(take(90 150 100 52, n))
                bid=long(take(70 200 200 68, n))

                TradeStream = streamTable(index1 as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,size1 as size)
                QuoteStream = streamTable(index2 as index, nanotimestamp(time2) as time,symbol2 as symbol,ask as ask,bid as bid)
            """,
            "TradeStream", "QuoteStream",
        ),
        "ShareStreamTable": (
            f"{shareName}_ALLTradeStream", f"{shareName}_ALLQuoteStream",
        ),
        "IndexedTable": (
            n_scripts + """
                index1=long(1..n)
                time1=take(1970.01.01 10:01:01 1970.01.01 10:01:03 1970.01.01 10:01:05 1970.01.01 10:01:05, n)
                symbol1=symbol(take(`X`Z`Y`Z,n))
                price1=take(3 3.3 3.2 3.1, n)
                size1=long(take(100 200 50 10, n))

                index2=long(1..n)
                time2=take(1970.01.01 10:01:01 1970.01.01 10:01:02 1970.01.01 10:01:02 1970.01.01 10:01:03, n)
                symbol2=symbol(take(`X`Z,n))
                ask=long(take(90 150 100 52, n))
                bid=long(take(70 200 200 68, n))

                TradeIndex = indexedTable(`index, index1 as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,size1 as size)
                QuoteIndex = indexedTable(`index, index2 as index, nanotimestamp(time2) as time,symbol2 as symbol,ask as ask,bid as bid)
            """,
            "TradeIndex", "QuoteIndex",
        ),
        "ShareIndexedTable": (
            f"{shareName}_ALLTradeIndex", f"{shareName}_ALLQuoteIndex",
        ),
        "KeyedTable": (
            n_scripts + """
                index1=long(1..n)
                time1=take(1970.01.01 10:01:01 1970.01.01 10:01:03 1970.01.01 10:01:05 1970.01.01 10:01:05, n)
                symbol1=symbol(take(`X`Z`Y`Z,n))
                price1=take(3 3.3 3.2 3.1, n)
                size1=long(take(100 200 50 10, n))

                index2=long(1..n)
                time2=take(1970.01.01 10:01:01 1970.01.01 10:01:02 1970.01.01 10:01:02 1970.01.01 10:01:03, n)
                symbol2=symbol(take(`X`Z,n))
                ask=long(take(90 150 100 52, n))
                bid=long(take(70 200 200 68, n))

                TradeKey = keyedTable(`index, index1 as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,size1 as size)
                QuoteKey = keyedTable(`index, index2 as index, nanotimestamp(time2) as time,symbol2 as symbol,ask as ask,bid as bid)
            """,
            "TradeKey", "QuoteKey",
        ),
        "ShareKeyedTable": (
            f"{shareName}_ALLTradeKey", f"{shareName}_ALLQuoteKey",
        ),
        "PartitionedTable": (dbPath, keys.VALUE, "`X`Z", "pt1", "pt2",),
        "loadText": ("loadText", ",", "df1.csv", "df2.csv",),
        "loadText$": ("loadText", "$", "df1_$.csv", "df2_$.csv",),
        "ploadText": ("ploadText", ",", "df1.csv", "df2.csv",),
        "ploadText$": ("ploadText", "$", "df1_$.csv", "df2_$.csv",),
        "loadTable": (
            "loadTable",
            n_scripts + """
                index1=long(1..n)
                time1=take(1970.01.01 10:01:01 1970.01.01 10:01:03 1970.01.01 10:01:05 1970.01.01 10:01:05, n)
                symbol1=symbol(take(`X`Z`Y`Z,n))
                price1=take(3 3.3 3.2 3.1, n)
                size1=long(take(100 200 50 10, n))
                Trade=table(index1 as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,size1 as size)

                index2=long(1..n)
                time2=take(1970.01.01 10:01:01 1970.01.01 10:01:02 1970.01.01 10:01:02 1970.01.01 10:01:03, n)
                symbol2=symbol(take(`X`Z,n))
                ask=long(take(90 150 100 52, n))
                bid=long(take(70 200 200 68, n))
                Quote=table(index2 as index, nanotimestamp(time2) as time,symbol2 as symbol,ask as ask,bid as bid)
            """,
            "Trade",
            "Quote",
        ),
        "loadPTable": ("loadTable", dbPath, "pt1", "pt2", keys.VALUE, "`X`Z",),
        "loadTableBySQL": ("loadTableBySQL", dbPath, "pt1", "pt2"),
        "loadTextEx": (
            "loadTextEx",
            n_scripts + f"""
            index1=long(1..n)
            time1=take(1970.01.01 10:01:01 1970.01.01 10:01:03 1970.01.01 10:01:05 1970.01.01 10:01:05, n)
            symbol1=symbol(take(`X`Z`Y`Z,n))
            price1=take(3 3.3 3.2 3.1, n)
            size1=long(take(100 200 50 10, n))
            Trade=table(int(index1) as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,int(size1) as size)

            index2=long(1..n)
            time2=take(1970.01.01 10:01:01 1970.01.01 10:01:02 1970.01.01 10:01:02 1970.01.01 10:01:03, n)
            symbol2=symbol(take(`X`Z,n))
            ask=long(take(90 150 100 52, n))
            bid=long(take(70 200 200 68, n))
            Quote=table(int(index2) as index, nanotimestamp(time2) as time,symbol2 as symbol,int(ask) as ask,int(bid) as bid)
            login("{USER}", "{PASSWD}")
            if(existsDatabase("{dbPath}2"))
                dropDatabase("{dbPath}2")
            db = database("{dbPath}2", VALUE, "X" "Z")
            db.createPartitionedTable(Trade,`pt1,`symbol)
            db.createPartitionedTable(Quote,`pt2,`symbol)
            """, dbPath + "2", "','", "df1.csv", "df2.csv", "symbol", "pt1", "pt2"
        ),
        "loadTextEx$": (
            "loadTextEx",
            n_scripts + f"""
            index1=long(1..n)
            time1=take(1970.01.01 10:01:01 1970.01.01 10:01:03 1970.01.01 10:01:05 1970.01.01 10:01:05, n)
            symbol1=symbol(take(`X`Z`Y`Z,n))
            price1=take(3 3.3 3.2 3.1, n)
            size1=long(take(100 200 50 10, n))
            Trade=table(int(index1) as index, nanotimestamp(time1) as time,symbol1 as symbol,price1 as price,int(size1) as size)

            index2=long(1..n)
            time2=take(1970.01.01 10:01:01 1970.01.01 10:01:02 1970.01.01 10:01:02 1970.01.01 10:01:03, n)
            symbol2=symbol(take(`X`Z,n))
            ask=long(take(90 150 100 52, n))
            bid=long(take(70 200 200 68, n))
            Quote=table(int(index2) as index, nanotimestamp(time2) as time,symbol2 as symbol,int(ask) as ask,int(bid) as bid)
            login("{USER}", "{PASSWD}")
            if(existsDatabase("{dbPath}2"))
                dropDatabase("{dbPath}2")
            db = database("{dbPath}2", VALUE, "X" "Z")
            db.createPartitionedTable(Trade,`pt1,`symbol)
            db.createPartitionedTable(Quote,`pt2,`symbol)
            """, dbPath + "2", "'$'", "df1_$.csv", "df2_$.csv", "symbol", "pt1", "pt2"
        ),
    }
    AllTrade_python = {
        "DataFrame": (
            pd.DataFrame({
                "index": np.arange(1, n + 1).astype('int64'),
                "time": np.tile(np.array(['1970-01-01T10:01:01', '1970-01-01T10:01:03',
                                          '1970-01-01T10:01:05', '1970-01-01T10:01:05'], dtype="datetime64[ns]"),
                                ceil(n / 4))[:n],
                "symbol": np.tile(["X", "Z", "Y", "Z"], ceil(n / 4))[:n],
                "price": np.tile([3, 3.3, 3.2, 3.1], ceil(n / 4))[:n],
                "size": np.tile([100, 200, 50, 10], ceil(n / 4))[:n].astype('int64'),
            }),
            pd.DataFrame({
                "index": np.arange(1, n + 1).astype('int64'),
                "time": np.tile(np.array(['1970-01-01T10:01:01', '1970-01-01T10:01:02',
                                          '1970-01-01T10:01:02', '1970-01-01T10:01:03'], dtype="datetime64[ns]"),
                                ceil(n / 4))[:n],
                "symbol": np.tile(["X", "Z", "X", "Z"], ceil(n / 4))[:n],
                "ask": np.tile([90, 150, 100, 52], ceil(n / 4))[:n].astype('int64'),
                "bid": np.tile([70, 200, 200, 68], ceil(n / 4))[:n].astype('int64'),
            }),
        ),
        "dict": (
            {
                "index": np.arange(1, n + 1).astype('int64'),
                "time": np.tile(np.array(['1970-01-01T10:01:01', '1970-01-01T10:01:03',
                                          '1970-01-01T10:01:05', '1970-01-01T10:01:05'], dtype="datetime64[ns]"),
                                ceil(n / 4))[:n],
                "symbol": np.tile(["X", "Z", "Y", "Z"], ceil(n / 4))[:n],
                "price": np.tile([3, 3.3, 3.2, 3.1], ceil(n / 4))[:n],
                "size": np.tile([100, 200, 50, 10], ceil(n / 4))[:n].astype('int64'),
            },
            {
                "index": np.arange(1, n + 1).astype('int64'),
                "time": np.tile(np.array(['1970-01-01T10:01:01', '1970-01-01T10:01:02',
                                          '1970-01-01T10:01:02', '1970-01-01T10:01:03'], dtype="datetime64[ns]"),
                                ceil(n / 4))[:n],
                "symbol": np.tile(["X", "Z", "X", "Z"], ceil(n / 4))[:n],
                "ask": np.tile([90, 150, 100, 52], ceil(n / 4))[:n].astype('int64'),
                "bid": np.tile([70, 200, 200, 68], ceil(n / 4))[:n].astype('int64'),
            },
        ),
        "dict_list": (
            {
                "index": np.arange(1, n + 1).astype('int64').tolist(),
                "time": np.tile(np.array(['1970-01-01T10:01:01', '1970-01-01T10:01:03',
                                          '1970-01-01T10:01:05', '1970-01-01T10:01:05'], dtype="datetime64[ns]"),
                                ceil(n / 4))[:n],
                "symbol": np.tile(["X", "Z", "Y", "Z"], ceil(n / 4))[:n].tolist(),
                "price": np.tile([3, 3.3, 3.2, 3.1], ceil(n / 4))[:n].tolist(),
                "size": np.tile([100, 200, 50, 10], ceil(n / 4))[:n].astype('int64').tolist(),
            },
            {
                "index": np.arange(1, n + 1).astype('int64').tolist(),
                "time": np.tile(np.array(['1970-01-01T10:01:01', '1970-01-01T10:01:02',
                                          '1970-01-01T10:01:02', '1970-01-01T10:01:03'], dtype="datetime64[ns]"),
                                ceil(n / 4))[:n],
                "symbol": np.tile(["X", "Z", "X", "Z"], ceil(n / 4))[:n].tolist(),
                "ask": np.tile([90, 150, 100, 52], ceil(n / 4))[:n].astype('int64').tolist(),
                "bid": np.tile([70, 200, 200, 68], ceil(n / 4))[:n].astype('int64').tolist(),
            },
        ),
        "csv_df": (
            pd.DataFrame({
                "index": np.arange(1, m + 1, dtype="int32"),
                "time": np.tile(np.array(['1970-01-01T10:01:01.123456789', '1970-01-01T10:01:03.234567891',
                                          '1970-01-01T10:01:05.345678912', '1970-01-01T10:01:05.456789123'],
                                         dtype="datetime64[ns]"), ceil(m / 4))[:m],
                "symbol": np.tile(["XX", "ZZ", "YY", "ZZ"], ceil(m / 4))[:m],
                "price": np.tile([3, 3.3, 3.2, 3.1, 1.6], ceil(m / 5))[:m],
                "size": np.tile([100, 200, 50, 10], ceil(m / 4))[:m].astype("int32"),
            }),
            pd.DataFrame({
                "index": np.arange(1, m + 1, dtype="int32"),
                "time": np.tile(np.array(['1970-01-01T10:01:01.123456789', '1970-01-01T10:01:02.234567891',
                                          '1970-01-01T10:01:02.345678912', '1970-01-01T10:01:03.456789123'],
                                         dtype="datetime64[ns]"), ceil(m / 4))[:m],
                "symbol": np.tile(["XX", "ZZ", "XX", "ZZ"], ceil(m / 4))[:m],
                "ask": np.tile([90, 150, 100, 52, 26], ceil(m / 5))[:m].astype("int32"),
                "bid": np.tile([70, 200, 200, 68], ceil(m / 4))[:m].astype("int32"),
            }),
            {
                "index": DATATYPE.DT_INT.value,
                "time": DATATYPE.DT_NANOTIMESTAMP.value,
                "symbol": DATATYPE.DT_SYMBOL.value,
                "price": DATATYPE.DT_DOUBLE.value,
                "size": DATATYPE.DT_INT.value,
            },
            {
                "index": DATATYPE.DT_INT.value,
                "time": DATATYPE.DT_NANOTIMESTAMP.value,
                "symbol": DATATYPE.DT_SYMBOL.value,
                "ask": DATATYPE.DT_INT.value,
                "bid": DATATYPE.DT_INT.value,
            },
        )
    }

    def get_script(*args, names=None, **kwargs):
        if names == "AllTrade":
            return n_scripts + m_scripts + AllTrade_scripts, AllTrade_names

    def get_python(*args, names=None, **kwargs):
        if names == "AllTrade":
            return AllTrade_python

    return get_script, get_python


def get_scripts(name):
    random = str(uuid.uuid4()).replace('-', '')
    if name == "login":
        return f"""
        login('{USER}','{PASSWD}')
        dbPath='dfs://test_{random}'
        if(existsDatabase(dbPath))
        dropDatabase(dbPath)
        db=database(dbPath,HASH,[INT,10])
        n=10
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`tmptb,`id).append!(tdata)
        1+1
        """
    if name == "undefShare":
        return "undef((exec name from objs(true) where shared=1),SHARED)"
    if name == "objExist":
        return "bool(size(exec bytes from objs(true) where name=`{tableName}))"
    if name == "objCounts":
        return "exec count(*) from objs() where name = '{name}'"
    if name == "tableRows":
        return "(select * from {tableName}).rows()"
    if name == "tableCols":
        return "{tableName}.columns()"
    if name == "colNames":
        return "exec name from schema({tableName})['colDefs']"
    if name == "loadTable":
        return "{tableHandle} = loadTable('{dbPath}', '{tableName}')"
    if name == "checkEqual":
        return """
            checkEqual_a = select * from {tableA};
            checkEqual_b = select * from {tableB};
            each(eqObj, checkEqual_a.values(), checkEqual_b.values());
        """
    if name == "clearDatabase":
        return """
            if(existsDatabase("{dbPath}"))
                dropDatabase("{dbPath}")
        """


def eval_table_with_data(conntmp: ddb.session, data, test_function):
    if isinstance(data[0], pd.DataFrame):
        for df in data:
            tmp = conntmp.table(data=df)
            test_function(conntmp, tmp)
    elif isinstance(data[0], dict):
        for dt in data:
            tmp = conntmp.table(data=dt)
            test_function(conntmp, tmp)
    elif isinstance(data[0], str):
        if len(data) > 3:
            for name in data[3:]:
                conntmp.run(get_scripts("loadTable").format(tableHandle=name, dbPath=data[0], tableName=name))
                tmp = conntmp.table(dbPath=data[0], data=name)
                test_function(conntmp, tmp)
        else:
            if len(data) == 3:
                conntmp.run(data[0])
                data = data[1:]
            for name in data:
                tmp = conntmp.table(data=name)
                test_function(conntmp, tmp)
    else:
        raise RuntimeError("unexcepted type.")


def eval_sql_with_data(conntmp: ddb.session, data, test_function, *args, **kwargs):
    if isinstance(data[0], pd.DataFrame):
        leftTable = conntmp.table(data=data[0])
        rightTable = conntmp.table(data=data[1])
        test_function(conntmp, leftTable, rightTable, leftTable.toDF(), rightTable.toDF(), *args, **kwargs)
    elif isinstance(data[0], dict):
        leftTable = conntmp.table(data=data[0])
        rightTable = conntmp.table(data=data[1])
        test_function(conntmp, leftTable, rightTable, leftTable.toDF(), rightTable.toDF(), *args, **kwargs)
    elif isinstance(data[0], str):
        if len(data) > 3:
            conntmp.run(get_scripts("loadTable").format(tableHandle=data[3], dbPath=data[0], tableName=data[4]))
            leftTable = conntmp.table(dbPath=data[0], data=data[3])
            rightTable = conntmp.table(dbPath=data[0], data=data[4])
            test_function(conntmp, leftTable, rightTable, leftTable.toDF(), rightTable.toDF(), *args, partitioned=True,
                          **kwargs)
        else:
            if len(data) == 3:
                conntmp.run(data[0])
                data = data[1:]
            leftTable = conntmp.table(data=data[0])
            rightTable = conntmp.table(data=data[1])
            test_function(conntmp, leftTable, rightTable, leftTable.toDF(), rightTable.toDF(), *args, **kwargs)
    else:
        raise RuntimeError("unexcepted type.")
