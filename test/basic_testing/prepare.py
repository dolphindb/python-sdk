import platform
import re
from uuid import UUID
import numpy as np
import math
from decimal import Decimal
import pandas as pd
import dolphindb.settings as keys
from importlib.util import find_spec
if find_spec("pyarrow") is not None:
    import pyarrow as pa
    PYARROW_VERSION = tuple(int(i) for i in pa.__version__.split('.'))

PANDAS_VERSION = tuple(int(i) for i in pd.__version__.split('.'))
PYTHON_VERSION = tuple(int(i) for i in platform.python_version().split('.'))


# todo:str & bytes empty
# todo:dtype,str
class DataUtils(object):
    DATA_UPLOAD = {
        # None
        'data_none': {
            'value': None,
            'expect_typestr': 'VOID',
            'expect_value': 'NULL',
            'dtype': 'object',
        },
        'data_numpy_nan': {
            'value': np.nan,
            'expect_typestr': 'DOUBLE',
            'expect_value': '00F',
            'dtype': np.float64,
        },
        'data_pandas_nat': {
            'value': pd.NaT,
            'expect_typestr': 'NANOTIMESTAMP',
            'expect_value': '00N',
            'dtype': 'datetime64[ns]',
        },
        # 'data_numpy_void':np.void,
        # 'data_pandas_na':pd.NA,
        # 'data_pyarrow_na':pa.NULL,

        # int
        'data_int_0': {
            'value': 0,
            'expect_typestr': 'LONG',
            'expect_value': '0',
            'dtype': np.int64,
        },
        'data_numpy_int8_0': {
            'value': np.int8(0),
            'expect_typestr': 'CHAR',
            'expect_value': '0',
            'dtype': np.int8,
        },  # byte
        'data_numpy_int8_max': {
            'value': np.int8(2 ** 7 - 1),
            'expect_typestr': 'CHAR',
            'expect_value': '127',
            'dtype': np.int8,
        },
        'data_numpy_int8_min': {
            'value': np.int8(-2 ** 7 + 1),
            'expect_typestr': 'CHAR',
            'expect_value': '-127',
            'dtype': np.int8,
        },
        'data_numpy_int8_none': {
            'value': np.int8(-2 ** 7),
            'expect_typestr': 'CHAR',
            'expect_value': '00c',
            'dtype': np.int8,
        },
        'data_numpy_int16_0': {
            'value': np.int16(0),
            'expect_typestr': 'SHORT',
            'expect_value': '0',
            'dtype': np.int16,
        },  # short
        'data_numpy_int16_max': {
            'value': np.int16(2 ** 15 - 1),
            'expect_typestr': 'SHORT',
            'expect_value': '32767',
            'dtype': np.int16,
        },
        'data_numpy_int16_min': {
            'value': np.int16(-2 ** 15 + 1),
            'expect_typestr': 'SHORT',
            'expect_value': '-32767',
            'dtype': np.int16,
        },
        'data_numpy_int16_none': {
            'value': np.int16(-2 ** 15),
            'expect_typestr': 'SHORT',
            'expect_value': '00h',
            'dtype': np.int16,
        },
        'data_numpy_int32_0': {
            'value': np.int32(0),
            'expect_typestr': 'INT',
            'expect_value': '0',
            'dtype': np.int32,
        },  # int intc int32
        'data_numpy_int32_max': {
            'value': np.int32(2 ** 31 - 1),
            'expect_typestr': 'INT',
            'expect_value': '2147483647',
            'dtype': np.int32,
        },
        'data_numpy_int32_min': {
            'value': np.int32(-2 ** 31 + 1),
            'expect_typestr': 'INT',
            'expect_value': '-2147483647',
            'dtype': np.int32,
        },
        'data_numpy_int32_none': {
            'value': np.int32(-2 ** 31),
            'expect_typestr': 'INT',
            'expect_value': '00i',
            'dtype': np.int32,
        },
        'data_numpy_int64_0': {
            'value': np.int64(0),
            'expect_typestr': 'LONG',
            'expect_value': '0',
            'dtype': np.int64,
        },  # long
        'data_numpy_int64_max': {
            'value': np.int64(2 ** 63 - 1),
            'expect_typestr': 'LONG',
            'expect_value': '9223372036854775807',
            'dtype': np.int64,
        },
        'data_numpy_int64_min': {
            'value': np.int64(-2 ** 63 + 1),
            'expect_typestr': 'LONG',
            'expect_value': '-9223372036854775807',
            'dtype': np.int64,
        },
        'data_numpy_int64_none': {
            'value': np.int64(-2 ** 63),
            'expect_typestr': 'LONG',
            'expect_value': '00l',
            'dtype': np.int64,
        },
        # 'data_numpy_uint8': np.uint8(),  # not support
        # 'data_numpy_uint16': np.uint16(),  # not support
        # 'data_numpy_uint': np.uint(),  # not support
        # 'data_numpy_uint64': np.uint64(),  # not support

        # float
        'data_float': {
            'value': 3.14,
            'expect_typestr': 'DOUBLE',
            'expect_value': '3.14',
            'dtype': np.float64,
        },
        'data_pi': {
            'value': math.pi,
            'expect_typestr': 'DOUBLE',
            'expect_value': 'pi',
            'dtype': np.float64,
        },
        'data_nan': {
            'value': float('NaN'),
            'expect_typestr': 'DOUBLE',
            'expect_value': '00F',
            'dtype': np.float64,
        },
        # 'data_inf': {
        #     'value': float('Inf'),
        #     'expect_typestr': 'DOUBLE',
        #     'expect_value': 'double("inf")',
        # },
        # 'data_numpy_inf': {
        #     'value': np.Inf,
        #     'expect_typestr': 'DOUBLE',
        #     'expect_value': 'double("inf")',
        # },
        # 'data_numpy_float16':np.float16(0),# not support
        'data_numpy_float32_0': {
            'value': np.float32(0),
            'expect_typestr': 'FLOAT',
            'expect_value': '0',
            'dtype': np.float32,
        },
        'data_numpy_float32_max': {
            'value': np.float32(3.4028235e+38),
            'expect_typestr': 'FLOAT',
            'expect_value': 'float(3.4028235e+38)',
            'dtype': np.float32,
        },
        'data_numpy_float32_none': {
            'value': np.float32(-3.4028235e+38),
            'expect_typestr': 'FLOAT',
            'expect_value': 'float(-3.4028235e+38)',
            'dtype': np.float32,
        },
        'data_numpy_float64_0': {
            'value': np.float64(0),
            'expect_typestr': 'DOUBLE',
            'expect_value': '0',
            'dtype': np.float64,
        },
        'data_numpy_float64_max': {
            'value': np.float64(1.7976931348623157e+308),
            'expect_typestr': 'DOUBLE',
            'expect_value': 'double(1.7976931348623157e+308)',
            'dtype': np.float64,
        },
        'data_numpy_float64_none': {
            'value': np.float64(-1.7976931348623157e+308),
            'expect_typestr': 'DOUBLE',
            'expect_value': '00F',
            'dtype': np.float64,
        },
        # 'data_numpy_longdouble_0': {
        #     'value': np.longdouble(0),
        #     'expect_typestr': 'DOUBLE',
        #     'expect_value': '0',
        # },
        # 'data_numpy_longdouble_max': {
        #     'value': np.longdouble(1.7976931348623157e+308),
        #     'expect_typestr': 'DOUBLE',
        #     'expect_value': 'double(1.7976931348623157e+308)',
        # },
        # 'data_numpy_longdouble_none': {
        #     'value': np.longdouble(-1.7976931348623157e+308),
        #     'expect_typestr': 'DOUBLE',
        #     'expect_value': '00F',
        # },

        # str
        'data_string': {
            'value': "abc!@#中文 123",
            'expect_typestr': 'STRING',
            'expect_value': "'abc!@#中文 123'",
            'dtype': 'object',
        },
        'data_numpy_str': {
            'value': np.str_("abc!@#中文 123"),
            'expect_typestr': 'STRING',
            'expect_value': "'abc!@#中文 123'",
            'dtype': 'object',
        },
        'data_bytes_utf8': {
            'value': "abc!@#中文 123".encode(),
            'expect_typestr': 'BLOB',
            'expect_value': "'abc!@#中文 123'",
            'dtype': 'object',
        },
        'data_bytes_gbk': {
            'value': "abc!@#中文 123".encode('gbk'),
            'expect_typestr': 'BLOB',
            'expect_value': "fromUTF8('abc!@#中文 123','gbk')",
            'dtype': 'object',
        },
        'data_numpy_bytes_utf8': {
            'value': np.bytes_("abc!@#中文 123".encode()),
            'expect_typestr': 'BLOB',
            'expect_value': "'abc!@#中文 123'",
            'dtype': 'object',
        },
        'data_numpy_bytes_gbk': {
            'value': np.bytes_("abc!@#中文 123".encode('gbk')),
            'expect_typestr': 'BLOB',
            'expect_value': "fromUTF8('abc!@#中文 123','gbk')",
            'dtype': 'object',
        },

        # bool
        'data_bool_true': {
            'value': True,
            'expect_typestr': 'BOOL',
            'expect_value': "true",
            'dtype': np.bool_,
        },
        'data_bool_false': {
            'value': False,
            'expect_typestr': 'BOOL',
            'expect_value': "false",
            'dtype': np.bool_,
        },
        'data_numpy_bool_true': {
            'value': np.bool_(True),
            'expect_typestr': 'BOOL',
            'expect_value': "true",
            'dtype': np.bool_,
        },
        'data_numpy_bool_false': {
            'value': np.bool_(False),
            'expect_typestr': 'BOOL',
            'expect_value': "false",
            'dtype': np.bool_,
        },

        # complex
        # 'data_complex':1+2j,# not support
        # 'data_numpy_complex64':np.complex64(),# not support
        # 'data_numpy_complex128': np.complex128(),  # not support
        # 'data_numpy_clongdouble': np.clongdouble(),  # not support

        # object
        # 'data_object':object(),# not support
        # 'data_numpy_object':np.object_('1'),# 和其他类型貌似重复

        # time
        # 'data_numpy_datetime64_as':np.datetime64(0,'as'),# not support
        # 'data_numpy_datetime64_fs':np.datetime64(0,'fs'),# not support
        # 'data_numpy_datetime64_ps':np.datetime64(0,'ps'),# not support
        'data_numpy_datetime64_ns_0': {
            'value': np.datetime64(0, 'ns'),
            'expect_typestr': 'NANOTIMESTAMP',
            'expect_value': "1970.01.01T00:00:00.000000000",
            'dtype': 'datetime64[ns]',
        },
        'data_numpy_datetime64_ns_max': {
            'value': np.datetime64('2262-04-11T23:47:16.854775807', 'ns'),
            'expect_typestr': 'NANOTIMESTAMP',
            'expect_value': "2262.04.11T23:47:16.854775807",
            'dtype': 'datetime64[ns]',
        },
        'data_numpy_datetime64_ns_min': {
            'value': np.datetime64('1677-09-21T00:12:43.145224193', 'ns'),
            'expect_typestr': 'NANOTIMESTAMP',
            'expect_value': "1677.09.21T00:12:43.145224193",
            'dtype': 'datetime64[ns]',
        },
        'data_numpy_datetime64_ns_none': {
            'value': np.datetime64('1677-09-21T00:12:43.145224192', 'ns'),
            'expect_typestr': 'NANOTIMESTAMP',
            'expect_value': "00N",
            'dtype': 'datetime64[ns]',
        },
        'data_numpy_datetime64_us_0': {
            'value': np.datetime64(0, 'us'),
            'expect_typestr': 'NANOTIMESTAMP',
            'expect_value': "1970.01.01T00:00:00.000000000",
            'dtype': 'datetime64[us]',
        },
        # 'data_numpy_datetime64_us_max': np.datetime64(f'2262-04-11T23:47:16.854775', 'us'),
        # 'data_numpy_datetime64_us_min': np.datetime64('1677-09-21T00:12:43.145225', 'us'),
        'data_numpy_datetime64_ms': {
            'value': np.datetime64(0, 'ms'),
            'expect_typestr': 'TIMESTAMP',
            'expect_value': "1970.01.01T00:00:00.000",
            'dtype': 'datetime64[ms]',
        },
        'data_numpy_datetime64_s': {
            'value': np.datetime64(0, 's'),
            'expect_typestr': 'DATETIME',
            'expect_value': "1970.01.01T00:00:00",
            'dtype': 'datetime64[s]',
        },
        'data_numpy_datetime64_m': {
            'value': np.datetime64(0, 'm'),
            'expect_typestr': 'DATETIME',
            'expect_value': "1970.01.01T00:00:00",
            'dtype': 'object',
        },
        'data_numpy_datetime64_h': {
            'value': np.datetime64(0, 'h'),
            'expect_typestr': 'DATEHOUR',
            'expect_value': "datehour('1970.01.01T00')",
            'dtype': 'object',
        },
        'data_numpy_datetime64_d_up': {
            'value': np.datetime64(0, 'D'),
            'expect_typestr': 'DATE',
            'expect_value': "1970.01.01",
            'dtype': 'object',
        },
        # 'data_numpy_datetime64_W': np.datetime64(0, 'W'),# not support
        'data_numpy_datetime64_m_up': {
            'value': np.datetime64(0, 'M'),
            'expect_typestr': 'MONTH',
            'expect_value': "1970.01M",
            'dtype': 'object',
        },
        # 'data_numpy_datetime64_Y': np.datetime64(0, 'Y'),# not support
        # 'data_numpy_timedelta64_as':np.timedelta64(0,'as'),# not support
        # 'data_numpy_timedelta64_fs':np.timedelta64(0,'fs'),# not support
        # 'data_numpy_timedelta64_ps':np.timedelta64(0,'ps'),# not support
        # 'data_numpy_timedelta64_ns': np.timedelta64(0, 'ns'),# not support
        # 'data_numpy_timedelta64_us': np.timedelta64(0, 'us'),# not support
        # 'data_numpy_timedelta64_ms': np.timedelta64(0, 'ms'),# not support
        # 'data_numpy_timedelta64_s': np.timedelta64(0, 's'),# not support
        # 'data_numpy_timedelta64_m': np.timedelta64(0, 'm'),# not support
        # 'data_numpy_timedelta64_h': np.timedelta64(0, 'h'),# not support
        # 'data_numpy_timedelta64_D': np.timedelta64(0, 'D'),# not support
        # 'data_numpy_timedelta64_W': np.timedelta64(0, 'W'),# not support
        # 'data_numpy_timedelta64_M': np.timedelta64(0, 'M'),# not support
        # 'data_numpy_timedelta64_Y': np.timedelta64(0, 'Y'),# not support

        # decimal
        'data_decimal_2': {
            'value': Decimal('0.00'),
            'expect_typestr': "DECIMAL64",
            'expect_value': "decimal64('0.00',2)",
            'dtype': 'object',
        },
        'data_decimal_nan': {
            'value': Decimal('nan'),
            'expect_typestr': "DECIMAL64",
            'expect_value': "decimal64('nan',0)",
            'dtype': 'object',
        },
        'data_decimal_17': {
            'value': Decimal('3.14159265358979323'),
            'expect_typestr': "DECIMAL64",
            'expect_value': "decimal64('3.14159265358979323',17)",
            'dtype': 'object',
        },
        'data_decimal_18': {
            'value': Decimal('-0.141592653589793238'),
            'expect_typestr': "DECIMAL128",
            'expect_value': "decimal128('-0.141592653589793238',18)",
            'dtype': 'object',
        },
        'data_decimal_38': {
            'value': Decimal('0.14159265358979323846264338327950288419'),
            'expect_typestr': "DECIMAL128",
            'expect_value': "decimal128('0.14159265358979323846264338327950288419',38)",
            'dtype': 'object',
        },
    }

    DATA_DOWNLOAD = {
        # null
        'void': {
            'value': 'NULL',  # ddb script
            'expect': None,  # python expect
            'dtype': 'object',  # python numpy expect dtype
            'contain_none': {  # contain none special
                'expect': None,  # python none expect
                'dtype': 'object',  # python contain none numpy expect dtype
            },
            'ddbtype': 'VOID',
        },

        # bool
        'bool_true': {
            'value': 'true',
            'expect': True,
            'dtype': np.bool_,
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'BOOL',
        },
        'bool_false': {
            'value': 'false',
            'expect': False,
            'dtype': np.bool_,
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'BOOL',
        },

        # char
        'char_0': {
            'value': '0c',
            'expect': 0,
            'dtype': np.int8,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'CHAR',
        },
        'char_max': {
            'value': '127c',
            'expect': 127,
            'dtype': np.int8,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'CHAR',
        },
        'char_min': {
            'value': '-127c',
            'expect': -127,
            'dtype': np.int8,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'CHAR',
        },
        'char_none': {
            'value': '00c',
            'expect': None,
            'dtype': np.int8,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'CHAR',
        },

        # short
        'short_0': {
            'value': '0h',
            'expect': 0,
            'dtype': np.int16,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'SHORT',
        },
        'short_max': {
            'value': '32767h',
            'expect': 32767,
            'dtype': np.int16,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'SHORT',
        },
        'short_min': {
            'value': '-32767h',
            'expect': -32767,
            'dtype': np.int16,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'SHORT',
        },
        'short_none': {
            'value': '00h',
            'expect': None,
            'dtype': np.int16,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'SHORT',
        },

        # int
        'int_0': {
            'value': '0i',
            'expect': 0,
            'dtype': np.int32,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'INT',
        },
        'int_max': {
            'value': '2147483647i',
            'expect': 2147483647,
            'dtype': np.int32,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'INT',
        },
        'int_min': {
            'value': '-2147483647i',
            'expect': -2147483647,
            'dtype': np.int32,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'INT',
        },
        'int_none': {
            'value': '00i',
            'expect': None,
            'dtype': np.int32,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'INT',
        },

        # long
        'long_0': {
            'value': '0l',
            'expect': 0,
            'dtype': np.int64,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'LONG',
        },
        'long_max': {
            'value': '9223372036854775807l',
            'expect': 9223372036854775807,
            'dtype': np.int64,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'LONG',
        },
        'long_min': {
            'value': '-9223372036854775807l',
            'expect': -9223372036854775807,
            'dtype': np.int64,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'LONG',
        },
        'long_none': {
            'value': '00l',
            'expect': None,
            'dtype': np.int64,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'LONG',
        },

        # date
        'date_0': {
            'value': '1970.01.01d',
            'expect': np.datetime64('1970-01-01', 'D'),
            'dtype': 'datetime64[D]',
            'contain_none': {
                'expect': np.datetime64('nat', 'D'),
                'dtype': 'datetime64[D]',
            },
            'ddbtype': 'DATE',
        },
        'date_none': {
            'value': '00d',
            'expect': None,
            'dtype': 'datetime64[D]',
            'contain_none': {
                'expect': np.datetime64('nat', 'D'),
                'dtype': 'datetime64[D]',
            },
            'ddbtype': 'DATE',
        },

        # month
        'month_0': {
            'value': '1970.01M',
            'expect': np.datetime64('1970-01', 'M'),
            'dtype': 'datetime64[M]',
            'contain_none': {
                'expect': np.datetime64('nat', 'M'),
                'dtype': 'datetime64[M]',
            },
            'ddbtype': 'MONTH',
        },
        'month_none': {
            'value': '00M',
            'expect': None,
            'dtype': 'datetime64[M]',
            'contain_none': {
                'expect': np.datetime64('nat', 'M'),
                'dtype': 'datetime64[M]',
            },
            'ddbtype': 'MONTH',
        },

        # time
        'time_0': {
            'value': '00:00:00.000t',
            'expect': np.datetime64('1970-01-01T00:00:00.000', 'ms'),
            'dtype': 'datetime64[ms]',
            'contain_none': {
                'expect': np.datetime64('nat', 'ms'),
                'dtype': 'datetime64[ms]',
            },
            'ddbtype': 'TIME',
        },
        'time_none': {
            'value': '00t',
            'expect': None,
            'dtype': 'datetime64[ms]',
            'contain_none': {
                'expect': np.datetime64('nat', 'ms'),
                'dtype': 'datetime64[ms]',
            },
            'ddbtype': 'TIME',
        },

        # minute
        'minute_0': {
            'value': '00:00m',
            'expect': np.datetime64('1970-01-01T00:00', 'm'),
            'dtype': 'datetime64[m]',
            'contain_none': {
                'expect': np.datetime64('nat', 'm'),
                'dtype': 'datetime64[m]',
            },
            'ddbtype': 'MINUTE',
        },
        'minute_none': {
            'value': '00m',
            'expect': None,
            'dtype': 'datetime64[m]',
            'contain_none': {
                'expect': np.datetime64('nat', 'm'),
                'dtype': 'datetime64[m]',
            },
            'ddbtype': 'MINUTE',
        },

        # second
        'second_0': {
            'value': '00:00:00s',
            'expect': np.datetime64('1970-01-01T00:00:00', 's'),
            'dtype': 'datetime64[s]',
            'contain_none': {
                'expect': np.datetime64('nat', 's'),
                'dtype': 'datetime64[s]',
            },
            'ddbtype': 'SECOND',
        },
        'second_none': {
            'value': '00s',
            'expect': None,
            'dtype': 'datetime64[s]',
            'contain_none': {
                'expect': np.datetime64('nat', 's'),
                'dtype': 'datetime64[s]',
            },
            'ddbtype': 'SECOND',
        },

        # datetime
        'datetime_0': {
            'value': '1970.01.01T00:00:00D',
            'expect': np.datetime64('1970-01-01T00:00:00', 's'),
            'dtype': 'datetime64[s]',
            'contain_none': {
                'expect': np.datetime64('nat', 's'),
                'dtype': 'datetime64[s]',
            },
            'ddbtype': 'DATETIME',
        },
        'datetime_none': {
            'value': '00D',
            'expect': None,
            'dtype': 'datetime64[s]',
            'contain_none': {
                'expect': np.datetime64('nat', 's'),
                'dtype': 'datetime64[s]',
            },
            'ddbtype': 'DATETIME',
        },

        # timestamp
        'timestamp_0': {
            'value': '1970.01.01 00:00:00.000T',
            'expect': np.datetime64('1970-01-01T00:00:00.000', 'ms'),
            'dtype': 'datetime64[ms]',
            'contain_none': {
                'expect': np.datetime64('nat', 'ms'),
                'dtype': 'datetime64[ms]',
            },
            'ddbtype': 'TIMESTAMP',
        },
        'timestamp_none': {
            'value': '00T',
            'expect': None,
            'dtype': 'datetime64[ms]',
            'contain_none': {
                'expect': np.datetime64('nat', 'ms'),
                'dtype': 'datetime64[ms]',
            },
            'ddbtype': 'TIMESTAMP',
        },

        # nanotime
        'nanotime_0': {
            'value': '00:00:00.000000000n',
            'expect': np.datetime64('1970-01-01T00:00:00.000000000', 'ns'),
            'dtype': 'datetime64[ns]',
            'contain_none': {
                'expect': np.datetime64('nat', 'ns'),
                'dtype': 'datetime64[ns]',
            },
            'ddbtype': 'NANOTIME',
        },
        'nanotime_none': {
            'value': '00n',
            'expect': None,
            'dtype': 'datetime64[ns]',
            'contain_none': {
                'expect': np.datetime64('nat', 'ns'),
                'dtype': 'datetime64[ns]',
            },
            'ddbtype': 'NANOTIME',
        },

        # nanotimestamp
        'nanotimestamp_0': {
            'value': '1970.01.01 00:00:00.000000000N',
            'expect': np.datetime64('1970-01-01T00:00:00.000000000', 'ns'),
            'dtype': 'datetime64[ns]',
            'contain_none': {
                'expect': np.datetime64('nat', 'ns'),
                'dtype': 'datetime64[ns]',
            },
            'ddbtype': 'NANOTIMESTAMP',
        },
        'nanotimestamp_none': {
            'value': '00N',
            'expect': None,
            'dtype': 'datetime64[ns]',
            'contain_none': {
                'expect': np.datetime64('nat', 'ns'),
                'dtype': 'datetime64[ns]',
            },
            'ddbtype': 'NANOTIMESTAMP',
        },

        # float
        'float_0': {
            'value': '0.0f',
            'expect': 0.0,
            'dtype': np.float32,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float32,
            },
            'ddbtype': 'FLOAT',
        },
        'float_nan': {
            'value': "float('nan')",
            'expect': float('nan'),
            'dtype': np.float32,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float32,
            },
            'ddbtype': 'FLOAT',
        },
        'float_inf': {
            'value': "float('inf')",
            'expect': float('inf'),
            'dtype': np.float32,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float32,
            },
            'ddbtype': 'FLOAT',
        },
        'float_none': {
            'value': '00f',
            'expect': None,
            'dtype': np.float32,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float32,
            },
            'ddbtype': 'FLOAT',
        },

        # double
        'double_0': {
            'value': '0.0F',
            'expect': 0.0,
            'dtype': np.float64,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'DOUBLE',
        },
        'double_pi': {
            'value': 'pi',
            'expect': math.pi,
            'dtype': np.float64,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'DOUBLE',
        },
        'double_none': {
            'value': '00F',
            'expect': None,
            'dtype': np.float64,
            'contain_none': {
                'expect': np.nan,
                'dtype': np.float64,
            },
            'ddbtype': 'DOUBLE',
        },

        # todo:symbol-vector

        # string
        'string': {
            'value': "'abc!@#中文 123'",
            'expect': 'abc!@#中文 123',
            'dtype': 'object',
            'contain_none': {
                'expect': '',
                'dtype': 'object',
            },
            'ddbtype': 'STRING',
        },

        # uuid
        'uuid': {
            'value': "uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')",
            'expect': '5d212a78-cc48-e3b1-4235-b4d91473ee87',
            'dtype': 'object',
            'contain_none': {
                'expect': '00000000-0000-0000-0000-000000000000',
                'dtype': 'object',
            },
            'ddbtype': 'UUID',
        },

        # TODO:any-vector

        # datehour
        'datehour_0': {
            'value': "datehour('1970.01.01T00')",
            'expect': np.datetime64('1970-01-01T00', 'h'),
            'dtype': 'datetime64[h]',
            'contain_none': {
                'expect': np.datetime64('nat', 'h'),
                'dtype': 'datetime64[h]',
            },
            'ddbtype': 'DATEHOUR',
        },

        # ipaddr
        'ipaddr': {
            'value': "ipaddr('127.0.0.1')",
            'expect': '127.0.0.1',
            'dtype': 'object',
            'contain_none': {
                'expect': '0.0.0.0',
                'dtype': 'object',
            },
            'ddbtype': 'IPADDR',
        },

        # int128
        'int128': {
            'value': "int128('e1671797c52e15f763380b45e841ec32')",
            'expect': 'e1671797c52e15f763380b45e841ec32',
            'dtype': 'object',
            'contain_none': {
                'expect': '00000000000000000000000000000000',
                'dtype': 'object',
            },
            'ddbtype': 'INT128',
        },

        # blob
        'blob': {
            'value': "blob('abc!@#中文 123')",
            'expect': 'abc!@#中文 123',
            'dtype': 'object',
            'contain_none': {
                'expect': '',
                'dtype': 'object',
            },
            'ddbtype': 'BLOB',
        },

        # decimal
        'decimal32': {
            'value': '"0"$DECIMAL32(3)',
            'expect': Decimal('0.000'),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL32(3)',
        },
        'decimal32_8': {
            'value': "'3.141592653589'$DECIMAL32(8)",
            'expect': Decimal('3.14159265'),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL32(8)',
        },
        'decimal32_9': {
            'value': "'-0.14159265358'$DECIMAL32(9)",
            'expect': Decimal('-0.141592654'),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL32(9)',
        },
        'decimal32_nan': {
            'value': 'decimal32("nan",0)',
            'expect': None,
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL32(0)',
        },
        'decimal64': {
            'value': '"0"$DECIMAL64(3)',
            'expect': Decimal('0.000'),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL64(3)',
        },
        'decimal64_17': {
            'value': "'3.14159265358979323846'$DECIMAL64(17)",
            'expect': Decimal('3.14159265358979324'),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL64(17)',
        },
        'decimal64_18': {
            'value': "'-0.14159265358979323846'$DECIMAL64(18)",
            'expect': Decimal('-0.141592653589793238'),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL64(18)',
        },
        'decimal64_nan': {
            'value': 'decimal64("nan",0)',
            'expect': None,
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL64(0)',
        },
        'decimal64_null': {
            'value': '00P',
            'expect': Decimal(0),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL64(0)',
        },
        'decimal128': {
            'value': '"0"$DECIMAL128(3)',
            'expect': Decimal('0.000'),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL128(3)',
        },
        'decimal128_37': {
            'value': "'3.14159265358979323846264338327950288419'$DECIMAL128(37)",
            'expect': Decimal('3.1415926535897932384626433832795028842'),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL128(37)',
        },
        'decimal128_38': {
            'value': "'-0.14159265358979323846264338327950288419'$DECIMAL128(38)",
            'expect': Decimal('-0.14159265358979323846264338327950288419'),
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL128(38)',
        },
        'decimal128_nan': {
            'value': 'decimal128("nan",0)',
            'expect': None,
            'dtype': 'object',
            'contain_none': {
                'expect': None,
                'dtype': 'object',
            },
            'ddbtype': 'DECIMAL128(0)',
        },
    }
    if find_spec("pyarrow") is not None:
        DATA_UPLOAD_ARROW = {
            'data_arrow_int8_0': {
                'value': 0,
                'dtype_arrow': pa.int8(),
                'expect_typestr': "'FAST CHAR VECTOR'",
                'expect_value': 0,
            },
            'data_arrow_int8_max': {
                'value': 2 ** 7 - 1,
                'dtype_arrow': pa.int8(),
                'expect_typestr': "'FAST CHAR VECTOR'",
                'expect_value': 127,
            },
            'data_arrow_int8_min': {
                'value': -2 ** 7 + 1,
                'dtype_arrow': pa.int8(),
                'expect_typestr': "'FAST CHAR VECTOR'",
                'expect_value': -127,
            },
            'data_arrow_int8_none': {
                'value': -2 ** 7,
                'dtype_arrow': pa.int8(),
                'expect_typestr': "'FAST CHAR VECTOR'",
                'expect_value': '00c',
            },
            'data_arrow_int16_0': {
                'value': 0,
                'dtype_arrow': pa.int16(),
                'expect_typestr': "'FAST SHORT VECTOR'",
                'expect_value': 0,
            },
            'data_arrow_int16_max': {
                'value': 2 ** 15 - 1,
                'dtype_arrow': pa.int16(),
                'expect_typestr': "'FAST SHORT VECTOR'",
                'expect_value': 2 ** 15 - 1,
            },
            'data_arrow_int16_min': {
                'value': -2 ** 15 + 1,
                'dtype_arrow': pa.int16(),
                'expect_typestr': "'FAST SHORT VECTOR'",
                'expect_value': -2 ** 15 + 1,
            },
            'data_arrow_int16_none': {
                'value': -2 ** 15,
                'dtype_arrow': pa.int16(),
                'expect_typestr': "'FAST SHORT VECTOR'",
                'expect_value': '00h',
            },
            'data_arrow_int32_0': {
                'value': 0,
                'dtype_arrow': pa.int32(),
                'expect_typestr': "'FAST INT VECTOR'",
                'expect_value': 0,
            },
            'data_arrow_int32_max': {
                'value': 2 ** 31 - 1,
                'dtype_arrow': pa.int32(),
                'expect_typestr': "'FAST INT VECTOR'",
                'expect_value': 2 ** 31 - 1,
            },
            'data_arrow_int32_min': {
                'value': -2 ** 31 + 1,
                'dtype_arrow': pa.int32(),
                'expect_typestr': "'FAST INT VECTOR'",
                'expect_value': -2 ** 31 + 1,
            },
            'data_arrow_int32_none': {
                'value': -2 ** 31,
                'dtype_arrow': pa.int32(),
                'expect_typestr': "'FAST INT VECTOR'",
                'expect_value': '00i',
            },
            'data_arrow_int64_0': {
                'value': 0,
                'dtype_arrow': pa.int64(),
                'expect_typestr': "'FAST LONG VECTOR'",
                'expect_value': 0,
            },
            'data_arrow_int64_max': {
                'value': 2 ** 63 - 1,
                'dtype_arrow': pa.int64(),
                'expect_typestr': "'FAST LONG VECTOR'",
                'expect_value': 2 ** 63 - 1,
            },
            'data_arrow_int64_min': {
                'value': -2 ** 63 + 1,
                'dtype_arrow': pa.int64(),
                'expect_typestr': "'FAST LONG VECTOR'",
                'expect_value': -2 ** 63 + 1,
            },
            'data_arrow_int64_none': {
                'value': -2 ** 63,
                'dtype_arrow': pa.int64(),
                'expect_typestr': "'FAST LONG VECTOR'",
                'expect_value': '00l',
            },
            'data_arrow_float32_0': {
                'value': 0,
                'dtype_arrow': pa.float32(),
                'expect_typestr': "'FAST FLOAT VECTOR'",
                'expect_value': 0,
            },
            'data_arrow_float32_nan': {
                'value': float('nan'),
                'dtype_arrow': pa.float32(),
                'expect_typestr': "'FAST FLOAT VECTOR'",
                'expect_value': '00f',
            },
            # 'data_arrow_float32_inf': {
            #     'value': float('inf'),
            #     'dtype_arrow': pa.float32(),
            #     'expect_typestr': "'FAST FLOAT VECTOR'",
            # },
            'data_arrow_float32_max': {
                'value': 3.4028235e+38,
                'dtype_arrow': pa.float32(),
                'expect_typestr': "'FAST FLOAT VECTOR'",
                'expect_value': 'float(3.4028235e+38)',
            },
            'data_arrow_float32_none': {
                'value': -3.4028235e+38,
                'dtype_arrow': pa.float32(),
                'expect_typestr': "'FAST FLOAT VECTOR'",
                'expect_value': '00f',
            },
            'data_arrow_float64_0': {
                'value': 0,
                'dtype_arrow': pa.float64(),
                'expect_typestr': "'FAST DOUBLE VECTOR'",
                'expect_value': 0,
            },
            'data_arrow_float64_nan': {
                'value': float('nan'),
                'dtype_arrow': pa.float64(),
                'expect_typestr': "'FAST DOUBLE VECTOR'",
                'expect_value': '00F',
            },
            # 'data_arrow_float64_inf': {
            #     'value': float('inf'),
            #     'dtype_arrow': pa.float64(),
            #     'expect_typestr': "'FAST DOUBLE VECTOR'",
            # },
            'data_arrow_float64_max': {
                'value': 1.7976931348623157e+308,
                'dtype_arrow': pa.float64(),
                'expect_typestr': "'FAST DOUBLE VECTOR'",
                'expect_value': 'double(1.7976931348623157e+308)',
            },
            'data_arrow_float64_none': {
                'value': -1.7976931348623157e+308,
                'dtype_arrow': pa.float64(),
                'expect_typestr': "'FAST DOUBLE VECTOR'",
                'expect_value': '00F',
            },
            'data_arrow_string': {
                'value': 'abc!@#中文 123',
                'dtype_arrow': pa.utf8(),
                'expect_typestr': "'STRING VECTOR'",
                'expect_value': "'abc!@#中文 123'",
            },
            'data_arrow_bytes_utf8': {
                'value': 'abc!@#中文 123'.encode(),
                'dtype_arrow': pa.large_binary(),
                'expect_typestr': "'BLOB VECTOR'",
                'expect_value': "'abc!@#中文 123'",
            },
            'data_arrow_bytes_gbk': {
                'value': 'abc!@#中文 123'.encode('gbk'),
                'dtype_arrow': pa.large_binary(),
                'expect_typestr': "'BLOB VECTOR'",
                'expect_value': "fromUTF8('abc!@#中文 123','gbk')",
            },
            'data_arrow_bool_true': {
                'value': True,
                'dtype_arrow': pa.bool_(),
                'expect_typestr': "'FAST BOOL VECTOR'",
                'expect_value': 'true',
            },
            'data_arrow_bool_false': {
                'value': False,
                'dtype_arrow': pa.bool_(),
                'expect_typestr': "'FAST BOOL VECTOR'",
                'expect_value': 'false',
            },
            'data_arrow_date32_0': {
                'value': 0,
                'dtype_arrow': pa.date32(),
                'expect_typestr': "'FAST DATE VECTOR'",
                'expect_value': "1970.01.01",
            },
            # not support
            # 'data_arrow_date64_0': {
            #     'value': 0,
            #     'dtype_arrow': pa.date64(),
            # },
            'data_arrow_time32_ms_0': {
                'value': 0,
                'dtype_arrow': pa.time32('ms'),
                'expect_typestr': "'FAST TIME VECTOR'",
                'expect_value': "00:00:00.000",
            },
            'data_arrow_time32_s_0': {
                'value': 0,
                'dtype_arrow': pa.time32('s'),
                'expect_typestr': "'FAST SECOND VECTOR'",
                'expect_value': "00:00:00",
            },
            'data_arrow_time64_ns_0': {
                'value': 0,
                'dtype_arrow': pa.time64('ns'),
                'expect_typestr': "'FAST NANOTIME VECTOR'",
                'expect_value': "00:00:00.000000000",
            },
            # not support
            # 'data_arrow_time64_us_0': {
            #     'value': 0,
            #     'dtype_arrow': pa.time64('us'),
            # },
            'data_arrow_timestamp_ns_0': {
                'value': 0,
                'dtype_arrow': pa.timestamp('ns'),
                'expect_typestr': "'FAST NANOTIMESTAMP VECTOR'",
                'expect_value': "1970.01.01T00:00:00.000000000",
            },
            'data_arrow_timestamp_ns_max': {
                'value': np.datetime64('2262-04-11T23:47:16.854775807', 'ns'),
                'dtype_arrow': pa.timestamp('ns'),
                'expect_typestr': "'FAST NANOTIMESTAMP VECTOR'",
                'expect_value': "2262.04.11T23:47:16.854775807",
            },
            'data_arrow_timestamp_ns_min': {
                'value': np.datetime64('1677-09-21T00:12:43.145224193', 'ns'),
                'dtype_arrow': pa.timestamp('ns'),
                'expect_typestr': "'FAST NANOTIMESTAMP VECTOR'",
                'expect_value': "1677.09.21T00:12:43.145224193",
            },
            'data_arrow_timestamp_ns_none': {
                'value': np.datetime64('1677-09-21T00:12:43.145224192', 'ns'),
                'dtype_arrow': pa.timestamp('ns'),
                'expect_typestr': "'FAST NANOTIMESTAMP VECTOR'",
                'expect_value': "00N",
            },
            # not support
            # 'data_arrow_timestamp_us_0': {
            #     'value': 0,
            #     'dtype_arrow': pa.timestamp('us'),
            # },
            'data_arrow_timestamp_ms_0': {
                'value': 0,
                'dtype_arrow': pa.timestamp('ms'),
                'expect_typestr': "'FAST TIMESTAMP VECTOR'",
                'expect_value': "1970.01.01T00:00:00.000",
            },
            'data_arrow_timestamp_s_0': {
                'value': 0,
                'dtype_arrow': pa.timestamp('s'),
                'expect_typestr': "'FAST DATETIME VECTOR'",
                'expect_value': "1970.01.01T00:00:00",
            },
            'data_arrow_decimal128': {
                'value': Decimal('0.00'),
                'dtype_arrow': pa.decimal128(3, 2),
                'expect_typestr': "'FAST DECIMAL128 VECTOR'",
                'expect_value': "decimal128('0.00',2)",
            },
            'data_arrow_decimal128_nan': {
                'value': Decimal('nan'),
                'dtype_arrow': pa.decimal128(3, 2),
                'expect_typestr': "'FAST DECIMAL128 VECTOR'",
                'expect_value': "decimal64(NULL,2)",
            },
            # not support
            # 'data_arrow_decimal256_0': {
            #     'value': 0,
            #     'dtype_arrow': pa.decimal256(2),
            # },
            'data_arrow_symbol': {
                'value': 'aaa',
                'dtype_arrow': pa.dictionary(pa.int32(), pa.utf8()),
                'expect_typestr': "'FAST SYMBOL VECTOR'",
                'expect_value': "'aaa'",
            },
            'data_arrow_uuid': {
                'value': UUID('5d212a78-cc48-e3b1-4235-b4d91473ee87').bytes,
                'dtype_arrow': pa.binary(16),
                'expect_typestr': "'FAST UUID VECTOR'",
                'expect_value': "uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87')"
            },
            'data_arrow_int128': {
                'value': UUID('e1671797c52e15f763380b45e841ec32').bytes,
                'dtype_arrow': pa.binary(16),
                'expect_typestr': "'FAST INT128 VECTOR'",
                'expect_value': "int128('e1671797c52e15f763380b45e841ec32')"
            },
        }

    DATA_TYPE = {
        'VOID': {
            'dtype': 'object',
        },
        'BOOL': {
            'dtype': np.bool_,
        },
        'CHAR': {
            'dtype': np.int8,
        },
        'SHORT': {
            'dtype': np.int16,
        },
        'INT': {
            'dtype': np.int32,
        },
        'LONG': {
            'dtype': np.int64,
        },
        'DATE': {
            'dtype': 'datetime64[D]',
        },
        'MONTH': {
            'dtype': 'datetime64[M]',
        },
        'TIME': {
            'dtype': 'datetime64[ms]',
        },
        'MINUTE': {
            'dtype': 'datetime64[m]',
        },
        'SECOND': {
            'dtype': 'datetime64[s]',
        },
        'DATETIME': {
            'dtype': 'datetime64[s]',
        },
        'TIMESTAMP': {
            'dtype': 'datetime64[ms]',
        },
        'NANOTIME': {
            'dtype': 'datetime64[ns]',
        },
        'NANOTIMESTAMP': {
            'dtype': 'datetime64[ns]',
        },
        'FLOAT': {
            'dtype': np.float32,
        },
        'DOUBLE': {
            'dtype': np.float64,
        },
        'SYMBOL': {
            'dtype': 'object',
        },
        'STRING': {
            'dtype': 'object',
        },
        'UUID': {
            'dtype': 'object',
        },
        # 'FUNCTIONDEF',
        # 'HANDLE',
        # 'CODE',
        # 'DATASOURCE',
        # 'RESOURCE',
        'ANY': {
            # TODO
        },
        # 'COMPRESS',
        # 'ANY DICTIONARY',
        'DATEHOUR': {
            'dtype': 'datetime64[h]',
        },
        'IPADDR': {
            'dtype': 'object',
        },
        'INT128': {
            'dtype': 'object',
        },
        'BLOB': {
            'dtype': 'object',
        },
        # 'COMPLEX',
        # 'POINT',
        # 'DURATION',
        'DECIMAL32(2)': {
            'dtype': 'object',
        },
        'DECIMAL64(2)': {
            'dtype': 'object',
        },
        'DECIMAL128(2)': {
            'dtype': 'object',
        },
    }

    @classmethod
    def getScalar(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'scalar'): v
                   for k, v in cls.DATA_UPLOAD.items()
                   if k not in (
                       'data_decimal_2',
                       'data_decimal_nan',
                       'data_decimal_17',
                       'data_decimal_18',
                       'data_decimal_38',
                   )}
            rtn.update({k.replace('data', 'scalar'): {
                'value': v['value'],
                'expect_typestr': "'DECIMAL64'",
                'expect_value': v['expect_value'],
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                )
            })
            rtn.update({k.replace('data', 'scalar'): {
                'value': v['value'],
                'expect_typestr': "'DECIMAL128'",
                'expect_value': v['expect_value'],
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_decimal_18',
                    'data_decimal_38',
                )
            })
            return rtn
        else:
            return {k: v for k, v in cls.DATA_DOWNLOAD.items()}

    @classmethod
    def getPair(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            return {}  # pair not support
        else:
            rtn = {k: {
                'value': f"{v['value']} : {v['value']}",
                'expect': [v['expect'], v['expect']],
            } for k, v in cls.DATA_DOWNLOAD.items() if '$' not in v['value']
            }
            rtn.update({
                k: {
                    'value': f"x={v['value']}\nx:x",
                    'expect': [v['expect'], v['expect']],
                } for k, v in cls.DATA_DOWNLOAD.items() if '$' in v['value']
            })
            return rtn

    @classmethod
    def getPairContainNone(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            return {}  # pair not support
        else:
            rtn = {k: {
                'value': f"NULL : {v['value']}",
                'expect': [None, v['expect']],
            } for k, v in cls.DATA_DOWNLOAD.items() if '$' not in v['value']
            }
            rtn.update({
                k: {
                    'value': f"x={v['value']}\nNULL:x",
                    'expect': [None, v['expect']],
                } for k, v in cls.DATA_DOWNLOAD.items() if '$' in v['value']
            })
            return rtn

    @classmethod
    def getVector(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            # list
            strongTypeVector = {k.replace('data', 'strongTypeVector'): {
                'value': [v['value'], v['value'], v['value']],
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB'
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},{v['expect_value']},{v['expect_value']}]"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }
            strongTypeVector.update({k.replace('data', 'strongTypeVector'): {
                'value': [v['value'], v['value'], v['value']],
                'expect_typestr': "'ANY VECTOR'",
                'expect_value': f"({v['expect_value']},{v['expect_value']},{v['expect_value']})"
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_none',
                )
            })

            # np.ndArray
            strongTypeVector_np = {k.replace('data', 'strongTypeVector_np'): {
                'value': np.array([v['value'], v['value'], v['value']], dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB'
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},{v['expect_value']},{v['expect_value']}]"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }
            strongTypeVector_np.update({k.replace('data', 'strongTypeVector_np'): {
                'value': np.array([v['value'], v['value'], v['value']], dtype='object'),
                'expect_typestr': "'ANY VECTOR'",
                'expect_value': f"({v['expect_value']},{v['expect_value']},{v['expect_value']})"
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_none',
                )
            })
            return {**strongTypeVector, **strongTypeVector_np}
        else:
            rtn = {k: {
                'value': f"[{v['value']},{v['value']},{v['value']}]",
                'expect': np.array([v['expect'], v['expect'], v['expect']], dtype=v['dtype']),
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                    'float_none',
                    'double_none',
                )
            }
            rtn.update({k: {
                'value': f"[{v['value']},{v['value']},{v['value']}]",
                'expect': np.array([np.nan, np.nan, np.nan], dtype=np.float64),
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k in (
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                    'double_none',
                )})
            rtn.update({
                'float_none': {
                    'value': f"[00f,00f,00f]",
                    'expect': np.array([np.nan, np.nan, np.nan], dtype=np.float32),
                },
                'symbol': {
                    'value': "sym=`IBM`C`MS\nsym$SYMBOL",
                    'expect': np.array(['IBM', 'C', 'MS'], dtype='object')
                },
                'any': {
                    'value': '(1,2,3)',
                    'expect': [1, 2, 3]
                },
            })
            return rtn

    @classmethod
    def getVectorContainNone(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            # list
            noneFirstVector = {k.replace('data', 'noneFirstVector'): {
                'value': [pd.NaT if 'datetime64' in k else None, v['value'], v['value']],
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB',
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[,{v['expect_value']},{v['expect_value']}]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }
            noneFirstVector.update({k.replace('data', 'noneFirstVector'): {
                'value': [None, v['value'], v['value']],
                'expect_typestr': "'ANY VECTOR'",
                'expect_value': "(,,)",
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_none',
                )
            })

            noneMiddleVector = {k.replace('data', 'noneMiddleVector'): {
                'value': [v['value'], pd.NaT if 'datetime64' in k else None, v['value']],
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB',
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},,{v['expect_value']}]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }

            noneLastVector = {k.replace('data', 'noneLastVector'): {
                'value': [v['value'], v['value'], pd.NaT if 'datetime64' in k else None],
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB',
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},{v['expect_value']},]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }

            # np.ndarray
            noneFirstVector_np = {k.replace('data', 'noneFirstVector_np'): {
                'value': np.array([pd.NaT if 'datetime64' in k else None, v['value'], v['value']], dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB',
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[,{v['expect_value']},{v['expect_value']}]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }
            noneFirstVector_np.update({k.replace('data', 'noneFirstVector_np'): {
                'value': np.array([None, v['value'], v['value']], dtype='object'),
                'expect_typestr': "'ANY VECTOR'",
                'expect_value': "(,,)",
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_none',
                )
            })

            noneMiddleVector_np = {k.replace('data', 'noneMiddleVector_np'): {
                'value': np.array([v['value'], pd.NaT if 'datetime64' in k else None, v['value']], dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB',
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},,{v['expect_value']}]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }

            noneLastVector_np = {k.replace('data', 'noneLastVector_np'): {
                'value': np.array([v['value'], v['value'], pd.NaT if 'datetime64' in k else None], dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB',
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},{v['expect_value']},]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }
            return {**noneFirstVector, **noneMiddleVector, **noneLastVector,
                    **noneFirstVector_np, **noneMiddleVector_np, **noneLastVector_np}
        else:
            rtn = {k: {
                'value': f"[NULL,{v['value']},{v['value']}]",
                'expect': np.array([v['contain_none']['expect'], v['expect'], v['expect']],
                                   dtype=v['contain_none']['dtype'])
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                    'float_none',
                    'double_none',
                )
            }
            rtn.update({
                'any': {
                    'value': '(NULL,2,3)',
                    'expect': [None, 2, 3]
                },
            })
            return rtn

    # todo:download
    @classmethod
    def getVectorMix(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            return {
                'mixVector': {
                    'value': [i['value'] for i in cls.DATA_UPLOAD.values()],
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': f"[{','.join([i['expect_value'] for i in cls.DATA_UPLOAD.values()])}]"
                },
                'mixVector_np': {
                    'value': np.array([i['value'] for i in cls.DATA_UPLOAD.values()], dtype='object'),
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': f"[{','.join([i['expect_value'] for i in cls.DATA_UPLOAD.values()])}]"
                }
            }
        else:
            return {}

    @classmethod
    def getVectorSpecial(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {'vectorSpecial_empty': {
                'value': [],
                'expect_typestr': "'FAST DOUBLE VECTOR'",
                'expect_value': '()',
            },
                'vectorSpecial_vector': {
                    'value': [[1]],
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': '[[1]]',
                },
                'vectorSpecial_set': {
                    'value': [{1}],
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': '[set([1])]',
                },
                'vectorSpecial_dict': {
                    'value': [{1: 1}],
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': '[dict([1],[1])]',
                },
                'vectorSpecial_table': {
                    'value': [pd.DataFrame({'a': [1]}, dtype='object')],
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': '[table([1] as `a)]',
                },
                'vectorSpecial_composite': {
                    'value': [1, [1], {1}, {1: 1}, pd.DataFrame({'a': [1]}, dtype='object')],
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': "[1,[1],set([1]),dict([1],[1]),table([1] as `a)]",
                },
            }
            rtn.update({
                'vectorSpecial_np_set': {
                    'value': np.array([{1}], dtype='object'),
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': '[set([1])]',
                },
                'vectorSpecial_np_dict': {
                    'value': np.array([{1: 1}], dtype='object'),
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': '[dict([1],[1])]',
                },
                # 'vectorSpecial_np_table': {
                #     'value': np.array([pd.DataFrame({'a': [1]}, dtype='object')],dtype='object'),
                #     'expect_typestr': "'ANY VECTOR'",
                #     'expect_value': '[table([1] as `a)]',
                # },
                'vectorSpecial_np_composite': {
                    'value': np.array([1, [1], {1}, {1: 1}, pd.DataFrame({'a': [1]}, dtype='object')], dtype='object'),
                    'expect_typestr': "'ANY VECTOR'",
                    'expect_value': "[1,[1],set([1]),dict([1],[1]),table([1] as `a)]",
                },
            })
            rtn.update({
                'vectorSpecial_np_empty_' + k.replace('[', '_').replace(']', ''): {
                    'value': np.array([], dtype=k),
                    'expect_typestr': v,
                    'expect_value': "()",
                } for k, v in {
                    'int8': "'FAST CHAR VECTOR'",
                    'int16': "'FAST SHORT VECTOR'",
                    'int32': "'FAST INT VECTOR'",
                    'int64': "'FAST LONG VECTOR'",
                    'float32': "'FAST FLOAT VECTOR'",
                    'float64': "'FAST DOUBLE VECTOR'",
                    # 'longdouble',
                    'bool': "'FAST BOOL VECTOR'",
                    'datetime64[ns]': "'FAST NANOTIMESTAMP VECTOR'",
                    'datetime64[us]': "'FAST NANOTIMESTAMP VECTOR'",
                    'datetime64[ms]': "'FAST TIMESTAMP VECTOR'",
                    'datetime64[s]': "'FAST DATETIME VECTOR'",
                    'datetime64[m]': "'FAST DATETIME VECTOR'",
                    'datetime64[h]': "'FAST DATEHOUR VECTOR'",
                    'datetime64[D]': "'FAST DATE VECTOR'",
                    'datetime64[M]': "'FAST MONTH VECTOR'",
                    'object': "'FAST DOUBLE VECTOR'",
                }.items()
            })
            return rtn
        else:
            rtn = {
                'vecorSpecial_empty': {
                    'value': '()',
                    'expect': [],
                },
                'vectorSpecial_pair': {
                    'value': '[1:2]',
                    'expect': [[1, 2]],
                },
                'vectorSpecial_vector': {
                    'value': '[[1]]',
                    'expect': [np.array([1], dtype=np.int32)],
                },
                'vectorSpecial_matrix': {
                    'value': '[1..6$2:3]',
                    'expect': [[np.array([[1, 3, 5], [2, 4, 6]], dtype=np.int32), None, None]],
                },
                'vectorSpecial_set': {
                    'value': '[set([1])]',
                    'expect': [{1}],
                },
                'vectorSpecial_dict': {
                    'value': '[dict([1],[1])]',
                    'expect': [{1: 1}],
                },
                'vectorSpecial_table': {
                    'value': '[table([1] as `a)]',
                    'expect': [pd.DataFrame({'a': [1]}, dtype=np.int32)],
                },
            }
            rtn.update({
                f'vectorSpecial_empty_{k}': {
                    'value': f"array({k},0,3)",
                    'expect': np.array([], dtype=v['dtype']),
                } for k, v in cls.DATA_TYPE.items()
                if k not in (
                    'VOID',
                    'ANY'
                )
            })
            return rtn

    @classmethod
    def getMatrix(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'matrix'): {
                'value': np.array([[v['value'], v['value'], v['value']], [v['value'], v['value'], v['value']]],
                                  dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} MATRIX'",
                'expect_value': f"matrix([[{v['expect_value']},{v['expect_value']}],[{v['expect_value']},{v['expect_value']}],[{v['expect_value']},{v['expect_value']}]])"}
                for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_string',
                    'data_numpy_str',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )}
            return rtn
        else:
            rtn = {k: {
                'value': f"matrix([{v['value']},{v['value']},{v['value']}])",
                'expect': [np.array([[v['expect']], [v['expect']], [v['expect']]], dtype=v['dtype']), None, None],
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'time_0',
                    'time_none',
                    'minute_0',
                    'minute_none',
                    'second_0',
                    'second_none',
                    'nanotime_0',
                    'nanotime_none',
                    'string',
                    'uuid',
                    'ipaddr',
                    'int128',
                    'blob',
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                    'float_none',
                    'double_none',
                    'decimal32',
                    'decimal32_8',
                    'decimal32_9',
                    'decimal32_nan',
                    'decimal64',
                    'decimal64_17',
                    'decimal64_18',
                    'decimal64_nan',
                    'decimal64_null',
                    'decimal128',
                    'decimal128_37',
                    'decimal128_38',
                    'decimal128_nan',
                )
            }
            rtn.update({k: {
                'value': f"matrix([{v['value']},{v['value']},{v['value']}])",
                'expect': [np.array([[np.nan], [np.nan], [np.nan]], dtype=np.float64), None, None],
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k in (
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                    'double_none',
                )
            })
            rtn.update({
                'float_none': {
                    'value': f"matrix([00f,00f,00f])",
                    'expect': [np.array([[np.nan], [np.nan], [np.nan]], dtype=np.float32), None, None],
                },
            })
            return rtn

    @classmethod
    def getMatrixContainNone(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'matrix'): {
                'value': np.array([[v['value'], v['value'], v['value']], [v['value'], None, v['value']]],
                                  dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} MATRIX'",
                'expect_value': f"matrix[[{v['expect_value']},{v['expect_value']}],[{v['expect_value']},NULL],[{v['expect_value']},{v['expect_value']}]]"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_string',
                    'data_numpy_str',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )}
            return rtn
        else:
            rtn = {k: {
                'value': f"matrix([NULL,{v['value']},{v['value']}])",
                'expect': [np.array([[v['contain_none']['expect']], [v['expect']], [v['expect']]],
                                    dtype=v['contain_none']['dtype']), None, None],
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'string',
                    'uuid',
                    'ipaddr',
                    'int128',
                    'blob',
                    'decimal32',
                    'decimal32_8',
                    'decimal32_9',
                    'decimal32_nan',
                    'decimal64',
                    'decimal64_17',
                    'decimal64_18',
                    'decimal64_nan',
                    'decimal64_null',
                    'decimal128',
                    'decimal128_37',
                    'decimal128_38',
                    'decimal128_nan',
                )
            }
            return rtn

    @classmethod
    def getMatrixSpecial(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {
                'matrixSpecial_empty_' + k.replace('[', '_').replace(']', ''): {
                    'value': np.array([[], []], dtype=k),
                    'expect_typestr': v,
                    # 'expect_value':f"matrix(array({v.split(' ')[1]}[],0,0).append!([[],[]]))"
                } for k, v in {
                    'int8': "'FAST CHAR MATRIX'",
                    'int16': "'FAST SHORT MATRIX'",
                    'int32': "'FAST INT MATRIX'",
                    'int64': "'FAST LONG MATRIX'",
                    'float32': "'FAST FLOAT MATRIX'",
                    'float64': "'FAST DOUBLE MATRIX'",
                    # 'longdouble',
                    'bool': "'FAST BOOL MATRIX'",
                    'datetime64[ns]': "'FAST NANOTIMESTAMP MATRIX'",
                    'datetime64[us]': "'FAST NANOTIMESTAMP MATRIX'",
                    'datetime64[ms]': "'FAST TIMESTAMP MATRIX'",
                    'datetime64[s]': "'FAST DATETIME MATRIX'",
                    'datetime64[m]': "'FAST DATETIME MATRIX'",
                    'datetime64[h]': "'FAST DATEHOUR MATRIX'",
                    'datetime64[D]': "'FAST DATE MATRIX'",
                    'datetime64[M]': "'FAST MONTH MATRIX'",
                    'object': "'FAST DOUBLE MATRIX'",
                }.items()
            }
            return rtn
        else:
            return {}

    @classmethod
    def getSet(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'set'): {
                'value': {v['value']},
                'expect_typestr': f"'{v['expect_typestr']} SET'",
                'expect_value': f"set([{v['expect_value']}])",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_numpy_nan',
                    'data_pandas_nat',
                    'data_nan',
                    'data_bool_true',
                    'data_bool_false',
                    'data_numpy_bool_true',
                    'data_numpy_bool_false',
                    'data_numpy_int8_none',
                    'data_numpy_int16_none',
                    'data_numpy_int32_none',
                    'data_numpy_int64_none',
                    'data_numpy_float32_none',
                    'data_numpy_float64_none',
                    'data_numpy_longdouble_none',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )}
            return rtn
        else:
            return {k: {
                'value': f"set([{v['value']}])",
                'expect': {v['expect']}
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'bool_true',
                    'bool_false',
                    'decimal32',
                    'decimal32_8',
                    'decimal32_9',
                    'decimal32_nan',
                    'decimal64',
                    'decimal64_17',
                    'decimal64_18',
                    'decimal64_nan',
                    'decimal64_null',
                    'decimal128',
                    'decimal128_37',
                    'decimal128_38',
                    'decimal128_nan',
                )
            }

    @classmethod
    def getSetSpecial(cls, _type):
        if _type.lower() == 'upload':
            return {
                # todo:bug
                'set_empty': {
                    'value': set(),
                    'expect_typestr': '',
                    'expect_value': '',
                },
            }
        else:
            return {
                f'setSpecial_embty_{k}': {
                    'value': f"set(array({k}, 0, 20))",
                    'expect': set(),
                } for k in cls.DATA_TYPE
                if k not in (
                    'VOID',
                    'BOOL',
                    'ANY',
                    'DECIMAL32(2)',
                    'DECIMAL64(2)',
                    'DECIMAL128(2)',
                )
            }

    @classmethod
    def getSetContainNone(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'set'): {
                'value': {v['value'], None},
                'expect_typestr': f"'{v['expect_typestr']} SET'",
                'expect_value': f"set([{v['expect_value']},NULL])",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_numpy_nan',
                    'data_pandas_nat',
                    'data_nan',
                    'data_bool_true',
                    'data_bool_false',
                    'data_numpy_bool_true',
                    'data_numpy_bool_false',
                    'data_numpy_int8_none',
                    'data_numpy_int16_none',
                    'data_numpy_int32_none',
                    'data_numpy_int64_none',
                    'data_numpy_float32_none',
                    'data_numpy_float64_none',
                    'data_numpy_longdouble_none',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )}
            return rtn
        else:
            return {k: {
                'value': f"set([{v['value']},NULL])",
                'expect': {v['expect'], None}
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'bool_true',
                    'bool_false',
                    'decimal32',
                    'decimal32_8',
                    'decimal32_9',
                    'decimal32_nan',
                    'decimal64',
                    'decimal64_17',
                    'decimal64_18',
                    'decimal64_nan',
                    'decimal64_null',
                    'decimal128',
                    'decimal128_37',
                    'decimal128_38',
                    'decimal128_nan',
                )
            }

    @classmethod
    def getDict(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'dict'): {
                'value': {'a': v['value']},
                'expect_typestr': f"'STRING->{v['expect_typestr']} DICTIONARY'",
                'expect_value': v['expect_value'],
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_numpy_nan',
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_int8_none',
                    'data_numpy_int16_none',
                    'data_numpy_int32_none',
                    'data_numpy_int64_none',
                    'data_numpy_float32_none',
                    'data_numpy_float64_none',
                    'data_numpy_longdouble_none',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )}
            rtn.update({k.replace('data', 'dict'): {
                'value': {'a': v['value']},
                'expect_typestr': f"'STRING->ANY DICTIONARY'",
                'expect_value': v['expect_value'],
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_decimal_2',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            })
            return rtn
        else:
            return {k: {
                'value': f"dict([1],[{v['value']}])",
                'expect': {1: v['expect']},
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                )
            }

    @classmethod
    def getDictContainNone(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'dict'): {
                'value': {'b': None, 'a': v['value']},
                'expect_typestr': f"'STRING->{v['expect_typestr']} DICTIONARY'",
                'expect_value': v['expect_value'],
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_numpy_nan',
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_int8_none',
                    'data_numpy_int16_none',
                    'data_numpy_int32_none',
                    'data_numpy_int64_none',
                    'data_numpy_float32_none',
                    'data_numpy_float64_none',
                    'data_numpy_longdouble_none',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )}
            rtn.update({k.replace('data', 'dict'): {
                'value': {'a': v['value'], 'b': None},
                'expect_typestr': f"'STRING->ANY DICTIONARY'",
                'expect_value': v['expect_value'],
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_decimal_2',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            })
            return rtn
        else:
            return {k: {
                'value': f"dict([1,2],[{v['value']},NULL])",
                'expect': {1: v['expect'], 2: None},
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                )
            }

    @classmethod
    def getDictMix(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {'dictMix': {
                'value': {k: v['value'] for k, v in cls.DATA_UPLOAD.items()},
                'expect_typestr': "'STRING->ANY DICTIONARY'",
                'expect_value': f"dict([{','.join('`' + i for i in cls.DATA_UPLOAD)}],[{','.join(i['expect_value'] for i in cls.DATA_UPLOAD.values())}])"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getDictKey(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'dict_key'): {
                'value': {v['value']: 'a'},
                'expect_typestr': f"'{v['expect_typestr']}->STRING DICTIONARY'",
                'expect_value': v['expect_value'],
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_numpy_nan',
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_int8_none',
                    'data_numpy_int16_none',
                    'data_numpy_int32_none',
                    'data_numpy_int64_none',
                    'data_numpy_float32_none',
                    'data_numpy_float64_none',
                    'data_numpy_longdouble_none',
                    'data_numpy_datetime64_ns_none',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_bool_true',
                    'data_bool_false',
                    'data_numpy_bool_true',
                    'data_numpy_bool_false',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                    'data_float',
                    'data_pi',
                    'data_numpy_float32_0',
                    'data_numpy_float32_max',
                    'data_numpy_float64_0',
                    'data_numpy_float64_max',
                    'data_numpy_datetime64_ns_0',
                    'data_numpy_datetime64_ns_max',
                    'data_numpy_datetime64_ns_min',
                    'data_numpy_datetime64_us_0',
                    'data_numpy_datetime64_ms',
                    'data_numpy_datetime64_s',
                    'data_numpy_datetime64_m',
                    'data_numpy_datetime64_h',
                    'data_numpy_datetime64_d_up',
                    'data_numpy_datetime64_m_up',
                )}
            return rtn
        else:
            return {k: {
                'value': f"dict([{v['value']}],[1])",
                'expect': {v['expect'] if k not in (
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                ) else -9223372036854775808: 1},
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'bool_true',
                    'bool_false',
                    'date_0',
                    'date_none',
                    'month_0',
                    'month_none',
                    'time_0',
                    'time_none',
                    'minute_0',
                    'minute_none',
                    'second_0',
                    'second_none',
                    'datetime_0',
                    'datetime_none',
                    'timestamp_0',
                    'timestamp_none',
                    'nanotime_0',
                    'nanotime_none',
                    'nanotimestamp_0',
                    'nanotimestamp_none',
                    'datehour_0',
                    'float_0',
                    'float_nan',
                    'float_inf',
                    'float_none',
                    'double_0',
                    'double_pi',
                    'double_none',
                    'uuid',
                    'ipaddr',
                    'int128',
                    'blob',
                    'decimal32',
                    'decimal32_8',
                    'decimal32_9',
                    'decimal32_nan',
                    'decimal64',
                    'decimal64_17',
                    'decimal64_18',
                    'decimal64_nan',
                    'decimal64_null',
                    'decimal128',
                    'decimal128_37',
                    'decimal128_38',
                    'decimal128_nan',
                )
            }

    # todo:download
    @classmethod
    def getDictKeyContainNone(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'dict_key'): {
                'value': {v['value']: 'a', None: 'b'},
                'expect_typestr': f"'{v['expect_typestr']}->STRING DICTIONARY'",
                'expect_value': v['expect_value'],
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_numpy_nan',
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_int8_none',
                    'data_numpy_int16_none',
                    'data_numpy_int32_none',
                    'data_numpy_int64_none',
                    'data_numpy_float32_none',
                    'data_numpy_float64_none',
                    'data_numpy_longdouble_none',
                    'data_numpy_datetime64_ns_none',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_bool_true',
                    'data_bool_false',
                    'data_numpy_bool_true',
                    'data_numpy_bool_false',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                    'data_float',
                    'data_pi',
                    'data_numpy_float32_0',
                    'data_numpy_float32_max',
                    'data_numpy_float64_0',
                    'data_numpy_float64_max',
                    'data_numpy_datetime64_ns_0',
                    'data_numpy_datetime64_ns_max',
                    'data_numpy_datetime64_ns_min',
                    'data_numpy_datetime64_us_0',
                    'data_numpy_datetime64_ms',
                    'data_numpy_datetime64_s',
                    'data_numpy_datetime64_m',
                    'data_numpy_datetime64_h',
                    'data_numpy_datetime64_d_up',
                    'data_numpy_datetime64_m_up',
                )}
            return rtn
        else:
            rtn = {k: {
                'value': f"dict([{v['value']},NULL],[1,2])",
                'expect': {v['expect']: 1},
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'bool_true',
                    'bool_false',
                    'date_0',
                    'date_none',
                    'month_0',
                    'month_none',
                    'time_0',
                    'time_none',
                    'minute_0',
                    'minute_none',
                    'second_0',
                    'second_none',
                    'datetime_0',
                    'datetime_none',
                    'timestamp_0',
                    'timestamp_none',
                    'nanotime_0',
                    'nanotime_none',
                    'nanotimestamp_0',
                    'nanotimestamp_none',
                    'datehour_0',
                    'float_0',
                    'float_nan',
                    'float_inf',
                    'float_none',
                    'double_0',
                    'double_pi',
                    'double_none',
                    'uuid',
                    'ipaddr',
                    'int128',
                    'blob',
                    'decimal32',
                    'decimal32_8',
                    'decimal32_9',
                    'decimal32_nan',
                    'decimal64',
                    'decimal64_17',
                    'decimal64_18',
                    'decimal64_nan',
                    'decimal64_null',
                    'decimal128',
                    'decimal128_37',
                    'decimal128_38',
                    'decimal128_nan',
                    'string',
                )
            }
            for key in rtn:
                if key in ('char_none', 'short_none', 'int_none', 'long_none'):
                    rtn[key]['expect'] = {-9223372036854775808: 2}
                elif key == 'string':
                    rtn[key]['expect'].update({'': 2})
                else:
                    rtn[key]['expect'].update({-9223372036854775808: 2})
            return rtn

    @classmethod
    def getDictSpecial(cls, _type):
        """
        _type:upload or download
        """
        # todo:dict_empty
        if _type.lower() == 'upload':
            rtn = {
                'dictSpecial_vector': {
                    'value': {'1': [1]},
                    'expect_typestr': "'STRING->ANY DICTIONARY'",
                    'expect_value': '[1]',
                },
                'dictSpecial_set': {
                    'value': {'1': {1}},
                    'expect_typestr': "'STRING->ANY DICTIONARY'",
                    'expect_value': 'set([1])',
                },
                'dictSpecial_dict': {
                    'value': {'1': {1: 1}},
                    'expect_typestr': "'STRING->ANY DICTIONARY'",
                    'expect_value': 'dict([1],[1])',
                },
                'dictSpecial_table': {
                    'value': {'1': pd.DataFrame({'a': [1]}, dtype='object')},
                    'expect_typestr': "'STRING->ANY DICTIONARY'",
                    'expect_value': 'table([[1] as `a])',
                },
                'dictSpecial_composite': {
                    'value': {
                        '1': 1,
                        '2': [1],
                        '3': {1},
                        '4': {1: 1},
                        '5': pd.DataFrame({'a': [1]}, dtype='object'),
                    },
                    'expect_typestr': "'STRING->ANY DICTIONARY'",
                    'expect_value': "dict(`1`2`3`4`5,[1,[1],set([1]),dict([1],[1]),table([[1] as `a])])"
                },
            }
            return rtn
        else:
            rtn = {
                'dictSpecial_pair': {
                    'value': 'dict(["1"],[1:2])',
                    'expect': {'1': [1, 2]},
                },
                'dictSpecial_vector': {
                    'value': 'dict(["1"],[[1]])',
                    'expect': {'1': np.array([1], dtype=np.int32)},
                },
                'dictSpecial_set': {
                    'value': 'dict(["1"],[set([1])])',
                    'expect': {'1': {1}},
                },
                'dictSpecial_dict': {
                    'value': 'dict(["1"],[dict([1],[1])])',
                    'expect': {'1': {1: 1}},
                },
                'dictSpecial_table': {
                    'value': 'dict(["1"],[table([[1] as `a])])',
                    'expect': {'1': pd.DataFrame({'a': [1]}, dtype=np.int32)},
                },
            }
            rtn.update({
                f'dictSpecial_empty_{k1}_{k2}': {
                    'value': f'dict({k1},{k2})',
                    'expect': {},
                } for k1 in cls.DATA_TYPE
                if k1 not in (
                    'VOID',
                    'BOOL',
                    'DATE',
                    'MONTH',
                    'TIME',
                    'MINUTE',
                    'SECOND',
                    'DATETIME',
                    'TIMESTAMP',
                    'NANOTIME',
                    'NANOTIMESTAMP',
                    'FLOAT',
                    'DOUBLE',
                    'UUID',
                    'ANY',
                    'DATEHOUR',
                    'IPADDR',
                    'INT128',
                    'BLOB',
                    'DECIMAL32(2)',
                    'DECIMAL64(2)',
                    'DECIMAL128(2)',
                ) for k2 in cls.DATA_TYPE
                if k2 not in (
                       'VOID',
                   )
            })
            return rtn

    @classmethod
    def getTable(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'table'): {
                'value': pd.DataFrame({'a': [v['value'], v['value'], v['value']]}, dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB'
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},{v['expect_value']},{v['expect_value']}]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }
            rtn.update({k.replace('data', 'table'): {
                'value': pd.DataFrame({'a': [v['value'], v['value'], v['value']]}, dtype='object'),
                'expect_typestr': f"'STRING VECTOR'",
                'expect_value': "(,,)",
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_none',
                )
            })
            rtn.update({k.replace('data', 'table_dtype'): {
                'value': pd.DataFrame({'a': [v['value'], v['value'], v['value']]}, dtype=v['dtype']),
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB'
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},{v['expect_value']},{v['expect_value']}]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            })
            if PANDAS_VERSION < (2, 0, 0):
                rtn['table_dtype_numpy_datetime64_ms']['expect_typestr'] = "'FAST NANOTIMESTAMP VECTOR'"
                rtn['table_dtype_numpy_datetime64_s']['expect_typestr'] = "'FAST NANOTIMESTAMP VECTOR'"
            return rtn
        else:
            rtn = {k: {
                'value': f"table([{v['value']},{v['value']}] as a)",
                'expect': pd.DataFrame({'a': [v['expect'], v['expect']]},
                                       dtype=v['dtype'] if not isinstance(v['dtype'], str) else re.sub(r'\[.*]',
                                                                                                       '[ns]',
                                                                                                       v['dtype'])),
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                )
            }
            rtn.update({k: {
                'value': f"table([{v['value']},{v['value']}] as a)",
                'expect': pd.DataFrame({'a': [v['expect'], v['expect']]}, dtype=np.float64),
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k in (
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                )
            })
            return rtn

    @classmethod
    def getTableContainNone(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn_first = {k.replace('data', 'table_firstNone'): {
                'value': pd.DataFrame({'a': [None, v['value'], v['value']]}, dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB'
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[NULL,{v['expect_value']},{v['expect_value']}]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }
            rtn_first.update({k.replace('data', 'table_firstNone'): {
                'value': pd.DataFrame({'a': [None, v['value'], v['value']]}, dtype='object'),
                'expect_typestr': f"'STRING VECTOR'",
                'expect_value': "(,,)",
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_none',
                )
            })
            rtn_middle = {k.replace('data', 'table_middleNone'): {
                'value': pd.DataFrame({'a': [v['value'], None, v['value']]}, dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB'
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},NULL,{v['expect_value']}]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }
            rtn_middle.update({k.replace('data', 'table_middleNone'): {
                'value': pd.DataFrame({'a': [v['value'], None, v['value']]}, dtype='object'),
                'expect_typestr': f"'STRING VECTOR'",
                'expect_value': "(,,)",
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_none',
                )
            })
            rtn_last = {k.replace('data', 'table_lastNone'): {
                'value': pd.DataFrame({'a': [v['value'], v['value'], None]}, dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']} VECTOR'" if v['expect_typestr'] not in (
                    'STRING',
                    'BLOB'
                ) else f"'{v['expect_typestr']} VECTOR'",
                'expect_value': f"[{v['expect_value']},{v['expect_value']},NULL]",
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                )
            }
            rtn_last.update({k.replace('data', 'table_lastNone'): {
                'value': pd.DataFrame({'a': [v['value'], v['value'], None]}, dtype='object'),
                'expect_typestr': f"'STRING VECTOR'",
                'expect_value': "(,,)",
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_none',
                )
            })
            return {**rtn_first, **rtn_middle, **rtn_last}
        else:
            rtn = {k: {
                'value': f"table([{v['value']},NULL] as a)",
                'expect': pd.DataFrame({'a': [v['expect'], v['contain_none']['expect']]},
                                       dtype=v['contain_none']['dtype'] if not isinstance(v['contain_none']['dtype'],
                                                                                          str) else re.sub(r'\[.*]',
                                                                                                           '[ns]', v[
                                                                                                               'contain_none'][
                                                                                                               'dtype'])),
            } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                )
            }
            return rtn

    @classmethod
    def getTableMix(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {'tableMix': {
                'value': pd.DataFrame({
                    k: [v['value'], v['value'], v['value']]
                    for k, v in cls.DATA_UPLOAD.items()
                    if k not in ('data_none',)
                }, dtype='object'),
            }
            }
            rtn.update(
                {'tableMix_dtype': {
                    'value': pd.DataFrame({
                        k: pd.Series([v['value'], v['value'], v['value']], dtype=v['dtype'])
                        for k, v in cls.DATA_UPLOAD.items()
                        if k not in ('data_none',)
                    }),
                }
                }
            )
            return rtn
        else:
            return {}

    @classmethod
    def getTableSpecial(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {
                'tableSpecial_empty_' + k.replace('[', '_').replace(']', ''): {
                    'value': pd.DataFrame({'a': []}, dtype=k),
                    'expect_typestr': v,
                    'expect_value': '()',
                } for k, v in {
                    'int8': "'FAST CHAR VECTOR'",
                    'int16': "'FAST SHORT VECTOR'",
                    'int32': "'FAST INT VECTOR'",
                    'int64': "'FAST LONG VECTOR'",
                    'float32': "'FAST FLOAT VECTOR'",
                    'float64': "'FAST DOUBLE VECTOR'",
                    # 'longdouble',
                    'bool': "'FAST BOOL VECTOR'",
                    'datetime64[ns]': "'FAST NANOTIMESTAMP VECTOR'",
                    'datetime64[us]': "'FAST NANOTIMESTAMP VECTOR'",
                    'datetime64[ms]': "'FAST TIMESTAMP VECTOR'" if PANDAS_VERSION >= (
                        2, 0, 0) else "'FAST NANOTIMESTAMP VECTOR'",
                    'datetime64[s]': "'FAST DATETIME VECTOR'" if PANDAS_VERSION >= (
                        2, 0, 0) else "'FAST NANOTIMESTAMP VECTOR'",
                    'object': "'FAST DOUBLE VECTOR'",
                }.items()
            }
            for k, v in {
                keys.DT_BOOL: "'FAST BOOL VECTOR'",
                keys.DT_CHAR: "'FAST CHAR VECTOR'",
                keys.DT_SHORT: "'FAST SHORT VECTOR'",
                keys.DT_INT: "'FAST INT VECTOR'",
                keys.DT_LONG: "'FAST LONG VECTOR'",
                keys.DT_DATE: "'FAST DATE VECTOR'",
                keys.DT_MONTH: "'FAST MONTH VECTOR'",
                keys.DT_TIME: "'FAST TIME VECTOR'",
                keys.DT_MINUTE: "'FAST MINUTE VECTOR'",
                keys.DT_SECOND: "'FAST SECOND VECTOR'",
                keys.DT_DATETIME: "'FAST DATETIME VECTOR'",
                keys.DT_TIMESTAMP: "'FAST TIMESTAMP VECTOR'",
                keys.DT_NANOTIME: "'FAST NANOTIME VECTOR'",
                keys.DT_NANOTIMESTAMP: "'FAST NANOTIMESTAMP VECTOR'",
                keys.DT_FLOAT: "'FAST FLOAT VECTOR'",
                keys.DT_SYMBOL: "'FAST SYMBOL VECTOR'",
                keys.DT_STRING: "'STRING VECTOR'",
                keys.DT_UUID: "'FAST UUID VECTOR'",
                keys.DT_DATEHOUR: "'FAST DATEHOUR VECTOR'",
                keys.DT_IPADDR: "'FAST IPADDR VECTOR'",
                keys.DT_INT128: "'FAST INT128 VECTOR'",
                keys.DT_BLOB: "'BLOB VECTOR'",
                keys.DT_DECIMAL32: "'FAST DECIMAL32 VECTOR'",
                keys.DT_DECIMAL64: "'FAST DECIMAL64 VECTOR'",
                keys.DT_DECIMAL128: "'FAST DECIMAL128 VECTOR'",
                # keys.DT_OBJECT: "'FAST OBJECT VECTOR'",
            }.items():
                df = pd.DataFrame({'a': []}, dtype='object')
                df.__DolphinDB_Type__ = {
                    'a': k
                }
                rtn.update({
                    f'tableSpecial_empty_setType_{k}': {
                        'value': df,
                        'expect_typestr': v,
                        'expect_value': '()',
                    }
                })
            return rtn
        else:
            rtn = {
                f'tableSpecial_empty_{k}': {
                    'value': f"table(1:0,[`test],[{k}])",
                    'expect': pd.DataFrame({'test': []},
                                           dtype=v['dtype'] if not isinstance(v['dtype'], str) else re.sub(r'\[.*]',
                                                                                                           '[ns]',
                                                                                                           v['dtype'])),
                } for k, v in cls.DATA_TYPE.items()
                if k not in (
                    'VOID',
                    'ANY',
                )
            }
            return rtn

    @classmethod
    def getTableExtensionDtype(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {
                'table_extension_boolean': {
                    'value': pd.DataFrame({'a': [True, False, None]}, dtype=pd.BooleanDtype()),
                    'expect_typestr': "'FAST BOOL VECTOR'",
                    'expect_value': "table([true,false,00b] as `a)",
                },
                'table_extension_int8': {
                    'value': pd.DataFrame({'a': [0, 1, None]}, dtype=pd.Int8Dtype()),
                    'expect_typestr': "'FAST CHAR VECTOR'",
                    'expect_value': "table([0c,1c,00c] as `a)",
                },
                'table_extension_int16': {
                    'value': pd.DataFrame({'a': [0, 1, None]}, dtype=pd.Int16Dtype()),
                    'expect_typestr': "'FAST SHORT VECTOR'",
                    'expect_value': "table([0h,1h,00h] as `a)",
                },
                'table_extension_int32': {
                    'value': pd.DataFrame({'a': [0, 1, None]}, dtype=pd.Int32Dtype()),
                    'expect_typestr': "'FAST INT VECTOR'",
                    'expect_value': "table([0i,1i,00i] as `a)",
                },
                'table_extension_int64': {
                    'value': pd.DataFrame({'a': [0, 1, None]}, dtype=pd.Int64Dtype()),
                    'expect_typestr': "'FAST LONG VECTOR'",
                    'expect_value': "table([0l,1l,00l] as `a)",
                },
                'table_extension_string': {
                    'value': pd.DataFrame({'a': ["0", "1", None]}, dtype=pd.StringDtype()),
                    'expect_typestr': "'STRING VECTOR'",
                    'expect_value': "table([\"0\",\"1\",\"\"] as `a)",
                },
            }
            if find_spec("pyarrow") is not None:
                if PANDAS_VERSION >= (1, 3, 0):
                    rtn["table_extension_string_pyarrow"] = {
                        'value': pd.DataFrame({'a': ["0", "1", None]}, dtype=pd.StringDtype(storage="pyarrow")),
                        'expect_typestr': "'STRING VECTOR'",
                        'expect_value': "table([\"0\",\"1\",\"\"] as `a)",
                    }
                    rtn["table_extension_string_python"] = {
                        'value': pd.DataFrame({'a': ["0", "1", None]}, dtype=pd.StringDtype(storage="python")),
                        'expect_typestr': "'STRING VECTOR'",
                        'expect_value': "table([\"0\",\"1\",\"\"] as `a)",
                    }
                if PANDAS_VERSION >= (2, 1, 0):
                    rtn['table_extension_string_pyarrow_numpy'] = {
                        'value': pd.DataFrame({'a': ["0", "1", None]}, dtype=pd.StringDtype(storage="pyarrow_numpy")),
                        'expect_typestr': "'STRING VECTOR'",
                        'expect_value': "table([\"0\",\"1\",\"\"] as `a)",
                    }
            if PANDAS_VERSION >= (1, 2, 0):
                rtn["table_extension_float32"] = {
                    'value': pd.DataFrame({'a': [0, 1, None]}, dtype=pd.Float32Dtype()),
                    'expect_typestr': "'FAST FLOAT VECTOR'",
                    'expect_value': "table([0f,1f,00f] as `a)",
                }
                rtn["table_extension_float64"] = {
                    'value': pd.DataFrame({'a': [0, 1, None]}, dtype=pd.Float64Dtype()),
                    'expect_typestr': "'FAST DOUBLE VECTOR'",
                    'expect_value': "table([0F,1F,00F] as `a)",
                }
            df_symbol = pd.DataFrame({'a': ["0", "1", None]}, dtype=pd.StringDtype())
            df_symbol.__DolphinDB_Type__ = {'a': keys.DT_SYMBOL}
            rtn['table_extension_symbol'] = {
                'value': df_symbol,
                'expect_typestr': "'FAST SYMBOL VECTOR'",
                'expect_value': "table(symbol([\"0\",\"1\",\"\"]) as `a)",
            }
            df_blob = pd.DataFrame({'a': ["0", "1", None]}, dtype=pd.StringDtype())
            df_blob.__DolphinDB_Type__ = {'a': keys.DT_BLOB}
            rtn['table_extension_blob'] = {
                'value': df_blob,
                'expect_typestr': "'BLOB VECTOR'",
                'expect_value': "table(blob([\"0\",\"1\",\"\"]) as `a)",
            }
            df_uuid = pd.DataFrame(
                {'a': ["5d212a78-cc48-e3b1-4235-b4d91473ee87", "00000000-0000-0000-0000-000000000000", None]},
                dtype=pd.StringDtype())
            df_uuid.__DolphinDB_Type__ = {'a': keys.DT_UUID}
            rtn['table_extension_uuid'] = {
                'value': df_uuid,
                'expect_typestr': "'FAST UUID VECTOR'",
                'expect_value': "table(uuid([\"5d212a78-cc48-e3b1-4235-b4d91473ee87\",\"00000000-0000-0000-0000-000000000000\",\"00000000-0000-0000-0000-000000000000\"]) as `a)",
            }
            df_int128 = pd.DataFrame(
                {'a': ["5d212a78cc48e3b14235b4d91473ee87", "00000000000000000000000000000000", None]},
                dtype=pd.StringDtype())
            df_int128.__DolphinDB_Type__ = {'a': keys.DT_INT128}
            rtn['table_extension_int128'] = {
                'value': df_int128,
                'expect_typestr': "'FAST INT128 VECTOR'",
                'expect_value': "table(int128([\"5d212a78cc48e3b14235b4d91473ee87\",\"00000000000000000000000000000000\",\"00000000000000000000000000000000\"]) as `a)",
            }
            return rtn
        else:
            return {}

    @classmethod
    def getArrayVector(cls, _type):
        if _type.lower() == 'upload':
            return {}  # not support
        else:
            rtn = {
                f'arrayVector_{k}': {
                    'value': f"x=array({v['ddbtype']}[],0,3).append!([[{v['value']},{v['value']}],[{v['value']},{v['value']}],[{v['value']},{v['value']}]]);x",
                    'expect': np.array([v['expect'], v['expect']], dtype=v['dtype']),
                } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                    'string',
                    'blob',
                )
            }
            rtn.update({
                f'arrayVector_{k}': {
                    'value': f"x=array({v['ddbtype']}[],0,3).append!([[{v['value']},{v['value']}],[{v['value']},{v['value']}],[{v['value']},{v['value']}]]);x",
                    'expect': np.array([v['expect'], v['expect']], dtype=np.float64),
                } for k, v in cls.DATA_DOWNLOAD.items()
                if k in (
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                )
            })
            return rtn

    @classmethod
    def getArrayVectorContainNone(cls, _type):
        if _type.lower() == 'upload':
            return {}  # not support
        else:
            rtn = {
                f'arrayVector_{k}': {
                    'value': f"x=array({v['ddbtype']}[],0,3).append!([[{v['value']},null],[{v['value']},null],[{v['value']},null]]);x",
                    'expect': np.array([v['expect'], v['contain_none']['expect']], dtype=v['contain_none']['dtype']),
                } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'string',
                    'blob',
                )
            }
            return rtn

    @classmethod
    def getArrayVectorSpecial(cls, _type):
        if _type.lower() == 'upload':
            return {}  # not support
        else:
            rtn = {
                f'arrayVector_{k}': {
                    'value': f"array({k}[],0,3)",
                    'expect': np.array([], dtype='object'),
                } for k, v in cls.DATA_TYPE.items()
                if k not in (
                    'VOID',
                    'STRING',
                    'BLOB',
                    'SYMBOL',
                    'ANY',
                )
            }
            return rtn

    @classmethod
    def getArrayVectorTable(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {k.replace('data', 'arrayVectorTable'): {
                'value': pd.DataFrame(
                    {'a': [[v['value'], v['value'], v['value']]], 'b': [[v['value'], v['value'], v['value']]]},
                    dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                'expect_value': f"table(array({v['expect_typestr']}[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}]]) as `a,array({v['expect_typestr']}[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}]]) as `b)"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_string',
                    'data_numpy_str',
                    'data_numpy_nan',  # __DolphinDB_Type__
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            }
            rtn.update({k.replace('data', 'arrayVectorTable'): {
                'value': pd.DataFrame(
                    {'a': [[v['value'], v['value'], v['value']]], 'b': [[v['value'], v['value'], v['value']]]},
                    dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                'expect_value': f"table(array({v['expect_typestr']}({k.split('_')[-1]})[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}]]) as `a,array({v['expect_typestr']}({k.split('_')[-1]})[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}]]) as `b)"
            } for k, v in cls.DATA_UPLOAD.items()
                if k in (
                    'data_decimal_2',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            })
            return rtn
        else:
            rtn = {
                f'arrayVectorTable_{k}': {
                    'value': f"x=array({v['ddbtype']}[],0,3).append!([[{v['value']},{v['value']}],[{v['value']},{v['value']}],[{v['value']},{v['value']}]]);table(x as `a)",
                    'expect': pd.DataFrame({'a': [np.array([v['expect'], v['expect']], dtype=v['dtype']),
                                                  np.array([v['expect'], v['expect']], dtype=v['dtype']),
                                                  np.array([v['expect'], v['expect']], dtype=v['dtype'])]},
                                           dtype='object'),
                } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'string',
                    'blob',
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                )
            }
            rtn.update({
                f'arrayVectorTable_{k}': {
                    'value': f"x=array({v['ddbtype']}[],0,3).append!([[{v['value']},{v['value']}],[{v['value']},{v['value']}],[{v['value']},{v['value']}]]);table(x as `a)",
                    'expect': pd.DataFrame({'a': [np.array([v['expect'], v['expect']], dtype=np.float64),
                                                  np.array([v['expect'], v['expect']], dtype=np.float64),
                                                  np.array([v['expect'], v['expect']], dtype=np.float64)]},
                                           dtype='object'),
                } for k, v in cls.DATA_DOWNLOAD.items()
                if k in (
                    'char_none',
                    'short_none',
                    'int_none',
                    'long_none',
                )
            })
            return rtn

    @classmethod
    def getArrayVectorTableContainNone(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            arrayVector_firstNone = {k.replace('data', 'arrayVectorTable_firstNone'): {
                'value': pd.DataFrame({'a': [[None, None, None], [v['value'], v['value'], v['value']], [v['value']]]},
                                      dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                'expect_value': f"table(array({v['expect_typestr']}[],0,3).append!([[{v['expect_typestr'].lower()}(NULL),{v['expect_typestr'].lower()}(NULL),{v['expect_typestr'].lower()}(NULL)],[{v['expect_value']},{v['expect_value']},{v['expect_value']}],[{v['expect_value']}]]) as `a)"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_string',
                    'data_numpy_str',
                    'data_numpy_nan',  # __DolphinDB_Type__
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            }
            arrayVector_firstNone.update(
                {k.replace('data', 'arrayVectorTable_firstNone'): {
                    'value': pd.DataFrame(
                        {'a': [[None, None, None], [v['value'], v['value'], v['value']], [v['value']]]},
                        dtype='object'),
                    'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                    'expect_value': f"table(array({v['expect_typestr']}({k.split('_')[-1]})[],0,3).append!([[{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]}),{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]}),{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]})],[{v['expect_value']},{v['expect_value']},{v['expect_value']}],[{v['expect_value']}]]) as `a)"
                } for k, v in cls.DATA_UPLOAD.items()
                    if k in (
                    'data_decimal_2',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
                }
            )

            arrayVector_middleNone = {k.replace('data', 'arrayVectorTable_middleNone'): {
                'value': pd.DataFrame({'a': [[v['value'], v['value'], v['value']], [None, None, None], [v['value']]]},
                                      dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                'expect_value': f"table(array({v['expect_typestr']}[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}],[{v['expect_typestr'].lower()}(NULL),{v['expect_typestr'].lower()}(NULL),{v['expect_typestr'].lower()}(NULL)],[{v['expect_value']}]]) as `a)"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_string',
                    'data_numpy_str',
                    'data_numpy_nan',  # __DolphinDB_Type__
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            }
            arrayVector_middleNone.update(
                {k.replace('data', 'arrayVectorTable_middleNone'): {
                    'value': pd.DataFrame(
                        {'a': [[v['value'], v['value'], v['value']], [None, None, None], [v['value']]]},
                        dtype='object'),
                    'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                    'expect_value': f"table(array({v['expect_typestr']}({k.split('_')[-1]})[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}],[{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]}),{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]}),{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]})],[{v['expect_value']}]]) as `a)"
                } for k, v in cls.DATA_UPLOAD.items()
                    if k in (
                    'data_decimal_2',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
                }
            )
            arrayVector_lastNone = {k.replace('data', 'arrayVectorTable_lastNone'): {
                'value': pd.DataFrame({'a': [[v['value'], v['value'], v['value']], [v['value']], [None, None, None]]},
                                      dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                'expect_value': f"table(array({v['expect_typestr']}[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}],[{v['expect_value']}],[{v['expect_typestr'].lower()}(NULL),{v['expect_typestr'].lower()}(NULL),{v['expect_typestr'].lower()}(NULL)]]) as `a)"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_string',
                    'data_numpy_str',
                    'data_numpy_nan',  # __DolphinDB_Type__
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            }
            arrayVector_lastNone.update(
                {k.replace('data', 'arrayVectorTable_lastNone'): {
                    'value': pd.DataFrame(
                        {'a': [[v['value'], v['value'], v['value']], [v['value']], [None, None, None]]},
                        dtype='object'),
                    'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                    'expect_value': f"table(array({v['expect_typestr']}({k.split('_')[-1]})[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}],[{v['expect_value']}],[{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]}),{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]}),{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]})]]) as `a)"
                } for k, v in cls.DATA_UPLOAD.items()
                    if k in (
                    'data_decimal_2',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
                }
            )
            arrayVector_innerNone_first = {k.replace('data', 'arrayVectorTable_innerNone_first'): {
                'value': pd.DataFrame({'a': [[None, v['value'], v['value']]]}, dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                'expect_value': f"table(array({v['expect_typestr']}[],0,3).append!([[{v['expect_typestr'].lower()}(NULL),{v['expect_value']},{v['expect_value']}]]) as `a)"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_string',
                    'data_numpy_str',
                    'data_numpy_nan',  # __DolphinDB_Type__
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            }
            arrayVector_innerNone_first.update(
                {k.replace('data', 'arrayVectorTable_innerNone_first'): {
                    'value': pd.DataFrame(
                        {'a': [[None, v['value'], v['value']]]},
                        dtype='object'),
                    'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                    'expect_value': f"table(array({v['expect_typestr']}({k.split('_')[-1]})[],0,3).append!([[{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]}),{v['expect_value']},{v['expect_value']}]]) as `a)"
                } for k, v in cls.DATA_UPLOAD.items()
                    if k in (
                    'data_decimal_2',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
                }
            )
            arrayVector_innerNone_middle = {k.replace('data', 'arrayVectorTable_innerNone_middle'): {
                'value': pd.DataFrame({'a': [[v['value'], None, v['value']]]}, dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                'expect_value': f"table(array({v['expect_typestr']}[],0,3).append!([[{v['expect_value']},{v['expect_typestr'].lower()}(NULL),{v['expect_value']}]]) as `a)"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_string',
                    'data_numpy_str',
                    'data_numpy_nan',  # __DolphinDB_Type__
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            }
            arrayVector_innerNone_middle.update(
                {k.replace('data', 'arrayVectorTable_innerNone_middle'): {
                    'value': pd.DataFrame(
                        {'a': [[v['value'], None, v['value']]]},
                        dtype='object'),
                    'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                    'expect_value': f"table(array({v['expect_typestr']}({k.split('_')[-1]})[],0,3).append!([[{v['expect_value']},{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]}),{v['expect_value']}]]) as `a)"
                } for k, v in cls.DATA_UPLOAD.items()
                    if k in (
                    'data_decimal_2',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
                }
            )
            arrayVector_innerNone_last = {k.replace('data', 'arrayVectorTable_innerNone_last'): {
                'value': pd.DataFrame({'a': [[v['value'], v['value'], None]]}, dtype='object'),
                'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                'expect_value': f"table(array({v['expect_typestr']}[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_typestr'].lower()}(NULL)]]) as `a)"
            } for k, v in cls.DATA_UPLOAD.items()
                if k not in (
                    'data_none',
                    'data_bytes_utf8',
                    'data_bytes_gbk',
                    'data_numpy_bytes_utf8',
                    'data_numpy_bytes_gbk',
                    'data_string',
                    'data_numpy_str',
                    'data_numpy_nan',  # __DolphinDB_Type__
                    'data_pandas_nat',
                    'data_nan',
                    'data_numpy_datetime64_ns_none',
                    'data_decimal_2',
                    'data_decimal_nan',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
            }
            arrayVector_innerNone_last.update(
                {k.replace('data', 'arrayVectorTable_innerNone_last'): {
                    'value': pd.DataFrame(
                        {'a': [[v['value'], v['value'], None]]},
                        dtype='object'),
                    'expect_typestr': f"'FAST {v['expect_typestr']}[] VECTOR'",
                    'expect_value': f"table(array({v['expect_typestr']}({k.split('_')[-1]})[],0,3).append!([[{v['expect_value']},{v['expect_value']},{v['expect_typestr'].lower()}(NULL,{k.split('_')[-1]})]]) as `a)"
                } for k, v in cls.DATA_UPLOAD.items()
                    if k in (
                    'data_decimal_2',
                    'data_decimal_17',
                    'data_decimal_18',
                    'data_decimal_38',
                )
                }
            )
            return {**arrayVector_firstNone, **arrayVector_middleNone, **arrayVector_lastNone,
                    **arrayVector_innerNone_first, **arrayVector_innerNone_middle, **arrayVector_innerNone_last}
        else:
            return {
                f'arrayVectorTable_contain_none_{k}': {
                    'value': f"x=array({v['ddbtype']}[],0,3).append!([[{v['value']},null],[{v['value']},null],[{v['value']},null]]);table(x as `a)",
                    'expect': pd.DataFrame(
                        {'a': [np.array([v['expect'], v['contain_none']['expect']], dtype=v['contain_none']['dtype']),
                               np.array([v['expect'], v['contain_none']['expect']], dtype=v['contain_none']['dtype']),
                               np.array([v['expect'], v['contain_none']['expect']], dtype=v['contain_none']['dtype'])]},
                        dtype='object'),
                } for k, v in cls.DATA_DOWNLOAD.items()
                if k not in (
                    'void',
                    'string',
                    'blob',
                )
            }

    @classmethod
    def getArrayVectorTableSpecial(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {}
            for k, v in {
                keys.DT_BOOL_ARRAY: "'FAST BOOL[] VECTOR'",
                keys.DT_CHAR_ARRAY: "'FAST CHAR[] VECTOR'",
                keys.DT_SHORT_ARRAY: "'FAST SHORT[] VECTOR'",
                keys.DT_INT_ARRAY: "'FAST INT[] VECTOR'",
                keys.DT_LONG_ARRAY: "'FAST LONG[] VECTOR'",
                keys.DT_DATE_ARRAY: "'FAST DATE[] VECTOR'",
                keys.DT_MONTH_ARRAY: "'FAST MONTH[] VECTOR'",
                keys.DT_TIME_ARRAY: "'FAST TIME[] VECTOR'",
                keys.DT_MINUTE_ARRAY: "'FAST MINUTE[] VECTOR'",
                keys.DT_SECOND_ARRAY: "'FAST SECOND[] VECTOR'",
                keys.DT_DATETIME_ARRAY: "'FAST DATETIME[] VECTOR'",
                keys.DT_TIMESTAMP_ARRAY: "'FAST TIMESTAMP[] VECTOR'",
                keys.DT_NANOTIME_ARRAY: "'FAST NANOTIME[] VECTOR'",
                keys.DT_NANOTIMESTAMP_ARRAY: "'FAST NANOTIMESTAMP[] VECTOR'",
                keys.DT_FLOAT_ARRAY: "'FAST FLOAT[] VECTOR'",
                # keys.DT_SYMBOL_ARRAY:"'FAST SYMBOL[] VECTOR'",
                # keys.DT_STRING_ARRAY:"'STRING[] VECTOR'",
                keys.DT_UUID_ARRAY: "'FAST UUID[] VECTOR'",
                keys.DT_DATEHOUR_ARRAY: "'FAST DATEHOUR[] VECTOR'",
                keys.DT_IPADDR_ARRAY: "'FAST IPADDR[] VECTOR'",
                keys.DT_INT128_ARRAY: "'FAST INT128[] VECTOR'",
                # keys.DT_BLOB_ARRAY:"'BLOB[] VECTOR'",
                keys.DT_DECIMAL32_ARRAY: "'FAST DECIMAL32[] VECTOR'",
                keys.DT_DECIMAL64_ARRAY: "'FAST DECIMAL64[] VECTOR'",
                keys.DT_DECIMAL128_ARRAY: "'FAST DECIMAL128[] VECTOR'",
            }.items():
                df = pd.DataFrame({'a': []}, dtype='object')
                df.__DolphinDB_Type__ = {
                    'a': k
                }
                rtn.update({
                    f'arrayVectorTableSpecial_empty_setType_{k}': {
                        'value': df,
                        'expect_typestr': v,
                    }
                })
            return rtn
        else:
            return {
                f'arrayVectorTable_empty_{k}': {
                    'value': f"table(array({k}[],0,3) as `a)",
                    'expect': pd.DataFrame({'a': []}, dtype='object'),
                } for k, v in cls.DATA_TYPE.items()
                if k not in (
                    'VOID',
                    'STRING',
                    'BLOB',
                    'ANY',
                    'SYMBOL',
                )
            }

    @classmethod
    def getArrayVectorTableMix(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            rtn = {'arrayVectorTableMix': {
                'value': pd.DataFrame({k: [[v['value'], v['value'], v['value']]] for k, v in cls.DATA_UPLOAD.items()
                                       if k not in (
                                           'data_none',
                                           'data_bytes_utf8',
                                           'data_bytes_gbk',
                                           'data_numpy_bytes_utf8',
                                           'data_numpy_bytes_gbk',
                                           'data_string',
                                           'data_numpy_str',
                                           'data_numpy_nan',  # __DolphinDB_Type__
                                           'data_pandas_nat',
                                           'data_nan',
                                           'data_numpy_datetime64_ns_none',
                                           'data_decimal_nan',
                                       )
                                       }, dtype='object'),
            }
            }
            return rtn
        else:
            return {}

    # @classmethod
    # def getTableSetTypeVOID(cls, _type):
    #     """
    #     _type:upload or download
    #     """
    #     if _type.lower() == 'upload':
    #         dt={k:[v['value'],v['value'],v['value']] for k,v in cls.DATA_UPLOAD.items()
    #                         # if k in (
    #                         #
    #                         # )
    #         }
    #         df=pd.DataFrame(dt)
    #         df.__DolphinDB_Type__={
    #             k:keys.DT_VOID for k in cls.DATA_UPLOAD
    #         }
    #         rtn={'tableSetTypeVOID':{
    #                 'value':df
    #             }
    #         }
    #         return rtn
    #     else:
    #         return {}

    @classmethod
    def getTableSetTypeBOOL(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                  )
                  }
            dt.update({
                'first_none': [None, True, False],
                'middle_none': [True, None, False],
                'last_none': [True, False, None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_BOOL for k in dt
            }
            rtn = {'tableSetTypeBOOL': {
                'value': df,
                'expect_typestr': "'FAST BOOL VECTOR'",
                'expect_value': "table("
                                "[00b,00b,00b] as `data_none,"
                                "[00b,00b,00b] as `data_numpy_nan,"
                                "[00b,00b,00b] as `data_pandas_nat,"
                                "[false,false,false] as `data_int_0,"
                                "[false,false,false] as `data_numpy_int8_0,"
                                "[true,true,true] as `data_numpy_int8_max,"
                                "[true,true,true] as `data_numpy_int8_min,"
                                "[true,true,true] as `data_numpy_int8_none,"
                                "[false,false,false] as `data_numpy_int16_0,"
                                "[true,true,true] as `data_numpy_int16_max,"
                                "[true,true,true] as `data_numpy_int16_min,"
                                "[true,true,true] as `data_numpy_int16_none,"
                                "[false,false,false] as `data_numpy_int32_0,"
                                "[true,true,true] as `data_numpy_int32_max,"
                                "[true,true,true] as `data_numpy_int32_min,"
                                "[true,true,true] as `data_numpy_int32_none,"
                                "[false,false,false] as `data_numpy_int64_0,"
                                "[true,true,true] as `data_numpy_int64_max,"
                                "[true,true,true] as `data_numpy_int64_min,"
                                "[true,true,true] as `data_numpy_int64_none,"
                                "[true,true,true] as `data_float,"
                                "[true,true,true] as `data_pi,"
                                "[00b,00b,00b] as `data_nan,"
                                "[false,false,false] as `data_numpy_float32_0,"
                                "[true,true,true] as `data_numpy_float32_max,"
                                "[true,true,true] as `data_numpy_float32_none,"
                                "[false,false,false] as `data_numpy_float64_0,"
                                "[true,true,true] as `data_numpy_float64_max,"
                                "[true,true,true] as `data_numpy_float64_none,"
                                "[true,true,true] as `data_bool_true,"
                                "[false,false,false] as `data_bool_false,"
                                "[true,true,true] as `data_numpy_bool_true,"
                                "[false,false,false] as `data_numpy_bool_false,"
                                "[false,false,false] as `data_numpy_datetime64_ns_0,"
                                "[true,true,true] as `data_numpy_datetime64_ns_max,"
                                "[true,true,true] as `data_numpy_datetime64_ns_min,"
                                "[true,true,true] as `data_numpy_datetime64_ns_none,"
                                "[false,false,false] as `data_numpy_datetime64_us_0,"
                                "[false,false,false] as `data_numpy_datetime64_ms,"
                                "[false,false,false] as `data_numpy_datetime64_s,"
                                "[false,false,false] as `data_numpy_datetime64_m,"
                                "[false,false,false] as `data_numpy_datetime64_h,"
                                "[false,false,false] as `data_numpy_datetime64_d_up,"
                                "[false,false,false] as `data_numpy_datetime64_m_up,"
                                "[false,false,false] as `data_decimal_2,"
                                "[true,true,true] as `data_decimal_nan,"
                                "[true,true,true] as `data_decimal_17,"
                                "[true,true,true] as `data_decimal_18,"
                                "[true,true,true] as `data_decimal_38,"
                                "[00b,true,false] as `first_none,"
                                "[true,00b,false] as `middle_none,"
                                "[true,false,00b] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeCHAR(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      'data_numpy_datetime64_ns_none',
                      'data_numpy_datetime64_us_0',
                      'data_numpy_datetime64_ms',
                      'data_numpy_datetime64_s',
                      'data_numpy_datetime64_m',
                      'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_nan',
                  )
                  }
            dt.update({
                'first_none': [None, 0, -1],
                'middle_none': [0, None, -1],
                'last_none': [0, -1, None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_CHAR for k in dt
            }
            rtn = {'tableSetTypeCHAR': {
                'value': df,
                'expect_typestr': "'FAST CHAR VECTOR'",
                'expect_value': "table("
                                "[00c,00c,00c] as `data_none,"
                                "[00c,00c,00c] as `data_numpy_nan,"
                                "[00c,00c,00c] as `data_pandas_nat,"
                                "[0c,0c,0c] as `data_int_0,"
                                "[0c,0c,0c] as `data_numpy_int8_0,"
                                "[127c,127c,127c] as `data_numpy_int8_max,"
                                "[-127c,-127c,-127c] as `data_numpy_int8_min,"
                                "[00c,00c,00c] as `data_numpy_int8_none,"
                                "[0c,0c,0c] as `data_numpy_int16_0,"
                                "[0c,0c,0c] as `data_numpy_int32_0,"
                                "[0c,0c,0c] as `data_numpy_int64_0,"
                                "[00c,00c,00c] as `data_nan,"
                                "[0c,0c,0c] as `data_numpy_float32_0,"
                # "[false,false,false] as `data_numpy_float64_0,"
                                "[1c,1c,1c] as `data_bool_true,"
                                "[0c,0c,0c] as `data_bool_false,"
                                "[1c,1c,1c] as `data_numpy_bool_true,"
                                "[0c,0c,0c] as `data_numpy_bool_false,"
                                "[0c,0c,0c] as `data_numpy_datetime64_ns_0,"
                                "[0c,0c,0c] as `data_decimal_2,"
                                "[3c,3c,3c] as `data_decimal_17,"
                                "[0c,0c,0c] as `data_decimal_18,"
                                "[0c,0c,0c] as `data_decimal_38,"
                                "[00c,0,-1] as `first_none,"
                                "[0,00c,-1] as `middle_none,"
                                "[0,-1,00c] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeSHORT(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      'data_numpy_datetime64_ns_none',
                      'data_numpy_datetime64_us_0',
                      'data_numpy_datetime64_ms',
                      'data_numpy_datetime64_s',
                      'data_numpy_datetime64_m',
                      'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_nan',
                  )
                  }
            dt.update({
                'first_none': [None, 0, -1],
                'middle_none': [0, None, -1],
                'last_none': [0, -1, None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_SHORT for k in dt
            }
            rtn = {'tableSetTypeSHORT': {
                'value': df,
                'expect_typestr': "'FAST SHORT VECTOR'",
                'expect_value': "table("
                                "[00h,00h,00h] as `data_none,"
                                "[00h,00h,00h] as `data_numpy_nan,"
                                "[00h,00h,00h] as `data_pandas_nat,"
                                "[0h,0h,0h] as `data_int_0,"
                                "[0h,0h,0h] as `data_numpy_int8_0,"
                                "[127h,127h,127h] as `data_numpy_int8_max,"
                                "[-127h,-127h,-127h] as `data_numpy_int8_min,"
                                "[-128h,-128h,-128h] as `data_numpy_int8_none,"
                                "[0h,0h,0h] as `data_numpy_int16_0,"
                                "[32767h,32767h,32767h] as `data_numpy_int16_max,"
                                "[-32767h,-32767h,-32767h] as `data_numpy_int16_min,"
                                "[00h,00h,00h] as `data_numpy_int16_none,"
                                "[0h,0h,0h] as `data_numpy_int32_0,"
                                "[0h,0h,0h] as `data_numpy_int64_0,"
                                "[00h,00h,00h] as `data_nan,"
                                "[0h,0h,0h] as `data_numpy_float32_0,"
                # "[false,false,false] as `data_numpy_float64_0,"
                                "[1h,1h,1h] as `data_bool_true,"
                                "[0h,0h,0h] as `data_bool_false,"
                                "[1h,1h,1h] as `data_numpy_bool_true,"
                                "[0h,0h,0h] as `data_numpy_bool_false,"
                                "[0h,0h,0h] as `data_numpy_datetime64_ns_0,"
                                "[0h,0h,0h] as `data_decimal_2,"
                                "[3h,3h,3h] as `data_decimal_17,"
                                "[0h,0h,0h] as `data_decimal_18,"
                                "[0h,0h,0h] as `data_decimal_38,"
                                "[00h,0,-1] as `first_none,"
                                "[0,00h,-1] as `middle_none,"
                                "[0,-1,00h] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeINT(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      'data_numpy_datetime64_ns_none',
                      'data_numpy_datetime64_us_0',
                      'data_numpy_datetime64_ms',
                      'data_numpy_datetime64_s',
                      'data_numpy_datetime64_m',
                      'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_nan',
                  )
                  }
            dt.update({
                'first_none': [None, 0, -1],
                'middle_none': [0, None, -1],
                'last_none': [0, -1, None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_INT for k in dt
            }
            rtn = {'tableSetTypeINT': {
                'value': df,
                'expect_typestr': "'FAST INT VECTOR'",
                'expect_value': "table("
                                "[00i,00i,00i] as `data_none,"
                                "[00i,00i,00i] as `data_numpy_nan,"
                                "[00i,00i,00i] as `data_pandas_nat,"
                                "[0i,0i,0i] as `data_int_0,"
                                "[0i,0i,0i] as `data_numpy_int8_0,"
                                "[127i,127i,127i] as `data_numpy_int8_max,"
                                "[-127i,-127i,-127i] as `data_numpy_int8_min,"
                                "[-128i,-128i,-128i] as `data_numpy_int8_none,"
                                "[0i,0i,0i] as `data_numpy_int16_0,"
                                "[32767i,32767i,32767i] as `data_numpy_int16_max,"
                                "[-32767i,-32767i,-32767i] as `data_numpy_int16_min,"
                                "[-32768i,-32768i,-32768i] as `data_numpy_int16_none,"
                                "[0i,0i,0i] as `data_numpy_int32_0,"
                                "[2147483647i,2147483647i,2147483647i] as `data_numpy_int32_max,"
                                "[-2147483647i,-2147483647i,-2147483647i] as `data_numpy_int32_min,"
                                "[00i,00i,00i] as `data_numpy_int32_none,"
                                "[0i,0i,0i] as `data_numpy_int64_0,"
                                "[00i,00i,00i] as `data_nan,"
                                "[0i,0i,0i] as `data_numpy_float32_0,"
                # "[false,false,false] as `data_numpy_float64_0,"
                                "[1i,1i,1i] as `data_bool_true,"
                                "[0i,0i,0i] as `data_bool_false,"
                                "[1i,1i,1i] as `data_numpy_bool_true,"
                                "[0i,0i,0i] as `data_numpy_bool_false,"
                                "[0i,0i,0i] as `data_numpy_datetime64_ns_0,"
                                "[0i,0i,0i] as `data_decimal_2,"
                                "[3i,3i,3i] as `data_decimal_17,"
                                "[0i,0i,0i] as `data_decimal_18,"
                                "[0i,0i,0i] as `data_decimal_38,"
                                "[00i,0,-1] as `first_none,"
                                "[0,00i,-1] as `middle_none,"
                                "[0,-1,00i] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeLONG(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      'data_numpy_datetime64_ns_none',
                      'data_numpy_datetime64_us_0',
                      'data_numpy_datetime64_ms',
                      'data_numpy_datetime64_s',
                      'data_numpy_datetime64_m',
                      'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_nan',
                  )
                  }
            dt.update({
                'first_none': [None, 0, -1],
                'middle_none': [0, None, -1],
                'last_none': [0, -1, None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_LONG for k in dt
            }
            rtn = {'tableSetTypeLONG': {
                'value': df,
                'expect_typestr': "'FAST LONG VECTOR'",
                'expect_value': "table("
                                "[00l,00l,00l] as `data_none,"
                                "[00l,00l,00l] as `data_numpy_nan,"
                                "[00l,00l,00l] as `data_pandas_nat,"
                                "[0l,0l,0l] as `data_int_0,"
                                "[0l,0l,0l] as `data_numpy_int8_0,"
                                "[127l,127l,127l] as `data_numpy_int8_max,"
                                "[-127l,-127l,-127l] as `data_numpy_int8_min,"
                                "[-128l,-128l,-128l] as `data_numpy_int8_none,"
                                "[0l,0l,0l] as `data_numpy_int16_0,"
                                "[32767l,32767l,32767l] as `data_numpy_int16_max,"
                                "[-32767l,-32767l,-32767l] as `data_numpy_int16_min,"
                                "[-32768l,-32768l,-32768l] as `data_numpy_int16_none,"
                                "[0l,0l,0l] as `data_numpy_int32_0,"
                                "[2147483647l,2147483647l,2147483647l] as `data_numpy_int32_max,"
                                "[-2147483647l,-2147483647l,-2147483647l] as `data_numpy_int32_min,"
                                "[-2147483648l,-2147483648l,-2147483648l] as `data_numpy_int32_none,"
                                "[0l,0l,0l] as `data_numpy_int64_0,"
                                "[9223372036854775807l,9223372036854775807l,9223372036854775807l] as `data_numpy_int64_max,"
                                "[-9223372036854775807l,-9223372036854775807l,-9223372036854775807l] as `data_numpy_int64_min,"
                                "[00l,00l,00l] as `data_numpy_int64_none,"
                                "[00l,00l,00l] as `data_nan,"
                                "[0l,0l,0l] as `data_numpy_float32_0,"
                                "[1l,1l,1l] as `data_bool_true,"
                                "[0l,0l,0l] as `data_bool_false,"
                                "[1l,1l,1l] as `data_numpy_bool_true,"
                                "[0l,0l,0l] as `data_numpy_bool_false,"
                                "[0l,0l,0l] as `data_numpy_datetime64_ns_0,"
                                "[0l,0l,0l] as `data_decimal_2,"
                                "[3l,3l,3l] as `data_decimal_17,"
                                "[0l,0l,0l] as `data_decimal_18,"
                                "[0l,0l,0l] as `data_decimal_38,"
                                "[00l,0,-1] as `first_none,"
                                "[0,00l,-1] as `middle_none,"
                                "[0,-1,00l] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDATE(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'D'), np.datetime64('1970-01-01', 'D')],
                'middle_none': [np.datetime64('1970-01-01', 'D'), None, np.datetime64('1970-01-01', 'D')],
                'last_none': [np.datetime64('1970-01-01', 'D'), np.datetime64('1970-01-01', 'D'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DATE for k in dt
            }
            rtn = {'tableSetTypeDATE': {
                'value': df,
                'expect_typestr': "'FAST DATE VECTOR'",
                'expect_value': "table("
                                "[00d,00d,00d] as `data_none,"
                                "[00d,00d,00d] as `data_numpy_nan,"
                                "[00d,00d,00d] as `data_pandas_nat,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_int_0,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_int16_0,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_int32_0,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_int64_0,"
                                "[00d,00d,00d] as `data_nan,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_datetime64_ns_0,"
                                "[2262.04.11d,2262.04.11d,2262.04.11d] as `data_numpy_datetime64_ns_max,"
                                "[1677.09.21d,1677.09.21d,1677.09.21d] as `data_numpy_datetime64_ns_min,"
                                "[00d,00d,00d] as `data_numpy_datetime64_ns_none,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_datetime64_us_0,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_datetime64_ms,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_datetime64_s,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_datetime64_m,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_datetime64_h,"
                                "[1970.01.01d,1970.01.01d,1970.01.01d] as `data_numpy_datetime64_d_up,"
                                "[00d,00d,00d] as `data_decimal_nan,"
                                "[00d,1970.01.01d,1970.01.01d] as `first_none,"
                                "[1970.01.01d,00d,1970.01.01d] as `middle_none,"
                                "[1970.01.01d,1970.01.01d,00d] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeMONTH(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_int_0',
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_0',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_0',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_0',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      'data_decimal_2',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'M'), np.datetime64('1970-01-01', 'M')],
                'middle_none': [np.datetime64('1970-01-01', 'M'), None, np.datetime64('1970-01-01', 'M')],
                'last_none': [np.datetime64('1970-01-01', 'M'), np.datetime64('1970-01-01', 'M'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_MONTH for k in dt
            }
            rtn = {'tableSetTypeMONTH': {
                'value': df,
                'expect_typestr': "'FAST MONTH VECTOR'",
                'expect_value': "table("
                                "[00M,00M,00M] as `data_none,"
                                "[00M,00M,00M] as `data_numpy_nan,"
                                "[00M,00M,00M] as `data_pandas_nat,"
                # "[0000.01M,0000.01M,0000.01M] as `data_int_0,"
                # "[0000.01M,0000.01M,0000.01M] as `data_numpy_int16_0,"
                # "[0000.01M,0000.01M,0000.01M] as `data_numpy_int32_0,"
                # "[0000.01M,0000.01M,0000.01M] as `data_numpy_int64_0,"
                                "[00M,00M,00M] as `data_nan,"
                                "[1970.01M,1970.01M,1970.01M] as `data_numpy_datetime64_ns_0,"
                                "[2262.04M,2262.04M,2262.04M] as `data_numpy_datetime64_ns_max,"
                                "[1677.09M,1677.09M,1677.09M] as `data_numpy_datetime64_ns_min,"
                                "[00M,00M,00M] as `data_numpy_datetime64_ns_none,"
                                "[1970.01M,1970.01M,1970.01M] as `data_numpy_datetime64_us_0,"
                                "[1970.01M,1970.01M,1970.01M] as `data_numpy_datetime64_ms,"
                                "[1970.01M,1970.01M,1970.01M] as `data_numpy_datetime64_s,"
                                "[1970.01M,1970.01M,1970.01M] as `data_numpy_datetime64_m,"
                                "[1970.01M,1970.01M,1970.01M] as `data_numpy_datetime64_h,"
                                "[1970.01M,1970.01M,1970.01M] as `data_numpy_datetime64_d_up,"
                                "[1970.01M,1970.01M,1970.01M] as `data_numpy_datetime64_m_up,"
                                "[00M,00M,00M] as `data_decimal_nan,"
                                "[00M,1970.01M,1970.01M] as `first_none,"
                                "[1970.01M,00M,1970.01M] as `middle_none,"
                                "[1970.01M,1970.01M,00M] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeTIME(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      # 'data_numpy_datetime64_ns_max',
                      # 'data_numpy_datetime64_ns_min',
                      # 'data_numpy_datetime64_ns_none',
                      # 'data_numpy_datetime64_us_0',
                      # 'data_numpy_datetime64_ms',
                      # 'data_numpy_datetime64_s',
                      # 'data_numpy_datetime64_m',
                      # 'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns')],
                'middle_none': [np.datetime64('1970-01-01', 'ns'), None, np.datetime64('1970-01-01', 'ns')],
                'last_none': [np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_TIME for k in dt
            }
            rtn = {'tableSetTypeTIME': {
                'value': df,
                'expect_typestr': "'FAST TIME VECTOR'",
                'expect_value': "table("
                                "[00t,00t,00t] as `data_none,"
                                "[00t,00t,00t] as `data_numpy_nan,"
                                "[00t,00t,00t] as `data_pandas_nat,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_int_0,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_numpy_int16_0,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_numpy_int32_0,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_numpy_int64_0,"
                                "[00t,00t,00t] as `data_nan,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_numpy_datetime64_ns_0,"
                                "[23:47:16.854t,23:47:16.854t,23:47:16.854t] as `data_numpy_datetime64_ns_max,"
                                "[00:12:43.145t,00:12:43.145t,00:12:43.145t] as `data_numpy_datetime64_ns_min,"
                                "[00t,00t,00t] as `data_numpy_datetime64_ns_none,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_numpy_datetime64_us_0,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_numpy_datetime64_ms,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_numpy_datetime64_s,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_numpy_datetime64_m,"
                                "[00:00:00.000t,00:00:00.000t,00:00:00.000t] as `data_numpy_datetime64_h,"
                                "[00t,00t,00t] as `data_decimal_nan,"
                                "[00t,00:00:00.000t,00:00:00.000t] as `first_none,"
                                "[00:00:00.000t,00t,00:00:00.000t] as `middle_none,"
                                "[00:00:00.000t,00:00:00.000t,00t] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeMINUTE(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      # 'data_numpy_datetime64_ns_max',
                      # 'data_numpy_datetime64_ns_min',
                      # 'data_numpy_datetime64_ns_none',
                      # 'data_numpy_datetime64_us_0',
                      # 'data_numpy_datetime64_ms',
                      # 'data_numpy_datetime64_s',
                      # 'data_numpy_datetime64_m',
                      # 'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns')],
                'middle_none': [np.datetime64('1970-01-01', 'ns'), None, np.datetime64('1970-01-01', 'ns')],
                'last_none': [np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_MINUTE for k in dt
            }
            rtn = {'tableSetTypeMINUTE': {
                'value': df,
                'expect_typestr': "'FAST MINUTE VECTOR'",
                'expect_value': "table("
                                "[00m,00m,00m] as `data_none,"
                                "[00m,00m,00m] as `data_numpy_nan,"
                                "[00m,00m,00m] as `data_pandas_nat,"
                                "[00:00m,00:00m,00:00m] as `data_int_0,"
                                "[00:00m,00:00m,00:00m] as `data_numpy_int16_0,"
                                "[00:00m,00:00m,00:00m] as `data_numpy_int32_0,"
                                "[00:00m,00:00m,00:00m] as `data_numpy_int64_0,"
                                "[00m,00m,00m] as `data_nan,"
                                "[00:00m,00:00m,00:00m] as `data_numpy_datetime64_ns_0,"
                                "[23:47m,23:47m,23:47m] as `data_numpy_datetime64_ns_max,"
                                "[00:12m,00:12m,00:12m] as `data_numpy_datetime64_ns_min,"
                                "[00m,00m,00m] as `data_numpy_datetime64_ns_none,"
                                "[00:00m,00:00m,00:00m] as `data_numpy_datetime64_us_0,"
                                "[00:00m,00:00m,00:00m] as `data_numpy_datetime64_ms,"
                                "[00:00m,00:00m,00:00m] as `data_numpy_datetime64_s,"
                                "[00:00m,00:00m,00:00m] as `data_numpy_datetime64_m,"
                                "[00:00m,00:00m,00:00m] as `data_numpy_datetime64_h,"
                                "[00m,00m,00m] as `data_decimal_nan,"
                                "[00m,00:00m,00:00m] as `first_none,"
                                "[00:00m,00m,00:00m] as `middle_none,"
                                "[00:00m,00:00m,00m] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeSECOND(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      # 'data_numpy_datetime64_ns_max',
                      # 'data_numpy_datetime64_ns_min',
                      # 'data_numpy_datetime64_ns_none',
                      # 'data_numpy_datetime64_us_0',
                      # 'data_numpy_datetime64_ms',
                      # 'data_numpy_datetime64_s',
                      # 'data_numpy_datetime64_m',
                      # 'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns')],
                'middle_none': [np.datetime64('1970-01-01', 'ns'), None, np.datetime64('1970-01-01', 'ns')],
                'last_none': [np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_SECOND for k in dt
            }
            rtn = {'tableSetTypeSECOND': {
                'value': df,
                'expect_typestr': "'FAST SECOND VECTOR'",
                'expect_value': "table("
                                "[00s,00s,00s] as `data_none,"
                                "[00s,00s,00s] as `data_numpy_nan,"
                                "[00s,00s,00s] as `data_pandas_nat,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_int_0,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_numpy_int16_0,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_numpy_int32_0,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_numpy_int64_0,"
                                "[00s,00s,00s] as `data_nan,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_numpy_datetime64_ns_0,"
                                "[23:47:16s,23:47:16s,23:47:16s] as `data_numpy_datetime64_ns_max,"
                                "[00:12:43s,00:12:43s,00:12:43s] as `data_numpy_datetime64_ns_min,"
                                "[00s,00s,00s] as `data_numpy_datetime64_ns_none,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_numpy_datetime64_us_0,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_numpy_datetime64_ms,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_numpy_datetime64_s,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_numpy_datetime64_m,"
                                "[00:00:00s,00:00:00s,00:00:00s] as `data_numpy_datetime64_h,"
                                "[00s,00s,00s] as `data_decimal_nan,"
                                "[00s,00:00:00s,00:00:00s] as `first_none,"
                                "[00:00:00s,00s,00:00:00s] as `middle_none,"
                                "[00:00:00s,00:00:00s,00s] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDATETIME(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      # 'data_numpy_datetime64_ns_none',
                      # 'data_numpy_datetime64_us_0',
                      # 'data_numpy_datetime64_ms',
                      # 'data_numpy_datetime64_s',
                      # 'data_numpy_datetime64_m',
                      # 'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns')],
                'middle_none': [np.datetime64('1970-01-01', 'ns'), None, np.datetime64('1970-01-01', 'ns')],
                'last_none': [np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DATETIME for k in dt
            }
            rtn = {'tableSetTypeDATETIME': {
                'value': df,
                'expect_typestr': "'FAST DATETIME VECTOR'",
                'expect_value': "table("
                                "[00D,00D,00D] as `data_none,"
                                "[00D,00D,00D] as `data_numpy_nan,"
                                "[00D,00D,00D] as `data_pandas_nat,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_int_0,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_numpy_int16_0,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_numpy_int32_0,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_numpy_int64_0,"
                                "[00D,00D,00D] as `data_nan,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_numpy_datetime64_ns_0,"
                                "[00D,00D,00D] as `data_numpy_datetime64_ns_none,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_numpy_datetime64_us_0,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_numpy_datetime64_ms,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_numpy_datetime64_s,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_numpy_datetime64_m,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `data_numpy_datetime64_h,"
                                "[00D,00D,00D] as `data_decimal_nan,"
                                "[00D,1970.01.01T00:00:00D,1970.01.01T00:00:00D] as `first_none,"
                                "[1970.01.01T00:00:00D,00D,1970.01.01T00:00:00D] as `middle_none,"
                                "[1970.01.01T00:00:00D,1970.01.01T00:00:00D,00D] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeTIMESTAMP(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_0',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_0',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      # 'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      # 'data_numpy_datetime64_ns_none',
                      # 'data_numpy_datetime64_us_0',
                      # 'data_numpy_datetime64_ms',
                      # 'data_numpy_datetime64_s',
                      # 'data_numpy_datetime64_m',
                      # 'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns')],
                'middle_none': [np.datetime64('1970-01-01', 'ns'), None, np.datetime64('1970-01-01', 'ns')],
                'last_none': [np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_TIMESTAMP for k in dt
            }
            rtn = {'tableSetTypeTIMESTAMP': {
                'value': df,
                'expect_typestr': "'FAST TIMESTAMP VECTOR'",
                'expect_value': "table("
                                "[00T,00T,00T] as `data_none,"
                                "[00T,00T,00T] as `data_numpy_nan,"
                                "[00T,00T,00T] as `data_pandas_nat,"
                                "[1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T] as `data_int_0,"
                                "[1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T] as `data_numpy_int64_0,"
                                "[00T,00T,00T] as `data_nan,"
                                "[1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T] as `data_numpy_datetime64_ns_0,"
                                "[2262.04.11T23:47:16.854T,2262.04.11T23:47:16.854T,2262.04.11T23:47:16.854T] as `data_numpy_datetime64_ns_max,"
                                "[00T,00T,00T] as `data_numpy_datetime64_ns_none,"
                                "[1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T] as `data_numpy_datetime64_us_0,"
                                "[1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T] as `data_numpy_datetime64_ms,"
                                "[1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T] as `data_numpy_datetime64_s,"
                                "[1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T] as `data_numpy_datetime64_m,"
                                "[1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T] as `data_numpy_datetime64_h,"
                                "[00T,00T,00T] as `data_decimal_nan,"
                                "[00T,1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T] as `first_none,"
                                "[1970.01.01T00:00:00.000T,00T,1970.01.01T00:00:00.000T] as `middle_none,"
                                "[1970.01.01T00:00:00.000T,1970.01.01T00:00:00.000T,00T] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeNANOTIME(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_0',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_0',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      # 'data_numpy_datetime64_ns_none',
                      # 'data_numpy_datetime64_us_0',
                      # 'data_numpy_datetime64_ms',
                      # 'data_numpy_datetime64_s',
                      # 'data_numpy_datetime64_m',
                      # 'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns')],
                'middle_none': [np.datetime64('1970-01-01', 'ns'), None, np.datetime64('1970-01-01', 'ns')],
                'last_none': [np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_NANOTIME for k in dt
            }
            rtn = {'tableSetTypeNANOTIME': {
                'value': df,
                'expect_typestr': "'FAST NANOTIME VECTOR'",
                'expect_value': "table("
                                "[00n,00n,00n] as `data_none,"
                                "[00n,00n,00n] as `data_numpy_nan,"
                                "[00n,00n,00n] as `data_pandas_nat,"
                                "[00:00:00.000000000n,00:00:00.000000000n,00:00:00.000000000n] as `data_int_0,"
                                "[00:00:00.000000000n,00:00:00.000000000n,00:00:00.000000000n] as `data_numpy_int64_0,"
                                "[00n,00n,00n] as `data_nan,"
                                "[00:00:00.000000000n,00:00:00.000000000n,00:00:00.000000000n] as `data_numpy_datetime64_ns_0,"
                                "[00n,00n,00n] as `data_numpy_datetime64_ns_none,"
                                "[00:00:00.000000000n,00:00:00.000000000n,00:00:00.000000000n] as `data_numpy_datetime64_us_0,"
                                "[00:00:00.000000000n,00:00:00.000000000n,00:00:00.000000000n] as `data_numpy_datetime64_ms,"
                                "[00:00:00.000000000n,00:00:00.000000000n,00:00:00.000000000n] as `data_numpy_datetime64_s,"
                                "[00:00:00.000000000n,00:00:00.000000000n,00:00:00.000000000n] as `data_numpy_datetime64_m,"
                                "[00:00:00.000000000n,00:00:00.000000000n,00:00:00.000000000n] as `data_numpy_datetime64_h,"
                                "[00n,00n,00n] as `data_decimal_nan,"
                                "[00n,00:00:00.000000000n,00:00:00.000000000n] as `first_none,"
                                "[00:00:00.000000000n,00n,00:00:00.000000000n] as `middle_none,"
                                "[00:00:00.000000000n,00:00:00.000000000n,00n] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeNANOTIMESTAMP(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_0',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_0',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      # 'data_numpy_datetime64_ns_none',
                      # 'data_numpy_datetime64_us_0',
                      # 'data_numpy_datetime64_ms',
                      # 'data_numpy_datetime64_s',
                      # 'data_numpy_datetime64_m',
                      # 'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns')],
                'middle_none': [np.datetime64('1970-01-01', 'ns'), None, np.datetime64('1970-01-01', 'ns')],
                'last_none': [np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_NANOTIMESTAMP for k in dt
            }
            rtn = {'tableSetTypeNANOTIMESTAMP': {
                'value': df,
                'expect_typestr': "'FAST NANOTIMESTAMP VECTOR'",
                'expect_value': "table("
                                "[00N,00N,00N] as `data_none,"
                                "[00N,00N,00N] as `data_numpy_nan,"
                                "[00N,00N,00N] as `data_pandas_nat,"
                                "[1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N] as `data_int_0,"
                                "[1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N] as `data_numpy_int64_0,"
                                "[00N,00N,00N] as `data_nan,"
                                "[1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N] as `data_numpy_datetime64_ns_0,"
                                "[00N,00N,00N] as `data_numpy_datetime64_ns_none,"
                                "[1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N] as `data_numpy_datetime64_us_0,"
                                "[1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N] as `data_numpy_datetime64_ms,"
                                "[1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N] as `data_numpy_datetime64_s,"
                                "[1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N] as `data_numpy_datetime64_m,"
                                "[1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N] as `data_numpy_datetime64_h,"
                                "[00N,00N,00N] as `data_decimal_nan,"
                                "[00N,1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N] as `first_none,"
                                "[1970.01.01T00:00:00.000000000N,00N,1970.01.01T00:00:00.000000000N] as `middle_none,"
                                "[1970.01.01T00:00:00.000000000N,1970.01.01T00:00:00.000000000N,00N] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeFLOAT(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      #     'data_numpy_int8_0',
                      #     'data_numpy_int8_max',
                      #     'data_numpy_int8_min',
                      #     'data_numpy_int8_none',
                      #     'data_numpy_int16_0',
                      #     'data_numpy_int16_max',
                      #     'data_numpy_int16_min',
                      #     'data_numpy_int16_none',
                      #     'data_numpy_int32_0',
                      #     'data_numpy_int32_max',
                      #     'data_numpy_int32_min',
                      #     'data_numpy_int32_none',
                      #     'data_numpy_int64_max',
                      #     'data_numpy_int64_min',
                      #     'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      #     'data_inf',
                      #     'data_numpy_inf',
                      #     'data_numpy_float32_0',
                      #     'data_numpy_float32_max',
                      #     'data_numpy_float32_min',
                      #     'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      #     'data_numpy_float64_0',
                      #     'data_numpy_longdouble_0',
                      #     'data_numpy_longdouble_max',
                      #     'data_numpy_longdouble_min',
                      #     'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      #     'data_bool_true',
                      #     'data_bool_false',
                      #     'data_numpy_bool_true',
                      #     'data_numpy_bool_false',
                      #     # 'data_numpy_datetime64_ns_max',
                      #     # 'data_numpy_datetime64_ns_min',
                      'data_numpy_datetime64_ns_none',
                      'data_numpy_datetime64_us_0',
                      'data_numpy_datetime64_ms',
                      'data_numpy_datetime64_s',
                      'data_numpy_datetime64_m',
                      'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                  )
                  }
            dt.update({
                'first_none': [None, 0, -1],
                'middle_none': [0, None, -1],
                'last_none': [0, -1, None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_FLOAT for k in dt
            }
            rtn = {'tableSetTypeFLOAT': {
                'value': df,
                'expect_typestr': "'FAST FLOAT VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDOUBLE(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      #     'data_numpy_int8_0',
                      #     'data_numpy_int8_max',
                      #     'data_numpy_int8_min',
                      #     'data_numpy_int8_none',
                      #     'data_numpy_int16_0',
                      #     'data_numpy_int16_max',
                      #     'data_numpy_int16_min',
                      #     'data_numpy_int16_none',
                      #     'data_numpy_int32_0',
                      #     'data_numpy_int32_max',
                      #     'data_numpy_int32_min',
                      #     'data_numpy_int32_none',
                      #     'data_numpy_int64_max',
                      #     'data_numpy_int64_min',
                      #     'data_numpy_int64_none',
                      #     'data_float',
                      #     'data_pi',
                      #     'data_inf',
                      #     'data_numpy_inf',
                      #     'data_numpy_float32_0',
                      #     'data_numpy_float32_max',
                      #     'data_numpy_float32_min',
                      #     'data_numpy_float32_none',
                      #     'data_numpy_float64_max',
                      #     'data_numpy_float64_min',
                      #     'data_numpy_float64_none',
                      #     'data_numpy_float64_0',
                      #     'data_numpy_longdouble_0',
                      #     'data_numpy_longdouble_max',
                      #     'data_numpy_longdouble_min',
                      #     'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      #     'data_bool_true',
                      #     'data_bool_false',
                      #     'data_numpy_bool_true',
                      #     'data_numpy_bool_false',
                      #     # 'data_numpy_datetime64_ns_max',
                      #     # 'data_numpy_datetime64_ns_min',
                      'data_numpy_datetime64_ns_none',
                      'data_numpy_datetime64_us_0',
                      'data_numpy_datetime64_ms',
                      'data_numpy_datetime64_s',
                      'data_numpy_datetime64_m',
                      'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                  )
                  }
            dt.update({
                'first_none': [None, 0, -1],
                'middle_none': [0, None, -1],
                'last_none': [0, -1, None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DOUBLE for k in dt
            }
            rtn = {'tableSetTypeDOUBLE': {
                'value': df,
                'expect_typestr': "'FAST DOUBLE VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeSYMBOL(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_int_0',
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_0',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_0',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_0',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      #     'data_string',
                      #     'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      'data_numpy_datetime64_ns_0',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      'data_numpy_datetime64_ns_none',
                      'data_numpy_datetime64_us_0',
                      'data_numpy_datetime64_ms',
                      'data_numpy_datetime64_s',
                      'data_numpy_datetime64_m',
                      'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, '0', '-1'],
                'middle_none': ['0', None, '-1'],
                'last_none': ['0', '-1', None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_SYMBOL for k in dt
            }
            rtn = {'tableSetTypeSYMBOL': {
                'value': df,
                'expect_typestr': "'FAST SYMBOL VECTOR'",
                'expect_value': "table("
                                '["","",""] as `data_none,'
                                '["","",""] as `data_numpy_nan,'
                                '["","",""] as `data_pandas_nat,'
                                '["","",""] as `data_nan,'
                                '["abc!@#中文 123","abc!@#中文 123","abc!@#中文 123"] as `data_string,'
                                '["abc!@#中文 123","abc!@#中文 123","abc!@#中文 123"] as `data_numpy_str,'
                                '["","0","-1"] as `first_none,'
                                '["0","","-1"] as `middle_none,'
                                '["0","-1",""] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeSTRING(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_int_0',
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_0',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_0',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_0',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      #     'data_string',
                      #     'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      'data_numpy_datetime64_ns_0',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      'data_numpy_datetime64_ns_none',
                      'data_numpy_datetime64_us_0',
                      'data_numpy_datetime64_ms',
                      'data_numpy_datetime64_s',
                      'data_numpy_datetime64_m',
                      'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, '0', '-1'],
                'middle_none': ['0', None, '-1'],
                'last_none': ['0', '-1', None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_STRING for k in dt
            }
            rtn = {'tableSetTypeSTRING': {
                'value': df,
                'expect_typestr': "'STRING VECTOR'",
                'expect_value': "table("
                                '["","",""] as `data_none,'
                                '["","",""] as `data_numpy_nan,'
                                '["","",""] as `data_pandas_nat,'
                                '["","",""] as `data_nan,'
                                '["abc!@#中文 123","abc!@#中文 123","abc!@#中文 123"] as `data_string,'
                                '["abc!@#中文 123","abc!@#中文 123","abc!@#中文 123"] as `data_numpy_str,'
                                '["","0","-1"] as `first_none,'
                                '["0","","-1"] as `middle_none,'
                                '["0","-1",""] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeUUID(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_none',
                      'data_numpy_nan',
                      'data_pandas_nat',
                      'data_nan'
                  )
                  }
            dt.update({
                'uuid': ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88',
                         '5d212a78-cc48-e3b1-4235-b4d91473ee89'],
                'first_none': [None, '5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88'],
                'middle_none': ['5d212a78-cc48-e3b1-4235-b4d91473ee87', None, '5d212a78-cc48-e3b1-4235-b4d91473ee88'],
                'last_none': ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '5d212a78-cc48-e3b1-4235-b4d91473ee88', None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_UUID for k in dt
            }
            rtn = {'tableSetTypeUUID': {
                'value': df,
                'expect_typestr': "'FAST UUID VECTOR'",
                'expect_value': "table("
                                '[uuid("00000000-0000-0000-0000-000000000000"),uuid("00000000-0000-0000-0000-000000000000"),uuid("00000000-0000-0000-0000-000000000000")] as `data_none,'
                                '[uuid("00000000-0000-0000-0000-000000000000"),uuid("00000000-0000-0000-0000-000000000000"),uuid("00000000-0000-0000-0000-000000000000")] as `data_numpy_nan,'
                                '[uuid("00000000-0000-0000-0000-000000000000"),uuid("00000000-0000-0000-0000-000000000000"),uuid("00000000-0000-0000-0000-000000000000")] as `data_pandas_nat,'
                                '[uuid("00000000-0000-0000-0000-000000000000"),uuid("00000000-0000-0000-0000-000000000000"),uuid("00000000-0000-0000-0000-000000000000")] as `data_nan,'
                                '[uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee88"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee89")] as `uuid,'
                                '[uuid("00000000-0000-0000-0000-000000000000"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee88")] as `first_none,'
                                '[uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),uuid("00000000-0000-0000-0000-000000000000"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee88")] as `middle_none,'
                                '[uuid("5d212a78-cc48-e3b1-4235-b4d91473ee87"),uuid("5d212a78-cc48-e3b1-4235-b4d91473ee88"),uuid("00000000-0000-0000-0000-000000000000")] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDATEHOUR(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_0',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_0',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      'data_string',
                      'data_numpy_str',
                      'data_bytes_utf8',
                      'data_bytes_gbk',
                      'data_numpy_bytes_utf8',
                      'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      # 'data_numpy_datetime64_ns_max',
                      # 'data_numpy_datetime64_ns_min',
                      # 'data_numpy_datetime64_ns_none',
                      # 'data_numpy_datetime64_us_0',
                      # 'data_numpy_datetime64_ms',
                      # 'data_numpy_datetime64_s',
                      # 'data_numpy_datetime64_m',
                      # 'data_numpy_datetime64_h',
                      # 'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns')],
                'middle_none': [np.datetime64('1970-01-01', 'ns'), None, np.datetime64('1970-01-01', 'ns')],
                'last_none': [np.datetime64('1970-01-01', 'ns'), np.datetime64('1970-01-01', 'ns'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DATEHOUR for k in dt
            }
            rtn = {'tableSetTypeDATEHOUR': {
                'value': df,
                'expect_typestr': "'FAST DATEHOUR VECTOR'",
                'expect_value': "table("
                                "[datehour(NULL),datehour(NULL),datehour(NULL)] as `data_none,"
                                "[datehour(NULL),datehour(NULL),datehour(NULL)] as `data_numpy_nan,"
                                "[datehour(NULL),datehour(NULL),datehour(NULL)] as `data_pandas_nat,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `data_int_0,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `data_numpy_int64_0,"
                                "[datehour(NULL),datehour(NULL),datehour(NULL)] as `data_nan,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `data_numpy_datetime64_ns_0,"
                                "[datehour('2262.04.11T23'),datehour('2262.04.11T23'),datehour('2262.04.11T23')] as `data_numpy_datetime64_ns_max,"
                                "[datehour('1677.09.21T00'),datehour('1677.09.21T00'),datehour('1677.09.21T00')] as `data_numpy_datetime64_ns_min,"
                                "[datehour(NULL),datehour(NULL),datehour(NULL)] as `data_numpy_datetime64_ns_none,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `data_numpy_datetime64_us_0,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `data_numpy_datetime64_ms,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `data_numpy_datetime64_s,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `data_numpy_datetime64_m,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `data_numpy_datetime64_h,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `data_numpy_datetime64_d_up,"
                                "[datehour(NULL),datehour('1970.01.01T00'),datehour('1970.01.01T00')] as `first_none,"
                                "[datehour('1970.01.01T00'),datehour(NULL),datehour('1970.01.01T00')] as `middle_none,"
                                "[datehour('1970.01.01T00'),datehour('1970.01.01T00'),datehour(NULL)] as `last_none"
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeIPADDR(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_none',
                      'data_numpy_nan',
                      'data_pandas_nat',
                      'data_nan'
                  )
                  }
            dt.update({
                'ipaddr': ['127.0.0.1', '127.0.0.2', '127.0.0.3'],
                'first_none': [None, '127.0.0.1', '127.0.0.2'],
                'middle_none': ['127.0.0.1', None, '127.0.0.2'],
                'last_none': ['127.0.0.1', '127.0.0.2', None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_IPADDR for k in dt
            }
            rtn = {'tableSetTypeIPADDR': {
                'value': df,
                'expect_typestr': "'FAST IPADDR VECTOR'",
                'expect_value': "table("
                                '[ipaddr("0.0.0.0"),ipaddr("0.0.0.0"),ipaddr("0.0.0.0")] as `data_none,'
                                '[ipaddr("0.0.0.0"),ipaddr("0.0.0.0"),ipaddr("0.0.0.0")] as `data_numpy_nan,'
                                '[ipaddr("0.0.0.0"),ipaddr("0.0.0.0"),ipaddr("0.0.0.0")] as `data_pandas_nat,'
                                '[ipaddr("0.0.0.0"),ipaddr("0.0.0.0"),ipaddr("0.0.0.0")] as `data_nan,'
                                '[ipaddr("127.0.0.1"),ipaddr("127.0.0.2"),ipaddr("127.0.0.3")] as `uuid,'
                                '[ipaddr("0.0.0.0"),ipaddr("127.0.0.1"),ipaddr("127.0.0.2")] as `first_none,'
                                '[ipaddr("127.0.0.1"),ipaddr("0.0.0.0"),ipaddr("127.0.0.2")] as `middle_none,'
                                '[ipaddr("127.0.0.1"),ipaddr("127.0.0.2"),ipaddr("0.0.0.0")] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeINT128(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_none',
                      'data_numpy_nan',
                      'data_pandas_nat',
                      'data_nan'
                  )
                  }
            dt.update({
                'int128': ['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33',
                           'e1671797c52e15f763380b45e841ec34'],
                'first_none': [None, 'e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33'],
                'middle_none': ['e1671797c52e15f763380b45e841ec32', None, 'e1671797c52e15f763380b45e841ec33'],
                'last_none': ['e1671797c52e15f763380b45e841ec32', 'e1671797c52e15f763380b45e841ec33', None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_INT128 for k in dt
            }
            rtn = {'tableSetTypeINT128': {
                'value': df,
                'expect_typestr': "'FAST INT128 VECTOR'",
                'expect_value': "table("
                                '[int128("00000000000000000000000000000000"),int128("00000000000000000000000000000000"),int128("00000000000000000000000000000000")] as `data_none,'
                                '[int128("00000000000000000000000000000000"),int128("00000000000000000000000000000000"),int128("00000000000000000000000000000000")] as `data_numpy_nan,'
                                '[int128("00000000000000000000000000000000"),int128("00000000000000000000000000000000"),int128("00000000000000000000000000000000")] as `data_pandas_nat,'
                                '[int128("00000000000000000000000000000000"),int128("00000000000000000000000000000000"),int128("00000000000000000000000000000000")] as `data_nan,'
                                '[int128("e1671797c52e15f763380b45e841ec32"),int128("e1671797c52e15f763380b45e841ec33"),int128("e1671797c52e15f763380b45e841ec34")] as `uuid,'
                                '[int128("00000000000000000000000000000000"),int128("e1671797c52e15f763380b45e841ec32"),int128("e1671797c52e15f763380b45e841ec33")] as `first_none,'
                                '[int128("e1671797c52e15f763380b45e841ec32"),int128("00000000000000000000000000000000"),int128("e1671797c52e15f763380b45e841ec33")] as `middle_none,'
                                '[int128("e1671797c52e15f763380b45e841ec32"),int128("e1671797c52e15f763380b45e841ec33"),int128("00000000000000000000000000000000")] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeBLOB(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k not in (
                      'data_int_0',
                      'data_numpy_int8_0',
                      'data_numpy_int8_max',
                      'data_numpy_int8_min',
                      'data_numpy_int8_none',
                      'data_numpy_int16_0',
                      'data_numpy_int16_max',
                      'data_numpy_int16_min',
                      'data_numpy_int16_none',
                      'data_numpy_int32_0',
                      'data_numpy_int32_max',
                      'data_numpy_int32_min',
                      'data_numpy_int32_none',
                      'data_numpy_int64_0',
                      'data_numpy_int64_max',
                      'data_numpy_int64_min',
                      'data_numpy_int64_none',
                      'data_float',
                      'data_pi',
                      'data_inf',
                      'data_numpy_inf',
                      'data_numpy_float32_0',
                      'data_numpy_float32_max',
                      'data_numpy_float32_min',
                      'data_numpy_float32_none',
                      'data_numpy_float64_max',
                      'data_numpy_float64_min',
                      'data_numpy_float64_none',
                      'data_numpy_float64_0',
                      'data_numpy_longdouble_0',
                      'data_numpy_longdouble_max',
                      'data_numpy_longdouble_min',
                      'data_numpy_longdouble_none',
                      #     'data_string',
                      #     'data_numpy_str',
                      #     'data_bytes_utf8',
                      #     'data_bytes_gbk',
                      #     'data_numpy_bytes_utf8',
                      #     'data_numpy_bytes_gbk',
                      'data_bool_true',
                      'data_bool_false',
                      'data_numpy_bool_true',
                      'data_numpy_bool_false',
                      'data_numpy_datetime64_ns_0',
                      'data_numpy_datetime64_ns_max',
                      'data_numpy_datetime64_ns_min',
                      'data_numpy_datetime64_ns_none',
                      'data_numpy_datetime64_us_0',
                      'data_numpy_datetime64_ms',
                      'data_numpy_datetime64_s',
                      'data_numpy_datetime64_m',
                      'data_numpy_datetime64_h',
                      'data_numpy_datetime64_d_up',
                      'data_numpy_datetime64_m_up',
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, '0', '-1'],
                'middle_none': ['0', None, '-1'],
                'last_none': ['0', '-1', None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_BLOB for k in dt
            }
            rtn = {'tableSetTypeBLOB': {
                'value': df,
                'expect_typestr': "'BLOB VECTOR'",
                'expect_value': "table("
                                '[blob(""),blob(""),blob("")] as `data_none,'
                                '[blob(""),blob(""),blob("")] as `data_numpy_nan,'
                                '[blob(""),blob(""),blob("")] as `data_pandas_nat,'
                                '[blob(""),blob(""),blob("")] as `data_nan,'
                                '[blob("abc!@#中文 123"),blob("abc!@#中文 123"),blob("abc!@#中文 123")] as `data_string,'
                                '[blob("abc!@#中文 123"),blob("abc!@#中文 123"),blob("abc!@#中文 123")] as `data_numpy_str,'
                                '[blob("abc!@#中文 123"),blob("abc!@#中文 123"),blob("abc!@#中文 123")] as `data_bytes_utf8,'
                                '[fromUTF8("abc!@#中文 123","gbk"),fromUTF8("abc!@#中文 123","gbk"),fromUTF8("abc!@#中文 123","gbk")] as `data_bytes_gbk,'
                                '[blob("abc!@#中文 123"),blob("abc!@#中文 123"),blob("abc!@#中文 123")] as `data_numpy_bytes_utf8,'
                                '[fromUTF8("abc!@#中文 123","gbk"),fromUTF8("abc!@#中文 123","gbk"),fromUTF8("abc!@#中文 123","gbk")] as `data_numpy_bytes_gbk,'
                                '[blob(""),blob("0"),blob("-1")] as `first_none,'
                                '[blob("0"),blob(""),blob("-1")] as `middle_none,'
                                '[blob("0"),blob("-1"),blob("")] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL32(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_none',
                      'data_numpy_nan',
                      'data_pandas_nat',
                      'data_nan',
                      'data_decimal_2',
                      'data_decimal_nan',
                  )
                  }
            dt.update({
                'first_none': [None, Decimal('0'), Decimal('-1')],
                'middle_none': [Decimal('0'), None, Decimal('-1')],
                'last_none': [Decimal('0'), Decimal('-1'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DECIMAL32 for k in dt
            }
            rtn = {'tableSetTypeDECIMAL32': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL32 VECTOR'",
                'expect_value': "table("
                                '[decimal32(NULL,0),decimal32(NULL,0),decimal32(NULL,0)] as `data_none,'
                                '[decimal32(NULL,0),decimal32(NULL,0),decimal32(NULL,0)] as `data_numpy_nan,'
                                '[decimal32(NULL,0),decimal32(NULL,0),decimal32(NULL,0)] as `data_pandas_nat,'
                                '[decimal32(NULL,0),decimal32(NULL,0),decimal32(NULL,0)] as `data_nan,'
                                '[decimal32("0",2),decimal32("0",2),decimal32("0",2)] as `data_decimal_2,'
                                '[decimal32(NULL,0),decimal32(NULL,0),decimal32(NULL,0)] as `data_decimal_nan,'
                                '[decimal32(NULL,0),decimal32("0",0),decimal32("-1",0)] as `first_none,'
                                '[decimal32("0",0),decimal32(NULL,0),decimal32("-1",0)] as `middle_none,'
                                '[decimal32("0",0),decimal32("-1",0),decimal32(NULL,0)] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL32Scale(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_none',
                      'data_numpy_nan',
                      'data_pandas_nat',
                      'data_nan',
                      'data_decimal_2',
                      'data_decimal_nan',
                  )
                  }
            dt.update({
                'first_none': [None, Decimal('0'), Decimal('-1')],
                'middle_none': [Decimal('0'), None, Decimal('-1')],
                'last_none': [Decimal('0'), Decimal('-1'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: [keys.DT_DECIMAL32, 5] for k in dt
            }
            rtn = {'tableSetTypeDECIMAL32': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL32 VECTOR'",
                'expect_value': "table("
                                '[decimal32(NULL,5),decimal32(NULL,5),decimal32(NULL,5)] as `data_none,'
                                '[decimal32(NULL,5),decimal32(NULL,5),decimal32(NULL,5)] as `data_numpy_nan,'
                                '[decimal32(NULL,5),decimal32(NULL,5),decimal32(NULL,5)] as `data_pandas_nat,'
                                '[decimal32(NULL,5),decimal32(NULL,5),decimal32(NULL,5)] as `data_nan,'
                                '[decimal32("0",5),decimal32("0",5),decimal32("0",5)] as `data_decimal_2,'
                                '[decimal32(NULL,5),decimal32(NULL,5),decimal32(NULL,5)] as `data_decimal_nan,'
                                '[decimal32(NULL,5),decimal32("0",5),decimal32("-1",5)] as `first_none,'
                                '[decimal32("0",5),decimal32(NULL,5),decimal32("-1",5)] as `middle_none,'
                                '[decimal32("0",5),decimal32("-1",5),decimal32(NULL,5)] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL64(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_none',
                      'data_numpy_nan',
                      'data_pandas_nat',
                      'data_nan',
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                  )
                  }
            dt.update({
                'first_none': [None, Decimal('0'), Decimal('-1')],
                'middle_none': [Decimal('0'), None, Decimal('-1')],
                'last_none': [Decimal('0'), Decimal('-1'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DECIMAL64 for k in dt
            }
            rtn = {'tableSetTypeDECIMAL64': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL64 VECTOR'",
                'expect_value': "table("
                                '[decimal64(NULL,0),decimal64(NULL,0),decimal64(NULL,0)] as `data_none,'
                                '[decimal64(NULL,0),decimal64(NULL,0),decimal64(NULL,0)] as `data_numpy_nan,'
                                '[decimal64(NULL,0),decimal64(NULL,0),decimal64(NULL,0)] as `data_pandas_nat,'
                                '[decimal64(NULL,0),decimal64(NULL,0),decimal64(NULL,0)] as `data_nan,'
                                '[decimal64("0",2),decimal64("0",2),decimal64("0",2)] as `data_decimal_2,'
                                '[decimal64(NULL,0),decimal64(NULL,0),decimal64(NULL,0)] as `data_decimal_nan,'
                                '[decimal64("3.14159265358979323",17),decimal64("3.14159265358979323",17),decimal64("3.14159265358979323",17)] as `data_decimal_17,'
                                '[decimal64("-0.141592653589793238",18),decimal64("-0.141592653589793238",18),decimal64("-0.141592653589793238",18)] as `data_decimal_18,'
                                '[decimal64(NULL,0),decimal64("0",0),decimal64("-1",0)] as `first_none,'
                                '[decimal64("0",0),decimal64(NULL,0),decimal64("-1",0)] as `middle_none,'
                                '[decimal64("0",0),decimal64("-1",0),decimal64(NULL,0)] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL64Scale(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_none',
                      'data_numpy_nan',
                      'data_pandas_nat',
                      'data_nan',
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                  )
                  }
            dt.update({
                'first_none': [None, Decimal('0'), Decimal('-1')],
                'middle_none': [Decimal('0'), None, Decimal('-1')],
                'last_none': [Decimal('0'), Decimal('-1'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: [keys.DT_DECIMAL64, 15] for k in dt
            }
            rtn = {'tableSetTypeDECIMAL64': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL64 VECTOR'",
                'expect_value': "table("
                                '[decimal64(NULL,15),decimal64(NULL,15),decimal64(NULL,15)] as `data_none,'
                                '[decimal64(NULL,15),decimal64(NULL,15),decimal64(NULL,15)] as `data_numpy_nan,'
                                '[decimal64(NULL,15),decimal64(NULL,15),decimal64(NULL,15)] as `data_pandas_nat,'
                                '[decimal64(NULL,15),decimal64(NULL,15),decimal64(NULL,15)] as `data_nan,'
                                '[decimal64("0",15),decimal64("0",15),decimal64("0",15)] as `data_decimal_2,'
                                '[decimal64(NULL,15),decimal64(NULL,15),decimal64(NULL,15)] as `data_decimal_nan,'
                                '[decimal64("3.14159265358979323",15),decimal64("3.14159265358979323",15),decimal64("3.14159265358979323",15)] as `data_decimal_17,'
                                '[decimal64("-0.141592653589793238",15),decimal64("-0.141592653589793238",15),decimal64("-0.141592653589793238",15)] as `data_decimal_18,'
                                '[decimal64(NULL,15),decimal64("0",15),decimal64("-1",15)] as `first_none,'
                                '[decimal64("0",15),decimal64(NULL,15),decimal64("-1",15)] as `middle_none,'
                                '[decimal64("0",15),decimal64("-1",15),decimal64(NULL,15)] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL128(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_none',
                      'data_numpy_nan',
                      'data_pandas_nat',
                      'data_nan',
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, Decimal('0'), Decimal('-1')],
                'middle_none': [Decimal('0'), None, Decimal('-1')],
                'last_none': [Decimal('0'), Decimal('-1'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DECIMAL128 for k in dt
            }
            rtn = {'tableSetTypeDECIMAL128': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL128 VECTOR'",
                'expect_value': "table("
                                '[decimal128(NULL,0),decimal128(NULL,0),decimal128(NULL,0)] as `data_none,'
                                '[decimal128(NULL,0),decimal128(NULL,0),decimal128(NULL,0)] as `data_numpy_nan,'
                                '[decimal128(NULL,0),decimal128(NULL,0),decimal128(NULL,0)] as `data_pandas_nat,'
                                '[decimal128(NULL,0),decimal128(NULL,0),decimal128(NULL,0)] as `data_nan,'
                                '[decimal128("0",2),decimal128("0",2),decimal128("0",2)] as `data_decimal_2,'
                                '[decimal128(NULL,0),decimal128(NULL,0),decimal128(NULL,0)] as `data_decimal_nan,'
                                '[decimal128("3.14159265358979323",17),decimal128("3.14159265358979323",17),decimal128("3.14159265358979323",17)] as `data_decimal_17,'
                                '[decimal128("-0.141592653589793238",18),decimal128("-0.141592653589793238",18),decimal128("-0.141592653589793238",18)] as `data_decimal_18,'
                                '[decimal128("0.14159265358979323846264338327950288419",38),decimal128("0.14159265358979323846264338327950288419",38),decimal128("0.14159265358979323846264338327950288419",38)] as `data_decimal_38,'
                                '[decimal128(NULL,0),decimal128("0",0),decimal128("-1",0)] as `first_none,'
                                '[decimal128("0",0),decimal128(NULL,0),decimal128("-1",0)] as `middle_none,'
                                '[decimal128("0",0),decimal128("-1",0),decimal128(NULL,0)] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL128Scale(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [v['value'], v['value'], v['value']] for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_none',
                      'data_numpy_nan',
                      'data_pandas_nat',
                      'data_nan',
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            dt.update({
                'first_none': [None, Decimal('0'), Decimal('-1')],
                'middle_none': [Decimal('0'), None, Decimal('-1')],
                'last_none': [Decimal('0'), Decimal('-1'), None],
            })
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: [keys.DT_DECIMAL128, 30] for k in dt
            }
            rtn = {'tableSetTypeDECIMAL128': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL128 VECTOR'",
                'expect_value': "table("
                                '[decimal128(NULL,30),decimal128(NULL,30),decimal128(NULL,30)] as `data_none,'
                                '[decimal128(NULL,30),decimal128(NULL,30),decimal128(NULL,30)] as `data_numpy_nan,'
                                '[decimal128(NULL,30),decimal128(NULL,30),decimal128(NULL,30)] as `data_pandas_nat,'
                                '[decimal128(NULL,30),decimal128(NULL,30),decimal128(NULL,30)] as `data_nan,'
                                '[decimal128("0",30),decimal128("0",30),decimal128("0",30)] as `data_decimal_2,'
                                '[decimal128(NULL,30),decimal128(NULL,30),decimal128(NULL,30)] as `data_decimal_nan,'
                                '[decimal128("3.14159265358979323",30),decimal128("3.14159265358979323",30),decimal128("3.14159265358979323",30)] as `data_decimal_17,'
                                '[decimal128("-0.141592653589793238",30),decimal128("-0.141592653589793238",30),decimal128("-0.141592653589793238",30)] as `data_decimal_18,'
                                '[decimal128("0.141592653589793238462643383279",30),decimal128("0.141592653589793238462643383279",30),decimal128("0.141592653589793238462643383279",30)] as `data_decimal_38,'
                                '[decimal128(NULL,30),decimal128("0",30),decimal128("-1",30)] as `first_none,'
                                '[decimal128("0",30),decimal128(NULL,30),decimal128("-1",30)] as `middle_none,'
                                '[decimal128("0",30),decimal128("-1",30),decimal128(NULL,30)] as `last_none'
                                ")"
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeBOOLARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None], [None, True], [None]],
                'b': [[None], [np.nan], [None]],
                'c': [[None], [pd.NaT], [None]],
                'd': [[None], [True], [None]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_BOOL_ARRAY for k in dt
            }
            rtn = {'tableSetTypeBOOLARRAY': {
                'value': df,
                'expect_typestr': "'FAST BOOL[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeCHARARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None, None], [None], [None, None]],
                'b': [[None, None], [np.nan], [None]],
                'c': [[None, None], [pd.NaT], [None]],
                'd': [[None, None], [None, 1], [None]],
                'e': [[None, None], [np.int8(1)], [None]]
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_CHAR_ARRAY for k in dt
            }
            rtn = {'tableSetTypeCHARARRAY': {
                'value': df,
                'expect_typestr': "'FAST CHAR[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeSHORTARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
                'd': [[1]],
                'e': [[np.int8(1)]]
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_SHORT_ARRAY for k in dt
            }
            rtn = {'tableSetTypeSHORTARRAY': {
                'value': df,
                'expect_typestr': "'FAST SHORT[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeINTARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
                'd': [[1]],
                'e': [[np.int8(1)]]
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_INT_ARRAY for k in dt
            }
            rtn = {'tableSetTypeINTARRAY': {
                'value': df,
                'expect_typestr': "'FAST INT[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeLONGARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
                'd': [[1]],
                'e': [[np.int8(1)]]
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_LONG_ARRAY for k in dt
            }
            rtn = {'tableSetTypeLONGARRAY': {
                'value': df,
                'expect_typestr': "'FAST LONG[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDATEARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DATE_ARRAY for k in dt
            }
            rtn = {'tableSetTypeDATEARRAY': {
                'value': df,
                'expect_typestr': "'FAST DATE[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeMONTHARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_MONTH_ARRAY for k in dt
            }
            rtn = {'tableSetTypeMONTHARRAY': {
                'value': df,
                'expect_typestr': "'FAST MONTH[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeTIMEARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_TIME_ARRAY for k in dt
            }
            rtn = {'tableSetTypeTIMEARRAY': {
                'value': df,
                'expect_typestr': "'FAST TIME[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeMINUTEARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_MINUTE_ARRAY for k in dt
            }
            rtn = {'tableSetTypeMINUTEARRAY': {
                'value': df,
                'expect_typestr': "'FAST MINUTE[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeSECONDARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_SECOND_ARRAY for k in dt
            }
            rtn = {'tableSetTypeSECONDARRAY': {
                'value': df,
                'expect_typestr': "'FAST SECOND[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDATETIMEARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DATETIME_ARRAY for k in dt
            }
            rtn = {'tableSetTypeDATETIMEARRAY': {
                'value': df,
                'expect_typestr': "'FAST DATETIME[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeTIMESTAMPARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_TIMESTAMP_ARRAY for k in dt
            }
            rtn = {'tableSetTypeTIMESTAMPARRAY': {
                'value': df,
                'expect_typestr': "'FAST TIMESTAMP[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeNANOTIMEARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_NANOTIME_ARRAY for k in dt
            }
            rtn = {'tableSetTypeNANOTIMEARRAY': {
                'value': df,
                'expect_typestr': "'FAST NANOTIME[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeNANOTIMESTAMPARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_NANOTIMESTAMP_ARRAY for k in dt
            }
            rtn = {'tableSetTypeNANOTIMESTAMPARRAY': {
                'value': df,
                'expect_typestr': "'FAST NANOTIMESTAMP[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeFLOATARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_FLOAT_ARRAY for k in dt
            }
            rtn = {'tableSetTypeFLOATARRAY': {
                'value': df,
                'expect_typestr': "'FAST FLOAT[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDOUBLEARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DOUBLE_ARRAY for k in dt
            }
            rtn = {'tableSetTypeDOUBLEARRAY': {
                'value': df,
                'expect_typestr': "'FAST DOUBLE[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeUUIDARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [['5d212a78-cc48-e3b1-4235-b4d91473ee87']],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_UUID_ARRAY for k in dt
            }
            rtn = {'tableSetTypeUUIDARRAY': {
                'value': df,
                'expect_typestr': "'FAST UUID[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDATEHOURARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [[None]],
                'b': [[np.nan]],
                'c': [[pd.NaT]],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DATEHOUR_ARRAY for k in dt
            }
            rtn = {'tableSetTypeDATEHOURARRAY': {
                'value': df,
                'expect_typestr': "'FAST DATEHOUR[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeIPADDRARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [['127.0.0.1']],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_IPADDR_ARRAY for k in dt
            }
            rtn = {'tableSetTypeIPADDRARRAY': {
                'value': df,
                'expect_typestr': "'FAST IPADDR[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeINT128ARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {
                'a': [['e1671797c52e15f763380b45e841ec32']],
            }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_INT128_ARRAY for k in dt
            }
            rtn = {'tableSetTypeINT128ARRAY': {
                'value': df,
                'expect_typestr': "'FAST INT128[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL32ARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [[None, None], [None, v['value'], None, v['value'], None], [None, None]]
                  for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_decimal_2',
                      'data_decimal_nan',
                  )
                  }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DECIMAL32_ARRAY for k in dt
            }
            rtn = {'tableSetTypeDECIMAL32ARRAY': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL32[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL32ARRAYScale(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [[v['value'], v['value'], v['value']]]
                  for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_decimal_2',
                      'data_decimal_nan',
                  )
                  }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: [keys.DT_DECIMAL32_ARRAY, 3] for k in dt
            }
            rtn = {'tableSetTypeDECIMAL32ARRAY': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL32[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL64ARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [[v['value'], v['value'], v['value']]]
                  for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                  )
                  }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DECIMAL64_ARRAY for k in dt
            }
            rtn = {'tableSetTypeDECIMAL64ARRAY': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL64[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL64ARRAYScale(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [[v['value'], v['value'], v['value']]]
                  for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                  )
                  }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: [keys.DT_DECIMAL64_ARRAY, 15] for k in dt
            }
            rtn = {'tableSetTypeDECIMAL64ARRAY': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL64[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL128ARRAY(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [[v['value'], v['value'], v['value']]]
                  for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: keys.DT_DECIMAL128_ARRAY for k in dt
            }
            rtn = {'tableSetTypeDECIMAL128ARRAY': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL128[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    @classmethod
    def getTableSetTypeDECIMAL128ARRAYScale(cls, _type):
        """
        _type:upload or download
        """
        if _type.lower() == 'upload':
            dt = {k: [[v['value'], v['value'], v['value']]]
                  for k, v in cls.DATA_UPLOAD.items()
                  if k in (
                      'data_decimal_2',
                      'data_decimal_nan',
                      'data_decimal_17',
                      'data_decimal_18',
                      'data_decimal_38',
                  )
                  }
            df = pd.DataFrame(dt, dtype='object')
            df.__DolphinDB_Type__ = {
                k: [keys.DT_DECIMAL128_ARRAY, 30] for k in dt
            }
            rtn = {'tableSetTypeDECIMAL128ARRAY': {
                'value': df,
                'expect_typestr': "'FAST DECIMAL128[] VECTOR'",
            }
            }
            return rtn
        else:
            return {}

    if PANDAS_VERSION >= (2, 0, 0):
        @classmethod
        def getTableArrow(cls, _type):
            """
            _type:upload or download
            """
            if _type.lower() == 'upload':
                rtn = {k.replace('data', 'table'): {
                    'value': pd.DataFrame({'a': [v['value'], v['value'], v['value']]},
                                          dtype=pd.ArrowDtype(v['dtype_arrow'])),
                    'expect_typestr': v['expect_typestr'],
                    'expect_value': f"table([{v['expect_value']},{v['expect_value']},{v['expect_value']}] as `a)"
                } for k, v in cls.DATA_UPLOAD_ARROW.items()
                }
                rtn['table_arrow_uuid']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_UUID,
                }
                rtn['table_arrow_int128']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_INT128,
                }
                return rtn
            else:
                return {}

        @classmethod
        def getTableArrowContainNone(cls, _type):
            """
            _type:upload or download
            """
            if _type.lower() == 'upload':
                rtn_first = {k.replace('data', 'table_first'): {
                    'value': pd.DataFrame({'a': [None, v['value'], v['value']]}, dtype=pd.ArrowDtype(v['dtype_arrow'])),
                    'expect_typestr': v['expect_typestr'],
                    'expect_value': f"table([NULL,{v['expect_value']},{v['expect_value']}] as `a)"
                } for k, v in cls.DATA_UPLOAD_ARROW.items()
                }
                rtn_first['table_first_arrow_uuid']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_UUID,
                }
                rtn_first['table_first_arrow_int128']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_INT128,
                }
                rtn_middle = {k.replace('data', 'table_middle'): {
                    'value': pd.DataFrame({'a': [v['value'], None, v['value']]}, dtype=pd.ArrowDtype(v['dtype_arrow'])),
                    'expect_typestr': v['expect_typestr'],
                    'expect_value': f"table([{v['expect_value']},NULL,{v['expect_value']}] as `a)"
                } for k, v in cls.DATA_UPLOAD_ARROW.items()
                }
                rtn_middle['table_middle_arrow_uuid']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_UUID,
                }
                rtn_middle['table_middle_arrow_int128']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_INT128,
                }
                rtn_last = {k.replace('data', 'table_last'): {
                    'value': pd.DataFrame({'a': [v['value'], v['value'], None]}, dtype=pd.ArrowDtype(v['dtype_arrow'])),
                    'expect_typestr': v['expect_typestr'],
                    'expect_value': f"table([{v['expect_value']},{v['expect_value']},NULL] as `a)"
                } for k, v in cls.DATA_UPLOAD_ARROW.items()
                }
                rtn_last['table_last_arrow_uuid']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_UUID,
                }
                rtn_last['table_last_arrow_int128']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_INT128,
                }
                return {**rtn_first, **rtn_middle, **rtn_last}
            else:
                return {}

        @classmethod
        def getTableArrowSpecial(cls, _type):
            """
            _type:upload or download
            """
            if _type.lower() == 'upload':
                rtn = {k.replace('data', 'table'): {
                    'value': pd.DataFrame({'a': []}, dtype=pd.ArrowDtype(v['dtype_arrow'])),
                    'expect_typestr': v['expect_typestr'],
                } for k, v in cls.DATA_UPLOAD_ARROW.items()
                }
                rtn['table_arrow_uuid']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_UUID,
                }
                rtn['table_arrow_int128']['value'].__DolphinDB_Type__ = {
                    'a': keys.DT_INT128,
                }
                rtn['table_arrow_composite'] = {
                    'value': pd.DataFrame({
                        k: pd.Series([v['value'], v['value'], v['value']], dtype=pd.ArrowDtype(v['dtype_arrow'])) for
                        k, v in cls.DATA_UPLOAD_ARROW.items()
                    })
                }
                return rtn
            else:
                return {}

        if find_spec("pyarrow") is not None:
            @classmethod
            def getTableArrowArrayVector(cls, _type):
                """
                _type:upload or download
                """
                if _type.lower() == 'upload':
                    rtn = {k.replace('data', 'tableArrayVector'): {
                        'value': pd.DataFrame(
                            {'a': [[v['value'], v['value'], v['value']], [v['value'], v['value'], v['value']]]},
                            dtype=pd.ArrowDtype(pa.list_(v['dtype_arrow']))),
                        'expect_typestr': v['expect_typestr'][:-8] + '[]' + v['expect_typestr'][-8:],
                        'expect_value': f"table(array({v['expect_typestr'].split(' ')[1]}[],0,2).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}],[{v['expect_value']},{v['expect_value']},{v['expect_value']}]]) as `a)" if
                        v['expect_typestr'] != "'FAST DECIMAL128 VECTOR'" else
                        f"table(array(DECIMAL128(2)[],0,2).append!([[{v['expect_value']},{v['expect_value']},{v['expect_value']}],[{v['expect_value']},{v['expect_value']},{v['expect_value']}]]) as `a)"
                    } for k, v in cls.DATA_UPLOAD_ARROW.items()
                        if k not in (
                            'data_arrow_string',
                            'data_arrow_bytes_utf8',
                            'data_arrow_bytes_gbk',
                            'data_arrow_symbol',
                        )
                    }
                    rtn['tableArrayVector_arrow_uuid']['value'].__DolphinDB_Type__ = {
                        'a': keys.DT_UUID_ARRAY,
                    }
                    rtn['tableArrayVector_arrow_int128']['value'].__DolphinDB_Type__ = {
                        'a': keys.DT_INT128_ARRAY,
                    }
                    return rtn
                else:
                    return {}

            # todo:None
            @classmethod
            def getTableArrowArrayVectorContainNone(cls, _type):
                """
                _type:upload or download
                """
                if _type.lower() == 'upload':
                    rtn = {k.replace('data', 'tableArrayVectorContainNone'): {
                        'value': pd.DataFrame({'a': [[None, None, None], [v['value'], None, v['value']], [v['value']]]},
                                              dtype=pd.ArrowDtype(pa.list_(v['dtype_arrow']))),
                        'expect_typestr': v['expect_typestr'][:-8] + '[]' + v['expect_typestr'][-8:],
                        'expect_value': f"table(array({v['expect_typestr'].split(' ')[1]}[],0,3).append!([[{v['expect_typestr'].lower().split(' ')[1]}(NULL),{v['expect_typestr'].lower().split(' ')[1]}(NULL),{v['expect_typestr'].lower().split(' ')[1]}(NULL)],[{v['expect_value']},NULL,{v['expect_value']}],[{v['expect_value']}]]) as `a)" if
                        v['expect_typestr'] != "'FAST DECIMAL128 VECTOR'" else
                        f"table(array(DECIMAL128(2)[],0,3).append!([[decimal128(NULL,2),decimal128(NULL,2),decimal128(NULL,2)],[{v['expect_value']},decimal64(NULL,2),{v['expect_value']}],[{v['expect_value']}]]) as `a)"
                    } for k, v in cls.DATA_UPLOAD_ARROW.items()
                        if k not in (
                            'data_arrow_string',
                            'data_arrow_bytes_utf8',
                            'data_arrow_bytes_gbk',
                            'data_arrow_symbol',
                        )
                    }
                    rtn['tableArrayVectorContainNone_arrow_uuid']['expect_value'] = \
                        rtn['tableArrayVectorContainNone_arrow_uuid']['expect_value'].replace('uuid(NULL)',
                                                                                              "uuid('00000000-0000-0000-0000-000000000000')")
                    rtn['tableArrayVectorContainNone_arrow_int128']['expect_value'] = \
                        rtn['tableArrayVectorContainNone_arrow_int128']['expect_value'].replace('int128(NULL)',
                                                                                                "int128('00000000000000000000000000000000')")
                    rtn['tableArrayVectorContainNone_arrow_uuid']['value'].__DolphinDB_Type__ = {
                        'a': keys.DT_UUID_ARRAY,
                    }
                    rtn['tableArrayVectorContainNone_arrow_int128']['value'].__DolphinDB_Type__ = {
                        'a': keys.DT_INT128_ARRAY,
                    }
                    return rtn
                else:
                    return {}

            @classmethod
            def getTableArrowArrayVectorContainEmpty(cls, _type):
                """
                _type:upload or download
                """
                if _type.lower() == 'upload':
                    rtn = {k.replace('data', 'tableArrayVectorContainEmpty'): {
                        'value': pd.DataFrame({'a': [[], [v['value'], None, v['value']], [v['value']]]},
                                              dtype=pd.ArrowDtype(pa.list_(v['dtype_arrow']))),
                        'expect_typestr': v['expect_typestr'][:-8] + '[]' + v['expect_typestr'][-8:],
                        # todo: why
                        # 'expect_value': f"table(array({v['expect_typestr'].split(' ')[1]}[],0,3).append!([[],[{v['expect_value']},NULL,{v['expect_value']}],[{v['expect_value']}]]) as `a)" if
                        # v['expect_typestr'] != "'FAST DECIMAL64 VECTOR'" else
                        # f"table(array(DECIMAL64(2)[],0,3).append!([[],[{v['expect_value']},decimal64(NULL,2),{v['expect_value']}],[{v['expect_value']}]]) as `a)"
                    } for k, v in cls.DATA_UPLOAD_ARROW.items()
                        if k not in (
                            'data_arrow_string',
                            'data_arrow_bytes_utf8',
                            'data_arrow_bytes_gbk',
                            'data_arrow_symbol',
                        )
                    }
                    rtn['tableArrayVectorContainEmpty_arrow_uuid']['value'].__DolphinDB_Type__ = {
                        'a': keys.DT_UUID_ARRAY,
                    }
                    rtn['tableArrayVectorContainEmpty_arrow_int128']['value'].__DolphinDB_Type__ = {
                        'a': keys.DT_INT128_ARRAY,
                    }
                    return rtn
                else:
                    return {}
