from decimal import Decimal
from random import randint

import dolphindb as ddb
import numpy as np
import pandas as pd
import pytest

from basic_testing.prepare import DataUtils
from basic_testing.utils import equalPlus
from setup.settings import HOST, PORT, USER, PASSWD


@pytest.mark.BASIC
class TestDownloadNew(object):
    conn: ddb.Session = ddb.Session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('data', DataUtils.getScalar('download').values(),
                             ids=[i for i in DataUtils.getScalar('download')])
    def test_download_scalar(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getPair('download').values(),
                             ids=[i for i in DataUtils.getPair('download')])
    def test_download_pair(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getPairContainNone('download').values(),
                             ids=[i for i in DataUtils.getPairContainNone('download')])
    def test_download_pair_contain_none(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getVector('download').values(),
                             ids=[i for i in DataUtils.getVector('download')])
    def test_download_vector(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getVectorContainNone('download').values(),
                             ids=[i for i in DataUtils.getVectorContainNone('download')])
    def test_download_vector_contain_none(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getVectorSpecial('download').values(),
                             ids=[i for i in DataUtils.getVectorSpecial('download')])
    def test_download_vector_special(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getMatrix('download').values(),
                             ids=[i for i in DataUtils.getMatrix('download')])
    def test_download_matrix(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getMatrixContainNone('download').values(),
                             ids=[i for i in DataUtils.getMatrixContainNone('download')])
    def test_download_matrix_contain_none(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getSet('download').values(),
                             ids=[i for i in DataUtils.getSet('download')])
    def test_download_set(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getSetSpecial('download').values(),
                             ids=[i for i in DataUtils.getSetSpecial('download')])
    def test_download_set_special(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getSetContainNone('download').values(),
                             ids=[i for i in DataUtils.getSetContainNone('download')])
    def test_download_set_contain_none(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getDict('download').values(),
                             ids=[i for i in DataUtils.getDict('download')])
    def test_download_dict(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getDictContainNone('download').values(),
                             ids=[i for i in DataUtils.getDictContainNone('download')])
    def test_download_dict_contain_none(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getDictKey('download').values(),
                             ids=[i for i in DataUtils.getDictKey('download')])
    def test_download_dict_key(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getDictKeyContainNone('download').values(),
                             ids=[i for i in DataUtils.getDictKeyContainNone('download')])
    def test_download_dict_key_contain_none(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getDictSpecial('download').values(),
                             ids=[i for i in DataUtils.getDictSpecial('download')])
    def test_download_dict_special(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getTable('download').values(),
                             ids=[i for i in DataUtils.getTable('download')])
    def test_download_table(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getTableContainNone('download').values(),
                             ids=[i for i in DataUtils.getTableContainNone('download')])
    def test_download_table_contain_none(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getTableSpecial('download').values(),
                             ids=[i for i in DataUtils.getTableSpecial('download')])
    def test_download_table_special(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getArrayVector('download').values(),
                             ids=[i for i in DataUtils.getArrayVector('download')])
    def test_download_array_vector(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x[0], data['expect'])
        assert equalPlus(x[1], data['expect'])
        assert equalPlus(x[2], data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getArrayVectorContainNone('download').values(),
                             ids=[i for i in DataUtils.getArrayVectorContainNone('download')])
    def test_download_array_vector_contain_none(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x[0], data['expect'])
        assert equalPlus(x[1], data['expect'])
        assert equalPlus(x[2], data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getArrayVectorSpecial('download').values(),
                             ids=[i for i in DataUtils.getArrayVectorSpecial('download')])
    def test_download_array_vector_special(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getArrayVectorTable('download').values(),
                             ids=[i for i in DataUtils.getArrayVectorTable('download')])
    def test_download_array_vector_table(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getArrayVectorTableContainNone('download').values(),
                             ids=[i for i in DataUtils.getArrayVectorTableContainNone('download')])
    def test_download_array_vector_table_contain_none(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    @pytest.mark.parametrize('data', DataUtils.getArrayVectorTableSpecial('download').values(),
                             ids=[i for i in DataUtils.getArrayVectorTableSpecial('download')])
    def test_download_array_vector_table_special(self, data):
        x = self.__class__.conn.run(data['value'])
        assert equalPlus(x, data['expect'])

    def test_download_decimal_enablePickle(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        with pytest.raises(RuntimeError, match="Cannot recognize the token decimal"):
            conn.run("table([decimal('3.14',2)] as `a)")

    def test_download_dfs_table(self):
        conn = ddb.Session(HOST, PORT, USER, PASSWD, enablePickle=True)
        x = conn.run("""
            db = database("", VALUE, 2019.01.01..2019.03.02)
            t = table(take("aaa", 10) as  uuid, 2019.02.03 + 1..10 as date, take(['AAPL', 'AMZN', 'AMZN'], 10) as ticker, rand(10.0, 10) as price)
            pt1 = db.createPartitionedTable(t, "pt1", "date").append!(t);
            pt1
        """)
        expect = pd.DataFrame({
            "uuid": pd.Series([], dtype="object"),
            "date": pd.Series([], dtype="datetime64[ns]"),
            "ticker": pd.Series([], dtype="object"),
            "price": pd.Series([], dtype=np.float64),
        })
        assert equalPlus(x, expect)

    @pytest.mark.parametrize('script,expect,dtype', [
        ["true false 00b", [True, False, None], "object"],
        ["0c 127c -127c 00c", [0., 127., -127., np.nan], np.float64],
        ["0h 32767h -32767h 00h", [0, 32767, -32767, np.nan], np.float64],
        ["0i 2147483647i -2147483647i 00i", [0, 2147483647, -2147483647, np.nan], np.float64],
        ["0l 9223372036854775807l -9223372036854775807l 00l", [0, 9223372036854775807, -9223372036854775807, np.nan],
         np.float64],
        ["1970.01.01d 1970.01.02d 00d",
         [np.datetime64('1970-01-01', 'D'), np.datetime64('1970-01-02', 'D'), np.datetime64('nat', 'D')],
         'datetime64[ns]'],
        ["1970.01M 1970.02M 00M",
         [np.datetime64('1970-01', 'M'), np.datetime64('1970-02', 'M'), np.datetime64('nat', 'M')], 'datetime64[ns]'],
        ["00:00:00.000t 00:00:00.001t 00t",
         [np.datetime64('1970-01-01T00:00:00.000', 'ms'), np.datetime64('1970-01-01T00:00:00.001', 'ms'),
          np.datetime64('nat', 'ms')], 'datetime64[ns]'],
        ["00:00m 00:01m 00m",
         [np.datetime64('1970-01-01T00:00', 'm'), np.datetime64('1970-01-01T00:01', 'm'), np.datetime64('nat', 'm')],
         'datetime64[ns]'],
        ["00:00:00s 00:00:01s 00s",
         [np.datetime64('1970-01-01T00:00:00', 's'), np.datetime64('1970-01-01T00:00:01', 's'),
          np.datetime64('nat', 's')], 'datetime64[ns]'],
        ["1970.01.01T00:00:00D 1970.01.01T00:00:01D 00D",
         [np.datetime64('1970-01-01T00:00:00', 's'), np.datetime64('1970-01-01T00:00:01', 's'),
          np.datetime64('nat', 's')], 'datetime64[ns]'],
        ["1970.01.01 00:00:00.000T 1970.01.01 00:00:00.001T 00T",
         [np.datetime64('1970-01-01T00:00:00.000', 'ms'), np.datetime64('1970-01-01T00:00:00.001', 'ms'),
          np.datetime64('nat', 'ms')], 'datetime64[ns]'],
        ["00:00:00.000000000n 00:00:00.000000001n 00n",
         [np.datetime64('1970-01-01T00:00:00.000000000', 'ns'), np.datetime64('1970-01-01T00:00:00.000000001', 'ns'),
          np.datetime64('nat', 'ns')], "datetime64[ns]"],
        ["1970.01.01 00:00:00.000000000N 1970.01.01 00:00:00.000000001N 00N",
         [np.datetime64('1970-01-01T00:00:00.000000000', 'ns'), np.datetime64('1970-01-01T00:00:00.000000001', 'ns'),
          np.datetime64('nat', 'ns')], "datetime64[ns]"],
        ["0.0f 3.14f 00f", [0., 3.14, np.nan], np.float32],
        ["0.0F 3.14F 00F", [0., 3.14, np.nan], np.float64],
        ["'abc!@#中文 123' \"\"", ['abc!@#中文 123', ''], "object"],
        ["[uuid('5d212a78-cc48-e3b1-4235-b4d91473ee87'),uuid('00000000-0000-0000-0000-000000000000')]",
         ['5d212a78-cc48-e3b1-4235-b4d91473ee87', '00000000-0000-0000-0000-000000000000'], "object"],
        ["[datehour('1970.01.01T00'),datehour(NULL)]", [np.datetime64('1970-01-01T00', 'h'), np.datetime64('nat', 'h')],
         "datetime64[ns]"],
        ["[ipaddr('127.0.0.1'),ipaddr('0.0.0.0')]", ['127.0.0.1', '0.0.0.0'], "object"],
        ["[int128('e1671797c52e15f763380b45e841ec32'),int128('00000000000000000000000000000000')]",
         ['e1671797c52e15f763380b45e841ec32', '00000000000000000000000000000000'], "object"],
        ["[blob('abc!@#中文 123'),blob('')]", ['abc!@#中文 123'.encode(), b''], "object"],
        ["[decimal32(\"0\",3),decimal32('3.141',3)]", [Decimal('0.000'), Decimal('3.141')], "object"],
        ["[decimal64(\"0\",3),decimal64('3.141',3)]", [Decimal('0.000'), Decimal('3.141')], "object"],
        ["[decimal128(\"0\",3),decimal128('3.141',3)]", [Decimal('0.000'), Decimal('3.141')], "object"],
        ["array(BOOL[]).append!([[true,false,00b],[true],[false],[00b]])",
         [np.array([True, False, None]), np.array([True]), np.array([False]), np.array([None])], "object"],
    ], ids=["BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH", "TIME", "MINUTE", "SECOND", "DATETIME",
            "TIMESTAMP", "NANOTIME", "NANOTIMESTAMP", "FLOAT", "DOUBLE", "STRING", "UUID", "DATEHOUR", "IPADDR",
            "INT128", "BLOB", "DECIMAL32", "DECIMAL64", "DECIMAL128", "BOOL_ARRAY"])
    def test_download_table_ge_8192(self, script, expect, dtype):
        n = 20000
        index = [randint(0, len(expect) - 1) for i in range(n)]
        conn = ddb.Session(HOST, PORT, USER, PASSWD)
        conn.upload({"index": index})
        result = conn.run(f"""
            x = {script}
            col = array(x,capacity={n})
            for (i in index){{
                if (type(x)<64){{
                    col.append!(x[i])
                }} else {{
                    col.append!(x[i,])
                }}
            }}
            table(col as c_type)
        """)
        expect_pd = pd.DataFrame({'c_type': pd.Series([expect[i] for i in index], dtype=dtype)})
        assert equalPlus(result, expect_pd)
