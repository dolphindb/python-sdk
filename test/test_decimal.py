import pytest
import dolphindb as ddb
import decimal
import numpy as np
import pandas as pd
from numpy.testing import *
from pandas.testing import *
from setup.settings import *
import time
import dolphindb.settings as keys
from setup.utils import get_pid


def get_ex_row0(quantileVal):
    return [decimal.Decimal("1").quantize(quantileVal),decimal.Decimal("0").quantize(quantileVal), None,decimal.Decimal("2.123").quantize(quantileVal), decimal.Decimal("1").quantize(quantileVal)]


def get_ex_row1(quantileVal):
    return [decimal.Decimal("-1.111111").quantize(quantileVal), decimal.Decimal("0").quantize(quantileVal),decimal.Decimal("3.2222222222").quantize(quantileVal), decimal.Decimal("-1.111111").quantize(quantileVal),decimal.Decimal("0").quantize(quantileVal)]


class TestDecimal:
    conn = ddb.session(enablePickle=False)

    def setup_method(self):
        try:
            self.conn.run("1")
        except RuntimeError:
            self.conn.connect(HOST, PORT, USER, PASSWD)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() +'\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def test_decimal_scalar_overflow_error(self):
        a = decimal.Decimal("0.3451111111")
        self.conn.upload({"a": a})
        assert self.conn.run("typestr(a) == 'DECIMAL64'")
        a = decimal.Decimal("0.345111111111111116")
        self.conn.upload({"a": a})
        assert self.conn.run("typestr(a) == 'DECIMAL128'")
        a = decimal.Decimal("0.345111111111111111111111111111161111111111111111111111111111")
        with pytest.raises(Exception, match="overflow"):
            self.conn.upload({"a": a})

    def test_decimal_scalar_out_of_range_error(self):
        a = decimal.Decimal("3451111111")
        self.conn.upload({"a": a})
        assert self.conn.run("typestr(a) == 'DECIMAL64'")
        a = decimal.Decimal("345111111111111116")
        self.conn.upload({"a": a})
        assert self.conn.run("typestr(a) == 'DECIMAL64'")
        a = decimal.Decimal("3451111111111111111111111111111611111111111111111111111111111111")
        with pytest.raises(Exception, match="overflow"):
            self.conn.upload({"a": a})

    def test_decimal_scalar_upload_NULL(self):
        a = decimal.Decimal("NaN")
        self.conn.upload({"a": a})
        b = self.conn.run("a")
        c = self.conn.run("typestr(a)")
        assert b is None
        assert c == "DECIMAL64"

    def test_decimal32_scalar_download_gt_zero(self):
        self.conn.run("a = decimal32(10, 1)")
        a = self.conn.run("a")
        b = decimal.Decimal("10").quantize(decimal.Decimal("0.0"))
        assert isinstance(a, decimal.Decimal)
        assert str(a) == str(b)

    def test_decimal32_scalar_download_eq_zero(self):
        self.conn.run("a = decimal32(0, 2)")
        a = self.conn.run("a")
        b = decimal.Decimal("0").quantize(decimal.Decimal("0.00"))
        assert isinstance(a, decimal.Decimal)
        assert str(a) == str(b)

    def test_decimal32_scalar_download_lt_zero(self):
        self.conn.run("a = decimal32(-10, 3)")
        a = self.conn.run("a")
        b = decimal.Decimal("-10").quantize(decimal.Decimal("0.000"))
        assert isinstance(a, decimal.Decimal)
        assert str(a) == str(b)

    def test_decimal64_scalar_download_gt_zero(self):
        self.conn.run("a = decimal64(10, 11)")
        a = self.conn.run("a")
        b = decimal.Decimal("10").quantize(decimal.Decimal("0.00000000000"))
        assert isinstance(a, decimal.Decimal)
        assert str(a) == str(b)

    def test_decimal64_scalar_download_eq_zero(self):
        self.conn.run("a = decimal64(0, 12)")
        a = self.conn.run("a")
        b = decimal.Decimal("0").quantize(decimal.Decimal("0.000000000000"))
        assert isinstance(a, decimal.Decimal)
        assert str(a) == str(b)

    def test_decimal64_scalar_download_lt_zero(self):
        self.conn.run("a = decimal64(-10, 13)")
        a = self.conn.run("a")
        b = decimal.Decimal("-10").quantize(decimal.Decimal("0.0000000000000"))
        assert isinstance(a, decimal.Decimal)
        assert str(a) == str(b)

    def test_decimal64_scalar_upload_lt_zero(self):
        a = decimal.Decimal("-1.010000")
        self.conn.upload({"a": a})
        b = self.conn.run("eqObj(a, decimal64(-1.01, 6))")
        c = self.conn.run("typestr(a)")
        assert b
        assert c == "DECIMAL64"

    def test_decimal64_scalar_upload_eq_zero(self):
        a = decimal.Decimal("0.000000")
        self.conn.upload({"a": a})
        b = self.conn.run("eqObj(a, decimal64(0, 6))")
        c = self.conn.run("typestr(a)")
        assert b
        assert c == "DECIMAL64"

    def test_decimal64_scalar_upload_gt_zero(self):
        a = decimal.Decimal("102.000010")
        self.conn.upload({"a": a})
        b = self.conn.run("eqObj(a, decimal64(102.00001, 6))")
        c = self.conn.run("typestr(a)")
        assert b
        assert c == "DECIMAL64"

    def test_decimal128_scalar_download_gt_zero(self):
        self.conn.run("a = decimal128(10, 11)")
        a = self.conn.run("a")
        b = decimal.Decimal("10").quantize(decimal.Decimal("0.00000000000"))
        assert isinstance(a, decimal.Decimal)
        assert str(a) == str(b)

    def test_decimal128_scalar_download_eq_zero(self):
        self.conn.run("a = decimal128(0, 24)")
        a = self.conn.run("a")
        b = decimal.Decimal("0").quantize(decimal.Decimal("0.000000000000000000000000"))
        assert isinstance(a, decimal.Decimal)
        assert str(a) == str(b)

    def test_decimal128_scalar_download_lt_zero(self):
        self.conn.run("a = decimal128(-1, 36)")
        a = self.conn.run("a")
        b = decimal.Decimal("-1.000000000000000000000000000000000000")
        assert isinstance(a, decimal.Decimal)
        assert str(a) == str(b)

    def test_decimal32_list_download(self):
        self.conn.run("a = decimal32(-10.1 -1 NULL 0.1 4.1 5.1, 3)")
        a = self.conn.run("a")
        b = np.array([decimal.Decimal("-10.100"),decimal.Decimal("-1.000"),None,decimal.Decimal("0.100"),decimal.Decimal("4.100"),decimal.Decimal("5.100")])
        assert_array_equal(a, b)

    def test_decimal64_list_download(self):
        self.conn.run("a = decimal64(-10.1 -1 NULL 0.1 4.1 5.1, 3)")
        a = self.conn.run("a")
        b = np.array([decimal.Decimal("-10.100"),decimal.Decimal("-1.000"),None,decimal.Decimal("0.100"),decimal.Decimal("4.100"),decimal.Decimal("5.100")])
        assert_array_equal(a, b)

    def test_decimal128_list_download(self):
        self.conn.run('a = decimal128("-10.1" "-1" NULL "0.1" "4.1" "5.1", 36)')
        a = self.conn.run("a")
        b = np.array([decimal.Decimal("-10.100000000000000000000000000000000000"),decimal.Decimal("-1.000000000000000000000000000000000000"),None,decimal.Decimal("0.100000000000000000000000000000000000"),decimal.Decimal("4.100000000000000000000000000000000000"),decimal.Decimal("5.100000000000000000000000000000000000")])
        assert_array_equal(a, b)

    def test_decimal_list_upload(self):
        a = list(map(decimal.Decimal, "-1.01 NaN 3.1 4.2".split(" ")))
        self.conn.upload({"a": a})
        b = self.conn.run("eqObj(a, decimal64(-1.01 NULL 3.1 4.2, 2))")
        assert b

    def test_decimal_with_other_type_list_upload(self):
        a = [None, decimal.Decimal("-10.100"), 1.1, np.nan, 123, "aa", pd.NaT]
        self.conn.upload({"a": a})
        b = self.conn.run("eqObj(a, (NULL, decimal64(-10.1, 3), 1.1, NULL, 123, `aa, NULL))")
        assert b

    def test_decimal_vector_upload(self):
        a = np.array([decimal.Decimal("NaN"), decimal.Decimal("0.1234567899999999999"),decimal.Decimal("0.0000000000000000000")])
        self.conn.upload({"a": a})
        res = self.conn.run("eqFloat(a, decimal128(NULL 0.1234567899999999999 0, 19), 10)")
        assert res.any()

    def test_decimal_np_array_upload(self):
        a = np.array([None, decimal.Decimal("-10.100"),decimal.Decimal("-1.000"),None,decimal.Decimal("0.100"),decimal.Decimal("4.100"),decimal.Decimal("5.100"),None])
        self.conn.upload({"a": a})
        b = self.conn.run("eqObj(a, decimal64(NULL -10.1 -1 NULL 0.1 4.1 5.1 NULL, 3))")
        assert b
        a = np.array([None, decimal.Decimal("-10.1234567899999999999"),decimal.Decimal("-1.1234567899999999999"),None,decimal.Decimal("0.1234567899999999999"),decimal.Decimal("4.1234567899999999999"),decimal.Decimal("5.1234567899999999999"),None])
        self.conn.upload({"a": a})
        b = self.conn.run("eqFloat(a, decimal128(NULL -10.1234567899999999999 -1.1234567899999999999 NULL 0.1234567899999999999 4.1234567899999999999 5.1234567899999999999 NULL, 19), 10)")
        assert b.all()

    def test_decimal_with_other_type_np_array_upload(self):
        a = np.array([None, decimal.Decimal("-10.100"),"456123",np.nan,1.1,123,pd.NaT])
        self.conn.upload({"a": a})
        b = self.conn.run("eqObj(a, (NULL, decimal64(-10.100, 3), `456123, NULL, 1.1, 123, NULL))")
        assert b
        a = np.array([None, decimal.Decimal("-10.1234567899999999999"),"456123",np.nan,1.1,123,pd.NaT])
        self.conn.upload({"a": a})
        b = self.conn.run("eqObj(a, (NULL, decimal128(-10.1234567899999999999, 19), `456123, NULL, 1.1, 123, NULL), 10)")
        assert b

    def test_decimal32_dict_download(self):
        self.conn.run("""
            x = 1 2 3
            y = decimal32(-10.1 NULL 5.1, 3)
            a = dict(x, y)
        """)
        a = self.conn.run("a")
        b_1 = decimal.Decimal("-10.1").quantize(decimal.Decimal("0.000"))
        assert isinstance(a[1], decimal.Decimal)
        assert str(a[1]) == str(b_1)
        assert a[2] is None
        b_3 = decimal.Decimal("5.1").quantize(decimal.Decimal("0.000"))
        assert isinstance(a[3], decimal.Decimal)
        assert str(a[3]) == str(b_3)

    def test_decimal64_dict_download(self):
        self.conn.run("""
            x = 1 2 3
            y = decimal64(-10.1 NULL 5.1, 6)
            a = dict(x, y)
        """)
        a = self.conn.run("a")
        b_1 = decimal.Decimal("-10.1").quantize(decimal.Decimal("0.000000"))
        assert isinstance(a[1], decimal.Decimal)
        assert str(a[1]) == str(b_1)
        assert a[2] is None
        b_3 = decimal.Decimal("5.1").quantize(decimal.Decimal("0.000000"))
        assert isinstance(a[3], decimal.Decimal)
        assert str(a[3]) == str(b_3)

    def test_decimal128_dict_download(self):
        self.conn.run("""
            x = 1 2 3
            y = decimal128("-10.1234567899999999999" NULL "5.1", 20)
            a = dict(x, y)
        """)
        a = self.conn.run("a")
        b_1 = decimal.Decimal("-10.12345678999999999990")
        assert isinstance(a[1], decimal.Decimal)
        assert str(a[1]) == str(b_1)
        assert a[2] is None
        b_3 = decimal.Decimal("5.10000000000000000000")
        assert isinstance(a[3], decimal.Decimal)
        assert str(a[3]) == str(b_3)

    def test_decimal_dict_value_upload(self):
        a = {
            "a": decimal.Decimal("-10.100"),
            "b": decimal.Decimal("NaN"),
            "c": decimal.Decimal("2.0")
        }
        self.conn.upload({"a": a})
        assert self.conn.run("eqObj(a[`a], decimal64(-10.1,3))")
        assert self.conn.run("eqObj(a[`b], decimal64(NULL,3))")
        assert self.conn.run("eqObj(a[`c], decimal64(2.0,3))")
        a = {
            "a": decimal.Decimal("-10.1234567899999999999"),
            "b": decimal.Decimal("NaN"),
            "c": decimal.Decimal("2.0")
        }
        self.conn.upload({"a": a})
        assert self.conn.run("eqObj(a[`a], decimal128('-10.1234567899999999999',19))")
        assert self.conn.run("eqObj(a[`b], decimal128(NULL,19))")
        assert self.conn.run("eqObj(a[`c], decimal128(2.0,19))")

    def test_decimial_dict_key_upload(self):
        a = {
            decimal.Decimal("-10.100"): decimal.Decimal("-10.100"),
            "b": decimal.Decimal("NaN"),
            "c": decimal.Decimal("2.0")
        }
        with pytest.raises(Exception):
            self.conn.upload({"a": a})

    def test_decimal_set_upload(self):
        a = {
            decimal.Decimal("-10.100"),
            decimal.Decimal("NaN"),
            decimal.Decimal("2.0")
        }
        with pytest.raises(Exception):
            self.conn.upload({"a": a})

    def test_multithreadTableWriterTest_decimal(self):
        script_decimal = """
            t = table(1000:0, `float`decimal32_3`decimal32_6`decimal64_9`decimal64_12`decimal128_0`decimal128_38,[FLOAT, DECIMAL32(3), DECIMAL32(6), DECIMAL64(9), DECIMAL64(12), DECIMAL128(0), DECIMAL128(38)])
            share t as decimal_t
        """
        self.conn.run(script_decimal)
        writer = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "", "decimal_t", False, False, [], 100, 0.1, 1, "")
        cont = decimal.Context(prec=39)
        col_float = np.array([1.0, np.nan, 0, -1.00], dtype=np.float32)
        col_decimal32_3 = np.array([decimal.Decimal("-1").quantize(decimal.Decimal("0.000")), None, decimal.Decimal("0").quantize(decimal.Decimal("0.000")), decimal.Decimal("1").quantize(decimal.Decimal("0.000"))],dtype=object)
        col_decimal32_6 = np.array([decimal.Decimal("-1").quantize(decimal.Decimal("0.000000")), None, decimal.Decimal("0").quantize(decimal.Decimal("0.000000")), decimal.Decimal("1").quantize(decimal.Decimal("0.000000"))],dtype=object)
        col_decimal64_9 = np.array([decimal.Decimal("-1").quantize(decimal.Decimal("0.000000000")), None, decimal.Decimal("0").quantize(decimal.Decimal("0.000000000")),decimal.Decimal("1").quantize(decimal.Decimal("0.000000000"))], dtype=object)
        col_decimal64_12 = np.array([decimal.Decimal("-1").quantize(decimal.Decimal("0.000000000000")), None, decimal.Decimal("0").quantize(decimal.Decimal("0.000000000000")),decimal.Decimal("1").quantize(decimal.Decimal("0.000000000000"))], dtype=object)
        col_decimal128_0 = np.array([decimal.Decimal("-1"), None, decimal.Decimal("0"), decimal.Decimal("1234567890")],dtype=object)
        col_decimal128_38 = np.array([decimal.Decimal("-1.00000000000000000000000000000000000000", context=cont), None, decimal.Decimal("0", context=cont).quantize(decimal.Decimal("0.00000000000000000000000000000000000000")),decimal.Decimal("1.00000000000000000000000000000000000000", context=cont)], dtype=object)
        self.conn.run("delete from decimal_t")
        first = self.conn.run("select count(*) from decimal_t")
        for i in range(4):
            writer.insert(col_float[i], col_decimal32_3[i], col_decimal32_6[i], col_decimal64_9[i], col_decimal64_12[i],col_decimal128_0[i], col_decimal128_38[i])
        writer.waitForThreadCompletion()
        time.sleep(3)
        last = self.conn.run("select count(*) from decimal_t")
        assert last["count"][0] - first["count"][0] == 4
        re = self.conn.run("select * from decimal_t")
        col_float = np.array([1.0, np.nan, 0, -1.00], dtype=np.float32)
        col_decimal32_3 = np.array([decimal.Decimal("-1").quantize(decimal.Decimal("0.000")), None, decimal.Decimal("0").quantize(decimal.Decimal("0.000")), decimal.Decimal("1").quantize(decimal.Decimal("0.000"))],dtype=object)
        col_decimal32_6 = np.array([decimal.Decimal("-1").quantize(decimal.Decimal("0.000000")), None, decimal.Decimal("0").quantize(decimal.Decimal("0.000000")), decimal.Decimal("1").quantize(decimal.Decimal("0.000000"))],dtype=object)
        col_decimal64_9 = np.array([decimal.Decimal("-1").quantize(decimal.Decimal("0.000000000")), None, decimal.Decimal("0").quantize(decimal.Decimal("0.000000000")),decimal.Decimal("1").quantize(decimal.Decimal("0.000000000"))], dtype=object)
        col_decimal64_12 = np.array([decimal.Decimal("-1").quantize(decimal.Decimal("0.000000000000")), None, decimal.Decimal("0").quantize(decimal.Decimal("0.000000000000")),decimal.Decimal("1").quantize(decimal.Decimal("0.000000000000"))], dtype=object)
        col_decimal128_0 = np.array([decimal.Decimal("-1"), None, decimal.Decimal("0"), decimal.Decimal("1234567890")],dtype=object)
        col_decimal128_38 = np.array([decimal.Decimal("-1.00000000000000000000000000000000000000"), None, decimal.Decimal("0").quantize(decimal.Decimal("0.00000000000000000000000000000000000000")),decimal.Decimal("1.00000000000000000000000000000000000000")], dtype=object)
        data = {
            "float": col_float,
            "decimal32_3": col_decimal32_3,
            "decimal32_6": col_decimal32_6,
            "decimal64_9": col_decimal64_9,
            "decimal64_12": col_decimal64_12,
            'decimal128_0': col_decimal128_0,
            'decimal128_38': col_decimal128_38
        }
        ex = pd.DataFrame(data)
        assert_frame_equal(re, ex)
        self.conn.run("undef(`decimal_t, SHARED)")

    def test_upload_decimalNaN_table_isNull_flag(self):
        df = pd.DataFrame({
            'a': [decimal.Decimal('NaN'), decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'b': [None, decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'c': [np.nan, decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'd': [pd.NaT, decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'e': [decimal.Decimal('NaN'), decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'f': [None, decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'g': [np.nan, decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'h': [pd.NaT, decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'i': [decimal.Decimal('NaN'), decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'j': [None, decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'k': [np.nan, decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
            'l': [pd.NaT, decimal.Decimal('0.000000'), decimal.Decimal('-1.123456')],
        }, dtype='object')
        df.__DolphinDB_Type__ = {
            'a': keys.DT_DECIMAL32,
            'b': keys.DT_DECIMAL32,
            'c': keys.DT_DECIMAL32,
            'd': keys.DT_DECIMAL32,
            'e': keys.DT_DECIMAL64,
            'f': keys.DT_DECIMAL64,
            'g': keys.DT_DECIMAL64,
            'h': keys.DT_DECIMAL64,
            'i': keys.DT_DECIMAL128,
            'j': keys.DT_DECIMAL128,
            'k': keys.DT_DECIMAL128,
            'l': keys.DT_DECIMAL128,
        }
        self.conn.upload({'tab': df})
        assert self.conn.run("res = [];for(i in 0:tab.columns()){res.append!(hasNull(tab.column(i)))};all(res)")

    def test_download_decimal32_table(self):
        res = self.conn.run("table(decimal32(``0`1.2345`2.0'-100', 3) as c1)")
        ex_df = pd.DataFrame({'c1': [None, decimal.Decimal("0.000"), decimal.Decimal("1.235"), decimal.Decimal("2.000"),
                                     decimal.Decimal("-100.000")]})
        assert_frame_equal(res, ex_df)

    def test_download_decimal64_table(self):
        res = self.conn.run("table(decimal64(``0`1.123123123123123`2.00000'-100', 12) as c1)")
        ex_df = pd.DataFrame({'c1': [None, decimal.Decimal("0.000000000000"), decimal.Decimal("1.123123123123"),decimal.Decimal("2.000000000000"), decimal.Decimal("-100.000000000000")]})
        assert_frame_equal(res, ex_df)

    def test_download_decimal128_table(self):
        res = self.conn.run("table(decimal128(``0`1.123123123123123123123123`2.00000'-100', 22) as c1)")
        ex_df = pd.DataFrame({'c1': [None, decimal.Decimal("0.0000000000000000000000"),decimal.Decimal("1.1231231231231231231231"),decimal.Decimal("2.000000000000000000000"),decimal.Decimal("-100.000000000000000000000")]})
        assert_frame_equal(res, ex_df)

    def test_download_decimal32_arrayVector(self):
        res = self.conn.run("x = array(DECIMAL32(5)[])\
                            .append!([decimal32(take(`1`0``2.123,5), 5)])\
                            .append!([decimal32(string(take('-1.123456''0''3.4958293825',5)), 5)]);x")
        ex_val0 = [decimal.Decimal("1.00000"), decimal.Decimal("0.00000"), None, decimal.Decimal("2.12300"),decimal.Decimal("1.00000")]
        ex_val1 = [decimal.Decimal("-1.12346"), decimal.Decimal("0.00000"), decimal.Decimal("3.495830"),decimal.Decimal("-1.12346"), decimal.Decimal("0.00000")]
        ex = np.array([ex_val0, ex_val1], dtype=object)
        for x, y in zip(res, ex):
            assert_array_equal(x, y)

    def test_download_decimal64_arrayVector(self):
        res = self.conn.run("x = array(DECIMAL64(10)[])\
                            .append!([decimal64(take(`1`0``2.123,5), 10)])\
                            .append!([decimal64(string(take('-1.1234567899999999999999''0.9999999999999999999999''34958293.111111111111111',5)), 10)]);x")
        ex_val0 = [decimal.Decimal("1.0000000000"), decimal.Decimal("0.0000000000"), None,decimal.Decimal("2.1230000000"), decimal.Decimal("1.0000000000")]
        ex_val1 = [decimal.Decimal("-1.1234567900"), decimal.Decimal("1.0000000000"),decimal.Decimal("34958293.1111111111"), decimal.Decimal("-1.1234567900"),decimal.Decimal("1.0000000000")]
        ex = np.array([ex_val0, ex_val1], dtype=object)
        for x, y in zip(res, ex):
            assert_array_equal(x, y)

    def test_download_decimal32_array_vector_gt65535(self):
        res = self.conn.run("""
            x = array(DECIMAL32(5)[])
            for (i in 0:(70000/2)){
                x.append!([decimal32(take(`1`0``2.123,5), 5)])
                x.append!([decimal32(string(take('-1.123456''0''3.4958293825',5)), 5)])
            }
            x
        """)
        ex_val0 = [decimal.Decimal("1.00000"), decimal.Decimal("0.00000"), None, decimal.Decimal("2.12300"),
                   decimal.Decimal("1.00000")]
        ex_val1 = [decimal.Decimal("-1.12346"), decimal.Decimal("0.00000"), decimal.Decimal("3.495830"),
                   decimal.Decimal("-1.12346"), decimal.Decimal("0.00000")]
        for ind, val in enumerate(res):
            if ind % 2 == 0:
                assert_array_equal(val, ex_val0)
            else:
                assert_array_equal(val, ex_val1)

    def test_download_decimal64_array_vector_gt65535(self):
        res = self.conn.run("""
            x = array(DECIMAL64(10)[])
            for (i in 0:(70000/2)){
                x.append!([decimal64(take(`1`0``2.123,5), 10)])
                x.append!([decimal64(string(take('-1.1234567899999999999999''0.9999999999999999999999''34958293.111111111111111',5)), 10)])
            }
            x
        """)
        ex_val0 = [decimal.Decimal("1.0000000000"), decimal.Decimal("0.0000000000"), None,decimal.Decimal("2.1230000000"), decimal.Decimal("1.0000000000")]
        ex_val1 = [decimal.Decimal("-1.1234567900"), decimal.Decimal("1.0000000000"),decimal.Decimal("34958293.1111111111"), decimal.Decimal("-1.1234567900"),decimal.Decimal("1.0000000000")]
        for ind, val in enumerate(res):
            if ind % 2 == 0:
                assert_array_equal(val, ex_val0)
            else:
                assert_array_equal(val, ex_val1)

    def test_download_table_with_decimal32_arrayVector(self):
        res = self.conn.run("""
            c1 = array(DECIMAL32(0)[]).append!([decimal32(take(`1`0``2.123,5), 0)]).append!([decimal32(string(take('-1.111111''0''3.2222222222',5)), 0)])
            c2 = array(DECIMAL32(2)[]).append!([decimal32(take(`1`0``2.123,5), 2)]).append!([decimal32(string(take('-1.111111''0''3.2222222222',5)), 2)])
            c3 = array(DECIMAL32(4)[]).append!([decimal32(take(`1`0``2.123,5), 4)]).append!([decimal32(string(take('-1.111111''0''3.2222222222',5)), 4)])
            c4 = array(DECIMAL32(6)[]).append!([decimal32(take(`1`0``2.123,5), 6)]).append!([decimal32(string(take('-1.111111''0''3.2222222222',5)), 6)])
            c5 = array(DECIMAL32(8)[]).append!([decimal32(take(`1`0``2.123,5), 8)]).append!([decimal32(string(take('-1.111111''0''3.2222222222',5)), 8)])
            go
            table(c1,c2,c3,c4,c5)
        """)
        for col in ['c1', 'c2', 'c3', 'c4', 'c5']:
            qfile = "0" if col[1] == "1" else "0.{:<0{}}".format("0", (int(col[1]) - 1) * 2)
            assert_array_equal(res[col].to_list()[0], get_ex_row0(decimal.Decimal(qfile)))
            assert_array_equal(res[col].to_list()[1], get_ex_row1(decimal.Decimal(qfile)))

    def test_download_table_with_decimal64_arrayVector(self):
        res = self.conn.run("""
            c1 = array(DECIMAL64(0)[]).append!([decimal64(take(`1`0``2.123,5), 0)]).append!([decimal64(string(take('-1.111111''0''3.2222222222',5)), 0)])
            c2 = array(DECIMAL64(4)[]).append!([decimal64(take(`1`0``2.123,5), 4)]).append!([decimal64(string(take('-1.111111''0''3.2222222222',5)), 4)])
            c3 = array(DECIMAL64(8)[]).append!([decimal64(take(`1`0``2.123,5), 8)]).append!([decimal64(string(take('-1.111111''0''3.2222222222',5)), 8)])
            c4 = array(DECIMAL64(12)[]).append!([decimal64(take(`1`0``2.123,5), 12)]).append!([decimal64(string(take('-1.111111''0''3.2222222222',5)), 12)])
            c5 = array(DECIMAL64(16)[]).append!([decimal64(take(`1`0``2.123,5), 16)]).append!([decimal64(string(take('-1.111111''0''3.2222222222',5)), 16)])
            go
            table(c1,c2,c3,c4,c5)
        """)
        for col in ['c1', 'c2', 'c3', 'c4', 'c5']:
            qfile = "0" if col[1] == "1" else "0.{:<0{}}".format("0", (int(col[1]) - 1) * 4)
            assert_array_equal(res[col].to_list()[0], get_ex_row0(decimal.Decimal(qfile)))
            assert_array_equal(res[col].to_list()[1], get_ex_row1(decimal.Decimal(qfile)))

    def test_download_table_with_decimal32_array_vector_gt65535(self):
        res = self.conn.run("""
            t = table(1:0, `c1`c2`c3`c4`c5, [DECIMAL32(0)[], DECIMAL32(2)[], DECIMAL32(4)[], DECIMAL32(6)[], DECIMAL32(8)[]])
            for (i in 0:70000){
                tableInsert(t, [decimal32(1 1,0)], [decimal32(2 2,2)], [decimal32(3 3,4)], [decimal32(4 4,6)], [decimal32(5 5,8)])
            }
            t
        """)
        assert_array_equal(res['c1'].to_list(), [[decimal.Decimal("1"), decimal.Decimal("1")]] * 70000)
        assert_array_equal(res['c2'].to_list(), [[decimal.Decimal("2.00"), decimal.Decimal("2.00")]] * 70000)
        assert_array_equal(res['c3'].to_list(), [[decimal.Decimal("3.0000"), decimal.Decimal("3.0000")]] * 70000)
        assert_array_equal(res['c4'].to_list(), [[decimal.Decimal("4.000000"), decimal.Decimal("4.000000")]] * 70000)
        assert_array_equal(res['c5'].to_list(),[[decimal.Decimal("5.00000000"), decimal.Decimal("5.00000000")]] * 70000)

    def test_download_table_with_decimal64_array_vector_gt65535(self):
        res = self.conn.run("""
            t = table(1:0, `c1`c2`c3`c4`c5, [DECIMAL64(0)[], DECIMAL64(4)[], DECIMAL64(8)[], DECIMAL64(12)[], DECIMAL64(16)[]])
            for (i in 0:70000){
                tableInsert(t, [decimal64(1 1,0)], [decimal64(2 2,4)], [decimal64(3 3,8)], [decimal64(4 4,12)], [decimal64(5 5,16)])
            }
            t
        """)
        assert_array_equal(res['c1'].to_list(), [[decimal.Decimal("1"), decimal.Decimal("1")]] * 70000)
        assert_array_equal(res['c2'].to_list(), [[decimal.Decimal("2.0000"), decimal.Decimal("2.0000")]] * 70000)
        assert_array_equal(res['c3'].to_list(),[[decimal.Decimal("3.00000000"), decimal.Decimal("3.00000000")]] * 70000)
        assert_array_equal(res['c4'].to_list(),[[decimal.Decimal("4.000000000000"), decimal.Decimal("4.000000000000")]] * 70000)
        assert_array_equal(res['c5'].to_list(),[[decimal.Decimal("5.0000000000000000"), decimal.Decimal("5.0000000000000000")]] * 70000)

    def test_upload_table_with_decimal32_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `c1`c2`c3`c4`c5, [DECIMAL32(0)[], DECIMAL32(2)[], DECIMAL32(4)[], DECIMAL32(6)[], DECIMAL32(8)[]])
            for (i in 0:70000){
                tableInsert(t, [decimal32(1 1,0)], [decimal32(2 2,2)], [decimal32(3 3,4)], [decimal32(4 4,6)], [decimal32(5 00i,8)])
            }
            t
        """)
        self.conn.upload({"tmp": tmp})
        assert all(self.conn.run("each(eqObj, tmp.values(), t.values())"))

    def test_upload_table_with_decimal64_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `c1`c2`c3`c4`c5, [DECIMAL64(0)[], DECIMAL64(4)[], DECIMAL64(8)[], DECIMAL64(12)[], DECIMAL64(16)[]])
            for (i in 0:70000){
                tableInsert(t, [decimal64(1 1,0)], [decimal64(2 2,4)], [decimal64(3 3,8)], [decimal64(4 4,12)], [decimal64(5 00i,16)])
            }
            t
        """)
        self.conn.upload({"tmp": tmp})
        assert all(self.conn.run("each(eqObj, tmp.values(), t.values())"))

    def test_tableAppender_with_decimal32_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `c1`c2`c3`c4`c5, [DECIMAL32(0)[], DECIMAL32(2)[], DECIMAL32(4)[], DECIMAL32(6)[], DECIMAL32(8)[]])
            t1 = table(1:0, `c1`c2`c3`c4`c5, [DECIMAL32(0)[], DECIMAL32(2)[], DECIMAL32(4)[], DECIMAL32(6)[], DECIMAL32(8)[]])
            for (i in 0:70000){
                tableInsert(t, [decimal32(1 1,0)], [decimal32(2 2,2)], [decimal32(3 3,4)], [decimal32(4 4,6)], [decimal32(5 00i,8)])
            }
            t
        """)
        appender = ddb.tableAppender(tableName='t1', ddbSession=self.conn)
        rows = appender.append(tmp)
        assert rows == 70000
        assert all(self.conn.run("each(eqObj, t1.values(), t.values())"))

    def test_tableAppender_with_decimal64_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `c1`c2`c3`c4`c5, [DECIMAL64(0)[], DECIMAL64(4)[], DECIMAL64(8)[], DECIMAL64(12)[], DECIMAL64(16)[]])
            t1 = table(1:0, `c1`c2`c3`c4`c5, [DECIMAL64(0)[], DECIMAL64(4)[], DECIMAL64(8)[], DECIMAL64(12)[], DECIMAL64(16)[]])
            for (i in 0:70000){
                tableInsert(t, [decimal64(1 1,0)], [decimal64(2 2,4)], [decimal64(3 3,8)], [decimal64(4 4,12)], [decimal64(5 00i,16)])
            }
            t
        """)
        appender = ddb.tableAppender(tableName='t1', ddbSession=self.conn)
        rows = appender.append(tmp)
        assert rows == 70000
        assert all(self.conn.run("each(eqObj, t1.values(), t.values())"))

    def test_tableUpsert_with_decimal32_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `ind`c1`c2, [INT, DECIMAL32(0)[], DECIMAL32(8)[]])
            t1 = table(1:0, `ind`c1`c2, [INT, DECIMAL32(0)[], DECIMAL32(8)[]])
            for (i in 0:70000){
                tableInsert(t, i, [decimal32(1 1,0)], [decimal32(4 00i,8)])
                tableInsert(t1, i, [decimal32(4 4,0)], [decimal32(1 00i,8)])
            }
            dbpath = "dfs://test_tu_tab_av"
            if(existsDatabase(dbpath))
                dropDatabase(dbpath)
            db = database(dbpath, RANGE, 0 70000, engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `ind, sortColumns=`ind).append!(t)
            pt1 = db.createPartitionedTable(t, `pt1, `ind, sortColumns=`ind).append!(t1)
            go
            select * from loadTable(dbpath, `pt)
        """)
        up = ddb.tableUpsert(dbPath="dfs://test_tu_tab_av", tableName='pt1', ddbSession=self.conn, keyColNames=['ind'])
        up.upsert(tmp)
        assert all(self.conn.run("""
             ex = select * from loadTable(dbpath, `pt)
             res = select * from loadTable(dbpath, `pt1)
             each(eqObj, ex.values(), res.values())
         """))

    def test_tableUpsert_with_decimal64_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `ind`c1`c2, [INT, DECIMAL64(0)[], DECIMAL64(16)[]])
            t1 = table(1:0, `ind`c1`c2, [INT, DECIMAL64(0)[], DECIMAL64(16)[]])
            for (i in 0:70000){
                tableInsert(t, i, [decimal64(1 1,0)], [decimal64(4 00i,16)])
                tableInsert(t1, i, [decimal64(4 4,0)], [decimal64(1 00i,16)])
            }
            dbpath = "dfs://test_tu_tab_av"
            if(existsDatabase(dbpath))
                dropDatabase(dbpath)
            db = database(dbpath, RANGE, 0 70000, engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `ind, sortColumns=`ind).append!(t)
            pt1 = db.createPartitionedTable(t, `pt1, `ind, sortColumns=`ind).append!(t1)
            go
            select * from loadTable(dbpath, `pt)
        """)
        up = ddb.tableUpsert(dbPath="dfs://test_tu_tab_av", tableName='pt1', ddbSession=self.conn, keyColNames=['ind'])
        up.upsert(tmp)
        assert all(self.conn.run("""
            ex = select * from loadTable(dbpath, `pt)
            res = select * from loadTable(dbpath, `pt1)
            each(eqObj, ex.values(), res.values())
        """))

    def test_partitionedTableAppender_with_decimal32_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `ind`c1`c2, [INT, DECIMAL64(0)[], DECIMAL64(16)[]])
            for (i in 0:70000){
                tableInsert(t, i, [decimal64(1 1,0)], [decimal64(4 00i,16)])
            }
            dbpath = "dfs://test_tu_tab_av"
            if(existsDatabase(dbpath))
                dropDatabase(dbpath)
            db = database(dbpath, RANGE, 0 70000, engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `ind, sortColumns=`ind)
            go
            select * from t order by ind
        """)
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD)
        appender = ddb.PartitionedTableAppender("dfs://test_tu_tab_av", 'pt', 'ind', pool)
        rows = appender.append(tmp)
        assert rows == 70000
        assert all(self.conn.run("res = select * from loadTable(dbpath, `pt) order by ind;each(eqObj, res.values(), t.values())"))

    def test_partitionedTableAppender_with_decimal64_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `ind`c1`c2, [INT, DECIMAL64(0)[], DECIMAL64(16)[]])
            for (i in 0:70000){
                tableInsert(t, i, [decimal64(1 1,0)], [decimal64(4 00i,16)])
            }
            dbpath = "dfs://test_tu_tab_av"
            if(existsDatabase(dbpath))
                dropDatabase(dbpath)
            db = database(dbpath, RANGE, 0 70000, engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `ind, sortColumns=`ind)
            go
            select * from t order by ind
        """)
        pool = ddb.DBConnectionPool(HOST, PORT, 2, USER, PASSWD)
        appender = ddb.PartitionedTableAppender("dfs://test_tu_tab_av", 'pt', 'ind', pool)
        rows = appender.append(tmp)
        assert rows == 70000
        assert all(self.conn.run("res = select * from loadTable(dbpath, `pt) order by ind;each(eqObj, res.values(), t.values())"))

    def test_MultithreadedTableWriter_with_decimal32_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `ind`c1`c2, [INT, DECIMAL32(0)[], DECIMAL64(8)[]])
            for (i in 0:70000){
                tableInsert(t, i, [decimal32(1 1,0)], [decimal32(4 00i,8)])
            }
            dbpath = "dfs://test_tu_tab_av"
            if(existsDatabase(dbpath))
                dropDatabase(dbpath)
            db = database(dbpath, RANGE, 0 70000, engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `ind, sortColumns=`ind)
            go
            select * from t
        """)
        mtwriter = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://test_tu_tab_av", 'pt', threadCount=2,partitionCol='ind')
        for _, rowData in tmp.iterrows():
            ind = rowData['ind']
            c1 = rowData['c1']
            c2 = rowData['c2']
            err = mtwriter.insert(ind, c1, c2)
            if err.hasError():
                raise Exception(err.errorInfo())
        mtwriter.waitForThreadCompletion()
        assert all(self.conn.run("res = select * from loadTable(dbpath, `pt);each(eqObj, res.values(), t.values())"))

    def test_MultithreadedTableWriter_with_decimal64_array_vector_gt65535(self):
        tmp = self.conn.run("""
            t = table(1:0, `ind`c1`c2, [INT, DECIMAL64(0)[], DECIMAL64(16)[]])
            for (i in 0:70000){
                tableInsert(t, i, [decimal64(1 1,0)], [decimal64(4 00i,16)])
            }
            dbpath = "dfs://test_tu_tab_av"
            if(existsDatabase(dbpath))
                dropDatabase(dbpath)
            db = database(dbpath, RANGE, 0 70000, engine='TSDB')
            pt = db.createPartitionedTable(t, `pt, `ind, sortColumns=`ind)
            go
            select * from t
        """)
        mtwriter = ddb.MultithreadedTableWriter(HOST, PORT, USER, PASSWD, "dfs://test_tu_tab_av", 'pt', threadCount=2,partitionCol='ind')
        for _, rowData in tmp.iterrows():
            ind = rowData['ind']
            c1 = rowData['c1']
            c2 = rowData['c2']
            err = mtwriter.insert(ind, c1, c2)
            if err.hasError():
                raise Exception(err.errorInfo())
        mtwriter.waitForThreadCompletion()
        assert all(self.conn.run("res = select * from loadTable(dbpath, `pt);each(eqObj, res.values(), t.values())"))

    def test_session_disableDecimal(self):
        result=self.conn.run("""
            v_decimal32=array(DECIMAL32(2)).append!(decimal32(["nan","3.15","nan"],2));
            v_decimal64=array(DECIMAL64(17)).append!(decimal64(["nan","3.14159265358979324","nan"],17));
            v_decimal128=array(DECIMAL128(38)).append!(decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38));
            av_decimal32=array(DECIMAL32(2)[]).append!([decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2)]);
            av_decimal64=array(DECIMAL64(17)[]).append!([decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17)]);
            av_decimal128=array(DECIMAL128(38)[]).append!([decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38)]);
            table(
                v_decimal32 as `v_decimal32,
                v_decimal64 as `v_decimal64,
                v_decimal128 as `v_decimal128,
                av_decimal32 as `av_decimal32,
                av_decimal64 as `av_decimal64,
                av_decimal128 as `av_decimal128,
                [1,2,3] as other
            );
        """,disableDecimal=True)
        expect=self.conn.run("""
            v_decimal32_d=array(DOUBLE).append!(decimal32(["nan","3.15","nan"],2));
            v_decimal64_d=array(DOUBLE).append!(decimal64(["nan","3.14159265358979324","nan"],17));
            v_decimal128_d=array(DOUBLE).append!(decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38));
            av_decimal32_d=array(DOUBLE[]).append!([decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2)]);
            av_decimal64_d=array(DOUBLE[]).append!([decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17)]);
            av_decimal128_d=array(DOUBLE[]).append!([decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38)]);
            table(
                v_decimal32_d as `v_decimal32,
                v_decimal64_d as `v_decimal64,
                v_decimal128_d as `v_decimal128,
                av_decimal32_d as `av_decimal32,
                av_decimal64_d as `av_decimal64,
                av_decimal128_d as `av_decimal128,
                [1,2,3] as other
            );
        """,disableDecimal=False)
        assert_frame_equal(result,expect)

    @pytest.mark.parametrize('compress', [True,False])
    def test_session_disableDecimal(self,compress):
        conn=ddb.Session(HOST,PORT,USER,PASSWD,compress=compress)
        result=conn.run("""
            v_decimal32=array(DECIMAL32(2)).append!(decimal32(["nan","3.15","nan"],2));
            v_decimal64=array(DECIMAL64(17)).append!(decimal64(["nan","3.14159265358979324","nan"],17));
            v_decimal128=array(DECIMAL128(38)).append!(decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38));
            av_decimal32=array(DECIMAL32(2)[]).append!([decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2)]);
            av_decimal64=array(DECIMAL64(17)[]).append!([decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17)]);
            av_decimal128=array(DECIMAL128(38)[]).append!([decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38)]);
            table(
                v_decimal32 as `v_decimal32,
                v_decimal64 as `v_decimal64,
                v_decimal128 as `v_decimal128,
                av_decimal32 as `av_decimal32,
                av_decimal64 as `av_decimal64,
                av_decimal128 as `av_decimal128,
                [1,2,3] as other
            );
        """,disableDecimal=True)
        expect=conn.run("""
            v_decimal32_d=array(DOUBLE).append!(decimal32(["nan","3.15","nan"],2));
            v_decimal64_d=array(DOUBLE).append!(decimal64(["nan","3.14159265358979324","nan"],17));
            v_decimal128_d=array(DOUBLE).append!(decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38));
            av_decimal32_d=array(DOUBLE[]).append!([decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2)]);
            av_decimal64_d=array(DOUBLE[]).append!([decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17)]);
            av_decimal128_d=array(DOUBLE[]).append!([decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38)]);
            table(
                v_decimal32_d as `v_decimal32,
                v_decimal64_d as `v_decimal64,
                v_decimal128_d as `v_decimal128,
                av_decimal32_d as `av_decimal32,
                av_decimal64_d as `av_decimal64,
                av_decimal128_d as `av_decimal128,
                [1,2,3] as other
            );
        """,disableDecimal=False)
        assert_frame_equal(result,expect)

    @pytest.mark.parametrize('compress', [True, False])
    def test_DBConnectionPool_disableDecimal(self,compress):
        pool = ddb.DBConnectionPool(HOST, PORT, 2,USER,PASSWD,compress=compress)
        result=pool.runTaskAsync("""
            v_decimal32=array(DECIMAL32(2)).append!(decimal32(["nan","3.15","nan"],2));
            v_decimal64=array(DECIMAL64(17)).append!(decimal64(["nan","3.14159265358979324","nan"],17));
            v_decimal128=array(DECIMAL128(38)).append!(decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38));
            av_decimal32=array(DECIMAL32(2)[]).append!([decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2)]);
            av_decimal64=array(DECIMAL64(17)[]).append!([decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17)]);
            av_decimal128=array(DECIMAL128(38)[]).append!([decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38)]);
            table(
                v_decimal32 as `v_decimal32,
                v_decimal64 as `v_decimal64,
                v_decimal128 as `v_decimal128,
                av_decimal32 as `av_decimal32,
                av_decimal64 as `av_decimal64,
                av_decimal128 as `av_decimal128,
                [1,2,3] as other
            );
        """,disableDecimal=True).result()
        expect=pool.runTaskAsync("""
            v_decimal32_d=array(DOUBLE).append!(decimal32(["nan","3.15","nan"],2));
            v_decimal64_d=array(DOUBLE).append!(decimal64(["nan","3.14159265358979324","nan"],17));
            v_decimal128_d=array(DOUBLE).append!(decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38));
            av_decimal32_d=array(DOUBLE[]).append!([decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2),decimal32(["nan","3.15","nan"],2)]);
            av_decimal64_d=array(DOUBLE[]).append!([decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17),decimal64(["nan","3.14159265358979324","nan"],17)]);
            av_decimal128_d=array(DOUBLE[]).append!([decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38),decimal128(["nan","0.14159265358979323846264338327950288418","nan"],38)]);
            table(
                v_decimal32_d as `v_decimal32,
                v_decimal64_d as `v_decimal64,
                v_decimal128_d as `v_decimal128,
                av_decimal32_d as `av_decimal32,
                av_decimal64_d as `av_decimal64,
                av_decimal128_d as `av_decimal128,
                [1,2,3] as other
            );
        """,disableDecimal=False).result()
        assert_frame_equal(result, expect)
