from importlib.util import find_spec

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
import pytest

from basic_testing.prepare import DataUtils, PANDAS_VERSION
from basic_testing.utils import assertPlus
from setup.settings import HOST, PORT, USER, PASSWD

if find_spec("pyarrow") is not None:
    import pyarrow as pa


@pytest.mark.BASIC
class TestUploadNew(object):
    conn: ddb.Session = ddb.Session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                      b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_64_scalar(self, data):
        data_ = np.frombuffer(data, dtype=np.float64)[0]
        self.__class__.conn.upload({'a': data_})
        assert self.__class__.conn.run('isNanInf(a)==false')
        assert self.__class__.conn.run('isNull(a)')
        assert self.__class__.conn.run("typestr a") == 'DOUBLE'

    @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                      b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_64_vector(self, data):
        data_ = np.frombuffer(data, dtype=np.float64)[0]
        self.__class__.conn.upload({'a': [data_, data_, data_]})
        assert all(self.__class__.conn.run('isNanInf(a)==false'))
        assert all(self.__class__.conn.run('isNull(a)'))
        assert self.__class__.conn.run("typestr a") == 'FAST DOUBLE VECTOR'

    @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                      b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_64_set(self, data):
        data_ = np.frombuffer(data, dtype=np.float64)[0]
        self.__class__.conn.upload({'a': {data_, 1.1}})
        assert all(self.__class__.conn.run('isNanInf(a.keys())==false'))
        assert any(self.__class__.conn.run('isNull(a.keys())'))
        assert self.__class__.conn.run("typestr a") == 'DOUBLE SET'

    @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                      b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_64_dict(self, data):
        data_ = np.frombuffer(data, dtype=np.float64)[0]
        self.__class__.conn.upload({'a': {'a': data_, 'b': 1.1}})
        assert self.__class__.conn.run('isNanInf(a["a"])==false')
        assert self.__class__.conn.run('isNull(a["a"])')
        assert self.__class__.conn.run("typestr a") == 'STRING->DOUBLE DICTIONARY'

    @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                      b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_64_table(self, data):
        data_ = np.frombuffer(data, dtype=np.float64)[0]
        self.__class__.conn.upload({'a': pd.DataFrame({'a': [data_]})})
        assert self.__class__.conn.run('isNanInf(a["a"][0])==false')
        assert self.__class__.conn.run('isNull(a["a"][0])')
        assert self.__class__.conn.run('typestr a["a"]') == 'FAST DOUBLE VECTOR'

    @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                      b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_64_table_array_vector(self, data):
        data_ = np.frombuffer(data, dtype=np.float64)[0]
        df = pd.DataFrame({'a': [[data_]]})
        df.__DolphinDB_Type__ = {
            'a': keys.DT_DOUBLE_ARRAY
        }
        self.__class__.conn.upload({'a': df})
        assert self.__class__.conn.run('isNanInf(a["a"][0][0])==false')
        assert self.__class__.conn.run('isNull(a["a"][0][0])')
        assert self.__class__.conn.run('typestr a["a"]') == 'FAST DOUBLE[] VECTOR'

    @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                      b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_64_table_setType_FLOAT(self, data):
        data_ = np.frombuffer(data, dtype=np.float64)[0]
        df = pd.DataFrame({'a': [data_]})
        df.__DolphinDB_Type__ = {
            'a': keys.DT_FLOAT
        }
        self.__class__.conn.upload({'a': df})
        assert self.__class__.conn.run('isNull(a["a"][0])')
        assert self.__class__.conn.run('typestr a["a"]') == 'FAST FLOAT VECTOR'

    @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                      b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_64_table_array_vector_setType_FLOAT(self, data):
        data_ = np.frombuffer(data, dtype=np.float64)[0]
        df = pd.DataFrame({'a': [[data_]]})
        df.__DolphinDB_Type__ = {
            'a': keys.DT_FLOAT_ARRAY
        }
        self.__class__.conn.upload({'a': df})
        assert self.__class__.conn.run('isNull(a["a"][0][0])')
        assert self.__class__.conn.run('typestr a["a"]') == 'FAST FLOAT[] VECTOR'

    @pytest.mark.parametrize('data',
                             [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_32_scalar(self, data):
        data_ = np.frombuffer(data, dtype=np.float32)[0]
        self.__class__.conn.upload({'a': data_})
        assert self.__class__.conn.run('isNull(a)')
        assert self.__class__.conn.run("typestr a") == 'FLOAT'

    @pytest.mark.parametrize('data',
                             [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_32_vector(self, data):
        data_ = np.frombuffer(data, dtype=np.float32)[0]
        self.__class__.conn.upload({'a': [data_, data_, data_]})
        assert all(self.__class__.conn.run('isNull(a)'))
        assert self.__class__.conn.run("typestr a") == 'FAST FLOAT VECTOR'

    @pytest.mark.parametrize('data',
                             [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_32_set(self, data):
        data_ = np.frombuffer(data, dtype=np.float32)[0]
        self.__class__.conn.upload({'a': {data_, np.float32(1.1)}})
        assert any(self.__class__.conn.run('isNull(a.keys())'))
        assert self.__class__.conn.run("typestr a") == 'FLOAT SET'

    @pytest.mark.parametrize('data',
                             [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_32_dict(self, data):
        data_ = np.frombuffer(data, dtype=np.float32)[0]
        self.__class__.conn.upload({'a': {'a': data_, 'b': np.float32(1.1)}})
        assert self.__class__.conn.run('isNull(a["a"])')
        assert self.__class__.conn.run("typestr a") == 'STRING->FLOAT DICTIONARY'

    @pytest.mark.parametrize('data',
                             [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_32_table(self, data):
        data_ = np.frombuffer(data, dtype=np.float32)[0]
        self.__class__.conn.upload({'a': pd.DataFrame({'a': [data_]}, dtype=np.float32)})
        assert self.__class__.conn.run('isNull(a["a"][0])')
        assert self.__class__.conn.run('typestr a["a"]') == 'FAST FLOAT VECTOR'

    @pytest.mark.parametrize('data',
                             [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_32_table_array_vector(self, data):
        data_ = np.frombuffer(data, dtype=np.float32)[0]
        df = pd.DataFrame({'a': [[data_]]})
        df.__DolphinDB_Type__ = {
            'a': keys.DT_FLOAT_ARRAY
        }
        self.__class__.conn.upload({'a': df})
        assert self.__class__.conn.run('isNull(a["a"][0][0])')
        assert self.__class__.conn.run('typestr a["a"]') == 'FAST FLOAT[] VECTOR'

    @pytest.mark.parametrize('data',
                             [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_32_table_setType_DOUBLE(self, data):
        data_ = np.frombuffer(data, dtype=np.float32)[0]
        df = pd.DataFrame({'a': [data_]})
        df.__DolphinDB_Type__ = {
            'a': keys.DT_DOUBLE
        }
        self.__class__.conn.upload({'a': df})
        assert self.__class__.conn.run('isNanInf(a["a"][0])==false')
        assert self.__class__.conn.run('isNull(a["a"][0])')
        assert self.__class__.conn.run('typestr a["a"]') == 'FAST DOUBLE VECTOR'

    @pytest.mark.parametrize('data',
                             [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                             ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
    def test_upload_nan_32_table_array_vector_setType_DOUBLE(self, data):
        data_ = np.frombuffer(data, dtype=np.float32)[0]
        df = pd.DataFrame({'a': [[data_]]})
        df.__DolphinDB_Type__ = {
            'a': keys.DT_DOUBLE_ARRAY
        }
        self.__class__.conn.upload({'a': df})
        assert self.__class__.conn.run('isNanInf(a["a"][0][0])==false')
        assert self.__class__.conn.run('isNull(a["a"][0][0])')
        assert self.__class__.conn.run('typestr a["a"]') == 'FAST DOUBLE[] VECTOR'

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getScalar('upload').items()],
                             ids=[i for i in DataUtils.getScalar('upload')])
    def test_upload_scalar(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            if v['value'] is None:
                with pytest.raises(RuntimeError, match="isn't initialized yet"):
                    self.__class__.conn.run(k)
                continue
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            if 'decimal' in k:
                assertPlus(self.__class__.conn.run(f"string({k})==string({v['expect_value']})"))
            else:
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getVector('upload').items()],
                             ids=[i for i in DataUtils.getVector('upload')])
    def test_upload_vector(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            if 'decimal' in k:
                self.__class__.conn.run(f"print string({k})")
                assertPlus(self.__class__.conn.run(f"string({k})==string({v['expect_value']})"))
            else:
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('order', ['K', 'A', 'C', 'F'], ids=['ORDER_K', 'ORDER_A', 'ORDER_C', 'ORDER_F'])
    def test_upload_vector_np_order(self, order):
        data = np.array([[1, 2, 3], [4, 5, 6]], dtype='int64', order=order)
        self.__class__.conn.upload({'order_' + order: data[0]})
        assertPlus(self.__class__.conn.run(f"typestr(order_{order})=='FAST LONG VECTOR'"))
        assertPlus(self.__class__.conn.run(f"order_{order}==[1,2,3]"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getMatrix('upload').items()],
                             ids=[i for i in DataUtils.getMatrix('upload')])
    def test_upload_matrix(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}")[0])

    @pytest.mark.parametrize('order', ['K', 'A', 'C', 'F'], ids=['ORDER_K', 'ORDER_A', 'ORDER_C', 'ORDER_F'])
    def test_upload_matrix_np_order(self, order):
        data = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]], dtype='int64', order=order)
        self.__class__.conn.upload({'order_' + order: data[0:2]})
        assertPlus(self.__class__.conn.run(f"typestr(order_{order})=='FAST LONG MATRIX'"))
        assertPlus(self.__class__.conn.run(f"order_{order}==matrix([[1,4],[2,5],[3,6]])")[0])

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getSet('upload').items()],
                             ids=[i for i in DataUtils.getSet('upload')])
    def test_upload_set(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getDict('upload').items()],
                             ids=[i for i in DataUtils.getDict('upload')])
    def test_upload_dict(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            if 'decimal' in k:
                assertPlus(self.__class__.conn.run(f"string({k}[`a])==string({v['expect_value']})"))
            else:
                assertPlus(self.__class__.conn.run(f"{k}[`a]=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTable('upload').items()],
                             ids=[i for i in DataUtils.getTable('upload')])
    def test_upload_table(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
            if 'decimal' in k:
                assertPlus(self.__class__.conn.run(f"string({k}[`a])==string({v['expect_value']})"))
            else:
                assertPlus(self.__class__.conn.run(f"{k}[`a]=={v['expect_value']}"))

    @pytest.mark.parametrize('dtype', ['object', 'int64', 'empty'], ids=['DTYPE_OBJECT', 'DTYPE_INT64', 'DTYPE_EMPTY'])
    @pytest.mark.parametrize('order', ['K', 'A', 'C', 'F'], ids=['ORDER_K', 'ORDER_A', 'ORDER_C', 'ORDER_F'])
    def test_upload_table_np_order(self, order, dtype):
        if dtype != 'empty':
            data = pd.DataFrame(np.array([[1, 2], [3, 4], [5, 6]], dtype=dtype, order=order), columns=['a', 'b'])
        else:
            data = pd.DataFrame(np.array([[1, 2], [3, 4], [5, 6]], order=order), columns=['a', 'b'])
        self.__class__.conn.upload({'order_' + order: data})
        assertPlus(self.__class__.conn.run(f"order_{order}==table([1,3,5] as `a,[2,4,6] as `b)"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getVectorContainNone('upload').items()],
                             ids=[i for i in DataUtils.getVectorContainNone('upload')])
    def test_upload_vector_contain_none(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            if 'decimal' in k:
                assertPlus(self.__class__.conn.run(f"string({k})==string({v['expect_value']})"))
            else:
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getVectorMix('upload').items()],
                             ids=[i for i in DataUtils.getVectorMix('upload')])
    def test_upload_vector_mix(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getVectorSpecial('upload').items()],
                             ids=[i for i in DataUtils.getVectorSpecial('upload')])
    def test_upload_vector_special(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getMatrixContainNone('upload').items()],
                             ids=[i for i in DataUtils.getMatrixContainNone('upload')])
    def test_upload_matrix_contain_none(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}")[0])

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getMatrixSpecial('upload').items()],
                             ids=[i for i in DataUtils.getMatrixSpecial('upload')])
    def test_upload_matrix_special(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getSetContainNone('upload').items()],
                             ids=[i for i in DataUtils.getSetContainNone('upload')])
    def test_upload_set_contain_none(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getSetSpecial('upload').items()],
                             ids=[i for i in DataUtils.getSetSpecial('upload')])
    def test_upload_set_special(self, data):
        for k, v in data.items():
            if k != 'set_empty':
                self.__class__.conn.upload({k: v['value']})
                assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))
            else:
                # APY-876
                with pytest.raises(RuntimeError, match="Cannot create a Set containing only null elements"):
                    self.__class__.conn.upload({k: v['value']})

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getDictContainNone('upload').items()],
                             ids=[i for i in DataUtils.getDictContainNone('upload')])
    def test_upload_dict_contain_none(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            if 'decimal' in k:
                assertPlus(self.__class__.conn.run(f"string({k}[`a])==string({v['expect_value']})"))
            else:
                assertPlus(self.__class__.conn.run(f"{k}[`a]=={v['expect_value']}"))
            assertPlus(self.__class__.conn.run(f"isNull({k}[`b])"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getDictMix('upload').items()],
                             ids=[i for i in DataUtils.getDictMix('upload')])
    def test_upload_dict_mix(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            self.__class__.conn.run(f"expect={v['expect_value']}")
            for _k in v['value']:
                assertPlus(self.__class__.conn.run(f"{k}['{_k}']==expect['{_k}']"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getDictKey('upload').items()],
                             ids=[i for i in DataUtils.getDictKey('upload')])
    def test_upload_dict_key(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}[{v['expect_value']}]==`a"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getDictKeyContainNone('upload').items()],
                             ids=[i for i in DataUtils.getDictKeyContainNone('upload')])
    def test_upload_dict_key_contain_none(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            print(self.__class__.conn.run(k))
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}[{v['expect_value']}]==`a"))

    # assertPlus(self.__class__.conn.run(f"{k}[NULL]==`b"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getDictSpecial('upload').items()],
                             ids=[i for i in DataUtils.getDictSpecial('upload')])
    def test_upload_dict_special(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            if k != 'dictSpecial_composite':
                assertPlus(self.__class__.conn.run(f"{k}[`1]=={v['expect_value']}"))
            else:
                self.__class__.conn.run(f"expect={v['expect_value']}")
                for i in ('1', '2', '3', '4', '5'):
                    assertPlus(self.__class__.conn.run(f"{k}[`{i}]==expect[`{i}]"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableContainNone('upload').items()],
                             ids=[i for i in DataUtils.getTableContainNone('upload')])
    def test_upload_table_contain_none(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
            if 'decimal' in k:
                assertPlus(self.__class__.conn.run(f"string({k}[`a])==string({v['expect_value']})"))
            else:
                assertPlus(self.__class__.conn.run(f"{k}[`a]=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableMix('upload').items()],
                             ids=[i for i in DataUtils.getTableMix('upload')])
    def test_upload_table_mix(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableSpecial('upload').items()],
                             ids=[i for i in DataUtils.getTableSpecial('upload')])
    def test_upload_table_special(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}[`a]=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableExtensionDtype('upload').items()],
                             ids=[i for i in DataUtils.getTableExtensionDtype('upload')])
    def test_upload_table_extension_dtype(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    def test_upload_table_extension_dtype_not_support(self):
        with pytest.raises(RuntimeError,
                           match="Cannot convert from pandas extension dtype UInt8"):
            self.__class__.conn.upload({
                'extension_dtype_not_support': pd.DataFrame({'a': [1, 2, 3]}, dtype=pd.UInt8Dtype())
            })

    def test_upload_table_extension_dtype_ddb_not_support(self):
        with pytest.raises(RuntimeError, match="Cannot convert pandas dtype boolean to BLOB"):
            df = pd.DataFrame({'a': [True, False]}, dtype=pd.BooleanDtype())
            df.__DolphinDB_Type__ = {
                'a': keys.DT_BLOB
            }
            self.__class__.conn.upload({
                'extension_dtype_not_support': df
            })

    def test_upload_table_dtype_not_support(self):
        with pytest.raises(RuntimeError):
            self.__class__.conn.upload({
                'extension_dtype_not_support': pd.DataFrame({'a': [1, 2, 3]}, dtype="uint8")
            })

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getArrayVectorTable('upload').items()],
                             ids=[i for i in DataUtils.getArrayVectorTable('upload')])
    def test_upload_array_vector_table(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
            if 'decimal' in k:
                assertPlus(self.__class__.conn.run(f"string({k})==string({v['expect_value']})"))
            else:
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getArrayVectorTableContainNone('upload').items()],
                             ids=[i for i in DataUtils.getArrayVectorTableContainNone('upload')])
    def test_upload_array_vector_table_contain_none(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
            if 'decimal' in k:
                assertPlus(self.__class__.conn.run(f"string({k})==string({v['expect_value']})"))
            else:
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getArrayVectorTableMix('upload').items()],
                             ids=[i for i in DataUtils.getArrayVectorTableMix('upload')])
    def test_upload_array_vector_table_mix(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            if 'expect_typestr' in v:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getArrayVectorTableSpecial('upload').items()],
                             ids=[i for i in DataUtils.getArrayVectorTableSpecial('upload')])
    def test_upload_array_vector_table_special(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            self.__class__.conn.run('print ' + f"typestr({k}[`a])")
            assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"rows({k}[`a])==0"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableSetTypeBOOL('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeBOOL('upload')])
    def test_upload_table_set_type_BOOL(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableSetTypeCHAR('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeCHAR('upload')])
    def test_upload_table_set_type_CHAR(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeSHORT('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeSHORT('upload')])
    def test_upload_table_set_type_SHORT(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeINT('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeINT('upload')])
    def test_upload_table_set_type_INT(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeLONG('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeLONG('upload')])
    def test_upload_table_set_type_LONG(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDATE('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDATE('upload')])
    def test_upload_table_set_type_DATE(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeMONTH('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeMONTH('upload')])
    def test_upload_table_set_type_MONTH(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeTIME('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeTIME('upload')])
    def test_upload_table_set_type_TIME(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeMINUTE('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeMINUTE('upload')])
    def test_upload_table_set_type_MINUTE(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeSECOND('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeSECOND('upload')])
    def test_upload_table_set_type_SECOND(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDATETIME('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDATETIME('upload')])
    def test_upload_table_set_type_DATETIME(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeTIMESTAMP('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeTIMESTAMP('upload')])
    def test_upload_table_set_type_TIMESTAMP(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeNANOTIME('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeNANOTIME('upload')])
    def test_upload_table_set_type_NANOTIME(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeNANOTIMESTAMP('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeNANOTIMESTAMP('upload')])
    def test_upload_table_set_type_NANOTIMESTAMP(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeFLOAT('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeFLOAT('upload')])
    def test_upload_table_set_type_FLOAT(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDOUBLE('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDOUBLE('upload')])
    def test_upload_table_set_type_DOUBLE(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            self.__class__.conn.run(f'share {k} as `ddd')
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeSYMBOL('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeSYMBOL('upload')])
    def test_upload_table_set_type_SYMBOL(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeSTRING('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeSTRING('upload')])
    def test_upload_table_set_type_STRING(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeUUID('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeUUID('upload')])
    def test_upload_table_set_type_UUID(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDATEHOUR('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDATEHOUR('upload')])
    def test_upload_table_set_type_DATEHOUR(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeIPADDR('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeIPADDR('upload')])
    def test_upload_table_set_type_IPADDR(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeINT128('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeINT128('upload')])
    def test_upload_table_set_type_INT128(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeBLOB('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeBLOB('upload')])
    def test_upload_table_set_type_BLOB(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL32('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL32('upload')])
    def test_upload_table_set_type_DECIMAL32(self, data):
        for k, v in data.items():
            pd.set_option('display.max_columns', None)
            pd.set_option('display.max_rows', None)
            print(k, v['value'])

            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL32Scale('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL32Scale('upload')])
    def test_upload_table_set_type_DECIMAL32_scale(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL64('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL64('upload')])
    def test_upload_table_set_type_DECIMAL64(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL64Scale('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL64Scale('upload')])
    def test_upload_table_set_type_DECIMAL64_scale(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            self.__class__.conn.run('print ' + k)
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL128('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL128('upload')])
    def test_upload_table_set_type_DECIMAL128(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL128Scale('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL128Scale('upload')])
    def test_upload_table_set_type_DECIMAL128_scale(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeBOOLARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeBOOLARRAY('upload')])
    def test_upload_table_set_type_BOOL_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeCHARARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeCHARARRAY('upload')])
    def test_upload_table_set_type_CHAR_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeSHORTARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeSHORTARRAY('upload')])
    def test_upload_table_set_type_SHORT_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeINTARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeINTARRAY('upload')])
    def test_upload_table_set_type_INT_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeLONGARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeLONGARRAY('upload')])
    def test_upload_table_set_type_LONG_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDATEARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDATEARRAY('upload')])
    def test_upload_table_set_type_DATE_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeMONTHARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeMONTHARRAY('upload')])
    def test_upload_table_set_type_MONTH_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeTIMEARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeTIMEARRAY('upload')])
    def test_upload_table_set_type_TIME_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeMINUTEARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeMINUTEARRAY('upload')])
    def test_upload_table_set_type_MINUTE_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeSECONDARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeSECONDARRAY('upload')])
    def test_upload_table_set_type_SECOND_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDATETIMEARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDATETIMEARRAY('upload')])
    def test_upload_table_set_type_DATETIME_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeTIMESTAMPARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeTIMESTAMPARRAY('upload')])
    def test_upload_table_set_type_TIMESTAMP_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeNANOTIMEARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeNANOTIMEARRAY('upload')])
    def test_upload_table_set_type_NANOTIME_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeNANOTIMESTAMPARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeNANOTIMESTAMPARRAY('upload')])
    def test_upload_table_set_type_NANOTIMESTAMP_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeFLOATARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeFLOATARRAY('upload')])
    def test_upload_table_set_type_FLOAT_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDOUBLEARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDOUBLEARRAY('upload')])
    def test_upload_table_set_type_DOUBLE_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeUUIDARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeUUIDARRAY('upload')])
    def test_upload_table_set_type_UUID_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDATEHOURARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDATEHOURARRAY('upload')])
    def test_upload_table_set_type_DATEHOUR_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assert self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}")

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeIPADDRARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeIPADDRARRAY('upload')])
    def test_upload_table_set_type_IPADDR_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeINT128ARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeINT128ARRAY('upload')])
    def test_upload_table_set_type_INT128_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL32ARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL32ARRAY('upload')])
    def test_upload_table_set_type_DECIMAL32_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            self.__class__.conn.run('print ' + k)
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL32ARRAYScale('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL32ARRAYScale('upload')])
    def test_upload_table_set_type_DECIMAL32_ARRAY_scale(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL64ARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL64ARRAY('upload')])
    def test_upload_table_set_type_DECIMAL64_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL64ARRAYScale('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL64ARRAYScale('upload')])
    def test_upload_table_set_type_DECIMAL64_ARRAY_scale(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL128ARRAY('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL128ARRAY('upload')])
    def test_upload_table_set_type_DECIMAL128_ARRAY(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    @pytest.mark.parametrize('data',
                             [{k: v} for k, v in DataUtils.getTableSetTypeDECIMAL128ARRAYScale('upload').items()],
                             ids=[i for i in DataUtils.getTableSetTypeDECIMAL128ARRAYScale('upload')])
    def test_upload_table_set_type_DECIMAL128_ARRAY_scale(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            for i in v['value'].columns:
                assertPlus(self.__class__.conn.run(f"typestr({k}[`{i}])=={v['expect_typestr']}"))

    if PANDAS_VERSION >= (2, 0, 0) and find_spec("pyarrow") is not None:

        @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                          b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                                 ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
        def test_upload_nan_64_table_arrow(self, data):
            data_ = np.frombuffer(data, dtype=np.float64)[0]
            df = pd.DataFrame({'a': [data_]}, dtype=pd.ArrowDtype(pa.float64()))
            self.__class__.conn.upload({'a': df})
            assert self.__class__.conn.run('isNanInf(a["a"][0])==false')
            assert self.__class__.conn.run('isNull(a["a"][0])')
            assert self.__class__.conn.run('typestr a["a"]') == 'FAST DOUBLE VECTOR'

        @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                          b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                                 ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
        def test_upload_nan_64_table_arrow_array_vector(self, data):
            data_ = np.frombuffer(data, dtype=np.float64)[0]
            df = pd.DataFrame({'a': [[data_]]}, dtype=pd.ArrowDtype(pa.list_(pa.float64())))
            self.__class__.conn.upload({'a': df})
            assert self.__class__.conn.run('isNanInf(a["a"][0][0])==false')
            assert self.__class__.conn.run('isNull(a["a"][0][0])')
            assert self.__class__.conn.run('typestr a["a"]') == 'FAST DOUBLE[] VECTOR'

        @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                          b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                                 ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
        def test_upload_nan_64_table_arrow_setType_FLOAT(self, data):
            data_ = np.frombuffer(data, dtype=np.float64)[0]
            df = pd.DataFrame({'a': [data_]}, dtype=pd.ArrowDtype(pa.float64()))
            df.__DolphinDB_Type__ = {
                'a': keys.DT_FLOAT
            }
            self.__class__.conn.upload({'a': df})
            assert self.__class__.conn.run('isNull(a["a"][0])')
            assert self.__class__.conn.run('typestr a["a"]') == 'FAST FLOAT VECTOR'

        @pytest.mark.parametrize('data', [b'\x00\x00\x00\x00\x00\x00\xf8\xff', b'\x00\x00\x00\x00\x00\x00\xf8\x7f',
                                          b'\xff\xff\xff\xff\xff\xff\xff\x7f', b'\xff\xff\xff\xff\xff\xff\xff\xff'],
                                 ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
        def test_upload_nan_64_table_arrow_array_vector_setType_FLOAT(self, data):
            data_ = np.frombuffer(data, dtype=np.float64)[0]
            df = pd.DataFrame({'a': [[data_]]}, dtype=pd.ArrowDtype(pa.list_(pa.float64())))
            df.__DolphinDB_Type__ = {
                'a': keys.DT_FLOAT_ARRAY
            }
            self.__class__.conn.upload({'a': df})
            assert self.__class__.conn.run('isNull(a["a"][0][0])')
            assert self.__class__.conn.run('typestr a["a"]') == 'FAST FLOAT[] VECTOR'

        @pytest.mark.skip
        @pytest.mark.parametrize('data',
                                 [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                                 ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
        def test_upload_nan_32_table_arrow(self, data):
            data_ = np.frombuffer(data, dtype=np.float32)[0]
            df = pd.DataFrame({'a': [data_]}, dtype=pd.ArrowDtype(pa.float32()))
            self.__class__.conn.upload({'a': df})
            assert self.__class__.conn.run('isNull(a["a"][0])')
            assert self.__class__.conn.run('typestr a["a"]') == 'FAST FLOAT VECTOR'

        @pytest.mark.skip
        @pytest.mark.parametrize('data',
                                 [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                                 ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
        def test_upload_nan_32_table_arrow_array_vector(self, data):
            data_ = np.frombuffer(data, dtype=np.float32)[0]
            df = pd.DataFrame({'a': [[data_]]}, dtype=pd.ArrowDtype(pa.list_(pa.float32())))
            self.__class__.conn.upload({'a': df})
            assert self.__class__.conn.run('isNull(a["a"][0][0])')
            assert self.__class__.conn.run('typestr a["a"]') == 'FAST FLOAT VECTOR'

        @pytest.mark.parametrize('data',
                                 [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                                 ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
        def test_upload_nan_32_table_arrow_setType_DOUBLE(self, data):
            data_ = np.frombuffer(data, dtype=np.float32)[0]
            df = pd.DataFrame({'a': [data_]}, dtype=pd.ArrowDtype(pa.float32()))
            df.__DolphinDB_Type__ = {
                'a': keys.DT_DOUBLE
            }
            self.__class__.conn.upload({'a': df})
            assert self.__class__.conn.run('isNanInf(a["a"][0])==false')
            assert self.__class__.conn.run('isNull(a["a"][0])')
            assert self.__class__.conn.run('typestr a["a"]') == 'FAST DOUBLE VECTOR'

        @pytest.mark.parametrize('data',
                                 [b'\x00\x00\xf8\xff', b'\x00\x00\xf8\x7f', b'\xff\xff\xff\x7f', b'\xff\xff\xff\xff'],
                                 ids=['+nan', '-nan', '+all_1_nan', '-all_1_nan'])
        def test_upload_nan_32_table_arrow_array_vector_setType_DOUBLE(self, data):
            data_ = np.frombuffer(data, dtype=np.float32)[0]
            df = pd.DataFrame({'a': [[data_]]}, dtype=pd.ArrowDtype(pa.list_(pa.float32())))
            df.__DolphinDB_Type__ = {
                'a': keys.DT_DOUBLE_ARRAY
            }
            self.__class__.conn.upload({'a': df})
            assert self.__class__.conn.run('isNanInf(a["a"][0][0])==false')
            assert self.__class__.conn.run('isNull(a["a"][0][0])')
            assert self.__class__.conn.run('typestr a["a"]') == 'FAST DOUBLE[] VECTOR'

        def test_upload_string_table_arrow_setType_SYMBOL(self):
            df = pd.DataFrame({'a': ['a', 'b', 'c', 'd']}, dtype=pd.ArrowDtype(pa.utf8()))
            df.__DolphinDB_Type__ = {
                'a': keys.DT_SYMBOL
            }
            self.__class__.conn.upload({'a': df})
            assert self.__class__.conn.run('typestr a["a"]') == 'FAST SYMBOL VECTOR'
            assert self.__class__.conn.run('eqObj(a[`a],symbol(`a`b`c`d))')

        @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableArrow('upload').items()],
                                 ids=[i for i in DataUtils.getTableArrow('upload')])
        def test_upload_table_arrow(self, data):
            for k, v in data.items():
                self.__class__.conn.upload({k: v['value']})
                assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

        @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableArrowContainNone('upload').items()],
                                 ids=[i for i in DataUtils.getTableArrowContainNone('upload')])
        def test_upload_table_arrow_contain_none(self, data):
            for k, v in data.items():
                self.__class__.conn.upload({k: v['value']})
                assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

        @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableArrowSpecial('upload').items()],
                                 ids=[i for i in DataUtils.getTableArrowSpecial('upload')])
        def test_upload_table_arrow_special(self, data):
            for k, v in data.items():
                self.__class__.conn.upload({k: v['value']})
                if 'expect_typestr' in v:
                    assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
                if 'expect_value' in v:
                    assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

        def test_upload_table_arrow_array_vector(self):
            for data in [{k: v} for k, v in DataUtils.getTableArrowArrayVector('upload').items()]:
                for k, v in data.items():
                    self.__class__.conn.upload({k: v['value']})
                    assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
                    assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

        def test_upload_table_arrow_array_vector_contain_none(self):
            for data in [{k: v} for k, v in DataUtils.getTableArrowArrayVectorContainNone('upload').items()]:
                for k, v in data.items():
                    self.__class__.conn.upload({k: v['value']})
                    assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
                    assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

        def test_upload_table_arrow_array_vector_contain_empty(self):
            for data in [{k: v} for k, v in DataUtils.getTableArrowArrayVectorContainEmpty('upload').items()]:
                for k, v in data.items():
                    self.__class__.conn.upload({k: v['value']})
                    if 'expect_typestr' in v:
                        assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
                    if 'expect_value' in v:
                        assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))
