import numpy as np
import pytest
import dolphindb as ddb
from pandas2_testing.utils import assertPlus
from setup.settings import *
from pandas2_testing.prepare import DataUtils
from setup.utils import get_pid
import pandas as pd

PANDAS_VERSION=tuple(int(i) for i in pd.__version__.split('.'))
# todo:pickle,compress
class TestUpload(object):

    @classmethod
    def setup_class(cls):
        cls.conn = ddb.Session(HOST, PORT, USER, PASSWD)
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() +'\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def setup_method(self):
        try:
            self.__class__.conn.run("1")
        except:
            self.__class__.conn.connect(HOST, PORT, USER, PASSWD)

    # def teardown_method(self):
    #     self.__class__.conn.undefAll()
    #     self.__class__.conn.clearAllCache()

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getScalar('upload').items()],
                             ids=[i for i in DataUtils.getScalar('upload')])
    def test_upload_scalar(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
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

    @pytest.mark.parametrize('order',['K','A','C','F'],ids=['ORDER_K','ORDER_A','ORDER_C','ORDER_F'])
    def test_upload_vector_np_order(self,order):
        data=np.array([[1,2,3],[4,5,6]],dtype='int64',order=order)
        self.__class__.conn.upload({'order_'+order:data[0]})
        assertPlus(self.__class__.conn.run(f"typestr(order_{order})=='FAST LONG VECTOR'"))
        assertPlus(self.__class__.conn.run(f"order_{order}==[1,2,3]"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getMatrix('upload').items()],
                             ids=[i for i in DataUtils.getMatrix('upload')])
    def test_upload_matrix(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}")[0])

    @pytest.mark.parametrize('order',['K','A','C','F'],ids=['ORDER_K','ORDER_A','ORDER_C','ORDER_F'])
    def test_upload_matrix_np_order(self,order):
        data=np.array([[1,2,3],[4,5,6],[7,8,9]],dtype='int64',order=order)
        self.__class__.conn.upload({'order_'+order:data[0:2]})
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

    @pytest.mark.parametrize('dtype',['object','int64','empty'],ids=['DTYPE_OBJECT','DTYPE_INT64','DTYPE_EMPTY'])
    @pytest.mark.parametrize('order',['K','A','C','F'],ids=['ORDER_K','ORDER_A','ORDER_C','ORDER_F'])
    def test_upload_table_np_order(self,order,dtype):
        if dtype!='empty':
            data=pd.DataFrame(np.array([[1,2],[3,4],[5,6]],dtype=dtype,order=order),columns=['a','b'])
        else:
            data = pd.DataFrame(np.array([[1, 2], [3, 4], [5, 6]], order=order), columns=['a', 'b'])
        self.__class__.conn.upload({'order_'+order:data})
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
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            assertPlus(self.__class__.conn.run(f"{k}[{v['expect_value']}]==`a"))
            # assertPlus(self.__class__.conn.run(f"{k}[NULL]==`b"))

    @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getDictSpecial('upload').items()],
                             ids=[i for i in DataUtils.getDictSpecial('upload')])
    def test_upload_dict_special(self, data):
        for k, v in data.items():
            self.__class__.conn.upload({k: v['value']})
            assertPlus(self.__class__.conn.run(f"typestr({k})=={v['expect_typestr']}"))
            if k!='dictSpecial_composite':
                assertPlus(self.__class__.conn.run(f"{k}[`1]=={v['expect_value']}"))
            else:
                self.__class__.conn.run(f"expect={v['expect_value']}")
                for i in ('1','2','3','4','5'):
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
            self.__class__.conn.run(f'share {k} as `ttINT')
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
            self.__class__.conn.run(f'share {k} as `ttMoth')
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

    if PANDAS_VERSION>=(2,0,0):
        @pytest.mark.PANDAS2
        @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableArrow('upload').items()],
                                 ids=[i for i in DataUtils.getTableArrow('upload')])
        def test_upload_table_arrow(self, data):
            for k, v in data.items():
                self.__class__.conn.upload({k: v['value']})
                assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

        @pytest.mark.PANDAS2
        @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableArrowContainNone('upload').items()],
                                 ids=[i for i in DataUtils.getTableArrowContainNone('upload')])
        def test_upload_table_arrow_contain_none(self, data):
            for k, v in data.items():
                self.__class__.conn.upload({k: v['value']})
                assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
                assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))

        @pytest.mark.PANDAS2
        @pytest.mark.parametrize('data', [{k: v} for k, v in DataUtils.getTableArrowSpecial('upload').items()],
                                 ids=[i for i in DataUtils.getTableArrowSpecial('upload')])
        def test_upload_table_arrow_special(self, data):
            for k, v in data.items():
                if k=='table_arrow_symbol':
                    assert False,'server bug' # todo:server bug
                self.__class__.conn.upload({k: v['value']})
                if 'expect_typestr' in v:
                    assertPlus(self.__class__.conn.run(f"typestr({k}[`a])=={v['expect_typestr']}"))
                if 'expect_value' in v:
                    assertPlus(self.__class__.conn.run(f"{k}=={v['expect_value']}"))
