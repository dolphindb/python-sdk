import pytest
from basic_testing.utils import equalPlus
from setup.prepare import *
from setup.settings import *
from basic_testing.prepare import DataUtils
from setup.utils import get_pid

class TestDownload(object):

    @classmethod
    def setup_class(cls):
        cls.conn = ddb.Session(HOST, PORT, USER, PASSWD,enablePickle=False)
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

    @pytest.mark.parametrize('data',DataUtils.getVectorContainNone('download').values(),ids=[i for i in DataUtils.getVectorContainNone('download')])
    def test_download_vector_contain_none(self,data):
        x=self.__class__.conn.run(data['value'])
        assert equalPlus(x,data['expect'])

    @pytest.mark.parametrize('data',DataUtils.getVectorSpecial('download').values(),ids=[i for i in DataUtils.getVectorSpecial('download')])
    def test_download_vector_special(self,data):
        x=self.__class__.conn.run(data['value'])
        assert equalPlus(x,data['expect'])

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

