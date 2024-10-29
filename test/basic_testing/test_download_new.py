import pytest

from basic_testing.prepare import DataUtils
from basic_testing.utils import equalPlus
from setup.prepare import *
from setup.settings import *
from setup.utils import get_pid


@pytest.mark.BASIC
class TestDownloadNew(object):
    conn: ddb.Session

    @classmethod
    def setup_class(cls):
        cls.conn = ddb.Session(enablePickle=False)
        cls.conn.connect(HOST, PORT)
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def setup_method(self):
        try:
            self.__class__.conn.run("1")
        except RuntimeError:
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
