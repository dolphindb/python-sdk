import dolphindb as ddb
import pytest
from numpy.testing import assert_almost_equal, assert_array_almost_equal, assert_array_equal, assert_equal
from pandas._testing import assert_frame_equal

from basic_testing.utils import equalPlus
from setup.prepare import DATATYPE, get_Scalar, get_ArrayVector, get_Dictionary, get_Matrix, get_Pair, get_Set, \
    get_Table, get_Table_arrayVetcor, get_Vector
from setup.settings import HOST, PORT, USER, PASSWD


class TestDownload:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Scalar(self, data_type, pickle):
        tmp_s, tmp_p = get_Scalar(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle)
        conn.run(tmp_s)
        for s, p in tmp_p:
            res = conn.run(s)
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p is not None:
                assert_almost_equal(res, p)
            else:
                assert res == p
        conn.close()

    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Vector(self, data_type, pickle):
        tmp_s, tmp_p = get_Vector(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle)
        conn.run(tmp_s)
        for s, p in tmp_p:
            res = conn.run(s)
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p is not None:
                assert_array_almost_equal(res, p)
            else:
                assert_array_equal(res, p)
        conn.close()

    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Matrix(self, data_type, pickle):
        tmp_s, tmp_p = get_Matrix(types=data_type, names="download", r=10, c=10)
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle)
        conn.run(tmp_s)
        for s, p in tmp_p:
            res = conn.run(s)
            for _i in [1, 2]:
                assert_array_equal(res[_i], p[_i])
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p[0] is not None:
                assert_array_almost_equal(res[0], p[0])
            else:
                assert_array_equal(res[0], p[0])
        conn.close()

    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Set(self, data_type, pickle):
        tmp_s, tmp_p = get_Set(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle)
        conn.run(tmp_s)
        for s, p in tmp_p:
            res = conn.run(s)
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p is not None:
                assert len(res) == len(p)
                for _i in res:
                    if _i is None:
                        assert _i in p
                    else:
                        assert round(_i, 2) in p
            else:
                assert_array_equal(res, p)
        conn.close()

    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Dictionary(self, data_type, pickle):
        tmp_s, tmp_p = get_Dictionary(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle)
        conn.run(tmp_s)
        for s, p in tmp_p:
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p is not None:
                assert_almost_equal(conn.run(s), p)
            else:
                assert_equal(conn.run(s), p)
        conn.close()

    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Pair(self, data_type, pickle):
        tmp_s, tmp_p = get_Pair(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle)
        conn.run(tmp_s)
        for s, p in tmp_p:
            res = conn.run(s)
            assert res == p
        conn.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('isShare', [False], ids=['unshare'])
    @pytest.mark.parametrize('table_type', ['table'], ids=['table'])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Table(self, table_type, data_type, pickle, compress, isShare):
        tmp_s, tmp_p = get_Table(typeTable=table_type, types=data_type, ishare=isShare, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            if pickle and data_type == DATATYPE.DT_BLOB:
                with pytest.raises(RuntimeError, match="not support BLOB"):
                    conn.run(s)
            else:
                res = conn.run(s)
                if data_type == DATATYPE.DT_MONTH and not pickle:
                    p['month_0'] = p['month_0'].astype('datetime64[ns]')
                    p['month_1'] = p['month_1'].astype('datetime64[ns]')
                    p['month_2'] = p['month_2'].astype('datetime64[ns]')
                assert_frame_equal(res, p)
        conn.close()

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    def test_download_ArrayVector(self, data_type, pickle):
        tmp_s, tmp_p = get_ArrayVector(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle)
        conn.run(tmp_s)
        for s, p in tmp_p:
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p is not None:
                for item1, item2 in zip(conn.run(s), p):
                    assert_array_equal(item1, item2)
            else:
                for item1, item2 in zip(conn.run(s), p):
                    assert_equal(item1, item2)
        conn.close()

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [False, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('typeTable', ["table", "streamTable"], ids=["table", "streamTable"])
    @pytest.mark.parametrize('isShare', [False, True], ids=["EnShare", "UnShare"])
    def test_download_tableArrayVector(self, data_type, pickle, compress, typeTable, isShare):
        tmp_s, tmp_p = get_Table_arrayVetcor(types=data_type, n=100, typeTable=typeTable, isShare=isShare,
                                             names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            assert equalPlus(conn.run(s), p)
        conn.close()
