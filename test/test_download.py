import pytest
from setup.prepare import *
from setup.settings import *
import dolphindb as ddb
from numpy.testing import *
from pandas.testing import *
from setup.utils import get_pid


class TestDownload:
    conn = ddb.session()

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
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Scalar(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Scalar(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD,enablePickle=pickle, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            res = conn.run(s)
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p is not None:
                assert_almost_equal(res, p)
            else:
                assert res == p
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Vector(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Vector(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD,enablePickle=pickle, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            res = conn.run(s)
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p is not None:
                assert_array_almost_equal(res, p)
            else:
                assert_array_equal(res, p)
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Matrix(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Matrix(types=data_type, names="download", r=10, c=10)
        conn = ddb.session(HOST, PORT, USER, PASSWD,enablePickle=pickle, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            res = conn.run(s)
            for _i in [1, 2]:
                assert_array_equal(res[_i], p[_i])
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p[0] is not None:
                assert_array_almost_equal(res[0], p[0])
            else:
                assert_array_equal(res[0], p[0])
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Set(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Set(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD, enablePickle=pickle, compress=compress)
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
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Dictionary(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Dictionary(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD,enablePickle=pickle, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p is not None:
                assert_almost_equal(conn.run(s), p)
            else:
                assert_equal(conn.run(s), p)
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Pair(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_Pair(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD,enablePickle=pickle, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            res = conn.run(s)
            assert res == p
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('isShare', [False], ids=['unshare'])
    @pytest.mark.parametrize('tabletype', ['table'], ids=['table'])
    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    def test_download_Table(self, tabletype, data_type, pickle, compress, isShare):
        tmp_s, tmp_p = get_Table(typeTable=tabletype, types=data_type, ishare=isShare, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD,enablePickle=pickle, compress=compress)
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
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    def test_download_ArrayVector(self, data_type, pickle, compress):
        tmp_s, tmp_p = get_ArrayVector(types=data_type, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD,enablePickle=pickle, compress=compress)
        conn.run(tmp_s)
        for s, p in tmp_p:
            if data_type in [DATATYPE.DT_FLOAT, DATATYPE.DT_DOUBLE] and p is not None:
                for item1, item2 in zip(conn.run(s), p):
                    assert_array_equal(item1, item2)
            else:
                for item1, item2 in zip(conn.run(s), p):
                    assert_equal(item1, item2)
        conn.undefAll()
        conn.close()

    @pytest.mark.parametrize('data_type', DATATYPE, ids=[x.name for x in DATATYPE])
    @pytest.mark.parametrize('pickle', [False, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('compress', [False, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('typeTable', ["table", "streamTable"], ids=["table", "streamTable"])
    @pytest.mark.parametrize('isShare', [False, True], ids=["EnShare", "UnShare"])
    def test_download_tableArrayVector(self, data_type, pickle, compress, typeTable, isShare):
        tmp_s, tmp_p = get_Table_arrayVetcor(types=data_type, n=100, typeTable=typeTable, isShare=isShare, names="download")
        conn = ddb.session(HOST, PORT, USER, PASSWD,enablePickle=pickle, compress=compress)
        conn.run(tmp_s)
        from basic_testing.utils import equalPlus
        for s, p in tmp_p:
            assert equalPlus(conn.run(s), p)
        conn.undefAll()
        conn.close()


class TestDownloadHugeData:
    # expect string value
    tmp = "abcd中文123"
    ex = ""
    for _ in range(100000):
        ex += tmp
    conn = ddb.session()

    def setup_method(self):
        try:
            self.conn.run("1")
        except RuntimeError:
            self.conn.connect(HOST, PORT, USER, PASSWD)

    def teardown_method(self):
        self.conn.undefAll()
        self.conn.clearAllCache()

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

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', ["symbol", "string", "blob"], ids=["symbol", "string", "blob"])
    def test_download_Scalar_with_hugedata(self, data_type, pickle, compress):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD,compress=compress, enablePickle=pickle)
        if data_type != "symbol":
            val = conn1.run(data_type + "(concat(take(`abcd中文123,100000)))")
        else:
            val = conn1.run("symbol([(concat(take(`abcd中文123,100000)))])[0]")
        assert self.ex == val
        conn1.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', ["symbol", "string", "blob", "any"], ids=["symbol", "string", "blob", "any"])
    def test_download_Vector_with_hugedata(self, data_type, pickle, compress):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD,compress=compress, enablePickle=pickle)
        if data_type in ['string', 'symbol']:
            try:
                conn1.run(data_type + "([concat(take(`abcd中文123,100000))])")
            except Exception as err:
                assert 'IO error type 4' in str(err)
        elif data_type == 'any':
            vals = conn1.run("[rand(100.00,1)[0],rand(100.00,1)[0],rand(100.00,1)[0],rand(100,1)[0],date(rand(10000,1))[0], blob(concat(take(`abcd中文123,100000)))]")
            assert list(vals)[-1] == self.ex
        else:
            vals = conn1.run(data_type + "([concat(take(`abcd中文123,100000))])")
            assert len(vals) == 1
            for val in list(vals):
                assert val == self.ex
        conn1.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', ["symbol", "string", "blob"], ids=["symbol", "string", "blob"])
    def test_download_Table_with_hugedata(self, data_type, pickle, compress):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD,compress=compress, enablePickle=pickle)
        if data_type in ['string', 'symbol']:
            try:
                conn1.run("table(" + data_type + "([concat(take(`abcd中文123,100000))]) as col1)")
            except Exception as err:
                assert 'IO error type 4' in str(err)
        elif not pickle:
            tab = conn1.run("table(" + data_type + "([concat(take(`abcd中文123,100000))]) as col1)")
            assert tab.size == 1
            assert tab['col1'].size == 1
            assert tab['col1'][0] == self.ex
        conn1.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', ["symbol", "string", "blob"], ids=["symbol", "string", "blob"])
    def test_download_Pair_with_hugedata(self, data_type, pickle, compress):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD,compress=compress, enablePickle=pickle)
        if data_type in ['string', 'symbol']:
            try:
                conn1.run(f"{data_type}([concat(take(`abcd中文123,100000))])[0]:{data_type}([concat(take(`abcd中文123,100000))])[0]")
            except Exception as err:
                assert 'IO error type 4' in str(err)
        else:
            matx = conn1.run(f"{data_type}(concat(take(`abcd中文123,100000))):{data_type}(concat(take(`abcd中文123,100000)))")
            assert len(matx) == 2
            for val in list(matx):
                assert val == self.ex
        conn1.close()

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', ["symbol", "string", "blob"], ids=["symbol", "string", "blob"])
    def test_download_Dict_with_hugedata(self, data_type, pickle, compress):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD,compress=compress, enablePickle=pickle)
        if data_type in ['string', 'symbol']:
            try:
                conn1.run(f"dict([1],{data_type}([concat(take(`abcd中文123,100000))]))")
            except Exception as err:
                assert 'IO error type 4' in str(err)
        else:
            conn1.run(f"dict([1],{data_type}([concat(take(`abcd中文123,100000))]))")

    @pytest.mark.parametrize('compress', [True, False], ids=["EnCompress", "UnCompress"])
    @pytest.mark.parametrize('pickle', [True, False], ids=["EnPickle", "UnPickle"])
    @pytest.mark.parametrize('data_type', ["symbol", "string", "blob"], ids=["symbol", "string", "blob"])
    def test_download_Set_with_hugedata(self, data_type, pickle, compress):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD,compress=compress, enablePickle=pickle)
        if data_type in ['string', 'symbol']:
            try:
                conn1.run(f"set({data_type}([concat(take(`abcd中文123,100000))]))")
            except Exception as err:
                assert 'IO error type 4' in str(err)
        else:
            conn1.run(f"set({data_type}([concat(take(`abcd中文123,100000))]))")
