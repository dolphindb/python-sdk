#include "config.h"
#include "DdbPythonUtil.h"

using namespace py::literals;

namespace dolphindb {
inline bool isNULL(const py::object &obj);
inline bool isNULL(const py::object &obj, int &pri, DATA_TYPE &nullType);
void throwExceptionAboutNumpyVersion(int row_index, const string &value, const string &type, const TableVectorInfo &info);
template <typename T>
bool addDecimalVector(const py::array &pyVec, size_t size, size_t offset, T nullValue, Type type,
    std::function<void(T *, int)> f, const TableVectorInfo &info);
void addStringVector(VectorSP &ddbVec, const py::array &pyVec, size_t size, ssize_t offset, const TableVectorInfo &info);
void setBlobVector(VectorSP &ddbVec, const py::array &pyVec, size_t size, ssize_t offset, const TableVectorInfo &info);
inline bool isArrayLike(const py::object &obj);
bool
checkElemType(const py::object              &obj,
                Type                          &type,
                const CHILD_VECTOR_OPTION     &option,
                bool                          &is_ArrayVector,
                bool                          &is_Null,
                int                           &nullpri,
                Type                          &nullType);



}

namespace {

class DdbPythonUtilCoverageTest : public testing::Test {
protected:
    static void SetUpTestCase() {
        DdbPythonUtil::preservedinit();
    }

    static void TearDownTestCase() {
        
    }

};


TEST_F(DdbPythonUtilCoverageTest, test_TableChecker) {
    py::object none = py::none();
    TableChecker checker(none);
    ASSERT_EQ(checker.begin(), checker.end());
    ASSERT_EQ(checker.size(), 0);
}


TEST_F(DdbPythonUtilCoverageTest, test_isNULL1) {
    // 0x000000000000f8ff
    uint64_t data = 9221120237041090561LL;
    double t;
    memcpy(&t, &data, 8);
    py::float_ a(t);
    ASSERT_EQ(std::isnan(t), true);
    ASSERT_EQ(dolphindb::isNULL(a), true);
}


TEST_F(DdbPythonUtilCoverageTest, test_isNULL2) {
    py::object na = DdbPythonUtil::preserved_->pandas_.attr("NA");
    int pri;
    DATA_TYPE nullType;
    ASSERT_EQ(isNULL(na, pri, nullType), true);
    ASSERT_EQ(pri, 2);
    ASSERT_EQ(nullType, DT_DOUBLE);
}


TEST_F(DdbPythonUtilCoverageTest, test_throwException) {
    std::string what = "";
    try {
        TableVectorInfo info{true, ""};
        throwExceptionAboutNumpyVersion(1, "xxx", "yyy", info);
    }
    catch (RuntimeException &e) {
        what = e.what();
    }
    ASSERT_EQ(what.empty(), false);
}


TEST_F(DdbPythonUtilCoverageTest, test_addDecimalVector) {
    py::list data;
    data.append(py::float_(1.1));
    data.append(py::str("xxxx"));
    py::array pyVec = DdbPythonUtil::preserved_->numpy_.attr("array")(data, "dtype"_a="object");
    TableVectorInfo info;
    std::string what = "";
    try {
        addDecimalVector<int>(pyVec, 1, 0, 0, Type{DT_DECIMAL32, 2}, [&](int* buf, int size){}, info);
    }
    catch (RuntimeException &e) {
        what = e.what();
    }
    ASSERT_EQ(what.empty(), false);
}


TEST_F(DdbPythonUtilCoverageTest, test_addStringVector) {
    py::list data;
    data.append(py::str(""));
    data.append(py::str(""));
    data.append(py::str(""));
    data.append(py::str("192.168.1.1"));
    data.append(py::str("192.168.xxxx"));
    py::array pyVec = DdbPythonUtil::preserved_->numpy_.attr("array")(data, "dtype"_a="object");
    TableVectorInfo info;
    std::string what = "";
    VectorSP ddbVec = Util::createVector(DT_IP, 3, 10);
    try {
        addStringVector(ddbVec, pyVec, 2, 3, info);
    }
    catch (RuntimeException &e) {
        what = e.what();
    }
    ASSERT_EQ(what.empty(), false);
    ASSERT_EQ(ddbVec->size(), 4);
}


TEST_F(DdbPythonUtilCoverageTest, test_setBlobVector) {
    py::list data;
    data.append(py::str(""));
    data.append(py::str(""));
    data.append(py::str(""));
    data.append(py::str("aaa"));
    data.append(py::str("bbbbb"));
    py::array pyVec = DdbPythonUtil::preserved_->numpy_.attr("array")(data, "dtype"_a="object");
    TableVectorInfo info;
    std::string what = "";
    VectorSP ddbVec = Util::createVector(DT_BLOB, 5, 10);
    try {
        setBlobVector(ddbVec, pyVec, 2, 3, info);
    }
    catch (RuntimeException &e) {
        what = e.what();
    }
    ASSERT_EQ(what.empty(), true);
    ASSERT_EQ(ddbVec->size(), 5);
}


TEST_F(DdbPythonUtilCoverageTest, test_isArrayLike) {
    py::list tmp1;
    tmp1.append(py::int_(16));
    py::object data1 = DdbPythonUtil::preserved_->numpy_.attr("array")(tmp1);
    ASSERT_EQ(isArrayLike(data1), true);

    py::list data2;
    data2.append(py::int_(16));
    ASSERT_EQ(isArrayLike(data2), true);

    py::tuple data3 = py::cast<py::tuple>(data2);
    ASSERT_EQ(isArrayLike(data3), true);

    py::object data4 = DdbPythonUtil::preserved_->pandas_.attr("Series")(tmp1);
    ASSERT_EQ(isArrayLike(data4), true);

    py::object data5 = DdbPythonUtil::preserved_->pandas_.attr("Index")(tmp1);
    ASSERT_EQ(isArrayLike(data5), true);
}


TEST_F(DdbPythonUtilCoverageTest, test_checkElemType_error_option) {
    CHILD_VECTOR_OPTION option = (CHILD_VECTOR_OPTION)3;
    py::object data = py::list();
    Type type;
    bool is_ArrayVector;
    bool is_Null;
    int nullpri;
    Type nullType;
    std::string what = "";
    try {
        checkElemType(data, type, option, is_ArrayVector, is_Null, nullpri, nullType);
    }
    catch (RuntimeException &e) {
        what = e.what();
    }
    ASSERT_EQ(what.empty(), false);
}


TEST_F(DdbPythonUtilCoverageTest, test_checkElemType_curType_Object) {
    CHILD_VECTOR_OPTION option = (CHILD_VECTOR_OPTION)1;
    py::object data = DdbPythonUtil::preserved_->numpy_.attr("str_")("xxx");
    Type type = {DT_UNK, EXPARAM_DEFAULT};
    bool is_ArrayVector = false;
    bool is_Null = false;
    int nullpri;
    Type nullType;
    std::string what = "";
    try {
        bool re = checkElemType(data, type, option, is_ArrayVector, is_Null, nullpri, nullType);
    }
    catch (RuntimeException &e) {
        what = e.what();
    }
    ASSERT_EQ(what.empty(), true);
    ASSERT_EQ(type.first, DT_ANY);
}





}



