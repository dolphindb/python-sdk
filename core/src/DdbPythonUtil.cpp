#include "DdbPythonUtil.h"
#include "Concurrent.h"
#include "ConstantImp.h"
#include "ConstantMarshall.h"
#include "DolphinDB.h"
#include "ScalarImp.h"
#include "Util.h"
#include "Pickle.h"
#include "MultithreadedTableWriter.h"
#include "Wrappers.h"

namespace ddb = dolphindb;
using namespace pybind11::literals;

namespace dolphindb {
#ifdef DLOG
    #undef DLOG
#endif
#define DLOG //DLogger::Info



Type Scalar2DolphinDBType(const py::object &obj, bool &isNull, int &nullpri, CHILD_VECTOR_OPTION option = CHILD_VECTOR_OPTION::DISABLE);
ConstantSP toDolphinDB_Scalar(py::object obj, Type typeIndicator);
ConstantSP toDolphinDB_Scalar(py::object obj);
ConstantSP toDolphinDB_Vector(py::object obj, Type typeIndicator, CHILD_VECTOR_OPTION option = CHILD_VECTOR_OPTION::DISABLE, TableVectorInfo info = {true, ""});
ConstantSP toDolphinDB_Matrix(py::object obj, Type typeIndicator);
ConstantSP toDolphinDB_Set(py::object obj);
ConstantSP toDolphinDB_Dictionary(py::object obj);
ConstantSP toDolphinDB_Table(py::object obj, TableChecker &checker);
ConstantSP toDolphinDB_NDArray(py::array obj, Type typeIndicator, const CHILD_VECTOR_OPTION &option);
ConstantSP toDolphinDB_Vector_SeriesOrIndex(py::object obj, Type typeIndicator, const CHILD_VECTOR_OPTION &options, const TableVectorInfo &info);
template <typename T>
ConstantSP
toDolphinDB_Vector_TupleOrList(T obj,
                         Type typeIndicator,
                         const CHILD_VECTOR_OPTION &option,
                         typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value, T>::type* = nullptr);


static inline std::string py2str(const py::handle &obj) {
    return py::str(obj).cast<std::string>();
}

static inline std::string py2str(const py::object &obj) {
    return py::str(obj).cast<std::string>();
}

inline void getVersionVector(const py::object &module, vector<int> &version) {
    string ver_ = py2str(module.attr("__version__"));
    const char split = '.';
    string verstr = ver_ + split;
    size_t pos;
    while(true) {
        pos = verstr.find(split);
        if(pos == string::npos)
            break;
        string tmpstr = verstr.substr(0, pos);
        version.push_back(atoi(tmpstr.c_str()));
        verstr = verstr.substr(pos + 1);
    }
} 

Preserved::Preserved(){
    vector<int> version;
    numpy_ = py::module::import("numpy");
    getVersionVector(numpy_, version);
    if (version.size() >= 2) {
        np_above_1_20_ = ((version[0] == 1) && (version[1] >= 20)) || (version[0] >= 2);
    }
    else {
        np_above_1_20_ = false;
    }
    
    version.clear();
    pandas_ = py::module::import("pandas");
    getVersionVector(pandas_, version);
    if (version.size() >= 1) {
        pd_above_2_00_ = version[0] >= 2;
    }
    else {
        pd_above_2_00_ = false;
    }

    py::module_ importUtil = py::module::import("importlib.util");
    has_arrow_ = !(importUtil.attr("find_spec")("pyarrow").is_none());
    pyarrow_import_ = false;

    if (has_arrow_) {
        pyarrow_ = py::module::import("pyarrow");
        pyarrow_import_ = true;
        paboolean_ = pyarrow_.attr("bool_")();
        paint8_ = pyarrow_.attr("int8")();
        paint16_ = pyarrow_.attr("int16")();
        paint32_ = pyarrow_.attr("int32")();
        paint64_ = pyarrow_.attr("int64")();
        pafloat32_ = pyarrow_.attr("float32")();
        pafloat64_ = pyarrow_.attr("float64")();
        padate32_ = pyarrow_.attr("date32")();
        patime32_s_ = pyarrow_.attr("time32")("s");
        patime32_ms_ = pyarrow_.attr("time32")("ms");
        patime64_ns_ = pyarrow_.attr("time64")("ns");
        patime64_ns_ = pyarrow_.attr("time64")("ns");
        patimestamp_s_ = pyarrow_.attr("timestamp")("s");
        patimestamp_ms_ = pyarrow_.attr("timestamp")("ms");
        patimestamp_ns_ = pyarrow_.attr("timestamp")("ns");
        pautf8_ = pyarrow_.attr("utf8")();
        padictionary_int32_utf8_ = pyarrow_.attr("dictionary")(paint32_, pautf8_);
        pafixed_size_binary_16_ = pyarrow_.attr("binary")(16);
        palarge_binary_ = pyarrow_.attr("large_binary")();
        padecimal128_ = pyarrow_.attr("Decimal128Type");
        palist_ = pyarrow_.attr("ListType");
        pachunkedarray_ = pyarrow_.attr("ChunkedArray");
        if (pd_above_2_00_) {
            pdarrowdtype_ = pandas_.attr("ArrowDtype");
        }
    }

    datetime64_ = numpy_.attr("datetime64");
    nan_ = getType(numpy_.attr("nan"));
    pddataframe_ = getType(pandas_.attr("DataFrame")());
    pdextensiondtype_ = pandas_.attr("core").attr("dtypes").attr("dtypes").attr("ExtensionDtype");
    pdNaT_ = getType(pandas_.attr("NaT"));
    pdNA_ = getType(pandas_.attr("NA"));
    pdseries_ = pandas_.attr("Series");
    pdtimestamp_ = pandas_.attr("Timestamp");
    pdindex_ = pandas_.attr("Index");

    nparray_ = getType(py::array());
    npmatrix_ = numpy_.attr("matrix");
    npbool_ = name2dtype("bool");
    npint8_ = name2dtype("int8");
    npint16_ = name2dtype("int16");
    npint32_ = name2dtype("int32");
    npint64_ = name2dtype("int64");
    npfloat32_ = name2dtype("float32");
    npfloat64_ = name2dtype("float64");
    npstrType_ = py::reinterpret_borrow<py::object>(name2dtype("str").get_type());

    if(np_above_1_20_) {
        npdatetime64M_ins_ = name2dtype("datetime64[M]");
        npdatetime64D_ins_ = name2dtype("datetime64[D]");
        npdatetime64m_ins_ = name2dtype("datetime64[m]");
        npdatetime64s_ins_ = name2dtype("datetime64[s]");
        npdatetime64h_ins_ = name2dtype("datetime64[h]");
        npdatetime64ms_ins_ = name2dtype("datetime64[ms]");
        npdatetime64us_ins_ = name2dtype("datetime64[us]");
        npdatetime64ns_ins_ = name2dtype("datetime64[ns]");
        npdatetime64_ins_ = name2dtype("datetime64");
    }

    npobject_ = name2dtype("object");
    
    pynone_ = getType(py::none());
    pybool_ = getType(py::bool_());
    pyint_ = getType(py::int_());
    pyfloat_ = getType(py::float_());
    pystr_ = getType(py::str());
    pybytes_ = getType(py::bytes());
    pyset_ = getType(py::set());
    pytuple_ = getType(py::tuple());
    pylist_ = getType(py::list());
    pydict_ = getType(py::dict());

    m_decimal_ = py::module::import("decimal");
    decimal_ = m_decimal_.attr("Decimal");
}

TableChecker::TableChecker(const py::dict &pydict) {
    if (pydict.is_none()) return;
    for(auto iter = pydict.begin(); iter != pydict.end(); ++iter) {
        py::object key = py::reinterpret_borrow<py::object>((*iter).first);
        py::object value = py::reinterpret_borrow<py::object>((*iter).second);
        Type type;
        if (py::isinstance(value, DdbPythonUtil::preserved_->pyint_)) {
            type = std::make_pair(static_cast<DATA_TYPE>(py::cast<int>(value)), -1);
        }
        else if (py::isinstance(value, DdbPythonUtil::preserved_->pylist_)) {
            py::list tmplist = py::reinterpret_borrow<py::list>(value);
            if (tmplist.size() <=0 || tmplist.size() > 2)
                throw RuntimeException("Error Form of TableChecker.");
            DATA_TYPE datatype = static_cast<DATA_TYPE>(py::cast<int>(tmplist[0]));
            int exparam = py::cast<int>(tmplist[1]);
            type = std::make_pair(datatype, exparam);
        }
        else {
            throw RuntimeException("Error Form of TableChecker.");
        }
        this->insert(std::make_pair(py::cast<std::string>(key), type));
    }
}

Preserved *DdbPythonUtil::preserved_ = nullptr;
const static long long npLongNull_ = 0x8000000000000000;
static inline void SET_NPNULL(long long *p, size_t len = 1) { std::fill((long long *)p, ((long long *)p) + len, npLongNull_); }
const static uint64_t npDoubleNull_ = 9221120237041090560LL;
static inline void SET_NPNULL(double *p, size_t len = 1) { std::fill((uint64_t *)p, ((uint64_t *)p) + len, npDoubleNull_); }
const static uint32_t npFloatNull_ = 2143289344;
static inline void SET_NPNULL(float *p, size_t len = 1) { std::fill((uint32_t *)p, ((uint32_t *)p) + len, npFloatNull_); }

static inline bool isValueNumpyNull(long long value) {
    return value == npLongNull_;
}

static inline bool isValueNumpyNull(double value) {
    return memcmp(&value, &npDoubleNull_, 8) == 0;
}

static inline bool isValueNumpyNull(float value) {
    return memcmp(&value, &npFloatNull_, 4) == 0;
}

static inline bool isNULL(PyObject *pobj) {
    return  PyObject_IsInstance(pobj,DdbPythonUtil::preserved_->pynone_.ptr()) ||
            PyObject_IsInstance(pobj,DdbPythonUtil::preserved_->pdNaT_.ptr())  ||
            PyObject_IsInstance(pobj,DdbPythonUtil::preserved_->pdNA_.ptr())   ||
            (PyObject_IsInstance(pobj,DdbPythonUtil::preserved_->nan_.ptr())&&isValueNumpyNull(py::cast<double>(pobj)));
}

static inline bool isNULL(const py::object &obj) {
    return  py::isinstance(obj, DdbPythonUtil::preserved_->pynone_) ||
            py::isinstance(obj, DdbPythonUtil::preserved_->pdNaT_) ||
            py::isinstance(obj, DdbPythonUtil::preserved_->pdNA_) ||
            (py::isinstance(obj, DdbPythonUtil::preserved_->nan_) && isValueNumpyNull(py::cast<double>(obj)));
}

static inline bool isNULL(const py::object &obj, int &pri, DATA_TYPE &nullType) {
    if (py::isinstance(obj, DdbPythonUtil::preserved_->pynone_)) {
        pri = 1;
        nullType = DT_ANY;
        return true;
    }
    else if (
        py::isinstance(obj, DdbPythonUtil::preserved_->pdNA_) ||
        (py::isinstance(obj, DdbPythonUtil::preserved_->nan_) && isValueNumpyNull(py::cast<double>(obj)))
    ) {
        pri = 2;
        nullType = DT_DOUBLE;
        return true;
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pdNaT_)) {
        pri = 3;
        nullType = DT_NANOTIMESTAMP;
        return true;
    }
    return false;
}

static inline py::object getDType(const py::object &obj){
    py::object dtype;
    if(py::hasattr(obj, "dtype")){
        dtype = py::getattr(obj, "dtype");
    }
    return dtype;
}

static inline int getPyDecimalScale(PyObject* obj) {
    PyObject* DecTuple = PyObject_CallMethod(obj, "as_tuple", NULL);
    PyObject* dataExp = PyObject_GetAttrString(DecTuple, "exponent");
    int res = -1;
    if(!PyObject_IsInstance(dataExp, DdbPythonUtil::preserved_->pystr_.ptr())) {
        res = (-1)*py::cast<int>(dataExp);
    }
    Py_DECREF(DecTuple);
    Py_DECREF(dataExp);
    return res;
}

template <typename T>
T getPyDecimalData(PyObject* obj, bool &hasNull) {
    PyObject* DecTuple = PyObject_CallMethod(obj, "as_tuple", NULL);
    PyObject* dataExp = PyObject_GetAttrString(DecTuple, "exponent");
    if(PyObject_IsInstance(dataExp, DdbPythonUtil::preserved_->pystr_.ptr())) {
        Py_DECREF(DecTuple);
        Py_DECREF(dataExp);
        hasNull = true;
        return std::numeric_limits<T>::min();
    }
        
    int sign = py::cast<int>(PyObject_GetAttrString(DecTuple, "sign"));
    PyObject* dataTuple = PyObject_GetAttrString(DecTuple, "digits");
    int dec_len = PyTuple_GET_SIZE(dataTuple);
    T dec_data = 0;
    for(int ti=0;ti<dec_len;ti++) {
        dec_data *= 10;
        dec_data += py::cast<T>(PyTuple_GET_ITEM(dataTuple, ti));
        if(dec_data < 0) {
            Py_DECREF(DecTuple);
            Py_DECREF(dataExp);
            Py_DECREF(dataTuple);
            throw RuntimeException("Decimal math overflow");
        }
    }
    Py_DECREF(DecTuple);
    Py_DECREF(dataExp);
    Py_DECREF(dataTuple);
    if(sign) {
        return (-1)*dec_data;
    }else {
        return dec_data;
    }
}

template <typename T>
void getDecimalDigits(T raw_data, vector<int>& digits) {
    vector<int> raw_vec;
    while(raw_data > 0) {
        raw_vec.emplace_back(raw_data % 10);
        raw_data /= 10;
    }
    for(auto riter=raw_vec.rbegin(); riter != raw_vec.rend(); riter++) {
        digits.emplace_back(*riter);
    }
}

template <typename T>
void processData(T *psrcData, size_t size, std::function<void(T *, int)> f) {
    int bufsize = std::min(65535, (int)size);
#ifdef _MSC_VER
    T *buf = new T[bufsize];   // this case for VS compile error
#else
    T buf[bufsize];            // this case for fix APY-474
#endif
    int startIndex = 0, len;
    while(startIndex < size){
        len = std::min((int)(size-startIndex), bufsize);
        memcpy(buf, psrcData+startIndex, sizeof(T)*len);
        f(buf,len);
        startIndex += len;
    }
#ifdef _MSC_VER
    delete[]buf;   // this case for VS compile error
#endif
}

void throwExceptionAboutNumpyVersion(int row_index, const string &value, const string &type, const TableVectorInfo &info) {
    string errinfo;
    if(!info.is_empty)
        errinfo = "The value " + value + " (column \"" + info.colName + "\", row " + std::to_string(row_index) + ") must be of " + type + " type";
    else
        errinfo = "The value " + value + " (at index " + std::to_string(row_index) + ") must be of " + type + " type";
    if(DdbPythonUtil::preserved_->np_above_1_20_) {
        throw RuntimeException(errinfo+".");
    }
    else {
        throw RuntimeException(errinfo+", or unsupport numpy type.");
    }
}

static inline void createAllNullVector(VectorSP &ddbVec, DATA_TYPE type, INDEX size, int exparam=0) {
    ddbVec = Util::createVector(type, size, size, true, exparam);
    ddbVec->fill(0, size, Constant::void_);
}

bool addBoolVector(const py::array &pyVec, size_t size, size_t offset, char nullValue, const Type &type,
    std::function<void(char *, int)> f, const TableVectorInfo &info) {
    int bufsize = std::min(65535, (int)size);
    std::unique_ptr<char[]> bufsp(new char[bufsize]);
    char* buf = bufsp.get();
    auto buffer = pyVec.request();
    int8_t *it = reinterpret_cast<int8_t *>(buffer.ptr);
    int startIndex = 0, len, i = 0;
    bool hasNull = false;
    int stride = buffer.strides[0];
    it += stride * offset;
    PyObject ** ptr = reinterpret_cast<PyObject **>(it);
    try{
        while(startIndex < size) {
            len = std::min((int)(size-startIndex), bufsize);
            for (i=0; i < len; ++i) {
                ptr = reinterpret_cast<PyObject **>(it);
                if (isNULL(*ptr)) {
                    buf[i] = nullValue;
                    hasNull = true;
                } else {
                    buf[i] = py::handle(*ptr).cast<bool>();
                }
                it += stride;
            }
            f(buf, len);
            startIndex += len;
        }
    }catch(...){
        throwExceptionAboutNumpyVersion(
            startIndex+i+offset, py::str(*ptr).cast<std::string>(), Util::getDataTypeString(type.first), info
        );
    }
    return hasNull;
}

bool addCharVector(const py::array &pyVec, size_t size, size_t offset, char nullValue, const Type &type,
    std::function<void(char *, int)> f, const TableVectorInfo &info) {
    int bufsize = std::min(65535, (int)size);
    std::unique_ptr<char[]> bufsp(new char[bufsize]);
    char* buf = bufsp.get();
    auto buffer = pyVec.request();
    int8_t *it = reinterpret_cast<int8_t *>(buffer.ptr);
    int startIndex = 0, len, i = 0;
    bool hasNull = false;
    int stride = buffer.strides[0];
    it += stride * offset;
    PyObject ** ptr = reinterpret_cast<PyObject **>(it);
    try{
        while(startIndex < size) {
            len = std::min((int)(size-startIndex), bufsize);
            for (i=0; i < len; ++i) {
                ptr = reinterpret_cast<PyObject **>(it);
                if (isNULL(*ptr)) {
                    buf[i] = nullValue;
                    hasNull = true;
                } else {
                    buf[i] = static_cast<char>(py::cast<int8_t>(*ptr));
                }
                it += stride;
            }
            f(buf, len);
            startIndex += len;
        }
    }catch(...){
        throwExceptionAboutNumpyVersion(
            startIndex+i+offset, py2str(*ptr), Util::getDataTypeString(type.first), info
        );
    }
    return hasNull;
}

template <typename T>
bool addNullVector(const py::array &pyVec, size_t size, size_t offset, T nullValue, const Type &type,
    std::function<void(T *, int)> f, const TableVectorInfo &info) {
    int bufsize = std::min(65535, (int)size);
    std::unique_ptr<T[]> bufsp(new T[bufsize]);
    T* buf = bufsp.get();
    auto buffer = pyVec.request();
    int8_t *it = reinterpret_cast<int8_t *>(buffer.ptr);
    int startIndex = 0, len, i = 0;
    bool hasNull = false;
    int stride = buffer.strides[0];
    it += stride * offset;
    PyObject ** ptr = reinterpret_cast<PyObject **>(it);
    try{
        while(startIndex < size) {
            len = std::min((int)(size-startIndex), bufsize);
            for (i=0; i < len; ++i) {
                ptr = reinterpret_cast<PyObject **>(it);
                if (isNULL(*ptr)) {
                    buf[i] = nullValue;
                    hasNull = true;
                } else {
                    buf[i] = py::cast<T>(*ptr);
                }
                it += stride;
            }
            f(buf, len);
            startIndex += len;
        }
    }catch(...){
        throwExceptionAboutNumpyVersion(
            startIndex+i+offset, py2str(*ptr), Util::getDataTypeString(type.first), info
        );
    }
    return hasNull;
}

template <typename T>
bool addDecimalVector(const py::array &pyVec, size_t size, size_t offset, T nullValue, Type type,
    std::function<void(T *, int)> f, const TableVectorInfo &info) {
    int bufsize = std::min(65535, (int)size);
    std::unique_ptr<T[]> bufsp(new T[bufsize]);
    T* buf = bufsp.get();
    auto buffer = pyVec.request();
    int8_t *it = reinterpret_cast<int8_t *>(buffer.ptr);
    int startIndex = 0, len, i = 0;
    bool hasNull = false;
    int stride = buffer.strides[0];
    it += stride * offset;
    PyObject ** ptr = reinterpret_cast<PyObject **>(it);
    try{
        while(startIndex < size) {
            len = std::min((int)(size-startIndex), bufsize);
            for (i=0; i < len; ++i) {
                ptr = reinterpret_cast<PyObject **>(it);
                if (isNULL(*ptr)) {
                    buf[i] = nullValue;
                    hasNull = true;
                } else {
                    buf[i] = getPyDecimalData<T>(*ptr, hasNull);
                }
                it += stride;
            }
            f(buf, len);
            startIndex += len;
        }
    }catch(...){
        throwExceptionAboutNumpyVersion(
            startIndex+i+offset, py2str(*ptr), Util::getDataTypeString(type.first), info
        );
    }
    return hasNull;
}

void addStringVector(VectorSP &ddbVec, const py::array &pyVec, size_t size, ssize_t offset, const TableVectorInfo &info) {
    int strbufsize = 1024;      // this case for fix APY-474
    std::unique_ptr<std::string[]> bufsp(new std::string[strbufsize]);
    std::string* strs = bufsp.get();
    DATA_TYPE type = ddbVec->getType();
    int addstrindex = 0;
    int addbatchindex = 0;
    INDEX ind = -1;
    char *buffer;
    size_t i = 0;
    Py_ssize_t length;
    auto it = pyVec.begin();
    for(size_t offseti = 0; offseti < offset && it != pyVec.end(); offseti++)
        ++it;
    try {
        for(; it != pyVec.end() && i < size; ++it, i++) {
            if(isNULL(it->ptr())==false){
                PyObject *str_value=it->ptr();
                if(str_value != NULL){
                    PyObject *utf8obj = PyUnicode_AsUTF8String(str_value);
                    if (utf8obj != NULL){
                        PyBytes_AsStringAndSize(utf8obj, &buffer, &length);
                        strs[addstrindex].assign(buffer, (size_t)length);
                        Py_DECREF(utf8obj);
                    }else{
                        DLOG("can't get utf-8 object from string.");
                        strs[addstrindex].clear();
                    }
                }else{
                    DLogger::Warn("Can't convert No.",offset+i,"value to string.");
                }
            }else{
                strs[addstrindex].clear();
            }
            addstrindex++;
            if(addstrindex >= strbufsize){
                if(!ddbVec->appendString(strs, addstrindex, ind)){
                    throw RuntimeException("");
                }
                addstrindex=0;
                addbatchindex++;
            }
        }
        if(!ddbVec->appendString(strs, addstrindex, ind)) {
            throw RuntimeException("");
        }
    }
    catch(...) {
        if(ind == -1) {         // failed to convert pystr to string
            auto errit = pyVec.begin();
            int row_index = offset+i;
            for(ssize_t offseti = 0; offseti < row_index && errit != pyVec.end(); offseti++)
                errit++;
            throwExceptionAboutNumpyVersion(offset+i, py::str(*errit).cast<std::string>(), Util::getDataTypeString(type), info);
        }
        else {                  // failed to convert STRING to IP/UUID/INT128
            auto errit = pyVec.begin();
            int row_index = offset+addbatchindex*strbufsize+ind;
            for(ssize_t offseti = 0; offseti < row_index && errit != pyVec.end(); offseti++)
                errit++;
            throwExceptionAboutNumpyVersion(row_index, py::str(*errit).cast<std::string>(), Util::getDataTypeString(type), info);
        }
    }
}

inline bool isArrayLike(const py::object &obj) {
    return  py::isinstance(obj, DdbPythonUtil::preserved_->nparray_) ||
            py::isinstance(obj, DdbPythonUtil::preserved_->pylist_) ||
            py::isinstance(obj, DdbPythonUtil::preserved_->pytuple_) ||
            py::isinstance(obj, DdbPythonUtil::preserved_->pdseries_) ||
            py::isinstance(obj, DdbPythonUtil::preserved_->pdindex_);
}

bool
checkElemType(const py::object              &obj,
              Type                          &type,
              const CHILD_VECTOR_OPTION     &option,
              bool                          &is_ArrayVector,
              bool                          &is_Null,
              int                           &nullpri,
              Type                          &nullType) {
    is_Null = false;
    if (isArrayLike(obj)) {
        switch (option)
        {
        case CHILD_VECTOR_OPTION::ANY_VECTOR:
            type.first = DT_ANY;
            break;
        case CHILD_VECTOR_OPTION::ARRAY_VECTOR:
            is_ArrayVector = true;
            break;
        default:
            throw RuntimeException("unexpected vector form in object.");
        }
        return true;
    }
    int curNullpri = 0;
    Type curType = Scalar2DolphinDBType(obj, is_Null, curNullpri, option);
    DLOG("curType: ", Util::getDataTypeString(curType.first), curType.second);
    DLOG("is_Null: ", is_Null);
    DLOG("curNullpri: ", curNullpri);
    if (Util::getCategory(curType.first) == DATA_CATEGORY::DENARY) {
        if (curType.second == -1) {
            is_Null = true;
            curNullpri = 4;
            curType = {DT_DECIMAL64, -1};
        }
    }
    if (is_Null) {
        if (curNullpri > nullpri) {
            nullpri = curNullpri;
            nullType = curType;
        }
        return false;
    }
    if (curType.first == DT_OBJECT) {
        // DType=object ==> DT_ANY
        type.first = DT_ANY;
        return true;
    }
    if (type.first == DT_OBJECT || type.first == DT_UNK) {
        type = curType;
        return false;
    }
    if (type.first != curType.first) {
        type.first = DT_ANY;
        return true;
    }
    return false;
}

Type
getChildrenType(const py::object            &children,
                const CHILD_VECTOR_OPTION   &option,
                bool                        &isArrayVector,
                bool                        &allNULL) {
    int nullpri = 0;
    Type nullType = {DT_UNK, -1};
    Type type = {DT_UNK, -1};
    bool isNull;
    bool canCheck = false;
    py::object one;
    for (auto &child : children) {
        one = py::cast<py::object>(child);
        canCheck = checkElemType(one, type, option, isArrayVector, isNull, nullpri, nullType);
        if (allNULL && !isNull) {
            allNULL = false;
        }
        if (canCheck) break;
    }
    if (allNULL) {
        if (nullType.first != DT_UNK) {
            type = nullType;
        }
    }
    return type;
}

Type
Scalar2DolphinDBType(const py::object       &obj,
                     bool                   &isNull,
                     int                    &nullpri,
                     CHILD_VECTOR_OPTION    option) {
    isNull = false;
    DATA_TYPE nullType;
    if (isNULL(obj, nullpri, nullType)) {
        isNull = true;
        return {nullType, -1};
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pybool_)) {
        return {DT_BOOL, -1};
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pyint_)) {
        auto result = obj.cast<long long>();
        isNull = isValueNumpyNull(result);
        return {DT_LONG, -1};
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pyfloat_)) {
        auto result = obj.cast<double>();
        isNull = isValueNumpyNull(result);
        return {DT_DOUBLE, -1};
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pystr_)) {
        return {DT_STRING, -1};
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pybytes_)) {
        return {DT_BLOB, -1};
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->decimal_)) {
        DLOG("is decimal.");
        return {DT_DECIMAL64, getPyDecimalScale(obj.ptr())};
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pdtimestamp_)) {
        return {DT_TIMESTAMP, -1};
    }
    // else {
    //     py::dtype dtype = py::cast<py::dtype>(getDType(obj));
    //     if (!bool(dtype)) {
    //         throw RuntimeException("unsupported type ["+ py2str(obj.get_type())+"].");
    //     }
    //     bool isArrowDType = false;
    //     return InferDataTypeFromDType(dtype, isArrowDType, false);
    // }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->datetime64_)) {
        py::object dtype = getDType(obj);
        if (!bool(dtype)) {
            throw RuntimeException("unsupported type ["+ py2str(obj.get_type())+"].");
        }
        long long value = obj.attr("astype")("int64").cast<long long>();
        isNull = isValueNumpyNull(value);
        if (isNull) {
            nullpri = 3;
        }
        if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64ns_())) {
            return {DT_NANOTIMESTAMP, -1};
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64D_())) {
            return {DT_DATE, -1};
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64M_())) {
            return {DT_MONTH, -1};
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64m_())) {
            return {DT_DATETIME, -1};
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64s_())) {
            return {DT_DATETIME, -1};
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64h_())) {
            return {DT_DATEHOUR, -1};
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64ms_())) {
            return {DT_TIMESTAMP, -1};
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64us_())) {
            return {DT_NANOTIMESTAMP, -1};
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64_())) {
            return {DT_NANOTIMESTAMP, -1};
        }
        else {
            throw RuntimeException(string("unsupported datetime type '") + py2str(dtype) + "', DolphinDB supports ns/D/M/m/s/h/ms/us.");
        }
    }
    py::object dtype = getDType(obj);
    if (!bool(dtype)) {
        if (option == CHILD_VECTOR_OPTION::ANY_VECTOR) {
            return {DT_ANY, -1};
        }
        throw RuntimeException("unsupported type ["+ py2str(obj.get_type())+"].");
    }
    if (dtype.equal(DdbPythonUtil::preserved_->npbool_)) {
        return {DT_BOOL, -1};
    }
    else if (dtype.equal(DdbPythonUtil::preserved_->npint8_)) {
        return {DT_CHAR, -1};
    }
    else if (dtype.equal(DdbPythonUtil::preserved_->npint16_)) {
        return {DT_SHORT, -1};
    }
    else if (dtype.equal(DdbPythonUtil::preserved_->npint32_)) {
        return {DT_INT, -1};
    }
    else if (dtype.equal(DdbPythonUtil::preserved_->npint64_)) {
        return {DT_LONG, -1};
    }
    else if (dtype.equal(DdbPythonUtil::preserved_->npfloat32_)) {
        return {DT_FLOAT, -1};
    }
    else if (dtype.equal(DdbPythonUtil::preserved_->npfloat64_)) {
        return {DT_DOUBLE, -1};
    }
    else if (DdbPythonUtil::preserved_->np_above_1_20_ && dtype.get_type().equal(DdbPythonUtil::preserved_->npstrType_)) {
        return {DT_STRING, -1};
    }
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64_())) {
        return {DT_NANOTIMESTAMP, -1};
    }
    else {
        // str dtype will not be detected if numpy version < 1.20,
        // so just return DT_OBJECT.
        if(!DdbPythonUtil::preserved_->np_above_1_20_)
            return {DT_OBJECT, -1};
        throw RuntimeException(string("unsupported type '") + py2str(dtype) + "'.");
    }
}

/*
    toDolphinDB: used to convert py::object -> ConstantSP
*/
ConstantSP
DdbPythonUtil::toDolphinDB(py::object       obj,
                           Type             typeIndicator,
                           TableChecker     checker) {
    if (py::isinstance(obj, DdbPythonUtil::preserved_->pddataframe_)) {
        // Indicate as DF_TABLE
        return toDolphinDB_Table(obj, checker);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pytuple_)) {
        // Indicate as DF_VECTOR
        return toDolphinDB_Vector_TupleOrList(py::reinterpret_borrow<py::tuple>(obj), typeIndicator, CHILD_VECTOR_OPTION::ANY_VECTOR);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pylist_)) {
        // Indicate as DF_VECTOR
        return toDolphinDB_Vector_TupleOrList(py::reinterpret_borrow<py::list>(obj), typeIndicator, CHILD_VECTOR_OPTION::ANY_VECTOR);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->nparray_)) {
        // Indicate as DF_VECTOR or DF_MATRIX
        return toDolphinDB_NDArray(py::reinterpret_borrow<py::array>(obj), typeIndicator, CHILD_VECTOR_OPTION::ANY_VECTOR);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pdseries_)) {
        // Indicate as DF_VECTOR
        TableVectorInfo info = {true, ""};
        return toDolphinDB_Vector_SeriesOrIndex(obj, typeIndicator, CHILD_VECTOR_OPTION::ANY_VECTOR, info);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pdindex_)) {
        // Indicate as DF_VECTOR
        TableVectorInfo info = {true, ""};
        return toDolphinDB_Vector_SeriesOrIndex(obj, typeIndicator, CHILD_VECTOR_OPTION::ANY_VECTOR, info);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pyset_)) {
        // Indicate as DF_SET
        return toDolphinDB_Set(obj);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pydict_)) {
        // Indicate as DF_DICTIONARY
        return toDolphinDB_Dictionary(obj);
    }
    else {
        // Indicate as DF_SCALAR
        return toDolphinDB_Scalar(obj, typeIndicator);
    }
}

Type InferDataTypeFromArrowType(const py::object &pyarrow_dtype) {
    if (py::isinstance(pyarrow_dtype, DdbPythonUtil::preserved_->palist_)) {
        py::object elemDtype = pyarrow_dtype.attr("value_type");
        Type elemType = InferDataTypeFromArrowType(elemDtype);
        return {(DATA_TYPE)(elemType.first + ARRAY_TYPE_BASE), elemType.second};
    }
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->paboolean_))
        return {DT_BOOL, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->paint8_))
        return {DT_CHAR, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->paint16_))
        return {DT_SHORT, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->paint32_))
        return {DT_INT, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->paint64_))
        return {DT_LONG, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->pafloat32_))
        return {DT_FLOAT, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->pafloat64_))
        return {DT_DOUBLE, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->padate32_))
        return {DT_DATE, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->patime32_s_))
        return {DT_SECOND, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->patime32_ms_))
        return {DT_TIME, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->patime64_ns_))
        return {DT_NANOTIME, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->patimestamp_s_))
        return {DT_DATETIME, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->patimestamp_ms_))
        return {DT_TIMESTAMP, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->patimestamp_ns_))
        return {DT_NANOTIMESTAMP, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->padictionary_int32_utf8_))
        return {DT_SYMBOL, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->pautf8_))
        return {DT_STRING, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->pafixed_size_binary_16_))
        return {DT_INT128, -1};
    else if (pyarrow_dtype.equal(DdbPythonUtil::preserved_->palarge_binary_))
        return {DT_BLOB, -1};
    else if (py::isinstance(pyarrow_dtype, DdbPythonUtil::preserved_->padecimal128_))
        return {DT_DECIMAL64, py::cast<int>(pyarrow_dtype.attr("scale"))};
    else {
        throw RuntimeException("unsupport pyarrow_dtype '" + py2str(pyarrow_dtype) + "'.");
    }
}

Type InferDataTypeFromDType(py::dtype &dtype, bool &Is_ArrowDType, bool needArrowCheck = false) {
    if (Is_ArrowDType || (needArrowCheck && py::isinstance(dtype, DdbPythonUtil::preserved_->pandas_.attr("ArrowDtype")))) {
        Is_ArrowDType = true;
        // Check ArrowDType
        DLOG("ArrowDtype.");
        py::object pyarrow_dtype = dtype.attr("pyarrow_dtype");
        return InferDataTypeFromArrowType(pyarrow_dtype);
    }
    // NumpyDType or PandasDType
    if (dtype.equal(DdbPythonUtil::preserved_->npbool_))
        return std::make_pair(DT_BOOL, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npint8_))
        return std::make_pair(DT_CHAR, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npint16_))
        return std::make_pair(DT_SHORT, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npint32_))
        return std::make_pair(DT_INT, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npint64_))
        return std::make_pair(DT_LONG, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npfloat32_))
        return std::make_pair(DT_FLOAT, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npfloat64_))
        return std::make_pair(DT_DOUBLE, -1);
    else if (DdbPythonUtil::preserved_->np_above_1_20_ && dtype.get_type().equal(DdbPythonUtil::preserved_->npstrType_)) {
        DLOG("Here!");
        return std::make_pair(DT_STRING, -1);
    }
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64D_()))
        return std::make_pair(DT_DATE, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64M_()))
        return std::make_pair(DT_MONTH, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64m_()))
        return std::make_pair(DT_DATETIME, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64s_()))
        return std::make_pair(DT_DATETIME, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64h_()))
        return std::make_pair(DT_DATEHOUR, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64ms_()))
        return std::make_pair(DT_TIMESTAMP, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64us_()))
        return std::make_pair(DT_NANOTIMESTAMP, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64ns_()))
        return std::make_pair(DT_NANOTIMESTAMP, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64_()))
        return std::make_pair(DT_NANOTIMESTAMP, -1);
    else if (dtype.equal(DdbPythonUtil::preserved_->npobject_)) {
        return std::make_pair(DT_OBJECT, -1);
    }
    else if (py::isinstance(dtype, DdbPythonUtil::preserved_->pdextensiondtype_))
        return std::make_pair(DT_EXTENSION, -1);
    else {
        if (!DdbPythonUtil::preserved_->np_above_1_20_) {
            return std::make_pair(DT_OBJECT, -1);
        }
        // str dtype will not be detected if numpy version < 1.20,
        // so just return DT_OBJECT. 
        throw RuntimeException(string("unsupported dtype '") + py2str(dtype) + "', DolphinDB supports object/bool/int/float/datetime.");
    }
}

ConstantSP
toDolphinDB_Table(py::object    obj,
                  TableChecker  &checker) {
    // Get TableChecker
    py::dict dict_checker = py::getattr(obj, "__DolphinDB_Type__", py::dict());
    TableChecker tmp_checker = TableChecker(dict_checker);
    for(auto iter = tmp_checker.begin(); iter != tmp_checker.end(); ++iter) {
        checker[(*iter).first] = (*iter).second;
    }
    // Get names of Columns
    vector<std::string> col_names;
    try {
        col_names = py::cast<vector<std::string>>(obj.attr("columns"));
    }
    catch (...) {
        throw RuntimeException("Column names must be strings indicating a valid variable name.");
    }
    
    size_t cols = col_names.size();
    vector<ConstantSP> columns;
    columns.resize(cols);
    // Convert Columns
    for (size_t ni = 0; ni < cols; ++ni) {
        if (checker.count(col_names[ni]) > 0) {
            TableVectorInfo info = {false, col_names[ni]};
            py::object tmpObj = obj[col_names[ni].data()];
            if (!isArrayLike(tmpObj)) {
                throw RuntimeException("Table columns must be vectors (np.array, series, tuple, or list) for upload.");
            }
            columns[ni] = toDolphinDB_Vector_SeriesOrIndex(
                tmpObj, checker[col_names[ni]],
                {CHILD_VECTOR_OPTION::ARRAY_VECTOR}, info);
        }
        else {
            TableVectorInfo info = {false, col_names[ni]};
            py::object tmpObj = obj[col_names[ni].data()];
            if (!isArrayLike(tmpObj)) {
                throw RuntimeException("Table columns must be vectors (np.array, series, tuple, or list) for upload.");
            }
            columns[ni] = toDolphinDB_Vector_SeriesOrIndex(
                tmpObj, std::make_pair(DT_UNK, -1),
                {CHILD_VECTOR_OPTION::ARRAY_VECTOR}, info);
        }
        columns[ni]->setTemporary(true);
    }
    // Create DDB TableSP
    TableSP ddbTbl = Util::createTable(col_names, columns);
    return ddbTbl;
}

ConstantSP
toDolphinDB_Dictionary(py::object obj) {
    py::dict pyDict = obj;
    size_t size = pyDict.size();
    vector<ConstantSP> _ddbKeyVec;
    vector<ConstantSP> _ddbValVec;
    DATA_TYPE keyType = DT_UNK;
    DATA_TYPE valType = DT_UNK;
    DATA_FORM keyForm = DF_SCALAR;
    DATA_FORM valForm = DF_SCALAR;
    int scale = 0;
    int keyTypes = 0;
    int valTypes = 0;
    int keyForms = 1;
    int valForms = 1;
    int scale_counts = 0;
    for (auto &it : pyDict) {
        _ddbKeyVec.push_back(DdbPythonUtil::toDolphinDB(py::reinterpret_borrow<py::object>(it.first), {DT_UNK, -1}));
        _ddbValVec.push_back(DdbPythonUtil::toDolphinDB(py::reinterpret_borrow<py::object>(it.second), {DT_UNK, -1}));
        if (!_ddbKeyVec.back()->isNull()) {
            DATA_TYPE tmpKeyType = _ddbKeyVec.back()->getType();
            DATA_FORM tmpKeyForm = _ddbKeyVec.back()->getForm();
            if (tmpKeyType != keyType) {
                keyTypes++;
                keyType = tmpKeyType;
            }
            if (tmpKeyForm != keyForm) { keyForms++; }
        }
        if (!_ddbValVec.back()->isNull()) {
            DATA_TYPE tmpValType = _ddbValVec.back()->getType();
            DATA_FORM tmpValForm = _ddbValVec.back()->getForm();
            if (tmpValType != valType) {
                valTypes++;
                valType = tmpValType;
            }
            if (tmpValForm != valForm) { valForms++; }
        }
    }
    if (keyTypes >= 2 || keyForms >= 2) { throw RuntimeException("the key type can not be ANY"); }
    if (valTypes >= 2 || valForms >= 2 || scale_counts >= 2) {
        valType = DT_ANY;
    } else if (keyTypes == 0 || valTypes == 0) {
        throw RuntimeException("can not create all None vector in dictionary");
    }
    DLOG("valForms: ", valForms);
    DLOG("valTypes: ", valTypes);
    DLOG("valType: ", Util::getDataTypeString(valType));
    VectorSP ddbKeyVec = Util::createVector(keyType, 0, size);
    VectorSP ddbValVec;
    if (valType == DT_DECIMAL32) {
        int scale = 0;
        for (int i = 0; i < size; ++i) {
            if (_ddbValVec[i]->isNull()) continue;
            scale = ((Decimal32SP)_ddbValVec[i])->getScale();
            break;
        }
        ddbValVec = Util::createVector(valType, 0, size, true, scale);
    }
    else if (valType == DT_DECIMAL64) {
        int scale = 0;
        for (int i = 0; i < size; ++i) {
            if (_ddbValVec[i]->isNull()) continue;
            scale = ((Decimal64SP)_ddbValVec[i])->getScale();
            break;
        }
        ddbValVec = Util::createVector(valType, 0, size, true, scale);
    }
    else {
        ddbValVec = Util::createVector(valType, 0, size);
    }
    for (size_t i = 0; i < size; ++i) {
        ddbKeyVec->append(_ddbKeyVec[i]);
        ddbValVec->append(_ddbValVec[i]);
    }
    DictionarySP ddbDict = Util::createDictionary(keyType, valType);
    if (ddbDict.isNull()) {
        throw RuntimeException("Not allowed to create a Dictionary with "+Util::getDataTypeString(keyType)+" key and "+Util::getDataTypeString(valType)+" value.");
    }
    ddbDict->set(ddbKeyVec, ddbValVec);
    return ddbDict;
}

ConstantSP toDolphinDB_Set(py::object obj) {
    py::set pySet = obj;
    size_t size = pySet.size();
    vector<ConstantSP> _ddbSet;
    DATA_TYPE type = DT_UNK;
    DATA_FORM form = DF_SCALAR;
    int types = 0;
    int forms = 0;
    for (auto &it : pySet) {
        _ddbSet.push_back(DdbPythonUtil::toDolphinDB(py::reinterpret_borrow<py::object>(it), {DT_UNK, -1}));
        if (_ddbSet.back()->isNull()) { continue; }
        DATA_TYPE tmpType = _ddbSet.back()->getType();
        DATA_FORM tmpForm = _ddbSet.back()->getForm();
        if (tmpType != type) {
            types++;
            type = tmpType;
        }
        if (tmpForm != form) {forms++;}
    }
    if (types >= 2 || forms >= 2) {
        throw RuntimeException("set in DolphinDB doesn't support multiple types");
    } else if (types == 0) {
        throw RuntimeException("can not create all None set");
    }
    SetSP ddbSet = Util::createSet(type, _ddbSet.size());
    if (ddbSet.isNull()) {
        throw RuntimeException("Not allowed to create a "+Util::getDataTypeString(type)+" Set.");
    }
    for (auto &v : _ddbSet) { ddbSet->append(v); }
    return ddbSet;
}

ConstantSP toDolphinDB_NDArray(py::array obj, Type typeIndicator, const CHILD_VECTOR_OPTION &option) {
    TableVectorInfo info = {true, ""};
    DATA_TYPE &typeInfer = typeIndicator.first;
    int &exparam = typeIndicator.second;
    int dim = obj.ndim();
    int rows = dim == 1 ? 1 : obj.shape(0);
    int cols = dim == 1 ? obj.size() : obj.shape(1);
    py::dtype dtype = obj.dtype();
    bool Is_ArrayVector = false;
    bool Is_DTypeObject = false;
    bool Is_ArrowDType = false;

    if (dim > 2) {
        throw RuntimeException("numpy.ndarray with dimension > 2 is not supported");
    }
    if (rows == 1) {
        // Create Vector
        if (dim == 2) {
            DLOG("xxxxxx");
            if (py::isinstance(obj, DdbPythonUtil::preserved_->npmatrix_)) {
                DLOG("np.matrix1_");
                obj = DdbPythonUtil::preserved_->numpy_.attr("array")(obj);
            }
            obj = obj.attr("transpose")().attr("reshape")(obj.size());
        }
        py::dtype dtype = obj.dtype();
        Is_DTypeObject = dtype.equal(DdbPythonUtil::preserved_->npobject_);
        if (typeInfer == DT_UNK) {  // Infer DType [NumpyDType, ...]
            typeIndicator = InferDataTypeFromDType(dtype, Is_ArrowDType);
        }
        
        DLOG("Infer: ", Util::getDataTypeString(typeIndicator.first), typeIndicator.second);
        DLOG("Is_DTypeObject: ", Is_DTypeObject);
        if (!Is_DTypeObject) {
            // Explicit Type
            // Data in Series with NumpyDType cannot be ArrayVector
            if (typeInfer == DT_OBJECT)
                // typeInfer == DT_OBJECT && !Is_DTypeObject ==> Indicate Type is OBJECT !!Not possible!!
                goto inferobjectdtype;
            if (typeInfer >= ARRAY_TYPE_BASE)
                throw RuntimeException("not support to convert to ARRAY VECTOR.");
            VectorSP ddbVec;
            switch (typeInfer) {
                case DT_BOOL:
                case DT_CHAR: {
                    py::array_t<int8_t> series_array = py::cast<py::array_t<int8_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendChar(reinterpret_cast<char*>(const_cast<int8_t*>(series_array.data())), size);
                    break;
                }
                case DT_SHORT: {
                    py::array_t<int16_t> series_array = py::cast<py::array_t<int16_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendShort(reinterpret_cast<short*>(const_cast<int16_t*>(series_array.data())), size);
                    break;
                }
                case DT_INT: {
                    py::array_t<int32_t> series_array = py::cast<py::array_t<int32_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(series_array.data())), size);
                    break;
                }
                case DT_LONG: {
                    py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                    break;
                }
                case DT_MONTH: {
                    py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    int64_t* buf = series_array.mutable_data();
                    for (int i = 0; i < size; ++i) {
                        if (buf[i] != LLONG_MIN) {
                            buf[i] += 23640;
                        }
                    }
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                    break;
                }
                case DT_DATE:
                case DT_TIME:
                case DT_MINUTE:
                case DT_SECOND:
                case DT_DATETIME:
                case DT_DATEHOUR:
                case DT_TIMESTAMP:
                case DT_NANOTIME:
                case DT_NANOTIMESTAMP: {
                    py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                    break;
                }
                case DT_FLOAT: {
                    py::array_t<float_t> series_array = py::cast<py::array_t<float_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    bool hasNull = false;
                    processData<float>(const_cast<float*>(series_array.data()), size, [&](float *buf, int size) {
                        for (int i = 0; i < size; ++i) {
                            if(isValueNumpyNull(buf[i])) {
                                buf[i] = FLT_NMIN;
                                hasNull = true;
                            }
                        }
                        ddbVec->appendFloat(buf, size);
                    });
                    ddbVec->setNullFlag(hasNull);
                    break;
                }
                case DT_DOUBLE: {
                    py::array_t<double_t> series_array = py::cast<py::array_t<double_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    bool hasNull = false;
                    processData<double>(const_cast<double*>(series_array.data()), size, [&](double *buf, int size) {
                        for (int i = 0; i < size; ++i) {
                            if(isValueNumpyNull(buf[i])) {
                                buf[i] = DBL_NMIN;
                                hasNull = true;
                            }
                        }
                        ddbVec->appendDouble(buf, size);
                    });
                    ddbVec->setNullFlag(hasNull);
                    break;
                }
                case DT_IP:
                case DT_UUID:
                case DT_INT128:
                case DT_SYMBOL:
                case DT_STRING: {
                    py::array series_array = py::cast<py::array>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    addStringVector(ddbVec, series_array, size, 0, info);
                    break;
                }
                case DT_BLOB: {
                    py::array series_array = py::cast<py::array>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(DT_BLOB, size, size);
                    py::object tmpObj;
                    int ni = 0;
                    for (auto &it : series_array) {
                        tmpObj = py::cast<py::object>(it);
                        if (isNULL(tmpObj)) {
                            ddbVec->setString(ni, "");
                        }
                        else {
                            ddbVec->setString(ni, py::cast<std::string>(tmpObj));
                        }
                        ++ni;
                    }
                    break;
                }
                case DT_DECIMAL32:
                case DT_DECIMAL64:
                case DT_DECIMAL128:
                case DT_ANY:
                default: {
                    throw RuntimeException("cannot convert " + py2str(dtype) + " to " + Util::getDataTypeString(typeInfer));
                }
            }
            return ddbVec;
        }
inferobjectdtype:
        DLOG("[toDolphinDB_NDArray dtype=object]");
        VectorSP ddbVec;
        size_t size = obj.size();
        bool canCheck = false;
        bool is_Null = false;
        bool all_Null = true;
        int nullpri = 0;
        Type nullType = {DT_UNK, -1};
        py::object tmpObj;
        for (auto &it : obj) {
            tmpObj = py::reinterpret_borrow<py::object>(it);
            canCheck = checkElemType(tmpObj, typeIndicator, option, Is_ArrayVector, is_Null, nullpri, nullType);
            if (!is_Null) {
                all_Null = false;
            }
            if (canCheck) {
                break;
            }
            if (typeInfer != DT_OBJECT && option == CHILD_VECTOR_OPTION::ARRAY_VECTOR) {
                break;
            }
        }
        DLOG("typeIndicator: ", Util::getDataTypeString(typeIndicator.first), typeIndicator.second);
        DLOG("is_ArrayVector: ", Is_ArrayVector);
        DLOG("all_Null: ", all_Null);
        if (!Is_ArrayVector && all_Null && nullType.first != DT_UNK) {
            typeIndicator = nullType;
        }
        if (all_Null) {
            DLOG("nullType: ", Util::getDataTypeString(nullType.first), nullType.second);
            DATA_TYPE finalType = typeInfer == DT_UNK || typeInfer == DT_OBJECT ? DT_DOUBLE : typeInfer;
            createAllNullVector(ddbVec, finalType, size);
            return ddbVec;
        }
        py::array &series_array = obj;
        switch (typeInfer) {
            case DT_BOOL: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addBoolVector(series_array, size, 0, CHAR_MIN, typeIndicator, [&](char* buf, int size) {
                    ddbVec->appendBool(buf, size);
                }, info));
                break;
            }
            case DT_CHAR: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addCharVector(series_array, size, 0, CHAR_MIN, typeIndicator, [&](char* buf, int size) {
                    ddbVec->appendChar(buf, size);
                }, info));
                break;
            }
            case DT_SHORT: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<short>(series_array, size, 0, SHRT_MIN, typeIndicator, [&](short * buf, int size) {
                    ddbVec->appendShort(buf, size);
                }, info));
                break;
            }
            case DT_INT: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<int>(series_array, size, 0, INT_MIN, typeIndicator, [&](int * buf, int size) {
                    ddbVec->appendInt(buf, size);
                }, info));
                break;
            }
            case DT_LONG: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<long long>(series_array, size, 0, LLONG_MIN, typeIndicator, [&](long long * buf, int size) {
                    ddbVec->appendLong(buf, size);
                }, info));
                break;
            }
            case DT_MONTH:
            case DT_DATE:
            case DT_TIME:
            case DT_MINUTE:
            case DT_SECOND:
            case DT_DATETIME:
            case DT_DATEHOUR:
            case DT_TIMESTAMP:
            case DT_NANOTIME:
            case DT_NANOTIMESTAMP: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ConstantSP tmpObj;
                size_t index = 0;
                for (auto &it : series_array) {
                    try {
                        tmpObj = toDolphinDB_Scalar(py::reinterpret_borrow<py::object>(it), typeIndicator);
                        ddbVec->append(tmpObj);
                    }
                    catch (...) {
                        throwExceptionAboutNumpyVersion(
                            index, py2str(py::reinterpret_borrow<py::object>(it)), Util::getDataTypeString(typeInfer), info
                        );
                    }
                    ++index;
                }
                break;
            }
            case DT_FLOAT: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<float>(series_array, size, 0, FLT_NMIN, typeIndicator, [&](float * buf, int size) {
                    ddbVec->appendFloat(buf, size);
                }, info));
                break;
            }
            case DT_DOUBLE: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<double>(series_array, size, 0, DBL_NMIN, typeIndicator, [&](double * buf, int size) {
                    ddbVec->appendDouble(buf, size);
                }, info));
                break;
            }
            case DT_IP:
            case DT_UUID:
            case DT_INT128:
            case DT_SYMBOL:
            case DT_STRING: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                addStringVector(ddbVec, series_array, size, 0, info);
                break;
            }
            case DT_BLOB: {
                ddbVec = Util::createVector(DT_BLOB, size, size);
                py::object tmpObj;
                int ni = 0;
                for (auto &it : series_array) {
                    tmpObj = py::cast<py::object>(it);
                    if (isNULL(tmpObj)) {
                        ddbVec->setString(ni, "");
                    }
                    else {
                        ddbVec->setString(ni, py::cast<std::string>(tmpObj));
                    }
                    ++ni;
                }
                break;
            }
            case DT_DECIMAL32:
            case DT_DECIMAL64: {
                size_t index = 0;
                int exparam = -1;
                if (typeIndicator.second == -1) {
                    // Need to Infer Exparam
                    int nullCount = 0;
                    for (auto &it : series_array) {
                        try {
                            if (isNULL(it.ptr())) {
                                ++nullCount;
                                ++index;
                                continue;
                            }
                            if (py::isinstance(it, DdbPythonUtil::preserved_->decimal_)) {
                                exparam = getPyDecimalScale(it.ptr());
                                if (exparam == -1) {
                                    ++nullCount;
                                    ++index;
                                    continue;
                                }
                                break;
                            }
                            throw RuntimeException("");
                        }
                        catch (...) {
                            throwExceptionAboutNumpyVersion(
                                index, py2str(py::reinterpret_borrow<py::object>(it)), Util::getDataTypeString(typeInfer), info
                            );
                        }
                    }
                }
                if (index == size) { // All None
                    createAllNullVector(ddbVec, typeInfer, size, 0);
                    break;
                }
                if (exparam != -1) {    // Infer exparam
                    ddbVec = Util::createVector(typeInfer, 0, size, true, exparam);
                }
                else {
                    ddbVec = Util::createVector(typeInfer, 0, size, true, typeIndicator.second);
                }
                if (typeInfer == DT_DECIMAL32) {
                    ddbVec->setNullFlag(addDecimalVector<int>(series_array, size, index, INT32_MIN, typeIndicator, [&](int *buf, int size) {
                        ((FastDecimal32VectorSP)ddbVec)->appendInt(buf, size);
                    }, info));
                    break;
                }
                else {
                    ddbVec->setNullFlag(addDecimalVector<long long>(series_array, size, index, INT64_MIN, typeIndicator, [&](long long *buf, int size) {
                        ((FastDecimal64VectorSP)ddbVec)->appendLong(buf, size);
                    }, info));
                    break;
                }
            }
            case DT_ANY: {
                ddbVec = Util::createVector(typeInfer, 0, size, true, typeIndicator.second == -1 ? 0 : typeIndicator.second);
                for (auto &it : series_array) {
                    tmpObj = py::reinterpret_borrow<py::object>(it);
                    ConstantSP item = DdbPythonUtil::toDolphinDB(tmpObj, typeInfer == DT_ANY ? Type{DT_UNK, -1} : typeIndicator);
                    ddbVec->append(item);
                }
                break;
            }
            default: {
                throw RuntimeException("type error in numpy: " + Util::getDataTypeString(typeInfer));
            }
        }

        return ddbVec;
    }
    // ndim == 2 & rows != 1
    // Create Matrix
    if (py::isinstance(obj, DdbPythonUtil::preserved_->npmatrix_)) {
        DLOG("np.matrix2_");
        obj = DdbPythonUtil::preserved_->numpy_.attr("array")(obj);
        DLOG("np.xxxxxx");
        DLOG(py2str(obj.get_type()));
    }
    obj = obj.attr("transpose")().attr("reshape")(obj.size());
    ConstantSP tmp = toDolphinDB_NDArray(obj, typeIndicator, CHILD_VECTOR_OPTION::DISABLE);
    ConstantSP matrix = Util::createMatrix(tmp->getType(), cols, rows, cols);
    matrix->set(0, 0, tmp);
    return matrix;
}

template <typename T>
ConstantSP
toDolphinDB_Vector_TupleOrList(T obj,
                               Type typeIndicator,
                               const CHILD_VECTOR_OPTION &option,
                               typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value, T>::type*) {
    TableVectorInfo info = {true, ""};
    DATA_TYPE &typeInfer = typeIndicator.first;
    int &exparam = typeIndicator.second;
    VectorSP ddbVec;
    size_t size = obj.size();
    py::object tmpObj;
    bool canCheck = false;
    bool is_ArrayVector = false;
    bool is_Null = false;
    bool all_Null = true;
    int nullpri = 0;
    Type nullType = {DT_UNK, -1};
    for (auto &it : obj) {
        tmpObj = py::reinterpret_borrow<py::object>(it);
        canCheck = checkElemType(tmpObj, typeIndicator, option, is_ArrayVector, is_Null, nullpri, nullType);
        DLOG("canCheck: ", canCheck, " typeInfer: ", Util::getDataTypeString(typeInfer), " exparam: ", typeIndicator.second);
        if (!is_Null) {
            all_Null = false;
        }
        if (canCheck) {
            break;
        }
        if (typeInfer != DT_OBJECT && option == CHILD_VECTOR_OPTION::ARRAY_VECTOR) {
            break;
        }
    }

    if (!is_ArrayVector && all_Null && nullType.first != DT_UNK) {
        typeIndicator = nullType;
    }
    if (all_Null) {
        DATA_TYPE finalType = typeInfer == DT_UNK ? DT_DOUBLE : typeInfer;
        createAllNullVector(ddbVec, finalType, size);
        return ddbVec;
    }
    DLOG("typeInfer: ", typeInfer);
    DLOG("exparam: ", exparam);
    ddbVec = Util::createVector(typeInfer, 0, size, true, exparam == -1 ? 0 : exparam);
    for (auto &it : obj) {
        tmpObj = py::reinterpret_borrow<py::object>(it);
        ConstantSP item = DdbPythonUtil::toDolphinDB(tmpObj, typeInfer == DT_ANY ? Type{DT_UNK, -1} : typeIndicator);
        ddbVec->append(item);
    }
    return ddbVec;
}

ConstantSP toDolphinDB_Vector_SeriesOrIndex(py::object obj, Type typeIndicator, const CHILD_VECTOR_OPTION &options, const TableVectorInfo &info) {
    // Convert from pd.Series to VectorSP
    DATA_TYPE &typeInfer = typeIndicator.first;
    bool Is_ArrayVector = false;
    bool Is_DTypeObject = false;
    bool Is_ArrowDType = false;
    bool need_check_arrow = DdbPythonUtil::preserved_->pd_above_2_00_ && DdbPythonUtil::preserved_->pyarrow_import_;
    py::dtype dtype = py::reinterpret_borrow<py::dtype>(obj.attr("dtype"));
    Is_DTypeObject = dtype.equal(DdbPythonUtil::preserved_->npobject_);
    if (typeInfer == DT_UNK) {  // Infer DType [NumpyDType, ...]
        typeIndicator = InferDataTypeFromDType(dtype, Is_ArrowDType, need_check_arrow);
    }
    else if (need_check_arrow) {
        if (py::isinstance(dtype, DdbPythonUtil::preserved_->pdarrowdtype_)) {
            Is_ArrowDType = true;
        }
    }
    DLOG("Infer: ", Util::getDataTypeString(typeIndicator.first), typeIndicator.second);
    DLOG("Is_DTypeObject: ", Is_DTypeObject);
    if (!Is_DTypeObject) {
        // Explicit Type
        // Data in Series with NumpyDType cannot be ArrayVector
        Type exactDtype = InferDataTypeFromDType(dtype, Is_ArrowDType, need_check_arrow);
        if (exactDtype.first == DT_EXTENSION) {
            if (typeInfer != DT_EXTENSION) {
                // Indicate Type & Dtype=pd.core.dtypes.dtypes.ExtensionDtype
                goto indicatetype;
            }
            typeInfer = DT_OBJECT;
            goto inferobjecttype;
        }
        if (typeInfer == DT_OBJECT)
            // typeInfer == DT_OBJECT && !Is_DTypeObject ==> Indicate Type is OBJECT !!Not possible!!
            throw RuntimeException("not support to be specificed as type DT_OBJECT.");
        if (typeInfer >= ARRAY_TYPE_BASE && Is_ArrowDType) {
            Is_ArrayVector = true;
            DLOG("[ArrayVector & Is_ArrowDType]");
            Type elemType = {(DATA_TYPE)(typeInfer-ARRAY_TYPE_BASE), typeIndicator.second};
            VectorSP ddbVec;
            obj = DdbPythonUtil::preserved_->pyarrow_.attr("array")(obj.attr("array"));
            if (py::isinstance(obj, DdbPythonUtil::preserved_->pachunkedarray_))
                obj = obj.attr("combine_chunks")();
            py::array_t<int32_t> offsets = DdbPythonUtil::preserved_->pdseries_(obj.attr("offsets"), "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_))
                                            .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
            VectorSP indVec = Util::createVector(DT_INT, 0, offsets.size()-1);
            indVec->appendInt(const_cast<int*>(offsets.data()) + 1, offsets.size() - 1);
            switch (elemType.first) {
                case DT_BOOL: {
                    py::array_t<int8_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"), "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint8_))
                                                    .attr("to_numpy")("dtype"_a="int8", "na_value"_a=CHAR_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size);
                    valVec->appendChar(reinterpret_cast<char*>(const_cast<int8_t*>(values.data())), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_CHAR: {
                    py::array_t<int8_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint8_))
                                                    .attr("to_numpy")("dtype"_a="int8", "na_value"_a=CHAR_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size);
                    valVec->appendChar(reinterpret_cast<char*>(const_cast<int8_t*>(values.data())), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_SHORT: {
                    py::array_t<int16_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint16_))
                                                    .attr("to_numpy")("dtype"_a="int16", "na_value"_a=SHRT_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size);
                    valVec->appendShort(reinterpret_cast<short*>(const_cast<int16_t*>(values.data())), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_INT: {
                    py::array_t<int32_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_))
                                                    .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size);
                    valVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_LONG: {
                    py::array_t<int64_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint64_))
                                                    .attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size);
                    valVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_FLOAT: {
                    py::array_t<float> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->pafloat32_))
                                                    .attr("to_numpy")("dtype"_a="float32", "na_value"_a=FLT_NMIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size);
                    valVec->appendFloat(const_cast<float*>(values.data()), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_DOUBLE: {
                    py::array_t<double> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->pafloat64_))
                                                    .attr("to_numpy")("dtype"_a="float64", "na_value"_a=DBL_NMIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size);
                    valVec->appendDouble(const_cast<double*>(values.data()), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_MONTH:
                case DT_DATE: {
                    py::array_t<int32_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_))
                                                    .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(DT_DATE, 0, size);
                    valVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
                    if (elemType.first != DT_DATE)
                        valVec = valVec->castTemporal(elemType.first);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_SECOND:
                case DT_MINUTE: {
                    py::array_t<int32_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_))
                                                    .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(DT_SECOND, 0, size);
                    valVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
                    if (elemType.first != DT_SECOND)
                        valVec = valVec->castTemporal(elemType.first);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_TIME: {
                    py::array_t<int32_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_))
                                                    .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(DT_TIME, 0, size);
                    valVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_NANOTIME: {
                    py::array_t<int64_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint64_))
                                                    .attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(DT_NANOTIME, 0, size);
                    valVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_DATETIME:
                case DT_DATEHOUR: {
                    py::array_t<int64_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint64_))
                                                    .attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(DT_DATETIME, 0, size);
                    valVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
                    if (elemType.first != DT_DATETIME)
                        valVec = valVec->castTemporal(elemType.first);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_NANOTIMESTAMP:
                case DT_TIMESTAMP: {
                    py::array_t<int64_t> values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                                    "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint64_))
                                                    .attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size);
                    valVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_IP: {
                    py::array values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                        "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->pautf8_))
                                        .attr("to_numpy")("dtype"_a="object", "na_value"_a="");
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size);
                    addStringVector(valVec, values, size, 0, info);
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_UUID:
                case DT_INT128: {
                    py::array values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                        "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->pafixed_size_binary_16_))
                                        .attr("to_numpy")("dtype"_a="object", "na_value"_a="");
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, size, size);
                    int index = 0;
                    py::object tmpObj;
                    std::string tmpstr;
                    unsigned char nullstr[16] = {0};
                    for (auto &it : values) {
                        tmpObj = py::cast<py::object>(obj);
                        tmpstr = py::cast<std::string>(it);
                        if (tmpstr == "") {
                            valVec->setBinary(index, 16, nullstr);
                        }
                        else {
                            std::reverse(tmpstr.begin(), tmpstr.end());
                            valVec->setBinary(index, 16, reinterpret_cast<const unsigned char*>(tmpstr.data()));
                        }
                        ++index;
                    }
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                case DT_DECIMAL32:
                case DT_DECIMAL64: {
                    if (elemType.second == -1) {
                        elemType.second = py::cast<int>(dtype.attr("pyarrow_dtype").attr("value_type").attr("scale"));
                    }
                    py::array values = DdbPythonUtil::preserved_->pdseries_(obj.attr("values"),
                                        "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(dtype.attr("pyarrow_dtype").attr("value_type")))
                                        .attr("to_numpy")("dtype"_a="object");
                    size_t size = values.size();
                    VectorSP valVec = Util::createVector(elemType.first, 0, size, true, elemType.second);
                    if (elemType.first == DT_DECIMAL32) {
                        valVec->setNullFlag(addDecimalVector<int>(values, size, 0, INT32_MIN, elemType, [&](int *buf, int size) {
                            ((FastDecimal32VectorSP)valVec)->appendInt(buf, size);
                        }, info));
                    }
                    else {
                        valVec->setNullFlag(addDecimalVector<long long>(values, size, 0, INT64_MIN, elemType, [&](long long *buf, int size) {
                            ((FastDecimal64VectorSP)valVec)->appendLong(buf, size);
                        }, info));
                    }
                    ddbVec = Util::createArrayVector(indVec, valVec);
                    break;
                }
                default:
                    throw RuntimeException("ERROR");
                    break;
            }
            return ddbVec;
        }
        else if (Is_ArrowDType) {
            VectorSP ddbVec;
            switch (typeInfer) {
                case DT_BOOL: {
                    py::array_t<int8_t> series_array = obj.attr("astype")(DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint8_)).attr("to_numpy")("dtype"_a="int8", "na_value"_a=CHAR_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendChar(reinterpret_cast<char*>(const_cast<int8_t*>(series_array.data())), size);
                    break;
                }
                case DT_CHAR: {
                    py::array_t<int8_t> series_array = obj.attr("to_numpy")("dtype"_a="int8", "na_value"_a=CHAR_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendChar(reinterpret_cast<char*>(const_cast<int8_t*>(series_array.data())), size);
                    break;
                }
                case DT_SHORT: {
                    py::array_t<int16_t> series_array = obj.attr("to_numpy")("dtype"_a="int16", "na_value"_a=SHRT_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendShort(reinterpret_cast<short*>(const_cast<int16_t*>(series_array.data())), size);
                    break;
                }
                case DT_INT: {
                    py::array_t<int32_t> series_array = obj.attr("astype")(DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_)).attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(series_array.data())), size);
                    break;
                }
                case DT_LONG: {
                    py::array_t<int64_t> series_array = obj.attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                    break;
                }
                case DT_FLOAT: {
                    py::array_t<float> series_array = obj.attr("to_numpy")("dtype"_a="float32", "na_value"_a=FLT_NMIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendFloat(const_cast<float*>(series_array.data()), size);
                    break;
                }
                case DT_DOUBLE: {
                    py::array_t<double> series_array = obj.attr("to_numpy")("dtype"_a="float64", "na_value"_a=DBL_NMIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendDouble(const_cast<double*>(series_array.data()), size);
                    break;
                }
                case DT_MONTH:
                case DT_DATE: {
                    py::array_t<int32_t> series_array = obj.attr("astype")(DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_)).attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(DT_DATE, 0, size);
                    ddbVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(series_array.data())), size);
                    if (typeInfer != DT_DATE)
                        ddbVec = ddbVec->castTemporal(typeInfer);
                    break;
                }
                case DT_SECOND:
                case DT_MINUTE: {
                    py::array_t<int32_t> series_array = obj.attr("astype")(DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_)).attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(DT_SECOND, 0, size);
                    ddbVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(series_array.data())), size);
                    if (typeInfer != DT_SECOND)
                        ddbVec = ddbVec->castTemporal(typeInfer);
                    break;
                }
                case DT_TIME: {
                    py::array_t<int32_t> series_array = obj.attr("astype")(DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_)).attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(DT_TIME, 0, size);
                    ddbVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(series_array.data())), size);
                    break;
                }
                case DT_NANOTIME: {
                    py::array_t<int64_t> series_array = obj.attr("astype")(DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint64_)).attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(DT_NANOTIME, 0, size);
                    ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                    break;
                }
                case DT_DATETIME:
                case DT_DATEHOUR: {
                    py::array_t<int64_t> series_array = obj.attr("astype")(DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint64_)).attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(DT_DATETIME, 0, size);
                    ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                    if (typeInfer != DT_DATETIME)
                        ddbVec = ddbVec->castTemporal(typeInfer);
                    break;
                }
                case DT_NANOTIMESTAMP:
                case DT_TIMESTAMP: {
                    py::array_t<int64_t> series_array = obj.attr("astype")(DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint64_)).attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                    break;
                }
                case DT_SYMBOL: {
                    obj = DdbPythonUtil::preserved_->pyarrow_.attr("array")(obj.attr("array"));
                    if (py::isinstance(obj, DdbPythonUtil::preserved_->pachunkedarray_))
                        obj = obj.attr("combine_chunks")();
                    py::array_t<int32_t> indices = DdbPythonUtil::preserved_->pdseries_(obj.attr("indices"), "dtype"_a=DdbPythonUtil::preserved_->pdarrowdtype_(DdbPythonUtil::preserved_->paint32_))
                                                    .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
                    py::array dictionary = obj.attr("dictionary");
                    vector<std::string> dict_string;
                    dict_string.reserve(dictionary.size());
                    for (auto &it : dictionary) {
                        dict_string.push_back(py::cast<std::string>(it));
                    }
                    size_t size = indices.size();
                    ddbVec = Util::createVector(DT_SYMBOL, size, size);
                    const int32_t *data = indices.data();
                    for (int it = 0; it < size; ++it) {
                        ddbVec->setString(it, data[it] == INT_MIN ? "" : dict_string[data[it]]);
                    }
                    break;
                }
                case DT_BLOB:
                case DT_STRING: {
                    py::array series_array = obj.attr("to_numpy")("dtype"_a="object", "na_value"_a="");
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, size, size);
                    int index = 0;
                    for (auto &it : series_array) {
                        ddbVec->setString(index, py::cast<std::string>(it));
                        ++index;
                    }
                    break;
                }
                case DT_IP: {
                    py::array series_array = obj.attr("to_numpy")("dtype"_a="object", "na_value"_a="");
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    addStringVector(ddbVec, series_array, size, 0, info);
                    break;
                }
                case DT_UUID:
                case DT_INT128: {
                    py::array series_array = obj.attr("to_numpy")("dtype"_a="object", "na_value"_a="");
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, size, size);
                    int index = 0;
                    py::object tmpObj;
                    std::string tmpstr;
                    unsigned char nullstr[16] = {0};
                    for (auto &it : series_array) {
                        tmpObj = py::cast<py::object>(obj);
                        tmpstr = py::cast<std::string>(it);
                        if (tmpstr == "") {
                            ddbVec->setBinary(index, 16, nullstr);
                        }
                        else {
                            std::reverse(tmpstr.begin(), tmpstr.end());
                            ddbVec->setBinary(index, 16, reinterpret_cast<const unsigned char*>(tmpstr.data()));
                        }
                        
                        ++index;
                    }
                    break;
                }
                // case DT_BLOB: {
                //     py::array series_array = obj.attr("to_numpy")("dtype"_a="object", "na_value"_a="");
                //     size_t size = series_array.size();
                //     ddbVec = Util::createVector(typeInfer, size, size);
                //     int index = 0;
                //     for (auto &it : series_array) {
                //         ddbVec->setString(index, py::cast<std::string>(it));
                //         ++index;
                //     }
                //     break;
                // }
                case DT_DECIMAL32:
                case DT_DECIMAL64: {
                    if (typeIndicator.second == -1) {
                        typeIndicator.second = py::cast<int>(dtype.attr("pyarrow_dtype").attr("scale"));
                    }
                    py::array series_array = obj.attr("to_numpy")("dtype"_a="object");
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size, true, typeIndicator.second);
                    if (typeInfer == DT_DECIMAL32) {
                        ddbVec->setNullFlag(addDecimalVector<int>(series_array, size, 0, INT32_MIN, typeIndicator, [&](int *buf, int size) {
                            ((FastDecimal32VectorSP)ddbVec)->appendInt(buf, size);
                        }, info));
                    }
                    else {
                        ddbVec->setNullFlag(addDecimalVector<long long>(series_array, size, 0, INT64_MIN, typeIndicator, [&](long long *buf, int size) {
                            ((FastDecimal64VectorSP)ddbVec)->appendLong(buf, size);
                        }, info));
                    }
                    break;
                }
                default:
                    throw RuntimeException("cannot convert " + py2str(dtype) + " to " + Util::getDataTypeString(typeInfer));
            }
            return ddbVec;
        }
        else {
            VectorSP ddbVec;
            switch (typeInfer) {
                case DT_BOOL:
                case DT_CHAR: {
                    py::array_t<int8_t> series_array = py::cast<py::array_t<int8_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendChar(reinterpret_cast<char*>(const_cast<int8_t*>(series_array.data())), size);
                    break;
                }
                case DT_SHORT: {
                    py::array_t<int16_t> series_array = py::cast<py::array_t<int16_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendShort(reinterpret_cast<short*>(const_cast<int16_t*>(series_array.data())), size);
                    break;
                }
                case DT_INT: {
                    py::array_t<int32_t> series_array = py::cast<py::array_t<int32_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(series_array.data())), size);
                    break;
                }
                case DT_LONG: {
                    py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                    break;
                }
                case DT_MONTH: 
                case DT_DATE:
                case DT_TIME:
                case DT_MINUTE:
                case DT_SECOND:
                case DT_DATETIME:
                case DT_DATEHOUR:
                case DT_TIMESTAMP:
                case DT_NANOTIME:
                case DT_NANOTIMESTAMP: {
                    // dtype = datetime64[ns] | datetime64[s] | datetime64[ms]
                    // In Pandas2.0 : s -> int32 | ms -> int64, need special case
                    if (!DdbPythonUtil::preserved_->pd_above_2_00_ || dtype.equal(DdbPythonUtil::preserved_->npdatetime64ns_())) {
                        py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(obj);
                        size_t size = series_array.size();
                        ddbVec = Util::createVector(DT_NANOTIMESTAMP, 0, size);
                        ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                        if (typeInfer != DT_NANOTIMESTAMP)
                            ddbVec = ddbVec->castTemporal(typeInfer);
                    }
                    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64ms_())) {
                        py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(obj);
                        size_t size = series_array.size();
                        ddbVec = Util::createVector(DT_TIMESTAMP, 0, size);
                        ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                        if (typeInfer != DT_TIMESTAMP)
                            ddbVec = ddbVec->castTemporal(typeInfer);
                    }
                    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64s_())) {
                        py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(obj);
                        size_t size = series_array.size();
                        ddbVec = Util::createVector(DT_DATETIME, 0, size);
                        ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                        if (typeInfer != DT_DATETIME)
                            ddbVec = ddbVec->castTemporal(typeInfer);
                    }
                    else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64us_())) {
                        py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(obj);
                        size_t size = series_array.size();
                        ddbVec = Util::createVector(DT_NANOTIMESTAMP, 0, size);
                        processData<long long>((long long*)series_array.data(), size, [&](long long *buf, int _size) {
                            for (int i = 0; i < _size; ++i) {
                                if (buf[i] != LLONG_MIN) {
                                    buf[i] *= 1000;
                                }
                                ddbVec->appendLong(buf, _size);
                            }
                        });
                        if (typeInfer != DT_NANOTIMESTAMP)
                            ddbVec = ddbVec->castTemporal(typeInfer);
                    }
                    else if (dtype.equal(DdbPythonUtil::preserved_->npint64_)) {
                        py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(obj);
                        size_t size = series_array.size();
                        ddbVec = Util::createVector(DT_NANOTIMESTAMP, 0, size);
                        ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
                        if (typeInfer != DT_NANOTIMESTAMP)
                            ddbVec = ddbVec->castTemporal(typeInfer);
                    }
                    else {
                        throw RuntimeException("cannot convert " + py2str(dtype) + " to " + Util::getDataTypeString(typeInfer));
                    }
                    ddbVec->setNullFlag(ddbVec->hasNull());
                    break;
                }
                case DT_FLOAT: {
                    py::array_t<float_t> series_array = py::cast<py::array_t<float_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    bool hasNull = false;
                    processData<float>(const_cast<float*>(series_array.data()), size, [&](float *buf, int size) {
                        for (int i = 0; i < size; ++i) {
                            if(isValueNumpyNull(buf[i])) {
                                buf[i] = FLT_NMIN;
                                hasNull = true;
                            }
                        }
                        ddbVec->appendFloat(buf, size);
                    });
                    ddbVec->setNullFlag(hasNull);
                    break;
                }
                case DT_DOUBLE: {
                    py::array_t<double_t> series_array = py::cast<py::array_t<double_t, py::array::f_style | py::array::forcecast>>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    bool hasNull = false;
                    processData<double>(const_cast<double*>(series_array.data()), size, [&](double *buf, int size) {
                        for (int i = 0; i < size; ++i) {
                            if(isValueNumpyNull(buf[i])) {
                                buf[i] = DBL_NMIN;
                                hasNull = true;
                            }
                        }
                        ddbVec->appendDouble(buf, size);
                    });
                    ddbVec->setNullFlag(hasNull);
                    break;
                }
                case DT_IP:
                case DT_UUID:
                case DT_INT128:
                case DT_SYMBOL:
                case DT_STRING: {
                    py::array series_array = py::cast<py::array>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(typeInfer, 0, size);
                    addStringVector(ddbVec, series_array, size, 0, info);
                    break;
                }
                case DT_BLOB: {
                    py::array series_array = py::cast<py::array>(obj);
                    size_t size = series_array.size();
                    ddbVec = Util::createVector(DT_BLOB, size, size);
                    py::object tmpObj;
                    int ni = 0;
                    for (auto &it : series_array) {
                        tmpObj = py::cast<py::object>(it);
                        if (isNULL(tmpObj)) {
                            ddbVec->setString(ni, "");
                        }
                        else {
                            ddbVec->setString(ni, py::cast<std::string>(tmpObj));
                        }
                        ++ni;
                    }
                    break;
                }
                case DT_DECIMAL32:
                case DT_DECIMAL64:
                case DT_DECIMAL128:
                case DT_ANY:
                default: {
                    throw RuntimeException("cannot convert " + py2str(dtype) + " to " + Util::getDataTypeString(typeInfer));
                }
            }
            return ddbVec;
        }
    }
    if (typeInfer != DT_OBJECT) {
        // typeInfer != DT_OBJECT &&  Is_DTypeObject ==> Indicate Explicit Type
indicatetype:
        DLOG("[typeInfer != DT_OBJECT &&  Is_DTypeObject ==> Indicate Explicit Type]");
        VectorSP ddbVec;
        py::array series_array = py::cast<py::array>(obj);
        size_t size = series_array.size();
        py::object tmpObj;
        if (typeInfer >= ARRAY_TYPE_BASE) {
            // Indicate as ARRAY VECTOR
            DLOG("is_ArrayVector.");
            Type typeExact = std::make_pair(DT_OBJECT, -1);
            Type elemType = {(DATA_TYPE)(typeInfer - ARRAY_TYPE_BASE), typeIndicator.second};
            DLOG("elemType: ", Util::getDataTypeString(elemType.first), elemType.second);
            bool isArrayVector;
            bool isNull;
            bool allNull;
            int Nullpri;
            bool canCheck = false;
            Type NullType = std::make_pair(DT_UNK, -1);
            vector<size_t> noneCounts;
            VectorSP pChildVector;
            VectorSP pAnyVector;
            noneCounts.reserve(size);
            bool ElemIsArrayLike;

            if (Util::getCategory(elemType.first) == DATA_CATEGORY::DENARY && elemType.second == -1) {
                // Get scale at first.
                size_t index = 0;
                for (auto &it : series_array) {
                    tmpObj = py::cast<py::object>(it);
                    if (!isArrayLike(tmpObj))
                        throwExceptionAboutNumpyVersion(index, py2str(it), Util::getDataTypeString(typeInfer), info);
                    allNull = true;
                    typeExact = getChildrenType(tmpObj, CHILD_VECTOR_OPTION::DISABLE, ElemIsArrayLike, allNull);
                    if (allNull) {
                        typeExact = {DT_UNK, -1};
                        noneCounts.push_back(py::len(tmpObj));
                        ++index;
                        continue;
                    }
                    if (typeExact.second == -1) {
                        throw RuntimeException("Cannot convert " + Util::getDataTypeString(typeExact.first) + " to " + Util::getDataTypeString(elemType.first) + ".");
                    }
                    if (typeIndicator.second == -1) {
                        typeIndicator.second = typeExact.second;
                        elemType.second = typeExact.second;
                    }
                    if (Util::getCategory(typeExact.first) != DATA_CATEGORY::DENARY) {
                        throw RuntimeException("Not allowed to create a Table column with type Any.");
                    }
                    if (ddbVec.isNull()) {
                        DLOG("Crate ArrayVector: ", Util::getDataTypeString(typeInfer), typeIndicator.second);
                        ddbVec = Util::createArrayVector(typeInfer, 0, size, true, typeIndicator.second == -1 ? 0 : typeIndicator.second);
                    }
                        
                    if (noneCounts.size() > 0) {
                        for (auto count : noneCounts) {
                            createAllNullVector(pChildVector, elemType.first, count);
                            ddbVec->append(pChildVector);
                        }
                        noneCounts.clear();
                    }
                    size_t len = py::len(tmpObj);
                    pChildVector = Util::createVector(elemType.first, 0, len, true, elemType.second);
                    try {
                        for (auto &item : tmpObj) {
                            pChildVector->append(toDolphinDB_Scalar(py::reinterpret_borrow<py::object>(item), elemType));
                        }
                        DLOG("here: ", pChildVector->getString());
                        pAnyVector = Util::createVector(DT_ANY, 0, 1);
                        pAnyVector->append(pChildVector);
                        ddbVec->append(pAnyVector);
                    }
                    catch(...) {
                        throwExceptionAboutNumpyVersion(index, py2str(it), Util::getDataTypeString(typeInfer), info);
                    }
                    ++index;
                }
                if (ddbVec.isNull()) {
                    ddbVec = Util::createArrayVector(typeInfer, 0, size);
                }
                if (noneCounts.size() > 0) {
                    for (auto count : noneCounts) {
                        createAllNullVector(pChildVector, elemType.first, count);
                        ddbVec->append(pChildVector);
                    }
                    noneCounts.clear();
                }
            }
            else {
                ddbVec = Util::createArrayVector(typeInfer, 0, size, true, typeIndicator.second == -1 ? 0 : typeIndicator.second);
                size_t index = 0;
                for (auto &it : series_array) {
                    tmpObj = py::cast<py::object>(it);
                    if (!isArrayLike(tmpObj))
                        throwExceptionAboutNumpyVersion(index, py2str(it), Util::getDataTypeString(typeInfer), info);
                    typeExact = getChildrenType(tmpObj, CHILD_VECTOR_OPTION::DISABLE, ElemIsArrayLike, allNull);
                    size_t len = py::len(tmpObj);
                    pChildVector = Util::createVector(elemType.first, 0, len, true, elemType.second);
                    try {
                        for (auto &item : tmpObj) {
                            pChildVector->append(toDolphinDB_Scalar(py::reinterpret_borrow<py::object>(item), elemType));
                        }
                        pAnyVector = Util::createVector(DT_ANY, 0, 1);
                        pAnyVector->append(pChildVector);
                        ddbVec->append(pAnyVector);
                    }
                    catch(...) {
                        throwExceptionAboutNumpyVersion(index, py2str(it), Util::getDataTypeString(typeInfer), info);
                    }
                    ++index;
                }
            }
            return ddbVec;
        }
        DLOG("[Infer Type and not Array Vector.]", Util::getDataTypeString(typeInfer));
        switch (typeInfer) {
            case DT_BOOL: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addBoolVector(series_array, size, 0, CHAR_MIN, typeIndicator, [&](char* buf, int size) {
                    ddbVec->appendBool(buf, size);
                }, info));
                break;
            }
            case DT_CHAR: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addCharVector(series_array, size, 0, CHAR_MIN, typeIndicator, [&](char* buf, int size) {
                    ddbVec->appendChar(buf, size);
                }, info));
                break;
            }
            case DT_SHORT: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<short>(series_array, size, 0, SHRT_MIN, typeIndicator, [&](short * buf, int size) {
                    ddbVec->appendShort(buf, size);
                }, info));
                break;
            }
            case DT_INT: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<int>(series_array, size, 0, INT_MIN, typeIndicator, [&](int * buf, int size) {
                    ddbVec->appendInt(buf, size);
                }, info));
                break;
            }
            case DT_LONG: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<long long>(series_array, size, 0, LLONG_MIN, typeIndicator, [&](long long * buf, int size) {
                    ddbVec->appendLong(buf, size);
                }, info));
                break;
            }
            case DT_MONTH:
            case DT_DATE:
            case DT_TIME:
            case DT_MINUTE:
            case DT_SECOND:
            case DT_DATETIME:
            case DT_DATEHOUR:
            case DT_TIMESTAMP:
            case DT_NANOTIME:
            case DT_NANOTIMESTAMP: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ConstantSP tmpObj;
                size_t index = 0;
                for (auto &it : series_array) {
                    try {
                        tmpObj = toDolphinDB_Scalar(py::reinterpret_borrow<py::object>(it), typeIndicator);
                        ddbVec->append(tmpObj);
                    }
                    catch (...) {
                        throwExceptionAboutNumpyVersion(
                            index, py2str(py::reinterpret_borrow<py::object>(it)), Util::getDataTypeString(typeInfer), info
                        );
                    }
                    ++index;
                }
                break;
            }
            case DT_FLOAT: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<float>(series_array, size, 0, FLT_NMIN, typeIndicator, [&](float * buf, int size) {
                    ddbVec->appendFloat(buf, size);
                }, info));
                break;
            }
            case DT_DOUBLE: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                ddbVec->setNullFlag(addNullVector<double>(series_array, size, 0, DBL_NMIN, typeIndicator, [&](double * buf, int size) {
                    ddbVec->appendDouble(buf, size);
                }, info));
                break;
            }
            case DT_IP:
            case DT_UUID:
            case DT_INT128:
            case DT_SYMBOL:
            case DT_STRING: {
                ddbVec = Util::createVector(typeInfer, 0, size);
                addStringVector(ddbVec, series_array, size, 0, info);
                break;
            }
            case DT_BLOB: {
                ddbVec = Util::createVector(DT_BLOB, size, size);
                py::object tmpObj;
                int ni = 0;
                for (auto &obj : series_array) {
                    tmpObj = py::cast<py::object>(obj);
                    if (isNULL(tmpObj)) {
                        ddbVec->setString(ni, "");
                    }
                    else {
                        ddbVec->setString(ni, py::cast<std::string>(obj));
                    }
                    ++ni;
                }
                break;
            }
            case DT_DECIMAL32:
            case DT_DECIMAL64: {
                size_t index = 0;
                int exparam = -1;
                if (typeIndicator.second == -1) {
                    // Need to Infer Exparam
                    int nullCount = 0;
                    for (auto &it : series_array) {
                        try {
                            if (isNULL(it.ptr())) {
                                ++nullCount;
                                ++index;
                                continue;
                            }
                            if (py::isinstance(it, DdbPythonUtil::preserved_->decimal_)) {
                                exparam = getPyDecimalScale(it.ptr());
                                if (exparam == -1) {
                                    ++nullCount;
                                    ++index;
                                    continue;
                                }
                                break;
                            }
                            throw RuntimeException("");
                        }
                        catch (...) {
                            throwExceptionAboutNumpyVersion(
                                index, py2str(py::reinterpret_borrow<py::object>(it)), Util::getDataTypeString(typeInfer), info
                            );
                        }
                    }
                }
                if (index == size) { // All None
                    createAllNullVector(ddbVec, typeInfer, size, 0);
                    break;
                }
                if (exparam != -1) {    // Infer exparam
                    ddbVec = Util::createVector(typeInfer, 0, size, true, exparam);
                }
                else {
                    ddbVec = Util::createVector(typeInfer, 0, size, true, typeIndicator.second);
                }
                if (typeInfer == DT_DECIMAL32) {
                    ddbVec->setNullFlag(addDecimalVector<int>(series_array, size, index, INT32_MIN, typeIndicator, [&](int *buf, int size) {
                        ((FastDecimal32VectorSP)ddbVec)->appendInt(buf, size);
                    }, info));
                    break;
                }
                else {
                    ddbVec->setNullFlag(addDecimalVector<long long>(series_array, size, index, INT64_MIN, typeIndicator, [&](long long *buf, int size) {
                        ((FastDecimal64VectorSP)ddbVec)->appendLong(buf, size);
                    }, info));
                    break;
                }
            }
            default: {
                throw RuntimeException("type error in numpy: " + Util::getDataTypeString(typeInfer));
            }
        }
        return ddbVec;
    }
inferobjecttype:
    // typeInfer == DT_OBJECT &&  Is_DTypeObject ==> Not Indicate Type
    DLOG("[typeInfer == DT_OBJECT &&  Is_DTypeObject ==> Not Indicate Type]");
    VectorSP ddbVec;
    py::array series_array = py::cast<py::array>(obj);
    size_t size = series_array.size();
    bool canCheck = false;
    bool is_ArrayVector = false;
    bool is_Null = false;
    bool all_Null = true;
    int nullpri = 0;
    Type nullType = {DT_UNK, -1};
    py::object tmpObj;
    for (auto &it : series_array) {
        tmpObj = py::reinterpret_borrow<py::object>(it);
        canCheck = checkElemType(tmpObj, typeIndicator, options, is_ArrayVector, is_Null, nullpri, nullType);
        if (!is_Null) {
            all_Null = false;
        }
        if (canCheck) {
            break;
        }
        if (typeInfer != DT_OBJECT && options == CHILD_VECTOR_OPTION::ARRAY_VECTOR) {
            break;
        }
    }
    DLOG("typeIndicator: ", Util::getDataTypeString(typeIndicator.first), typeIndicator.second);
    DLOG("is_ArrayVector: ", is_ArrayVector);
    DLOG("all_Null: ", all_Null);
    if (!is_ArrayVector && all_Null && nullType.first != DT_UNK) {
        typeIndicator = nullType;
    }
    if (all_Null) {
        if (typeInfer == DT_ANY && options == CHILD_VECTOR_OPTION::ARRAY_VECTOR) {
            typeInfer = DT_STRING;
        }
        if (typeInfer == DT_UNK || typeInfer == DT_OBJECT) {
            typeInfer = DT_DOUBLE;
        }
        createAllNullVector(ddbVec, typeInfer, size);
        return ddbVec;
    }
    if (typeInfer == DT_ANY && options == CHILD_VECTOR_OPTION::ARRAY_VECTOR) {
        if (is_ArrayVector) {
            throw RuntimeException("Not allowed to create a Table column with type Any.");
        }
        else {
            typeInfer = DT_STRING;
        }
    }
    if (is_ArrayVector) {           // options == CHILD_VERTOR_OPTION::ARRAY_VECTOR
        // Infer as ARRAY VECTOR
        DLOG("is_ArrayVector.");
        typeIndicator = {DT_UNK, -1};
        Type typeExact = std::make_pair(DT_UNK, -1);
        bool ElemIsArrayLike = false;
        vector<size_t> noneCounts;
        VectorSP pChildVector;
        VectorSP pAnyVector;
        size_t index = 0;
        for (auto &it : series_array) {
            tmpObj = py::cast<py::object>(it);
            if (!isArrayLike(tmpObj))
                throw RuntimeException("Not allowed to create a Table column with type Any.");
                // throwExceptionAboutNumpyVersion(index, py2str(it), Util::getDataTypeString(typeExact.first), info);
            all_Null = true;
            typeExact = getChildrenType(tmpObj, CHILD_VECTOR_OPTION::DISABLE, ElemIsArrayLike, all_Null);
            DLOG("typeExact: ", Util::getDataTypeString(typeExact.first), typeExact.second);
            DLOG("all_Null: ", all_Null);
            if (all_Null) {
                typeExact = {DT_UNK, -1};
                noneCounts.push_back(py::len(tmpObj));
                ++index;
                continue;
            }
            if (typeInfer == DT_UNK) {  // Not Infer Type
                typeIndicator = typeExact;
            }
            DLOG("typeIndicator: ", Util::getDataTypeString(typeIndicator.first), typeIndicator.second);
            if (typeInfer != typeExact.first) { // Infer as ANY VECTOR
                throw RuntimeException("Not allowed to create a Table column with type Any.");
            }
            if (ddbVec.isNull())
                ddbVec = Util::createArrayVector((DATA_TYPE)(typeInfer + 64), 0, size, true, typeIndicator.second == -1 ? 0 : typeIndicator.second);
            if (noneCounts.size() > 0) {
                for (auto count : noneCounts) {
                    createAllNullVector(pChildVector, typeInfer, count);
                    ddbVec->append(pChildVector);
                }
                noneCounts.clear();
            }
            size_t len = py::len(tmpObj);
            pChildVector = Util::createVector(typeInfer, 0, len, true, typeIndicator.second);
            try {
                for (auto &item : tmpObj) {
                    pChildVector->append(toDolphinDB_Scalar(py::reinterpret_borrow<py::object>(item), typeIndicator));
                }
                pAnyVector = Util::createVector(DT_ANY, 0, 1);
                pAnyVector->append(pChildVector);
                ddbVec->append(pAnyVector);
            }
            catch (...) {
                throwExceptionAboutNumpyVersion(index, py2str(it), Util::getDataTypeString(typeInfer), info);
            }
            ++index;
        }
        if (ddbVec.isNull()) {
            throw RuntimeException("Not allowed to create Array Vector with all None, Try with indicate DATA_TYPE.");
        }
        if (noneCounts.size() > 0) {
            for (auto count : noneCounts) {
                createAllNullVector(pChildVector, typeInfer, count);
                ddbVec->append(pChildVector);
            }
            noneCounts.clear();
        }
        return ddbVec;
    }

    switch (typeInfer) {
        case DT_BOOL: {
            ddbVec = Util::createVector(typeInfer, 0, size);
            ddbVec->setNullFlag(addBoolVector(series_array, size, 0, CHAR_MIN, typeIndicator, [&](char* buf, int size) {
                ddbVec->appendBool(buf, size);
            }, info));
            break;
        }
        case DT_CHAR: {
            ddbVec = Util::createVector(typeInfer, 0, size);
            ddbVec->setNullFlag(addCharVector(series_array, size, 0, CHAR_MIN, typeIndicator, [&](char* buf, int size) {
                ddbVec->appendChar(buf, size);
            }, info));
            break;
        }
        case DT_SHORT: {
            ddbVec = Util::createVector(typeInfer, 0, size);
            ddbVec->setNullFlag(addNullVector<short>(series_array, size, 0, SHRT_MIN, typeIndicator, [&](short * buf, int size) {
                ddbVec->appendShort(buf, size);
            }, info));
            break;
        }
        case DT_INT: {
            ddbVec = Util::createVector(typeInfer, 0, size);
            ddbVec->setNullFlag(addNullVector<int>(series_array, size, 0, INT_MIN, typeIndicator, [&](int * buf, int size) {
                ddbVec->appendInt(buf, size);
            }, info));
            break;
        }
        case DT_LONG: {
            ddbVec = Util::createVector(typeInfer, 0, size);
            ddbVec->setNullFlag(addNullVector<long long>(series_array, size, 0, LLONG_MIN, typeIndicator, [&](long long * buf, int size) {
                ddbVec->appendLong(buf, size);
            }, info));
            break;
        }
        case DT_MONTH:
        case DT_DATE:
        case DT_TIME:
        case DT_MINUTE:
        case DT_SECOND:
        case DT_DATETIME:
        case DT_DATEHOUR:
        case DT_TIMESTAMP:
        case DT_NANOTIME:
        case DT_NANOTIMESTAMP: {
            ddbVec = Util::createVector(typeInfer, 0, size);
            ConstantSP tmpObj;
            size_t index = 0;
            for (auto &it : series_array) {
                try {
                    tmpObj = toDolphinDB_Scalar(py::reinterpret_borrow<py::object>(it), typeIndicator);
                    ddbVec->append(tmpObj);
                }
                catch (...) {
                    throwExceptionAboutNumpyVersion(
                        index, py2str(py::reinterpret_borrow<py::object>(it)), Util::getDataTypeString(typeInfer), info
                    );
                }
                ++index;
            }
            break;
        }
        case DT_FLOAT: {
            ddbVec = Util::createVector(typeInfer, 0, size);
            ddbVec->setNullFlag(addNullVector<float>(series_array, size, 0, FLT_NMIN, typeIndicator, [&](float * buf, int size) {
                ddbVec->appendFloat(buf, size);
            }, info));
            break;
        }
        case DT_DOUBLE: {
            ddbVec = Util::createVector(typeInfer, 0, size);
            ddbVec->setNullFlag(addNullVector<double>(series_array, size, 0, DBL_NMIN, typeIndicator, [&](double * buf, int size) {
                ddbVec->appendDouble(buf, size);
            }, info));
            break;
        }
        case DT_IP:
        case DT_UUID:
        case DT_INT128:
        case DT_SYMBOL:
        case DT_STRING: {
            ddbVec = Util::createVector(typeInfer, 0, size);
            addStringVector(ddbVec, series_array, size, 0, info);
            break;
        }
        case DT_BLOB: {
            ddbVec = Util::createVector(DT_BLOB, size, size);
            py::object tmpObj;
            int ni = 0;
            for (auto &obj : series_array) {
                tmpObj = py::cast<py::object>(obj);
                if (isNULL(tmpObj)) {
                    ddbVec->setString(ni, "");
                }
                else {
                    ddbVec->setString(ni, py::cast<std::string>(obj));
                }
                ++ni;
            }
            break;
        }
        case DT_DECIMAL32:
        case DT_DECIMAL64: {
            size_t index = 0;
            int exparam = -1;
            if (typeIndicator.second == -1) {
                // Need to Infer Exparam
                int nullCount = 0;
                for (auto &it : series_array) {
                    try {
                        if (isNULL(it.ptr())) {
                            ++nullCount;
                            ++index;
                            continue;
                        }
                        if (py::isinstance(it, DdbPythonUtil::preserved_->decimal_)) {
                            exparam = getPyDecimalScale(it.ptr());
                            if (exparam == -1) {
                                ++nullCount;
                                ++index;
                                continue;
                            }
                            break;
                        }
                        throw RuntimeException("");
                    }
                    catch (...) {
                        throwExceptionAboutNumpyVersion(
                            index, py2str(py::reinterpret_borrow<py::object>(it)), Util::getDataTypeString(typeInfer), info
                        );
                    }
                }
            }
            if (index == size) { // All None
                createAllNullVector(ddbVec, typeInfer, size, 0);
                break;
            }
            if (exparam != -1) {    // Infer exparam
                ddbVec = Util::createVector(typeInfer, 0, size, true, exparam);
            }
            else {
                ddbVec = Util::createVector(typeInfer, 0, size, true, typeIndicator.second);
            }
            if (typeInfer == DT_DECIMAL32) {
                ddbVec->setNullFlag(addDecimalVector<int>(series_array, size, index, INT32_MIN, typeIndicator, [&](int *buf, int size) {
                    ((FastDecimal32VectorSP)ddbVec)->appendInt(buf, size);
                }, info));
                break;
            }
            else {
                ddbVec->setNullFlag(addDecimalVector<long long>(series_array, size, index, INT64_MIN, typeIndicator, [&](long long *buf, int size) {
                    ((FastDecimal64VectorSP)ddbVec)->appendLong(buf, size);
                }, info));
                break;
            }
        }
        case DT_ANY: {
            ddbVec = Util::createVector(typeInfer, 0, size, true, typeIndicator.second == -1 ? 0 : typeIndicator.second);
            for (auto &it : series_array) {
                tmpObj = py::reinterpret_borrow<py::object>(it);
                ConstantSP item = DdbPythonUtil::toDolphinDB(tmpObj, typeInfer == DT_ANY ? Type{DT_OBJECT, -1} : typeIndicator);
                ddbVec->append(item);
            }
        }
        default: {
            throw RuntimeException("type error in numpy: " + Util::getDataTypeString(typeInfer));
        }
    }
    return ddbVec;
}

ConstantSP toDolphinDB_Vector(py::object obj, Type typeIndicator, CHILD_VECTOR_OPTION option, TableVectorInfo info) {
    if (py::isinstance(obj, DdbPythonUtil::preserved_->pytuple_)) {
        // Indicate as DF_VECTOR
        return toDolphinDB_Vector_TupleOrList(py::reinterpret_borrow<py::tuple>(obj), typeIndicator, option);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pylist_)) {
        // Indicate as DF_VECTOR
        return toDolphinDB_Vector_TupleOrList(py::reinterpret_borrow<py::list>(obj), typeIndicator, option);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->nparray_)) {
        // Indicate as DF_VECTOR or DF_MATRIX
        return toDolphinDB_NDArray(py::reinterpret_borrow<py::array>(obj), typeIndicator, option);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pdseries_)) {
        // Indicate as DF_VECTOR
        TableVectorInfo info = {true, ""};
        return toDolphinDB_Vector_SeriesOrIndex(obj, typeIndicator, option, info);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pdindex_)) {
        // Indicate as DF_VECTOR
        TableVectorInfo info = {true, ""};
        return toDolphinDB_Vector_SeriesOrIndex(obj, typeIndicator, option, info);
    }
    else {
        throw RuntimeException("cannot convert " + py2str(obj) + " to Vector.");
    }
}

ConstantSP toDolphinDB_Scalar(py::object obj) {
    Type typeIndicator = {DT_UNK, -1};
    return toDolphinDB_Scalar(obj, typeIndicator);
}

ConstantSP toDolphinDB_Scalar(py::object obj, Type typeIndicator) {
    bool isNull = false;
    int nullpri = 0;
    if (typeIndicator.first >= ARRAY_TYPE_BASE) {
        // Element in ArrayVector is Vector
        Type elemType = {(DATA_TYPE)(typeIndicator.first-ARRAY_TYPE_BASE), typeIndicator.second};
        TableVectorInfo info = {true, ""};
        ConstantSP dataVector = toDolphinDB_Vector(obj, elemType, CHILD_VECTOR_OPTION::DISABLE, info);
        VectorSP anyVector = Util::createVector(DT_ANY, 0, 1);
        anyVector->append(dataVector);
        return anyVector;
    }
    if (typeIndicator.first == DT_UNK)
        typeIndicator = Scalar2DolphinDBType(obj, isNull, nullpri);
    DATA_TYPE &type = typeIndicator.first;
    int &exparam    = typeIndicator.second;
    // Assert type < ARRAY_TYPE_BASE
    if (py::isinstance(obj, DdbPythonUtil::preserved_->pynone_) ||
        py::isinstance(obj, DdbPythonUtil::preserved_->pdNaT_)) {
        return Util::createNullConstant(type);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pybool_)) {
        auto result = obj.cast<bool>();
        return Util::createObject(type, result);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pyint_)) {
        if (type == DT_FLOAT) {
            auto result = obj.cast<float>();
            if (isValueNumpyNull(result)) {
                return Util::createNullConstant(type);
            }
            return Util::createObject(type, result);
        }
        else if (type == DT_DOUBLE) {
            auto result = obj.cast<double>();
            if (isValueNumpyNull(result)) {
                return Util::createNullConstant(type);
            }
            return Util::createObject(type, result);
        }
        else {
            auto result = obj.cast<long long>();
            if (isValueNumpyNull(result)) {
                return Util::createNullConstant(type);
            }
            return Util::createObject(type, result);
        }
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pyfloat_)) {
        auto result = obj.cast<double>();
        if (isValueNumpyNull(result)) {
            return Util::createNullConstant(type);
        }
        return Util::createObject(type, result);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pystr_)) {
        auto result = obj.cast<std::string>();
        return Util::createObject(type, result);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->decimal_)) {
        bool hasNull=false;
        int scale = getPyDecimalScale(obj.ptr());
        if (scale == -1) {
            return Util::createNullConstant(type);
        }
        if (type == DT_DECIMAL32) {
            return Util::createObject(type, getPyDecimalData<int32_t>(obj.ptr(), hasNull), nullptr, scale);
        }
        else {
            return Util::createObject(type, getPyDecimalData<int64_t>(obj.ptr(), hasNull), nullptr, scale);
        }
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pybytes_)) {
        auto result = obj.cast<std::string>();
        return Util::createObject(type, result);
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->pdtimestamp_)) {
        long long result=obj.attr("value").cast<long long>();
        return Util::createObject(type, result/1000000);       //to ms
    }
    else if (py::isinstance(obj, DdbPythonUtil::preserved_->datetime64_)) {
        py::object dtype=getDType(obj);
        if(bool(dtype)==false){
            throw RuntimeException("unsupported scalar type ["+ py2str(obj.get_type())+"].");
        }
        long long value=obj.attr("astype")("int64").cast<long long>();
        if(isValueNumpyNull(value)){
            return Util::createNullConstant(type);
        }
        ConstantSP constobj;
        if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64ns_())) {
            constobj=Util::createNanoTimestamp(value);
            if(type==DT_NANOTIMESTAMP)
                return constobj;
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64D_())) {
            constobj=Util::createDate(value);
            if(type==DT_DATE)
                return constobj;
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64M_())) {
            //DLOG("toDolphinDBScalar_Pythontype npdatetime64M_. ");
            constobj=Util::createMonth(1970, 1 + value);
            if(type==DT_MONTH)
                return constobj;
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64m_())) {
            //DLOG("toDolphinDBScalar_Pythontype npdatetime64m_. ");
            constobj=Util::createDateTime(value*60);
            if(type==DT_DATETIME)
                return constobj;
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64s_())) {
            //DLOG("toDolphinDBScalar_Pythontype npdatetime64s_. ");
            constobj=Util::createDateTime(value);
            if(type==DT_DATETIME)
                return constobj;
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64h_())) {
            //DLOG("toDolphinDBScalar_Pythontype npdatetime64h_. ");
            constobj=Util::createDateHour(value);
            if(type==DT_DATEHOUR)
                return constobj;
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64ms_())) {
            //DLOG("toDolphinDBScalar_Pythontype npdatetime64ms_. ");
            constobj=Util::createTimestamp(value);
            if(type==DT_TIMESTAMP)
                return constobj;
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64us_())) {
            //DLOG("toDolphinDBScalar_Pythontype npdatetime64us_. ");
            constobj=Util::createNanoTimestamp(value * 1000ll);
            if(type==DT_NANOTIMESTAMP)
                return constobj;
        }
        else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64_())) {
            //DLOG("toDolphinDBScalar_Pythontype npdatetime64_. ");
            constobj = Util::createObject(type, value);
            return constobj;
        }
        else {
            throw RuntimeException("unsupported scalar numpy.datetime64 ["+ py::str(obj.get_type()).cast<std::string>()+"].");
        }
        constobj = constobj->castTemporal(type);
        return constobj;
    }
    py::object dtype;
    dtype = getDType(obj);
    if(bool(type)==false){
        throw RuntimeException("unsupported type ["+ py2str(obj.get_type())+"].");
    }
    if (dtype.equal(DdbPythonUtil::preserved_->npbool_)){
        auto result = obj.cast<bool>();
        return Util::createObject(type, result);
    } else if (dtype.equal(DdbPythonUtil::preserved_->npint8_)) {
        auto result = obj.cast<short>();
        return Util::createObject(type, result);
    } else if (dtype.equal(DdbPythonUtil::preserved_->npint16_)) {
        auto result = obj.cast<int>();
        return Util::createObject(type, result);
    } else if (dtype.equal(DdbPythonUtil::preserved_->npint32_)) {
        auto result = obj.cast<int>();
        return Util::createObject(type, result);
    } else if (dtype.equal(DdbPythonUtil::preserved_->npint64_)) {
        auto result = obj.cast<long long>();
        return Util::createObject(type, result);
    } else if (dtype.equal(DdbPythonUtil::preserved_->npfloat32_)) {
        auto result = obj.cast<float>();
        if (isValueNumpyNull(result)) {
            return Util::createNullConstant(type);
        }
        return Util::createObject(type, result);
    } else if (dtype.equal(DdbPythonUtil::preserved_->npfloat64_)) {
        auto result = obj.cast<double>();
        return Util::createObject(type, result);
    } else if (DdbPythonUtil::preserved_->np_above_1_20_ && dtype.get_type().equal(DdbPythonUtil::preserved_->npstrType_)) {
        auto result = obj.cast<std::string>();
        return Util::createObject(type, result);
    } else if (dtype.equal(DdbPythonUtil::preserved_->npdatetime64_())) {
        auto result = obj.cast<long long>();
        return Util::createObject(type, result);
    } else {
        throw RuntimeException("unrecognized scalar Python data [" + py2str(obj.get_type()) + "].");
    }
}

//tableFlag true: convert datetime to ns, like dataframe process numpy.array.
void DdbPythonUtil::createPyVector(const ConstantSP &obj,py::object &pyObject,bool tableFlag,ToPythonOption *poption){
    VectorSP ddbVec = obj;
    size_t size = ddbVec->size();
    DATA_TYPE type = obj->getType();
    RECORDTIME("createPyVector_"+std::to_string(type));
    switch (type) {
        case DT_VOID: {
            py::array pyVec(py::dtype("object"));
            pyVec.resize({size});
            pyObject=std::move(pyVec);
            break;
        }
        case DT_BOOL: {
            if (UNLIKELY(ddbVec->getNullFlag())) {
                // Play with the raw api of Python, be careful about the ref count
                DLOG("has null.");
                py::array pyVec(py::dtype("object"), {size}, {});
                PyObject **pyVecPointer = (PyObject **)pyVec.mutable_data();
                static py::none pyNone;
                static py::bool_ pyTrue(true);
                static py::bool_ pyFalse(false);
                Util::enumBoolVector(ddbVec,[&](const char *pbuf, INDEX startIndex, INDEX size){
                    for (INDEX i = 0, index = startIndex; i < size; i++, index++) {
                        if(UNLIKELY(pbuf[i] == INT8_MIN)) {
                            pyVecPointer[index] = pyNone.ptr();
                        }else{
                            if(pbuf[i] != 0)
                                pyVecPointer[index] = pyTrue.ptr();
                            else
                                pyVecPointer[index] = pyFalse.ptr();
                        }
                        Py_INCREF(pyVecPointer[index]);
                    }
                    return true;
                });
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("bool"), {size}, {});
                ddbVec->getBool(0, size, (char *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_CHAR: {
            if (UNLIKELY(ddbVec->getNullFlag())) {
                DLOG("has null.");
                py::array pyVec(py::dtype("float64"), {size}, {});
                double *p = (double *)pyVec.mutable_data();
                Util::enumCharVector(ddbVec,[&](const char *pbuf, INDEX startIndex, INDEX size){
                    for (INDEX i = 0, index = startIndex; i < size; i++, index++) {
                        if(UNLIKELY(pbuf[i] == INT8_MIN)) {
                            SET_NPNULL(p + index, 1);
                        }else{
                            p[index] = pbuf[i];
                        }
                    }
                    return true;
                });
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("int8"), {size}, {});
                ddbVec->getChar(0, size, (char *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_SHORT: {
            if (UNLIKELY(ddbVec->getNullFlag())) {
                DLOG("has null.");
                py::array pyVec(py::dtype("float64"), {size}, {});
                double *p = (double *)pyVec.mutable_data();
                Util::enumShortVector(ddbVec,[&](const short *pbuf, INDEX startIndex, INDEX size){
                    for (INDEX i = 0, index = startIndex; i < size; i++, index++) {
                        if(UNLIKELY(pbuf[i] == INT16_MIN)) {
                            SET_NPNULL(p + index, 1);
                        }else{
                            p[index] = pbuf[i];
                        }
                    }
                    return true;
                });
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("int16"), {size}, {});
                ddbVec->getShort(0, size, (short *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_INT: {
            if (UNLIKELY(ddbVec->getNullFlag())) {
                DLOG("has null.");
                py::array pyVec(py::dtype("float64"), {size}, {});
                double *p = (double *)pyVec.mutable_data();
                Util::enumIntVector(ddbVec,[&](const int *pbuf, INDEX startIndex, INDEX size){
                    for (INDEX i = 0, index = startIndex; i < size; i++, index++) {
                        if(UNLIKELY(pbuf[i] == INT32_MIN)) {
                            SET_NPNULL(p + index, 1);
                        }else{
                            p[index] = pbuf[i];
                        }
                    }
                    return true;
                });
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("int32"), {size}, {});
                ddbVec->getInt(0, size, (int *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_LONG: {
            if (UNLIKELY(ddbVec->getNullFlag())) {
                DLOG("has null.");
                py::array pyVec(py::dtype("float64"), {size}, {});
                double *p = (double *)pyVec.mutable_data();
                Util::enumLongVector(ddbVec,[&](const long long *pbuf, INDEX startIndex, INDEX size){
                    for (INDEX i = 0, index = startIndex; i < size; i++, index++) {
                        if(UNLIKELY(pbuf[i] == INT64_MIN)) {
                            SET_NPNULL(p + index, 1);
                        }else{
                            p[index] = pbuf[i];
                        }
                    }
                    return true;
                });
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("int64"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_DATE: {
            if(tableFlag) {
                py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                long long *p = (long long *)pyVec.mutable_data();
                if (UNLIKELY(ddbVec->getNullFlag())) {
                    DLOG("has null.");
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(p[i] == INT64_MIN)){
                            SET_NPNULL(p + i,1);
                        }else{
                            p[i] *= 86400000000000;
                        }
                    }
                }
                else {
                    for (size_t i = 0; i < size; ++i) {
                        p[i] *= 86400000000000;
                    }
                }
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("datetime64[D]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_MONTH: {
            if(tableFlag) {
                //It's hard to calculate Month->ns, let numpy do it better.
                py::array pyVec(py::dtype("datetime64[M]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                long long *p = (long long *)pyVec.mutable_data();
                const static long long subValue = 1970 * 12;
                if (UNLIKELY(ddbVec->getNullFlag())) {
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(p[i] == INT64_MIN)) {
                            SET_NPNULL(p + i,1);
                            continue;
                        }
                        p[i] -= subValue;
                    }
                }else{
                    for (size_t i = 0; i < size; ++i) {
                        p[i] -= subValue;
                    }
                }
                pyVec = pyVec.attr("astype")("datetime64[ns]");
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("datetime64[M]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                long long *p = (long long *)pyVec.mutable_data();
                const static long long subValue = 1970 * 12;
                if (UNLIKELY(ddbVec->getNullFlag())) {
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(p[i] == INT64_MIN)) {
                            SET_NPNULL(p + i,1);
                            continue;
                        }
                        p[i] -= subValue;
                    }
                }else{
                    for (size_t i = 0; i < size; ++i) {
                        p[i] -= subValue;
                    }
                }
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_TIME: {
            if(tableFlag) {
                py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                long long *p = (long long *)pyVec.mutable_data();
                if (UNLIKELY(ddbVec->getNullFlag())) {
                    DLOG("has null.");
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(p[i] == INT64_MIN)) {
                            SET_NPNULL(p + i,1);
                            continue;
                        }
                        p[i] *= 1000000;
                    }
                }
                else {
                    for (size_t i = 0; i < size; ++i) {
                        p[i] *= 1000000;
                    }
                }
                pyObject=std::move(pyVec);
            }
            else{
                py::array pyVec(py::dtype("datetime64[ms]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_MINUTE: {
            if(tableFlag) {
                py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                long long *p = (long long *)pyVec.mutable_data();
                if (UNLIKELY(ddbVec->getNullFlag())) {
                    DLOG("has null.");
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(p[i] == INT64_MIN)) {
                            SET_NPNULL(p + i,1);
                            continue;
                        }
                        p[i] *= 60000000000;
                    }
                }
                else {
                    for (size_t i = 0; i < size; ++i) {
                        p[i] *= 60000000000;
                    }
                }
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("datetime64[m]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                //DLOG(".datetime64[m] array.");
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_SECOND: {
            if(tableFlag) {
                py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                long long *p = (long long *)pyVec.mutable_data();
                if (UNLIKELY(ddbVec->getNullFlag())) {
                    DLOG("has null.");
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(p[i] == INT64_MIN)) {
                            SET_NPNULL(p + i,1);
                            continue;
                        }
                        p[i] *= 1000000000;
                    }
                }
                else {
                    for (size_t i = 0; i < size; ++i) {
                        p[i] *= 1000000000;
                    }
                }
                pyObject=std::move(pyVec);
            }
            else{
                py::array pyVec(py::dtype("datetime64[s]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_DATETIME: {
            if(tableFlag) {
                py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                long long *p = (long long *)pyVec.mutable_data();
                if (UNLIKELY(ddbVec->getNullFlag())) {
                    DLOG("has null.");
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(p[i] == INT64_MIN)) {
                            SET_NPNULL(p + i,1);
                            continue;
                        }
                        p[i] *= 1000000000;
                    }
                }
                else {
                    for (size_t i = 0; i < size; ++i) {
                        p[i] *= 1000000000;
                    }
                }
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("datetime64[s]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_TIMESTAMP: {
            if(tableFlag) {
                py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                long long *p = (long long *)pyVec.mutable_data();
                if (UNLIKELY(ddbVec->getNullFlag())) {
                    DLOG("has null.");
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(p[i] == INT64_MIN)) {
                            SET_NPNULL(p + i,1);
                            continue;
                        }
                        p[i] *= 1000000;
                    }
                }
                else {
                    for (size_t i = 0; i < size; ++i) {
                        p[i] *= 1000000;
                    }
                }
                pyObject=std::move(pyVec);
            }else{
                py::array pyVec(py::dtype("datetime64[ms]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_NANOTIME: {
            //Server processed Null value properly, client doesn't need to handle it any more
            py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
            ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
            pyObject=std::move(pyVec);
            break;
        }
        case DT_NANOTIMESTAMP: {
            //Server processed Null value properly, client doesn't need to handle it any more
            py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
            ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
            pyObject=std::move(pyVec);
            break;
        }
        case DT_DATEHOUR: {
            if(tableFlag) {
                py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                long long *p = (long long *)pyVec.mutable_data();
                if (UNLIKELY(ddbVec->getNullFlag())) {
                    DLOG("has null.");
                    for (size_t i = 0; i < size; ++i) {
                        if (UNLIKELY(p[i] == INT64_MIN)) {
                            SET_NPNULL(p + i,1);
                            continue;
                        }
                        p[i] *= 3600000000000ll;
                    }
                }
                else {
                    for (size_t i = 0; i < size; ++i) {
                        p[i] *= 3600000000000ll;
                    }
                }
                pyObject=std::move(pyVec);
            }
            else {
                py::array pyVec(py::dtype("datetime64[h]"), {size}, {});
                ddbVec->getLong(0, size, (long long *)pyVec.mutable_data());
                pyObject=std::move(pyVec);
            }
            break;
        }
        case DT_FLOAT: {
            py::array pyVec(py::dtype("float32"), {size}, {});
            ddbVec->getFloat(0, size, (float *)pyVec.mutable_data());
            if (UNLIKELY(ddbVec->getNullFlag())) {
                DLOG("has null.");
                float *p = (float *)pyVec.mutable_data();
                for (size_t i = 0; i < size; ++i) {
                    if (UNLIKELY(p[i] == FLT_NMIN)) {
                        SET_NPNULL(p + i,1);
                    }
                }
            }
            pyObject=std::move(pyVec);
            break;
        }
        case DT_DOUBLE: {
            py::array pyVec(py::dtype("float64"), {size}, {});
            ddbVec->getDouble(0, size, (double *)pyVec.mutable_data());
            if (UNLIKELY(ddbVec->getNullFlag())) {
                DLOG("has null.");
                double *p = (double *)pyVec.mutable_data();
                for (size_t i = 0; i < size; ++i) {
                    if (UNLIKELY(p[i] == DBL_NMIN)) {
                        SET_NPNULL(p + i,1);
                    }
                }
            }
            pyObject=std::move(pyVec);
            break;
        }
        case DT_SYMBOL: {
            py::array pyVec(py::dtype("object"), {size}, {});
            FastSymbolVector *pSymbolVector=(FastSymbolVector*)ddbVec.get();
            SymbolBaseSP symbolBase=pSymbolVector->getSymbolBase();
            int symbolBaseSize=symbolBase->size();
            PyObject** symbolStrList=new PyObject*[symbolBaseSize];
            string symbolText,emptyText;
            for(int i = 0; i < symbolBaseSize; i++){
                symbolText = symbolBase->getSymbol(i);
                symbolStrList[i] = PickleUnmarshall::decodeUtf8Text(symbolText.data(),symbolText.size());
            }
            int *pbuf=new int[size];
            pSymbolVector->getInt(0,size,pbuf);
            PyObject **pyArray = (PyObject **)pyVec.mutable_data();
            PyObject *pyStr;
            for(size_t i = 0; i < size; i++){
                if(pbuf[i] != INT_MIN){
                    pyStr=symbolStrList[pbuf[i]];
                    Py_IncRef(pyStr);
                    pyArray[i]=pyStr;
                }
            }
            for(int i=0;i<symbolBaseSize;i++){
                Py_DecRef(symbolStrList[i]);
            }
            delete[] symbolStrList;
            delete[] pbuf;
            pyObject=std::move(pyVec);
            break;
        }
        case DT_IP: {
            py::array pyVec(py::dtype("object"), {size}, {});
            PyObject **pyArray = (PyObject **)pyVec.mutable_data();
            Util::enumInt128Vector(ddbVec,[&](const Guid *pbuf, INDEX startIndex, INDEX size){
                for (INDEX i = 0, index=startIndex; i < size; i++, index++) {
                    string text = std::move(IPAddr::toString(pbuf[i].bytes()));
                    pyArray[index] = PyUnicode_FromStringAndSize(text.c_str(), text.length());
                }
                return true;
            });
            pyObject=std::move(pyVec);
            break;
        }
        case DT_UUID: {
            py::array pyVec(py::dtype("object"), {size}, {});
            PyObject **pyArray = (PyObject **)pyVec.mutable_data();
            const int buflen=36;
            char textBuf[buflen];
	        Util::enumInt128Vector(ddbVec,[&](const Guid *pbuf, INDEX startIndex, INDEX size){
                for (INDEX i = 0, index=startIndex; i < size; i++, index++) {
                    Util::toGuid(pbuf[i].bytes(), textBuf);
                    pyArray[index] = PyUnicode_FromStringAndSize(textBuf, buflen);
                }
                return true;
            });
            pyObject=std::move(pyVec);
            break;
        }
        case DT_INT128: {
            py::array pyVec(py::dtype("object"), {size}, {});
            PyObject **pyArray = (PyObject **)pyVec.mutable_data();
            const int buflen=32;
            char textBuf[buflen];
	        Util::enumInt128Vector(ddbVec,[&](const Guid *pbuf, INDEX startIndex, INDEX size){
                for (INDEX i = 0, index=startIndex; i < size; i++, index++) {
                    Util::toHex(pbuf[i].bytes(), 16, Util::LITTLE_ENDIAN_ORDER, textBuf);
                    pyArray[index] = PyUnicode_FromStringAndSize(textBuf, buflen);
                }
                return true;
            });
            pyObject=std::move(pyVec);
            break;
        }
        case DT_BLOB:
        case DT_STRING: {
            py::array pyVec(py::dtype("object"), {size}, {});
            PyObject **pyArray = (PyObject **)pyVec.mutable_data();
            Util::enumStringVector(ddbVec,[&](string **pbuf, INDEX startIndex, INDEX size){
                for (INDEX i = 0, index=startIndex; i < size; i++, index++) {
                    pyArray[index] = PickleUnmarshall::decodeUtf8Text(pbuf[i]->data(), pbuf[i]->size());
                }
                return true;
            });
            pyObject=std::move(pyVec);
            break;
        }
        case DT_DECIMAL32: {
            py::array pyVec(py::dtype("object"), {size}, {});
            PyObject **pyArray = (PyObject **)pyVec.mutable_data();
            int c_scale = ((FastDecimal32VectorSP)ddbVec)->getScale();
            auto scale = DdbPythonUtil::preserved_->decimal_(decimal_util::exp10_i32(c_scale));
            Util::enumDecimal32Vector(ddbVec,[&](const int *pbuf, INDEX startIndex, INDEX size){
                for (INDEX i = 0, index=startIndex; i < size; i++, index++) {
                    if (UNLIKELY(pbuf[i] == INT32_MIN)) {
                        pyArray[index] = py::none().ptr();
                        Py_INCREF(pyArray[index]);
                        continue;
                    }
                    bool sign = pbuf[i]>=0 ? 0 : 1;
                    vector<int> digits;
                    getDecimalDigits<int64_t>(sign ? -pbuf[i] : pbuf[i], digits);
                    py::object tuple = DdbPythonUtil::preserved_->m_decimal_.attr("DecimalTuple")(sign, digits, -1*c_scale);
                    auto decimaldata = std::move(DdbPythonUtil::preserved_->decimal_(tuple));
                    // auto decimaldata = (DdbPythonUtil::preserved_->decimal_(pbuf[i]) / scale);
                    pyArray[index] = decimaldata.ptr();
                    Py_INCREF(pyArray[index]);
                }
                return true;
            });
            pyObject=std::move(pyVec);
            break;
        }
        case DT_DECIMAL64: {
            py::array pyVec(py::dtype("object"), {size}, {});
            PyObject **pyArray = (PyObject **)pyVec.mutable_data();
            int c_scale = ((FastDecimal64VectorSP)ddbVec)->getScale();
            auto scale = DdbPythonUtil::preserved_->decimal_(decimal_util::exp10_i64(c_scale));
            Util::enumDecimal64Vector(ddbVec,[&](const int64_t *pbuf, INDEX startIndex, INDEX size){
                for (INDEX i = 0, index=startIndex; i < size; i++, index++) {
                    if (UNLIKELY(pbuf[i] == INT64_MIN)) {
                        pyArray[index] = py::none().ptr();
                        Py_INCREF(pyArray[index]);
                        continue;
                    }
                    bool sign = pbuf[i]>=0 ? 0 : 1;
                    vector<int> digits;
                    getDecimalDigits<int64_t>(sign ? -pbuf[i] : pbuf[i], digits);
                    py::object tuple = DdbPythonUtil::preserved_->m_decimal_.attr("DecimalTuple")(sign, digits, -1*c_scale);
                    auto decimaldata = std::move(DdbPythonUtil::preserved_->decimal_(tuple));
                    // auto decimaldata = (DdbPythonUtil::preserved_->decimal_(pbuf[i]) / scale);
                    pyArray[index] = decimaldata.ptr();
                    Py_INCREF(pyArray[index]);
                }
                return true;
            });
            pyObject=std::move(pyVec);
            break;
        }
        case DT_ANY: {
            // handle numpy.array of objects
            py::list list(size);
            for (size_t i = 0; i < size; ++i) {
                list[i]=toPython(ddbVec->get(i),false,poption);
            }
            pyObject = std::move(list);
            break;
        }
        default: {
            throw RuntimeException("type error in Vector convertion! ");
        };
    }
}

/*
    toPython: used to convert ConstantSP -> py::object
    
    tableFlag true: convert datetime to ns, like dataframe process numpy.array.
*/
py::object DdbPythonUtil::toPython(ConstantSP obj,bool tableFlag,ToPythonOption *poption) {
    //RECORDTIME("toPython");
    if(obj.isNull()){
        DLOG("{ toPython NULL to None. }");
        return py::none();
    }
    DLOG("{ toPython", Util::getDataTypeString(obj->getType()).data(),Util::getDataFormString(obj->getForm()).data(),obj->size(),"table",tableFlag);
    py::object pyObject;
    static ToPythonOption defaultOption;
    if(poption == NULL)
        poption = &defaultOption;
    DATA_TYPE type = obj->getType();
    DATA_FORM form = obj->getForm();
    if (form == DF_VECTOR) {
        if(type == 128 + DT_SYMBOL){
            //SymbolVector
            FastSymbolVector *symbolVector=(FastSymbolVector*)obj.get();
            size_t size = symbolVector->size();
            py::array pyVec(py::dtype("object"), {size}, {});
            for (size_t i = 0; i < size; ++i) {
                py::str temp(symbolVector->getString(i));
                Py_IncRef(temp.ptr());
                memcpy(pyVec.mutable_data(i), &temp, sizeof(py::object));
            }
            pyObject = std::move(pyVec);
        }else if(type >= ARRAY_TYPE_BASE){
            //ArrayVector
            FastArrayVector *arrayVector=(FastArrayVector*)obj.get();
            if(poption->table2List==false){
                DLOG("arrayvector to df",type);
                size_t size = arrayVector->size();
                py::array pyVec(py::dtype("object"), {size}, {});
                for (size_t i = 0; i < size; ++i) {
                    VectorSP subArray = arrayVector->get(i);
                    if (subArray->getNullFlag()) {
                        subArray->setNullFlag(subArray->hasNull());
                    }
                    //table flag set false in arrayvector
                    py::object pySubVec = toPython(subArray, false, poption);
                    Py_IncRef(pySubVec.ptr());
                    memcpy(pyVec.mutable_data(i), &pySubVec, sizeof(py::object));
                }
                pyObject = std::move(pyVec);
            }else{
                DLOG("arrayvector to list",type);
                size_t cols = arrayVector->checkVectorSize();
                if(cols < 0){
                    throw RuntimeException("array vector can't convert to a 2D array!");
                }
                VectorSP valueSP = arrayVector->getFlatValueArray();
                py::object py1darray;
                //table flag set false in arrayvector
                createPyVector(valueSP, py1darray, false, poption);
                py::array py2DVec(py1darray);
                size_t rows = arrayVector->rows();
                py2DVec.resize( {rows, cols} );
                pyObject = std::move(py2DVec);;
            }
        }else{
            createPyVector(obj,pyObject,tableFlag,poption);
        }
    } else if (form == DF_TABLE) {
        TableSP ddbTbl = obj;
        if(poption->table2List==false){
            size_t columnSize = ddbTbl->columns();
            using namespace py::literals;
            py::object dataframe;
            auto dict = py::dict();
            for(size_t i = 0; i < columnSize; ++i) {
                dict[ddbTbl->getColumnName(i).data()] = toPython(ddbTbl->getColumn(i), true, poption);
            }
            dataframe = DdbPythonUtil::preserved_->pandas_.attr("DataFrame")(dict);
            pyObject=std::move(dataframe);
        }else{
            size_t columnSize = ddbTbl->columns();
            py::list pyList(columnSize);
            for (size_t i = 0; i < columnSize; ++i) {
                py::object temp = toPython(ddbTbl->getColumn(i),true,poption);
                pyList[i] = temp;
            }
            pyObject=std::move(pyList);
        }
    } else if (form == DF_SCALAR) {
        if(obj->isNull()){
            return py::none();
        }
        switch (type) {
            case DT_VOID:
                pyObject=std::move(py::none());
                break;
            case DT_BOOL:
                pyObject=std::move(py::bool_(obj->getBool()));
                break;
            case DT_CHAR:
            case DT_SHORT:
            case DT_INT:
            case DT_LONG:
                pyObject=std::move(py::int_(obj->getLong()));
                break;
            case DT_DATE:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong(), "D"));
                break;
            case DT_MONTH:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong() - 23640, "M"));
                break;    // ddb starts from 0000.0M
            case DT_TIME:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong(), "ms"));
                break;
            case DT_MINUTE:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong(), "m"));
                break;
            case DT_SECOND:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong(), "s"));
                break;
            case DT_DATETIME:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong(), "s"));
                break;
            case DT_DATEHOUR:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong(), "h"));
                break;
            case DT_TIMESTAMP:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong(), "ms"));
                break;
            case DT_NANOTIME:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong(), "ns"));
                break;
            case DT_NANOTIMESTAMP:
                pyObject=std::move(DdbPythonUtil::preserved_->datetime64_(obj->getLong(), "ns"));
                break;
            case DT_FLOAT:
            case DT_DOUBLE:
                pyObject=std::move(py::float_(obj->getDouble()));
                break;
            case DT_IP:
            case DT_UUID:
            case DT_INT128:
            case DT_SYMBOL: {
                PyObject *tmp = PickleUnmarshall::decodeUtf8Text(obj->getString().data(), obj->getString().size());
                pyObject=py::reinterpret_borrow<py::object>(tmp);
                break;
            }
            case DT_BLOB:
            case DT_STRING: {
                PyObject *tmp = PickleUnmarshall::decodeUtf8Text(obj->getStringRef().data(), obj->getStringRef().size());
                pyObject=py::reinterpret_borrow<py::object>(tmp);
                break;
            }
            case DT_DECIMAL32: {
                auto raw_data = ((Decimal32SP)obj)->getRawData();
                auto scale = ((Decimal32SP)obj)->getScale();
                bool sign = raw_data>=0 ? 0 : 1;
                vector<int> digits;
                getDecimalDigits<int32_t>(sign ? -raw_data : raw_data, digits);
                py::object tuple = DdbPythonUtil::preserved_->m_decimal_.attr("DecimalTuple")(sign, digits, -scale);
                pyObject = std::move(DdbPythonUtil::preserved_->decimal_(tuple));
                break;
            }
            case DT_DECIMAL64: {
                auto raw_data = ((Decimal64SP)obj)->getRawData();
                auto scale = ((Decimal64SP)obj)->getScale();
                bool sign = raw_data>=0 ? 0 : 1;
                vector<int> digits;
                getDecimalDigits<int64_t>(sign ? -raw_data : raw_data, digits);
                py::object tuple = DdbPythonUtil::preserved_->m_decimal_.attr("DecimalTuple")(sign, digits, -scale);
                pyObject = std::move(DdbPythonUtil::preserved_->decimal_(tuple));
                break;
            }
            default: throw RuntimeException("type error in Scalar convertion!");
        }
    } else if (form == DF_DICTIONARY) {
        DictionarySP ddbDict = obj;
        DATA_TYPE keyType = ddbDict->getKeyType();
        if (keyType != DT_STRING && keyType != DT_SYMBOL && ddbDict->keys()->getCategory() != INTEGRAL) {
            throw RuntimeException("currently only string, symbol or integral key is supported in dictionary");
        }
        VectorSP keys = ddbDict->keys();
        VectorSP values = ddbDict->values();
        py::dict pyDict;
        if (keyType == DT_STRING) {
            for (int i = 0; i < keys->size(); ++i) { pyDict[keys->getString(i).data()] = toPython(values->get(i),false,poption); }
        } else {
            for (int i = 0; i < keys->size(); ++i) { pyDict[py::int_(keys->getLong(i))] = toPython(values->get(i),false,poption); }
        }
        pyObject=std::move(pyDict);
    } else if (form == DF_MATRIX) {
        ConstantSP ddbMat = obj;
        size_t rows = ddbMat->rows();
        size_t cols = ddbMat->columns();
        // FIXME: currently only support numerical matrix
        if (ddbMat->getCategory() == MIXED) { throw RuntimeException("currently only support single typed matrix"); }
        ddbMat->setForm(DF_VECTOR);
        py::array pyMat = toPython(ddbMat,false,poption);
        py::object pyMatRowLabel = toPython(ddbMat->getRowLabel(),false,poption);
        py::object pyMatColLabel = toPython(ddbMat->getColumnLabel(),false,poption);
        pyMat.resize({cols, rows});
        pyMat = pyMat.attr("transpose")();
        py::list pyMatList;
        pyMatList.append(pyMat);
        pyMatList.append(pyMatRowLabel);
        pyMatList.append(pyMatColLabel);
        pyObject=std::move(pyMatList);
    } else if (form == DF_PAIR) {
        VectorSP ddbPair = obj;
        py::list pyPair;
        for (int i = 0; i < ddbPair->size(); ++i) { pyPair.append(toPython(ddbPair->get(i),false,poption)); }
        pyObject=std::move(pyPair);
    } else if (form == DF_SET) {
        VectorSP ddbSet = obj->keys();
        py::set pySet;
        for (int i = 0; i < ddbSet->size(); ++i) { pySet.add(toPython(ddbSet->get(i),false,poption)); }
        pyObject=std::move(pySet);
    } else {
        throw RuntimeException("the form is not supported! ");
    }
    DLOG("toPython", Util::getDataTypeString(obj->getType()).data(),Util::getDataFormString(obj->getForm()).data(),obj->size(), tableFlag,"}");
    return pyObject;
}

py::object DdbPythonUtil::loadPickleFile(const std::string &filepath){
    py::dict statusDict;
    string shortFilePath=filepath+"_s";
    FILE *pf;
    pf=fopen(shortFilePath.data(),"rb");
    if(pf==NULL){
        pf=fopen(filepath.data(),"rb");
        if(pf==NULL){
            statusDict["errorCode"]=-1;
            statusDict["errorInfo"]=filepath+" can't open.";
            return statusDict;
        }
        FILE *pfwrite=fopen(shortFilePath.data(),"wb");
        if(pfwrite==NULL){
            statusDict["errorCode"]=-1;
            statusDict["errorInfo"]=shortFilePath+" can't open.";
            return statusDict;
        }
        char buf[4096];
        int readlen;
        {
            char tmp;
            char header=0x80;
            int version;
            while(fread(&tmp,1,1,pf)==1){
                while(tmp==header){
                    if(fread(&tmp,1,1,pf)!=1)
                        break;
                    version=(unsigned char)tmp;
                    DLOG(version);
                    if(version==4){
                        fwrite(&header,1,1,pfwrite);
                        fwrite(&tmp,1,1,pfwrite);
                        goto nextread;
                    }
                }
            }
        }
    nextread:
        while((readlen=fread(buf,1,4096,pf))>0){
            fwrite(buf,1,readlen,pfwrite);
        }
        fclose(pf);
        fclose(pfwrite);
        pf=fopen(shortFilePath.data(),"rb");
    }
    if(pf==NULL){
        statusDict["errorCode"]=-1;
        statusDict["errorInfo"]=filepath+" can't open.";
        return statusDict;
    }
    DataInputStreamSP dis=new DataInputStream(pf);
    std::unique_ptr<PickleUnmarshall> unmarshall(new PickleUnmarshall(dis));
    IO_ERR ret;
    short flag=0;
    if (!unmarshall->start(flag, true, ret)) {
        unmarshall->reset();
        statusDict["errorCode"]=(int)ret;
        statusDict["errorInfo"]="unmarshall failed";
        return statusDict;
    }
    PyObject * result = unmarshall->getPyObj();
    unmarshall->reset();
    py::object res = py::handle(result).cast<py::object>();
    res.dec_ref();
    return res;
}

PytoDdbRowPool::PytoDdbRowPool(MultithreadedTableWriter &writer)
                                : writer_(writer)
                                ,exitWhenEmpty_(false)
                                ,convertingCount_(0)
                                {
    idle_.release();
    thread_=new Thread(new ConvertExecutor(*this));
    thread_->start();
}

PytoDdbRowPool::~PytoDdbRowPool(){
    DLOG("~PytoDdbRowPool",rows_.size(),failedRows_.size());
    if(!rows_.empty()||!failedRows_.empty()){
        ProtectGil protectGil(false,"~PytoDdbRowPool");
        while(!rows_.empty()){
            delete rows_.front();
            rows_.pop();
        }
        while(!failedRows_.empty()){
            delete failedRows_.front();
            failedRows_.pop();
        }
    }
}

void PytoDdbRowPool::startExit(){
    DLOG("startExit with",rows_.size(),failedRows_.size());
    pGil_=new ProtectGil(true, "startExit");
    
    exitWhenEmpty_ = true;
    nonempty_.set();
    thread_->join();
}

void PytoDdbRowPool::endExit(){
    DLOG("endExit with",rows_.size(),failedRows_.size());
    pGil_.clear();
}

void PytoDdbRowPool::convertLoop(){
    vector<std::vector<py::object>*> convertRows;
    ErrorCodeInfo errorCodeInfo;
    while(writer_.hasError_ == false){
        nonempty_.wait();
        SemLock idleLock(idle_);
        idleLock.acquire();
        {
            LockGuard<Mutex> LockGuard(&mutex_);
            size_t size = rows_.size();
            if(size < 1){
                if(exitWhenEmpty_)
                    break;
                nonempty_.reset();
                continue;
            }
			if (size > 65535)
				size = 65535;
			convertRows.reserve(size);
            while(!rows_.empty()){
                convertRows.push_back(rows_.front());
                rows_.pop();
                if(convertRows.size() >= size)
                    break;
            }
            convertingCount_=convertRows.size();
        }
        {
            DLOG("convert start ",convertRows.size(),"/",rows_.size());
            //RECORDTIME("rowPool:ConvertExecutor");
            vector<vector<ConstantSP>*> insertRows;
            insertRows.reserve(convertRows.size());
            vector<ConstantSP> *pDdbRow = NULL;
            try
            {
                ProtectGil protectGil(false,"convertLoop");
                const DATA_TYPE *pcolType = writer_.getColType();
                int i, size;
                for (auto &prow : convertRows)
                {
                    pDdbRow = new vector<ConstantSP>;
                    size = prow->size();
                    for (i = 0; i < size; i++)
                    {
                        pDdbRow->emplace_back(toDolphinDB_Scalar(prow->at(i), {pcolType[i], -1}));
                    }
                    insertRows.emplace_back(pDdbRow);
                    pDdbRow = NULL;
                }
            }catch (RuntimeException &e){
                writer_.setError(ErrorCodeInfo::EC_InvalidObject, std::string("Data conversion error: ") + e.what());
                delete pDdbRow;
            }
            if(!insertRows.empty()){
                if(!writer_.insertUnwrittenData(insertRows,errorCodeInfo)){
                    writer_.setError(errorCodeInfo);
                    for (auto& prow : insertRows) {
                        delete prow;
                    }
                    insertRows.clear();
                }
            }
            if (insertRows.size() != convertRows.size()){                   // has error, rows left some
                LockGuard<Mutex> LockGuard(&mutex_);
                for(size_t ni = insertRows.size(); ni < convertRows.size(); ++ni){
                    failedRows_.push(convertRows[ni]);
                }
            }
            for (size_t ni = 0; ni < insertRows.size(); ++ni) {
                ProtectGil protectGil(false, "clearconvertRows");
                delete convertRows[ni];                                     // must delete it in GIL lock
            }
            DLOG("convert end ",insertRows.size(),failedRows_.size(),"/",rows_.size());
            convertingCount_ = 0;
            convertRows.clear();
        }
    }
}

void PytoDdbRowPool::getStatus(MultithreadedTableWriter::Status &status){
    ProtectGil release(true, "getStatus");
    writer_.getStatus(status);
    MultithreadedTableWriter::ThreadStatus threadStatus;
    LockGuard<Mutex> guard(&mutex_);
    status.unsentRows += rows_.size() + convertingCount_;
    status.sendFailedRows += failedRows_.size();
    
    threadStatus.unsentRows += rows_.size() + convertingCount_;
    threadStatus.sendFailedRows += failedRows_.size();
    
    status.threadStatus.insert(status.threadStatus.begin(),threadStatus);
}

void PytoDdbRowPool::getUnwrittenData(vector<vector<py::object>*> &pyData,vector<vector<ConstantSP>*> &ddbData){
    ProtectGil release(true, "getUnwrittenData");
    writer_.getUnwrittenData(ddbData);
    {
        SemLock idleLock(idle_);
        idleLock.acquire();
        LockGuard<Mutex> LockGuard(&mutex_);
        while(!failedRows_.empty()){
            pyData.push_back(failedRows_.front());
            failedRows_.pop();
        }
        while(!rows_.empty()){
            pyData.push_back(rows_.front());
            rows_.pop();
        }
    }
}

}

