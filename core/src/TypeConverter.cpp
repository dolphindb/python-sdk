#include "TypeDefine.h"
#include "TypeConverter.h"
#include "TypeHelper.h"

#include "TypeException.h"

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/pytypes.h>
#include <pybind11/stl.h>

using converter::PyObjs;
using namespace pybind11::literals;


namespace converter {

///////////////////////////////////////////////////////////////////////////
///  HELPER FUNCIONS
///////////////////////////////////////////////////////////////////////////
#ifdef DLOG
#undef DLOG
#define DLOG(...) //print(__VA_ARGS__);
template<typename T>
void print_single(const T& value) {
    std::cout << value;
}
template<typename T, typename... Args>
void print(const T& first, const Args&... args) {
    print_single(first);
    std::initializer_list<int>{ (std::cout << ' ' << args, 0)... };
    std::cout << std::endl;
}
#endif

#define NEED_TODO throw ConversionException("Need TODO!!!");
///////////////////////////////////////////////////////////////////////////

static inline py::dtype getdtype(const py::handle &obj) {
    py::dtype dtype;
    if (py::hasattr(obj, "dtype")) {
        dtype = obj.attr("dtype");
    }
    return dtype;
}


inline bool checkModuleVersionAbove(const py::module_ &module, const std::string &comparedVersion) {
    py::object Version = py::module::import("packaging.version").attr("Version");
    py::object ver1 = Version(module.attr("__version__"));
    py::object ver2 = Version(comparedVersion);
    return py::cast<bool>(py::bool_(ver1 >= ver2));
}

inline bool checkHasModule(const std::string &name) {
    py::module_ importUtil = py::module::import("importlib.util");
    return !(importUtil.attr("find_spec")(name).is_none());
}

PyCache* PyObjs::cache_ = nullptr;

PyCache::PyCache() {
    // modules
    numpy_ = py::module::import("numpy");
    np_above_1_20_ = checkModuleVersionAbove(numpy_, "1.20");

    pandas_ = py::module::import("pandas");
    pd_above_2_0_ = checkModuleVersionAbove(pandas_, "2.0");
    pd_above_1_2_ = checkModuleVersionAbove(pandas_, "1.2");

    has_arrow_ = checkHasModule("pyarrow");
    pyarrow_import_ = false;

    socket_ = py::module::import("socket");
    decimal_ = py::module::import("decimal");
    datetime_ = py::module::import("datetime");

    // instances
    // np_nan_ = numpy_.attr("nan");

    // python types
    py_none_ = py::type::of(py::none());
    py_bool_ = py::type::of(py::bool_());
    py_int_ = py::type::of(py::int_());
    py_float_ = py::type::of(py::float_());
    py_str_ = py::type::of(py::str());
    py_bytes_ = py::type::of(py::bytes());
    py_set_ = py::type::of(py::set());
    py_tuple_ = py::type::of(py::tuple());
    py_list_ = py::type::of(py::list());
    py_dict_ = py::type::of(py::dict());
    py_slice_ = py::type::of(py::slice(0, 1, 1));
    py_decimal_ = decimal_.attr("Decimal");
    py_datetime_ = datetime_.attr("datetime");
    py_date_ = datetime_.attr("date");
    py_time_ = datetime_.attr("time");
    py_timedelta_ = datetime_.attr("timedelta");

    // numpy types
    np_array_ = py::type::of(py::array());
    np_matrix_ = numpy_.attr("matrix");
    np_object_ = name2dtype("object");
    np_bool_ = name2dtype("bool");
    np_int8_ = name2dtype("int8");
    np_int16_ = name2dtype("int16");
    np_int32_ = name2dtype("int32");
    np_int64_ = name2dtype("int64");
    np_float32_ = name2dtype("float32");
    np_float64_ = name2dtype("float64");
    np_str_type_ = numpy_.attr("str_");
    np_bytes_type_ = numpy_.attr("bytes_");
    np_datetime64_type_ = numpy_.attr("datetime64");
    np_float32_type_ = numpy_.attr("float32");

    np_datetime64_ = name2dtype("datetime64");
    np_datetime64M_ = name2dtype("datetime64[M]");
    np_datetime64D_ = name2dtype("datetime64[D]");
    np_datetime64m_ = name2dtype("datetime64[m]");
    np_datetime64s_ = name2dtype("datetime64[s]");
    np_datetime64h_ = name2dtype("datetime64[h]");
    np_datetime64ms_ = name2dtype("datetime64[ms]");
    np_datetime64us_ = name2dtype("datetime64[us]");
    np_datetime64ns_ = name2dtype("datetime64[ns]");

    // pandas types
    pd_NA_ = py::type::of(pandas_.attr("NA"));
    pd_NaT_ = py::type::of(pandas_.attr("NaT"));
    pd_dataframe_ = pandas_.attr("DataFrame");
    pd_series_ = pandas_.attr("Series");
    pd_index_ = pandas_.attr("Index");
    pd_timestamp_ = pandas_.attr("Timestamp");
    pd_extension_dtype_ = pandas_.attr("core").attr("dtypes").attr("dtypes").attr("ExtensionDtype");

    // pandas extension dtypes
    pd_BooleanDtype_ = pandas_.attr("BooleanDtype")();
    if (pd_above_1_2_) {
        pd_Float32Dtype_ = pandas_.attr("Float32Dtype")();
        pd_Float64Dtype_ = pandas_.attr("Float64Dtype")();
    }
    pd_Int8Dtype_ = pandas_.attr("Int8Dtype")();
    pd_Int16Dtype_ = pandas_.attr("Int16Dtype")();
    pd_Int32Dtype_ = pandas_.attr("Int32Dtype")();
    pd_Int64Dtype_ = pandas_.attr("Int64Dtype")();
    pd_StringDtype_ = pandas_.attr("StringDtype");

    // pyarrow types
    if (has_arrow_) {
        pyarrow_ = py::module::import("pyarrow");
        pyarrow_import_ = true;
        
        pa_boolean_ = pyarrow_.attr("bool_")();
        pa_int8_ = pyarrow_.attr("int8")();
        pa_int16_ = pyarrow_.attr("int16")();
        pa_int32_ = pyarrow_.attr("int32")();
        pa_int64_ = pyarrow_.attr("int64")();
        pa_float32_ = pyarrow_.attr("float32")();
        pa_float64_ = pyarrow_.attr("float64")();
        pa_date32_ = pyarrow_.attr("date32")();
        pa_time32_s_ = pyarrow_.attr("time32")("s");
        pa_time32_ms_ = pyarrow_.attr("time32")("ms");
        pa_time64_ns_ = pyarrow_.attr("time64")("ns");
        pa_time64_ns_ = pyarrow_.attr("time64")("ns");
        pa_timestamp_s_ = pyarrow_.attr("timestamp")("s");
        pa_timestamp_ms_ = pyarrow_.attr("timestamp")("ms");
        pa_timestamp_ns_ = pyarrow_.attr("timestamp")("ns");
        pa_utf8_ = pyarrow_.attr("utf8")();
        pa_large_utf8_ = pyarrow_.attr("large_utf8")();
        pa_dictionary_type_ = pyarrow_.attr("DictionaryType");
        pa_fixed_size_binary_16_ = pyarrow_.attr("binary")(16);
        pa_large_binary_ = pyarrow_.attr("large_binary")();
        pa_decimal128_ = pyarrow_.attr("Decimal128Type");
        pa_list_ = pyarrow_.attr("ListType");
        pa_chunked_array_ = pyarrow_.attr("ChunkedArray");
        pa_table_ = pyarrow_.attr("Table");
        if (pd_above_2_0_) {
            pd_arrow_dtype_ = pandas_.attr("ArrowDtype");
        }
    }


}


TableChecker::TableChecker(const py::dict &pydict) {
    for (const auto &iter : pydict) {
        py::object key = py::reinterpret_borrow<py::object>(iter.first);
        py::object val = py::reinterpret_borrow<py::object>(iter.second);
        Type type = createType(val);
        this->insert(std::make_pair(py::cast<std::string>(key), type));
    }
}



inline bool isArrayLike(const py::handle &obj) {
    if (CHECK_INS(obj, np_array_) || CHECK_INS(obj, py_list_) || CHECK_INS(obj, py_tuple_) ||
        CHECK_INS(obj, pd_series_) || CHECK_INS(obj, pd_index_)) {
        return true;
    }
    return false;
}


bool isNULL(const py::handle &data) {
    return CHECK_INS(data, py_none_) || CHECK_INS(data, pd_NA_) || CHECK_INS(data, pd_NaT_) ||
           (py::isinstance<py::float_>(data) && std::isnan(py::cast<double>(data)));
}

// PRI for NULL
// 1: None / NA
// 2: nan(float)
// 3: nan(double)
// 4: NaT
// 5: nan(Decimal)
bool isNULL(const py::handle &data, int &pri, Type &nullType) {
    if (CHECK_INS(data, py_none_) || CHECK_INS(data, pd_NA_)) {
        pri = 1;
        nullType = {HT_VOID, EXPARAM_DEFAULT};
        return true;
    }
    else if (CHECK_INS(data, np_float32_type_) && std::isnan(py::cast<float>(data))) {
        pri = 2;
        nullType = {HT_FLOAT, EXPARAM_DEFAULT};
        return true;
    }
    else if (py::isinstance<py::float_>(data) && std::isnan(py::cast<double>(data))) {
        pri = 3;
        nullType = {HT_DOUBLE, EXPARAM_DEFAULT};
        return true;
    }
    else if (CHECK_INS(data, pd_NaT_)) {
        pri = 4;
        nullType = {HT_NANOTIMESTAMP, EXPARAM_DEFAULT};
        return true;
    }
    return false;
}


int getPyDecimalScale(const py::handle &obj) {
    py::object decTuple = obj.attr("as_tuple")();
    py::object dataExp = decTuple.attr("exponent");
    int res = EXPARAM_DEFAULT;
    if (!CHECK_INS(dataExp, py_str_)) {
        res = (-1) * py::cast<int>(dataExp);
    }
    return res;
}

int getPyDecimalScale(PyObject* obj) {
    return getPyDecimalScale(obj);
}

template <typename T>
T _getPyDecimalData_helper(const py::handle &data, bool &hasNull, int scale) {
    py::object decTuple = data.attr("as_tuple")();
    py::object dataExp = decTuple.attr("exponent");

    if (CHECK_INS(dataExp, py_str_)) {
        hasNull = true;
        return std::numeric_limits<T>::min();
    }

    py::object dataSign = decTuple.attr("sign");
    int sign = py::cast<int>(dataSign);
    py::list dataTuple = py::cast<py::list>(decTuple.attr("digits"));
    long long dec_len = dataTuple.size();
    T dec_data = 0;
    long long exp = (-1) * py::cast<long long>(dataExp);
    if (scale == EXPARAM_DEFAULT) scale = exp;
    for (auto ti = 0; ti < dec_len - exp + scale; ++ti) {
        dec_data *= 10;
        if (ti < dec_len) {
            py::object item = dataTuple[ti];
            dec_data += (T)(py::cast<int>(item));
        }
        if (dec_data < 0) {
            throw ConversionException("Decimal math overflow.");
        }
    }
    if (sign) {
        return (-1) * dec_data;
    }
    else {
        return dec_data;
    }
}


template <>
int getPyDecimalData<int>(const py::handle &data, bool &hasNull, int scale) {
    return _getPyDecimalData_helper<int>(data, hasNull, scale);
}

template <>
long long getPyDecimalData<long long>(const py::handle &data, bool &hasNull, int scale) {
    return _getPyDecimalData_helper<long long>(data, hasNull, scale);
}

template <>
int128 getPyDecimalData<int128>(const py::handle &data, bool &hasNull, int scale) {
    return _getPyDecimalData_helper<int128>(data, hasNull, scale);
}

template <typename T>
void getDecimalDigits(T raw_data, std::vector<int> &digits) {
    std::vector<int> raw_vec;
    while (raw_data > 0) {
        raw_vec.emplace_back(raw_data % 10);
        raw_data /= 10;
    }
    for (auto riter = raw_vec.rbegin(); riter != raw_vec.rend(); ++riter) {
        digits.emplace_back(*riter);
    }
}


bool appendBoolToVector(const py::array &pyVec, size_t size, size_t offset, char nullValue, const Type &type,
    std::function<void(char *, int)> f, const VectorInfo &info) {
    int bufsize = std::min(65535, (int)size);
    std::unique_ptr<char[]> bufsp(new char[bufsize]);
    char* buf = bufsp.get();
    auto buffer = pyVec.request();
    int8_t *it = reinterpret_cast<int8_t *>(buffer.ptr);
    int startIndex = offset, len, i = 0;
    bool hasNull = false;
    int stride = buffer.strides[0];
    it += stride * offset;
    PyObject **ptr = reinterpret_cast<PyObject **>(it);
    try {
        while (startIndex < size) {
            len = std::min((int)(size-startIndex), bufsize);
            for (i=0; i < len; ++i) {
                ptr = reinterpret_cast<PyObject **>(it);
                if (isNULL(*ptr)) {
                    buf[i] = nullValue;
                    hasNull = true;
                } else {
                    int8_t val = py::handle(*ptr).cast<int8_t>();
                    buf[i] = (val == (int8_t)-128) ? -128 : (int8_t)(val != 0);
                }
                it += stride;
            }
            f(buf, len);
            startIndex += len;
        }
    }
    catch (...) {
        throwExceptionAboutNumpyVersion(
            startIndex+i+offset, py2str(*ptr), getDataTypeString(type), info
        );
    }
    return hasNull;
}


bool appendCharToVector(const py::array &pyVec, size_t size, size_t offset, char nullValue, const Type &type,
    std::function<void(char *, int)> f, const VectorInfo &info) {
    int bufsize = std::min(65535, (int)size);
    std::unique_ptr<char[]> bufsp(new char[bufsize]);
    char* buf = bufsp.get();
    auto buffer = pyVec.request();
    int8_t *it = reinterpret_cast<int8_t *>(buffer.ptr);
    int startIndex = offset, len, i = 0;
    bool hasNull = false;
    int stride = buffer.strides[0];
    it += stride * offset;
    PyObject **ptr = reinterpret_cast<PyObject **>(it);
    try {
        while (startIndex < size) {
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
    }
    catch (...) {
        throwExceptionAboutNumpyVersion(
            startIndex+i+offset, py2str(*ptr), getDataTypeString(type), info
        );
    }
    return hasNull;
}


template <typename T>
bool appendNumericToVector(const py::array &pyVec, size_t size, size_t offset, T nullValue, Type type,
    std::function<void(T *, int)> f, const VectorInfo &info) {
    int bufsize = std::min(65535, (int)size);
    std::unique_ptr<T[]> bufsp(new T[bufsize]);
    T* buf = bufsp.get();
    auto buffer = pyVec.request();
    int8_t *it = reinterpret_cast<int8_t *>(buffer.ptr);
    int startIndex = offset, len, i = 0;
    bool hasNull = false;
    int stride = buffer.strides[0];
    it += stride * offset;
    PyObject **ptr = reinterpret_cast<PyObject **>(it);
    try {
        while (startIndex < size) {
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
    }
    catch (...) {
        throwExceptionAboutNumpyVersion(
            startIndex+i+offset, py2str(*ptr), getDataTypeString(type), info
        );
    }
    return hasNull;
}


template <typename T>
bool appendDecimaltoVector(const py::array &pyVec, size_t size, size_t offset, T nullValue, Type type,
    std::function<void(T *, int)> f, const VectorInfo &info) {
    int scale = type.second;
    int bufsize = std::min(65535, (int)size);
    std::unique_ptr<T[]> bufsp(new T[bufsize]);
    T* buf = bufsp.get();
    auto buffer = pyVec.request();
    int8_t *it = reinterpret_cast<int8_t *>(buffer.ptr);
    int startIndex = offset, len, i = 0;
    bool hasNull = false;
    int stride = buffer.strides[0];
    it += stride * offset;
    PyObject **ptr = reinterpret_cast<PyObject **>(it);
    try {
        while(startIndex < size) {
            len = std::min((int)(size-startIndex), bufsize);
            for (i=0; i < len; ++i) {
                ptr = reinterpret_cast<PyObject **>(it);
                if (isNULL(*ptr)) {
                    buf[i] = nullValue;
                    hasNull = true;
                } else {
                    if (!py::isinstance(*ptr, PyObjs::cache_->py_decimal_)) {
                        throw ConversionException("");
                    }
                    buf[i] = getPyDecimalData<T>(*ptr, hasNull, scale);
                }
                it += stride;
            }
            f(buf, len);
            startIndex += len;
        }
    }
    catch (...) {
        throwExceptionAboutNumpyVersion(
            startIndex+i+offset, py2str(*ptr), getDataTypeString(type), info
        );
    }
    return hasNull;
}



// TODO: [True, np.int8(12), np.int64(33)] => LONG VECTOR ? / ERROR ?
Type
checkScalarType(
    const py::handle            &obj,
    bool                        &isNull,
    int                         &nullpri,
    const CHILD_VECTOR_OPTION   &option
) {
    isNull = false;
    Type nullType;
    if (isNULL(obj, nullpri, nullType)) {
        isNull = true;
        return nullType;
    }
    if (CHECK_INS(obj, py_bool_)) {
        return {HT_BOOL, EXPARAM_DEFAULT};
    }
    if (CHECK_INS(obj, py_int_)) {
        return {HT_LONG, EXPARAM_DEFAULT};
    }
    if (CHECK_INS(obj, py_float_)) {
        return {HT_DOUBLE, EXPARAM_DEFAULT};
    }
    if (CHECK_INS(obj, py_str_)) {
        return {HT_STRING, EXPARAM_DEFAULT};
    }
    if (CHECK_INS(obj, py_bytes_)) {
        return {HT_BLOB, EXPARAM_DEFAULT};
    }
    if (CHECK_INS(obj, py_decimal_)) {
        int exparam = getPyDecimalScale(obj);
        if (exparam == EXPARAM_DEFAULT) {
            isNull = true;
            nullpri = 5;
        }
        if (exparam > 17) return {HT_DECIMAL128, exparam};
        return {HT_DECIMAL64, exparam};
    }
    if (CHECK_INS(obj, py_datetime_)) {
        return {HT_TIMESTAMP, EXPARAM_DEFAULT};
    }
    if (CHECK_INS(obj, py_date_)) {
        return {HT_DATE, EXPARAM_DEFAULT};
    }
    if (CHECK_INS(obj, py_time_)) {
        return {HT_NANOTIME, EXPARAM_DEFAULT};
    }

    if (CHECK_INS(obj, pd_timestamp_)) {
        return {HT_TIMESTAMP, EXPARAM_DEFAULT};
    }

    // HAS dtype (np.xxxx)
    py::dtype dtype = getdtype(obj);

    if (!bool(dtype)) {
        if (option == CHILD_VECTOR_OPTION::ANY_VECTOR) {
            return {HT_ANY, EXPARAM_DEFAULT};
        }
        throw ConversionException("Cannot convert " + py2str(obj.get_type()) + " to Scalar as it is an unsupported numpy dtype.");
    }

    if (CHECK_INS(obj, np_datetime64_type_)) {
        long long value = obj.attr("astype")("int64").cast<long long>();
        isNull = ISNUMPYNULL_INT64(value);
        if (isNull) {
            nullpri = 4;
        }
        if (CHECK_EQUAL(dtype, np_datetime64ns_)) {
            return {HT_NANOTIMESTAMP, EXPARAM_DEFAULT};
        }
        else if (CHECK_EQUAL(dtype, np_datetime64D_)) {
            return {HT_DATE, EXPARAM_DEFAULT};
        }
        else if (CHECK_EQUAL(dtype, np_datetime64M_)) {
            return {HT_MONTH, EXPARAM_DEFAULT};
        }
        else if (CHECK_EQUAL(dtype, np_datetime64m_)) {
            return {HT_DATETIME, EXPARAM_DEFAULT};
        }
        else if (CHECK_EQUAL(dtype, np_datetime64s_)) {
            return {HT_DATETIME, EXPARAM_DEFAULT};
        }
        else if (CHECK_EQUAL(dtype, np_datetime64h_)) {
            return {HT_DATEHOUR, EXPARAM_DEFAULT};
        }
        else if (CHECK_EQUAL(dtype, np_datetime64ms_)) {
            return {HT_TIMESTAMP, EXPARAM_DEFAULT};
        }
        else if (CHECK_EQUAL(dtype, np_datetime64us_)) {
            return {HT_NANOTIMESTAMP, EXPARAM_DEFAULT};
        }
        else if (CHECK_EQUAL(dtype, np_datetime64_)) {
            return {HT_NANOTIMESTAMP, EXPARAM_DEFAULT};
        }
        else {
            throw ConversionException("Cannot convert " + py2str(dtype) + " to Scalar as it is an unsupported numpy dtype.");
        }
    }

    if (CHECK_EQUAL(dtype, np_bool_)) {
        return {HT_BOOL, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_int8_)) {
        return {HT_CHAR, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_int16_)) {
        return {HT_SHORT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_int32_)) {
        return {HT_INT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_int64_)) {
        return {HT_LONG, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_float32_)) {
        float value = obj.cast<float>();
        if (ISNUMPYNULL_FLOAT32(value)) {
            isNull = true;
            nullpri = 2;
        }
        return {HT_FLOAT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_float64_)) {
        double value = obj.cast<double>();
        if (ISNUMPYNULL_FLOAT64(value)) {
            isNull = true;
            nullpri = 3;
        }
        return {HT_DOUBLE, EXPARAM_DEFAULT};
    }
    else if (PyObjs::cache_->np_above_1_20_ && CHECK_EQUAL(py::reinterpret_borrow<py::object>(dtype.attr("type")), np_str_type_)) {
        return {HT_STRING, EXPARAM_DEFAULT};
    }
    else {
        // str dtype will not be detected if numpy version < 1.20,
        // so just return HT_OBJECT.
        if (!PyObjs::cache_->np_above_1_20_)
            return {HT_OBJECT, EXPARAM_DEFAULT};
        throw ConversionException("Cannot convert " + py2str(dtype) + " to Scalar as it is an unsupported numpy dtype.");
    }
}


bool
checkElemType(
    const py::handle            &obj,
    Type                        &type,
    const CHILD_VECTOR_OPTION   &option,
    bool                        &is_Vector,
    bool                        &is_Null,
    int                         &nullpri,
    Type                        &nullType
) {
    is_Null = false;
    ConstantSP tmp;
    int curNullpri = 0;
    Type curType;
    if (checkInnerType(obj, tmp)) {
        if (tmp->getForm() != DATA_FORM::DF_SCALAR) {
            if (option == CHILD_VECTOR_OPTION::ANY_VECTOR)
                type.first = HT_ANY;
            else
                is_Vector = true;
            return true;
        }
        if (tmp->isNull()) {
            is_Null = true;
            curNullpri = MAX_NULL_PRI;
        }
        curType = createType(tmp);
    }
    else {
        if (isArrayLike(obj)) {
            if (option == CHILD_VECTOR_OPTION::ANY_VECTOR)
                type.first = HT_ANY;
            else
                is_Vector = true;
            return true;
        }
        curType = checkScalarType(obj, is_Null, curNullpri, option);
    }
    if (getCategory(curType) == DATA_CATEGORY::DENARY) {
        if (curType.second == EXPARAM_DEFAULT) {
            is_Null = true;
            curNullpri = 5;
            curType = {HT_DECIMAL64, EXPARAM_DEFAULT};
        }
    }
    DLOG("curType: ", curType.first, " exparam: ", curType.second, " is_Null: ", is_Null);
    if (is_Null) {
        if (curNullpri > nullpri) {
            nullpri = curNullpri;
            nullType = curType;
        }
        return false;
    }
    if (curType.first == HT_OBJECT) {
        // DType=object ==> HT_ANY
        type.first = HT_ANY;
        return true;
    }
    if (type.first == HT_OBJECT || type.first == HT_UNK) {
        type = curType;
        return false;
    }
    Type res;
    // TODO: need to speed up
    if (!canConvertTo(type, curType, type)) {
        return true;
    }
    return false;
}


Type
getChildrenType(
    const py::handle            &children,
    const CHILD_VECTOR_OPTION   &option,
    bool                        &isVector,
    bool                        &allNull,
    int                         &nullpri,
    Type                        &nullType
) {
    isVector = false;
    allNull = true;
    // int nullpri = 0;
    // Type nullType = {HT_UNK, EXPARAM_DEFAULT};
    Type type = {HT_UNK, EXPARAM_DEFAULT};
    bool isNull;
    bool canCheck = false;
    for (const auto &child : children) {
        canCheck = checkElemType(child, type, option, isVector, isNull, nullpri, nullType);
        if (allNull && !isNull) allNull = false;
        if (canCheck) break;
    }
    if (allNull) {
        type = nullType;
    }
    return type;
}


bool checkInnerType(const py::handle &data, ConstantSP &constantsp) {
    return false;
}


Type InferDataTypeFromArrowType(const py::object &pyarrow_dtype) {
    if (CHECK_INS(pyarrow_dtype, pa_list_)) {
        py::object elemDtype = pyarrow_dtype.attr("value_type");
        Type elemType = InferDataTypeFromArrowType(elemDtype);
        return {(HELPER_TYPE)(elemType.first + ARRAY_TYPE_BASE), elemType.second};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_boolean_)) {
        return {HT_BOOL, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_int8_)) {
        return {HT_CHAR, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_int16_)) {
        return {HT_SHORT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_int32_)) {
        return {HT_INT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_int64_)) {
        return {HT_LONG, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_float32_)) {
        return {HT_FLOAT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_float64_)) {
        return {HT_DOUBLE, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_date32_)) {
        return {HT_DATE, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_time32_s_)) {
        return {HT_SECOND, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_time32_ms_)) {
        return {HT_TIME, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_time64_ns_)) {
        return {HT_NANOTIME, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_timestamp_s_)) {
        return {HT_DATETIME, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_timestamp_ms_)) {
        return {HT_TIMESTAMP, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_timestamp_ns_)) {
        return {HT_NANOTIMESTAMP, EXPARAM_DEFAULT};
    }
    else if (CHECK_INS(pyarrow_dtype, pa_dictionary_type_)) {
        py::object index_type = pyarrow_dtype.attr("index_type");
        py::object value_type = pyarrow_dtype.attr("value_type");
        if (!CHECK_EQUAL(index_type, pa_int32_)) {
            goto unsupport;
        }
        if (!CHECK_EQUAL(value_type, pa_utf8_) && !CHECK_EQUAL(value_type, pa_large_utf8_)) {
            goto unsupport;
        }
        return {HT_SYMBOL, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_utf8_)) {
        return {HT_STRING, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_fixed_size_binary_16_)) {
        return {HT_INT128, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(pyarrow_dtype, pa_large_binary_)) {
        return {HT_BLOB, EXPARAM_DEFAULT};
    }
    else if (CHECK_INS(pyarrow_dtype, pa_decimal128_)) {
        int scale = py::cast<int>(pyarrow_dtype.attr("scale"));
        return {HT_DECIMAL128, scale};
    }
unsupport:
    // throw unsupport exception
    throw ConversionException("Cannot convert from pandas pyarrow dtype " + py2str(pyarrow_dtype) + ".");
}


Type InferDataTypeFromExtensionDType(const py::dtype &dtype) {
    if (CHECK_EQUAL(dtype, pd_BooleanDtype_)) {
        return {HT_BOOL, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, pd_Int8Dtype_)) {
        return {HT_CHAR, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, pd_Int16Dtype_)) {
        return {HT_SHORT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, pd_Int32Dtype_)) {
        return {HT_INT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, pd_Int64Dtype_)) {
        return {HT_LONG, EXPARAM_DEFAULT};
    }
    else if (PyObjs::cache_->pd_above_1_2_ && CHECK_EQUAL(dtype, pd_Float32Dtype_)) {
        return {HT_FLOAT, EXPARAM_DEFAULT};
    }
    else if (PyObjs::cache_->pd_above_1_2_ && CHECK_EQUAL(dtype, pd_Float64Dtype_)) {
        return {HT_DOUBLE, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype.get_type(), pd_StringDtype_)) {
        return {HT_STRING, EXPARAM_DEFAULT};
    }
    else {
        throw ConversionException("Cannot convert from pandas extension dtype " + py2str(dtype) + ".");
    }
}


Type InferDataTypeFromDType(py::dtype &dtype, bool &Is_ArrowDType, bool &Is_PandasExtensionDtype, bool needArrowCheck = false) {
    if (Is_ArrowDType || (needArrowCheck && CHECK_INS(dtype, pd_arrow_dtype_))) {
        Is_ArrowDType = true;
        py::object pyarrow_dtype = dtype.attr("pyarrow_dtype");
        return InferDataTypeFromArrowType(pyarrow_dtype);
    }
    // NumpyDtype or PandasDtype
    if (CHECK_EQUAL(dtype, np_bool_)) {
        return {HT_BOOL, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_int8_)) {
        return {HT_CHAR, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_int16_)) {
        return {HT_SHORT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_int32_)) {
        return {HT_INT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_int64_)) {
        return {HT_LONG, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_float32_)) {
        return {HT_FLOAT, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_float64_)) {
        return {HT_DOUBLE, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(py::reinterpret_borrow<py::object>(dtype.attr("type")), np_str_type_)) {
        return {HT_STRING, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(py::reinterpret_borrow<py::object>(dtype.attr("type")), np_bytes_type_)) {
        return {HT_BLOB, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_datetime64D_)) {
        return {HT_DATE, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_datetime64M_)) {
        return {HT_MONTH, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_datetime64m_)) {
        return {HT_DATETIME, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_datetime64s_)) {
        return {HT_DATETIME, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_datetime64h_)) {
        return {HT_DATEHOUR, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_datetime64ms_)) {
        return {HT_TIMESTAMP, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_datetime64us_)) {
        return {HT_NANOTIMESTAMP, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_datetime64ns_)) {
        return {HT_NANOTIMESTAMP, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_datetime64_)) {
        return {HT_NANOTIMESTAMP, EXPARAM_DEFAULT};
    }
    else if (CHECK_EQUAL(dtype, np_object_)) {
        return {HT_OBJECT, EXPARAM_DEFAULT};
    }
    else if (Is_PandasExtensionDtype || CHECK_INS(dtype, pd_extension_dtype_)) {
        Is_PandasExtensionDtype = true;
        return InferDataTypeFromExtensionDType(dtype);
    }
    else {
        throw ConversionException(std::string("Cannot convert from numpy dtype ") + py2str(dtype) + ".");
    }
}


ConstantSP
Converter::toDolphinDB(
    const py::handle    &data,
    const Type          &type
) {
    ConstantSP res;
    if (checkInnerType(data, res)) {
        return res;
    }
    if (CHECK_INS(data, pd_dataframe_)) {
        return Converter::toDolphinDB_Table_fromDataFrame(py::reinterpret_borrow<py::object>(data), converter::TableChecker(py::dict()));
    }
    if (CHECK_INS(data, py_tuple_)) {
        return Converter::toDolphinDB_Vector_fromTupleorListorSet(py::reinterpret_borrow<py::tuple>(data), type, CHILD_VECTOR_OPTION::ANY_VECTOR);
    }
    if (CHECK_INS(data, py_list_)) {
        return Converter::toDolphinDB_Vector_fromTupleorListorSet(py::reinterpret_borrow<py::list>(data), type, CHILD_VECTOR_OPTION::ANY_VECTOR);
    }
    if (CHECK_INS(data, np_array_)) {
        return Converter::toDolphinDB_VectorOrMatrix_fromNDArray(py::reinterpret_borrow<py::array>(data), type, CHILD_VECTOR_OPTION::ANY_VECTOR, ARRAY_OPTION::AO_UNSPEC);
    }
    if (CHECK_INS(data, pd_series_)) {
        return Converter::toDolphinDB_Vector_fromSeriesOrIndex(py::reinterpret_borrow<py::array>(data), type, CHILD_VECTOR_OPTION::ANY_VECTOR, VectorInfo());
    }
    if (CHECK_INS(data, pd_index_)) {
        return Converter::toDolphinDB_Vector_fromSeriesOrIndex(py::reinterpret_borrow<py::array>(data), type, CHILD_VECTOR_OPTION::ANY_VECTOR, VectorInfo());
    }
    if (CHECK_INS(data, py_set_)) {
        return Converter::toDolphinDB_Set_fromSet(py::reinterpret_borrow<py::set>(data), type);
    }
    if (CHECK_INS(data, py_dict_)) {
        return Converter::toDolphinDB_Dictionary_fromDict(py::reinterpret_borrow<py::dict>(data), type, {HT_UNK, EXPARAM_DEFAULT}, true);
    }
    return Converter::toDolphinDB_Scalar(data, type, false);
}

ConstantSP
Converter::toDolphinDB_Scalar(
    const py::handle    &data,
    Type                type,
    bool                checkInner
) {
    DLOG("[toDolphinDB_Scalar] type: ", getDataTypeString(type), " exparam: ", type.second);
    if (checkInner) {
        ConstantSP tmp;
        if (checkInnerType(data, tmp)) {
            return castTemporal(tmp, type);
        }
    }
    if (CHECK_INS(data, py_none_)) {
        return createNullConstant(type);
    }
    // FIXME: change to Util / Helper function
    if (CHECK_INS(data, py_bool_)) {
        if (type.first == HT_UNK) type.first = HT_BOOL;
        return createObject(type, py2str(py::type::of(data)).data(), py::cast<bool>(data), nullptr, false);
    }
    if (CHECK_INS(data, py_int_)) {
        if (type.first == HT_UNK) type.first = HT_LONG;
        return createObject(type, py2str(py::type::of(data)).data(), py::cast<long long>(data), nullptr, false);
    }
    if (CHECK_INS(data, py_float_)) {
        if (type.first == HT_UNK) type.first = HT_DOUBLE;
        return createObject(type, py2str(py::type::of(data)).data(), py::cast<double>(data), nullptr, false);
    }
    if (CHECK_INS(data, py_str_)) {
        if (type.first == HT_UNK) type.first = HT_STRING;
        return createObject(type, py2str(py::type::of(data)).data(), py::cast<std::string>(data), nullptr, false);
    }
    if (CHECK_INS(data, py_bytes_)) {
        if (type.first == HT_UNK) type.first = HT_BLOB;
        return createObject(type, py2str(py::type::of(data)).data(), py::cast<std::string>(data), nullptr, false);
    }
    if (CHECK_INS(data, py_decimal_)) {
        bool hasNull = false;
        // TODO: Python API default is DECIMAL64
        if (type.second == EXPARAM_DEFAULT) type.second = getPyDecimalScale(data);
        if (type.second == EXPARAM_DEFAULT) {
            if (type.first == HT_UNK) type.first = HT_DECIMAL64;
            return createNullConstant(type);
        }
        if (type.first == HT_UNK) {
            if (type.second > 17) type.first = HT_DECIMAL128;
            else type.first = HT_DECIMAL64;
        }
        DLOG("type: ", getDataTypeString(type), " exparam: ", type.second);
        if (type.first == HT_DECIMAL128) {
            return createObject(type, py2str(py::type::of(data)).data(), getPyDecimalData<int128>(data, hasNull, type.second), nullptr, true);
        }
        if (type.first == HT_DECIMAL64) {
            return createObject(type, py2str(py::type::of(data)).data(), getPyDecimalData<long long>(data, hasNull, type.second), nullptr, true);
        }
        if (type.first == HT_DECIMAL32) {
            return createObject(type, py2str(py::type::of(data)).data(), getPyDecimalData<int>(data, hasNull, type.second), nullptr, true);
        }
        return createObject(type, py2str(py::type::of(data)).data(), py::cast<double>(data), nullptr, false);
    }
    if (CHECK_INS(data, pd_NaT_)) {
        if (type.first == HT_UNK) type.first = HT_NANOTIMESTAMP;
        return createNullConstant(type);
    }
    if (CHECK_INS(data, py_datetime_)) {
        return converter::convertTemporalFromPyDateTime(data, type.first == HT_UNK ? Type{HT_TIMESTAMP, EXPARAM_DEFAULT} : type);
    }
    if (CHECK_INS(data, py_date_)) {
        return converter::convertTemporalFromPyDate(data, type.first == HT_UNK ? Type{HT_DATE, EXPARAM_DEFAULT} : type);
    }
    if (CHECK_INS(data, py_time_)) {
        return converter::convertTemporalFromPyTime(data, type.first == HT_UNK ? Type{HT_NANOTIME, EXPARAM_DEFAULT} : type);
    }
    if (CHECK_INS(data, pd_NA_)) {
        return createNullConstant(type);
    }
    if (CHECK_INS(data, pd_timestamp_)) {
        long long result = data.attr("value").cast<long long>();
        ConstantSP tmpobj = createTimestamp(result / 1000000);
        if (type.first == HT_TIMESTAMP) return tmpobj;
        if (getCategory(type) == DATA_CATEGORY::TEMPORAL) {
            return castTemporal(tmpobj, type);
        }
        ERROR_INVALIDTARGET("Timestamp", type, nullptr);
    }

    // HAS dtype (np.xxxx)
    py::dtype dtype = getdtype(data);
    if (!bool(dtype)) {
        throw ConversionException("Cannot convert " + py2str(data.get_type()) + " to Scalar as it is an unsupported python type.");
    }

    if (CHECK_INS(data, np_datetime64_type_)) {
        long long value = data.attr("astype")("int64").cast<long long>();
        ConstantSP constobj;
        if (CHECK_EQUAL(dtype, np_datetime64ns_)) {
            if (type.first == HT_UNK) type.first = HT_NANOTIMESTAMP;
            if (ISNUMPYNULL_INT64(value)) return createNullConstant(type);
            constobj = createNanoTimestamp(value);
            if (type.first == HT_NANOTIMESTAMP) return constobj;
        }
        else if (CHECK_EQUAL(dtype, np_datetime64D_)) {
            if (type.first == HT_UNK) type.first = HT_DATE;
            if (ISNUMPYNULL_INT64(value)) return createNullConstant(type);
            constobj = createDate(value);
            if (type.first == HT_DATE) return constobj;
        }
        else if (CHECK_EQUAL(dtype, np_datetime64M_)) {
            if (type.first == HT_UNK) type.first = HT_MONTH;
            if (ISNUMPYNULL_INT64(value)) return createNullConstant(type);
            constobj = createMonth(1970, 1 + value);
            if (type.first == HT_MONTH) return constobj;
        }
        else if (CHECK_EQUAL(dtype, np_datetime64m_)) {
            if (type.first == HT_UNK) type.first = HT_DATETIME;
            if (ISNUMPYNULL_INT64(value)) return createNullConstant(type);
            constobj = createDateTime(value * 60);
            if (type.first == HT_DATETIME) return constobj;
        }
        else if (CHECK_EQUAL(dtype, np_datetime64s_)) {
            if (type.first == HT_UNK) type.first = HT_DATETIME;
            if (ISNUMPYNULL_INT64(value)) return createNullConstant(type);
            constobj = createDateTime(value);
            if (type.first == HT_DATETIME) return constobj;
        }
        else if (CHECK_EQUAL(dtype, np_datetime64h_)) {
            if (type.first == HT_UNK) type.first = HT_DATEHOUR;
            if (ISNUMPYNULL_INT64(value)) return createNullConstant(type);
            constobj = createDateHour(value);
            if (type.first == HT_DATEHOUR) return constobj;
        }
        else if (CHECK_EQUAL(dtype, np_datetime64ms_)) {
            if (type.first == HT_UNK) type.first = HT_TIMESTAMP;
            if (ISNUMPYNULL_INT64(value)) return createNullConstant(type);
            constobj = createTimestamp(value);
            if (type.first == HT_TIMESTAMP) return constobj;
        }
        else if (CHECK_EQUAL(dtype, np_datetime64us_)) {
            if (type.first == HT_UNK) type.first = HT_NANOTIMESTAMP;
            if (ISNUMPYNULL_INT64(value)) return createNullConstant(type);
            constobj = createNanoTimestamp(value * 1000ll);
            if (type.first == HT_NANOTIMESTAMP) return constobj;
        }
        else if (CHECK_EQUAL(dtype, np_datetime64_)) {
            if (type.first == HT_UNK) type.first = HT_NANOTIMESTAMP;
            if (ISNUMPYNULL_INT64(value)) return createNullConstant(type);
            constobj = createNanoTimestamp(value);
            if (type.first == HT_NANOTIMESTAMP) return constobj;
        }
        else {
            throw ConversionException("Cannot convert " + py2str(dtype) + " to Scalar as it is an unsupported numpy dtype.");
        }
        constobj = converter::castTemporal(constobj, type);
        return constobj;
    }
    
    if (CHECK_EQUAL(dtype, np_bool_)) {
        if (type.first == HT_UNK) type.first = HT_BOOL;
        auto result = data.cast<bool>();
        return createObject(type, py2str(py::type::of(data)).data(), result, nullptr, false);
    }
    else if (CHECK_EQUAL(dtype, np_int8_)) {
        if (type.first == HT_UNK) type.first = HT_CHAR;
        auto result = static_cast<char>(data.cast<short>());
        return createObject(type, py2str(py::type::of(data)).data(), result, nullptr, false);
    }
    else if (CHECK_EQUAL(dtype, np_int16_)) {
        if (type.first == HT_UNK) type.first = HT_SHORT;
        auto result = static_cast<short>(data.cast<int>());
        return createObject(type, py2str(py::type::of(data)).data(), result, nullptr, false);
    }
    else if (CHECK_EQUAL(dtype, np_int32_)) {
        if (type.first == HT_UNK) type.first = HT_INT;
        auto result = data.cast<int>();
        return createObject(type, py2str(py::type::of(data)).data(), result, nullptr, false);
    }
    else if (CHECK_EQUAL(dtype, np_int64_)) {
        if (type.first == HT_UNK) type.first = HT_LONG;
        auto result = data.cast<long long>();
        return createObject(type, py2str(py::type::of(data)).data(), result, nullptr, false);
    }
    else if (CHECK_EQUAL(dtype, np_float32_)) {
        if (type.first == HT_UNK) type.first = HT_FLOAT;
        auto result = data.cast<float>();
        if (ISNUMPYNULL_FLOAT32(result)) {
            return createNullConstant(type);
        }
        return createObject(type, py2str(py::type::of(data)).data(), result, nullptr, false);
    }
    else if (CHECK_EQUAL(dtype, np_float64_)) {
        if (type.first == HT_UNK) type.first = HT_DOUBLE;
        auto result = data.cast<double>();
        if (ISNUMPYNULL_FLOAT64(result)) {
            return createNullConstant(type);
        }
        return createObject(type, py2str(py::type::of(data)).data(), result, nullptr, false);
    }
    else if (CHECK_EQUAL(py::reinterpret_borrow<py::object>(dtype.attr("type")), np_str_type_)) {
        if (type.first == HT_UNK) type.first = HT_STRING;
        auto result = data.cast<std::string>();
        return createObject(type, py2str(py::type::of(data)).data(), result, nullptr, false);
    }
    else {
        throw ConversionException("Cannot convert " + py2str(dtype) + " to Scalar as it is an unsupported numpy dtype.");
    }
}


template <typename T>
ConstantSP
_create_And_Indicate_As_Vector(
    const T                     &data,
    Type                        type,
    size_t                      size,
    VectorInfo                  info,
    typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value || std::is_same<T, py::set>::value || std::is_same<T, py::array>::value || std::is_same<T, py::object>::value, T>::type* = nullptr
) {
    // for py::list / py::tuple / py::set / py::object
    DLOG("_create_And_Indicate_As_Vector: py::list | py::tuple | py::set | py::object")
    VectorSP ddbVec;
    if (DATA_CATEGORY::DENARY == getCategory(type) && type.second == EXPARAM_DEFAULT) {
        size_t index = 0;
        bool hasNull = false;
        // Need to Infer Exparam
        int nullCount = 0;
        ConstantSP tmp;
        for (const auto &it : data) {
            try {
                if (checkInnerType(it, tmp)) {
                    if (tmp->getForm() != DATA_FORM::DF_SCALAR) throw ConversionException("");
                    if (tmp->getCategory() != DATA_CATEGORY::DENARY) throw ConversionException("");
                    if (tmp->isNull()) {
                        ++nullCount;
                        ++index;
                        continue;
                    }
                    type.second = tmp->getExtraParamForType();
                    break;
                }
                if (isNULL(it)) {
                    ++nullCount;
                    ++index;
                    continue;
                }
                if (CHECK_INS(it, py_decimal_)) {
                    int exparam = getPyDecimalScale(it);
                    if (exparam == EXPARAM_DEFAULT) {
                        ++nullCount;
                        ++index;
                        continue;
                    }
                    type.second = exparam;
                    break;
                }
                throw ConversionException("");
            }
            catch (...) {
                throwExceptionAboutNumpyVersion(
                    index, py2str(it), getDataTypeString(type), info
                );
            }
        }
        if (index == size) { // All None
            ddbVec = createAllNullVector(type, size);
            return ddbVec;
        }
    }
    ddbVec = createVector(type, 0, size);
    for (const auto &it : data) {
        ConstantSP item = Converter::toDolphinDB_Scalar(it, type, true);
        ddbVec->append(item);
    }
    return ddbVec;
}


// TODO: TRY to DELETE this ? just check like list & tuple
template <>
ConstantSP
_create_And_Indicate_As_Vector<py::array>(
    const py::array             &data,
    Type                        type,
    size_t                      size,
    VectorInfo                  info,
    typename std::enable_if<std::is_same<py::array, py::list>::value || std::is_same<py::array, py::tuple>::value || std::is_same<py::array, py::set>::value || std::is_same<py::array, py::array>::value || std::is_same<py::array, py::object>::value, py::array>::type*
) {
    // for py::array
    DLOG("_create_And_Indicate_As_Vector: py::array")
    VectorSP ddbVec;
    switch (type.first)
    {
    case HT_BOOL: {
        ddbVec = createVector(type, 0, size);
        ddbVec->setNullFlag(appendBoolToVector(data, size, 0, CHAR_MIN, type, [&](char * buf, int len) {
            VectorAppendChar(ddbVec, buf, len);
        }, info));
        break;
    }
    case HT_CHAR: {
        ddbVec = createVector(type, 0, size);
        ddbVec->setNullFlag(appendCharToVector(data, size, 0, CHAR_MIN, type, [&](char * buf, int len) {
            VectorAppendChar(ddbVec, buf, len);
        }, info));
        break;
    }
    case HT_SHORT: {
        ddbVec = createVector(type, 0, size);
        ddbVec->setNullFlag(appendNumericToVector<short>(data, size, 0, SHRT_MIN, type, [&](short * buf, int len) {
            VectorAppendShort(ddbVec, buf, len);
        }, info));
        break;
    }
    case HT_INT: {
        ddbVec = createVector(type, 0, size);
        ddbVec->setNullFlag(appendNumericToVector<int>(data, size, 0, INT_MIN, type, [&](int * buf, int len) {
            VectorAppendInt(ddbVec, buf, len);
        }, info));
        break;
    }
    case HT_LONG: {
        ddbVec = createVector(type, 0, size);
        ddbVec->setNullFlag(appendNumericToVector<long long>(data, size, 0, LLONG_MIN, type, [&](long long * buf, int len) {
            VectorAppendLong(ddbVec, buf, len);
        }, info));
        break;
    }
    case HT_MONTH:
    case HT_DATE:
    case HT_TIME:
    case HT_MINUTE:
    case HT_SECOND:
    case HT_DATETIME:
    case HT_DATEHOUR:
    case HT_TIMESTAMP:
    case HT_NANOTIME:
    case HT_NANOTIMESTAMP: {
        ddbVec = createVector(type, 0, size);
        size_t index = 0;
        for (const auto &it : data) {
            try {
                ddbVec->append(Converter::toDolphinDB_Scalar(it, type, true));
            }
            catch (...) {
                throwExceptionAboutNumpyVersion(
                    index, py2str(it), getDataTypeString(type), info
                );
            }
            ++index;
        }
        break;
    }
    case HT_FLOAT: {
        ddbVec = createVector(type, 0, size);
        ddbVec->setNullFlag(appendNumericToVector<float>(data, size, 0, FLT_NMIN, type, [&](float * buf, int len) {
            VectorAppendFloat(ddbVec, buf, len);
        }, info));
        break;
    }
    case HT_DOUBLE: {
        ddbVec = createVector(type, 0, size);
        ddbVec->setNullFlag(appendNumericToVector<double>(data, size, 0, DBL_NMIN, type, [&](double * buf, int len) {
            VectorAppendDouble(ddbVec, buf, len);
        }, info));
        break;
    }
    case HT_IP:
    case HT_UUID:
    case HT_INT128:
    case HT_SYMBOL:
    case HT_STRING: {
        ddbVec = createVector(type, 0, size);
        appendStringtoVector(ddbVec, data, size, 0, info);
        break;
    }
    case HT_BLOB: {
        ddbVec = createVector(type, size, size);
        setBlobVector(ddbVec, data, size, 0, info);
        break;
    }
    case HT_DECIMAL32:
    case HT_DECIMAL64:
    case HT_DECIMAL128: {
        size_t index = 0;
        bool hasNull = false;
        if (type.second == EXPARAM_DEFAULT) {
            // Need to Infer Exparam
            int nullCount = 0;
            ConstantSP tmp;
            for (const auto &it : data) {
                try {
                    if (checkInnerType(it, tmp)) {
                        if (tmp->getForm() != DATA_FORM::DF_SCALAR) throw ConversionException("");
                        if (tmp->getCategory() != DATA_CATEGORY::DENARY) throw ConversionException("");
                        if (tmp->isNull()) {
                            ++nullCount;
                            ++index;
                            continue;
                        }
                        type.second = tmp->getExtraParamForType();
                        break;
                    }
                    if (isNULL(it)) {
                        ++nullCount;
                        ++index;
                        continue;
                    }
                    if (CHECK_INS(it, py_decimal_)) {
                        int exparam = getPyDecimalScale(it);
                        if (exparam == EXPARAM_DEFAULT) {
                            ++nullCount;
                            ++index;
                            continue;
                        }
                        type.second = exparam;
                        break;
                    }
                    throw ConversionException("");
                }
                catch (...) {
                    throwExceptionAboutNumpyVersion(
                        index, py2str(it), getDataTypeString(type), info
                    );
                }
            }
        }
        if (index == size) { // All None
            ddbVec = createAllNullVector(type, size);
            break;
        }
        ddbVec = createVector(type, index, size);
        if (index != 0) {
            VectorFillNull(ddbVec, 0, index);
            hasNull = true;
        }
        if (type.first == HT_DECIMAL32) {
            ddbVec->setNullFlag(appendDecimaltoVector<int>(
                data, size, index, INT32_MIN, type,
                [&](int *buf, int len) {
                    VectorAppendDecimal32(ddbVec, buf, len, type.second);
                }, info) || hasNull);
        }
        else if (type.first == HT_DECIMAL64) {
            ddbVec->setNullFlag(appendDecimaltoVector<long long>(
                data, size, index, INT64_MIN, type,
                [&](long long *buf, int len) {
                    VectorAppendDecimal64(ddbVec, buf, len, type.second);
                }, info) || hasNull);
        }
        else {
            ddbVec->setNullFlag(appendDecimaltoVector<int128>(
                data, size, index, std::numeric_limits<int128>::min(), type,
                [&](int128 *buf, int len) {
                    VectorAppendDecimal128(ddbVec, buf, len, type.second);
                }, info) || hasNull);
        }
        break;
    }
    default:
        throw ConversionException("Cannot convert to data type " + getDataTypeString(type) + ".");
    }
    return ddbVec;
}


ConstantSP
_create_And_Indicate_As_AnyVector(
    const py::handle            &data,
    size_t                      size,
    VectorInfo                  info
) {
    DLOG("_create_And_Indicate_As_AnyVector");
    VectorSP ddbVec;
    ddbVec = createVector({HT_ANY, EXPARAM_DEFAULT}, 0, size);
    for (const auto &it : data) {
        ConstantSP item = Converter::toDolphinDB(it, {HT_UNK, EXPARAM_DEFAULT});
        ddbVec->append(item);
    }
    return ddbVec;
}


template <typename T>
ConstantSP
_create_And_Indicate_As_ArrayVector(
    const T                     &data,
    Type                        type,
    size_t                      size,
    VectorInfo                  info,
    typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value || std::is_same<T, py::set>::value || std::is_same<T, py::array>::value || std::is_same<T, py::object>::value, T>::type* = nullptr
) {
    VectorSP ddbVec;
    Type typeExact = {HT_UNK, EXPARAM_DEFAULT};
    Type elemType = {(HELPER_TYPE)(type.first - ARRAY_TYPE_BASE), type.second};
    DLOG("elemType: ", getDataTypeString(elemType), elemType.second);

    bool allNull;
    int nullpri = 0;
    Type nullType = {HT_UNK, EXPARAM_DEFAULT};
    bool elemIsArrayLike;
    VectorSP pChildVector;
    if (getCategory(elemType) == DATA_CATEGORY::DENARY && elemType.second == EXPARAM_DEFAULT) {
        // need to infer exparam for DECIMAL32/64/128
        size_t index = 0;
        vector<size_t> noneCounts;
        noneCounts.reserve(size);
        ConstantSP tmp;
        for (const auto &it : data) {
            size_t len = py::len(it);
            allNull = true;
            if (checkInnerType(it, tmp)) {
                if (tmp->getForm() != DATA_FORM::DF_PAIR && tmp->getForm() != DATA_FORM::DF_VECTOR) {
                    throwExceptionAboutNumpyVersion(index, py2str(it), getDataTypeString(type), info);
                }
                allNull = false;
                typeExact = createType(it);
            }
            else {
                if (!isArrayLike(it))
                    throwExceptionAboutNumpyVersion(index, py2str(it), getDataTypeString(type), info);
                typeExact = getChildrenType(it, CHILD_VECTOR_OPTION::NORMAL_VECTOR, elemIsArrayLike, allNull, nullpri, nullType); 
            }
            if (allNull) {
                typeExact = {HT_UNK, EXPARAM_DEFAULT};
                noneCounts.push_back(len);
                ++index;
                continue;
            }
            if (type.second == EXPARAM_DEFAULT) {
                type.second = typeExact.second;
                elemType.second = typeExact.second;
            }
            if (getCategory(typeExact) != DATA_CATEGORY::DENARY) {
                throw ConversionException("Cannot convert object with ANY type to an ArrayVector.");
            }
            if (ddbVec.isNull()) {
                DLOG("Crate ArrayVector: ", getDataTypeString(type), type.second);
                ddbVec = createArrayVector(type, 0, 0, size, size);
            }

            if (noneCounts.size() > 0) {
                for (auto &count : noneCounts) {
                    pChildVector = createAllNullVector(elemType, count);
                    VectorAppendChild(ddbVec, pChildVector);
                }
                noneCounts.clear();
            }
            
            pChildVector = createVector(elemType, 0, len);
            try {
                for (const auto &item : it) {
                    pChildVector->append(Converter::toDolphinDB_Scalar(item, elemType, true));
                }
                VectorAppendChild(ddbVec, pChildVector);
            }
            catch (...) {
                throwExceptionAboutNumpyVersion(index, py2str(it), getDataTypeString(type), info);
            }
            ++index;
        }
        if (ddbVec.isNull()) {
            ddbVec = createArrayVector(type, 0, 0, size, size);
        }
        if (noneCounts.size() > 0) {
            for (const auto &count : noneCounts) {
                pChildVector = createAllNullVector(elemType, count);
                VectorAppendChild(ddbVec, pChildVector);
            }
            noneCounts.clear();
        }
    }
    else {
        ddbVec = createArrayVector(type, 0, 0, size, size);
        size_t index = 0;
        for (const auto &it : data) {
            try {
                pChildVector = Converter::toDolphinDB_Vector(it, elemType, CHILD_VECTOR_OPTION::NORMAL_VECTOR, info);
                if (pChildVector->getForm() != DATA_FORM::DF_PAIR && pChildVector->getForm() != DATA_FORM::DF_VECTOR) {
                    throw ConversionException("...");
                }
                VectorAppendChild(ddbVec, pChildVector);
            }
            catch (...) {
                throwExceptionAboutNumpyVersion(index, py2str(it), getDataTypeString(type), info);
            }
            ++index;
        }
    }
    return ddbVec;
}


template <typename T>
ConstantSP
_create_And_Infer_As_ArrayVector(
    const T                     &data,
    size_t                      size,
    VectorInfo                  info,
    typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value || std::is_same<T, py::set>::value || std::is_same<T, py::array>::value || std::is_same<T, py::object>::value, T>::type* = nullptr
) {
    DLOG("[_create_And_Infer_As_ArrayVector]");
    Type type = {HT_UNK, EXPARAM_DEFAULT};
    Type typeExact = {HT_UNK, EXPARAM_DEFAULT};
    bool all_Null;
    bool ElemIsArrayLike = false;
    int nullpri = 0;
    Type nullType = {HT_UNK, EXPARAM_DEFAULT};
    size_t index = 0;

    ConstantSP tmp;
    for (const auto &it : data) {
        all_Null = true;
        if (checkInnerType(it, tmp)) {
            if (tmp->getForm() != DATA_FORM::DF_PAIR && tmp->getForm() != DATA_FORM::DF_VECTOR) {
                throwExceptionAboutNumpyVersion(index, py2str(it), getDataTypeString(type), info);
            }
            all_Null = false;
            typeExact = createType(tmp);
        }
        else {
            if (!isArrayLike(it))
                throwExceptionAboutNumpyVersion(index, py2str(it), getDataTypeString(type), info);
            typeExact = getChildrenType(it, CHILD_VECTOR_OPTION::NORMAL_VECTOR, ElemIsArrayLike, all_Null, nullpri, nullType);
        }
        DLOG("typeExact: ", getDataTypeString(typeExact), typeExact.second);
        DLOG("all_Null: ", all_Null);
        if (all_Null) {
            typeExact = {HT_UNK, EXPARAM_DEFAULT};
            ++index;
            continue;
        }
        if (type.first == HT_UNK) {
            type = typeExact;
        }
        if (!canConvertTo(type, typeExact, type)) {
            throw ConversionException("Cannot create an ArrayVector with mixing incompatible types.");
        }
        ++index;
    }
    if (type.first == HT_UNK) {
        type = nullType;
    }
    if (type.first == HT_VOID) {
        throw ConversionException("Cannot create an ArrayVector with VOID type.");
    }
    return _create_And_Indicate_As_ArrayVector(data, {(HELPER_TYPE)(type.first + ARRAY_TYPE_BASE), type.second}, size, info);
}


/*
 * Create and Infer with Object
 * 1. py::tuple / py::list / arrayLike(Series / Index) && dtype == object
 * 2. py::array && dtype == object
**/
template <typename T>
ConstantSP
_create_And_Infer_with_Object(
    const T                     &data,
    Type                        type,
    size_t                      size,
    const CHILD_VECTOR_OPTION   &option,
    VectorInfo                  info,
    typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value || std::is_same<T, py::set>::value || std::is_same<T, py::array>::value || std::is_same<T, py::object>::value, T>::type* = nullptr
) {
    DLOG("[_create_And_Infer_with_Object] type: ", getDataTypeString(type));
    if (type.first != HT_UNK && type.first != HT_OBJECT) {
        // Indicate type
        if ((int)type.first >= ARRAY_TYPE_BASE) {
            // Indicate as ARRAY_VECTOR
            return _create_And_Indicate_As_ArrayVector(data, type, size, info);
        }
        if (type.first == HT_ANY) {
            // Indicate as ANY_VECTOR
            return _create_And_Indicate_As_AnyVector(data, size, info);
        }
        // Indicate as VECTOR (array: dtpye=object / tuple / list)
        return _create_And_Indicate_As_Vector(data, type, size, info);
    }

    // Infer as UNK
    VectorSP ddbVec;

    bool canCheck = false;
    bool is_ArrayVector = false;
    bool is_Null = false;
    bool all_Null = true;
    int nullpri = 0;
    Type nullType = {HT_UNK, EXPARAM_DEFAULT};

    for (const auto &it : data) {
        canCheck = checkElemType(it, type, option, is_ArrayVector, is_Null, nullpri, nullType);
        if (!is_Null) {
            all_Null = false;
        }
        if (canCheck) {
            break;
        }
    }

    DLOG("canCheck: ", canCheck, " type: ", getDataTypeString(type), " exparam: ", type.second);
    DLOG("nullType: ", getDataTypeString(nullType), " nullpri: ", nullpri);
    DLOG("isCol: ", info.isCol, " colName: ", info.colName);

    if (all_Null) {
        if (nullType.first != HT_UNK) {
            type = nullType;
        }
        if ((option == CHILD_VECTOR_OPTION::SET_VECTOR || option == CHILD_VECTOR_OPTION::KEY_VECTOR) && (type.first == HT_UNK || type.first == HT_VOID)) {
            throwExceptionAboutChildOption(option, "containing only null elements.");
        }
        if (!info.colName.empty() && type.first == HT_VOID && option == CHILD_VECTOR_OPTION::ARRAY_VECTOR) {
            type.first = HT_STRING;
        }
        if (type.first == HT_VOID) type.first = HT_ANY;
    }

    if (option != CHILD_VECTOR_OPTION::ANY_VECTOR && type.first == HT_ANY) {
        throwExceptionAboutChildOption(option, "with mixing incompatible types.");
    }

    if (type.first == HT_ANY) {
        return _create_And_Indicate_As_AnyVector(data, size, info);
    }

    if (option == CHILD_VECTOR_OPTION::NORMAL_VECTOR && is_ArrayVector) {
        throw ConversionException("Cannot convert the specified object to a 1D Vector as it contains array-like elements.");
    }

    if (option == CHILD_VECTOR_OPTION::ARRAY_VECTOR && is_ArrayVector) {
        // Infer as ARRAY VECTOR
        return _create_And_Infer_As_ArrayVector(data, size, info);
    }

    if (all_Null) {
        Type finalType = (type.first == HT_UNK || type.first == HT_OBJECT) ? Type{HT_DOUBLE, EXPARAM_DEFAULT} : type;
        ddbVec = createAllNullVector(finalType, size);
        return ddbVec;
    }
    // TODO: NEED TO SPEED UP LIKE API
    ddbVec = createVector(type, 0, size);
    for (const auto &it : data) {
        ConstantSP item = Converter::toDolphinDB_Scalar(it, type, true);
        ddbVec->append(item);
    }
    return ddbVec;
}


ConstantSP
Converter::toDolphinDB_Vector(
    const py::handle            &data,
    Type                        type,
    const CHILD_VECTOR_OPTION   &option,
    const VectorInfo            &info,
    bool                        checkInner
) {
    if (checkInner) {
        ConstantSP res;
        if (checkInnerType(data, res)) {
            return castTemporal(res, type);
        }
    }
    if (CHECK_INS(data, py_list_)) {
        return Converter::toDolphinDB_Vector_fromTupleorListorSet(py::reinterpret_borrow<py::list>(data), type, option, info);
    }
    else if (CHECK_INS(data, py_tuple_)) {
        return Converter::toDolphinDB_Vector_fromTupleorListorSet(py::reinterpret_borrow<py::tuple>(data), type, option, info);
    }
    else if (CHECK_INS(data, np_array_)) {
        return Converter::toDolphinDB_VectorOrMatrix_fromNDArray(py::reinterpret_borrow<py::array>(data), type, option, ARRAY_OPTION::AO_VECTOR, info);
    }
    else if (CHECK_INS(data, pd_series_)) {
        return Converter::toDolphinDB_Vector_fromSeriesOrIndex(py::reinterpret_borrow<py::object>(data), type, option, info);
    }
    else if (CHECK_INS(data, pd_index_)) {
        return Converter::toDolphinDB_Vector_fromSeriesOrIndex(py::reinterpret_borrow<py::object>(data), type, option, info);
    }
    else {
        throw ConversionException("Cannot convert " + py2str(data.get_type()) + " to Vector as it is an unsupported python type.");
    }
}


ConstantSP
Converter::toDolphinDB_Matrix(
    const py::handle            &data,
    Type                        type,
    bool                        checkInner
) {
    if (checkInner) {
        ConstantSP tmp;
        if (checkInnerType(data, tmp)) {
            return castTemporal(tmp, type);
        }
    }
    if (CHECK_INS(data, py_list_)) {
        return Converter::toDolphinDB_Matrix_fromTupleOrListOrNDArray(py::reinterpret_borrow<py::list>(data), type);
    }
    else if (CHECK_INS(data, py_tuple_)) {
        return Converter::toDolphinDB_Matrix_fromTupleOrListOrNDArray(py::reinterpret_borrow<py::tuple>(data), type);
    }
    else if (CHECK_INS(data, np_array_)) {
        return Converter::toDolphinDB_VectorOrMatrix_fromNDArray(
            py::reinterpret_borrow<py::array>(data), type,
            CHILD_VECTOR_OPTION::NORMAL_VECTOR, ARRAY_OPTION::AO_MATRIX
        );
    }
    else {
        throw ConversionException("Cannot convert " + py2str(data.get_type()) + " to Matrix as it is an unsupported python type.");
    }
}


template <typename T>
ConstantSP
Converter::toDolphinDB_Vector_fromTupleorListorSet(
    const T                     &data,
    Type                        type,
    const CHILD_VECTOR_OPTION   &option,
    const VectorInfo            &info,
    typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value || std::is_same<T, py::set>::value, T>::type*
) {
    size_t size = data.size();
    return _create_And_Infer_with_Object(data, type, size, option, info);
}


template <typename T>
ConstantSP _create_And_Indicate_As_2DMatrix(
    const T                     &data,
    Type                        type,
    size_t                      rows,
    size_t                      cols,
    typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value || std::is_same<T, py::array>::value, T>::type* = nullptr
) {
    size_t col = 0;
    std::vector<ConstantSP> columns;
    for (const auto &it : data) {
        VectorInfo info(col);
        columns.push_back(Converter::toDolphinDB_Vector(it, type, CHILD_VECTOR_OPTION::NORMAL_VECTOR, info));
        if (rows != columns.back()->size()) {
            throw ConversionException("Each column of the Matrix must have the same length.");
        }
    }
    return createMatrixWithVectors(columns, type, cols, rows);
}


template <typename T>
ConstantSP _create_And_Indicate_As_Matrix(
    const T                     &data,
    Type                        type,
    size_t                      cols,
    typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value || std::is_same<T, py::array>::value, T>::type* = nullptr
) {
    int dim = -1;
    size_t rows = 1;

    dim = isArrayLike(data[0]) ? 2 : 1;
    if (dim == 2) {
        rows = py::len(data[0]);
    }
    
    if (dim == 1) {
        VectorSP v = Converter::toDolphinDB_Vector(data, type, CHILD_VECTOR_OPTION::NORMAL_VECTOR);
        ConstantSP matrix = createMatrixWithVector(v, cols, rows);
        return matrix;
    }

    // dim == 2
    return _create_And_Indicate_As_2DMatrix(data, type, rows, cols);
}


template <typename T>
ConstantSP
_create_And_Infer_As_2DMatrix(
    const T                     &data,
    size_t                      rows,
    size_t                      cols,
    typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value || std::is_same<T, py::array>::value, T>::type* = nullptr
) {
    VectorSP ddbVec;
    Type type = {HT_UNK, EXPARAM_DEFAULT};
    Type typeExact = {HT_UNK, EXPARAM_DEFAULT};
    bool all_Null;
    bool ElemIsArrayLike = false;
    int nullpri = 0;
    Type nullType = {HT_UNK, EXPARAM_DEFAULT};
    size_t index = 0;

    for (const auto &it : data) {
        all_Null = true;
        ConstantSP tmp;
        if (checkInnerType(it, tmp)) {
            if (tmp->getForm() != DATA_FORM::DF_PAIR && tmp->getForm() != DATA_FORM::DF_VECTOR) {
                throw ConversionException("Cannot create a Matrix with mixing incompatible types.");
            }
            all_Null = false;
            typeExact = createType(tmp);
        }
        else {
            if (!isArrayLike(it))
                throw ConversionException("Cannot create a Matrix with mixing incompatible types.");
            typeExact = getChildrenType(it, CHILD_VECTOR_OPTION::NORMAL_VECTOR, ElemIsArrayLike, all_Null, nullpri, nullType);
        }
        if (all_Null) {
            typeExact = {HT_UNK, EXPARAM_DEFAULT};
            ++index;
            continue;
        }
        if (type.first == HT_UNK) {
            type = typeExact;
        }
        if (!canConvertTo(type, typeExact, type)) {
            throw ConversionException("Cannot create a Matrix with mixing incompatible types.");
        }
        ++index;
    }
    if (type.first == HT_UNK) {
        type = nullType;
    }
    if (type.first == HT_VOID) {
        throw ConversionException("Cannot create a Matrix with VOID type.");
    }
    return _create_And_Indicate_As_2DMatrix(data, type, rows, cols);
}

template <typename T>
ConstantSP
Converter::toDolphinDB_Matrix_fromTupleOrListOrNDArray(
    const T                     &data,
    Type                        type,
    typename std::enable_if<std::is_same<T, py::list>::value || std::is_same<T, py::tuple>::value || std::is_same<T, py::array>::value, T>::type*
) {
    VectorInfo info;
    DLOG("[toDolphinDB_Matrix_fromTupleOrListOrNDArray]")

    size_t cols = py::len(data);

    if (cols == 0) {
        throw ConversionException("Cannot create a Matrix with 0 rows or 0 columns.");
    }

    if (type.first != HT_UNK) {
        if ((int)type.first >= ARRAY_TYPE_BASE || type.first == HT_ANY) {
            throw ConversionException("Cannot create a Matrix with " + getDataTypeString(type) + " type.");
        }
        return _create_And_Indicate_As_Matrix(data, type, cols);
    }

    // type.first == HT_UNK
    int dim = -1;
    size_t rows = 1;
    info = VectorInfo(0, false);
    dim = isArrayLike(data[py::int_(0)]) ? 2 : 1;
    if (dim == 2) {
        rows = py::len(data[py::int_(0)]);
    }
    
    if (dim == 1) {
        VectorSP v = Converter::toDolphinDB_Vector(data, type, CHILD_VECTOR_OPTION::NORMAL_VECTOR);
        ConstantSP matrix = createMatrixWithVector(v, rows, cols);
        return matrix;
    }
    
    // dim == 2
    return _create_And_Infer_As_2DMatrix(data, rows, cols);   
}


ConstantSP
Converter::toDolphinDB_Dictionary(const py::handle &data, Type keyType, Type valType, bool isOrdered) {
    if (CHECK_INS(data, py_dict_)) {
        return Converter::toDolphinDB_Dictionary_fromDict(py::reinterpret_borrow<py::dict>(data), keyType, valType, isOrdered);
    }
    else {
        throw ConversionException("Cannot convert " + py2str(data.get_type()) + " to Dictionary as it is an unsupported python type.");
    }
}


ConstantSP
Converter::toDolphinDB_Table(const py::handle &data, const TableChecker &checker) {
    if (CHECK_INS(data, pd_dataframe_)) {
        return Converter::toDolphinDB_Table_fromDataFrame(py::reinterpret_borrow<py::object>(data), checker);
    }
    if (CHECK_INS(data, py_dict_)) {
        return Converter::toDolphinDB_Table_fromDict(py::reinterpret_borrow<py::dict>(data), checker);
    }
    else {
        throw ConversionException("Cannot convert " + py2str(data.get_type()) + " to Table as it is an unsupported python type.");
    }
}


ConstantSP
_create_And_Append_With_ARRAYT(
    const py::array         &data,
    const Type              &type,
    const VectorInfo        &info
) {
    VectorSP ddbVec;
    py::dtype dtype = data.dtype();
    switch (type.first)
    {
    case HT_BOOL:
    case HT_CHAR: {
        py::array_t<int8_t> series_array = py::cast<py::array_t<int8_t, py::array::f_style | py::array::forcecast> >(data);
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        VectorAppendChar(ddbVec, reinterpret_cast<char*>(const_cast<int8_t*>(series_array.data())), size);
        break;
    }
    case HT_SHORT: {
        py::array_t<int16_t> series_array = py::cast<py::array_t<int16_t, py::array::f_style | py::array::forcecast>>(data);
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        VectorAppendShort(ddbVec, reinterpret_cast<short*>(const_cast<int16_t*>(series_array.data())), size);
        break;
    }
    case HT_INT: {
        py::array_t<int32_t> series_array = py::cast<py::array_t<int32_t, py::array::f_style | py::array::forcecast>>(data);
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        VectorAppendInt(ddbVec, reinterpret_cast<int*>(const_cast<int32_t*>(series_array.data())), size);
        break;
    }
    case HT_LONG: {
        py::array_t<int64_t> series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(data);
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
        break;
    }
    case HT_MONTH:
    case HT_DATE:
    case HT_TIME:
    case HT_MINUTE:
    case HT_SECOND:
    case HT_DATETIME:
    case HT_DATEHOUR:
    case HT_TIMESTAMP:
    case HT_NANOTIME:
    case HT_NANOTIMESTAMP: {
        // dtype = datetime64[ns / D / M / m / s / h / ms / us]
        py::array_t<int64_t> series_array;
        try {
            series_array = py::cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>(data);
        }
        catch (py::error_already_set &e) {
            throw ConversionException("Cannot convert " + py2str(dtype) + " to " + getDataTypeString(type));
        }
        size_t size = series_array.size();
        if (CHECK_EQUAL(dtype, np_datetime64ns_)) {
            ddbVec = createVector({HT_NANOTIMESTAMP, EXPARAM_DEFAULT}, 0, size);
            VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
            if (type.first != HT_NANOTIMESTAMP)
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64D_)) {
            ddbVec = createVector({HT_DATE, EXPARAM_DEFAULT}, 0, size);
            VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
            if (type.first != HT_DATE)
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64M_)) {
            ddbVec = createVector({HT_MONTH, EXPARAM_DEFAULT}, 0, size);
            processData<long long>((long long*)series_array.data(), size, [&](long long *buf, int _size) {
                for (int i = 0; i < _size; ++i) {
                    if (buf[i] != LLONG_MIN) {
                        buf[i] += 23640;
                    }
                }
                VectorAppendLong(ddbVec, buf, _size);
            });
            if (type.first != HT_MONTH)
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64m_)) {
            ddbVec = createVector({HT_DATETIME, EXPARAM_DEFAULT}, 0, size);
            processData<long long>((long long*)series_array.data(), size, [&](long long *buf, int _size) {
                for (int i = 0; i < _size; ++i) {
                    if (buf[i] != LLONG_MIN) {
                        buf[i] *= 60;
                    }
                }
                VectorAppendLong(ddbVec, buf, _size);
            });
            if (type.first != HT_DATETIME)
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64s_)) {
            ddbVec = createVector({HT_DATETIME, EXPARAM_DEFAULT}, 0, size);
            VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
            if (type.first != HT_DATETIME)
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64h_)) {
            ddbVec = createVector({HT_DATEHOUR, EXPARAM_DEFAULT}, 0, size);
            VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
            if (type.first != HT_DATEHOUR)
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64ms_)) {
            ddbVec = createVector({HT_TIMESTAMP, EXPARAM_DEFAULT}, 0, size);
            VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
            if (type.first != HT_TIMESTAMP)
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64us_)) {
            ddbVec = createVector({HT_NANOTIMESTAMP, EXPARAM_DEFAULT}, 0, size);
            processData<long long>((long long*)series_array.data(), size, [&](long long *buf, int _size) {
                for (int i = 0; i < _size; ++i) {
                    if (buf[i] != LLONG_MIN) {
                        buf[i] *= 1000ll;
                    }
                }
                VectorAppendLong(ddbVec, buf, _size);
            });
            if (type.first != HT_NANOTIMESTAMP)
                ddbVec = castTemporal(ddbVec, type);
        }
        else {
            ddbVec = createVector(type, 0, size);
            VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
        }
        break;
    }
    case HT_FLOAT: {
        py::array_t<float_t> series_array = py::cast<py::array_t<float_t, py::array::f_style | py::array::forcecast>>(data);
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        bool hasNull = false;
        processData<float>(const_cast<float*>(series_array.data()), size, [&](float *buf, int size_) {
            for (int i = 0; i < size_; ++i) {
                if(ISNUMPYNULL_FLOAT32(buf[i])) {
                    buf[i] = FLT_NMIN;
                    hasNull = true;
                }
            }
            VectorAppendFloat(ddbVec, buf, size_);
        });
        ddbVec->setNullFlag(hasNull);
        break;
    }
    case HT_DOUBLE: {
        py::array_t<double_t> series_array = py::cast<py::array_t<double_t, py::array::f_style | py::array::forcecast>>(data);
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        bool hasNull = false;
        processData<double>(const_cast<double*>(series_array.data()), size, [&](double *buf, int size_) {
            for (int i = 0; i < size_; ++i) {
                if(ISNUMPYNULL_FLOAT64(buf[i])) {
                    buf[i] = DBL_NMIN;
                    hasNull = true;
                }
            }
            VectorAppendDouble(ddbVec, buf, size_);
        });
        ddbVec->setNullFlag(hasNull);
        break;
    }
    case HT_IP:
    case HT_UUID:
    case HT_INT128:
    case HT_SYMBOL:
    case HT_STRING: {
        py::array series_array = py::cast<py::array>(data);
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        appendStringtoVector(ddbVec, series_array, size, 0, info);
        break;
    }
    case HT_BLOB: {
        py::array series_array = py::cast<py::array>(data);
        size_t size = series_array.size();
        ddbVec = createVector({HT_BLOB, EXPARAM_DEFAULT}, size, size);
        setBlobVector(ddbVec, series_array, size, 0, info);
        break;
    }
    case HT_DECIMAL32:
    case HT_DECIMAL64:
    case HT_DECIMAL128:
    case HT_ANY: {
        py::array series_array = py::cast<py::array>(data);
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        Type elemType = {type.first == HT_ANY ? HT_UNK : type.first, type.second};
        size_t index = 0;
        py::handle ptr;
        try {
            for (const auto &it : series_array) {
                ptr = it;
                ddbVec->append(Converter::toDolphinDB_Scalar(it, elemType));
                ++index;
            }
        }
        catch (...) {
            throwExceptionAboutNumpyVersion(
                index, py2str(ptr), getDataTypeString(type), info
            );
        }
        break;
    }
    default:
        throw ConversionException("Cannot convert " + py2str(dtype) + " to " + getDataTypeString(type));
    }
    return ddbVec;
}



/**
 * TODO: need to fix
*/
ConstantSP
_create_And_Append_ArrayVector_With_PANDAS_ARROW(
    const py::handle    &data,
    const py::handle    &dtype,         /* pandas.Series.dtype */
    Type                &elemType,
    const VectorInfo    &info
) {
    VectorSP ddbVec;
    py::object n_data = PyObjs::cache_->pyarrow_.attr("array")(data.attr("array"));
    if (CHECK_INS(n_data, pa_chunked_array_))
        n_data = n_data.attr("combine_chunks")();
    py::array_t<int32_t> offsets = PyObjs::cache_->pd_series_(n_data.attr("offsets"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                    .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
    VectorSP indVec = createVector({HT_INT, EXPARAM_DEFAULT}, 0, offsets.size()-1);
    VectorAppendInt(indVec, const_cast<int*>(offsets.data()) + 1, offsets.size() - 1);
    VectorSP valVec;
    size_t size = py::len(n_data.attr("values"));

    switch (elemType.first)
    {
    case HT_BOOL: {
        py::array_t<int8_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int8_))
                                        .attr("to_numpy")("dtype"_a="int8", "na_value"_a=CHAR_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendChar(valVec, reinterpret_cast<char*>(const_cast<int8_t*>(values.data())), size);
        break;
    }
    case HT_CHAR: {
        py::array_t<int8_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int8_))
                                        .attr("to_numpy")("dtype"_a="int8", "na_value"_a=CHAR_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendChar(valVec, reinterpret_cast<char*>(const_cast<int8_t*>(values.data())), size);
        break;
    }
    case HT_SHORT: {
        py::array_t<int16_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int16_))
                                        .attr("to_numpy")("dtype"_a="int16", "na_value"_a=SHRT_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendShort(valVec, reinterpret_cast<short*>(const_cast<int16_t*>(values.data())), size);
        break;
    }
    case HT_INT: {
        py::array_t<int32_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                        .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendInt(valVec, reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
        break;
    }
    case HT_LONG: {
        py::array_t<int64_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int64_))
                                        .attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendLong(valVec, reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
        break;
    }
    case HT_FLOAT: {
        py::array_t<float> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_float32_))
                                        .attr("to_numpy")("dtype"_a="float32", "na_value"_a=FLT_NMIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendFloat(valVec, const_cast<float*>(values.data()), size);
        break;
    }
    case HT_DOUBLE: {
        py::array_t<double> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_float64_))
                                        .attr("to_numpy")("dtype"_a="float64", "na_value"_a=DBL_NMIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendDouble(valVec, const_cast<double*>(values.data()), size);
        break;
    }
    case HT_MONTH:
    case HT_DATE: {
        py::array_t<int32_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                        .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendInt(valVec, reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
        if (elemType.first != HT_DATE)
            valVec = castTemporal(valVec, elemType);
        break;
    }
    case HT_SECOND:
    case HT_MINUTE: {
        py::array_t<int32_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                        .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendInt(valVec, reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
        if (elemType.first != HT_SECOND)
            valVec = castTemporal(valVec, elemType);
        break;
    }
    case HT_TIME: {
        py::array_t<int32_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                        .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendInt(valVec, reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
        break;
    }
    case HT_NANOTIME:
    case HT_NANOTIMESTAMP:
    case HT_TIMESTAMP: {
        py::array_t<int64_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int64_))
                                        .attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendLong(valVec, reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
        break;
    }
    case HT_DATETIME:
    case HT_DATEHOUR: {
        py::array_t<int64_t> values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int64_))
                                        .attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
        valVec = createVector(elemType, 0, size);
        VectorAppendLong(valVec, reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
        if (elemType.first != HT_DATETIME)
            valVec = castTemporal(valVec, elemType);
        break;
    }
    case HT_IP: {
        py::array values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_utf8_))
                            .attr("to_numpy")("dtype"_a="object", "na_value"_a="");
        valVec = createVector(elemType, 0, size);
        appendStringtoVector(valVec, values, size, 0, info);
        break;
    }
    case HT_UUID:
    case HT_INT128: {
        char tmp[16] = {0};
        py::bytes na_value = py::bytes(tmp, 16);
        py::array values = PyObjs::cache_->pd_series_(n_data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_fixed_size_binary_16_))
                            .attr("to_numpy")("dtype"_a="object", "na_value"_a=na_value);
        valVec = createVector(elemType, size, size);
        int index = 0;
        std::string tmpStr;
        unsigned char nullstr[16] = {0};
        for (auto &it : values) {
            tmpStr = py::cast<std::string>(it);
            if (tmpStr == "") {
                valVec->setBinary(index, 16, nullstr);
            }
            else {
                std::reverse(tmpStr.begin(), tmpStr.end());
                valVec->setBinary(index, 16, reinterpret_cast<const unsigned char*>(tmpStr.data()));
            }
            ++index;
        }
        break;
    }
    case HT_DECIMAL32:
    case HT_DECIMAL64:
    case HT_DECIMAL128: {
        if (elemType.second == EXPARAM_DEFAULT) {
            elemType.second = py::cast<int>(dtype.attr("scale")); // dtype = series.dtype.pyarrow_dtype.value_type
        }
        py::array values = PyObjs::cache_->pd_series_(n_data.attr("values"),
                            "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(dtype.attr("pyarrow_dtype").attr("value_type")))
                            .attr("to_numpy")("dtype"_a="object");
        valVec = createVector(elemType, 0, size);
        int scale = valVec->getExtraParamForType();
        if (elemType.first == HT_DECIMAL32) {
            valVec->setNullFlag(appendDecimaltoVector<int>(
                values, size, 0, INT32_MIN, elemType,
                [&](int *buf, int size) {
                    VectorAppendDecimal32(valVec, buf, size, scale);
                }, info));
        }
        else if (elemType.first == HT_DECIMAL64) {
            valVec->setNullFlag(appendDecimaltoVector<long long>(
                values, size, 0, INT64_MIN, elemType,
                [&](long long *buf, int size) {
                    VectorAppendDecimal64(valVec, buf, size, scale);
                }, info));
        }
        else {
            valVec->setNullFlag(appendDecimaltoVector<int128>(
                values, size, 0, std::numeric_limits<int128>::min(), elemType,
                [&](int128 *buf, int size) {
                    VectorAppendDecimal128(valVec, buf, size, scale);
                }, info));
        }
        break;
    }
    default:
        throw ConversionException("Cannot convert pandas arrow extension type " + py2str(dtype) + " to " + getDataTypeString(elemType) + ".");
    }
    ddbVec = createArrayVectorWithIndexAndValue(indVec, valVec);
    return ddbVec;
}

/**
 * TODO: need to fix
*/
ConstantSP
_create_And_Append_Vector_With_PANDAS_ARROW(
    const py::handle    &data,
    const py::handle    &dtype,         /* pandas.Series.dtype */
    Type                &elemType,
    const VectorInfo    &info
) {
    DLOG("dtype: ", py2str(dtype), " elemType: ", getDataTypeString(elemType));
    VectorSP ddbVec;
    size_t size = py::len(data);

    switch (elemType.first)
    {
    case HT_BOOL: {
        py::array_t<int8_t> values = data.attr("astype")(PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int8_))
                                         .attr("to_numpy")("dtype"_a="int8", "na_value"_a=CHAR_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendChar(ddbVec, reinterpret_cast<char*>(const_cast<int8_t*>(values.data())), size);
        break;
    }
    case HT_CHAR: {
        py::array_t<int8_t> values = data.attr("to_numpy")("dtype"_a="int8", "na_value"_a=CHAR_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendChar(ddbVec, reinterpret_cast<char*>(const_cast<int8_t*>(values.data())), size);
        break;
    }
    case HT_SHORT: {
        py::array_t<int16_t> values = data.attr("to_numpy")("dtype"_a="int16", "na_value"_a=SHRT_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendShort(ddbVec, reinterpret_cast<short*>(const_cast<int16_t*>(values.data())), size);
        break;
    }
    case HT_INT: {
        py::array_t<int32_t> values = data.attr("astype")(PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                          .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendInt(ddbVec, reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
        break;
    }
    case HT_LONG: {
        py::array_t<int64_t> values = data.attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
        break;
    }
    case HT_FLOAT: {
        py::array_t<float> values = data.attr("to_numpy")("dtype"_a="float32", "na_value"_a=FLT_NMIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendFloat(ddbVec, const_cast<float*>(values.data()), size);
        break;
    }
    case HT_DOUBLE: {
        py::array_t<double> values = data.attr("to_numpy")("dtype"_a="float64", "na_value"_a=DBL_NMIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendDouble(ddbVec, const_cast<double*>(values.data()), size);
        break;
    }
    case HT_MONTH:
    case HT_DATE: {
        py::array_t<int32_t> values = data.attr("astype")(PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                          .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendInt(ddbVec, reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
        if (elemType.first != HT_DATE)
            ddbVec = castTemporal(ddbVec, elemType);
        break;
    }
    case HT_SECOND:
    case HT_MINUTE: {
        py::array_t<int32_t> values = data.attr("astype")(PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                          .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendInt(ddbVec, reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
        if (elemType.first != HT_SECOND)
            ddbVec = castTemporal(ddbVec, elemType);
        break;
    }
    case HT_TIME: {
        py::array_t<int32_t> values = data.attr("astype")(PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                          .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendInt(ddbVec, reinterpret_cast<int*>(const_cast<int32_t*>(values.data())), size);
        break;
    }
    case HT_NANOTIME:
    case HT_NANOTIMESTAMP:
    case HT_TIMESTAMP: {
        py::array_t<int64_t> values = data.attr("astype")(PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int64_))
                                          .attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
        break;
    }
    case HT_DATETIME:
    case HT_DATEHOUR: {
        py::array_t<int64_t> values = data.attr("astype")(PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int64_))
                                          .attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
        ddbVec = createVector(elemType, 0, size);
        VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(values.data())), size);
        if (elemType.first != HT_DATETIME)
            ddbVec = castTemporal(ddbVec, elemType);
        break;
    }
    case HT_SYMBOL: {
        py::object ntype = dtype.attr("pyarrow_dtype");
        if (CHECK_INS(ntype, pa_dictionary_type_)) {
            py::object ndata = PyObjs::cache_->pyarrow_.attr("array")(data.attr("array"));
            if (CHECK_INS(ndata, pa_chunked_array_))
                ndata = ndata.attr("combine_chunks")();
            py::array_t<int32_t> indices = PyObjs::cache_->pd_series_(ndata.attr("indices"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_int32_))
                                        .attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
            py::array dictionary = ndata.attr("dictionary");
            ndata = py::none();
            std::vector<std::string> dict_string;
            dict_string.reserve(dictionary.size());
            for (auto &it : dictionary) {
                dict_string.push_back(py::cast<std::string>(it));
            }
            ddbVec = createVector({HT_SYMBOL, EXPARAM_DEFAULT}, size, size);
            const int32_t *data = indices.data();
            for (int it = 0; it < size; ++it) {
                ddbVec->setString(it, data[it] == INT_MIN ? "" : dict_string[data[it]]);
            }
        }
        else {
            // maybe string or others
            py::array series_array = data.attr("to_numpy")("dtype"_a="object", "na_value"_a="");
            ddbVec = createVector(elemType, size, size);
            int index = 0;
            for (auto &it : series_array) {
                ddbVec->setString(index, py::cast<std::string>(it));
                ++index;
            }
        }
        break;
    }
    case HT_BLOB:
    case HT_STRING: {
        py::array series_array = data.attr("to_numpy")("dtype"_a="object", "na_value"_a="");
        ddbVec = createVector(elemType, size, size);
        int index = 0;
        for (auto &it : series_array) {
            ddbVec->setString(index, py::cast<std::string>(it));
            ++index;
        }
        break;
    }
    case HT_IP: {
        py::array values = data.attr("to_numpy")("dtype"_a="object", "na_value"_a="");
        ddbVec = createVector(elemType, 0, size);
        appendStringtoVector(ddbVec, values, size, 0, info);
        break;
    }
    case HT_UUID:
    case HT_INT128: {
        char tmp[16] = {0};
        py::bytes na_value = py::bytes(tmp, 16);
        py::array values = PyObjs::cache_->pd_series_(data.attr("values"), "dtype"_a=PyObjs::cache_->pd_arrow_dtype_(PyObjs::cache_->pa_fixed_size_binary_16_))
                            .attr("to_numpy")("dtype"_a="object", "na_value"_a=na_value);
        ddbVec = createVector(elemType, size, size);
        int index = 0;
        std::string tmpStr;
        unsigned char nullstr[16] = {0};
        for (auto &it : values) {
            tmpStr = py::cast<std::string>(it);
            if (tmpStr == "") {
                ddbVec->setBinary(index, 16, nullstr);
            }
            else {
                std::reverse(tmpStr.begin(), tmpStr.end());
                ddbVec->setBinary(index, 16, reinterpret_cast<const unsigned char*>(tmpStr.data()));
            }
            ++index;
        }
        break;
    }
    case HT_DECIMAL32:
    case HT_DECIMAL64:
    case HT_DECIMAL128: {
        py::object ntype = dtype.attr("pyarrow_dtype");
        if (elemType.second == EXPARAM_DEFAULT) {
            elemType.second = py::cast<int>(ntype.attr("scale")); // dtype = series.dtype.pyarrow_dtype.value_type
        }
        py::array values = PyObjs::cache_->pd_series_(data.attr("values"), "dtype"_a=dtype)
                            .attr("to_numpy")("dtype"_a="object");
        size_t size = values.size();
        ddbVec = createVector(elemType, 0, size);
        int scale = ddbVec->getExtraParamForType();
        if (elemType.first == HT_DECIMAL32) {
            ddbVec->setNullFlag(appendDecimaltoVector<int>(
                values, size, 0, INT32_MIN, elemType,
                [&](int *buf, int size) {
                    VectorAppendDecimal32(ddbVec, buf, size, scale);
                }, info));
        }
        else if (elemType.first == HT_DECIMAL64) {
            ddbVec->setNullFlag(appendDecimaltoVector<long long>(
                values, size, 0, INT64_MIN, elemType,
                [&](long long *buf, int size) {
                    VectorAppendDecimal64(ddbVec, buf, size, scale);
                }, info));
        }
        else {
            ddbVec->setNullFlag(appendDecimaltoVector<int128>(
                values, size, 0, std::numeric_limits<int128>::min(), elemType,
                [&](int128 *buf, int size) {
                    VectorAppendDecimal128(ddbVec, buf, size, scale);
                }, info));
        }
        break;
    }
    default:
        throw ConversionException("Cannot convert pandas arrow extension type " + py2str(dtype) + " to " + getDataTypeString(elemType) + ".");
    }
    return ddbVec;
}


/**
 * create Vector From Pandas ExtensionDtype
 */
ConstantSP
_create_And_Append_Vector_With_PANDAS_EXTENSION(
    const py::handle    &data,
    const py::handle    &dtype,
    Type                &type,
    Type                &exactDtype,
    const VectorInfo    &info
) {
    VectorSP ddbVec;
    size_t size = py::len(data);
    
    switch (type.first) {
        case HT_BOOL:
        case HT_CHAR: {
            if (exactDtype.first != HT_BOOL && exactDtype.first != HT_CHAR &&
                exactDtype.first != HT_SHORT && exactDtype.first != HT_INT && exactDtype.first != HT_LONG) {
                throw ConversionException(std::string("Cannot convert pandas dtype ") + py2str(dtype) + " to " + getDataTypeString(type) + ".");
            }
            ddbVec = createVector(type, 0, size);
            py::array_t<int8_t> change_array = data.attr("to_numpy")("dtype"_a="int8", "na_value"_a=CHAR_MIN);
            ddbVec->appendChar(reinterpret_cast<char*>(const_cast<int8_t*>(change_array.data())), size);
            break;
        }
        case HT_SHORT: {
            if (exactDtype.first != HT_BOOL && exactDtype.first != HT_CHAR &&
                exactDtype.first != HT_SHORT && exactDtype.first != HT_INT && exactDtype.first != HT_LONG) {
                throw ConversionException(std::string("Cannot convert pandas dtype ") + py2str(dtype) + " to " + getDataTypeString(type) + ".");
            }
            ddbVec = createVector(type, 0, size);
            py::array_t<int16_t> change_array = data.attr("to_numpy")("dtype"_a="int16", "na_value"_a=SHRT_MIN);
            ddbVec->appendShort(reinterpret_cast<short*>(const_cast<int16_t*>(change_array.data())), size);
            break;
        }
        case HT_INT: {
            if (exactDtype.first != HT_BOOL && exactDtype.first != HT_CHAR &&
                exactDtype.first != HT_SHORT && exactDtype.first != HT_INT && exactDtype.first != HT_LONG) {
                throw ConversionException(std::string("Cannot convert pandas dtype ") + py2str(dtype) + " to " + getDataTypeString(type) + ".");
            }
            ddbVec = createVector(type, 0, size);
            py::array_t<int32_t> change_array = data.attr("to_numpy")("dtype"_a="int32", "na_value"_a=INT_MIN);
            ddbVec->appendInt(reinterpret_cast<int*>(const_cast<int32_t*>(change_array.data())), size);
            break;
        }
        case HT_LONG: {
            if (exactDtype.first != HT_BOOL && exactDtype.first != HT_CHAR &&
                exactDtype.first != HT_SHORT && exactDtype.first != HT_INT && exactDtype.first != HT_LONG) {
                throw ConversionException(std::string("Cannot convert pandas dtype ") + py2str(dtype) + " to " + getDataTypeString(type) + ".");
            }
            ddbVec = createVector(type, 0, size);
            py::array_t<int64_t> change_array = data.attr("to_numpy")("dtype"_a="int64", "na_value"_a=LLONG_MIN);
            ddbVec->appendLong(reinterpret_cast<long long*>(const_cast<int64_t*>(change_array.data())), size);
            break;
        }
        case HT_FLOAT: {
            if (exactDtype.first != HT_BOOL && exactDtype.first != HT_CHAR &&
                exactDtype.first != HT_SHORT && exactDtype.first != HT_INT && exactDtype.first != HT_LONG &&
                exactDtype.first != HT_FLOAT && exactDtype.first != HT_DOUBLE) {
                throw ConversionException(std::string("Cannot convert pandas dtype ") + py2str(dtype) + " to " + getDataTypeString(type) + ".");
            }
            ddbVec = createVector(type, 0, size);
            py::array_t<float> change_array = data.attr("to_numpy")("dtype"_a="float32", "na_value"_a=FLT_NMIN);
            ddbVec->appendFloat(const_cast<float*>(change_array.data()), size);
            break;
        }
        case HT_DOUBLE: {
            if (exactDtype.first != HT_BOOL && exactDtype.first != HT_CHAR &&
                exactDtype.first != HT_SHORT && exactDtype.first != HT_INT && exactDtype.first != HT_LONG &&
                exactDtype.first != HT_FLOAT && exactDtype.first != HT_DOUBLE) {
                throw ConversionException(std::string("Cannot convert pandas dtype ") + py2str(dtype) + " to " + getDataTypeString(type) + ".");
            }
            ddbVec = createVector(type, 0, size);
            py::array_t<double> change_array = data.attr("to_numpy")("dtype"_a="float64", "na_value"_a=DBL_NMIN);
            ddbVec->appendDouble(const_cast<double*>(change_array.data()), size);
            break;
        }
        case HT_IP:
        case HT_UUID:
        case HT_INT128:
        case HT_SYMBOL:
        case HT_STRING: {
            if (exactDtype.first != HT_STRING && exactDtype.first != HT_BLOB) {
                throw ConversionException(std::string("Cannot convert pandas dtype ") + py2str(dtype) + " to " + getDataTypeString(type) + ".");
            }
            ddbVec = createVector(type, 0, size);
            py::array series_array = py::cast<py::array>(data);
            appendStringtoVector(ddbVec, series_array, size, 0, info);
            break;
        }
        case HT_BLOB: {
            if (exactDtype.first != HT_STRING && exactDtype.first != HT_BLOB) {
                throw ConversionException(std::string("Cannot convert pandas dtype ") + py2str(dtype) + " to " + getDataTypeString(type) + ".");
            }
            py::array series_array = py::cast<py::array>(data);
            ddbVec = createVector(type, size, size);
            setBlobVector(ddbVec, series_array, size, 0, info);
            break;
        }
        default: {
            throw ConversionException(std::string("Cannot convert pandas dtype ") + py2str(dtype) + " to " + getDataTypeString(type) + ".");
        }
    }
    return ddbVec;
}


ConstantSP
_create_And_Append_Vector_With_PANDAS(
    const py::handle    &data,
    const py::dtype     &dtype,
    const Type          &type,
    const VectorInfo    &info
) {
    VectorSP ddbVec;
    switch (type.first)
    {
    case HT_BOOL:
    case HT_CHAR: {
        py::array_t<int8_t> series_array = data.cast<py::array_t<int8_t, py::array::f_style | py::array::forcecast>>();
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        VectorAppendChar(ddbVec, reinterpret_cast<char*>(const_cast<int8_t*>(series_array.data())), size);
        break;
    }
    case HT_SHORT: {
        py::array_t<int16_t> series_array = data.cast<py::array_t<int16_t, py::array::f_style | py::array::forcecast>>();
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        VectorAppendShort(ddbVec, reinterpret_cast<short*>(const_cast<int16_t*>(series_array.data())), size);
        break;
    }
    case HT_INT: {
        py::array_t<int32_t> series_array = data.cast<py::array_t<int32_t, py::array::f_style | py::array::forcecast>>();
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        VectorAppendInt(ddbVec, reinterpret_cast<int*>(const_cast<int32_t*>(series_array.data())), size);
        break;
    }
    case HT_LONG: {
        py::array_t<int64_t> series_array = data.cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>();
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
        break;
    }
    case HT_MONTH:
    case HT_DATE:
    case HT_TIME:
    case HT_MINUTE:
    case HT_SECOND:
    case HT_DATETIME:
    case HT_DATEHOUR:
    case HT_TIMESTAMP:
    case HT_NANOTIME:
    case HT_NANOTIMESTAMP: {
        // dtype = datetime64[ns] | datetime64[s] | datetime64[ms]
        // In Pandas2.0 : s -> int32 | ms -> int64, need special case
        if (!PyObjs::cache_->pd_above_2_0_ || CHECK_EQUAL(dtype, np_datetime64ns_)) {
            py::array_t<int64_t> series_array = data.cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>();
            size_t size = series_array.size();
            ddbVec = createVector({HT_NANOTIMESTAMP, EXPARAM_DEFAULT}, 0, size);
            VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
            if (type.first != (HELPER_TYPE)ddbVec->getType())
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64ms_)) {
            py::array_t<int64_t> series_array = data.cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>();
            size_t size = series_array.size();
            ddbVec = createVector({HT_TIMESTAMP, EXPARAM_DEFAULT}, 0, size);
            VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
            if (type.first != (HELPER_TYPE)ddbVec->getType())
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64s_)) {
            py::array_t<int64_t> series_array = data.cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>();
            size_t size = series_array.size();
            ddbVec = createVector({HT_DATETIME, EXPARAM_DEFAULT}, 0, size);
            VectorAppendLong(ddbVec, reinterpret_cast<long long*>(const_cast<int64_t*>(series_array.data())), size);
            if (type.first != (HELPER_TYPE)ddbVec->getType())
                ddbVec = castTemporal(ddbVec, type);
        }
        else if (CHECK_EQUAL(dtype, np_datetime64us_)) {
            py::array_t<int64_t> series_array = data.cast<py::array_t<int64_t, py::array::f_style | py::array::forcecast>>();
            size_t size = series_array.size();
            ddbVec = createVector({HT_NANOTIMESTAMP, EXPARAM_DEFAULT}, 0, size);
            processData<long long>((long long*)series_array.data(), size, [&](long long *buf, int _size) {
                for (int i = 0; i < _size; ++i) {
                    if (buf[i] != LLONG_MIN) {
                        buf[i] *= 1000;
                    }
                }
                VectorAppendLong(ddbVec, buf, _size);
            });
            if (type.first != (HELPER_TYPE)ddbVec->getType())
                ddbVec = castTemporal(ddbVec, type);
        }
        else {
            throw ConversionException("Cannot convert pandas dtype " + py2str(dtype) + " to " + getDataTypeString(type));
        }
        ddbVec->setNullFlag(ddbVec->hasNull());
        break;
    }
    case HT_FLOAT: {
        py::array_t<float_t> series_array = data.cast<py::array_t<float_t, py::array::f_style | py::array::forcecast>>();
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        bool hasNull = false;
        processData<float>(const_cast<float*>(series_array.data()), size, [&](float *buf, int size) {
            for (int i = 0; i < size; ++i) {
                if(ISNUMPYNULL_FLOAT32(buf[i])) {
                    buf[i] = FLT_NMIN;
                    hasNull = true;
                }
            }
            VectorAppendFloat(ddbVec, buf, size);
        });
        ddbVec->setNullFlag(hasNull);
        break;
    }
    case HT_DOUBLE: {
        py::array_t<double_t> series_array = data.cast<py::array_t<double_t, py::array::f_style | py::array::forcecast>>();
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        bool hasNull = false;
        processData<double>(const_cast<double*>(series_array.data()), size, [&](double *buf, int size) {
            for (int i = 0; i < size; ++i) {
                if(ISNUMPYNULL_FLOAT64(buf[i])) {
                    buf[i] = DBL_NMIN;
                    hasNull = true;
                }
            }
            VectorAppendDouble(ddbVec, buf, size);
        });
        ddbVec->setNullFlag(hasNull);
        break;
    }
    case HT_IP:
    case HT_UUID:
    case HT_INT128:
    case HT_SYMBOL:
    case HT_STRING: {
        py::array series_array = data.cast<py::array>();
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        appendStringtoVector(ddbVec, series_array, size, 0, info);
        break;
    }
    case HT_BLOB: {
        py::array series_array = data.cast<py::array>();
        size_t size = series_array.size();
        ddbVec = createVector({HT_BLOB, EXPARAM_DEFAULT}, size, size);
        setBlobVector(ddbVec, series_array, size, 0, info);
        break;
    }
    case HT_DECIMAL32:
    case HT_DECIMAL64:
    case HT_DECIMAL128:
    case HT_ANY: {
        py::array series_array = py::cast<py::array>(data);
        size_t size = series_array.size();
        ddbVec = createVector(type, 0, size);
        Type elemType = {type.first == HT_ANY ? HT_UNK : type.first, type.second};
        size_t index = 0;
        py::handle ptr;
        try {
            for (const auto &it : series_array) {
                ptr = it;
                ddbVec->append(Converter::toDolphinDB_Scalar(it, elemType));
                ++index;
            }
        }
        catch (...) {
            throwExceptionAboutNumpyVersion(
                index, py2str(ptr), getDataTypeString(type), info
            );
        }
        break;
    }
    default:
        throw ConversionException("Cannot convert pandas dtype " + py2str(dtype) + " to " + getDataTypeString(type));
    }
    return ddbVec;
}


ConstantSP
_toDolphinDB_Vector_fromNDArray(
    py::array                   data,
    Type                        type,
    const CHILD_VECTOR_OPTION   &option
) {
    VectorInfo info;
    
    bool Is_ArrayVector = false;
    bool Is_DTypeObject = false;
    bool Is_ArrowDType = false;
    bool Is_ExtensionDType = false;
    py::dtype dtype = data.dtype();
    Is_DTypeObject = CHECK_EQUAL(dtype, np_object_);

    DLOG("Infer: ", getDataTypeString(type), type.second);
    DLOG("Is_DTypeObject: ", Is_DTypeObject);
    if (!Is_DTypeObject) {
        if (type.first == HT_UNK) {  // Infer DType [NumpyDType, ...]
            type = InferDataTypeFromDType(dtype, Is_ArrowDType, Is_ExtensionDType);
        }
        if (type.first == HT_ANY) {
            if (option == CHILD_VECTOR_OPTION::ARRAY_VECTOR) {
                throw ConversionException("Cannot create an ArrayVector with mixing incompatible types.");
            }
        }
        if ((int)type.first >= ARRAY_TYPE_BASE) {
            // pandas arrow dtype & pa.list_
            if (option != CHILD_VECTOR_OPTION::ARRAY_VECTOR) {
                throw ConversionException("Vector types cannot be specified as arrays (e.g., BOOL[], INT[]).");
            }
            throw ConversionException("To create an ArrayVector from numpy.ndarray, dtype must be 'object'.");
        }
        VectorSP ddbVec;
        ddbVec = _create_And_Append_With_ARRAYT(data, type, info);
        return ddbVec;
    }
    DLOG("[_toDolphinDB_NDArray dtype=object]");
    return _create_And_Infer_with_Object(data, type, data.size(), option, info);
}




ConstantSP Converter::toDolphinDB_VectorOrMatrix_fromNDArray(
    py::array                   data,
    Type                        type,
    const CHILD_VECTOR_OPTION   &option,
    const ARRAY_OPTION          &arrOption,
    const VectorInfo            &info
) {
    int &exparam = type.second;
    int dim = data.ndim();
    int rows = dim == 1 ? 1 : data.shape(0);
    int cols = dim == 1 ? data.size() : data.shape(1);
    py::dtype dtype = data.dtype();
    bool Is_ArrayVector = false;
    bool Is_DTypeObject = false;
    bool Is_ArrowDType = false;

    // TODO: need to support Tensor / NDMatrix
    if (dim > 2) {
        throw ConversionException("Cannot create a Matrix from the given numpy.ndarray with dimension greater than 2.");
    }

    if (arrOption == AO_UNSPEC) {
        if (dim == 1) {
            // Create as Vector
            return _toDolphinDB_Vector_fromNDArray(data, type, option);
        }
        if (CHECK_INS(data, np_matrix_)) {
            data = PyObjs::cache_->numpy_.attr("array")(data);
        }
        // Create as Matrix
        data = data.attr("transpose")().attr("reshape")(data.size());
        ConstantSP tmp = _toDolphinDB_Vector_fromNDArray(data, type, option);
        ConstantSP matrix = createMatrixWithVector(tmp, cols, rows);
        return matrix;
    }
    else if (arrOption == AO_VECTOR) {
        // Create as Vector
        // 1. dim = 1 => OK
        // 2. dim = 2 && row = 1 / col = 1
        if (dim == 2) {
            throw ConversionException("Cannot create a Vector from numpy.ndarray with dimension greater than 1.");
        }
        return _toDolphinDB_Vector_fromNDArray(data, type, option);
    }
    else {
        // Create as Matrix
        if (CHECK_INS(data, np_matrix_)) {
            data = PyObjs::cache_->numpy_.attr("array")(data);
        }
        if (dim == 2) {
            data = data.attr("transpose")().attr("reshape")(data.size());
            ConstantSP tmp = _toDolphinDB_Vector_fromNDArray(data, type, option);
            ConstantSP matrix = createMatrixWithVector(tmp, cols, rows);
            return matrix;
        }
        // dim == 1
        return Converter::toDolphinDB_Matrix_fromTupleOrListOrNDArray(data, type);
    }
}


ConstantSP
Converter::toDolphinDB_Vector_fromSeriesOrIndex(
    py::object                  data,
    Type                        type,
    const CHILD_VECTOR_OPTION   &option,
    const VectorInfo            &info
) {
    bool Has_Child_Vector = false;
    bool Is_DTypeObject = false;
    bool Is_ArrowDType = false;
    bool Is_ExtensionDType = false;
    bool need_check_arrow = PyObjs::cache_->pd_above_2_0_ && PyObjs::cache_->pyarrow_import_;

    py::dtype dtype = py::reinterpret_borrow<py::dtype>(data.attr("dtype"));
    Is_DTypeObject = CHECK_EQUAL(dtype, np_object_);

    if (type.first == HT_UNK) {
        type = InferDataTypeFromDType(dtype, Is_ArrowDType, Is_ExtensionDType, need_check_arrow);
    }
    else if (need_check_arrow) {
        if (CHECK_INS(dtype, pd_arrow_dtype_)) {
            Is_ArrowDType = true;
        }
    }

    if (!Is_DTypeObject) {
        // Explicit Type
        // Data in Series with NumpyDType cannot be ArrayVector
        Type exactDtype = InferDataTypeFromDType(dtype, Is_ArrowDType, Is_ExtensionDType, need_check_arrow);
        if (Is_ExtensionDType) {
            // dtype is Pandas ExtensionDtype
            return _create_And_Append_Vector_With_PANDAS_EXTENSION(data, dtype, type, exactDtype, info);
        }
        if (type.first == HT_OBJECT)
            // type == DT_OBJECT && !Is_DTypeObject ==> Indicate Type is OBJECT !!Not possible!!
            throw ConversionException("Unsupported to be specificed as OBJECT type.");
        if ((int)type.first >= ARRAY_TYPE_BASE && option != CHILD_VECTOR_OPTION::ARRAY_VECTOR) {
            throw ConversionException("Vector types cannot be specified as arrays (e.g., BOOL[], INT[]).");
        }
        if ((int)type.first >= ARRAY_TYPE_BASE && Is_ArrowDType) {
            Has_Child_Vector = true;
            Type elemType = {(HELPER_TYPE)(type.first - ARRAY_TYPE_BASE), type.second};
            return _create_And_Append_ArrayVector_With_PANDAS_ARROW(data, dtype, elemType, info);
        }
        else if (Is_ArrowDType) {
            return _create_And_Append_Vector_With_PANDAS_ARROW(data, dtype, type, info);
        }
        else {
            // Normal Pandas Dtype
            return _create_And_Append_Vector_With_PANDAS(data, dtype, type, info);
        }
    }
    py::array series_array = py::cast<py::array>(data);
    size_t size = series_array.size();
    return _create_And_Infer_with_Object(series_array, type, size, option, info);
}


ConstantSP
Converter::toDolphinDB_Table_fromDataFrame(
    const py::object            &data,
    const TableChecker          &checker
) {
    // Get TableChecker
    TableChecker new_checker = checker;
    py::dict dict_checker = py::getattr(data, "__DolphinDB_Type__", py::dict());
    TableChecker tmp_checker = TableChecker(dict_checker);
    for(const auto &iter : tmp_checker) {
        new_checker[iter.first] = iter.second;
    }
    // Get names of Columns
    std::vector<std::string> colNames;
    try {
        colNames = py::cast<std::vector<std::string> >(data.attr("columns"));
    }
    catch (...) {
        throw ConversionException("DataFrame column names must be strings.");
    }

    size_t cols = colNames.size();
    std::vector<ConstantSP> columns;
    columns.resize(cols);

    // Convert Columns
    for (size_t ni = 0; ni < cols; ++ni) {
        VectorInfo info(colNames[ni]);
        py::object tmpObj = data[colNames[ni].data()];
        if (!isArrayLike(tmpObj)) {
            throw ConversionException("Table columns must be vectors (numpy.ndarray, pandas.Series, tuple, or list).");
        }
        if (new_checker.count(colNames[ni]) > 0) {
            columns[ni] = Converter::toDolphinDB_Vector_fromSeriesOrIndex(
                tmpObj, new_checker.at(colNames[ni]),
                CHILD_VECTOR_OPTION::ARRAY_VECTOR, info);
        }
        else {
            columns[ni] = Converter::toDolphinDB_Vector_fromSeriesOrIndex(
                tmpObj, {HT_UNK, EXPARAM_DEFAULT},
                CHILD_VECTOR_OPTION::ARRAY_VECTOR, info);
        }
    }
    TableSP ddbTbl = createTable(colNames, columns);
    return ddbTbl;
}


ConstantSP
Converter::toDolphinDB_Table_fromDict(
    const py::dict              &data,
    const TableChecker          &checker
) {
    size_t cols = data.size();
    std::vector<std::string> colNames;
    colNames.reserve(cols);
    std::vector<ConstantSP> columns;
    columns.reserve(cols);
    for (const auto &it : data) {
        try {
            colNames.emplace_back(py::cast<std::string>(it.first));
        }
        catch (...) {
            throw ConversionException("The dict keys must be strings indicating columns names.");
        }
        VectorInfo info(colNames.back());
        if (checker.count(colNames.back()) > 0) {
            columns.emplace_back(Converter::toDolphinDB_Vector(it.second, checker.at(colNames.back()), CHILD_VECTOR_OPTION::ARRAY_VECTOR, info));
        }
        else {
            columns.emplace_back(Converter::toDolphinDB_Vector(it.second, {HT_UNK, EXPARAM_DEFAULT}, CHILD_VECTOR_OPTION::ARRAY_VECTOR, info));
        }
    }
    TableSP ddbTbl = createTable(colNames, columns);
    return ddbTbl;
}


ConstantSP
Converter::toDolphinDB_Set_fromSet(
    const py::set       &data,
    Type                type
) {
    ConstantSP ddbVec = Converter::toDolphinDB_Vector_fromTupleorListorSet(data, type, CHILD_VECTOR_OPTION::SET_VECTOR);
    return createSet(ddbVec);
}


ConstantSP
Converter::toDolphinDB_Dictionary_fromDict(
    const py::dict      &data,
    Type                keyTypeHint,
    Type                valTypeHint,
    bool                isOrdered
) {
    py::list keys = py::cast<py::list>(data.attr("keys")());
    py::list vals = py::cast<py::list>(data.attr("values")());

    if (py::len(keys) == 0 && keyTypeHint.first == HT_UNK) {
        throw ConversionException("Empty Dictionary creation requires a specific type.");
    }

    VectorSP ddbKeyVec = Converter::toDolphinDB_Vector(keys, keyTypeHint, CHILD_VECTOR_OPTION::KEY_VECTOR);
    VectorSP ddbValVec = Converter::toDolphinDB_Vector(vals, valTypeHint, CHILD_VECTOR_OPTION::ANY_VECTOR);

    return createDictionary(ddbKeyVec, ddbValVec, isOrdered);
}


py::object
Converter::toPython(const ConstantSP &data, const ToPythonOption &option) {
    DATA_FORM form = data->getForm();
    if (form == DATA_FORM::DF_SCALAR)
        return Converter::toPython_Scalar(data, option);
    if (form == DATA_FORM::DF_VECTOR)
        return Converter::toNumpy_Vector(data, option);
    if (form == DATA_FORM::DF_PAIR)
        return Converter::toPyList_Vector(data, option);
    if (form == DATA_FORM::DF_MATRIX)
        return Converter::toNumpy_Matrix(data, option);
    if (form == DATA_FORM::DF_SET)
        return Converter::toPySet_Set(data, option);
    if (form == DATA_FORM::DF_DICTIONARY)
        return Converter::toPyDict_Dictionary(data, option);
    if (form == DATA_FORM::DF_TABLE)
        return Converter::toPandas_Table(data, option);
    throw ConversionException("Cannot convert " + Util::getDataFormString(form) + " to a python object.");
}


py::object
Converter::toPython_Scalar(const ConstantSP &data, const ToPythonOption &option) {
    // if (data->isNull()) {
    //     return py::none();      // STRING ?     "" or None
    // }
    Type type = createType(data);
    switch (type.first)
    {
    case HT_VOID: {
        return py::none();
    }
    case HT_BOOL: {
        if (data->isNull()) return py::none();
        return py::bool_(data->getBool());
    }
    case HT_CHAR:
    case HT_SHORT:
    case HT_INT:
    case HT_LONG: {
        if (data->isNull()) return py::none();
        return py::int_(data->getLong());
    }
    case HT_DATE:
    case HT_MONTH:
    case HT_TIME:
    case HT_MINUTE:
    case HT_SECOND:
    case HT_DATETIME:
    case HT_TIMESTAMP:
    case HT_NANOTIME:
    case HT_NANOTIMESTAMP:
    case HT_DATEHOUR: {
        return convertTemporalToNumpy(data);
    }
    case HT_FLOAT: {
        if (data->isNull()) return py::none();
        return py::float_(data->getFloat());
    }
    case HT_DOUBLE: {
        if (data->isNull()) return py::none();
        return py::float_(data->getDouble());
    }
    case HT_IP:
    case HT_UUID:
    case HT_INT128:
    case HT_SYMBOL: {
        std::string tmp_str = data->getString();
        PyObject *tmp = decodeUtf8Text(tmp_str.data(), tmp_str.size());
        return py::reinterpret_steal<py::object>(tmp);
    }
    case HT_BLOB: {
        if (data->isNull()) return py::bytes("");
        return py::bytes(data->getStringRef().data(), data->getStringRef().size());
    }
    case HT_STRING: {
        PyObject *tmp = decodeUtf8Text(data->getStringRef().data(), data->getStringRef().size());
        return py::reinterpret_steal<py::object>(tmp);
    }
    case HT_DECIMAL32: {
        if (data->isNull()) return py::none();
        auto raw_data = ((SmartPointer<Decimal32>)data)->getRawData();
        auto scale = ((SmartPointer<Decimal32>)data)->getScale();
        bool sign = raw_data < 0;
        std::vector<int> digits;
        getDecimalDigits<int32_t>(sign ? -raw_data : raw_data, digits);
        py::object tuple = PyObjs::cache_->decimal_.attr("DecimalTuple")(sign, digits, -scale);
        return PyObjs::cache_->py_decimal_(tuple);
    }
    case HT_DECIMAL64: {
        if (data->isNull()) return py::none();
        auto raw_data = ((SmartPointer<Decimal64>)data)->getRawData();
        auto scale = ((SmartPointer<Decimal64>)data)->getScale();
        bool sign = raw_data < 0;
        std::vector<int> digits;
        getDecimalDigits<int64_t>(sign ? -raw_data : raw_data, digits);
        py::object tuple = PyObjs::cache_->decimal_.attr("DecimalTuple")(sign, digits, -scale);
        return PyObjs::cache_->py_decimal_(tuple);
    }
    case HT_DECIMAL128: {
        if (data->isNull()) return py::none();
        auto raw_data = ((SmartPointer<Decimal128>)data)->getRawData();
        auto scale = ((SmartPointer<Decimal128>)data)->getScale();
        bool sign = raw_data < 0;
        std::vector<int> digits;
        getDecimalDigits<int128>(sign ? -raw_data : raw_data, digits);
        py::object tuple = PyObjs::cache_->decimal_.attr("DecimalTuple")(sign, digits, -scale);
        return PyObjs::cache_->py_decimal_(tuple);
    }
    default:
        throw ConversionException("Cannot convert " + getDataTypeString(type) + " Scalar to a python object.");
    }
}


py::list
Converter::toPyList_Vector(const ConstantSP &data, const ToPythonOption &option) {
    size_t size = data->size();
    py::list pyList = py::list(size);
    for (size_t i = 0; i < size; ++i) {
        pyList[i] = Converter::toPython(data->get(i), option);
    }
    return pyList;
}


py::list
Converter::toPyList_Matrix(const ConstantSP &data, const ToPythonOption &option) {
    size_t rows = data->rows();
    py::list pyList = py::list(rows);
    for (size_t i = 0; i < rows; ++i) {
        pyList[i] = Converter::toPython(data->getRow(i), option);
    }
    return pyList;
}


py::list
Converter::toPyList_Table(const ConstantSP &data, const ToPythonOption &option) {
    /**
     * 'a': [1, 2, 3], 'b': [True, False, None]
     * ->
     * [{'a': 1, 'b': True}, {'a': 2, 'b': False}, {'a': 3, 'b': None}]
    */
    return py::list();
}


py::dict
Converter::toPyDict_Table(const ConstantSP &data, const ToPythonOption &option) {
    TableSP ddbTable = data;
    size_t cols = ddbTable->columns();
    py::dict pyDict = py::dict();
    for (size_t i = 0; i < cols; ++i) {
        pyDict[ddbTable->getColumnName(i).data()] = Converter::toPyList_Vector(ddbTable->getColumn(i), option);
    }
    return pyDict;
}


py::dict
Converter::toPyDict_Dictionary(const ConstantSP &data, const ToPythonOption &option) {
    DictionarySP ddbDict = data;
    VectorSP keys = ddbDict->keys();
    VectorSP values = ddbDict->values();
    Type keyType = createType(keys);
    if (keyType.first != HT_STRING && keyType.first != HT_SYMBOL && getCategory(keyType) != DATA_CATEGORY::INTEGRAL) {
        throw ConversionException("Only dictionary with string, symbol or integral keys can be converted to dict.");
    }
    py::dict pyDict = py::dict();
    size_t size = ddbDict->size();
    if (keyType.first == HT_STRING) {
        for (int i = 0; i < size; ++i) { pyDict[keys->getString(i).data()] = toPython(values->get(i), option); }
    }
    else {
        for (int i = 0; i < size; ++i) { pyDict[py::int_(keys->getLong(i))] = toPython(values->get(i), option); }
    }
    return pyDict;
}


py::set
Converter::toPySet_Set(const ConstantSP &data, const ToPythonOption &option) {
    VectorSP ddbVec = data->keys();
    py::set pySet;
    for (int i = 0; i < ddbVec->size(); ++i) {
        pySet.add(Converter::toPython(ddbVec->get(i), option));
    }
    return pySet;
}


#define TO_NUMPY_VECTOR_WITHNULL_NUMERIC(T, data, size, func, min_) \
    py::array pyVec(py::dtype("float64"), {size}, {}); \
    double *p = (double *)pyVec.mutable_data(); \
    VectorEnumNumeric<T>(data, &Vector::func, [&](const T *pbuf, INDEX startIndex, INDEX size_) -> bool { \
        for (INDEX i = 0, index = startIndex; i < size_; ++i, ++index) { \
            if (UNLIKELY(pbuf[i] == min_)) { \
                SET_NPNULL(p+index, 1); \
            } \
            else { \
                p[index] = pbuf[i]; \
            } \
        } \
        return true; \
    }, 0);

/**
 * Default is not ZeroCopyOnly Mode
*/
py::object
Converter::toNumpy_Vector(const ConstantSP &data, const ToPythonOption &option, bool hasNull) {
    Type type = createType(data);
    size_t size = data->size();
    if (option.table2List_ && getCategory(type) == DATA_CATEGORY::ARRAY) {
        auto arrayVector = (dolphindb::FastArrayVector*)data.get();
        size_t v_cols = arrayVector->checkVectorSize();
        if (v_cols < 0) {
            throw ConversionException("Cannot convert ArrayVector to 2D array. All elements in ArrayVector must have the same length.");
        }
        VectorSP valueSP = arrayVector->getFlatValueArray();
        py::array py2DVec = toNumpy_Vector(valueSP, option);
        size_t v_rows = arrayVector->rows();
        py2DVec.resize({v_rows, v_cols});
        return py2DVec;
    }
    if (128 + HT_SYMBOL == (int)type.first) {
        // Special SymbolVector
        auto symbolVector = (dolphindb::FastSymbolVector*)data.get();
        py::array pyVec(py::dtype("object"), {size}, {});
        for (size_t i = 0; i < size; ++i) {
            py::str temp(symbolVector->getString(i));
            Py_IncRef(temp.ptr());
            memcpy(pyVec.mutable_data(i), &temp, sizeof(py::object));
        }
        return pyVec;
    }
    if ((int)type.first >= ARRAY_TYPE_BASE) {
        py::array pyVec(py::dtype("object"), {size}, {});
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        bool subHasNull = data->getNullFlag();
        for (size_t i = 0; i < size; ++i) {
            VectorSP subArray = data->get(i);
            pyArray[i] = Converter::toNumpy_Vector(subArray, option, subHasNull | hasNull);
        }
        return pyVec;
    }
    switch (type.first)
    {
    case HT_VOID: {
        py::array pyVec(py::dtype("object"));
        pyVec.resize({size});
        return pyVec;
    }
    case HT_BOOL: {
        if (LIKELY(!data->getNullFlag() && !hasNull)) {
            py::array pyVec(py::dtype("bool"), {size}, {});
            data->getBool(0, size, (char *)pyVec.mutable_data());
            return pyVec;
        }
        py::array pyVec(py::dtype("object"), {size}, {});
        auto buf = pyVec.request();
        py::object* ptr = static_cast<py::object*>(buf.ptr);
        static py::none pyNone;
        static py::bool_ pyTrue(true);
        static py::bool_ pyFalse(false);
        VectorEnumNumeric<char>(data, &Vector::getBoolConst, [&](const char *pbuf, INDEX startIndex, INDEX size) -> bool {
            for (INDEX i = 0, index = startIndex; i < size; ++i, ++index) {
                if (UNLIKELY(pbuf[i] == CHAR_MIN)) {
                    ptr[i] = pyNone;
                }
                else {
                    if (pbuf[i] != 0)
                        ptr[i] = pyTrue;
                    else
                        ptr[i] = pyFalse;
                }
            }
            return true;
        }, 0);
        return pyVec;
    }
    case HT_CHAR: {
        if (LIKELY(!data->getNullFlag() && !hasNull)) {
            py::array pyVec(py::dtype("int8"), {size}, {});
            data->getChar(0, size, (char *)pyVec.mutable_data());
            return pyVec;
        }
        TO_NUMPY_VECTOR_WITHNULL_NUMERIC(char, data, size, getCharConst, CHAR_MIN)
        return pyVec;
    }
    case HT_SHORT: {
        if (LIKELY(!data->getNullFlag() && !hasNull)) {
            py::array pyVec(py::dtype("int16"), {size}, {});
            data->getShort(0, size, (short *)pyVec.mutable_data());
            return pyVec;
        }
        TO_NUMPY_VECTOR_WITHNULL_NUMERIC(short, data, size, getShortConst, SHRT_MIN)
        return pyVec;
    }
    case HT_INT: {
        if (LIKELY(!data->getNullFlag() && !hasNull)) {
            py::array pyVec(py::dtype("int32"), {size}, {});
            data->getInt(0, size, (int *)pyVec.mutable_data());
            return pyVec;
        }
        TO_NUMPY_VECTOR_WITHNULL_NUMERIC(int, data, size, getIntConst, INT_MIN)
        return pyVec;
    }
    case HT_LONG: {
        if (LIKELY(!data->getNullFlag() && !hasNull)) {
            py::array pyVec(py::dtype("int64"), {size}, {});
            data->getLong(0, size, (long long *)pyVec.mutable_data());
            return pyVec;
        }
        TO_NUMPY_VECTOR_WITHNULL_NUMERIC(long long, data, size, getLongConst, LLONG_MIN)
        return pyVec;
    }
    case HT_DATE: {
        py::array pyVec(py::dtype("datetime64[D]"), {size}, {});
        data->getLong(0, size, (long long *)pyVec.mutable_data());
        return pyVec;
    }
    case HT_MONTH: {
        py::array pyVec(py::dtype("datetime64[M]"), {size}, {});
        data->getLong(0, size, (long long *)pyVec.mutable_data());
        long long *p = (long long *)pyVec.mutable_data();
        const static long long subValue = 1970 * 12;
        if (UNLIKELY(data->getNullFlag())) {
            for (size_t i = 0; i < size; ++i) {
                if (UNLIKELY(p[i] == LLONG_MIN)) {
                    SET_NPNULL(p+i, 1);
                    continue;
                }
                p[i] -= subValue;
            }
        }
        else {
            for (size_t i = 0; i < size; ++i) {
                p[i] -= subValue;
            }
        }
        return pyVec;
    }
    case HT_TIME:
    case HT_TIMESTAMP: {
        py::array pyVec(py::dtype("datetime64[ms]"), {size}, {});
        data->getLong(0, size, (long long *)pyVec.mutable_data());
        return pyVec;
    }
    case HT_MINUTE: {
        py::array pyVec(py::dtype("datetime64[m]"), {size}, {});
        data->getLong(0, size, (long long *)pyVec.mutable_data());
        return pyVec;
    }
    case HT_SECOND:
    case HT_DATETIME: {
        py::array pyVec(py::dtype("datetime64[s]"), {size}, {});
        data->getLong(0, size, (long long *)pyVec.mutable_data());
        return pyVec;
    }
    case HT_NANOTIME:
    case HT_NANOTIMESTAMP: {
        py::array pyVec(py::dtype("datetime64[ns]"), {size}, {});
        data->getLong(0, size, (long long *)pyVec.mutable_data());
        return pyVec;
    }
    case HT_DATEHOUR: {
        py::array pyVec(py::dtype("datetime64[h]"), {size}, {});
        data->getLong(0, size, (long long *)pyVec.mutable_data());
        return pyVec;
    }
    case HT_FLOAT: {
        py::array pyVec(py::dtype("float32"), {size}, {});
        data->getFloat(0, size, (float *)pyVec.mutable_data());
        if (UNLIKELY(data->getNullFlag())) {
            float *p = (float *)pyVec.mutable_data();
            for (size_t i = 0; i < size; ++i) {
                if (UNLIKELY(p[i] == FLT_NMIN)) {
                    SET_NPNULL(p+i, 1);
                }
            }
        }
        return pyVec;
    }
    case HT_DOUBLE: {
        py::array pyVec(py::dtype("float64"), {size}, {});
        data->getDouble(0, size, (double *)pyVec.mutable_data());
        if (UNLIKELY(data->getNullFlag())) {
            double *p = (double *)pyVec.mutable_data();
            for (size_t i = 0; i < size; ++i) {
                if (UNLIKELY(p[i] == DBL_NMIN)) {
                    SET_NPNULL(p+i, 1);
                }
            }
        }
        return pyVec;
    }
    case HT_SYMBOL: {
        py::array pyVec(py::dtype("object"), {size}, {});
        FastSymbolVector *pSymbolVector = (FastSymbolVector *)data.get();
        SymbolBaseSP symbolBase = pSymbolVector->getSymbolBase();
        int symbolBaseSize = symbolBase->size();
        py::object* symbolStrList = new py::object[symbolBaseSize];
        std::string symbolText, emptyText;
        for(int i = 0; i < symbolBaseSize; i++){
            symbolText = symbolBase->getSymbol(i);
            symbolStrList[i] = py::reinterpret_steal<py::object>(decodeUtf8Text(symbolText.data(),symbolText.size()));
        }
        int *pbuf = new int[size];
        data->getInt(0,size,pbuf);
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        py::object pyStr;
        for(size_t i = 0; i < size; i++){
            if(pbuf[i] != INT_MIN){
                pyStr = symbolStrList[pbuf[i]];
                pyArray[i] = pyStr;
            }
        }
        delete[] symbolStrList;
        delete[] pbuf;
        return pyVec;
    }
    case HT_IP: {
        py::array pyVec(py::dtype("object"), {size}, {});
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        VectorEnumBinary<Guid>(data, [&](const Guid *pbuf, INDEX startIndex, INDEX size_) -> bool {
            for (INDEX i = 0, index = startIndex; i < size_; ++i, ++index) {
                std::string text = std::move(IPAddr::toString(pbuf[i].bytes()));
                pyArray[index] = py::str(text);
            }
            return true;
        }, 0);
        return pyVec;
    }
    case HT_UUID: {
        py::array pyVec(py::dtype("object"), {size}, {});
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        const int buflen = 36;
        char textBuf[buflen];
        VectorEnumBinary<Guid>(data, [&](const Guid *pbuf, INDEX startIndex, INDEX size_) -> bool {
            for (INDEX i = 0, index = startIndex; i < size_; ++i, ++index) {
                Util::toGuid(pbuf[i].bytes(), textBuf);
                pyArray[index] = py::str(textBuf, buflen);
            }
            return true;
        }, 0);
        return pyVec;
    }
    case HT_INT128: {
        py::array pyVec(py::dtype("object"), {size}, {});
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        const int buflen = 32;
        char textBuf[buflen];
        VectorEnumBinary<Guid>(data, [&](const Guid *pbuf, INDEX startIndex, INDEX size_) -> bool {
            for (INDEX i = 0, index = startIndex; i < size_; ++i, ++index) {
                Util::toHex(pbuf[i].bytes(), 16, Util::LITTLE_ENDIAN_ORDER, textBuf);
                pyArray[index] = py::str(textBuf, buflen);
            }
            return true;
        }, 0);
        return pyVec;
    }
    case HT_BLOB: {
        py::array pyVec(py::dtype("object"), {size}, {});
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        VectorEnumString(data, [&](char **pbuf, INDEX startIndex, INDEX size_) -> bool {
            for (INDEX i = 0, index = startIndex; i < size_; ++i, ++index) {
                pyArray[index] = py::bytes(pbuf[i], std::strlen(pbuf[i]));
            }
            return true;
        }, 0);
        return pyVec;
    }
    case HT_STRING: {
        py::array pyVec(py::dtype("object"), {size}, {});
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        VectorEnumString(data, [&](char **pbuf, INDEX startIndex, INDEX size_) -> bool {
            for (INDEX i = 0, index = startIndex; i < size_; ++i, ++index) {
                pyArray[index] = py::reinterpret_steal<py::object>(decodeUtf8Text(pbuf[i], std::strlen(pbuf[i])));
            }
            return true;
        }, 0);
        return pyVec;
    }
    case HT_DECIMAL32: {
        py::array pyVec(py::dtype("object"), {size}, {});
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        int c_scale = data->getExtraParamForType();
        VectorEnumBinary<int32_t>(data, [&](const int32_t *pbuf, INDEX startIndex, INDEX size_) -> bool {
            for (INDEX i = 0, index = startIndex; i < size_; ++i, ++index) {
                if (UNLIKELY(pbuf[i] == INT32_MIN)) {
                    pyArray[index] = py::none();
                    continue;
                }
                bool sign = pbuf[i] < 0;
                std::vector<int> digits;
                getDecimalDigits<int32_t>(sign ? -pbuf[i] : pbuf[i], digits);
                py::object tuple = PyObjs::cache_->decimal_.attr("DecimalTuple")(sign, digits, -1 * c_scale);
                pyArray[index] = PyObjs::cache_->py_decimal_(tuple);
            }
            return true;
        }, 0);
        return pyVec;
    }
    case HT_DECIMAL64: {
        py::array pyVec(py::dtype("object"), {size}, {});
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        int c_scale = data->getExtraParamForType();
        VectorEnumBinary<int64_t>(data, [&](const int64_t *pbuf, INDEX startIndex, INDEX size_) -> bool {
            for (INDEX i = 0, index = startIndex; i < size_; ++i, ++index) {
                if (UNLIKELY(pbuf[i] == INT64_MIN)) {
                    pyArray[index] = py::none();
                    continue;
                }
                bool sign = pbuf[i] < 0;
                std::vector<int> digits;
                getDecimalDigits<int64_t>(sign ? -pbuf[i] : pbuf[i], digits);
                py::object tuple = PyObjs::cache_->decimal_.attr("DecimalTuple")(sign, digits, -1 * c_scale);
                pyArray[index] = PyObjs::cache_->py_decimal_(tuple);
            }
            return true;
        }, 0);
        return pyVec;
    }
    case HT_DECIMAL128: {
        py::array pyVec(py::dtype("object"), {size}, {});
        py::object *pyArray = (py::object *)pyVec.mutable_data();
        int c_scale = data->getExtraParamForType();
        VectorEnumBinary<int128>(data, [&](const int128 *pbuf, INDEX startIndex, INDEX size_) -> bool {
            for (INDEX i = 0, index = startIndex; i < size_; ++i, ++index) {
                if (UNLIKELY(pbuf[i] == std::numeric_limits<int128>::min())) {
                    pyArray[index] = py::none();
                    continue;
                }
                bool sign = pbuf[i] < 0;
                std::vector<int> digits;
                getDecimalDigits<int128>(sign ? -pbuf[i] : pbuf[i], digits);
                py::object tuple = PyObjs::cache_->decimal_.attr("DecimalTuple")(sign, digits, -1 * c_scale);
                pyArray[index] = PyObjs::cache_->py_decimal_(tuple);
            }
            return true;
        }, 0);
        return pyVec;
    }
    case HT_ANY: {
        // handle numpy.array of objects
        py::list pyList(size);
        for (size_t i = 0; i < size; ++i) {
            pyList[i] = Converter::toPython(data->get(i), option);
        }
        return pyList;
    }
    default:
        throw ConversionException("Cannot convert " + getDataTypeString(type) + " to numpy ndarray.");
    }
}


py::object
Converter::toNumpy_Matrix(const ConstantSP &data, const ToPythonOption &option) {
    ConstantSP ddbMatrix = data;
    Type type = createType(data);
    size_t rows = ddbMatrix->rows();
    size_t cols = ddbMatrix->columns();
    if (getCategory(type) == DATA_CATEGORY::MIXED) { throw ConversionException("Cannot create Matrix with mixing incompatible types."); }
    ddbMatrix->setForm(DATA_FORM::DF_VECTOR);
    py::array pyMat = Converter::toNumpy_Vector(ddbMatrix, option);
    ddbMatrix->setForm(DATA_FORM::DF_MATRIX);
    pyMat.resize({cols, rows});
    pyMat = pyMat.attr("transpose")();
    py::object pyRowLabel = Converter::toPython(ddbMatrix->getRowLabel(), option);
    py::object pyColLabel = Converter::toPython(ddbMatrix->getColumnLabel(), option);
    py::list pyList;
    pyList.append(pyMat);
    pyList.append(pyRowLabel);
    pyList.append(pyColLabel);
    return pyList;
}


py::object
Converter::toPandas_Vector(const ConstantSP &data, const ToPythonOption &option) {
    py::object series = PyObjs::cache_->pd_series_(Converter::toNumpy_Vector(data, option));
    if (getCategory(createType(data)) == DATA_CATEGORY::TEMPORAL) {
        series = series.attr("astype")("datetime64[ns]");
    }
    return series;
}


py::object
Converter::toPandas_Table(const ConstantSP &data, const ToPythonOption &option) {
    TableSP ddbTable = data;
    size_t cols = ddbTable->columns();
    if (!option.table2List_) {
        // convert to pandas.DataFrame
        py::object dataframe;
        auto dict = py::dict();
        for (size_t i = 0; i < cols; ++i) {
            dict[ddbTable->getColumnName(i).data()] = Converter::toPandas_Vector(ddbTable->getColumn(i), option);
        }
        dataframe = PyObjs::cache_->pandas_.attr("DataFrame")(dict);
        return dataframe;
    }
    else {
        // convert to list[numpy]
        py::list pyList(cols);
        ConstantSP col;
        for (size_t i = 0; i < cols; ++i) {
            col = ddbTable->getColumn(i);
            pyList[i] = toNumpy_Vector(col, option);
        }
        return pyList;
    }
}

void _toPython_Old_createPyVector(
    const ConstantSP        &obj, 
    py::object              &pyObject, 
    bool                    tableFlag,
    const ToPythonOption    &poption)
{
    VectorSP ddbVec = obj;
    Type type = createType(obj);
    DATA_CATEGORY category = getCategory(type);
    size_t size = obj->size();
    if (type.first == HT_ANY) {
        py::list list(size);
        for (size_t i = 0; i < size; ++i) {
            list[i] = Converter::toPython_Old(ddbVec->get(i), poption);
        }
        pyObject = std::move(list);
    }
    else if (DATA_CATEGORY::TEMPORAL == category && tableFlag) {
        if (HT_DATE == type.first) {
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
        }
        else if (HT_MONTH == type.first) {
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
        }
        else if (HT_TIME == type.first) {
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
        else if (HT_MINUTE == type.first) {
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
        }
        else if (HT_SECOND == type.first) {
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
        else if (HT_DATETIME == type.first) {
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
        else if (HT_TIMESTAMP == type.first) {
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
        else if (HT_DATEHOUR == type.first) {
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
            pyObject = Converter::toNumpy_Vector(obj, poption, false);
        }
    }
    else {
        // other situations.
        pyObject = Converter::toNumpy_Vector(obj, poption, false);
    }
}

/*
    toPython_Old: used to convert ConstantSP -> py::object ()
    
    tableFlag true: convert datetime to ns, like dataframe process numpy.array.
*/
py::object
_toPython_Old(ConstantSP obj, bool tableFlag, const ToPythonOption &option) {
    if (obj.isNull()) {
        DLOG("{ _toPython_Old NULL to None. }");
        return py::none();
    }
    DLOG("{ _toPython_Old", Util::getDataTypeString(obj->getType()).data(),Util::getDataFormString(obj->getForm()).data(),obj->size(),"table",tableFlag);
    py::object pyObject;
    DATA_TYPE type = obj->getType();
    DATA_FORM form = obj->getForm();
    if (form == DATA_FORM::DF_VECTOR) {
        if ((int)type == 128 + HT_SYMBOL) {
            // SymbolVector
            FastSymbolVector *symbolVector = (FastSymbolVector *)obj.get();
            size_t size = symbolVector->size();
            py::array pyVec(py::dtype("object"), {size}, {});
            for (size_t i = 0; i < size; ++i) {
                py::str temp(symbolVector->getString(i));
                Py_IncRef(temp.ptr());
                memcpy(pyVec.mutable_data(i), &temp, sizeof(py::object));
            }
            pyObject = std::move(pyVec);
        }
        else if (type >= ARRAY_TYPE_BASE) {
            // ArrayVector
            dolphindb::FastArrayVector *arrayVector = (dolphindb::FastArrayVector *)obj.get();
            if (option.table2List_ == false) {
                size_t size = arrayVector->size();
                py::array pyVec(py::dtype("object"), {size}, {});
                for (size_t i = 0; i < size; ++i) {
                    VectorSP subArray = arrayVector->get(i);
                    if (subArray->getNullFlag()) {
                        subArray->setNullFlag(subArray->hasNull());
                    }
                    // table flag set false in arrayvector
                    py::object pySubVec = _toPython_Old(subArray, false, option);
                    Py_IncRef(pySubVec.ptr());
                    memcpy(pyVec.mutable_data(i), &pySubVec, sizeof(py::object));
                }
                pyObject = std::move(pyVec);
            }
            else {
                size_t cols = arrayVector->checkVectorSize();
                if (cols < 0) {
                    throw ConversionException("Cannot convert ArrayVector to 2D array. All elements in ArrayVector must have the same length.");
                }
                VectorSP valueSP = arrayVector->getFlatValueArray();
                py::object py1darray;
                // table flag set false in arrayvector
                _toPython_Old_createPyVector(valueSP, py1darray, false, option);
                py::array py2DVec(py1darray);
                size_t rows = arrayVector->rows();
                py2DVec.resize({rows, cols});
                pyObject = std::move(py2DVec);
                ;
            }
        }
        else {
            _toPython_Old_createPyVector(obj, pyObject, tableFlag, option);
        }
    } else if (form == DATA_FORM::DF_TABLE) {
        TableSP ddbTbl = obj;
        if(option.table2List_==false) {
            size_t columnSize = ddbTbl->columns();
            using namespace py::literals;
            py::object dataframe;
            auto dict = py::dict();
            for(size_t i = 0; i < columnSize; ++i) {
                dict[ddbTbl->getColumnName(i).data()] = _toPython_Old(ddbTbl->getColumn(i), true, option);
            }
            dataframe = PyObjs::cache_->pandas_.attr("DataFrame")(dict);
            pyObject=std::move(dataframe);
        }else{
            size_t columnSize = ddbTbl->columns();
            py::list pyList(columnSize);
            for (size_t i = 0; i < columnSize; ++i) {
                py::object temp = _toPython_Old(ddbTbl->getColumn(i),true,option);
                pyList[i] = temp;
            }
            pyObject=std::move(pyList);
        }
    } else if (form == DATA_FORM::DF_SCALAR) {
        if(obj->isNull()){
            return py::none();
        }
        Type type_ = createType(obj);
        DATA_CATEGORY category = getCategory(type_);
        if (DATA_CATEGORY::TEMPORAL == category) {
            pyObject = convertTemporalToNumpy(obj);
        }
        else {
            pyObject = Converter::toPython_Scalar(obj, option);
        }
    }
    else if (form == DATA_FORM::DF_DICTIONARY) {
        DictionarySP ddbDict = obj;
        VectorSP keys = ddbDict->keys();
        Type keyType = createType(keys);
        if (keyType.first != HT_STRING && keyType.first != HT_SYMBOL && keys->getCategory() != DATA_CATEGORY::INTEGRAL) {
            throw ConversionException("Only dictionary with string, symbol or integral keys can be converted to dict.");
        }
        VectorSP values = ddbDict->values();
        py::dict pyDict;
        if (keyType.first == HT_STRING) {
            for (int i = 0; i < keys->size(); ++i) { pyDict[keys->getString(i).data()] = _toPython_Old(values->get(i),false,option); }
        } else {
            for (int i = 0; i < keys->size(); ++i) { pyDict[py::int_(keys->getLong(i))] = _toPython_Old(values->get(i),false,option); }
        }
        pyObject=std::move(pyDict);
    } else if (form == DATA_FORM::DF_MATRIX) {
        ConstantSP ddbMat = obj;
        size_t rows = ddbMat->rows();
        size_t cols = ddbMat->columns();
        // FIXME: currently only support numerical matrix
        if (ddbMat->getCategory() == DATA_CATEGORY::MIXED) { throw ConversionException("Cannot create Matrix with mixing incompatible types."); }
        ddbMat->setForm(DATA_FORM::DF_VECTOR);
        py::array pyMat = _toPython_Old(ddbMat,false,option);
        py::object pyMatRowLabel = _toPython_Old(ddbMat->getRowLabel(),false,option);
        py::object pyMatColLabel = _toPython_Old(ddbMat->getColumnLabel(),false,option);
        pyMat.resize({cols, rows});
        pyMat = pyMat.attr("transpose")();
        py::list pyMatList;
        pyMatList.append(pyMat);
        pyMatList.append(pyMatRowLabel);
        pyMatList.append(pyMatColLabel);
        pyObject=std::move(pyMatList);
    } else if (form == DATA_FORM::DF_PAIR) {
        VectorSP ddbPair = obj;
        py::list pyPair;
        for (int i = 0; i < ddbPair->size(); ++i) { pyPair.append(_toPython_Old(ddbPair->get(i),false,option)); }
        pyObject=std::move(pyPair);
    } else if (form == DATA_FORM::DF_SET) {
        VectorSP ddbSet = obj->keys();
        py::set pySet;
        for (int i = 0; i < ddbSet->size(); ++i) { pySet.add(_toPython_Old(ddbSet->get(i),false,option)); }
        pyObject=std::move(pySet);
    } else {
        throw ConversionException("Cannot convert " + Util::getDataFormString(form) + " to a python object.");
    }
    DLOG("toPython", Util::getDataTypeString(obj->getType()).data(),Util::getDataFormString(obj->getForm()).data(),obj->size(), tableFlag,"}");
    return pyObject;
}


py::object
Converter::toPython_Old(const ConstantSP &data, const ToPythonOption &option) {
    return _toPython_Old(data, false, option);
}



} /* namespace converter */