#ifndef PYBIND_EXTENSION_H_
#define PYBIND_EXTENSION_H_

#include "TypeConverter.h"
#include "TypeBinding.h"


namespace converter {

// TODO: auto little_endian check
inline int128 py_int_to_int128(PyObject* obj) {
    if (!PyLong_Check(obj)) {
        throw pybind_dolphindb::ConversionException("Expected a Python int");
    }
    int128 result = 0;
    _PyLong_AsByteArray((PyLongObject*)obj, (unsigned char*)&result, sizeof(result), 1, 1);
    return result;
}

// FIXME: has problem when val is 128MIN, to python int is 0
inline PyObject* int128_to_python_int(const int128& value) {
    return _PyLong_FromByteArray((unsigned char*)&value, sizeof(value), 1, 1);
}


} // namespace converter




namespace pybind11 { namespace detail {

template <> struct type_caster<converter::int128> {
public:
    PYBIND11_TYPE_CASTER(converter::int128, _("__int128"));

    // Conversion part 1 (Python -> C++)
    bool load(handle src, bool) {
        if (!src) return false;
        value = converter::py_int_to_int128(src.ptr());
        return true;
    }

    // Conversion part 2 (C++ -> Python)
    static handle cast(const converter::int128& src, return_value_policy, handle) {
        return py::reinterpret_steal<py::object>(converter::int128_to_python_int(src));
    }
};


}}  // namespace pybind11::detail


#endif // PYBIND_EXTENSION_H_