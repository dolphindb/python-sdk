#ifndef CONVERTER_TYPEEXCEPTION_H_
#define CONVERTER_TYPEEXCEPTION_H_

#include <pybind11/pybind11.h>
#include <Exceptions.h>



namespace py = pybind11;

using dolphindb::RuntimeException;


namespace pybind_dolphindb {


class EXPORT_DECL BindBaseException {

};

class EXPORT_DECL BindException : public BindBaseException, public RuntimeException {
public:
    BindException(const std::string &errMsg) : RuntimeException(errMsg) {}
};


class EXPORT_DECL ConversionException : public BindException {
public:
    ConversionException(const std::string &errMsg) : BindException(errMsg) {}
};


void Init_Module_Exception(py::module &m);

} // namespace pybind_dolphindb








#endif // CONVERTER_TYPEEXCEPTION_H_