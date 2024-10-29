#include "TypeException.h"

namespace pybind_dolphindb {

void Init_Module_Exception(py::module &m) {

    py::register_exception<BindException>(m, "BindError", PyExc_RuntimeError);

    py::register_exception<ConversionException>(m, "ConversionError", m.attr("BindError").ptr());

}



} // namespace pybind_dolphindb