#ifndef IOBINDING_H_
#define IOBINDING_H_


#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>


namespace py = pybind11;


namespace pybind_dolphindb {

void Init_Module_IO(py::module &m);

} // namespace pybind_dolphindb




#endif // IOBINDING_H_