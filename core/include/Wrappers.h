#ifndef __WRAPPERS_DOLPHINDB_H
#define __WRAPPERS_DOLPHINDB_H

#include "DolphinDB.h"
#include "pybind11/pybind11.h"

namespace py = pybind11;

namespace dolphindb{

class EXPORT_DECL InputStreamWrapper{
public:
    InputStreamWrapper(){};
    void setInputStream(DataInputStreamSP inputStream){
        _inputStream = inputStream;
    }
    py::bytes read(size_t size){
        char* buf = new char[size+1];
        size_t actual_len = -1;
        IO_ERR ret = _inputStream->readBytes(buf, size, actual_len);
        return py::bytes(buf, actual_len);
    }
    bool closed(){
        return !(_inputStream->getSocket()->isValid());
    }
private:
    DataInputStreamSP _inputStream;
};

}

#endif // __WRAPPERS_DOLPHINDB_H