#include "IOBinding.h"
#include <pybind11/pytypes.h>
#include <cstddef>
#include <cstring>
#include <memory>
#include "SysIO.h"
#include "Types.h"
#include "ConstantMarshall.h"

#include "TypeDefine.h"
#include "TypeConverter.h"
#include "TypeHelper.h"

#define STREAM_BUFFER_SIZE 2048
#define STREAM_PYTHONIO 999



using namespace pybind_dolphindb;

using converter::PyObjs;

using dolphindb::STREAM_TYPE;
using dolphindb::IO_ERR;
using dolphindb::DataInputStream;
using dolphindb::DataInputStreamSP;
using dolphindb::DataOutputStream;
using dolphindb::DataOutputStreamSP;
using dolphindb::ConstantMarshallFactory;
using dolphindb::ConstantUnmarshallFactory;
using dolphindb::ConstantMarshall;
using dolphindb::ConstantUnmarshall;

class PyInputStream : public DataInputStream {
public:
    PyInputStream(const py::handle &TextIOWrapper, int bufSize = STREAM_BUFFER_SIZE)
        : DataInputStream((STREAM_TYPE)STREAM_PYTHONIO, bufSize), iowrapper_(py::reinterpret_borrow<py::object>(TextIOWrapper)) {}
    ~PyInputStream() override {}
private:
    IO_ERR internalStreamRead(char* buf, size_t length, size_t &actualLength) override {
        std::string res = iowrapper_.attr("read")(length).cast<std::string>();
        actualLength = res.size();
        memcpy(buf, res.c_str(), actualLength);
        return IO_ERR::OK;
    }
    IO_ERR internalClose() override {
        return IO_ERR::OK;
    }
private:
    py::object iowrapper_;
};


class PyOutputStream : public DataOutputStream {
public:
    PyOutputStream(const py::handle &TextIOWrapper)
        : DataOutputStream((STREAM_TYPE)STREAM_PYTHONIO),
          iowrapper_(py::reinterpret_borrow<py::object>(TextIOWrapper)),
          bufsp_(new char[STREAM_BUFFER_SIZE]) {}
private:
    char* createBuffer(size_t& capacity) override {
        capacity = STREAM_BUFFER_SIZE;
        return bufsp_.get();
    }
    IO_ERR internalFlush(size_t size) override {
        char* buf = bufsp_.get();
        py::bytes pybytes(buf, size);
        size_t actualLength = iowrapper_.attr("write")(pybytes).cast<size_t>();
        return IO_ERR::OK;
    }
    IO_ERR internalClose() override {
        return IO_ERR::OK;
    }
private:
    py::object iowrapper_;
    std::unique_ptr<char[]> bufsp_;
};


typedef SmartPointer<PyInputStream> PyInputStreamSP;
typedef SmartPointer<PyOutputStream> PyOutputStreamSP;


py::object dump(const py::handle &obj, const py::handle &IO, const py::handle &types) {
    PyOutputStreamSP pyout = new PyOutputStream(IO);
    ConstantSP objsp;
    if (CHECK_INS(obj, pd_dataframe_)) {
        converter::TableChecker types_ = types.is_none() ? converter::TableChecker() : converter::TableChecker(py::reinterpret_borrow<py::dict>(types));
        objsp = converter::Converter::toDolphinDB_Table_fromDataFrame(py::reinterpret_borrow<py::object>(obj), types_);
    }
    else if (CHECK_INS(obj, py_dict_)) {
        converter::Type type_, val_type_;
        if (types.is_none()) {
            type_ = converter::Type{converter::HT_UNK, EXPARAM_DEFAULT};
            val_type_ = converter::Type{converter::HT_UNK, EXPARAM_DEFAULT};
        }
        else if (CHECK_INS(types, py_list_) && py::len(types) >= 2) {
            py::handle key_type = py::reinterpret_borrow<py::list>(types)[0];
            py::handle val_type = py::reinterpret_borrow<py::list>(types)[1];
            type_ = key_type.is_none() ? converter::Type{converter::HT_UNK, EXPARAM_DEFAULT} : converter::createType(key_type);
            val_type_ = val_type.is_none() ? converter::Type{converter::HT_UNK, EXPARAM_DEFAULT} : converter::createType(val_type);
        }
        else {
            throw converter::ConversionException("Conversion failed. Specify a valid type.");
        }
        objsp = converter::Converter::toDolphinDB_Dictionary_fromDict(py::reinterpret_borrow<py::dict>(obj), type_, val_type_);
    }
    else {
        converter::Type type_ = types.is_none() ? converter::Type{converter::HT_UNK, EXPARAM_DEFAULT} : converter::createType(types);
        objsp = converter::Converter::toDolphinDB(obj, type_);
    }
    ConstantMarshallFactory factory(pyout);
    ConstantMarshall* marshal = factory.getConstantMarshall(objsp->getForm());
    IO_ERR ret = IO_ERR::OK;
    marshal->start(objsp, true, false, ret);
    if (ret != IO_ERR::OK) {
        throw dolphindb::RuntimeException("Failed to serialize data.");
    }
    marshal->flush();
    return py::none();
}


py::object load(const py::handle &IO) {
    PyInputStreamSP pyin = new PyInputStream(IO);
    ConstantUnmarshallFactory factory(pyin);
    short flag;
    IO_ERR ret = IO_ERR::OK;
    ret = pyin->readShort(flag);
    if (ret != IO_ERR::OK) {
        throw dolphindb::RuntimeException("Failed to deserialize data.");
    }
    DATA_FORM form = static_cast<DATA_FORM>(flag>>8);
    ConstantUnmarshall* unmarshal = factory.getConstantUnmarshall(form);
    unmarshal->start(flag, true, ret);
    if (ret != IO_ERR::OK) {
        throw dolphindb::RuntimeException("Failed to deserialize data.");
    }
    ConstantSP res = unmarshal->getConstant();
    return converter::Converter::toPython_Old(res);
}


py::bytes dumps(const py::handle &obj, const py::handle &types) {
    DataOutputStreamSP out = new DataOutputStream(STREAM_BUFFER_SIZE);
    ConstantSP objsp;
    if (CHECK_INS(obj, pd_dataframe_)) {
        converter::TableChecker types_ = types.is_none() ? converter::TableChecker() : converter::TableChecker(py::reinterpret_borrow<py::dict>(types));
        objsp = converter::Converter::toDolphinDB_Table_fromDataFrame(py::reinterpret_borrow<py::object>(obj), types_);
    }
    else if (CHECK_INS(obj, py_dict_)) {
        converter::Type type_, val_type_;
        if (types.is_none()) {
            type_ = converter::Type{converter::HT_UNK, EXPARAM_DEFAULT};
            val_type_ = converter::Type{converter::HT_UNK, EXPARAM_DEFAULT};
        }
        else if (CHECK_INS(types, py_list_) && py::len(types) >= 2) {
            py::handle key_type = py::reinterpret_borrow<py::list>(types)[0];
            py::handle val_type = py::reinterpret_borrow<py::list>(types)[1];
            type_ = key_type.is_none() ? converter::Type{converter::HT_UNK, EXPARAM_DEFAULT} : converter::createType(key_type);
            val_type_ = val_type.is_none() ? converter::Type{converter::HT_UNK, EXPARAM_DEFAULT} : converter::createType(val_type);
        }
        else {
            throw converter::ConversionException("Conversion failed. Specify a valid type.");
        }
        objsp = converter::Converter::toDolphinDB_Dictionary_fromDict(py::reinterpret_borrow<py::dict>(obj), type_, val_type_);
    }
    else {
        converter::Type type_ = types.is_none() ? converter::Type{converter::HT_UNK, EXPARAM_DEFAULT} : converter::createType(types);
        objsp = converter::Converter::toDolphinDB(obj, type_);
    }
    ConstantMarshallFactory factory(out);
    ConstantMarshall* marshal = factory.getConstantMarshall(objsp->getForm());
    IO_ERR ret = IO_ERR::OK;
    marshal->start(objsp, true, false, ret);
    if (ret != IO_ERR::OK) {
        throw dolphindb::RuntimeException("Failed to serialize data.");
    }
    marshal->flush();
    return py::bytes(out->getBuffer(), out->size());
}


py::object loads(const py::bytes &str) {
    const char* c_str = PyBytes_AS_STRING(str.ptr());
    size_t length = py::len(str);
    DataInputStreamSP in = new DataInputStream(c_str, length, false);
    ConstantUnmarshallFactory factory(in);
    short flag;
    IO_ERR ret = IO_ERR::OK;
    ret = in->readShort(flag);
    if (ret != IO_ERR::OK) {
        throw dolphindb::RuntimeException("Failed to deserialize data.");
    }
    DATA_FORM form = static_cast<DATA_FORM>(flag>>8);
    ConstantUnmarshall* unmarshal = factory.getConstantUnmarshall(form);
    unmarshal->start(flag, true, ret);
    if (ret != IO_ERR::OK) {
        throw dolphindb::RuntimeException("Failed to deserialize data.");
    }
    ConstantSP res = unmarshal->getConstant();
    return converter::Converter::toPython_Old(res);
}



namespace pybind_dolphindb {


void Init_Module_IO(py::module &m) {
    m.def("dump", &dump, 
            py::arg("obj"),
            py::arg("file"),
            py::kw_only(),
            py::arg("types") = py::none()
    );
    m.def("load", &load, py::arg("file"));
    m.def("dumps", &dumps, 
            py::arg("obj"),
            py::kw_only(),
            py::arg("types") = py::none()
    );
    m.def("loads", &loads, py::arg("data"));
}

} // namespace pybind_dolphindb
