#pragma once

#include "Constant.h"
#include "Concurrent.h"
#include "pybind11/pybind11.h"

namespace py = pybind11;

namespace dolphindb {

class TaskStatusMgmt{
public:
    enum TASK_STAGE{WAITING, FINISHED, ERRORED};
    struct Result{
        Result(TASK_STAGE s = WAITING, const ConstantSP c = Constant::void_ , const py::object& pc = py::none(), const string &msg = "") : stage(s), result(c), pyResult(pc), errMsg(msg){}
        TASK_STAGE stage;
        ConstantSP result;
        py::object pyResult;
        string errMsg;
    };

    bool isFinished(int identity);
    ConstantSP getData(int identity);
    py::object getPyData(int identity);
    void setResult(int identity, Result);
private:
    Mutex mutex_;
    std::unordered_map<int, Result> results;
};

}