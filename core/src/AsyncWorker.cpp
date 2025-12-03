#include "AsyncWorker.h"
#include "DolphinDB.h"
#include "ScalarImp.h"

namespace dolphindb {
void AsyncWorker::run() {
    while(true) {
        if(pool_.isShutDown()){
            conn_->close();
            latch_->countDown();
            break;
        }
        Task task;
        ConstantSP result = new Void();
        py::object pyResult;
        bool errorFlag = false;
        if (!queue_->blockingPop(task, 1000))
            continue;
        if (task.script.empty())
            continue;
        while(true) {
            try {
                if(task.isPyTask){
                    py::gil_scoped_acquire gil;
                    if(task.isFunc){
                        pyResult = conn_->runPy(task.script, task.arguments, task.priority, task.parallelism, 0, task.clearMemory, task.pickleTableToList, task.disableDecimal);
                    }
                    else{
                        pyResult = conn_->runPy(task.script, task.priority, task.parallelism, 0, task.clearMemory,task.pickleTableToList, task.disableDecimal);
                    }
                }
                else {
                    if(task.isFunc){
                        result = conn_->run(task.script, task.arguments, task.priority, task.parallelism, 0, task.clearMemory);
                    }
                    else{
                        result = conn_->run(task.script, task.priority, task.parallelism, 0, task.clearMemory);
                    }
                }
                break;
            }
            catch(std::exception & ex){
                errorFlag = true;
                LOG_ERR("Async task worker come across exception :", ex.what());
                taskStatus_.setResult(task.identity, TaskStatusMgmt::Result(TaskStatusMgmt::ERRORED, Constant::void_, py::none(), ex.what()));
                break;
            }
        }
        if(!errorFlag) {
            if (task.isPyTask) {
                py::gil_scoped_acquire gil;
                taskStatus_.setResult(task.identity, TaskStatusMgmt::Result(TaskStatusMgmt::FINISHED, result, pyResult));
                pyResult = py::none();
            }
            else {
                taskStatus_.setResult(task.identity, TaskStatusMgmt::Result(TaskStatusMgmt::FINISHED, result, pyResult));
            }
        }
    }
}
}