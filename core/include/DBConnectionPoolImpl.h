#pragma once

#include "TaskStatusMgmt.h"
#include "DolphinDB.h"

namespace dolphindb {

class DBConnectionPoolImpl{
public:
    struct Task{
        Task(const string& sc = "", int id = 0, int pr = 4, int pa = 64, bool clearM = false,
                bool isPy = false, bool pickleTableToL = false, bool disableDec = false)
                : script(sc), identity(id), priority(pr), parallelism(pa), clearMemory(clearM)
                , isPyTask(isPy), pickleTableToList(pickleTableToL), disableDecimal(disableDec){}
        Task(const string& function, const std::vector<ConstantSP>& args, int id = 0, int pr = 4, int pa = 64, bool clearM = false,
                bool isPy = false, bool pickleTableToL = false, bool disableDec = false)
                : script(function), arguments(args), identity(id), priority(pr), parallelism(pa), clearMemory(clearM)
                , isPyTask(isPy), pickleTableToList(pickleTableToL), disableDecimal(disableDec){ isFunc = true; }
        string script;
        std::vector<ConstantSP> arguments;
        int identity;
        int priority;
        int parallelism;
        bool clearMemory;
        bool isFunc = false;
        bool isPyTask = true;
        bool pickleTableToList = false;
        bool disableDecimal = false;
    };
    
    DBConnectionPoolImpl(const string& hostName, int port, int threadNum = 10, const string& userId = "", const string& password = "",
        bool loadBalance = true, bool highAvailability = true, bool compress = false, bool reConnect = false,
        PARSER_TYPE parser = PARSER_TYPE::PARSER_DOLPHINDB, PROTOCOL protocol = PROTOCOL_DDB, bool show_output = true, int sqlStd = 0, int tryReconnectNums = -1);
    
    ~DBConnectionPoolImpl(){
        shutDown();
        Task emptyTask;
        for (size_t i = 0; i < workers_.size(); i++)
            queue_->push(emptyTask);
        for (auto& work : workers_) {
            work->join();
        }
    }
    void run(const string& script, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false){
        queue_->push(Task(script, identity, priority, parallelism, clearMemory));
        taskStatus_.setResult(identity, TaskStatusMgmt::Result());
    }

    void run(const string& functionName, const std::vector<ConstantSP>& args, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false){
        queue_->push(Task(functionName, args, identity, priority, parallelism, clearMemory));
        taskStatus_.setResult(identity, TaskStatusMgmt::Result());
    }

    bool isFinished(int identity){
        return taskStatus_.isFinished(identity);
    }

    ConstantSP getData(int identity){
        return taskStatus_.getData(identity);
    }

    void shutDown(){
        shutDownFlag_.store(true);
        py::gil_scoped_release gil;
        latch_->wait();
    }

    bool isShutDown(){
        return shutDownFlag_.load();
    }

    int getConnectionCount(){
        return static_cast<int>(workers_.size());
    }

    void runPy(
        const string& script, int identity,
        int priority = 4, int parallelism = 64,
        int fetchSize = 0, bool clearMemory = false,
        bool pickleTableToList = false, bool disableDecimal = false
    ){
        queue_->push(Task(
            script, identity, priority, parallelism,
            clearMemory, true, pickleTableToList, disableDecimal
        ));
        taskStatus_.setResult(identity, TaskStatusMgmt::Result());
    }

    void runPy(
        const string& functionName, const vector<ConstantSP>& args, int identity,
        int priority = 4, int parallelism = 64,
        int fetchSize = 0, bool clearMemory = false,
        bool pickleTableToList = false, bool disableDecimal = false
    ){
        queue_->push(Task(
            functionName, args, identity, priority, parallelism,
            clearMemory, true, pickleTableToList, disableDecimal
        ));
        taskStatus_.setResult(identity, TaskStatusMgmt::Result());
    }
    
    py::object getPyData(int identity){
        return taskStatus_.getPyData(identity);
    }

    std::vector<string> getSessionId(){
        for(int i = 0; i < connections_.size(); i++)
        {
            sessionIds_[i] = connections_[i]->getSessionId();
        }
        return sessionIds_;
    }
    
private:
    std::atomic<bool> shutDownFlag_;
    CountDownLatchSP latch_;
    std::vector<ThreadSP> workers_;
    SmartPointer<SynchronizedQueue<Task>> queue_;
    TaskStatusMgmt taskStatus_;
    std::vector<string> sessionIds_;
    std::vector<SmartPointer<DBConnection>> connections_;
};

}