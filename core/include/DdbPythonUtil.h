#ifndef DdbUtil_H_
#define DdbUtil_H_
#include "DolphinDB.h"
#include "Concurrent.h"
#include "MultithreadedTableWriter.h"

#include "pybind11/numpy.h"

#ifdef _MSC_VER
	#ifdef _USRDLL	
		#define EXPORT_DECL _declspec(dllexport)
	#else
		#define EXPORT_DECL __declspec(dllimport)
	#endif
#else
    #define EXPORT_DECL
#endif

namespace dolphindb {

class EXPORT_DECL DdbPythonUtil{
public:
    static py::object loadPickleFile(const std::string &filepath);

protected:
    friend class PytoDdbRowPool;
};

class HIDEVISIBILITY PytoDdbRowPool{
public:
    PytoDdbRowPool(MultithreadedTableWriter &writer);
    ~PytoDdbRowPool();
    void startExit();
    void endExit();
    bool isExit(){
        return exitWhenEmpty_ || writer_.hasError_;
    }
    bool add(vector<py::object> *prow){
        if(isExit())
            return false;
        LockGuard<Mutex> LockGuard(&mutex_);
        rows_.push(prow);
        nonempty_.set();
        return true;
    }
    void getStatus(MultithreadedTableWriter::Status &status);
    void getUnwrittenData(vector<vector<py::object>*> &pyData,vector<vector<ConstantSP>*> &ddbData);

    class ConvertExecutor : public Runnable
    {
    public:
        ConvertExecutor(PytoDdbRowPool &rowPool) : rowPool_(rowPool){};
        virtual void run(){ rowPool_.convertLoop(); }
    private:
        PytoDdbRowPool &rowPool_;
    };

private:
    friend class ConvertExecutor;
    void convertLoop();
    MultithreadedTableWriter &writer_;
    ThreadSP thread_;
    bool exitWhenEmpty_;
    Semaphore idle_;
    Signal nonempty_;
    Mutex mutex_;
    int convertingCount_;
	std::queue<vector<py::object>*> rows_;
    std::queue<vector<py::object>*> failedRows_;
    //when main thread call exit, new pGilRelease_ to release GIL, then run can exit
    SmartPointer<ProtectGil> pGil_;
};

}

#endif //DdbUtil_H_