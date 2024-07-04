#ifndef MUTITHREADEDTABLEWRITER_H_
#define MUTITHREADEDTABLEWRITER_H_

#include "Exports.h"
#include "Concurrent.h"
#include "Util.h"
#include "Types.h"
#include "Exceptions.h"
#include <unordered_map>
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <tuple>
#include <cassert>
#include <unordered_map>


namespace dolphindb{

class DBConnection;

class PytoDdbRowPool;
class EXPORT_DECL MultithreadedTableWriter {
public:
    enum Mode{
        M_Append,
        M_Upsert,
    };
    struct ThreadStatus{
        long threadId;
        long sentRows,unsentRows,sendFailedRows;
        ThreadStatus(){
            threadId = 0;
            sentRows = unsentRows = sendFailedRows = 0;
        }
    };
    struct Status : ErrorCodeInfo{
        bool isExiting;
        long sentRows, unsentRows, sendFailedRows;
        std::vector<ThreadStatus> threadStatus;
        void plus(const ThreadStatus &threadStatus){
            sentRows += threadStatus.sentRows;
            unsentRows += threadStatus.unsentRows;
            sendFailedRows += threadStatus.sendFailedRows;
        }
    };
    /**
     * If fail to connect to the specified DolphinDB server, this function throw an exception.
     */
    MultithreadedTableWriter(const std::string& host, int port, const std::string& userId, const std::string& password,
                            const string& dbPath, const string& tableName, bool useSSL, bool enableHighAvailability = false, const vector<string> *pHighAvailabilitySites = nullptr,
							int batchSize = 1, float throttle = 0.01f,int threadCount = 1, const string& partitionCol ="",
							const vector<COMPRESS_METHOD> *pCompressMethods = nullptr, Mode mode = M_Append,
                            vector<string> *pModeOption = nullptr);

    virtual ~MultithreadedTableWriter();

    template<typename... TArgs>
    bool insert(ErrorCodeInfo &errorInfo, TArgs... args) {
        if (hasError_.load()) {
            throw RuntimeException("Thread is exiting.");
        }
        {
            auto argSize = sizeof...(args);
            if (argSize != colTypes_.size()) {
                errorInfo.set(ErrorCodeInfo::EC_InvalidParameter, "Column counts don't match " + std::to_string(argSize));
                return false;
            }
        }
        {
            errorInfo.clearError();
            int colIndex1 = 0, colIndex2 = 0;
            ConstantSP result[] = { Util::createObject(getColDataType(colIndex1++), args, &errorInfo, colExtras_[colIndex2++])... };
            if (errorInfo.hasError()){
                return false;
            }
            std::vector<ConstantSP>* prow;
            if (!unusedQueue_.pop(prow)) {
                prow = new std::vector<ConstantSP>;
            }
            prow->resize(colIndex1);
            for (int i = 0; i < colIndex1; i++) {
                prow->at(i) = result[i];
            }
            return insert(&prow, 1, errorInfo);
        }
    }

	void waitForThreadCompletion();
    void getStatus(Status &status);
    void getUnwrittenData(std::vector<std::vector<ConstantSP>*> &unwrittenData);
	bool insertUnwrittenData(std::vector<std::vector<ConstantSP>*> &records, ErrorCodeInfo &errorInfo) { return insert(records.data(), static_cast<int>(records.size()), errorInfo); }
    
	bool isExit(){ return hasError_; }
    const DATA_TYPE* getColType(){ return colTypes_.data(); }
    int getColSize(){ return colTypes_.size(); }
private:
	bool insert(std::vector<ConstantSP> **records, int recordCount, ErrorCodeInfo &errorInfo);
	void setError(int code, const string &info);
    void setError(const ErrorCodeInfo &errorInfo);
    DATA_TYPE getColDataType(int colIndex) {
		DATA_TYPE dataType = colTypes_[colIndex];
		if (dataType >= ARRAY_TYPE_BASE)
			dataType = (DATA_TYPE)(dataType - ARRAY_TYPE_BASE);
		return dataType;
	}
	void insertThreadWrite(int threadhashkey, std::vector<ConstantSP> *prow);

    struct WriterThread {
		WriterThread() : nonemptySignal(false,true){}
		SmartPointer<DBConnection> conn;
        
        SynchronizedQueue<std::vector<ConstantSP>*> writeQueue;
        SynchronizedQueue<std::vector<ConstantSP>*> failedQueue;
        ThreadSP writeThread;
        Signal nonemptySignal;

        Semaphore idleSem;
        unsigned int threadId;
        long sentRows, sendingRows;
		bool exit;
    };
    class SendExecutor : public dolphindb::Runnable {
    public:
		SendExecutor(MultithreadedTableWriter &tableWriter,WriterThread &writeThread):
                        tableWriter_(tableWriter),
                        writeThread_(writeThread){};
        virtual void run();
    private:
		bool isExit() { return tableWriter_.hasError_.load() || writeThread_.exit; }
        bool init();
        bool writeAllData();
        MultithreadedTableWriter &tableWriter_;
        WriterThread &writeThread_;
    };
    
private:
    friend class SendExecutor;
	friend class InsertExecutor;
    const std::string dbName_;
    const std::string tableName_;
    const int batchSize_;
    const int throttleMilsecond_;
    bool isPartionedTable_, exited_;
	std::atomic_bool hasError_;
    std::vector<string> colNames_,colTypeString_;
    std::vector<DATA_TYPE> colTypes_;
    std::vector<int> colExtras_;
    std::vector<COMPRESS_METHOD> compressMethods_;
    //Following parameters only valid in multithread mode
    SmartPointer<Domain> partitionDomain_;
    int partitionColumnIdx_;
    int threadByColIndexForNonPartion_;
	//End of following parameters only valid in multithread mode
    std::vector<WriterThread> threads_;
	Mutex exitMutex_;
    ErrorCodeInfo errorInfo_;
    std::string scriptTableInsert_;
    std::string scriptSaveTable_;
	SynchronizedQueue<std::vector<ConstantSP>*> unusedQueue_;
    friend class PytoDdbRowPool;
    PytoDdbRowPool *pytoDdb_;
    Mode mode_;
public:
    PytoDdbRowPool * getPytoDdb(){ return pytoDdb_;}
};

};

#endif //MUTITHREADEDTABLEWRITER_H_