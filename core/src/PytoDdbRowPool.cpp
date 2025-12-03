#include "PytoDdbRowPool.h"

#include "Concurrent.h"
#include "DolphinDB.h"
#include "MultithreadedTableWriter.h"
#include "TypeConverter.h"
#include "TypeHelper.h"
#include "Util.h"

#include <pybind11/pytypes.h>

using namespace pybind11::literals;

namespace dolphindb {
#ifdef DLOG
    #undef DLOG
#endif
#define DLOG    //dolphindb::DLogger::Info


PytoDdbRowPool::PytoDdbRowPool(MultithreadedTableWriter &writer)
                                : writer_(writer)
                                ,exitWhenEmpty_(false)
                                ,convertingCount_(0)
                                {
    idle_.release();
    thread_=new Thread(new ConvertExecutor(*this));
    thread_->start();
}

PytoDdbRowPool::~PytoDdbRowPool(){
    if(!rows_.empty()||!failedRows_.empty()){
        py::gil_scoped_acquire gil;
        while(!rows_.empty()){
            delete rows_.front();
            rows_.pop();
        }
        while(!failedRows_.empty()){
            delete failedRows_.front();
            failedRows_.pop();
        }
    }
}

void PytoDdbRowPool::startExit(){
    DLOG("startExit with",rows_.size(),failedRows_.size());

    exitWhenEmpty_ = true;
    nonempty_.set();
    thread_->join();
}

void PytoDdbRowPool::endExit(){
    DLOG("endExit with",rows_.size(),failedRows_.size());
}

void PytoDdbRowPool::convertLoop(){
    vector<std::vector<py::object>*> convertRows;
    ErrorCodeInfo errorCodeInfo;
    while(writer_.hasError_ == false){
        nonempty_.wait();
        SemLock idleLock(idle_);
        idleLock.acquire();
        {
            LockGuard<Mutex> LockGuard(&mutex_);
            size_t size = rows_.size();
            if(size < 1){
                if(exitWhenEmpty_)
                    break;
                nonempty_.reset();
                continue;
            }
			if (size > 65535)
				size = 65535;
			convertRows.reserve(size);
            while(!rows_.empty()){
                convertRows.push_back(rows_.front());
                rows_.pop();
                if(convertRows.size() >= size)
                    break;
            }
            convertingCount_=convertRows.size();
        }
        {
            DLOG("convert start ",convertRows.size(),"/",rows_.size());
            vector<vector<ConstantSP>*> insertRows;
            insertRows.reserve(convertRows.size());
            vector<ConstantSP> *pDdbRow = NULL;
            try
            {
                py::gil_scoped_acquire gil;
                const DATA_TYPE *pcolType = writer_.getColType();
                const int *pcolExtra = writer_.getColExtra();
                int i, size;
                for (auto &prow : convertRows)
                {
                    pDdbRow = new vector<ConstantSP>;
                    size = prow->size();
                    for (i = 0; i < size; i++)
                    {
                        if ((int)pcolType[i] >= ARRAY_TYPE_BASE) {
                            pDdbRow->emplace_back(
                                converter::Converter::toDolphinDB_Vector(
                                    prow->at(i),
                                    converter::createType((DATA_TYPE)(pcolType[i]-ARRAY_TYPE_BASE), pcolExtra[i]),
                                    CHILD_VECTOR_OPTION::NORMAL_VECTOR
                                )
                            );
                        }
                        else {
                            pDdbRow->emplace_back(
                                converter::Converter::toDolphinDB_Scalar(
                                    prow->at(i),
                                    converter::createType(pcolType[i], pcolExtra[i])
                                )
                            );
                        }
                    }
                    insertRows.emplace_back(pDdbRow);
                    pDdbRow = NULL;
                }
            }catch (RuntimeException &e){
                writer_.setError(ErrorCodeInfo::EC_InvalidObject, std::string("Data conversion error: ") + e.what());
                delete pDdbRow;
            }
            catch (std::exception &e) {
                writer_.setError(ErrorCodeInfo::EC_InvalidObject, std::string("Data conversion error: ") + e.what());
                delete pDdbRow;
            }
            catch (...) {
                writer_.setError(ErrorCodeInfo::EC_InvalidObject, std::string("Data conversion unknown error."));
                delete pDdbRow;
            }
            if(!insertRows.empty()){
                if(!writer_.insertUnwrittenData(insertRows,errorCodeInfo)){
                    writer_.setError(errorCodeInfo);
                    for (auto& prow : insertRows) {
                        delete prow;
                    }
                    insertRows.clear();
                }
            }
            if (insertRows.size() != convertRows.size()){                   // has error, rows left some
                LockGuard<Mutex> LockGuard(&mutex_);
                for(size_t ni = insertRows.size(); ni < convertRows.size(); ++ni){
                    failedRows_.push(convertRows[ni]);
                }
            }
            {
                py::gil_scoped_acquire gil;
                for (size_t ni = 0; ni < insertRows.size(); ++ni) {
                    delete convertRows[ni];                                     // must delete it in GIL lock
                }
            }
            DLOG("convert end ",insertRows.size(),failedRows_.size(),"/",rows_.size());
            convertingCount_ = 0;
            convertRows.clear();
        }
    }
}

void PytoDdbRowPool::getStatus(MultithreadedTableWriter::Status &status){
    py::gil_scoped_release gil;
    writer_.getStatus(status);
    MultithreadedTableWriter::ThreadStatus threadStatus;
    LockGuard<Mutex> guard(&mutex_);
    status.unsentRows += rows_.size() + convertingCount_;
    status.sendFailedRows += failedRows_.size();

    threadStatus.unsentRows += rows_.size() + convertingCount_;
    threadStatus.sendFailedRows += failedRows_.size();

    status.threadStatus.insert(status.threadStatus.begin(),threadStatus);
}

void PytoDdbRowPool::getUnwrittenData(vector<vector<py::object>*> &pyData,vector<vector<ConstantSP>*> &ddbData){
    py::gil_scoped_release gil;
    writer_.getUnwrittenData(ddbData);
    {
        SemLock idleLock(idle_);
        idleLock.acquire();
        LockGuard<Mutex> LockGuard(&mutex_);
        while(!failedRows_.empty()){
            pyData.push_back(failedRows_.front());
            failedRows_.pop();
        }
        while(!rows_.empty()){
            pyData.push_back(rows_.front());
            rows_.pop();
        }
    }
}

} // namespace dolphindb
