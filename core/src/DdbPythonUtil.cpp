#include "DdbPythonUtil.h"
#include <pybind11/pytypes.h>
#include "Concurrent.h"
#include "ConstantImp.h"
#include "ConstantMarshall.h"
#include "DolphinDB.h"
#include "ScalarImp.h"
#include "Util.h"
#include "Pickle.h"
#include "Set.h"
#include "MultithreadedTableWriter.h"
#include "Wrappers.h"
#include "WideInteger.h"

#include "TypeConverter.h"
#include "TypeHelper.h"

namespace ddb = dolphindb;
using namespace pybind11::literals;

namespace dolphindb {
#ifdef DLOG
    #undef DLOG
#endif
#define DLOG    //dolphindb::DLogger::Info

#define RECORDTIME(name) //RecordTime _##name(name)


py::object DdbPythonUtil::loadPickleFile(const std::string &filepath){
    py::dict statusDict;
    string shortFilePath=filepath+"_s";
    FILE *pf;
    pf=fopen(shortFilePath.data(),"rb");
    if(pf==NULL){
        pf=fopen(filepath.data(),"rb");
        if(pf==NULL){
            statusDict["errorCode"]=-1;
            statusDict["errorInfo"]=filepath+" can't open.";
            return statusDict;
        }
        FILE *pfwrite=fopen(shortFilePath.data(),"wb");
        if(pfwrite==NULL){
            statusDict["errorCode"]=-1;
            statusDict["errorInfo"]=shortFilePath+" can't open.";
            return statusDict;
        }
        char buf[4096];
        int readlen;
        {
            char tmp;
            char header=0x80;
            int version;
            while(fread(&tmp,1,1,pf)==1){
                while(tmp==header){
                    if(fread(&tmp,1,1,pf)!=1)
                        break;
                    version=(unsigned char)tmp;
                    DLOG(version);
                    if(version==4){
                        fwrite(&header,1,1,pfwrite);
                        fwrite(&tmp,1,1,pfwrite);
                        goto nextread;
                    }
                }
            }
        }
    nextread:
        while((readlen=fread(buf,1,4096,pf))>0){
            fwrite(buf,1,readlen,pfwrite);
        }
        fclose(pf);
        fclose(pfwrite);
        pf=fopen(shortFilePath.data(),"rb");
    }
    if(pf==NULL){
        statusDict["errorCode"]=-1;
        statusDict["errorInfo"]=filepath+" can't open.";
        return statusDict;
    }
    DataInputStreamSP dis=new DataInputStream(pf);
    std::unique_ptr<PickleUnmarshall> unmarshall(new PickleUnmarshall(dis));
    IO_ERR ret;
    short flag=0;
    if (!unmarshall->start(flag, true, ret)) {
        unmarshall->reset();
        statusDict["errorCode"]=(int)ret;
        statusDict["errorInfo"]="unmarshall failed";
        return statusDict;
    }
    PyObject * result = unmarshall->getPyObj();
    unmarshall->reset();
    py::object res = py::handle(result).cast<py::object>();
    res.dec_ref();
    return res;
}

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
    DLOG("~PytoDdbRowPool",rows_.size(),failedRows_.size());
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
            //RECORDTIME("rowPool:ConvertExecutor");
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
            for (size_t ni = 0; ni < insertRows.size(); ++ni) {
                py::gil_scoped_acquire gil;
                delete convertRows[ni];                                     // must delete it in GIL lock
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

}

