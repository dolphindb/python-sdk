#include "MultithreadedTableWriter.h"
#include "ScalarImp.h"
#include <thread>
#include "DdbPythonUtil.h"

namespace dolphindb{

#ifdef DLOG
    #undef DLOG
#endif
#define DLOG true?DLogger::GetMinLevel() : DLogger::Info

MultithreadedTableWriter::MultithreadedTableWriter(const std::string& hostName, int port, const std::string& userId, const std::string& password,
										const string& dbName, const string& tableName, bool useSSL,
										bool enableHighAvailability, const vector<string> *pHighAvailabilitySites,
										int batchSize, float throttle, int threadCount, const string& partitionCol,
										const vector<COMPRESS_METHOD> *pCompressMethods, MultithreadedTableWriter::Mode mode,
                                        vector<string> *pModeOption):
                        dbName_(dbName),
                        tableName_(tableName),
                        batchSize_(batchSize),
                        throttleMilsecond_(throttle*1000),
						exited_(false),
                        hasError_(false)
                        ,pytoDdb_(new PytoDdbRowPool(*this))
                        ,mode_(mode)
                        {
    if(threadCount < 1){
        throw RuntimeException("The parameter threadCount must be greater than or equal to 1");
    }
    if(batchSize < 1){
        throw RuntimeException("The parameter batchSize must be greater than or equal to 1");
    }
    if(throttle < 0){
        throw RuntimeException("The parameter throttle must be greater than 0");
    }
	if (threadCount > 1 && partitionCol.empty()) {
		throw RuntimeException("The parameter partitionCol must be specified when threadCount is greater than 1");
	}
	bool isCompress = false;
	int keepAliveTime = 7200;
	if (pCompressMethods != nullptr && pCompressMethods->size() > 0) {
		for (auto one : *pCompressMethods) {
			if (one != COMPRESS_DELTA && one != COMPRESS_LZ4) {
				throw RuntimeException("Unsupported compression method "+one);
			}
		}
		compressMethods_ = *pCompressMethods;
		isCompress = true;
	}
    SmartPointer<DBConnection> pConn=new DBConnection(useSSL, false, keepAliveTime, isCompress);
	vector<string> highAvailabilitySites;
	if (pHighAvailabilitySites != nullptr) {
		highAvailabilitySites.assign(pHighAvailabilitySites->begin(), pHighAvailabilitySites->end());
	}
    bool ret = pConn->connect(hostName, port, userId, password, "", enableHighAvailability, highAvailabilitySites, 30);
    if(!ret){
        throw RuntimeException("Failed to connect to server "+hostName+":"+std::to_string(port));
    }

    DictionarySP schema;
    if(dbName.empty()){
        schema = pConn->run("schema(" + tableName + ")");
    }else{
        schema = pConn->run(std::string("schema(loadTable(\"") + dbName + "\",\"" + tableName + "\"))");
    }
    ConstantSP partColNames = schema->getMember("partitionColumnName");
    if(partColNames->isNull()==false){//partitioned table
        isPartionedTable_ = true;
    }else{//Not partitioned table
        if (dbName.empty() == false) {//Single partitioned table
			if (threadCount > 1) {
				throw RuntimeException("The parameter threadCount must be 1 for a dimension table");
			}
		}
		isPartionedTable_ = false;
    }

    TableSP colDefs = schema->getMember("colDefs");

    ConstantSP colDefsTypeInt = colDefs->getColumn("typeInt");
    size_t columnSize = colDefs->size();
	if (compressMethods_.size() > 0 && compressMethods_.size() != columnSize) {
		throw RuntimeException("The number of elements in parameter compressMethods does not match the column size "+std::to_string(columnSize));
	}
 
	colExtras_.resize(columnSize);
	int colIndex = colDefs->getColumnIndex("extra");
	if (colIndex >= 0) {
		ConstantSP colExtra = colDefs->getColumn(colIndex);
		for (int i = 0; i < static_cast<int>(colExtra->size()); i++) {
			colExtras_[i] = colExtra->getInt(i);
		}
	} 
    
    ConstantSP colDefsName = colDefs->getColumn("name");
    ConstantSP colDefsTypeString = colDefs->getColumn("typeString");
    for(size_t i = 0; i < columnSize; i++){
        colNames_.push_back(colDefsName->getString(i));
        colTypes_.push_back(static_cast<DATA_TYPE>(colDefsTypeInt->getInt(i)));
        colTypeString_.push_back(colDefsTypeString->getString(i));
    }
	if (threadCount > 1) {//Only multithread need partition col info
		if (isPartionedTable_) {
			ConstantSP partitionSchema;
			int partitionType;
			DATA_TYPE partitionColType;
			if (partColNames->isScalar()) {
				if (partColNames->getString() != partitionCol) {
					throw RuntimeException("The parameter partionCol must be the partitioning column '" + partColNames->getString() + "' in the partitioned table");
				}
				partitionColumnIdx_ = schema->getMember("partitionColumnIndex")->getInt();
				partitionSchema = schema->getMember("partitionSchema");
				partitionType = schema->getMember("partitionType")->getInt();
				partitionColType = (DATA_TYPE)schema->getMember("partitionColumnType")->getInt();
			}
			else {
				int dims = partColNames->size();
				if (dims > 1 && partitionCol.empty()) {
					throw RuntimeException("The parameter partitionCol must be specified for a partitioned table");
				}
				int index = -1;
				for (int i = 0; i < dims; ++i) {
					if (partColNames->getString(i) == partitionCol) {
						index = i;
						break;
					}
				}
				if (index < 0)
					throw RuntimeException("The parameter partionCol must be the partitioning columns in the partitioned table");
				partitionColumnIdx_ = schema->getMember("partitionColumnIndex")->getInt(index);
				partitionSchema = schema->getMember("partitionSchema")->get(index);
				partitionType = schema->getMember("partitionType")->getInt(index);
				partitionColType = (DATA_TYPE)schema->getMember("partitionColumnType")->getInt(index);
			}
			if (colTypes_[partitionColumnIdx_] >= ARRAY_TYPE_BASE) {//arrayVector can't be partitioned
				throw RuntimeException("The parameter partitionCol cannot be array vector");
			}
			partitionDomain_ = Util::createDomain((PARTITION_TYPE)partitionType, partitionColType, partitionSchema);
		}
		else {//isPartionedTable_==false
			if (partitionCol.empty() == false) {
				int threadcolindex = -1;
				for (unsigned int i = 0; i < colNames_.size(); i++) {
					if (colNames_[i] == partitionCol) {
						threadcolindex = i;
						break;
					}
				}
				if (threadcolindex < 0) {
					throw RuntimeException("No match found for " + partitionCol);
				}
				if (colTypes_[threadcolindex] >= ARRAY_TYPE_BASE) {//arrayVector can't be partitioned
					throw RuntimeException("The parameter partitionCol cannot be array vector");
				}
				threadByColIndexForNonPartion_ = threadcolindex;
			}
		}
	}
    if(mode == M_Append){
        if(dbName_.empty()){//memory table
            scriptTableInsert_ = std::move(std::string("tableInsert{") + tableName_ + "}");
        }else if(isPartionedTable_){//partitioned table
            scriptTableInsert_ = std::move(std::string("tableInsert{loadTable(\"") + dbName_ + "\",\"" + tableName_ + "\")}");
        }else{//single partitioned table
            scriptTableInsert_ = std::move(std::string("tableInsert{loadTable(\"") + dbName_ + "\",\"" + tableName_ + "\")}");
        }
    }else if(mode == M_Upsert){
        //upsert!(obj, newData, [ignoreNull=false], [keyColNames], [sortColumns])
        if(dbName_.empty()){//memory table
            scriptTableInsert_ = std::move(std::string("upsert!{") + tableName_);
        }else if(isPartionedTable_){//partitioned table
            scriptTableInsert_ = std::move(std::string("upsert!{loadTable(\"") + dbName_ + "\",\"" + tableName_ + "\")");
        }else{//single partitioned table
            scriptTableInsert_ = std::move(std::string("upsert!{loadTable(\"") + dbName_ + "\",\"" + tableName_ + "\")");
        }
        //ignore newData
        scriptTableInsert_+=",";
        if(pModeOption!=nullptr && pModeOption->empty()==false){
            for(auto &one : *pModeOption){
                scriptTableInsert_+=","+one;
            }
        }
        scriptTableInsert_+="}";
    }
    // init done, start thread now.
    threads_.resize(threadCount);
    for(unsigned int i = 0; i < threads_.size(); i++){
        WriterThread &writerThread = threads_[i];
        writerThread.threadId = 0;
        writerThread.sentRows = 0;
        writerThread.sendingRows = 0;
		writerThread.exit = false;
        writerThread.idleSem.release();
        writerThread.conn = new DBConnection(useSSL, false, keepAliveTime, isCompress);
        if(writerThread.conn->connect(hostName, port, userId, password, "", enableHighAvailability, highAvailabilitySites, 30, true)==false){
            throw RuntimeException("Failed to connect to server "+hostName+":"+std::to_string(port));
        }
        writerThread.writeThread = new Thread(new SendExecutor(*this,writerThread));
        writerThread.writeThread->start();
    }
}

MultithreadedTableWriter::~MultithreadedTableWriter(){
    waitForThreadCompletion();
	{
		std::vector<ConstantSP>* pitem = nullptr;
		for (auto &thread : threads_) {
			while (thread.writeQueue.pop(pitem)) {
				delete pitem;
			}
			while (thread.failedQueue.pop(pitem)) {
				delete pitem;
			}
		}
		while (unusedQueue_.pop(pitem)) {
			delete pitem;
		}
        delete pytoDdb_;
	}
}

void MultithreadedTableWriter::waitForThreadCompletion() {
	LockGuard<Mutex> LockGuard(&exitMutex_);
	if (exited_)
		return;
    //if(pytoDdb_->isExit())
    //    return;
    pytoDdb_->startExit();
    for(auto &thread : threads_){
		thread.exit = true;
		thread.nonemptySignal.set();
    }
	for(auto &thread : threads_){
		thread.writeThread->join();
    }
	for (auto &thread : threads_) {
		thread.conn->close();
	}
    pytoDdb_->endExit();
	setError(0, "");
	exited_ = true;
}

void MultithreadedTableWriter::setError(const ErrorCodeInfo &errorInfo){
    if(hasError_.exchange(true))
        return;
	errorInfo_.set(errorInfo);
}

void MultithreadedTableWriter::setError(int code, const string &info){
    ErrorCodeInfo errorInfo;
    errorInfo.set(code,info);
    setError(errorInfo);
}

bool MultithreadedTableWriter::SendExecutor::init(){
	writeThread_.threadId = Util::getCurThreadId();
    return true;
}

bool MultithreadedTableWriter::insert(std::vector<ConstantSP> **records, int recordCount, ErrorCodeInfo &errorInfo){
	if (hasError_.load()) {
		errorInfo.set(ErrorCodeInfo::EC_DestroyedObject, "Thread is exiting.");
        return false;
	}
	//To speed up, ignore check
	/*
	for (auto &row : vectorOfVector) {
		if (row.size() != colNames_.size()) {
			errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Invalid vector size " + std::to_string(row.size()) + ", expect " + std::to_string(colNames_.size()));
			return false;
		}
		int index = 0;
		DATA_TYPE dataType;
		for (auto &param : row) {
			dataType = getColDataType(index);
			if (param->getType() != dataType && dataType != DATA_TYPE::DT_SYMBOL) {
				errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Object type mismatch " + Util::getDataTypeString(param->getType()) + ", expect " + Util::getDataTypeString(dataType));
				return false;
			}
			index++;
		}
	}
	*/
    if(threads_.size() > 1){
        if (isPartionedTable_) {
			VectorSP pvector = Util::createVector(getColDataType(partitionColumnIdx_), recordCount, 0);
			for (int i = 0; i < recordCount; i++) {
				pvector->set(i, records[i]->at(partitionColumnIdx_));
			}
			vector<int> threadindexes = partitionDomain_->getPartitionKeys(pvector);
			for (unsigned int row = 0; row < threadindexes.size(); row++) {
				insertThreadWrite(threadindexes[row], records[row]);
			}
		}else{
            int threadindex;
            for(int i=0; i < recordCount; i++){
                threadindex = records[i]->at(threadByColIndexForNonPartion_)->getHash(threads_.size());
                insertThreadWrite(threadindex, records[i]);
            }
        }
    }else{
        for(int i=0; i < recordCount; i++){
            insertThreadWrite(0, records[i]);
        }
    }
	return true;
}

void MultithreadedTableWriter::getStatus(Status &status){
    status.isExiting = hasError_.load();
    status.errorCode = errorInfo_.errorCode;
	status.errorInfo = errorInfo_.errorInfo;
	status.sentRows = status.unsentRows = status.sendFailedRows = 0;
	status.threadStatus.resize(threads_.size());
    for(size_t i = 0; i < threads_.size(); i++){
        ThreadStatus &threadStatus = status.threadStatus[i];
        WriterThread &writeThread = threads_[i];
		SemLock idleLock(writeThread.idleSem);
		idleLock.acquire();
        threadStatus.threadId = writeThread.threadId;
        threadStatus.sentRows = writeThread.sentRows;
        threadStatus.unsentRows = writeThread.writeQueue.size() + writeThread.sendingRows;
        threadStatus.sendFailedRows = writeThread.failedQueue.size();
        status.plus(threadStatus);
    }
}

void MultithreadedTableWriter::getUnwrittenData(std::vector<std::vector<ConstantSP>*> &unwrittenData){
    for(auto &writeThread : threads_){
        SemLock idleLock(writeThread.idleSem);
        idleLock.acquire();
        writeThread.failedQueue.pop(unwrittenData, writeThread.failedQueue.size());
        writeThread.writeQueue.pop(unwrittenData, writeThread.writeQueue.size());
    }
}

void MultithreadedTableWriter::insertThreadWrite(int threadhashkey, std::vector<ConstantSP> *prow){
    if(threadhashkey < 0){
        threadhashkey = 0;
    }
    int threadIndex = threadhashkey % threads_.size();
    WriterThread &writerThread = threads_[threadIndex];
    writerThread.writeQueue.push(prow);
    writerThread.nonemptySignal.set();
}

void MultithreadedTableWriter::SendExecutor::run(){
    if(init()==false){
        return;
    }
	long batchWaitTimeout = 0, diff;
    while(isExit() == false){
        {
            if(writeThread_.writeQueue.size() < 1){//Wait for first data
				writeThread_.nonemptySignal.wait();
            }
            if (isExit())
                break;
            //wait for batchsize
			if (tableWriter_.batchSize_ > 1 && tableWriter_.throttleMilsecond_ > 0) {
                batchWaitTimeout = Util::getEpochTime() + tableWriter_.throttleMilsecond_;
                while (isExit() == false && writeThread_.writeQueue.size() < tableWriter_.batchSize_) {//check batchsize
                    diff = batchWaitTimeout - Util::getEpochTime();
                    if (diff > 0) {
						writeThread_.nonemptySignal.tryWait(diff);
                    }
                    else {
                        break;
                    }
                }
            }
        }
        while (isExit() == false && writeAllData());//write all data
    }
    //write left data
    while (tableWriter_.hasError_.load() == false && writeAllData());
}

bool MultithreadedTableWriter::SendExecutor::writeAllData(){
    //reset idle
	DLOG("writeAllData", writeThread_.writeQueue.size());
    SemLock idleLock(writeThread_.idleSem);
    idleLock.acquire();
    long blockSize = (tableWriter_.batchSize_ > 65535) ? tableWriter_.batchSize_ : 65535;
    std::vector<std::vector<ConstantSP>*> items;
    {
        long size = writeThread_.writeQueue.size();
        if (size < 1){
            return false;
        }
        if(size > blockSize)
            size = blockSize;
        items.reserve(size);
        writeThread_.writeQueue.pop(items, size);
    }
    int size = items.size();
    if(size < 1){
        return false;
    }
    DLOG("writeAllData",size,"/",writeThread_.writeQueue.size());
    writeThread_.sendingRows = size;
    string runscript;
	bool writeOK = true;
    try{
        TableSP writeTable;
		int addRowCount = 0;
        {//create table
			writeTable = Util::createTable(tableWriter_.colNames_, tableWriter_.colTypes_, size, 0, tableWriter_.colExtras_);
			writeTable->setColumnCompressMethods(tableWriter_.compressMethods_);
            INDEX colSize= tableWriter_.colTypes_.size();
			for (INDEX col = 0; col < colSize; col++) {
				Vector *pcol = (Vector*) writeTable->getColumn(col).get();
				std::vector<ConstantSP>** pcur = items.data();
				if (pcol->getVectorType() != VECTOR_TYPE::ARRAYVECTOR) {
					for (int i = 0; i < size; i++, pcur++) {
						pcol->set(i, (*pcur)->at(col));
					}
				}
				else {
					pcol->clear();
					for (int i = 0; i < size; i++, pcur++) {
						pcol->append((*pcur)->at(col));
					}
				}
			}
			addRowCount += size;
        }
		if(writeOK && addRowCount > 0){//save table
            std::vector<ConstantSP> args(1);
            args[0] = writeTable;
            runscript = tableWriter_.scriptTableInsert_;
            ConstantSP constsp = writeThread_.conn->run(runscript, args);
            runscript.clear();
            if(constsp->getType() == DT_INT && constsp->getForm() == DF_SCALAR){
                int addresult = constsp->getInt();
                if (addresult != addRowCount) {
                    std::cout << "Rows changed: " << addresult << " / "<< addRowCount<< std::endl;
                }
            }else{
                std::cout << "None row changed of "<< addRowCount<< std::endl;
            }
            if (tableWriter_.scriptSaveTable_.empty() == false){
                runscript = tableWriter_.scriptSaveTable_;
                writeThread_.conn->run(runscript);
                runscript.clear();
            }
            {
                writeThread_.sentRows += addRowCount;
                writeThread_.sendingRows = 0;
            }
			{
				for (auto &one : items) {
					if (tableWriter_.unusedQueue_.size() < 65535) {
                        one->clear();
						tableWriter_.unusedQueue_.push(one);
					}
					else {
						delete one;
					}
				}
			}
        }
    }catch (std::exception &e){
        string errmsg=e.what();
        if(runscript.empty()==false && errmsg.find(" script:")==string::npos){
            errmsg+=" script: "+runscript;
        }
        DLogger::Error("threadid", writeThread_.threadId, "Failed to save the inserted data: ", errmsg);
        tableWriter_.setError(ErrorCodeInfo::EC_Server,std::string("Failed to save the inserted data: ")+errmsg);
		writeOK = false;
    }
    if (writeOK == false){
        for (auto &unwriteItem : items)
            writeThread_.failedQueue.push(unwriteItem);
		writeThread_.sendingRows = 0;
    }
	return true;
}

};
