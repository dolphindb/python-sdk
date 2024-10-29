/*
 * DolphinDB.cpp
 *
 *  Created on: Sep 22, 2018
 *      Author: dzhou
 */

#include <ctime>
#include <fstream>
#include <istream>
#include <stack>
#include "Concurrent.h"
#ifndef WINDOWS
#include <uuid/uuid.h>
#else
#include <Objbase.h>
#endif

#include "ConstantImp.h"
#include "ConstantMarshall.h"
#include "DolphinDB.h"
#include "ScalarImp.h"
#include "DolphinDBImp.h"
#include "Util.h"
#include "Logger.h"
#include "Domain.h"
#include "DBConnectionPoolImpl.h"
#include "Pickle.h"
#include "DdbPythonUtil.h"
#include "Wrappers.h"
#include "pybind11/numpy.h"

using std::ifstream;

#ifdef INDEX64
namespace std {
int min(int a, INDEX b) {
    return a < b ? a : (int)b;
}

int min(INDEX a, int b) {
    return a < b ? (int)a : b;
}
}    // namespace std
#endif

//#define APIMinVersionRequirement 100




#define RECORDTIME(name) //RecordTime _##name(name)


#ifdef DLOG
    #undef DLOG
#endif
#define DLOG true?DLogger::GetMinLevel() : DLogger::Info



namespace dolphindb {

ProtectGil::ProtectGil(bool release, const string &name) {
    name_ = name;
    DLOG("GIL...",name_,!release);
    if(release == false){
        gstate_ = PyGILState_Ensure();
        acquired_ = true;
    }else{
        acquired_ = false;
        if(PyGILState_Check() == 1){
            pgilRelease_ = new py::gil_scoped_release;
        }
    }
    DLOG("GIL",name_,acquired_);
}
void ProtectGil::acquire(){
    if(acquired_)
        return;
    DLOG("acquireGIL...",name_,acquired_);
    pgilRelease_.clear();
    gstate_ = PyGILState_Ensure();
    acquired_ = true;
    DLOG("acquireGIL",name_,acquired_);
}
ProtectGil::~ProtectGil() {
    DLOG("~Gil...",name_,acquired_);
    if(acquired_)
        PyGILState_Release(gstate_);
    else
        pgilRelease_.clear();
    DLOG("~Gil",name_,acquired_);
}








class HIDEVISIBILITY AsynWorker: public Runnable {
public:
    using Task = DBConnectionPoolImpl::Task;
    AsynWorker(DBConnectionPoolImpl& pool, CountDownLatchSP latch, const SmartPointer<DBConnection>& conn,
               const SmartPointer<SynchronizedQueue<Task>>& queue, TaskStatusMgmt& status,
               const string& hostName, int port, const string& userId , const string& password)
            : pool_(pool), latch_(latch), conn_(conn), queue_(queue),taskStatus_(status),
              hostName_(hostName), port_(port), userId_(userId), password_(password){}
protected:
    virtual void run();

private:
    DBConnectionPoolImpl& pool_;
    CountDownLatchSP latch_;
    SmartPointer<DBConnection> conn_;
	SmartPointer<SynchronizedQueue<Task>> queue_;
    TaskStatusMgmt& taskStatus_;
    const string hostName_;
    int port_;
    const string userId_;
    const string password_;
};

string Constant::EMPTY("");
string Constant::NULL_STR("NULL");
ConstantSP Constant::void_(new Void());
ConstantSP Constant::null_(new Void(true));
ConstantSP Constant::true_(new Bool(true));
ConstantSP Constant::false_(new Bool(false));
ConstantSP Constant::one_(new Int(1));



int Constant::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int cellCountToSerialize, int& numElement, int& partial) const {
    throw RuntimeException(Util::getDataFormString(getForm())+"_"+Util::getDataTypeString(getType())+" serialize cell method not supported");
}

int Constant::serialize(char* buf, int bufSize, INDEX indexStart, int offset, int& numElement, int& partial) const {
    throw RuntimeException(Util::getDataFormString(getForm())+"_"+Util::getDataTypeString(getType())+" serialize method not supported");
}

IO_ERR Constant::deserialize(DataInputStream* in, INDEX indexStart, INDEX targetNumElement, INDEX& numElement) {
    throw RuntimeException(Util::getDataFormString(getForm())+"_"+Util::getDataTypeString(getType())+" deserialize method not supported");
}

ConstantSP Constant::getRowLabel() const {
    return void_;
}

ConstantSP Constant::getColumnLabel() const {
    return void_;
}









DBConnection::DBConnection(bool enableSSL, bool asynTask, int keepAliveTime, bool compress, bool python, bool isReverseStreaming, int sqlStd) :
	conn_(new DBConnectionImpl(enableSSL, asynTask, keepAliveTime, compress, python, isReverseStreaming, sqlStd)), uid_(""), pwd_(""), ha_(false),
		enableSSL_(enableSSL), asynTask_(asynTask), compress_(compress), python_(python), nodes_({}), protocol_(PROTOCOL_DDB),
		lastConnNodeIndex_(0), reconnect_(false), closed_(true), msg_(true), tryReconnectNums_(-1) {
}

DBConnection::DBConnection(DBConnection&& oth) :
		conn_(move(oth.conn_)), uid_(move(oth.uid_)), pwd_(move(oth.pwd_)),
		initialScript_(move(oth.initialScript_)), ha_(oth.ha_), enableSSL_(oth.enableSSL_),
		asynTask_(oth.asynTask_),compress_(oth.compress_),nodes_(oth.nodes_),lastConnNodeIndex_(0),
		reconnect_(oth.reconnect_), closed_(oth.closed_), msg_(oth.msg_){}

DBConnection& DBConnection::operator=(DBConnection&& oth) {
    if (this == &oth) { return *this; }
    conn_ = move(oth.conn_);
    uid_ = move(oth.uid_);
    pwd_ = move(oth.pwd_);
    initialScript_ = move(oth.initialScript_);
    ha_ = oth.ha_;
    nodes_ = oth.nodes_;
    oth.nodes_.clear();
    enableSSL_ = oth.enableSSL_;
    asynTask_ = oth.asynTask_;
	compress_ = oth.compress_;
	lastConnNodeIndex_ = oth.lastConnNodeIndex_;
	reconnect_ = oth.reconnect_;
	closed_ = oth.closed_;
    return *this;
}

DBConnection::~DBConnection() {
    close();
}

bool DBConnection::connect(const string& hostName, int port, const string& userId, const string& password, const string& startup,
                           bool ha, const vector<string>& highAvailabilitySites, int keepAliveTime, bool reconnect, int tryReconnectNums, int readTimeout, int writeTimeout) {
    ha_ = ha;
	uid_ = userId;
	pwd_ = password;
    initialScript_ = startup;
	reconnect_ = reconnect;
	closed_ = false;
    setKeepAliveTime(keepAliveTime);
    setTimeout(readTimeout, writeTimeout);

    tryReconnectNums_ = (tryReconnectNums <= 0) ? -1 : tryReconnectNums;

    if (ha_) {
		for (auto &one : highAvailabilitySites)
			nodes_.push_back(Node(one));
		{
			bool foundfirst = false;
			Node firstnode(hostName, port);
			for (auto &one : nodes_)
				if (one.isEqual(firstnode)) {
					foundfirst = true;
					break;
				}
			if(!foundfirst)
				nodes_.push_back(firstnode);
		}
		Node connectedNode;
		TableSP table;
		while (closed_ == false) {
            int attempt = 0;
			while(conn_->isConnected()==false && closed_ == false) {
                ++attempt;
				for (auto &one : nodes_) {
					if (connectNode(one.hostName, one.port, keepAliveTime)) {
						connectedNode = one;
						break;
					}
					Thread::sleep(100);
				}
                if (tryReconnectNums_ > 0 && attempt >= tryReconnectNums_) {
                    std::cerr<< "Connect failed after " << tryReconnectNums_ << " reconnect attempts for every node in high availability sites.";
                    return false;
                }
			}
			try {
				table = conn_->run("rpc(getControllerAlias(), getClusterPerf)");
                break;
			}
			catch (exception& e) {
				std::cerr << "ERROR getting other data nodes, exception: " << e.what() << std::endl;
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						continue;
					else if (type == ET_NEWLEADER || type == ET_NODENOTAVAIL) {
						switchDataNode(host, port);
					}
				}
				else {
                    parseException(e.what(), host, port);
					switchDataNode(host, port);
				}
			}
		}
        if(table->getForm() != DF_TABLE){
            throw IOException("Run getClusterPerf() failed.");
        }
		VectorSP colHost = table->getColumn("host");
		VectorSP colPort = table->getColumn("port");
		VectorSP colMode = table->getColumn("mode");
		VectorSP colmaxConnections = table->getColumn("maxConnections");
		VectorSP colconnectionNum = table->getColumn("connectionNum");
		VectorSP colworkerNum = table->getColumn("workerNum");
		VectorSP colexecutorNum = table->getColumn("executorNum");
		double load;
		for (int i = 0; i < colMode->rows(); i++) {
			if (colMode->getInt(i) == 0) {
				string nodeHost = colHost->getString(i);
				int nodePort = colPort->getInt(i);
				Node *pexistNode = NULL;
				if (!highAvailabilitySites.empty()) {
					for (auto &node : nodes_) {
						if (node.hostName.compare(nodeHost) == 0 &&
							node.port == nodePort) {
							pexistNode = &node;
							break;
						}
					}
					//node is out of highAvailabilitySites
					if (pexistNode == NULL) {
						DLogger::Info("Site", nodeHost, ":", nodePort,"is not in cluster.");
						continue;
					}
				}
				if (colconnectionNum->getInt(i) < colmaxConnections->getInt(i)) {
					load = (colconnectionNum->getInt(i) + colworkerNum->getInt(i) + colexecutorNum->getInt(i)) / 3.0;
					//DLogger::Info("Site", nodeHost, ":", nodePort,"load",load);
				}
				else {
					load = DBL_MAX;
				}
				if (pexistNode != NULL) {
					pexistNode->load = load;
				}
				else {
					nodes_.push_back(Node(colHost->get(i)->getString(), colPort->get(i)->getInt(), load));
				}
			}
		}
		Node *pMinNode=NULL;
		for (auto &one : nodes_) {
			if (pMinNode == NULL || 
				(one.load >= 0 && pMinNode->load > one.load)) {
				pMinNode = &one;
			}
		}
		//DLogger::Info("Connect to min load", pMinNode->load, "site", pMinNode->hostName, ":", pMinNode->port);
		
		if (!pMinNode->isEqual(connectedNode)) {
			conn_->close();
			switchDataNode(pMinNode->hostName, pMinNode->port);
			return true;
		}
    } else {
		if (reconnect_) {
			nodes_.push_back(Node(hostName, port));
            try {
                switchDataNode(hostName, port);
            }
            catch (std::exception &e) {
                std::cerr << e.what() << std::endl;
                return false;
            }
		}
		else {
			if (!connectNode(hostName, port, keepAliveTime))
				return false;
		}
    }
	if (!initialScript_.empty()) {
		run(initialScript_);
	}
	return true;
}

bool DBConnection::connected() {
    try {
        ConstantSP ret = conn_->run("1+1");
        return !ret.isNull() && (ret->getInt() == 2);
    } catch (exception&) {
        return false;
    }
}

bool DBConnection::connectNode(string hostName, int port, int keepAliveTime) {
	DLOG("Connect to",hostName ,":",port,".");
	//int attempt = 0;
	while (closed_ == false) {
		try {
			return conn_->connect(hostName, port, uid_, pwd_, enableSSL_, asynTask_, keepAliveTime, compress_, python_);
		}
		catch (IOException& e) {
			if (connected()) {
				ExceptionType type = parseException(e.what(), hostName, port);
				if (type != ET_NEWLEADER) {
					if (type == ET_IGNORE)
						return true;
					else if (type == ET_NODENOTAVAIL)
						return false;
					else { //UNKNOW
						std::cerr << "Connect " << hostName << ":" << port << " failed, exception message: " << e.what() << std::endl;
						return false;
						//throw;
					}
				}
			}
			else {
                std::cerr << e.what() << std::endl;
				return false;
			}
		}
		Util::sleep(100);
	}
	return false;
}

DBConnection::ExceptionType DBConnection::parseException(const string &msg, string &host, int &port) {
	size_t index = msg.find("<NotLeader>");
	if (index != string::npos) {
		index = msg.find(">");
		string ipport = msg.substr(index + 1);
		parseIpPort(ipport,host,port);
		DLogger::Info("Got NotLeaderException, switch to leader node [",host,":",port,"] to run script");
		return ET_NEWLEADER;
	}
	else {
		static string ignoreMsgs[] = { "<ChunkInTransaction>","<DataNodeNotAvail>","<DataNodeNotReady>","DFS is not enabled" };
		static int ignoreMsgSize = sizeof(ignoreMsgs) / sizeof(string);
		for (int i = 0; i < ignoreMsgSize; i++) {
			index = msg.find(ignoreMsgs[i]);
			if (index != string::npos) {
				if (i == 0) {//case ChunkInTransaction should sleep 1 minute for transaction timeout
					Util::sleep(10000);
				}
				host.clear();
				return ET_NODENOTAVAIL;
			}
		}
		return ET_UNKNOW;
	}
}

void DBConnection::switchDataNode(const string &host, int port) {
    int attempt = 0;
    bool connected = false;
    while (closed_ == false && connected == false && (tryReconnectNums_ < 0 || attempt < tryReconnectNums_)){
        if (!host.empty()) {
            if (connectNode(host, port)) {
                connected = true;
                break;
            }
        }
        else {
            if (nodes_.empty()) {
                throw RuntimeException("Failed to connect to " + host + ":" + std::to_string(port));
            }
            for (int i = static_cast<int>(nodes_.size() - 1); i >= 0; i--) {
                lastConnNodeIndex_ = (lastConnNodeIndex_ + 1) % nodes_.size();
                if (connectNode(nodes_[lastConnNodeIndex_].hostName, nodes_[lastConnNodeIndex_].port)) {
                    connected = true;
                    break;
                }
            }
        }
        Thread::sleep(1000);
        ++attempt;
    }
    if (!closed_ && !connected) {
        if (host.empty()) {
            throw RuntimeException(std::string("Connect to nodes failed after ") + std::to_string(attempt) + " reconnect attempts.");
        }
        throw RuntimeException(std::string("Connect to ") + host + ":" + std::to_string(port) + " failed after " + std::to_string(attempt) + " reconnect attempts.");
    }
    if (connected && initialScript_.empty() == false)
        run(initialScript_);
}

void DBConnection::login(const string& userId, const string& password, bool enableEncryption) {
    conn_->login(userId, password, enableEncryption);
    uid_ = userId;
    pwd_ = password;
}

ConstantSP DBConnection::run(const string& script, int priority, int parallelism, int fetchSize, bool clearMemory) {
    if (nodes_.empty()==false) {
		while(closed_ == false){
			try {
				return conn_->run(script, priority, parallelism, fetchSize, clearMemory);
			}
			catch (IOException& e) {
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						return new Void();
					else if (type == ET_UNKNOW)
						throw;
				}
				else {
					parseException(e.what(), host, port);
				}
				switchDataNode(host, port);
			}
		}
    } else {
        return conn_->run(script, priority, parallelism, fetchSize, clearMemory);
    }
    return NULL;
}

py::object DBConnection::runPy(
    const string &script, int priority, int parallelism,
    int fetchSize, bool clearMemory, bool pickleTableToList, bool disableDecimal
) {
    if (nodes_.empty() == false) {
        while (closed_ == false) {
            try {
                return conn_->runPy(script, priority, parallelism, fetchSize, clearMemory, pickleTableToList, disableDecimal);
            } catch (IOException& e) {
                string host;
                int port = 0;
                if (connected()) {
                    ExceptionType type = parseException(e.what(), host, port);
                    if (type == ET_IGNORE)
                        return py::none();
                    else if (type == ET_UNKNOW)
                        throw;
                }
                else{
                	parseException(e.what(), host, port);
                }
                if (!ha_) {
                    switchDataNode(nodes_.back().hostName, nodes_.back().port);
                }
                else {
                    switchDataNode(host, port);
                }
            }
        }
        return py::none();
    } else {
        return conn_->runPy(script, priority, parallelism, fetchSize, clearMemory, pickleTableToList, disableDecimal);
    }
}

ConstantSP DBConnection::run(const string& funcName, vector<dolphindb::ConstantSP>& args, int priority, int parallelism, int fetchSize, bool clearMemory) {
    if (nodes_.empty() == false) {
        while (closed_ == false) {
			try {
				return conn_->run(funcName, args, priority, parallelism, fetchSize, clearMemory);
			}
			catch (IOException& e) {
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						return new Void();
					else if (type == ET_UNKNOW)
						throw;
				}
				else {
					parseException(e.what(), host, port);
				}
				switchDataNode(host, port);
			}
		}
    } else {
        return conn_->run(funcName, args, priority, parallelism, fetchSize, clearMemory);
    }
    return NULL;
}

py::object DBConnection::runPy(
    const string &funcName, vector<ConstantSP> &args, int priority, int parallelism,
    int fetchSize, bool clearMemory, bool pickleTableToList, bool disableDecimal
) {
    if (nodes_.empty() == false) {
        while (closed_ == false) {
            try {
                return conn_->runPy(funcName, args, priority, parallelism, fetchSize, clearMemory, pickleTableToList, disableDecimal);
            } catch (IOException& e) {
                string host;
                int port = 0;
                if (connected()) {
                    ExceptionType type = parseException(e.what(), host, port);
                    if (type == ET_IGNORE)
                        return py::none();
                    else if (type == ET_UNKNOW)
                        throw;
                }else{
                	parseException(e.what(), host, port);
                }
                switchDataNode(host, port);
            }
        }
        return py::none();
    } else {
        return conn_->runPy(funcName, args, priority, parallelism, fetchSize, clearMemory, pickleTableToList, disableDecimal);
    }
}

ConstantSP DBConnection::upload(const string& name, const ConstantSP& obj) {
    if (nodes_.empty() == false) {
		while (closed_ == false) {
			try {
				return conn_->upload(name, obj);
			}
			catch (IOException& e) {
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						return Constant::void_;
					else if (type == ET_UNKNOW)
						throw;
				}
				else {
					parseException(e.what(), host, port);
				}
				switchDataNode(host, port);
			}
		}
    } else {
        return conn_->upload(name, obj);
    }
	return Constant::void_;
}

ConstantSP DBConnection::upload(vector<string>& names, vector<ConstantSP>& objs) {
    if (nodes_.empty() == false) {
		while(closed_ == false){
			try {
				return conn_->upload(names, objs);
			}
			catch (IOException& e) {
				string host;
				int port = 0;
				if (connected()) {
					ExceptionType type = parseException(e.what(), host, port);
					if (type == ET_IGNORE)
						return Constant::void_;
					else if (type == ET_UNKNOW)
						throw;
				}
				else {
					parseException(e.what(), host, port);
				}
				switchDataNode(host, port);
			}
		}
    } else {
        return conn_->upload(names, objs);
    }
	return Constant::void_;
}

void DBConnection::parseIpPort(const string &ipport, string &ip, int &port) {
	auto v = Util::split(ipport, ':');
	if (v.size() < 2) {
		throw RuntimeException("The format of highAvailabilitySite " + ipport +
			" is incorrect, should be host:port, e.g. 192.168.1.1:8848");
	}
	ip = v[0];
	port = std::stoi(v[1]);
	if (port <= 0 || port > 65535) {
		throw RuntimeException("The format of highAvailabilitySite " + ipport +
			" is incorrect, port should be a positive integer less or equal to 65535");
	}
}

DBConnection::Node::Node(const string &ipport, double loadValue) {
	DBConnection::parseIpPort(ipport, hostName, port);
	load = loadValue;
}

void DBConnection::close() {
	closed_ = true;
    if (conn_) conn_->close();
}

const std::string& DBConnection::getInitScript() const {
    return initialScript_;
}

DataInputStreamSP DBConnection::getDataInputStream()
{
    return conn_->getDataInputStream();
}

void DBConnection::setInitScript(const std::string & script) {
    initialScript_ = script;
}

void DBConnection::setKeepAliveTime(int keepAliveTime){
    conn_->setkeepAliveTime(keepAliveTime);
}

void DBConnection::setTimeout(int readTimeout, int writeTimeout) {
    conn_->setTimeout(readTimeout, writeTimeout);
}

void DBConnection::setProtocol(PROTOCOL protocol) {
    protocol_ = protocol;
    conn_->setProtocol(protocol);
}

void DBConnection::setShowOutput(bool flag) {
    msg_ = flag;
    conn_->setShowOutput(flag);
}

const string DBConnection::getSessionId() const {
    return conn_->getSessionId();
}


BlockReader::BlockReader(const DataInputStreamSP& in ) : in_(in), total_(0), currentIndex_(0){
    int rows, cols;
    if(in->readInt(rows) != OK)
        throw IOException("Failed to read rows for data block.");
    if(in->readInt(cols) != OK)
        throw IOException("Faield to read col for data block.");
    total_ = (long long)rows * (long long)cols;
}

BlockReader::~BlockReader(){
}

ConstantSP BlockReader::read(){
    if(currentIndex_>=total_)
        return NULL;
    IO_ERR ret;
    short flag;
    if ((ret = in_->readShort(flag)) != OK)
        throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));

    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    ConstantUnmarshallFactory factory(in_);
    ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
    if(unmarshall==NULL)
        throw IOException("Failed to parse the incoming object" + std::to_string(form));
    if (!unmarshall->start(flag, true, ret)) {
        unmarshall->reset();
        throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
    }
    ConstantSP result = unmarshall->getConstant();
    unmarshall->reset();
    currentIndex_ ++;
    return result;
}

void BlockReader::skipAll(){
    while(read().isNull()==false);
}






DBConnectionPool::DBConnectionPool(const string& hostName, int port, int threadNum, const string& userId, const string& password,
				bool loadBalance, bool highAvailability, bool compress, bool reConnect, bool python, PROTOCOL protocol, bool showOutput, int sqlStd, int tryReconnectNums)
    : pool_(new DBConnectionPoolImpl(hostName, port, threadNum, userId, password, loadBalance, highAvailability, compress,reConnect,python,protocol,showOutput, sqlStd, tryReconnectNums))
{}
DBConnectionPool::~DBConnectionPool(){}
void DBConnectionPool::run(const string& script, int identity, int priority, int parallelism, int fetchSize, bool clearMemory){
    if(identity < 0)
        throw RuntimeException("Invalid identity: " + std::to_string(identity) + ". Identity must be a non-negative integer.");
    pool_->run(script, identity, priority, parallelism, fetchSize, clearMemory);
}

void DBConnectionPool::run(const string& functionName, const vector<ConstantSP>& args, int identity, int priority, int parallelism, int fetchSize, bool clearMemory){
    if(identity < 0)
        throw RuntimeException("Invalid identity: " + std::to_string(identity) + ". Identity must be a non-negative integer.");
    pool_->run(functionName, args, identity, priority, parallelism, fetchSize, clearMemory);
}

void DBConnectionPool::runPy(const string& script, int identity, int priority, int parallelism, int fetchSize, bool clearMemory, bool pickleTableToList, bool disableDecimal){
    if(identity < 0)
        throw RuntimeException("Invalid identity: " + std::to_string(identity) + ". Identity must be a non-negative integer.");
    pool_->runPy(script, identity, priority, parallelism, fetchSize, clearMemory, pickleTableToList, disableDecimal);
}

void DBConnectionPool::runPy(const string& functionName, const vector<ConstantSP>& args, int identity, int priority, int parallelism, int fetchSize, bool clearMemory, bool pickleTableToList, bool disableDecimal){
    if(identity < 0)
        throw RuntimeException("Invalid identity: " + std::to_string(identity) + ". Identity must be a non-negative integer.");
    pool_->runPy(functionName, args, identity, priority, parallelism, fetchSize, clearMemory, pickleTableToList, disableDecimal);
}

bool DBConnectionPool::isFinished(int identity){
    return pool_->isFinished(identity);
}

ConstantSP DBConnectionPool::getData(int identity){
    return pool_->getData(identity);
}

py::object DBConnectionPool::getPyData(int identity){
    return pool_->getPyData(identity);
}

void DBConnectionPool::shutDown(){
    pool_->shutDown();
}

bool DBConnectionPool::isShutDown(){
    return pool_->isShutDown();
}

int DBConnectionPool::getConnectionCount(){
    return pool_->getConnectionCount();
}

vector<string> DBConnectionPool::getSessionId(){
    return pool_->getSessionId();
}

PartitionedTableAppender::PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, DBConnectionPool& pool) {
    pool_ = pool.pool_;
    init(dbUrl, tableName, partitionColName, "");
}

PartitionedTableAppender::PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, string appendFunction, DBConnectionPool& pool) {
    pool_ = pool.pool_;
    init(dbUrl, tableName, partitionColName, appendFunction);
}
PartitionedTableAppender::~PartitionedTableAppender(){}
void PartitionedTableAppender::init(string dbUrl, string tableName, string partitionColName, string appendFunction){
    threadCount_ = pool_->getConnectionCount();
    chunkIndices_.resize(threadCount_);
    ConstantSP partitionSchema;
    TableSP colDefs;
    VectorSP colNames;
    VectorSP typeInts;
    VectorSP exparams;
    int partitionType;
    DATA_TYPE partitionColType;
    
    try {
        string task;
        if(dbUrl == ""){
            task = "schema(" + tableName+ ")";
            appendScript_ = "tableInsert{" + tableName + "}";
        }
        else{
            task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
            appendScript_ = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
        }
        if(appendFunction != ""){
            appendScript_ = appendFunction;
        }
        
        pool_->run(task,identity_);

        while(!pool_->isFinished(identity_)){
            Util::sleep(10);
        }
        
        tableInfo_ = pool_->getData(identity_);
        identity_ --;
        ConstantSP partColNames = tableInfo_->getMember("partitionColumnName");
        if(partColNames->isNull())
            throw RuntimeException("Can't find specified partition column name.");
        
        if(partColNames->isScalar()){
            if(partColNames->getString() != partitionColName)
                throw  RuntimeException("Can't find specified partition column name.");
            partitionColumnIdx_ = tableInfo_->getMember("partitionColumnIndex")->getInt();
            partitionSchema = tableInfo_->getMember("partitionSchema");
            partitionType =  tableInfo_->getMember("partitionType")->getInt();
            partitionColType = (DATA_TYPE)tableInfo_->getMember("partitionColumnType")->getInt();
        }
        else{
            int dims = partColNames->size();
            int index = -1;
            for(int i=0; i<dims; ++i){
                if(partColNames->getString(i) == partitionColName){
                    index = i;
                    break;
                }
            }
            if(index < 0)
                throw RuntimeException("Can't find specified partition column name.");
            partitionColumnIdx_ = tableInfo_->getMember("partitionColumnIndex")->getInt(index);
            partitionSchema = tableInfo_->getMember("partitionSchema")->get(index);
            partitionType =  tableInfo_->getMember("partitionType")->getInt(index);
			partitionColType = (DATA_TYPE)tableInfo_->getMember("partitionColumnType")->getInt(index);
        }

        colDefs = tableInfo_->getMember("colDefs");
        cols_ = colDefs->rows();
        typeInts = colDefs->getColumn("typeInt");
        colNames = colDefs->getColumn("name");
        bool hasExtra = false;
        for (int i = 0; i < colDefs->columns(); ++i) {
            if (colDefs->getColumnName(i) == "extra") {
                hasExtra = true;
                break;
            }
        }
        if (hasExtra)
            exparams = colDefs->getColumn("extra");
        columnCategories_.resize(cols_);
        columnTypes_.resize(cols_);
        columnNames_.resize(cols_);
        for (int i = 0; i < cols_; ++i) {
            DATA_TYPE type = (DATA_TYPE)typeInts->getInt(i);
            if (hasExtra)
                columnTypes_[i] = converter::createType(type, exparams->getInt(i));
            else
                columnTypes_[i] = converter::createType(type, 0);
            columnCategories_[i] = Util::getCategory(type);
            columnNames_[i] = colNames->getString(i);
        }
        
        domain_ = Util::createDomain((PARTITION_TYPE)partitionType, partitionColType, partitionSchema);
    } catch (exception&) {
        throw;
    } 
}

int PartitionedTableAppender::append(TableSP table){
    if(cols_ != table->columns())
        throw RuntimeException("The input table doesn't match the schema of the target table.");
    for(int i=0; i<cols_; ++i){
        VectorSP curCol = table->getColumn(i);
        checkColumnType(i, curCol->getCategory(), curCol->getType());
		// if (columnCategories_[i] == TEMPORAL && curCol->getType() != columnTypes_[i]) {
		// 	curCol = curCol->castTemporal(columnTypes_[i]);
		// 	table->setColumn(i, curCol);
		// }
    }
    
    for(int i=0; i<threadCount_; ++i)
        chunkIndices_[i].clear();
    vector<int> keys = domain_->getPartitionKeys(table->getColumn(partitionColumnIdx_));
    vector<int> tasks;
    int rows = static_cast<int>(keys.size());
    for(int i=0; i<rows; ++i){
        int key = keys[i];
        if(key >= 0)
            chunkIndices_[key % threadCount_].emplace_back(i);
		else {
			throw RuntimeException("A value-partition column contain null value at row " + std::to_string(i) + ".");
		}
    }
    for(int i=0; i<threadCount_; ++i){
        if(chunkIndices_[i].size() == 0)
            continue;
        TableSP subTable = table->getSubTable(chunkIndices_[i]);
        tasks.push_back(identity_);
        vector<ConstantSP> args = {subTable};
        pool_->run(appendScript_, args, identity_--); 
        
    }
    int affected = 0;
    for(auto& task : tasks){
        while(!pool_->isFinished(task)){
            Util::sleep(100);
        }
        ConstantSP res = pool_->getData(task);
        if(res->isNull()){
            affected = 0;
        }
        else{
            affected += res->getInt();
        }
    }
    return affected;
}

vector<Type> PartitionedTableAppender::getColTypes() {
    return columnTypes_;
}

vector<string> PartitionedTableAppender::getColNames() {
    return columnNames_;
}

void PartitionedTableAppender::checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type) {
    if((DATA_TYPE)columnTypes_[col].first != type){
        DATA_CATEGORY expectCategory = columnCategories_[col];
        if (category != expectCategory) {
            throw RuntimeException("column [" + columnNames_[col] + "], expect type " + converter::getDataTypeString(columnTypes_[col]) + ", got type " + Util::getDataTypeString(type));
        }
    }
}

AutoFitTableAppender::AutoFitTableAppender(string dbUrl, string tableName, DBConnection& conn) : conn_(conn){
    ConstantSP schema;
    TableSP colDefs;
    VectorSP typeInts;
    VectorSP exparams;
    DictionarySP tableInfo;
    VectorSP colNames;
    try {
        string task;
        if(dbUrl == ""){
            task = "schema(" + tableName+ ")";
            appendScript_ = "tableInsert{" + tableName + "}";
        }
        else{
            task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
            appendScript_ = "tableInsert{loadTable('" + dbUrl + "', '" + tableName + "')}";
        }
        
        tableInfo =  conn_.run(task);
        colDefs = tableInfo->getMember("colDefs");
        cols_ = colDefs->rows();
        typeInts = colDefs->getColumn("typeInt");
        colNames = colDefs->getColumn("name");
        bool hasExtra = false;
        for (int i = 0; i < colDefs->columns(); ++i) {
            if (colDefs->getColumnName(i) == "extra") {
                hasExtra = true;
                break;
            }
        }
        if (hasExtra)
            exparams = colDefs->getColumn("extra");
        columnCategories_.resize(cols_);
        columnTypes_.resize(cols_);
        columnNames_.resize(cols_);
        for (int i = 0; i < cols_; ++i) {
            DATA_TYPE type = (DATA_TYPE)typeInts->getInt(i);
            if (hasExtra)
                columnTypes_[i] = converter::createType(type, exparams->getInt(i));
            else
                columnTypes_[i] = converter::createType(type, 0);
            columnCategories_[i] = Util::getCategory(type);
            columnNames_[i] = colNames->getString(i);
        }
        
    } catch (exception&) {
        throw;
    } 
}

int AutoFitTableAppender::append(TableSP table){
    if(cols_ != table->columns())
        throw RuntimeException("The input table columns doesn't match the columns of the target table.");
    
    vector<ConstantSP> columns;
    for(int i = 0; i < cols_; i++){
        VectorSP curCol = table->getColumn(i);
        checkColumnType(i, curCol->getCategory(), curCol->getType());
        if(columnCategories_[i] == TEMPORAL && curCol->getType() != (DATA_TYPE)columnTypes_[i].first){
            columns.push_back(curCol->castTemporal((DATA_TYPE)columnTypes_[i].first));
        }else{
            columns.push_back(curCol);
        }
    }
    TableSP tableInput = Util::createTable(columnNames_, columns);
    vector<ConstantSP> arg = {tableInput};
    ConstantSP res =  conn_.run(appendScript_, arg);
    if(res->isNull())
        return 0;
    else
        return res->getInt();
}

vector<Type> AutoFitTableAppender::getColTypes() {
    return columnTypes_;
}

vector<string> AutoFitTableAppender::getColNames() {
    return columnNames_;
}

void AutoFitTableAppender::checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type) {
    if((DATA_TYPE)columnTypes_[col].first != type){
        DATA_CATEGORY expectCategory = columnCategories_[col];
        if (category != expectCategory) {
            throw  RuntimeException("column [" + columnNames_[col] + "], expect type " + converter::getDataTypeString(columnTypes_[col]) + ", got type " + Util::getDataTypeString(type));
        }
    }
}


std::string defineInsertScript(
    DBConnection &conn,
    const std::string &dbUrl,
    const std::string &tableName,
    bool ignoreNull=false,
    std::vector<std::string> *pkeyColNames=nullptr,
    std::vector<std::string> *psortColumns=nullptr
) {
    /**
     * (def(mutable tb, data){upsert!(tb, data);return 0;}){tableName}
     * (def(mutable tb, data){upsert!(tb, data);return 0;}){loadTable(dbUrl, tableName)}
    */
    std::string script = "(def(mutable tb, data){upsert!(tb, data";
    if (!ignoreNull) script += ",ignoreNull=false";
    else script += ",ignoreNull=true";

    if (pkeyColNames != nullptr && pkeyColNames->empty() == false) {
        script += ",keyColNames=";
        for (const auto &one : *pkeyColNames) {
            script += "`" + one;
        }
    }
    if (psortColumns != nullptr && psortColumns->empty() == false) {
        script += ",sortColumns=";
        for (const auto &one : *psortColumns) {
            script += "`" + one;
        }
    }
    script += ");return 0;}){";

    if (dbUrl == "") script += tableName + "}";
    else script += "loadTable('" + dbUrl + "', '" + tableName + "')}";

    return script;
}


AutoFitTableUpsert::AutoFitTableUpsert(string dbUrl, string tableName, DBConnection& conn,bool ignoreNull,
                                        vector<string> *pkeyColNames,vector<string> *psortColumns)
                        : conn_(conn){
    ConstantSP schema;
    TableSP colDefs;
    VectorSP typeInts;
    VectorSP exparams;
    DictionarySP tableInfo;
    VectorSP colNames;
    string functionDef;
    try {
        std::string task;
        if (dbUrl == "") task = "schema(" + tableName + ")";
        else task = "schema(loadTable(\"" + dbUrl + "\", \"" + tableName + "\"))";
        tableInfo = conn_.run(task);
        colDefs = tableInfo->getMember("colDefs");
        cols_ = colDefs->rows();
        typeInts = colDefs->getColumn("typeInt");
        colNames = colDefs->getColumn("name");

        bool hasExtra = false;
        for (int i = 0; i < colDefs->columns(); ++i) {
            if (colDefs->getColumnName(i) == "extra") {
                hasExtra = true;
                break;
            }
        }
        if (hasExtra)
            exparams = colDefs->getColumn("extra");
        columnCategories_.resize(cols_);
        columnTypes_.resize(cols_);
        columnNames_.resize(cols_);
        for (int i = 0; i < cols_; ++i) {
            DATA_TYPE type = (DATA_TYPE)typeInts->getInt(i);
            if (hasExtra)
                columnTypes_[i] = converter::createType(type, exparams->getInt(i));
            else
                columnTypes_[i] = converter::createType(type, 0);
            columnCategories_[i] = Util::getCategory(type);
            columnNames_[i] = colNames->getString(i);
        }

        upsertScript_ = defineInsertScript(conn_, dbUrl, tableName, ignoreNull, pkeyColNames, psortColumns);
    } catch (exception&) {
        throw;
    } 
}

int AutoFitTableUpsert::upsert(TableSP table){
    if(cols_ != table->columns())
        throw RuntimeException("The input table columns doesn't match the columns of the target table.");
    
    vector<ConstantSP> columns;
    for(int i = 0; i < cols_; i++){
        VectorSP curCol = table->getColumn(i);
        checkColumnType(i, curCol->getCategory(), curCol->getType());
        if(columnCategories_[i] == TEMPORAL && curCol->getType() != (DATA_TYPE)columnTypes_[i].first){
            columns.push_back(curCol->castTemporal((DATA_TYPE)columnTypes_[i].first));
        }else{
            columns.push_back(curCol);
        }
    }
    TableSP tableInput = Util::createTable(columnNames_, columns);
    vector<ConstantSP> arg = {tableInput};
    ConstantSP res =  conn_.run(upsertScript_, arg);
    if(res->getType() == DT_INT && res->getForm() == DF_SCALAR)
        return res->getInt();
    else
        return 0;
}

vector<Type> AutoFitTableUpsert::getColTypes() {
    return columnTypes_;
}

vector<string> AutoFitTableUpsert::getColNames() {
    return columnNames_;
}

void AutoFitTableUpsert::checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type) {
    if((DATA_TYPE)columnTypes_[col].first != type){
        DATA_CATEGORY expectCategory = columnCategories_[col];
        if (category != expectCategory) {
            throw  RuntimeException("column [" + columnNames_[col] + "], expect type " + converter::getDataTypeString(columnTypes_[col]) + ", got type " + Util::getDataTypeString(type));
        }
    }
}



DLogger::Level DLogger::minLevel_ = DLogger::LevelDebug;
std::string DLogger::levelText_[] = { "Debug","Info","Warn","Error" };
//std::string DLogger::logFilePath_="/tmp/ddb_python_api.log";
std::string DLogger::logFilePath_;
void DLogger::SetMinLevel(Level level) {
	minLevel_ = level;
}

bool DLogger::WriteLog(std::string &text){
    puts(text.data());
    if(logFilePath_.empty()==false){
        text+="\n";
        Util::writeFile(logFilePath_.data(),text.data(),text.length());
    }
    return true;
}

bool DLogger::FormatFirst(std::string &text, Level level) {
	if (level < minLevel_) {
		return false;
	}
	std::chrono::system_clock::time_point now = std::chrono::system_clock::now();
	text = text + Util::toMicroTimestampStr(now, true) + ": [" +
		std::to_string(Util::getCurThreadId()) + "] " + levelText_[level] + ":";
	return true;
}

std::unordered_map<std::string, RecordTime::Node*> RecordTime::codeMap_;
Mutex RecordTime::mapMutex_;
long RecordTime::lastRecordOrder_ = 0;
RecordTime::RecordTime(const string &name) :
	name_(name) {
	startTime_ = Util::getNanoEpochTime();
	LockGuard<Mutex> LockGuard(&mapMutex_);
	lastRecordOrder_++;
	recordOrder_ = lastRecordOrder_;
	//std::cout<<Util::getEpochTime()<<" "<<name_<<recordOrder_<<" start..."<<std::endl;
}
RecordTime::~RecordTime() {
	long long diff = Util::getNanoEpochTime() - startTime_;
	LockGuard<Mutex> LockGuard(&mapMutex_);
	std::unordered_map<std::string, RecordTime::Node*>::iterator iter = codeMap_.find(name_);
	RecordTime::Node *pnode;
	if (iter != codeMap_.end()) {
		pnode = iter->second;
	}
	else {
		pnode = new Node();
		pnode->minOrder = recordOrder_;
		pnode->name = name_;
		codeMap_[name_] = pnode;
	}
	if (pnode->minOrder > recordOrder_) {
		pnode->minOrder = recordOrder_;
	}
	pnode->costTime.push_back(diff);
}
std::string RecordTime::printAllTime() {
	std::string output;
	LockGuard<Mutex> LockGuard(&mapMutex_);
	std::vector<RecordTime::Node*> nodes;
	nodes.reserve(codeMap_.size());
	for (std::unordered_map<std::string, RecordTime::Node*>::iterator iter = codeMap_.begin(); iter != codeMap_.end(); iter++) {
		nodes.push_back(iter->second);
	}
	std::sort(nodes.begin(), nodes.end(), [](RecordTime::Node *a, RecordTime::Node *b) {
		return a->minOrder < b->minOrder;
	});
	static double ns2s = 1000000.0;
	for (RecordTime::Node *node : nodes) {
		long sumNsOverflow = 0;
		long long sumNs = 0;//ns
		double maxNs = 0, minNs = 0;
		for (long long one : node->costTime) {
			sumNs += one;
			if (sumNs < 0) {
				sumNsOverflow++;
				sumNs = -(sumNs + LLONG_MAX);
			}
			if (maxNs < one) {
				maxNs = static_cast<double>(one);
			}
			if (minNs == 0 || minNs > one) {
				minNs = static_cast<double>(one);
			}
		}
        size_t timeCount = node->costTime.size();
		double sum = sumNsOverflow * (LLONG_MAX / ns2s) + sumNs / ns2s;
        double avg = sum / timeCount;
		double min = minNs / ns2s;
		double max = maxNs / ns2s;
        double stdDev = 0.0;
        if(timeCount>1){
            double diff;
            for (long long one : node->costTime) {
                diff = one/ ns2s - avg;
                stdDev += (diff * diff) / timeCount;
            }
            stdDev = sqrt(stdDev);
        }
		output = output + node->name + ": sum = " + std::to_string(sum) + " count = " + std::to_string(node->costTime.size()) +
			" avg = " + std::to_string(avg) + " stdDev = " + std::to_string(stdDev) +
			" min = " + std::to_string(min) + " max = " + std::to_string(max) + "\n";
		delete node;
	}
	codeMap_.clear();
	return output;
}

void ErrorCodeInfo::set(int apiCode, const string &info){
    set(formatApiCode(apiCode), info);
}

void ErrorCodeInfo::set(const string &code, const string &info) {
	errorCode = code;
	errorInfo = info;
}

void ErrorCodeInfo::set(const ErrorCodeInfo &src) {
	set(src.errorCode, src.errorInfo);
}

};    // namespace dolphindb

