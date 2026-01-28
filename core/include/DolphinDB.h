/*
 * DolphinDB.h
 *
 *  Created on: Sep 22, 2018
 *      Author: dzhou
 */

#ifndef DOLPHINDB_H_
#define DOLPHINDB_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <set>
#include <vector>
#include <deque>
#include <algorithm>
#include <memory>
#include <chrono>
#include <cstring>
#include <functional>

#include "Exports.h"
#include "Types.h"
#include "SmartPointer.h"
#include "Exceptions.h"
#include "SysIO.h"
#include "WideInteger.h"
#include "Constant.h"
#include "Dictionary.h"
#include "Table.h"
#include "pybind11/pybind11.h"

#include "TypeHelper.h"


namespace py = pybind11;



#ifdef _MSC_VER
	#if defined(_Py_static_string_init) && defined(PY_MINOR_VERSION) && (PY_MINOR_VERSION >= 10)
		#undef _Py_static_string_init
		#define _Py_static_string_init(value) { value, -1 }
	#elif defined(_Py_static_string_init)
		#undef _Py_static_string_init
		#define _Py_static_string_init(value) { NULL, value, NULL }
	#endif // _Py_static_string_init
#endif

using std::string;
using std::vector;

namespace dolphindb {

class DBConnectionImpl;
class BlockReader;
class Domain;
class DBConnectionPoolImpl;
class PartitionedTableAppender;
class DBConnection;
class DBConnectionPool;

typedef SmartPointer<BlockReader> BlockReaderSP;
typedef SmartPointer<Domain> DomainSP;
typedef SmartPointer<DBConnection> DBConnectionSP;
typedef SmartPointer<DBConnectionPool> DBConnectionPoolSP;
typedef SmartPointer<PartitionedTableAppender> PartitionedTableAppenderSP;

using converter::PyObjs;
using converter::Type;
using converter::TableChecker;
using converter::CHILD_VECTOR_OPTION;


class EXPORT_DECL DBConnection {
public:
	DBConnection(bool enableSSL = false, bool asyncTask = false, int keepAliveTime = 7200, bool compress = false, PARSER_TYPE parser = PARSER_TYPE::PARSER_DOLPHINDB, bool isReverseStreaming = false, int sqlStd = 0);
	virtual ~DBConnection();
	DBConnection(DBConnection&& oth);
	DBConnection& operator=(DBConnection&& oth);

	/**
	 * Connect to the specified DolphinDB server. If userId and password are specified, authentication
	 * will be performed along with connecting. If one would send userId and password in encrypted mode,
	 * please use the login function for authentication separately.
	 */
    bool connect(const string &hostName, int port, const string &userId = "", const string &password = "",
                 const string &initialScript = "", bool highAvailability = false,
                 const vector<string> &highAvailabilitySites = vector<string>(), int keepAliveTime = 7200,
                 bool reconnect = false, int tryReconnectNums = -1, int readTimeout = -1, int writeTimeout = -1,
                 bool usePublicName = false);

	/**
	 * Log onto the DolphinDB server using the given userId and password. If the parameter enableEncryption
	 * is true, the client obtains a public key from the server and then encrypts the userId and password
	 * using the public key. If the parameter enableEncryption is false, userId and password are communicated
	 * in plain text.
	 */
	void login(const string& userId, const string& password, bool enableEncryption);

	/**
	 * Run the script on the DolphinDB server and return the result to the client.If nothing returns,
	 * the function returns a void object. If error is raised on the server, the function throws an
	 * exception.
	 */
	ConstantSP run(const string& script, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);

	/**
	 * Run the given function on the DolphinDB server using the local objects as the arguments
	 * for the function and return the result to the client. If nothing returns, the function
	 * returns a void object. If error is raised on the server, the function throws an exception.
	 */
	ConstantSP run(const string& funcName, vector<ConstantSP>& args, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);

	/**
	 * upload a local object to the DolphinDB server and assign the given name in the session.
	 */
	ConstantSP upload(const string& name, const ConstantSP& obj);

	/**
	 * upload multiple local objects to the DolphinDB server and assign each object the given
	 * name in the session.
	 */
	ConstantSP upload(vector<string>& names, vector<ConstantSP>& objs);

	/**
	 * Close the current session and release all resources.
	 */
	void close();

	/**
	 * It is required to call initialize function below before one uses the DolphinDB API.
	 */
	static void initialize(){}

	void setInitScript(const string& script);

	const string& getInitScript() const;

	DataInputStreamSP getDataInputStream();

	py::object runPy(
		const string& script, int priority=4, int parallelism=64,
		int fetchSize=0, bool clearMemory=false,
		bool pickleTableToList=false, bool disableDecimal=false, bool withTableSchema = false);
	py::object runPy(
		const string& funcName, vector<ConstantSP>& args, int priority=4, int parallelism=64,
		int fetchSize=0, bool clearMemory=false,
		bool pickleTableToList=false, bool disableDecimal=false, bool withTableSchema = false);
	void setKeepAliveTime(int keepAliveTime);
	void setTimeout(int readTimeout, int writeTimeout);
	const string getSessionId() const;
	const string getHostName() const;
    int getPort() const;
    const string getUserId() const;
    const string getPassword() const;
    bool isClosed() const;
	void setProtocol(PROTOCOL protocol);
	void setShowOutput(bool flag);
    std::shared_ptr<Logger> getMsgLogger();

private:
    DBConnection(DBConnection& oth); // = delete
    DBConnection& operator=(DBConnection& oth); // = delete

private:
	enum ExceptionType {
		ET_IGNORE = 0,
		ET_UNKNOW = 1,
		ET_NEWLEADER = 2,
		ET_NODENOTAVAIL = 3,
	};
    void switchDataNode(const string &host = "", int port = -1, bool reconnect = false);
	bool connectNode(string hostName, int port, int keepAliveTime = -1);
    bool connected();
	//0 - ignored exception, eg : other data node not avail;
	//1 - throw exception;
	//2 - new leader, host&port is valid
	//3 - this data node not avail
	ExceptionType parseException(const TagResponse &e, std::string &host, int &port);

private:
	struct Node{
		string hostName;
		int port;
		double load;//DBL_MAX : unknow

		bool isEqual(const Node &node) {
			return hostName.compare(node.hostName) == 0 && port == node.port;
		}
		Node(){}
		Node(const string &hostName, int port, double load = DBL_MAX): hostName(hostName), port(port), load(load){}
		Node(const string &ipport, double loadValue = DBL_MAX);
    };
    static void parseIpPort(const string &ipport, string &ip, int &port);
    bool parseAsNotLeaderException(const string &msg, string &host, int &port, ExceptionType &type);
    bool parseAsOtherException(const string &msg, string &host, int &port, ExceptionType &type);

    std::unique_ptr<DBConnectionImpl> conn_;
    string uid_;
    string pwd_;
    string initialScript_;
    bool ha_;
	bool enableSSL_;
    bool asynTask_;
    bool compress_;
	vector<Node> nodes_;
	int lastConnNodeIndex_;
	PARSER_TYPE parser_;
	PROTOCOL protocol_;
	bool reconnect_, closed_;
	bool msg_;
	int tryReconnectNums_;
};

class EXPORT_DECL BlockReader : public Constant{
public:
    BlockReader(const DataInputStreamSP& in );
	~BlockReader() override;
    ConstantSP read();
    void skipAll();
    bool hasNext() const {return currentIndex_ < total_;}
    DATA_TYPE getType() const override {return DT_ANY;}
    DATA_TYPE getRawType() const override {return DT_ANY;}
    DATA_CATEGORY getCategory() const override {return MIXED;}
    ConstantSP getInstance() const override {return nullptr;}
    ConstantSP getValue() const override {return nullptr;}
private:
    DataInputStreamSP in_;
    int total_;
    int currentIndex_;
};


class EXPORT_DECL DBConnectionPool{
public:
    DBConnectionPool(const string& hostName, int port, int threadNum = 10, const string& userId = "", const string& password = "",
		bool loadBalance = false, bool highAvailability = false, bool compress = false, bool reConnect = false, PARSER_TYPE parser = PARSER_TYPE::PARSER_DOLPHINDB,
		PROTOCOL protocol = PROTOCOL_DDB, bool showOutput = true, int sqlStd = 0, int tryReconnectNums = -1, bool usePublicName = false);
	virtual ~DBConnectionPool();

	void run(const string& script, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);

	void run(const string& functionName, const vector<ConstantSP>& args, int identity, int priority=4, int parallelism=64, int fetchSize=0, bool clearMemory = false);

	bool isFinished(int identity);

    ConstantSP getData(int identity);

    void shutDown();

    bool isShutDown();

	int getConnectionCount();

	void runPy(
		const string& script, int identity,
		int priority=4, int parallelism=64,
		int fetchSize=0, bool clearMemory = false,
		bool pickleTableToList=false, bool disableDecimal=false);
	void runPy(
		const string& functionName, const vector<ConstantSP>& args,
		int identity, int priority=4, int parallelism=64,
		int fetchSize=0, bool clearMemory = false,
		bool pickleTableToList=false, bool disableDecimal=false);
	py::object getPyData(int identity);
	vector<string> getSessionId();
private:
	// SmartPointer<DBConnectionPoolImpl> pool_;
	std::shared_ptr<DBConnectionPoolImpl> pool_;
	friend class PartitionedTableAppender;

};

class EXPORT_DECL PartitionedTableAppender {
public:
	PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, DBConnectionPool& pool);

	PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, string appendFunction, DBConnectionPool& pool);
	virtual ~PartitionedTableAppender();
	int append(TableSP table);
	vector<Type> getColTypes();
	vector<string> getColNames();

private:
 	void init(string dbUrl, string tableName, string partitionColName, string appendFunction);

	void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type);

private:
	// SmartPointer<DBConnectionPoolImpl> pool_;
	std::shared_ptr<DBConnectionPoolImpl> pool_;
	string appendScript_;
	int threadCount_;
    DictionarySP tableInfo_;
	int partitionColumnIdx_;
	int cols_;
    DomainSP domain_;
    vector<DATA_CATEGORY> columnCategories_;
 	vector<Type> columnTypes_;
	vector<string> columnNames_;
	int identity_ = -1;
    vector<vector<int>> chunkIndices_;
};


class EXPORT_DECL AutoFitTableAppender {
public:
	AutoFitTableAppender(string dbUrl, string tableName, DBConnection& conn);
	virtual ~AutoFitTableAppender() = default;
	int append(TableSP table);
	vector<Type> getColTypes();
	vector<string> getColNames();

private:
	void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type);

private:
    DBConnection& conn_;
	string appendScript_;
	int cols_;
    vector<DATA_CATEGORY> columnCategories_;
 	vector<Type> columnTypes_;
	vector<string> columnNames_;
};

class EXPORT_DECL AutoFitTableUpsert {
public:
	AutoFitTableUpsert(string dbUrl, string tableName, DBConnection& conn,bool ignoreNull=false,
                                        vector<string> *pkeyColNames=nullptr,vector<string> *psortColumns=nullptr);
	virtual ~AutoFitTableUpsert() {
		try{
			conn_.run("undef('" + upsertScript_ + "', DEF)");
		}catch(...){}
	}
	int upsert(TableSP table);
	vector<Type> getColTypes();
	vector<string> getColNames();

private:
	void checkColumnType(int col, DATA_CATEGORY category, DATA_TYPE type);

private:
    DBConnection& conn_;
	string upsertScript_;
	int cols_;
    vector<DATA_CATEGORY> columnCategories_;
 	vector<Type> columnTypes_;
	vector<string> columnNames_;
};


class RecordTime {
public:
	RecordTime(const string &name);
	~RecordTime();
	static std::string printAllTime();
private:
	const string name_;
	long recordOrder_;
	long long startTime_;
	struct Node {
		string name;
		long minOrder;
		std::vector<long long> costTime;//ns
	};
	static long lastRecordOrder_;
	static Mutex mapMutex_;
	static std::unordered_map<std::string, RecordTime::Node*> codeMap_;
};

}




#endif /* DOLPHINDB_H_ */
