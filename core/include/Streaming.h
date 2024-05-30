#ifndef _STREAMING_H_
#define _STREAMING_H_
#include <functional>
#include <string>
#include <vector>
#include "Concurrent.h"
#include "TableImp.h"
#include "DolphinDB.h"
#include "Util.h"
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

#define DLOG true?DLogger::GetMinLevel() : DLogger::Info

class EXPORT_DECL Message : public ConstantSP {
public:
	Message() {
	}
	Message(const ConstantSP &sp) : ConstantSP(sp) {
	}
	Message(const ConstantSP &sp, const string &symbol) : ConstantSP(sp), symbol_(symbol) {
	}
	Message(const Message &msg) : ConstantSP(msg), symbol_(msg.symbol_) {
	}
	~Message() {
		clear();
	}
	Message& operator =(const Message& msg) {
		ConstantSP::operator=(msg);
		symbol_ = msg.symbol_;
		return *this;
	}
	const string& getSymbol() { return symbol_; }
private:
	string symbol_;
};


class MessageTableQueue {
public:
	MessageTableQueue(size_t maxItems, size_t batchSize_) 
		: capacity_(maxItems), batchSize_(batchSize_), size_(0), exitflag_(false) {}
	~MessageTableQueue(){}
	int size();
	void setExitFlag();
	bool getExitFlag();
	void push(const std::vector<string>& colLabels, const ConstantSP &colItems);
	bool pop(ConstantSP &item, int milliSeconds);
private:
	bool exitflag_;
	size_t capacity_;
	size_t batchSize_;
	size_t size_;
	size_t colSize_;
	TableSP messageTable_;
	Mutex lock_;
	ConditionalVariable full_;
    ConditionalVariable batch_;
};



bool EXPORT_DECL mergeTable(const Message &dest, const vector<Message> &src);

//using Message = ConstantSP;
using MessageQueue = BlockingQueue<Message>;
using MessageQueueSP = SmartPointer<MessageQueue>;
using MessageTableQueueSP = SmartPointer<MessageTableQueue>;
using MessageHandler = std::function<void(Message)>;
using MessageBatchHandler = std::function<void(vector<Message>)>;

#define DEFAULT_ACTION_NAME "cppStreamingAPI"
constexpr int DEFAULT_QUEUE_CAPACITY = 65536;

class StreamingClientImpl;
class EXPORT_DECL StreamDeserializer {
public:
	//symbol->[dbPath,tableName], dbPath can be empty for table in memory.
	StreamDeserializer(const unordered_map<string, pair<string, string>> &sym2tableName, DBConnection *pconn = nullptr);
	StreamDeserializer(const unordered_map<string, DictionarySP> &sym2schema);
	StreamDeserializer(const unordered_map<string, vector<DATA_TYPE>> &symbol2col);
	bool parseBlob(const ConstantSP &src, vector<VectorSP> &rows, vector<string> &symbols, ErrorCodeInfo &errorInfo);
private:
	void create(DBConnection &conn);
	void parseSchema(const unordered_map<string, DictionarySP> &sym2schema);
	unordered_map<string, pair<string, string>> sym2tableName_;
	unordered_map<string, vector<DATA_TYPE>> symbol2col_;
	Mutex mutex_;
	friend class StreamingClientImpl;
};
typedef SmartPointer<StreamDeserializer> StreamDeserializerSP;

struct SubscribeInfo {
	SubscribeInfo()
		: host("INVAILD"),
			port(-1),
			tableName("INVALID"),
			actionName("INVALID"),
			offset(-1),
			resub(false),
			filter(nullptr),
			msgAsTable(false),
			allowExists(false),
			haSites(0),
			queue(nullptr),
			tqueue(nullptr),
			userName(""),
			password(""),
			streamDeserializer(nullptr) {}
	explicit SubscribeInfo(const string &host, int port, const string &tableName, const string &actionName, long long offset, bool resub,
							const VectorSP &filter, bool msgAsTable, bool allowExists, int batchSize,
							const string &userName, const string &password, const StreamDeserializerSP &blobDeserializer, const bool istqueue)
		: host(move(host)),
			port(port),
			tableName(move(tableName)),
			actionName(move(actionName)),
			offset(offset),
			resub(resub),
			filter(filter),
			msgAsTable(msgAsTable),
			allowExists(allowExists),
			attributes(),
			haSites(0),
			queue(istqueue?nullptr:new MessageQueue(std::max(DEFAULT_QUEUE_CAPACITY, batchSize), batchSize)),
			tqueue(istqueue?new MessageTableQueue(std::max(DEFAULT_QUEUE_CAPACITY, batchSize), batchSize):nullptr),
			userName(move(userName)),
			password(move(password)),
			istqueue(istqueue),
			streamDeserializer(blobDeserializer){
	}

	string host;
	int port;
	string tableName;
	string actionName;
	long long offset;
	bool resub;
	VectorSP filter;
	bool msgAsTable;
	bool allowExists;
	vector<string> attributes;
	vector<pair<string, int>> haSites;
	MessageQueueSP queue;
	MessageTableQueueSP tqueue;
	bool istqueue;
	string userName, password;
	StreamDeserializerSP streamDeserializer;
	SocketSP socket;
	
	vector<ThreadSP> handleThread;
	void setExitFlag() {
		if (istqueue) {
			tqueue->setExitFlag();
		} else {
			queue->push(Message());
		}
	}
	void exit() {
		if (!socket.isNull()) {
			socket->close();
		}
		if(queue.isNull() && tqueue.isNull())
			return;
		if(istqueue) {
			tqueue->setExitFlag();
		}else{
			queue->push(Message());
		}
		for (auto &one : handleThread) {
			one->join();
		}
		handleThread.clear();
	}
};


class EXPORT_DECL StreamingClient {
public:
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
	explicit StreamingClient(int listeningPort);
    virtual ~StreamingClient();
	bool isExit();
	void exit();

protected:
    SubscribeInfo subscribeInternal(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME,
                                     int64_t offset = -1, bool resubscribe = true, const VectorSP &filter = nullptr,
                                     bool msgAsTable = false, bool allowExists = false, int batchSize  = 1,
									 string userName="", string password="",
									 const StreamDeserializerSP &blobDeserializer = nullptr, bool istqueue = false);
    void unsubscribeInternal(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);

protected:
    SmartPointer<StreamingClientImpl> impl_;
};

class EXPORT_DECL ThreadedClient : public StreamingClient {
public:
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
    explicit ThreadedClient(int listeningPort);
    ~ThreadedClient() override = default;
    ThreadSP subscribe(string host, int port, const MessageHandler &handler, string tableName,
                       string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true,
                       const VectorSP &filter = nullptr, bool msgAsTable = false, bool allowExists = false,
						string userName="", string password="",
					   const StreamDeserializerSP &blobDeserializer = nullptr);
    ThreadSP subscribe(string host, int port, const MessageBatchHandler &handler, string tableName,
                       string actionName = DEFAULT_ACTION_NAME, int64_t offset = -1, bool resub = true,
                       const VectorSP &filter = nullptr, bool allowExists = false, int batchSize = 1,
						double throttle = 1,bool msgAsTable = false,
						string userName = "", string password = "",
						const StreamDeserializerSP &blobDeserializer = nullptr);
	size_t getQueueDepth(const ThreadSP &thread);
    void unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
};

class EXPORT_DECL ThreadPooledClient : public StreamingClient {
public:
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
    explicit ThreadPooledClient(int listeningPort, int threadCount);
    ~ThreadPooledClient() override = default;
    vector<ThreadSP> subscribe(string host, int port, const MessageHandler &handler, string tableName,
                               string actionName, int64_t offset = -1, bool resub = true,
                               const VectorSP &filter = nullptr, bool msgAsTable = false, bool allowExists = false,
								string userName = "", string password = "",
							   const StreamDeserializerSP &blobDeserializer = nullptr);
    void unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
	size_t getQueueDepth(const ThreadSP &thread);

private:
    int threadCount_;
};

class EXPORT_DECL PollingClient : public StreamingClient {
public:
	//listeningPort > 0 : listen mode, wait for server connection
	//listeningPort = 0 : active mode, connect server by DBConnection socket
    explicit PollingClient(int listeningPort);
    ~PollingClient() override = default;
    MessageQueueSP subscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME,
                             int64_t offset = -1, bool resub = true, const VectorSP &filter = nullptr,
                             bool msgAsTable = false, bool allowExists = false,
							string userName="", string password="",
							 const StreamDeserializerSP &blobDeserializer = nullptr);
    void unsubscribe(string host, int port, string tableName, string actionName = DEFAULT_ACTION_NAME);
};

}  // namespace dolphindb
#endif  // _STREAMING_H_