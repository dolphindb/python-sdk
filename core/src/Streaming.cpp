#include "Streaming.h"
#include "Concurrent.h"
#include "ConstantMarshall.h"
#include "Exceptions.h"
#include "Types.h"
#include "Util.h"
#include "TableImp.h"
#include "ScalarImp.h"
#include "Logger.h"
#include "DolphinDB.h"
#include <list>
#include <map>
#ifndef WINDOWS
#include <arpa/inet.h>
#else
#include <ws2tcpip.h>
#endif

constexpr bool LOGGING_ENABLED_STREAMING = false;

#ifdef DLOG
    #undef DLOG
#endif
#define DLOG(...) \
    do { \
        if constexpr (LOGGING_ENABLED_STREAMING) { \
            dolphindb::DLogger::Debug(__VA_ARGS__); \
        } \
    } while(0)

#ifdef WINDOWS
namespace {
bool WSAStarted = false;

int startWSA() {
    WORD wVersionRequested;
    WSADATA wsaData;
    int err;
    wVersionRequested = MAKEWORD(2, 2);
    err = WSAStartup(wVersionRequested, &wsaData);
    if (err != 0) {
        LOG_ERR("WSAStartup failed with error:", err);
        return 1;
    }
    if (LOBYTE(wsaData.wVersion) != 2 || HIBYTE(wsaData.wVersion) != 2) {
        LOG_ERR("Could not find a usable version of Winsock.dll");
        WSACleanup();
        return 1;
    } else
        LOG_INFO("The Winsock 2.2 dll was found okay");
    return 0;
}
}  // namespace
#endif

using std::pair;
using std::set;
using std::unordered_map;

class Executor : public dolphindb::Runnable {
    using Func = std::function<void()>;

public:
    explicit Executor(Func f) : func_(std::move(f)){};
    void run() override { func_(); };

private:
    Func func_;
};

namespace dolphindb {

template <typename T>
ConstantSP arg(SmartPointer<T> v) {
	if (UNLIKELY(v.isNull())) {
		static ConstantSP void_ = Util::createConstant(DT_VOID);
		return void_;
	}
	return ConstantSP(v);
}

template <typename T>
ConstantSP arg(T v);

template <>
ConstantSP arg(bool v) {
	return Util::createBool((char)v);
}

template <>
ConstantSP arg(char v) {
	return Util::createChar(v);
}

template <>
ConstantSP arg(short v) {
	return Util::createShort(v);
}

template <>
ConstantSP arg(int v) {
	return Util::createInt(v);
}

template <>
ConstantSP arg(long long v) {
	return Util::createLong(v);
}

template <>
ConstantSP arg(float v) {
	return Util::createFloat(v);
}

template <>
ConstantSP arg(double v) {
	return Util::createDouble(v);
}

template <>
ConstantSP arg(string v) {
	return Util::createString(v);
}

template <>
ConstantSP arg(const char *c) {
	return arg(string(c));
}

template <typename T>
vector<ConstantSP> argVec(T &&v) {
	return{ arg(std::forward<T>(v)) };
}

template <typename T, typename... Args>
vector<ConstantSP> argVec(T &&first, Args &&... oth) {
	auto ret = argVec(std::forward<Args>(oth)...);
	ret.emplace_back(arg(std::forward<T>(first)));
	return ret;
}

template <typename... Args>
ConstantSP run(DBConnection &conn, const string &func, Args &&... args) {
    auto v = argVec(std::forward<Args>(args)...);
    std::reverse(v.begin(), v.end());
    return conn.run(func, v);
}

template <typename Key, typename T, typename Hash = std::hash<Key>>
class Hashmap {
public:
    Hashmap() = default;
	Hashmap(unordered_map<Key, T> &src) : mp_(src) {}
    size_t count(const Key &key) {
        LockGuard<Mutex> _(&mtx_);
        return mp_.count(key);
    }
    bool find(const Key &key, T &t) {
        LockGuard<Mutex> _(&mtx_);
        auto kv = mp_.find(key);
        if (kv == mp_.end()) {
            return false;
        } else {
            t = kv->second;
            return true;
        }
    }
    bool findWithExternalLock(const Key &key, T &t) {
        auto kv = mp_.find(key);
        if (kv == mp_.end()) {
            return false;
        } else {
            t = kv->second;
            return true;
        }
    }
    void op(std::function<void(unordered_map<Key, T, Hash> &mp)> func) {
        LockGuard<Mutex> _(&mtx_);
        func(mp_);
    }
    void upsert(const Key &key, std::function<void(T &v)> processor, const T &default_value) {
        LockGuard<Mutex> _(&mtx_);
        auto kv = mp_.find(key);
        if (kv != mp_.end()) {
            processor(mp_[key]);
        } else
            mp_[key] = default_value;
    }
    void insert(const Key &key, const T &val) {
        LockGuard<Mutex> _(&mtx_);
        mp_.insert({key, val});
    }
    void erase(const Key &key) {
        LockGuard<Mutex> _(&mtx_);
        mp_.erase(key);
    }
    void erase_if_eq(const Key &key, const T &val) {
        LockGuard<Mutex> _(&mtx_);
        auto kv = mp_.find(key);
        if (kv != mp_.end() && kv->second == val) {
            mp_.erase(key);
        }
    }
    void erase_if_eq(const Key &key, const T &val, std::function<void()> &&extra) {
        LockGuard<Mutex> _(&mtx_);
        auto kv = mp_.find(key);
        if (kv != mp_.end() && kv->second == val) {
            mp_.erase(key);
            extra();
        }
    }
    vector<pair<Key, T>> getElements() {
        LockGuard<Mutex> _(&mtx_);
        vector<pair<Key, T>> ret;
        for (auto kv = mp_.begin(); kv != mp_.end(); ++kv) {
            ret.emplace_back(*kv);
        }
        return ret;
    }
    T &operator[](const Key &key) {
        LockGuard<Mutex> _(&mtx_);
        return mp_[key];
    }
    Mutex *getLock() { return &mtx_; }
    int size() { return mp_.size(); }

private:
    unordered_map<Key, T, Hash> mp_;
    Mutex mtx_;
};

ConstantSP convertTupleToTable(const vector<string>& colLabels, const ConstantSP& msg) {
    int colCount = static_cast<int>(colLabels.size());
    vector<ConstantSP> cols(colCount);
    for(int i=0; i<colCount; ++i){
        cols[i] = msg->get(i);
        cols[i]->setTemporary(true);
    }
    return new BasicTable(cols, colLabels);
}

bool mergeTable(const Message &dest, const vector<Message> &src) {
	Table *table = (Table*)dest.get();
	INDEX colSize = table->columns();
	for(auto &one : src) {
		if (one.isNull())
			return false;
		Table *src = (Table*)one.get();
		for (INDEX colIndex = 0; colIndex < colSize; colIndex++) {
			((Vector*)table->getColumn(colIndex).get())->append(src->getColumn(colIndex));
		}
	}
	auto basicTable = dynamic_cast<BasicTable*>(table);
	if(basicTable != nullptr){
		basicTable->updateSize();
	}
	return true;
}

}  // namespace dolphindb

namespace dolphindb {

StreamDeserializer::StreamDeserializer(const unordered_map<string, pair<string, string>> &sym2tableName, DBConnection *pconn)
					: sym2tableName_(sym2tableName) {
	if (pconn != NULL) {
		create(*pconn);
	}
}

StreamDeserializer::StreamDeserializer(const unordered_map<string, DictionarySP> &sym2schema) {
	parseSchema(sym2schema);
}

StreamDeserializer::StreamDeserializer(const unordered_map<string, vector<DATA_TYPE>> &symbol2col)
					:symbol2col_(symbol2col){

}

void StreamDeserializer::create(DBConnection &conn) {
	if (symbol2col_.size() > 0 || sym2tableName_.empty())
		return;
	unordered_map<string, DictionarySP> sym2schema;
	DictionarySP schema;
	for (auto &one : sym2tableName_) {
		if (one.second.first.empty()) {
            if (one.second.second.find(".orca_table.") != std::string::npos) {
                schema = conn.run("useOrcaStreamTable(\"" + one.second.second + "\", schema)");
            } else {
                schema = conn.run("schema(" + one.second.second + ")");
            }
		}
		else {
			schema = conn.run(std::string("schema(loadTable(\"") + one.second.first + "\",\"" + one.second.second + "\"))");
		}
		sym2schema[one.first] = schema;
	}
	parseSchema(sym2schema);
}
bool StreamDeserializer::parseBlob(const ConstantSP &src, vector<VectorSP> &rows, vector<string> &symbols, ErrorCodeInfo &errorInfo) {
	const VectorSP &symbolVec = src->get(1);
	const VectorSP &blobVec = src->get(2);
	INDEX rowSize = symbolVec->rows();
	rows.resize(rowSize);
	symbols.resize(rowSize);
	unordered_map<string, vector<DATA_TYPE>>::iterator colTypeIter;
    unordered_map<string, vector<int>>::iterator colScaleIter;
	for (INDEX rowIndex = 0; rowIndex < rowSize; rowIndex++) {
		string symbol = symbolVec->getString(rowIndex);
		{
			LockGuard<Mutex> lock(&mutex_);
            colTypeIter = symbol2col_.find(symbol);
            colScaleIter = symbol2scale_.find(symbol);
			if (colTypeIter == symbol2col_.end()) {
				errorInfo.set(ErrorCodeInfo::EC_InvalidParameter, string("Unknown symbol ") + symbol);
				return false;
			}
		}
		symbols[rowIndex] = std::move(symbol);

		vector<DATA_TYPE> &cols = colTypeIter->second;
        vector<int> scales;
        if (colScaleIter != symbol2scale_.end()) {
            scales = colScaleIter->second;
        }

		const string &blob = blobVec->getStringRef(rowIndex);
		DataInputStreamSP dis = new DataInputStream(blob.data(), blob.size(), false);
		INDEX num;
		IO_ERR ioError;
		ConstantSP value;
		int colIndex = 0;
		VectorSP rowVec = Util::createVector(DT_ANY, static_cast<INDEX>(cols.size()));
        for (auto i = 0; i < (int)cols.size(); i++) {
            auto &colOne = cols[i];
			num = 0;

            // scale for decimal scalar and decimal array
            auto scale = 0;
            if (Util::getCategory(colOne) == DENARY || colOne == DT_DECIMAL32_ARRAY || colOne == DT_DECIMAL64_ARRAY || colOne == DT_DECIMAL128_ARRAY) {
                if (scales.empty()) {
                    errorInfo.set(ErrorCodeInfo::EC_InvalidParameter, string("Unknown scale for decimal. StreamDeserializer should be initialized with sym2schema"));
                    return false;
                }
                scale = scales[i];
            }

			if (colOne < ARRAY_TYPE_BASE) {
				value = Util::createConstant(colOne, scale);
				ioError = value->deserialize(dis.get(), 0, 1, num);
				if (ioError != OK) {
					errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Deserialize blob error " + std::to_string(ioError));
					return false;
				}
				rowVec->set(colIndex, value);
			}
			else {
                value = Util::createArrayVector(colOne, 1, 1, true, scale);
				ioError = value->deserialize(dis.get(), 0, 1, num);
				if (ioError != OK) {
					errorInfo.set(ErrorCodeInfo::EC_InvalidObject, "Deserialize blob error " + std::to_string(ioError));
					return false;
				}
				rowVec->set(colIndex, value);
			}
			colIndex++;
		}
		rows[rowIndex] = rowVec;
	}
	return true;
}
void StreamDeserializer::parseSchema(const unordered_map<string, DictionarySP> &sym2schema) {

    LockGuard<Mutex> lock(&mutex_);

	for (auto &one : sym2schema) {
		const DictionarySP &schema = one.second;
		TableSP colDefs = schema->getMember("colDefs");
		size_t columnSize = colDefs->size();

        // types
        ConstantSP colDefsTypeInt = colDefs->getColumn("typeInt");
        vector<DATA_TYPE> colTypes(columnSize);
        for (auto i = 0; i < (int)columnSize; i++) {
            colTypes[i] = (DATA_TYPE)colDefsTypeInt->getInt(i);
        }
        symbol2col_[one.first] = colTypes;

        // scales for decimals (server 130 doesn't have this column)
        if (colDefs->contain("extra")) {
            ConstantSP colDefsScales = colDefs->getColumn("extra");
            vector<int> colScales(columnSize);
            for (auto i = 0; i < (int)columnSize; i++) {
                colScales[i] = colDefsScales->getInt(i);
            }
            symbol2scale_[one.first] = colScales;
        }
	}
}

class StreamingClientImpl {
    struct HAStreamTableInfo{
        std::string      followIp;
        int              followPort;
        std::string      tableName;
        std::string      action;
        std::string      leaderIp;
        int              leaderPort;
    };
	class ActivePublisher {
	public:
		ActivePublisher(std::shared_ptr<DBConnection> conn) : conn_(conn) {}
		~ActivePublisher() {}
		DataInputStreamSP getDataInputStream(){ return conn_->getDataInputStream();}
	private:
		enum SubscriberRPCType { RPC_OK, RPC_START, RPC_END };
		enum SubscriberFromType { FROM_DDB, FROM_API };
		std::shared_ptr<DBConnection> conn_;
	};
	typedef SmartPointer<ActivePublisher> ActivePublisherSP;
	struct SocketThread {
		SocketThread() {}
		SocketThread(const SocketThread &src) {
			thread = src.thread;
			socket = src.socket;
			publisher = src.publisher;
		}
		SocketThread(const SocketSP &socket1, const ThreadSP &thread1,const ActivePublisherSP &publisher1) {
			thread = thread1;
			socket = socket1;
			publisher = publisher1;
		}
		SocketThread& operator =(const SocketThread& src) {
			thread = src.thread;
			socket = src.socket;
			publisher = src.publisher;
			return *this;
		}
		void stop() {
			if (socket.isNull() == false) {
				socket->close();
			}
		}
		ThreadSP thread;
		SocketSP socket;
		ActivePublisherSP publisher;
	};

    struct KeepAliveAttr {
        int enabled = 1;             // default = 1 enabled
        int idleTime = 30;           // default = 30s idle time will trigger detection
        int interval = 5;            // default = 5s/detection
        int count = 3;               // default = 3 unsuccessful detections mean disconnected
    };

public:
    explicit StreamingClientImpl(int listeningPort) : listeningPort_(listeningPort), publishers_(5), isInitialized_(false){
		if (listeningPort_ < 0) {
			throw RuntimeException("Invalid listening port value " + std::to_string(listeningPort));
		}
#ifdef WINDOWS
        if (!WSAStarted && startWSA()) {
            throw RuntimeException("Can't start WSA");
        }
        WSAStarted = true;
#endif
    }
    ~StreamingClientImpl() {
		exit();
    }
	void exit() {
		if (exit_)
			return;
		DLOG("exit start.");
		exit_ = true;
		if(!isListenMode()){
			publishers_.push(ActivePublisherSP());
		}
		if(listenerSocket_.isNull()==false)
			listenerSocket_->close();
		if(reconnectThread_.isNull()==false)
			reconnectThread_->join();
		if(daemonThread_.isNull()==false)
			daemonThread_->join();
		{
			SocketThread socketthread;
			while (parseSocketThread_.pop(socketthread)) {
				socketthread.stop();
				socketthread.thread->join();
			}
            while (gcSocketThread_.pop(socketthread)) {
                socketthread.stop();
                socketthread.thread->join();
            }
		}
		topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp) {
			for (auto &one : mp) {
				one.second.exit();
			}
		});

		DLOG("exit done.");
	}
	inline bool isExit() {
		return exit_;
	}
	SubscribeInfo subscribeInternal(const string &host, int port, const string &tableName,
                                    const string &actionName = DEFAULT_ACTION_NAME, int64_t offset = -1,
                                    bool resubscribe = true, const VectorSP &filter = nullptr, bool msgAsTable = false,
                                    bool allowExists = false, int batchSize  = 1,
									const string &userName="", const string &password="",
									const StreamDeserializerSP &sdsp = nullptr, bool istqueue = false,
									const std::vector<std::string> &backupSites = std::vector<std::string>(),
									bool isEvent = false,
									int resubTimeout = 100, bool subOnce = false);
    string subscribeInternal(DBConnection &conn, SubscribeInfo &info);
    void insertMeta(SubscribeInfo &info, const string &topic);
    bool delMeta(const string &topic, bool exitFlag);
    void unsubscribeInternal(const string &host, int port, const string &tableName,
                             const string &actionName = DEFAULT_ACTION_NAME);
	void addHandleThread(const MessageQueueSP& queue, const ThreadSP &thread) {
		topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp) {
			for (auto &one : mp) {
				if (one.second.queue == queue) {
					one.second.handleThread.push_back(thread);
					return;
				}
			}
			LOG_ERR("can't find message queue in exist topic.");
		});
	}
	void addHandleThread(const MessageTableQueueSP& tqueue, const ThreadSP &thread) {
		topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp) {
			for (auto &one : mp) {
				if (one.second.tqueue == tqueue) {
					one.second.handleThread.push_back(thread);
					return;
				}
			}
			LOG_ERR("can't find message queue in exist topic.");
		});
	}
	std::size_t getQueueDepth(const ThreadSP &thread) {
		MessageQueueSP queue;
		MessageTableQueueSP tqueue;
		findMessageQueue(thread, queue, tqueue);
		if (queue.isNull() && tqueue.isNull()) {
			return 0;
		}
		if (queue.isNull()) return tqueue->size();
		else return queue->size();
	}
	void findMessageQueue(const ThreadSP &thread, MessageQueueSP &queue, MessageTableQueueSP &tqueue) {
		topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp) {
			for (auto &one : mp) {
				for(auto &handle : one.second.handleThread) {
					if (handle == thread) {
						if(one.second.istqueue){
							tqueue = one.second.tqueue;
						}else{
							queue = one.second.queue;
						}
						return;
					}
				}
			}
		});
	}
private:
    //if server support reverse connect, set the port to 0
    //if server Not support reverse connect and the port is 0, throw a RuntimeException
    void checkServerVersion(std::string host, int port, const std::vector<std::string>& backupSites);
    void init();
	bool isListenMode() {
		return listeningPort_ > 0;
	}
    void parseMessage(DataInputStreamSP in, ActivePublisherSP publisher);
	void sendPublishRequest(DBConnection &conn, SubscribeInfo &info);
    void reconnect();
	static bool initSocket(const SocketSP &socket) {
		if (socket.isNull())
			return false;
		static KeepAliveAttr keepAliveAttr;
#ifdef WINDOWS
		if (::setsockopt(socket->getHandle(), SOL_SOCKET, SO_KEEPALIVE, (const char*)&keepAliveAttr.enabled, sizeof(int)) != 0) {
			LOG_ERR("Subscription socket failed to enable TCP_KEEPALIVE with error:", errno);
			return false;
		}
#elif defined MAC
        if(::setsockopt(socket->getHandle(), SOL_SOCKET, SO_KEEPALIVE, (const char*)&keepAliveAttr.enabled, sizeof(int)) != 0) {
            LOG_ERR("Subscription socket failed to enable TCP_KEEPALIVE with error:", errno);
            return false;
        }
#else
		if (::setsockopt(socket->getHandle(), SOL_SOCKET, SO_KEEPALIVE, &keepAliveAttr.enabled, sizeof(int)) != 0) {
			LOG_ERR("Subscription socket failed to enable TCP_KEEPALIVE with error:", errno);
		}
		::setsockopt(socket->getHandle(), SOL_TCP, TCP_KEEPIDLE, &keepAliveAttr.idleTime, sizeof(int));
		::setsockopt(socket->getHandle(), SOL_TCP, TCP_KEEPINTVL, &keepAliveAttr.interval, sizeof(int));
		::setsockopt(socket->getHandle(), SOL_TCP, TCP_KEEPCNT, &keepAliveAttr.count, sizeof(int));
#endif
		return true;
	}

private:
    void daemon() {
		DLOG("daemon start.");
		DataInputStreamSP inputStream;
		ActivePublisherSP publisher;
		//IO_ERR ret;
        while (isExit() == false) {
            try {
				if (isListenMode()) {
					SocketSP socket = listenerSocket_->accept();
					if (!initSocket(socket))
						break;
					inputStream = new DataInputStream(socket);
				}
				else {
					publishers_.pop(publisher);
					if (publisher.isNull())
						break;
					inputStream = publisher->getDataInputStream();
				}
                if (inputStream->getSocket().isNull()) {
                    break;
                };

                ThreadSP t = new Thread(new Executor(std::bind(&StreamingClientImpl::parseMessage, this, inputStream, publisher)));
                t->start();
				parseSocketThread_.push(SocketThread(inputStream->getSocket(),t,publisher));
            } catch (exception &e) {
                LOG_ERR("Daemon exception:", e.what());
                LOG_ERR("Restart Daemon in 1 second");
                Util::sleep(1000);
            } catch (...) {
                LOG_ERR("Daemon unknown exception: ");
                LOG_ERR("Restart Daemon in 1 second");
                Util::sleep(1000);
            }
        }
		DLOG("daemon exit.");
    }

    inline string stripActionName(string topic) { return topic.substr(0, topic.find_last_of('/')); }
    inline string getSite(string topic) { return topic.substr(0, topic.find_first_of('/')); }
    DBConnection buildConn(string host, int port) {
        DBConnection conn;
        if (!conn.connect(host, port)) {
            throw RuntimeException("Failed to connect to server: " + host + " " + std::to_string(port));
        }

        return conn;
    }

    string getLocalIP() {
        if (localIP_.empty()) localIP_ = "localhost";
        return localIP_;
    }

    void parseIpPort(const string &ipport, string &ip, int &port) {
        auto v = Util::split(ipport, ':');
        if (v.size() < 2) {
            throw RuntimeException("Incorrect input " + ipport + " for backupSite. The correct format is host:port, e.g. 192.168.1.1:8848");
        }
        ip = v[0];
        port = std::stoi(v[1]);
        if (port <= 0 || port > 65535) {
            throw RuntimeException("Incorrect input " + ipport + " for backupSite. The port number must be a positive integer no greater than 65535");
        }
    }

    bool getNewLeader(const string& s, string &host, int &port) {
        string msg{s};
		size_t index = msg.find("<NotLeader>");
		if (index != string::npos) {
			index = msg.find(">");
			string ipport = msg.substr(index + 1);

			auto v = Util::split(ipport, ':');
			if (v.size() < 2) {
				return false;
			}
			host = v[0];
			port = std::stoi(v[1]);
			if (port <= 0 || port > 65535) {
				return false;
			}
			return true;
		}
		return false;
    }

private:
    SocketSP listenerSocket_;
    ThreadSP daemonThread_;
    ThreadSP reconnectThread_;
    SynchronizedQueue<SocketThread> parseSocketThread_;
    SynchronizedQueue<SocketThread> gcSocketThread_;
	int listeningPort_;
    string localIP_;
    Hashmap<string, SubscribeInfo> topicSubInfos_;
    Hashmap<string, int> actionCntOnTable_;
    Hashmap<string, set<string>> liveSubsOnSite_;  // living site -> topic
    Hashmap<string, pair<long long, long long>> topicReconn_;
	BlockingQueue<ActivePublisherSP> publishers_;
    Mutex mtx_;
    std::queue<SubscribeInfo> initResub_;
	bool exit_;
	Mutex readyMutex_;
    std::list<HAStreamTableInfo> haStreamTableInfo_;
    bool isInitialized_;
    std::map<std::string, std::string> topics_;     //ID -> current topic
#ifdef WINDOWS
    static bool WSAStarted_;
    static void WSAStart();
#endif
};


void StreamingClientImpl::init(){
    if(isInitialized_){
        return;
    }
    isInitialized_ = true;
    if (isListenMode()) {
        listenerSocket_ = new Socket("", listeningPort_, true, 30);
        if (listenerSocket_->bind() != OK) {
            throw RuntimeException("Failed to bind the socket on port " + Util::convert(listeningPort_) +
                ". Couldn't start the subscription daemon.");
        }
        if (listenerSocket_->listen() != OK) {
            throw RuntimeException("Failed to listen the socket on port " + Util::convert(listeningPort_) +
                ". Couldn't start the subscription daemon.");
        }
    }
    exit_ = false;
    reconnectThread_ = new Thread(new Executor(std::bind(&StreamingClientImpl::reconnect, this)));
    reconnectThread_->start();
    daemonThread_ = new Thread(new Executor(std::bind(&StreamingClientImpl::daemon, this)));
    daemonThread_->start();
}

void StreamingClientImpl::checkServerVersion(std::string host, int port, const std::vector<std::string>& backupSites){
    DBConnection conn;
    unsigned index = 0;
    while(true){
        try{
            conn = buildConn(host, port);
            break;
        }
        catch(const std::exception& e){
            if(index == backupSites.size()){
                throw RuntimeException("Failed to connect to primary and all backup nodes.");
            }
            parseIpPort(backupSites[index], host, port);
            index++;
        }
    }

    vector<ConstantSP> args;
    auto versionStr = conn.run("version", args)->getString();
    auto _ = Util::split(Util::split(versionStr, ' ')[0], '.');
    auto v0 = std::stoi(_[0]);
    auto v1 = std::stoi(_[1]);
    auto v2 = std::stoi(_[2]);

    if (v0 == 3 || (v0 == 2 && v1 == 0 && v2 >= 9) || (v0 == 2 && v1 == 10)) {
        //server only support reverse connection
        if(listeningPort_ != 0){
            LOG_WARN("The server only supports transferring subscribed data using the connection initiated by the subsriber. The specified port will not take effect.");
        }
        listeningPort_ = 0;
    } else {
        //server Not support reverse connection
        if(listeningPort_ == 0){
            throw RuntimeException("The server does not support transferring subscribed data using the connection initiated by the subsriber. Specify a valid port parameter.");
        }
    }
}

void StreamingClientImpl::reconnect() {
    DLOG("reconnect start.");

    while (!isExit()) {
        if (isExit()) {
            DLOG("reconnect exit by flag.");
            return;
        }

        topicReconn_.op([&](unordered_map<string, pair<long long, long long>> &mp) {
            for (auto &p : mp) {
                SubscribeInfo info;
                if (!topicSubInfos_.find(p.first, info)) continue;
                if (!info.resub) continue;
                string topic = p.first;
                string host = info.host;
                int port = info.port;
                string newTopic = topic;
                bool isReconnected = false;

                // reconn every info.resubTimeout ms
                if (Util::getEpochTime() - p.second.first <= info.resubTimeout) continue;

                if(info.availableSites.empty()){
                    for (int i = 0; i < 3; ++i) {
                        if (isExit()) {
                            DLOG("reconnect exit by flag.");
                            return;
                        }
                        DLOG("reconnect", host,"for", topic);
                        try {
                            auto conn = buildConn(host, port);
                            LockGuard<Mutex> lock(&readyMutex_);
                            newTopic = subscribeInternal(conn, info);
                            if (newTopic != topic) {
                                delMeta(topic, false);
                                insertMeta(info, newTopic);
                            }
                            break;
                        } catch (exception &e) {
                            string msg = e.what();
                            if (getNewLeader(e.what(), host, port)) {
                                LOG_ERR("In reconnect: Got NotLeaderException, switch to leader node [", host, ":", port, "] for subscription");
                                HAStreamTableInfo haInfo{info.host, info.port, info.tableName, info.actionName, host, port};
                                haStreamTableInfo_.push_back(haInfo);
                                info.host = host;
                                info.port = port;
                            } else {
                                std::string errMsg = "#attempt=" + std::to_string(p.second.second++) + ", failed to resubscribe, exception: " + e.what();
                                if (!info.haSites.empty()) {
                                    int k = rand() % info.haSites.size();
                                    host = info.haSites[k].first;
                                    port = info.haSites[k].second;
                                    errMsg += ", will retry site: " + host + ":" + std::to_string(port);
                                }
                                LOG_ERR(errMsg);
                            }
                        }
                    }
                }
                else{
                    // if current node needs to reconnect for the first time, it will be recorded
                    if (info.lastSiteIndex == -1) {
                        info.lastSiteIndex = info.currentSiteIndex;
                    }

                    // try every site twice
                    int currentSiteIndex = info.currentSiteIndex;
                    for (unsigned i = 0; i < info.availableSites.size() && !isReconnected; ++i) {
                        // info.availableSites.size is not empty
                        // init new currentSite
                        info.currentSiteIndex = currentSiteIndex;
                        info.host = info.availableSites[info.currentSiteIndex].first;
                        info.port = info.availableSites[info.currentSiteIndex].second;
                        host = info.host;
                        port = info.port;
                        topicSubInfos_.upsert(p.first, [&](SubscribeInfo &_info) { _info = info; }, info);

                        // currentSite will be reconnected twice
                        for (int j = 0; j < 2 && !isReconnected; ++ j) {
                            if (isExit()) {
                                DLOG("reconnect exit by flag.");
                                return;
                            }
                            DLOG("reconnect", host,"for", topic);
                            try {
                                auto conn = buildConn(host, port);
                                LockGuard<Mutex> lock(&readyMutex_);
                                newTopic = subscribeInternal(conn, info);
                                if (newTopic != topic) {
                                    delMeta(topic, false);
                                    insertMeta(info, newTopic);
                                }

                                // set status flag
                                isReconnected = true;
                                // update info data
                                info.updateByReconnect(currentSiteIndex, topic);
                                topicSubInfos_.upsert(newTopic, [&](SubscribeInfo &_info) { _info = info; }, info);
                                break;
                            } catch (exception &e) {
                                string msg = e.what();
                                if(!info.availableSites.empty()){
                                    LOG_ERR("Failed to resubscribe after disconnection. #attempt=", p.second.second++, ". Exception:", e.what());
                                }
                                else if (getNewLeader(e.what(), host, port)) {
                                    LOG_ERR("In reconnect: Got NotLeaderException, switch to leader node [", host, ":", port, "] for subscription");
                                    HAStreamTableInfo haInfo{info.host, info.port, info.tableName, info.actionName, host, port};
                                    haStreamTableInfo_.push_back(haInfo);
                                    info.host = host;
                                    info.port = port;
                                } else {
                                    std::string errMsg = "#attempt=" + std::to_string(p.second.second++) + ", failed to resubscribe, exception: " + e.what();
                                    if (!info.haSites.empty()) {
                                        int k = rand() % info.haSites.size();
                                        host = info.haSites[k].first;
                                        port = info.haSites[k].second;
                                        errMsg += ", will retry site: " + host + ":" + std::to_string(port);
                                    }
                                    LOG_ERR(errMsg);
                                }
                            }
                        }
                        currentSiteIndex = (currentSiteIndex + 1) % info.availableSites.size();
                    }

                    // clear currentSite
                    if (!isReconnected) {
                        info.currentSiteIndex = 0;
                    }
                }
                p.second.first = Util::getEpochTime();
            }
        });
        if (isExit()) {
            DLOG("reconnect exit by flag.");
            return;
        }

        // if init unsuccessfully
        {
            LockGuard<Mutex> _(&mtx_);
            vector<SubscribeInfo> v;
            while (!initResub_.empty() && isExit()==false) {
                auto info = initResub_.front();
                string _host;
                int _port = 0;
                initResub_.pop();
                try {
                    if(!info.availableSites.empty()){
                        info.currentSiteIndex = (info.currentSiteIndex + 1) % info.availableSites.size();
                        info.host = info.availableSites[info.currentSiteIndex].first;
                        info.port = info.availableSites[info.currentSiteIndex].second;
                    }
                    DBConnection conn = buildConn(info.host, info.port);
                    LockGuard<Mutex> lock(&readyMutex_);
                    auto topic = subscribeInternal(conn, info);
                    insertMeta(info, topic);
                } catch (exception &e) {
                    if (!info.availableSites.empty()){
                        LOG_ERR("Failed to resubscribe with exception:", e.what());
                    }
                    else if (getNewLeader(e.what(), _host, _port)) {
                        LOG_ERR("when handle initResub_, Got NotLeaderException, switch to leader node [", _host, ":", _port, "] for subscription");
                        HAStreamTableInfo haInfo{info.host, info.port, info.tableName, info.actionName, _host, _port};
                        haStreamTableInfo_.push_back(haInfo);
                        info.host = _host;
                        info.port = _port;
                    }else {
                        LOG_ERR("Failed to resubscribe with exception:", e.what());
                    }
                    v.emplace_back(info);
                }
            }
            if (isExit()) {
                DLOG("reconnect exit by flag.");
                return;
            }

            for (auto &i : v) {
                initResub_.push(i);
            }
        }

        // check reconnected interval time
        Util::sleep(10);
    }
    DLOG("reconnect exit.");
}

void StreamingClientImpl::parseMessage(DataInputStreamSP in, ActivePublisherSP  /*publisher*/) {
	DLOG("parseMessage start.");
    auto factory = ConstantUnmarshallFactory(in);
    ConstantUnmarshall *unmarshall = nullptr;

    IO_ERR ret = OK;
    long long sentTime;
    long long offset = -1;
    short previousDataFormFlag = 0x7fff;
    // Have no idea which topic it is parsing until one message came in
    string aliasTableName;
    string topicMsg;
    vector<string> topics;
	vector<string> symbols;

    while (isExit() == false) {
        if (ret != OK) {  // blocking mode, ret won't be NODATA
			DLOG("parseMessage exit with error",ret);
            if (!actionCntOnTable_.count(aliasTableName) || actionCntOnTable_[aliasTableName] == 0) {
                break;
            };
            if (topicMsg.empty()) {
                LOG_WARN("WARNING: ERROR occured before receiving first message, can't do recovery.");
                break;
            }
            // close this socket, and do resub
            in->close();
            in->getSocket().clear();
			if (topics.empty())
				break;
			auto site = getSite(topics[0]);
            set<string> ts;
            if (liveSubsOnSite_.find(site, ts)) {
                for (auto &t : ts) {
                    topicReconn_.insert(t, {Util::getEpochTime(), 0});
                }
            }
            break;
        }

        char littleEndian;
        ret = in->readChar(littleEndian);
        if (ret != OK) continue;

        ret = in->bufferBytes(16);
        if (ret != OK) continue;

		ret = in->readLong(sentTime);
		if (ret != OK) continue;
		ret = in->readLong(offset);
		if (ret != OK) continue;

        ret = in->readString(topicMsg);
		if (ret != OK) continue;
        topics = Util::split(topicMsg, ',');
		if (topics.empty()) {
			break;
		}
        aliasTableName = stripActionName(topics[0]);

        short flag;
        ret = in->readShort(flag);
        if (ret != OK) continue;

        if (UNLIKELY(flag != previousDataFormFlag)) {
            auto form = static_cast<DATA_FORM>((unsigned short)flag >> 8u);
            unmarshall = factory.getConstantUnmarshall(form);
            if (UNLIKELY(unmarshall == nullptr)) {
                std::ostringstream errMsg;
                errMsg << "[ERROR] Invalid data from: 0x" << std::hex << flag << ", unable to continue. Will stop this parseMessage thread.";
                LOG_ERR(errMsg.str());
                ret = OTHERERR;
                continue;
            }
            previousDataFormFlag = flag;
        }

        try {
            unmarshall->start(flag, true, ret);
        }
        catch (const std::exception &e) {
            LOG_ERR("[ERROR] Exception during parseMessage: ", e.what(), ", stopping this parse thread.");
            ret = OTHERERR;
            continue;
        }
        if (ret != OK) continue;

        ConstantSP obj = unmarshall->getConstant();
        if (obj->isTable()) {
            if (obj->rows() != 0) {
                LOG_ERR("[ERROR] schema table shuold have zero rows, stopping this parse thread.");
                return;
            }
            for (auto &t : topics) {
                topicReconn_.erase(t);
            }
        } else if (LIKELY(obj->isVector())) {
			int colSize = obj->size();
            int rowSize = obj->get(0)->size();
//            offset += rowSize;
			if (isListenMode() && rowSize == 1) {
                // 1d array to 2d
                VectorSP newObj = Util::createVector(DT_ANY, colSize);
                for (int i = 0; i < colSize; ++i) {
                    ConstantSP val = obj->get(i);
                    VectorSP col;
					if(val->getForm() == DF_VECTOR) {
						col = Util::createArrayVector((DATA_TYPE)((int)val->getType()+ARRAY_TYPE_BASE), 1);
					}else{
						col = Util::createVector(val->getType(), 1);
					}
                    col->set(0, val);
                    newObj->set(i, col);
                }
                obj = newObj;
            }
            vector<VectorSP> cache, rows;
			ErrorCodeInfo errorInfo;

            //wait for insertMeta() finish, or there is no topic in topicSubInfos_
            LockGuard<Mutex> lock(&readyMutex_);

            for (auto &t : topics) {
                SubscribeInfo info;
                if (topicSubInfos_.find(t, info)) {
                    if (info.queue.isNull() && info.tqueue.isNull()) continue;
					if (info.isEvent_) {
						info.queue->push(Message(obj));
					}
					else if (info.streamDeserializer.isNull()==false) {
						if (rows.empty()) {
							if (!info.streamDeserializer->parseBlob(obj, rows, symbols, errorInfo)) {
								LOG_ERR("[ERROR] parse BLOB field failed:", errorInfo.errorInfo, ", stopping this parse thread.");
								return;
							}
						}
						for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) {
							info.queue->push(Message(rows[rowIdx], symbols[rowIdx]));
						}
					}
					else if (info.istqueue) {
						info.tqueue->push(info.attributes, obj);
					}
					else if (info.msgAsTable) {
						if (info.attributes.empty()) {
							LOG_ERR("table colName is empty, can not convert to table");
							info.queue->push(Message(obj));
						}
						else {
							info.queue->push(Message(convertTupleToTable(info.attributes, obj)));
						}
					}
					else {
						if (UNLIKELY(cache.empty())) { // split once
							cache.resize(rowSize);
							for (int rowIdx = 0; rowIdx < rowSize; ++rowIdx) {
								VectorSP tmp = Util::createVector(DT_ANY, colSize, colSize);
								for (int colIdx = 0; colIdx < colSize; ++colIdx) {
									VectorSP tmpCol = obj->get(colIdx);
									ConstantSP tmpElm;
									if (tmpCol->getType()>=ARRAY_TYPE_BASE) {
										tmpElm = tmpCol->get(rowIdx);
										VectorSP tmpVElm = (VectorSP)tmpElm;
										if (tmpVElm->getNullFlag()) {
											tmpVElm->setNullFlag(tmpVElm->hasNull());
										}
									} else {
										tmpElm = tmpCol->get(rowIdx);
									}
									tmp->set(colIdx, tmpElm);
								}
								cache[rowIdx] = tmp;
							}
						}
						for (auto &one : cache) {
							info.queue->push(Message(one));
						}
					}
					topicSubInfos_.op([&](unordered_map<string, SubscribeInfo>& mp){
						if(mp.count(t) != 0)
                        	mp[t].offset = offset + 1;
                    });
//                    topicSubInfos_.upsert(
//                        t, [&](SubscribeInfo &info) { info.offset = offset; }, SubscribeInfo());
                }
            }
        } else {
			LOG_ERR("Message body has an invalid format. Vector is expected.");
			break;
        }
    }

	parseSocketThread_.removeItem([&](const SocketThread &socketthread) {
		if (socketthread.socket == in->getSocket()){
			gcSocketThread_.push(socketthread);
			return true;
		}
		return false;
	});
	DLOG("parseMessage exit");
}

void StreamingClientImpl::sendPublishRequest(DBConnection &conn, SubscribeInfo &info){
	ConstantSP re;
	if (info.userName.empty()) {
		re = run(conn, "publishTable", getLocalIP(), listeningPort_, info.tableName, info.actionName,
			info.offset, info.filter, info.allowExists);
	}
	else {
		conn.login(info.userName, info.password, true);
		re = run(conn, "publishTable", getLocalIP(), listeningPort_, info.tableName, info.actionName,
			info.offset, info.filter, info.allowExists);
	}

	if (re->isVector() && re->getType() == DT_ANY) {
		info.haSites.clear();
		auto vec = re->get(1);
		for (int i = 0; i < vec->size(); ++i) {
			auto s = vec->get(i)->getString();
			auto p = Util::split(s, ':');
			info.haSites.emplace_back(p[0], std::stoi(p[1]));
		}
	}
}

string StreamingClientImpl::subscribeInternal(DBConnection &conn, SubscribeInfo &info) {
	if (info.userName.empty() == false)
		conn.login(info.userName, info.password, true);
	ConstantSP result = run(conn, "getSubscriptionTopic", info.tableName, info.actionName);
	auto topic = result->get(0)->getString();
	ConstantSP colLabels = result->get(1);
	if (!colLabels->isArray()) throw RuntimeException("The publisher doesn't have the table [" + info.tableName + "].");

	if (info.streamDeserializer.isNull() == false) {
		info.streamDeserializer->create(conn);
	}

	int colCount = colLabels->size();
	vector<string> colNames;
	colNames.reserve(colCount);
	for (int i = 0; i < colCount; ++i) colNames.push_back(colLabels->getString(i));
	info.attributes = colNames;

	if (isListenMode() == false) {
		std::shared_ptr<DBConnection> activeConn = std::make_shared<DBConnection>(false, false, 30, false, PARSER_TYPE::PARSER_DOLPHINDB, true);
		if (!activeConn->connect(info.host, info.port, "", "", "", false, vector<string>(), 30)) {
			throw RuntimeException("Failed to connect to server: " + info.host + " " + std::to_string(info.port));
		}
		sendPublishRequest(*activeConn, info);
		ActivePublisherSP publisher = new ActivePublisher(activeConn);
		info.socket = publisher->getDataInputStream()->getSocket();
		publishers_.push(publisher);
	}
	else {
		sendPublishRequest(conn, info);
	}
	return topic;
}

void StreamingClientImpl::insertMeta(SubscribeInfo &info, const string &topic) {
    topicSubInfos_.upsert(
        topic, [&](SubscribeInfo &_info) { _info = info; }, info);
    liveSubsOnSite_.upsert(getSite(topic), [&](set<string> &s) { s.insert(topic); }, {topic});
    actionCntOnTable_.upsert(
        stripActionName(topic), [&](int &cnt) { ++cnt; }, 1);
    topics_[info.ID] = topic;
}

bool StreamingClientImpl::delMeta(const string &topic, bool exitFlag) {
	if (topicSubInfos_.count(topic) < 1)
		return false;
	SubscribeInfo oldinfo;
	topicSubInfos_.op([&](unordered_map<string, SubscribeInfo> &mp) {
		oldinfo=mp[topic];
		mp.erase(topic);
	});
    topics_.erase(oldinfo.ID);
    liveSubsOnSite_.upsert(getSite(topic), [&](set<string> &s) { s.erase(topic); }, {});
    actionCntOnTable_.upsert(
        stripActionName(topic), [&](int &cnt) { --cnt; }, 0);
	if(exitFlag)
		oldinfo.exit();
	return true;
}

SubscribeInfo StreamingClientImpl::subscribeInternal(const string &host, int port, const string &tableName,
                                                      const string &actionName, int64_t offset, bool resubscribe,
                                                      const VectorSP &filter, bool msgAsTable, bool allowExists, int batchSize,
													  const string &userName, const string &password,
													  const StreamDeserializerSP &blobDeserializer, bool istqueue,
													  const std::vector<std::string> &backupSites, bool isEvent,
													  int resubTimeout, bool subOnce) {
	if (msgAsTable && !blobDeserializer.isNull()) {
		throw RuntimeException("msgAsTable must be false when StreamDeserializer is set.");
	}
    string topic;
    int attempt = 0;
    string _host = host;
    string _id = host + std::to_string(port) + tableName + actionName;
    int _port = port;
    checkServerVersion(host, port, backupSites);
    init();
    while (isExit()==false) {
        ++attempt;
        SubscribeInfo info(_id, _host, _port, tableName, actionName, offset, resubscribe, filter, msgAsTable, allowExists,
			batchSize, userName, password, blobDeserializer, istqueue, isEvent, resubTimeout, subOnce);
		if(!backupSites.empty()){
            info.availableSites.push_back({host, port});
            info.currentSiteIndex = 0;
            std::string backupIP;
            int         backupPort;
            for(const auto& backupSite : backupSites){
                parseIpPort(backupSite, backupIP, backupPort);
                info.availableSites.push_back({backupIP, backupPort});
            }
        }
        try {
            DBConnection conn = buildConn(_host, _port);
			LockGuard<Mutex> lock(&readyMutex_);
			topic = subscribeInternal(conn, info);
			insertMeta(info, topic);
            return info;
        } catch (exception &e) {
            if(!backupSites.empty()){
                LockGuard<Mutex> _(&mtx_);
                initResub_.push(info);
                insertMeta(info, topic);
                return info;
            }
            else if (attempt <= 10 && getNewLeader(e.what(), _host, _port)) {
                LOG_ERR("Got NotLeaderException, switch to leader node [", _host, ":", _port, "] for subscription");
                HAStreamTableInfo info{host, port, tableName, actionName, _host, _port};
                haStreamTableInfo_.push_back(info);
            } else if (resubscribe) {
                LockGuard<Mutex> _(&mtx_);
                initResub_.push(info);
                insertMeta(info, topic);
                return info;
            } else {
                throw;
            }
        }
    }
	return SubscribeInfo();
}

void StreamingClientImpl::unsubscribeInternal(const string &host, int port, const string &tableName,
                                              const string &actionName) {
    std::string host_ = host;
    int port_ = port;
    string topic;
    string _id = host + std::to_string(port) + tableName + actionName;
    DBConnection conn;
    auto iter = topics_.find(_id);
    if(iter != topics_.end()){
        topic = iter->second;
        SubscribeInfo info;
        if (!topicSubInfos_.find(topic, info)) {
            LOG_WARN("[WARN] subscription of topic", topic, "not existed");
            return;
        }
        if(!info.availableSites.empty()){
            host_ = info.availableSites[info.currentSiteIndex].first;
            port_ = info.availableSites[info.currentSiteIndex].second;
        }
        conn = buildConn(host_, port_);
    }
    else{
        //when unsubscribe using the follow node, get the leader node info from haStreamTableInfo_
        auto iter1 = std::find_if(haStreamTableInfo_.begin(), haStreamTableInfo_.end(), [=](const HAStreamTableInfo& info){
            return info.followIp == host && info.followPort == port && info.tableName == tableName && info.action == actionName;
        });
        if(iter1 != haStreamTableInfo_.end()){
            host_ = iter1->leaderIp;
            port_ = iter1->leaderPort;
            haStreamTableInfo_.erase(iter1);
        }
        else{
            //in case unsubscribe using the leader node and subscribe using the follow node
            auto iter2 = std::find_if(haStreamTableInfo_.begin(), haStreamTableInfo_.end(), [=](const HAStreamTableInfo& info){
                return info.leaderIp == host && info.leaderPort == port && info.tableName == tableName && info.action == actionName;
            });
            if(iter2 != haStreamTableInfo_.end()){
                haStreamTableInfo_.erase(iter2);
            }
        }
        conn = buildConn(host_, port_);
        topic = run(conn, "getSubscriptionTopic", tableName, actionName)->get(0)->getString();
        if (!topicSubInfos_.count(topic)) {
            LOG_WARN("[WARN] subscription of topic", topic, "not existed");
            return;
        }
    }
	delMeta(topic, true);
	while(gcSocketThread_.size() > 0){
		gcSocketThread_.removeItem([&](const SocketThread &socketthread){
			return socketthread.thread->isComplete();
		});
		Util::sleep(100);
	}
	if (isListenMode()) {
    	run(conn, "stopPublishTable", getLocalIP(), listeningPort_, tableName, actionName);
	}
}

StreamingClient::StreamingClient(int listeningPort) : impl_(new StreamingClientImpl(listeningPort)) {}

StreamingClient::~StreamingClient() {
	exit();
}

void StreamingClient::exit() {
	impl_->exit();
}

bool StreamingClient::isExit() {
	return impl_->isExit();
}

SubscribeInfo StreamingClient::subscribeInternal(string host, int port, string tableName, string actionName,
                                                  int64_t offset, bool resubscribe, const dolphindb::VectorSP &filter,
                                                  bool msgAsTable, bool allowExists, int batchSize,
												  string userName, string password,
												  const StreamDeserializerSP &blobDeserializer, bool istqueue,
												  const std::vector<std::string> &backupSites,
												  bool isEvent, int resubTimeout, bool subOnce) {
    return impl_->subscribeInternal(host, port, tableName, actionName, offset, resubscribe, filter, msgAsTable,
                                    allowExists, batchSize,
									userName,password,
									blobDeserializer, istqueue,
									backupSites, isEvent, resubTimeout, subOnce);
}

void StreamingClient::unsubscribeInternal(string host, int port, string tableName, string actionName) {
    impl_->unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// ThreadedClient impl
ThreadedClient::ThreadedClient(int listeningPort) : StreamingClient(listeningPort) {}

size_t ThreadedClient::getQueueDepth(const ThreadSP &thread) {
	return impl_->getQueueDepth(thread);
}

ThreadSP ThreadedClient::subscribe(string host, int port, const MessageBatchHandler &handler, string tableName,
                                   string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                   bool allowExists, int batchSize, double throttle, bool msgAsTable,
									string userName, string password,
								   const StreamDeserializerSP &blobDeserializer,
								   const std::vector<std::string> &backupSites, int resubTimeout, bool subOnce) {
    SubscribeInfo info = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset,
                                           resub, filter, msgAsTable, allowExists, batchSize,
										   userName, password, blobDeserializer, msgAsTable,
										   backupSites, false, resubTimeout, subOnce);
    if (info.queue.isNull() && info.tqueue.isNull()) {
        LOG_ERR("Subscription already made, handler loop not created.");
        ThreadSP t = new Thread(new Executor([]() {}));
        t->start();
        return t;
    }
    int throttleTime;
    if(batchSize <= 0){
        throttleTime = 0;
    }else{
        throttleTime = std::max(1, (int)(throttle * 1000));
    }
	SmartPointer<StreamingClientImpl> impl=impl_;
	ThreadSP thread = new Thread(new Executor([handler, info, throttleTime, impl, msgAsTable]() {
		try {
			bool foundnull = false;
			if (msgAsTable) {
				ConstantSP msg;
				bool isexit;
				while (impl->isExit() == false && info.tqueue->getExitFlag() == false) {
					if (info.tqueue->pop(msg, throttleTime)) {
						if(!msg.isNull()){
							handler({Message(msg)});
						}
					}
				}
			}
			else {
				vector<Message> msgs;
				while (impl->isExit() == false && foundnull == false) {
					if (info.queue->pop(msgs, throttleTime)) {
						while (msgs.empty() == false && msgs.back().isNull()) {
							msgs.pop_back();
							foundnull=true;
						}
						if(!msgs.empty()) {
							handler(msgs);
						}
					}
				}

			}
		}
		catch(exception &e) {
            LOG_ERR(e.what());
		}
	}));
	if (msgAsTable) {
		impl_->addHandleThread(info.tqueue, thread);
	} else {
		impl_->addHandleThread(info.queue, thread);
	}

	thread->start();
    return thread;
}

ThreadSP newHandleThread(const MessageHandler handler, MessageQueueSP queue, bool msgAsTable, SmartPointer<StreamingClientImpl> impl) {
	ThreadSP thread = new Thread(new Executor([handler, queue, msgAsTable, impl]() {
		try {
			DLOG("nht handle start.");
			Message msg;
			vector<Message> tables;
			bool foundnull = false;
			while (foundnull == false && impl->isExit() == false) {
				queue->pop(msg);
				// quit handler loop if msg is nullptr
				if (UNLIKELY(msg.isNull())){
					foundnull = true;
					break;
				}
				if (msgAsTable && queue->pop(tables, 0)) {
					if (!mergeTable(msg, tables)) {
						foundnull = true;
					}
				}
				handler(msg);
			}
			queue->push(Message());
			DLOG("nht handle exit.");
		}
		catch(exception &e) {
            LOG_ERR(e.what());
		}
	}));
	impl->addHandleThread(queue,thread);
	return thread;
}

ThreadSP ThreadedClient::subscribe(string host, int port, const MessageHandler &handler, string tableName,
                                   string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                   bool msgAsTable, bool  /*allowExists*/,
									string userName, string password,
									const StreamDeserializerSP &blobDeserializer,
									const std::vector<std::string> &backupSites, int resubTimeout, bool subOnce) {
    SubscribeInfo info = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset,
                                           resub, filter, msgAsTable, false,1,
										   userName, password, blobDeserializer, false,
										   backupSites, false, resubTimeout, subOnce);
    if (info.queue.isNull()) {
        LOG_ERR("Subscription already made, handler loop not created.");
        ThreadSP t = new Thread(new Executor([]() {}));
        t->start();
        return t;
    }

	ThreadSP t = newHandleThread(handler, info.queue, false, impl_);
    t->start();
    return t;
}

void ThreadedClient::unsubscribe(string host, int port, string tableName, string actionName) {
    unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// PollingClient IMPL
PollingClient::PollingClient(int listeningPort) : StreamingClient(listeningPort) {}

MessageQueueSP PollingClient::subscribe(string host, int port, string tableName, string actionName, int64_t offset,
                                        bool resub, const VectorSP &filter, bool msgAsTable, bool allowExists,
										string userName, string password,
										const StreamDeserializerSP &blobDeserializer,
										const std::vector<std::string> &backupSites, int resubTimeout, bool subOnce) {
    auto info = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset, resub, filter,
                             	  msgAsTable, allowExists,1,
								  userName, password, blobDeserializer, false,
								  backupSites, false, resubTimeout, subOnce);
	return info.queue;
}

void PollingClient::unsubscribe(string host, int port, string tableName, string actionName) {
    unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

/// ThreadPooledClient IMPL
ThreadPooledClient::ThreadPooledClient(int listeningPort, int threadCount)
    : StreamingClient(listeningPort), threadCount_(threadCount) {}

void ThreadPooledClient::unsubscribe(string host, int port, string tableName, string actionName) {
    unsubscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName));
}

size_t ThreadPooledClient::getQueueDepth(const ThreadSP &thread){
	return impl_->getQueueDepth(thread);
}

vector<ThreadSP> ThreadPooledClient::subscribe(string host, int port, const MessageHandler &handler, string tableName,
                                               string actionName, int64_t offset, bool resub, const VectorSP &filter,
                                               bool msgAsTable, bool allowExists,
												string userName, string password,
												const StreamDeserializerSP &blobDeserializer,
												const std::vector<std::string> &backupSites, int resubTimeout, bool subOnce) {
    auto info = subscribeInternal(std::move(host), port, std::move(tableName), std::move(actionName), offset, resub,
                                  filter, msgAsTable, allowExists, 1,
								  userName, password, blobDeserializer, false, backupSites, false, resubTimeout, subOnce);
    vector<ThreadSP> ret;
    for (int i = 0; i < threadCount_ && isExit() == false; ++i) {
		ThreadSP t = newHandleThread(handler, info.queue, msgAsTable, impl_);
        t->start();
        ret.emplace_back(t);
    }
    return ret;
}

EventClient::EventClient(const std::vector<EventSchema>& eventSchema, const std::vector<std::string>& eventTimeKeys, const std::vector<std::string>& commonKeys)
    : StreamingClient(0), eventHandler_(eventSchema, eventTimeKeys, commonKeys)
{

}

ThreadSP EventClient::subscribe(const string& host, int port, const EventMessageHandler &handler, const string& tableName, const string& actionName, int64_t offset, bool resub, const string& userName, const string& password){
    if(tableName.empty()){
        throw RuntimeException("tableName must not be empty.");
    }

    DBConnection tempConn;
    if(!tempConn.connect(host, port, userName, password)){
        throw RuntimeException("Subscribe Fail, cannot connect to " + host + " : " + std::to_string(port));
    }
    std::string sql = "select top 0 * from " + tableName;
    std::string errMsg;
    ConstantSP outputTable = tempConn.run(sql);
    if(!eventHandler_.checkOutputTable(outputTable, errMsg)){
        throw RuntimeException(errMsg);
    }
    tempConn.close();

    auto info = subscribeInternal(host, port, tableName, actionName, offset, resub, nullptr, false, false, 1, userName, password, nullptr, false, std::vector<std::string>(), true, 100, false);
    if (info.queue.isNull()) {
        LOG_ERR("Subscription already made, handler loop not created.");
        return nullptr;
    }

    SmartPointer<StreamingClientImpl> impl = impl_;
    ThreadSP thread = new Thread(new Executor([this, handler, info, impl]() {
		DLOG("nht handle start.");
		Message msg;
		vector<Message> tables;
		bool foundnull = false;
        std::vector<std::string> eventTypes;
        std::vector<std::vector<ConstantSP>> attributes;
        ErrorCodeInfo errorInfo;
		while (foundnull == false && impl->isExit() == false) {
			info.queue->pop(msg);
			// quit handler loop if msg is nullptr
			if (UNLIKELY(msg.isNull())){
				foundnull = true;
				break;
			}
            eventTypes.clear();
            attributes.clear();
            if(!eventHandler_.deserializeEvent(msg, eventTypes, attributes, errorInfo)){
                LOG_ERR("deserialize fail", errorInfo.errorInfo);
                continue;
            }
            unsigned rowSize = eventTypes.size();
            for(unsigned i = 0; i < rowSize; ++i){
                handler(eventTypes[i], attributes[i]);
            }
			//handler(msg);
		}
		info.queue->push(Message());
		DLOG("nht handle exit.");
	}));
	impl_->addHandleThread(info.queue, thread);
	thread->start();
    return thread;
}

void EventClient::unsubscribe(const string& host, int port, const string& tableName, const string& actionName){
    unsubscribeInternal(host, port, tableName, actionName);
}


/// pop
int MessageTableQueue::size() {
	LockGuard<Mutex> guard(&lock_);
	return size_;
}

void MessageTableQueue::setExitFlag() {
	LockGuard<Mutex> guard(&lock_);
	exitflag_ = true;
}

bool MessageTableQueue::getExitFlag() {
	LockGuard<Mutex> guard(&lock_);
	return exitflag_;
}
void MessageTableQueue::push(const std::vector<string>& colLabels, const ConstantSP &colItems) {
	LockGuard<Mutex> guard(&lock_);
	if (exitflag_) return;
	colSize_ = colLabels.size();
	vector<ConstantSP> cols(colSize_);
	for (int i = 0; i < colSize_; ++i) {
		cols[i] = colItems->get(i);
		cols[i]->setTemporary(true);
	}
	while(size_ >= capacity_) full_.wait(lock_);
	if (messageTable_.isNull()) {
		messageTable_ = Util::createTable(colLabels, cols);
		colSize_ = messageTable_->columns();
		size_ = messageTable_->size();
		// empty_.notifyAll();
	}else{
		// bool empty = false;
		// if(size_ == 0) empty=true;
		for (INDEX colIndex = 0; colIndex < colSize_; ++colIndex) {
			((Vector*)(messageTable_->getColumn(colIndex).get()))->append(cols[colIndex]);
		}
		auto basicTable = dynamic_cast<BasicTable*>(messageTable_.get());
		if(basicTable != nullptr) {
			basicTable->updateSize();
		}
		size_ = messageTable_->size();
		// if (empty) empty_.notifyAll();
	}
	if (size_ >= batchSize_) {
		batch_.notifyAll();
		full_.wait(lock_);
	}
}
bool MessageTableQueue::pop(ConstantSP &item, int milliSeconds) {
	LockGuard<Mutex> guard(&lock_);
	if (size_ < batchSize_)
		batch_.wait(lock_, milliSeconds);
	if (size_ == 0)
		return false;
	item = messageTable_;
	messageTable_.clear();
	size_ = 0;
	full_.notifyAll();
	return true;
}

}  // namespace dolphindb
