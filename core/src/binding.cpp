//
// Created by jccai on 3/28/19.
//

#include "Constant.h"
#include "DolphinDB.h"
#include "Streaming.h"
#include "BatchTableWriter.h"
#include "MultithreadedTableWriter.h"
#include "ConstantImp.h"
#include "DdbPythonUtil.h"
#include "Util.h"
#include "Logger.h"
#include "Wrappers.h"
#include "EventHandler.h"

#include "TypeConverter.h"
#include "TypeHelper.h"
#include "IOBinding.h"

#include "pybind11/numpy.h"
#include "pybind11/pybind11.h"
#include "pybind11/eval.h"
#include "pybind11/stl.h"
#include <pybind11/cast.h>
#include <pybind11/pytypes.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <map>
#include <algorithm>

#ifndef MAC
    #include <signal.h>
#else
    #include <sys/signal.h>
#endif

#ifndef LINUX
	typedef void(*sighandler_t)(int);
#endif

namespace py = pybind11;
namespace ddb = dolphindb;


using converter::Converter;
using converter::createType;
using converter::PyObjs;
using converter::TableChecker;
using converter::Type;

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#ifdef DLOG
    #undef DLOG
#endif
#define DLOG        // ddb::DLogger::Info

#ifndef MAC
    #define TRY                     try {

    #define CATCH_EXCEPTION(text)   } catch (std::exception &ex) { \
                                        throw std::runtime_error(std::string(text) + ex.what()); \
                                    } catch (...) { \
                                        throw std::runtime_error(std::string(text) + " unknow exception."); \
                                    }
#else
    #define TRY
    #define CATCH_EXCEPTION(text)
#endif

#define DEFAULT_PRIORITY 4
#define DEFAULT_PARALLELISM 64


void ddbinit() {
    if (converter::PyObjs::cache_ == nullptr) {
        converter::PyObjs::Initialize();
        dolphindb::DLogger::init();
    }
}

class Defer {
public:
    Defer(std::function<void()> code): code(code) {}
    ~Defer() { code(); }
private:
    std::function<void()> code;
};

class SessionImpl;
void signal_handler_fun(int signum);

class DBConnectionPoolImpl {
public:
    DBConnectionPoolImpl(const std::string& hostName, int port, int threadNum = 10, const std::string& userId = "", const std::string& password = "",
            bool loadBalance = false, bool highAvailability = false, bool compress = false, bool reConnect = false, bool python = false,
            int protocol = ddb::PROTOCOL_PICKLE, bool show_output = true, int sqlStd = 0, int tryReconnectNums = -1)
            :dbConnectionPool_(hostName, port, threadNum, userId, password,loadBalance,highAvailability,compress,reConnect,python,ddb::PROTOCOL(protocol),show_output, sqlStd, tryReconnectNums),
                host_(hostName), port_(port), threadNum_(threadNum), userId_(userId), password_(password) {}
    ~DBConnectionPoolImpl() {}
    py::object run(const std::string &script, int taskId, const py::args &args,
                   const py::handle &clearMemory = py::none(), const py::handle &pickleTableToList = py::none(),
                   const py::handle &priority = py::none(), const py::handle &parallelism = py::none(),
                   const py::handle &disableDecimal = py::none()) {
        bool clearMemory_ = false;
        if (!clearMemory.is_none()) {
            clearMemory_ = clearMemory.cast<bool>();
        }

        bool pickleTableToList_ = false;
        if (!pickleTableToList.is_none()) {
            pickleTableToList_ = pickleTableToList.cast<bool>();
        }

        int priority_ = DEFAULT_PRIORITY;
        if (!priority.is_none()) {
            priority_ = priority.cast<int>();
        }

        int parallelism_ = DEFAULT_PARALLELISM;
        if (!parallelism.is_none()) {
            parallelism_ = parallelism.cast<int>();
        }

        bool disableDecimal_ = false;
        if (!disableDecimal.is_none()) {
            disableDecimal_ = disableDecimal.cast<bool>();
        }

        if (args.empty()) {
            // script mode
            TRY dbConnectionPool_.runPy(script, taskId, priority_, parallelism_, 0, clearMemory_, pickleTableToList_,
                                        disableDecimal_);
            CATCH_EXCEPTION("<Exception> in run: ")
        } else {
            // function mode
            std::vector<ddb::ConstantSP> ddbArgs;
            for (const auto &one : args) {
                py::object pyobj = py::reinterpret_borrow<py::object>(one);
                ddb::ConstantSP pcp = Converter::toDolphinDB(pyobj);
                ddbArgs.push_back(pcp);
            }
            TRY dbConnectionPool_.runPy(script, ddbArgs, taskId, priority_, parallelism_, 0, clearMemory_,
                                        pickleTableToList_, disableDecimal_);
            CATCH_EXCEPTION("<Exception> in run: ")
        }
        return py::none();
    }
    bool isFinished(int taskId) {
        bool isFinished;
        TRY
            isFinished = dbConnectionPool_.isFinished(taskId);
        CATCH_EXCEPTION("<Exception> in isFinished: ")
        return isFinished;
    }
    py::object getData(int taskId) {
        py::object result;
        TRY
            result = dbConnectionPool_.getPyData(taskId);
        CATCH_EXCEPTION("<Exception> in getData: ")
        return result;
    }
    void shutDown() {
        host_ = "";
        port_ = 0;
        userId_ = "";
        password_ = "";
        dbConnectionPool_.shutDown();
    }

    py::object getSessionId() {
        vector<string> sessionId = dbConnectionPool_.getSessionId();
        py::list ret;
        for(auto &id: sessionId){
            ret.append(py::str(id));
        }
        return ret;
    }

    ddb::DBConnectionPool& getPool() {
        return dbConnectionPool_;
    }
    
private:
    ddb::DBConnectionPool dbConnectionPool_;
    std::string host_;
    int port_;
    int threadNum_;
    std::string userId_;
    std::string password_;
};

class BlockReader{
public:
    BlockReader(ddb::BlockReaderSP reader): reader_(reader){
    }
    ~BlockReader(){
    }
    void skipAll() {
        reader_->skipAll();
    }
    py::bool_ hasNext(){
        return py::bool_(reader_->hasNext());
    }
    py::object read(){
        py::object ret;
        TRY
            ret = Converter::toPython_Old(reader_->read());
        CATCH_EXCEPTION("<Exception> in read: ")
        return ret;
    }

private:
    ddb::BlockReaderSP reader_;
};

class PartitionedTableAppender{
public:
    PartitionedTableAppender(string dbUrl, string tableName, string partitionColName, DBConnectionPoolImpl& pool)
    :partitionedTableAppender_(dbUrl,tableName,partitionColName,pool.getPool()){}
    int append(py::object table){
        if (!CHECK_INS(table, pd_dataframe_))
            throw std::runtime_error(std::string("table must be a DataFrame!"));
        int insertRows;
        vector<ddb::Type> colTypes = partitionedTableAppender_.getColTypes();
        vector<std::string> colNames = partitionedTableAppender_.getColNames();
        ddb::TableChecker checker;
        for(int i=0;i<colTypes.size();++i){
            checker[colNames[i]] = colTypes[i];
        }
        TRY
            auto data = Converter::toDolphinDB_Table_fromDataFrame(table, checker);
            py::gil_scoped_release gil_release;
            insertRows = partitionedTableAppender_.append(data);
        CATCH_EXCEPTION("<Exception> in append: ")
        return insertRows;
    }
private:
    ddb::PartitionedTableAppender partitionedTableAppender_;
};

class StreamDeserializer{
public:
    //sym2tableName: symbol -> [dbPath,tableName] or symbol -> [tableName]
    StreamDeserializer(py::dict &sym2dbTableName){
        if(sym2dbTableName.size() > 0){
            pydict2map(sym2dbTableName, sym2tableName_);
        }
        sessionImpl_ = nullptr;
    }
    void setSession(SessionImpl& session){
        sessionImpl_ = &session;
    }
    ddb::SmartPointer<ddb::StreamDeserializer> get();
private:
    static void pydict2map(py::dict &pydict,unordered_map<string, pair<string, string>>& map){
        for (auto it = pydict.begin(); it != pydict.end(); ++it) {
            string symbol=it->first.cast<std::string>();
            string dbpath,tablename;
            if (CHECK_INS(it->second, py_str_)) {
                tablename=it->second.cast<std::string>();
            }
            else if (CHECK_INS(it->second, py_list_)) {
                py::list list=it->second.cast<py::list>();
                if(list.size() != 2){
                    throw std::runtime_error(std::string("<Exception> in StreamDeserializer: list size must be 2"));
                }
                dbpath=list[0].cast<std::string>();
                tablename=list[1].cast<std::string>();
            }
            else if (CHECK_INS(it->second, py_tuple_)) {
                py::tuple list=it->second.cast<py::tuple>();
                if(list.size() != 2){
                    throw std::runtime_error(std::string("<Exception> in StreamDeserializer: tuple size must be 2"));
                }
                dbpath=list[0].cast<std::string>();
                tablename=list[1].cast<std::string>();
            }
            else {
                throw std::runtime_error(std::string("<Exception> in StreamDeserializer: unsupported type in dict, support string, list and tuple."));
            }
            map[symbol]=std::make_pair(dbpath,tablename);
        }
    }
    unordered_map<string, pair<string, string>> sym2tableName_;
    SessionImpl *sessionImpl_;
    ddb::SmartPointer<ddb::StreamDeserializer> streamDeserializer_;
};

// FIXME: not thread safe
class SessionImpl {
public:
    SessionImpl(bool enableSSL=false, bool enableASYN=false, int keepAliveTime=7200, bool compress=false, bool python=false, int protocol=ddb::PROTOCOL_DDB, bool show_output=true, int sqlStd=0)
        : host_(), port_(-1), userId_(), password_(), encrypted_(true),
            dbConnection_(enableSSL,enableASYN, keepAliveTime, compress, python, false, sqlStd), nullValuePolicy_([](ddb::VectorSP) {}), subscriber_(nullptr),subscriberPool_(nullptr),keepAliveTime_(keepAliveTime) {
                dbConnection_.setProtocol(ddb::PROTOCOL(protocol));
                dbConnection_.setShowOutput(show_output);
            }

    bool connect(const std::string &host, const int &port, const std::string &userId,
                    const std::string &password, const std::string &startup = "",
                    const bool &highAvailability = false, const std::vector<string> &highAvailabilitySites = {},
                    const int &keepAliveTime = 30, bool reconnect = false, int tryReconnectNums = -1, int readTimeout = -1, int writeTimeout = -1) {
        host_ = host;
        port_ = port;
        userId_ = userId;
        password_ = password;
        bool isSuccess = false;
        if (keepAliveTime > 0) {
            dbConnection_.setKeepAliveTime(keepAliveTime);
        }
        TRY isSuccess = dbConnection_.connect(host_, port_, userId_, password_, startup, highAvailability,
                                                highAvailabilitySites, keepAliveTime, reconnect, tryReconnectNums, readTimeout, writeTimeout);
        CATCH_EXCEPTION("<Exception> in connect: ")
        return isSuccess;
    }

    void setInitScript(string script) {
        TRY
            dbConnection_.setInitScript(script);
        CATCH_EXCEPTION("<Exception> in setInitScript: ")
    }

    string getInitScript() {
        TRY
            return dbConnection_.getInitScript();
        CATCH_EXCEPTION("<Exception> in getInitScript: ")
    }

    void login(const std::string &userId, const std::string &password, bool enableEncryption) {
        TRY
           dbConnection_.login(userId, password, enableEncryption);
        CATCH_EXCEPTION("<Exception> in login: ")
    }

    void close() {
        host_ = "";
        port_ = 0;
        userId_ = "";
        password_ = "";
        dbConnection_.close();
    }

    const string getSessionId(){
        return dbConnection_.getSessionId();
    }

    py::object loadPickleFile(const std::string &filepath){
        return ddb::DdbPythonUtil::loadPickleFile(filepath);
    }
    py::object upload(const py::dict &namedObjects) {
        vector<std::string> names;
        vector<ddb::ConstantSP> objs;
        for (auto it = namedObjects.begin(); it != namedObjects.end(); ++it) {
            if (!CHECK_INS(it->first, py_str_) && !CHECK_INS(it->first, py_bytes_)) { throw std::runtime_error("non-string key in upload dictionary is not allowed"); }
            names.push_back(it->first.cast<std::string>());
            objs.push_back(Converter::toDolphinDB(it->second));
        }
        TRY
            auto addr = dbConnection_.upload(names, objs);
            if (addr == NULL || addr->getType() == ddb::DT_VOID ||addr->isNothing()) {
                return py::int_(-1);
            } else if(addr->isScalar()){
                return py::int_(addr->getLong());
            } else {
                size_t size = addr->size();
                py::list pyAddr;
                for (size_t i = 0; i < size; ++i) { pyAddr.append(py::int_(addr->getLong(i))); }
                return pyAddr;
            }
        CATCH_EXCEPTION("<Exception> in upload: ")
    }

    ddb::ConstantSP runcpp(const string &script) {
        ddb::ConstantSP result;
        TRY
            result = dbConnection_.run(script);
            
        CATCH_EXCEPTION("<Exception> in runcpp: ")
        return result;
    }

    ddb::ConstantSP runcpp(const string &funcName, vector<ddb::ConstantSP> &args) {
        ddb::ConstantSP result;
        TRY
            result = dbConnection_.run(funcName, args);
            
        CATCH_EXCEPTION("<Exception> in runcpp: ")
        return result;
    }

    py::object run(const std::string &script, const py::args &args, const py::handle &clearMemory = py::none(),
                   const py::handle &pickleTableToList = py::none(), const py::handle &priority = py::none(),
                   const py::handle &parallelism = py::none(), const py::handle &disableDecimal = py::none(), const py::handle &withTableSchema = py::none()) {
        if (enableJobCancellation_) {
            ddb::LockGuard<ddb::Mutex> LockGuard(&SessionImpl::mapMutex_);
            if (SessionImpl::runningMap_.count(this) == 0) {
                SessionImpl::runningMap_.insert(pair<SessionImpl *, int>(this, 1));
            } else {
                SessionImpl::runningMap_[this] += 1;
            }

            sighandler_t old_handler;

            old_handler = signal(SIGINT, signal_handler_fun);
            if ((long long)old_handler != (long long)signal_handler_fun) {
                sighandler_ = old_handler;
            }
        }
        Defer df([=]() {
            if (enableJobCancellation_) {
                ddb::LockGuard<ddb::Mutex> LockGuard(&SessionImpl::mapMutex_);

                if (SessionImpl::runningMap_.count(this) > 0) {
                    SessionImpl::runningMap_[this] -= 1;
                    if (SessionImpl::runningMap_[this] <= 0) {
                        SessionImpl::runningMap_.erase(this);
                    }
                } else {
                    throw std::runtime_error("Session: " + getSessionId() + " is not exited.");
                }

                if (!SessionImpl::isSigint_) {
                    if (SessionImpl::runningMap_.size() == 0) {
                        signal(SIGINT, SessionImpl::sighandler_);
                        SessionImpl::sighandler_ = nullptr;
                    }

                } else {
                    if (SessionImpl::runningMap_.size() == 0) {
                        signal(SIGINT, SessionImpl::sighandler_);
                        SessionImpl::sighandler_ = nullptr;
                        SessionImpl::isSigint_ = false;
#ifndef MAC
                        raise(SIGINT);
#endif
                    }
                }
            }
        });

        bool clearMemory_ = false;
        if (!clearMemory.is_none()) {
            clearMemory_ = clearMemory.cast<bool>();
        }

        bool pickleTableToList_ = false;
        if (!pickleTableToList.is_none()) {
            pickleTableToList_ = pickleTableToList.cast<bool>();
        }

        int priority_ = DEFAULT_PRIORITY;
        if (!priority.is_none()) {
            priority_ = priority.cast<int>();
        }

        int parallelism_ = DEFAULT_PARALLELISM;
        if (!parallelism.is_none()) {
            parallelism_ = parallelism.cast<int>();
        }

        bool disableDecimal_ = false;
        if (!disableDecimal.is_none()) {
            disableDecimal_ = disableDecimal.cast<bool>();
        }

        bool withTableSchema_ = false;
        if (!withTableSchema.is_none()) {
            withTableSchema_ = withTableSchema.cast<bool>();
        }

        py::object result;
        if (args.empty()) {
            // script mode
            TRY result = dbConnection_.runPy(script, priority_, parallelism_, 0, clearMemory_, pickleTableToList_,
                                             disableDecimal_, withTableSchema_);
            CATCH_EXCEPTION("<Exception> in run: ")
        } else {
            // function mode
            TRY std::vector<ddb::ConstantSP> ddbArgs;
            for (const auto &it : args) {
                ddbArgs.push_back(Converter::toDolphinDB(it));
            }
            result = dbConnection_.runPy(script, ddbArgs, priority_, parallelism_, 0, clearMemory_, pickleTableToList_,
                                         disableDecimal_, withTableSchema_);
            CATCH_EXCEPTION("<Exception> in run: ")
        }
        return result;
    }

    BlockReader runBlock(const string &script, const py::handle &clearMemory = py::none(),
                         const py::handle &fetchSize = py::none(), const py::handle &priority = py::none(),
                         const py::handle &parallelism = py::none()) {
        bool clearMemory_ = false;
        if (!clearMemory.is_none()) {
            clearMemory_ = clearMemory.cast<bool>();
        }

        int fetchSize_ = 0;
        if (!fetchSize.is_none()) {
            fetchSize_ = fetchSize.cast<int>();
        }

        int priority_ = DEFAULT_PRIORITY;
        if (!priority.is_none()) {
            priority_ = priority.cast<int>();
        }

        int parallelism_ = DEFAULT_PARALLELISM;
        if (!parallelism.is_none()) {
            parallelism_ = parallelism.cast<int>();
        }

        if (fetchSize_ < 8192) {
            throw std::runtime_error(std::string("<Exception> in runBlock: fetchSize must be greater than 8192"));
        }
        ddb::ConstantSP result;
        TRY result = dbConnection_.run(script, priority_, parallelism_, fetchSize_, clearMemory_);
        CATCH_EXCEPTION("<Exception> in runBlock: ")
        BlockReader blockReader(result);
        return blockReader;
    }

    void nullValueToZero() {
        nullValuePolicy_ = [](ddb::VectorSP vec) {
            if (!vec->hasNull() || vec->getCategory() == ddb::TEMPORAL || vec->getType() == ddb::DT_STRING || vec->getType() == ddb::DT_SYMBOL) {
                return;
            } else {
                ddb::ConstantSP val = ddb::Util::createConstant(ddb::DT_LONG);
                val->setLong(0);
                vec->nullFill(val);
                assert(!vec->hasNull());
            }
        };
    }

    void nullValueToNan() {
        nullValuePolicy_ = [](ddb::VectorSP) {};
    }

    void enableStreaming(int listeningPort, int threadcount) {
        if (subscriber_.isNull() && subscriberPool_.isNull()) {
            if(threadcount <= 1){
                subscriber_ = new ddb::ThreadedClient(listeningPort);
            }else{
                subscriberPool_ = new ddb::ThreadPooledClient(listeningPort, threadcount);
            }
            ddb::Util::sleep(100);
        } else {
            throw std::runtime_error("streaming is already enabled");
        }
    }

    // static void disableJobCancellation() {
    //     if (SessionImpl::enableJobCancellation_ == true) {
    //         SessionImpl::enableJobCancellation_ = false;
    //     } else {
    //         throw std::runtime_error("Job cancellation is already disabled");
    //     }
    // }

    static void enableJobCancellation() {
        if (SessionImpl::enableJobCancellation_ == false) {
            SessionImpl::enableJobCancellation_ = true;
        } else {
            throw std::runtime_error("Job cancellation is already enabled");
        }
    }

    static void setTimeout(double timeout) {
        unsigned int t = (unsigned int)(round(timeout*1000));
        ddb::Socket::setTcpTimeout(t);
    }

    void subscribe(const string &host, const int &port, py::object handler, const string &tableName, const string &actionName,
                        const long long &offset, const bool &resub, py::object filter,const string &userName,const string &password,
                        StreamDeserializer &streamDeserializer,
                        const std::vector<std::string> backupSites = std::vector<std::string>(),
                        int resubTimeout = 100, bool subOnce = false) {
        if (subscriber_.isNull() && subscriberPool_.isNull() ) { throw std::runtime_error("streaming is not enabled"); }
        ddb::LockGuard<ddb::Mutex> LockGuard(&subscriberMutex_);
        string topic = host + "/" + std::to_string(port) + "/" + tableName + "/" + actionName;
        if (topicThread_.find(topic) != topicThread_.end()) { throw std::runtime_error("subscription " + topic + " already exists"); }
        bool hasStreamDeser = streamDeserializer.get().isNull()==false;
        ddb::MessageHandler ddbHandler = [handler, this, hasStreamDeser](ddb::Message msg) {
            // handle GIL
            py::gil_scoped_acquire acquire;
            py::list row = Converter::toPython_Old(msg);
            if(hasStreamDeser){
                row.append(msg.getSymbol());
            }
            handler(row);
        };

        ddb::VectorSP ddbFilter;
        if (py::isinstance<py::array>(filter)) {
            py::array arr_filter = py::cast<py::array>(filter);
            ddbFilter = arr_filter.size() ? Converter::toDolphinDB(arr_filter) : nullptr;
        } else if (py::isinstance<py::str>(filter)) {
            std::string strFilter = py::str(filter);
            ddb::ConstantSP ddbStrFilter = new ddb::String(strFilter);
            ddbFilter = (ddb::VectorSP)ddbStrFilter;
        }

        TRY
        vector<ddb::ThreadSP> threads;
        if(subscriber_.isNull() == false){
            ddb::ThreadSP thread = subscriber_->subscribe(host, port, ddbHandler, tableName, actionName, offset, resub, ddbFilter,
                    false,false,userName,password,streamDeserializer.get(), backupSites, resubTimeout, subOnce);
            threads.push_back(thread);
        }else{
            threads = subscriberPool_->subscribe(host, port, ddbHandler, tableName, actionName, offset, resub, ddbFilter,
                    false,false,userName,password,streamDeserializer.get(), backupSites, resubTimeout, subOnce);
        }
        topicThread_[topic] = threads;
        CATCH_EXCEPTION("<Exception> in subscribe: ")
    }

    // FIXME: not thread safe
    void subscribeBatch(string &host,int port, py::object handler,string &tableName,string &actionName,long long offset, bool resub, py::object filter,
            const bool & msgAsTable, int batchSize, double throttle,const string &userName,const string &password,StreamDeserializer &streamDeserializer,
            const std::vector<std::string> backupSites = std::vector<std::string>(),
            int resubTimeout = 100, bool subOnce = false) {
        if (subscriber_.isNull()) {
            if(subscriberPool_.isNull()){
                throw std::runtime_error("streaming is not enabled");
            }else{
                throw std::runtime_error("Thread pool streaming doesn't support batch subscribe");
            }
        }
        ddb::LockGuard<ddb::Mutex> LockGuard(&subscriberMutex_);
        string topic = host + "/" + std::to_string(port) + "/" + tableName + "/" + actionName;
        if (topicThread_.find(topic) != topicThread_.end()) { throw std::runtime_error("subscription " + topic + " already exists"); }
        bool hasStreamDeser = streamDeserializer.get().isNull()==false;
        ddb::MessageBatchHandler ddbHandler = [handler, msgAsTable, this, hasStreamDeser](std::vector<ddb::Message> msgs) {
            // handle GIL
            py::gil_scoped_acquire acquire;
            size_t size = msgs.size();   
            py::list pyMsg(size);
            if(hasStreamDeser){
                for (size_t i = 0; i < size; ++i) {
                    py::list row = Converter::toPython_Old(msgs[i]);
                    row.append(msgs[i].getSymbol());
                    pyMsg[i] = row;
                }
            }else{
                for (size_t i = 0; i < size; ++i) {
                    pyMsg[i] = Converter::toPython_Old(msgs[i]);
                }
            }
            if(msgAsTable && hasStreamDeser == false){
                // py::object dataframe = preserved_->pandas_.attr("concat")(pyMsg);
                handler(pyMsg[0]);
            }else {
                handler(pyMsg);
            }
        };

        ddb::VectorSP ddbFilter;
        if (py::isinstance<py::array>(filter)) {
            py::array arr_filter = py::cast<py::array>(filter);
            ddbFilter = arr_filter.size() ? Converter::toDolphinDB(arr_filter) : nullptr;
        } else if (py::isinstance<py::str>(filter)) {
            std::string strFilter = py::str(filter);
            ddb::ConstantSP ddbStrFilter = new ddb::String(strFilter);
            ddbFilter = (ddb::VectorSP)ddbStrFilter;
        }

        TRY
        vector<ddb::ThreadSP> threads;
        ddb::ThreadSP thread = subscriber_->subscribe(host, port, ddbHandler, tableName, actionName, offset, resub, ddbFilter, false, 
            batchSize, throttle,msgAsTable,userName,password,streamDeserializer.get(), backupSites, resubTimeout, subOnce);
        threads.push_back(thread);
        topicThread_[topic] = threads;
        CATCH_EXCEPTION("<Exception> in subscribeBatch: ")
    }

    // FIXME: not thread safe
    void unsubscribe(string host, int port, string tableName, string actionName) {
        if (subscriber_.isNull() && subscriberPool_.isNull()) { throw std::runtime_error("streaming is not enabled"); }
        ddb::LockGuard<ddb::Mutex> LockGuard(&subscriberMutex_);
        string topic = host + "/" + std::to_string(port) + "/" + tableName + "/" + actionName;
        if (topicThread_.find(topic) == topicThread_.end()) { throw std::runtime_error("subscription " + topic + " not exists"); }
        {
            TRY
            ddb::SmartPointer<py::gil_scoped_release> pgilRelease;
            if(PyGILState_Check() == 1)
                pgilRelease = new py::gil_scoped_release;
            subscriber_->unsubscribe(host, port, tableName, actionName);
            CATCH_EXCEPTION("<Exception> in unsubscribe: ")
        }
        vector<ddb::ThreadSP> &threads = topicThread_[topic];
        for(auto thread : threads){
            if(thread->isRunning()) {
                gcThread_.push_back(thread);
                auto it = std::remove_if(gcThread_.begin(), gcThread_.end(), [](const ddb::ThreadSP& th) {
                    return th->isComplete();
                });
                gcThread_.erase(it, gcThread_.end());
            }
        }
        topicThread_.erase(topic);
    }

    // FIXME: not thread safe
    py::list getSubscriptionTopics() {
        ddb::LockGuard<ddb::Mutex> LockGuard(&subscriberMutex_);
        py::list topics;
        for (auto &it : topicThread_) { topics.append(it.first); }
        return topics;
    }

    py::object hashBucket(const py::object& obj, int nBucket) {
        auto c = Converter::toDolphinDB(obj);
        const static auto errMsg = "Key must be integer, date/time, or string.";
        auto dt = c->getType();
        auto cat = ddb::Util::getCategory(dt);
        if (cat != ddb::INTEGRAL && cat != ddb::TEMPORAL && dt != ddb::DT_STRING) {
            throw std::runtime_error(errMsg);
        }

        if(c->isVector()) {
            int n = c->size();
            py::array h(py::dtype("int32"), n, {});
            c->getHash(0, n, nBucket, (int*)h.mutable_data());
            return h;
        } else {
            int h = c->getHash(nBucket);
            return py::int_(h);
        }
    }

    ~SessionImpl() {
        vector<std::string> topics;
        for (auto &it : topicThread_) {
            topics.emplace_back(it.first);
        }
        for (auto &it : topics) {
            vector<std::string> args = ddb::Util::split(it, '/');
            try {
                unsubscribe(args[0], std::stoi(args[1]), args[2], args[3]);
            }
            catch (...) { }
        }
    }

    ddb::DBConnection& getConnection() {
        return dbConnection_;
    }
    void printPerformance(){
        LOG_INFO(ddb::RecordTime::printAllTime());
    }

    std::shared_ptr<dolphindb::Logger> getMsgLogger() { return dbConnection_.getMsgLogger(); }

private:
    using policy = void (*)(ddb::VectorSP);

private:
    static bool isSigint_;
    static bool enableJobCancellation_;
    static sighandler_t sighandler_;
    static ddb::Mutex sigintMutex_;
    static ddb::Mutex mapMutex_;
    static std::map<SessionImpl*, int> runningMap_;
    friend void signal_handler_fun(int signum);

private:
    //static inline void SET_NPNAN(void *p, size_t len = 1) { std::fill((uint64_t *)p, ((uint64_t *)p) + len, 9221120237041090560LL); }
    //static inline void SET_DDBNAN(void *p, size_t len = 1) { std::fill((double *)p, ((double *)p) + len, ddb::DBL_NMIN); }
    //static inline bool IS_NPNAN(void *p) { return *(uint64_t *)p == 9221120237041090560LL; }


private:
    std::string host_;
    int port_;
    std::string userId_;
    std::string password_;
    bool encrypted_;
    ddb::DBConnection dbConnection_;
    policy nullValuePolicy_;

    ddb::SmartPointer<ddb::ThreadedClient> subscriber_;
    ddb::SmartPointer<ddb::ThreadPooledClient> subscriberPool_;
    std::unordered_map<string, std::vector<ddb::ThreadSP>> topicThread_;
    ddb::Mutex subscriberMutex_;
    std::vector<ddb::ThreadSP> gcThread_;
    int keepAliveTime_;
};

bool SessionImpl::isSigint_ = false;
bool SessionImpl::enableJobCancellation_ = false;
sighandler_t SessionImpl::sighandler_ = nullptr;
ddb::Mutex SessionImpl::sigintMutex_ = ddb::Mutex();
ddb::Mutex SessionImpl::mapMutex_ = ddb::Mutex();
std::map<SessionImpl*, int> SessionImpl::runningMap_ = {};

void signal_handler_fun(int signum) {
    ddb::LockGuard<ddb::Mutex> LockGuard(&SessionImpl::sigintMutex_);
    if(!SessionImpl::isSigint_) {
        SessionImpl::isSigint_ = true;

        for (auto &item : SessionImpl::runningMap_) {
            SessionImpl *session = new SessionImpl(false, false, 7200, false, false, ddb::PROTOCOL_DDB);
            session->connect(item.first->host_, item.first->port_, item.first->userId_, item.first->password_);
            string id = item.first->getSessionId();
            ddb::ConstantSP jobs = session->runcpp("string(exec rootJobId from getConsoleJobs() where sessionId = "+ id + ")");
            if(jobs->isVector() && jobs->size() > 0 ) {
                vector<ddb::ConstantSP> args;
                args.emplace_back(jobs);
                ddb::ConstantSP res = session->runcpp("cancelConsoleJob", args);
            }
            delete session;
        }


        // signal(SIGINT, SessionImpl::sighandler_);
        // SessionImpl::sighandler_ = nullptr;

    }
    
}

ddb::SmartPointer<ddb::StreamDeserializer> StreamDeserializer::get(){
    if(sym2tableName_.empty())
        return nullptr;
    if(streamDeserializer_.isNull()){
        if(sessionImpl_==nullptr)
            streamDeserializer_ = new ddb::StreamDeserializer(sym2tableName_);
        else
            streamDeserializer_ = new ddb::StreamDeserializer(sym2tableName_,&sessionImpl_->getConnection());
    }
    return streamDeserializer_;
}

class AutoFitTableAppender{
public:
    AutoFitTableAppender(const std::string dbUrl, const std::string tableName, SessionImpl & session)
    : autoFitTableAppender_(dbUrl,tableName,session.getConnection()){}
    int append(py::object table){
        if (!CHECK_INS(table, pd_dataframe_))
            throw std::runtime_error(std::string("table must be a DataFrame!"));
        int insertRows;
        vector<ddb::Type> colTypes = autoFitTableAppender_.getColTypes();
        vector<std::string> colNames = autoFitTableAppender_.getColNames();
        ddb::TableChecker checker;
        for(int i=0;i<colTypes.size();++i){
            checker[colNames[i]] = colTypes[i];
        }
        TRY
            insertRows = autoFitTableAppender_.append(Converter::toDolphinDB_Table_fromDataFrame(table, checker));
        CATCH_EXCEPTION("<Exception> in append: ")
        return insertRows;
    }
private:
    ddb::AutoFitTableAppender autoFitTableAppender_;
};

class AutoFitTableUpsert{
public:
    AutoFitTableUpsert(const std::string dbUrl, const std::string tableName, SessionImpl & session,
                bool ignoreNull = false, const py::list& keyColNames = py::list(0), const py::list& sortColumns = py::list(0))
            : autoFitTableUpsert_(dbUrl,tableName,session.getConnection(),ignoreNull,
                        pylist2Stringvector(keyColNames).get(),
                        pylist2Stringvector(sortColumns).get()){}
    int upsert(py::object table){
        if (!CHECK_INS(table, pd_dataframe_))
            throw std::runtime_error(std::string("table must be a DataFrame!"));
        int insertRows = 0;
        vector<ddb::Type> colTypes = autoFitTableUpsert_.getColTypes();
        vector<std::string> colNames = autoFitTableUpsert_.getColNames();
        ddb::TableChecker checker;
        for(int i=0;i<colTypes.size();++i){
            checker[colNames[i]] = colTypes[i];
        }
        TRY
            insertRows = autoFitTableUpsert_.upsert(Converter::toDolphinDB_Table_fromDataFrame(table, checker));
        CATCH_EXCEPTION("<Exception> in append: ")
        return insertRows;
    }
private:
    static std::unique_ptr<vector<string>> pylist2Stringvector(py::list pylist){
        std::unique_ptr<vector<string>> psites(new vector<string>);
        for (py::handle o : pylist) { psites->emplace_back(py::cast<std::string>(o)); }
        return psites;
    }
    ddb::AutoFitTableUpsert autoFitTableUpsert_;
};

class BatchTableWriter{
public:
    BatchTableWriter(const std::string& hostName, int port, const std::string& userId, const std::string& password, bool acquireLock=true)
    : writer_(hostName, port, userId, password, acquireLock){}
    ~BatchTableWriter(){}
    void addTable(const string& dbName="", const string& tableName="", bool partitioned=true){
        TRY
            writer_.addTable(dbName, tableName, partitioned);
        CATCH_EXCEPTION("<Exception> in addTable: ")
    }
    py::object getStatus(const string& dbName, const string& tableName=""){
        TRY
            std::tuple<int,bool,bool> tem = writer_.getStatus(dbName, tableName);
            py::list ret;
            ret.append(py::int_(std::get<0>(tem)));
            ret.append(py::bool_(std::get<1>(tem)));
            ret.append(py::bool_(std::get<2>(tem)));
            return ret;
        CATCH_EXCEPTION("<Exception> in getStatus: ")
    }
    py::object getAllStatus(){
        TRY
            ddb::ConstantSP ret = writer_.getAllStatus();
            return Converter::toPython_Old(ret);
        CATCH_EXCEPTION("<Exception> in getAllStatus: ")
    }
    py::object getUnwrittenData(const string& dbName, const string& tableName=""){
        TRY
            ddb::ConstantSP ret = writer_.getUnwrittenData(dbName, tableName);
            return Converter::toPython_Old(ret);
        CATCH_EXCEPTION("<Exception> in getUnwrittenData: ")
    }
    void removeTable(const string& dbName, const string& tableName=""){
        TRY
            writer_.removeTable(dbName, tableName);
        CATCH_EXCEPTION("<Exception> in removeTable: ")
    }
    void insert(const string& dbName, const string& tableName, const py::args &args){
        ddb::SmartPointer<vector<ddb::ConstantSP>> ddbArgs(new std::vector<ddb::ConstantSP>());
        int size = args.size();
        TRY
            for (int i = 0; i < size; ++i){
                ddb::ConstantSP test = Converter::toDolphinDB(args[i]);
                ddbArgs->push_back(test);
            }
            writer_.insertRow(dbName, tableName, ddbArgs.get());
        CATCH_EXCEPTION("<Exception> in insert: ")
    }
private:
    ddb::BatchTableWriter writer_;
};

class MultithreadedTableWriter{
public:
    MultithreadedTableWriter(const std::string& host, int port, const std::string& userId, const std::string& password,
                                const string& dbPath, const string& tableName, bool useSSL, bool enableHighAvailability = false,
                                const py::list &highAvailabilitySites = py::list(0),
                                int batchSize = 1, float throttle = 0.01f,int threadCount = 1, const string& partitionCol ="",
                                const py::list &compressMethods = py::list(0),
                                const string &mode = "", const py::list &modeOption = py::list(0), bool reconnect = true){
        TRY
            writer_ = new ddb::MultithreadedTableWriter(host, port, userId, password,
                                dbPath, tableName, useSSL,
                                enableHighAvailability, pylist2Stringvector(highAvailabilitySites).get(),
                                batchSize,throttle,threadCount,partitionCol,
                                pylist2Compressvector(compressMethods).get(),
                                pymode2Mtwmode(mode), pymodelist2Vector(modeOption).get(), reconnect);
        CATCH_EXCEPTION("<Exception> in MultithreadedTableWriter: ")
    }
    ~MultithreadedTableWriter(){}
    void waitForThreadCompletion(){
        writer_->waitForThreadCompletion();
    }
    py::object getStatus(){
        py::dict pystatus;
        ddb::MultithreadedTableWriter::Status status;
        writer_->getPytoDdb()->getStatus(status);
        pystatus["isExiting"]=status.isExiting;
        pystatus["errorCode"]=status.errorCode;
        pystatus["errorInfo"]=status.errorInfo;
        pystatus["sentRows"]=status.sentRows;
        pystatus["unsentRows"]=status.unsentRows;
        pystatus["sendFailedRows"]=status.sendFailedRows;
        {
            py::list list(status.threadStatus.size());
            int index=0;
            for(auto thread : status.threadStatus){
                py::dict pythreadstatus;
                pythreadstatus["threadId"] = thread.threadId;
                pythreadstatus["sentRows"] = thread.sentRows;
                pythreadstatus["unsentRows"] = thread.unsentRows;
                pythreadstatus["sendFailedRows"] = thread.sendFailedRows;
                list[index++] = pythreadstatus;
            }
            pystatus["threadStatus"]=list;
        }
        return pystatus;
    }
    py::object getUnwrittenData(){
        TRY
            std::vector<std::vector<ddb::ConstantSP>*> unwriteDdbVecs;
            std::vector<std::vector<py::object>*> unwritePyVecs;
            writer_->getPytoDdb()->getUnwrittenData(unwritePyVecs, unwriteDdbVecs);
            py::list pyunwrite(unwriteDdbVecs.size() + unwritePyVecs.size());
            int rowindex, colindex;
            rowindex = 0;
            for(auto &dbvec : unwriteDdbVecs){
                py::list pyrow(dbvec->size());
                colindex=0;
                for(auto &dvone : *dbvec){
                    pyrow[colindex++] = Converter::toPython_Old(dvone);
                }
                pyunwrite[rowindex++] = pyrow;
                delete dbvec;
            }
            for(auto &pyvec : unwritePyVecs){
                py::list pyrow(pyvec->size());
                colindex = 0;
                for(auto &pyone : *pyvec){
                    pyrow[colindex++] =  pyone;
                }
                pyunwrite[rowindex++] = pyrow;
                delete pyvec;
            }
            return pyunwrite;
        CATCH_EXCEPTION("<Exception> in getUnwrittenData: ")
    }
    py::dict insert(const py::args &args){
        if(writer_->isExit()){
            throw std::runtime_error(std::string("<Exception> in insert: thread is exiting."));
        }
        TRY
            py::dict errorinfo;
            int size = args.size();
            if(size != writer_->getColSize()){
                errorinfo["errorCode"] = ddb::ErrorCodeInfo::formatApiCode(ddb::ErrorCodeInfo::EC_InvalidParameter);
                errorinfo["errorInfo"] = std::string("Column counts don't match ") + std::to_string(writer_->getColSize());
                return errorinfo;
            }
            std::vector<py::object> *pobjs=new std::vector<py::object>;
            pobjs->reserve(size);
            for (int i = 0; i < size; ++i){
                pobjs->push_back(py::reinterpret_borrow<py::object>(args[i]));
            }
            if(writer_->getPytoDdb()->add(pobjs) == false){
                delete pobjs;
                if(writer_->isExit()){
                    throw std::runtime_error(std::string("<Exception> in insert: thread is exiting."));
                }
                errorinfo["errorCode"] = ddb::ErrorCodeInfo::formatApiCode(ddb::ErrorCodeInfo::EC_InvalidObject);
                errorinfo["errorInfo"] = std::string("Invalid object");
                return errorinfo;
            }
            errorinfo["errorCode"] = "";
            //ddb::g_OutputDestroyMsg=false;
            return errorinfo;
        CATCH_EXCEPTION("<Exception> in insert: ")
    }
    py::dict insertUnwrittenData(const py::list &records){
        if(writer_->isExit()){
            throw std::runtime_error(std::string("<Exception> in insert: thread is exiting."));
        }
        TRY
            py::dict errorinfo;
            //std::vector<std::vector<ddb::ConstantSP>*> vectorOfVector(records.size());
            for(pybind11::size_t row = 0; row < records.size(); row++){
                py::list pylist = records[row];
                int size = pylist.size();
                if(size != writer_->getColSize()){
                    errorinfo["errorCode"] = ddb::ErrorCodeInfo::formatApiCode(ddb::ErrorCodeInfo::EC_InvalidObject);
                    errorinfo["errorInfo"] = std::string("arg size mismatch col size ") + std::to_string(writer_->getColSize());
                    return errorinfo;
                }
                std::vector<py::object> *pobjs=new std::vector<py::object>;
                pobjs->reserve(size);
                for (int i = 0; i < size; ++i){
                    pobjs->push_back(py::reinterpret_borrow<py::object>(pylist[i]));
                }
                if(writer_->getPytoDdb()->add(pobjs) == false){
                    delete pobjs;
                    if(writer_->isExit()){
                        throw std::runtime_error(std::string("<Exception> in insert: thread is exiting."));
                    }
                    errorinfo["errorCode"] = ddb::ErrorCodeInfo::formatApiCode(ddb::ErrorCodeInfo::EC_InvalidObject);
                    errorinfo["errorInfo"] = std::string("Invalid object");
                    return errorinfo;
                }
            }
            errorinfo["errorCode"] = "";
            return errorinfo;
        CATCH_EXCEPTION("<Exception> in insertUnwrittenData: ")
    }
private:
    static ddb::MultithreadedTableWriter::Mode pymode2Mtwmode(string mode){
        if(mode.empty())
            return ddb::MultithreadedTableWriter::M_Append;
        transform(mode.begin(), mode.end(), mode.begin(), ::toupper);
        if(mode=="APPEND")
            return ddb::MultithreadedTableWriter::M_Append;
        else if(mode=="UPSERT")
            return ddb::MultithreadedTableWriter::M_Upsert;
        else{
            throw std::runtime_error(std::string("Unsupported mtw mode ") + mode);
        }
    }
    static std::unique_ptr<vector<string>> pymodelist2Vector(py::list pylist){
        std::unique_ptr<vector<string>> pvector(new vector<string>);
        for (auto &one : pylist) {
            pvector->push_back(one.cast<std::string>());
        }
        return pvector;
    }
    static std::unique_ptr<vector<string>> pylist2Stringvector(py::list pylist){
        std::unique_ptr<vector<string>> psites(new vector<string>);
        for (py::handle o : pylist) { psites->emplace_back(py::cast<std::string>(o)); }
        return psites;
    }
    static std::unique_ptr<vector<ddb::COMPRESS_METHOD>> pylist2Compressvector(py::list pylist){
        std::unique_ptr<vector<ddb::COMPRESS_METHOD>> pcompresstypes(new vector<ddb::COMPRESS_METHOD>);
        for (py::handle o : pylist) {
            std::string typeStr = py::cast<std::string>(o);
            transform(typeStr.begin(), typeStr.end(), typeStr.begin(), ::toupper);
            ddb::COMPRESS_METHOD type;
            if(typeStr == "LZ4"){
                type=ddb::COMPRESS_LZ4;
            }else if(typeStr == "DELTA"){
                type=ddb::COMPRESS_DELTA;
            }else{
                throw std::runtime_error(std::string("Unsupported compression method ") + typeStr);
            }
            pcompresstypes->emplace_back(type);
        }
        return pcompresstypes;
    }
    ddb::SmartPointer<ddb::MultithreadedTableWriter> writer_;
};


class PyEventScheme {
public:
    PyEventScheme(py::object pyScheme) {
        pyScheme_ = pyScheme;
        name_ = py::cast<std::string>(pyScheme.attr("_event_name"));
        std::vector<std::string> attrKeys;
        std::vector<ddb::DATA_TYPE> attrTypes;
        std::vector<ddb::DATA_FORM> attrForms;
        std::vector<int> attrExtraParams;
        py::dict typeCache = py::reinterpret_borrow<py::dict>(pyScheme.attr("_type_cache"));
        for (auto &item : typeCache) {
            attrKeys.push_back(py::cast<std::string>(item.first));
            py::list tmpList = py::reinterpret_borrow<py::list>(item.second);
            attrTypes.push_back((ddb::DATA_TYPE)(py::cast<int>(tmpList[0])));
            attrForms.push_back((ddb::DATA_FORM)(py::cast<int>(tmpList[1])));
            attrExtraParams.push_back(py::cast<int>(tmpList[2]));
        }
        scheme_ = ddb::EventSchema{name_, attrKeys, attrTypes, attrForms, attrExtraParams};
    }
    py::object createEvent(const std::vector<ddb::ConstantSP> &attrs) {
        py::object event = pyScheme_();
        int len = scheme_.fieldNames_.size();
        for (int i = 0; i < len; ++i) {
            py::setattr(event, scheme_.fieldNames_[i].c_str(), Converter::toPython_Old(attrs[i]));
        }
        return std::move(event);
    }
    ddb::EventSchema scheme() {
        return scheme_;
    }
    py::object pyScheme() {
        return pyScheme_;
    }
    std::string name() {
        return name_;
    }
private:
    std::string         name_;
    py::object          pyScheme_;
    ddb::EventSchema    scheme_;
};


class PyEventSender {
public:
    PyEventSender(
        SessionImpl &session,
        const std::string& tableName,
        const py::list &eventSchemes,
        const std::vector<std::string>& eventTimeKeys,
        const std::vector<std::string>& commonKeys): isReleased_(false) {
        std::vector<ddb::EventSchema> schemes;
        for (auto &scheme : eventSchemes) {
            ddb::SmartPointer<PyEventScheme> pyScheme = new PyEventScheme(py::reinterpret_borrow<py::object>(scheme));
            schemeMap_[pyScheme->name()] = pyScheme;
            schemes.push_back(pyScheme->scheme());
        }
        sender_ = new ddb::EventSender(session.getConnection(), tableName, schemes, eventTimeKeys, commonKeys);
    }
    ~PyEventSender() {
        release();
    }
    void release() {
        if (!isReleased_) {
            sender_.clear();
            isReleased_ = true;
        }
    }
    void sendEvent(const py::object& event) {
        std::string eventType = py::cast<std::string>(event.attr("_event_name"));
        if (schemeMap_.find(eventType) == schemeMap_.end()) {
            throw std::runtime_error("Unknown eventType " + eventType);
        }
        ddb::EventSchema scheme = schemeMap_[eventType]->scheme();
        std::vector<ddb::ConstantSP> attributes;
        int len = scheme.fieldNames_.size();
        attributes.reserve(len);

        for (int i = 0; i < len; ++i) {
            py::object obj = py::reinterpret_borrow<py::object>(event.attr(py::cast(scheme.fieldNames_[i])));
            switch (scheme.fieldForms_[i])
            {
            case ddb::DATA_FORM::DF_SCALAR: {
                attributes.push_back(Converter::toDolphinDB_Scalar(
                    obj, createType(scheme.fieldTypes_[i], scheme.fieldExtraParams_[i])
                ));
                break;
            }
            case ddb::DATA_FORM::DF_VECTOR: {
                attributes.push_back(Converter::toDolphinDB_Vector(
                    obj, createType(scheme.fieldTypes_[i], scheme.fieldExtraParams_[i]),
                    ddb::CHILD_VECTOR_OPTION::ARRAY_VECTOR
                ));
                break;
            }
            default:
                throw std::runtime_error("Invalid data form for the field " + scheme.fieldNames_[i] + " of event " + scheme.eventType_ + ".");
                break;
            }
        }
        sender_->sendEvent(eventType, attributes);
    }
private:
    std::map<std::string, ddb::SmartPointer<PyEventScheme>> schemeMap_;
    ddb::SmartPointer<ddb::EventSender> sender_;
    bool isReleased_;
};


class PyStreamingClient {
public:
    PyStreamingClient() {}
    ~PyStreamingClient() {}
public:
    py::list getSubscriptionTopics() {
        ddb::LockGuard<ddb::Mutex> LockGuard(&mutex_);
        py::list topics;
        for (auto &it : topicThread_) { topics.append(it.first); }
        return topics;
    }
protected:
    void unsubscribeImpl(
        const std::string   &host,
        const int           &port,
        const std::string   &tableName,
        const std::string   &actionName,
        std::function<void(std::string, int, std::string, std::string)> unsubscribe) {
        ddb::LockGuard<ddb::Mutex> lockGuard(&mutex_);
        std::string topic = concatTopic(host, port, tableName, actionName);
        if (!hasTopic(topic)) { throw std::runtime_error("subscription " + topic + " not exists"); }
        {
            TRY
            ddb::SmartPointer<py::gil_scoped_release> pgilRelease;
            if(PyGILState_Check() == 1)
                pgilRelease = new py::gil_scoped_release;
            unsubscribe(host, port, tableName, actionName);
            CATCH_EXCEPTION("<Exception> in unsubscribe: ")
        }
        vector<ddb::ThreadSP> &threads = topicThread_[topic];
        for(auto thread : threads){
            if(thread->isRunning()) {
                gcThread_.push_back(thread);
                auto it = std::remove_if(gcThread_.begin(), gcThread_.end(), [](const ddb::ThreadSP& th) {
                    return th->isComplete();
                });
                gcThread_.erase(it, gcThread_.end());
            }
        }
        topicThread_.erase(topic);
    }
    void clearAllSubscribeImpl(std::function<void(std::string, int, std::string, std::string)> unsubscribe) {
        vector<std::string> topics;
        for (auto &it : topicThread_) {
            topics.emplace_back(it.first);
        }
        for (auto &it : topics) {
            vector<std::string> args = ddb::Util::split(it, '/');
            try {
                unsubscribeImpl(args[0], std::stoi(args[1]), args[2], args[3], unsubscribe);
            }
            catch (...) { }
        }
    }
    inline std::string concatTopic(
        const std::string   &host,
        const int           &port,
        const std::string   &tableName,
        const std::string   &actionName) {
        return host + "/" + std::to_string(port) + "/" + tableName + "/" + actionName;
    }
    inline bool hasTopic(const std::string &topic) {
        return topicThread_.find(topic) != topicThread_.end();
    }
protected:
    ddb::Mutex mutex_;
    std::unordered_map<std::string, std::vector<ddb::ThreadSP>> topicThread_;
    std::vector<ddb::ThreadSP> gcThread_;
};


class PyEventClient : public PyStreamingClient {
public:
    PyEventClient(
        const py::list &eventSchemes,
        const std::vector<std::string> &eventTimeKeys,
        const std::vector<std::string> &commonKeys) : PyStreamingClient() {
        std::vector<ddb::EventSchema> schemes;
        for (const auto &scheme : eventSchemes) {
            ddb::SmartPointer<PyEventScheme> pyScheme = new PyEventScheme(py::reinterpret_borrow<py::object>(scheme));
            schemeMap_[pyScheme->name()] = pyScheme;
            schemes.push_back(pyScheme->scheme());
        }
        client_ = new ddb::EventClient(schemes, eventTimeKeys, commonKeys);
    }
    ~PyEventClient() {
        clearAllSubscribeImpl([this](std::string host, int port, std::string tableName, std::string actionName) {
            this->client_->unsubscribe(host, port, tableName, actionName);
        });
    }
    void subscribe(
        const std::string &host,
        const int &port,
        py::object handler,
        const std::string &tableName,
        const std::string &actionName,
        const long long &offset,
        const bool &resub,
        const std::string &userName,
        const std::string &passWord) {
        ddb::LockGuard<ddb::Mutex> lockGuard(&mutex_);
        std::string topic = concatTopic(host, port, tableName, actionName);
        if (hasTopic(topic)) { throw std::runtime_error("subscription " + topic + " already exists"); }
        ddb::EventMessageHandler ddbHanlder = [handler, this](const std::string &name, const std::vector<ddb::ConstantSP> &attributes) {
            // handle GIL
            py::gil_scoped_acquire acquire;
            ddb::SmartPointer<PyEventScheme> pyScheme = this->schemeMap_[name];
            handler(pyScheme->createEvent(attributes));
        };
        TRY
        std::vector<ddb::ThreadSP> threads;
        threads.push_back(client_->subscribe(host, port, ddbHanlder, tableName, actionName, offset, resub, userName, passWord));
        topicThread_[topic] = threads;
        CATCH_EXCEPTION("<Exception> in subscribe: ")
    }
    void unsubscribe(
        const std::string &host,
        const int &port,
        const std::string &tableName,
        const std::string &actionName) {
        unsubscribeImpl(host, port, tableName, actionName, [this](std::string host_, int port_, std::string tableName_, std::string actionName_) {
            this->client_->unsubscribe(host_, port_, tableName_, actionName_);
        });
    }
private:
    std::map<std::string, ddb::SmartPointer<PyEventScheme>> schemeMap_;
    ddb::SmartPointer<ddb::EventClient> client_;
};


std::shared_ptr<dolphindb::Logger> dolphindb::DLogger::defaultLogger_ = std::make_shared<dolphindb::Logger>();


PYBIND11_MODULE(_dolphindbcpp, m) {
    m.doc() = R"pbdoc(_dolphindbcpp: this is a C++ boosted DolphinDB Python API)pbdoc";
    m.def("init", &ddbinit);

    py::enum_<dolphindb::Logger::Level>(m, "Level")
        .value("DEBUG", dolphindb::Logger::LevelDebug)
        .value("INFO", dolphindb::Logger::LevelInfo)
        .value("WARNING", dolphindb::Logger::LevelWarn)
        .value("ERROR", dolphindb::Logger::LevelError)
        .export_values();

    py::class_<dolphindb::LogMessage>(m, "LogMessage")
        .def(py::init<dolphindb::Logger::Level, const std::string &>())
        .def_readwrite("level", &dolphindb::LogMessage::level_)
        .def_readwrite("log", &dolphindb::LogMessage::msg_);

    py::class_<dolphindb::custom_sink, std::shared_ptr<dolphindb::custom_sink>, dolphindb::py_custom_sink>(m, "Sink")
        .def(py::init<const std::string &>())
        .def("handle", &dolphindb::custom_sink::handle)
        .def("flush", &dolphindb::custom_sink::pyflush)
        .def("__str__", &dolphindb::custom_sink::print)
        .def_property_readonly("name", &dolphindb::custom_sink::get_identifier);

    py::class_<dolphindb::Logger, std::shared_ptr<dolphindb::Logger>>(m, "Logger")
        .def("enable_stdout_sink", [](dolphindb::Logger& self) {
            self.setStdoutFlag(true);
        })
        .def("disable_stdout_sink", [](dolphindb::Logger& self) {
            self.setStdoutFlag(false);
        })
        .def("enable_file_sink", &dolphindb::Logger::setFilePath)
        .def("disable_file_sink", [](dolphindb::Logger& self) {
            self.setFilePath("");
        })
        .def("add_sink", &dolphindb::Logger::addSink)
        .def("remove_sink", &dolphindb::Logger::removeSink)
        .def("list_sinks", &dolphindb::Logger::listSinks)
        .def_property(
            "min_level",
            [](dolphindb::Logger& self) {
                return self.GetMinLevel();
            },
            [](dolphindb::Logger& self, dolphindb::Logger::Level level) {
                self.SetMinLevel(level);
            }
        );

    m.add_object("default_logger", py::cast(dolphindb::DLogger::defaultLogger_));

    py::class_<DBConnectionPoolImpl>(m, "dbConnectionPoolImpl")
        .def(py::init<const std::string &,int,int,const std::string &,const std::string &,bool, bool, bool, bool, bool, int, bool, int, int>())
        .def("run", &DBConnectionPoolImpl::run,
            py::arg("script"),
            py::arg("taskId"),
            py::kw_only(),
            py::arg("clearMemory") = py::none(),
            py::arg("pickleTableToList") = py::none(),
            py::arg("priority") = py::none(),
            py::arg("parallelism") = py::none(),
            py::arg("disableDecimal") = py::none()
        )
        .def("isFinished",(bool(DBConnectionPoolImpl::*)(int)) & DBConnectionPoolImpl::isFinished)
        .def("getData",(py::object(DBConnectionPoolImpl::*)(int)) & DBConnectionPoolImpl::getData)
        .def("shutDown",&DBConnectionPoolImpl::shutDown)
        .def("getSessionId",&DBConnectionPoolImpl::getSessionId);

    py::class_<SessionImpl>(m, "sessionimpl")
        .def(py::init<bool,bool,int,bool,bool,int,bool, int>())
        .def("connect", &SessionImpl::connect)
        .def("login", &SessionImpl::login)
        .def("getInitScript", &SessionImpl::getInitScript)
        .def("setInitScript", &SessionImpl::setInitScript)
        .def("close", &SessionImpl::close)
        .def("getSessionId", &SessionImpl::getSessionId)
        .def("run", &SessionImpl::run,
            py::arg("script"),
            py::kw_only(),
            py::arg("clearMemory") = py::none(),
            py::arg("pickleTableToList") = py::none(),
            py::arg("priority") = py::none(),
            py::arg("parallelism") = py::none(),
            py::arg("disableDecimal") = py::none(),
            py::arg("withTableSchema") = py::none()
        )
        .def("runBlock",&SessionImpl::runBlock,
            py::arg("script"),
            py::kw_only(),
            py::arg("clearMemory") = py::none(),
            py::arg("fetchSize") = py::none(),
            py::arg("priority") = py::none(),
            py::arg("parallelism") = py::none()
        )
        .def("upload", &SessionImpl::upload)
        .def("nullValueToZero", &SessionImpl::nullValueToZero)
        .def("nullValueToNan", &SessionImpl::nullValueToNan)
        .def("enableStreaming", &SessionImpl::enableStreaming)
        // .def_static("disableJobCancellation", &SessionImpl::disableJobCancellation)
        .def_static("enableJobCancellation", &SessionImpl::enableJobCancellation)
        .def_static("setTimeout", &SessionImpl::setTimeout)
        .def("subscribe", &SessionImpl::subscribe)
        .def("subscribeBatch", &SessionImpl::subscribeBatch)
        .def("unsubscribe", &SessionImpl::unsubscribe)
        .def("hashBucket", &SessionImpl::hashBucket)
        .def("getSubscriptionTopics", &SessionImpl::getSubscriptionTopics)
        .def("printPerformance", &SessionImpl::printPerformance)
        .def("loadPickleFile", &SessionImpl::loadPickleFile)
        .def_property_readonly("msg_logger", &SessionImpl::getMsgLogger);
    
    py::class_<StreamDeserializer>(m, "streamDeserializer")
        .def(py::init<py::dict&>())
        .def("setSession", &StreamDeserializer::setSession);

    py::class_<BlockReader>(m, "blockReader")
        .def(py::init<ddb::BlockReaderSP>())
        .def("read", (py::object(BlockReader::*)()) &BlockReader::read)
        .def("skipAll", &BlockReader::skipAll)
        .def("hasNext", (py::bool_(BlockReader::*)())&BlockReader::hasNext);

    py::class_<PartitionedTableAppender>(m, "partitionedTableAppender")
        .def(py::init<const std::string &,const std::string &,const std::string &,DBConnectionPoolImpl&>())
        .def("append", &PartitionedTableAppender::append);

    py::class_<AutoFitTableAppender>(m, "autoFitTableAppender")
        .def(py::init<const std::string &, const std::string&, SessionImpl&>())
        .def("append", &AutoFitTableAppender::append);
    
    py::class_<AutoFitTableUpsert>(m, "autoFitTableUpsert")
        .def(py::init<const std::string &, const std::string&, SessionImpl&,bool,const py::list &,const py::list &>())
        .def("upsert", &AutoFitTableUpsert::upsert);

    py::class_<BatchTableWriter>(m, "batchTableWriter")
        .def(py::init<const std::string &,int,const std::string &,const std::string &,bool>())
        .def("addTable", &BatchTableWriter::addTable)
        .def("getStatus", &BatchTableWriter::getStatus)
        .def("getAllStatus", &BatchTableWriter::getAllStatus)
        .def("getUnwrittenData", &BatchTableWriter::getUnwrittenData)
        .def("removeTable", &BatchTableWriter::removeTable)
        .def("insert", &BatchTableWriter::insert);

    py::class_<MultithreadedTableWriter>(m, "multithreadedTableWriter")
        .def(py::init<const std::string &, int, const std::string &, const std::string &,const std::string &, const std::string &,
                bool, bool,const py::list &,int, float,int, const std::string&,const py::list &,
                const string &, const py::list &, bool>())
        .def("getStatus", &MultithreadedTableWriter::getStatus)
        .def("getUnwrittenData", &MultithreadedTableWriter::getUnwrittenData)
        .def("insert", &MultithreadedTableWriter::insert)
        .def("insertUnwrittenData", &MultithreadedTableWriter::insertUnwrittenData)
        .def("waitForThreadCompletion", &MultithreadedTableWriter::waitForThreadCompletion);

    py::class_<ddb::InputStreamWrapper>(m, "InputStreamWrapper")
        .def(py::init<>())
        .def_property_readonly("closed", &ddb::InputStreamWrapper::closed)
        .def("read", &ddb::InputStreamWrapper::read);
    
    py::class_<ddb::TableChecker>(m, "TableChecker")
        .def(py::init<const py::dict &>());

    py::class_<PyEventSender>(m, "EventSender")
        .def(py::init<SessionImpl&, const std::string&, const py::list&, const std::vector<std::string>&, const std::vector<std::string>&>())
        .def("sendEvent", &PyEventSender::sendEvent);

    py::class_<PyEventClient>(m, "EventClient")
        .def(py::init<const py::list&, const std::vector<std::string>&, const std::vector<std::string>&>())
        .def("subscribe", &PyEventClient::subscribe)
        .def("unsubscribe", &PyEventClient::unsubscribe)
        .def("getSubscriptionTopics", &PyEventClient::getSubscriptionTopics);

#ifdef VERSION_INFO
    m.attr("__version__") = VERSION_INFO;
#else
    m.attr("__version__") = "dev";
#endif

    pybind_dolphindb::Init_Module_Exception(m);
    pybind_dolphindb::Init_Module_IO(m);
}
