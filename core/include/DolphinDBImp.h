#pragma once

#include "ConstantImp.h"
#include <string>
#include "RequestArgument.h"
#include "pybind11/pybind11.h"
#include "PytoDdbRowPool.h"
#include "TypeConverter.h"

namespace py = pybind11;

namespace dolphindb {

std::string formatSeqNoSuffix(const std::string& clientId, long seqNum);

class EXPORT_DECL DdbInit {
public:
    DdbInit() {
    #ifdef WINDOWS
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 1), &wsaData) != 0) {
            throw IOException("Failed to initialize the windows socket.");
        }
    #endif
        initFormatters();
    }
};

class EXPORT_DECL DBConnectionImpl {
public:
    DBConnectionImpl(bool sslEnable = false, bool asynTask = false, bool compress = false, PARSER_TYPE parser = PARSER_TYPE::PARSER_DOLPHINDB, bool isReverseStreaming = false, int sqlStd = 0);
    ~DBConnectionImpl();
    bool connect(const string& hostName, int port, const string& userId = "", const string& password = "",bool sslEnable = false, bool asynTask = false, bool compress= false, PARSER_TYPE parser = PARSER_TYPE::PARSER_DOLPHINDB, int netTimeout = -1, int timeout = -1);
    void login(const string& userId, const string& password, bool enableEncryption);
    ConstantSP run(const string& script, int priority = 4, int parallelism = 64, int fetchSize = 0, bool clearMemory = false, long seqNum = 0);
    ConstantSP run(const string& funcName, vector<ConstantSP>& args, int priority = 4, int parallelism = 64, int fetchSize = 0, bool clearMemory = false, long seqNum = 0);
    ConstantSP upload(const string& name, const ConstantSP& obj);
    ConstantSP upload(vector<string>& names, vector<ConstantSP>& objs);
    void close();
    bool isConnected() { return isConnected_; }
    void getHostPort(string &host, int &port) {
        host = hostName_;
        port = port_;
    }
    void getUserPwd(string &user, string &pwd) {
        user = userId_;
        pwd = pwd_;
    }
    void setProtocol(PROTOCOL protocol) {
        protocol_ = protocol;
        if (protocol_ == PROTOCOL_ARROW) {
            if (!converter::PyObjs::cache_->has_arrow_) {
                throw RuntimeException("No module named 'pyarrow'");
            }
        }
    }
    void setShowOutput(bool flag) {
        msg_ = flag;
    }
    void setClientId(const std::string& clientId) {
        runClientId_ = clientId;
    }
    py::object runPy(
        const string& script, int priority = 4, int parallelism = 64,
        int fetchSize = 0, bool clearMemory = false,
        bool pickleTableToList = false, bool disableDecimal = false, bool withTableSchema = false,
        long seqNum = 0, const converter::ArrayVectorToColumns &arrayVectorToColumns = {});
    py::object runPy(
        const string& funcName, vector<ConstantSP>& args, int priority = 4, int parallelism = 64,
        int fetchSize = 0, bool clearMemory = false,
        bool pickleTableToList = false, bool disableDecimal = false, bool withTableSchema = false,
        long seqNum = 0, const converter::ArrayVectorToColumns &arrayVectorToColumns = {});
    py::object runPy(
        const string& funcName, vector<RequestArgument>& args, int priority = 4, int parallelism = 64,
        int fetchSize = 0, bool clearMemory = false, REQUEST_FORMAT requestFormat = REQUEST_FORMAT_AUTO,
        bool pickleTableToList = false, bool disableDecimal = false, bool withTableSchema = false,
        long seqNum = 0, const converter::ArrayVectorToColumns &arrayVectorToColumns = {});
    py::object uploadPy(vector<string>& names, vector<RequestArgument>& objs,
                        REQUEST_FORMAT requestFormat = REQUEST_FORMAT_AUTO);
    void setTimeout(int netTimeout, int timeout) {
        netTimeout_ = netTimeout;
        timeout_ = timeout;
    }
    const string getSessionId() const {
        return sessionId_;
    }
    PROTOCOL getProtocol() const {
        return protocol_;
    }
    DataInputStreamSP getDataInputStream(){return inputStream_;}
    std::shared_ptr<Logger> getMsgLogger() { return logger_; }
private:
    long generateRequestFlag(bool clearSessionMemory = false, REQUEST_FORMAT requestFormat = REQUEST_FORMAT_AUTO,
                             bool pickleTableToList = false, bool disableDecimal = false);
    ConstantSP run(const string& script, const string& scriptType, vector<ConstantSP>& args, int priority = 4, int parallelism = 64,int fetchSize = 0, bool clearMemory = false, long seqNum = 0);
    py::object runPy(
        const string& script, const string& scriptType, vector<ConstantSP>& args,
        int priority = 4, int parallelism = 64, int fetchSize = 0, bool clearMemory = false,
        bool pickleTableToList = false, bool disableDecimal = false, bool withTableSchema = false,
        long seqNum = 0, const converter::ArrayVectorToColumns &arrayVectorToColumns = {});
    py::object runPy(
        const string& script, const string& scriptType, vector<RequestArgument>& args,
        int priority = 4, int parallelism = 64, int fetchSize = 0, bool clearMemory = false,
        REQUEST_FORMAT requestFormat = REQUEST_FORMAT_AUTO,
        bool pickleTableToList = false, bool disableDecimal = false, bool withTableSchema = false,
        long seqNum = 0, const converter::ArrayVectorToColumns &arrayVectorToColumns = {});
    bool connect();
    void login();

private:
    SocketSP conn_;
    string sessionId_;
    string hostName_;
    int port_;
    string userId_;
    string pwd_;
    bool encrypted_;
    bool isConnected_;
    bool littleEndian_;
    bool sslEnable_;
    bool asynTask_;
    bool compress_;
    PARSER_TYPE parser_;
    PROTOCOL protocol_;
    bool msg_;
    static DdbInit ddbInit_;
    bool isReverseStreaming_;
    int sqlStd_;
    std::string runClientId_;
    int netTimeout_;
    int timeout_;
    DataInputStreamSP inputStream_;
    Mutex mutex_;
    std::shared_ptr<Logger> logger_;
};

}
