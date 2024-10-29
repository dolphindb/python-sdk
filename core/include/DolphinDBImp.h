#pragma once

#include "ConstantImp.h"
#include <string>
#include "pybind11/pybind11.h"
#include "DdbPythonUtil.h"

namespace py = pybind11;

namespace dolphindb {

class DdbInit {
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

class DBConnectionImpl {
public:
    DBConnectionImpl(bool sslEnable = false, bool asynTask = false, int keepAliveTime = 7200, bool compress = false, bool python = false, bool isReverseStreaming = false, int sqlStd = 0);
    ~DBConnectionImpl();
    bool connect(const string& hostName, int port, const string& userId = "", const string& password = "",bool sslEnable = false, bool asynTask = false, int keepAliveTime = -1, bool compress= false, bool python = false, int readTimeout = -1, int writeTimeout = -1);
    void login(const string& userId, const string& password, bool enableEncryption);
    ConstantSP run(const string& script, int priority = 4, int parallelism = 64, int fetchSize = 0, bool clearMemory = false);
    ConstantSP run(const string& funcName, vector<ConstantSP>& args, int priority = 4, int parallelism = 64, int fetchSize = 0, bool clearMemory = false);
    ConstantSP upload(const string& name, const ConstantSP& obj);
    ConstantSP upload(vector<string>& names, vector<ConstantSP>& objs);
    void close();
    bool isConnected() { return isConnected_; }
    void getHostPort(string &host, int &port) { host = hostName_; port = port_; }

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
    py::object runPy(
        const string& script, int priority = 4, int parallelism = 64,
        int fetchSize = 0, bool clearMemory = false,
        bool pickleTableToList = false, bool disableDecimal = false);
    py::object runPy(
        const string& funcName, vector<ConstantSP>& args, int priority = 4, int parallelism = 64,
        int fetchSize = 0, bool clearMemory = false,
        bool pickleTableToList = false, bool disableDecimal = false);
    void setkeepAliveTime(int keepAliveTime){
        if (keepAliveTime > 0)
            keepAliveTime_ = keepAliveTime;
    }
    void setTimeout(int readTimeout, int writeTimeout) {
        if (readTimeout > 0)
            readTimeout_ = readTimeout;
        if (writeTimeout > 0)
            writeTimeout_ = writeTimeout;
    }
    const string getSessionId() const {
        return sessionId_;
    }
    DataInputStreamSP getDataInputStream(){return inputStream_;}
private:
    long generateRequestFlag(bool clearSessionMemory = false, bool disableprotocol = false, bool pickleTableToList = false, bool disableDecimal = false);
    ConstantSP run(const string& script, const string& scriptType, vector<ConstantSP>& args, int priority = 4, int parallelism = 64,int fetchSize = 0, bool clearMemory = false);
    py::object runPy(
        const string& script, const string& scriptType, vector<ConstantSP>& args,
        int priority = 4, int parallelism = 64, int fetchSize = 0, bool clearMemory = false,
        bool pickleTableToList = false, bool disableDecimal = false);
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
    int keepAliveTime_;
    bool compress_;
    bool python_;
    PROTOCOL protocol_;
    bool msg_;
    static DdbInit ddbInit_;
    bool isReverseStreaming_;
    int sqlStd_;
    int readTimeout_;
    int writeTimeout_;
    DataInputStreamSP inputStream_;
    Mutex mutex_;
};

}