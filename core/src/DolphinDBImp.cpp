#include "DolphinDBImp.h"
#include "DolphinDB.h"
#include "Logger.h"
#include "ConstantMarshall.h"
#include "DdbPythonUtil.h"
#include "Pickle.h"
#include "Wrappers.h"
#include "ConstantFactory.h"

#define APIMinVersionRequirement 300

namespace dolphindb {

DdbInit DBConnectionImpl::ddbInit_;
DBConnectionImpl::DBConnectionImpl(bool sslEnable, bool asynTask, int keepAliveTime, bool compress, PARSER_TYPE parser, bool isReverseStreaming, int sqlStd)
		: port_(0), encrypted_(false), isConnected_(false), littleEndian_(Util::isLittleEndian()), 
		sslEnable_(sslEnable),asynTask_(asynTask), keepAliveTime_(keepAliveTime), compress_(compress),
		parser_(parser), protocol_(PROTOCOL_DDB), msg_(true), isReverseStreaming_(isReverseStreaming),
        sqlStd_(sqlStd), readTimeout_(-1), writeTimeout_(-1), logger_(std::make_shared<Logger>(Logger::Level::LevelInfo)) {
    logger_->init(false);
}

DBConnectionImpl::~DBConnectionImpl() {
    close();
    conn_.clear();
    logger_->clear();
}

void DBConnectionImpl::close() {
    if (!isConnected_)
        return;
    isConnected_ = false;
    if (!conn_.isNull()) {
        conn_->close();
    }
    sessionId_ = "";
}

bool DBConnectionImpl::connect(const string& hostName, int port, const string& userId,
        const string& password, bool sslEnable,bool asynTask, int keepAliveTime, bool compress,
        PARSER_TYPE parser, int readTimeout, int writeTimeout) {
    hostName_ = hostName;
    port_ = port;
    userId_ = userId;
    pwd_ = password;
    encrypted_ = false;
    sslEnable_ = sslEnable;
    asynTask_ = asynTask;
    if(keepAliveTime > 0){
        keepAliveTime_ = keepAliveTime;
    }
    compress_ = compress;
    parser_ = parser;
    if (readTimeout > 0) {
        readTimeout_ = readTimeout ;
    }
    if (writeTimeout > 0) {
        writeTimeout_ = writeTimeout;
    }
    return connect();
}

bool DBConnectionImpl::connect() {
    DLOG("Imp.connect start");
    close();

    SocketSP conn = new Socket(hostName_, port_, true, keepAliveTime_, sslEnable_);
    conn->setTimeout(readTimeout_, writeTimeout_);
    IO_ERR ret = conn->connect();
    if (ret != OK) {
        return false;
    }
    DLOG("Imp.connect socket ready");

    string body = "connect\n";
    if (!userId_.empty() && !encrypted_)
        body.append("login\n" + userId_ + "\n" + pwd_ + "\nfalse");
    string out("API 0 ");
    out.append(Util::convert((int)body.size()));
    out.append(" / "+ std::to_string(generateRequestFlag())+"_1_" + std::to_string(4) + "_" + std::to_string(2));
    out.append(1, '\n');
    out.append(body);
    size_t actualLength;
    ret = conn->write(out.c_str(), out.size(), actualLength);
    if (ret != OK)
        throw IOException("Couldn't send login message to the given host/port with IO error type " + std::to_string(ret));

    DataInputStreamSP in = new DataInputStream(conn);
    string line;
    ret = in->readLine(line);
    if (ret != OK)
        throw IOException("Failed to read message from the socket with IO error type " + std::to_string(ret));

    vector<string> headers;
    Util::split(line.c_str(), ' ', headers);
    if (headers.size() != 3)
        throw IOException("Received invalid header");
    string sessionId = headers[0];
    int numObject = atoi(headers[1].c_str());
    bool remoteLittleEndian = (headers[2] != "0");

    if ((ret = in->readLine(line)) != OK)
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret));
    if (line != "OK")
        throw IOException("Server connection response: '" + line);

    if (numObject == 1) {
        short flag;
        if ((ret = in->readShort(flag)) != OK)
            throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));
        DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);

        ConstantUnmarshallFactory factory(in);
        ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
        if(unmarshall==NULL)
            throw IOException("Failed to parse the incoming object" + std::to_string(form));
        if (!unmarshall->start(flag, true, ret)) {
            unmarshall->reset();
            throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
        }
        ConstantSP result = unmarshall->getConstant();
        unmarshall->reset();
        if (!result->getBool())
            throw IOException("Failed to authenticate the user");
    }

    conn_ = conn;
    inputStream_ = new DataInputStream(conn_);
    sessionId_ = sessionId;
    isConnected_ = true;
    littleEndian_ = remoteLittleEndian;

    if (!userId_.empty() && encrypted_) {
        try {
            login();
        } catch (...) {
            close();
            throw;
        }
    }

    ConstantSP requiredVersion;
    
    try {
        if(asynTask_) {
            SmartPointer<DBConnection> newConn = new DBConnection(false, false);
            newConn->connect(hostName_, port_, userId_, pwd_);
            vector<ConstantSP> args;
            requiredVersion =newConn->run("getRequiredAPIVersion", args);
        }else{
            vector<ConstantSP> args;
            requiredVersion = run("getRequiredAPIVersion", args);
        }
    }
    catch(...){
        return true;
    }
    if(!requiredVersion->isTuple()){
        return true;
    }else{
        int apiVersion = requiredVersion->get(0)->getInt();
        if(apiVersion > APIMinVersionRequirement){
            close();
            throw IOException("Required C++ API version at least "  + std::to_string(apiVersion) + ". Current C++ API version is "+ std::to_string(APIMinVersionRequirement) +". Please update DolphinDB C++ API. ");
        }
        if(requiredVersion->size() >= 2 && requiredVersion->get(1)->getString() != ""){
            LOG_WARN(requiredVersion->get(1)->getString());
        }
    }
    return true;
}

void DBConnectionImpl::login(const string& userId, const string& password, bool enableEncryption) {
    userId_ = userId;
    pwd_ = password;
    encrypted_ = enableEncryption;
    login();
}

void DBConnectionImpl::login() {
    DLOG("Imp.connect login");
    // TODO: handle the case of encryption.
    vector<ConstantSP> args;
    args.push_back(new String(userId_));
    args.push_back(new String(pwd_));
    args.push_back(new Bool(false));
    ConstantSP result = run("login", args);
    if (!result->getBool())
        throw IOException("Failed to authenticate the user " + userId_);
}

ConstantSP DBConnectionImpl::run(const string& script, int priority, int parallelism, int fetchSize, bool clearMemory) {
    vector<ConstantSP> args;
    return run(script, "script", args, priority, parallelism, fetchSize, clearMemory);
}

ConstantSP DBConnectionImpl::run(const string& funcName, vector<ConstantSP>& args, int priority, int parallelism, int fetchSize, bool clearMemory) {
    return run(funcName, "function", args, priority, parallelism, fetchSize, clearMemory);
}

py::object DBConnectionImpl::runPy(
    const string &script, int priority,
    int parallelism, int fetchSize, bool clearMemory,
    bool pickleTableToList, bool disableDecimal, bool withTableSchema
) {
    vector<ConstantSP> args;
    return runPy(
        script, "script", args, priority, parallelism,
        fetchSize, clearMemory, pickleTableToList, disableDecimal, withTableSchema
    );
}

py::object DBConnectionImpl::runPy(
    const string &funcName, vector<ConstantSP> &args, int priority,
    int parallelism, int fetchSize, bool clearMemory,
    bool pickleTableToList, bool disableDecimal, bool withTableSchema
) {
    return runPy(
        funcName, "function", args, priority, parallelism,
        fetchSize, clearMemory, pickleTableToList, disableDecimal, withTableSchema
    );
}

ConstantSP DBConnectionImpl::upload(const string& name, const ConstantSP& obj) {
    if (!Util::isVariableCandidate(name))
        throw RuntimeException(name + " is not a qualified variable name.");
    vector<ConstantSP> args(1, obj);
    return run(name, "variable", args);
}

ConstantSP DBConnectionImpl::upload(vector<string>& names, vector<ConstantSP>& objs) {
    if (names.size() != objs.size())
        throw RuntimeException("the size of variable names doesn't match the size of objects.");
    if (names.empty())
        return Constant::void_;

    string varNames;
    for (unsigned int i = 0; i < names.size(); ++i) {
        if (!Util::isVariableCandidate(names[i]))
            throw RuntimeException(names[i] + " is not a qualified variable name.");
        if (i > 0)
            varNames.append(1, ',');
        varNames.append(names[i]);
    }
    return run(varNames, "variable", objs);
}

long DBConnectionImpl::generateRequestFlag(bool clearSessionMemory, bool disableprotocol, bool pickleTableToList, bool disableDecimal) {
    long flag = 32; //32 API client
    if (asynTask_) {
        flag += 4;
    }
    if (clearSessionMemory) {
        flag += 16;
    }
    if (protocol_ == PROTOCOL_DDB || disableprotocol) {
        if (compress_)
            flag += 64;
    } 
    else if (protocol_ == PROTOCOL_PICKLE) {
        DLOG("pickle",pickleTableToList?"toList":"");
        flag += 8;
        if (pickleTableToList) {
            flag += (1 << 15);
        }
    }
    else if (protocol_ == PROTOCOL_ARROW) {
        DLOG("arrow");
        flag += (1 << 15);
    }
    else {
        throw RuntimeException("unsupport PROTOCOL Type: " + std::to_string(protocol_));
    }
    if (parser_ == PARSER_TYPE::PARSER_PYTHON) {
        flag += 2048;
    }
    else if (parser_ == PARSER_TYPE::PARSER_KDB) {
        flag += 4096;
    }
    if (isReverseStreaming_) {
        flag += 131072;
    }
    if (sqlStd_) {
        flag += sqlStd_ << 19;
    }
    if (disableDecimal) {
        flag += (1 << 23);
    }
    return flag;
}

ConstantSP DBConnectionImpl::run(const string& script, const string& scriptType, vector<ConstantSP>& args,
            int priority, int parallelism, int fetchSize, bool clearMemory) {
    DLOG("run1",script,"start");
    if (!isConnected_)
        throw IOException("Couldn't send script/function to the remote host because the connection has been closed");

    if(fetchSize > 0 && fetchSize < 8192)
        throw IOException("fetchSize must be greater than 8192");

    string body;
    size_t argCount = args.size();
    if (scriptType == "script")
        body = "script\n" + script;
    else {
        body = scriptType + "\n" + script;
        body.append("\n" + std::to_string(argCount));
        body.append("\n");
        body.append(Util::isLittleEndian() ? "1" : "0");
    }
    string out("API2 " + sessionId_ + " ");
    out.append(Util::convert((int)body.size()));
    out.append(" / " + std::to_string(generateRequestFlag(clearMemory,true)) + "_1_" + std::to_string(priority) + "_" + std::to_string(parallelism));
    out.append("__" + std::to_string(fetchSize));
    out.append(1, '\n');
    out.append(body);
    DLOG("run1",script,"header",out);

    LockGuard<Mutex> runGuard(&mutex_);

    IO_ERR ret;
    if (argCount > 0) {
        for (size_t i = 0; i < argCount; ++i) {
            if (args[i]->containNotMarshallableObject()) {
                throw IOException("The function argument or uploaded object is not marshallable.");
            }
        }
        DataOutputStreamSP outStream = new DataOutputStream(conn_);
        ConstantMarshallFactory marshallFactory(outStream);
        bool enableCompress = false;
        for (size_t i = 0; i < argCount; ++i) {
            enableCompress = (args[i]->getForm() == DATA_FORM::DF_TABLE) ? compress_ : false;
            ConstantMarshall* marshall = marshallFactory.getConstantMarshall(args[i]->getForm());
            if (i == 0)
                marshall->start(out.c_str(), out.size(), args[i], true, enableCompress, ret);
            else
                marshall->start(args[i], true, enableCompress, ret);
            marshall->reset();
            if (ret != OK) {
                close();
                throw IOException("Couldn't send function argument to the remote host with IO error type " + std::to_string(ret));
            }
        }
        ret = outStream->flush();
        if (ret != OK) {
            close();
            throw IOException("Failed to marshall code with IO error type " + std::to_string(ret));
        }
    } else {
        size_t actualLength;
        IO_ERR ret = conn_->write(out.c_str(), out.size(), actualLength);
        if (ret != OK) {
            close();
            throw IOException("Couldn't send script/function to the remote host because the connection has been closed, IO error type " + std::to_string(ret));
        }
    }
    
    if(asynTask_)
        return new Void();
    DLOG("run1",script,"read");
    
    if (littleEndian_ != (char)Util::isLittleEndian())
        inputStream_->enableReverseIntegerByteOrder();

    string line;
    if ((ret = inputStream_->readLine(line)) != OK) {
        close();
        throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
    }
    while (line == "MSG") {
        if ((ret = inputStream_->readString(line)) != OK) {
            close();
            throw IOException("Failed to read response msg from the socket with IO error type " + std::to_string(ret));
        }
        if ((ret = inputStream_->readLine(line)) != OK) {
            close();
            throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
        }
    }
    vector<string> headers;
    Util::split(line.c_str(), ' ', headers);
    if (headers.size() != 3) {
        close();
        throw IOException("Received invalid header");
    }
    sessionId_ = headers[0];
    int numObject = atoi(headers[1].c_str());

    if ((ret = inputStream_->readLine(line)) != OK) {
        close();
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret));
    }

    if (line != "OK") {
        throw IOException(hostName_+":"+std::to_string(port_)+" Server response: '" + line + "' script: '" + script + "'");
    }

    if (numObject == 0) {
        return new Void();
    }
    
    short flag;
    if ((ret = inputStream_->readShort(flag)) != OK) {
        close();
        throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));
    }
    
    DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);
    DATA_TYPE type = static_cast<DATA_TYPE >(flag & 0xff);
    if(fetchSize > 0 && form == DF_VECTOR && type == DT_ANY)
        return new BlockReader(inputStream_);
    ConstantUnmarshallFactory factory(inputStream_);
    ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
    if(unmarshall == NULL){
        LOG_ERR("Unknow incoming object form",form,"of type",type);
        inputStream_->reset(0);
        conn_->skipAll();
        return Constant::void_;
    }
    if (!unmarshall->start(flag, true, ret)) {
        unmarshall->reset();
        close();
        throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
    }

    ConstantSP result = unmarshall->getConstant();
    unmarshall->reset();
    DLOG("run1",script,"end");
    return result;
}

py::dict getTableSchema(ConstantSP result)
{
    TableSP ddbTbl = result;
    py::dict column_types;
    size_t columnSize = ddbTbl->columns();

    for (size_t i = 0; i < columnSize; ++i)
    {
        py::str column_name = ddbTbl->getColumnName(i);
        DATA_TYPE type = ddbTbl->getColumnType(i);
        py::str typeString = Util::getDataTypeString(type);

        py::object scale = py::none();
        if ((type >= DT_DECIMAL32 && type <= DT_DECIMAL128) || (type >= DT_DECIMAL32_ARRAY && type <= DT_DECIMAL128_ARRAY)) {
            scale = py::int_(ddbTbl->getColumn(i)->getExtraParamForType());
        }

        column_types[column_name] = py::make_tuple(typeString, scale);
    }
    return column_types;
}

py::object DBConnectionImpl::runPy(
    const string &script, const string &scriptType, vector<ConstantSP> &args,
    int priority, int parallelism, int fetchSize, bool clearMemory,
    bool pickleTableToList, bool disableDecimal, bool withTableSchema
) {
    //RecordTime record("Db.runPy"+script);
    DLOG("runPy",script,"start argsize",args.size());
    //force Python release GIL
    if (!isConnected_)
        throw IOException("Couldn't send script/function to the remote host because the connection has been closed");
    py::gil_scoped_release gil;

    //RecordTime record("Db.server");
    string body;
    int argCount = args.size();
    if (scriptType == "script")
        body = "script\n" + script;
    else {
        body = scriptType + "\n" + script;
        body.append("\n" + std::to_string(argCount));
        body.append("\n");
        body.append(Util::isLittleEndian() ? "1" : "0");
    }
    string out("API2 " + sessionId_ + " ");
    out.append(Util::convert((int)body.size()));
    long flag = generateRequestFlag(clearMemory, false, pickleTableToList, disableDecimal);
    DLOG("runPy flag: ", flag);
    DLOG("protocol: ", protocol_, " pickleTableToList: ", pickleTableToList);
    DLOG("compress: ", compress_, " parser: ", parser_, " disableDecimal: ", disableDecimal);
    out.append(" / " + std::to_string(flag) + "_1_" + std::to_string(priority) + "_" + std::to_string(parallelism));
    if(fetchSize > 0)
        out.append("__" + std::to_string(fetchSize));

    out.append(1, '\n');
    out.append(body);

    LockGuard<Mutex> runGuard(&mutex_);

    IO_ERR ret;
    if (argCount > 0) {
        for (int i = 0; i < argCount; ++i) {
            if (args[i]->containNotMarshallableObject()) {
                throw IOException("The function argument or uploaded object is not marshallable.");
            }
        }
        DataOutputStreamSP outStream = new DataOutputStream(conn_);
        ConstantMarshallFactory marshallFactory(outStream);
        bool enableCompress = false;
        for (int i = 0; i < argCount; ++i) {
            enableCompress = (args[i]->getForm() == DATA_FORM::DF_TABLE) ? compress_ : false;
            ConstantMarshall* marshall = marshallFactory.getConstantMarshall(args[i]->getForm());
            if (i == 0)
                marshall->start(out.c_str(), out.size(), args[i], true, enableCompress,ret);
            else
                marshall->start(args[i], true, enableCompress,ret);
            marshall->reset();
            if (ret != OK) {
                close();
                throw IOException("Couldn't send function argument to the remote host with IO error type " + std::to_string(ret));
            }
        }
        ret = outStream->flush();
        if (ret != OK) {
            close();
            throw IOException("Failed to marshall code with IO error type " + std::to_string(ret));
        }
    } else {
        size_t actualLength;
        ret = conn_->write(out.c_str(), out.size(), actualLength);
        if (ret != OK) {
            close();
            throw IOException("Couldn't send script/function to the remote host because the connection has been closed");
        }
    }
    if(asynTask_){
        return py::none();
    }
    DataInputStreamSP in = new DataInputStream(conn_);
    if (littleEndian_ != (char)Util::isLittleEndian())
        in->enableReverseIntegerByteOrder();

    string line;
    if ((ret = in->readLine(line)) != OK) {
        close();
        throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
    }
    while(line == "MSG"){
        if ((ret = in->readString(line)) != OK) {
            close();
            throw IOException("Failed to read response msg from the socket with IO error type " + std::to_string(ret));
        }
        if (msg_) { logger_->Info(line); }
        if ((ret = in->readLine(line)) != OK) {
            close();
            throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret));
        }
    }

    //RecordTime pickleRecord("Db.ProcessReply");
    DLOG("runPy header",line);
    vector<string> headers;
    Util::split(line.c_str(), ' ', headers);
    if (headers.size() != 3) {
        close();
        throw IOException("Received invalid header: "+line);
    }
    sessionId_ = headers[0];
    int numObject = atoi(headers[1].c_str());

    DLOG("runPy OK?");
    if ((ret = in->readLine(line)) != OK) {
        close();
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret));
    }
    
    if (line != "OK") {
        throw IOException("Server response: '" + line + "' script: '" + script + "'");
    }

    if (numObject == 0) {
        if (withTableSchema) {
            py::gil_scoped_acquire pgil;
            return py::make_tuple(py::none(), py::none());
        }
        return py::none();
    }

    DLOG("runPy flag?");
    short retFlag;
    if ((ret = in->readShort(retFlag)) != OK) {
        close();
        throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret));
    }

    DATA_FORM form = static_cast<DATA_FORM>(retFlag >> 8);
    DLOG("runPy form",form);
    if (form & 32) {
        if (protocol_ == PROTOCOL_ARROW) {
            InputStreamWrapper wrapper = InputStreamWrapper();
            wrapper.setInputStream(in);
            py::gil_scoped_acquire pgil;
            py::object pywrapper = py::cast(wrapper);
            py::object reader = converter::PyObjs::cache_->pyarrow_.attr("ipc").attr("RecordBatchStreamReader")(pywrapper);
            py::object pa = reader.attr("read_all")();
            return pa;
        }
        if (protocol_ == PROTOCOL_PICKLE) {
            py::gil_scoped_acquire pgil;
#if (PY_MINOR_VERSION >= 6) && (PY_MINOR_VERSION <= 12)
            DLOG("runPy pickle",retFlag);
            std::unique_ptr<PickleUnmarshall> unmarshall(new PickleUnmarshall(in));
            if (!unmarshall->start(retFlag, true, ret)) {
                unmarshall->reset();
                close();
                if(ret != OK)
                    throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
                else
                    throw IOException("Received invalid serialized data during deserialization!");
            }
            DLOG("runPy getpyobj");
            PyObject * result = unmarshall->getPyObj();
            if(form - 32 == DF_MATRIX) {
                PyObject * tem = PyList_GetItem(result, 0);
                py::array mat = py::handle(tem).cast<py::array>();
                py::object dtype = py::getattr(mat, "dtype");
                if (UNLIKELY(CHECK_EQUAL(dtype, np_object_))) {
                    mat = mat.attr("transpose")();
                    Py_IncRef(mat.ptr());
                    PyList_SetItem(result, 0, mat.ptr());
                }
            }
            DLOG("runPy return");
            unmarshall->reset();
            py::object res = py::handle(result).cast<py::object>();
            res.dec_ref();
            return res;
#else
            return py::none();
#endif
        }
    }
    ConstantSP result;
    {
        //RecordTime record("Db.ConstUnma");
        ConstantUnmarshallFactory factory(in);
        ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
        if(unmarshall == NULL){
            DLOG("runPy ",script," invalid form",form);
            //FIXME: ignore it now until server fix this bug.
            in->reset(0);
            conn_->skipAll();
            return py::none();
        }
        DLOG("runPy unmarshall",form);
        bool unmarshallRet = unmarshall->start(retFlag, true, ret);
        result = unmarshall->getConstant();
        unmarshall->reset();
        if (!unmarshallRet) {
            close();
            throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret));
        }
    }
    py::gil_scoped_acquire pgil;

    converter::ToPythonOption option(pickleTableToList);

    if (withTableSchema) {
        py::object dataframe = converter::Converter::toPython_Old(result, option);
        if (result->getForm() == DATA_FORM::DF_TABLE) {
            return py::make_tuple(dataframe, getTableSchema(result));
        }
        return py::make_tuple(dataframe, py::none());
    }

    return converter::Converter::toPython_Old(result, option);
}

}