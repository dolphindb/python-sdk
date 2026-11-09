#include "DolphinDBImp.h"
#include "DolphinDB.h"
#include "Exceptions.h"
#include "Logger.h"
#include "ConstantMarshall.h"
#include "PytoDdbRowPool.h"
#include "Pickle.h"
#include "Wrappers.h"
#include "ConstantFactory.h"

#define APIMinVersionRequirement 300

namespace dolphindb {

std::string formatSeqNoSuffix(const std::string& clientId, long seqNum) {
    if (clientId.empty() || seqNum == 0)
        return std::string();
    return "__" + clientId + "_" + std::to_string(seqNum);
}

namespace {

constexpr long long ARROW_UPLOAD_SINGLE_BATCH_THRESHOLD_BYTES = 64LL * 1024 * 1024;
constexpr long long ARROW_UPLOAD_TARGET_BATCH_BYTES = 32LL * 1024 * 1024;
constexpr long long ARROW_UPLOAD_MIN_CHUNK_ROWS = 65536;
constexpr long long ARROW_UPLOAD_MAX_CHUNK_ROWS = 1048576;
constexpr char ARROW_UPLOAD_TOTAL_ROWS_METADATA_KEY[] = "ddbTotalRows";

REQUEST_FORMAT resolveRequestFormat(PROTOCOL protocol, REQUEST_FORMAT requestFormat) {
    if (requestFormat != REQUEST_FORMAT_AUTO) {
        return requestFormat;
    }
    if (protocol == PROTOCOL_ARROW) {
        return REQUEST_FORMAT_ARROW;
    }
    return REQUEST_FORMAT_DDB;
}

const char* protocolTypeToString(PROTOCOL protocol) {
    switch (protocol) {
    case PROTOCOL_DDB:
        return "ddb";
    case PROTOCOL_PICKLE:
        return "pickle";
    case PROTOCOL_ARROW:
        return "arrow";
    default:
        return "unknown";
    }
}

const char* requestFormatToString(REQUEST_FORMAT requestFormat) {
    switch (requestFormat) {
    case REQUEST_FORMAT_AUTO:
        return "auto";
    case REQUEST_FORMAT_DDB:
        return "ddb";
    case REQUEST_FORMAT_ARROW:
        return "arrow";
    case REQUEST_FORMAT_ARROW_RETURN_LIMIT:
        return "arrow_return_limit";
    default:
        return "unknown";
    }
}

void validateRequestArguments(const std::vector<RequestArgument>& args) {
    for (const auto& arg : args) {
        if (arg.isConstant() && arg.constant->containNotMarshallableObject()) {
            throw IOException("The function argument or uploaded object is not marshallable.");
        }
    }
}

void writeSocketBytes(const DataOutputStreamSP& outStream, const char* buffer,
                      size_t length, const std::string& context) {
    size_t actualWritten = 0;
    IO_ERR ret = outStream->write(buffer, length, actualWritten);
    if (ret != OK) {
        throw IOException(context + " with IO error type " + std::to_string(ret), ret);
    }
    if (actualWritten != length) {
        throw IOException(context + " because only part of the payload was written.");
    }
}

long long estimateArrowTableBytes(const py::object& table) {
    try {
        return table.attr("get_total_buffer_size")().cast<long long>();
    } catch (const py::error_already_set&) {
        return table.attr("nbytes").cast<long long>();
    }
}

long long chooseArrowChunkRows(const py::object& table) {
    const long long totalRows = table.attr("num_rows").cast<long long>();
    if (totalRows <= 0) {
        return 0;
    }

    const long long totalBytes = estimateArrowTableBytes(table);
    if (totalBytes <= 0 || totalBytes <= ARROW_UPLOAD_SINGLE_BATCH_THRESHOLD_BYTES) {
        return 0;
    }

    const long long averageRowBytes = std::max(1LL, totalBytes / totalRows);
    long long chunkRows = ARROW_UPLOAD_TARGET_BATCH_BYTES / averageRowBytes;
    chunkRows = std::max(ARROW_UPLOAD_MIN_CHUNK_ROWS, chunkRows);
    chunkRows = std::min(ARROW_UPLOAD_MAX_CHUNK_ROWS, chunkRows);
    if (chunkRows >= totalRows) {
        return 0;
    }
    return chunkRows;
}

py::object attachArrowUploadMetadata(const py::object& table) {
    py::object schema = table.attr("schema");
    py::dict metadata;
    py::object existingMetadata = schema.attr("metadata");
    if (!existingMetadata.is_none()) {
        metadata = py::dict(existingMetadata);
    }
    metadata[py::bytes(ARROW_UPLOAD_TOTAL_ROWS_METADATA_KEY)] =
        py::bytes(std::to_string(table.attr("num_rows").cast<long long>()));
    return table.attr("replace_schema_metadata")(metadata);
}

void streamArrowTableArgument(const RequestArgument& arg,
                              const DataOutputStreamSP& outStream) {
    if (!arg.isArrowTable() || !arg.pythonObject) {
        throw IOException("Invalid Arrow request argument.");
    }
    short flag = static_cast<short>(((DF_TABLE + 32) << 8) | BASICTBL);
    writeSocketBytes(outStream, reinterpret_cast<const char*>(&flag), sizeof(flag),
                     "Couldn't send Arrow object flag to the remote host");

    py::gil_scoped_acquire acquire;
    py::object bufferSink = converter::PyObjs::cache_->pyarrow_.attr("BufferOutputStream")();
    py::object table = attachArrowUploadMetadata(arg.pythonObject.get());
    py::object writer = converter::PyObjs::cache_->pyarrow_.attr("ipc").attr("RecordBatchStreamWriter")(
        bufferSink, table.attr("schema"));
    const long long chunkRows = chooseArrowChunkRows(table);
    if (chunkRows > 0) {
        writer.attr("write_table")(table, py::arg("max_chunksize") = chunkRows);
    } else {
        writer.attr("write_table")(table);
    }
    writer.attr("close")();

    py::object payload = bufferSink.attr("getvalue")().attr("to_pybytes")();
    char* buffer = nullptr;
    py::ssize_t length = 0;
    if (PyBytes_AsStringAndSize(payload.ptr(), &buffer, &length) < 0) {
        throw py::error_already_set();
    }
    if (length > 0) {
        writeSocketBytes(outStream, buffer, static_cast<size_t>(length),
                         "Couldn't send Arrow payload to the remote host");
    }
    IO_ERR flushRet = outStream->flush();
    if (flushRet != OK) {
        throw IOException("Couldn't flush Arrow payload to the remote host with IO error type " +
                          std::to_string(flushRet), flushRet);
    }
}

}  // namespace

DdbInit DBConnectionImpl::ddbInit_;
DBConnectionImpl::DBConnectionImpl(bool sslEnable, bool asynTask, bool compress, PARSER_TYPE parser, bool isReverseStreaming, int sqlStd)
		: port_(0), encrypted_(false), isConnected_(false), littleEndian_(Util::isLittleEndian()),
		sslEnable_(sslEnable),asynTask_(asynTask), compress_(compress),
		parser_(parser), protocol_(PROTOCOL_DDB), msg_(true), isReverseStreaming_(isReverseStreaming),
	        sqlStd_(sqlStd), netTimeout_(-1), timeout_(-1), logger_(std::make_shared<Logger>(Logger::Level::LevelInfo)) {
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
        const string& password, bool sslEnable,bool asynTask, bool compress,
        PARSER_TYPE parser, int netTimeout, int timeout) {
    hostName_ = hostName;
    port_ = port;
    userId_ = userId;
    pwd_ = password;
    encrypted_ = false;
    sslEnable_ = sslEnable;
    asynTask_ = asynTask;
    compress_ = compress;
    parser_ = parser;
    netTimeout_ = netTimeout;
    timeout_ = timeout;
    return connect();
}

bool DBConnectionImpl::connect() {
    DLOG("Imp.connect start");
    close();

    SocketSP conn = new Socket(hostName_, port_, true, -1, sslEnable_);
    conn->setTimeout(netTimeout_, timeout_);
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
        throw IOException("Couldn't send login message to the given host/port with IO error type " + std::to_string(ret), ret);

    DataInputStreamSP in = new DataInputStream(conn);
    string line;
    ret = in->readLine(line);
    if (ret != OK)
        throw IOException("Failed to read message from the socket with IO error type " + std::to_string(ret), ret);

    vector<string> headers;
    Util::split(line.c_str(), ' ', headers);
    if (headers.size() != 3)
        throw IOException("Received invalid header");
    string sessionId = headers[0];
    int numObject = atoi(headers[1].c_str());
    bool remoteLittleEndian = (headers[2] != "0");

    if ((ret = in->readLine(line)) != OK)
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret), ret);
    if (line != "OK")
        throw IOException("Server connection response: '" + line);

    if (numObject == 1) {
        short flag;
        if ((ret = in->readShort(flag)) != OK)
            throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret), ret);
        DATA_FORM form = static_cast<DATA_FORM>(flag >> 8);

        ConstantUnmarshallFactory factory(in);
        ConstantUnmarshall* unmarshall = factory.getConstantUnmarshall(form);
        if(unmarshall==NULL)
            throw IOException("Failed to parse the incoming object" + std::to_string(form), ret);
        if (!unmarshall->start(flag, true, ret)) {
            unmarshall->reset();
            throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret), ret);
        }
        ConstantSP result = unmarshall->getConstant();
        unmarshall->reset();
        if (!result->getBool())
            throw IOException("Failed to authenticate the user", ret);
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

ConstantSP DBConnectionImpl::run(const string& script, int priority, int parallelism, int fetchSize, bool clearMemory, long seqNum) {
    vector<ConstantSP> args;
    return run(script, "script", args, priority, parallelism, fetchSize, clearMemory, seqNum);
}

ConstantSP DBConnectionImpl::run(const string& funcName, vector<ConstantSP>& args, int priority, int parallelism, int fetchSize, bool clearMemory, long seqNum) {
    return run(funcName, "function", args, priority, parallelism, fetchSize, clearMemory, seqNum);
}

py::object DBConnectionImpl::runPy(
    const string &script, int priority,
    int parallelism, int fetchSize, bool clearMemory,
    bool pickleTableToList, bool disableDecimal, bool withTableSchema, long seqNum,
    const converter::ArrayVectorToColumns &arrayVectorToColumns
) {
    vector<RequestArgument> args;
    return runPy(
        script, "script", args, priority, parallelism,
        fetchSize, clearMemory, REQUEST_FORMAT_AUTO, pickleTableToList, disableDecimal, withTableSchema, seqNum, arrayVectorToColumns
    );
}

py::object DBConnectionImpl::runPy(
    const string &funcName, vector<ConstantSP> &args, int priority,
    int parallelism, int fetchSize, bool clearMemory,
    bool pickleTableToList, bool disableDecimal, bool withTableSchema, long seqNum,
    const converter::ArrayVectorToColumns &arrayVectorToColumns
) {
    vector<RequestArgument> requestArgs;
    requestArgs.reserve(args.size());
    for (const auto& arg : args) {
        requestArgs.emplace_back(arg);
    }
    return runPy(
        funcName, "function", requestArgs, priority, parallelism,
        fetchSize, clearMemory, REQUEST_FORMAT_AUTO, pickleTableToList, disableDecimal, withTableSchema, seqNum, arrayVectorToColumns
    );
}

py::object DBConnectionImpl::runPy(
    const string &funcName, vector<RequestArgument> &args, int priority,
    int parallelism, int fetchSize, bool clearMemory, REQUEST_FORMAT requestFormat,
    bool pickleTableToList, bool disableDecimal, bool withTableSchema, long seqNum,
    const converter::ArrayVectorToColumns &arrayVectorToColumns
) {
    return runPy(
        funcName, "function", args, priority, parallelism, fetchSize, clearMemory,
        requestFormat, pickleTableToList, disableDecimal, withTableSchema, seqNum, arrayVectorToColumns
    );
}

py::object DBConnectionImpl::uploadPy(vector<string>& names, vector<RequestArgument>& objs,
                                      REQUEST_FORMAT requestFormat) {
    if (names.size() != objs.size())
        throw RuntimeException("the size of variable names doesn't match the size of objects.");
    if (names.empty())
        return py::none();

    string varNames;
    for (unsigned int i = 0; i < names.size(); ++i) {
        if (!Util::isVariableCandidate(names[i]))
            throw RuntimeException(names[i] + " is not a qualified variable name.");
        if (i > 0)
            varNames.append(1, ',');
        varNames.append(names[i]);
    }
    return runPy(varNames, "variable", objs, 4, 64, 0, false,
                 requestFormat, false, false, false);
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

long DBConnectionImpl::generateRequestFlag(bool clearSessionMemory, REQUEST_FORMAT requestFormat,
                                           bool pickleTableToList, bool disableDecimal) {
    long flag = 32; //32 API client
    if (asynTask_) {
        flag += 4;
    }
    if (clearSessionMemory) {
        flag += 16;
    }
    REQUEST_FORMAT effectiveFormat = resolveRequestFormat(protocol_, requestFormat);
    if (protocol_ == PROTOCOL_PICKLE && requestFormat == REQUEST_FORMAT_AUTO) {
        DLOG("pickle",pickleTableToList?"toList":"");
        flag += 8;
        if (pickleTableToList) {
            flag += (1 << 15);
        }
    }
    else if (effectiveFormat == REQUEST_FORMAT_DDB) {
        if (compress_)
            flag += 64;
    }
    else if (effectiveFormat == REQUEST_FORMAT_ARROW) {
        DLOG("arrow");
        flag += (1 << 15);
    }
    else if (effectiveFormat == REQUEST_FORMAT_ARROW_RETURN_LIMIT) {
        DLOG("arrow returnLimit");
        flag += (1 << 15);
        flag += (1 << 18);
    }
    else {
        throw RuntimeException(
            "Unsupported request format for PROTOCOL Type '" +
            std::string(protocolTypeToString(protocol_)) +
            "'. request format='" + std::string(requestFormatToString(effectiveFormat)) +
            "' (" + std::to_string(static_cast<int>(effectiveFormat)) + ").");
    }
    if (parser_ == PARSER_TYPE::PARSER_PYTHON) {
        flag += 2048;
    } else if (parser_ == PARSER_TYPE::PARSER_KDB) {
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
            int priority, int parallelism, int fetchSize, bool clearMemory, long seqNum) {
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
    out.append(" / " + std::to_string(generateRequestFlag(clearMemory, REQUEST_FORMAT_DDB)) + "_1_" + std::to_string(priority) + "_" + std::to_string(parallelism));
    out.append("__" + std::to_string(fetchSize));
    out.append(formatSeqNoSuffix(runClientId_, seqNum));
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
                throw IOException("Couldn't send function argument to the remote host with IO error type " + std::to_string(ret), ret);
            }
        }
        ret = outStream->flush();
        if (ret != OK) {
            close();
            throw IOException("Failed to marshall code with IO error type " + std::to_string(ret), ret);
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
        throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret), ret);
    }
    while (line == "MSG") {
        if ((ret = inputStream_->readString(line)) != OK) {
            close();
            throw IOException("Failed to read response msg from the socket with IO error type " + std::to_string(ret), ret);
        }
        if ((ret = inputStream_->readLine(line)) != OK) {
            close();
            throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret), ret);
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
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret), ret);
    }

    if (line != "OK") {
        ServerResponse::throwServerResponse(line, script);
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
    if (fetchSize > 0) {
        if (form != DF_VECTOR || type != DT_ANY) {
            close();
            throw IOException("Fetch size can only be set when executing scripts that return a table.");
        }
        return new BlockReader(inputStream_);
    }
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
    const string &script, const string &scriptType, vector<RequestArgument> &args,
    int priority, int parallelism, int fetchSize, bool clearMemory,
    REQUEST_FORMAT requestFormat, bool pickleTableToList, bool disableDecimal, bool withTableSchema, long seqNum,
    const converter::ArrayVectorToColumns &arrayVectorToColumns
) {
    if (arrayVectorToColumns.enabled()) {
        if (protocol_ != PROTOCOL_DDB || asynTask_ || fetchSize != 0 || pickleTableToList ||
            resolveRequestFormat(protocol_, requestFormat) != REQUEST_FORMAT_DDB || withTableSchema) {
            throw RuntimeException("arrayVectorToColumns requires synchronous DDB table results without "
                                   "fetchSize, pickleTableToList or withTableSchema.");
        }
    }
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
    long flag = generateRequestFlag(clearMemory, requestFormat, pickleTableToList, disableDecimal);
    DLOG("runPy flag: ", flag);
    DLOG("protocol: ", protocol_, " pickleTableToList: ", pickleTableToList);
    DLOG("compress: ", compress_, " parser: ", parser_, " disableDecimal: ", disableDecimal);
    out.append(" / " + std::to_string(flag) + "_1_" + std::to_string(priority) + "_" + std::to_string(parallelism));
    out.append("__" + std::to_string(fetchSize));
    out.append(formatSeqNoSuffix(runClientId_, seqNum));

    out.append(1, '\n');
    out.append(body);

    LockGuard<Mutex> runGuard(&mutex_);

    IO_ERR ret;
    if (argCount > 0) {
        validateRequestArguments(args);
        DataOutputStreamSP outStream = new DataOutputStream(conn_);
        ConstantMarshallFactory marshallFactory(outStream);
        bool requestHeaderWritten = false;
        for (int i = 0; i < argCount; ++i) {
            if (!requestHeaderWritten &&
                (args[i].isRaw() || args[i].isArrowTable())) {
                writeSocketBytes(outStream, out.c_str(), out.size(),
                                 "Couldn't send script/function header to the remote host");
                requestHeaderWritten = true;
            }
            if (args[i].isRaw()) {
                size_t actualWritten = 0;
                ret = outStream->write(args[i].raw.data(), args[i].raw.size(), actualWritten);
            } else if (args[i].isArrowTable()) {
                streamArrowTableArgument(args[i], outStream);
                ret = OK;
            } else {
                ConstantSP &constantArg = args[i].constant;
                bool enableCompress = (constantArg->getForm() == DATA_FORM::DF_TABLE) ? compress_ : false;
                ConstantMarshall* marshall = marshallFactory.getConstantMarshall(constantArg->getForm());
                if (!requestHeaderWritten) {
                    marshall->start(out.c_str(), out.size(), constantArg, true, enableCompress, ret);
                    requestHeaderWritten = true;
                } else {
                    marshall->start(constantArg, true, enableCompress, ret);
                }
                marshall->reset();
            }
            if (ret != OK) {
                close();
                throw IOException("Couldn't send function argument to the remote host with IO error type " + std::to_string(ret), ret);
            }
        }
        ret = outStream->flush();
        if (ret != OK) {
            close();
            throw IOException("Failed to marshall code with IO error type " + std::to_string(ret), ret);
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
        throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret), ret);
    }
    while(line == "MSG"){
        if ((ret = in->readString(line)) != OK) {
            close();
            throw IOException("Failed to read response msg from the socket with IO error type " + std::to_string(ret), ret);
        }
        if (msg_) { logger_->Info(line); }
        if ((ret = in->readLine(line)) != OK) {
            close();
            throw IOException("Failed to read response header from the socket with IO error type " + std::to_string(ret), ret);
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
        throw IOException("Failed to read response message from the socket with IO error type " + std::to_string(ret), ret);
    }

    if (line != "OK") {
        ServerResponse::throwServerResponse(line, script);
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
        throw IOException("Failed to read object flag from the socket with IO error type " + std::to_string(ret), ret);
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
                    throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret), ret);
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
            throw IOException("Failed to parse the incoming object with IO error type " + std::to_string(ret), ret);
        }
    }
    py::gil_scoped_acquire pgil;

    converter::ToPythonOption option(pickleTableToList);
    if (result->getForm() == DATA_FORM::DF_TABLE) {
        option.arrayVectorToColumns_ = arrayVectorToColumns;
    }

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
