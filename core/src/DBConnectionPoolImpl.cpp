#include "DBConnectionPoolImpl.h"

#include "DolphinDB.h"
#include "AsyncWorker.h"
#include "Vector.h"
namespace dolphindb {

DBConnectionPoolImpl::DBConnectionPoolImpl(const string &hostName, int port, int threadNum, const string &userId,
                                           const string &password, bool loadBalance, bool highAvailability,
                                           bool compress, bool reConnect, PARSER_TYPE parser, PROTOCOL protocol,
                                           bool show_output, int sqlStd, int tryReconnectNums, bool usePublicName)
    : shutDownFlag_(false), queue_(new SynchronizedQueue<Task>) {
    latch_ = new CountDownLatch(threadNum);
    if(!loadBalance){
        for(int i = 0 ;i < threadNum; i++){
            SmartPointer<DBConnection> conn = new DBConnection(false, false, 7200, compress, parser, false, sqlStd);
            conn->setProtocol(protocol);
            conn->setShowOutput(show_output);
			bool ret = conn->connect(hostName, port, userId, password, "", highAvailability, {},7200, reConnect, tryReconnectNums, -1, -1, usePublicName);
            if(!ret)
                throw IOException("Failed to connect to " + hostName + ":" + std::to_string(port));
            sessionIds_.push_back(conn->getSessionId());
            connections_.push_back(conn);
            workers_.push_back(new Thread(new AsyncWorker(*this,latch_, conn, queue_, taskStatus_, hostName, port, userId, password)));
            workers_.back()->start();
        }
    }
    else{
        SmartPointer<DBConnection> entryPoint = new DBConnection(false, false, 7200, compress, parser, false, sqlStd);
        bool ret = entryPoint->connect(hostName, port, userId, password, "", highAvailability, {},7200, reConnect, tryReconnectNums, -1, -1, usePublicName);
        if(!ret)
            throw IOException("Failed to connect to " + hostName + ":" + std::to_string(port));
        TableSP nodes = entryPoint->run("rpc(getControllerAlias(), getClusterPerf)");
        vector<string> hosts;
        vector<int> ports;
        VectorSP colHost = nodes->getColumn("host");
        VectorSP colPort = nodes->getColumn("port");
        VectorSP colMode = nodes->getColumn("mode");
        VectorSP colState = nodes->getColumn("state");
        VectorSP colPublicName;
        bool realUsePublicName = false;
        if (usePublicName && nodes->contain("publicName")) {
            colPublicName = nodes->getColumn("publicName");
            realUsePublicName = true;
        }
        INDEX nodeCount = 0;

        for(int i = 0; i < nodes->size(); i++){
            std::string host = usePublicName ? Util::split(colPublicName->getString(i), ';')[0] : colHost->getString(i);
            int port = colPort->getInt(i);
            int mode = colMode->getInt(i);
            if (mode != 0 && mode != 4) {
                continue;
            }
            if (colState->getInt(i) != 1) {
                continue;
            }
            hosts.push_back(host);
            ports.push_back(port);
            nodeCount++;
        }
        if (nodeCount == 0) {
            throw RuntimeException("No available data or compute nodes found in the cluster.");
        }
        for(int i = 0 ;i < threadNum; i++){
            SmartPointer<DBConnection> conn = new DBConnection(false, false, 7200, compress, parser, false, sqlStd);
            conn->setProtocol(protocol);
            conn->setShowOutput(show_output);
            string &curhost = hosts[i % nodeCount];
            int &curport = ports[i % nodeCount];
            bool ret = conn->connect(curhost, curport, userId, password, "", highAvailability, {}, 7200, reConnect, tryReconnectNums, -1, -1, realUsePublicName);
            if(!ret)
                throw IOException("Failed to connect to " + curhost + ":" + std::to_string(curport));
            sessionIds_.push_back(conn->getSessionId());
            connections_.push_back(conn);
            workers_.push_back(new Thread(new AsyncWorker(*this,latch_, conn, queue_, taskStatus_, curhost, curport, userId, password)));
            workers_.back()->start();
        }
    }
}


}