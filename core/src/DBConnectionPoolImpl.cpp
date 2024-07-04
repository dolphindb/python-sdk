#include "DBConnectionPoolImpl.h"

#include "DolphinDB.h"
#include "AsynWorker.h"
namespace dolphindb {

DBConnectionPoolImpl::DBConnectionPoolImpl(const string& hostName, int port, int threadNum, const string& userId, const string& password,
	bool loadBalance, bool highAvailability, bool compress,bool reConnect, bool python, PROTOCOL protocol, bool show_output) :shutDownFlag_(
        false), queue_(new SynchronizedQueue<Task>){
    latch_ = new CountDownLatch(threadNum);
    if(!loadBalance){
        for(int i = 0 ;i < threadNum; i++){
            SmartPointer<DBConnection> conn = new DBConnection(false, false, 7200, compress, python);
            conn->setProtocol(protocol);
            conn->setShowOutput(show_output);
			bool ret = conn->connect(hostName, port, userId, password, "", highAvailability, {},7200, reConnect);
            if(!ret)
                throw IOException("Failed to connect to " + hostName + ":" + std::to_string(port));
            sessionIds_.push_back(conn->getSessionId());
            connections_.push_back(conn);
            workers_.push_back(new Thread(new AsynWorker(*this,latch_, conn, queue_, taskStatus_, hostName, port, userId, password)));
            workers_.back()->start();
        }
    }
    else{
        SmartPointer<DBConnection> entryPoint = new DBConnection(false, false, 7200, compress, python);
        bool ret = entryPoint->connect(hostName, port, userId, password, "", highAvailability, {},7200, reConnect);
        if(!ret)
           throw IOException("Failed to connect to " + hostName + ":" + std::to_string(port));
        ConstantSP nodes = entryPoint->run("rpc(getControllerAlias(), getClusterLiveDataNodes{false})");
        INDEX nodeCount = nodes->size();
        vector<string> hosts(nodeCount);
        vector<int> ports(nodeCount);
        for(int i = 0; i < nodeCount; i++){
            string fields = nodes->getString(i);
            size_t p = fields.find(":");
            if(p == string::npos)
                throw  RuntimeException("Invalid data node address: " + fields);
            hosts[i] = fields.substr(0, p);
            ports[i] = std::atoi(fields.substr(p + 1, fields.size()).data());
        }
        for(int i = 0 ;i < threadNum; i++){
            SmartPointer<DBConnection> conn = new DBConnection(false, false, 7200, compress, python);
            conn->setProtocol(protocol);
            conn->setShowOutput(show_output);
            string &curhost = hosts[i % nodeCount];
            int &curport = ports[i % nodeCount];
            bool ret = conn->connect(curhost, curport, userId, password, "", highAvailability, {}, 7200, reConnect);
            if(!ret)
                throw IOException("Failed to connect to " + curhost + ":" + std::to_string(curport));
            sessionIds_.push_back(conn->getSessionId());
            connections_.push_back(conn);
            workers_.push_back(new Thread(new AsynWorker(*this,latch_, conn, queue_, taskStatus_, curhost, curport, userId, password)));
            workers_.back()->start();
        }
    }
}


}