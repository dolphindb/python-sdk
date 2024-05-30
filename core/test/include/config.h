// #pragma once

#include "DolphinDB.h"
#include "Util.h"
#include "BatchTableWriter.h"
#include "MultithreadedTableWriter.h"
#include "ConstantImp.h"
#include "Streaming.h"
#include "ConstantMarshall.h"
#include "TableImp.h"
#include "ConstantFactory.h"
#include "Format.h"
#include <vector>
#include <string>
#include <climits>
#include <thread>
#include <atomic>
#include <cstdio>
#include <random>
#include <fstream>
#include "ctime"
#include <cstdlib>
#include <stdexcept>

using namespace dolphindb;
using namespace std;
using std::atomic_long;
using std::cout;
using std::endl;

extern string hostName;
extern string errCode;
extern int port, ctl_port;
extern string table;
extern vector<int> listenPorts;
extern string alphas;
extern int pass, fail;
extern bool assertObj;
extern int vecSize;
extern vector<int> usedPorts;

extern int const INDEX_MAX_1;
extern int const INDEX_MIN_2;

extern vector<string> sites;
extern string raftsGroup;
extern vector<string> nodeNames;

using namespace std::chrono;


string hostName = "127.0.0.1";
string errCode = "0";
int port = 13902;
int ctl_port = 13900;
string table = "trades";
vector<int> listenPorts = { 18901,18902,18903,18904,18905,18906,18907,18908,18909,18910 };
vector<int> usedPorts = {};
string alphas = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
int pass, fail;
bool assertObj = true;
int vecSize = 20;

int const INDEX_MAX_1 = 1;
int const INDEX_MIN_2 = -1;

vector<string> sites = {"127.0.0.1:13902:datanode1", "127.0.0.1:13903:datanode2", "127.0.0.1:13904:datanode3"};
string raftsGroup = "11";
vector<string> nodeNames = {"datanode1", "datanode2", "datanode3"};

static DBConnection conn(false, false);
static DBConnection connReconn(false, false);
static DBConnection conn_compress(false, false, 7200, true);

// check server version
bool isNewVersion = true;
