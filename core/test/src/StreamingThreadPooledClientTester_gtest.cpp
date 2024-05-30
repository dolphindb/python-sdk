namespace STPCT
{
    class StreamingThreadPooledClientTester : public testing::Test, public ::testing::WithParamInterface<int>
    {
    public:
        // Suite
        static void SetUpTestCase()
        {
            // DBConnection conn;

            conn.initialize();
            bool ret = conn.connect(hostName, port, "admin", "123456");
            if (!ret)
            {
                cout << "Failed to connect to the server" << endl;
            }
            else
            {
                cout << "connect to " + hostName + ":" + std::to_string(port) << endl;
                isNewVersion = conn.run("flag = 1;v = split(version(), ' ')[0];\
                                tmp=int(v.split('.'));\
                                if((tmp[0]==2 && tmp[1]==00 && tmp[2]>=9 )||(tmp[0]==2 && tmp[1]==10)){flag=1;}else{flag=0};\
                                flag")
                                   ->getBool();
            }
        }
        static void TearDownTestCase()
        {
            usedPorts.clear();
            conn.close();
        }

        // Case
        virtual void SetUp()
        {
            cout << "check connect...";
            try
            {
                ConstantSP res = conn.run("1+1");
            }
            catch(const std::exception& e)
            {
                conn.connect(hostName, port, "admin", "123456");
            }

            cout << "ok" << endl;
            string del_streamtable = "login(\"admin\",\"123456\");"
                                     "try{ dropStreamTable(`outTables);}catch(ex){};"
                                     "try{ dropStreamTable(`st1);}catch(ex){};"
                                     "try{ dropStreamTable(`arrayVectorTable);}catch(ex){};";
            conn.run(del_streamtable);
        }
        virtual void TearDown()
        {
            string del_streamtable = "login(\"admin\",\"123456\");"
                                     "try{ dropStreamTable(`outTables);}catch(ex){};"
                                     "try{ dropStreamTable(`st1);}catch(ex){};"
                                     "try{ dropStreamTable(`arrayVectorTable);}catch(ex){};";
            conn.run(del_streamtable+"go;undef all;");
        }
    };

    static void createSharedTableAndReplay(int rows)
    {
        string script = "login(\"admin\",\"123456\")\n\
                st1 = streamTable(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE])\n\
                enableTableShareAndPersistence(table=st1, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0)\n\
                go\n\
                setStreamTableFilterColumn(outTables, `sym)";
        conn.run(script);

        string replayScript = "n = " + to_string(rows) + ";table1_STPCT = table(100:0, `datetimev`timestampv`sym`price1`price2, [DATETIME, TIMESTAMP, SYMBOL, DOUBLE, DOUBLE]);\
                tableInsert(table1_STPCT, 2012.01.01T01:21:23 + 1..n, 2018.12.01T01:21:23.000 + 1..n, take(`a`b`c,n), rand(100,n)+rand(1.0, n), rand(100,n)+rand(1.0, n));\
                replay(inputTables=table1_STPCT, outputTables=`outTables, dateColumn=`timestampv, timeColumn=`timestampv)";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withAllDataType()
    {
        srand(time(NULL));
        int scale32 = rand() % 9;
        int scale64 = rand() % 18;
        string replayScript = "colName =  `cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cblob`cipaddr`cuuid`cint128`cdecimal32`cdecimal64;"
                              "colType = [BOOL, CHAR, SHORT, INT,LONG, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING, BLOB, IPADDR, UUID, INT128, DECIMAL32(" +
                              to_string(scale32) + "), DECIMAL64(" + to_string(scale64) + ")];"
                                                                                          "st1 = streamTable(100:0,colName, colType);"
                                                                                          "enableTableShareAndPersistence(table=st1, tableName=`outTables, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);go;"
                                                                                          "setStreamTableFilterColumn(outTables, `csymbol);go;"
                                                                                          "row_num = 1000;"
                                                                                          "table1_SPCT = table(100:0, colName, colType);"
                                                                                          "col1 = rand(2 ,row_num);col2 = rand(256 ,row_num);col3 = rand(-row_num..row_num ,row_num);col4 = rand(-row_num..row_num ,row_num);col5 = rand(-row_num..row_num ,row_num);col6 = rand(0..row_num ,row_num);col7 = rand(0..row_num ,row_num);"
                                                                                          "col8 = rand(0..row_num ,row_num);col9 = rand(0..row_num ,row_num);col10 = rand(0..row_num ,row_num);col11 = rand(0..row_num ,row_num);col12 = rand(0..row_num ,row_num);col13 = rand(0..row_num ,row_num);col14= rand(0..row_num ,row_num);"
                                                                                          "col15 = rand(round(row_num,2) ,row_num);col16 = rand(round(row_num,2) ,row_num);col17 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col18 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col19 =rand(`a`s`sym`d`zdg`f`y`ddvb,row_num);col20 =  take(ipaddr(\"192.168.1.13\"),row_num);"
                                                                                          "col21 = take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num);col22 = take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num);col23=rand((-1000..1000)/1000.0000$DECIMAL32(" +
                              to_string(scale32) + "),row_num);col24=rand((-1000..1000)/1000.0000$DECIMAL64(" + to_string(scale64) + "),row_num);go;"
                                                                                                                                     "for (i in 0..(row_num-1)){tableInsert(table1_SPCT,col1[i],col2[i],col3[i],col4[i],col5[i],col6[i],col7[i],col8[i],col9[i],col10[i],col11[i],col12[i],col13[i],col14[i],col15[i],col16[i],col17[i],col18[i],col19[i],col20[i],col21[i],col22[i],col23[i],col24[i])};"
                                                                                                                                     "go;"
                                                                                                                                     "replay(inputTables=table1_SPCT, outputTables=`outTables, dateColumn=`ctimestamp, timeColumn=`ctimestamp);";
        conn.run(replayScript);
    }

    static void createSharedTableAndReplay_withArrayVector()
    {
        string replayScript = "colName =  `ts`cbool`cchar`cshort`cint`clong`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`cipaddr`cuuid`cint128;"
                              "colType = [TIMESTAMP,BOOL[], CHAR[], SHORT[], INT[],LONG[], DATE[], MONTH[], TIME[], MINUTE[], SECOND[], DATETIME[], TIMESTAMP[], NANOTIME[], NANOTIMESTAMP[], FLOAT[], DOUBLE[], IPADDR[], UUID[], INT128[]];"
                              "st1 = streamTable(100:0,colName, colType);"
                              "enableTableShareAndPersistence(table=st1, tableName=`arrayVectorTable, asynWrite=true, compress=true, cacheSize=200000, retentionMinutes=180, preCache = 0);go;"
                              "row_num = 1000;"
                              "table1_SPCT = table(100:0, colName, colType);"
                              "col0=rand(0..row_num ,row_num);col1 = arrayVector(1..row_num,rand(2 ,row_num));col2 = arrayVector(1..row_num,rand(256 ,row_num));col3 = arrayVector(1..row_num,rand(-row_num..row_num ,row_num));"
                              "col4 = arrayVector(1..row_num,rand(-row_num..row_num ,row_num));col5 = arrayVector(1..row_num,rand(-row_num..row_num ,row_num));col6 = arrayVector(1..row_num,rand(0..row_num ,row_num));"
                              "col7 = arrayVector(1..row_num,rand(0..row_num ,row_num));col8 = arrayVector(1..row_num,rand(0..row_num ,row_num));col9 = arrayVector(1..row_num,rand(0..row_num ,row_num));"
                              "col10 = arrayVector(1..row_num,rand(0..row_num ,row_num));col11 = arrayVector(1..row_num,rand(0..row_num ,row_num));col12 = arrayVector(1..row_num,rand(0..row_num ,row_num));"
                              "col13 = arrayVector(1..row_num,rand(0..row_num ,row_num));col14= arrayVector(1..row_num,rand(0..row_num ,row_num));col15 = arrayVector(1..row_num,rand(round(row_num,2) ,row_num));"
                              "col16 = arrayVector(1..row_num,rand(round(row_num,2) ,row_num));col17 =  arrayVector(1..row_num,take(ipaddr(\"192.168.1.13\"),row_num));col18 = arrayVector(1..row_num,take(uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\"),row_num));"
                              "col19 = arrayVector(1..row_num,take(int128(\"e1671797c52e15f763380b45e841ec32\"),row_num));go;"
                              "for (i in 0..(row_num-1)){tableInsert(table1_SPCT,col0[0][i],col1[0][i],col2[0][i],col3[0][i],col4[0][i],col5[0][i],col6[0][i],col7[0][i],col8[0][i],col9[0][i],col10[0][i],col11[0][i],col12[0][i],col13[0][i],col14[0][i],col15[0][i],col16[0][i],col17[0][i],col18[0][i],col19[0][i])};go;"
                              "replay(inputTables=table1_SPCT, outputTables=`arrayVectorTable, dateColumn=`ts, timeColumn=`ts);";
        conn.run(replayScript);
    }

    void StopCurNode(string cur_node)
    {
        DBConnection conn1(false, false);
        conn1.connect(hostName, ctl_port, "admin", "123456");

        conn1.run("try{stopDataNode(\"" + cur_node + "\")}catch(ex){};");
        cout << cur_node + " has stopped..." << endl;
        // std::this_thread::sleep_for(std::chrono::seconds(5));
        // std::this_thread::yield();
        conn1.run("try{startDataNode(\"" + cur_node + "\")}catch(ex){};");
        if (conn1.run("(exec state from getClusterPerf() where name = `" + cur_node + ")[0]")->getInt() == 1)
        {
            cout << "restart the datanode: " + cur_node + " successfully..." << endl;
            conn1.close();
            return;
        }
        else
        {
            cout << "restart datanode failed." << endl;
        }
    }
    INSTANTIATE_TEST_CASE_P(StreamingReverse, StreamingThreadPooledClientTester, testing::Values(0, rand() % 1000 + 13000));
    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_1)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total == 1000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadPooledClient client(listenport, 1);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 1);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_2)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total == 1000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadPooledClient client(listenport, 2);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 2);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_4)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total == 1000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadPooledClient client(listenport, 4);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 4);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_8)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total == 1000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadPooledClient client(listenport, 8);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 8);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_16)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total == 1000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadPooledClient client(listenport, 16);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 16);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_32)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total == 1000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadPooledClient client(listenport, 32);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 32);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_hostNull)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        usedPorts.push_back(listenport);
        Util::sleep(1000);
        EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        ThreadPooledClient client(listenport, 2);
        EXPECT_ANY_THROW(client.subscribe("", port, onehandler, "outTables", "actionTest", 0));
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_portNull)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        usedPorts.push_back(listenport);
        Util::sleep(1000);
        EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());
        ThreadPooledClient client(listenport, 2);
        EXPECT_ANY_THROW(client.subscribe(hostName, NULL, onehandler, "outTables", "actionTest", 0));
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_actionNameNull)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total == 1000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "", 0);
            notify.wait();
            TableSP stat = conn.run("getStreamingStat().pubTables");

            EXPECT_EQ(stat->getColumn(0)->getRow(0)->getString(), "outTables");
            EXPECT_EQ(stat->getColumn(2)->getRow(0)->getInt(), 1000);
            EXPECT_EQ(stat->getColumn(3)->getRow(0)->getString(), "");

            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_tableNameNull)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, "", "actionTest", 0, false));
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_offsetNegative)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", -1));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", -1);
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 0);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_filter)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total > 0)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        VectorSP filter = Util::createVector(DT_SYMBOL, 1, 1);
        filter->setString(0, "b");
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, filter));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, filter);
            cout << "total size:" << msg_total << endl;
            notify.wait();

            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total > 0, true);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_msgAsTable)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            EXPECT_EQ(msg->getForm(), 6);
            msg_total += msg->rows();
            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr, true));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr, true);
            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_onehandler_allowExists)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total == 1000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 2);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr, false, true));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true, nullptr, false, true);
            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_resub_false)
    {
        // STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 1);
        EXPECT_ANY_THROW(auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, false));
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_resub_true)
    {
        // STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        auto onehandler = [&](Message msg)
        {
            msg_total += 1;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        ThreadPooledClient client(listenport, 1);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, true);
            Util::sleep(5000);
            client.unsubscribe(hostName, port, "outTables", "actionTest");
        }

        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_subscribe_twice)
    {
        STPCT::createSharedTableAndReplay(1000);
        int msg_total = 0;
        Signal notify;
        Mutex mutex;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;

            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        int threadCount = rand() % 10 + 1;
        ThreadPooledClient client(listenport, threadCount);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, false));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, false);
            EXPECT_ANY_THROW(auto threadVec1 = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, false));
            EXPECT_EQ(threadVec.size(), threadCount);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_withAllDataType)
    {
        STPCT::createSharedTableAndReplay_withAllDataType();
        int msg_total = 0;

        Signal notify;
        Mutex mutex;

        TableSP ex_table = conn.run("select * from outTables");
        int index = 0;

        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;

            /***
             * Msg will not be in order of original table when Multithread-subscribing.
             ***/

            // for(auto i=0; i<ex_table->columns(); i++)
            //     EXPECT_EQ(ex_table->getColumn(i)->get(index)->getString(), msg->get(i)->getString());

            index++;

            if (msg_total == 1000)
            {
                notify.set();
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        int threadCount = rand() % 10 + 1;
        ThreadPooledClient client(listenport, threadCount);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            EXPECT_EQ(threadVec.size(), threadCount);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_client_threadCount_arrayVector)
    {
        STPCT::createSharedTableAndReplay_withArrayVector();
        int msg_total = 0;

        Signal notify;
        Mutex mutex;

        TableSP ex_tab = conn.run("select * from arrayVectorTable");
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total == 1000)
            {
                notify.set();
            }
            for (auto i = 1; i < msg->size(); i++)
            {
                // EXPECT_EQ(ex_tab->getColumn(i)->getType(), msg->get(i)->getType());
                EXPECT_EQ(msg->get(i)->getForm(), DF_VECTOR);
            }
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;

        int threadCount = rand() % 10 + 1;
        ThreadPooledClient client(listenport, threadCount);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "arrayVectorTable", "arrayVectorTableTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "arrayVectorTable", "arrayVectorTableTest", 0);
            EXPECT_EQ(threadVec.size(), threadCount);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "arrayVectorTable", "arrayVectorTableTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`arrayVectorTable) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_hugetable)
    {
        STPCT::createSharedTableAndReplay(1000000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            msg_total += 1;
            if (msg_total % 100000 == 0)
                cout << "now subscribed rows: " << msg_total << endl;
            if (msg_total == 1000000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadPooledClient client(listenport, 4);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0);
            EXPECT_EQ(threadVec.size(), 4);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }

    TEST_P(StreamingThreadPooledClientTester, test_subscribe_hugetable_msgAsTable)
    {
        STPCT::createSharedTableAndReplay(1000000);
        int msg_total = 0;

        Signal notify;
        Mutex mutex;
        auto onehandler = [&](Message msg)
        {
            LockGuard<Mutex> lock(&mutex);
            EXPECT_EQ(msg->getForm(), DF_TABLE);
            msg_total += msg->rows();
            if (msg_total % 100000 == 0)
                cout << "now subscribed rows: " << msg_total << endl;
            if (msg_total == 1000000)
            {
                notify.set();
            }
            // cout << msg->getString() << endl;
        };

        int listenport = GetParam();
        cout << "current listenport is " << listenport << endl;
        ThreadPooledClient client(listenport, 4);
        if (!isNewVersion && listenport == 0)
        {
            EXPECT_ANY_THROW(client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, false, nullptr, true));
        }
        else
        {
            auto threadVec = client.subscribe(hostName, port, onehandler, "outTables", "actionTest", 0, false, nullptr, true);
            EXPECT_EQ(threadVec.size(), 4);

            notify.wait();
            cout << "total size:" << msg_total << endl;
            client.unsubscribe(hostName, port, "outTables", "actionTest");

            Util::sleep(1000);
            EXPECT_TRUE(conn.run("(exec count(*) from getStreamingStat()[`pubConns] where tables =`outTables) ==0")->getBool());

            EXPECT_EQ(msg_total, 1000000);
            for (auto &t : threadVec)
            {
                EXPECT_EQ(client.getQueueDepth(t), 0);
            }
        }
        usedPorts.push_back(listenport);
    }
}
