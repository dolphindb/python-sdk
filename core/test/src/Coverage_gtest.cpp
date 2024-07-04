#include "config.h"

namespace {

class BatchTableWriterCoverageTest : public testing::Test {
protected:
    // Suite
    static void SetUpTestCase() {
        //DBConnection conn;
        conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456");
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
        }
        else {
            cout << "connect to " + hostName + ":" + std::to_string(port)<< endl;
        }
    }

    static void TearDownTestCase() {
        conn.close();
    }

    //Case
    virtual void SetUp() {
        cout<<"check connect...";
        try {
            ConstantSP res = conn.run("1+1");
        }
        catch(const std::exception& e) {
            conn.connect(hostName, port, "admin", "123456");
        }
            cout<<"ok"<<endl;
        }
    virtual void TearDown() {
        conn.run("undef all;");
    }
};

TEST_F(BatchTableWriterCoverageTest, test_insert_unaddTable) {
    BatchTableWriter btw(hostName, port, "admin", "123456", true);
    std::string re = "";
    try {
        btw.insert("", "xxxyyyxxx", int(0));
    }
    catch (std::exception &e) {
        re = e.what();
    }
    ASSERT_EQ(re, "Failed to insert into table, please use addTable to add infomation of database and table first.");
}

TEST_F(BatchTableWriterCoverageTest, test_addTable_DiskTable) {
    std::string re = R"""(
        try {dropDatabase("/tmp/test")} catch (ex) {}
        db=database(directory="/tmp/test")
        t = table(1..10 as a, 1..10 as b)
        saveTable(db, t, `dt)
    )""";
    conn.run(re);
    BatchTableWriter btw(hostName, port, "admin", "123456", true);
    btw.addTable("/tmp/test", "dt", false);
    btw.insert("/tmp/test", "dt", int(1), int(2));
    conn.run(R"""(try {dropDatabase("/tmp/test")} catch (ex) {})""");
}

TEST_F(BatchTableWriterCoverageTest, test_insertRow_unaddTable) {
    BatchTableWriter btw(hostName, port, "admin", "123456", true);
    std::string re = "";
    std::vector<ConstantSP> data;
    data.push_back(new Int(0));
    try {
        btw.insertRow("", "xxxyyyxxx", &data);
    }
    catch (std::exception &e) {
        re = e.what();
    }
    ASSERT_EQ(re, "Failed to insert into table, please use addTable to add infomation of database and table first.");
}

TEST_F(BatchTableWriterCoverageTest, test_insertRow_columns_noeq) {
    std::string re = R"""(
        share table(1..10 as a) as tmp;
    )""";
    conn.run(re);
    BatchTableWriter btw(hostName, port, "admin", "123456", true);
    btw.addTable("", "tmp", false);
    std::vector<ConstantSP> data;
    data.push_back(new Int(0));
    data.push_back(new Int(1));
    try {
        btw.insertRow("", "tmp", &data);
    }
    catch (std::exception &e) {
        re = e.what();
    }
    ASSERT_EQ(re, "Failed to insert into table, number of arguments must match the number of columns of table.");
    conn.run(R"""(undef all)""");
}

TEST_F(BatchTableWriterCoverageTest, test_createObject1) {
    std::string re = R"""(
        share table(1..10 as a) as tmp;
    )""";
    conn.run(re);
    BatchTableWriter btw(hostName, port, "admin", "123456", true);
    btw.addTable("", "tmp", false);
    btw.insert("", "tmp", ConstantSP(new Int(10)));
    conn.run(R"""(undef all)""");
}

TEST_F(BatchTableWriterCoverageTest, test_createObject2) {
    std::string re = R"""(
        share table(symbol(["a", "b"]) as a) as tmp;
    )""";
    conn.run(re);
    BatchTableWriter btw(hostName, port, "admin", "123456", true);
    btw.addTable("", "tmp", false);
    btw.insert("", "tmp", "a");
    conn.run(R"""(undef all)""");
}

TEST_F(BatchTableWriterCoverageTest, test_createObject3) {
    std::string re = R"""(
        share table(1:0, ["a"], [INT]) as tmp;
    )""";
    conn.run(re);
    BatchTableWriter btw(hostName, port, "admin", "123456", true);
    btw.addTable("", "tmp", false);
    unsigned char *data = new unsigned char[16];
    for (int i = 0; i < 16; ++i)
        data[i] = 0;
    try {
        btw.insert("", "tmp", data);
    }
    catch (std::exception &ex) {}
    delete data;
    conn.run(R"""(undef all)""");
}


class ConcurrentCoverageTest : public testing::Test {
protected:
    // Suite
    static void SetUpTestCase() {
        //DBConnection conn;
        conn.initialize();
        bool ret = conn.connect(hostName, port, "admin", "123456");
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
        }
        else {
            cout << "connect to " + hostName + ":" + std::to_string(port)<< endl;
        }
    }

    static void TearDownTestCase() {
        conn.close();
    }

    //Case
    virtual void SetUp() {
        cout<<"check connect...";
        try {
            ConstantSP res = conn.run("1+1");
        }
        catch(const std::exception& e) {
            conn.connect(hostName, port, "admin", "123456");
        }
            cout<<"ok"<<endl;
        }
    virtual void TearDown() {
        conn.run("undef all;");
    }
};


TEST_F(ConcurrentCoverageTest, test_ConditionalVariable) {
    ConditionalVariable cv;
    Mutex mutex;
    ThreadSP t = new Thread(new Executor([&](){
        cv.wait(mutex);
    }));
    t->start();
    Util::sleep(100);
    cv.notify();
    t->join();
}


TEST_F(ConcurrentCoverageTest, test_LockGuard_double_unlock) {
    Mutex mutex;
    LockGuard<Mutex> guard(&mutex);
    guard.unlock();
    guard.unlock();
}


TEST_F(ConcurrentCoverageTest, test_LockGuard_unlock_and_destruct) {
    Mutex mutex;
    {
        LockGuard<Mutex> guard(&mutex);
        guard.unlock();
    }
}


TEST_F(ConcurrentCoverageTest, test_Future_wait_lt_600ms) {
    Future<int> future;
    ThreadSP tsp = new Thread(new Executor([&](){
        Util::sleep(500);
        future.set(100);
    }));
    tsp->start();
    long long t1 = Util::getEpochTime();
    bool re = future.wait(600);
    long long t2 = Util::getEpochTime();
    tsp->join();
    ASSERT_LT(t2 - t1, 600);
    ASSERT_GT(t2 - t1, 450);
    ASSERT_EQ(future.get(), 100);
    ASSERT_EQ(re, true);
}


TEST_F(ConcurrentCoverageTest, test_Future_wait_gt_600ms) {
    Future<int> future;
    ThreadSP tsp = new Thread(new Executor([&](){
        Util::sleep(700);
        future.set(100);
    }));
    tsp->start();
    long long t1 = Util::getEpochTime();
    bool re = future.wait(600);
    long long t2 = Util::getEpochTime();
    tsp->join();
    ASSERT_LT(t2 - t1, 650);
    ASSERT_GT(t2 - t1, 550);
    // ASSERT_EQ(future.get(), 100);
    ASSERT_EQ(re, false);
}


TEST_F(ConcurrentCoverageTest, test_Future_wait) {
    Future<int> future;
    ThreadSP tsp = new Thread(new Executor([&](){
        Util::sleep(500);
        future.set(100);
    }));
    tsp->start();
    long long t1 = Util::getEpochTime();
    future.wait();
    long long t2 = Util::getEpochTime();
    tsp->join();
    ASSERT_LT(t2 - t1, 510);
    ASSERT_GT(t2 - t1, 450);
    ASSERT_EQ(future.get(), 100);
}


TEST_F(ConcurrentCoverageTest, test_ConditionalNotifier_wait) {
    ConditionalNotifier cn;
    int re = 0;
    long long t1 = 0, t2 = 0;
    ThreadSP tsp = new Thread(new Executor([&](){
        t1 = Util::getEpochTime();
        cn.wait();
        t2 = Util::getEpochTime();
        re = 1;
    }));
    tsp->start();
    Util::sleep(100);
    cn.notify();
    tsp->join();
    ASSERT_EQ(re, 1);
    ASSERT_LT(t2 - t1, 110);
    ASSERT_GT(t2 - t1, 90);
}


TEST_F(ConcurrentCoverageTest, test_ConditionalNotifier_wait_lt100ms) {
    ConditionalNotifier cn;
    int re = 0;
    long long t1 = 0, t2 = 0;
    bool wt;
    ThreadSP tsp = new Thread(new Executor([&](){
        t1 = Util::getEpochTime();
        wt = cn.wait(100);
        t2 = Util::getEpochTime();
        re = 1;
    }));
    tsp->start();
    Util::sleep(90);
    cn.notify();
    tsp->join();
    ASSERT_EQ(re, 1);
    ASSERT_LT(t2 - t1, 100);
    ASSERT_GT(t2 - t1, 80);
    ASSERT_EQ(wt, true);
}


TEST_F(ConcurrentCoverageTest, test_ConditionalNotifier_wait_gt100ms) {
    ConditionalNotifier cn;
    int re = 0;
    long long t1 = 0, t2 = 0;
    bool wt;
    ThreadSP tsp = new Thread(new Executor([&](){
        t1 = Util::getEpochTime();
        wt = cn.wait(100);
        t2 = Util::getEpochTime();
        re = 1;
    }));
    tsp->start();
    Util::sleep(110);
    cn.notify();
    tsp->join();
    ASSERT_EQ(re, 1);
    ASSERT_LT(t2 - t1, 110);
    ASSERT_GT(t2 - t1, 90);
    ASSERT_EQ(wt, false);
}


TEST_F(ConcurrentCoverageTest, test_ConditionalNotifier_notifyAll) {
    ConditionalNotifier cn;
    int re1 = 0, re2 = 0;
    ThreadSP tsp1 = new Thread(new Executor([&](){
        cn.wait();
        re1 = 1;
    }));
    ThreadSP tsp2 = new Thread(new Executor([&](){
        cn.wait();
        re2 = 2;
    }));
    tsp1->start();
    tsp2->start();
    Util::sleep(100);
    cn.notifyAll();
    tsp1->join();
    tsp2->join();
    ASSERT_EQ(re1, 1);
    ASSERT_EQ(re2, 2);
}


TEST_F(ConcurrentCoverageTest, test_BoundedBlockingQueue_full) {
    BoundedBlockingQueue<int> q(20);
    ThreadSP t = new Thread(new Executor([&](){
        for (int i = 0; i < 30; ++i) {
            q.push(i);
        }
    }));
    t->start();
    Util::sleep(100);
    int re = 0;
    for (int i = 0; i < 30; ++i) {
        q.pop(re);
    }
    t->join();
}


TEST_F(ConcurrentCoverageTest, test_BoundedBlockingQueue_empty) {
    BoundedBlockingQueue<int> q(20);
    ThreadSP t = new Thread(new Executor([&](){
        int re = 0;
        for (int i = 0; i < 30; ++i) {
            q.pop(re);
        }
    }));
    t->start();
    Util::sleep(100);
    int re = 0;
    for (int i = 0; i < 30; ++i) {
        q.push(i);
    }
    t->join();
}


TEST_F(ConcurrentCoverageTest, test_SynchronizedQueue_peek) {
    SynchronizedQueue<int> q;
    int data = 0;
    bool re;
    re = q.peek(data);
    ASSERT_EQ(re, false);

    q.push(1);
    re = q.peek(data);
    ASSERT_EQ(re, true);
    ASSERT_EQ(data, 1);
    ASSERT_EQ(q.size(), 1);
    re = q.pop(data);
    ASSERT_EQ(re, true);
    ASSERT_EQ(data, 1);
    ASSERT_EQ(q.size(), 0);
}


TEST_F(ConcurrentCoverageTest, test_SynchronizedQueue_blockingPop) {
    SynchronizedQueue<int> q;
    int data = 0;
    ThreadSP tsp = new Thread(new Executor([&](){
        q.blockingPop(data);
    }));
    tsp->start();
    Util::sleep(100);
    q.push(2);
    tsp->join();
    ASSERT_EQ(data, 2);

    q.push(1);
    q.push(2);
    q.push(3);
    ThreadSP tsp2 = new Thread(new Executor([&](){
        for (int i = 0; i < 3; ++i) {
            q.blockingPop(data);
        }
    }));
    tsp2->start();
    tsp2->join();
    ASSERT_EQ(data, 3);
}


TEST_F(ConcurrentCoverageTest, test_SynchronizedQueue_clear) {
    SynchronizedQueue<int> q;
    q.push(1);
    q.push(2);
    q.push(3);
    ASSERT_EQ(q.size(), 3);
    q.clear();
    ASSERT_EQ(q.size(), 0);
}


TEST_F(ConcurrentCoverageTest, test_Thread_isRunning_emptyRunnableSP) {
    RunnableSP run;
    ThreadSP t = new Thread(run);
    ASSERT_EQ(t->isRunning(), false);
    ASSERT_EQ(t->isComplete(), false);
    ASSERT_EQ(t->isStarted(), false);
}


TEST_F(ConcurrentCoverageTest, test_Thread_isRunning) {
    int stage = 0;
    RunnableSP run = new Executor([&](){
        stage = 1;
        Util::sleep(1000);
        stage = 2;
    });
    ThreadSP t = new Thread(run);
    ASSERT_EQ(t->isRunning(), false);
    ASSERT_EQ(t->isComplete(), false);
    ASSERT_EQ(t->isStarted(), false);
    ASSERT_EQ(stage, 0);

    t->start();
    Util::sleep(100);
    ASSERT_EQ(t->isRunning(), true);
    ASSERT_EQ(t->isComplete(), false);
    ASSERT_EQ(t->isStarted(), true);
    ASSERT_EQ(stage, 1);

    t->join();
    ASSERT_EQ(t->isRunning(), false);
    ASSERT_EQ(t->isComplete(), true);
    ASSERT_EQ(t->isStarted(), true);
    ASSERT_EQ(stage, 2);
}


TEST_F(ConcurrentCoverageTest, test_SemLock_tryAcquire_lt100ms) {
    Semaphore sem(1);
    sem.acquire();
    bool re;
    SemLock lock(sem);
    long long t1, t2;
    ThreadSP t = new Thread(new Executor([&](){
        t1 = Util::getEpochTime();
        re = lock.tryAcquire(100);
        t2 = Util::getEpochTime();
    }));
    t->start();
    Util::sleep(50);
    sem.release();
    t->join();
    ASSERT_EQ(re, true);
    ASSERT_LT(t2 - t1, 55);
    ASSERT_GT(t2 - t1, 45);
}


TEST_F(ConcurrentCoverageTest, test_SemLock_tryAcquire_gt100ms) {
    Semaphore sem(1);
    sem.acquire();
    bool re;
    SemLock lock(sem);
    long long t1, t2;
    ThreadSP t = new Thread(new Executor([&](){
        t1 = Util::getEpochTime();
        re = lock.tryAcquire(100);
        t2 = Util::getEpochTime();
    }));
    t->start();
    Util::sleep(150);
    sem.release();
    t->join();
    ASSERT_EQ(re, false);
    ASSERT_LT(t2 - t1, 105);
    ASSERT_GT(t2 - t1, 95);
}


TEST_F(ConcurrentCoverageTest, test_SemLock_release) {
    Semaphore sem(1);
    sem.release();
}


TEST_F(ConcurrentCoverageTest, test_Signal) {
    Signal sig(true, true);
    sig.tryWait(100);
    ASSERT_EQ(sig.isSignaled(), false);
}


TEST_F(ConcurrentCoverageTest, test_BlockingQueue_emplace) {
    BlockingQueue<int> q(20);
    ThreadSP t = new Thread(new Executor([&](){
        for (int i = 0; i < 30; ++i) {
            q.emplace(int(i));
        }
    }));
    t->start();
    Util::sleep(100);
    int data;
    for (int i = 0; i < 30; ++i) {
        q.pop(data);
    }
    t->join();
}


TEST_F(ConcurrentCoverageTest, test_BlockingQueue_poll_waitlt0ms) {
    BlockingQueue<int> q(20);
    q.push(1);
    int data;
    bool re;
    re = q.poll(data, -1);
    ASSERT_EQ(re, true);
    ASSERT_EQ(data, 1);
}

TEST_F(ConcurrentCoverageTest, test_BlockingQueue_poll_waitlt100ms) {
    BlockingQueue<int> q(20);
    bool re;
    int data;
    q.push(0);
    ThreadSP t = new Thread(new Executor([&](){
        Util::sleep(80);
        q.push(1);
    }));
    t->start();
    re = q.poll(data, 100);
    ASSERT_EQ(re, true);
    ASSERT_EQ(data, 0);
    long long t1 = Util::getEpochTime();
    re = q.poll(data, 100);
    long long t2 = Util::getEpochTime();
    t->join();
    ASSERT_EQ(re, true);
    ASSERT_EQ(data, 1);
    ASSERT_LT(t2 - t1, 85);
    ASSERT_GT(t2 - t1, 75);
}


TEST_F(ConcurrentCoverageTest, test_BlockingQueue_poll_waitgt100ms) {
    BlockingQueue<int> q(20);
    bool re;
    int data;
    q.push(0);
    ThreadSP t = new Thread(new Executor([&](){
        Util::sleep(120);
        q.push(1);
    }));
    t->start();
    re = q.poll(data, 100);
    ASSERT_EQ(re, true);
    ASSERT_EQ(data, 0);
    long long t1 = Util::getEpochTime();
    re = q.poll(data, 100);
    long long t2 = Util::getEpochTime();
    t->join();
    ASSERT_EQ(re, false);
    ASSERT_EQ(data, 0);
    ASSERT_LT(t2 - t1, 105);
    ASSERT_GT(t2 - t1, 95);
}


TEST_F(ConcurrentCoverageTest, test_CountDownLatch_waitlt100ms) {
    CountDownLatch a(1);
    bool re;
    ThreadSP t = new Thread(new Executor([&](){
        Util::sleep(80);
        a.countDown();
    }));
    t->start();
    long long t1 = Util::getEpochTime();
    re = a.wait(100);
    long long t2 = Util::getEpochTime();
    t->join();
    ASSERT_EQ(re, true);
    ASSERT_LT(t2 - t1, 85);
    ASSERT_GT(t2 - t1, 75);
}


TEST_F(ConcurrentCoverageTest, test_CountDownLatch_waitgt100ms) {
    CountDownLatch a(1);
    bool re;
    ThreadSP t = new Thread(new Executor([&](){
        Util::sleep(120);
        a.countDown();
    }));
    t->start();
    long long t1 = Util::getEpochTime();
    re = a.wait(100);
    long long t2 = Util::getEpochTime();
    t->join();
    ASSERT_EQ(re, false);
    ASSERT_LT(t2 - t1, 105);
    ASSERT_GT(t2 - t1, 95);
}


TEST_F(ConcurrentCoverageTest, test_CountDownLatch_waitlt0ms) {
    CountDownLatch a(1);
    bool re;
    ThreadSP t = new Thread(new Executor([&](){
        Util::sleep(80);
        a.countDown();
    }));
    t->start();
    long long t1 = Util::getEpochTime();
    re = a.wait(-1);
    long long t2 = Util::getEpochTime();
    t->join();
    ASSERT_EQ(re, false);
    ASSERT_LT(t2 - t1, 5);
}


TEST_F(ConcurrentCoverageTest, test_CountDownLatch_getCount) {
    CountDownLatch a(1);
    ASSERT_EQ(a.getCount(), 1);
}


TEST_F(ConcurrentCoverageTest, test_CountDownLatch_clear) {
    CountDownLatch a(1);
    ASSERT_EQ(a.getCount(), 1);
    a.clear();
    ASSERT_EQ(a.getCount(), 0);
}


TEST_F(ConcurrentCoverageTest, test_CountDownLatch_reset) {
    CountDownLatch a(1);
    ASSERT_EQ(a.resetCount(2), false);
    a.countDown();
    ASSERT_EQ(a.resetCount(-1), false);
    ASSERT_EQ(a.resetCount(0), false);
    ASSERT_EQ(a.resetCount(2), true);
    ASSERT_EQ(a.getCount(), 2);
}


TEST_F(ConcurrentCoverageTest, test_Semaphore_tryAcquire_waitlt100ms) {
    Semaphore sem(1);
    sem.acquire();
    bool re;
    ThreadSP t = new Thread(new Executor([&](){
        Util::sleep(80);
        sem.release();
    }));
    t->start();
    long long t1 = Util::getEpochTime();
    re = sem.tryAcquire(100);
    long long t2 = Util::getEpochTime();
    t->join();
    ASSERT_EQ(re, true);
    ASSERT_LT(t2 - t1, 85);
    ASSERT_GT(t2 - t1, 75);
}


TEST_F(ConcurrentCoverageTest, test_Semaphore_tryAcquire_waitgt100ms) {
    Semaphore sem(1);
    sem.acquire();
    bool re;
    ThreadSP t = new Thread(new Executor([&](){
        Util::sleep(120);
        sem.release();
    }));
    t->start();
    long long t1 = Util::getEpochTime();
    re = sem.tryAcquire(100);
    long long t2 = Util::getEpochTime();
    t->join();
    ASSERT_EQ(re, false);
    ASSERT_LT(t2 - t1, 105);
    ASSERT_GT(t2 - t1, 95);
}



}