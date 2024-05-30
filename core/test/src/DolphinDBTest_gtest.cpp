class DolphinDBTest:public testing::Test
{
protected:
    //Suite
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
    static void TearDownTestCase(){
        conn.close();
    }

    //Case
    virtual void SetUp()
    {
        cout<<"check connect...";
		try
		{
			ConstantSP res = conn.run("1+1");
		}
		catch(const std::exception& e)
		{
			conn.connect(hostName, port, "admin", "123456");
		}
		
        cout<<"ok"<<endl;
    }
    virtual void TearDown()
    {
        conn.run("undef all;");
    }
};

int64_t getTimeStampMs() {
	return Util::getEpochTime();
}

string genRandString(int maxSize) {
	string result;
	int size = rand() % maxSize;
	for (int i = 0;i < size;i++) {
		int r = rand() % alphas.size();
		result += alphas[r];
	}
	return result;
}

static string getSymbolVector(const string& name, int size)
{
	int kind = 50;
	int count = size / kind;

	string result;
	char temp[200];
	result += name;
	sprintf(temp, "=symbol(array(STRING,%d,%d,`0));", count, count);
	result += temp;
	for (int i = 1;i<kind;i++) {
		sprintf(temp, ".append!(symbol(array(STRING,%d,%d,`%d)));", count, count, i);
		result += name;
		result += string(temp);
	}

	return result;
}

TEST(DolphinDBDataTypeTest,testDataTypeWithoutConnect){
        VectorSP arrayVector = Util::createArrayVector(DT_DATETIME_ARRAY, 0, 100);
        for (int i = 0; i < 10; i++) {
            VectorSP time = Util::createVector(DT_DATETIME, 5);
            for (int j = 0; j < 5; j++) {
                time->set(j, Util::createDateTime(j * 100000));
            }
            arrayVector->append(time);
        }
		cout<< arrayVector->getString()<<endl;

		ConstantSP intval= Util::createConstant(DT_INT);
		intval->setInt(1);
		EXPECT_EQ(intval->getInt(),1);

		ConstantSP boolval= Util::createConstant(DT_BOOL);
		boolval->setBool(1);
		EXPECT_EQ(boolval->getBool(),true);

		ConstantSP floatval= Util::createConstant(DT_FLOAT);
		floatval->setFloat(2.33);
		EXPECT_EQ(floatval->getFloat(),(float)2.33);

		ConstantSP longval= Util::createConstant(DT_LONG);
		longval->setLong(10000000);
		EXPECT_EQ(longval->getLong(),(long)10000000);

		ConstantSP stringval= Util::createConstant(DT_STRING);
		stringval->setString("134asd");
		EXPECT_EQ(stringval->getString(),"134asd");

		ConstantSP dateval= Util::createConstant(DT_DATE);
		dateval=Util::createDate(1);
		EXPECT_EQ(dateval->getString(),"1970.01.02");

		ConstantSP minuteval= Util::createMinute(1439);
		EXPECT_EQ(minuteval->getString(),"23:59m");

		ConstantSP nanotimestampval= Util::createNanoTimestamp((long long)100000000000000000);
		EXPECT_EQ(nanotimestampval->getString(),"1973.03.03T09:46:40.000000000");

		ConstantSP uuidval= Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87");
		EXPECT_EQ(uuidval->getString(),"5d212a78-cc48-e3b1-4235-b4d91473ee87");

		ConstantSP ipaddrval= Util::parseConstant(DT_IP,"192.168.0.16");
		EXPECT_EQ(ipaddrval->getString(),"192.168.0.16");

		vector<string> colname={"col1","col2","col3"};
		vector<DATA_TYPE> coltype={DT_INT,DT_BLOB, DT_SYMBOL};
		TableSP tableval= Util::createTable(colname,coltype,0,3);
		cout<< tableval->getString()<<endl;	
}

TEST_F(DolphinDBTest,testSymbol){
	vector<string> expectResults = { "XOM","y" };
	string script;
	script += "x=`XOM`y;y=symbol x;y;";
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0;i < expectResults.size(); i++) {
		EXPECT_EQ(result->getString(i), expectResults[i]);
	}
}

TEST_F(DolphinDBTest,testSymbolBase){
	int64_t startTime, time;

	conn.run("v=symbol(string(1..2000000))");
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector: " << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run(getSymbolVector("v", 2000000));
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector optimize:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=table(symbol(string(1..2000000)) as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run(getSymbolVector("v", 2000000));
	conn.run("t=table(v as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector optimize:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=table(symbol(string(1..2000000)) as sym,symbol(string(1..2000000)) as sym1,symbol(string(1..2000000)) as sym2,symbol(string(1..2000000)) as sym3,symbol(string(1..2000000)) as sym4,symbol(string(1..2000000)) as sym5,symbol(string(1..2000000)) as sym6,symbol(string(1..2000000)) as sym7,symbol(string(1..2000000)) as sym8,symbol(string(1..2000000)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with same symbol vectors:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=table(symbol(take(string(0..20000),2000000)) as sym,symbol(take(string(20000..40000),2000000)) as sym1,symbol(take(string(40000..60000),2000000)) as sym2,symbol(take(string(60000..80000),2000000)) as sym3,symbol(take(string(80000..100000),2000000)) as sym4,symbol(take(string(100000..120000),2000000)) as sym5,symbol(take(string(120000..140000),2000000)) as sym6,symbol(take(string(140000..160000),2000000)) as sym7,symbol(take(string(160000..180000),2000000)) as sym8,symbol(take(string(180000..200000),2000000)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with diff symbol vectors:" << time << "ms" << endl;

	//    conn.run("undef(all)");
	//    conn.run("m=symbol(string(1..2000000))$1000:2000");
	//    startTime = getTimeStampMs();
	//    conn.run("m");
	//    time = getTimeStampMs()-startTime;
	//    cout << "symbol matrix:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("d=dict(symbol(string(1..2000000)),symbol(string(1..2000000)))");
	startTime = getTimeStampMs();
	conn.run("d");
	time = getTimeStampMs() - startTime;
	cout << "symbol dict:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("s=set(symbol(string(1..2000000)))");
	startTime = getTimeStampMs();
	conn.run("s");
	time = getTimeStampMs() - startTime;
	cout << "symbol set:" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=(symbol(string(1..2000000)),symbol(string(1..2000000)))");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "tuple symbol tuple:" << time << "ms" << endl;

	conn.run("undef(all)");
}

TEST_F(DolphinDBTest,testSymbolSmall){
	int64_t startTime, time;
	conn.run("v=symbol(string(1..200))");
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run(getSymbolVector("v", 200));
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector optimize:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(symbol(string(1..200)) as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run(getSymbolVector("v", 200));
	conn.run("t=table(v as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector optimize:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(symbol(string(1..200)) as sym,symbol(string(1..200)) as sym1,symbol(string(1..200)) as sym2,symbol(string(1..200)) as sym3,symbol(string(1..200)) as sym4,symbol(string(1..200)) as sym5,symbol(string(1..200)) as sym6,symbol(string(1..200)) as sym7,symbol(string(1..200)) as sym8,symbol(string(1..200)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with same symbol vectors:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(symbol(take(string(0..20000),200)) as sym,symbol(take(string(20000..40000),200)) as sym1,symbol(take(string(40000..60000),200)) as sym2,symbol(take(string(60000..80000),200)) as sym3,symbol(take(string(80000..100000),200)) as sym4,symbol(take(string(100000..120000),200)) as sym5,symbol(take(string(120000..140000),200)) as sym6,symbol(take(string(140000..160000),200)) as sym7,symbol(take(string(160000..180000),200)) as sym8,symbol(take(string(180000..200000),200)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with diff symbol vectors:" << time << "ms" << endl;
	conn.run("undef(all)");

	//    conn.run("m =symbol(string(1..200))$10:20");
	//    startTime = getTimeStampMs();
	//    conn.run("m");
	//    time = getTimeStampMs()-startTime;
	//    cout << "symbol matrix:" << time << "ms" << endl;
	//    conn.run("undef(all)");

	conn.run("d=dict(symbol(string(1..200)),symbol(string(1..200)))");
	startTime = getTimeStampMs();
	conn.run("d");
	time = getTimeStampMs() - startTime;
	cout << "symbol dict:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("s=set(symbol(string(1..200)))");
	startTime = getTimeStampMs();
	conn.run("s");
	time = getTimeStampMs() - startTime;
	cout << "symbol set:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=(symbol(string(1..200)),symbol(string(1..200)))");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "tuple symbol tuple:" << time << "ms" << endl;
	conn.run("undef(all)");
}

TEST_F(DolphinDBTest,testSymbolNull){
	int64_t startTime, time;
	conn.run("v=take(symbol(`cy`fty``56e`f65dfyfv),2000000)");
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym1,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym2,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym3,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym4,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym5,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym6,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym7,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym8,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with same symbol vectors:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(take(symbol(`cdwy`fty``56e`f652dfyfv),2000000) as sym,take(symbol(`cy`f8ty``56e`f65dfyfv),2000000) as sym1,take(symbol(`c2587y`fty``56e`f65dfyfv),2000000) as sym2,take(symbol(`cy````f65dfy4fv),2000000) as sym3,take(symbol(`cy```56e`f65dfgyfv),2000000) as sym4,take(symbol(`cy`fty``56e`12547),2000000) as sym5,take(symbol(`cy`fty``e`f65d728fyfv),2000000) as sym6,take(symbol(`cy`fty``56e`),2000000) as sym7,take(symbol(`cy`fty``56e`111),2000000) as sym8,take(symbol(`c412y`ft575y```f65dfyfv),2000000) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with diff symbol vectors:" << time << "ms" << endl;
	conn.run("undef(all)");

	//    conn.run("m =take(symbol(`cy`fty```f65dfyfv),2000000)$1000:2000");
	//    startTime = getTimeStampMs();
	//    conn.run("m");
	//    time = getTimeStampMs()-startTime;
	//    cout << "symbol matrix:" << time << "ms" << endl;
	//    conn.run("undef(all)");

	conn.run("d=dict(take(symbol(`cy`fty``56e`f65dfyfv),2000000),take(symbol(`cy`fty``56e`f65dfyfv),2000000))");
	startTime = getTimeStampMs();
	conn.run("d");
	time = getTimeStampMs() - startTime;
	cout << "symbol dict:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("s=set(take(symbol(`cy`fty``56e`f65dfyfv),2000000))");
	startTime = getTimeStampMs();
	conn.run("s");
	time = getTimeStampMs() - startTime;
	cout << "symbol set:" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=(take(symbol(`cy`fty``56e`f65dfyfv),2000000),take(symbol(`cy`fty``56e`f65dfyfv),2000000))");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "tuple symbol tuple:" << time << "ms" << endl;
	conn.run("undef(all)");
}



TEST_F(DolphinDBTest,testmixtimevectorUpload){
	VectorSP dates = Util::createVector(DT_ANY, 5, 100);
	dates->set(0, Util::createMonth(2016, 6));
	dates->set(1, Util::createDate(2016, 5, 16));
	dates->set(2, Util::createDateTime(2016, 6, 6, 6, 12, 12));
	dates->set(3, Util::createNanoTime(6, 28, 36, 00));
	dates->set(4, Util::createNanoTimestamp(2020, 8, 20, 2, 20, 20, 00));
	vector<ConstantSP> mixtimedata = { dates };
	vector<string> mixtimename = { "Mixtime" };
	conn.upload(mixtimename, mixtimedata);
}

TEST_F(DolphinDBTest,testFunctionDef){
	string script = "def funcAdd(a,b){return a + b};funcAdd(100,200);";
	ConstantSP result = conn.run(script);
	EXPECT_EQ(result->getString(), string("300"));
}


TEST_F(DolphinDBTest,testMatrix){
	vector<string> expectResults = { "{1,2}","{3,4}","{5,6}" };
	string script = "1..6$2:3";
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0;i < expectResults.size(); i++) {
		EXPECT_EQ(result->getString(i), expectResults[i]);
	}
}

TEST_F(DolphinDBTest,testTable){
	string script;
	script += "n=20000\n";
	script += "syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n";
	script += "mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price, 1..n as number,rand(syms,n) as sym_2);\n";
	script += "select min(number) as minNum, max(number) as maxNum from mytrades";

	ConstantSP table = conn.run(script);
	EXPECT_EQ(table->getColumn(0)->getString(0), string("1"));
	EXPECT_EQ(table->getColumn(1)->getString(0), string("20000"));
}

TEST_F(DolphinDBTest,testDictionary){
	string script;
	script += "dict(1 2 3,symbol(`IBM`MSFT`GOOG))";
	DictionarySP dict = conn.run(script);

	EXPECT_EQ(dict->get(Util::createInt(1))->getString(), string("IBM"));
	EXPECT_EQ(dict->get(Util::createInt(2))->getString(), string("MSFT"));
	EXPECT_EQ(dict->get(Util::createInt(3))->getString(), string("GOOG"));
}

TEST_F(DolphinDBTest,testSet){
	string script;
	script += "x=set(4 5 5 2 3 11 11 11 6 6  6 6  6);x;";
	ConstantSP set = conn.run(script);
	EXPECT_EQ(set->size(), 6);
}

TEST_F(DolphinDBTest,testCharVectorHash){
	vector<char> testValues{ 127,-127,12,0,-12,-128 };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 10,12,12,0,10,-1 },
	{ 41,18,12,0,4,-1 },
	{ 56,24,12,0,68,-1 },
	{ 30,5,12,0,23,-1 },
	{ 127,129,12,0,244,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createChar(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
		
	}
	VectorSP v = Util::createVector(DT_CHAR, 0);
	v->appendChar(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testShortVectorHash){
	vector<short> testValues{ 32767,-32767,12,0,-12,-32768 };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 7,2,12,0,10,-1 },
	{ 1,15,12,0,4,-1 },
	{ 36,44,12,0,68,-1 },
	{ 78,54,12,0,23,-1 },
	{ 4088,265,12,0,244,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createShort(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_SHORT, 0);
	v->appendShort(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testIntVectorHash){
	vector<int> testValues{ INT_MAX,INT_MAX*(-1),12,0,-12,INT_MIN };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 10,12,12,0,10,-1 },
	{ 7,9,12,0,4,-1 },
	{ 39,41,12,0,68,-1 },
	{ 65,67,12,0,23,-1 },
	{ 127,129,12,0,244,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createInt(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_INT, 0);
	v->appendInt(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testLongVectorHash){
	vector<long long> testValues{ LLONG_MAX,(-1)*LLONG_MAX,12,0,-12,LLONG_MIN };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 7,9,12,0,4,-1 },
	{ 41,0,12,0,29,-1 },
	{ 4,6,12,0,69,-1 },
	{ 78,80,12,0,49,-1 },
	{ 4088,4090,12,0,4069,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createLong(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_LONG, 0);
	v->appendLong(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testStringVectorHash){
	vector<string> testValues{ "9223372036854775807","helloworldabcdefghijklmnopqrstuvwxyz","智臾科技a","hello,智臾科技a","123abc您好！a","" };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 8,1,9,3,3,0 },
	{ 37,20,25,25,27,0 },
	{ 31,0,65,54,15,0 },
	{ 24,89,46,52,79,0 },
	{ 739,3737,2208,1485,376,0 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createString(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_STRING, 0);
	v->appendString(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testUUIDvectorHash){
	string script;
	script = "a=rand(uuid(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
	TableSP t = conn.run(script);

	int buckets[5] = { 13,43,71,97,4097 };
	int hv[6] = { 0 };
	int expected[5][6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (int i = 0;i < t->size(); i++) {
			ConstantSP val = Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i));
			hv[i] = val->getHash(buckets[j]);
			expected[j][i] = t->getColumn(j + 1)->getInt(i);
		}
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_UUID, 0);
	for (int i = 0;i < t->size(); i++)
		v->append(Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i)));
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testIpAddrvectorHash){
	string script;
	script = "a=rand(ipaddr(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
	TableSP t = conn.run(script);

	int buckets[5] = { 13,43,71,97,4097 };
	int hv[6] = { 0 };
	int expected[5][6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (int i = 0;i < t->size(); i++) {
			ConstantSP val = Util::parseConstant(DT_IP, t->getColumn(0)->getString(i));
			hv[i] = val->getHash(buckets[j]);
			expected[j][i] = t->getColumn(j + 1)->getInt(i);
		}
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_IP, 0);
	for (int i = 0;i < t->size(); i++)
		v->append(Util::parseConstant(DT_IP, t->getColumn(0)->getString(i)));
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}

TEST_F(DolphinDBTest,testInt128vectorHash){
	string script;
	script = "a=rand(int128(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
	TableSP t = conn.run(script);

	int buckets[5] = { 13,43,71,97,4097 };
	int hv[6] = { 0 };
	int expected[5][6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (int i = 0;i < t->size(); i++) {
			ConstantSP val = Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i));
			hv[i] = val->getHash(buckets[j]);
			expected[j][i] = t->getColumn(j + 1)->getInt(i);
		}
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
	VectorSP v = Util::createVector(DT_INT128, 0);
	for (int i = 0;i < t->size(); i++)
		v->append(Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i)));
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
	}
}


TEST_F(DolphinDBTest,testCharVectorHash2){
    vector<char> testValues { 127, -127, 12, 0, -12, -128 };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 10, 12, 12, 0, 10, -1 }, { 41, 18, 12, 0, 4, -1 }, { 56, 24, 12, 0, 68, -1 }, { 30, 5, 12, 0, 23, -1 }, { 127, 129, 12, 0, 244, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createChar(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_CHAR, 0);
    v->appendChar(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testShortVectorHash2){
    vector<short> testValues { 32767, -32767, 12, 0, -12, -32768 };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 7, 2, 12, 0, 10, -1 }, { 1, 15, 12, 0, 4, -1 }, { 36, 44, 12, 0, 68, -1 }, { 78, 54, 12, 0, 23, -1 }, { 4088, 265, 12, 0, 244, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createShort(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_SHORT, 0);
    v->appendShort(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testIntVectorHash2){
    vector<int> testValues { INT_MAX, INT_MAX * (-1), 12, 0, -12, INT_MIN };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 10, 12, 12, 0, 10, -1 }, { 7, 9, 12, 0, 4, -1 }, { 39, 41, 12, 0, 68, -1 }, { 65, 67, 12, 0, 23, -1 }, { 127, 129, 12, 0, 244, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createInt(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_INT, 0);
    v->appendInt(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testLongVectorHash2){
    vector<long long> testValues { LLONG_MAX, (-1) * LLONG_MAX, 12, 0, -12, LLONG_MIN };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 7, 9, 12, 0, 4, -1 }, { 41, 0, 12, 0, 29, -1 }, { 4, 6, 12, 0, 69, -1 }, { 78, 80, 12, 0, 49, -1 }, { 4088, 4090, 12, 0, 4069, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createLong(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_LONG, 0);
    v->appendLong(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testStringVectorHash2){
    vector < string > testValues { "9223372036854775807", "helloworldabcdefghijklmnopqrstuvwxyz", "智臾科技a", "hello,智臾科技a", "123abc您好！a", ""};
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 8, 1, 9, 3, 3, 0 }, { 37, 20, 25, 25, 27, 0 }, { 31, 0, 65, 54, 15, 0 }, { 24, 89, 46, 52, 79, 0 }, { 739, 3737, 2208, 1485, 376, 0 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createString(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_STRING, 0);
    v->appendString(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < testValues.size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}


TEST_F(DolphinDBTest,testUUIDvectorHash2){
    string script;
    script = "a=rand(uuid(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int hv[6] = { 0 };
    int expected[5][6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (int i = 0; i < t->size(); i++) {
            ConstantSP val = Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i));
            hv[i] = val->getHash(buckets[j]);
            expected[j][i] = t->getColumn(j + 1)->getInt(i);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_UUID, 0);
    for (int i = 0; i < t->size(); i++)
        v->append(Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i)));
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testIpAddrvectorHash2){
    string script;
    script = "a=rand(ipaddr(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int hv[6] = { 0 };
    int expected[5][6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (int i = 0; i < t->size(); i++) {
            ConstantSP val = Util::parseConstant(DT_IP, t->getColumn(0)->getString(i));
            hv[i] = val->getHash(buckets[j]);
            expected[j][i] = t->getColumn(j + 1)->getInt(i);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_IP, 0);
    for (int i = 0; i < t->size(); i++)
        v->append(Util::parseConstant(DT_IP, t->getColumn(0)->getString(i)));
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}

TEST_F(DolphinDBTest,testInt128vectorHash2){
    string script;
    script = "a=rand(int128(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int hv[6] = { 0 };
    int expected[5][6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (int i = 0; i < t->size(); i++) {
            ConstantSP val = Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i));
            hv[i] = val->getHash(buckets[j]);
            expected[j][i] = t->getColumn(j + 1)->getInt(i);
            EXPECT_EQ(hv[i], expected[j][i]);
        }
    }
    VectorSP v = Util::createVector(DT_INT128, 0);
    for (int i = 0; i < t->size(); i++)
        v->append(Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i)));
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
		for (unsigned int k = 0;k < t->size(); k++){
			EXPECT_EQ(hv[k], expected[j][k]);
		}
    }
}
/*
void testshare(){
string dbPath = conn.run(" getHomeDir()+\"/cpp_test\"")->getString();
string script;
script += "TickDB = database(dbPath, RANGE, `A`M`ZZZZ, `DFS_NODE1`DFS_NODE2);";
script += "t=table(rand(`AAPL`IBM`C`F,100) as sym, rand(1..10, 100) as qty, rand(10.25 10.5 10.75, 100) as price);";
script += "share t as TickDB.Trades on sym;";
//script += "dropTable(TickDB,`TickDB.Trades);";
script += "select top 10 * from TickDB.Trades;";

//script += "select count(*) from TickDB.Trades;";
TableSP result = conn.run(script);
cout<<result->getString()<<endl;

}
*/

TEST_F(DolphinDBTest,test_symbol_optimization){
	string script;
	conn.run(script);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10" };
	vector<DATA_TYPE> colTypes = { DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL };
	int colNum = 10, rowNum = 2000000;
	TableSP t11 = Util::createTable(colNames, colTypes, rowNum, 2000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t11->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[1]->set(i, Util::createString("B" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createString("C" + std::to_string(i % 1000)));
		columnVecs[3]->set(i, Util::createString("D" + std::to_string(i % 1000)));
		columnVecs[4]->set(i, Util::createString("E" + std::to_string(i % 1000)));
		columnVecs[5]->set(i, Util::createString("F" + std::to_string(i % 1000)));
		columnVecs[6]->set(i, Util::createString("G" + std::to_string(i % 1000)));
		columnVecs[7]->set(i, Util::createString("H" + std::to_string(i % 1000)));
		columnVecs[8]->set(i, Util::createString("I" + std::to_string(i % 1000)));
		columnVecs[9]->set(i, Util::createString("J" + std::to_string(i % 1000)));
	}
	int64_t startTime, time;
	startTime = getTimeStampMs();
	conn.upload("t11", { t11 });
	time = getTimeStampMs() - startTime;
	cout << "symbol table:" << time << "ms" << endl;
	string script1;
	script1 += "n=2000000;";
	script1 += "tmp=table(take(symbol(\"A\"+string(0..999)), n) as col1, take(symbol(\"B\"+string(0..999)), n) as col2,\
	 take(symbol(\"C\"+string(0..999)), n) as col3, take(symbol(\"D\"+string(0..999)), n) as col4, \
	 take(symbol(\"E\"+string(0..999)), n) as col5, take(symbol(\"F\"+string(0..999)), n) as col6,\
	  take(symbol(\"G\"+string(0..999)), n) as col7, take(symbol(\"H\"+string(0..999)), n) as col8, \
	  take(symbol(\"I\"+string(0..999)), n) as col9, take(symbol(\"J\"+string(0..999)), n) as col10);";
	script1 += "each(eqObj, t11.values(), tmp.values());";
	ConstantSP result = conn.run(script1);
	for (int i = 0; i<10; i++){
		cout<<result->getInt(i);
		EXPECT_EQ(result->getInt(i), 1);}
	ConstantSP res = conn.run("t11");
}

TEST_F(DolphinDBTest,testClearMemory_var){
	string script, script1;
	script += "login('admin', '123456');";
	script += "testVar=1000000";
	conn.run(script, 4, 2, 0, true);
	string result = conn.run("objs()[`name][0]")->getString();

	EXPECT_EQ(result.length(), size_t(0));	
}

TEST_F(DolphinDBTest,testClearMemory_){
	string script, script1, script2;
	script += "login('admin', '123456');";
	script += "dbPath='dfs://testClearMemory_';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath, VALUE, 2012.01.01..2012.01.10);";
	script += "t=table(1:0, [`date], [DATE]);";
	script += "pt=db.createPartitionedTable(t, `pt, `date);";
	script += "getSessionMemoryStat()[`memSize][0];";
	script += "select * from pt;";
	conn.run(script, 4, 2, 0, true);
	string result = conn.run("objs()[`name][0]")->getString();
	EXPECT_EQ(result.length(), size_t(0));
}

TEST_F(DolphinDBTest,test_symbol_base_exceed_2097152){
	vector < string > colNames = { "name", "id", "str" };
	vector<DATA_TYPE> colTypes = { DT_SYMBOL, DT_INT, DT_STRING };
	int colNum = 3, rowNum = 30000000;
	ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
	vector<VectorSP> columnVecs;
	for (int i = 0;i<colNum;i++) {
		columnVecs.push_back(table->getColumn(i));
	}
	try {
		for (int i = 0;i<rowNum;i++) {
			columnVecs[0]->set(i, Util::createString("name_" + std::to_string(i)));
			columnVecs[1]->setInt(i, i);
			columnVecs[2]->setString(i, std::to_string(i));
		}
	}
	catch (exception e) {
		cout << e.what() << endl;
	}
}

TEST_F(DolphinDBTest,test_printMsg){
	conn.setShowOutput(true);
	string script5 = "a=int(1);\
						b=bool(1);\
						c=char(1);\
						d=NULL;\
						ee=short(1);\
						f=long(1);\
						g=date(1);\
						h=month(1);\
						i=time(1);\
						j=minute(1);\
						k=second(1);\
						l=datetime(1);\
						m=timestamp(1);\
						n=nanotime(1);\
						o=nanotimestamp(1);\
						p=float(1);\
						q=double(1);\
						r=\"1\";\
						s=uuid(\"5d212a78-cc48-e3b1-4235-b4d91473ee87\");\
						t=blob(string[1]);\
						u=table(1 2 3 as col1, `a`b`c as col2);\
        				v=arrayVector(1 2 3 , 9 9 9);\
						w=dict(`a`b, '中文123￥……，'`)";
	conn.run(script5);
	ofstream file("output.txt");
	std::streambuf* originalBuffer = std::cout.rdbuf();
	std::cout.rdbuf(file.rdbuf());

 	conn.run("print(a,b,c,d,ee,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w)");

	std::cout.rdbuf(originalBuffer);
	file.close();
	ifstream infile("output.txt");
	string content((std::istreambuf_iterator<char>(infile)),
                        std::istreambuf_iterator<char>());
	cout << content<<endl;
	const string expectedContent = "1\n1\n1\n1\n1\n1970.01.02\n0000.02M\n00:00:00.001\n00:01m\n00:00:01\n1970.01.01T00:00:01\n1970.01.01T00:00:00.001\n00:00:00.000000001\n1970.01.01T00:00:00.000000001\n1\n1\n1\n5d212a78-cc48-e3b1-4235-b4d91473ee87\n[\"1\"]\ncol1 col2\n---- ----\n1    a   \n2    b   \n3    c   \n\n[[9],[9],[9]]\nb->\na->中文123￥……，\n\n";
	EXPECT_EQ(content, expectedContent);
	infile.close();

	remove("output.txt");
}