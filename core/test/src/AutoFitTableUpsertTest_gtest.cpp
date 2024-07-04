#include "config.h"

class AutoFitTableUpsertTest:public testing::Test
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

		CLEAR_ENV(conn);
    }
    virtual void TearDown()
    {
        CLEAR_ENV(conn);
    }
};


TEST_F(AutoFitTableUpsertTest,test_AutoFitTableUpsert_unconnected){
	DBConnection connNew(false, false);
	EXPECT_ANY_THROW(AutoFitTableUpsert aftu("", "temp_tab", connNew));
}

TEST_F(AutoFitTableUpsertTest,test_AutoFitTableUpsert_upsertToinMemoryTable){
	conn.run("tab = table(`a`x`d`s`cs as col1, 2 3 4 5 6 as col2);share tab as temp_tab");
	AutoFitTableUpsert aftu("", "temp_tab", conn);
	vector<string> colNames = {"col1", "col2"};
	vector<ConstantSP> cols ={Util::createString("zzz123中文a"), Util::createInt(1000)};
	TableSP t = Util::createTable(colNames, cols);
	EXPECT_ANY_THROW(aftu.upsert(t));
}

TEST_F(AutoFitTableUpsertTest,test_AutoFitTableUpsert_upsertErrTableColNums){
	conn.run("tab = table(`a`x`d`s`cs as col1, 2 3 4 5 6 as col2);keyedtab = keyedTable(`col2, tab);share keyedtab as temp_tab");
	vector<string> colNames = {"col1", "col2", "col3"};
	vector<ConstantSP> cols ={Util::createString("zzz123中文a"), Util::createInt(7), Util::createInt(5)};
	TableSP t = Util::createTable(colNames, cols);

    AutoFitTableUpsert aftu("", "temp_tab", conn);
	EXPECT_ANY_THROW(aftu.upsert(t));
}

TEST_F(AutoFitTableUpsertTest,test_AutoFitTableUpsert_upsertErrTableColType){
	conn.run("tab = table(`a`x`d`s`cs as col1, 2 3 4 5 6 as col2);keyedtab = keyedTable(`col2, tab);share keyedtab as temp_tab");
	AutoFitTableUpsert aftu("", "temp_tab", conn);
	vector<string> colNames = {"col1", "col2"};
	vector<ConstantSP> cols ={Util::createString("zzz123中文a"), Util::createDate(1000)};
	TableSP t = Util::createTable(colNames, cols);
	EXPECT_ANY_THROW(aftu.upsert(t));
}

TEST_F(AutoFitTableUpsertTest,test_AutoFitTableUpsert_upsertWithNullKeyColNames){
	conn.run("tab = table(`a`x`d`s`cs as col1, 2 3 4 5 6 as col2);keyedtab = keyedTable(`col2, tab);share keyedtab as temp_tab");
	vector<string> KeyCols = {};

	AutoFitTableUpsert aftu("", "temp_tab", conn, false, &KeyCols);
	vector<string> colNames = {"col1", "col2"};
	vector<ConstantSP> cols ={Util::createString("zzz123中文a"), Util::createInt(1000)};
	TableSP t = Util::createTable(colNames, cols);
	aftu.upsert(t);
	EXPECT_TRUE(conn.run("exec count(*) from temp_tab")->getInt() == 6);
}

TEST_F(AutoFitTableUpsertTest,test_AutoFitTableUpsert_upsertAllDataTypesToindexedTable){
	srand((int)time(NULL));
	int colNum = 25, rowNum = 1000;
	int scale32 = rand()%9, scale64 = rand()%18, scale128 = rand()%38;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_CHAR);
	colTypesVec1.emplace_back(DT_BOOL);
	colTypesVec1.emplace_back(DT_SHORT);
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_LONG);
	colTypesVec1.emplace_back(DT_DATE);
	colTypesVec1.emplace_back(DT_MONTH);
	colTypesVec1.emplace_back(DT_TIME);
	colTypesVec1.emplace_back(DT_MINUTE);
	colTypesVec1.emplace_back(DT_DATETIME);
	colTypesVec1.emplace_back(DT_SECOND);
	colTypesVec1.emplace_back(DT_TIMESTAMP);
	colTypesVec1.emplace_back(DT_NANOTIME);
	colTypesVec1.emplace_back(DT_NANOTIMESTAMP);
	colTypesVec1.emplace_back(DT_FLOAT);
	colTypesVec1.emplace_back(DT_DOUBLE);
	colTypesVec1.emplace_back(DT_STRING);
	colTypesVec1.emplace_back(DT_UUID);
	colTypesVec1.emplace_back(DT_IP);
	colTypesVec1.emplace_back(DT_INT128);
	colTypesVec1.emplace_back(DT_BLOB);
	colTypesVec1.emplace_back(DT_DATEHOUR);
	colTypesVec1.emplace_back(DT_DECIMAL32);
	colTypesVec1.emplace_back(DT_DECIMAL64);
	colTypesVec1.emplace_back(DT_DECIMAL128);

	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(scale32,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(scale64,rand()/double(RAND_MAX)));
		columnVecs[24]->set(i, Util::createDecimal128(scale128,rand()/double(RAND_MAX)));
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1, SHARED)}catch(ex){};go;";
	script1 += "temp = table(100:0, take(`col,25)+string(take(0..24,25)), \
	[CHAR, BOOL, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, DATETIME, SECOND, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, IPADDR, INT128, BLOB, DATEHOUR, DECIMAL32("+to_string(scale32)+"), DECIMAL64("+to_string(scale64)+"), DECIMAL128("+to_string(scale128)+")]);";
	script1 += "st1 = indexedTable(`col16,temp);";
	conn.run(script1);
    vector<string> keycolName = {"col16"};
    AutoFitTableUpsert aftu("", "st1", conn, false);
	aftu.upsert(tab1);

	conn.upload("tab1",tab1);
	string script3;
	script3 += "each(eqObj, tab1.values(), st1.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<result2->size(); i++)
		EXPECT_TRUE(result2->get(i)->getBool());

	// conn.run("undef(`st1, SHARED)");
}

TEST_F(AutoFitTableUpsertTest,test_AutoFitTableUpsert_upsertAllDataTypesTokeyedTable){
	srand((int)time(NULL));
	int colNum = 25, rowNum = 1000;
	int scale32 = rand()%9, scale64 = rand()%18, scale128 = rand()%38;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_CHAR);
	colTypesVec1.emplace_back(DT_BOOL);
	colTypesVec1.emplace_back(DT_SHORT);
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_LONG);
	colTypesVec1.emplace_back(DT_DATE);
	colTypesVec1.emplace_back(DT_MONTH);
	colTypesVec1.emplace_back(DT_TIME);
	colTypesVec1.emplace_back(DT_MINUTE);
	colTypesVec1.emplace_back(DT_DATETIME);
	colTypesVec1.emplace_back(DT_SECOND);
	colTypesVec1.emplace_back(DT_TIMESTAMP);
	colTypesVec1.emplace_back(DT_NANOTIME);
	colTypesVec1.emplace_back(DT_NANOTIMESTAMP);
	colTypesVec1.emplace_back(DT_FLOAT);
	colTypesVec1.emplace_back(DT_DOUBLE);
	colTypesVec1.emplace_back(DT_STRING);
	colTypesVec1.emplace_back(DT_UUID);
	colTypesVec1.emplace_back(DT_IP);
	colTypesVec1.emplace_back(DT_INT128);
	colTypesVec1.emplace_back(DT_BLOB);
	colTypesVec1.emplace_back(DT_DATEHOUR);
	colTypesVec1.emplace_back(DT_DECIMAL32);
	colTypesVec1.emplace_back(DT_DECIMAL64);
	colTypesVec1.emplace_back(DT_DECIMAL128);

	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(scale32,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(scale64,rand()/double(RAND_MAX)));
		columnVecs[24]->set(i, Util::createDecimal128(scale128,rand()/double(RAND_MAX)));
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1, SHARED)}catch(ex){};go;";
	script1 += "temp = table(100:0, take(`col,25)+string(take(0..24,25)), \
	[CHAR, BOOL, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, DATETIME, SECOND, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, IPADDR, INT128, BLOB, DATEHOUR, DECIMAL32("+to_string(scale32)+"), DECIMAL64("+to_string(scale64)+"), DECIMAL128("+to_string(scale128)+")]);";
	script1 += "st1 = keyedTable(`col16,temp);";
	conn.run(script1);
    vector<string> keycolName = {"col16"};
    AutoFitTableUpsert aftu("", "st1", conn, false);
	aftu.upsert(tab1);

	conn.upload("tab1",tab1);
	string script3;
	script3 += "each(eqObj, tab1.values(), st1.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<result2->size(); i++)
		EXPECT_TRUE(result2->get(i)->getBool());

	// conn.run("undef(`st1, SHARED)");
}


TEST_F(AutoFitTableUpsertTest,test_AutoFitTableUpsert_upsertAllDataTypesTopartitionedTableOLAP){// OLAP not support datatype blob
	srand((int)time(NULL));
	int colNum = 24, rowNum = 1000;
	int scale32 = rand()%9, scale64 = rand()%18, scale128 = rand()%38;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_CHAR);
	colTypesVec1.emplace_back(DT_BOOL);
	colTypesVec1.emplace_back(DT_SHORT);
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_LONG);
	colTypesVec1.emplace_back(DT_DATE);
	colTypesVec1.emplace_back(DT_MONTH);
	colTypesVec1.emplace_back(DT_TIME);
	colTypesVec1.emplace_back(DT_MINUTE);
	colTypesVec1.emplace_back(DT_DATETIME);
	colTypesVec1.emplace_back(DT_SECOND);
	colTypesVec1.emplace_back(DT_TIMESTAMP);
	colTypesVec1.emplace_back(DT_NANOTIME);
	colTypesVec1.emplace_back(DT_NANOTIMESTAMP);
	colTypesVec1.emplace_back(DT_FLOAT);
	colTypesVec1.emplace_back(DT_DOUBLE);
	colTypesVec1.emplace_back(DT_STRING);
	colTypesVec1.emplace_back(DT_UUID);
	colTypesVec1.emplace_back(DT_IP);
	colTypesVec1.emplace_back(DT_INT128);
	colTypesVec1.emplace_back(DT_DATEHOUR);
	colTypesVec1.emplace_back(DT_DECIMAL32);
	colTypesVec1.emplace_back(DT_DECIMAL64);
	colTypesVec1.emplace_back(DT_DECIMAL128);

	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[21]->set(i, Util::createDecimal32(scale32,rand()/float(RAND_MAX)));
		columnVecs[22]->set(i, Util::createDecimal64(scale64,rand()/double(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal128(scale128,rand()/double(RAND_MAX)));
	}

	for (int j = 0; j < colNum; j++){
		if(j == 3)
			columnVecs[3]->set(rowNum-1, Util::createInt(rand()%INT_MAX));  //partition-column's value must be not null
		else
			columnVecs[j]->setNull(rowNum-1);
	}

    string dbName ="dfs://test_AutoFitTableUpsert_upsertAllDataTypesTopartitionedTable";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableUpsert_upsertAllDataTypesTopartitionedTable\";"
			"if(exists(dbName)){dropDatabase(dbName)};"
			"db  = database(dbName, HASH,[INT,1]);"
			"temp = table(1000:0, take(`col,24)+string(take(0..23,24)), \
			[CHAR, BOOL, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, DATETIME, SECOND, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, IPADDR, INT128, DATEHOUR, DECIMAL32("+to_string(scale32)+"), DECIMAL64("+to_string(scale64)+"), DECIMAL128("+to_string(scale128)+")]);"
			"pt = createPartitionedTable(db,temp,`pt,`col3);";
	conn.run(script);
    vector<string> keycolName = {"col16"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
	aftu.upsert(tab1);

	conn.upload("tab1",tab1);
	string script3;
	script3 += "st1 = select * from pt;";
	script3 += "each(eqObj, tab1.values(), st1.values());";
	ConstantSP result2 = conn.run(script3);
	// cout<<conn.run("st1")->getString();
	for (int i = 0; i<result2->size(); i++)
		EXPECT_TRUE(result2->get(i)->getBool());

	// conn.run("undef(`st1, SHARED)");
}

TEST_F(AutoFitTableUpsertTest,test_AutoFitTableUpsert_upsertAllDataTypesTopartitionedTableTSDB){
	int colNum = 25, rowNum = 1000;
	int scale32 = rand()%9, scale64 = rand()%18, scale128 = rand()%38;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_CHAR);
	colTypesVec1.emplace_back(DT_BOOL);
	colTypesVec1.emplace_back(DT_SHORT);
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_LONG);
	colTypesVec1.emplace_back(DT_DATE);
	colTypesVec1.emplace_back(DT_MONTH);
	colTypesVec1.emplace_back(DT_TIME);
	colTypesVec1.emplace_back(DT_MINUTE);
	colTypesVec1.emplace_back(DT_DATETIME);
	colTypesVec1.emplace_back(DT_SECOND);
	colTypesVec1.emplace_back(DT_TIMESTAMP);
	colTypesVec1.emplace_back(DT_NANOTIME);
	colTypesVec1.emplace_back(DT_NANOTIMESTAMP);
	colTypesVec1.emplace_back(DT_FLOAT);
	colTypesVec1.emplace_back(DT_DOUBLE);
	colTypesVec1.emplace_back(DT_STRING);
	colTypesVec1.emplace_back(DT_UUID);
	colTypesVec1.emplace_back(DT_IP);
	colTypesVec1.emplace_back(DT_INT128);
	colTypesVec1.emplace_back(DT_BLOB);
	colTypesVec1.emplace_back(DT_DATEHOUR);
	colTypesVec1.emplace_back(DT_DECIMAL32);
	colTypesVec1.emplace_back(DT_DECIMAL64);
	colTypesVec1.emplace_back(DT_DECIMAL128);

	srand((int)time(NULL));
	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(i));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(scale32,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(scale64,rand()/double(RAND_MAX)));
		columnVecs[24]->set(i, Util::createDecimal128(scale128,rand()/double(RAND_MAX)));
	}
	for (int j = 0; j < colNum; j++){
		if(j == 3)
			columnVecs[3]->set(rowNum-1, Util::createInt(rand()%INT_MAX));  //partition-column's value must be not null
		else
			columnVecs[j]->setNull(rowNum-1);
	}
    string dbName ="dfs://test_AutoFitTableUpsert_upsertAllDataTypesTopartitionedTable";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_AutoFitTableUpsert_upsertAllDataTypesTopartitionedTable\";"
			"if(exists(dbName)){dropDatabase(dbName)};"
			"db  = database(dbName, HASH,[STRING,1],,'TSDB');"
			"temp = table(100:0, take(`col,25)+string(take(0..24,25)), \
			[CHAR, BOOL, SHORT, INT, LONG, DATE, MONTH, TIME, MINUTE, DATETIME, SECOND, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, STRING, UUID, IPADDR, INT128, BLOB, DATEHOUR, DECIMAL32("+to_string(scale32)+"), DECIMAL64("+to_string(scale64)+"), DECIMAL128("+to_string(scale128)+")]);"
			"pt = db.createPartitionedTable(temp,`pt,`col16,,`col16);";
	conn.run(script);
    vector<string> keycolName = {"col16"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
	aftu.upsert(tab1);

	conn.upload("tab1",tab1);
	string script3;
	script3 += "st1 = select * from pt order by col3;res = select * from tab1 order by col3;";
	script3 += "each(eqObj, res.values(), st1.values());";
	ConstantSP result2 = conn.run(script3);
	// cout<<conn.run("st1")->getString();
	for (int i = 0; i<result2->size(); i++)
		EXPECT_TRUE(result2->get(i)->getBool());

	// conn.run("undef(`st1, SHARED)");
}

TEST_F(AutoFitTableUpsertTest, test_upsertToPartitionTableRangeType){
    string script1;

    string dbName = "dfs://test_upsertToPartitionTableRangeType";
    string tableName = "pt";
    script1 += "dbName = \""+dbName+"\"\n";
    script1 += "tableName=\""+tableName+"\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = db.createPartitionedTable(t,tableName,`id);";
    //cout<<script1<<endl;
    conn.run(script1);
    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString(sym[rand()%4]));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
    // cout<< tmp1->getString()<<endl;
    // cout<< res->getString()<<endl;
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);
    EXPECT_EQ(tmp1->getString(), res->getString());


}


TEST_F(AutoFitTableUpsertTest, test_upsertToPartitionTableRangeTypeIgnoreNull){
    string script1;
    string dbName = "dfs://test_upsertToPartitionTableRangeTypeIgnoreNull";
    string tableName = "pt";
    script1 += "dbName = \""+dbName+"\"\n";
    script1 += "tableName=\""+tableName+"\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
    script1 += "t = table(10:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
               "tableInsert(t,[`asd,0,10]);"\
               "pt = db.createPartitionedTable(t,tableName,`id);"\
               "tableInsert(pt,t)";
    //cout<<script1<<endl;
    conn.run(script1);
    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, true, &keycolName);
	int colNum = 3, rowNum = 10;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString("D"));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    cout<< tmp1->getString()<<endl;
    aftu.upsert(tmp1);
    TableSP res=conn.run("select * from pt;");

	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);

    // cout<< tmp1->getString()<<endl<<res->getString()<<endl;
    EXPECT_EQ(res->getRow(0)->getMember(Util::createString("value"))->getInt(), 10);
	EXPECT_EQ(res->getRow(0)->getMember(Util::createString("symbol"))->getString(), "D");
	EXPECT_EQ(res->getRow(0)->getMember(Util::createString("id"))->getInt(), 0);

    for (int i = 1; i < rowNum; i++) {
        // cout<<tmp1->getColumn(0)->getRow(i)->getString()<<" "<<res->getColumn(0)->getRow(i)->getString()<<endl;
        EXPECT_EQ(tmp1->getColumn(0)->getRow(i)->getString(), res->getColumn(0)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(1)->getRow(i)->getString(), res->getColumn(1)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(2)->getRow(i)->getString(), res->getColumn(2)->getRow(i)->getString());
    }

}

TEST_F(AutoFitTableUpsertTest, test_upsertToKeyedTable){
    string script1;

    string tableName = "pt";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = keyedTable(`id, t);";
    //cout<<script1<<endl;
    conn.run(script1);

    AutoFitTableUpsert aftu("", tableName, conn, false);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString(sym[rand()%4]));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
    // cout<< tmp1->getString()<<endl;
    // cout<< res->getString()<<endl;
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);
    EXPECT_EQ(tmp1->getString(), res->getString());


}


TEST_F(AutoFitTableUpsertTest, test_upsertToKeyedTableIgnoreNull){
    string script1;

    string tableName = "pt";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = keyedTable(`id, t);\
              tableInsert(pt,`asd,0,10)";
    //cout<<script1<<endl;
    conn.run(script1);

    AutoFitTableUpsert aftu("", tableName, conn, true);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString("D"));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);
    // cout<< tmp1->getString()<<endl<<res->getString()<<endl;

    EXPECT_EQ(res->getRow(0)->getMember(Util::createString("value"))->getInt(), 10);
	EXPECT_EQ(res->getRow(0)->getMember(Util::createString("symbol"))->getString(), "D");
	EXPECT_EQ(res->getRow(0)->getMember(Util::createString("id"))->getInt(), 0);

    for (int i = 1; i < rowNum; i++) {
        // cout<<tmp1->getColumn(0)->getRow(i)->getString()<<" "<<res->getColumn(0)->getRow(i)->getString()<<endl;
        EXPECT_EQ(tmp1->getColumn(0)->getRow(i)->getString(), res->getColumn(0)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(1)->getRow(i)->getString(), res->getColumn(1)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(2)->getRow(i)->getString(), res->getColumn(2)->getRow(i)->getString());
    }

}

TEST_F(AutoFitTableUpsertTest, test_upsertToindexedTable){
    string script1;

    string tableName = "pt";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = indexedTable(`id, t);";
    //cout<<script1<<endl;
    conn.run(script1);

    AutoFitTableUpsert aftu("", tableName, conn, false);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString(sym[rand()%4]));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
    // cout<< tmp1->getString()<<endl;
    // cout<< res->getString()<<endl;
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);
    EXPECT_EQ(tmp1->getString(), res->getString());


}

TEST_F(AutoFitTableUpsertTest, test_upsertToindexedTableIgnoreNull){
    string script1;

    string tableName = "pt";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
              "pt = indexedTable(`id, t);\
              tableInsert(pt,`asd,0,10)";
    //cout<<script1<<endl;
    conn.run(script1);

    AutoFitTableUpsert aftu("", tableName, conn, true);

	int colNum = 3, rowNum = 1000;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString("D"));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setNull(0);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(i));
        columnVecs[2]->set(i, Util::createInt((int)(rand()%1000)));
	}
    aftu.upsert(tmp1);

    TableSP res=conn.run("select * from pt;");
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);

    // cout<< tmp1->getString()<<endl<<res->getString()<<endl;

    EXPECT_EQ(res->getRow(0)->getMember(Util::createString("value"))->getInt(), 10);
	EXPECT_EQ(res->getRow(0)->getMember(Util::createString("symbol"))->getString(), "D");
	EXPECT_EQ(res->getRow(0)->getMember(Util::createString("id"))->getInt(), 0);

    for (int i = 1; i < rowNum; i++) {
        // cout<<tmp1->getColumn(0)->getRow(i)->getString()<<" "<<res->getColumn(0)->getRow(i)->getString()<<endl;
        EXPECT_EQ(tmp1->getColumn(0)->getRow(i)->getString(), res->getColumn(0)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(1)->getRow(i)->getString(), res->getColumn(1)->getRow(i)->getString());
        EXPECT_EQ(tmp1->getColumn(2)->getRow(i)->getString(), res->getColumn(2)->getRow(i)->getString());
    }

}

TEST_F(AutoFitTableUpsertTest, test_upsertToPartitionTableRangeTypeWithsortColumns){
    string script1;

    string dbName = "dfs://test_upsertToPartitionTableRangeType";
    string tableName = "pt";
    script1 += "dbName = \""+dbName+"\"\n";
    script1 += "tableName=\""+tableName+"\"\n";
    script1 += "login(\"admin\",\"123456\")\n";
    script1 += "if(existsDatabase(dbName)){\n";
    script1 += " dropDatabase(dbName)\n";
    script1 += "}\n";
    script1 += "db  = database(dbName, RANGE,0 5000 15000);\n";
    script1 += "t = table(1000:0, `symbol`id`value,[SYMBOL, INT, INT]);"\
               "pt = db.createPartitionedTable(t,tableName,`id);";
    //cout<<script1<<endl;
    conn.run(script1);

	int colNum = 3;
    int rowNum = 1000;
    vector<VectorSP> columnVecs;
    vector<VectorSP> columnVecs2;
    vector<string> colNames = {"symbol", "id", "value"};
    vector<DATA_TYPE> colTypes = {DT_SYMBOL, DT_INT, DT_INT};

    TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);

	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tmp1->getColumn(i));
        }
    string sym[] = {"A", "B", "C", "D"};
    columnVecs[0]->set(0, Util::createString("D"));
    columnVecs[1]->set(0, Util::createInt(0));
    columnVecs[2]->setInt(1000);
    for (int i = 1; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString(sym[rand()%4]));
        columnVecs[1]->set(i, Util::createInt(0));
        columnVecs[2]->set(i, Util::createInt((int)(rowNum-i)));
	}
    conn.upload("tmp1",{tmp1});
    conn.run("tableInsert(pt,tmp1)");
    Util::sleep(2000);
    vector<string> keycolName = {"id"};
    vector<string> sortColName = {"value"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName, &sortColName);

	int rowNum2 = 1;

    TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum2, rowNum2);

	columnVecs2.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs2.emplace_back(tmp2->getColumn(i));
        }

    columnVecs2[0]->set(0, Util::createString("D"));
    columnVecs2[1]->set(0, Util::createInt(0));
    columnVecs2[2]->setInt(0);

    aftu.upsert(tmp2);

    TableSP res=conn.run("select * from pt;");
    // cout<< res->getString()<<endl;
	EXPECT_EQ((res->getColumnType(0)==18 || res->getColumnType(0)==17),true);
    EXPECT_EQ(res->getColumnType(1), 4);
    EXPECT_EQ(res->getColumnType(2), 4);

    for (int i = 1; i < rowNum; i++){
        // cout<<res->getColumn(2)->getRow(i-1)->getInt()<<" "<<res->getColumn(2)->getRow(i)->getString()<<endl;
        EXPECT_EQ((res->getColumn(2)->getRow(i)->getInt() > res->getColumn(2)->getRow(i-1)->getInt()), true);
    }
}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithIntArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, INT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);

    v1->setInt(0, 1);
    v1->setInt(1, 100);
    v1->setInt(2, 9999);

    VectorSP av1 = Util::createArrayVector(DT_INT_ARRAY, 0, 3);
    av1->append(v1);
    av1->append(v1);
    av1->append(v1);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_INT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithIntArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithIntArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithIntArrayVectorNullToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, INT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_INT, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_INT_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_INT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithIntArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithIntArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, INT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_INT, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->setInt(i, i);

	VectorSP av1 = Util::createArrayVector(DT_INT_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_INT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithCharArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithCharArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithCharArrayVectorToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, CHAR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_CHAR, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setChar(0, 1);
	v2->setChar(1, 0);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_CHAR_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_CHAR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithCharArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithCharArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithCharArrayVectorNullToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, CHAR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_CHAR, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_CHAR_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_CHAR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithCharArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithCharArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithCharArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, CHAR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_CHAR, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->setChar(i, i);

	VectorSP av1 = Util::createArrayVector(DT_CHAR_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_CHAR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithFloatArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithFloatArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithFloatArrayVectorToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, FLOAT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_FLOAT, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setFloat(0, 1);
	v2->setFloat(1, 0);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_FLOAT_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_FLOAT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithFloatArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithFloatArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithFloatArrayVectorNullToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, FLOAT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_FLOAT, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_FLOAT_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_FLOAT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithFloatArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithFloatArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithFloatArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, FLOAT[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_FLOAT, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->setFloat(i, i);

	VectorSP av1 = Util::createArrayVector(DT_FLOAT_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_FLOAT_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDateArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithDateArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDateArrayVectorToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATE[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_DATE, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createDate(0));
	v2->set(1, Util::createDate(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_DATE_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATE_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDateArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithDateArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDateArrayVectorNullToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATE[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_DATE, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_DATE_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATE_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDateArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithDateArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDateArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATE[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_DATE, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
	    v2->set(i, Util::createDate(i));

	VectorSP av1 = Util::createArrayVector(DT_DATE_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATE_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithMonthArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithMonthArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithMonthArrayVectorToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, MONTH[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_MONTH, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createMonth(0));
	v2->set(1, Util::createMonth(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_MONTH_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_MONTH_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithMonthArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithMonthArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithMonthArrayVectorNullToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, MONTH[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_MONTH, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_MONTH_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_MONTH_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithMonthArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithMonthArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithMonthArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, MONTH[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_MONTH, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
	    v2->set(i, Util::createMonth(i));

	VectorSP av1 = Util::createArrayVector(DT_MONTH_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_MONTH_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithTimeArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithTimeArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithTimeArrayVectorToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, TIME[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_TIME, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createTime(0));
	v2->set(1, Util::createTime(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_TIME_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_TIME_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithTimeArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithTimeArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithTimeArrayVectorNullToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, TIME[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_TIME, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_TIME_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_TIME_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithTimeArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithTimeArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithTimeArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, TIME[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_TIME, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
	    v2->set(i, Util::createTime(i));

	VectorSP av1 = Util::createArrayVector(DT_TIME_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_TIME_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithSecondArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithSecondArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithSecondArrayVectorToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, SECOND[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_SECOND, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createSecond(0));
	v2->set(1, Util::createSecond(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SECOND_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithSecondArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithSecondArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithSecondArrayVectorNullToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, SECOND[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_SECOND, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SECOND_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithSecondArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithSecondArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithSecondArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, SECOND[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_SECOND, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
	    v2->set(i, Util::createSecond(i));

	VectorSP av1 = Util::createArrayVector(DT_SECOND_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SECOND_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDatehourArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithDatehourArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDatehourArrayVectorToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATEHOUR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_DATEHOUR, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->set(0, Util::createDateHour(0));
	v2->set(1, Util::createDateHour(1000));
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATEHOUR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDatehourArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithDatehourArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDatehourArrayVectorNullToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATEHOUR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_DATEHOUR, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATEHOUR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithDatehourArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithDatehourArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithDatehourArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, DATEHOUR[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_DATEHOUR, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
	    v2->set(i, Util::createDateHour(i));

	VectorSP av1 = Util::createArrayVector(DT_DATEHOUR_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_DATEHOUR_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithUuidArrayVectorToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithUuidArrayVectorToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithUuidArrayVectorToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, UUID[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
    VectorSP v2 = Util::createVector(DT_UUID, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setString(0, "5d212a78-cc48-e3b1-4235-b4d91473ee87");
	v2->setString(1, "5d212a78-cc48-e3b1-4235-b4d91473ee99");
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_UUID_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_UUID_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithUuidArrayVectorNullToPartitionTableRangeType){
    string dbName ="dfs://test_upsertTablewithUuidArrayVectorNullToPartitionTableRangeType";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithUuidArrayVectorNullToPartitionTableRangeType\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,1 10000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, UUID[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_UUID, 3, 3);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	v2->setNull(0);
	v2->setNull(1);
	v2->setNull(2);

	VectorSP av1 = Util::createArrayVector(DT_UUID_ARRAY, 0, 3);
	av1->append(v2);
	av1->append(v2);
	av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_UUID_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

TEST_F(AutoFitTableUpsertTest, test_upsertTablewithUuidArrayVectorToPartitionTableRangeTypeMorethan65535){
    string dbName ="dfs://test_upsertTablewithUuidArrayVectorToPartitionTableRangeTypeMorethan65535";
    string tableName = "pt";
	string script = "dbName = \"dfs://test_upsertTablewithUuidArrayVectorToPartitionTableRangeTypeMorethan65535\"\n"
			"if(exists(dbName)){\n"
			"\tdropDatabase(dbName)\t\n"
			"}\n"
			"db  = database(dbName, RANGE,0 70000,,'TSDB')\n"
			"t = table(1000:0, `id`value,[ INT, UUID[]])\n"
			"pt = db.createPartitionedTable(t,`pt,`id,,`id)";
	conn.run(script);

	VectorSP v1 = Util::createVector(DT_INT, 3, 3);
	VectorSP v2 = Util::createVector(DT_UUID, 70000, 70000);
	v1->setInt(0, 1);
	v1->setInt(1, 100);
	v1->setInt(2, 9999);
	for(unsigned int i=0;i<70000;i++)
		v2->setString(i, "5d212a78-cc48-e3b1-4235-b4d91473ee87");

	VectorSP av1 = Util::createArrayVector(DT_UUID_ARRAY, 0, 70000);
	for(unsigned int i=0;i<3;i++)
		av1->append(v2);

	vector<string> colNames = { "id", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_UUID_ARRAY };
	int colNum = 2, rowNum = 3;
	TableSP tab = Util::createTable(colNames, colTypes, 0, 10);
	vector<ConstantSP> colVecs{ v1,av1 };
	INDEX insertrows;
	string errmsg;
	if (tab->append(colVecs, insertrows, errmsg) == false) {
			std::cerr << errmsg;
	}

    vector<string> keycolName = {"id"};
    AutoFitTableUpsert aftu(dbName, tableName, conn, false, &keycolName);
    aftu.upsert(tab);

    TableSP res=conn.run("select * from pt;");
    EXPECT_EQ(tab->getString(), res->getString());
    EXPECT_EQ(tab->getColumn(0)->getType(), res->getColumn(0)->getType());
    EXPECT_EQ(tab->getColumn(1)->getType(), res->getColumn(1)->getType());

}

class AFTU_append_null : public AutoFitTableUpsertTest, public testing::WithParamInterface<tuple<string, DATA_TYPE>>
{
public:
	static vector<tuple<string, DATA_TYPE>> data_prepare()
	{
		vector<string> testTypes = {"BOOL", "CHAR", "SHORT", "INT", "LONG", "DATE", "MONTH", "TIME", "MINUTE", "SECOND", "DATETIME", "TIMESTAMP", "NANOTIME", "NANOTIMESTAMP", "DATEHOUR", "FLOAT", "DOUBLE", "STRING", "SYMBOL", "BLOB", "IPADDR", "UUID", "INT128", "DECIMAL32(8)", "DECIMAL64(15)", "DECIMAL128(28)",
				"BOOL[]", "CHAR[]", "SHORT[]", "INT[]", "LONG[]", "DATE[]", "MONTH[]", "TIME[]", "MINUTE[]", "SECOND[]", "DATETIME[]", "TIMESTAMP[]", "NANOTIME[]", "NANOTIMESTAMP[]", "DATEHOUR[]", "FLOAT[]", "DOUBLE[]", "IPADDR[]", "UUID[]", "INT128[]", "DECIMAL32(8)[]", "DECIMAL64(15)[]", "DECIMAL128(25)[]"};
		vector<DATA_TYPE> dataTypes = {DT_BOOL, DT_CHAR, DT_SHORT, DT_INT, DT_LONG, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP, DT_DATEHOUR, DT_FLOAT, DT_DOUBLE, DT_STRING,DT_SYMBOL,DT_BLOB, DT_IP, DT_UUID, DT_INT128, DT_DECIMAL32, DT_DECIMAL64, DT_DECIMAL128,
		DT_BOOL_ARRAY, DT_CHAR_ARRAY, DT_SHORT_ARRAY, DT_INT_ARRAY, DT_LONG_ARRAY, DT_DATE_ARRAY, DT_MONTH_ARRAY, DT_TIME_ARRAY, DT_MINUTE_ARRAY, DT_SECOND_ARRAY, DT_DATETIME_ARRAY, DT_TIMESTAMP_ARRAY, DT_NANOTIME_ARRAY, DT_NANOTIMESTAMP_ARRAY, DT_DATEHOUR_ARRAY, DT_FLOAT_ARRAY, DT_DOUBLE_ARRAY, DT_IP_ARRAY, DT_UUID_ARRAY, DT_INT128_ARRAY, DT_DECIMAL32_ARRAY, DT_DECIMAL64_ARRAY, DT_DECIMAL128_ARRAY};
		vector<tuple<string, DATA_TYPE>> data;
		for	(auto i = 0; i < testTypes.size(); i++)
			data.push_back(make_tuple(testTypes[i], dataTypes[i]));
		return data;
	}

};
INSTANTIATE_TEST_SUITE_P(, AFTU_append_null, testing::ValuesIn(AFTU_append_null::data_prepare()));

TEST_P(AFTU_append_null, test_append_empty_table)
{
	string type = std::get<0>(GetParam());
	DATA_TYPE dataType = std::get<1>(GetParam());
	cout << "test type: " << type << endl;
	string colName = "c1";
	string script1 =
		"colName = [`ind,`time, `" + colName + "];"
		"colType = [INT, DATETIME, " + type + "];"
		"t=table(1:0, colName, colType);"
		"if(existsDatabase('dfs://test_append_empty')) dropDatabase('dfs://test_append_empty');go;"
		"db = database('dfs://test_append_empty', VALUE, 1..10,,'TSDB');"
		"db.createPartitionedTable(t, `pt, `ind,,`ind`time)";

	conn.run(script1);
	VectorSP col0 = Util::createVector(DT_INT, 0);
	VectorSP col1 = Util::createVector(DT_DATETIME, 0);
	VectorSP col2 = Util::createVector(dataType, 0);
	vector<string> colNames = { "ind", "time", "c1"};
	vector<ConstantSP> cols = { col0, col1, col2};
	TableSP empty2 = Util::createTable(colNames, cols);

	vector<string>* kcols = new vector<string>{"ind", "time"};
    AutoFitTableUpsert upsert("dfs://test_append_empty", "pt", conn, true, kcols);
	upsert.upsert(empty2);

	auto res = conn.run("exec * from loadTable('dfs://test_append_empty', `pt)");
	EXPECT_EQ(res->rows(), 0);
	conn.run("dropDatabase('dfs://test_append_empty');go");
	delete kcols;
}
