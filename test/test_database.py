import pytest
from setup.settings import *
from setup.utils import get_pid
import dolphindb as ddb
import numpy as np
import pandas as pd
from numpy.testing import *
from pandas.testing import *
import dolphindb.settings as keys
PANDAS_VERSION=tuple(int(i) for i in pd.__version__.split('.'))


class DBInfo:
    dfsDBName = 'dfs://testDatabase'
    diskDBName = WORK_DIR + '/testDatabase'


def existsDB(dbName) -> bool:
    s = ddb.session()
    s.connect(HOST, PORT, USER, PASSWD)
    res = s.run("existsDatabase('{db}')".format(db=dbName))
    s.close()
    return res


def dropDB(dbName)-> bool:
    s = ddb.session()
    s.connect(HOST, PORT, USER, PASSWD)
    s.run("dropDatabase('{db}')".format(db=dbName))
    s.close()

class TestDatabase:
    conn = ddb.session()
    dbPaths = [DBInfo.dfsDBName, DBInfo.diskDBName]

    def setup_method(self):
        try:
            self.conn.run("1")
        except:
            self.conn.connect(HOST, PORT, USER, PASSWD)
            for dbPath in self.dbPaths:
                script = """
                if(existsDatabase('{dbPath}'))
                    dropDatabase('{dbPath}')
                if(exists('{dbPath}'))
                    rmdir('{dbPath}', true)
                """.format(dbPath=dbPath)
                self.conn.run(script)
    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() +'\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')
        
    #TODO: error to create a SEQ database
    #TODO: error to run function dropPartition()
    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_database_workflow_OLAP(self, _compress, _pickle):
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        conn1.run("""
                  if(existsDatabase("dfs://db_value")){dropDatabase("dfs://db_value")};
                  if(existsDatabase("dfs://db_range")){dropDatabase("dfs://db_range")};
                  if(existsDatabase("dfs://db_combo")){dropDatabase("dfs://db_combo")};
                  if(existsDatabase("dfs://db_hash")){dropDatabase("dfs://db_hash")};
                  if(existsDatabase("dfs://db_list")){dropDatabase("dfs://db_list")};
                  """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-03', freq="D"), dtype="datetime64[D]")
        ids = [1,2,3]
        
        db_v = conn1.database(dbName="db_value",partitionType=keys.VALUE,partitions=dates,dbPath="dfs://db_value", engine="OLAP")
        db_r = conn1.database(dbName="db_range",partitionType=keys.RANGE,partitions=ids,dbPath="dfs://db_range", engine="OLAP")
        db_c = conn1.database(dbName="db_combo",partitionType=keys.COMPO,partitions=[db_v,db_r],dbPath="dfs://db_combo", engine="OLAP")
        db_h = conn1.database(dbName="db_hash",partitionType=keys.HASH,partitions=[keys.DT_INT,3],dbPath="dfs://db_hash", engine="OLAP")
        # db_s = conn1.database(dbName="db_seq",partitionType=keys.SEQ,partitions=[1], engine="OLAP")
        db_l = conn1.database(dbName="db_list",partitionType=keys.LIST,partitions=ids,dbPath="dfs://db_list", engine="OLAP")
        
        assert db_v._getDbName() == "db_value"
        assert db_r._getDbName() == "db_range"
        assert db_c._getDbName() == "db_combo"
        assert db_h._getDbName() == "db_hash"
        assert db_l._getDbName() == "db_list"
        
        assert(conn1.run("""existsDatabase("dfs://db_value")""") == True)
        assert(conn1.run("""existsDatabase("dfs://db_range")""") == True)
        assert(conn1.run("""existsDatabase("dfs://db_combo")""") == True)
        assert(conn1.run("""existsDatabase("dfs://db_hash")""") == True)
        assert(conn1.run("""existsDatabase("dfs://db_list")""") == True)

        
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE])"))

        db_v_tab = db_v.createPartitionedTable(table=t, tableName="db_v_tab",partitionColumns="col3")
        db_r_tab = db_r.createPartitionedTable(table=t, tableName="db_r_tab",partitionColumns="col2")
        db_c_tab = db_c.createPartitionedTable(table=t, tableName="db_c_tab",partitionColumns=["col3","col2"])
        db_h_tab = db_h.createPartitionedTable(table=t, tableName="db_h_tab",partitionColumns="col2")
        db_l_tab = db_l.createPartitionedTable(table=t, tableName="db_l_tab",partitionColumns="col2")
        
        # assert(conn1.run("""existsTable("dfs://db_value","db_v_tab")""") == True)
        # assert(conn1.run("""existsTable("dfs://db_range","db_r_tab")""") == True)
        # assert(conn1.run("""existsTable("dfs://db_combo","db_c_tab")""") == True)
        # assert(conn1.run("""existsTable("dfs://db_hash","db_h_tab")""") == True)
        # assert(conn1.run("""existsTable("dfs://db_list","db_l_tab")""") == True)

        assert(conn1.existsTable("dfs://db_value","db_v_tab") == True)
        assert(conn1.existsTable("dfs://db_range","db_r_tab") == True)
        assert(conn1.existsTable("dfs://db_combo","db_c_tab") == True)
        assert(conn1.existsTable("dfs://db_hash","db_h_tab") == True)
        assert(conn1.existsTable("dfs://db_list","db_l_tab") == True)
        assert(conn1.run("""schema(loadTable("dfs://db_value","db_v_tab"))[`engineType]""") == "OLAP")
        assert(conn1.run("""schema(loadTable("dfs://db_range","db_r_tab"))[`engineType]""") == "OLAP")
        assert(conn1.run("""schema(loadTable("dfs://db_combo","db_c_tab"))[`engineType]""") == "OLAP")
        assert(conn1.run("""schema(loadTable("dfs://db_hash","db_h_tab"))[`engineType]""") == "OLAP")
        assert(conn1.run("""schema(loadTable("dfs://db_list","db_l_tab"))[`engineType]""") == "OLAP")
        
        # conn1.dropPartition(dbPath="dfs://db_value",partitionPaths='2000-01-02',tableName='db_v_tab')
        # conn1.dropPartition(dbPath="dfs://db_value",partitionPaths=['2000-01-03'],tableName='db_v_tab')
        # print(conn1.run("""schema(loadTable("dfs://db_value","db_v_tab"))"""))
        
        conn1.dropTable("dfs://db_value","db_v_tab")
        conn1.dropTable("dfs://db_range","db_r_tab")
        conn1.dropTable("dfs://db_combo","db_c_tab")
        conn1.dropTable("dfs://db_hash","db_h_tab")
        conn1.dropTable("dfs://db_list","db_l_tab")
        
        assert(conn1.existsTable("dfs://db_value","db_v_tab") == False)
        assert(conn1.existsTable("dfs://db_range","db_r_tab") == False)
        assert(conn1.existsTable("dfs://db_combo","db_c_tab") == False)
        assert(conn1.existsTable("dfs://db_hash","db_h_tab") == False)
        assert(conn1.existsTable("dfs://db_list","db_l_tab") == False)
        
        conn1.dropDatabase("dfs://db_value")
        conn1.dropDatabase("dfs://db_range")
        conn1.dropDatabase("dfs://db_combo")
        conn1.dropDatabase("dfs://db_hash")
        conn1.dropDatabase("dfs://db_list")
        
        assert(conn1.run("""existsDatabase("dfs://db_value")""") == False)
        assert(conn1.run("""existsDatabase("dfs://db_range")""") == False)
        assert(conn1.run("""existsDatabase("dfs://db_combo")""") == False)
        assert(conn1.run("""existsDatabase("dfs://db_hash")""") == False)
        assert(conn1.run("""existsDatabase("dfs://db_list")""") == False)
        
        conn1.undefAll()
        conn1.close()
        
    #TODO: error to create a SEQ database
    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_database_workflow_TSDB(self, _compress, _pickle):
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        conn1.run("""
                  if(existsDatabase("dfs://db_value")){dropDatabase("dfs://db_value")};
                  if(existsDatabase("dfs://db_range")){dropDatabase("dfs://db_range")};
                  if(existsDatabase("dfs://db_combo")){dropDatabase("dfs://db_combo")};
                  if(existsDatabase("dfs://db_hash")){dropDatabase("dfs://db_hash")};
                  if(existsDatabase("dfs://db_list")){dropDatabase("dfs://db_list")};
                  """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-03', freq="D"), dtype="datetime64[D]")
        ids = [1,2,3]
        
        db_v = conn1.database(dbName="db_value",partitionType=keys.VALUE,partitions=dates,dbPath="dfs://db_value",engine="TSDB")
        db_r = conn1.database(dbName="db_range",partitionType=keys.RANGE,partitions=ids,dbPath="dfs://db_range",engine="TSDB")
        db_c = conn1.database(dbName="db_combo",partitionType=keys.COMPO,partitions=[db_v,db_r],dbPath="dfs://db_combo",engine="TSDB")
        db_h = conn1.database(dbName="db_hash",partitionType=keys.HASH,partitions=[keys.DT_INT,3],dbPath="dfs://db_hash",engine="TSDB")
        # db_s = conn1.database(dbName="db_seq",partitionType=keys.SEQ,partitions=[1],engine="TSDB")
        db_l = conn1.database(dbName="db_list",partitionType=keys.LIST,partitions=ids,dbPath="dfs://db_list",engine="TSDB")
        
        assert db_v._getDbName() == "db_value"
        assert db_r._getDbName() == "db_range"
        assert db_c._getDbName() == "db_combo"
        assert db_h._getDbName() == "db_hash"
        assert db_l._getDbName() == "db_list"
        
        assert(conn1.run("""existsDatabase("dfs://db_value")""") == True)
        assert(conn1.run("""existsDatabase("dfs://db_range")""") == True)
        assert(conn1.run("""existsDatabase("dfs://db_combo")""") == True)
        assert(conn1.run("""existsDatabase("dfs://db_hash")""") == True)
        assert(conn1.run("""existsDatabase("dfs://db_list")""") == True)
        
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE])"))
        # db_s_tab = db_s.createTable(table=t, tableName="db_s_tab",sortColumns="col2")
        # print(db_s_tab)
        db_v_tab = db_v.createPartitionedTable(table=t, tableName="db_v_tab",partitionColumns="col3",sortColumns="col2")
        db_r_tab = db_r.createPartitionedTable(table=t, tableName="db_r_tab",partitionColumns="col2",sortColumns="col2")
        db_c_tab = db_c.createPartitionedTable(table=t, tableName="db_c_tab",partitionColumns=["col3","col2"],sortColumns="col2")
        db_h_tab = db_h.createPartitionedTable(table=t, tableName="db_h_tab",partitionColumns="col2",sortColumns="col2")
        db_l_tab = db_l.createPartitionedTable(table=t, tableName="db_l_tab",partitionColumns="col2",sortColumns="col2")
        
        assert(conn1.existsTable("dfs://db_value","db_v_tab") == True)
        assert(conn1.existsTable("dfs://db_range","db_r_tab") == True)
        assert(conn1.existsTable("dfs://db_combo","db_c_tab") == True)
        assert(conn1.existsTable("dfs://db_hash","db_h_tab") == True)
        assert(conn1.existsTable("dfs://db_list","db_l_tab") == True)
        assert(conn1.run("""schema(loadTable("dfs://db_value","db_v_tab"))[`engineType]""") == "TSDB")
        assert(conn1.run("""schema(loadTable("dfs://db_range","db_r_tab"))[`engineType]""") == "TSDB")
        assert(conn1.run("""schema(loadTable("dfs://db_combo","db_c_tab"))[`engineType]""") == "TSDB")
        assert(conn1.run("""schema(loadTable("dfs://db_hash","db_h_tab"))[`engineType]""") == "TSDB")
        assert(conn1.run("""schema(loadTable("dfs://db_list","db_l_tab"))[`engineType]""") == "TSDB")
                
        conn1.dropTable("dfs://db_value","db_v_tab")
        conn1.dropTable("dfs://db_range","db_r_tab")
        conn1.dropTable("dfs://db_combo","db_c_tab")
        conn1.dropTable("dfs://db_hash","db_h_tab")
        conn1.dropTable("dfs://db_list","db_l_tab")
        
        assert(conn1.existsTable("dfs://db_value","db_v_tab") == False)
        assert(conn1.existsTable("dfs://db_range","db_r_tab") == False)
        assert(conn1.existsTable("dfs://db_combo","db_c_tab") == False)
        assert(conn1.existsTable("dfs://db_hash","db_h_tab") == False)
        assert(conn1.existsTable("dfs://db_list","db_l_tab") == False)
        
        conn1.dropDatabase("dfs://db_value")
        conn1.dropDatabase("dfs://db_range")
        conn1.dropDatabase("dfs://db_combo")
        conn1.dropDatabase("dfs://db_hash")
        conn1.dropDatabase("dfs://db_list")
        
        assert(conn1.run("""existsDatabase("dfs://db_value")""") == False)
        assert(conn1.run("""existsDatabase("dfs://db_range")""") == False)
        assert(conn1.run("""existsDatabase("dfs://db_combo")""") == False)
        assert(conn1.run("""existsDatabase("dfs://db_hash")""") == False)
        assert(conn1.run("""existsDatabase("dfs://db_list")""") == False)
        
        conn1.undefAll()
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_database_createpartitionedtable_compressMethods(self, _compress, _pickle):
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        conn1.run("""
                  if(existsDatabase("dfs://db_value")){dropDatabase("dfs://db_value")};
                  if(existsDatabase("dfs://db_range")){dropDatabase("dfs://db_range")};
                  if(existsDatabase("dfs://db_combo")){dropDatabase("dfs://db_combo")};
                  if(existsDatabase("dfs://db_hash")){dropDatabase("dfs://db_hash")};
                  if(existsDatabase("dfs://db_list")){dropDatabase("dfs://db_list")};
                  """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-03', freq="D"), dtype="datetime64[D]")
        ids = [1,2,3]
        
        db_v = conn1.database(dbName="db_value",partitionType=keys.VALUE,partitions=dates,dbPath="dfs://db_value")
        db_r = conn1.database(dbName="db_range",partitionType=keys.RANGE,partitions=ids,dbPath="dfs://db_range")
        db_c = conn1.database(dbName="db_combo",partitionType=keys.COMPO,partitions=[db_v,db_r],dbPath="dfs://db_combo")
        db_h = conn1.database(dbName="db_hash",partitionType=keys.HASH,partitions=[keys.DT_INT,3],dbPath="dfs://db_hash")
        # db_s = conn1.database(dbName="db_seq",partitionType=keys.SEQ,partitions=[1])
        db_l = conn1.database(dbName="db_list",partitionType=keys.LIST,partitions=ids,dbPath="dfs://db_list")
        
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE])"))
        # db_s_tab = db_s.createTable(table=t, tableName="db_s_tab",sortColumns="col2")
        # print(db_s_tab)
        db_v_tab = db_v.createPartitionedTable(table=t, tableName="db_v_tab",partitionColumns="col3",compressMethods={"col1":"lz4",
                                                                                                                      "col2":"delta",
                                                                                                                      "col3":"delta"})
        db_r_tab = db_r.createPartitionedTable(table=t, tableName="db_r_tab",partitionColumns="col2",compressMethods={"col1":"lz4",
                                                                                                                      "col2":"delta",
                                                                                                                      "col3":"delta"})
        db_c_tab = db_c.createPartitionedTable(table=t, tableName="db_c_tab",partitionColumns=["col3","col2"],compressMethods={"col1":"lz4",
                                                                                                                      "col2":"delta",
                                                                                                                      "col3":"delta"})
        db_h_tab = db_h.createPartitionedTable(table=t, tableName="db_h_tab",partitionColumns="col2",compressMethods={"col1":"lz4",
                                                                                                                      "col2":"delta",
                                                                                                                      "col3":"delta"})
        db_l_tab = db_l.createPartitionedTable(table=t, tableName="db_l_tab",partitionColumns="col2",compressMethods={"col1":"lz4",
                                                                                                                      "col2":"delta",
                                                                                                                      "col3":"delta"})
        assert(conn1.existsTable("dfs://db_value","db_v_tab") == True)
        assert(conn1.existsTable("dfs://db_range","db_r_tab") == True)
        assert(conn1.existsTable("dfs://db_combo","db_c_tab") == True)
        assert(conn1.existsTable("dfs://db_hash","db_h_tab") == True)
        assert(conn1.existsTable("dfs://db_list","db_l_tab") == True)
        
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_database_createpartitionedtable_keepDuplicates(self, _compress, _pickle):
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        conn1.run("""
                  if(existsDatabase("dfs://db_value")){dropDatabase("dfs://db_value")};
                  """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-05', freq="D"), dtype="datetime64[D]")
        ids = [1,2,3]
        
        db_v = conn1.database(dbName="db_value",partitionType=keys.VALUE,partitions=dates,dbPath="dfs://db_value",engine="TSDB")
        conn1.run("t0=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);tableInsert(t0,`a`b`c`d, 1 2 2 4, [2000.01.01,2000.01.02,2000.01.02,2000.01.04])")
        t = conn1.table(data='t0')
        # db_s_tab = db_s.createTable(table=t, tableName="db_s_tab",sortColumns="col2")
        # print(db_s_tab)
        db_v_tab = db_v.createPartitionedTable(table=t, tableName="db_v_tab",partitionColumns="col3",sortColumns=["col3","col2"],keepDuplicates="LAST")
        db_v_tab2 = db_v.createPartitionedTable(table=t, tableName="db_v_tab2",partitionColumns="col3",sortColumns=["col3","col2"],keepDuplicates="FIRST")
        db_v_tab3 = db_v.createPartitionedTable(table=t, tableName="db_v_tab3",partitionColumns="col3",sortColumns=["col3","col2"],keepDuplicates="ALL")  
        conn1.run("""
                      tableInsert(loadTable("dfs://db_value","db_v_tab"), t0);
                      tableInsert(loadTable("dfs://db_value","db_v_tab2"), t0);
                      tableInsert(loadTable("dfs://db_value","db_v_tab3"), t0);
                      """)
        assert_array_equal(conn1.run("""select * from loadTable("dfs://db_value","db_v_tab")""")["col1"].values, ['a','c','d'])
        assert_array_equal(conn1.run("""select * from loadTable("dfs://db_value","db_v_tab2")""")["col1"].values, ['a','b','d'])
        assert_array_equal(conn1.run("""select * from loadTable("dfs://db_value","db_v_tab3")""")["col1"].values, ['a','b','c','d'])
        
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_database_createtable_OLAP(self, _compress, _pickle):
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        ids = [1,2,3]
        conn1.run("if(existsDatabase('dfs://db_example')){dropDatabase('dfs://db_example')};")
        
        db = conn1.database(dbName="db_value",partitionType=keys.VALUE,partitions=ids,dbPath="dfs://db_example",engine="OLAP")
        t = conn1.table(data=conn1.run("table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3)"))
        db.createTable(table=t,tableName="pt").append(t)
        
        assert_frame_equal(conn1.run("select * from loadTable('dfs://db_example', 'pt')"), pd.DataFrame({
                                                                                                'col1': np.array(['APPL','TESLA','GOOGLE','PDD']),
                                                                                                'col2': np.array([1,2,3,4],dtype=np.int32),
                                                                                                'col3': np.array(['2022-01-01','2022-01-02','2022-01-03','2022-01-04'],dtype='datetime64[ns]')}))
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_database_createtable_TSDB(self, _compress, _pickle):
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        ids = [1,2,3]
        conn1.run("if(existsDatabase('dfs://db_example')){dropDatabase('dfs://db_example')};")
        
        db = conn1.database(dbName="db_value",partitionType=keys.VALUE,partitions=ids,dbPath="dfs://db_example",engine="TSDB")
        t = conn1.table(data=conn1.run("table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3)"))
        db.createTable(table=t,tableName="pt",sortColumns='col2').append(t)
        
        assert_frame_equal(conn1.run("select * from loadTable('dfs://db_example', 'pt')"), pd.DataFrame({
                                                                                                'col1': np.array(['APPL','TESLA','GOOGLE','PDD']),
                                                                                                'col2': np.array([1,2,3,4],dtype=np.int32),
                                                                                                'col3': np.array(['2022-01-01','2022-01-02','2022-01-03','2022-01-04'],dtype='datetime64[ns]')}))
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_create_dfs_database_range_partition(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))

        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 11, 21]),
              'partitionSites': None,
              'partitionTypeName': 'RANGE',
              'partitionType': 2}
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21,dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_database_hash_partition(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db = conn1.database('db', partitionType=keys.HASH, partitions=[keys.DT_INT, 2], dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))

        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': 2,
              'partitionSites': None,
              'partitionTypeName': 'HASH',
              'partitionType': 5}
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert(re['partitionSchema'] == dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id':[1,2,3,4,5], 'val':[10, 20, 30, 40, 50]})
        t = conn1.table(data=df)
        pt = db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id')
        pt.append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(re['id']), df['id'])
        assert_array_equal(np.sort(re['val']), df['val'])
        dt = db.createTable(table=t, tableName='dt')
        dt.append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(re['id']), df['id'])
        assert_array_equal(np.sort(re['val']), df['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_database_value_partition(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db = conn1.database('db', partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 2, 3]),
              'partitionSites': None,
              'partitionTypeName': 'VALUE',
              'partitionType': 1}
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id':np.array([1, 2, 3, 1, 2, 3],dtype=np.int32), 'val':[11, 12, 13, 14, 15, 16]})
        t = conn1.table(data=df)
        pt = db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(df['id']), np.sort(re['id']))
        assert_array_equal(np.sort(df['val']), np.sort(re['val']))
        dt = db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(np.sort(df['id']), np.sort(re['id']))
        assert_array_equal(np.sort(df['val']), np.sort(re['val']))
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_database_list_partition(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db = conn1.database('db', partitionType=keys.LIST, partitions=[['IBM', 'ORCL', 'MSFT'], ['GOOG', 'FB']],
                        dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([np.array(['IBM', 'ORCL', 'MSFT']), np.array(['GOOG', 'FB'])],dtype=object),
              'partitionSites': None,
              'partitionTypeName': 'LIST',
              'partitionType': 3}
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'][0], dct['partitionSchema'][0])
        assert_array_equal(re['partitionSchema'][1], dct['partitionSchema'][1])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'sym':['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'], 'val':[1,2,3,4,5]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='sym').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_database_value_partition_np_date(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        dates=np.array(pd.date_range(start='20120101', end='20120110'), dtype="datetime64[D]")
        db = conn1.database('db', partitionType=keys.VALUE, partitions=dates,
                        dbPath=DBInfo.dfsDBName)
        
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
              'partitionSchema': np.array(pd.date_range(start='20120101', end='20120110'), dtype="datetime64[D]"),
              'partitionSites': None
              }
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert(re['partitionType'] == dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'datetime':[np.datetime64('2012-01-01','D'), np.datetime64('2012-01-02','D')], 'sym':['AA', 'BB'], 'val':[1,2]},dtype='object')
        df_= pd.DataFrame({'datetime':pd.Series([np.datetime64('2012-01-01','D'), np.datetime64('2012-01-02','D')],dtype='datetime64[ns]'), 'sym':['AA', 'BB'], 'val':[1,2]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datetime').append(t)
        re = conn1.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(re['name'], ['datetime', 'sym', 'val'])
        assert_array_equal(re['typeString'], ['DATE', 'STRING', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datetime'], df_['datetime'])
        assert_array_equal(re['sym'], df_['sym'])
        assert_array_equal(re['val'], df_['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datetime'], df_['datetime'])
        assert_array_equal(re['sym'], df_['sym'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_database_value_partition_np_month(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        months=np.array(pd.date_range(start='2012-01', end='2012-10', freq="M"), dtype="datetime64[M]")
        db = conn1.database('db', partitionType=keys.VALUE, partitions=months,
                      dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
              'partitionSchema': months,
              'partitionSites': None
              }
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert(re['partitionType'] == dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'date': [np.datetime64('2012-01','M'), np.datetime64('2012-02','M'), np.datetime64('2012-03','M'),np.datetime64('2012-04','M')], 'val':[1,2,3,4]},dtype='object')
        df_ = pd.DataFrame({'date': pd.Series([np.datetime64('2012-01','M'), np.datetime64('2012-02','M'), np.datetime64('2012-03','M'),np.datetime64('2012-04','M')],dtype='datetime64[s]' if PANDAS_VERSION>(2,0,0) and _pickle else 'datetime64[ns]'), 'val':[1,2,3,4]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='date').append(t)
        scm = conn1.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(scm['name'], ['date', 'val'])
        assert_array_equal(scm['typeString'], ['MONTH', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['date'], df_['date'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_database_value_partition_np_datehour(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        times=np.array(pd.date_range(start='2012-01-01T00', end='2012-01-01T05', freq='h'), dtype="datetime64[h]")
        db=conn1.database('db', partitionType=keys.VALUE, partitions=times,
                      dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
              'partitionSchema': times,
              'partitionSites': None
              }
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert(re['partitionType'] == dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'hour': [np.datetime64('2012-01-01T00', 'h'), np.datetime64('2012-01-01T01', 'h'),
                                    np.datetime64('2012-01-01T02', 'h')], 'val': [1, 2, 3]}, dtype='object')
        df_ = pd.DataFrame({'hour': pd.Series([np.datetime64('2012-01-01T00', 'h'), np.datetime64('2012-01-01T01', 'h'),
                                               np.datetime64('2012-01-01T02', 'h')], dtype='datetime64[ns]'),
                            'val': [1, 2, 3]})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='hour').append(t)
        rtn = conn1.run(f'schema(loadTable("{DBInfo.dfsDBName}","pt"))["colDefs"]')
        assert_array_equal(rtn['name'], ['hour', 'val'])
        assert_array_equal(rtn['typeString'], ['DATEHOUR', 'LONG'])
        rtn=conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(rtn['hour'],df_['hour'])
        assert_array_equal(rtn['val'],df_['val'])
        db.createTable(table=t, tableName='dt').append(t)
        rtn = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(rtn['hour'],df_['hour'])
        assert_array_equal(rtn['val'],df_['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_database_value_partition_np_arange_date(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        dates=np.arange('2012-01-01', '2012-01-10', dtype='datetime64[D]')
        db = conn1.database('db', partitionType=keys.VALUE, partitions=dates,
                        dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
              'partitionSchema': dates,
              'partitionSites': None
              }
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert(re['partitionType'] == dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'datetime':[np.datetime64('2012-01-01','D'), np.datetime64('2012-01-02','D')], 'sym':['AA', 'BB'], 'val':[1,2]},dtype='object')
        df_= pd.DataFrame({'datetime':pd.Series([np.datetime64('2012-01-01','D'), np.datetime64('2012-01-02','D')],dtype='datetime64[ns]'), 'sym':['AA', 'BB'], 'val':[1,2]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datetime').append(t)
        re = conn1.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(re['name'], ['datetime', 'sym', 'val'])
        assert_array_equal(re['typeString'], ['DATE', 'STRING', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datetime'], df_['datetime'])
        assert_array_equal(re['sym'], df_['sym'])
        assert_array_equal(re['val'], df_['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datetime'], df_['datetime'])
        assert_array_equal(re['sym'], df_['sym'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_database_value_partition_np_arange_month(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        months=np.arange('2012-01', '2012-10', dtype='datetime64[M]')
        db = conn1.database('db', partitionType=keys.VALUE, partitions=months,
                        dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
              'partitionSchema': months,
              'partitionSites': None
              }
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert(re['partitionType'] == dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': 1,
              'partitionSchema': months,
              'partitionSites': None
              }
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert(re['partitionType'] == dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'date': [np.datetime64('2012-01','M'), np.datetime64('2012-02','M'), np.datetime64('2012-03','M'),np.datetime64('2012-04','M')], 'val':[1,2,3,4]},dtype='object')
        df_ = pd.DataFrame({'date': pd.Series([np.datetime64('2012-01','M'), np.datetime64('2012-02','M'), np.datetime64('2012-03','M'),np.datetime64('2012-04','M')],dtype='datetime64[s]' if _pickle else 'datetime64[ns]'), 'val':[1,2,3,4]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='date').append(t)
        scm = conn1.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(scm['name'], ['date', 'val'])
        assert_array_equal(scm['typeString'], ['MONTH', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['date'], df_['date'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_database_compo_partition(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db1 = conn1.database('db1', partitionType=keys.VALUE,
                  partitions=np.array(["2012-01-01", "2012-01-06"], dtype="datetime64"), dbPath='')
        db2 = conn1.database('db2', partitionType=keys.RANGE,
                        partitions=[1, 6, 11], dbPath='')
        db = conn1.database('db', keys.COMPO, partitions=[db1, db2], dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
                'partitionType': [1, 2],
              'partitionSchema': [np.array(["2012-01-01", "2012-01-06"], dtype="datetime64"), np.array([1, 6, 11])],
              'partitionSites': None
              }
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionType'], dct['partitionType'])
        assert_array_equal(re['partitionSchema'][0], dct['partitionSchema'][0])
        assert_array_equal(re['partitionSchema'][1], dct['partitionSchema'][1])
        df = pd.DataFrame({'date':np.array(['2012-01-01', '2012-01-01', '2012-01-06', '2012-01-06'], dtype='datetime64'), 'val': np.array([1, 6, 1, 6],dtype=np.int32)})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns=['date', 'val']).append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['date'], df['date'])
        assert_array_equal(re['val'], df['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['date'], df['date'])
        assert_array_equal(re['val'], df['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_dfs_table_with_chineses_column_name(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 11, 21], dtype=np.int32),
              'partitionSites': None,
              'partitionTypeName':'RANGE',
              'partitionType': 2}
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'编号': np.arange(1, 21,dtype=np.int32), '值': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='编号').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['编号'], np.arange(1, 21))
        assert_array_equal(re['值'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['编号'], np.arange(1, 21))
        assert_array_equal(re['值'], np.repeat(1, 20))
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_database_already_exists_with_partition_none(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        dbPath=DBInfo.dfsDBName
        script='''
        dbPath='{db}'
        db = database(dbPath, VALUE, 1 2 3 4 5)
        t = table(1..5 as id, rand(string('A'..'Z'),5) as val)
        pt = db.createPartitionedTable(t, `pt, `id).append!(t)
        '''.format(db=dbPath)
        conn1.run(script)
        assert(existsDB(DBInfo.dfsDBName))
        db = conn1.database(dbPath=dbPath)
        df = pd.DataFrame({'id':np.array([1,2,3],dtype=np.int32), 'sym':['A', 'B','C']})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t,tableName='pt1',partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt1',dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['sym'],np.array(['A','B','C']))
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])
    def test_database_dfs_table_value_datehour_as_partitionSchema(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        datehour = np.array(["2021-01-01T01","2021-01-01T02","2021-01-01T03","2021-01-01T04"],dtype="datetime64[h]")
        db = conn1.database('db', partitionType=keys.VALUE, partitions=datehour, dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
        'partitionType': 1,
        'partitionSchema': datehour,
        'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert(re['partitionType'] == dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'datehour': [np.datetime64("2021-01-01T01",'h'),np.datetime64("2021-01-01T02",'h'),np.datetime64("2021-01-01T03",'h'),np.datetime64("2021-01-01T04",'h')],'val':[1,2,3,4]},dtype='object')
        df_=pd.DataFrame({'datehour':pd.Series([np.datetime64('2021-01-01T01','h'), np.datetime64('2021-01-01T02','h'), np.datetime64('2021-01-01T03','h'),np.datetime64("2021-01-01T04",'h')],dtype='datetime64[ns]'),'val':[1,2,3,4]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datehour').append(t)
        scm = conn1.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(scm['name'],['datehour','val'])
        assert_array_equal(scm['typeString'],['DATEHOUR','LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datehour'], df_['datehour'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_database_dfs_table_range_datehour_as_partitionSchema(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        datehour = np.array(["2012-01-01T00","2012-01-01T01","2012-01-01T02","2012-01-01T03","2012-01-01T04"],dtype="datetime64")
        db = conn1.database('db', partitionType=keys.RANGE, partitions=datehour, dbPath=DBInfo.dfsDBName)
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
        'partitionType': 2,
        'partitionSchema': datehour,
        'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert(re['databaseDir'] == dct['databaseDir'])
        assert(re['partitionType'] == dct['partitionType'])
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'datehour': [np.datetime64('2012-01-01T00', 'h'), np.datetime64('2012-01-01T01', 'h'),
                                    np.datetime64('2012-01-01T02', 'h')], 'val': [1, 2, 3]}, dtype='object')
        df_ = pd.DataFrame({'datehour': pd.Series([np.datetime64('2012-01-01T00', 'h'), np.datetime64('2012-01-01T01', 'h'),
                                               np.datetime64('2012-01-01T02', 'h')], dtype='datetime64[ns]'),
                            'val': [1, 2, 3]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datehour').append(t)
        scm = conn1.run("schema(loadTable('{dbPath}', 'pt')).colDefs".format(dbPath=DBInfo.dfsDBName))
        assert_array_equal(scm['name'],['datehour','val'])
        assert_array_equal(scm['typeString'],['DATEHOUR','LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['datehour'], df_['datehour'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_database_engine_olap(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="OLAP")
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 11, 21]),
              'partitionSites': None,
              'partitionTypeName': 'RANGE',
              'partitionType': 2,
              'engineType':'OLAP'}
        re = conn1.run("schema(db)")
        assert(re['engineType'] == dct['engineType'])
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21,dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_database_engine_tsdb(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="TSDB")
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 11, 21]),
              'partitionSites': None,
              'partitionTypeName': 'RANGE',
              'partitionType': 2,
              'engineType':'TSDB'}
        re = conn1.run("schema(db)")
        assert(re['engineType'] == dct['engineType'])
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21,dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', sortColumns="val").append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        df = pd.DataFrame({'id': np.arange(20, 0, -1), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createTable(table=t, tableName='dt', sortColumns="id").append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_database_atomic_TRANS(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, atomic="TRANS")
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 11, 21]),
              'partitionSites': None,
              'partitionTypeName': 'RANGE',
              'partitionType': 2,
              'atomic':'TRANS'}
        re = conn1.run("schema(db)")
        assert(re['atomic'] == dct['atomic'])
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21,dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_database_atomic_CHUNK(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, atomic="CHUNK")
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 11, 21]),
              'partitionSites': None,
              'partitionTypeName': 'RANGE',
              'partitionType': 2,
              'atomic':'CHUNK'}
        re = conn1.run("schema(db)")
        assert(re['atomic'] == dct['atomic'])
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21,dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    #需要设置enableChunkGranularityConfig=true
    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_database_chunkGranularity_TABLE(self, _compress, _pickle):
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        conn1 = ddb.session(HOST,PORT,USER,PASSWD,compress=_compress,enablePickle=_pickle,enableChunkGranularityConfig=True)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, chunkGranularity="TABLE")
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 11, 21]),
              'partitionSites': None,
              'partitionTypeName': 'RANGE',
              'partitionType': 2,
              'chunkGranularity':'TABLE'}
        re = conn1.run("schema(db)")
        assert(re['chunkGranularity'] == dct['chunkGranularity'])
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21,dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    #需要设置enableChunkGranularityConfig=true
    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_database_chunkGranularity_DATABASE(self, _compress, _pickle):
        conn1 = ddb.session(enableChunkGranularityConfig=True,compress=_compress,enablePickle=_pickle)
        conn1.connect(HOST, PORT, USER, PASSWD)
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, chunkGranularity="DATABASE")
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 11, 21]),
              'partitionSites': None,
              'partitionTypeName': 'RANGE',
              'partitionType': 2,
              'chunkGranularity':'DATABASE'}
        re = conn1.run("schema(db)")
        assert(re['chunkGranularity'] == dct['chunkGranularity'])
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21,dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    #需要设置enableChunkGranularityConfig=true
    @pytest.mark.DATABASE
    @pytest.mark.parametrize('_compress', [True, False], ids=["COMPRESS_OPEN", "COMPRESS_CLOSE"])
    @pytest.mark.parametrize('_pickle', [True, False], ids=["PICKLE_OPEN", "PICKLE_CLOSE"])   
    def test_create_database_engine_atomic_chunkGranularity(self, _compress, _pickle):
        conn1 = ddb.session(enableChunkGranularityConfig=True,compress=_compress,enablePickle=_pickle)
        conn1.connect(HOST, PORT, USER, PASSWD)
        if existsDB(DBInfo.dfsDBName):
            dropDB(DBInfo.dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=DBInfo.dfsDBName, engine="TSDB", atomic="CHUNK", chunkGranularity="DATABASE")
        assert(existsDB(DBInfo.dfsDBName))
        dct = {'databaseDir': DBInfo.dfsDBName,
              'partitionSchema': np.array([1, 11, 21]),
              'partitionSites': None,
              'partitionTypeName': 'RANGE',
              'partitionType': 2,
              'engineType':'TSDB',
              'atomic':'CHUNK',
              'chunkGranularity':'DATABASE'}
        re = conn1.run("schema(db)")
        assert(re['engineType'] == dct['engineType'])
        assert(re['atomic'] == dct['atomic'])
        assert(re['chunkGranularity'] == dct['chunkGranularity'])
        assert(re['databaseDir'] == dct['databaseDir'])
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert(re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'id': np.arange(1, 21,dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', sortColumns="val").append(t)
        re = conn1.loadTable(tableName='pt', dbPath=DBInfo.dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    @pytest.mark.DATABASE
    def test_database_save_table(self):
        tb=self.conn.table(data=pd.DataFrame({'a':[1,2,3]}))
        assert self.conn.saveTable(tb,WORK_DIR+'/test')
        
if __name__ == '__main__':
    pytest.main(["-s", "test/test_database.py"])