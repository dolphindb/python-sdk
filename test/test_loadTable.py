import dolphindb as ddb
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal

from setup.settings import *
from setup.utils import get_pid


class DBInfo:
    dfsDBName = 'dfs://testLoadTable'
    diskDBName = REMOTE_WORK_DIR + '/testLoadTable'
    table1 = 'tb1'
    table2 = 'tb2'


def create_dfs_dimension_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,RANGE,1..10)
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createTable(tdata,`{DBInfo.table1}).append!(tdata)
        db.createTable(tdata,`{DBInfo.table2}).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_range_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,RANGE,0..10*10000+1)
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_hash_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[INT,10])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_value_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,VALUE,2010.01.01..2010.01.30)
        n=100000
        tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_list_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`sym).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`sym).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_compo_range_range_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',RANGE,1 3 5 7 9 11)
        db=database(dbPath,COMPO,[db1,db2])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_compo_range_hash_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',HASH,[INT,10])
        db=database(dbPath,COMPO,[db1,db2])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_compo_range_value_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',VALUE,1..10)
        db=database(dbPath,COMPO,[db1,db2])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_compo_range_list_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        db=database(dbPath,COMPO,[db1,db2])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`sym).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`sym).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_compo_range_hash_list_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',HASH,[INT,10])
        db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        db=database(dbPath,COMPO,[db1,db2,db3])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id`sym).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id`sym).append!(tdata)
    '''
    s.run(ddb_script)


def create_dfs_compo_range_value_list_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',VALUE,1..10)
        db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        db=database(dbPath,COMPO,[db1,db2,db3])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id`sym).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id`sym).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_unpartitioned_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath)
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        saveTable(db,tdata,`{DBInfo.table1})
        saveTable(db,tdata,`{DBInfo.table2})
    '''
    s.run(ddb_script)


def create_disk_range_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,RANGE,0..10*10000+1)
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_hash_db(s: ddb.session):
    s.connect(HOST, PORT, USER, PASSWD)
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[INT,10])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_value_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,VALUE,2010.01.01..2010.01.30)
        n=100000
        tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_list_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`sym).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`sym).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_compo_range_range_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',RANGE,1 3 5 7 9 11)
        db=database(dbPath,COMPO,[db1,db2])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_compo_range_hash_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',HASH,[INT,10])
        db=database(dbPath,COMPO,[db1,db2])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_compo_range_value_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',VALUE,1..10)
        db=database(dbPath,COMPO,[db1,db2])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_compo_range_list_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        db=database(dbPath,COMPO,[db1,db2])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`sym).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`sym).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_compo_range_hash_list_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',HASH,[INT,10])
        db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        db=database(dbPath,COMPO,[db1,db2,db3])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id`sym).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id`sym).append!(tdata)
    '''
    s.run(ddb_script)


def create_disk_compo_range_value_list_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.diskDBName}'
        if(exists(dbPath))
            dropDatabase(dbPath)
        db1=database('',RANGE,2010.01M+0..12)
        db2=database('',VALUE,1..10)
        db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        db=database(dbPath,COMPO,[db1,db2,db3])
        n=100000
        tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
        db.createPartitionedTable(tdata,`{DBInfo.table1},`date`id`sym).append!(tdata)
        db.createPartitionedTable(tdata,`{DBInfo.table2},`date`id`sym).append!(tdata)
    '''
    s.run(ddb_script)


class TestLoadTable:
    conn = ddb.session()
    dbPaths = [DBInfo.dfsDBName, DBInfo.diskDBName]

    def setup_method(self):
        try:
            self.conn.run("1")
        except RuntimeError:
            self.conn.connect(HOST, PORT, USER, PASSWD)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def test_loadTable_dfs_dimension(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_dimension_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_range(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_range_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_range_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_range_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tbName1, dbPath, [5000, 15000])

    def test_loadTable_dfs_range_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_range_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_hash(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_hash_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_hash_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_hash_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[1, 2])

    def test_loadTable_dfs_hash_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_hash_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_value(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_value_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_value_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_value_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_value_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_value_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_list_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_list_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["`DOP", "`BBVC"])

    def test_loadTable_dfs_list_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_list_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_range(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_range_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_range_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_range_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_range_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_range_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_hash(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_hash_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_hash_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_value(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_value_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_value_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_list_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_list_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_list_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_list_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_hash_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_hash_list_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_list_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_hash_list_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_list_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_value_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_value_list_param_partitions(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_list_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_value_list_param_memoryMode(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_list_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_disk_unpartitioned(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_unpartitioned_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_range(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_range_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_range_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_range_db(self.conn)
        rs = self.conn.run("select * from loadTable('{db}','{tb}') where id<20001".format(db=dbPath, tb=tbName1))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[5000, 15000])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_range_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_range_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_hash(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_hash_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_hash_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_hash_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}') where id in [1,3,5]")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[1, 3, 5])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_hash_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_hash_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")

        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_value(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_value_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_value_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_value_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.01, 2010.01.30]")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_value_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_value_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_list_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}') where sym in `DOP`ASZ`FSD`BBVC`AWQ`DS")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["DOP", "FSD", "AWQ"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_list_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_range(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_range_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_range_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_range_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_range_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_range_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_hash(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_value(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_list_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_list_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_hash_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_list_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_list_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_value_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_list_param_partitions(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_list_param_memoryMode(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_list_db(self.conn)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(self.conn.run(f"exec memSize from getSessionMemoryStat() where userId='{USER}'"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_value_partition_string_scalar(self):
        myDBName = REMOTE_WORK_DIR + "/db1"
        script = f'''
            login("{USER}","{PASSWD}")
            if(exists("{myDBName}"))
                dropDatabase("{myDBName}")
            db=database("{myDBName}", VALUE, ["AAA", "BBB", "CCC"])
            t=table(take(["AAA", "BBB", "CCC"], 1000) as sym, rand(100.0, 1000) as val)
            db.createPartitionedTable(t, "pt", "sym").append!(t)
        '''
        self.conn.run(script)
        res = self.conn.loadTable(tableName="pt", dbPath=myDBName, partitions="AAA", memoryMode=True).toDF()
        expected = self.conn.run(f"select * from loadTable('{myDBName}', 'pt') where sym='AAA'")
        assert_frame_equal(res, expected)

    def test_loadTable_disk_value_partition_string_vector(self):
        myDBName = REMOTE_WORK_DIR + "/db1"
        script = f'''
            login("{USER}","{PASSWD}")
            if(exists("{myDBName}"))
                dropDatabase("{myDBName}")
            db=database("{myDBName}", VALUE, ["AAA", "BBB", "CCC"])
            t=table(take(["AAA", "BBB", "CCC"], 1000) as sym, rand(100.0, 1000) as val)
            db.createPartitionedTable(t, "pt", "sym").append!(t)
        '''
        self.conn.run(script)
        res = self.conn.loadTable(tableName="pt", dbPath=myDBName, partitions=["AAA", "BBB"], memoryMode=True).toDF()
        expected = self.conn.run(f"select * from loadTable('{myDBName}', 'pt') where sym='AAA' or sym='BBB'")
        assert_frame_equal(res, expected)

    def test_loadTable_in_menory_table_drop_str(self):
        self.conn.run("in_menory_table=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)")
        in_menory_table = self.conn.loadTable(tableName='in_menory_table')
        in_menory_table.drop('c1')
        expect = pd.DataFrame({'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(in_menory_table.toDF(), expect)

    def test_loadTable_in_menory_table_drop_list_len_eq_0(self):
        self.conn.run("in_menory_table=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)")
        in_menory_table = self.conn.loadTable(tableName='in_menory_table')
        in_menory_table.drop([])
        expect = pd.DataFrame({'c1': pd.Series([1, 2, 3], dtype='int32'), 'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']})
        assert_frame_equal(in_menory_table.toDF(), expect)

    def test_loadTable_in_menory_table_drop_list_len_eq_1(self):
        self.conn.run("in_menory_table=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)")
        in_menory_table = self.conn.loadTable(tableName='in_menory_table')
        in_menory_table.drop(['c1'])
        expect = pd.DataFrame({'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(in_menory_table.toDF(), expect)

    def test_loadTable_in_menory_table_drop_list_len_gt_1(self):
        self.conn.run("in_menory_table=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)")
        in_menory_table = self.conn.loadTable(tableName='in_menory_table')
        in_menory_table.drop(['c1', 'c2'])
        expect = pd.DataFrame({'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(in_menory_table.toDF(), expect)

    def test_loadTable_olap_table_drop_str(self):
        self.conn.run("""
            if (existsDatabase('dfs://test_loadTable_olap_table_drop')){
                dropDatabase('dfs://test_loadTable_olap_table_drop')
            }
            db=database('dfs://test_loadTable_olap_table_drop',VALUE,`1`2`3,engine=`OLAP)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`tb,`c2).append!(t)
        """)
        olap_table = self.conn.loadTable(tableName='tb', dbPath='dfs://test_loadTable_olap_table_drop')
        olap_table.drop('c1')
        expect = pd.DataFrame({'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(olap_table.toDF(), expect)

    def test_loadTable_olap_table_drop_list_len_eq_0(self):
        self.conn.run("""
            if (existsDatabase('dfs://test_loadTable_olap_table_drop')){
                dropDatabase('dfs://test_loadTable_olap_table_drop')
            }
            db=database('dfs://test_loadTable_olap_table_drop',VALUE,`1`2`3,engine=`OLAP)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`tb,`c2).append!(t)
        """)
        olap_table = self.conn.loadTable(tableName='tb', dbPath='dfs://test_loadTable_olap_table_drop')
        olap_table.drop([])
        expect = pd.DataFrame({'c1': pd.Series([1, 2, 3], dtype='int32'), 'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']})
        assert_frame_equal(olap_table.toDF(), expect)

    def test_loadTable_olap_table_drop_list_len_eq_1(self):
        self.conn.run("""
            if (existsDatabase('dfs://test_loadTable_olap_table_drop')){
                dropDatabase('dfs://test_loadTable_olap_table_drop')
            }
            db=database('dfs://test_loadTable_olap_table_drop',VALUE,`1`2`3,engine=`OLAP)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`tb,`c2).append!(t)
        """)
        olap_table = self.conn.loadTable(tableName='tb', dbPath='dfs://test_loadTable_olap_table_drop')
        olap_table.drop(['c1'])
        expect = pd.DataFrame({'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(olap_table.toDF(), expect)

    def test_loadTable_olap_table_drop_list_len_gt_1(self):
        self.conn.run("""
            if (existsDatabase('dfs://test_loadTable_olap_table_drop')){
                dropDatabase('dfs://test_loadTable_olap_table_drop')
            }
            db=database('dfs://test_loadTable_olap_table_drop',VALUE,`1`2`3,engine=`OLAP)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`tb,`c2).append!(t)
        """)
        olap_table = self.conn.loadTable(tableName='tb', dbPath='dfs://test_loadTable_olap_table_drop')
        with pytest.raises(RuntimeError):
            olap_table.drop(['c1', 'c2'])

    def test_loadTable_tsdb_table_drop_list_len_eq_1(self):
        self.conn.run("""
            if (existsDatabase('dfs://test_loadTable_tsdb_table_drop')){
                dropDatabase('dfs://test_loadTable_tsdb_table_drop')
            }
            db=database('dfs://test_loadTable_tsdb_table_drop',VALUE,`1`2`3,engine=`TSDB)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`tb,`c2,sortColumns=`c3).append!(t)
        """)
        tsdb_table = self.conn.loadTable(tableName='tb', dbPath='dfs://test_loadTable_tsdb_table_drop')
        with pytest.raises(RuntimeError):
            tsdb_table.drop(['c1'])


class TestLoadTableBySQL:
    conn = ddb.session()
    dbPaths = [DBInfo.dfsDBName, DBInfo.diskDBName]

    def setup_method(self):
        try:
            self.conn.run("1")
        except RuntimeError:
            self.conn.connect(HOST, PORT, USER, PASSWD)

    # def teardown_method(self):
    #     self.conn.undefAll()
    #     self.conn.clearAllCache()

    @classmethod
    def setup_class(cls):
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' start, pid: ' + get_pid() + '\n')

    @classmethod
    def teardown_class(cls):
        cls.conn.close()
        if AUTO_TESTING:
            with open('progress.txt', 'a+') as f:
                f.write(cls.__name__ + ' finished.\n')

    def test_loadTableBySQL_dfs_dimension(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_dimension_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")

    def test_loadTableBySQL_dfs_range(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_range_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_hash(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_hash_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_value(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_value_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_range(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_range_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_hash(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_value(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_hash_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_hash_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_value_list(self):
        dbPath = DBInfo.dfsDBName
        tbName1 = DBInfo.table1
        create_dfs_compo_range_value_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_unpartitioned(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_unpartitioned_db(self.conn)
        with pytest.raises(RuntimeError):
            self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")

    def test_loadTableBySQL_disk_range(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_range_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_hash(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_hash_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_value(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_value_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_range(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_range_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_hash(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_value(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_hash_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_hash_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_value_list(self):
        dbPath = DBInfo.diskDBName
        tbName1 = DBInfo.table1
        create_disk_compo_range_value_list_db(self.conn)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())
