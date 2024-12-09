import inspect

import dolphindb as ddb
import pandas as pd
import pytest
from numpy.testing import assert_array_equal
from pandas.testing import assert_frame_equal

from setup.settings import HOST, PORT, USER, PASSWD, REMOTE_WORK_DIR


class TestLoadTable:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    def test_loadTable_dfs_dimension(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,1..10)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createTable(tdata,`{tbName1}).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_range_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tbName1, dbPath, [5000, 15000])

    def test_loadTable_dfs_range_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_hash_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[1, 2])

    def test_loadTable_dfs_hash_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
            n=100
            tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_value_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
            n=100
            tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_value_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
            n=100
            tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_list_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["`DOP", "`BBVC"])

    def test_loadTable_dfs_list_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_range_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_range_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_hash_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_hash_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_value_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_value_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_list_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_list_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_hash_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_hash_list_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_hash_list_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_dfs_compo_range_value_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_dfs_compo_range_value_list_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])

    def test_loadTable_dfs_compo_range_value_list_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)

    def test_loadTable_disk_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_range_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run("select * from loadTable('{db}','{tb}') where id<20001".format(db=dbPath, tb=tbName1))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[5000, 15000])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_range_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_hash_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}') where id in [1,3,5]")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=[1, 3, 5])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_hash_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
            n=100
            tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_value_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
            n=100
            tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.01, 2010.01.30]")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.01.30"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_value_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
            n=100
            tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_list_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}') where sym in `DOP`ASZ`FSD`BBVC`AWQ`DS")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["DOP", "FSD", "AWQ"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_list_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_range_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_range_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_list_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_list_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_hash_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_list_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_hash_list_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_compo_range_value_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_list_param_partitions(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date between 2010.01.01:2010.01.31 or date between 2010.04.01:2010.04.30")
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["2010.01.01", "2010.04.25"])
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTable_disk_compo_range_value_list_param_memoryMode(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(f"select * from loadTable('{dbPath}','{tbName1}')")
        before = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        tmp = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, memoryMode=True)
        after = list(
            self.conn.run("exec memSize from getSessionMemoryStat() where sessionId=getCurrentSessionAndUser()[0]"))
        assert_frame_equal(tmp.toDF(), rs)
        assert_array_equal(after >= before, True)

    def test_loadTable_disk_value_partition_string_scalar(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        script = f'''
            login("{USER}","{PASSWD}")
            if(existsDatabase("{dbPath}"))
                dropDatabase("{dbPath}")
            db=database("{dbPath}", VALUE, ["AAA", "BBB", "CCC"])
            t=table(take(["AAA", "BBB", "CCC"], 1000) as sym, rand(100.0, 1000) as val)
            db.createPartitionedTable(t, "{tbName1}", "sym").append!(t)
        '''
        self.conn.run(script)
        res = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions="AAA", memoryMode=True).toDF()
        expected = self.conn.run(f"select * from loadTable('{dbPath}', '{tbName1}') where sym='AAA'")
        assert_frame_equal(res, expected)

    def test_loadTable_disk_value_partition_string_vector(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        script = f'''
            login("{USER}","{PASSWD}")
            if(exists("{dbPath}"))
                dropDatabase("{dbPath}")
            db=database("{dbPath}", VALUE, ["AAA", "BBB", "CCC"])
            t=table(take(["AAA", "BBB", "CCC"], 1000) as sym, rand(100.0, 1000) as val)
            db.createPartitionedTable(t, "{tbName1}", "sym").append!(t)
        '''
        self.conn.run(script)
        res = self.conn.loadTable(tableName=tbName1, dbPath=dbPath, partitions=["AAA", "BBB"], memoryMode=True).toDF()
        expected = self.conn.run(f"select * from loadTable('{dbPath}', '{tbName1}') where sym='AAA' or sym='BBB'")
        assert_frame_equal(res, expected)

    def test_loadTable_in_memory_table_drop_str(self):
        self.conn.run("in_memory_table=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)")
        in_memory_table = self.conn.loadTable(tableName='in_memory_table')
        in_memory_table.drop('c1')
        expect = pd.DataFrame({'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(in_memory_table.toDF(), expect)

    def test_loadTable_in_memory_table_drop_list_len_eq_0(self):
        self.conn.run("in_memory_table=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)")
        in_memory_table = self.conn.loadTable(tableName='in_memory_table')
        in_memory_table.drop([])
        expect = pd.DataFrame({'c1': pd.Series([1, 2, 3], dtype='int32'), 'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']})
        assert_frame_equal(in_memory_table.toDF(), expect)

    def test_loadTable_in_memory_table_drop_list_len_eq_1(self):
        self.conn.run("in_memory_table=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)")
        in_memory_table = self.conn.loadTable(tableName='in_memory_table')
        in_memory_table.drop(['c1'])
        expect = pd.DataFrame({'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(in_memory_table.toDF(), expect)

    def test_loadTable_in_memory_table_drop_list_len_gt_1(self):
        self.conn.run("in_memory_table=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)")
        in_memory_table = self.conn.loadTable(tableName='in_memory_table')
        in_memory_table.drop(['c1', 'c2'])
        expect = pd.DataFrame({'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(in_memory_table.toDF(), expect)

    def test_loadTable_olap_table_drop_str(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        self.conn.run(f"""
            if (existsDatabase('{dbPath}')){{
                dropDatabase('{dbPath}')
            }}
            db=database('{dbPath}',VALUE,`1`2`3,engine=`OLAP)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`{tbName1},`c2).append!(t)
        """)
        olap_table = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        olap_table.drop('c1')
        expect = pd.DataFrame({'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(olap_table.toDF(), expect)

    def test_loadTable_olap_table_drop_list_len_eq_0(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        self.conn.run(f"""
            if (existsDatabase('{dbPath}')){{
                dropDatabase('{dbPath}')
            }}
            db=database('{dbPath}',VALUE,`1`2`3,engine=`OLAP)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`{tbName1},`c2).append!(t)
        """)
        olap_table = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        olap_table.drop([])
        expect = pd.DataFrame({'c1': pd.Series([1, 2, 3], dtype='int32'), 'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']})
        assert_frame_equal(olap_table.toDF(), expect)

    def test_loadTable_olap_table_drop_list_len_eq_1(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        self.conn.run(f"""
            if (existsDatabase('{dbPath}')){{
                dropDatabase('{dbPath}')
            }}
            db=database('{dbPath}',VALUE,`1`2`3,engine=`OLAP)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`{tbName1},`c2).append!(t)
        """)
        olap_table = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        olap_table.drop(['c1'])
        expect = pd.DataFrame({'c2': ['1', '2', '3'], 'c3': ['a', 'b', 'c']}, dtype='object')
        assert_frame_equal(olap_table.toDF(), expect)

    def test_loadTable_olap_table_drop_list_len_gt_1(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        self.conn.run(f"""
            if (existsDatabase('{dbPath}')){{
                dropDatabase('{dbPath}')
            }}
            db=database('{dbPath}',VALUE,`1`2`3,engine=`OLAP)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`{tbName1},`c2).append!(t)
        """)
        olap_table = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        with pytest.raises(RuntimeError):
            olap_table.drop(['c1', 'c2'])

    def test_loadTable_tsdb_table_drop_list_len_eq_1(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        self.conn.run(f"""
            if (existsDatabase('{dbPath}')){{
                dropDatabase('{dbPath}')
            }}
            db=database('{dbPath}',VALUE,`1`2`3,engine=`TSDB)
            t=table(1 2 3 as c1,`1`2`3 as c2,`a`b`c as c3)
            tb=db.createPartitionedTable(t,`{tbName1},`c2,sortColumns=`c3).append!(t)
        """)
        tsdb_table = self.conn.loadTable(tableName=tbName1, dbPath=dbPath)
        with pytest.raises(RuntimeError):
            tsdb_table.drop(['c1'])


class TestLoadTableBySQL:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    def test_loadTableBySQL_dfs_dimension(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,1..10)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createTable(tdata,`{tbName1}).append!(tdata)
        '''
        self.conn.run(ddb_script)
        with pytest.raises(RuntimeError):
            self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                     sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")

    def test_loadTableBySQL_dfs_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
            n=100
            tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_hash_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_dfs_compo_range_value_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, 1..n as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
            n=100
            tdata=table(sort(take(2010.01.01..2010.01.30, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_hash_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())

    def test_loadTableBySQL_disk_compo_range_value_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        tbName1 = func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
            n=100
            tdata=table(sort(take(2010.01.01..2010.12.31, n)) as date, take(1..10,n) as id,take(`AMD`QWE`CES`DOP`ASZ`FSD`BBVC`AWQ`DS, n) as sym,rand(100,n) as val)
            db.createPartitionedTable(tdata,`{tbName1},`date`id`sym).append!(tdata)
        '''
        self.conn.run(ddb_script)
        rs = self.conn.run(
            f"select * from loadTable('{dbPath}','{tbName1}') where date in [2010.01.05,2010.01.15,2010.01.19]")
        tmp = self.conn.loadTableBySQL(tableName=tbName1, dbPath=dbPath,
                                       sql=f"select * from {tbName1} where date in [2010.01.05,2010.01.15,2010.01.19]")
        assert_frame_equal(rs, tmp.toDF())
