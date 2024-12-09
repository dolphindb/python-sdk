import inspect

import dolphindb as ddb
from pandas.testing import assert_frame_equal

from setup.settings import HOST, PORT, USER, PASSWD, DATA_DIR, REMOTE_WORK_DIR


class TestLoadText:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    def test_loadText_param_fileName(self):
        data = DATA_DIR + "trades.csv"
        tb = self.conn.loadText(data)
        rs = self.conn.run(f"select * from loadText('{data}')")
        assert_frame_equal(tb.toDF(), rs)

    def test_loadText_param_delimiter(self):
        data = DATA_DIR + "trades.csv"
        tb = self.conn.loadText(data, ";")
        rs = self.conn.run(f"select * from loadText('{data}', ';')")
        assert_frame_equal(tb.toDF(), rs)

    def test_loadText_param_delimiter2(self):
        data = DATA_DIR + "trades.csv"
        self.conn.loadText(remoteFilePath=data, delimiter=";")


class TestLoadTextEx:
    conn = ddb.session(HOST, PORT, USER, PASSWD)
    data1 = DATA_DIR + "loadTextExTest1.csv"
    data2 = DATA_DIR + "loadTextExTest2.csv"

    def test_loadTextEx_dfs_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["id"], self.data1)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `id, '{self.data1}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["id"], self.data1)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `id, '{self.data1}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["sym"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `sym, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id"], self.data1)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id, '{self.data1}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id"], self.data1)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id, '{self.data1}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01.01..2010.01.30)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "sym"], self.data1)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`sym, '{self.data1}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_hash_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01.01..2010.01.30)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id", "sym"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id`sym, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_value_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(existsDatabase(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01.01..2010.01.30)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id", "sym"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id`sym, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,RANGE,0..10*10000+1)
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["id"], self.data1)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `id, '{self.data1}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,HASH,[INT,10])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["id"], self.data1)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `id, '{self.data1}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30)
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["sym"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `sym, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_range(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01.01..2010.01.30)
            db2=database('',RANGE,1 3 5 7 9 11)
            db=database(dbPath,COMPO,[db1,db2])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_hash(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01.01..2010.01.30)
            db2=database('',HASH,[INT,10])
            db=database(dbPath,COMPO,[db1,db2])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_value(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01.01..2010.01.30)
            db2=database('',VALUE,1..10)
            db=database(dbPath,COMPO,[db1,db2])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "sym"], self.data1)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`sym, '{self.data1}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_hash_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01M+0..12)
            db2=database('',HASH,[INT,10])
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id", "sym"], self.data1)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id`sym, '{self.data1}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_value_list(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = REMOTE_WORK_DIR + func_name
        ddb_script = f'''
            login('{USER}','{PASSWD}')
            dbPath='{dbPath}'
            if(exists(dbPath))
                dropDatabase(dbPath)
            db1=database('',RANGE,2010.01.01..2010.01.30)
            db2=database('',VALUE,1..10)
            db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
            db=database(dbPath,COMPO,[db1,db2,db3])
        '''
        self.conn.run(ddb_script)
        tmp = self.conn.loadTextEx(dbPath, 'tb1', ["date", "id", "sym"], self.data2)
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb2, `date`id`sym, '{self.data2}')")
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_sortColumns(self):
        func_name = inspect.currentframe().f_code.co_name
        dbPath = f"dfs://{func_name}"
        self.conn.run(f"""
            dbPath="{dbPath}"
            if(exists(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30,engine=`TSDB)
        """)
        tmp1 = self.conn.loadTextEx(dbPath, 'tb1', partitionColumns=['date'], remoteFilePath=self.data2,
                                    sortColumns=['date'])
        tmp2 = self.conn.loadTextEx(dbPath, 'tb1', partitionColumns=['date'], remoteFilePath=self.data2,
                                    sortColumns=['date', 'id'])
        rs = self.conn.run(f"select * from loadTextEx(database('{dbPath}'), `tb1, 'date', '{self.data2}')")
        assert_frame_equal(tmp1.toDF(), rs)
        assert_frame_equal(tmp2.toDF(), rs)
