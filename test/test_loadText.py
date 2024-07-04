import pytest
import dolphindb as ddb
from pandas.testing import assert_frame_equal
from setup.settings import *
from setup.utils import get_pid


class TestLoadText:
    conn = ddb.session()

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

    def test_loadText_param_fileName(self):
        data = DATA_DIR + "/trades.csv"
        tb = self.conn.loadText(data)
        rs = self.conn.run("select * from loadText('{data}')".format(data=data))
        assert_frame_equal(tb.toDF(), rs)

    def test_loadText_param_delimiter(self):
        data = DATA_DIR + "/trades.csv"
        tb = self.conn.loadText(data, ";")
        rs = self.conn.run("select * from loadText('{data}', ';')".format(data=data))
        assert_frame_equal(tb.toDF(), rs)

    def test_loadText_param_delimiter2(self):
        data = DATA_DIR + "/trades.csv"
        self.conn.loadText(remoteFilePath=data, delimiter=";")


class DBInfo:
    dfsDBName = 'dfs://testLoadTextEx'
    diskDBName = WORK_DIR + '/testLoadTextEx'
    table1 = 'tb1'
    table2 = 'tb2'


def create_dfs_range_db(s: ddb.session):
    ddb_script = f'''
        login('{USER}','{PASSWD}')
        dbPath='{DBInfo.dfsDBName}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,RANGE,0..10*10000+1)
    '''
    s.run(ddb_script)


def create_dfs_hash_db(s: ddb.session):
    ddb_script = '''
        login('admin','123456')
        dbPath='{db}'
        if(existsDatabase(dbPath))
            dropDatabase(dbPath)
        db=database(dbPath,HASH,[INT,10])
    '''.format(db=DBInfo.dfsDBName)
    s.run(ddb_script)


def create_dfs_value_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,VALUE,2010.01.01..2010.01.30)
    '''.format(db=DBInfo.dfsDBName)
    s.run(ddb_script)


def create_dfs_list_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    '''.format(db=DBInfo.dfsDBName)
    s.run(ddb_script)


def create_dfs_compo_range_range_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',RANGE,1 3 5 7 9 11)
    db=database(dbPath,COMPO,[db1,db2])
    '''.format(db=DBInfo.dfsDBName)
    s.run(ddb_script)


def create_dfs_compo_range_hash_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',HASH,[INT,10])
    db=database(dbPath,COMPO,[db1,db2])
    '''.format(db=DBInfo.dfsDBName)
    s.run(ddb_script)


def create_dfs_compo_range_value_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01.01..2010.01.30)
    db2=database('',VALUE,1..10)
    db=database(dbPath,COMPO,[db1,db2])
    '''.format(db=DBInfo.dfsDBName)
    s.run(ddb_script)


def create_dfs_compo_range_list_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2])
    '''.format(db=DBInfo.dfsDBName)
    s.run(ddb_script)


def create_dfs_compo_range_hash_list_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01.01..2010.01.30)
    db2=database('',HASH,[INT,10])
    db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2,db3])
    '''.format(db=DBInfo.dfsDBName)
    s.run(ddb_script)


def create_dfs_compo_range_value_list_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(existsDatabase(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01.01..2010.01.30)
    db2=database('',VALUE,1..10)
    db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2,db3])
    '''.format(db=DBInfo.dfsDBName)
    s.run(ddb_script)


def create_disk_range_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,RANGE,0..10*10000+1)
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


def create_disk_hash_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,HASH,[INT,10])
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


def create_disk_value_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,VALUE,2010.01.01..2010.01.30)
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


def create_disk_list_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db=database(dbPath,LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


def create_disk_compo_range_range_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01.01..2010.01.30)
    db2=database('',RANGE,1 3 5 7 9 11)
    db=database(dbPath,COMPO,[db1,db2])
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


def create_disk_compo_range_hash_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01.01..2010.01.30)
    db2=database('',HASH,[INT,10])
    db=database(dbPath,COMPO,[db1,db2])
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


def create_disk_compo_range_value_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01.01..2010.01.30)
    db2=database('',VALUE,1..10)
    db=database(dbPath,COMPO,[db1,db2])
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


def create_disk_compo_range_list_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2])
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


def create_disk_compo_range_hash_list_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01M+0..12)
    db2=database('',HASH,[INT,10])
    db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2,db3])
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


def create_disk_compo_range_value_list_db(s: ddb.session):
    ddb_script = '''
    login('admin','123456')
    dbPath='{db}'
    if(exists(dbPath))
        dropDatabase(dbPath)
    db1=database('',RANGE,2010.01.01..2010.01.30)
    db2=database('',VALUE,1..10)
    db3=database('',LIST,[`AMD`QWE`CES,`DOP`ASZ,`FSD`BBVC,`AWQ`DS])
    db=database(dbPath,COMPO,[db1,db2,db3])
    '''.format(db=DBInfo.diskDBName)
    s.run(ddb_script)


class TestLoadTextEx:
    conn = ddb.session()
    data1 = DATA_DIR + "/loadTextExTest1.csv"
    data2 = DATA_DIR + "/loadTextExTest2.csv"

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

    def test_loadTextEx_dfs_range(self):
        create_dfs_range_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["id"], self.data1)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `id, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_hash(self):
        create_dfs_hash_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["id"], self.data1)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `id, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_value(self):
        create_dfs_value_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["date"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_list(self):
        create_dfs_list_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["sym"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `sym, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_range(self):
        create_dfs_compo_range_range_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["date", "id"], self.data1)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_hash(self):
        create_dfs_compo_range_hash_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["date", "id"], self.data1)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_value(self):
        create_dfs_compo_range_value_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["date", "id"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_list(self):
        create_dfs_compo_range_list_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["date", "sym"], self.data1)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`sym, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_hash_list(self):
        create_dfs_compo_range_hash_list_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["date", "id", "sym"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id`sym, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_dfs_compo_range_value_list(self):
        create_dfs_compo_range_value_list_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.dfsDBName, DBInfo.table1, ["date", "id", "sym"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id`sym, '{data}')"
                           .format(db=DBInfo.dfsDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_range(self):
        create_disk_range_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["id"], self.data1)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `id, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_hash(self):
        create_disk_hash_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["id"], self.data1)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `id, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_value(self):
        create_disk_value_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["date"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_list(self):
        create_disk_list_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["sym"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `sym, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_range(self):
        create_disk_compo_range_range_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["date", "id"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_hash(self):
        create_disk_compo_range_hash_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["date", "id"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_value(self):
        create_disk_compo_range_value_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["date", "id"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_list(self):
        create_disk_compo_range_list_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["date", "sym"], self.data1)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`sym, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_hash_list(self):
        create_disk_compo_range_hash_list_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["date", "id", "sym"], self.data1)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id`sym, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data1))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_disk_compo_range_value_list(self):
        create_disk_compo_range_value_list_db(self.conn)
        tmp = self.conn.loadTextEx(DBInfo.diskDBName, DBInfo.table1, ["date", "id", "sym"], self.data2)
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, `date`id`sym, '{data}')"
                           .format(db=DBInfo.diskDBName, tb=DBInfo.table2, data=self.data2))
        assert_frame_equal(tmp.toDF(), rs)

    def test_loadTextEx_sortColumns(self):
        self.conn.run(f"""
            dbPath="{DBInfo.dfsDBName}"
            if(exists(dbPath))
                dropDatabase(dbPath)
            db=database(dbPath,VALUE,2010.01.01..2010.01.30,engine=`TSDB)
        """)
        tmp1 = self.conn.loadTextEx(DBInfo.dfsDBName, 'tb1', partitionColumns=['date'], remoteFilePath=self.data2,
                                    sortColumns=['date'])
        tmp2 = self.conn.loadTextEx(DBInfo.dfsDBName, 'tb1', partitionColumns=['date'], remoteFilePath=self.data2,
                                    sortColumns=['date', 'id'])
        rs = self.conn.run("select * from loadTextEx(database('{db}'), `{tb}, 'date', '{data}')"
                           .format(db=DBInfo.dfsDBName, tb='tb1', data=self.data2))
        assert_frame_equal(tmp1.toDF(), rs)
        assert_frame_equal(tmp2.toDF(), rs)


if __name__ == '__main__':
    pytest.main(['-s', '-v', 'test_loadText.py'])
