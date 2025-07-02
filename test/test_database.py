import inspect

import dolphindb as ddb
import dolphindb.settings as keys
import numpy as np
import pandas as pd
from numpy.testing import assert_array_equal
from pandas._testing import assert_frame_equal

from setup.settings import HOST, PORT, USER, PASSWD, REMOTE_WORK_DIR


class TestDatabase:
    conn = ddb.session(HOST, PORT, USER, PASSWD)

    # TODO: error to create a SEQ database
    # TODO: error to run function dropPartition()
    def test_database_workflow_OLAP(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
            if(existsDatabase("dfs://{func_name}_db_range")){{dropDatabase("dfs://{func_name}_db_range")}};
            if(existsDatabase("dfs://{func_name}_db_combo")){{dropDatabase("dfs://{func_name}_db_combo")}};
            if(existsDatabase("dfs://{func_name}_db_hash")){{dropDatabase("dfs://{func_name}_db_hash")}};
            if(existsDatabase("dfs://{func_name}_db_list")){{dropDatabase("dfs://{func_name}_db_list")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-03', freq="D"), dtype="datetime64[D]")
        ids = [1, 2, 3]
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value",
                              engine="OLAP")
        db_r = conn1.database(dbName="db_range", partitionType=keys.RANGE, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_range",
                              engine="OLAP")
        db_c = conn1.database(dbName="db_combo", partitionType=keys.COMPO, partitions=[db_v, db_r],
                              dbPath=f"dfs://{func_name}_db_combo", engine="OLAP")
        db_h = conn1.database(dbName="db_hash", partitionType=keys.HASH, partitions=[keys.DT_INT, 3],
                              dbPath=f"dfs://{func_name}_db_hash", engine="OLAP")
        # db_s = conn1.database(dbName="db_seq",partitionType=keys.SEQ,partitions=[1], engine="OLAP")
        db_l = conn1.database(dbName="db_list", partitionType=keys.LIST, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_list",
                              engine="OLAP")
        assert db_v._getDbName() == "db_value"
        assert db_r._getDbName() == "db_range"
        assert db_c._getDbName() == "db_combo"
        assert db_h._getDbName() == "db_hash"
        assert db_l._getDbName() == "db_list"
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_value")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_range")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_combo")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_hash")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_list")')
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE])"))
        db_v.createPartitionedTable(table=t, tableName="db_v_tab", partitionColumns="col3")
        db_r.createPartitionedTable(table=t, tableName="db_r_tab", partitionColumns="col2")
        db_c.createPartitionedTable(table=t, tableName="db_c_tab", partitionColumns=["col3", "col2"])
        db_h.createPartitionedTable(table=t, tableName="db_h_tab", partitionColumns="col2")
        db_l.createPartitionedTable(table=t, tableName="db_l_tab", partitionColumns="col2")
        assert conn1.existsTable(f"dfs://{func_name}_db_value", "db_v_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_range", "db_r_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_list", "db_l_tab")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_value","db_v_tab"))[`engineType]') == "OLAP")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_range","db_r_tab"))[`engineType]') == "OLAP")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_combo","db_c_tab"))[`engineType]') == "OLAP")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_hash","db_h_tab"))[`engineType]') == "OLAP")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_list","db_l_tab"))[`engineType]') == "OLAP")
        conn1.dropTable(f"dfs://{func_name}_db_value", "db_v_tab")
        conn1.dropTable(f"dfs://{func_name}_db_range", "db_r_tab")
        conn1.dropTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        conn1.dropTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        conn1.dropTable(f"dfs://{func_name}_db_list", "db_l_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_value", "db_v_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_range", "db_r_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_list", "db_l_tab")
        conn1.dropDatabase(f"dfs://{func_name}_db_value")
        conn1.dropDatabase(f"dfs://{func_name}_db_range")
        conn1.dropDatabase(f"dfs://{func_name}_db_combo")
        conn1.dropDatabase(f"dfs://{func_name}_db_hash")
        conn1.dropDatabase(f"dfs://{func_name}_db_list")
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_value")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_range")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_combo")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_hash")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_list")')
        conn1.close()

    # TODO: error to create a SEQ database
    def test_database_workflow_TSDB(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://db_value")}};
            if(existsDatabase("dfs://{func_name}_db_range")){{dropDatabase("dfs://db_range")}};
            if(existsDatabase("dfs://{func_name}_db_combo")){{dropDatabase("dfs://db_combo")}};
            if(existsDatabase("dfs://{func_name}_db_hash")){{dropDatabase("dfs://db_hash")}};
            if(existsDatabase("dfs://{func_name}_db_list")){{dropDatabase("dfs://db_list")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-03', freq="D"), dtype="datetime64[D]")
        ids = [1, 2, 3]
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value",
                              engine="TSDB")
        db_r = conn1.database(dbName="db_range", partitionType=keys.RANGE, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_range",
                              engine="TSDB")
        db_c = conn1.database(dbName="db_combo", partitionType=keys.COMPO, partitions=[db_v, db_r],
                              dbPath=f"dfs://{func_name}_db_combo", engine="TSDB")
        db_h = conn1.database(dbName="db_hash", partitionType=keys.HASH, partitions=[keys.DT_INT, 3],
                              dbPath=f"dfs://{func_name}_db_hash", engine="TSDB")
        # db_s = conn1.database(dbName="db_seq",partitionType=keys.SEQ,partitions=[1],engine="TSDB")
        db_l = conn1.database(dbName="db_list", partitionType=keys.LIST, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_list",
                              engine="TSDB")
        assert db_v._getDbName() == "db_value"
        assert db_r._getDbName() == "db_range"
        assert db_c._getDbName() == "db_combo"
        assert db_h._getDbName() == "db_hash"
        assert db_l._getDbName() == "db_list"
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_value")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_range")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_combo")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_hash")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_list")')
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE])"))
        # db_s_tab = db_s.createTable(table=t, tableName="db_s_tab",sortColumns="col2")
        # print(db_s_tab)
        db_v.createPartitionedTable(table=t, tableName="db_v_tab", partitionColumns="col3", sortColumns="col2")
        db_r.createPartitionedTable(table=t, tableName="db_r_tab", partitionColumns="col2", sortColumns="col2")
        db_c.createPartitionedTable(table=t, tableName="db_c_tab", partitionColumns=["col3", "col2"],
                                    sortColumns="col2")
        db_h.createPartitionedTable(table=t, tableName="db_h_tab", partitionColumns="col2", sortColumns="col2")
        db_l.createPartitionedTable(table=t, tableName="db_l_tab", partitionColumns="col2", sortColumns="col2")
        assert conn1.existsTable(f"dfs://{func_name}_db_value", "db_v_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_range", "db_r_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_list", "db_l_tab")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_value","db_v_tab"))[`engineType]') == "TSDB")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_range","db_r_tab"))[`engineType]') == "TSDB")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_combo","db_c_tab"))[`engineType]') == "TSDB")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_hash","db_h_tab"))[`engineType]') == "TSDB")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_list","db_l_tab"))[`engineType]') == "TSDB")
        conn1.dropTable(f"dfs://{func_name}_db_value", "db_v_tab")
        conn1.dropTable(f"dfs://{func_name}_db_range", "db_r_tab")
        conn1.dropTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        conn1.dropTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        conn1.dropTable(f"dfs://{func_name}_db_list", "db_l_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_value", "db_v_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_range", "db_r_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_list", "db_l_tab")
        conn1.dropDatabase(f"dfs://{func_name}_db_value")
        conn1.dropDatabase(f"dfs://{func_name}_db_range")
        conn1.dropDatabase(f"dfs://{func_name}_db_combo")
        conn1.dropDatabase(f"dfs://{func_name}_db_hash")
        conn1.dropDatabase(f"dfs://{func_name}_db_list")
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_value")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_range")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_combo")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_hash")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_list")')
        conn1.close()

    def test_database_workflow_PKEY(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
            if(existsDatabase("dfs://{func_name}_db_range")){{dropDatabase("dfs://{func_name}_db_range")}};
            if(existsDatabase("dfs://{func_name}_db_combo")){{dropDatabase("dfs://{func_name}_db_combo")}};
            if(existsDatabase("dfs://{func_name}_db_hash")){{dropDatabase("dfs://{func_name}_db_hash")}};
            if(existsDatabase("dfs://{func_name}_db_list")){{dropDatabase("dfs://{func_name}_db_list")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-03', freq="D"), dtype="datetime64[D]")
        ids = [1, 2, 3]
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value",
                              engine="PKEY")
        db_r = conn1.database(dbName="db_range", partitionType=keys.RANGE, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_range",
                              engine="PKEY")
        db_c = conn1.database(dbName="db_combo", partitionType=keys.COMPO, partitions=[db_v, db_r],
                              dbPath=f"dfs://{func_name}_db_combo", engine="PKEY")
        db_h = conn1.database(dbName="db_hash", partitionType=keys.HASH, partitions=[keys.DT_INT, 3],
                              dbPath=f"dfs://{func_name}_db_hash", engine="PKEY")
        # db_s = conn1.database(dbName="db_seq",partitionType=keys.SEQ,partitions=[1],engine="TSDB")
        db_l = conn1.database(dbName="db_list", partitionType=keys.LIST, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_list",
                              engine="PKEY")
        assert db_v._getDbName() == "db_value"
        assert db_r._getDbName() == "db_range"
        assert db_c._getDbName() == "db_combo"
        assert db_h._getDbName() == "db_hash"
        assert db_l._getDbName() == "db_list"
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_value")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_range")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_combo")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_hash")')
        assert conn1.run(f'existsDatabase("dfs://{func_name}_db_list")')
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [LONG,INT,DATE])"))
        # db_s_tab = db_s.createTable(table=t, tableName="db_s_tab",sortColumns="col2")
        # print(db_s_tab)
        db_v.createPartitionedTable(table=t, tableName="db_v_tab", partitionColumns="col3", primaryKey="col3",
                                    indexes={"col1": "bloomfilter"})
        db_r.createPartitionedTable(table=t, tableName="db_r_tab", partitionColumns="col2", primaryKey=["col2"],
                                    indexes={"col1": "bloomfilter"})
        db_c.createPartitionedTable(table=t, tableName="db_c_tab", partitionColumns=["col3", "col2"],
                                    primaryKey=["col2", "col3"], indexes={"col1": "bloomfilter"})
        db_h.createPartitionedTable(table=t, tableName="db_h_tab", partitionColumns="col2", primaryKey="col2",
                                    indexes={"col1": "bloomfilter"})
        db_l.createPartitionedTable(table=t, tableName="db_l_tab", partitionColumns="col2", primaryKey="col2",
                                    indexes={"col1": "bloomfilter"})
        assert conn1.existsTable(f"dfs://{func_name}_db_value", "db_v_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_range", "db_r_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_list", "db_l_tab")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_value","db_v_tab"))[`engineType]') == "PKEY")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_range","db_r_tab"))[`engineType]') == "PKEY")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_combo","db_c_tab"))[`engineType]') == "PKEY")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_hash","db_h_tab"))[`engineType]') == "PKEY")
        assert (conn1.run(f'schema(loadTable("dfs://{func_name}_db_list","db_l_tab"))[`engineType]') == "PKEY")
        expect_v = pd.DataFrame({
            "name": ["col1_zonemap", "col1_bloomfilter", "col2_zonemap", "_primary_key_bloomfilter"],
            "type": ["zonemap", "bloomfilter", "zonemap", "bloomfilter"],
            "columnName": ["col1", "col1", "col2", "col3"],
        })
        expect = pd.DataFrame({
            "name": ["col1_zonemap", "col1_bloomfilter", "_primary_key_bloomfilter", "col2_zonemap", "col3_zonemap"],
            "type": ["zonemap", "bloomfilter", "bloomfilter", "zonemap", "zonemap"],
            "columnName": ["col1", "col1", "col2", "col2", "col3"],
        })
        expect_c = pd.DataFrame({
            "name": ["_primary_key_bloomfilter", "col1_zonemap", "col1_bloomfilter", "col2_zonemap"],
            "type": ["bloomfilter", "zonemap", "bloomfilter", "zonemap"],
            "columnName": ["_composite_primary_key", "col1", "col1", "col2"],
        })
        assert_frame_equal(conn1.run(f'schema(loadTable("dfs://{func_name}_db_value","db_v_tab"))[`indexes]'), expect_v)
        assert_frame_equal(conn1.run(f'schema(loadTable("dfs://{func_name}_db_range","db_r_tab"))[`indexes]'), expect)
        assert_frame_equal(conn1.run(f'schema(loadTable("dfs://{func_name}_db_combo","db_c_tab"))[`indexes]'), expect_c)
        assert_frame_equal(conn1.run(f'schema(loadTable("dfs://{func_name}_db_hash","db_h_tab"))[`indexes]'), expect)
        assert_frame_equal(conn1.run(f'schema(loadTable("dfs://{func_name}_db_list","db_l_tab"))[`indexes]'), expect)
        conn1.dropTable(f"dfs://{func_name}_db_value", "db_v_tab")
        conn1.dropTable(f"dfs://{func_name}_db_range", "db_r_tab")
        conn1.dropTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        conn1.dropTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        conn1.dropTable(f"dfs://{func_name}_db_list", "db_l_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_value", "db_v_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_range", "db_r_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        assert not conn1.existsTable(f"dfs://{func_name}_db_list", "db_l_tab")
        conn1.dropDatabase(f"dfs://{func_name}_db_value")
        conn1.dropDatabase(f"dfs://{func_name}_db_range")
        conn1.dropDatabase(f"dfs://{func_name}_db_combo")
        conn1.dropDatabase(f"dfs://{func_name}_db_hash")
        conn1.dropDatabase(f"dfs://{func_name}_db_list")
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_value")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_range")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_combo")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_hash")')
        assert not conn1.run(f'existsDatabase("dfs://{func_name}_db_list")')
        conn1.close()

    def test_database_createPartitionedTable_compressMethods(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
            if(existsDatabase("dfs://{func_name}_db_range")){{dropDatabase("dfs://{func_name}_db_range")}};
            if(existsDatabase("dfs://{func_name}_db_combo")){{dropDatabase("dfs://{func_name}_db_combo")}};
            if(existsDatabase("dfs://{func_name}_db_hash")){{dropDatabase("dfs://{func_name}_db_hash")}};
            if(existsDatabase("dfs://{func_name}_db_list")){{dropDatabase("dfs://{func_name}_db_list")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-03', freq="D"), dtype="datetime64[D]")
        ids = [1, 2, 3]
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value")
        db_r = conn1.database(dbName="db_range", partitionType=keys.RANGE, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_range")
        db_c = conn1.database(dbName="db_combo", partitionType=keys.COMPO, partitions=[db_v, db_r],
                              dbPath=f"dfs://{func_name}_db_combo")
        db_h = conn1.database(dbName="db_hash", partitionType=keys.HASH, partitions=[keys.DT_INT, 3],
                              dbPath=f"dfs://{func_name}_db_hash")
        # db_s = conn1.database(dbName="db_seq",partitionType=keys.SEQ,partitions=[1])
        db_l = conn1.database(dbName="db_list", partitionType=keys.LIST, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_list")
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE])"))
        # db_s_tab = db_s.createTable(table=t, tableName="db_s_tab",sortColumns="col2")
        db_v.createPartitionedTable(table=t, tableName="db_v_tab", partitionColumns="col3",
                                    compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_r.createPartitionedTable(table=t, tableName="db_r_tab", partitionColumns="col2",
                                    compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_c.createPartitionedTable(table=t, tableName="db_c_tab", partitionColumns=["col3", "col2"],
                                    compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_h.createPartitionedTable(table=t, tableName="db_h_tab", partitionColumns="col2",
                                    compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_l.createPartitionedTable(table=t, tableName="db_l_tab", partitionColumns="col2",
                                    compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        assert conn1.existsTable(f"dfs://{func_name}_db_value", "db_v_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_range", "db_r_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_list", "db_l_tab")
        conn1.close()

    def test_database_createTable_compressMethods(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
            if(existsDatabase("dfs://{func_name}_db_range")){{dropDatabase("dfs://{func_name}_db_range")}};
            if(existsDatabase("dfs://{func_name}_db_combo")){{dropDatabase("dfs://{func_name}_db_combo")}};
            if(existsDatabase("dfs://{func_name}_db_hash")){{dropDatabase("dfs://{func_name}_db_hash")}};
            if(existsDatabase("dfs://{func_name}_db_list")){{dropDatabase("dfs://{func_name}_db_list")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-03', freq="D"), dtype="datetime64[D]")
        ids = [1, 2, 3]
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value")
        db_r = conn1.database(dbName="db_range", partitionType=keys.RANGE, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_range")
        db_c = conn1.database(dbName="db_combo", partitionType=keys.COMPO, partitions=[db_v, db_r],
                              dbPath=f"dfs://{func_name}_db_combo")
        db_h = conn1.database(dbName="db_hash", partitionType=keys.HASH, partitions=[keys.DT_INT, 3],
                              dbPath=f"dfs://{func_name}_db_hash")
        # db_s = conn1.database(dbName="db_seq",partitionType=keys.SEQ,partitions=[1])
        db_l = conn1.database(dbName="db_list", partitionType=keys.LIST, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_list")
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE])"))
        # db_s_tab = db_s.createTable(table=t, tableName="db_s_tab",sortColumns="col2")
        db_v.createTable(table=t, tableName="db_v_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_r.createTable(table=t, tableName="db_r_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_c.createTable(table=t, tableName="db_c_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_h.createTable(table=t, tableName="db_h_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_l.createTable(table=t, tableName="db_l_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        assert conn1.existsTable(f"dfs://{func_name}_db_value", "db_v_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_range", "db_r_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_list", "db_l_tab")
        conn1.close()

    def test_database_createDimensionTable_compressMethods(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
            if(existsDatabase("dfs://{func_name}_db_range")){{dropDatabase("dfs://{func_name}_db_range")}};
            if(existsDatabase("dfs://{func_name}_db_combo")){{dropDatabase("dfs://{func_name}_db_combo")}};
            if(existsDatabase("dfs://{func_name}_db_hash")){{dropDatabase("dfs://{func_name}_db_hash")}};
            if(existsDatabase("dfs://{func_name}_db_list")){{dropDatabase("dfs://{func_name}_db_list")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-03', freq="D"), dtype="datetime64[D]")
        ids = [1, 2, 3]
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value")
        db_r = conn1.database(dbName="db_range", partitionType=keys.RANGE, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_range")
        db_c = conn1.database(dbName="db_combo", partitionType=keys.COMPO, partitions=[db_v, db_r],
                              dbPath=f"dfs://{func_name}_db_combo")
        db_h = conn1.database(dbName="db_hash", partitionType=keys.HASH, partitions=[keys.DT_INT, 3],
                              dbPath=f"dfs://{func_name}_db_hash")
        # db_s = conn1.database(dbName="db_seq",partitionType=keys.SEQ,partitions=[1])
        db_l = conn1.database(dbName="db_list", partitionType=keys.LIST, partitions=ids,
                              dbPath=f"dfs://{func_name}_db_list")
        t = conn1.table(data=conn1.run("table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE])"))
        # db_s_tab = db_s.createDimensionTable(table=t, tableName="db_s_tab",sortColumns="col2")
        db_v.createDimensionTable(table=t, tableName="db_v_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_r.createDimensionTable(table=t, tableName="db_r_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_c.createDimensionTable(table=t, tableName="db_c_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_h.createDimensionTable(table=t, tableName="db_h_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        db_l.createDimensionTable(table=t, tableName="db_l_tab",
                         compressMethods={"col1": "lz4", "col2": "delta", "col3": "delta"})
        assert conn1.existsTable(f"dfs://{func_name}_db_value", "db_v_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_range", "db_r_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_combo", "db_c_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_hash", "db_h_tab")
        assert conn1.existsTable(f"dfs://{func_name}_db_list", "db_l_tab")
        conn1.close()

    def test_database_createPartitionedTable_keepDuplicates(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-05', freq="D"), dtype="datetime64[D]")
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value",
                              engine="TSDB")
        conn1.run(
            "t0=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);tableInsert(t0,`a`b`c`d, 1 2 2 4, [2000.01.01,2000.01.02,2000.01.02,2000.01.04])")
        t = conn1.table(data='t0')
        # db_s_tab = db_s.createTable(table=t, tableName="db_s_tab",sortColumns="col2")
        db_v.createPartitionedTable(table=t, tableName="db_v_tab", partitionColumns="col3",
                                    sortColumns=["col3", "col2"], keepDuplicates="LAST")
        db_v.createPartitionedTable(table=t, tableName="db_v_tab2", partitionColumns="col3",
                                    sortColumns=["col3", "col2"], keepDuplicates="FIRST")
        db_v.createPartitionedTable(table=t, tableName="db_v_tab3", partitionColumns="col3",
                                    sortColumns=["col3", "col2"], keepDuplicates="ALL")
        conn1.run(f"""
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab"), t0);
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab2"), t0);
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab3"), t0);
        """)
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab")""")["col1"].values,
            ['a', 'c', 'd'])
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab2")""")["col1"].values,
            ['a', 'b', 'd'])
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab3")""")["col1"].values,
            ['a', 'b', 'c', 'd'])
        conn1.close()

    def test_database_createTable_keepDuplicates(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-05', freq="D"), dtype="datetime64[D]")
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value",
                              engine="TSDB")
        conn1.run(
            "t0=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);tableInsert(t0,`a`b`c`d, 1 2 2 4, [2000.01.01,2000.01.02,2000.01.02,2000.01.04])")
        t = conn1.table(data='t0')
        # db_s_tab = db_s.createTable(table=t, tableName="db_s_tab",sortColumns="col2")
        db_v.createTable(table=t, tableName="db_v_tab", sortColumns=["col3", "col2"], keepDuplicates="LAST")
        db_v.createTable(table=t, tableName="db_v_tab2", sortColumns=["col3", "col2"], keepDuplicates="FIRST")
        db_v.createTable(table=t, tableName="db_v_tab3", sortColumns=["col3", "col2"], keepDuplicates="ALL")
        conn1.run(f"""
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab"), t0);
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab2"), t0);
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab3"), t0);
        """)
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab")""")["col1"].values,
            ['a', 'c', 'd'])
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab2")""")["col1"].values,
            ['a', 'b', 'd'])
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab3")""")["col1"].values,
            ['a', 'b', 'c', 'd'])
        conn1.close()

    def test_database_createDimensionTable_keepDuplicates(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-05', freq="D"), dtype="datetime64[D]")
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value",
                              engine="TSDB")
        conn1.run(
            "t0=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);tableInsert(t0,`a`b`c`d, 1 2 2 4, [2000.01.01,2000.01.02,2000.01.02,2000.01.04])")
        t = conn1.table(data='t0')
        # db_s_tab = db_s.createDimensionTable(table=t, tableName="db_s_tab",sortColumns="col2")
        db_v.createDimensionTable(table=t, tableName="db_v_tab", sortColumns=["col3", "col2"], keepDuplicates="LAST")
        db_v.createDimensionTable(table=t, tableName="db_v_tab2", sortColumns=["col3", "col2"], keepDuplicates="FIRST")
        db_v.createDimensionTable(table=t, tableName="db_v_tab3", sortColumns=["col3", "col2"], keepDuplicates="ALL")
        conn1.run(f"""
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab"), t0);
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab2"), t0);
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab3"), t0);
        """)
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab")""")["col1"].values,
            ['a', 'c', 'd'])
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab2")""")["col1"].values,
            ['a', 'b', 'd'])
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab3")""")["col1"].values,
            ['a', 'b', 'c', 'd'])
        conn1.close()

    def test_database_createPartitionedTable_softDelete(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-05', freq="D"), dtype="datetime64[D]")
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value",
                              engine="TSDB")
        conn1.run(
            "t0=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);tableInsert(t0,`a`b`c`d, 1 2 2 4, [2000.01.01,2000.01.02,2000.01.02,2000.01.04])")
        t = conn1.table(data='t0')
        db_v.createPartitionedTable(table=t, tableName="db_v_tab", partitionColumns="col3",
                                    sortColumns=["col3", "col2"], keepDuplicates="LAST", softDelete=True)
        conn1.run(f"""
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab"), t0);
        """)
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab")""")["col1"].values,
            ['a', 'c', 'd'])
        conn1.close()

    def test_database_createTable_softDelete(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-05', freq="D"), dtype="datetime64[D]")
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value",
                              engine="TSDB")
        conn1.run(
            "t0=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);tableInsert(t0,`a`b`c`d, 1 2 2 4, [2000.01.01,2000.01.02,2000.01.02,2000.01.04])")
        t = conn1.table(data='t0')
        db_v.createTable(table=t, tableName="db_v_tab", sortColumns=["col3", "col2"], keepDuplicates="LAST",
                         softDelete=True)
        conn1.run(f"""
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab"), t0);
        """)
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab")""")["col1"].values,
            ['a', 'c', 'd'])
        conn1.close()

    def test_database_createDimensionTable_softDelete(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(f"""
            if(existsDatabase("dfs://{func_name}_db_value")){{dropDatabase("dfs://{func_name}_db_value")}};
        """)
        dates = np.array(pd.date_range(start='2000-01-01', end='2000-01-05', freq="D"), dtype="datetime64[D]")
        db_v = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=dates,
                              dbPath=f"dfs://{func_name}_db_value",
                              engine="TSDB")
        conn1.run(
            "t0=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);tableInsert(t0,`a`b`c`d, 1 2 2 4, [2000.01.01,2000.01.02,2000.01.02,2000.01.04])")
        t = conn1.table(data='t0')
        db_v.createDimensionTable(table=t, tableName="db_v_tab", sortColumns=["col3", "col2"], keepDuplicates="LAST",
                         softDelete=True)
        conn1.run(f"""
            tableInsert(loadTable("dfs://{func_name}_db_value","db_v_tab"), t0);
        """)
        assert_array_equal(
            conn1.run(f"""select * from loadTable("dfs://{func_name}_db_value","db_v_tab")""")["col1"].values,
            ['a', 'c', 'd'])
        conn1.close()

    def test_database_createTable_OLAP(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        ids = [1, 2, 3]
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(
            f"if(existsDatabase('dfs://{func_name}_db_example')){{dropDatabase('dfs://{func_name}_db_example')}};")
        db = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=ids,
                            dbPath=f"dfs://{func_name}_db_example",
                            engine="OLAP")
        t = conn1.table(
            data=conn1.run("table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3)"))
        db.createTable(table=t, tableName="pt").append(t)
        assert_frame_equal(conn1.run(f"select * from loadTable('dfs://{func_name}_db_example', 'pt')"), pd.DataFrame({
            'col1': np.array(['APPL', 'TESLA', 'GOOGLE', 'PDD']),
            'col2': np.array([1, 2, 3, 4], dtype=np.int32),
            'col3': np.array(['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04'], dtype='datetime64[ns]')}))
        conn1.close()

    def test_database_createTable_TSDB(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        ids = [1, 2, 3]
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(
            f"if(existsDatabase('dfs://{func_name}_db_example')){{dropDatabase('dfs://{func_name}_db_example')}};")
        db = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=ids,
                            dbPath=f"dfs://{func_name}_db_example",
                            engine="TSDB")
        t = conn1.table(
            data=conn1.run("table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3)"))
        db.createTable(table=t, tableName="pt", sortColumns='col2').append(t)
        assert_frame_equal(conn1.run(f"select * from loadTable('dfs://{func_name}_db_example', 'pt')"), pd.DataFrame({
            'col1': np.array(['APPL', 'TESLA', 'GOOGLE', 'PDD']),
            'col2': np.array([1, 2, 3, 4], dtype=np.int32),
            'col3': np.array(['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04'], dtype='datetime64[ns]')}))
        conn1.close()

    def test_database_createTable_PKEY(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        ids = [1, 2, 3]
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(
            f"if(existsDatabase('dfs://{func_name}_db_example')){{dropDatabase('dfs://{func_name}_db_example')}};")
        db = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=ids,
                            dbPath=f"dfs://{func_name}_db_example",
                            engine="PKEY")
        t = conn1.table(
            data=conn1.run("table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3)"))
        db.createTable(table=t, tableName="pt", primaryKey='col1', indexes={"col2": "bloomfilter"}).append(t)
        assert_frame_equal(conn1.run(f"select * from loadTable('dfs://{func_name}_db_example', 'pt')"), pd.DataFrame({
            'col1': np.array(['APPL', 'GOOGLE', 'PDD', 'TESLA']),
            'col2': np.array([1, 3, 4, 2], dtype=np.int32),
            'col3': np.array(['2022-01-01', '2022-01-03', '2022-01-04', '2022-01-02'], dtype='datetime64[ns]')}))
        conn1.close()

    def test_database_createDimensionTable_OLAP(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        ids = [1, 2, 3]
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(
            f"if(existsDatabase('dfs://{func_name}_db_example')){{dropDatabase('dfs://{func_name}_db_example')}};")
        db = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=ids,
                            dbPath=f"dfs://{func_name}_db_example",
                            engine="OLAP")
        t = conn1.table(
            data=conn1.run("table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3)"))
        db.createDimensionTable(table=t, tableName="pt").append(t)
        assert_frame_equal(conn1.run(f"select * from loadTable('dfs://{func_name}_db_example', 'pt')"), pd.DataFrame({
            'col1': np.array(['APPL', 'TESLA', 'GOOGLE', 'PDD']),
            'col2': np.array([1, 2, 3, 4], dtype=np.int32),
            'col3': np.array(['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04'], dtype='datetime64[ns]')}))
        conn1.close()

    def test_database_createDimensionTable_TSDB(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        ids = [1, 2, 3]
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(
            f"if(existsDatabase('dfs://{func_name}_db_example')){{dropDatabase('dfs://{func_name}_db_example')}};")
        db = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=ids,
                            dbPath=f"dfs://{func_name}_db_example",
                            engine="TSDB")
        t = conn1.table(
            data=conn1.run("table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3)"))
        db.createDimensionTable(table=t, tableName="pt", sortColumns='col2').append(t)
        assert_frame_equal(conn1.run(f"select * from loadTable('dfs://{func_name}_db_example', 'pt')"), pd.DataFrame({
            'col1': np.array(['APPL', 'TESLA', 'GOOGLE', 'PDD']),
            'col2': np.array([1, 2, 3, 4], dtype=np.int32),
            'col3': np.array(['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04'], dtype='datetime64[ns]')}))
        conn1.close()

    def test_database_createDimensionTable_PKEY(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        ids = [1, 2, 3]
        func_name = inspect.currentframe().f_code.co_name
        conn1.run(
            f"if(existsDatabase('dfs://{func_name}_db_example')){{dropDatabase('dfs://{func_name}_db_example')}};")
        db = conn1.database(dbName="db_value", partitionType=keys.VALUE, partitions=ids,
                            dbPath=f"dfs://{func_name}_db_example",
                            engine="PKEY")
        t = conn1.table(
            data=conn1.run("table(`APPL`TESLA`GOOGLE`PDD as col1, 1 2 3 4 as col2, 2022.01.01..2022.01.04 as col3)"))
        db.createDimensionTable(table=t, tableName="pt", primaryKey='col1', indexes={"col2": "bloomfilter"}).append(t)
        assert_frame_equal(conn1.run(f"select * from loadTable('dfs://{func_name}_db_example', 'pt')"), pd.DataFrame({
            'col1': np.array(['APPL', 'GOOGLE', 'PDD', 'TESLA']),
            'col2': np.array([1, 3, 4, 2], dtype=np.int32),
            'col3': np.array(['2022-01-01', '2022-01-03', '2022-01-04', '2022-01-02'], dtype='datetime64[ns]')}))
        conn1.close()

    def test_database_create_dfs_database_range_partition(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21]),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.arange(1, 21, dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    def test_database_create_dfs_database_hash_partition(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.HASH, partitions=[keys.DT_INT, 2], dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': 2,
            'partitionSites': None,
            'partitionTypeName': 'HASH',
            'partitionType': 5
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert re['partitionSchema'] == dct['partitionSchema']
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': [1, 2, 3, 4, 5], 'val': [10, 20, 30, 40, 50]})
        t = conn1.table(data=df)
        pt = db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id')
        pt.append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(np.sort(re['id']), df['id'])
        assert_array_equal(np.sort(re['val']), df['val'])
        dt = db.createTable(table=t, tableName='dt')
        dt.append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(np.sort(re['id']), df['id'])
        assert_array_equal(np.sort(re['val']), df['val'])
        conn1.close()

    def test_database_create_dfs_database_value_partition(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.VALUE, partitions=[1, 2, 3], dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 2, 3]),
            'partitionSites': None,
            'partitionTypeName': 'VALUE',
            'partitionType': 1
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.array([1, 2, 3, 1, 2, 3], dtype=np.int32), 'val': [11, 12, 13, 14, 15, 16]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(np.sort(df['id']), np.sort(re['id']))
        assert_array_equal(np.sort(df['val']), np.sort(re['val']))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(np.sort(df['id']), np.sort(re['id']))
        assert_array_equal(np.sort(df['val']), np.sort(re['val']))
        conn1.close()

    def test_database_create_dfs_database_list_partition(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.LIST, partitions=[['IBM', 'ORCL', 'MSFT'], ['GOOG', 'FB']],
                            dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {'databaseDir': dfsDBName,
               'partitionSchema': np.array([np.array(['IBM', 'ORCL', 'MSFT']), np.array(['GOOG', 'FB'])], dtype=object),
               'partitionSites': None,
               'partitionTypeName': 'LIST',
               'partitionType': 3}
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'][0], dct['partitionSchema'][0])
        assert_array_equal(re['partitionSchema'][1], dct['partitionSchema'][1])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'sym': ['IBM', 'ORCL', 'MSFT', 'GOOG', 'FB'], 'val': [1, 2, 3, 4, 5]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='sym').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['sym'], df['sym'])
        assert_array_equal(re['val'], df['val'])
        conn1.close()

    def test_database_create_dfs_database_value_partition_np_date(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        dates = np.array(pd.date_range(start='20120101', end='20120110'), dtype="datetime64[D]")
        db = conn1.database('db', partitionType=keys.VALUE, partitions=dates, dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionType': 1,
            'partitionSchema': np.array(pd.date_range(start='20120101', end='20120110'), dtype="datetime64[D]"),
            'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert re['partitionType'] == dct['partitionType']
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({
            'datetime': [np.datetime64('2012-01-01', 'D'), np.datetime64('2012-01-02', 'D')],
            'sym': ['AA', 'BB'], 'val': [1, 2]
        }, dtype='object')
        df_ = pd.DataFrame({
            'datetime': pd.Series([np.datetime64('2012-01-01', 'D'), np.datetime64('2012-01-02', 'D')],
                                  dtype='datetime64[ns]'),
            'sym': ['AA', 'BB'], 'val': [1, 2]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datetime').append(t)
        re = conn1.run(f"schema(loadTable('{dfsDBName}', 'pt')).colDefs")
        assert_array_equal(re['name'], ['datetime', 'sym', 'val'])
        assert_array_equal(re['typeString'], ['DATE', 'STRING', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['datetime'], df_['datetime'])
        assert_array_equal(re['sym'], df_['sym'])
        assert_array_equal(re['val'], df_['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['datetime'], df_['datetime'])
        assert_array_equal(re['sym'], df_['sym'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    def test_database_create_dfs_database_value_partition_np_month(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        months = np.array(pd.date_range(start='2012-01', end='2012-10', freq="M"), dtype="datetime64[M]")
        db = conn1.database('db', partitionType=keys.VALUE, partitions=months, dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionType': 1,
            'partitionSchema': months,
            'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert re['partitionType'] == dct['partitionType']
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({
            'date': [np.datetime64('2012-01', 'M'), np.datetime64('2012-02', 'M'), np.datetime64('2012-03', 'M'),
                     np.datetime64('2012-04', 'M')],
            'val': [1, 2, 3, 4]
        }, dtype='object')
        df_ = pd.DataFrame({'date': pd.Series(
            [np.datetime64('2012-01', 'M'), np.datetime64('2012-02', 'M'), np.datetime64('2012-03', 'M'),
             np.datetime64('2012-04', 'M')],
            dtype='datetime64[ns]'),
            'val': [1, 2, 3, 4]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='date').append(t)
        scm = conn1.run(f"schema(loadTable('{dfsDBName}', 'pt')).colDefs")
        assert_array_equal(scm['name'], ['date', 'val'])
        assert_array_equal(scm['typeString'], ['MONTH', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['date'], df_['date'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    def test_database_create_dfs_database_value_partition_np_datehour(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        times = np.array(pd.date_range(start='2012-01-01T00', end='2012-01-01T05', freq='h'), dtype="datetime64[h]")
        db = conn1.database('db', partitionType=keys.VALUE, partitions=times, dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionType': 1,
            'partitionSchema': times,
            'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert re['partitionType'] == dct['partitionType']
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({'hour': [np.datetime64('2012-01-01T00', 'h'), np.datetime64('2012-01-01T01', 'h'),
                                    np.datetime64('2012-01-01T02', 'h')], 'val': [1, 2, 3]}, dtype='object')
        df_ = pd.DataFrame({'hour': pd.Series([np.datetime64('2012-01-01T00', 'h'), np.datetime64('2012-01-01T01', 'h'),
                                               np.datetime64('2012-01-01T02', 'h')], dtype='datetime64[ns]'),
                            'val': [1, 2, 3]})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='hour').append(t)
        rtn = conn1.run(f'schema(loadTable("{dfsDBName}","pt"))["colDefs"]')
        assert_array_equal(rtn['name'], ['hour', 'val'])
        assert_array_equal(rtn['typeString'], ['DATEHOUR', 'LONG'])
        rtn = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(rtn['hour'], df_['hour'])
        assert_array_equal(rtn['val'], df_['val'])
        db.createTable(table=t, tableName='dt').append(t)
        rtn = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(rtn['hour'], df_['hour'])
        assert_array_equal(rtn['val'], df_['val'])
        conn1.close()

    def test_database_create_dfs_database_value_partition_np_arange_date(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        dates = np.arange('2012-01-01', '2012-01-10', dtype='datetime64[D]')
        db = conn1.database('db', partitionType=keys.VALUE, partitions=dates, dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionType': 1,
            'partitionSchema': dates,
            'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert re['partitionType'] == dct['partitionType']
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({
            'datetime': [np.datetime64('2012-01-01', 'D'), np.datetime64('2012-01-02', 'D')],
            'sym': ['AA', 'BB'],
            'val': [1, 2]
        }, dtype='object')
        df_ = pd.DataFrame({'datetime': pd.Series([np.datetime64('2012-01-01', 'D'), np.datetime64('2012-01-02', 'D')],
                                                  dtype='datetime64[ns]'), 'sym': ['AA', 'BB'], 'val': [1, 2]})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datetime').append(t)
        re = conn1.run(f"schema(loadTable('{dfsDBName}', 'pt')).colDefs")
        assert_array_equal(re['name'], ['datetime', 'sym', 'val'])
        assert_array_equal(re['typeString'], ['DATE', 'STRING', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['datetime'], df_['datetime'])
        assert_array_equal(re['sym'], df_['sym'])
        assert_array_equal(re['val'], df_['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['datetime'], df_['datetime'])
        assert_array_equal(re['sym'], df_['sym'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    def test_database_create_dfs_database_value_partition_np_arange_month(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        months = np.arange('2012-01', '2012-10', dtype='datetime64[M]')
        db = conn1.database('db', partitionType=keys.VALUE, partitions=months, dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionType': 1,
            'partitionSchema': months,
            'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert re['partitionType'] == dct['partitionType']
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        dct = {
            'databaseDir': dfsDBName,
            'partitionType': 1,
            'partitionSchema': months,
            'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert re['partitionType'] == dct['partitionType']
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({
            'date': [np.datetime64('2012-01', 'M'), np.datetime64('2012-02', 'M'), np.datetime64('2012-03', 'M'),
                     np.datetime64('2012-04', 'M')],
            'val': [1, 2, 3, 4]
        }, dtype='object')
        df_ = pd.DataFrame({
            'date': pd.Series(
                [np.datetime64('2012-01', 'M'), np.datetime64('2012-02', 'M'), np.datetime64('2012-03', 'M'),
                 np.datetime64('2012-04', 'M')], dtype='datetime64[ns]'),
            'val': [1, 2, 3, 4]
        })
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='date').append(t)
        scm = conn1.run(f"schema(loadTable('{dfsDBName}', 'pt')).colDefs")
        assert_array_equal(scm['name'], ['date', 'val'])
        assert_array_equal(scm['typeString'], ['MONTH', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['date'], df_['date'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    def test_database_create_dfs_database_compo_partition(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db1 = conn1.database('db1', partitionType=keys.VALUE,
                             partitions=np.array(["2012-01-01", "2012-01-06"], dtype="datetime64"), dbPath='')
        db2 = conn1.database('db2', partitionType=keys.RANGE, partitions=[1, 6, 11], dbPath='')
        db = conn1.database('db', keys.COMPO, partitions=[db1, db2], dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionType': [1, 2],
            'partitionSchema': [np.array(["2012-01-01", "2012-01-06"], dtype="datetime64"), np.array([1, 6, 11])],
            'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionType'], dct['partitionType'])
        assert_array_equal(re['partitionSchema'][0], dct['partitionSchema'][0])
        assert_array_equal(re['partitionSchema'][1], dct['partitionSchema'][1])
        df = pd.DataFrame({
            'date': np.array(['2012-01-01', '2012-01-01', '2012-01-06', '2012-01-06'], dtype='datetime64'),
            'val': np.array([1, 6, 1, 6], dtype=np.int32)
        })
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns=['date', 'val']).append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['date'], df['date'])
        assert_array_equal(re['val'], df['val'])
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['date'], df['date'])
        assert_array_equal(re['val'], df['val'])
        conn1.close()

    def test_database_create_dfs_table_with_chinese_column_name(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21], dtype=np.int32),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert (re['partitionSites'] == dct['partitionSites'])
        df = pd.DataFrame({'编号': np.arange(1, 21, dtype=np.int32), '值': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='编号').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['编号'], np.arange(1, 21))
        assert_array_equal(re['值'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['编号'], np.arange(1, 21))
        assert_array_equal(re['值'], np.repeat(1, 20))
        conn1.close()

    def test_database_already_exists_with_partition_none(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        script = f'''
            dbPath='{dfsDBName}'
            db = database(dbPath, VALUE, 1 2 3 4 5)
            t = table(1..5 as id, rand(string('A'..'Z'),5) as val)
            pt = db.createPartitionedTable(t, `pt, `id).append!(t)
        '''
        conn1.run(script)
        assert conn1.existsDatabase(dfsDBName)
        db = conn1.database(dbPath=dfsDBName)
        df = pd.DataFrame({'id': np.array([1, 2, 3], dtype=np.int32), 'sym': ['A', 'B', 'C']})
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt1', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt1', dbPath=dfsDBName).toDF()
        assert_array_equal(re['sym'], np.array(['A', 'B', 'C']))
        conn1.close()

    def test_database_dfs_table_value_datehour_as_partitionSchema(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        datehour = np.array(["2021-01-01T01", "2021-01-01T02", "2021-01-01T03", "2021-01-01T04"], dtype="datetime64[h]")
        db = conn1.database('db', partitionType=keys.VALUE, partitions=datehour, dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionType': 1,
            'partitionSchema': datehour,
            'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert re['partitionType'] == dct['partitionType']
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({
            'datehour': [np.datetime64("2021-01-01T01", 'h'), np.datetime64("2021-01-01T02", 'h'),
                         np.datetime64("2021-01-01T03", 'h'), np.datetime64("2021-01-01T04", 'h')],
            'val': [1, 2, 3, 4]
        }, dtype='object')
        df_ = pd.DataFrame({
            'datehour': pd.Series([np.datetime64('2021-01-01T01', 'h'), np.datetime64('2021-01-01T02', 'h'),
                                   np.datetime64('2021-01-01T03', 'h'), np.datetime64("2021-01-01T04", 'h')],
                                  dtype='datetime64[ns]'),
            'val': [1, 2, 3, 4]
        })
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datehour').append(t)
        scm = conn1.run(f"schema(loadTable('{dfsDBName}', 'pt')).colDefs")
        assert_array_equal(scm['name'], ['datehour', 'val'])
        assert_array_equal(scm['typeString'], ['DATEHOUR', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['datehour'], df_['datehour'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    def test_database_dfs_table_range_datehour_as_partitionSchema(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        datehour = np.array(["2012-01-01T00", "2012-01-01T01", "2012-01-01T02", "2012-01-01T03", "2012-01-01T04"],
                            dtype="datetime64")
        db = conn1.database('db', partitionType=keys.RANGE, partitions=datehour, dbPath=dfsDBName)
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionType': 2,
            'partitionSchema': datehour,
            'partitionSites': None
        }
        re = conn1.run("schema(db)")
        assert re['databaseDir'] == dct['databaseDir']
        assert re['partitionType'] == dct['partitionType']
        assert_array_equal(np.sort(re['partitionSchema']), dct['partitionSchema'])
        df = pd.DataFrame({
            'datehour': [np.datetime64('2012-01-01T00', 'h'), np.datetime64('2012-01-01T01', 'h'),
                         np.datetime64('2012-01-01T02', 'h')], 'val': [1, 2, 3]
        }, dtype='object')
        df_ = pd.DataFrame({
            'datehour': pd.Series([np.datetime64('2012-01-01T00', 'h'), np.datetime64('2012-01-01T01', 'h'),
                                   np.datetime64('2012-01-01T02', 'h')], dtype='datetime64[ns]'),
            'val': [1, 2, 3]
        })
        t = conn1.table(data=df)
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='datehour').append(t)
        scm = conn1.run(f"schema(loadTable('{dfsDBName}', 'pt')).colDefs")
        assert_array_equal(scm['name'], ['datehour', 'val'])
        assert_array_equal(scm['typeString'], ['DATEHOUR', 'LONG'])
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['datehour'], df_['datehour'])
        assert_array_equal(re['val'], df_['val'])
        conn1.close()

    def test_database_create_database_engine_olap(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName,
                            engine="OLAP")
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21]),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2,
            'engineType': 'OLAP'
        }
        re = conn1.run("schema(db)")
        assert re['engineType'] == dct['engineType']
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.arange(1, 21, dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    def test_database_create_database_engine_tsdb(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName,
                            engine="TSDB")
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21]),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2,
            'engineType': 'TSDB'
        }
        re = conn1.run("schema(db)")
        assert re['engineType'] == dct['engineType']
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.arange(1, 21, dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', sortColumns="val").append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        df = pd.DataFrame({'id': np.arange(20, 0, -1), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createTable(table=t, tableName='dt', sortColumns="id").append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    def test_database_create_database_engine_pkey(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName,
                            engine="PKEY")
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21]),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2,
            'engineType': 'PKEY'
        }
        re = conn1.run("schema(db)")
        assert re['engineType'] == dct['engineType']
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.arange(1, 21, dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', primaryKey="id").append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        df = pd.DataFrame({'id': np.arange(20, 0, -1), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createTable(table=t, tableName='dt', primaryKey="id").append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    def test_database_create_database_atomic_TRANS(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName,
                            atomic="TRANS")
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21]),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2,
            'atomic': 'TRANS'
        }
        re = conn1.run("schema(db)")
        assert re['atomic'] == dct['atomic']
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.arange(1, 21, dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    def test_database_create_database_atomic_CHUNK(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName,
                            atomic="CHUNK")
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21]),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2,
            'atomic': 'CHUNK'
        }
        re = conn1.run("schema(db)")
        assert re['atomic'] == dct['atomic']
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.arange(1, 21, dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    def test_database_create_database_chunkGranularity_TABLE(self):
        conn1 = ddb.session(HOST, PORT, USER, PASSWD, enableChunkGranularityConfig=True)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName,
                            chunkGranularity="TABLE")
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21]),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2,
            'chunkGranularity': 'TABLE'
        }
        re = conn1.run("schema(db)")
        assert re['chunkGranularity'] == dct['chunkGranularity']
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.arange(1, 21, dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    def test_database_create_database_chunkGranularity_DATABASE(self):
        conn1 = ddb.session(enableChunkGranularityConfig=True)
        conn1.connect(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName,
                            chunkGranularity="DATABASE")
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21]),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2,
            'chunkGranularity': 'DATABASE'
        }
        re = conn1.run("schema(db)")
        assert re['chunkGranularity'] == dct['chunkGranularity']
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.arange(1, 21, dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id').append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        db.createTable(table=t, tableName='dt').append(t)
        re = conn1.loadTable(tableName='dt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    def test_database_create_database_engine_atomic_chunkGranularity(self):
        conn1 = ddb.session(enableChunkGranularityConfig=True)
        conn1.connect(HOST, PORT, USER, PASSWD)
        func_name = inspect.currentframe().f_code.co_name
        dfsDBName = f"dfs://{func_name}"
        if conn1.existsDatabase(dfsDBName):
            conn1.dropDatabase(dfsDBName)
        db = conn1.database('db', partitionType=keys.RANGE, partitions=[1, 11, 21], dbPath=dfsDBName,
                            engine="TSDB", atomic="CHUNK", chunkGranularity="DATABASE")
        assert conn1.existsDatabase(dfsDBName)
        dct = {
            'databaseDir': dfsDBName,
            'partitionSchema': np.array([1, 11, 21]),
            'partitionSites': None,
            'partitionTypeName': 'RANGE',
            'partitionType': 2,
            'engineType': 'TSDB',
            'atomic': 'CHUNK',
            'chunkGranularity': 'DATABASE'
        }
        re = conn1.run("schema(db)")
        assert re['engineType'] == dct['engineType']
        assert re['atomic'] == dct['atomic']
        assert re['chunkGranularity'] == dct['chunkGranularity']
        assert re['databaseDir'] == dct['databaseDir']
        assert_array_equal(re['partitionSchema'], dct['partitionSchema'])
        assert re['partitionSites'] == dct['partitionSites']
        df = pd.DataFrame({'id': np.arange(1, 21, dtype=np.int32), 'val': np.repeat(1, 20)})
        t = conn1.table(data=df, tableAliasName='t')
        db.createPartitionedTable(table=t, tableName='pt', partitionColumns='id', sortColumns="val").append(t)
        re = conn1.loadTable(tableName='pt', dbPath=dfsDBName).toDF()
        assert_array_equal(re['id'], np.arange(1, 21))
        assert_array_equal(re['val'], np.repeat(1, 20))
        conn1.close()

    def test_database_save_table(self):
        tb = self.conn.table(data=pd.DataFrame({'a': [1, 2, 3]}))
        assert self.conn.saveTable(tb, REMOTE_WORK_DIR + 'test')
