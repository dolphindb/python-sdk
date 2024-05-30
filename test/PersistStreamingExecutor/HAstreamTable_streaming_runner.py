import dolphindb as ddb
import time
import random
import paramiko

dbname_test_HA_sub_HA = "dfs://test_8_HA_sub_py"
tablename_HA = "trades8_HA"

streamtable_HA = "trades_stream_HA_py"

create_HAstreamTable_init = """
    login(`admin,`123456)
    try{dropStreamTable(`trades_stream_HA_py);}catch(ex){print(ex)};
    try{undef(`im_trades_stream_HA_py, SHARED);}catch(ex){print(ex)};
    go
    t=table(1:0,`marketType`securityCode`origTime`tradingPhaseCode`preClosePrice`openPrice`highPrice`lowPrice`lastPrice`closePrice`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5`bidPrice6`bidPrice7`bidPrice8`bidPrice9`bidPrice10`bidVolume1`bidVolume2`bidVolume3`bidVolume4`bidVolume5`bidVolume6`bidVolume7`bidVolume8`bidVolume9`bidVolume10`offerPrice1`offerPrice2`offerPrice3`offerPrice4`offerPrice5`offerPrice6`offerPrice7`offerPrice8`offerPrice9`offerPrice10`offerVolume1`offerVolume2`offerVolume3`offerVolume4`offerVolume5`offerVolume6`offerVolume7`offerVolume8`offerVolume9`offerVolume10`numTrades`totalVolumeTrade`totalValueTrade`totalBidVolume`totalOfferVolume`weightedAvgBidPrice`weightedAvgOfferPrice`ioPV`yieldToMaturity`highLimited`lowLimited`priceEarningRatio1`priceEarningRatio2`change1`change2`channelNo`mdStreamID`instrumentStatus`preCloseIOPV`altWeightedAvgBidPrice`altWeightedAvgOfferPrice`etfBuyNumber`etfBuyAmount`etfBuyMoney`etfSellNumber`etfSellAmount`etfSellMoney`totalWarrantExecVolume`warLowerPrice`warUpperPrice`withdrawBuyNumber`withdrawBuyAmount`withdrawBuyMoney`withdrawSellNumber`withdrawSellAmount`withdrawSellMoney`totalBidNumber`totalOfferNumber`bidTradeMaxDuration`offerTradeMaxDuration`numBidOrders`bnumOfferOrders`lastTradeTime`varietyCategory`receivedTime`dailyIndex,
        [INT, SYMBOL, TIMESTAMP, STRING, 
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        INT, STRING, STRING,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        INT, INT, INT, INT, DOUBLE, CHAR, NANOTIMESTAMP, INT]);
    go
    //创建高可用流表
    haStreamTable(11,t,`trades_stream_HA_py,100000);
    //创建和高可用流表一模一样的共享内存表
    share t as im_trades_stream_HA_py;
"""

create_dfs_init = """
    login(`admin,`123456)
    t=table(1:0,`marketType`securityCode`origTime`tradingPhaseCode`preClosePrice`openPrice`highPrice`lowPrice`lastPrice`closePrice`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5`bidPrice6`bidPrice7`bidPrice8`bidPrice9`bidPrice10`bidVolume1`bidVolume2`bidVolume3`bidVolume4`bidVolume5`bidVolume6`bidVolume7`bidVolume8`bidVolume9`bidVolume10`offerPrice1`offerPrice2`offerPrice3`offerPrice4`offerPrice5`offerPrice6`offerPrice7`offerPrice8`offerPrice9`offerPrice10`offerVolume1`offerVolume2`offerVolume3`offerVolume4`offerVolume5`offerVolume6`offerVolume7`offerVolume8`offerVolume9`offerVolume10`numTrades`totalVolumeTrade`totalValueTrade`totalBidVolume`totalOfferVolume`weightedAvgBidPrice`weightedAvgOfferPrice`ioPV`yieldToMaturity`highLimited`lowLimited`priceEarningRatio1`priceEarningRatio2`change1`change2`channelNo`mdStreamID`instrumentStatus`preCloseIOPV`altWeightedAvgBidPrice`altWeightedAvgOfferPrice`etfBuyNumber`etfBuyAmount`etfBuyMoney`etfSellNumber`etfSellAmount`etfSellMoney`totalWarrantExecVolume`warLowerPrice`warUpperPrice`withdrawBuyNumber`withdrawBuyAmount`withdrawBuyMoney`withdrawSellNumber`withdrawSellAmount`withdrawSellMoney`totalBidNumber`totalOfferNumber`bidTradeMaxDuration`offerTradeMaxDuration`numBidOrders`bnumOfferOrders`lastTradeTime`varietyCategory`receivedTime`dailyIndex,
        [INT, SYMBOL, TIMESTAMP, STRING, 
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        INT, STRING, STRING,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        INT, INT, INT, INT, DOUBLE, CHAR, NANOTIMESTAMP, INT]);
    dbname1 = "{}"
    tablename1 = "{}"
    if(existsDatabase(dbname1)){{
    dropDatabase(dbname1)
    }};
    db = database(dbname1, partitionType=VALUE, partitionScheme=1..4,chunkGranularity='TABLE')
    db.createPartitionedTable(table=t, tableName=tablename1, partitionColumns=`marketType)
""".format(dbname_test_HA_sub_HA, tablename_HA)

assert_init = """
                data = select * from loadTable("{dfspath}", "{dfs_tablename}") order by origTime limit {start_row},{rows}
                undef(`sqlSelect,DEF)
                def sqlSelect(){{
                    return select * from objByName(`im_trades_stream_HA_py) order by origTime limit {start_row},{rows}
                }}
                ex = rpc('P1-datanode', sqlSelect)
                res = each(eqObj, data.values(),ex.values())
                all(res)
                """

insert_s_init = """
    tmp=table(1:0,`marketType`securityCode`origTime`tradingPhaseCode`preClosePrice`openPrice`highPrice`lowPrice`lastPrice`closePrice`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5`bidPrice6`bidPrice7`bidPrice8`bidPrice9`bidPrice10`bidVolume1`bidVolume2`bidVolume3`bidVolume4`bidVolume5`bidVolume6`bidVolume7`bidVolume8`bidVolume9`bidVolume10`offerPrice1`offerPrice2`offerPrice3`offerPrice4`offerPrice5`offerPrice6`offerPrice7`offerPrice8`offerPrice9`offerPrice10`offerVolume1`offerVolume2`offerVolume3`offerVolume4`offerVolume5`offerVolume6`offerVolume7`offerVolume8`offerVolume9`offerVolume10`numTrades`totalVolumeTrade`totalValueTrade`totalBidVolume`totalOfferVolume`weightedAvgBidPrice`weightedAvgOfferPrice`ioPV`yieldToMaturity`highLimited`lowLimited`priceEarningRatio1`priceEarningRatio2`change1`change2`channelNo`mdStreamID`instrumentStatus`preCloseIOPV`altWeightedAvgBidPrice`altWeightedAvgOfferPrice`etfBuyNumber`etfBuyAmount`etfBuyMoney`etfSellNumber`etfSellAmount`etfSellMoney`totalWarrantExecVolume`warLowerPrice`warUpperPrice`withdrawBuyNumber`withdrawBuyAmount`withdrawBuyMoney`withdrawSellNumber`withdrawSellAmount`withdrawSellMoney`totalBidNumber`totalOfferNumber`bidTradeMaxDuration`offerTradeMaxDuration`numBidOrders`bnumOfferOrders`lastTradeTime`varietyCategory`receivedTime`dailyIndex,
        [INT, SYMBOL, TIMESTAMP, STRING, 
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        INT, STRING, STRING,
        DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,
        INT, INT, INT, INT, DOUBLE, CHAR, NANOTIMESTAMP, INT]);
    
    for(i in 0:{rows}){{
        tableInsert(tmp, 
                rand(1 2 3 4, 1), rand(`apl`goog`ms`ama, 1), now(), rand(`s1`s2`s3`s4,1), 
                rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),
                rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),
                rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),
                rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),
                rand(100000, 1),rand(`a1`a2`a3`a4, 1),rand(`b1`b2`b3`b4, 1),
                rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),rand(1000.00,1),
                rand(100000, 1),rand(100000, 1),rand(100000, 1),rand(100000, 1),rand(10000000l, 1),rand(127c, 1), now(true),rand(100000, 1));sleep(5)
    }}

    objByName(`im_trades_stream_HA_py).append!(tmp)

    def insert_job(t){{
        objByName(`trades_stream_HA_py).append!(t)
        
    }}
    rpc(getStreamingLeader(11), insert_job, tmp)
    """

# 创建SSH Client对象
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())


# 定义服务器IP地址和登录信息
server7_ip = '192.168.100.7'
server8_ip = '192.168.100.8'
server9_ip = '192.168.100.9'
server7_nodeName = 'P1-datanode'
server8_nodeName = 'P2-datanode'
server9_nodeName = 'P3-datanode'
username = 'yzou'
password = 'DolphinDB123'

USER = 'admin'
PASSWD = '123456'

# 定义文件路径
file_path_8 = '/hdd/hdd0/yzou/persistStreaming_server/py/output8_HA_py.log'

def run_remote_subscribe(ip, user, passwd):
    ssh.connect(hostname=ip, username=user, password=passwd)
    ssh.exec_command(
        'cd /hdd/hdd0/yzou/persistStreaming_server/py && sh startSubscribe_py.sh HA')
    ssh.close()

def run_remote_unsubscribe(ip, user, passwd):
    ssh.connect(hostname=ip, username=user, password=passwd)
    ssh.exec_command(
        'cd /hdd/hdd0/yzou/persistStreaming_server/py && sh stopSubscribe_py.sh HA')
    ssh.close()


# 获取远程文件的最后修改时间
def get_remote_file_last_modify_time(ip, user, passwd, file_path):
    ssh.connect(hostname=ip, username=user, password=passwd)
    stdin, stdout, stderr = ssh.exec_command('stat -c "%Y" ' + file_path)
    timestamp = stdout.read().decode('utf-8')
    ssh.close()
    if timestamp == '':
        return 0
    return int(timestamp)


def init_environment(host, port):
    conn = ddb.session()
    conn.connect(host, port, USER, PASSWD)
    conn.run(create_dfs_init)
    conn.close()

def create_HAstreamTable(host, port):
    conn = ddb.session()
    conn.connect(host, port, USER, PASSWD)
    conn.run(create_HAstreamTable_init)
    conn.close()


def insert_to_streamTable(host, port, rows):
    conn = ddb.session()
    conn.connect(host, port, USER, PASSWD)
    nodes = [server7_nodeName,server8_nodeName,server9_nodeName]
    target_node = nodes[random.randint(0, 2)]
    print(f"切换流订阅leader节点为：{target_node}，并向流表写入数据, 行数：", rows)
    conn.run(f"""rpc('{target_node}',streamCampaignForLeader,11);""")
    time.sleep(10)
    conn.run(insert_s_init.format(rows=rows))
    conn.close()
    print("写入完成")

def clearEnv(host, port):  # 清理环境，包括流表、数据库
    drop_db_scr = """
        if(existsDatabase('{db}'))
            dropDatabase('{db}')
    """
    drop_streamTable_scr = """
        rpc('{nodeName}', dropStreamTable, `{streamTableName})
        for (obj in exec name from objs(true) where shared=true){
            undef(obj, SHARED)
        }
        undef all;
        clearAllCache();
        go
    """

    conn = ddb.session(host, port, USER, PASSWD)
    conn.run(drop_db_scr.format(db = dbname_test_HA_sub_HA))

    conn.run(drop_streamTable_scr.format(
        nodeName=server7_nodeName, streamTableName=streamtable_HA))

    conn.close()
    time.sleep(10)


# 主要监控逻辑
last_modify_time = get_remote_file_last_modify_time(server8_ip, username, password, file_path_8)


def wait_and_assert(row_start=0, row_count=1) -> bool:
    print("开始判断当前批订阅数据是否与插入数据一致...")
    interval = 30             # 时间间隔为30秒
    global last_modify_time

    while True:
        remote_modify_time = get_remote_file_last_modify_time(server9_ip, username, password, file_path_8)

        if remote_modify_time != last_modify_time:     # 如果文件被修改
            last_modify_time = remote_modify_time
            print("等待订阅完成...")

        else: # 如果日志文件已经超过60s没被修改，则认定为订阅已完成
            time.sleep(interval*2) 
            remote_modify_time = get_remote_file_last_modify_time(server9_ip, username, password, file_path_8)
            if remote_modify_time == last_modify_time:
                print("订阅已完成")
                break
        time.sleep(interval)

    def connect_ddb_assert(host, port):
        conn = ddb.session()
        conn.connect(host, port, USER, PASSWD)
        assert_script = assert_init.format(dfspath=dbname_test_HA_sub_HA, dfs_tablename=tablename_HA,
                                             start_row=row_start, rows=row_count)
        res1 = conn.run(assert_script)
        if not res1:
            print(assert_script)
            print(f"[{host}:{port}]: 本次高可用订阅中存在不同数据，断言失败")

        conn.close()

        return res1
    ans1 = connect_ddb_assert(server8_ip, 13802)

    return ans1


if __name__ == '__main__':

    start_row = 0
    try:
        run_remote_unsubscribe(server8_ip, username, password) # 清理残留订阅
    except:
        pass

    time.sleep(2)

    init_environment(server8_ip, 13802)
    create_HAstreamTable(server7_ip, 13802)
    run_remote_subscribe(server8_ip, username, password)

    ind = 0
    while True:
        ind += 1
        try:
            if ind % 5000 == 0:  # 设置最大的写入行数限制，当前最多到5000w行就清空环境并重新订阅，避免服务器负载过高
                print("定时清理环境开始")
                run_remote_unsubscribe(server8_ip, username, password) 

                time.sleep(2)
                clearEnv(server8_ip, 13802)
                print("清理完成，重新构造流表")

                init_environment(server8_ip, 13802)
                create_HAstreamTable(server7_ip, 13802)

                run_remote_subscribe(server8_ip, username, password) 
                start_row = 0
            rows = random.randint(2000, 10000)
            # rows = 100
            insert_to_streamTable(server7_ip, 13802, rows)
            if not wait_and_assert(start_row, rows):
                break
            print("数据全部一致，断言通过")
            start_row = rows + start_row

        except Exception as e:
            print("some error occurred: ", str(e))
            break