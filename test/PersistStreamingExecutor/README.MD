
# 长期性流订阅自动化脚本

## 简介

1. [subscriber.py](./subscriber.py)为具体的Python API流订阅客户端源码，里面包含三种不同类型流表的订阅函数，运行时传参'normal', 'HA', 'd'分别代表订阅普通流表、高可用流表、异构流表，每个订阅均为远程订阅。
2. 订阅方案如下：

    |订阅客户端所在物理机 | 订阅方式 | 订阅类型 |
    |-------------|----------|-------------|
    |100.7 | 远程订阅（100.9）| 普通订阅 |
    |100.8 | 远程订阅（100.7）| 高可用订阅 |
    |100.9 | 远程订阅（100.8）| 异构流表订阅 |

3. 当前目录下3个Python文件分别是对应的流订阅操作脚本，里面包含`建库建表，创建被订阅流表，向流表写入数据，数据正确性断言`等操作，可在本地运行py文件开启测试。

## 服务器文件信息

Python API流订阅客户端文件所在服务器上的路径：

```shell
/hdd/hdd0/yzou/persistStreaming_server/py/subscriber.py
```

订阅日志文件路径，可tailf实时查看已订阅条数：

```shell
/hdd/hdd0/yzou/persistStreaming_server/py/output_xxx.log
```

dolphindb数据节点服务器上的路径：

```shell
/hdd/hdd0/yzou/persistStreaming_server/server_p1 # 100.7
/hdd/hdd0/yzou/persistStreaming_server/server_p2 # 100.8
/hdd/hdd0/yzou/persistStreaming_server/server_p3 # 100.9
```