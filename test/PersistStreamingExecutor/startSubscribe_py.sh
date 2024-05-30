#! /bin/bash
if [ $1 == "normal" ];then
    nohup python -u subscriber.py normal > output7_py.log 2>&1 &
elif [ $1 == "HA" ];then
    nohup python -u subscriber.py HA > output8_HA_py.log 2>&1 &
else
    nohup python -u subscriber.py d > output9_d_py.log 2>&1 &
fi
