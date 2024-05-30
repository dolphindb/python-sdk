#! /bin/bash
ps aux |grep "subscriber.py" | grep -v grep | awk '{print $2}' | xargs kill -9
