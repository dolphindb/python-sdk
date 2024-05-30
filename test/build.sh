#!/bin/bash

# $1: Python Version:               like 3.10
# $2: Coverage test.cov PATH:       like /project/python-sdk

cd /project/python-sdk

# if coverage test
if [ -n "$2" ]
then
    echo `pwd`
    export CXXFLAGS="-std=gnu++11 -static-libgcc -static-libstdc++"
    export PATH="/opt/BullseyeCoverage/bin:$PATH"
    export COVFILE="$2/test$1.cov"
    cov01 -1
    while IFS= read -r line
    do
        covselect -a "$line"
    done < "test/rule.txt"

fi

# build gtest cases
pip$1 install -r test/build_requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
rm -rf build
rm -rf dist
rm -rf wheelhouse
python$1 setup.py bdist_wheel