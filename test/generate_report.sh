#!/bin/bash

# $1: Python Version:               like 3.10
# $2: Coverage test.cov PATH:       like /project/python-sdk
# $3: Coverage report PATH:         like /project/result

if [ -z "$1" ]
then
    echo "ERROR: need to specify python version like '3.10'."
    exit 1
fi

if [ -z "$2" ]
then
    echo "ERROR: need to specify coverage file path like '/project/python-sdk'."
    exit 1
fi

if [ -z "$3" ]
then
    echo "ERROR: need to specify coverage report path like '/project/result'."
    exit 1
fi

covhtml -f $2/test$1.cov $3