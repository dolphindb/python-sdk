# How to build & test

## test with Docker

```shell
docker build -t python-coverage:3.10.13 python-sdk/.dockerfiles/linux_coverage/
```

## step 1: prepare OPENSSL 1.0.2u

1. build & install OPENSSL 1.0.2u
2. export OPENSSL_ROOT_DIR=/path_to_openssl_1.0.2u/

## step 2: prepare Python

1. prepare Python Excutor and add into PATH

## step 3: prepare BullseyeCoverage [option]

1. Create a symbolic link to enable access to BullseyeCoverage via `/opt/BullseyeCoverage`.
2. check your BullseyeCoverage lic.

## step 4: build & test

### build with docker

> **NOTICE:** 
> During execution, the 'python-sdk' should be included  in the current directory and the current directory should be mapped to '/project'.


#### 1. build python-sdk whl

```
sudo docker run --rm -v `pwd`:/project -v /opt/BullseyeCoverage/:/opt/BullseyeCoverage python-coverage:3.10.13 /project/python-sdk/test/build.sh 3.10 /project/python-sdk
```

#### 2. build C++ gtest excutable

```
sudo docker run --rm -v `pwd`:/project -v /opt/BullseyeCoverage/:/opt/BullseyeCoverage python-coverage:3.10.13 /project/python-sdk/core/test/build.sh 3.10 /project/python-sdk
```

#### 3. test whl & excutable

if using BullseyeCoverage, excute `export COVFILE=python-sdk/test3.10.cov`

1- test with pytest
```
pip install --force-reinstall python-sdk/dist/xxxxx.whl
pytest test_files.py
```

2- test with gtest
```
export LD_LIBRARY_PATH=/path_to_python_lib/
python-sdk/core/test/build/gt_main
```

#### 4. build report

```
sudo docker run --rm -v `pwd`:/project -v /opt/BullseyeCoverage/:/opt/BullseyeCoverage python-coverage:3.10.13 /project/python-sdk/test/generate_report.sh 3.10 /project/python-sdk /project/result
```

### build with external environment

# How to change covselect rule

If you want to modify the rules for code coverage testing, please add the rules in the `python-sdk/test/rule.txt` file.

You can refer to the link [here](https://www.bullseye.com/help/build-exclude.html) for guidance on writing rules.

> **NOTICE:** The last line must be followed by an empty line, otherwise the final rule won't be recognized.
