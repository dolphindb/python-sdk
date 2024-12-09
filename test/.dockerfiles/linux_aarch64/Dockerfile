FROM quay.io/pypa/manylinux2014_aarch64
WORKDIR /root
RUN mkdir /root/.pip
RUN echo -e "[global]\nindex-url=https://mirrors.aliyun.com/pypi/simple/\n[install]\ntrusted-host=mirrors.aliyun.com" > /root/.pip/pip.conf
