FROM quay.io/pypa/manylinux_2_28_aarch64:2026.01.16-1
WORKDIR /root
RUN yum update -y
RUN yum install gdb -y
RUN mkdir /root/.pip
RUN echo -e "[global]\nindex-url=https://mirrors.aliyun.com/pypi/simple/\n[install]\ntrusted-host=mirrors.aliyun.com" > /root/.pip/pip.conf