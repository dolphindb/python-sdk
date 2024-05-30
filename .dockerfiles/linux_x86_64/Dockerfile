FROM quay.io/pypa/manylinux2014_x86_64
RUN yum update -y
RUN yum install -y libuuid
RUN yum install -y libuuid-devel
RUN yum install -y devtoolset-8-gcc-c++.x86_64
ENV PATH=/opt/rh/devtoolset-8/root/usr/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

WORKDIR /tmp
RUN curl https://www.openssl.org/source/old/1.0.2/openssl-1.0.2u.tar.gz > openssl-1.0.2u.tar.gz
RUN tar -xzf openssl-1.0.2u.tar.gz
WORKDIR /tmp/openssl-1.0.2u
ENV CFLAGS=-fPIC
RUN ./config shared no-asm --prefix=/usr/local/openssl --openssldir=/usr/local/openssl 
RUN make -j
RUN make install
ENV OPENSSL_ROOT_DIR=/usr/local/openssl


