FROM quay.io/pypa/manylinux2014_aarch64:2025.05.16-1
ARG https_proxy
RUN yum update -y && \
    yum install -y libuuid libuuid-devel && \
    yum install -y devtoolset-8-gcc-c++.aarch64 && \
    yum install -y wget && \
    yum install -y perl-IPC-Cmd
ENV PATH=/opt/rh/devtoolset-8/root/usr/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

WORKDIR /tmp
RUN wget https://github.com/openssl/openssl/releases/download/openssl-3.5.0/openssl-3.5.0.tar.gz
RUN tar -xzf openssl-3.5.0.tar.gz
WORKDIR /tmp/openssl-3.5.0
RUN mkdir build && cd build
WORKDIR /tmp/openssl-3.5.0/build
RUN ../Configure no-apps no-docs no-shared --prefix=/usr/local/openssl
RUN make -j16 && make install
ENV OPENSSL_ROOT_DIR=/usr/local/openssl

WORKDIR /

