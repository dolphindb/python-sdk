FROM quay.io/pypa/manylinux_2_28_x86_64:2026.08.08-1

ARG http_proxy
ARG https_proxy

RUN yum clean all && \
    yum install -y \
        libuuid \
        libuuid-devel \
        wget \
        perl-IPC-Cmd && \
    yum clean all && \
    rm -rf /var/cache/yum

ENV PATH=/opt/rh/gcc-toolset-14/root/usr/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

WORKDIR /tmp

RUN wget https://github.com/openssl/openssl/releases/download/openssl-3.5.0/openssl-3.5.0.tar.gz && \
    tar -xzf openssl-3.5.0.tar.gz

WORKDIR /tmp/openssl-3.5.0

RUN mkdir build

WORKDIR /tmp/openssl-3.5.0/build

RUN ../Configure no-apps no-docs no-shared --prefix=/usr/local/openssl && \
    make -j"$(nproc)" && \
    make install

ENV OPENSSL_ROOT_DIR=/usr/local/openssl

WORKDIR /