FROM python:3.10.13-bullseye
RUN sed -i 's/deb.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list
RUN sed -i 's/security.debian.org/mirrors.tuna.tsinghua.edu.cn/g' /etc/apt/sources.list
RUN apt update -y
RUN apt install -y cmake

WORKDIR /tmp
RUN curl https://www.openssl.org/source/old/1.0.2/openssl-1.0.2u.tar.gz > openssl-1.0.2u.tar.gz
RUN tar -xzf openssl-1.0.2u.tar.gz
WORKDIR /tmp/openssl-1.0.2u
ENV CFLAGS=-fPIC
ENV CXXFLAGS=-std=gnu++11
RUN ./config shared no-asm --prefix=/usr/local/openssl --openssldir=/usr/local/openssl 
RUN make -j
RUN make install
ENV OPENSSL_ROOT_DIR=/usr/local/openssl

WORKDIR /
ENV CXXFLAGS="-std=gnu++11 -static-libgcc -static-libstdc++"
ENV PATH="/opt/BullseyeCoverage/bin:$PATH"