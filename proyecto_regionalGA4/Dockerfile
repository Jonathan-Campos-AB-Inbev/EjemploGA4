#FROM centos:7
FROM python:3.10.7-alpine3.16
#RUN yum install -y python3\
    #&& pip3 install --upgrade pip
#BUILD PYARROW
RUN apk update \
&& apk upgrade \
&& apk add --no-cache build-base \
autoconf \
bash \
bison \
boost-dev \
cmake \
flex \
libressl-dev \
zlib-dev

RUN pip install --no-cache-dir six pytest numpy cython
RUN pip install --no-cache-dir pandas

ARG ARROW_VERSION=3.0.0
ARG ARROW_SHA1=c1fed962cddfab1966a0e03461376ebb28cf17d3
ARG ARROW_BUILD_TYPE=release

ENV ARROW_HOME=/usr/local \
PARQUET_HOME=/usr/local

#Download and build apache-arrow
RUN mkdir /arrow \
&& wget -q https://github.com/apache/arrow/archive/apache-arrow-${ARROW_VERSION}.tar.gz -O /tmp/apache-arrow.tar.gz \
&& echo "${ARROW_SHA1} *apache-arrow.tar.gz" | sha1sum /tmp/apache-arrow.tar.gz \
&& tar -xvf /tmp/apache-arrow.tar.gz -C /arrow --strip-components 1 \
&& mkdir -p /arrow/cpp/build \
&& cd /arrow/cpp/build \
&& cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
-DOPENSSL_ROOT_DIR=/usr/local/ssl \
-DCMAKE_INSTALL_LIBDIR=lib \
-DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
-DARROW_WITH_BZ2=ON \
-DARROW_WITH_ZLIB=ON \
-DARROW_WITH_ZSTD=ON \
-DARROW_WITH_LZ4=ON \
-DARROW_WITH_SNAPPY=ON \
-DARROW_PARQUET=ON \
-DARROW_PYTHON=ON \
-DARROW_PLASMA=ON \
-DARROW_BUILD_TESTS=OFF \
.. \
&& make -j$(nproc) \
&& make install \
&& cd /arrow/python \
&& python setup.py build_ext --build-type=$ARROW_BUILD_TYPE --with-parquet \
&& python setup.py install \
&& rm -rf /arrow /tmp/apache-arrow.tar.gz
#END BUILD PYARROW

WORKDIR /app

RUN apk add --no-cache --update \
    python3 python3-dev gcc \
    gfortran musl-dev g++ \
    libffi-dev openssl-dev \
    libxml2 libxml2-dev \
    libxslt libxslt-dev \
    libjpeg-turbo-dev zlib-dev

COPY . /app
RUN apk add build-base linux-headers
RUN pip3 install --upgrade pip
RUN pip3 install grpcio
RUN pip3 --no-cache-dir install -r requirements.txt

#CMD ["python3", "src/main.py"]
CMD exec gunicorn --bind 0.0.0.0:8082 --workers 1 --threads 8 --timeout 3600 main:app
