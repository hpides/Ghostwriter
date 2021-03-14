FROM ubuntu:20.04

RUN apt update && \
    apt upgrade -y && \
		apt install -y \
		aptitude \
		autotools-dev \
		binutils-dev \
		bison \
		build-essential \
		ccache \
		flex \
		g++ \
		git \
		libboost-all-dev \
		libbz2-dev \
		libdouble-conversion-dev \
		libevent-dev \
		libffi-dev \
		libgflags-dev \
		libgoogle-glog-dev \
		libgtest-dev \
		libiberty-dev \
		libicu-dev \
		libjemalloc-dev \
		liblz4-dev \
		liblzma-dev \
		libsnappy-dev \
		libssl-dev \
		libtbb-dev \
		libxml2-dev \
		make \
		pkg-config \
		python-dev \
		zlib1g-dev \
		wget

RUN yum -y update && yum install -y centos-release-scl \
    && yum -y install git dnf wget libtool perl-core zlib-devel openssl openssl-devel rdma-core-devel \
    && yum -y install python-devel bzip2-devel-1.0.6-13.el7.x86_64 numactl numactl-devel binutils-devel \
    && yum clean all && dnf -y install devtoolset-9 && dnf clean all && yum clean all \
    && source /opt/rh/devtoolset-9/enable \
    && cd ${HOME} \
    && wget https://github.com/Kitware/CMake/releases/download/v3.17.3/cmake-3.17.3.tar.gz \
    && tar -zxvf cmake-3.17.3.tar.gz \
    && cd cmake-3.17.3 \
    && ./bootstrap --parallel=4 \
    && make -j \
    && make install \
    && cd ${HOME} \
    && rm -rf cmake-3.17.3 \
    && rm -rf cmake-3.17.3.tar.gz 

RUN cd && \
    apt remove --purge --auto-remove cmake && \
    version=3.16 && \
    build=2 && \
    mkdir ~/temp && \
    cd ~/temp && \
    wget https://cmake.org/files/v$version/cmake-$version.$build.tar.gz && \
    tar -xzvf cmake-$version.$build.tar.gz && \
    cd cmake-$version.$build/ && \
    ./bootstrap && \
    make -j$(nproc) && \
    make install

RUN cd /usr/src/gtest && \
    cmake . && \
		make && \
		cp *.a /usr/lib && \
		mkdir -p /usr/local/lib/gtest && \
		ln -s /usr/lib/libgtest.a /usr/local/lib/gtest/libgtest.a && \
		ln -s /usr/lib/libgtest_main.a /usr/local/lib/gtest/libgtest_main.a

RUN cd && \
    git clone --depth 1 https://github.com/google/benchmark.git && \
		cd benchmark && \
		mkdir build && \
		cd build && \
		cmake .. -DCMAKE_BUILD_TYPE=Release -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON && \
		make -j$(nproc) && \
		make install

RUN cd && \
    git clone https://github.com/llvm/llvm-project.git && \
		cd llvm-project && \
		git checkout e3a94ba4a92 && \
		mkdir build && \
		cd build && \
		cmake -DLLVM_ENABLE_PROJECTS=clang -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_SHARED_LIBS=ON -DCLANG_INCLUDE_DOCS=OFF -DCLANG_INCLUDE_TESTS=OFF \
    -DCLANG_INSTALL_SCANBUILD=OFF -DCLANG_INSTALL_SCANVIEW=OFF -DCLANG_PLUGIN_SUPPORT=OFF \
    -DLLVM_TARGETS_TO_BUILD=X86 -G "Unix Makefiles" ../llvm && \
		make -j$(nproc) && \
		make install

RUN ln -s /root/llvm-project/build/bin/clang++ /usr/lib/ccache/ && \
    ln -s /root/llvm-project/build/bin/clang /usr/lib/ccache/


ENV LLVM_HOME=/root/llvm-project/build
ENV PATH=$LLVM_HOME/bin:$PATH
ENV LIBRARY_PATH=$LLVM_HOME/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIBRARY_PATH
ENV PATH=/usr/lib/ccache:$PATH
