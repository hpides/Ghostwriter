FROM ubuntu:20.04

RUN apt update && \
    apt upgrade -y && \
	DEBIAN_FRONTEND="noninteractive" TZ="Europe/Berlin" apt install -y \
		aptitude \
		autotools-dev \
		binutils-dev \
		bison \
		build-essential \
		flex \
		g++ \
		gdb \
		git \
		libbz2-dev \
		libdaxctl-dev \
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
		libndctl-dev \
		libnuma-dev \
		libsnappy-dev \
		libssl-dev \
		libtbb-dev \
		libxml2-dev \
		make \
		ndctl \
		numactl \
        pandoc \
		pkg-config \
		python-dev \
		rdma-core \
		rsync \
		ssh \
		wget \
		zlib1g-dev && \
    apt clean

RUN cd && \
    apt remove --purge --auto-remove cmake && \
    version=3.16 &&\
    build=3 && \
    mkdir ~/temp && \
    cd ~/temp && \
    wget https://cmake.org/files/v$version/cmake-$version.$build.tar.gz && \
    tar -xzvf cmake-$version.$build.tar.gz && \
    cd cmake-$version.$build/ && \
    ./bootstrap && \
    make -j$(nproc) && \
    make install && \
    cd && \
    rm -rf ~/temp

RUN cd && \
    apt remove --purge --auto-remove libboost-all-dev && \
    major=1 && \
    minor=68 && \
    build=0 && \
    mkdir ~/temp && \
    cd ~/temp && \
    wget http://sourceforge.net/projects/boost/files/boost/$major.$minor.$build/boost_${major}_${minor}_$build.tar.gz && \
    tar -xvzf boost_${major}_${minor}_$build.tar.gz && \
    cd boost_${major}_${minor}_$build/ && \
    ./bootstrap.sh && \
    ./b2 -j2 cxxstd=17 numa=on && \
    ./b2 install numa=on && \
    cd && \
    rm -rf ~/temp

RUN cd /usr/src/gtest && \
    cmake . && \
		make && \
		cp lib/*.a /usr/lib && \
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

RUN cd && \
    mkdir ~/temp && \
    cd ~/temp && \
    version=2020_U3 && \
    wget https://github.com/oneapi-src/oneTBB/archive/$version.tar.gz && \
    tar -xvzf $version.tar.gz && \
    cd oneTBB-$version && \
    make -j && \
    cd && \
    rm -rf ~/temp

RUN cd && \
    mkdir ~/temp && \
    cd ~/temp && \
    version=1.10 && \
    wget https://github.com/pmem/pmdk/archive/$version.tar.gz && \
    tar -xvzf $version.tar.gz && \
    cd pmdk-$version && \
    make -j && \
    make install && \
    cd && \
    rm -rf ~/temp

RUN cd && \
    mkdir ~/temp && \
    cd ~/temp && \
    version=1.10 && \
    wget https://github.com/pmem/libpmemobj-cpp/archive/$version.tar.gz && \
    tar -xvzf $version.tar.gz && \
    cd libpmemobj-cpp-$version && \
    mkdir build && \
    cd build && \
    cmake .. && \
    make -j 2 && \
    make install && \
    cd && \
    rm -rf ~/temp

RUN cd && \
    mkdir ~/temp && \
    cd ~/temp && \
    version=1.10.0 && \
    build=-rc1 && \
    wget https://github.com/openucx/ucx/releases/download/v$version$build/ucx-$version.tar.gz && \
    tar -xvzf ucx-$version.tar.gz && \
    cd ucx-$version && \
    ./contrib/configure-release && \
    make -j && \
    make install && \
    cd && \
    rm -rf ~/temp

RUN ( \
    echo 'LogLevel DEBUG2'; \
    echo 'PermitRootLogin yes'; \
    echo 'PasswordAuthentication yes'; \
    echo 'Subsystem sftp /usr/lib/openssh/sftp-server'; \
  ) > /etc/ssh/sshd_config_test_clion \
  && mkdir /run/sshd

RUN yes password | passwd root

ENV LLVM_HOME=/root/llvm-project/build
ENV PATH=$LLVM_HOME/bin:$PATH
ENV LIBRARY_PATH=$LLVM_HOME/lib:/usr/lib:/usr/local/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIBRARY_PATH
ENV UCX_BUILD_DIR=/usr

CMD ["/usr/sbin/sshd", "-D", "-e", "-f", "/etc/ssh/sshd_config_test_clion"]
