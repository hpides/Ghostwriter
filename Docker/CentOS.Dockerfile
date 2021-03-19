FROM centos:7

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


RUN source /opt/rh/devtoolset-9/enable \
 && wget http://sourceforge.net/projects/boost/files/boost/1.68.0/boost_1_68_0.tar.gz \
    && tar -zxvf boost_1_68_0.tar.gz && cd boost_1_68_0 && ln -s $CXX /usr/bin/g++ \
    && ./bootstrap.sh && ./b2 -j2 cxxstd=17 numa=on && ./b2 install && cd ${HOME} && rm -rf boost_1_68_0*


RUN source /opt/rh/devtoolset-9/enable \
    && cd && git clone https://github.com/llvm/llvm-project.git && \
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

RUN source /opt/rh/devtoolset-9/enable \
    && cd && wget https://github.com/oneapi-src/oneTBB/archive/2020_U3.tar.gz && tar zxvf 2020_U3.tar.gz \
    && cd oneTBB-2020_U3 && make -j && cd ${HOME} && rm -rf 2020_U3.tar.gz && cd && \
   git clone https://github.com/google/benchmark.git && cd benchmark \
    && git clone https://github.com/google/googletest.git && mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release \
    && make -j && make install && cd ../googletest && mkdir build && cd build && cmake .. -DCMAKE_BUILD_TYPE=Release \
    && make -j && make install && cd ${HOME}

RUN yum -y install epel-release && yum install -y ndctl-devel daxctl-devel pandoc jemalloc-devel && yum clean all

RUN source /opt/rh/devtoolset-9/enable \
    && cd ${HOME} && wget https://github.com/pmem/pmdk/archive/1.9.tar.gz && tar -xvzf 1.9.tar.gz \
    && rm 1.9.tar.gz && cd pmdk-1.9 && make -j$(nproc) \
    && make install prefix=/opt/rh/devtoolset-9/root && cd ${HOME} && rm -rf pmdk-1.9

RUN source /opt/rh/devtoolset-9/enable \ && export LIBRARY_PATH=${LIBRARY_PATH}:/root/pmdk \
    && cd ${HOME} && wget https://github.com/pmem/libpmemobj-cpp/archive/1.9.tar.gz && tar -xvzf 1.9.tar.gz \
    && rm 1.9.tar.gz && cd libpmemobj-cpp-1.9 && mkdir build && cd build && cmake .. \
    && make -j$(nproc) && make install && cd ${HOME} && rm -rf libpmemobj-cpp-1.9

RUN source /opt/rh/devtoolset-9/enable \
    && cd ${HOME} && git clone https://github.com/openucx/ucx.git && cd ucx \
    && git checkout c0a9704a2 && ./autogen.sh && ./contrib/configure-release --prefix=${PWD}/build-release \
    && make -j$(nproc) && make install && cd ${HOME}

RUN yum -y update && yum install -y openssh-server && yum clean all

RUN yes password | passwd root && echo 'source /opt/rh/devtoolset-9/enable' >> ~/.bashrc

RUN ssh-keygen -t rsa -f /etc/ssh/ssh_host_rsa_key -N ''
RUN ssh-keygen -t dsa -f /etc/ssh/ssh_host_dsa_key -N ''
#ADD src/sshd/sshd_config /etc/ssh/sshd_config

ENV LLVM_HOME=/root/llvm-project/build
ENV PATH=$LLVM_HOME/bin:$PATH
ENV LIBRARY_PATH=$LLVM_HOME/lib:$LIBRARY_PATH
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIBRARY_PATH
ENV TBB_BUILD_DIR=/root/oneTBB-2020_U3/build
ENV TBB_BUILD_PREFIX=linux_intel64_gcc_cc9.3.1_libc2.17_kernel5.4.0
ENV TBB_ROOT=/root/oneTBB-2020_U3
ENV TBBROOT=/root/oneTBB-2020_U3
ENV UCX_BUILD_DIR=/root/ucx/build-release

ENV CC=/opt/rh/devtoolset-9/root/bin/gcc
ENV CXX=/opt/rh/devtoolset-9/root/bin/g++
