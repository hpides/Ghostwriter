# Configure scratch dir
export SCRATCH_ROOT=/scratch/hendrik.makait && \
export TMP=$SCRATCH_ROOT/tmp && \
export USR=$SCRATCH_ROOT/usr
# Install boost==1.68.0
major=1 && \
minor=68 && \
build=0 && \
mkdir $TMP && \
cd $TMP && \
wget http://sourceforge.net/projects/boost/files/boost/$major.$minor.$build/boost_${major}_${minor}_$build.tar.gz --no-check-certificate && \
tar -xvzf boost_${major}_${minor}_$build.tar.gz && \
cd boost_${major}_${minor}_$build/ && \
./bootstrap.sh --prefix=$USR --without-libraries=python && \
./b2 -j$(nproc) cxxstd=17 numa=on && \
./b2 --prefix=$USR install numa=on && \
cd ~ && \
rm -rf $TMP && \
# Install jemalloc
mkdir $TMP && \
cd $TMP && \
version=5.3.0 && \
wget https://github.com/jemalloc/jemalloc/archive/refs/tags/$version.tar.gz --no-check-certificate && \
tar -xvzf $version.tar.gz && \
cd jemalloc-$version && \
./autogen.sh --prefix=$USR && \
make -j && \
make install && \
cd ~ && \
rm -rf $TMP && \

mkdir $TMP && \
cd $TMP && \
version=1.11.2 && \
wget https://github.com/openucx/ucx/releases/download/v$version/ucx-$version.tar.gz --no-check-certificate && \
tar -xvzf ucx-$version.tar.gz && \
cd ucx-$version && \
./contrib/configure-release --prefix=$USR && \
make -j && \
make install && \
cd ~ && \
# rm -rf $TMP

cd $SCRATCH_ROOT && \
git clone https://github.com/llvm/llvm-project.git && \
cd llvm-project && \
git checkout e3a94ba4a92 && \
mkdir build && \
cd build && \
cmake -DLLVM_ENABLE_PROJECTS=clang -DCMAKE_BUILD_TYPE=Release \
-DCMAKE_INSTALL_PREFIX=$USR \
-DBUILD_SHARED_LIBS=ON -DCLANG_INCLUDE_DOCS=OFF -DCLANG_INCLUDE_TESTS=OFF \
-DCLANG_INSTALL_SCANBUILD=OFF -DCLANG_INSTALL_SCANVIEW=OFF -DCLANG_PLUGIN_SUPPORT=OFF \
-DLLVM_TARGETS_TO_BUILD=X86 -G "Unix Makefiles" ../llvm && \
make -j$(nproc) && \
make install

cd $TMP && \
version=2020_U3 && \
wget https://github.com/oneapi-src/oneTBB/archive/$version.tar.gz --no-check-certificate && \
tar -xvzf $version.tar.gz && \
cd oneTBB-$version && \
make -j