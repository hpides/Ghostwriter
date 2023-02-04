mkdir temp && \
cd temp && \
version=1.11.2 && \
wget https://github.com/openucx/ucx/releases/download/v$version/ucx-$version.tar.gz --no-check-certificate && \
tar -xvzf ucx-$version.tar.gz && \
cd ucx-$version && \
./contrib/configure-release --prefix=/scratch/hendrik.makait/usr && \
make -j && \
make install