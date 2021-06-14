#!/bin/bash

echo "Start packing..."

cd ../docker-build
tar -cvzf binaries.tar.gz benchmark_broker_node benchmark_storage_node ysb_ghostwriter_consumer ysb_ghostwriter_producer simple_producer simple_consumer lib/libhdr_histogram.so.5.0.0 lib/liboperatorJITLib.so yahoo_benchmark local_ysb_runner lightsaber_data_generator

echo "Start shipping..."

scp binaries.tar.gz hendrik.makait@nvram02.delab.i.hpi.de:/hpi/fs00/home/hendrik.makait/ghostwriter/

echo "All done..."

