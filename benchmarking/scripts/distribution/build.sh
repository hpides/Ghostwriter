#!/usr/bin/env bash
set -e
set -o pipefail

echo "Start building..."
docker run --rm -v $GHOSTWRITER_HOME/:/ghostwriter delab/env /bin/bash /ghostwriter/benchmarking/scripts/distribution/docker/build.sh
echo "Finished building!"
