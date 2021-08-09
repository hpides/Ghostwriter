#!/usr/bin/env bash

echo "Stopping storage node"
echo "====================="

kill -9 $(cat /tmp/gw_storage.pid 2> /dev/null) &> /dev/null
