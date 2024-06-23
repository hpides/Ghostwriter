#!/usr/bin/env bash

echo "Stopping broker node"
echo "====================="

kill -9 $(cat /tmp/gw_broker.pid 2> /dev/null) &> /dev/null
