#!/bin/bash

whereis_golint=$(whereis golint)
if [ "$whereis_golint" = "golint:" ]
then
    echo "golint command not found, please install it with sudo apt install golint (or similar)."
    exit 2
fi
golint
cd alerts
golint
cd ../bucket
golint
cd ../client_relay
golint
cd ../cluster
golint
cd ../clusterio
golint
cd ../compatibility
golint
cd ../data
golint
cd ../historian
golint
cd ../integration
golint
cd ../merkle
golint
cd ../node
golint
cd ../raft
golint
cd ../routes
golint
cd ../server
golint
cd ../site
golint
cd ../storage
golint
cd ../sync
golint
cd ../transfer
golint
cd ../util
golint
cd ..
