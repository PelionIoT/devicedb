#!/bin/bash

cd alerts
go vet
cd ../bucket
go vet
cd ../client_relay
go vet
cd ../cluster
go vet
cd ../clusterio
go vet
cd ../compatibility
go vet
cd ../data
go vet
cd ../historian
go vet
cd ../integration
go vet
cd ../merkle
go vet
cd ../node
go vet
cd ../raft
go vet
cd ../routes
go vet
cd ../server
go vet
cd ../site
go vet
cd ../storage
go vet
cd ../sync
go vet
cd ../transfer
go vet
cd ../util
go vet
cd ..