package main

import (
    "fmt"
)

var templateConfig string =
`# The db field specifies the directory where the database files reside on
# disk. If it doesn't exist it will be created.
# **REQUIRED**
db: /tmp/devicedb

# The port field specifies the port number on which to run the database server
port: 9090

# The sync session limit is the number of sync sessions that can happen
# concurrently. Adjusting this field allows the database node to synchronize
# with its peers more or less quickly. It is reccomended that this field be
# half the number of cores on the machine where the devicedb node is running
# **REQUIRED**
syncSessionLimit: 2

# The merkle depth adjusts how efficiently the sync process resolves
# differences between database nodes. A rule of thumb is to set this as high
# as memory constraints allow. Estimated memory overhead for a given depth is
# calculated with the formula: M = 3*(2^(d + 4)). The following table gives a
# quick reference to choose an appropriate depth.
#
# depth   |   memory overhead
# 2       |   192         bytes  (0.1      KiB)
# 4       |   768         bytes  (0.8      KiB)
# 6       |   3072        bytes  (3.0      KiB)
# 8       |   12288       bytes  (12       KiB)
# 10      |   49152       bytes  (48       KiB)
# 12      |   196608      bytes  (192      KiB) (0.2   MiB)
# 14      |   786432      bytes  (768      KiB) (0.7   MiB)
# 16      |   3145728     bytes  (3072     KiB) (3.0   MiB)
# 18      |   12582912    bytes  (12288    KiB) (12    MiB)
# 20      |   50331648    bytes  (49152    KiB) (48    MiB)
# 22      |   201326592   bytes  (196608   KiB) (192   MiB) (0.2 GiB)
# 24      |   805306368   bytes  (786432   KiB) (768   MiB) (0.8 GiB)
# 26      |   3221225472  bytes  (3145728  KiB) (3072  MiB) (3   GiB)
# 28      |   12884901888 bytes  (12582912 KiB) (12288 MiB) (12  GiB)
#
# A larger merkle depth also allows more concurrency when processing many
# concurrent updates
# **REQUIRED**
merkleDepth: 19

# The peer list specifies a list of other database nodes that are in the same
# cluster as this node. This database node will contiually try to connect to
# and sync with the nodes in this list. Alternatively peers can be added at
# runtime if an authorized client requests that the node connect to another 
# node.
# **REQUIRED**
peers:
# Uncomment these next lines if there are other peers in the cluster to connect
# to and edit accordingly
#    - id: WWRL000001
#      host: 127.0.0.1
#      port: 9191
#    - id: WWRL000002
#      host: 127.0.0.1
#      port: 9292

# The TLS options specify file paths to PEM encoded SSL certificates and keys
# All connections between database nodes use TLS to identify and authenticate
# each other. The common name on the clint and server certificate must match
# and is used as the identity of this database node. The rootCA file is the 
# root certificate chain that was used to generate these certificates and is
# shared between nodes in a cluster. A database client does not need to 
# provide a client certificate when sending a request to a database node but
# does need to verify the database node's server certificate against the same
# root certificate chain.
# **REQUIRED**
tls:
    # A PEM encoded 'client' type certificate
    # **REQUIRED**
    clientCertificate: ../test_certs/WWRL000000.client.cert.pem
    
    # A PEM encoded key corresponding to the specified client certificate
    # **REQUIRED**
    clientKey: ../test_certs/WWRL000000.client.key.pem
    
    # A PEM encoded 'server' type certificate
    # **REQUIRED**
    serverCertificate: ../test_certs/WWRL000000.server.cert.pem
    
    # A PEM encoded key corresponding to the specified server certificate
    # **REQUIRED**
    serverKey: ../test_certs/WWRL000000.server.key.pem
    
    # A PEM encoded certificate chain that can be used to verify the previous
    # certificates
    # **REQUIRED**
    rootCA: ../test_certs/ca-chain.cert.pem
`

func init() {
    registerCommand("conf", generateConfig, confUsage)
}

var confUsage string = 
`Usage: devicedb conf > path/to/output.yaml
`

func generateConfig() {
    fmt.Print(templateConfig)
}