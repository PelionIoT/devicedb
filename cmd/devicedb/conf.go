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

# The sync session period is the time in milliseconds that determines the rate
# at which sync sessions are initiated with neighboring peers. Adjusting this
# too high will result in unwanted amounts of network traffic and too low may
# result in slow convergence rates for replicas that have not been in contact
# for some time. A rate on the order of seconds is reccomended generally
# **REQUIRED**
syncSessionPeriod: 1000

# In addition to background syncing, updates can also be forwarded directly
# to neighbors when a connection is established in order to reduce the time
# that replicas remain divergent. An update will immediately get forwarded
# to the number of connected peers indicated by this number. If this value is
# zero then the update is forwarded to ALL connected peers. In a small network
# of nodes it may be better to set this to zero.
# **REQUIRED**
syncPushBroadcastLimit: 0

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

# These are the possible log levels in order from lowest to highest level.
# Specifying a particular log level means you will see all messages at that
# level and below. For example, if debug is specified, all log messages will
# be seen. If no level is specified or if the log level specified is not valid
# then the level defaults to the error level
# critical
# error
# warning
# notice
# info
# debug
logLevel: info

# The TLS options specify file paths to PEM encoded SSL certificates and keys
# All connections between database nodes use TLS to identify and authenticate
# each other. A single certificate and key can be used if that certificate has
# the server and client extended key usage options enabled. If seperate
# certificates are used for the client and server certificates then the common
# name on the clint and server certificate must match. The common name of the
# certificate is used to identify this database node with other database nodes
# The rootCA file is the root certificate chain that was used to generate these 
# certificates and is shared between nodes in a cluster. A database client does 
# not need to provide a client certificate when sending a request to a database 
# node but does need to verify the database node's server certificate against 
# the same root certificate chain.
# **REQUIRED**
tls:
    # If using a single certificate for both client and server authentication
    # then it is specified using the certificate and key options as shown below
    # If using seperate client and server certificates then uncomment the options
    # below for clientCertificate, clientKey, serverCertificate, and serverKey
    
    # A PEM encoded certificate with the 'server' and 'client' extendedKeyUsage 
    # options set
    certificate: path/to/cert.pem
    
    # A PEM encoded key corresponding to the specified certificate
    key: path/to/key.pem
    
    # A PEM encoded 'client' type certificate
    # clientCertificate: path/to/clientCert.pem
    
    # A PEM encoded key corresponding to the specified client certificate
    # clientKey: path/to/clientKey.pem
    
    # A PEM encoded 'server' type certificate
    # serverCertificate: path/to/serverCert.pem
    
    # A PEM encoded key corresponding to the specified server certificate
    # serverKey: path/to/serverKey.pem
    
    # A PEM encoded certificate chain that can be used to verify the previous
    # certificates
    rootCA: path/to/ca-chain.pem
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