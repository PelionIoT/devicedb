# DeviceDB
DeviceDB is a key value store that runs both on IoT gateways and in the cloud to allow configuration and state data to be easily shared between applictations running in the cloud and applications running on the gateway.

## Data Model
DeviceDB is intended to run in an environment where many IoT gateways, referred to as relays henceforth, exist and are separated into distinct sites, where a handful of relays belong to each site. In most contexts a site represents some physical location such as a building/room/facility. Each relay at a site contains a copy of the *site database*. Furthermore, the DeviceDB cloud cluster also contains a replica of each site database providing quick access to site related data for applications running in the cloud, a common node whereby updates to a site database can be propogated to replicas on other relays in the site, and a natural backup for any configuration data in case relays in the site need to be replaced. Each site database further breaks down data into predefined *buckets*. A bucket is a namespace for keys that has its own replication and conflict resolution settings.

### Consistency
In its current iteration DeviceDB always tries to maximize availability at the cost of data consistency. An update submitted to some node (whether that node is a relay or the cloud) is considered successful as long as that node successfully processes the update. Reads submitted to a particular node are considered successful as long as that one node successfully processes the read. For readers that are familiar with Cassandra consistency levels this is similar to setting the consistency levels for reads and writes both to ONE. Future versions may improve on this, allowing tunable consistency controls, but for most current applications these settings are acceptable.

### Conflicts
Conflicts in keys can occur if updates are made to the same key in parallel. Different buckets can provide different conflict resolution strategies depending on the use case. Last writer wins uses the timestamp attached to an update to determine which version of a key should be kept. The default conflict resolution strategy is to keep conflicting versions and allow the client to decide which version to keep. Conflicts are detected using logical clocks attached to each key version.

### Buckets
DeviceDB has four predefined buckets for data each with a different combination of conflict resolution and replication settings. The replication settings determine which nodes can update keys in that bucket and which nodes can read keys in that bucket.

Bucket Name | Conflict Resolution Strategy | Writes | Reads
----------- | ---------------------------- | ------ | -----
default     | Allow Multiple               | Any    | Any
lww         | Last writer wins             | Any    | Any
cloud       | Allow Multiple               | Cloud  | Any
local       | Allow Multiple               | Local  | Local

*default and lww can be updated by any node and are replicated to every node*

*cloud can only be updated by the cloud node and is replicated from the cloud down to relays only*

*local is not replicated beyond the local node. Each node contains a local bucket that only it can update and read from*

# Getting Started

## Pre-requisites

* `docker-compose` - Version `1.22.0` or higher
* `docker`- Version `1.13.0` or higher
* `go`
* `uuid` (apt-get)

## Usage

The DeviceDB binary can be used for 3 things:
* Hosting a DeviceDB edge server (Runs paired with maestro, keeps a local copy of the data on the gateway)
* Hosting a DeviceDB cloud server (Runs "in the cloud", syncs all available gateway information)
* Querying a DeviceDB edge server

## Use Cases

Below are two use cases while developing DeviceDB
1. [Running DeviceDB Cloud and DeviceDB Edge on the same workstation](#running-devicedb-cloud-and-edge-on-the-same-workstation). This is useful for development flow.
1. [Running DeviceDB Cloud and DeviceDB Edge on different workstations](#running-devicedb-cloud-and-edge-on-different-workstations). This is useful for production flows.

### Running DeviceDB Cloud and Edge on the same workstation

This use case will spin up a DeviceDB Edge docker container and a DeviceDB Cloud docker container on the same workstation. This is useful for when you are debugging DeviceDB and want to monitor both the gateway side of things, as well as the synced data in the cloud.

1. Clone DeviceDB and go to the directory you cloned to:
    ```bash
    git clone git@github.com:armPelionEdge/devicedb.git
    cd devicedb
    ```

1. Create a configuration folder:
   ```bash
   mkdir <my-edge-config>
   ```

1. Run `docker-compose up`
   ```bash
   EDGE_CLIENT_RESOURCES=<my-edge-config> docker-compose up
   ```
   Replace `<my-edge-config>` with the absolute path of the directory you created in step 2. Let all the services initialize - it may take a few seconds
   ```bash
   devicedb-edge_1        | [2019-08-28T22:41:04.064] [INFO] [] Connected to devicedb cloud
   ```

1. You can find your device's ID in the log. Look for a log message like this
   ```bash
   devicedb-cloud_1       | [2019-08-28T22:41:04.065] [INFO] [] Accepted peer connection from 96d45e02c9e311e9a8880242ac1c0002
   ```
   This is your device ID, `96d45e02c9e311e9a8880242ac1c0002` in this example.

1. In another terminal, verify that the edge node is talking to the cloud by running the following command:
   ```bash
   $ EDGE_CLIENT_RESOURCES=<my-edge-config> docker-compose exec devicedb-cloud devicedb cluster relay_status -relay <device-id>
   Relay ID: 96d45e02c9e311e9a8880242ac1c0002
   Connected To: 14432897153250237318
   Ping: 2.532741ms
   ```
   Where `<my-edge-config>` is the absolute path of the directory you created in step 2, and `<device-id>` is the device ID from step 4


### Running DeviceDB Cloud and Edge on different workstations

This use case is for when you would like to run DeviceDB edge alongside of maestro, and have a separate instance of DeviceDB that may not live on the gateway.

#### Cloud Setup

Run the following steps to install DeviceDB on your cloud server:

1. Clone DeviceDB and go to the directory you cloned to:
    ```bash
    go get github.com/armPelionEdge/devicedb
    cd $GOPATH/src/github.com/armPelionEdge/devicedb
    ```
    Note: if you have a previous manually cloned devicedb (the folder would be located at `$GOPATH/src/github.com/armPelionEdge/devicedb`), you will need to delete the devicedb folder before running the go get command. 

1. Generate certificates and identity files:
    ```bash
    cd hack
    mkdir -p certs
    ./generate-certs-and-identity.sh certs
    ```
    Note: The `certs` folder that you created is very important. This contains certificates that are needed by your gateways. The contents of this folder will need to be deployed to every gateway that you create that needs to be connected to the cloud.

1. Create storage space:
    ```bash
    mkdir -p /var/lib/devicedb/data
    mkdir -p /var/lib/devicedb/snapshots
    ```
    Needed for DeviceDB to dump snapshots and other storage

1. Start DeviceDB:
    ```bash
    devicedb cluster start -store /var/lib/devicedb/data -snapshot_store /var/lib/devicedb/snapshots -replication_factor "1" -host "0.0.0.0"
    ```

Step 4 can be placed within a systemd service to guarentee DeviceDB runs on boot and restarts on failures.

#### Edge Setup

Run the following steps to install DeviceDB on your gateway:

1. Clone DeviceDB and go to the directory you cloned to:
    ```bash
    go get github.com/armPelionEdge/devicedb
    cd $GOPATH/src/github.com/armPelionEdge/devicedb
    ```
    Note: if you have a previous manually cloned devicedb (the folder would be located at `$GOPATH/src/github.com/armPelionEdge/devicedb`), you will need to delete the devicedb folder before running the go get command. 

1. Copy the `certs` folder that was created during [Cloud Setup](#cloud-setup) to the gateway. It is recommended that the `certs` folder ends up in the following folder to match the Cloud server: `$GOPATH/src/github.com/armPelionEdge/devicedb/hack/certs`. For future commands, we will consider `<certs>` as the folder location containing your cloud certificates.

1. Setup environment variables:
    ```bash
    export CLOUD_HOST=arm.com
    ```
    Replace `arm.com` with the location of your DeviceDB cloud instance

1. Create UUIDs for the device and site ID:
    ```bash
    uuid | sed 's/-//g' > <certs>/device_id
    uuid | sed 's/-//g' > <certs>/site_id
    ```
    Where the `<certs>` folder is the folder you copied from [Cloud Setup](#cloud-setup)

1. Sync the gateway's ID with the cloud server:
    ```bash
    cd hack
    ./compose-cloud-add-device.sh <certs>
    ```
    Where the `<certs>` folder is the folder you copied from [Cloud Setup](#cloud-setup)

1. Start DeviceDB:
    ```bash
    devicedb start -conf <certs>/devicedb.conf
    ```
    Where the `<certs>` folder is the folder you copied from [Cloud Setup](#cloud-setup)

Step 6 can be placed within a systemd service to guarentee DeviceDB runs on boot and restarts on failures.

You now should have a DeviceDB Edge connecting to a DeviceDB Cloud.

### Disaster Recovery

If you would like to see how to perform disaster recovery with DeviceDB, see: [Disaster Recovery](/docs/disaster_recovery.md)

### Cluster Management

If you would like to see how to manage cluster information with DeviceDB, see: [Cluster Management](/docs/scaling.md)