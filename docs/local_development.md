# Setting Up A Sandbox Environment

## docker-compose
You can use docker-compose to create a local single-node devicedb cloud cluster and connect a local devicedb edge node to it.

1. Make sure you are in the root directory of the devicedb project
   ```bash
   $ cd devicedb
   ```

1. Pick a directory where important devicedb edge certificates and configuration will be placed or create a new one.

1. Run `docker-compose up`
   ```bash
   $ EDGE_CLIENT_RESOURCES=<my-edge-config> docker-compose up
   ```
   Replace `<my-edge-config>` with whatever directory you chose in the last step. Let all the services initialize. It may take a few seconds. Use an absolute path. Wait until you see something like this:
   ```
   devicedb-edge_1        | [2019-08-28T22:41:04.064] [INFO] [] Connected to devicedb cloud
   ```

1. You can find your device's ID in the log. Look for a log message like this
   ```
   devicedb-cloud_1       | [2019-08-28T22:41:04.065] [INFO] [] Accepted peer connection from 96d45e02c9e311e9a8880242ac1c0002
   ```
   This is your device ID, `96d45e02c9e311e9a8880242ac1c0002` in this example.

1. On the same machine run this command, replacing the device ID with your own
   ```bash
   $ devicedb cluster relay_status -relay 96d45e02c9e311e9a8880242ac1c0002
   Relay ID: 96d45e02c9e311e9a8880242ac1c0002
   Connected To: 14432897153250237318
   Ping: 2.532741ms
   ```
   This verifies that the edge node is talking to the cloud

## Running Devicedb Edge Externally
You may want to just run the cloud cluster in docker and run your devicedb edge node elsewhere, such as a rasberry pi or another machine.

1. Stop the `devicedb-edge` service
   ```bash
   $ docker-compose stop devicedb-edge
   ```
   This stops the simulated edge node while keeping all the other services such as devicedb cloud up and running. This will let you run the edge node externally

1. All the configuration you need to run the devicedb edge node elsewhere is contained in `<my-edge-config>`. Copy the files in this directory to your target device.
   ```
   $ ls <my-edge-config>
   client.crt  client.key  devicedb.conf  myCA.pem
   ```

1. Edit `devicedb.conf` on your target device.

   ```yaml
    db: <replace>
    port: 9090
    syncSessionLimit: 2
    syncSessionPeriod: 1000
    syncExplorationPathLimit: 1000
    syncPushBroadcastLimit: 0
    gcInterval: 300000
    gcPurgeAge: 600000
    merkleDepth: 19
    logLevel: info
    cloud:
      noValidate: true
      uri: ws://<replace>:8080/sync
    alerts:
      forwardInterval: 60000
    history:
       purgeOnForward: true
       eventLimit: 1000
       eventFloor: 500
       purgeBatchSize: 1000
       forwardInterval: 60000
       forwardThreshold: 100
       forwardBatchSize: 1000
    tls:
      certificate: ./client.crt
      key: ./client.key
      rootCA: ./myCA.pem
    ```

    You need to replace the db field with the path on your target device where you want the devicedb edge node to store its data.
    Replace the host inside `cloud.uri` with the ip address or hostname of the machine where the cloud node is running.

1. Start up devicedb on your target device
   ```bash
   $ ./devicedb start -conf ~/Desktop/devicedb.conf 
   [2019-08-28T17:56:26.481] [INFO] [] Next row ID = 0
   [2019-08-28T17:56:26.483] [INFO] [] Next row ID = 0
   [2019-08-28T17:56:26.484] [INFO] [] Next row ID = 0
   [2019-08-28T17:56:26.484] [INFO] [] Next row ID = 0
   [2019-08-28T17:56:26.484] [WARNING] [] The cloud.noValidate option is set to true. The cloud server's certificate chain and identity will not be verified. !!! THIS    OPTION SHOULD NOT BE SET TO TRUE IN PRODUCTION !!!
   [2019-08-28T17:56:26.487] [INFO] [] Connected to devicedb cloud
   [2019-08-28T17:56:26.492] [INFO] [] Node 96d45e02c9e311e9a8880242ac1c0002 listening on port 9090
   ```

1. If there is trouble connecting double check the hostname and make sure your edge node can reach the machine running the cloud node.

1. Use myCA.pem as your relay CA for edge-based devicedb clients.

