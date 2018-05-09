**Backup**
$ devicedb cluster snapshot
$ devicedb cluster download_snapshot

**Restore**
$ devicedb cluster start -store /var/devicedb/data -host devicedb-0.devicedb
$ devicedb cluster start -store /var/devicedb/data -host devicedb-1.devicedb -join "devicedb-0.devicedb:8080"
$ devicedb cluster start -store /var/devicedb/data -host devicedb-2.devicedb -join "devicedb-1.devicedb:8080"