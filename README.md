# devicedb
## Build Instructions
### Create Your Go Workspace
If you don't already have a go workspace on your system, create a directory for your workspace:
```
mkdir my-go-workspace
mkdir my-go-workspace/bin
mkdir my-go-workspace/pkg
mkdir my-go-workspace/src
```
Now navigate to my-go-workspace/src and run the following command
```
git clone git@github.com:WigWagCo/devicedb.git
```
Wait for the project to be cloned into the src directory then do the following to build devicedb
```
export GOPATH=`pwd`/..
```
This should set your GOPATH environment variable to the full file path to your my-go-workspace directory
Now build devicedb like so
```
go install devicedb/cmd/devicedb
```
The devicedb binary should now be located at my-go-workspace/bin/devicedb

# Running With Docker
sudo docker run -d --name=devicedb0 --hostname=devicedb0 -p 8080:8080 -p 9090:9090 -v "/home/jrife/Desktop/devicedb0:/devicedb/data" -v "/home/jrife/Desktop/certs:/devicedb/certs" devicedb -relay_host devicedb0 -host devicedb0

sudo docker run -d --name=devicedb1 --hostname=devicedb1 -p 8181:8080 -p 9191:9090 -v "/home/jrife/Desktop/devicedb1:/devicedb/data" -v "/home/jrife/Desktop/certs:/devicedb/certs" devicedb -relay_host devicedb1 -host devicedb1 -join devicedb0
