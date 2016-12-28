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
go build devicedb/cmd/devicedb
```
The devicedb binary should now be located at my-go-workspace/bin/devicedb
