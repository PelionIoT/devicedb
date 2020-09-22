FROM golang
ADD . /go/src/devicedb
RUN go install devicedb
ENTRYPOINT [ "/bin/sh", "/go/src/devicedb/startup-kube.sh" ]
EXPOSE 8080
EXPOSE 9090