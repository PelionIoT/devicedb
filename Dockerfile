FROM golang
ADD . /go/src/devicedb
RUN go install devicedb
ENTRYPOINT [ "/go/bin/devicedb", "cluster", "start", "-port", "8080", "-relay_port", "9090", "-store", "/devicedb/data", "-cert", "/devicedb/certs/cert.pem", "-key", "/devicedb/certs/key.pem", "-relay_ca", "/devicedb/certs/relay_ca.pem" ]
EXPOSE 8080
EXPOSE 9090