FROM golang
ADD . /go/src/devicedb
RUN go install devicedb
EXPOSE 8080
EXPOSE 9090