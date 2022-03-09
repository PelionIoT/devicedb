FROM golang:1.15
ADD . /go/src/github.com/armPelionEdge/devicedb
RUN cd src/github.com/armPelionEdge/devicedb && go install

# Use the ubuntu base image to install dependencies required by scripts inside of hack/
# Copy over binary built in first stage
FROM ubuntu:16.04
COPY --from=0 /go/bin/devicedb /usr/local/bin/devicedb
COPY --from=0 /go/src/github.com/armPelionEdge/devicedb/hack/client.ext /root/client.ext
COPY --from=0 /go/src/github.com/armPelionEdge/devicedb/hack/devicedb.conf.tpl.yaml /root/devicedb.conf.tpl.yaml
COPY --from=0 /go/src/github.com/armPelionEdge/devicedb/hack/*.sh /usr/local/bin/

# Install script dependencies
RUN apt update
RUN apt install -y openssl uuid gettext
WORKDIR /root
