#!/bin/bash

set -e

if [ -z "$1" ]
then
    echo "Usage: generate-certs-and-identity.sh <OUTPUT_DIRECTORY>"

    exit 1
fi

OUT_DIR=$1

if [ ! -d "$OUT_DIR" ]; then
    echo "Output directory $OUT_DIR does not exist"

    exit 1
fi

echo "$(cat devicedb.conf.tpl.yaml | envsubst)" > $OUT_DIR/devicedb.conf

if [ -f "$OUT_DIR/ready" ]; then
    echo "Identity already exists. Skipping identity generation"

    exit 0
fi

DEVICE_ID=`uuid | sed 's/-//g'`
SITE_ID=`uuid | sed 's/-//g'`

# Write the identity variables to files
echo $DEVICE_ID > $OUT_DIR/device_id
echo $SITE_ID > $OUT_DIR/site_id

# Generate device certificate CA
openssl genrsa \
    -des3 \
    -out $OUT_DIR/myCA.key \
    -passout pass:password \
    2048

openssl req \
    -x509 \
    -new -nodes \
    -key $OUT_DIR/myCA.key \
    -sha256 \
    -days 1825 \
    -passin pass:password \
    -subj "/C=US/ST=TX/L=City/O=my_o/OU=my_ou/CN=root_ca/emailAddress=email@example.com" \
    -out $OUT_DIR/myCA.pem

# Generate device certificates for devicedb edge node using CA
openssl genrsa \
    -out $OUT_DIR/client.key \
    2048

openssl req \
    -new \
    -key $OUT_DIR/client.key \
    -subj "/C=US/ST=TX/L=City/O=my_o/OU=$SITE_ID/CN=$DEVICE_ID/emailAddress=email@example.com" \
    -passout pass:password \
    -out $OUT_DIR/client.csr

openssl x509 \
    -req \
    -in $OUT_DIR/client.csr \
    -CA $OUT_DIR/myCA.pem \
    -CAkey $OUT_DIR/myCA.key \
    -CAcreateserial \
    -days 1825 \
    -sha256 \
    -extfile client.ext \
    -passin pass:password \
    -out $OUT_DIR/client.crt

# Generate self-signed certificate for devicedb cloud node
openssl req \
    -x509 \
    -newkey rsa:4096 \
    -keyout $OUT_DIR/server.key \
    -out $OUT_DIR/server.crt \
    -subj "/C=US/ST=TX/L=City/O=my_o/OU=my_ou/CN=$CLOUD_HOST/emailAddress=email@example.com" \
    -passout pass:password \
    -days 365

cp $OUT_DIR/myCA.pem $EDGE_CLIENT_RESOURCES/myCA.pem
cp $OUT_DIR/client.crt $EDGE_CLIENT_RESOURCES/client.crt
cp $OUT_DIR/client.key $EDGE_CLIENT_RESOURCES/client.key
cp $OUT_DIR/devicedb.conf $EDGE_CLIENT_RESOURCES/devicedb.conf

touch $OUT_DIR/ready