#!/bin/bash

if [ -z "$1" ]
then
    echo "Usage: compose-cloud-add-device.sh <CONFIG_DIRECTORY>"

    exit 1
fi

DEVICE_ID_FILE=$1/device_id
SITE_ID_FILE=$1/site_id

if [ ! -f "$DEVICE_ID_FILE" ]; then
    echo "Device identity file at $DEVICE_ID_FILE does not exist"

    exit 1
fi

if [ ! -f "$SITE_ID_FILE" ]; then
    echo "Site identity file at $SITE_ID_FILE does not exist"

    exit 1
fi

DEVICE_ID=`cat $DEVICE_ID_FILE`
SITE_ID=`cat $SITE_ID_FILE`

echo "Create site $SITE_ID"

until devicedb cluster add_site -host devicedb-cloud -site $SITE_ID
do
    echo "Unable to create site $SITE_ID. Trying again in 5 seconds"
    sleep 5
done

echo "Create device $DEVICE_ID"

until devicedb cluster add_relay -host devicedb-cloud -relay $DEVICE_ID
do
    echo "Unable to create device $DEVICE_ID. Trying again in 5 seconds"
    sleep 5
done

echo "Move device $DEVICE_ID into site $SITE_ID"

until devicedb cluster move_relay -host devicedb-cloud -relay $DEVICE_ID -site $SITE_ID
do
    echo "Unable to move device $DEVICE_ID into site $SITE_ID. Trying again in 5 seconds"
    sleep 5
done