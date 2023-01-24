#!/bin/bash
#
# Copyright (c) 2019 ARM Limited.
# Copyright (c) 2023 Izuma Networks
#
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


if [ -z "$1" ]
then
    echo "Usage: compose-cloud-add-device.sh <CONFIG_DIRECTORY>"

    exit 1
fi

DEVICE_ID_FILE="$1/device_id"
SITE_ID_FILE="$1/site_id"

if [ ! -f "$DEVICE_ID_FILE" ]; then
    echo "Device identity file at $DEVICE_ID_FILE does not exist"

    exit 1
fi

if [ ! -f "$SITE_ID_FILE" ]; then
    echo "Site identity file at $SITE_ID_FILE does not exist"

    exit 1
fi

DEVICE_ID=$(cat "$DEVICE_ID_FILE")
SITE_ID=$(cat "$SITE_ID_FILE")

echo "Create site $SITE_ID"

until devicedb cluster add_site -host "$CLOUD_HOST" -site "$SITE_ID"
do
    echo "Unable to create site $SITE_ID. Trying again in 5 seconds"
    sleep 5
done

echo "Create device $DEVICE_ID"

until devicedb cluster add_relay -host "$CLOUD_HOST" -relay "$DEVICE_ID"
do
    echo "Unable to create device $DEVICE_ID. Trying again in 5 seconds"
    sleep 5
done

echo "Move device $DEVICE_ID into site $SITE_ID"

until devicedb cluster move_relay -host "$CLOUD_HOST" -relay "$DEVICE_ID" -site "$SITE_ID"
do
    echo "Unable to move device $DEVICE_ID into site $SITE_ID. Trying again in 5 seconds"
    sleep 5
done