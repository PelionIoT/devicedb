#
# Copyright (c) 2019 ARM Limited.
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
#



# Expected Variables:
# SEED_NODE_NAME: The name of the seed node
# NODE_NAME: The name of this node
# DATA_STORAGE_PATH
# SNAPSHOT_STORAGE_PATH
# REPLICATION_FACTOR
# PORT
# HOST
# SEED_NODE_ADDRESS
# LOG_LEVEL
if [ $NODE_NAME = $SEED_NODE_NAME ]; then 
    /go/bin/devicedb cluster start -store $DATA_STORAGE_PATH -snapshot_store $SNAPSHOT_STORAGE_PATH -replication_factor $REPLICATION_FACTOR -port $PORT -host $HOST -log_level $LOG_LEVEL;
else
    /go/bin/devicedb cluster start -store $DATA_STORAGE_PATH -snapshot_store $SNAPSHOT_STORAGE_PATH -replication_factor $REPLICATION_FACTOR -port $PORT -host $HOST -log_level $LOG_LEVEL -join $SEED_NODE_ADDRESS;
fi;