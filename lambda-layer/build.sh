#!/bin/bash -x

set -e

rm -rf layer && mkdir -p layer/python
docker build -t py37-apsw-builder -f Dockerfile .
CONTAINER=$(docker run -d py37-apsw-builder false)
docker cp \
    $CONTAINER:/var/lang/lib/python3.7/site-packages/apsw-3.30.1.post1-py3.7-linux-x86_64.egg/apsw.cpython-37m-x86_64-linux-gnu.so \
    layer/python/.
docker cp \
    $CONTAINER:/var/lang/lib/python3.7/site-packages/apsw-3.30.1.post1-py3.7-linux-x86_64.egg/apsw.py \
    layer/python/.
docker rm $CONTAINER