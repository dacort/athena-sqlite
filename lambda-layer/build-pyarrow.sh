docker build -t py37-pyarrow-builder -f Dockerfile.pyarrow .
CONTAINER=$(docker run -d py37-pyarrow-builder false)
docker cp \
    $CONTAINER:/var/task/pyarrow_lite.zip \
    layer/.
pushd layer
unzip pyarrow_lite.zip
rm pyarrow_lite.zip
popd
docker rm $CONTAINER