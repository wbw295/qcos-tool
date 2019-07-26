#!/usr/bin/env bash

set -eo pipefail

cur_dir=$(cd $(dirname $0); pwd)

cd $cur_dir && \
./gradlew bootJar && \
cp -f build/libs/qcos-tool-1.0.jar docker && \
cd docker && \
docker build -t qcos-tool .