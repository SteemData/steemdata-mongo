#!/bin/bash

docker build -t mongosteem .
docker tag mongosteem furion/mongosteem
docker push furion/mongosteem
