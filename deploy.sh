#!/bin/bash

docker build -t steemdata-mongo .
docker tag steemdata-mongo furion/steemdata-mongo
docker push furion/steemdata-mongo
