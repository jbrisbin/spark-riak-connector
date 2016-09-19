#!/bin/bash

set -ex

BUILD_CONTAINERS=$(docker ps -aqf label=asciibuild.name='Spark Riak Connector')
[[ ! -z $BUILD_CONTAINERS ]] && docker rm -f $BUILD_CONTAINERS

CLUSTER_CONTAINERS=$(docker ps -aqf label=com.basho.riak.cluster.name=riak)
[[ ! -z $CLUSTER_CONTAINERS ]] && docker rm -f $CLUSTER_CONTAINERS

rm -f .RIAK_HOSTS
