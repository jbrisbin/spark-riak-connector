#!/bin/bash

SBT_CMD="sbt $PBC_OPTS ++$TRAVIS_SCALA_VERSION"

if [ "$RIAK_FLAVOR" == "riak-kv" ]; then
  $SBT_CMD runRiakKVTests;
else
  $SBT_CMD test;
fi

if [[ "develop" == "$TRAVIS_BRANCH" ] || [ "master" == "$TRAVIS_BRANCH" ]] && [ "false" == "$TRAVIS_PULL_REQUEST" ]; then
  $SBT_CMD sparkRiakConnector/publishSigned;
fi
