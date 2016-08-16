#!/bin/bash

PBC_OPTS="-Dcom.basho.riak.pbchost=localhost:8087"
SBT_CMD="sbt $PBC_OPTS ++$TRAVIS_SCALA_VERSION"

set -e

if [ "$RIAK_FLAVOR" == "riak-kv" ]; then
  $SBT_CMD runRiakKVTests
else
  $SBT_CMD test
fi

if [ "develop" == "$TRAVIS_BRANCH" -o "master" == "$TRAVIS_BRANCH" ] && [ "false" == "$TRAVIS_PULL_REQUEST" ]; then
  openssl aes-256-cbc -K $encrypted_e4898ca93742_key -iv $encrypted_e4898ca93742_iv -in secrets.tgz.enc -out secrets.tgz -d && tar -zxf secrets.tgz
  $SBT_CMD sparkRiakConnector/publishSigned
fi
