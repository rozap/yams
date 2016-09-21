#!/bin/bash
while inotifywait -r -e modify ./test ./lib ./config; do
  if [ -z "$1" ]
  then
    echo "Running unit tests"
    mix test
  else
    echo "Running tests that match $1"
    mix test $1
  fi
done