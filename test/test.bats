# shellcheck shell=bash

export DIR

setup() {
  load 'test_helper/bats-support/load'
  load 'test_helper/bats-assert/load'
  # get the containing directory of this file
  # use $BATS_TEST_FILENAME instead of ${BASH_SOURCE[0]} or $0,
  # as those will point to the bats executable's location or the preprocessed file respectively
  DIR="$( cd "$( dirname -- "$BATS_TEST_FILENAME" )" >/dev/null 2>&1 && pwd )"
}

@test "can build the project" {
  if [[ ! -f bin/sqlq ]] || [[ "$(find src shard.yml -newer bin/sqlq | wc -l)" -gt 0 ]]; then
    shards build --error-trace --progress
  fi
}

@test "gives error with status code 7 without any arguments" {
  run ./bin/sqlq
  assert_output -p "no help available"
  assert_success
}

export -n DIR
