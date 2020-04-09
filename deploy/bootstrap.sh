#!/bin/bash

PROJECT_DIR="$(cd "$(dirname "$0")/../"; pwd -P)"

function list_all_go_dirs {
  go list -f '{{.Dir}}' "${PROJECT_DIR}/..."
}

# This script is used by Makefile to run commands.

case $1 in
lint)
  echo "Running lint..." >&2
  ${GOPATH}/bin/golangci-lint run $(list_all_go_dirs)
  exit $?
  ;;

fmt)
  err_count=0
  echo "Running style check..." >&2
  case $2 in
  --fix)
    gofmt -w ${PROJECT_DIR}
    ;;
  *)
    out=$(gofmt -l -e ${PROJECT_DIR})

    if [[ -n ${out} ]]; then
      echo ${out} >&2
      exit 1
    fi
    ;;
  esac
  ;;

spell)
  echo "Running spell check..." >&2
  case $2 in
  --fix)
    ${GOPATH}/bin/misspell -w -locale=US ${PROJECT_DIR}
    ;;
  *)
    ${GOPATH}/bin/misspell -error -locale=US ${PROJECT_DIR}
    ;;
  esac
  ;;

test)
  echo "Running tests..." >&2
  SECONDS=0
  errs=$(go test -v -p 1 -parallel 4 -count 1 -timeout 1m "${PROJECT_DIR}/..." 2>&1 | tee -a /dev/stderr | grep -ae "^FAIL\|^--- FAIL")
  err_count=$(echo "${errs}" | wc -l)
  echo "Tests took: $((SECONDS/3600))h$(((SECONDS%3600)/60))m$((SECONDS%60))s"
  if [[ -n ${errs} ]]; then
    echo "${errs}" >&2
    echo "test: ${err_count} failed" >&2
    exit 1
  fi
  exit 0
  ;;

*)
  echo "unsupported argument $1"
  exit 1
  ;;
esac
