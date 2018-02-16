#!/usr/bin/env bash

DEPS_LOCATION=deps
DESTINATION=librdkafka

if [ -f "$DEPS_LOCATION/$DESTINATION/src/librdkafka.a" ]; then
    echo "librdkafka fork already exist. delete $DEPS_LOCATION/$DESTINATION for a fresh checkout."
    exit 0
fi

REPO=https://github.com/edenhill/librdkafka.git
BRANCH=master
REV=b581d0d9df282847f76e8b9e87337161959d39c9

function fail_check
{
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "error with $1" >&2
        exit 1
    fi
}

function DownloadLib()
{
	echo "repo=$REPO rev=$REV branch=$BRANCH"

	mkdir -p $DEPS_LOCATION
	pushd $DEPS_LOCATION

	if [ ! -d "$DESTINATION" ]; then
	    fail_check git clone -b $BRANCH $REPO $DESTINATION
    fi

	pushd $DESTINATION
	fail_check git checkout $REV
	popd
	popd
}

function BuildLib()
{
	pushd $DEPS_LOCATION
	pushd $DESTINATION

    OS=$(uname -s)

	case $OS in
        Darwin)
            brew install openssl
            OPENSSL_ROOT_DIR=$(brew --prefix openssl)
            export CPPFLAGS=-I$OPENSSL_ROOT_DIR/include/
            export LDFLAGS=-L$OPENSSL_ROOT_DIR/lib
            ;;
    esac

    fail_check ./configure
    fail_check make

    rm src/*.dylib
    rm src/*.so

	popd
	popd
}

DownloadLib
BuildLib
