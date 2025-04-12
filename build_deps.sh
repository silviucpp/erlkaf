#!/usr/bin/env bash

ROOT=$(pwd)
DEPS_LOCATION=_build/deps
OS=$(uname -s)
KERNEL=$(echo $(lsb_release -ds 2>/dev/null || cat /etc/*release 2>/dev/null | head -n1 | awk '{print $1;}') | awk '{print $1;}')
CPUS=`getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu`

# Enable ccache based on $ERLKAF_USE_CCACHE environment variable

CCACHE_ENABLED=0
if [ "$ERLKAF_USE_CCACHE" = "1" ]; then
    if command -v ccache &> /dev/null; then
        echo "ccache is enabled and will be used."
        export PATH="/usr/local/bin/ccache:$PATH"
        export CC="ccache gcc"
        export CXX="ccache g++"
        CCACHE_ENABLED=1
    else
        echo "ccache is not installed. Proceeding without ccache..."
    fi
fi

# https://github.com/confluentinc/librdkafka.git

LIBRDKAFKA_DESTINATION=librdkafka
LIBRDKAFKA_REPO=https://github.com/confluentinc/librdkafka.git
LIBRDKAFKA_BRANCH=master
LIBRDKAFKA_REV=9b72ca3aa6c49f8f57eea02f70aadb1453d3ba1f
LIBRDKAFKA_SUCCESS=src/librdkafka.a

# https://github.com/cameron314/concurrentqueue.git

CQ_DESTINATION=concurrentqueue
CQ_REPO=https://github.com/cameron314/concurrentqueue.git
CQ_BRANCH=master
CQ_REV=cff1001582db67e357d5f77c31e73566003311f2
CQ_SUCCESS=concurrentqueue.h

fail_check()
{
    "$@"
    local status=$?
    if [ $status -ne 0 ]; then
        echo "error with $1" >&2
        exit 1
    fi
}

CheckoutLib()
{
    if [ -f "$DEPS_LOCATION/$4/$5" ]; then
        echo "$4 fork already exists. Delete $DEPS_LOCATION/$4 for a fresh checkout ..."
    else
        #repo rev branch destination

        echo "repo=$1 rev=$2 branch=$3"

        mkdir -p $DEPS_LOCATION
        pushd $DEPS_LOCATION

        if [ ! -d "$4" ]; then
            fail_check git clone -b $3 $1 $4
        fi

        pushd $4
        fail_check git checkout $2
        BuildLibrary $4
        popd
        popd
    fi
}

BuildLibrary()
{
    unset CFLAGS
    unset CXXFLAGS

    case $1 in
        $LIBRDKAFKA_DESTINATION)
            case $OS in
                Darwin)
                    export HOMEBREW_NO_INSTALL_UPGRADE=true
                    export HOMEBREW_NO_INSTALL_CLEANUP=true
                    export HOMEBREW_NO_AUTO_UPDATE=1
                    brew install openssl@1.1 lz4 zstd curl
                    OPENSSL_ROOT_DIR=$(brew --prefix openssl@1.1)
                    export CPPFLAGS=-I$OPENSSL_ROOT_DIR/include/
                    export LDFLAGS=-L$OPENSSL_ROOT_DIR/lib
                    ;;
            esac

            fail_check ./configure
            fail_check make -j $CPUS

            rm src/*.dylib
            rm src/*.so
            ;;
        *)
            ;;
    esac
}

CheckoutLib $LIBRDKAFKA_REPO $LIBRDKAFKA_REV $LIBRDKAFKA_BRANCH $LIBRDKAFKA_DESTINATION $LIBRDKAFKA_SUCCESS
CheckoutLib $CQ_REPO $CQ_REV $CQ_BRANCH $CQ_DESTINATION $CQ_SUCCESS
