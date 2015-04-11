#!/bin/bash
pushd thirdparty/tachyon
git am $IMPALA_HOME/patches/tachyon/*
popd
