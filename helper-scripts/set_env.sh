#!/bin/bash

echo "Setting env variables"
export LD_PRELOAD=/usr/local/lib/libjemalloc.so
#export LD_PRELOAD=/usr/local/lib/libhoard.so
export LD_LIBRARY_PATH=/usr/lib/jvm/java-1.11.0-openjdk-amd64/lib/server:$LD_LIBRARY_PATH
export ASAN_OPTIONS=new_delete_type_mismatch=0
