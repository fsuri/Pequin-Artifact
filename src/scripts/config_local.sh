#!/bin/bash

TARGET_DIR="/root/Pequin-Artifact/src/scripts/config/"

echo 'Update hotstuff header file local_config_dir.h as'
echo '#define LOCAL_CONFIG_DIR "'$TARGET_DIR'"'

echo '#define LOCAL_CONFIG_DIR "'$TARGET_DIR'"' > ../store/hotstuffstore/libhotstuff/examples/local_config_dir.h

