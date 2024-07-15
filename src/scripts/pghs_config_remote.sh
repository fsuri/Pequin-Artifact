#!/bin/bash

if [ -z "$1" ]
  then
    echo "No argument supplied. Please add your CloudLab username as argument."
fi

CL_USERNAME=$1

TARGET_DIR="/users/${CL_USERNAME}/config_pghs/"

echo 'Update hotstuff header file remote_config_dir.h as'
echo '#define REMOTE_CONFIG_DIR "'$TARGET_DIR'"'

#echo '#define REMOTE_CONFIG_DIR "'$TARGET_DIR'"' > ../store/hotstuffstore/libhotstuff/examples/remote_config_dir.h



cat ./hosts_pg_smr | while read machine
do
    echo "#### send config to machine ${machine}"
    #scp  -r config ${CL_USERNAME}@${machine}.indicus.morty-pg0.utah.cloudlab.us:/users/${CL_USERNAME}/
    rsync -rtuv config_pghs ${CL_USERNAME}@${machine}.pequin.pequin-pg0.utah.cloudlab.us:/users/${CL_USERNAME}/
done

