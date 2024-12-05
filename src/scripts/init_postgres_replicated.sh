#!/bin/bash


declare -a arr_servers=("us-east-1-0" "us-east-1-1")

USER="shir"
# EXP_NAME="pg-smr-wis"
EXP_NAME="pg-smr"
# CLUSTER_NAME="wisc"
CLUSTER_NAME="utah"
PROJECT_NAME="pequin"

while getopts u:e:b:pr option; do
# while getopts 'urscn:v' flag; do
case "${option}" in
u) USER=${OPTARG};;
e) EXP_NAME=${OPTARG};;
esac;
done


# primary scripts
parallel "rsync -v -r -e ssh ./scripts/postgres_primary.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 
parallel "rsync -v -r -e ssh ./scripts/postgres_primary2.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 
parallel "rsync -v -r -e ssh ./scripts/primary_aux.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 


# replica scripts
parallel "rsync -v -r -e ssh ./scripts/postgres_replica.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 


