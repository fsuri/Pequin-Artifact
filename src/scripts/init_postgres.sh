#!/bin/bash




## declare an array variable
# declare -a arr_servers=("us-east-1-0" "us-east-1-1" "us-east-1-2" "eu-west-1-0")
declare -a arr_servers=("us-east-1-0")

USER="fs435"
EXP_NAME="pequin"
CLUSTER_NAME="utah"
PROJECT_NAME="pequin"
BENCHMARK_NAME="tpcc"

while getopts u:e:b: option; do
case "${option}" in
u) USER=${OPTARG};;
e) EXP_NAME=${OPTARG};;
b) BENCHMARK_NAME=${OPTARG};;
esac;
done


#Upload script to servers
parallel "rsync -v -r -e ssh ./scripts/postgres_service.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 
# parallel "ssh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us  'sudo /users/${USER}/postgres_service.sh -r;' " ::: ${arr_servers[@]} 
# parallel "ssh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us  'sudo /users/${USER}/postgres_service.sh -n 1;' " ::: ${arr_servers[@]} 
