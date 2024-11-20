#!/bin/bash




## declare an array variable
# declare -a arr_servers=("us-east-1-0" "us-east-1-1" "us-east-1-2" "eu-west-1-0")
declare -a arr_servers=("us-east-1-0" "us-east-1-1")
declare -a rep_servers=("us-east-1-1" "us-east-1-2" "eu-west-1-0")
replica_servers=("us-east-1-1" "us-east-1-2" "eu-west-1-0")


primary_server="us-east-1-0"

USER="shir"
# EXP_NAME="pg-smr-wis"
EXP_NAME="pg-smr"
# CLUSTER_NAME="wisc"
CLUSTER_NAME="utah"
PROJECT_NAME="pequin"
BENCHMARK_NAME="tpcc"
primary=false
replica=false

while getopts u:e:b:pr option; do
# while getopts 'urscn:v' flag; do
case "${option}" in
u) USER=${OPTARG};;
e) EXP_NAME=${OPTARG};;
b) BENCHMARK_NAME=${OPTARG};;
p) primary=true ;;
r) replica=true ;;
esac;
done


#Upload script to servers
# parallel "rsync -v -r -e ssh ./scripts/postgres_primary.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 
# parallel "rsync -v -r -e ssh ./scripts/postgres_primary2.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 
# parallel "rsync -v -r -e ssh ./scripts/postgres_replica.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 

# parallel "ssh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us  'sudo /users/${USER}/postgres_replica.sh ;' " ::: ${rep_servers[@]} 

# i=0
# for server_name in "${replica_servers[@]}"; do
#     i=$((i+1))
#     # ip_address=$(getent hosts "$server_name" | awk '{ print $1 }')
#     # echo $ip_address
#     ssh  ${USER}@${server_name}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us  "/users/${USER}/postgres_replica.sh $i"

# done

if [ "$primary" = true ] ; then

    parallel "rsync -v -r -e ssh ./scripts/postgres_primary.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 
    parallel "rsync -v -r -e ssh ./scripts/postgres_primary2.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 

    ssh  ${USER}@${primary_server}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us  "/users/${USER}/postgres_primary.sh"

    # ssh  ${USER}@${primary_server}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us  "/usr/lib/postgresql/12/bin/pg_ctl -D ~/primary/db start"

    # ssh  ${USER}@${primary_server}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us  "/users/${USER}/postgres_primary2.sh"

    exit 1
fi


if [ "$replica" = true ] ; then
    parallel "rsync -v -r -e ssh ./scripts/postgres_replica.sh  ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/" ::: ${arr_servers[@]} 

    # i=0
    # for server_name in "${replica_servers[@]}"; do
    #     i=$((i+1))
    #     # ip_address=$(getent hosts "$server_name" | awk '{ print $1 }')
    #     # echo $ip_address
    #     ssh  ${USER}@${server_name}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us  "/users/${USER}/postgres_replica.sh $i" &

    # done

    exit 1
fi