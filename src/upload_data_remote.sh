#!/bin/bash


#usage() { echo "Usage: $0 [-user <cloudlab-username>] [-bench <benchmark_name>]" 1>&2; exit 1; }
##use this if I want to upload things onto the machines..
##scp  -r file fs435@${machine}.indicus.morty-pg0.utah.cloudlab.us:/users/fs435/
## ssh ... sudo mv .. ...



## declare an array variable
declare -a arr_servers=("us-east-1-0" "us-east-1-1" "us-east-1-2" "eu-west-1-0" "eu-west-1-1" "eu-west-1-2")
#declare -a arr_servers=("us-east-1-0")

#declare -a arr_clients=("client-0-0" "client-0-1" "client-0-2" "client-0-3" "client-0-4" "client-0-5") ##Use this for postgres
declare -a arr_clients=("client-0-0" "client-1-0" "client-2-0" "client-3-0" "client-4-0" "client-5-0") ##Use this otherwise
#declare -a arr_clients=("client-0-0")

FIRST_TIME_CONNECTION=0

USER="fs435"
EXP_NAME="pequin"
PROJECT_NAME="pequin"
CLUSTER_NAME="utah"
BENCHMARK_NAME="tpcc"
NUM_SHARDS=1
PG_MODE=0


while getopts u:e:b:s:f:c: option; do
case "${option}" in
u) USER=${OPTARG};;
e) EXP_NAME=${OPTARG};;
b) BENCHMARK_NAME=${OPTARG};;
s) NUM_SHARDS=${OPTARG};;
f) FIRST_TIME_CONNECTION=${OPTARG};;
c) CLUSTER_NAME=${OPTARG};;
esac;
done

if [ $PG_MODE = 1 ]; then
  arr_clients=("client-0-0" "client-0-1" "client-0-2" "client-0-3" "client-0-4" "client-0-5") ##Use this for postgres
fi

echo "If you have never connected to the target machines before, then run with -f 1"
echo "NUM_SHARDS:" $NUM_SHARDS

if [ $NUM_SHARDS = 2 ]; then
   arr_servers=("us-east-1-0" "us-east-1-1" "us-east-1-2" "eu-west-1-0" "eu-west-1-1" "eu-west-1-2" "ap-northeast-1-0" "ap-northeast-1-1" "ap-northeast-1-2" "us-west-1-0" "us-west-1-1" "us-west-1-2")
   arr_clients=("client-0-0" "client-1-0" "client-2-0" "client-3-0" "client-4-0" "client-5-0" "client-6-0" "client-7-0" "client-8-0" "client-9-0" "client-10-0" "client-11-0")
fi
if [ $NUM_SHARDS = 3 ]; then
   arr_servers=("us-east-1-0" "us-east-1-1" "us-east-1-2" "eu-west-1-0" "eu-west-1-1" "eu-west-1-2" "ap-northeast-1-0" "ap-northeast-1-1" "ap-northeast-1-2" "us-west-1-0" "us-west-1-1" "us-west-1-2" "eu-central-1-0" "eu-central-1-1" "eu-central-1-2" "ap-southeast-2-0" "ap-southeast-2-1" "ap-southeast-2-2")
   arr_clients=("client-0-0" "client-1-0" "client-2-0" "client-3-0" "client-4-0" "client-5-0" "client-6-0" "client-7-0" "client-8-0" "client-9-0" "client-10-0" "client-11-0" "client-12-0" "client-13-0" "client-14-0" "client-15-0" "client-16-0" "client-17-0")
fi

if [ $FIRST_TIME_CONNECTION = 1 ]; then
	#connect to all servers once to establish auth.
	for host in "${arr_clients[@]}"
	do
	   echo "connecting to host: $host"
	   ssh ${USER}@$host.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us "echo"
	done

	for host in "${arr_servers[@]}"
	do
	   echo "connecting to host: $host"
	   ssh ${USER}@$host.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us "echo"
	done
fi

#Upload schemas to clients //TODO: make sure generator has been run locally so this file exists.
parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-tables-schema.json ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_clients[@]} 

#Upload schemas to servers
parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-tables-schema.json ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_servers[@]} 

#Upload data to servers
parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-data ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_servers[@]} 


if [ "$BENCHMARK_NAME" = "tpcc" ]; then
	#no profile info.
	echo ""
fi

if [ "$BENCHMARK_NAME" = "seats" ]; then
	#upload profile info to clients
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-data/cached_flights.csv ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/benchmark_data/sql-${BENCHMARK_NAME}-data/" ::: ${arr_clients[@]} 
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-data/airport_ids.csv ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/benchmark_data/sql-${BENCHMARK_NAME}-data/" ::: ${arr_clients[@]} 
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-data/airport_flights.csv ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/benchmark_data/sql-${BENCHMARK_NAME}-data/" ::: ${arr_clients[@]} 
		
fi
 
if [ "$BENCHMARK_NAME" = "auctionmark" ]; then
	#upload profile info to clients
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/auctionmark_profile ${USER}@{}.${EXP_NAME}.${PROJECT_NAME}-pg0.${CLUSTER_NAME}.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_clients[@]} 
fi


#done
