#!/bin/bash


#usage() { echo "Usage: $0 [-user <cloudlab-username>] [-bench <benchmark_name>]" 1>&2; exit 1; }
##use this if I want to upload things onto the machines..
##scp  -r file fs435@${machine}.indicus.morty-pg0.utah.cloudlab.us:/users/fs435/
## ssh ... sudo mv .. ...



## declare an array variable
declare -a arr_servers=("us-east-1-0" "us-east-1-1" "us-east-1-2" "eu-west-1-0" "eu-west-1-1" "eu-west-1-2")

declare -a arr_clients=("client-0-0" "client-1-0" "client-2-0" "client-3-0" "client-4-0" "client-5-0")

USER="fs435"
BENCHMARK_NAME="auctionmark"


while getopts u:b: option; do
case "${option}" in
u) USER=${OPTARG};;
b) BENCHMARK_NAME=${OPTARG};;
esac;
done


#Upload schemas to clients //TODO: make sure generator has been run locally so this file exists.
parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-tables-schema.json ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_clients[@]} 

#Upload schemas to servers
parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-tables-schema.json ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_servers[@]} 

#Upload data to servers
parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-data ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_servers[@]} 


if [ "$BENCHMARK_NAME" = "tpcc" ]; then
	#no profile info.
	echo ""
fi

if [ "$BENCHMARK_NAME" = "seats" ]; then
	#upload profile info to clients
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-data/cached_flights.csv ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/sql-${BENCHMARK_NAME}-data/" ::: ${arr_clients[@]} 
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-data/airport_ids.csv ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/sql-${BENCHMARK_NAME}-data/" ::: ${arr_clients[@]} 
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-data/airport_flights.csv ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/sql-${BENCHMARK_NAME}-data/" ::: ${arr_clients[@]} 
		
fi
 
if [ "$BENCHMARK_NAME" = "auctionmark" ]; then
	#upload profile info to clients
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/auctionmark_profile ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_clients[@]} 
fi


#done
