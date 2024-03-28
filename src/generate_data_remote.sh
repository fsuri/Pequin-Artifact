#!/bin/bash

##use this if I want to upload things onto the machines..
##scp  -r file fs435@${machine}.indicus.morty-pg0.utah.cloudlab.us:/users/fs435/
## ssh ... sudo mv .. ...



## declare an array variable
declare -a arr_servers=("us-east-1-0" "us-east-1-1" "us-east-1-2" "eu-west-1-0" "eu-west-1-1" "eu-west-1-2")

declare -a arr_clients=("client-0-0" "client-1-0" "client-2-0" "client-3-0" "client-4-0" "client-5-0")

USER="fs435"
BENCHMARK_NAME="tpcc"
GENERATOR_NAME=sql_${BENCHMARK_NAME}_generator
SCALE_FACTOR=1 
NUM_WAREHOUSES=1

MAX_AIRPORTS=-1
K_NEAREST_AIRPORTS=10

while getopts user:bench:scale_factor:num_warehouses:max_airports:k_nearest_airports: option; do
case "${option}" in
user) USER=${OPTARG};;
bench) BENCHMARK_NAME=${OPTARG};;
scale_factor) SCALE_FACTOR=${OPTARG};;
num_warehouses) NUM_WAREHOUSES=${OPTARG};;
max_airports) MAX_AIRPORTS=${OPTARG};;
k_nearest_airports) K_NEAREST_AIRPORTS=${OPTARG};;
esac;
done

echo "DEPRECATED-DO NOT USE. Generating data separately at servers may result in different local data views"
exit 1

#Upload schemas to clients //TODO: make sure generator has been run locally so this file exists.
parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-tables-schema.json ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_clients[@]} 

#Upload schemas to servers
#parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/sql-${BENCHMARK_NAME}-tables-schema.json ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_servers[@]} 



##Upload generator binaries to servers
parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/$BENCHMARK_NAME/$GENERATOR_NAME ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_servers[@]} 

 #pick which benchmark to run, and run the generator at server (will generate both schema and data)
 
if [ "$BENCHMARK_NAME" = "tpcc" ]; then
#FIXME: must seed TPCC so all servers generate same data...
	parallel "ssh ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us \"source /opt/intel/oneapi/setvars.sh --force; source /usr/local/etc/set_env.sh; cd benchmark_data; ./$GENERATOR_NAME -num_warehouses=$NUM_WAREHOUSES\"" ::: ${arr_servers[@]}  
fi

if [ "$BENCHMARK_NAME" = "seats" ]; then
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/resources ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/" ::: ${arr_servers[@]} 
	
	parallel "ssh ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us \"source /opt/intel/oneapi/setvars.sh --force; source /usr/local/etc/set_env.sh; cd benchmark_data; ./$GENERATOR_NAME --max_airports=$MAX_AIRPORTS --k_nearest_airports=$K_NEAREST_AIRPORTS\"" ::: ${arr_servers[@]}  
fi
 
if [ "$BENCHMARK_NAME" = "auctionmark" ]; then
	parallel "rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/utils/categories.txt ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/utils/" ::: ${arr_servers[@]} 
	
	parallel "ssh ${USER}@{}.pequin.pequin-pg0.utah.cloudlab.us \"source /opt/intel/oneapi/setvars.sh --force; source /usr/local/etc/set_env.sh; cd benchmark_data; ./$GENERATOR_NAME\"" ::: ${arr_servers[@]}  
fi


## Sequential version 
## now loop through the above array
#for host in "${arr_servers[@]}"
#do
#   echo "Generating data for benchmark $BENCHMARK_NAME on $host"
    
   #upload binary to folder benchmark_data/
#   rsync -v -r -e ssh ./store/benchmark/async/sql/$BENCHMARK_NAME/$GENERATOR_NAME ${USER}@$host.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/
   
   #pick which benchmark to run, and run the generator
#   if [ "$BENCHMARK_NAME" = "tpcc" ]; then
#   	ssh ${USER}@$host.pequin.pequin-pg0.utah.cloudlab.us "source /opt/intel/oneapi/setvars.sh --force; source /usr/local/etc/set_env.sh; cd benchmark_data; ./$GENERATOR_NAME --num_warehouses=$NUM_WAREHOUSE"
#   fi
#   if [ "$BENCHMARK_NAME" = "seats" ]; then
#   	ssh ${USER}@$host.pequin.pequin-pg0.utah.cloudlab.us "source /opt/intel/oneapi/setvars.sh --force; source /usr/local/etc/set_env.sh; cd benchmark_data; ./$GENERATOR_NAME --max_airports=MAX_AIRPORTS --k_nearest_airports=$K_NEAREST_AIRPORTS"
#   fi
#    if [ "$BENCHMARK_NAME" = "auctionmark" ]; then
#    	rsync -v -r -e ssh ./store/benchmark/async/sql/${BENCHMARK_NAME}/utils/categories.txt ${USER}@$host.pequin.pequin-pg0.utah.cloudlab.us:/users/${USER}/benchmark_data/utils/
    	
#   	ssh ${USER}@$host.pequin.pequin-pg0.utah.cloudlab.us "source /opt/intel/oneapi/setvars.sh --force; source /usr/local/etc/set_env.sh; cd benchmark_data; ./$GENERATOR_NAME"
#   fi
      
#done
