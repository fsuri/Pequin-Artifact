#!/bin/bash

F=0
NUM_GROUPS=1
CONFIG="0_local_test_outputs/configs/shard-r1.config"
POLICY_CONFIG="0_local_test_outputs/configs/policy-grouped.config"
POLICY_FUNCTION="grouped"
PROTOCOL="sintr"
STORE=${PROTOCOL}store
ZIPF=0.0
NUM_OPS_TX=2
NUM_KEYS_IN_DB=1
KEY_PATH="keys"
STORE_MODE="false"
SQL_BENCH="false"

FILE_PATH="/usr/local/etc/tpcc-1-warehouse"
# FILE_PATH="0_local_test_outputs/rw-sql/rw-sql.json"
#FILE_PATH="store/benchmark/async/sql/tpcc/sql-tpcc-tables-schema.json"
#FILE_PATH="store/benchmark/async/sql/seats/sql-seats-tables-schema.json"
#FILE_PATH="store/benchmark/async/sql/auctionmark/sql-auctionmark-tables-schema.json"
DEBUG_FILES="store/$STORE/server.cc store/$STORE/servertools.cc"

while getopts f:g:p:s:z:o:k: option; do
case "${option}" in
f) F=${OPTARG};;
g) NUM_GROUPS=${OPTARG};;
p) CONFIG=${OPTARG};;
s) PROTOCOL=${OPTARG};;
z) ZIPF=${OPTARG};;
o) NUM_OPS_TX=${OPTARG};;
k) NUM_KEYS_IN_DB=${OPTARG};;
esac;
done

N=$((5*$F+1))

echo '[1] Shutting down possibly open servers'
for j in `seq 0 $((NUM_GROUPS-1))`; do
	for i in `seq 0 $((N-1))`; do
		#echo $((8000+$j*$N+$i))
		lsof -ti:$((8000+i)) | xargs kill -9 &>/dev/null   
	done;
done;
killall store/server

echo '[2] Starting new servers'
for j in `seq 0 $((NUM_GROUPS-1))`; do
	#echo Starting Group $j
	for i in `seq 0 $((N-1))`; do
		#echo Starting Replica $(($i+$j*$N))
		#valgrind --tool=callgrind --instr-atstart=no
		DEBUG=$DEBUG_FILES store/server --config_path $CONFIG --group_idx $j \
			--num_groups $NUM_GROUPS --num_shards $NUM_GROUPS --replica_idx $i --protocol $PROTOCOL \
			--num_keys $NUM_KEYS_IN_DB --sql_bench=$SQL_BENCH --data_file_path $FILE_PATH \
			--debug_stats --indicus_key_path $KEY_PATH --optimize_tpool_for_dev_machine \
			--store_mode=$STORE_MODE --indicus_hash_digest=true --indicus_verify_deps=false \
			--sintr_policy_config_path $POLICY_CONFIG --sintr_policy_function_name $POLICY_FUNCTION \
			--indicus_no_fallback=false --sintr_hash_endorsements=true &> ./0_local_test_outputs/server$(($i+$j*$N)).out &
	done;
done;
