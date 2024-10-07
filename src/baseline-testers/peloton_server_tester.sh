#!/bin/bash

F=1
NUM_GROUPS=1
CONFIG="0_local_test_outputs/configs/shard-r4.config"
PROTOCOL="peloton-smr"
STORE=${PROTOCOL}store
ZIPF=0.0
NUM_OPS_TX=1
NUM_KEYS_IN_DB=1
KEY_PATH="keys"
SQL_BENCH="true"
LOCAL="true"
# FILE_PATH="0_local_test_outputs/rw-sql/rw-sql.json"
#FILE_PATH="sql-seats-tables-schema.json"
#"0_local_test_outputs/kv_example/kv-tables-schema.json"
ASYNC_SERVER="true"
HS_DUMMY_TO=50
SMR_MODE=1

# FILE_PATH="0_local_test_outputs/rw-sql/rw-sql.json"
FILE_PATH="store/benchmark/async/sql/tpcc/sql-tpcc-tables-schema.json"
#FILE_PATH="store/benchmark/async/sql/seats/sql-seats-tables-schema.json"
#FILE_PATH="store/benchmark/async/sql/auctionmark/sql-auctionmark-tables-schema.json"
#FILE_PATH="store/benchmark/async/sql/tpcc/sql-tpcc-tables-schema.json"


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

N=$((3*$F+1))

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
             	#echo Starting server $i
	        DEBUG=* store/server --config_path $CONFIG --group_idx $j --num_groups $NUM_GROUPS --num_shards $NUM_GROUPS --replica_idx $i --protocol $PROTOCOL --num_keys $NUM_KEYS_IN_DB  --sql_bench=$SQL_BENCH --data_file_path $FILE_PATH --debug_stats --indicus_key_path $KEY_PATH --optimize_tpool_for_dev_machine --pg_SMR_mode=$SMR_MODE --local_config=$LOCAL --hs_dummy_to=$HS_DUMMY_TO --indicus_sign_messages='false' &> 0_local_test_outputs/server$(($i+$j*$N)).out &
		
	done;
done;
