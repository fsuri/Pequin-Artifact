#!/bin/bash

CLIENTS=1
F=0
NUM_GROUPS=1
CONFIG="0_local_test_outputs/configs/shard-r1.config"
PROTOCOL="pequin"
STORE=${PROTOCOL}store
DURATION=5
ZIPF=0.0
NUM_OPS_TX=1
NUM_KEYS_IN_DB=1
KEY_PATH="keys"

SQL_BENCH="true"

BENCHMARK="rw-sql"
FILE_PATH="0_local_test_outputs/rw-sql/rw-sql.json"

#BENCHMARK="tpcc-sql"
#FILE_PATH="store/benchmark/async/sql/tpcc/sql-tpcc-tables-schema.json"

#BENCHMARK="seats-sql"
#FILE_PATH="store/benchmark/async/sql/seats/sql-seats-tables-schema.json"

#BENCHMARK="auctionmark-sql"
#FILE_PATH="store/benchmark/async/sql/auctionmark/sql-auctionmark-tables-schema.json"


while getopts c:f:g:p:s:d:z:o:k:b: option; do
case "${option}" in
c) CLIENTS=${OPTARG};;
f) F=${OPTARG};;
g) NUM_GROUPS=${OPTARG};;
p) CONFIG=${OPTARG};;
s) PROTOCOL=${OPTARG};;
d) DURATION=${OPTARG};;
z) ZIPF=${OPTARG};;
o) NUM_OPS_TX=${OPTARG};;
k) NUM_KEYS_IN_DB=${OPTARG};;
b) BENCHMARK=${OPTARG};;
esac;
done

N=$((5*$F+1))

echo '[1] Starting new clients'
for i in `seq 1 $((CLIENTS-1))`; do
  #valgrind
 DEBUG=store/$STORE/*client.cc store/benchmark/async/benchmark --config_path $CONFIG --num_groups $NUM_GROUPS \
    --num_shards $NUM_GROUPS \
    --protocol_mode $PROTOCOL --num_keys $NUM_KEYS_IN_DB --benchmark $BENCHMARK --sql_bench=$SQL_BENCH --data_file_path $FILE_PATH --num_ops_txn $NUM_OPS_TX \
    --exp_duration $DURATION --client_id $i --num_client_hosts $CLIENTS --warmup_secs 0 --cooldown_secs 0 \
    --key_selector zipf --zipf_coefficient $ZIPF --indicus_key_path $KEY_PATH &> ./0_local_test_outputs/client-$i.out &
done;

#valgrind
DEBUG=store/$STORE/* store/benchmark/async/benchmark --config_path $CONFIG --num_groups $NUM_GROUPS \
  --num_shards $NUM_GROUPS --protocol_mode $PROTOCOL --num_keys $NUM_KEYS_IN_DB --benchmark $BENCHMARK --sql_bench=$SQL_BENCH --data_file_path $FILE_PATH \
  --num_ops_txn $NUM_OPS_TX --exp_duration $DURATION --client_id 0 --num_client_hosts $CLIENTS --warmup_secs 0 \
  --cooldown_secs 0 --key_selector zipf --zipf_coefficient $ZIPF \
  --stats_file "stats-0.json" --indicus_key_path $KEY_PATH &> ./0_local_test_outputs/client-0.out &


sleep $((DURATION+4))
echo '[2] Shutting down possibly open servers and clients'
killall store/benchmark/async/benchmark
#callgrind_control --dump
killall store/server

