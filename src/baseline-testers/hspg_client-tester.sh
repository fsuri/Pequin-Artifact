#!/bin/bash

CLIENTS=1
F=0
NUM_GROUPS=1
CONFIG="0_local_test_outputs/configs/shard-r4.config"
PROTOCOL="pg-smr"
STORE=${PROTOCOL}store
DURATION=13
ZIPF=0.0
NUM_OPS_TX=1
NUM_KEYS_IN_DB=1
KEY_PATH="keys"
# BENCHMARK="toy"
# BENCHMARK="rw-sql"
SMR_MODE=1
SQL_BENCH="true"

# BENCHMARK="rw-sql"
# FILE_PATH="0_local_test_outputs/rw-sql/rw-sql.json"

BENCHMARK="tpcc-sql"
FILE_PATH="store/benchmark/async/sql/tpcc/sql-tpcc-tables-schema.json"

# BENCHMARK="seats-sql"
# FILE_PATH="store/benchmark/async/sql/seats/sql-seats-tables-schema.json"

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

N=$((3*$F+1))

echo '[1] Starting new clients'

for i in `seq 0 $((CLIENTS-1))`; do
  #valgrind

  echo "store/$STORE/*client.cc store/benchmark/async/benchmark --config_path $CONFIG --num_groups $NUM_GROUPS \
    --num_shards $NUM_GROUPS \
    --protocol_mode $PROTOCOL --num_keys $NUM_KEYS_IN_DB  --benchmark $BENCHMARK --pg_SMR_mode=$SMR_MODE --sql_bench=$SQL_BENCH --data_file_path $FILE_PATH \
    --num_ops_txn $NUM_OPS_TX --exp_duration $DURATION --client_id $i --num_client_hosts $CLIENTS --warmup_secs 0 --cooldown_secs 0 \
    --key_selector zipf --zipf_coefficient $ZIPF --pg_fake_SMR=true \
    --tpcc_num_warehouses 10 --tpcc_w_id 1 --tpcc_C_c_id 0 --tpcc_C_c_last 0 --tpcc_stock_level_ratio 0 --tpcc_delivery_ratio 0 --tpcc_order_status_ratio 0 --tpcc_payment_ratio 100 --tpcc_new_order_ratio 0 --tpcc_run_sequential=True \
    --stats_file "stats-0.json"  --indicus_key_path $KEY_PATH &> ./0_local_test_outputs/client-$i.out &"
 
 DEBUG=* store/benchmark/async/benchmark --config_path $CONFIG --num_groups $NUM_GROUPS \
    --num_shards $NUM_GROUPS \
    --protocol_mode $PROTOCOL --num_keys $NUM_KEYS_IN_DB  --benchmark $BENCHMARK  --sql_bench=$SQL_BENCH --data_file_path $FILE_PATH \
    --num_ops_txn $NUM_OPS_TX --exp_duration $DURATION --client_id $i --num_client_hosts $CLIENTS --warmup_secs 0 --cooldown_secs 0 \
    --key_selector zipf --zipf_coefficient $ZIPF \
    --stats_file "stats-0.json"  --indicus_key_path $KEY_PATH &> ./0_local_test_outputs/client-$i.out &
done;

sleep $((DURATION+4))
echo '[2] Shutting down possibly open servers and clients'
killall store/benchmark/async/benchmark
#callgrind_control --dump
killall store/server




# --warmup_secs 5 --cooldown_secs 2  --num_client_threads 1  --trans_protocol tcp
#  --message_timeout 10000 --abort_backoff 2 --retry_aborted=true
# --max_attempts -1 --max_backoff 75 --delay 2 --partitioner warehouse --store_mode=true 


