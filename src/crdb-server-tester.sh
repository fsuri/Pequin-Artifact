./store/cockroachdb/asian_slipper.sh

DEBUG=store/$STORE/* store/server --config_path ./store/testing/toy.config \
--group_idx 0 \
--num_groups 1 \
--num_shards 1 \
--replica_idx 0 \
--indicus_total_processes 4 \
--protocol crdb \
--num_keys 1 \
--rw_or_retwis false \
--indicus_key_path ./keys

DEBUG=store/$STORE/* store/server --config_path ./store/testing/toy.config \
--group_idx 0 \
--num_groups 1 \
--num_shards 1 \
--replica_idx 1 \
--indicus_total_processes 4 \
--protocol crdb \
--num_keys 1 \
--rw_or_retwis false \
--indicus_key_path ./keys

store/server --config_path ./store/testing/toy.config \
--group_idx 0 \
--num_groups 1 \
--num_shards 1 \
--replica_idx 2 \
--indicus_total_processes 4 \
--protocol crdb \
--num_keys 1 \
--rw_or_retwis false \
--indicus_key_path ./keys