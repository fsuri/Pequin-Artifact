sh store/cockroachdb/asian_slipper.sh
id=$1
DEBUG=store/$STORE/* store/server --config_path ./store/testing/roachy9.config \
--group_idx $(( $id/3 )) \
--num_groups 3 \
--num_shards 3 \
--replica_idx $(( $id%3 )) \
--indicus_total_processes 4 \
--protocol crdb \
--num_keys 1 \
--rw_or_retwis false \
--indicus_key_path ./keys
