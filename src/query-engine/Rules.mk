d := $(dir $(lastword $(MAKEFILE_LIST)))

# ADR
#SRCS += $(addprefix $(d), adaptive_radix_tree/Tree.cc)

# Binder
SRCS += $(addprefix $(d), binder/bind_node_visitor.cc binder/binder_context.cc)

# Catalog catalog/database_metrics_catalog.cc catalog/index_metrics_catalog.cc catalog/query_metrics_catalog.cc catalog/table_metrics_catalog.cc
SRCS += $(addprefix $(d), catalog/abstract_catalog.cc catalog/catalog_cache.cc catalog/catalog.cc catalog/column_catalog.cc catalog/column_stats_catalog.cc \
catalog/column.cc catalog/constraint_catalog.cc catalog/constraint.cc catalog/database_catalog.cc catalog/index_catalog.cc \
catalog/language_catalog.cc catalog/layout_catalog.cc catalog/manager.cc catalog/proc_catalog.cc catalog/query_history_catalog.cc \
catalog/schema_catalog.cc catalog/schema.cc catalog/settings_catalog.cc catalog/system_catalogs.cc catalog/table_catalog.cc \
catalog/trigger_catalog.cc catalog/zone_map_catalog.cc)

# Codegen expression
#SRCS += $(addprefix $(d), codegen/expression/arithmetic_translator.cc codegen/expression/case_translator.cc codegen/expression/comparison_translator.cc \
codegen/expression/conjunction_translator.cc codegen/expression/constant_translator.cc codegen/expression/expression_translator.cc \
codegen/expression/function_translator.cc codegen/expression/negation_translator.cc codegen/expression/null_check_translator.cc \
codegen/expression/parameter_translator.cc codegen/expression/tuple_value_translator.cc)

# Codegen interpreter
#SRCS += $(addprefix $(d), codegen/interpreter/bytecode_builder.cc codegen/interpreter/bytecode_function.cc codegen/interpreter/bytecode_interpreter.cc)

# Codegen lang
#SRCS += $(addprefix $(d), codegen/lang/if.cc codegen/lang/loop.cc codegen/lang/vectorized_loop.cc)

# Codegen operator
#SRCS += $(addprefix $(d), codegen/operator/block_nested_loop_join_translator.cc codegen/operator/csv_scan_translator.cc codegen/operator/delete_translator.cc \
codegen/operator/global_group_by_translator.cc codegen/operator/hash_group_by_translator.cc codegen/operator/hash_join_translator.cc \
codegen/operator/hash_translator.cc codegen/operator/insert_translator.cc codegen/operator/limit_translator.cc codegen/operator/operator_translator.cc \
codegen/operator/order_by_translator.cc codegen/operator/projection_translator.cc codegen/operator/table_scan_translator.cc \
codegen/operator/update_translator.cc)

# Codegen proxy
#SRCS += $(addprefix $(d), codegen/proxy/bloom_filter_proxy.cc codegen/proxy/buffer_proxy.cc codegen/proxy/csv_scanner_proxy.cc \
codegen/proxy/data_table_proxy.cc codegen/proxy/date_functions_proxy.cc codegen/proxy/deleter_proxy.cc codegen/proxy/executor_context_proxy.cc \
codegen/proxy/hash_table_proxy.cc codegen/proxy/inserter_proxy.cc codegen/proxy/numeric_functions_proxy.cc codegen/proxy/oa_hash_table_proxy.cc \
codegen/proxy/pool_proxy.cc codegen/proxy/query_parameters_proxy.cc codegen/proxy/runtime_functions_proxy.cc codegen/proxy/sorter_proxy.cc \
codegen/proxy/storage_manager_proxy.cc codegen/proxy/string_functions_proxy.cc codegen/proxy/target_proxy.cc codegen/proxy/tile_group_proxy.cc \
codegen/proxy/timestamp_functions_proxy.cc codegen/proxy/transaction_context_proxy.cc codegen/proxy/transaction_runtime_proxy.cc \
codegen/proxy/tuple_proxy.cc codegen/proxy/updater_proxy.cc codegen/proxy/value_proxy.cc codegen/proxy/values_runtime_proxy.cc \
codegen/proxy/varlen_proxy.cc codegen/proxy/zone_map_proxy.cc)

# Codegen type
#SRCS += $(addprefix $(d), codegen/type/array_type.cc codegen/type/bigint_type.cc codegen/type/boolean_type.cc codegen/type/date_type.cc \
codegen/type/decimal_type.cc codegen/type/integer_type.cc codegen/type/smallint_type.cc codegen/type/sql_type.cc codegen/type/timestamp_type.cc \
codegen/type/tinyint_type.cc codegen/type/type_system.cc codegen/type/type.cc codegen/type/varbinary_type.cc codegen/type/varchar_type.cc)

# Codegen util
#SRCS += $(addprefix $(d), codegen/util/bloom_filter.cc codegen/util/buffer.cc codegen/util/csv_scanner.cc codegen/util/hash_table.cc \
codegen/util/oa_hash_table.cc codegen/util/sorter.cc)

# Codegen
#SRCS += $(addprefix $(d), codegen/aggregation.cc codegen/bloom_filter_accessor.cc codegen/buffer_accessor.cc codegen/buffering_consumer.cc \
codegen/code_context.cc codegen/codegen.cc codegen/compact_storage.cc codegen/compilation_context.cc codegen/consumer_context.cc \
codegen/counting_consumer.cc codegen/deleter.cc codegen/execution_consumer.cc codegen/function_builder.cc codegen/hash_table.cc \
codegen/hash.cc codegen/inserter.cc codegen/oa_hash_table.cc codegen/parameter_cache.cc codegen/pipeline.cc codegen/query_cache.cc \
codegen/query_compiler.cc codegen/query_state.cc codegen/query.cc codegen/row_batch.cc codegen/runtime_functions.cc codegen/sorter.cc \
codegen/table_storage.cc codegen/table.cc codegen/tile_group.cc codegen/transaction_runtime.cc codegen/translator_factory.cc \
codegen/updateable_storage.cc codegen/updater.cc codegen/value.cc codegen/values_runtime.cc codegen/vector.cc)


# Common
SRCS += $(addprefix $(d), common/container/circular_buffer.cc common/container/cuckoo_map.cc common/container/lock_free_array.cc common/allocator.cc \
common/cache.cc common/exception.cc common/init.cc common/internal_types.cc common/item_pointer.cc common/notifiable_task.cc common/portal.cc common/printable.cc \
common/sql_node_visitor.cc common/stack_trace.cc common/statement_cache_manager.cc common/statement_cache.cc common/statement.cc common/utility.cc)

# Concurrency
SRCS += $(addprefix $(d), concurrency/decentralized_epoch_manager.cc concurrency/epoch_manager_factory.cc concurrency/local_epoch.cc \
concurrency/timestamp_ordering_transaction_manager.cc concurrency/transaction_context.cc concurrency/transaction_manager_factory.cc concurrency/transaction_manager.cc)

# Executor
SRCS += $(addprefix $(d), executor/abstract_executor.cc executor/abstract_join_executor.cc executor/abstract_scan_executor.cc executor/aggregate_executor.cc \
executor/aggregator.cc executor/analyze_executor.cc executor/append_executor.cc executor/copy_executor.cc executor/create_executor.cc \
executor/create_function_executor.cc executor/delete_executor.cc executor/drop_executor.cc executor/executor_context.cc executor/hash_executor.cc \
executor/hash_join_executor.cc executor/hash_set_op_executor.cc executor/hybrid_scan_executor.cc executor/index_scan_executor.cc executor/insert_executor.cc \
executor/limit_executor.cc executor/logical_tile_factory.cc executor/logical_tile.cc executor/materialization_executor.cc executor/merge_join_executor.cc \
executor/nested_loop_join_executor.cc executor/order_by_executor.cc executor/plan_executor.cc executor/populate_index_executor.cc executor/projection_executor.cc \
executor/seq_scan_executor.cc executor/update_executor.cc)

# Expression
SRCS += $(addprefix $(d), expression/abstract_expression.cc expression/aggregate_expression.cc expression/case_expression.cc expression/comparison_expression.cc \
expression/conjunction_expression.cc expression/constant_value_expression.cc expression/function_expression.cc expression/operator_expression.cc \
expression/parameter_value_expression.cc expression/star_expression.cc expression/tuple_value_expression.cc)

# Function
SRCS += $(addprefix $(d), function/date_functions.cc function/functions.cc function/numeric_functions.cc function/old_engine_string_functions.cc \
function/string_functions.cc function/timestamp_functions.cc)

# GC
SRCS += $(addprefix $(d), gc/gc_manager_factory.cc gc/gc_manager.cc gc/transaction_level_gc_manager.cc)

# Index
SRCS += $(addprefix $(d), index/bwtree_index.cc index/bwtree.cc index/index_factory.cc index/index_util.cc index/index.cc index/skiplist_index.cc \
index/skiplist.cc)

# Murmur3
SRCS += $(addprefix $(d), murmur3/MurmurHash3.cc)

# Optimizer
SRCS += $(addprefix $(d), optimizer/stats/child_stats_deriver.cc optimizer/stats/column_stats_collector.cc optimizer/stats/selectivity.cc optimizer/stats/stats_calculator.cc \
optimizer/stats/stats_storage.cc optimizer/stats/stats.cc optimizer/stats/table_stats_collector.cc optimizer/stats/table_stats.cc optimizer/stats/tuple_sample.cc \
optimizer/stats/tuple_sampler.cc optimizer/stats/tuple_samples_storage.cc optimizer/abstract_optimizer.cc optimizer/binding.cc optimizer/child_property_deriver.cc \
optimizer/column_manager.cc optimizer/column.cc optimizer/group_expression.cc optimizer/group.cc optimizer/input_column_deriver.cc optimizer/memo.cc \
optimizer/operator_expression.cc optimizer/operator_node.cc optimizer/operators.cc optimizer/optimizer_task.cc optimizer/optimizer.cc optimizer/pattern.cc \
optimizer/plan_generator.cc optimizer/properties.cc optimizer/property_enforcer.cc optimizer/property_set.cc optimizer/property.cc optimizer/query_to_operator_transformer.cc \
optimizer/rule_impls.cc optimizer/rule.cc optimizer/util.cc)

# Parser
SRCS += $(addprefix $(d), parser/analyze_statement.cc parser/copy_statement.cc parser/create_statement.cc parser/delete_statement.cc parser/drop_statement.cc \
parser/execute_statement.cc parser/insert_statement.cc parser/postgresparser.cc parser/prepare_statement.cc parser/select_statement.cc parser/sql_statement.cc \
parser/table_ref.cc parser/transaction_statement.cc parser/update_statement.cc)

# Planner
SRCS += $(addprefix $(d), planner/abstract_join_plan.cc planner/abstract_plan.cc planner/abstract_scan_plan.cc planner/aggregate_plan.cc planner/analyze_plan.cc \
planner/create_function_plan.cc planner/create_plan.cc planner/csv_scan_plan.cc planner/delete_plan.cc planner/drop_plan.cc planner/export_external_file_plan.cc \
planner/hash_join_plan.cc planner/hash_plan.cc planner/hybrid_scan_plan.cc planner/index_scan_plan.cc planner/insert_plan.cc planner/nested_loop_join_plan.cc \
planner/order_by_plan.cc planner/plan_util.cc planner/populate_index_plan.cc planner/project_info.cc planner/projection_plan.cc planner/seq_scan_plan.cc \
planner/update_plan.cc)

# Settings
SRCS += $(addprefix $(d), settings/settings_manager.cc)

# Statistics
#SRCS += $(addprefix $(d), statistics/abstract_metric.cc statistics/access_metric.cc statistics/backend_stats_context.cc statistics/counter_metric.cc \
statistics/database_metric.cc statistics/index_metric.cc statistics/latency_metric.cc statistics/processor_metric.cc statistics/query_metric.cc \
statistics/stats_aggregator.cc statistics/table_metric.cc)

# Storage
SRCS += $(addprefix $(d), storage/abstract_table.cc storage/backend_manager.cc storage/data_table.cc storage/database.cc storage/layout.cc storage/storage_manager.cc \
storage/table_factory.cc storage/temp_table.cc storage/tile_group_factory.cc storage/tile_group_header.cc storage/tile_group_iterator.cc \
storage/tile_group.cc storage/tile.cc storage/tuple.cc storage/zone_map_manager.cc)

# Threadpool
SRCS += $(addprefix $(d), threadpool/worker_pool.cc)

# Traffic cop
SRCS += $(addprefix $(d), traffic_cop/traffic_cop.cc)

# Trigger
SRCS += $(addprefix $(d), trigger/trigger.cc)

# Tuning
#SRCS += $(addprefix $(d), tuning/clusterer.cc tuning/index_tuner.cc tuning/layout_tuner.cc tuning/sample.cc)

# Type
SRCS += $(addprefix $(d), type/array_type.cc type/bigint_type.cc type/boolean_type.cc type/date_type.cc type/decimal_type.cc type/integer_parent_type.cc \
type/integer_type.cc type/numeric_type.cc type/smallint_type.cc type/timestamp_type.cc type/tinyint_type.cc type/type.cc type/value.cc type/value.cc type/varlen_type.cc)

# UDF
#SRCS += $(addprefix $(d), udf/ast_nodes.cc udf/udf_handler.cc udf/udf_parser.cc)

# Util
SRCS += $(addprefix $(d), util/file.cc util/string_util.cc util/stringbox_util.cc)

# Queryexec test
SRCS += $(addprefix $(d), queryexec-test.cc)

# Object files

# ADR
#LIB-adr := $(o)adaptive_radix_tree/Tree.o

# Binder
LIB-binder := $(o)binder/bind_node_visitor.o $(o)binder/binder_context.o

# Catalog $(o)catalog/database_metrics_catalog.o $(o)catalog/index_metrics_catalog.o $(o)catalog/query_metrics_catalog.o $(o)catalog/table_metrics_catalog.o
LIB-catalog := $(o)catalog/abstract_catalog.o $(o)catalog/catalog_cache.o $(o)catalog/catalog.o $(o)catalog/column_catalog.o $(o)catalog/column_stats_catalog.o \
$(o)catalog/column.o $(o)catalog/constraint_catalog.o $(o)catalog/constraint.o $(o)catalog/database_catalog.o $(o)catalog/index_catalog.o \
$(o)catalog/language_catalog.o $(o)catalog/layout_catalog.o $(o)catalog/manager.o $(o)catalog/proc_catalog.o $(o)catalog/query_history_catalog.o \
$(o)catalog/schema_catalog.o $(o)catalog/schema.o $(o)catalog/settings_catalog.o $(o)catalog/system_catalogs.o $(o)catalog/table_catalog.o \
$(o)catalog/trigger_catalog.o $(o)catalog/zone_map_catalog.o

# Codegen expression
#LIB-codegen-expression := $(o)codegen/expression/arithmetic_translator.o $(o)codegen/expression/case_translator.o $(o)codegen/expression/comparison_translator.o \
$(o)codegen/expression/conjunction_translator.o $(o)codegen/expression/constant_translator.o $(o)codegen/expression/expression_translator.o \
$(o)codegen/expression/function_translator.o $(o)codegen/expression/negation_translator.o $(o)codegen/expression/null_check_translator.o \
$(o)codegen/expression/parameter_translator.o $(o)codegen/expression/tuple_value_translator.o

# Codegen interpreter
#LIB-codegen-interpreter := $(o)codegen/interpreter/bytecode_builder.o $(o)codegen/interpreter/bytecode_function.o $(o)codegen/interpreter/bytecode_interpreter.o

# Codegen lang
#LIB-codegen-lang := $(o)codegen/lang/if.o $(o)codegen/lang/loop.o $(o)codegen/lang/vectorized_loop.o

# Codegen operator
#LIB-codegen-operator := $(o)codegen/operator/block_nested_loop_join_translator.o $(o)codegen/operator/csv_scan_translator.o $(o)codegen/operator/delete_translator.o \
$(o)codegen/operator/global_group_by_translator.o $(o)codegen/operator/hash_group_by_translator.o $(o)codegen/operator/hash_join_translator.o \
$(o)codegen/operator/hash_translator.o $(o)codegen/operator/insert_translator.o $(o)codegen/operator/limit_translator.o $(o)codegen/operator/operator_translator.o \
$(o)codegen/operator/order_by_translator.o $(o)codegen/operator/projection_translator.o $(o)codegen/operator/table_scan_translator.o \
$(o)codegen/operator/update_translator.o

# Codegen proxy
#LIB-codegen-proxy := $(o)codegen/proxy/bloom_filter_proxy.o $(o)codegen/proxy/buffer_proxy.o $(o)codegen/proxy/csv_scanner_proxy.o \
$(o)codegen/proxy/data_table_proxy.o $(o)codegen/proxy/date_functions_proxy.o $(o)codegen/proxy/deleter_proxy.o $(o)codegen/proxy/executor_context_proxy.o \
$(o)codegen/proxy/hash_table_proxy.o $(o)codegen/proxy/inserter_proxy.o $(o)codegen/proxy/numeric_functions_proxy.o $(o)codegen/proxy/oa_hash_table_proxy.o \
$(o)codegen/proxy/pool_proxy.o $(o)codegen/proxy/query_parameters_proxy.o $(o)codegen/proxy/runtime_functions_proxy.o $(o)codegen/proxy/sorter_proxy.o \
$(o)codegen/proxy/storage_manager_proxy.o $(o)codegen/proxy/string_functions_proxy.o $(o)codegen/proxy/target_proxy.o $(o)codegen/proxy/tile_group_proxy.o \
$(o)codegen/proxy/timestamp_functions_proxy.o $(o)codegen/proxy/transaction_context_proxy.o $(o)codegen/proxy/transaction_runtime_proxy.o \
$(o)codegen/proxy/tuple_proxy.o $(o)codegen/proxy/updater_proxy.o $(o)codegen/proxy/value_proxy.o $(o)codegen/proxy/values_runtime_proxy.o \
$(o)codegen/proxy/varlen_proxy.o $(o)codegen/proxy/zone_map_proxy.o

# Codegen type
#LIB-codegen-type := $(o)codegen/type/array_type.o $(o)codegen/type/bigint_type.o $(o)codegen/type/boolean_type.o $(o)codegen/type/date_type.o \
$(o)codegen/type/decimal_type.o $(o)codegen/type/integer_type.o $(o)codegen/type/smallint_type.o $(o)codegen/type/sql_type.o $(o)codegen/type/timestamp_type.o \
$(o)codegen/type/tinyint_type.o $(o)codegen/type/type_system.o $(o)codegen/type/type.o $(o)codegen/type/varbinary_type.o $(o)codegen/type/varchar_type.o

# Codegen util
#LIB-codegen-util := $(o)codegen/util/bloom_filter.o $(o)codegen/util/buffer.o $(o)codegen/util/csv_scanner.o $(o)codegen/util/hash_table.o \
$(o)codegen/util/oa_hash_table.o $(o)codegen/util/sorter.o

# Codegen
#LIB-codegen := $(o)codegen/aggregation.o $(o)codegen/bloom_filter_accessor.o $(o)codegen/buffer_accessor.o $(o)codegen/buffering_consumer.o \
$(o)codegen/code_context.o $(o)codegen/codegen.o $(o)codegen/compact_storage.o $(o)codegen/compilation_context.o $(o)codegen/consumer_context.o \
$(o)codegen/counting_consumer.o $(o)codegen/deleter.o $(o)codegen/execution_consumer.o $(o)codegen/function_builder.o $(o)codegen/hash_table.o \
$(o)codegen/hash.o $(o)codegen/inserter.o $(o)codegen/oa_hash_table.o $(o)codegen/parameter_cache.o $(o)codegen/pipeline.o $(o)codegen/query_cache.o \
$(o)codegen/query_compiler.o $(o)codegen/query_state.o $(o)codegen/query.o $(o)codegen/row_batch.o $(o)codegen/runtime_functions.o $(o)codegen/sorter.o \
$(o)codegen/table_storage.o $(o)codegen/table.o $(o)codegen/tile_group.o $(o)codegen/transaction_runtime.o $(o)codegen/translator_factory.o \
$(o)codegen/updateable_storage.o $(o)codegen/updater.o $(o)codegen/value.o $(o)codegen/values_runtime.o $(o)codegen/vector.o

# Common
LIB-common := $(o)common/container/circular_buffer.o $(o)common/container/cuckoo_map.o $(o)common/container/lock_free_array.o $(o)common/allocator.o \
$(o)common/cache.o $(o)common/exception.o $(o)common/init.o $(o)common/internal_types.o $(o)common/item_pointer.o $(o)common/notifiable_task.o $(o)common/portal.o $(o)common/printable.o \
$(o)common/sql_node_visitor.o $(o)common/stack_trace.o $(o)common/statement_cache_manager.o $(o)common/statement_cache.o $(o)common/statement.o $(o)common/utility.o

# Concurrency
LIB-concurrency := $(o)concurrency/decentralized_epoch_manager.o $(o)concurrency/epoch_manager_factory.o $(o)concurrency/local_epoch.o \
$(o)concurrency/timestamp_ordering_transaction_manager.o $(o)concurrency/transaction_context.o $(o)concurrency/transaction_manager_factory.o $(o)concurrency/transaction_manager.o

# Executor
LIB-executor := $(o)executor/abstract_executor.o $(o)executor/abstract_join_executor.o $(o)executor/abstract_scan_executor.o $(o)executor/aggregate_executor.o \
$(o)executor/aggregator.o $(o)executor/analyze_executor.o $(o)executor/append_executor.o $(o)executor/copy_executor.o $(o)executor/create_executor.o \
$(o)executor/create_function_executor.o $(o)executor/delete_executor.o $(o)executor/drop_executor.o $(o)executor/executor_context.o $(o)executor/hash_executor.o \
$(o)executor/hash_join_executor.o $(o)executor/hash_set_op_executor.o $(o)executor/hybrid_scan_executor.o $(o)executor/index_scan_executor.o $(o)executor/insert_executor.o \
$(o)executor/limit_executor.o $(o)executor/logical_tile_factory.o $(o)executor/logical_tile.o $(o)executor/materialization_executor.o $(o)executor/merge_join_executor.o \
$(o)executor/nested_loop_join_executor.o $(o)executor/order_by_executor.o $(o)executor/plan_executor.o $(o)executor/populate_index_executor.o $(o)executor/projection_executor.o \
$(o)executor/seq_scan_executor.o $(o)executor/update_executor.o

# Expression
LIB-expression := $(o)expression/abstract_expression.o $(o)expression/aggregate_expression.o $(o)expression/case_expression.o $(o)expression/comparison_expression.o \
$(o)expression/conjunction_expression.o $(o)expression/constant_value_expression.o $(o)expression/function_expression.o $(o)expression/operator_expression.o \
$(o)expression/parameter_value_expression.o $(o)expression/star_expression.o $(o)expression/tuple_value_expression.o

# Function
LIB-function := $(o)function/date_functions.o $(o)function/functions.o $(o)function/numeric_functions.o $(o)function/old_engine_string_functions.o \
$(o)function/string_functions.o $(o)function/timestamp_functions.o

# GC
LIB-gc := $(o)gc/gc_manager_factory.o $(o)gc/gc_manager.o $(o)gc/transaction_level_gc_manager.o

# Index $(o)index/art_index.o
LIB-index := $(o)index/bwtree_index.o $(o)index/bwtree.o $(o)index/index_factory.o $(o)index/index_util.o $(o)index/index.o $(o)index/skiplist_index.o \
$(o)index/skiplist.o

# Murmur
LIB-murmur := $(o)murmur3/MurmurHash3.o

# Optimizer
LIB-optimizer := $(o)optimizer/stats/child_stats_deriver.o $(o)optimizer/stats/column_stats_collector.o $(o)optimizer/stats/selectivity.o $(o)optimizer/stats/stats_calculator.o \
$(o)optimizer/stats/stats_storage.o $(o)optimizer/stats/stats.o $(o)optimizer/stats/table_stats_collector.o $(o)optimizer/stats/table_stats.o $(o)optimizer/stats/tuple_sample.o \
$(o)optimizer/stats/tuple_sampler.o $(o)optimizer/stats/tuple_samples_storage.o $(o)optimizer/abstract_optimizer.o $(o)optimizer/binding.o $(o)optimizer/child_property_deriver.o \
$(o)optimizer/column_manager.o $(o)optimizer/column.o $(o)optimizer/group_expression.o $(o)optimizer/group.o $(o)optimizer/input_column_deriver.o $(o)optimizer/memo.o \
$(o)optimizer/operator_expression.o $(o)optimizer/operator_node.o $(o)optimizer/operators.o $(o)optimizer/optimizer_task.o $(o)optimizer/optimizer.o $(o)optimizer/pattern.o \
$(o)optimizer/plan_generator.o $(o)optimizer/properties.o $(o)optimizer/property_enforcer.o $(o)optimizer/property_set.o $(o)optimizer/property.o $(o)optimizer/query_to_operator_transformer.o \
$(o)optimizer/rule_impls.o $(o)optimizer/rule.o $(o)optimizer/util.o

# Parser
LIB-parser := $(o)parser/analyze_statement.o $(o)parser/copy_statement.o $(o)parser/create_statement.o $(o)parser/delete_statement.o $(o)parser/drop_statement.o \
$(o)parser/execute_statement.o $(o)parser/insert_statement.o $(o)parser/postgresparser.o $(o)parser/prepare_statement.o $(o)parser/select_statement.o $(o)parser/sql_statement.o \
$(o)parser/table_ref.o $(o)parser/transaction_statement.o $(o)parser/update_statement.o

# Planner
LIB-planner := $(o)planner/abstract_join_plan.o $(o)planner/abstract_plan.o $(o)planner/abstract_scan_plan.o $(o)planner/aggregate_plan.o $(o)planner/analyze_plan.o \
$(o)planner/create_function_plan.o $(o)planner/create_plan.o $(o)planner/csv_scan_plan.o $(o)planner/delete_plan.o $(o)planner/drop_plan.o $(o)planner/export_external_file_plan.o \
$(o)planner/hash_join_plan.o $(o)planner/hash_plan.o $(o)planner/hybrid_scan_plan.o $(o)planner/index_scan_plan.o $(o)planner/insert_plan.o $(o)planner/nested_loop_join_plan.o \
$(o)planner/order_by_plan.o $(o)planner/plan_util.o $(o)planner/populate_index_plan.o $(o)planner/project_info.o $(o)planner/projection_plan.o $(o)planner/seq_scan_plan.o \
$(o)planner/update_plan.o

# Settings
LIB-settings := $(o)settings/settings_manager.o

# Statistics
#LIB-statistics := $(o)statistics/abstract_metric.o $(o)statistics/access_metric.o $(o)statistics/backend_stats_context.o $(o)statistics/counter_metric.o \
$(o)statistics/database_metric.o $(o)statistics/index_metric.o $(o)statistics/latency_metric.o $(o)statistics/processor_metric.o $(o)statistics/query_metric.o \
$(o)statistics/stats_aggregator.o $(o)statistics/table_metric.o

# Storage
LIB-storage := $(o)storage/abstract_table.o $(o)storage/backend_manager.o $(o)storage/data_table.o $(o)storage/database.o $(o)storage/layout.o $(o)storage/storage_manager.o \
$(o)storage/table_factory.o $(o)storage/temp_table.o $(o)storage/tile_group_factory.o $(o)storage/tile_group_header.o $(o)storage/tile_group_iterator.o \
$(o)storage/tile_group.o $(o)storage/tile.o $(o)storage/tuple.o $(o)storage/zone_map_manager.o

# Threadpool
LIB-threadpool := $(o)threadpool/worker_pool.o

# Traffic cop
LIB-traffic-cop := $(o)traffic_cop/traffic_cop.o

# Trigger
LIB-trigger := $(o)trigger/trigger.o

# Tuning
#LIB-tuning := $(o)tuning/clusterer.o $(o)tuning/index_tuner.o $(o)tuning/layout_tuner.o $(o)tuning/sample.o

# Type
LIB-type := $(o)type/array_type.o $(o)type/bigint_type.o $(o)type/boolean_type.o $(o)type/date_type.o $(o)type/decimal_type.o $(o)type/integer_parent_type.o \
$(o)type/integer_type.o $(o)type/numeric_type.o $(o)type/smallint_type.o $(o)type/timestamp_type.o $(o)type/tinyint_type.o $(o)type/type.o $(o)type/value.o $(o)type/value.o $(o)type/varlen_type.o

# Udf
#LIB-udf := $(o)udf/ast_nodes.o $(o)udf/udf_handler.o $(o)udf/udf_parser.o

# Util
LIB-util := $(o)util/file.o $(o)util/string_util.o $(o)util/stringbox_util.o

#$(LIB-codegen-expression) $(LIB-codegen-interpreter) $(LIB-codegen-lang) $(LIB-codegen-operator) $(LIB-codegen-proxy) \
$(LIB-codegen-type) $(LIB-codegen-util) $(LIB-codegen) $(LIB-udf) $(LIB-tuning) $(LIB-adr) $(LIB-statistics)

$(d)queryexec-test: $(LIB-binder) $(LIB-catalog) $(LIB-common) $(LIB-concurrency) $(LIB-executor) $(LIB-expression) $(LIB-function) \
$(LIB-gc) $(LIB-index) $(LIB-murmur) $(LIB-optimizer) $(LIB-parser) $(LIB-planner) $(LIB-settings) $(LIB-storage) $(LIB-threadpool) $(LIB-traffic-cop) \
$(LIB-type) $(LIB-trigger) $(LIB-util) $(o)queryexec-test.o
BINS += $(d)queryexec-test