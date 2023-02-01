d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), SQLParser.cc SQLParserResult.cc sql_translator.cc types.cc sql_identifier_resolver_proxy.cc sql_identifier_resolver.cc \
sql_identifier.cc sql/Expr.cc sql/CreateStatement.cc sql/PrepareStatement.cc sql/SQLStatement.cc sql/statements.cc util/sqlhelper.cc parser/bison_parser.cc \
parser/flex_lexer.cc expression/abstract_expression.cc expression/abstract_predicate_expression.cc expression/between_expression.cc expression/binary_predicate_expression.cc \
expression/correlated_parameter_expression.cc expression/expression_utils.cc expression/is_null_expression.cc expression/logical_expression.cc \
expression/lqp_column_expression.cc expression/unary_minus_expression.cc expression/value_expression.cc logical_query_plan/abstract_lqp_node.cc \
logical_query_plan/lqp_utils.cc logical_query_plan/predicate_node.cc logical_query_plan/projection_node.cc logical_query_plan/union_node.cc \
operators/operator_scan_predicate.cc storage/lqp_view.cc utils/string_utils.cc all_parameter_variant.cc all_type_variant.cc parameter_id_allocator.cc \
operators/operator_scan_predicate.cc storage/lqp_view.cc utils/string_utils.cc all_parameter_variant.cc \
all_type_variant.cc parameter_id_allocator.cc operators/abstract_operator.cc)

#PROTOS += $(addprefix $(d), queryprocess.proto)

LIB-queryprocess := $(o)SQLParser.o $(o)SQLParserResult.o $(o)sql_translator.o $(o)types.o $(o)sql_identifier_resolver_proxy.o $(o)sql_identifier_resolver.o \
	$(o)sql_identifier.o $(o)sql/Expr.o $(o)sql/SQLStatement.o $(o)sql/statements.o $(o)sql/CreateStatement.o $(o)sql/PrepareStatement.o $(o)util/sqlhelper.o \
	$(o)expression/abstract_expression.o $(o)expression/abstract_predicate_expression.o $(o)expression/between_expression.o $(o)expression/binary_predicate_expression.o \
	$(o)expression/correlated_parameter_expression.o $(o)expression/expression_utils.o $(o)expression/is_null_expression.o $(o)expression/logical_expression.o \
	$(o)expression/lqp_column_expression.o $(o)expression/unary_minus_expression.o $(o)expression/value_expression.o $(o)logical_query_plan/abstract_lqp_node.o \
	$(o)logical_query_plan/lqp_utils.o $(o)logical_query_plan/predicate_node.o $(o)logical_query_plan/projection_node.o $(o)logical_query_plan/union_node.o $(o)operators/abstract_operator.o \
	$(o)operators/operator_scan_predicate.o $(o)storage/lqp_view.o $(o)utils/string_utils.o $(o)all_parameter_variant.o $(o)all_type_variant.o $(o)parameter_id_allocator.o

LIB-queryprocess-flex := $(o)parser/flex_lexer.o


#LIB-proto := $(o)pequin-proto.o $(o)query-proto.o
#-I/home/floriansuri/Research/Projects/Pequin/Pequin-Artifact/src/store/common
#$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o

#BINS += $(d)proto_bench

#include $(d)parser/Rules.mk
#include $(d)sql/Rules.mk
#include $(d)util/Rules.mk

