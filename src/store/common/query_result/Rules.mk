d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), query_result_proto_wrapper_field.cc \
				query_result_proto_wrapper_row.cc query_result_proto_wrapper.cc \
				query_result_proto_builder.cc taopq_query_result_wrapper_field.cc \
				taopq_query_result_wrapper_row.cc taopq_query_result_wrapper.cc)

PROTOS += $(addprefix $(d), query-result-proto.proto)

LIB-store-common += $(o)query-result-proto.o \
        $(o)query_result_proto_wrapper.o $(o)query_result_proto_wrapper_row.o \
		$(o)query_result_proto_wrapper_field.o $(o)query_result_proto_builder.o \
		$(o)taopq_query_result_wrapper.o $(o)taopq_query_result_wrapper_row.o \
		$(o)taopq_query_result_wrapper_field.o

