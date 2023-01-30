d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), CreateStatement.cc Expr.cc PrepareStatement.cc SQLStatement.cc statements.cc)

#PROTOS += $(addprefix $(d), queryprocess.proto)

#LIB-queryprocess-sql := $(o)CreateStatement.o \
	$(o)Expr.o $(o)PrepareStatement.o $(o)SQLStatement.o $(o)statements.o


#LIB-proto := $(o)pequin-proto.o $(o)query-proto.o
#-I/home/floriansuri/Research/Projects/Pequin/Pequin-Artifact/src/store/common
#$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o

#BINS += $(d)proto_bench

#include $(d)tests/Rules.mk

