d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), sqlhelper.cc)

#PROTOS += $(addprefix $(d), queryprocess.proto)

#LIB-queryprocess-util := $(o)sqlhelper.o


#LIB-proto := $(o)pequin-proto.o $(o)query-proto.o
#-I/home/floriansuri/Research/Projects/Pequin/Pequin-Artifact/src/store/common
#$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o

#BINS += $(d)proto_bench

#include $(d)tests/Rules.mk

