d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), query_executor.cc)

LIB-toystore-executor := $(o)query_executor.o
