d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), table_kv_encoder.cc)

LIB-store-backend-sql-engine := 
LIB-store-backend-sql-encoding:= $(o)table_kv_encoder.o

