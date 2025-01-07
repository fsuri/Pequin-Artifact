d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d))

LIB-store-backend-sql-engine := 

