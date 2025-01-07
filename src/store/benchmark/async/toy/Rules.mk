d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), toy_client.cc)

OBJ-toy := $(o)toy_client.o

LIB-toy := $(OBJ-toy)

