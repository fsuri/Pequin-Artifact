d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), new_user.cc)
