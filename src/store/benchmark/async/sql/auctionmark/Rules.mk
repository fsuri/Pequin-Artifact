d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), auctionmark_client.cc auctionmark_transaction.cc new_user.cc category_parser.cc)

PROTOS += $(addprefix $(d), auctionmark.proto)

OBJ-auctionmark-transaction := $(LIB-store-frontend) $(o)auctionmark_transaction.o

OBJ-auctionmark-client := $(o)auctionmark_client.o

LIB-auctionmark := $(OBJ-auctionmark-client) $(OBJ-auctionmark-transaction) $(o)new_user.o $(o)category_parser.o

# $(d)auctionmark_generator: $(LIB-io-utils) $(o)auctionmark_generator.o $(o)category_parser.o

# BINS += $(d)auctionmark_generator
