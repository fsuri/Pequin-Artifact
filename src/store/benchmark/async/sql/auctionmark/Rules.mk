d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), auctionmark_client.cc auctionmark_transaction.cc new_user.cc \
		new_item.cc new_bid.cc new_comment.cc new_comment_response.cc  new_purchase.cc \
		new_feedback.cc get_item.cc update_item.cc check_winning_bids.cc post_auction.cc \
		get_comment.cc get_user_info.cc get_watched_items.cc category_parser.cc \
		auctionmark_generator.cc auctionmark_utils.cc)

OBJ-auctionmark-transaction := $(LIB-store-frontend) $(o)auctionmark_transaction.o

OBJ-auctionmark-client := $(o)auctionmark_client.o

LIB-auctionmark := $(OBJ-auctionmark-client) $(OBJ-auctionmark-transaction) $(o)new_user.o \
					$(o)new_item.o $(o)new_bid.o $(o)new_comment.o $(o)new_comment_response.o \
					$(o)new_purchase.o $(o)new_feedback.o $(o)get_item.o $(o)update_item.o \
					$(o)check_winning_bids.o $(o)post_auction.o $(o)get_comment.o $(o)get_user_info.o \
					$(o)get_watched_items.o $(o)category_parser.o $(o)auctionmark_utils.o

$(d)auctionmark_generator: $(LIB-io-utils) $(o)auctionmark_generator.o $(o)category_parser.o $(o)auctionmark_utils.o

BINS += $(d)auctionmark_generator
