d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), seats_client.cc seats_transaction.cc delete_reservation.cc find_flights.cc find_open_seats.cc new_reservation.cc update_customer.cc update_reservation.cc)

OBJ-sql-seats-transaction := $(LIB-store-frontend) $(o)seats_transaction.o

OBJ-sql-seats-client := $(o)seats_client.o

LIB-sql-seats := $(OBJ-sql-seats-client) $(OBJ-sql-seats-transaction) $(o)delete_reservation.o \
	$(o)find_flights.o $(o)find_open_seats.o $(o)new_reservation.o $(o)update_customer.o $(o)update_reservation.o
