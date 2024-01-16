d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), tpcch_client.cc tpcch_transaction.cc tpcch_generator.cc q1.cc q2.cc q3.cc q4.cc q5.cc q6.cc q7.cc q8.cc q9.cc q10.cc q11.cc q12.cc q13.cc q14.cc q15.cc q16.cc q17.cc q18.cc q19.cc q20.cc q21.cc q22.cc)

OBJ-sql-tpcch-transaction := $(LIB-store-frontend) $(o)tpcch_transaction.o

OBJ-sql-tpcch-client := $(o)tpcch_client.o

LIB-sql-tpcch := $(OBJ-sql-tpcch-client) $(OBJ-sql-tpcch-transaction) $(o)q1.o \
	$(o)q2.o $(o)q3.o $(o)q4.o $(o)q5.o $(o)q6.o $(o)q7.o $(o)q8.o $(o)q9.o \
	$(o)q10.o $(o)q11.o $(o)q12.o $(o)q13.o $(o)q14.o $(o)q15.o $(o)q16.o \
	$(o)q17.o $(o)q18.o $(o)q19.o $(o)q20.o $(o)q21.o $(o)q22.o

$(d)sql_tpcch_generator: $(LIB-io-utils) $(o)tpcch_generator.o 

BINS += $(d)sql_tpcch_generator