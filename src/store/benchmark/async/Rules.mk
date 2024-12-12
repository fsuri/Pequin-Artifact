d := $(dir $(lastword $(MAKEFILE_LIST)))

SRCS += $(addprefix $(d), benchmark.cc benchmark_oneshot.cc bench_client.cc async_transaction_bench_client.cc sync_transaction_bench_client.cc)

OBJS-all-store-clients := $(OBJS-strong-client) $(OBJS-weak-client) \
		$(LIB-tapir-client) $(LIB-morty-client) $(LIB-janus-client) \
		$(LIB-indicus-client) $(LIB-pbft-store) $(LIB-hotstuff-store) $(LIB-hotstuff-pg-store) $(LIB-augustus-store) \
		$(LIB-bftsmart-store) $(LIB-bftsmart-augustus-store) $(LIB-bftsmart-stable-store) \
		$(LIB-pequin-client) $(LIB-postgres-client) $(LIB-blackhole-client) \
		$(LIB-cockroachdb-store) $(LIB-peloton-store) $(LIB-sintr-client)

LIB-bench-client := $(o)benchmark.o $(o)bench_client.o \
		$(o)async_transaction_bench_client.o $(o)sync_transaction_bench_client.o

OBJS-all-bench-clients := $(LIB-retwis) $(LIB-tpcc) $(LIB-sync-tpcc) $(LIB-async-tpcc) $(LIB-sql-tpcc) \
	$(LIB-smallbank) $(LIB-rw) $(LIB-toy) $(LIB-rw-sql) $(LIB-sql-seats) $(LIB-sql-tpcch) $(LIB-auctionmark)

$(d)benchmark: $(LIB-key-selector) $(LIB-bench-client) $(LIB-latency) $(LIB-tcptransport) $(LIB-udptransport) $(OBJS-all-store-clients) $(OBJS-all-bench-clients) $(LIB-bench-client) $(LIB-store-common)

BINS += $(d)benchmark
