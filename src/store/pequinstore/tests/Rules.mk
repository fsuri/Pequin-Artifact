d := $(dir $(lastword $(MAKEFILE_LIST)))

#GTEST_SRCS += $(addprefix $(d), common-test.cc server-test.cc common.cc)

# $(d)common-test: $(o)common-test.o $(LIB-indicus-store) \
# 		$(GTEST_MAIN) $(o)common.o $(GMOCK)

#$(d)server-test: $(o)server-test.o $(LIB-indicus-store) \
		$(GTEST_MAIN) $(o)common.o $(GMOCK)


# TEST_BINS += $(d)common-test $(d)server-test

SRCS += $(addprefix $(d), proto_bench.cc tbb_test.cc compression_test.cc snapshot_test.cc)

$(d)proto_bench: $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(o)proto_bench.o
$(d)tbb_test: $(o)tbb_test.o
$(d)compression_test: $(LIB-transport) $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(LIB-pequin-common) $(o)compression_test.o
$(d)snapshot_test: $(LIB-transport) $(LIB-latency) $(LIB-crypto) $(LIB-batched-sigs) $(LIB-store-common) $(LIB-proto) $(LIB-pequin-common) $(o)snapshot_test.o

BINS += $(d)proto_bench $(d)tbb_test $(d)compression_test $(d)snapshot_test
