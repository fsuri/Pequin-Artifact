d := $(dir $(lastword $(MAKEFILE_LIST)))


dt := ./TurboPFor-Integer-Compression/
LIB-turbo-pfor := $(dt)vint.o $(dt)vp4c.o  $(dt)vp4d.o $(dt)bitpack.o $(dt)bitpack_sse.o $(dt)bitpack_avx2.o $(dt)bitunpack.o $(dt)bitunpack_sse.o $(dt)bitunpack_avx2.o $(dt)bitutil.o 

LIB-frame-of-ref := ./FrameOfReference/bpacking.o

LIB-compression := $(LIB-turbo-pfor) $(LIB-frame-of-ref)

EXT_OBJS += $(LIB-compression)