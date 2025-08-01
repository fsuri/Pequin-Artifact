# powturbo (c) Copyright 2013-2019
# Download or clone TurboPFor:
# git clone git://github.com/powturbo/TurboPFor.git 
# make
#
# To benchmark external libraries:
# git clone --recursive git://github.com/powturbo/TurboPFor.git 
#        make: "make CODEC1=1 CODEC2=1 LZ=1" 
# on arm make: "make CODEC1=1 LZ=1"
include ../../../Sanitizer.mk

CC ?= gcc
CXX ?= g++

#CC=powerpc64le-linux-gnu-gcc
CL = $(CC)
#DEBUG=-DDEBUG -g
DEBUG=-DNDEBUG -s

PREFIX ?= /usr/local
DIRBIN ?= $(PREFIX)/bin
DIRINC ?= $(PREFIX)/include
DIRLIB ?= $(PREFIX)/lib

#LDFLAGS+= $(SANITIZE_FLAG)
#CFLAGS+= $(SANITIZE_FLAG)
#CXXFLAGS+=$(SANITIZE_FLAG)
OPT=-fstrict-aliasing #$(SANITIZE_FLAG)
ifeq (,$(findstring clang, $(CC)))
OPT+=-falign-loops 
endif

#------- OS/ARCH -------------------
ifneq (,$(filter Windows%,$(OS)))
  OS := Windows
  CC=gcc
  CXX=g++
  ARCH=x86_64
else
  OS := $(shell uname -s)
  ARCH := $(shell uname -m)

ifneq (,$(findstring aarch64,$(CC)))
  ARCH = aarch64
else ifneq (,$(findstring powerpc64le,$(CC)))
  ARCH = ppc64le
endif
endif

ifeq ($(ARCH),ppc64le)
  SSE=-D__SSSE3__
  CFLAGS=-mcpu=power9 -mtune=power9 $(SSE)
else ifeq ($(ARCH),aarch64)
  CFLAGS=-march=armv8-a 
ifneq (,$(findstring clang, $(CC)))
  OPT+=-fomit-frame-pointer 
#-fmacro-backtrace-limit=0
endif
  SSE=-march=armv8-a
else ifeq ($(ARCH),$(filter $(ARCH),x86_64))
# set minimum arch sandy bridge SSE4.1 + AVX
  SSE=-march=corei7-avx -mtune=corei7-avx 
# SSE+=-mno-avx -mno-aes
  CFLAGS=$(SSE)
  AVX2=-march=haswell
#  SSE=$(AVX2)
endif

CFLAGS+=-w -Wall $(DEBUG) $(OPT) 

ifeq ($(OS),$(filter $(OS),Linux GNU/kFreeBSD GNU OpenBSD FreeBSD DragonFly NetBSD MSYS_NT Haiku))
LDFLAGS+=-lrt -lm
endif

# compiler supports float16
ifeq ($(FLOAT16),1)
CFLAGS+=-DUSE_FLOAT16
endif
ifeq ($(STATIC),1)
LDFLAGS+=-static
endif

all: icapp 

vp4c_sse.o: vp4c.c
	$(CC) -O3 -w $(SSE) -DSSE2_ON $(OPT) -c vp4c.c -o vp4c_sse.o

vp4c_avx2.o: vp4c.c
	$(CC) -O3 -w $(AVX2) -DAVX2_ON $(OPT) -c vp4c.c -o vp4c_avx2.o

vp4d_sse.o: vp4d.c
	$(CC) -O3 -w $(SSE) -DSSE2_ON $(OPT) -c vp4d.c -o vp4d_sse.o

vp4d_avx2.o: vp4d.c
	$(CC) -O3 -w $(AVX2) -DAVX2_ON $(OPT) -c vp4d.c -o vp4d_avx2.o

bitpack_sse.o: bitpack.c
	$(CC) -O3 -w $(SSE) -DSSE2_ON $(OPT) -c bitpack.c -o bitpack_sse.o

bitpack_avx2.o: bitpack.c
	$(CC) -O3 -w $(AVX2) -DAVX2_ON $(OPT) -c bitpack.c -o bitpack_avx2.o

bitunpack_sse.o: bitunpack.c
	$(CC) -O3 -w $(SSE) -DSSE2_ON $(OPT) -c bitunpack.c -o bitunpack_sse.o

bitunpack_avx2.o: bitunpack.c
	$(CC) -O3 -w $(AVX2) -DAVX2_ON $(OPT) -c bitunpack.c -o bitunpack_avx2.o

transpose_sse.o: transpose.c
	$(CC) -O3 -w $(SSE) -DSSE2_ON $(OPT) -c transpose.c -o transpose_sse.o

transpose_avx2.o: transpose.c
	$(CC) -O3 -w $(AVX2) -DAVX2_ON $(OPT) -c transpose.c -o transpose_avx2.o

-include ext/libext.mak

LIB=bitpack.o bitpack_sse.o bitunpack.o bitunpack_sse.o \
    vp4c.o vp4c_sse.o vp4d.o vp4d_sse.o \
	bitutil.o fp.o v8.o vint.o transpose.o transpose_sse.o trlec.o trled.o vsimple.o eliasfano.o
#bic.o 	
ifeq ($(ARCH),x86_64)
LIB+=bitpack_avx2.o bitunpack_avx2.o vp4c_avx2.o vp4d_avx2.o transpose_avx2.o
endif

libic.a: $(LIB)
	ar cr $@ $+

icapp: icapp.o libic.a $(OB)
	$(CL) $^ $(LDFLAGS) -o icapp

myapp: myapp.o libic.a
	$(CC) $^ $(LDFLAGS) -o myapp

mycpp: mycpp.o libic.a
	$(CXX) $^ $(LDFLAGS) -o mycpp

.c.o:
	$(CC) -O3 $(CFLAGS) $< -c -o $@  

.cc.o:
	$(CXX) -O2 $(CXXFLAGS) $< -c -o $@ 

.cpp.o:
	$(CXX) -O2 $(CXXFLAGS) $< -c -o $@ 

ifeq ($(OS),Windows_NT)
clean:
	del /S *.o
	del /S *~
else
clean:
	@find . -type f -name "*\.o" -delete -or -name "*\~" -delete -or -name "core" -delete -or -name "icbench" -delete -or -name "libic.a" -delete
	@find . -type f -name "icbench" -delete -or -name "idxqry" -delete -or -name "idxseg" -delete -or -name "idxcr" -delete -or -name "icapp" -delete
endif
