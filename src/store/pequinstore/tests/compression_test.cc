/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/

#include <iostream>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>
//#include"simdcomp/include/simdfor.h"
//#include"simdcomp/include/simdcomp.h"

#include <cstring>
#include <vector>
#include <iomanip>
#include <algorithm>
#include "lib/compression/FrameOfReference/include/util.h"
#include "lib/compression/FrameOfReference/include/compression.h"
#include "lib/compression/FrameOfReference/include/turbocompression.h"

//#include "TurboPFor-Integer-Compression/vint.h"
#include "lib/compression/TurboPFor-Integer-Compression/vp4.h"
//#include "TurboPFor-Integer-Compression/bitpack.h"

#include "store/pequinstore/common.h"
//#include "store/common/timestamp.h"

using namespace std;
using namespace pequinstore;

// for tests: Use current time, and then randomly add/subtract values

/////////////// Timestamps
void timestamp_test(){
  struct timeval now;
	uint64_t timestamp1;
	uint64_t timestamp2;

	gettimeofday(&now, NULL);

  uint64_t randomness = std::rand() % 10;
	now.tv_usec += randomness;
    if (now.tv_usec > 999999) {
        now.tv_usec -= 1000000;
        now.tv_sec++;
    } else if (now.tv_usec < 0) {
        now.tv_usec += 1000000;
        now.tv_sec--;
    }

	timestamp1 = ((uint64_t)now.tv_sec << 32) | (uint64_t) (now.tv_usec);
	timestamp2 = ((uint64_t)now.tv_sec << 20) | (uint64_t) (now.tv_usec);
	//TODO: u_sec is a max of 2^20, so shifting 32 is enough ==> 52 bit total for time. 12 left over for ids.

	fprintf(stderr, "Time: %lx %lx %lx %lx \n", (uint64_t) now.tv_sec, (uint64_t) now.tv_usec,timestamp1, timestamp2);

}

////////////////  //////////////// 64 bit Test
void bit64_tests(){
  
  struct timeval now;
	std::vector<uint64_t> timestamps;
	
	uint64_t sum = 0;
	uint64_t bonus_sec = std::rand() % 3;
	
	for(int i=0; i<1000; ++i){
		uint64_t timestamp;
		gettimeofday(&now, NULL);
  		uint64_t randomness = 100;  

		now.tv_usec += randomness;
		if (now.tv_usec > 999999) {
		    now.tv_usec -= 1000000;
		    now.tv_sec++;
		} else if (now.tv_usec < 0) {
		    now.tv_usec += 1000000;
		    now.tv_sec--;
		}
		
		uint64_t client_id = std::rand() % 2^12;
		timestamp = (((uint64_t)now.tv_sec + bonus_sec) << 32) | (uint64_t) (now.tv_usec) << 12 | client_id;
		bonus_sec = std::rand() % 2;  //90 --> exp maximum 90 sec..) //1200; // all 10k tx are 20 min apart
		//input unsorted, and in total 3 sec apart.

			//uint64_t back = timestamps.back();
		uint64_t insert = timestamp; // - sum;
		std::cerr << "sum: " << sum << std::endl;
		std::cerr << "new: " << timestamp << std::endl;
		timestamps.push_back(insert);
		std::cerr << "Ins: " << insert << std::endl;
		std::cerr << "bonus: " << bonus_sec << std::endl; 
		std::cerr << "b-shift: " << (bonus_sec << 32) << std::endl; 
		sum+=insert;
	}
	
	std::sort(timestamps.begin(), timestamps.end());
	sum = timestamps[0];
	for(int i=0; i<timestamps.size(); ++i){
		//timestamps[i]=timestamps[i] - sum;
		sum+=timestamps[i]; 
		std::cerr << "timestamp: " << (timestamps[i]) << std::endl;
	}
	//std::sort(timestamps.begin(), timestamps.end());
	 
	std::cerr << "begin compression" << std::endl;

	//uint64_t *inputdata = timestamps.data();
	
	vector<uint8_t> comp(8 * timestamps.size() + 1024);
  	   
	uint8_t *out = turbocompress64(timestamps.data(), timestamps.size(), comp.data());
	// compressed data lies between compresseddata and out
	cout << "compression rate:" << std::setprecision(2)
       << timestamps.size() * 1.0 * sizeof(uint64_t) / (out - comp.data())
       << endl;
	cout << "bits/int:" << std::setprecision(4)
		   << (out - comp.data()) * 8.0 / timestamps.size() << endl;
	uint32_t compsize = (out - comp.data());
	std::cerr << "TESTING SIZE FRAMEOFREF (bytes): "<< compsize << std::endl;
	comp.resize(compsize);
	uint32_t nvalue = 0;
	vector<uint64_t> recover(timestamps.size() + 1024);
	
	turbouncompress64(comp.data(), recover.data(), nvalue);
	recover.resize(nvalue);

	fprintf(stderr, "Nvalue: %d. Timestamps: %lu %lu  \n", nvalue, timestamps[0], timestamps[1]);
	//return timestamp;
	
////////////////////////////////////////////////////////

    //TURBOPFOR
    
    std::vector<unsigned char> vp_compress(8*timestamps.size() + 1024);
    
    std::cerr << "TESTING. First ts:" << timestamps[0] << " Last ts: " << timestamps.back() << std::endl;
	size_t compressed_size = p4ndenc64(timestamps.data(), timestamps.size(), vp_compress.data()); //use p4ndenc for sorted
	std::cerr << "TESTING SIZE (bytes): "<< compressed_size << std::endl;  //232 * 8; 1000*64
	std::cerr << "TESTING SIZE (bit/int): " << std::setprecision(4) << (compressed_size * 8) / timestamps.size() << std::endl;  //232 * 8; 1000*64
	compressed_size = p4nddec64(vp_compress.data(), timestamps.size(), recover.data());
	std::cerr << "TESTING. First recovered ts:" << recover[0] << " Last ts: " << timestamps.back() << std::endl;
}

////////////////	//////////////// 32 bit Test
void bit32_tests(){
  std::cerr << "begin 32 compression" << std::endl;

  struct timeval now;
	
  std::vector<uint64_t> timestamps;
  uint64_t sum = 0;
	uint64_t bonus_sec = std::rand() % 3;
	
	for(int i=0; i<100; ++i){
		uint64_t timestamp;
		gettimeofday(&now, NULL);
  		uint64_t randomness = std::rand() % 1000000;  

		now.tv_usec += randomness;
		if (now.tv_usec > 999999) {
		    now.tv_usec -= 1000000;
		    now.tv_sec++;
		} else if (now.tv_usec < 0) {
		    now.tv_usec += 1000000;
		    now.tv_sec--;
		}
		
		bonus_sec = std::rand() % 2;  //90 --> exp maximum 90 sec..) //1200; // all 10k tx are 20 min apart
		//input unsorted, and in total 3 sec apart.
		timestamp = (((uint64_t)now.tv_sec + bonus_sec) << 20) | (uint64_t) (now.tv_usec);

		uint64_t insert = timestamp; // - sum;
		timestamps.push_back(insert);
	}
	
	std::sort(timestamps.begin(), timestamps.end());
	sum = timestamps[0];
	for(int i=0; i<timestamps.size(); ++i){
		std::cerr << "timestamp: " << (timestamps[i]) << std::endl;
		timestamps[i]=timestamps[i] - sum;
		sum+=timestamps[i]; 
		std::cerr << "delta: timestamp: " << (timestamps[i]) << std::endl;
		
	}
	//std::vector<uint32_t> input;
	std::cerr << "Test base: " << timestamps[0] << std::endl;
	std::cerr << "Test downcast: " << ((uint32_t) timestamps[0]) << std::endl;
	std::vector<uint32_t> input(timestamps.begin(), timestamps.end());
	std::sort(input.begin(), input.end());
	for(int i=0; i<input.size(); ++i){
		std::cerr << "32 delta timestamp: " << (input[i]) << std::endl;
	}
	//std::transform(timestamps.begin(), timestamps.end(), input.begin(), [](uint64_t x) {return (uint32_t) x;}  );
	 
	//std::cerr << "begin 32 compression" << std::endl;
	
	uint32_t * inputdata = input.data(); // length values
	std::vector<uint8_t> compressed(4* input.size() + 1024);
	uint8_t * compresseddata = compressed.data(); // enough data
	uint8_t *out = turbocompress(inputdata, input.size(), compresseddata);
	// compressed data lies between compresseddata and out
	
	cout << "compression rate:" << std::setprecision(2)
       << input.size() * 1.0 * sizeof(uint32_t) / (out - compressed.data())
       << endl;
	cout << "bits/int:" << std::setprecision(4)
		   << (out - compressed.data()) * 8.0 / input.size() << endl;
	uint32_t compsize = (out - compressed.data());
	std::cerr << "compsize:" << compsize << std::endl;  //10*32 = 320  ==> 52*8=416
	compressed.resize(compsize);
	
	uint32_t nvalue = 0;
	std::vector<uint32_t> recovery(input.size() + 1024);
	uint32_t * recoverydata = recovery.data(); // available buffer with at least length elements
	turbouncompress(compresseddata, recoverydata, nvalue);
	// nvalue will be equal to length
	
	//TURBO PFOR
	std::vector<unsigned char> vp_compress32(4*input.size() + 1024);
    
    std::cerr << "TESTING. First ts:" << input[0] << " Last ts: " << input.back() << std::endl;
	size_t compressed_size = p4ndenc32(input.data(), input.size(), vp_compress32.data()); //use p4ndenc for sorted
	std::cerr << "TESTING SIZE (bytes): "<< compressed_size << std::endl;  //232 * 8; 1000*64
	std::cerr << "TESTING SIZE (bit/int): " << std::setprecision(4) << (compressed_size * 8) / timestamps.size() << std::endl;  //232 * 8; 1000*64
	compressed_size = p4nddec32(vp_compress32.data(), input.size(), recovery.data());
	std::cerr << "TESTING. First recovered ts:" << recovery[0] << " Last ts: " << timestamps.back() << std::endl;
}

void TimestampCompressorTest(){
	TimestampCompressor t_comp = TimestampCompressor();
	proto::LocalSnapshot *local_ss = new proto::LocalSnapshot();
	t_comp.InitializeLocal(local_ss, true);

	struct timeval now;
	TimestampMessage ts = TimestampMessage();

	uint64_t bonus_sec = std::rand() % 3;
	
	for(int i=0; i<5; ++i){
		uint64_t timestamp;
		gettimeofday(&now, NULL);
  		uint64_t randomness = std::rand() % 1000000;  

		now.tv_usec += randomness;
		if (now.tv_usec > 999999) {
		    now.tv_usec -= 1000000;
		    now.tv_sec++;
		} else if (now.tv_usec < 0) {
		    now.tv_usec += 1000000;
		    now.tv_sec--;
		}
		
		bonus_sec = std::rand() % 2;  //90 --> exp maximum 90 sec..) //1200; // all 10k tx are 20 min apart
		//input unsorted, and in total 3 sec apart.
		timestamp = (((uint64_t)now.tv_sec + bonus_sec) << 32) | ((uint64_t) now.tv_usec << 12);

		uint64_t client_id = std::rand() % (1 << 12);
		ts.set_timestamp(timestamp);
		ts.set_id(client_id);
		Debug("Generated Timestamp: %lx, Id: %lx", timestamp, client_id);

		t_comp.AddToBucket(ts);
	}

	for(const uint64_t ts: local_ss->local_txns_committed_ts()){
		Debug("Bucket Timestamp: %lx", ts);
	}

	t_comp.CompressAll();
	t_comp.ClearLocal();
	t_comp.DecompressAll();

	for(const uint64_t ts: local_ss->local_txns_committed_ts()){
		Debug("Decompressed Timestamp: %lx", ts);
	}


}
  

int main(){
	
  std::cerr<< "Running Compression tests" << std::endl;
 
  timestamp_test();
  
	bit64_tests();

  bit32_tests();
	
	////TODO:
  TimestampCompressorTest();
	
	return 0;

}

//Can I use a vector as input and output?
// probably not, want a byte format


/*
1. Lists of integers in random order.

const uint32_t b = maxbits(datain);// computes bit width
simdpackwithoutmask(datain, buffer, b);//compressed to buffer, compressing 128 32-bit integers down to b*32 bytes
simdunpack(buffer, backbuffer, b);//uncompressed to backbuffer

*/

/*
2. Sorted List.

Uses differential encoding (delta)

uint32_t offset = 0;
uint32_t b1 = simdmaxbitsd1(offset,datain); // bit width
simdpackwithoutmaskd1(offset, datain, buffer, b1);//compressing 128 32-bit integers down to b1*32 bytes
simdunpackd1(offset, buffer, backbuffer, b1);//uncompressed

*/

/*

int compress_decompress_demo() {
  size_t k, N = 9999;
  __m128i * endofbuf;
  uint32_t * datain = malloc(N * sizeof(uint32_t));
  uint8_t * buffer;
  uint32_t * backbuffer = malloc(N * sizeof(uint32_t));
  uint32_t b;

  for (k = 0; k < N; ++k){        // start with k=0, not k=1!
    datain[k] = k;
  }

  b = maxbits_length(datain, N);
  buffer = malloc(simdpack_compressedbytes(N,b)); // allocate just enough memory
  endofbuf = simdpack_length(datain, N, (__m128i *)buffer, b);
  // compressed data is stored between buffer and endofbuf using (endofbuf-buffer)*sizeof(__m128i) bytes
  // would be safe to do : buffer = realloc(buffer,(endofbuf-(__m128i *)buffer)*sizeof(__m128i));
  simdunpack_length((const __m128i *)buffer, N, backbuffer, b);

  for (k = 0; k < N; ++k){
    if(datain[k] != backbuffer[k]) {
      printf("bug\n");
      return -1;
    }
  }
  return 0;
}

*/
    
 