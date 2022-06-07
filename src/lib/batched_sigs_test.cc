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
#include "lib/batched_sigs.h"
#include "lib/assert.h"
#include "lib/latency.h"

#include <gflags/gflags.h>

#include <string>
#include <vector>
#include <iostream>
#include <random>

DEFINE_uint64(message_size, 32, "size of data to verify.");
DEFINE_uint64(batches, 1000, "number of iterations to measure.");
DEFINE_uint64(batch_size, 128, "number of iterations to measure.");
DEFINE_uint64(branch_factor, 128, "number of iterations to measure.");

void GenerateRandomString(uint64_t size, std::mt19937 &rd, std::string *s) {
  s->clear();
  for (uint64_t i = 0; i < size; ++i) {
    s->push_back(static_cast<char>(rd()));
  }
}

int main(int argc, char *argv[]) {
  gflags::SetUsageMessage("merkle hash benchmark.");
	gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::pair<crypto::PrivKey*, crypto::PubKey*> keypair = crypto::GenerateKeypair(crypto::SECP, false);
  crypto::PrivKey* privKey = keypair.first;
  crypto::PubKey* pubKey = keypair.second;

  Latency_t generateLat;
  _Latency_Init(&generateLat, "generate");

  Latency_t computeLat;
  _Latency_Init(&computeLat, "compute");

  Latency_t verifyLat;
  _Latency_Init(&verifyLat, "verify");

  std::mt19937 rd;
  std::vector<std::string *> messages;
  std::vector<const std::string *> constMessages;
  for (size_t i = 0; i < FLAGS_batches; ++i) {
    for (int j = 0; j < FLAGS_batch_size; ++j) {
      if (i == 0) {
        messages.push_back(new std::string());
        constMessages.push_back(messages[j]);
      }
      GenerateRandomString(FLAGS_message_size, rd, messages[j]);
    }
    std::vector<std::string> sigs;
    Latency_Start(&generateLat);
    BatchedSigs::generateBatchedSignatures(constMessages, privKey, sigs,
        FLAGS_branch_factor);
    Latency_End(&generateLat);
    for (size_t j = 0; j < messages.size(); ++j) {
      std::string hashStr;
      std::string rootSig;
      Latency_Start(&computeLat);
      UW_ASSERT(BatchedSigs::computeBatchedSignatureHash(&sigs[j], messages[j], pubKey,
          hashStr, rootSig, FLAGS_branch_factor));
      Latency_End(&computeLat);
      if (j == 0) {
        Latency_Start(&verifyLat);
        UW_ASSERT(crypto::Verify(pubKey, &hashStr[0], hashStr.length(),
            &rootSig[0]));
        Latency_End(&verifyLat);
      }
    }
  }

  std::cerr << "Merkle to crypto ratio: " << static_cast<double>(computeLat.dists['=']->total) / verifyLat.dists['=']->total << std::endl;

  for (auto msg : messages) {
    delete msg;
  }

  Latency_Dump(&generateLat);
  Latency_Dump(&computeLat);
  Latency_Dump(&verifyLat);

  return 0;
}
