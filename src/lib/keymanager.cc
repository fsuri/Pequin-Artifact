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
#include "lib/keymanager.h"

#include <string>
#include <sys/stat.h>

KeyManager::KeyManager(const std::string &keyPath, crypto::KeyType t, bool precompute,
   uint64_t replica_total, uint64_t client_total) :
  keyPath(keyPath), keyType(t), precompute(precompute), 
  num_replicas(replica_total), num_clients(client_total) {

  //Check if we have enough keys generated.
  std::string last_required_key_file = keyPath + "/" + std::to_string(replica_total+client_total-1) + ".priv";
  struct stat buffer;
  if(stat (last_required_key_file.c_str(), &buffer) != 0){
   Panic("Insufficient number of keys for number of replicas and clients. "
     "Require %d keys.", replica_total + client_total);
  }
}

KeyManager::~KeyManager() {
}

crypto::PubKey* KeyManager::GetPublicKey(uint64_t id) {
  std::unique_lock<std::mutex> lock(keyMutex);
  auto itr = publicKeys.find(id);
  if (itr == publicKeys.end()) {
    crypto::PubKey* publicKey =  crypto::LoadPublicKey(keyPath + "/" +
        std::to_string(id) + ".pub", keyType, precompute);
    auto pairItr = publicKeys.insert(std::make_pair(id, publicKey));
    return pairItr.first->second;
  } else {
    return itr->second;
  }
}

crypto::PrivKey* KeyManager::GetPrivateKey(uint64_t id) {
  std::unique_lock<std::mutex> lock(keyMutex);
  auto itr = privateKeys.find(id);
  if (itr == privateKeys.end()) {
    crypto::PrivKey* privateKey =  crypto::LoadPrivateKey(keyPath + "/" +
        std::to_string(id) + ".priv", keyType, precompute);
    auto pairItr = privateKeys.insert(std::make_pair(id, privateKey));
    return pairItr.first->second;
  } else {
    return itr->second;
  }
}

void KeyManager::PreLoadPubKeys(bool isServer){
  //Assumes Id's are perfectly matched to key space; Client keyIds start after servers.
  for(int id=0; id<(num_replicas+ isServer * num_clients); ++id){ //only loads client keys at replicas.
    GetPublicKey(id);
  }
}

void KeyManager::PreLoadPrivKey(uint64_t id, bool isClient){
  isClient? GetPrivateKey(GetClientKeyId(id)) : GetPrivateKey(id);
}

uint64_t KeyManager::GetClientKeyId(uint64_t client_id){
  return client_id + num_replicas;
}

//Todo add function support for Client keys also. Just add a second key path folder for those keys.
//Or split the keyspace (i.e. < 100 servers, > 100 clients...) -- check what keyspace replicas use.
//If I do this -- turn it into a flag or make it dynamic depending on the size of n (the latter seems best).
//Throw Panic error during start up if #available keys < required num for servers + clients.
//Load in required amount of keys preemptively, and not reactively

//Note: clientIds are shifted by 6, i.e. (x 64): first client process = 0, second client process = 64.., 
//      144th process = 9152 --> Cannot directly use id as is currently with only 1000 active keys.
//TODO: take information about threadcount into account to translate into a contiguous space.
//      e.g. "real ID" = (top bits) cID >> 6 + bottom bits (=tid) * client_total_processes
//      e.g. process 0, thread 0 => cID = 0, realID=0; p1,t0 => cID = 64, realId=1; p0,t1 = cID = 1, realId=144
//  Alternatively: realID = ((top bits) cID >> 6) * num_threads + bottom bits (=tid)

//TODO: instead of changing this conversion: Simply update the benchmark.cc -- since we pass the 
//number of threads, there is no need to blindly shift IDs by 6 bits if we only use 2 threads.