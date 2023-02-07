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
#include "store/pequinstore/common.h"

#include <sstream>
#include <list>
#include <iomanip>

#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include <utility>

#include "lib/compression/TurboPFor-Integer-Compression/vp4.h"
#include "lib/compression/FrameOfReference/include/compression.h"
#include "lib/compression/FrameOfReference/include/turbocompression.h"


namespace pequinstore {

SnapshotManager::SnapshotManager(proto::Query *query, uint64_t replica_id, proto::SyncReply *syncReply, bool optimisticTxId, bool compressOptimisticTxIds): optimisticTxId(optimisticTxId) {
    local_ss = syncReply->mutable_local_ss();   //TODO: Figure out whats the best way to keep it cached (if we even want to). Probably need to copy if we want too keep this.
    syncReply->set_optimistic_tx_id(optimisticTxId);
    
    if(optimisticTxId) ts_comp.InitializeLocal(local_ss, compressOptimisticTxIds);
}
SnapshotManager::~SnapshotManager(){}



//TODO: Create Snapshot Manager class
// Attributes: LocalSnapshot (add compressed value to it), bucket structure for compression. (could be another struct). 
//Functions: Add, Close, Open, Iterate
//
void SnapshotManager::AddToSnapshot(std::string &txnDigest, proto::Transaction *txn, bool committed_or_prepared){ //optimistTxId = params.query_params.optimisticTxId && retry_version == 0.

  if(!optimisticTxId){
    //Just add txnDig to RepeatedPtr directly
    if(committed_or_prepared){ //TODO: Make one general structure for prepared/committed.
       local_ss->add_local_txns_committed(txnDigest);
    }
    else{
       local_ss->add_local_txns_prepared(txnDigest);
    }
    return;
  }
  else{
    ts_comp.AddToBucket(txn->timestamp());
  }
}

void SnapshotManager::CloseSnapshot(){
    
  if(optimisticTxId){
    ts_comp.CompressAll();
    //TODO: want to get return value and add it to local_ss
    //local_ss->mutable_compressed_ss() = ...
  }
  else{
    local_ss->mutable_local_txns_committed()->erase(std::unique(local_ss->mutable_local_txns_committed()->begin(), local_ss->mutable_local_txns_committed()->end()), local_ss->mutable_local_txns_committed()->end()); //Erase duplicates...
    //TODO: for prepared too.
     //else, do nothing:
  }
 
  return;
}

//Assume receiving client also calls to instantiate SnapshotManager with optimisticId and syncReply.
proto::LocalSnapshot* SnapshotManager::OpenSnapshot(){
  if(optimisticTxId){
    ts_comp.DecompressAll(); //TODO: Needs input = compressed id... //TODO: use output to write to local_ss. 
  }
  return local_ss;
}

// std::string AccessTxnId(  ){
//   //given input: Ts, ref to map that stores mapping.
//   //return txDigest
// }

//Problem: How to operate on snapshot at the client.
//TODO: add counting data structures inside the SnapshotManager. Let SnapshotManager also construct the MergedSS  --> Snapshot manager function takes in LocalSnapshot, and processes it. (I.e. it exposes functions to produce, and consume Local, and to produce and consume Merged)
//Client just calls: construct Merged Snapshot, it automatically loops through local_ss and computes the result.

 //1) store mapping between tx-dig and ts (think where is the best place to establish the mapping) -- throw error/report if 2 tx with same ts are mapped.
  //2) store timestamps directly into existing buckets within some delta (check vs smallest and largest val in bucket). If none, open new bucket.

  //close function: finalize all buckets and compress the vector using int compression lib. (possibly this works on protobuf STL repeated list directly, but maybe not)
  //open function: de-compress to reveal all timestamps
  //access wrapper for iterator: turn timestamp into tx by returning from mapping.

//TimestampCompressor:

//TODO: Need to refactor this so it is useable for merged snapshot too.. (just pass more arguments) //Or call this LocalTimestamp comp/make one for merged
TimestampCompressor::TimestampCompressor(){ //proto::LocalSnapshot *local_ss, bool compressOptimisticTxIds): local_ss(local_ss), compressOptimisticTxIds(compressOptimisticTxIds) {
  //TODO: Add reference to protobuf --> Instead of copying vector, just do operations on protobuf STL directly. Should work
  //ts_ids = local_ss.mutable_local_committed_txn(); //TODO: make this general purpose. (If not, then instantiate separate Timestamp Compressors for commit/prepared)
}
TimestampCompressor::~TimestampCompressor(){}

void TimestampCompressor::InitializeLocal(proto::LocalSnapshot *_local_ss, bool _compressOptimisticTxIds){
    local_ss = _local_ss;
    compressOptimisticTxIds = _compressOptimisticTxIds;

}


void TimestampCompressor::AddToBucket(const TimestampMessage &ts){

  const uint64_t &timestamp = ts.timestamp(); 
  const uint64_t &id = ts.id(); 

  //option1): store ts and id separately.
  // timestamps.push_back(timestamp);
  // ids.push_back(id);


  //option2): merge ts and id
 
  uint64_t time_mask = 0xFFF; //(1 << 12) - 1; //all bottom 12 bits = 1
  //1) Check whether Id < 2^12
  //uint64_t id_mask = ~0UL - (time_mask);

  Debug("Add to Bucket: Timestamp: %lx, Id: %lx, Timemask: %lx, TS & Mask: %lx ", timestamp, id, time_mask, (timestamp & time_mask));
  
  if(id > time_mask){
    Panic("Cannot merge id's this large.");
  }
  //TODO: FIXME: Client id's are 2^6 offsets.. we're running with up to 500 clients = 2^9 ==> 2^15 total.

  //2) Check whether TS already is shifted by 12, else do it manually here.
        //merge id into bottom bits of timestamp

  //        if((timestamp & time_mask) != timestamp){
  
  if( (timestamp & time_mask) == 0UL){ // no bottom 12 bit from mask should survive
    local_ss->add_local_txns_committed_ts((timestamp | id));
    timestamps.push_back((timestamp | id));
    
  }
  else{ //manually shift.
    Panic("Cannot merge id into timestamp; bottom bits not free.");
    //top 32 bits (wipe bottom)
    uint64_t top = timestamp & ((uint64_t) (1 << 32 - 1) << 32);
    //bottom 20 bits
    uint64_t bot = timestamp & ((uint64_t) (1 << 20 - 1));
    //shift bottom bits and add to top
    uint64_t new_ts = top + (bot << 12);
    local_ss->add_local_txns_committed_ts((new_ts | id));
    timestamps.push_back((new_ts| id));
  }
   //merge id into bottom bits of timestamp
  //timestamps.push_back((timestamp | id));
  //local_ss->add_local_txns_committed_ts((timestamp | id));
}


    //put timestamps into vector in sorted order            --> run manual delta compression on this -> output is unsorted -> run through unsorted compression. OR: run through sorted compression directly. (this needs 32 bit though)
                                                                                                                                                                  //either try to prune to 32, or must use delta between 2 to produce 32 bit
                                                                                                                                                                  // or use 64bit compression alg.
    //put client id into vector in order to match the ts.   --> run unsorted compression here.

void TimestampCompressor::ClearLocal(){
  local_ss->clear_local_txns_committed_ts();
  timestamps.clear();
  //local_ss->mutable_local_txns_committed_ts()->Clear();
  return;
}

void TimestampCompressor::CompressAll(){
    // Test if I can allocate less space for timestamps:  if(timestamp << log(num_clients) < -1)
    // 1) change timestampedMessage/Timestamp = (timestamp, client) to be stored as one 64 bit Timestamp //alternatively, store both separately and compress..!!
    // 2) Create delta encoding:
    //     a) sort, b) subtract 2nd from 1st (and so on); store first value (offset)
    // 3) Throw integer compression at it. (Ideally 64 bit; but delta compression may have already made it 32 bit)


  //TODO: Erase duplicates.
  local_ss->mutable_local_txns_committed_ts()->erase(std::unique(local_ss->mutable_local_txns_committed_ts()->begin(), local_ss->mutable_local_txns_committed_ts()->end()), local_ss->mutable_local_txns_committed_ts()->end()); //Erase duplicates...
    //TODO: for prepared too.

  if(compressOptimisticTxIds){
  
    //1) sort (only if compressing)
    std::sort(timestamps.begin(), timestamps.end()); //TODO: replace this with the repeated field directly.
    std::sort(local_ss->mutable_local_txns_committed()->begin(), local_ss->mutable_local_txns_committed()->end());

    //2) compress
    //std::vector<unsigned char> vp_compress(8*timestamps.size() + 1024);
    num_timestamps = timestamps.size();
    num_timestamps = local_ss->mutable_local_txns_committed_ts()->size();
    Debug("Compressing %d Timestamps", num_timestamps);

    compressed_timestamps.reserve(8*num_timestamps + 1024);
    //TODO: use local data structure and then move it to bytes.
    //local_ss->mutable_local_txns_committed_ts_compressed()->Reserve(8*num_timestamps + 1024);

     UW_ASSERT((num_timestamps*64) < (1<<32) + 1024); //Ensure it can fit within a single protobuf byte field (max size 2^32); 1024 as safety buffer ==> Implies we can only have 2^26 = 67 million tx-ids in a snapshot for the current code


    std::cerr << "TESTING. First ts:" << timestamps[0] << " Last ts: " << timestamps.back() << std::endl;
    size_t compressed_size = p4ndenc64(timestamps.data(), num_timestamps, compressed_timestamps.data()); //use p4ndenc for sorted
    compressed_size = p4ndenc64(local_ss->mutable_local_txns_committed_ts()->mutable_data(), num_timestamps, compressed_timestamps.data()); //(unsigned char*) local_ss->mutable_local_txns_committed_ts_compressed()); //use p4ndenc for sorted  (output = num bytes)
    //compressed_timestamps.resize(compressed_size);
    //local_ss->mutable_local_txns_committed()->Resize((int) compressed_size);
    Debug("Compressed size: %d. Compression factor: %f, Bits/Timestamp: %d", compressed_size, (float)(num_timestamps * sizeof(uint64_t) /  (float) compressed_size), ((compressed_size * 8) / num_timestamps));

  }
  else{
    //return unsorted (but merged Timestamps)
  }
    
return;  //==> snapshot set is in committed_ts() if uncompressed, and in committed() if compressed

  //If trying to produce 32 bit inputs for compressor: May need to bucketize:
  // // Split vectors into delta buckets; req: all numbers < 32 bit  ==> resulting buckets are unsorted
  // std::vector<uint64_t> compressable;
  // std::vector<uint64_t> non_compressable;
}

void TimestampCompressor::DecompressAll(){

  if(compressOptimisticTxIds){
      //decompress datastructure
    Debug("Decompressing %d Timestamps", num_timestamps);
    out_timestamps.clear();
    out_timestamps.reserve(num_timestamps);// + 1024);
    out_timestamps.resize(num_timestamps);
    size_t compressed_size = p4nddec64(compressed_timestamps.data(), num_timestamps, out_timestamps.data());
    
    local_ss->mutable_local_txns_committed_ts()->Reserve(num_timestamps + 1024);
    local_ss->mutable_local_txns_committed_ts()->Resize(num_timestamps, 0UL); //default value
    compressed_size = p4nddec64(compressed_timestamps.data(), num_timestamps, local_ss->mutable_local_txns_committed_ts()->mutable_data());
    //FIXME: Cannot work on repeated field --> Try storing compressed as a single bytes field. If not possible, must resort to copying/moving.


    Debug("Finished decompressing %d bytes into %d timestamps", compressed_size, out_timestamps.size());

  }
  return; //result is in committed_ts
}



} // namespace pequinstore

