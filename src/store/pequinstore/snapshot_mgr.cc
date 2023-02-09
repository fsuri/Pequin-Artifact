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

SnapshotManager::SnapshotManager(const QueryParameters *query_params): query_params(query_params), numSnapshotReplies(0UL), useOptimisticTxId(false), config_f(0UL), local_ss(nullptr), merged_ss(nullptr) {}
SnapshotManager::~SnapshotManager(){}

////////////////// Manage Local Snapshot:

void SnapshotManager::InitLocalSnapshot(proto::LocalSnapshot *_local_ss, const uint64_t &query_seq_num, const uint64_t &client_id, const uint64_t &replica_id, bool _useOptimisticTxId){
  useOptimisticTxId = _useOptimisticTxId;
  local_ss = _local_ss;
  local_ss->Clear();
  local_ss->set_query_seq_num(query_seq_num);
  local_ss->set_client_id(client_id);
  local_ss->set_replica_id(replica_id);

  //t_comp.InitializeLocal(local_ss, query_params->compressOptimisticTxIDs);
}

void SnapshotManager::ResetLocalSnapshot(bool _useOptimisticTxId){
  useOptimisticTxId = _useOptimisticTxId;
  if(local_ss){
    local_ss->clear_local_txns_committed();  //note, if ts are compressed, they might be stored in here.
    local_ss->clear_local_txns_committed_ts(); 
    local_ss->clear_local_txns_prepared();
    local_ss->clear_local_txns_prepared_ts();

    local_ss->clear_local_txns_committed_ts_compressed();  //Note: max length 2^32 
    local_ss->clear_local_txns_prepared_ts_compressed(); //Note: max length 2^32 
 
  }
}


uint64_t MergeTimestampId(const uint64_t &timestamp, const uint64_t &id){
   uint64_t time_mask = 0xFFF; //(1 << 12) - 1; //all bottom 12 bits = 1
  Debug("Merging Timestamp: %lx, Id: %lx, Timemask: %lx, TS & Mask: %lx ", timestamp, id, time_mask, (timestamp & time_mask));
    
    //1) Check whether Id < 2^12
    //uint64_t id_mask = ~0UL - (time_mask);
    if(id > time_mask){
      Panic("Cannot merge id's this large.");   //TODO: FIXME: Client id's are 2^6 offsets.. we're running with up to 500 clients = 2^9 ==> 2^15 total.
    }

    //2) Check whether TS already is shifted by 12, else do it manually here.  //merge id into bottom bits of timestamp
    if( (timestamp & time_mask) == 0UL){ // no bottom 12 bit from mask should survive
      return (timestamp | id); 
    }
    else{ //manually shift.
      Panic("Cannot merge id into timestamp; bottom bits not free.");
      uint64_t top = timestamp & ((uint64_t) (1 << 32 - 1) << 32);  //top 32 bits (wipe bottom)
      uint64_t bot = timestamp & ((uint64_t) (1 << 20 - 1));  //bottom 20 bits
      uint64_t new_ts = top + (bot << 12); //shift bottom bits and add to top
      return (new_ts | id);
    }
}

void SnapshotManager::AddToLocalSnapshot(const std::string &txnDigest, const proto::Transaction *txn, bool committed_or_prepared){ //optimistTxId = params.query_params.optimisticTxId && retry_version == 0.

  if(!useOptimisticTxId){ //Add txnDigest to snapshot
    //Just add txnDig to RepeatedPtr directly  //TODO: Make one general structure for prepared/committed.
    committed_or_prepared? local_ss->add_local_txns_committed(txnDigest) : local_ss->add_local_txns_prepared(txnDigest);
  }
  else{ //Add (merged) Timestamp to snapshot
    //ts_comp.AddToBucket(txn->timestamp()); //If we want to compute a delta manually first use this.

    // merge ts and id into one 64 bit number and add to snapshot
    const uint64_t &timestamp = txn->timestamp().timestamp(); 
    const uint64_t &id = txn->timestamp().id(); 
    committed_or_prepared? local_ss->add_local_txns_committed_ts(MergeTimestampId(timestamp, id)) : local_ss->add_local_txns_prepared_ts(MergeTimestampId(timestamp, id));
  
  }
  return;
}

void SnapshotManager::SealLocalSnapshot(){
   //Erase duplicates... Could have inserted duplicates if we read multiple keys from the same tx.   //NOTE: committed/prepared might have the same tx ids if we read from committed first, but prepared has not been cleaned yet. (Actions are not atomic)
  if(!useOptimisticTxId){
    local_ss->mutable_local_txns_committed()->erase(std::unique(local_ss->mutable_local_txns_committed()->begin(), local_ss->mutable_local_txns_committed()->end()), local_ss->mutable_local_txns_committed()->end()); 
    local_ss->mutable_local_txns_prepared()->erase(std::unique(local_ss->mutable_local_txns_prepared()->begin(), local_ss->mutable_local_txns_prepared()->end()), local_ss->mutable_local_txns_prepared()->end()); 
  }
  else{
    local_ss->mutable_local_txns_committed_ts()->erase(std::unique(local_ss->mutable_local_txns_committed_ts()->begin(), local_ss->mutable_local_txns_committed_ts()->end()), local_ss->mutable_local_txns_committed_ts()->end()); 
    local_ss->mutable_local_txns_prepared_ts()->erase(std::unique(local_ss->mutable_local_txns_prepared_ts()->begin(), local_ss->mutable_local_txns_prepared_ts()->end()), local_ss->mutable_local_txns_prepared_ts()->end()); 
      //ts_comp.CompressAll();
    //For optimistic Ids (TS) additionally optionally compress. 
    for(auto &ts_id: local_ss->local_txns_committed_ts()){
      printf("Local Snapshot contains TS: %lu \n", ts_id);
    }
    if(query_params->compressOptimisticTxIDs) ts_comp.CompressLocal(local_ss); //will write snapshot to local_ss.compressed and clear txns_committed/prepared_ts
  }
  return;
}

//Assume receiving client also calls to instantiate SnapshotManager with optimisticId and syncReply.
void SnapshotManager::OpenLocalSnapshot(proto::LocalSnapshot *local_ss){
  if(useOptimisticTxId && query_params->compressOptimisticTxIDs) ts_comp.DecompressLocal(local_ss);
  return;
}

void SnapshotManager::InitMergedSnapshot(proto::MergedSnapshot *_merged_ss, const uint64_t &query_seq_num, const uint64_t &client_id, const uint64_t &retry_version, const uint64_t &_config_f){
  config_f = _config_f;
  merged_ss = _merged_ss;
  merged_ss->Clear();
  merged_ss->set_query_seq_num(query_seq_num);
  merged_ss->set_client_id(client_id);
  merged_ss->set_retry_version(retry_version);
  //TODO: Ensure queryDigest is set too

  useOptimisticTxId = query_params->optimisticTxID && !retry_version; // Only true for first retry (to avoid fail resync due to equivocated timestamps)
  return;
}

//Functions to Generate MergedSnapshot at client:
bool SnapshotManager::ProcessReplicaLocalSnapshot(proto::LocalSnapshot* local_ss){
    //client calls ProcessReplicaLocalSnapshot (feeds in new local_snapshot)
    
    //1) Open Snapshot  
    OpenLocalSnapshot(local_ss); // decompresses if applicable.

    //2) Compute Merged Snapshot -- path for both tx-id and ts-id
    if(!useOptimisticTxId){
            //what if some replicas have it as committed, and some as prepared. If >=f+1 committed ==> count as committed, include only those replicas in list.. If mixed, count as prepared
        //DOES client need to consider at all whether a txn is committed/prepared? --> don't think so; replicas can determine dependency set at exec time (and either inform client, or cache locally)
        //TODO: probably don't need separate lists! --> FIXME: Change back to single list in protobuf.

        for(const std::string &txn_dig : local_ss->local_txns_committed()){
            proto::ReplicaList &replica_list = (*merged_ss->mutable_merged_txns())[txn_dig];
            replica_list.add_replicas(local_ss->replica_id());
            replica_list.set_commit_count(replica_list.commit_count()+1);
        }
        for(const std::string &txn_dig : local_ss->local_txns_prepared()){
            proto::ReplicaList &replica_list = (*merged_ss->mutable_merged_txns())[txn_dig];
            replica_list.add_replicas(local_ss->replica_id());
        }
    }
    else{
        for(const uint64_t &ts : local_ss->local_txns_committed_ts()){
            proto::ReplicaList &replica_list = (*merged_ss->mutable_merged_ts())[ts];
            replica_list.add_replicas(local_ss->replica_id());
            replica_list.set_commit_count(replica_list.commit_count()+1);
        }
        for(const uint64_t &ts : local_ss->local_txns_prepared_ts()){
            proto::ReplicaList &replica_list = (*merged_ss->mutable_merged_ts())[ts];
            replica_list.add_replicas(local_ss->replica_id());
            replica_list.set_commit_count(replica_list.commit_count()+1);
        }

        //NOTE: TODO: If we are trying to compress the Timestamps --> cannot store this as map from ts -> replica_lists.
    }
    //TODO: Is there a better way to store f+1 responsible replicas? Right now we might include the same replica ids #tx_ids many times.
    //Could of course simply not send this, and let the replica sync best effort, and then sync to all.
   

    //3) If last remaining Snapshot: Call SealMergedSnapshot --> outuput
    numSnapshotReplies++;
    if(numSnapshotReplies == query_params->syncQuorum){
         SealMergedSnapshot();
         return true;
    }
     std::cerr << "numSnapshotReplies: " << numSnapshotReplies << " syncquorum: " << query_params->syncQuorum<< std::endl;
    return false;

}

//OLD code for merging: Count in separate STL map; copy only if mergeThreshold.
    //  for(const std::string &txn_dig : local_ss->local_txns_committed()){
    //       std::set<uint64_t> &replica_set = txn_freq[txn_dig];
    //       replica_set.insert(local_ss->replica_id());
    //       if(replica_set.size() == query_params->mergeThreshold){
    //           *(*merged_ss.mutable_merged_txns())[txn_dig].mutable_replicas() = {replica_set.begin(), replica_set.end()}; //creates a temp copy, and moves it into replica list.
    //       }
    //       //TODO: Operate on merged_txn directly, and erase keys that don't have enough (requires an extra loop, but saves all the copies.)

    //     }
    //     // for(std::string &txn_dig : local_ss.local_txns_prepared()){ 
    //     //    pendingQueries->txn_freq[txn_dig].insert(local_ss->replica_id());
    //     // }
// map<string, ReplicaList> merged_txns = 3; //map from txn digest to replicas that have txn.
// map<string, ReplicaList> merged_txns_prepared = 4; //dont think one needs to distinguish at this point.

void SnapshotManager::SealMergedSnapshot(){
  if(!useOptimisticTxId){
     //remove all keys with insufficient replicas.
    for (auto it = merged_ss->mutable_merged_txns()->begin(), next_it = it; it != merged_ss->mutable_merged_txns()->end();  it = next_it){ /* not hoisted */; /* no increment */
        ++next_it;
        proto::ReplicaList &replica_list = it->second;
        if (replica_list.replicas().size() < query_params->mergeThreshold){
          merged_ss->mutable_merged_txns()->erase(it);   
          continue; 
        }
        replica_list.mutable_replicas()->Truncate(config_f +1);   //prune replica lists to be f+1 max.
        if(replica_list.commit_count() < config_f+1) replica_list.set_prepared(true);
    }
  }
  else{
     //remove all keys with insufficient replicas.
    for (auto it = merged_ss->mutable_merged_ts()->begin(), next_it = it; it != merged_ss->mutable_merged_ts()->end();  it = next_it){ /* not hoisted */; /* no increment */
        ++next_it;
        proto::ReplicaList &replica_list = it->second;
        if (replica_list.replicas().size() < query_params->mergeThreshold){
          merged_ss->mutable_merged_ts()->erase(it);    
          continue;
        }
        replica_list.mutable_replicas()->Truncate(config_f +1);   //prune replica lists to be f+1 max.
        if(replica_list.commit_count() < config_f+1) replica_list.set_prepared(true);
    }
    //TODO: If want to compress --> cannot store in a map -> must store ids and replicas in 2 lists. 

    //TODO: compress merged snapshot if applicable.
    //t_comp.CompressMerged();
  }
}

void SnapshotManager::OpenMergedSnapshot(proto::MergedSnapshot *merged_ss){
  //TODO: implement decompression if applicable
  return;
}


///////////////////////////

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

///////////////TimestampCompressor:

//TODO: Need to refactor this so it is useable for merged snapshot too..

TimestampCompressor::TimestampCompressor(){}
TimestampCompressor::~TimestampCompressor(){}

void TimestampCompressor::CompressLocal(proto::LocalSnapshot *local_ss){
  
  //Use integer compression to further compress the snapshots. Note: Currenty code expects 64 bit TS  -- For 32 bit need to perform explicit delta compression (requires bucketing) 

  //1) sort
  std::sort(local_ss->mutable_local_txns_committed_ts()->begin(), local_ss->mutable_local_txns_committed_ts()->end());
  std::sort(local_ss->mutable_local_txns_prepared_ts()->begin(), local_ss->mutable_local_txns_prepared_ts()->end());

  //2) compress
  //committed:
  num_timestamps = local_ss->mutable_local_txns_committed_ts()->size();
  UW_ASSERT((num_timestamps*64) < (1<<32) + 1024); //Ensure it can fit within a single protobuf byte field (max size 2^32); 1024 as safety buffer ==> Implies we can only have 2^26 = 67 million tx-ids in a snapshot for the current code
 
  Debug("Committed: Compressing %d Timestamps", num_timestamps);
  local_ss->mutable_local_txns_committed_ts_compressed()->reserve(8*num_timestamps + 1024);
  size_t compressed_size = p4ndenc64(local_ss->mutable_local_txns_committed_ts()->mutable_data(), num_timestamps, (unsigned char*) local_ss->mutable_local_txns_committed_ts_compressed()->data());
  local_ss->clear_local_txns_committed_ts();
  local_ss->set_num_committed_txn_ids(num_timestamps);
  Debug("Committed: Compressed size: %d. Compression factor: %f, Bits/Timestamp: %d", compressed_size, (float)(num_timestamps * sizeof(uint64_t) /  (float) compressed_size), ((compressed_size * 8) / num_timestamps));

  //prepared:
  num_timestamps = local_ss->mutable_local_txns_prepared_ts()->size();
  UW_ASSERT((num_timestamps*64) < (1<<32) + 1024); //Ensure it can fit within a single protobuf byte field (max size 2^32); 1024 as safety buffer ==> Implies we can only have 2^26 = 67 million tx-ids in a snapshot for the current code
 
  Debug("Prepared: Compressing %d Timestamps", num_timestamps);
  local_ss->mutable_local_txns_prepared_ts_compressed()->reserve(8*num_timestamps + 1024);
  compressed_size = p4ndenc64(local_ss->mutable_local_txns_prepared_ts()->mutable_data(), num_timestamps, (unsigned char*) local_ss->mutable_local_txns_prepared_ts_compressed()->data());
  local_ss->clear_local_txns_prepared_ts();
  local_ss->set_num_prepared_txn_ids(num_timestamps);
  Debug("Prepared: Compressed size: %d. Compression factor: %f, Bits/Timestamp: %d", compressed_size, (float)(num_timestamps * sizeof(uint64_t) /  (float) compressed_size), ((compressed_size * 8) / num_timestamps));


  return;  //snapshot is in comitted/prepared_ts_compressed; 
}

void TimestampCompressor::DecompressLocal(proto::LocalSnapshot *local_ss){
  
  //decompress committed
  const uint64_t &num_committed_timestamps = local_ss->num_committed_txn_ids();
    //decompress datastructure
  Debug("Committed: Decompressing %d Timestamps", num_committed_timestamps);
  local_ss->mutable_local_txns_committed_ts()->Reserve(num_committed_timestamps + 1024);
  local_ss->mutable_local_txns_committed_ts()->Resize(num_committed_timestamps, 0UL); //default value
  size_t compressed_size = p4nddec64((unsigned char*) local_ss->mutable_local_txns_committed_ts_compressed()->data(), num_committed_timestamps, local_ss->mutable_local_txns_committed_ts()->mutable_data());
  Debug("Committed: Finished decompressing %d bytes into %d timestamps", compressed_size, local_ss->local_txns_committed_ts().size());

  //decompress prepared
  const uint64_t &num_prepared_timestamps = local_ss->num_prepared_txn_ids();
    //decompress datastructure
  Debug("Prepared: Decompressing %d Timestamps", num_prepared_timestamps);
  local_ss->mutable_local_txns_prepared_ts()->Reserve(num_prepared_timestamps + 1024);
  local_ss->mutable_local_txns_prepared_ts()->Resize(num_prepared_timestamps, 0UL); //default value
  compressed_size = p4nddec64((unsigned char*) local_ss->mutable_local_txns_prepared_ts_compressed()->data(), num_prepared_timestamps, local_ss->mutable_local_txns_prepared_ts()->mutable_data());
  Debug("Prepared: Finished decompressing %d bytes into %d timestamps", compressed_size, local_ss->local_txns_prepared_ts().size());


  return; //snapshot is in committed/prepared_ts
}



//DEPRECATED BELOW HERE: Currently only used for testing.

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
    std::sort(local_ss->mutable_local_txns_committed_ts()->begin(), local_ss->mutable_local_txns_committed_ts()->end());

    //2) compress
    //std::vector<unsigned char> vp_compress(8*timestamps.size() + 1024);
    num_timestamps = timestamps.size();
    num_timestamps = local_ss->mutable_local_txns_committed_ts()->size();
    Debug("Compressing %d Timestamps", num_timestamps);

    compressed_timestamps.reserve(8*num_timestamps + 1024);
    //compressed_timestamps.resize(8*num_timestamps + 1024);
    //TODO: use local data structure and then move it to bytes.
    //local_ss->mutable_local_txns_committed_ts_compressed()->reserve(8*num_timestamps + 1024); //protobuf seems to handle this automatically
    //local_ss->mutable_local_txns_committed_ts_compressed()->resize(8*num_timestamps + 1024, '0');

    UW_ASSERT((num_timestamps*64 + 1024) < (0xFFFFFFFF)); //Ensure it can fit within a single protobuf byte field (max size 2^32); 1024 as safety buffer ==> Implies we can only have 2^26 = 67 million tx-ids in a snapshot for the current code

    std::string *test = new std::string(); 
    test->reserve(8*num_timestamps + 1024);
    test->resize(8*num_timestamps+1024);
    unsigned char *test_data = (unsigned char*) test->data();
    std::cerr << "TESTING. First ts:" << timestamps[0] << " Last ts: " << timestamps.back() << std::endl;
    size_t compressed_size = p4ndenc64(timestamps.data(), num_timestamps, compressed_timestamps.data()); //use p4ndenc for sorted
    compressed_size = p4ndenc64(local_ss->mutable_local_txns_committed_ts()->mutable_data(), num_timestamps, (unsigned char*) local_ss->mutable_local_txns_committed_ts_compressed()->data()); //use p4ndenc for sorted  (output = num bytes)
    

    std::string copy = std::string(compressed_timestamps.begin(), compressed_timestamps.end());
  
    
    for(int i=0; i<< 100; ++i){
       std::cerr << "Compressed data: " << compressed_timestamps.data()[i];
    }
    std::cerr << std::endl;
   

    //local_ss->set_local_txns_committed_ts_compressed(copy);
    delete test;
	
    
    //local_ss->set_allocated_local_txns_committed_ts_compressed(test);
    
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
    compressed_size = p4nddec64((unsigned char*) local_ss->mutable_local_txns_committed_ts_compressed()->data(), num_timestamps, local_ss->mutable_local_txns_committed_ts()->mutable_data());
  
    // std::string* test = local_ss->release_local_txns_committed_ts_compressed();
    // delete test;
    //free(test);
    //FIXME: Cannot work on repeated field --> Try storing compressed as a single bytes field. If not possible, must resort to copying/moving.


    Debug("Finished decompressing %d bytes into %d timestamps", compressed_size, out_timestamps.size());

  }
  return; //result is in committed_ts
}


/// De-precated functions of TimestampCompressor

//Un-used currently
void TimestampCompressor::InitializeLocal(proto::LocalSnapshot *_local_ss, bool _compressOptimisticTxIds){
    local_ss = _local_ss;
    compressOptimisticTxIds = _compressOptimisticTxIds;
}

//Un-used currently
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

} // namespace pequinstore

