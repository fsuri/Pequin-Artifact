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
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_hash_map.h"

using namespace std;
using namespace pequinstore;

typedef tbb::concurrent_hash_map<std::string, std::pair<Timestamp, const proto::Transaction *>> preparedMap;
static preparedMap prepared;
static tbb::concurrent_unordered_map<std::string, proto::CommittedProof *> committed;

void test_snapshot_tx_id(){

  std::cerr << "TEST SNAPSHOT TX ID" << std::endl;
     uint64_t config_f = 0;
     QueryParameters query_params(true,
                                  2*config_f + 1, //syncQuorumSize,
                                  2*config_f + 1,    //  queryMessages,
                                  1*config_f + 1,    //  mergeThreshold,
                                  1*config_f + 1,    //  syncMessages,
                                  1*config_f + 1,    //  resultQuorum,
                                  false,  // FLAGS_pequin_query_eager_exec,
                                  false, 
                                  true,    //  FLAGS_pequin_query_read_prepared,
                                  false,    //  FLAGS_pequin_query_cache_read_set,
                                  false,    //  FLAGS_pequin_query_optimistic_txid,
                                  false,    //  FLAGS_pequin_query_compress_optimistic_txid, 
                                  false,    //  FLAGS_pequin_query_merge_active_at_client,
                                  false,    //  FLAGS_pequin_sign_client_queries,
                                  false,
                                  false    //  FLAGS_pequin_parallel_queries
                                  );

    proto::LocalSnapshot local_ss;
    bool useOptimistixTxId = false;

    SnapshotManager snapshot_mgr(&query_params);
    snapshot_mgr.InitLocalSnapshot(&local_ss, 0, 0, 0, useOptimistixTxId);

    for(auto const&[tx_id, proof] : committed){
        const proto::Transaction *txn = &proof->txn();
        snapshot_mgr.AddToLocalSnapshot(tx_id, txn, true);
        //printf("Proposing txn_id [%s] for local Query Sync State[%lu:%lu:%d] \n", tx_id.c_str(), 0,0, 0);
    }
    
    for(preparedMap::iterator i=prepared.begin(); i!=prepared.end(); ++i ) {
        const std::string &tx_id = i->first;
        const proto::Transaction *txn = i->second.second;
        snapshot_mgr.AddToLocalSnapshot(tx_id, txn, false);
        //printf("Proposing txn_id [%s] for local Query Sync State[%lu:%lu:%d] \n", tx_id.c_str(), 0,0, 0);
    }

        //committed[test_txn_id] = test_proof;  //this should allow other replicas to find it during sync.; but validation of commit proof will fail. Note: Will probably fail Panic because fields not set.
    snapshot_mgr.SealLocalSnapshot(); //Remove duplicate ids and compress if applicable.

    proto::MergedSnapshot merged_ss;

    snapshot_mgr.InitMergedSnapshot(&merged_ss, 0, 0, 0, config_f);

    bool finished_merge = snapshot_mgr.ProcessReplicaLocalSnapshot(&local_ss);

    if(!finished_merge) std:cerr << "Error: Not done with merge" << std::endl;

    snapshot_mgr.OpenMergedSnapshot(&merged_ss);

    for(auto &[tx_id, replica_list]: merged_ss.merged_txns()){
      printf("Merged Snapshot contains Tx-id: %s \n", tx_id.c_str());
    }

}

void test_snapshot_optimistic_tx_id(bool compress){

  std::cerr << "TEST SNAPSHOT OPTIMISTIC TX ID. Compress = " << compress << std::endl;
     uint64_t config_f = 0;
     QueryParameters query_params(true,
                                  2*config_f + 1, //syncQuorumSize,
                                  2*config_f + 1,    //  queryMessages,
                                  1*config_f + 1,    //  mergeThreshold,
                                  1*config_f + 1,    //  syncMessages,
                                  1*config_f + 1,    //  resultQuorum,
                                  false, // FLAGS_pequin_query_eager_exec,
                                  false, 
                                  true,    //  FLAGS_pequin_query_read_prepared,
                                  false,    //  FLAGS_pequin_query_cache_read_set,
                                  true,    //  FLAGS_pequin_query_optimistic_txid,
                                  compress,    //  FLAGS_pequin_query_compress_optimistic_txid, 
                                  false,    //  FLAGS_pequin_query_merge_active_at_client,
                                  false,    //  FLAGS_pequin_sign_client_queries,
                                  false, // FLAGS_pequin_sign_replica_to_replica
                                  false    //  FLAGS_pequin_parallel_queries
                                  );
    
    proto::LocalSnapshot local_ss;
    bool useOptimistixTxId = true;

    SnapshotManager snapshot_mgr(&query_params);
    snapshot_mgr.InitLocalSnapshot(&local_ss, 0, 0, 0, useOptimistixTxId);

    for(auto const&[tx_id, proof] : committed){
        const proto::Transaction *txn = &proof->txn();
        snapshot_mgr.AddToLocalSnapshot(tx_id, txn, true);
        //printf("Proposing txn_id [%s] for local Query Sync State[%lu:%lu:%d] \n", tx_id.c_str(), 0,0, 0);
    }
    for(preparedMap::iterator i=prepared.begin(); i!=prepared.end(); ++i ) {
        const std::string &tx_id = i->first;
        const proto::Transaction *txn = i->second.second;
        snapshot_mgr.AddToLocalSnapshot(tx_id, txn, false);
        //printf("Proposing txn_id [%s] for local Query Sync State[%lu:%lu:%d] \n", tx_id.c_str(), 0,0, 0);
    }

        //committed[test_txn_id] = test_proof;  //this should allow other replicas to find it during sync.; but validation of commit proof will fail. Note: Will probably fail Panic because fields not set.
    snapshot_mgr.SealLocalSnapshot(); //Remove duplicate ids and compress if applicable.

    proto::MergedSnapshot merged_ss;

    snapshot_mgr.InitMergedSnapshot(&merged_ss, 0, 0, 0, config_f);

    bool finished_merge = snapshot_mgr.ProcessReplicaLocalSnapshot(&local_ss);

    if(!finished_merge) std:cerr << "Error: Not done with merge" << std::endl;
    
    snapshot_mgr.OpenMergedSnapshot(&merged_ss);

    for(auto &[ts_id, replica_list]: merged_ss.merged_ts()){
      printf("Merged Snapshot contains TS: %lu \n", ts_id);
    }
  
}


int main(){

   std::cerr<< "Running Snapshot test" << std::endl;

  //Generate some committed values
  for(int i=0; i<5; ++i){
    uint64_t timestamp;
    struct timeval now;
		gettimeofday(&now, NULL);
		timestamp = (((uint64_t)now.tv_sec + i) << 32) | ((uint64_t) now.tv_usec << 12);

    std::string txn_id = "dummy_id" + std::to_string(i);

    proto::CommittedProof *proof = new proto::CommittedProof();
    proof->mutable_txn()->set_client_id(i);
    proof->mutable_txn()->set_client_seq_num(i);
    proof->mutable_txn()->mutable_timestamp()->set_timestamp(timestamp);
    proof->mutable_txn()->mutable_timestamp()->set_id(i);

    timestamp = timestamp | i;

    printf("Init committed with: txid[%s] with merged ts[%lu] \n", txn_id.c_str(), timestamp);

    committed.insert(std::make_pair(txn_id, proof));
  }

  //Generate some prepared values
  for(int i=5; i<10; ++i){
    uint64_t timestamp;
    struct timeval now;
		gettimeofday(&now, NULL);
		timestamp = (((uint64_t)now.tv_sec + i) << 32) | ((uint64_t) now.tv_usec << 12);

    std::string txn_id = "dummy_id" + std::to_string(i);

    proto::Transaction *txn = new proto::Transaction();
    txn->set_client_id(i);
    txn->set_client_seq_num(i);
    txn->mutable_timestamp()->set_timestamp(timestamp);
    txn->mutable_timestamp()->set_id(i);

    timestamp = timestamp | i;

    printf("Init prepared with: txid[%s] with merged ts[%lu] \n", txn_id.c_str(), timestamp);

    preparedMap::accessor p;
    prepared.insert(p, txn_id);
    p->second = std::make_pair(Timestamp(timestamp, i), txn);
    p.release();
  }


  test_snapshot_tx_id();

  bool compress = false;
  test_snapshot_optimistic_tx_id(compress);

  compress = true;
  test_snapshot_optimistic_tx_id(compress);

  for(auto [tx_id, proof]: committed){
    delete proof;
  }

  for(preparedMap::iterator i=prepared.begin(); i!=prepared.end(); ++i ) {
        const proto::Transaction *txn = i->second.second;
       delete txn;
    }

	return 0;

}
