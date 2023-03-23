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


#include <cryptopp/sha.h>
#include <cryptopp/blake2.h>


#include "store/pequinstore/server.h"

#include <bitset>
#include <queue>
#include <ctime>
#include <chrono>
#include <sys/time.h>
#include <sstream>
#include <list>
#include <utility>

#include "lib/assert.h"
#include "lib/tcptransport.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/pequinstore/common.h"
#include "store/pequinstore/phase1validator.h"
#include "store/pequinstore/localbatchsigner.h"
#include "store/pequinstore/sharedbatchsigner.h"
#include "store/pequinstore/basicverifier.h"
#include "store/pequinstore/localbatchverifier.h"
#include "store/pequinstore/sharedbatchverifier.h"
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

namespace pequinstore {

//TODO: Problem: FIXME: Probably not safe to modify transaction. -- must hold ongoing lock? --> not possible..
//Solution: Store elsewhere (don't override read-set) -- Refactor CC and Prepare to take read set pointer as argument -- in non-query case, let that read set point to normal readset.
//Can we still edit read_set_merge field? If so, that is a good place to store it to re-use for Commit -- Test if that causes problems with sending out tx in parallel.
    //Note: 2 threads may try to call DoOCC for a Tx in parallel (barrier is only at BufferP1) -> they might try to write the same value in parallel. 
    //FIX: Hold ongoing lock while updating merged_set. + Check if merged_set exists, if so, re-use it.


//Problem: Don't want to wake Tx before we subscribed to ALL missing. 
// I.e. we don't want to subscribe to one missing, then it gets completed and triggers the TryPrepare; while we still were trying to add more missing.
// TODO: Must hold a per tx lock first. Queries hold query_md lock. If Query Exec gets it first, then it will update query_md cache, and check whether there is any tx waiting on the query
//; if there is, then the CC must have already held and released the query_md lock (added missing query). Query exec tries to take the Tx lock, but will fail until Tx finishes all tx.
//If there isn't, then query_md can't have been held yet. Query exec will take query_md lock after and see query.

//If the Transaction is waiting on a Query to be cached, call 
void Server::subscribeMissingQuery(const std::string &query_id, const uint64_t &retry_version, const std::string &txnDigest, uint64_t req_id, proto::Transaction *txn, const TransportAddress &remote, bool isGossip){ 
  //Note: This is being called while query_md is held. --> won't be released until we checked for all queries.
    subscribedQueryMap::accessor sq;
    subscribedQuery.insert(sq, query_id);
    sq->second = txnDigest;
    missingQueriesMap::accessor mq;
    bool first = missingQueries.insert(mq, txnDigest); 
    if(first){
      waitingOnQueriesMeta *waiting_meta = new waitingOnQueriesMeta(req_id, txn, remote, isGossip);
      mq->second = waiting_meta;
    }
    waitingOnQueriesMeta *waiting_meta = mq->second;


    waiting_meta->missing_query_id_versions[query_id] = retry_version;

    mq.release();
    sq.release();
}

// bool updateWaitingOnQuery(const std::string &txnDigest, const TransportAddress* remote){
//    missingQueriesMap::accessor mq;
//    bool is_waiting = missingQueries.find(mq, txnDigest); 
//     if(!is_waiting) return false; //either not asleep, or called after wakeup (i.e. would be missing out on reply)
    
//     waitingOnQueriesMeta *waiting_meta = mq->second;
//     waiting_meta->original_client=true;
//     waiting_meta->isGossip=false;
//     if(waiting_meta->remote != nullptr) delete waiting_meta->remote;
//     waiting_meta->remote = remote;

//   mq.release();
//   return true; //called before wakeup 
// }
//Note: It is possible that the client issues this request only after Tx has woken up, but before it has set 
//Problem: Gossip P1 -> Waiting on Query -> result = wait. original client sees wait, issues this call. But TryPrepare has already started in parallel (before the call completed). --> will not reply to original client.
//TODO: Either have a flag called started and allow the original client to re-do P1 (not worth it -- duplicate effort), or delete missingqueries only at the end. (better!)

void Server::wakeSubscribedQuery(const std::string query_id, const uint64_t &retry_version){
  //Note: This is being called while query_md is held. --> will trigger only before adding any queries, or after adding all
    std::string txnDigest;
    bool ready = false;
    waitingOnQueriesMeta *waiting_meta;
    //MissingQueryMeta *waking_tx_md = nullptr;

    subscribedQueryMap::accessor sq;
    bool has_subscriber = subscribedQuery.find(sq, query_id);
    if(has_subscriber){
      txnDigest = std::move(sq->second);

      missingQueriesMap::accessor mq;
      bool isMissing = missingQueries.find(mq, txnDigest);
      if(!isMissing) return;

      waiting_meta = mq->second;
      auto query_id_version = std::make_pair(query_id, retry_version);
      auto itr = waiting_meta->missing_query_id_versions.find(query_id); //Check if buffered query id + retry version is correct.
      if(itr != waiting_meta->missing_query_id_versions.end() || itr->second != retry_version) return; //Don't wake up.

      bool erased = waiting_meta->missing_query_id_versions.erase(query_id); //does nothing if not present
      if(erased && waiting_meta->missing_query_id_versions.empty()) ready = true; //only set ready the first time.
      
      missingQueries.erase(mq);
      mq.release();
      subscribedQuery.erase(sq);
    }
    sq.release();

    if(ready){ //Wakeup Tx for processing
      TryPrepare(waiting_meta->reqId, *waiting_meta->remote, waiting_meta->txn, txnDigest, waiting_meta->isGossip); //Includes call to HandlePhase1CB(..); 
      //TODO: Need to send to fallback clients too. (If we add tests for this)
      delete waiting_meta; //FIXME: If I delete waiting_meta will also delete remote ==> will cause segfault: copy remote again
      
    }


}

//returns pointer to query read set (either from cache, or from txn itself)
proto::ConcurrencyControl::Result Server::fetchQueryReadSet(const proto::QueryResultMetaData &query_md, proto::QueryReadSet const *query_rs){
    //pick respective server group from group meta
    const proto::QueryGroupMeta *query_group_md;
    auto query_itr = query_md.group_meta().find(groupIdx);
    //query_md.qroup_meta[id];
    if(query_itr != query_md.group_meta().end()){
      query_group_md = &query_itr->second;
    }
    else{
      query_rs = nullptr;
      return proto::ConcurrencyControl::COMMIT; //return empty readset; ideally don't return anything... -- could make this a void function, and pass return field as argument pointer.
    }
    if(query_group_md->has_query_read_set()){
      query_rs = &query_group_md->query_read_set();
    }
    else{ //else go to cache (via query_id) and check if query_result hash matches. if so, use read set.

      // If tx includes no read_set_hash => abort; invalid transaction //TODO: Could strengthen Abstain into Abort by incuding proof...
      if(!query_group_md->has_read_set_hash()) return proto::ConcurrencyControl::ABSTAIN;

      queryMetaDataMap::const_accessor q;
      bool has_query = queryMetaData.find(q, query_md.query_id());

      //1) Check whether the replica a) has seen the query, and b) has computed a result/read-set. If not ==> Stop processing
      if(!has_query || !q->second->has_result){
        Panic("query has no result cached yet");
        return proto::ConcurrencyControl::WAIT; //NOTE: Replying WAIT is a hack to not respond -- it will never wake up.
      } 

      QueryMetaData *cached_query_md = q->second;

      //2) Check for matching retry version. If not ==> Stop processing
      if(query_md.retry_version() != cached_query_md->retry_version){
          Panic("query has wrong retry version");
          return proto::ConcurrencyControl::WAIT; ///NOTE: Replying WAIT is a hack to not respond -- it will never wake up.
           //Note: Byz client can send retry version to relicas that have not gotten it yet. If we vote abstain, we may produce partial abort.
           //However, due to TCP FIFO we expect honest clients to send the retry version sync first -- thus it's ok to not vote/wait     //TODO: Ensure that multithreading does not violate this guarantee
           // ==>  Instead of aborting due to mismatched hash, should keep waiting/don't reply if retry vote doesnt match!
      }

      proto::QueryResultReply *cached_queryResultReply = cached_query_md->queryResultReply;

      //3) Check that replica has cached a read set, and that proposed read set hash matches cached read set hash. If not => return Abstain
      if(!cached_queryResultReply->has_result()){
        Panic("Result should be set");
        return proto::ConcurrencyControl::WAIT; //Checks whether result or signed result is set. Must contain un-signed result
      }
      const proto::QueryResult &cached_queryResult = cached_queryResultReply->result();
  
      if(!cached_queryResult.has_query_result_hash() || query_group_md->read_set_hash() != cached_queryResult.query_result_hash()) return proto::ConcurrencyControl::ABSTAIN;
      
      if(!cached_queryResult.has_query_read_set()) return proto::ConcurrencyControl::ABSTAIN;

      //TODO: Add check: Only client that issued the query may use the cached query in it's transaction ==> it follows that honest clients will not use the same query twice. (Must include txn as argument in order to check)

      //4) Use Read set.
      query_rs = &cached_queryResult.query_read_set();
  
      q.release();
    }
  
    return proto::ConcurrencyControl::COMMIT;
  }


//TODO:
// In DoOCCCheck make txn non-constant, only constant in MVTSOCheck
// then call mergeTxReadSets ==> move read sets + query sets into new field of txn called mergedTxn ==> use that for CC and Prepare
// afterwards, remove/revert that field
void Server::restoreTxn(proto::Transaction &txn){ //TODO: add to server.h
    if(txn.query_set().empty()) return;

    txn.mutable_read_set()->Swap(txn.mutable_read_set_copy());
    txn.clear_read_set_copy();
}

//TODO: Call this function before locking keys (only call if query set non empty.) ==> need to lock in this order; need to perform CC on this. 
// ==> Add the read set as a field to the txn. //FIXME: Txn can't be const in order to do that...  //Return value should be Result, in order to abort early if merge fails (due to incompatible duplicate)
proto::ConcurrencyControl::Result Server::mergeTxReadSets(proto::Transaction &txn){ //TODO: add to server.h
  //Note: Don't want to override transaction -> else digest changes / we cannot provide the right tx for sync (because stored txn is different)
  //TODO: store a backup of read_set; and override it for the timebeing (this way we don't have to change any of the DoMVTSOCheck and Prepare code.)
  
  if(txn.query_set().empty()) return proto::ConcurrencyControl::COMMIT;

  *txn.mutable_read_set_copy() = txn.read_set(); //store backup

  ReadSet *mergedReadSet = txn.mutable_read_set();

  std::vector<const std::string*> missing_queries; //List of query id's for which we have not received the latest version yet, but which the Tx is referencing.


  missingQueriesMap.accessor mq;
  missingQueries.find(mq, txnDigest); //Note: Reading should still take a write lock. (If not, can insert here, and erase at the end if empty.)
  //Hold write lock for the entirety of the loop ==> guarantee all of nothing.

  //fetch query read sets
  for(const proto::QueryResultMetaData &query_md : txn.query_set()){
    proto::QueryReadSet const *query_rs;
    proto::ConcurrencyControl::Result res = fetchQueryReadSet(query_md, query_rs); 

    if(res == proto::ConcurrencyControl::WAIT){ //Set up waiting.
      //TODO: Subscribe query.
      // Add to waitingOnQuery object.
      missing_queries.push_back(&query_md.query_id());
    }
    else if(res != proto::ConcurrencyControl::COMMIT){ //No need to continue CC check.
      return res; 
    }
    if(query_rs != nullptr && missing_queries.empty()){ //If we are waiting on queries, don't need to build mergedSet.
       //mergedReatSet.extend(query_rs);
        mergedReadSet->MergeFrom(query_rs->read_set()); //Merge from copies; If we don't want that, can loop through fetchedRead set and move 1 by 1.
        //mergedReadSet add readSet
    }
  }

  mq.release();

//TODO: STORE IN MERGED_READ_SET in its own data structure.
//FIXME: It is not safe to handle a Tx over multiple threads if one of them is writing to it. However, it seems to work fine for txnDigest field? Do we need to fix that?

  //TODO: Store mergedReadSet somewhere and only increment it on demand? Nah, easier to just re-do whole

  //Sort mergedReadSet  //NOTE: mergedReadSet will contain duplicate keys -- but those duplicates are guaranteed to be compatible (i.e. identical version/value) //Note: Lock function already ignores read duplicates.
      //Alternatively, instead of throwing error inside sort, just let CC handle it --> it's not possible for 2 different reads on the same key to vote commit. (but eager aborting here is more efficient)
    //Define sort function
    //In sort function: If we detect duplicate -> abort
    //Implement by throwing an error inside sort, and wrapping it with a catch block.
  if(params.parallel_CCC){
    try {
    //add mergedreadSet to tx - return success
    std::sort(mergedReadSet->begin(), mergedReadSet->end(), sortReadSetByKey);
    }
    catch(...) {
      //return abort. (return bool = success, and return abort as part of DoOCCCheck)
      restoreTxn(txn); //TODO: Maybe don't delete merged set -- we do want to use it for Commit again. //TODO: Maybe we cannot store mergedSet inside read after all? What if another thread tries to use Tx in parallel mid modification..
      return proto::ConcurrencyControl::ABSTAIN;
    }
  }
  

  return proto::ConcurrencyControl::COMMIT;
  //Invariant: If return true, then txn.read_set = merged active read sets --> use this for Occ 
}

//TODO: IMPORTANT: When caching read set, must ensure that DoOCCcheck is called only after sync request has finished --> must ensure they are serialized either on the same thread -- or use some lock per query id.
// Maybe the lock on QueryMetaData will do the trick. Note: However, even though TCP is FIFO, it could be that the syncProposal is received first, but executed after the Prepare -- that would be bad (unecessary abort)

proto::ConcurrencyControl::Result Server::DoOCCCheck(
    uint64_t reqId, const TransportAddress &remote,
    const std::string &txnDigest, proto::Transaction &txn, //const proto::Transaction &txn,
    Timestamp &retryTs, const proto::CommittedProof* &conflict,
    const proto::Transaction* &abstain_conflict,
    bool fallback_flow, bool isGossip) {

  //Merge
  proto::ConcurrencyControl::Result result;
  result = mergeTxReadSets(txn);
  //If we have an early abstain or Wait (due to invalid request) then return early.
  if(result != proto::ConcurrencyControl::COMMIT){
    return result;  //NOTE: Could optimize and turn Abstains into full Abort if we used duplicate reads as proof. (would have to distinguish from the abstains caused by cached mismatch)
  }
  //Note: if we wait, we may never garbage collect TX from ongoing (and possibly from other replicas Prepare set); Can garbage collect after some time if desired (since we didn't process, theres no impact on decisions)
  //If another client is interested, then it should start fallback and provide read set as well (forward SyncProposal with correct retry version)
  

  locks_t locks;
  //lock keys to perform an atomic OCC check when parallelizing OCC checks.
  if(params.parallel_CCC){
    locks = LockTxnKeys_scoped(txn);
  }

  
  switch (occType) {
    case MVTSO:
      result = DoMVTSOOCCCheck(reqId, remote, txnDigest, txn, conflict, abstain_conflict, fallback_flow, isGossip);
    case TAPIR:
      result = DoTAPIROCCCheck(txnDigest, txn, retryTs);
    default:
      Panic("Unknown OCC type: %d.", occType);
      return proto::ConcurrencyControl::ABORT;
  }
  //TODO: Call Restore
  restoreTxn(txn);
  return result;
}

//TODO: Abort by default if we receive a Timestamp that already exists for a key (duplicate version) -- byz client might do this, but would get immediately reported.
proto::ConcurrencyControl::Result Server::DoMVTSOOCCCheck(
    uint64_t reqId, const TransportAddress &remote,
    const std::string &txnDigest, const proto::Transaction &txn,
    const proto::CommittedProof* &conflict, const proto::Transaction* &abstain_conflict,
    bool fallback_flow, bool isGossip) {
  Debug("PREPARE[%lu:%lu][%s] with ts %lu.%lu.",
      txn.client_id(), txn.client_seq_num(),
      BytesToHex(txnDigest, 16).c_str(),
      txn.timestamp().timestamp(), txn.timestamp().id());
  Timestamp ts(txn.timestamp());


  preparedMap::const_accessor a;
  bool preparedItr = prepared.find(a, txnDigest);
  //if (preparedItr == prepared.end()) {
  if(!preparedItr){
    a.release();
    //1) Reject Transactions with TS above Watermark
    if (CheckHighWatermark(ts)) {
      Debug("[%lu:%lu][%s] ABSTAIN ts %lu beyond high watermark.",
          txn.client_id(), txn.client_seq_num(),
          BytesToHex(txnDigest, 16).c_str(),
          ts.getTimestamp());
      stats.Increment("cc_abstains", 1);
      stats.Increment("cc_abstains_watermark", 1);
      return proto::ConcurrencyControl::ABSTAIN;
    }
    //2) Validate read set conflicts.
    for (const auto &read : txn.read_set()) {
      // TODO: remove this check when txns only contain read set/write set for the
      //   shards stored at this replica
      if (!IsKeyOwned(read.key())) {
        continue;
      }
      // Check for conflicts against committed writes
      std::vector<std::pair<Timestamp, Server::Value>> committedWrites;
      GetCommittedWrites(read.key(), read.readtime(), committedWrites);
      for (const auto &committedWrite : committedWrites) {
        // readVersion < committedTs < ts
        //     GetCommittedWrites only returns writes larger than readVersion
        if (committedWrite.first < ts) {
          if (params.validateProofs) {
            conflict = committedWrite.second.proof;
          }
          Debug("[%lu:%lu][%s] ABORT wr conflict committed write for key %s:"
              " this txn's read ts %lu.%lu < committed ts %lu.%lu < this txn's ts %lu.%lu.",
              txn.client_id(),
              txn.client_seq_num(),
              BytesToHex(txnDigest, 16).c_str(),
              BytesToHex(read.key(), 16).c_str(),
              read.readtime().timestamp(),
              read.readtime().id(), committedWrite.first.getTimestamp(),
              committedWrite.first.getID(), ts.getTimestamp(), ts.getID());
          stats.Increment("cc_aborts", 1);
          stats.Increment("cc_aborts_wr_conflict", 1);
          return proto::ConcurrencyControl::ABORT;
        }
      }
      // Check for conflicts against prepared writes
      const auto preparedWritesItr = preparedWrites.find(read.key());
      if (preparedWritesItr != preparedWrites.end()) {

        std::shared_lock lock(preparedWritesItr->second.first);
        for (const auto &preparedTs : preparedWritesItr->second.second) {
          if (Timestamp(read.readtime()) < preparedTs.first && preparedTs.first < ts) {
            Debug("[%lu:%lu][%s] ABSTAIN wr conflict prepared write for key %s:"
              " this txn's read ts %lu.%lu < prepared ts %lu.%lu < this txn's ts %lu.%lu.",
                txn.client_id(),
                txn.client_seq_num(),
                BytesToHex(txnDigest, 16).c_str(),
                BytesToHex(read.key(), 16).c_str(),
                read.readtime().timestamp(),
                read.readtime().id(), preparedTs.first.getTimestamp(),
                preparedTs.first.getID(), ts.getTimestamp(), ts.getID());
            stats.Increment("cc_abstains", 1);
            stats.Increment("cc_abstains_wr_conflict", 1);

            // if(fallback_flow){
            //   std::cerr<< "Abstain ["<<BytesToHex(txnDigest, 16)<<"] against prepared write from tx[" << BytesToHex(TransactionDigest(*preparedTs.second, params.hashDigest), 16) << "]" << std::endl;
            // }
            //std::cerr << "Abstain caused by txn: " << BytesToHex(TransactionDigest(*abstain_conflict, params.hashDigest), 16) << std::endl;
     
            abstain_conflict = preparedTs.second;
        
            //TODO: add handling for returning full signed p1.
            return proto::ConcurrencyControl::ABSTAIN;
          }
        }
      }
    }

    //3) Validate write set for conflicts.
    for (const auto &write : txn.write_set()) {
      if (!IsKeyOwned(write.key())) { //Only do OCC check for keys in this group.
        continue;
      }
      // Check for conflicts against committed reads.
      auto committedReadsItr = committedReads.find(write.key());

      if (committedReadsItr != committedReads.end()){
         std::shared_lock lock(committedReadsItr->second.first);
         if(committedReadsItr->second.second.size() > 0) {
          for (auto ritr = committedReadsItr->second.second.rbegin();
              ritr != committedReadsItr->second.second.rend(); ++ritr) {
            if (ts >= std::get<0>(*ritr)) {
              // iterating over committed reads from largest to smallest committed txn ts
              //    if ts is larger than current itr, it is also larger than all subsequent itrs
              break;
            } else if (std::get<1>(*ritr) < ts) {
              if (params.validateProofs) {
                conflict = std::get<2>(*ritr);
              }
              Debug("[%lu:%lu][%s] ABORT rw conflict committed read for key %s: committed"
                  " read ts %lu.%lu < this txn's ts %lu.%lu < committed ts %lu.%lu.",
                  txn.client_id(),
                  txn.client_seq_num(),
                  BytesToHex(txnDigest, 16).c_str(),
                  BytesToHex(write.key(), 16).c_str(),
                  std::get<1>(*ritr).getTimestamp(),
                  std::get<1>(*ritr).getID(), ts.getTimestamp(),
                  ts.getID(), std::get<0>(*ritr).getTimestamp(),
                  std::get<0>(*ritr).getID());
              stats.Increment("cc_aborts", 1);
              stats.Increment("cc_aborts_rw_conflict", 1);
              return proto::ConcurrencyControl::ABORT;
            }
          }
        }
      }
      // check for conflicts against prepared reads
      const auto preparedReadsItr = preparedReads.find(write.key());
      if (preparedReadsItr != preparedReads.end()) {

        std::shared_lock lock(preparedReadsItr->second.first);

        for (const auto preparedReadTxn : preparedReadsItr->second.second) {
          bool isDep = false;
          for (const auto &dep : preparedReadTxn->deps()) {
            if (txnDigest == dep.write().prepared_txn_digest()) {  
              isDep = true;
              break;
            }
          }

          bool isReadVersionEarlier = false;
          Timestamp readTs;
          for (const auto &read : preparedReadTxn->read_set()) { //TODO: It's quite inefficient to loop through the read-set of the txn again, if we already know the relevant key. 
                                                                 //Instead of storing preparedReads := key -> *Tx, we should store key -> {read-version, TS}
            if (read.key() == write.key()) {
              readTs = Timestamp(read.readtime());
              isReadVersionEarlier = readTs < ts;
              break;
            }
          }
          if (!isDep && isReadVersionEarlier &&
              ts < Timestamp(preparedReadTxn->timestamp())) {
            Debug("[%lu:%lu][%s] ABSTAIN rw conflict prepared read for key %s: prepared"
                " read ts %lu.%lu < this txn's ts %lu.%lu < committed ts %lu.%lu.",
                txn.client_id(),
                txn.client_seq_num(),
                BytesToHex(txnDigest, 16).c_str(),
                BytesToHex(write.key(), 16).c_str(),
                readTs.getTimestamp(),
                readTs.getID(), ts.getTimestamp(),
                ts.getID(), preparedReadTxn->timestamp().timestamp(),
                preparedReadTxn->timestamp().id());
            stats.Increment("cc_abstains", 1);
            stats.Increment("cc_abstains_rw_conflict", 1);

            // if(fallback_flow){
            //   std::cerr<< "Abstain ["<<BytesToHex(txnDigest, 16)<<"] against prepared read from tx[" << BytesToHex(TransactionDigest(*preparedReadTxn, params.hashDigest), 16) << "]" << std::endl;
            // }
            return proto::ConcurrencyControl::ABSTAIN;
          }
        }
      }

      // 4) check for write conflicts with tentative reads (not prepared)

      // TODO: add additional rts dep check to shrink abort window  (aka Exceptions)
      //    Is this still a thing?  -->> No currently not
      if(params.rtsMode == 1){
        //Single RTS version
        auto rtsItr = rts.find(write.key());
        if(rtsItr != rts.end()){
          if(rtsItr->second > ts.getTimestamp()){
            ///TODO XXX Re-introduce ID also, for finer ordering. This is safe, since the
            //RTS check is just an additional heuristic; The prepare/commit checks guarantee serializability on their own
            stats.Increment("cc_abstains", 1);
            stats.Increment("cc_abstains_rts", 1);
            return proto::ConcurrencyControl::ABSTAIN;
          }
        }
      }
      else if(params.rtsMode == 2){
        //Multiple RTS versions
        std::pair<std::shared_mutex, std::set<Timestamp>> &rts_set = rts_list[write.key()];
        {
          std::shared_lock lock(rts_set.first);
          auto rtsRBegin = rts_set.second.rbegin();
          if (rtsRBegin != rts_set.second.rend()) {
            Debug("Largest rts for write to key %s: %lu.%lu.", BytesToHex(write.key(), 16).c_str(), rtsRBegin->getTimestamp(), rtsRBegin->getID());
          }
          //find largest RTS greater equal to Ts
          auto rtsLB = rts_set.second.lower_bound(ts);
          if (rtsLB != rts_set.second.end()) {
            Debug("Lower bound rts for write to key %s: %lu.%lu.", BytesToHex(write.key(), 16).c_str(), rtsLB->getTimestamp(), rtsLB->getID());
            if (*rtsLB == ts) {
              rtsLB++;
            }
            if (rtsLB != rts_set.second.end()) {
              if (*rtsLB > ts) { //TODO: Can refine. Technically only need to abort if this Reader read something smaller than TS.
                Debug("[%lu:%lu][%s] ABSTAIN larger rts acquired for key %s: rts %lu.%lu > this txn's ts %lu.%lu.",
                    txn.client_id(), txn.client_seq_num(), BytesToHex(txnDigest, 16).c_str(), BytesToHex(write.key(), 16).c_str(),
                    rtsLB->getTimestamp(), rtsLB->getID(), ts.getTimestamp(), ts.getID());
                stats.Increment("cc_abstains", 1);
                stats.Increment("cc_abstains_rts", 1);
                return proto::ConcurrencyControl::ABSTAIN;
              }
            }
          }
        }
      }
      else{
        //No RTS
      }
    }

    //5) Validate Deps: Check whether server has prepared dep itself
    if (params.validateProofs && params.signedMessages && !params.verifyDeps) {

      Debug("Exec MessageToSign by CPU: %d", sched_getcpu());
      for (const auto &dep : txn.deps()) {
        if (dep.involved_group() != groupIdx) {
          continue;
        }
         //Checks only for those deps that have not committed/aborted already.

        //If a dep has already aborted, abort as well.
        if(aborted.find(dep.write().prepared_txn_digest()) != aborted.end()){
          stats.Increment("cc_aborts", 1);
          stats.Increment("cc_aborts_dep_aborted_early", 1);
          return proto::ConcurrencyControl::ABSTAIN; //Technically could fully Abort here: But then need to attach also proof of dependency abort --> which currently is not implemented/supported
        }
       
        if (committed.find(dep.write().prepared_txn_digest()) == committed.end() && aborted.find(dep.write().prepared_txn_digest()) == aborted.end() ) {
        //If it has not yet aborted, nor committed, check whether the replica has prepared the tx itself: This alleviates having to verify dep proofs, although slightly more pessimistically

          preparedMap::const_accessor a2;
          auto isPrepared = prepared.find(a2, dep.write().prepared_txn_digest());
          if(!isPrepared){
            stats.Increment("cc_aborts", 1);
            stats.Increment("cc_aborts_dep_not_prepared", 1);
            return proto::ConcurrencyControl::ABSTAIN;
          }
          a2.release();
        }
        
      }
    }
    //6) Prepare Transaction: No conflicts, No dependencies aborted --> Make writes visible.
    Prepare(txnDigest, txn);
  }
  else{
     a.release();
  }

  //7) Check whether all outstanding dependencies have committed
    // If not, wait.
  bool allFinished = ManageDependencies(txnDigest, txn, remote, reqId, fallback_flow, isGossip);

  if (!allFinished) {
    stats.Increment("cc_waits", 1);
    return proto::ConcurrencyControl::WAIT;
  } else {
    //8) Check whether all dependencies are committed (i.e. none abort), and whether TS still valid
    return CheckDependencies(txn); //abort checks are redundant with new abort check in 5)
    //TODO: Current Implementation iterates through dependencies 3 times -- re-factor code to do this once.
    //Move check 5) up and outside the if/else case for whether prepared exists: if !params.verifyDeps, then CheckDependencies is mostly obsolete.
  }
}

//TODO: relay Deeper depth when result is already wait. (If I always re-did the P1 it would be handled)
// PRoblem: Dont want to re-do P1, otherwise a past Abort can turn into a commit. Hence we
// ForwardWriteback
bool Server::ManageDependencies(const std::string &txnDigest, const proto::Transaction &txn, const TransportAddress &remote, uint64_t reqId, bool fallback_flow, bool isGossip){
  
  bool allFinished = true;

  if(params.maxDepDepth > -2){

      //if(params.mainThreadDispatching) dependentsMutex.lock();
     if(params.mainThreadDispatching) waitingDependenciesMutex.lock();
     //TODO: instead, take a per txnDigest lock in the loop for each dep, (add the mutex if necessary, and remove it at the end)

     Debug("Called ManageDependencies for txn: %s", BytesToHex(txnDigest, 16).c_str());
     Debug("Manage Dependencies runs on Thread: %d", sched_getcpu());
     for (const auto &dep : txn.deps()) {
       if (dep.involved_group() != groupIdx) { //only check deps at the responsible shard.
         continue;
       }

       // tbb::concurrent_hash_map<std::string, std::mutex>::const_accessor z;
       // bool currently_completing = completing.find(z, dep.write().prepared_txn_digest());
       // if(currently_completing) //z->second.lock();

       if (committed.find(dep.write().prepared_txn_digest()) == committed.end() &&
           aborted.find(dep.write().prepared_txn_digest()) == aborted.end()) {
         Debug("[%lu:%lu][%s] WAIT for dependency %s to finish.",
             txn.client_id(), txn.client_seq_num(),
             BytesToHex(txnDigest, 16).c_str(),
             BytesToHex(dep.write().prepared_txn_digest(), 16).c_str());

         //XXX start RelayP1 to initiate Fallback handling

         if(!params.no_fallback && true && !isGossip){ //do not send relay if it is a gossiped message. Unless we are doinig replica leader gargabe Collection (unimplemented)
           // ongoingMap::const_accessor o;
           // bool inOngoing = ongoing.find(o, dep.write().prepared_txn_digest()); //TODO can remove this redundant lookup since it will be checked again...
           // if (inOngoing) {
           //   std::string dependency_txnDig = dep.write().prepared_txn_digest();
           //   RelayP1(dep.write().prepared_txn_digest(), fallback_flow, reqId, remote, txnDigest);
             uint64_t conflict_id = !fallback_flow ? reqId : -1;
             SendRelayP1(remote, dep.write().prepared_txn_digest(), conflict_id, txnDigest);
           // }
           // o.release();
         }

         allFinished = false;
         //dependents[dep.write().prepared_txn_digest()].insert(txnDigest);

         // auto dependenciesItr = waitingDependencies.find(txnDigest);
         // if (dependenciesItr == waitingDependencies.end()) {
         //   auto inserted = waitingDependencies.insert(std::make_pair(txnDigest,
         //         WaitingDependency()));
         //   UW_ASSERT(inserted.second);
         //   dependenciesItr = inserted.first;
         // }
         // dependenciesItr->second.reqId = reqId;
         // dependenciesItr->second.remote = remote.clone();  //&remote;
         // dependenciesItr->second.deps.insert(dep.write().prepared_txn_digest());

         Debug("Tx:[%s] Added tx %s to %s dependents.", BytesToHex(txnDigest, 16).c_str(), BytesToHex(txnDigest, 16).c_str(), BytesToHex(dep.write().prepared_txn_digest(), 16).c_str());
         dependentsMap::accessor e;
         dependents.insert(e, dep.write().prepared_txn_digest());
         e->second.insert(txnDigest);
         e.release();

         Debug("Tx:[%s] Added %s to waitingDependencies.", BytesToHex(txnDigest, 16).c_str(), BytesToHex(dep.write().prepared_txn_digest(), 16).c_str());
         waitingDependenciesMap::accessor f;
         bool dependenciesItr = waitingDependencies_new.find(f, txnDigest);
         if (!dependenciesItr) {
           waitingDependencies_new.insert(f, txnDigest);
           //f->second = WaitingDependency();
         }
         f->second.reqId = reqId;
        //  if(!fallback_flow && !isGossip){ //NOTE: Original Client subscription moved to P1Meta
        //    f->second.original_client = true;
        //    f->second.reqId = reqId;
        //    f->second.remote = remote.clone();  //&remote;
        //  }
         f->second.deps.insert(dep.write().prepared_txn_digest());
         f.release();
       }
      // if(currently_completing) //z->second.unlock();
      // z.release();
     }

      //if(params.mainThreadDispatching) dependentsMutex.unlock();
      if(params.mainThreadDispatching) waitingDependenciesMutex.unlock();
  }

  return allFinished;
}


////////////////////////////////////////////////////////// Concurrency Control Helper Functions

void Server::GetPreparedReadTimestamps(
    std::unordered_map<std::string, std::set<Timestamp>> &reads) {
  // gather up the set of all writes that are currently prepared
   if(params.mainThreadDispatching) preparedMutex.lock_shared();
  for (const auto &t : prepared) {
    for (const auto &read : t.second.second->read_set()) {
      if (IsKeyOwned(read.key())) {
        reads[read.key()].insert(t.second.first);
      }
    }
  }
   if(params.mainThreadDispatching) preparedMutex.unlock_shared();
}

void Server::GetPreparedReads(
    std::unordered_map<std::string, std::vector<const proto::Transaction*>> &reads) {
  // gather up the set of all writes that are currently prepared
   if(params.mainThreadDispatching) preparedMutex.lock_shared();
  for (const auto &t : prepared) {
    for (const auto &read : t.second.second->read_set()) {
      if (IsKeyOwned(read.key())) {
        reads[read.key()].push_back(t.second.second);
      }
    }
  }
   if(params.mainThreadDispatching) preparedMutex.unlock_shared();
}

void Server::GetCommittedWrites(const std::string &key, const Timestamp &ts,
    std::vector<std::pair<Timestamp, Server::Value>> &writes) {

  std::vector<std::pair<Timestamp, Server::Value>> values;
  if (store.getCommittedAfter(key, ts, values)) {
    for (const auto &p : values) {
      writes.push_back(p);
    }
  }
}

void Server::CheckDependents(const std::string &txnDigest) {
  //Latency_Start(&waitingOnLocks);
   //if(params.mainThreadDispatching) dependentsMutex.lock(); //read lock
   if(params.mainThreadDispatching) waitingDependenciesMutex.lock();
  //Latency_End(&waitingOnLocks);
  Debug("Called CheckDependents for txn: %s", BytesToHex(txnDigest, 16).c_str());
  
  dependentsMap::const_accessor e;
  bool dependentsItr = dependents.find(e, txnDigest);
  
  //auto dependentsItr = dependents.find(txnDigest);
  if(dependentsItr){
  //if (dependentsItr != dependents.end()) {
    for (const auto &dependent : e->second) {
    //for (const auto &dependent : dependentsItr->second) {
      waitingDependenciesMap::accessor f;
      bool dependenciesItr = waitingDependencies_new.find(f, dependent);
      //if(!dependenciesItr){
      UW_ASSERT(dependenciesItr);  //technically this should never fail, since if it were not
      // in the waitingDep struct anymore, it wouldve also removed itself from the
      //dependents set of txnDigest. XXX Need to reason carefully whether this is still true
      // with parallel OCC --> or rather parallel Commit (this is only affected by parallel commit)

      f->second.deps.erase(txnDigest);
      Debug("Removed %s from waitingDependencies of %s.", BytesToHex(txnDigest, 16).c_str(), BytesToHex(dependent, 16).c_str());
      if (f->second.deps.size() == 0) {
        Debug("Dependencies of %s have all committed or aborted.",
            BytesToHex(dependent, 16).c_str());

        proto::ConcurrencyControl::Result result = CheckDependencies(
            dependent);
        UW_ASSERT(result != proto::ConcurrencyControl::ABORT);
        Debug("print remote: %p", f->second.remote);
        //waitingDependencies.erase(dependent);
        const proto::CommittedProof *conflict = nullptr;

       // BufferP1Result(result, conflict, dependent, 2);

        TransportAddress *remote_original = nullptr;
        uint64_t req_id;
        bool sub_original = BufferP1Result(result, conflict, dependent, req_id, 2, remote_original);
        //std::cerr << "[Normal] release lock for txn: " << BytesToHex(txnDigest, 64) << std::endl;
        if(sub_original){
          Debug("Sending Phase1 Reply for txn: %s, id: %d", BytesToHex(dependent, 64).c_str(), req_id);
          SendPhase1Reply(req_id, result, conflict, dependent, remote_original);
        }

        // if(f->second.original_client){
        //   Debug("Sending Phase1 Reply for txn: %s, id: %d", BytesToHex(dependent, 64).c_str(), f->second.reqId);
        //   SendPhase1Reply(f->second.reqId, result, conflict, dependent, f->second.remote);
        //   delete f->second.remote;
        // }

        //Send it to all interested FB clients too:
          interestedClientsMap::accessor i;
          bool hasInterested = interestedClients.find(i, dependent);
          if(hasInterested){
            if(!ForwardWritebackMulti(dependent, i)){
              
              P1FBorganizer *p1fb_organizer = new P1FBorganizer(0, dependent, this);
              SetP1(0, p1fb_organizer->p1fbr->mutable_p1r(), dependent, result, conflict);
              Debug("Sending Phase1FBReply MULTICAST for txn: %s with result %d", BytesToHex(dependent, 64).c_str(), result);

              p2MetaDataMap::const_accessor p;
              p2MetaDatas.insert(p, dependent);
              if(p->second.hasP2){
                proto::CommitDecision decision = p->second.p2Decision;
                uint64_t decision_view = p->second.decision_view;
                SetP2(0, p1fb_organizer->p1fbr->mutable_p2r(), dependent, decision, decision_view);
                Debug("Including P2 Decision %d in Phase1FBReply MULTICAST for txn: %s", decision, BytesToHex(dependent, 64).c_str());
              }
              p.release();
              //TODO: If need reqId, can store it as pairs with the interested client.
              
              SendPhase1FBReply(p1fb_organizer, dependent, true);
            }
          }
          i.release();
        /////

        waitingDependencies_new.erase(f);
      }
      f.release();
    }
  }
  e.release();
   //if(params.mainThreadDispatching) dependentsMutex.unlock();
   if(params.mainThreadDispatching) waitingDependenciesMutex.unlock();
}

proto::ConcurrencyControl::Result Server::CheckDependencies(
    const std::string &txnDigest) {
      //Latency_Start(&waitingOnLocks);
   //if(params.mainThreadDispatching) ongoingMutex.lock_shared();
   //Latency_End(&waitingOnLocks);
  ongoingMap::const_accessor o;
  bool isOngoing = ongoing.find(o, txnDigest);
  if(!isOngoing){

  //if(txnItr == ongoing.end()){
    Debug("Tx with txn digest [%s] has already committed/aborted", BytesToHex(txnDigest, 16).c_str());
    {
      //std::shared_lock lock(committedMutex);
      //std::shared_lock lock2(abortedMutex);
      if(committed.find(txnDigest) != committed.end()){
        //if(params.mainThreadDispatching) ongoingMutex.unlock_shared();
        return proto::ConcurrencyControl::COMMIT;
      }
      else if(aborted.find(txnDigest) != aborted.end()){
        //if(params.mainThreadDispatching) ongoingMutex.unlock_shared();
        return proto::ConcurrencyControl::ABSTAIN;
      }
      else{
        Panic("has to be either committed or aborted");
      }
    }
  }

  //UW_ASSERT(txnItr != ongoing.end());
   //if(params.mainThreadDispatching) ongoingMutex.unlock_shared();
  //return CheckDependencies(*txnItr->second);
  return CheckDependencies(*o->second.txn);
}

proto::ConcurrencyControl::Result Server::CheckDependencies(
    const proto::Transaction &txn) {

   //if(params.mainThreadDispatching) committedMutex.lock_shared();
  for (const auto &dep : txn.deps()) {
    if (dep.involved_group() != groupIdx) {
      continue;
    }
    if (committed.find(dep.write().prepared_txn_digest()) != committed.end()) {
      //Check if dependency still has smaller timestamp than reader: Could be violated if dependency re-tried with higher TS and committed -- Currently retries are not implemented.
      if (Timestamp(dep.write().prepared_timestamp()) > Timestamp(txn.timestamp())) {
        stats.Increment("cc_aborts", 1);
        stats.Increment("cc_aborts_dep_ts", 1);
         //if(params.mainThreadDispatching) committedMutex.unlock_shared();
        return proto::ConcurrencyControl::ABSTAIN;
      }
    } else {
      stats.Increment("cc_aborts", 1);
      stats.Increment("cc_aborts_dep_aborted", 1);
       //if(params.mainThreadDispatching) committedMutex.unlock_shared();
      return proto::ConcurrencyControl::ABSTAIN;
    }
  }
   //if(params.mainThreadDispatching) committedMutex.unlock_shared();
  return proto::ConcurrencyControl::COMMIT;
}

void Server::CleanDependencies(const std::string &txnDigest) {
   //if(params.mainThreadDispatching) dependentsMutex.lock();
   Debug("Called CleanDependencies for txn %s", BytesToHex(txnDigest, 16).c_str());
   if(params.mainThreadDispatching) waitingDependenciesMutex.lock();

  waitingDependenciesMap::accessor f;
  bool dependenciesItr = waitingDependencies_new.find(f, txnDigest);
  if (dependenciesItr ) {
  //auto dependenciesItr = waitingDependencies.find(txnDigest);
  //if (dependenciesItr != waitingDependencies.end()) {
    //for (const auto &dependency : dependenciesItr->second.deps) {
    for (const auto &dependency : f->second.deps) {
      dependentsMap::accessor e;
      auto dependentItr = dependents.find(e, dependency);
      if (dependentItr) {
        e->second.erase(txnDigest);
      }
      e.release();
      // auto dependentItr = dependents.find(dependency);
      // if (dependentItr != dependents.end()) {
      //   dependentItr->second.erase(txnDigest);
      // }

    }
    waitingDependencies_new.erase(f);
    //waitingDependencies.erase(dependenciesItr);
  }
  f.release();
  dependentsMap::accessor e;
  if(dependents.find(e, txnDigest)){
    dependents.erase(e);
  }
  e.release();
  //dependents.erase(txnDigest);
   //if(params.mainThreadDispatching) dependentsMutex.unlock();
   if(params.mainThreadDispatching) waitingDependenciesMutex.unlock();
}

uint64_t Server::DependencyDepth(const proto::Transaction *txn) const {
  uint64_t maxDepth = 0;
  std::queue<std::pair<const proto::Transaction *, uint64_t>> q;
  q.push(std::make_pair(txn, 0UL));

  //auto ongoingMutexScope = params.mainThreadDispatching ? std::shared_lock<std::shared_mutex>(ongoingMutex) : std::shared_lock<std::shared_mutex>();

  while (!q.empty()) {
    std::pair<const proto::Transaction *, uint64_t> curr = q.front();
    q.pop();
    maxDepth = std::max(maxDepth, curr.second);
    for (const auto &dep : curr.first->deps()) {
      ongoingMap::const_accessor o;
      bool oitr = ongoing.find(o, dep.write().prepared_txn_digest());
      if(oitr){
      //if (oitr != ongoing.end()) {
        //q.push(std::make_pair(oitr->second, curr.second + 1));
        q.push(std::make_pair(o->second.txn, curr.second + 1));
      }
      o.release();
    }
  }
  return maxDepth;
}

bool Server::CheckHighWatermark(const Timestamp &ts) {
  Timestamp highWatermark(timeServer.GetTime());
  // add delta to current local time
  highWatermark.setTimestamp(highWatermark.getTimestamp() + timeDelta);
  Debug("High watermark: %lu.", highWatermark.getTimestamp());
  return ts > highWatermark;
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
proto::ConcurrencyControl::Result Server::DoTAPIROCCCheck(
    const std::string &txnDigest, const proto::Transaction &txn,
    Timestamp &retryTs) {
  Debug("[%s] START PREPARE", txnDigest.c_str());


  if(params.mainThreadDispatching) preparedMutex.lock_shared();
  preparedMap::const_accessor a;
  auto preparedItr = prepared.find(a, txnDigest);
  if(preparedItr){
      if(a->second.first == txn.timestamp()){
  //if (preparedItr != prepared.end()) {
  //  if (preparedItr->second.first == txn.timestamp()) {
      Warning("[%s] Already Prepared!", txnDigest.c_str());
      if(params.mainThreadDispatching) preparedMutex.unlock_shared();
      return proto::ConcurrencyControl::COMMIT;
    } else {
      // run the checks again for a new timestamp
      if(params.mainThreadDispatching) preparedMutex.unlock_shared();
      Clean(txnDigest);
    }
  }
  a.release();
  // do OCC checks
  std::unordered_map<std::string, std::set<Timestamp>> pReads;
  GetPreparedReadTimestamps(pReads);

  // check for conflicts with the read set
  for (const auto &read : txn.read_set()) {
    std::pair<Timestamp, Timestamp> range;
     //if(params.mainThreadDispatching) storeMutex.lock();
    bool ret = store.getRange(read.key(), read.readtime(), range);
     //if(params.mainThreadDispatching) storeMutex.unlock();

    Debug("Range %lu %lu %lu", Timestamp(read.readtime()).getTimestamp(),
        range.first.getTimestamp(), range.second.getTimestamp());

    // if we don't have this key then no conflicts for read
    if (!ret) {
      continue;
    }

    // if we don't have this version then no conflicts for read
    if (range.first != read.readtime()) {
      continue;
    }

    // if the value is still valid
    if (!range.second.isValid()) {
      // check pending writes.
      //auto preparedWritesMutexScope = params.mainThreadDispatching ? std::shared_lock<std::shared_mutex>(preparedWritesMutex) : std::shared_lock<std::shared_mutex>();

      if (preparedWrites.find(read.key()) != preparedWrites.end()) {
        Debug("[%lu,%lu] ABSTAIN rw conflict w/ prepared key %s.",
            txn.client_id(),
            txn.client_seq_num(),
            BytesToHex(read.key(), 16).c_str());
        stats.Increment("cc_abstains", 1);
        stats.Increment("cc_abstains_rw_conflict", 1);
        return proto::ConcurrencyControl::ABSTAIN;
      }
    } else {
      // if value is not still valtxnDigest, then abort.
      /*if (Timestamp(txn.timestamp()) <= range.first) {
        Warning("timestamp %lu <= range.first %lu (range.second %lu)",
            txn.timestamp().timestamp(), range.first.getTimestamp(),
            range.second.getTimestamp());
      }*/
      //UW_ASSERT(timestamp > range.first);
      Debug("[%s] ABORT rw conflict: %lu > %lu", txnDigest.c_str(),
          txn.timestamp().timestamp(), range.second.getTimestamp());
      stats.Increment("cc_aborts", 1);
      stats.Increment("cc_aborts_rw_conflict", 1);
      return proto::ConcurrencyControl::ABORT;
    }
  }

  // check for conflicts with the write set
  for (const auto &write : txn.write_set()) {
    std::pair<Timestamp, Server::Value> val;
    // if this key is in the store
     //if(params.mainThreadDispatching) storeMutex.lock();
    if (store.get(write.key(), val)) {
      Timestamp lastRead;
      bool ret;

      // if the last committed write is bigger than the timestamp,
      // then can't accept
      if (val.first > Timestamp(txn.timestamp())) {
        Debug("[%s] RETRY ww conflict w/ prepared key:%s", txnDigest.c_str(),
            write.key().c_str());
        retryTs = val.first;
        stats.Increment("cc_retries_committed_write", 1);
         //if(params.mainThreadDispatching) storeMutex.unlock();
        return proto::ConcurrencyControl::ABSTAIN;
      }

      // if last committed read is bigger than the timestamp, can't
      // accept this transaction, but can propose a retry timestamp

      // we get the timestamp of the last read ever on this object
      ret = store.getLastRead(write.key(), lastRead);

      // if this key is in the store and has been read before
      if (ret && lastRead > Timestamp(txn.timestamp())) {
        Debug("[%s] RETRY wr conflict w/ prepared key:%s", txnDigest.c_str(),
            write.key().c_str());
        retryTs = lastRead;
         //if(params.mainThreadDispatching) storeMutex.unlock();
        return proto::ConcurrencyControl::ABSTAIN;
      }
    }
     //if(params.mainThreadDispatching) storeMutex.unlock();

    // if there is a pending write for this key, greater than the
    // proposed timestamp, retry

     //if(params.mainThreadDispatching) preparedWritesMutex.lock_shared();

     auto itr = preparedWrites.find(write.key());

    if (itr != preparedWrites.end()) {
      itr->second.first.lock_shared();
      std::map<Timestamp, const proto::Transaction *>::iterator it =
          itr->second.second.upper_bound(txn.timestamp());
      if (it != itr->second.second.end() ) {
        Debug("[%s] RETRY ww conflict w/ prepared key:%s", txnDigest.c_str(),
            write.key().c_str());
        retryTs = it->first;
        stats.Increment("cc_retries_prepared_write", 1);
         //if(params.mainThreadDispatching) preparedWritesMutex.unlock_shared();
         itr->second.first.unlock_shared();
        return proto::ConcurrencyControl::ABSTAIN;
      }
      itr->second.first.unlock_shared();
    }
     //if(params.mainThreadDispatching) preparedWritesMutex.unlock_shared();


    //if there is a pending read for this key, greater than the
    //propsed timestamp, abstain
    if (pReads.find(write.key()) != pReads.end() &&
        pReads[write.key()].upper_bound(txn.timestamp()) !=
        pReads[write.key()].end()) {
      Debug("[%s] ABSTAIN wr conflict w/ prepared key: %s",
            txnDigest.c_str(), write.key().c_str());
      stats.Increment("cc_abstains", 1);
      return proto::ConcurrencyControl::ABSTAIN;
    }
  }

  // Otherwise, prepare this transaction for commit
  Prepare(txnDigest, txn);

  Debug("[%s] PREPARED TO COMMIT", txnDigest.c_str());

  return proto::ConcurrencyControl::COMMIT;
}


locks_t Server::LockTxnKeys_scoped(const proto::Transaction &txn) {
    // timeval tv1;
    // gettimeofday(&tv1, 0);
    // int id = std::rand();
    // std::cerr << "starting locking for id: " << id << std::endl;
    locks_t locks;

    auto itr_r = txn.read_set().begin();
    auto itr_w = txn.write_set().begin();

    //for(int i = 0; i < txn.read_set().size() + txn.write_set().size(); ++i){
    while(itr_r != txn.read_set().end() || itr_w != txn.write_set().end()){
      //skip duplicate keys (since the list is sorted they should be next)
      if(itr_r != txn.read_set().end() && std::next(itr_r) != txn.read_set().end()){
        if(itr_r->key() == std::next(itr_r)->key()){
          itr_r++;
          continue;
        }
      }
      if(itr_w != txn.write_set().end() && std::next(itr_w) != txn.write_set().end()){
        if(itr_w->key() == std::next(itr_w)->key()){
          itr_w++;
          continue;
        }
      }
      //lock and advance read/write respectively if the other set is done
      if(itr_r == txn.read_set().end()){
        //std::cerr<< "Locking Write [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_w->key(), 16).c_str() << "]" << std::endl;
        locks.emplace_back(mutex_map[itr_w->key()]);
        itr_w++;
      }
      else if(itr_w == txn.write_set().end()){
        //std::cerr<< "Locking Read [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_r->key(), 16).c_str() << "]" << std::endl;
        locks.emplace_back(mutex_map[itr_r->key()]);
        itr_r++;
      }
      //lock and advance read/write iterators in order
      else{
        if(itr_r->key() <= itr_w->key()){
          //std::cerr<< "Locking Read/Write [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_r->key(), 16).c_str() << "]" << std::endl;
          locks.emplace_back(mutex_map[itr_r->key()]);
          //If read set and write set share keys, must not acquire lock twice.
          if(itr_r->key() == itr_w->key()) {
            itr_w++;
          }
          itr_r++;
        }
        else{
          //std::cerr<< "Locking Write [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_w->key(), 16).c_str() << "]" << std::endl;
          locks.emplace_back(mutex_map[itr_w->key()]);
          itr_w++;
        }
      }
    }
    // timeval tv2;
    // gettimeofday(&tv2, 0);
    // total_lock_time_ms += (tv2.tv_sec * 1000  + tv2.tv_usec / 1000) - (tv1.tv_sec * 1000  + tv1.tv_usec / 1000);
    // std::cerr << "ending locking for id: " << id << std::endl;
    return locks;
}

//XXX DEPRECATED
void Server::LockTxnKeys(proto::Transaction &txn){
  // Lock all (read/write) keys in order for atomicity if using parallel OCC

    auto itr_r = txn.read_set().begin();
    auto itr_w = txn.write_set().begin();
    //for(int i = 0; i < txn.read_set().size() + txn.write_set().size(); ++i){
    while(itr_r != txn.read_set().end() || itr_w != txn.write_set().end()){
      if(itr_r == txn.read_set().end()){
        lock_keys[itr_w->key()].lock();
        itr_w++;
      }
      else if(itr_w == txn.write_set().end()){
        lock_keys[itr_r->key()].lock();
        itr_r++;
      }
      else{
        if(itr_r->key() <= itr_w->key()){
          lock_keys[itr_r->key()].lock();
          //If read set and write set share keys, must not acquire lock twice.
          if(itr_r->key() == itr_w->key()) { itr_w++;}
          itr_r++;
        }
        else{
          lock_keys[itr_w->key()].lock();
          itr_w++;
        }
      }
    }
}
//XXX DEPRECATED
void Server::UnlockTxnKeys(proto::Transaction &txn){
  // Lock all (read/write) keys in order for atomicity if using parallel OCC
    auto itr_r = txn.read_set().rbegin();
    auto itr_w = txn.write_set().rbegin();
    //for(int i = 0; i < txn.read_set().size() + txn.write_set().size(); ++i){
    while(itr_r != txn.read_set().rend() || itr_w != txn.write_set().rend()){
      if(itr_r == txn.read_set().rend()){
        lock_keys[itr_w->key()].unlock();
        itr_w++;
      }
      else if(itr_w == txn.write_set().rend()){
        lock_keys[itr_r->key()].unlock();
        itr_r++;
      }
      else{
        if(itr_r->key() > itr_w->key()){
          lock_keys[itr_r->key()].unlock();
          itr_r++;
        }
        else{
          lock_keys[itr_w->key()].unlock();
          if(itr_r->key() == itr_w->key()) { itr_r++;}
          itr_w++;
        }
      }
    }
}


}