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

//TODO: Problem: FIXME: Probably not safe to modify transaction. -- must hold ongoing lock for entire duration of any tx uses? --> not possible..
//Solution: Store elsewhere (don't override read-set) -- Refactor CC and Prepare to take read set pointer as argument -- in non-query case, let that read set point to normal readset.
//Can we still edit read_set_merge field? If so, that is a good place to store it to re-use for Commit -- Test if that causes problems with sending out tx in parallel. (might result in corrupted protobuf messages)
    //Note: 2 threads may try to call DoOCC for a Tx in parallel (barrier is only at BufferP1) -> they might try to write the same value in parallel. 
    //FIX: Hold ongoing lock while updating merged_set. + Check if merged_set exists, if so, re-use it.

    //Best solution: Store the merged set as part of ongoingMap -- I.e. keep TX const. In CC check pass pointer to read set instead of Txn. Same for prepare.
    //In this case, can remove restoreTxn. 

//TODO: Must also ensure WAIT invariant: Should never cause WAIT to be decided as a result of honest clients sync and prepare being processed in parallel


//Problem: Don't want to wake Tx before we subscribed to ALL missing. 
// I.e. we don't want to subscribe to one missing, then it gets completed and triggers the TryPrepare; while we still were trying to add more missing.
// TODO: Must hold a per tx lock first. Queries hold query_md lock. If Query Exec gets it first, then it will update query_md cache, and check whether there is any tx waiting on the query
//; if there is, then the CC must have already held and released the query_md lock (added missing query). Query exec tries to take the Tx lock, but will fail until Tx finishes all tx.
//If there isn't, then query_md can't have been held yet. Query exec will take query_md lock after and see query.

//If the Transaction is waiting on a Query to be cached, call 
void Server::subscribeTxOnMissingQuery(const std::string &query_id, const std::string &txnDigest){ 
  //Note: This is being called while lock on TxnMissingQueries object and query_md is held. --> either query is in cache already, in which case subscribe is never called; or it is cached after subscribe, in which case it tries to wake the tx up
    subscribedQueryMap::accessor sq;
    subscribedQuery.insert(sq, query_id);
    sq->second = txnDigest;
    sq.release();
}


void Server::wakeSubscribedTx(const std::string query_id, const uint64_t &retry_version){
  //Note: This is being called while query_md is held. --> will trigger only before adding any queries, or after adding all

  Debug("Trying to wake potential transaction subscribed on query [%s;%d]", BytesToHex(query_id, 16).c_str(), retry_version);

    std::string txnDigest;
    bool ready = false;
    waitingOnQueriesMeta *waiting_meta;
    //MissingQueryMeta *waking_tx_md = nullptr;

    subscribedQueryMap::accessor sq;
    bool has_subscriber = subscribedQuery.find(sq, query_id);
    //Note: has_subscriber is only true IF function mergeReadsets already holds accessors mq (for txnDigest) AND q (for query_id). Thus there can be no lock-order inversion with mq.
    if(has_subscriber){
      txnDigest = std::move(sq->second);
      Debug("Found subscribed Txn: %s", BytesToHex(txnDigest, 16).c_str());
      subscribedQuery.erase(sq);

      TxnMissingQueriesMap::accessor mq;
      bool isMissing = TxnMissingQueries.find(mq, txnDigest);
      if(!isMissing){
        Debug("Txn: %s is not missing any queries", BytesToHex(txnDigest, 16).c_str());
         return; //No Txn waiting ==> nothing to do: return
      }
    
      //if Txn still waiting
      waiting_meta = mq->second;
      //auto query_id_version = std::make_pair(query_id, retry_version);
      auto itr = waiting_meta->missing_query_id_versions.find(query_id); //Check if buffered query id + retry version is correct.
      //TODO: Is missing query_id.
      if(itr == waiting_meta->missing_query_id_versions.end()){
        Debug("query id not missing: %s", BytesToHex(query_id, 16).c_str());
        return;
      } 
      if(itr->second != retry_version){
        Debug("Has query_id, but has wrong version: Missing version %d. Executed Query version %d", itr->second, retry_version);
        return; //Don' wake up.
      }

      Debug("Retry version of subscribed Query matches supplied Query.");

      bool erased = waiting_meta->missing_query_id_versions.erase(query_id); //does nothing if not present
      if(erased && waiting_meta->missing_query_id_versions.empty()) ready = true; //only set ready the first time.
      
      TxnMissingQueries.erase(mq);
      mq.release();
    }
    sq.release();

    if(ready){ //Wakeup Tx for processing
     Debug("Ready to wakeup and resume P1 processing for Txn: %s", BytesToHex(txnDigest, 16).c_str());
      if(waiting_meta->prepare_or_commit==0) TryPrepare(waiting_meta->reqId, *waiting_meta->remote, waiting_meta->txn, txnDigest, waiting_meta->isGossip); //Includes call to HandlePhase1CB(..); 
      else if(waiting_meta->prepare_or_commit==1){
        Timestamp ts(waiting_meta->txn->timestamp());
        UpdateCommittedReads(waiting_meta->txn, txnDigest, ts, waiting_meta->proof);//Commit(txnDigest, waiting_meta->txn, waiting_meta->groupedSigs, waiting_meta->p1Sigs, waiting_meta->view); //Note: Confirm that it's ok to issue Commit from any thread (I believe it is); If not, must IssueCB_maingit 
      }
      else Panic("Must bei either prepare or commit");
      //TODO: Need to send to fallback clients too. (If we add tests for this)
      delete waiting_meta; //Note: delete waiting_meta will also delete stored remote ==> TryPrepare creates a copy of remote to avoid a segfault
      
    }
}

//returns pointer to query read set (either from cache, or from txn itself)
proto::ConcurrencyControl::Result Server::fetchReadSet(const proto::QueryResultMetaData &query_md, const proto::ReadSet *&query_rs, const std::string &txnDigest, const proto::Transaction &txn){

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
      Debug("Merging Query Read Set from Transaction"); //TODO:: In this case, could avoid copies by letting client put all active read sets into the main read set. 
                                                        // --> Client should apply sort function -> will find invalid duplicates and abort early!
      query_rs = &query_group_md->query_read_set();
    }
    else{ //else go to cache (via query_id) and check if query_result hash matches. if so, use read set.
      Debug("Merging Query Read Set from Cache");
      // If tx includes no read_set_hash => abort; invalid transaction //TODO: Could strengthen Abstain into Abort by incuding proof...
      if(!query_group_md->has_read_set_hash()) return proto::ConcurrencyControl::IGNORE; //ABSTAIN;

      queryMetaDataMap::const_accessor q;
      bool has_query = queryMetaData.find(q, query_md.query_id());
    
      //1) Check whether the replica a) has seen the query, and b) has computed a result/read-set. If not ==> Stop processing
      if(!has_query || !q->second->has_result){
        //Panic("query has no result cached yet. has_query: %d", has_query);
        Debug("query has either not been received or no result was cached yet. has_query: %d. SUBSCRIBING", has_query, q->second->has_result);
        subscribeTxOnMissingQuery(query_md.query_id(), txnDigest);
        return proto::ConcurrencyControl::WAIT; //NOTE: Replying WAIT is a hack to not respond -- it will never wake up.
      } 

      QueryMetaData *cached_query_md = q->second;

      //2) Only client that issued the query may use the cached query in it's transaction ==> it follows that honest clients will not use the same query twice. (Must include txn as argument in order to check)
      if(cached_query_md->client_id != txn.client_id()){
        Panic("Query client %d does not match transaction client %d", cached_query_md->client_id, txn.client_id());
        return proto::ConcurrencyControl::IGNORE;
      } 


      //3) Check for matching retry version. If local version smaller ==> Stop processing (Wait), if local version larger ==> PoM
      if(query_md.retry_version() != cached_query_md->retry_version){
          Panic("query has wrong cached retry version; shouldn't happen in testing");

  
          if(query_md.retry_version() < cached_query_md->retry_version){
            Panic("Report PoM");
            return proto::ConcurrencyControl::ABSTAIN;
             // Note: Any interested client that learns of prepared dependency will learn correct version. Problem: byz client couldve updated version at other replicas in the meantime.
              //To avoid them defaulting (and cascade aborting correct dependencies), they must either
                      //a) keep older versions read-sets, or (better) accept older versions read set on demand IF supported by Prepare msg that picks version. //(ONLY accept older version in this case)
                      //b) Treat old versions as PoM 
                      //Note: If byz client prepares with version (v), but before that updated the version to v'>v ==> proof of misbehavior (report client, and abort by default -- in this case thats ok).
          } 

          //else: query_md.retry_version() > cached_query_md->retry_version ==> Wait
          subscribeTxOnMissingQuery(query_md.query_id(), txnDigest);
          return proto::ConcurrencyControl::WAIT; ///NOTE: Replying WAIT is a hack to not respond -- it will never wake up.
           //Note: Byz client can send retry version to relicas that have not gotten it yet. If we vote abstain, we may produce partial abort.
           //However, due to TCP FIFO we expect honest clients to send the retry version sync first -- thus it's ok to not vote/wait     //TODO: Ensure that multithreading does not violate this guarantee
           // ==>  Instead of aborting due to mismatched hash, should keep waiting/don't reply if retry vote doesnt match!
      }

      proto::QueryResultReply *cached_queryResultReply = cached_query_md->queryResultReply;

      //4) Check that replica has cached a read set, and that proposed read set hash matches cached read set hash. If not => return Abstain
      if(!cached_queryResultReply->has_result()){
        Panic("Protobuf Result should be set");
        subscribeTxOnMissingQuery(query_md.query_id(), txnDigest);
        return proto::ConcurrencyControl::WAIT; //Checks whether result or signed result is set. Must contain un-signed result
      }
      const proto::QueryResult &cached_queryResult = cached_queryResultReply->result();

      if(!cached_queryResult.has_query_read_set()){
        Debug("Cached QueryFailure for current query version");
        return proto::ConcurrencyControl::ABSTAIN;
      } 

      if(cached_query_md->failure){
        Debug("Cached QueryFailure for current query version");
        return proto::ConcurrencyControl::ABSTAIN; //Replica has already previously voted to abstain by reporting an exec failure (conflicting tx already committed, or sync request aborted) -- choice won't change
      } 
  
      if(!cached_queryResult.has_query_result_hash() || query_group_md->read_set_hash() != cached_queryResult.query_result_hash()){
        Debug("Cached wrong read-set");
        return proto::ConcurrencyControl::ABSTAIN;
      } 
     
      //5) Use Read set.
      query_rs = &cached_queryResult.query_read_set();
  
      q.release();
    }
  
    return proto::ConcurrencyControl::COMMIT;
  }

//DEPRECATED 
void Server::restoreTxn(proto::Transaction &txn){ 
    if(txn.query_set().empty()) return;

    if(txn.has_merged_read_set()){
       txn.mutable_read_set()->Swap(txn.mutable_merged_read_set()->mutable_read_set());
       txn.clear_merged_read_set();
    }
   
}


//NOTE: If storing mergedReadSet inside tx is threadsafe, then technically don't need to pass readSet/depSet as function args, but can just pull from txn.

proto::ConcurrencyControl::Result Server::mergeTxReadSets(const ReadSet *&readSet, const DepSet *&depSet, proto::Transaction &txn, const std::string &txnDigest, uint64_t req_id, const TransportAddress &remote, bool isGossip){
  return mergeTxReadSets(readSet, depSet, txn, txnDigest, 0, req_id, &remote, isGossip, nullptr);
}
proto::ConcurrencyControl::Result Server::mergeTxReadSets(const ReadSet *&readSet, const DepSet *&depSet, proto::Transaction &txn, const std::string &txnDigest, proto::CommittedProof *proof){ //, proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view){
  return mergeTxReadSets(readSet, depSet, txn, txnDigest, 1, 0, nullptr, false, proof); //, groupedSigs, p1Sigs, view);
} //TODO: In Commit: Call mergeTxReadSets -- if result != Commit, return (implies it's buffered -- commit itself implies that decision cannot be ignore/abort/abstain )

// Call this function before locking keys (only call if query set non empty.) ==> need to lock in this order; need to perform CC on this. 
// ==> Add the read set as a field to the txn. //FIXME: Txn can't be const in order to do that...  //Return value should be Result, in order to abort early if merge fails (due to incompatible duplicate)
proto::ConcurrencyControl::Result Server::mergeTxReadSets(const ReadSet *&readSet, const DepSet *&depSet, proto::Transaction &txn, const std::string &txnDigest, uint8_t prepare_or_commit,
     uint64_t req_id, const TransportAddress *remote, bool isGossip,    //Args for Prepare
     proto::CommittedProof *proof)//, proto::GroupedSignatures *groupedSigs, bool p1Sigs, uint64_t view){  //Args for Commit
  {
  //Note: Don't want to override transaction -> else digest changes / we cannot provide the right tx for sync (because stored txn is different)
  //TODO: store a backup of read_set; and override it for the timebeing (this way we don't have to change any of the DoMVTSOCheck and Prepare code.)
  
  if(!params.query_params.cacheReadSet && params.query_params.mergeActiveAtClient){
    Debug("Query Read Sets are already merged into main read Set by client."); 
    return proto::ConcurrencyControl::COMMIT;
  }

   Debug("Trying to merge TxReadsets for txn: %s", BytesToHex(txnDigest, 16).c_str());
  
  if(txn.query_set().empty()){
    Debug("Txn has no queries. ReadSet = base ReadSet");
    return proto::ConcurrencyControl::COMMIT;
  } 

  //*txn.mutable_merged_read_set() = txn.read_set(); //store backup
  //ReadSet *mergedReadSet = txn.mutable_read_set();

   //If tx already has mergedReadSet -> just return that, no need in re-doing the work.
  if(txn.has_merged_read_set()){
    Debug("Already cached a merged read set. Re-using.");
    readSet = &txn.merged_read_set().read_set();
    depSet = &txn.merged_read_set().deps();
    return proto::ConcurrencyControl::COMMIT;
  }

  //Else: Try to generate a new mergedReadSet
  proto::ReadSet *mergedReadSet = new proto::ReadSet(); //if this doesn't work, just create a local proto::ReadSet
   //For now: Try to store it inside Tx (use set_allocated)
   //However, if that causes a problem (since it's technically not threadsafe) ==> Create a map that holds mergedReadSet (map from txnDigest -> ReadSet) -- 
         //while trying to use it, release it (to avoid concurrency issues of the object on the map) from protobuf (only create it if .has_read_set = false.); after DoMVTSO restore it ==> put it back into the map.
         //In Clean: Remove the entry from the map
  
  //Hold mq allocator to ensure that we subscribe *all* missing queries at once.
  TxnMissingQueriesMap::accessor mq;
  bool already_subscribed = !TxnMissingQueries.insert(mq, txnDigest); //Note: Reading should still take a write lock. (If not, can insert here, and erase at the end if empty.)

  if(already_subscribed){
    if(prepare_or_commit==1) mq->second->setToCommit(proof); //groupedSigs, p1Sigs, view, 1); //Upgrade subscription to commit.
    return proto::ConcurrencyControl::WAIT; //The txn is missing queries and is already subscribed.
  } 

  //Hold write lock for the entirety of the loop ==> guarantee all of nothing.
  std::vector<std::pair<const std::string*, const uint64_t>> missing_queries; //List of query id's for which we have not received the latest version yet, but which the Tx is referencing.
  //bool has_missing = false;

  //fetch query read sets
  for(proto::QueryResultMetaData &query_md : *txn.mutable_query_set()){
    const proto::ReadSet *query_rs;
    proto::ConcurrencyControl::Result res = fetchReadSet(query_md, query_rs, txnDigest, txn); 

    if(res == proto::ConcurrencyControl::WAIT){ //Set up waiting.
      //TODO: Subscribe query.
      // Add to waitingOnQuery object.
      Debug("Waiting on Query. Add to missing");
      missing_queries.push_back(std::make_pair(query_md.mutable_query_id(), query_md.retry_version()));
      //has_missing = true;
    }
    else if(res != proto::ConcurrencyControl::COMMIT){ //No need to continue CC check.
      Debug("Query invalid or doomed to abort. Stopping Merge");
      return res;  //Note: Might have already subscribed some queries on a tx. If they wake up and there is no waiting tx object thats fine -- nothing happens
    }
    if(query_rs != nullptr && missing_queries.empty()){ //If we are waiting on queries, don't need to build mergedSet.
       //mergedReatSet.extend(query_rs);
        mergedReadSet->mutable_read_set()->MergeFrom(query_rs->read_set()); //Merge from copies; If we don't want that, can loop through fetchedRead set and move 1 by 1.
        //mergedReadSet add readSet
        //Merge Query Dependency sets. (If empty, this merge does nothing.)
        mergedReadSet->mutable_deps()->MergeFrom(query_rs->deps());
        //mergedReadSet->mutable_dep_ids()->MergeFrom(query_rs->dep_ids());
        //mergedReadSet->mutable_dep_ts_ids()->MergeFrom(query_rs->dep_ts_ids());

        //TODO: Also need to account for dep_ts
    }
  }

  //Subscribe if we are missing queries (and we have not yet subscribed previously -- could happen in parallel on another thread)
  if(!missing_queries.empty()){
      //Subscribe to each missing query inside fetchReadSet.
      //create waitingObject
      waitingOnQueriesMeta *waiting_meta = prepare_or_commit==0 ? new waitingOnQueriesMeta(req_id, &txn, remote, isGossip, prepare_or_commit) : new waitingOnQueriesMeta(&txn, proof, prepare_or_commit); //, groupedSigs, p1Sigs, view, prepare_or_commit);
      mq->second = waiting_meta;
    
      for(auto [query_id, retry_version] : missing_queries){
         Debug("Added missing query id: %s. version: %d", BytesToHex(*query_id, 16).c_str(), retry_version);
        waiting_meta->missing_query_id_versions[*query_id] = retry_version;
      }
    //mq.release(); Implicit
    Debug("Subscribed Txn on missing queries. Stopping Merge");
    return proto::ConcurrencyControl::WAIT;
  }
  else{
    TxnMissingQueries.erase(mq); //Erase if we added the data structure for no reason.
  }

  mq.release();

  //If queries had no active read sets ==> return default
  if(mergedReadSet->read_set().empty()){
    Debug("Tx has no queries with active read sets.");
    return proto::ConcurrencyControl::COMMIT;
  } 

   //attach base read-set 
  mergedReadSet->mutable_read_set()->MergeFrom(txn.read_set());
  //attach base dep-set
  mergedReadSet->mutable_deps()->MergeFrom(txn.deps());

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
      std::sort(mergedReadSet->mutable_read_set()->begin(), mergedReadSet->mutable_read_set()->end(), sortReadSetByKey);
      mergedReadSet->mutable_read_set()->erase(std::unique(mergedReadSet->mutable_read_set()->begin(), mergedReadSet->mutable_read_set()->end(), equalReadMsg), mergedReadSet->mutable_read_set()->end()); //Erase duplicates...

      std::sort(mergedReadSet->mutable_deps()->begin(), mergedReadSet->mutable_deps()->end(), sortDepSet);
      mergedReadSet->mutable_deps()->erase(std::unique(mergedReadSet->mutable_deps()->begin(), mergedReadSet->mutable_deps()->end(), equalDep), mergedReadSet->mutable_deps()->end()); //Erase duplicates...
   }
    catch(...) {
      //restoreTxn(txn); //TODO: Maybe don't delete merged set -- we do want to use it for Commit again. //TODO: Maybe we cannot store mergedSet inside read after all? What if another thread tries to use Tx in parallel mid modification..
      Debug("Merge indicates duplicate key with different version. Vote Abstain");
      return proto::ConcurrencyControl::ABSTAIN;
    }
  }
  
   //add mergedreadSet to tx - return success
  txn.set_allocated_merged_read_set(mergedReadSet);
  readSet = &txn.merged_read_set().read_set();
  depSet = &txn.merged_read_set().deps();

  //TODO: Add depSet handling here: Merge in original dep set; erase duplicates; when merging //FIXME: Turn into a dep. (ReadSet must store Deps then too.. ==> Re-factor this to be deps directly: That way Optimistic TS also can just include the TS in Write!)
  //TODO: At client: mergedSet only works on strings; would need to feed in the equality function for uniqueness.  Instantiate merged_list with KeyEqual. (could store pointers and make euqlity on deref pointers -> no copies made!)
                                                                                                                  //Use ordered set: That way don't need to supply hash function.

  Debug("Merge successful");
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

  proto::ConcurrencyControl::Result result;

  //Default -- merge does nothing if there are no queries
  const ReadSet *readSet = &txn.read_set(); 
  const DepSet *depSet = &txn.deps(); 

  Debug("BASE READ SET");
  for(auto &read : *readSet){
      Debug("[group base] Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
  }
   Debug("Base Deps"); //FIXME: Remove.
  for(auto &dep : *depSet){
      Debug("[group base] Dep %s", BytesToHex(dep.write().prepared_txn_digest(), 16).c_str());
  }

  //Merge query read sets -- returs immediately if there are no queries 
  result = mergeTxReadSets(readSet, depSet, txn, txnDigest, reqId, remote, isGossip);
  //If we have an early abstain or Wait (due to invalid request) then return early.
  if(result != proto::ConcurrencyControl::COMMIT){  //query either invalid (Ignore), doomed to fail (abstain/abort), or queries not ready (Wait)
    Debug("Returning. Merge indicated query read sets are not ready or invalid");
    return result;  //NOTE: Could optimize and turn Abstains into full Abort if we used duplicate reads as proof. (would have to distinguish from the abstains caused by cached mismatch)
  }
  //Note: if we wait, we might end up never garbage collecting TX from ongoing (and possibly from other replicas Prepare set - since the tx is blocked); Can garbage collect after some time if desired (since we didn't process, theres no impact on decisions)
  //If another client is interested, then it should start fallback and provide read set as well (forward SyncProposal with correct retry version)
    
  Debug("TESTING MERGED READ"); //FIXME: Remove.
  for(auto &read : *readSet){
      Debug("[group Merged] Read key %s with version [%lu:%lu]", read.key().c_str(), read.readtime().timestamp(), read.readtime().id());
  }

  Debug("TESTING MERGED Deps"); //FIXME: Remove.
  for(auto &dep : *depSet){
      Debug("[group Merged] Dep %s", BytesToHex(dep.write().prepared_txn_digest(), 16).c_str());
  }


  locks_t locks;
  //lock keys to perform an atomic OCC check when parallelizing OCC checks.
  if(params.parallel_CCC){
    Debug("Parallel OCC: Locking read/write keys for txn: %s", BytesToHex(txnDigest, 16).c_str());
    locks = LockTxnKeys_scoped(txn, *readSet);
  }

  //TESTCODE for dependency wake-up
  // std::string dummyTx("dummyTx");
  // proto::Dependency dep;
  // dep.set_involved_group(0);
  // *dep.mutable_write()->mutable_prepared_txn_digest() = dummyTx;
  // proto::Dependency *new_dep = txn.add_deps();
  // *new_dep = dep;
  
  switch (occType) {
    case MVTSO:
      result = DoMVTSOOCCCheck(reqId, remote, txnDigest, txn, *readSet, *depSet, conflict, abstain_conflict, fallback_flow, isGossip);
      break;
    case TAPIR:
      result = DoTAPIROCCCheck(txnDigest, txn, retryTs);
      break;
    default:
      Panic("Unknown OCC type: %d.", occType);
      return proto::ConcurrencyControl::ABORT;
  }
  //TODO: Call Restore
  //restoreTxn(txn);
  return result;
}

//TODO: Create argument: const ReadSet &readSet
//TODO: Abort by default if we receive a Timestamp that already exists for a key (duplicate version) -- byz client might do this, but would get immediately reported.
proto::ConcurrencyControl::Result Server::DoMVTSOOCCCheck(
    uint64_t reqId, const TransportAddress &remote,
    const std::string &txnDigest, const proto::Transaction &txn, const ReadSet &readSet, const DepSet &depSet,
    const proto::CommittedProof* &conflict, const proto::Transaction* &abstain_conflict,
    bool fallback_flow, bool isGossip) {
  Debug("DoMVTSOCheck[%lu:%lu][%s] with ts %lu.%lu.",
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
    for (const auto &read : readSet){//txn.read_set()) {
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

        for (const auto preparedReadTxn : preparedReadsItr->second.second) {  //Check if we conflict with any prepared Read ==> TODO: Technically only need to check for all reads with ts greater than this tx... 
          
          if(! (ts <Timestamp(preparedReadTxn->timestamp())) ) continue; // Txn writes do not conflict with prepared Txn of smaller TS

          
        
           //Yes... Because if prepared Read (issued by a correct client; else it doesnt need ser) has this txn as dep --> and a correct replica mustve reported it --> thus our timestamp must be lower than the prep tx.
        
          
          // const DepSet *depSet = &preparedReadTxn->deps();
          // // if(preparedReadTxn->has_merged_read_set){  //Note: Could check whether it has queries and those have deps, and if so Assert that txn must have merged_read_set!   UW_ASSERT(txn.has_merged_read_set());
          // //   Debug("txn %s has merged_read_set; using instead.", BytesToHex(txnDigest, 16).c_str());
          // //   depSet = &preparedReadTxn->merged_read_set().deps();
          // // } 
          //    //Note: MergedReadSet guarantees no duplicate Reads. 
          //   //Cornercase: What if we get rid off a read during merge, but still have the associated dependency recorded? -> Won't happen, if we get rid of a read then we vote abort!!
          //   //Necessary Invariant: All dependencies in merged_read_set belong to a read in merged_read_set; and merged_read_set is consistent.-> Can have more deps than reads. 
          //   //However: can have dependencies for txn that are in sync snapshot but not in read set. (Can happen if byz reports everything in snapshot as dep) 
          //   // --> this check can use a dep that is not part of the read set to skip conflicts. That is unsafe. -> Don't use it for query dep sets.

          // bool isDep = false;
          // for (const auto &dep : depSet){ //preparedReadTxn->deps()) {
          //   if (txnDigest == dep.write().prepared_txn_digest()) {  
          //     isDep = true;
          //     break;
          //   }
          // }
          //  if(isDep) continue; //Txn writes do not conflict with prepared Txn that read (depend on) the write

          //FIXME: Remove faulty isDep check: not safe for multithreading reads and prepare; safe but obsolete for single threading: a correct isDep should be equivalent to isReadVersionEarlier returning false ==> since dep on the key implies readTS == ts.
          //Note: Faulty isDep check could result in some false Prepares (when it should have aborted); however, the odds of this are exceedingly rare for experiments with correct replicas, so removing the test should not affect performance
                                                                                                        //Scenario: Tx1 prepares at R1, and both w_a1 and w_b1 are written to prepared Writes (non-atomically)
                                                                                                        //          Tx2 reads r_a1 and forms a dependency, but reads r_b0
                                                                                                        //          Tx2 prepares at R2.
                                                                                                        // Consequently, R2 must vote to abort Tx1 because it conflicts with the prepared r_b0. This would cascade to abort Tx2 as well
                                                                                                        // However our isDep check would skip evaluating any conflicts because of the dep on r_a1. Then both Tx falsely commit.



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
          // if (!isDep && isReadVersionEarlier &&
          //     ts < Timestamp(preparedReadTxn->timestamp())) {
          if (isReadVersionEarlier) { //If prepared Txn with greater TS read a write with Ts smaller than the current tx -> abstain from preparing curr tx
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
            //fprintf(stderr, "rts TS: %lx bigger than ts %lx from client %d", rtsItr->second.load(), ts.getTimestamp(), ts.getID());
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

    //5) Validate Deps: If we don't check for dep signature proofs --> check whether server has prepared dep itself
    if (params.validateProofs && params.signedMessages && !params.verifyDeps) {
      proto::ConcurrencyControl::Result res = proto::ConcurrencyControl_Result_COMMIT;
      //Check Read deps
      CheckDepLocalPresence(txn, depSet, res);
      if(res != proto::ConcurrencyControl::COMMIT) return res;
    
    }
    //6) Prepare Transaction: No conflicts, No dependencies aborted --> Make writes visible.
    Prepare(txnDigest, txn, readSet);
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

//TODO: CheckDepPresence, ManageDeps, CheckDependencies txn --> implement versions for queries:  Input: const ReadSet &readSet,
//Then: re-factor existing deps to also just be strings? Or re-factor new ones to be deps..  (Move those deps into readSet)
//Then: Upon waking, retrieve mergedreadset from buffer to call checkDependencies on query deps.
//TODO: Client side dep merging

void Server::CheckTxLocalPresence(const std::string &txn_id, proto::ConcurrencyControl::Result &res){
  //Checks only for those deps that have not committed/aborted already.

  //If a dep has already aborted, abort as well.
  if(aborted.find(txn_id) != aborted.end()){
      stats.Increment("cc_aborts", 1);
      stats.Increment("cc_aborts_dep_aborted_early", 1);
      res = proto::ConcurrencyControl::ABSTAIN;
      return; //Technically could fully Abort here: But then need to attach also proof of dependency abort --> which currently is not implemented/supported
    }
    
    if (committed.find(txn_id) == committed.end() && aborted.find(txn_id) == aborted.end() ) {
    //If it has not yet aborted, nor committed, check whether the replica has prepared the tx itself: This alleviates having to verify dep proofs, although it is slightly more pessimistic

      preparedMap::const_accessor a2;
      auto isPrepared = prepared.find(a2, txn_id);
      if(!isPrepared){
        stats.Increment("cc_aborts", 1);
        stats.Increment("cc_aborts_dep_not_prepared", 1);
        res = proto::ConcurrencyControl::ABSTAIN;
        return; 
      }
      a2.release();
    }
}

// void Server::CheckQueryDepLocalPresence(const ReadSet &readSet, proto::ConcurrencyControl::Result &res){
//   Debug("Exec Dep Local Verification by CPU: %d", sched_getcpu());
//   for (const auto &dep_id : readSet.dep_ids()) {
//       CheckTxLocalPresence(dep_id, res);
//       if(res != proto::ConcurrencyControl::COMMIT) return;
//   }

//   //Same for timestamp id's
//    for (const auto &dep_ts_id : readSet.dep_ts_ids()) {
//       CheckTxLocalPresence(dep_ts_id.dep_it(), res);
//       if(res != proto::ConcurrencyControl::COMMIT) return;
//   }
//   return;
// }


void Server::CheckDepLocalPresence(const proto::Transaction &txn, const DepSet &depSet, proto::ConcurrencyControl::Result &res){
  Debug("Exec Dep Local Verification by CPU: %d", sched_getcpu());
  for (const auto &dep : depSet) { //txn.deps() {
    if (dep.involved_group() != groupIdx) {
      continue;
    }
    CheckTxLocalPresence(dep.write().prepared_txn_digest(), res);
    if(res != proto::ConcurrencyControl::COMMIT) return;
  }

  // //Check queries;
  // for (const auto &dep_id : readSet.dep_ids()) {
  //     CheckTxLocalPresence(dep_id, res);
  //     if(res != proto::ConcurrencyControl::COMMIT) return;
  // }

  // //Same for timestamp id's
  //  for (const auto &dep_ts_id : readSet.dep_ts_ids()) {
  //     CheckTxLocalPresence(dep_ts_id.dep_id(), res);
  //     if(res != proto::ConcurrencyControl::COMMIT) return;
  // }
  return;
}

//TODO: relay Deeper depth when result is already wait. (If I always re-did the P1 it would be handled)
// PRoblem: Dont want to re-do P1, otherwise a past Abort can turn into a commit. Hence we
// ForwardWriteback
bool Server::ManageDependencies_WithMutex(const std::string &txnDigest, const proto::Transaction &txn, const TransportAddress &remote, uint64_t reqId, bool fallback_flow, bool isGossip){
  
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

         if(true && !params.no_fallback && !isGossip){ //do not send relay if it is a gossiped message. Unless we are doinig replica leader gargabe Collection (unimplemented)
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
         waitingDependencies_new.insert(f, txnDigest);
        //  bool dependenciesItr = waitingDependencies_new.find(f, txnDigest);
        //  if (!dependenciesItr) {
        //    waitingDependencies_new.insert(f, txnDigest);
        //    //f->second = WaitingDependency();
        //  }
         
        if(!fallback_flow && !isGossip){ //NOTE: Original Client subscription moved to P1Meta
        //    f->second.original_client = true;
            f->second.reqId = reqId;
        //    f->second.remote = remote.clone();  //&remote;
        }
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

//MUTEX FREE DEPENDENCY WAIT VERSIONS

bool Server::RegisterWaitingTxn(const std::string &dep_id, const std::string &txnDigest, const proto::Transaction &txn, const TransportAddress &remote, 
const uint64_t &reqId, bool fallback_flow, bool isGossip, std::vector<const std::string*> &missing_deps, const bool new_waiting_dep){
  bool isFinished = true;
  
  dependentsMap::accessor e;
  bool first_dependent = false;
  
  //If waiting_dep is already logged then don't take any accessor on e to modify dependents 
  if(new_waiting_dep) first_dependent = dependents.insert(e, dep_id);
  
  if (committed.find(dep_id) == committed.end() && aborted.find(dep_id) == aborted.end()) {
      Debug("[%lu:%lu][%s] WAIT for dependency %s to finish.", txn.client_id(), txn.client_seq_num(), BytesToHex(txnDigest, 16).c_str(), BytesToHex(dep_id, 16).c_str());
    
    isFinished = false;
    missing_deps.push_back(&dep_id);
    if(new_waiting_dep){
      e->second.insert(txnDigest);
      Debug("Tx:[%s] Added tx %s to %s dependents.", BytesToHex(txnDigest, 16).c_str(), BytesToHex(txnDigest, 16).c_str(), BytesToHex(dep_id, 16).c_str());
    } 

    //XXX start RelayP1 to initiate Fallback handling
      if(true && !params.no_fallback && !isGossip){ //do not send relay if it is a gossiped message. Unless we are doinig replica leader gargabe Collection (unimplemented)
          uint64_t conflict_id = !fallback_flow ? reqId : -1;
          SendRelayP1(remote, dep_id, conflict_id, txnDigest);
      }
  }
  else{ //Don't add to dependents for no reason.
    if(new_waiting_dep && first_dependent) dependents.erase(e);
  }
  e.release();
  
  return isFinished;
}
//FIXME: Problem: IF several Queries (or query and main read set) have the same deps then Concurrency DeadLock free invariant does not hold?
//--> Must merge and erase all duplicates :: TODO: In Merge: Filter out only the involved groups. ==> Easiest: Merge all queries into a dep set (with no duplicates). And then erase from it everything in main Deps.
//TODO: Create for all deps a Dep item; Sort and erase duplicates
//OR: Easier: record all deps -- skip duplicates.

//TODO: When Calling ManageDeps directly (In handleP1 or HandleP1FB) must fetch merged ReadSet from buffer.
//TODO: Change ManageDependencies and Check dependencies to take in pointer to DepSet. (can either be direct read set, or merged ReadSet)

bool Server::ManageDependencies(const std::string &txnDigest, const proto::Transaction &txn, const TransportAddress &remote, const uint64_t reqId, bool fallback_flow, bool isGossip){
    
    
    const DepSet *depSet = &txn.deps();

    if(txn.has_merged_read_set()){  //Note: Could check whether it has queries and those have deps, and if so Assert that txn must have merged_read_set!   UW_ASSERT(txn.has_merged_read_set());
      Debug("txn %s has merged_read_set; using instead.", BytesToHex(txnDigest, 16).c_str());
      depSet = &txn.merged_read_set().deps();
    } 
    return ManageDependencies(*depSet, txnDigest, txn, remote, reqId, fallback_flow, isGossip);
}

//TODO: relay Deeper depth when result is already wait. (If I always re-did the P1 it would be handled)
// PRoblem: Dont want to re-do P1, otherwise a past Abort can turn into a commit. Hence we ForwardWriteback
bool Server::ManageDependencies(const DepSet &depSet, const std::string &txnDigest, const proto::Transaction &txn, const TransportAddress &remote, const uint64_t &reqId, bool fallback_flow, bool isGossip){
  
  bool allFinished = true;

  if(params.maxDepDepth > -2){ //Only check if deps enabled

    //Why Concurrency is safe:
    //Inuition: If ManageDependencies does not hold f and e, then CheckDependents dependents.find(e) will return false -> and thus it will never try to lock f --> and thus a lock order inversion is impossible
    //Cornercase: What ManageDependencies is called twice (e.g. by 2 threads): 
    //    E.g. ManageDependencies.1 finishes fully (writes to f and e) --> Then CheckDependents "dependents.find(e)" will return false (and thus it will try to lock f). Concurrently ManageDeps.2 locks f and tries to lock e ==> deadlock
        //Solution: ManageDependencies must only acquire a lock on e if waitingDeps does not exist (i.e. only the first time) ==> new_waiting_dep ensures this. 
                    // Any consecutive ManageDependency calls only loop through dependencies to check for RelayP1 (but don't set e)

    waitingDependenciesMap::accessor f;
    bool new_waiting_dep = waitingDependencies_new.insert(f, txnDigest);

    std::unordered_set<std::string> checked_deps; //Ignore duplicates

    std::vector<const std::string*> missing_deps;

    Debug("Called ManageDependencies for txn: %s", BytesToHex(txnDigest, 16).c_str());
    Debug("Manage Dependencies runs on Thread: %d", sched_getcpu());
    for (const auto &dep : depSet) { //txn.deps()) {
       if (dep.involved_group() != groupIdx) { //only check deps at the responsible shard.
         continue;
       }
      //Check if dep is committed/aborted -- if not, register waiting txn in dependents and startRelayP1.
      allFinished = RegisterWaitingTxn(dep.write().prepared_txn_digest(), txnDigest, txn, remote, reqId, fallback_flow, isGossip, missing_deps, new_waiting_dep);
    }

    if(!allFinished){
      if(!fallback_flow && !isGossip) f->second.reqId = reqId;

      if(new_waiting_dep){ //don't copy missing_deps into waitingDeps if it already exists.
        for(auto missing_dep : missing_deps){
            f->second.deps.insert(*missing_dep);
        }
      }
    }
    else{ //If there was no reason to add to waitingDeps erase again.
      if(new_waiting_dep) waitingDependencies_new.erase(f);
    }

    f.release();
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

void Server::CheckDependents_WithMutex(const std::string &txnDigest) {
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
        //Debug("print remote: %p", f->second.remote);
        //waitingDependencies.erase(dependent);
       //Note: When waking up from dependency commit/abort -> cannot have any conflict or abstain_conflict for dependent
        const proto::CommittedProof *conflict = nullptr;
        const proto::Transaction *abstain_conflict = nullptr;

       // BufferP1Result(result, conflict, dependent, 2);

        const TransportAddress *remote_original = nullptr;
        uint64_t req_id;
        bool wake_fallbacks = false;
        bool sub_original = BufferP1Result(result, conflict, dependent, req_id, remote_original, wake_fallbacks, false, 2);
        //std::cerr << "[Normal] release lock for txn: " << BytesToHex(txnDigest, 64) << std::endl;
        if(sub_original){
          Debug("Sending Phase1 Reply for txn: %s, id: %d", BytesToHex(dependent, 64).c_str(), req_id);
          SendPhase1Reply(req_id, result, conflict, dependent, remote_original, abstain_conflict); 
        }

        // if(f->second.original_client){
        //   Debug("Sending Phase1 Reply for txn: %s, id: %d", BytesToHex(dependent, 64).c_str(), f->second.reqId);
        //   SendPhase1Reply(f->second.reqId, result, conflict, dependent, f->second.remote);
        //   delete f->second.remote;
        // }

        //Send it to all interested FB clients too:
        WakeAllInterestedFallbacks(dependent, result, conflict);
        /////

        waitingDependencies_new.erase(f);   //TODO: Can remove CleanDependencies since it's already deleted here?
      }
      f.release();
    }
    //dependents.erase(e);
  }
  e.release();
   //if(params.mainThreadDispatching) dependentsMutex.unlock();
   if(params.mainThreadDispatching) waitingDependenciesMutex.unlock();
}

void Server::CheckDependents(const std::string &txnDigest) {

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
      if(!dependenciesItr) continue;

      f->second.deps.erase(txnDigest);
      Debug("Removed %s from waitingDependencies of %s.", BytesToHex(txnDigest, 16).c_str(), BytesToHex(dependent, 16).c_str());
      if (f->second.deps.size() == 0) {
        Debug("Dependencies of %s have all committed or aborted.",
            BytesToHex(dependent, 16).c_str());

        proto::ConcurrencyControl::Result result = CheckDependencies(
            dependent);
        UW_ASSERT(result != proto::ConcurrencyControl::ABORT);
      
       //Note: When waking up from dependency commit/abort -> cannot have any conflict or abstain_conflict for dependent
        const proto::CommittedProof *conflict = nullptr;
        const proto::Transaction *abstain_conflict = nullptr;

       // BufferP1Result(result, conflict, dependent, 2);

        const TransportAddress *remote_original = nullptr;
        uint64_t req_id;
        bool wake_fallbacks = false;
        bool sub_original = BufferP1Result(result, conflict, dependent, req_id, remote_original, wake_fallbacks, false, 2);
        //std::cerr << "[Normal] release lock for txn: " << BytesToHex(txnDigest, 64) << std::endl;
        if(sub_original){
          Debug("Sending Phase1 Reply for txn: %s, id: %d", BytesToHex(dependent, 64).c_str(), req_id);
          SendPhase1Reply(req_id, result, conflict, dependent, remote_original, abstain_conflict); 
        }
        //Send it to all interested FB clients too:
        WakeAllInterestedFallbacks(dependent, result, conflict);
       
        waitingDependencies_new.erase(f);   //TODO: Can remove CleanDependencies since it's already deleted here?
      }
      f.release();
    }
    dependents.erase(e);
  }
  e.release();
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

  const DepSet *depSet = &txn.deps();
  if(txn.has_merged_read_set()){  //Note: Could check whether it has queries and those have deps, and if so Assert that txn must have merged_read_set!   UW_ASSERT(txn.has_merged_read_set());
      Debug("txn [%lu:%lu] has merged_read_set; using instead.", txn.client_id(), txn.client_seq_num());
      depSet = &txn.merged_read_set().deps();
  } 
  return CheckDependencies(txn, *depSet);
}

proto::ConcurrencyControl::Result Server::CheckDependencies(
    const proto::Transaction &txn, const DepSet &depSet) {
  
   //if(params.mainThreadDispatching) committedMutex.lock_shared();
  for (const auto &dep : depSet) { // txn.deps()) {
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

void Server::CleanDependencies_WithMutex(const std::string &txnDigest) {
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

void Server::CleanDependencies(const std::string &txnDigest) {
  Debug("Called CleanDependencies for txn %s", BytesToHex(txnDigest, 16).c_str());

  waitingDependenciesMap::accessor f;
  bool dependenciesItr = waitingDependencies_new.find(f, txnDigest);
  if (dependenciesItr ) {
    waitingDependencies_new.erase(f);
  }
  f.release();

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
  //if(ts>highWatermark) Panic("ts: %lx, highWatermark: %lx", ts.getTimestamp(), highWatermark.getTimestamp());
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
  Prepare(txnDigest, txn, txn.read_set());

  Debug("[%s] PREPARED TO COMMIT", txnDigest.c_str());

  return proto::ConcurrencyControl::COMMIT;
}


locks_t Server::LockTxnKeys_scoped(const proto::Transaction &txn, const ReadSet &readSet) {

    // timeval tv1;
    // gettimeofday(&tv1, 0);
    // int id = std::rand();
    // std::cerr << "starting locking for id: " << id << std::endl;
    locks_t locks;

    //const ReadSet &readSet = txn.read_set();
    const WriteSet &writeSet = txn.write_set();

    auto itr_r = readSet.begin();
    auto itr_w = writeSet.begin();

    //for(int i = 0; i < readSet.size() + writeSet.size(); ++i){
    while(itr_r != readSet.end() || itr_w != writeSet.end()){
      //skip duplicate keys (since the list is sorted they should be next)
      if(itr_r != readSet.end() && std::next(itr_r) != readSet.end()){
        if(itr_r->key() == std::next(itr_r)->key()){
          itr_r++;
          continue;
        }
      }
      if(itr_w != writeSet.end() && std::next(itr_w) != writeSet.end()){
        if(itr_w->key() == std::next(itr_w)->key()){
          itr_w++;
          continue;
        }
      }
      //lock and advance read/write respectively if the other set is done
      if(itr_r == readSet.end()){
        // std::cerr<< "Locking Write [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_w->key(), 16).c_str() << "]" << std::endl;
        locks.emplace_back(mutex_map[itr_w->key()]);
        itr_w++;
      }
      else if(itr_w == writeSet.end()){
        // std::cerr<< "Locking Read [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_r->key(), 16).c_str() << "]" << std::endl;
        locks.emplace_back(mutex_map[itr_r->key()]);
        itr_r++;
      }
      //lock and advance read/write iterators in order
      else{
        if(itr_r->key() <= itr_w->key()){
          // std::cerr<< "Locking Read/Write [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_r->key(), 16).c_str() << "]" << std::endl;
          locks.emplace_back(mutex_map[itr_r->key()]);
          //If read set and write set share keys, must not acquire lock twice.
          if(itr_r->key() == itr_w->key()) {
            itr_w++;
          }
          itr_r++;
        }
        else{
          // std::cerr<< "Locking Write [" << txn.client_id() << "," << txn.client_seq_num()  << " : " << BytesToHex(itr_w->key(), 16).c_str() << "]" << std::endl;
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

    const ReadSet &readSet = txn.read_set();
    const WriteSet &writeSet = txn.write_set();

    auto itr_r = readSet.begin();
    auto itr_w = writeSet.begin();
    //for(int i = 0; i < readSet.size() + writeSet.size(); ++i){
    while(itr_r != readSet.end() || itr_w != writeSet.end()){
      if(itr_r == readSet.end()){
        lock_keys[itr_w->key()].lock();
        itr_w++;
      }
      else if(itr_w == writeSet.end()){
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

    const ReadSet &readSet = txn.read_set();
    const WriteSet &writeSet = txn.write_set();

  // Lock all (read/write) keys in order for atomicity if using parallel OCC
    auto itr_r = readSet.rbegin();
    auto itr_w = writeSet.rbegin();
    //for(int i = 0; i < readSet.size() + writeSet.size(); ++i){
    while(itr_r != readSet.rend() || itr_w != writeSet.rend()){
      if(itr_r == readSet.rend()){
        lock_keys[itr_w->key()].unlock();
        itr_w++;
      }
      else if(itr_w == writeSet.rend()){
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