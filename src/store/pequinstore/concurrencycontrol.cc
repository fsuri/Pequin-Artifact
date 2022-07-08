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

proto::ConcurrencyControl::Result Server::DoOCCCheck(
    uint64_t reqId, const TransportAddress &remote,
    const std::string &txnDigest, const proto::Transaction &txn,
    Timestamp &retryTs, const proto::CommittedProof* &conflict,
    const proto::Transaction* &abstain_conflict,
    bool fallback_flow, bool isGossip) {

  locks_t locks;
  //lock keys to perform an atomic OCC check when parallelizing OCC checks.
  if(params.parallel_CCC){
    locks = LockTxnKeys_scoped(txn);
  }

  switch (occType) {
    case MVTSO:
      return DoMVTSOOCCCheck(reqId, remote, txnDigest, txn, conflict, abstain_conflict, fallback_flow, isGossip);
    case TAPIR:
      return DoTAPIROCCCheck(txnDigest, txn, retryTs);
    default:
      Panic("Unknown OCC type: %d.", occType);
      return proto::ConcurrencyControl::ABORT;
  }
}

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
          for (const auto &read : preparedReadTxn->read_set()) {
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
           // ongoingMap::const_accessor b;
           // bool inOngoing = ongoing.find(b, dep.write().prepared_txn_digest()); //TODO can remove this redundant lookup since it will be checked again...
           // if (inOngoing) {
           //   std::string dependency_txnDig = dep.write().prepared_txn_digest();
           //   RelayP1(dep.write().prepared_txn_digest(), fallback_flow, reqId, remote, txnDigest);
             uint64_t conflict_id = !fallback_flow ? reqId : -1;
             SendRelayP1(remote, dep.write().prepared_txn_digest(), conflict_id, txnDigest);
           // }
           // b.release();
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
         if(!fallback_flow && !isGossip){
           f->second.original_client = true;
           f->second.reqId = reqId;
           f->second.remote = remote.clone();  //&remote;
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

        //p1MetaDataMap::accessor c;
        //BufferP1Result(c, result, conflict, dependent, 2);
        //c.release();
        BufferP1Result(result, conflict, dependent, 2);

        if(f->second.original_client){
          Debug("Sending Phase1 Reply for txn: %s, id: %d", BytesToHex(dependent, 64).c_str(), f->second.reqId);
          SendPhase1Reply(f->second.reqId, result, conflict, dependent,
              f->second.remote);
          delete f->second.remote;
        }

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
  ongoingMap::const_accessor b;
  bool isOngoing = ongoing.find(b, txnDigest);
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
  return CheckDependencies(*b->second.txn);
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
      ongoingMap::const_accessor b;
      bool oitr = ongoing.find(b, dep.write().prepared_txn_digest());
      if(oitr){
      //if (oitr != ongoing.end()) {
        //q.push(std::make_pair(oitr->second, curr.second + 1));
        q.push(std::make_pair(b->second.txn, curr.second + 1));
      }
      b.release();
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