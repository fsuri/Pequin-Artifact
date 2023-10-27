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
#ifndef PEQUIN_COMMON_H
#define PEQUIN_COMMON_H

#include "lib/configuration.h"
#include "lib/keymanager.h"
#include "store/common/timestamp.h"
#include "store/pequinstore/query-proto.pb.h"
#include "store/pequinstore/pequin-proto.pb.h"
#include "lib/latency.h"
#include "store/pequinstore/verifier.h"
#include "lib/tcptransport.h"

#include <stack>
#include <map>
#include <string>
#include <vector>
#include <functional>
#include <algorithm>
#include <mutex>
#include "tbb/concurrent_vector.h"

#include <google/protobuf/message.h>

#include "store/common/stats.h"

#include "store/common/failures.h"

#include "store/common/backend/sql_engine/table_kv_encoder.h"

#include "lib/compression/TurboPFor-Integer-Compression/vp4.h"
#include "lib/compression/FrameOfReference/include/compression.h"
#include "lib/compression/FrameOfReference/include/turbocompression.h"

namespace pequinstore {



static bool LocalDispatch = true; //TODO: Turn into config flag if a viable option.

typedef std::function<void()> signedCallback;
typedef std::function<void()> cleanCallback;
//typedef std::function<void(void*)> verifyCallback;
typedef std::function<void(void*)> mainThreadCallback; //TODO change back to this...
//typedef std::function<void(bool)> mainThreadCallback;


struct Triplet {
  Triplet() {};
  Triplet(::google::protobuf::Message* msg,
  proto::SignedMessage* sig_msg,
  signedCallback cb) : msg(msg), sig_msg(sig_msg), cb(cb) { };
  ~Triplet() { };
  ::google::protobuf::Message* msg;
  proto::SignedMessage* sig_msg;
  signedCallback cb;
};



//static bool True = true;
//static bool False = false;

static std::vector<std::string*> MessageStrings;
static std::mutex msgStr_mutex;
std::string* GetUnusedMessageString();
void FreeMessageString(std::string *str);

//TODO: re-use objects?
struct asyncVerification{
  asyncVerification(uint32_t _quorumSize, mainThreadCallback mcb, int groupTotals,
    proto::CommitDecision _decision, Transport* tp) :  quorumSize(_quorumSize),
    mcb(mcb), groupTotals(groupTotals), decision(_decision),
    terminate(false), callback(true), tp(tp) { }
  ~asyncVerification() { deleteMessages();}

  std::mutex objMutex;
  Transport* tp;
  std::vector<std::string*> ccMsgs;

  void deleteMessages(){
    for(auto ccMsg : ccMsgs){
      FreeMessageString(ccMsg);//delete ccMsg;
    }
  }

  uint32_t quorumSize;
  //std::function<void(bool)> mainThreadCallback;
  mainThreadCallback mcb;

  std::map<uint64_t, uint32_t> groupCounts;
  int groupTotals;
  int groupsVerified = 0;

  proto::CommitDecision decision;
  //proto::Transaction *txn;
  //std::set<int> groupsVerified;

  int deletable;
  bool terminate;
  bool callback;
};


template<typename T> static void* pointerWrapper(std::function<T()> func){
    T* t = new T; //(T*) malloc(sizeof(T));
    *t = func();
    return (void*) t;
}

void* BoolPointerWrapper(std::function<bool()> func);

void SignMessage(const ::google::protobuf::Message* msg,
    crypto::PrivKey* privateKey, uint64_t processId,
    proto::SignedMessage *signedMessage);

void* asyncSignMessage(const::google::protobuf::Message* msg,
    crypto::PrivKey* privateKey, uint64_t processId,
    proto::SignedMessage *signedMessage);

void SignMessages(const std::vector<::google::protobuf::Message*>& msgs,
    crypto::PrivKey* privateKey, uint64_t processId,
    const std::vector<proto::SignedMessage*>& signedMessages,
    uint64_t merkleBranchFactor);

void SignMessages(const std::vector<Triplet>& batch,
    crypto::PrivKey* privateKey, uint64_t processId,
    uint64_t merkleBranchFactor);

void* asyncSignMessages(const std::vector<::google::protobuf::Message*> msgs,
    crypto::PrivKey* privateKey, uint64_t processId,
    const std::vector<proto::SignedMessage*> signedMessages,
    uint64_t merkleBranchFactor);

void asyncValidateCommittedConflict(const proto::CommittedProof &proof,
    const std::string *committedTxnDigest, const proto::Transaction *txn,
    const std::string *txnDigest, bool signedMessages, KeyManager *keyManager,
    const transport::Configuration *config, Verifier *verifier,
    mainThreadCallback mcb, Transport *transport, bool multithread = false, bool batchVerification = false);

void asyncValidateCommittedProof(const proto::CommittedProof &proof,
    const std::string *committedTxnDigest, KeyManager *keyManager,
    const transport::Configuration *config, Verifier *verifier,
    mainThreadCallback mcb, Transport *transport, bool multithread = false, bool batchVerification = false);

bool ValidateCommittedConflict(const proto::CommittedProof &proof,
    const std::string *committedTxnDigest, const proto::Transaction *txn,
    const std::string *txnDigest, bool signedMessages, KeyManager *keyManager,
    const transport::Configuration *config, Verifier *verifier);

bool ValidateCommittedProof(const proto::CommittedProof &proof,
    const std::string *committedTxnDigest, KeyManager *keyManager,
    const transport::Configuration *config, Verifier *verifier);

void* ValidateP1RepliesWrapper(proto::CommitDecision decision,
    bool fast,
    const proto::Transaction *txn,
    const std::string *txnDigest,
    const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager,
    const transport::Configuration *config,
    int64_t myProcessId, proto::ConcurrencyControl::Result myResult, Verifier *verifier);

void asyncBatchValidateP1Replies(proto::CommitDecision decision, bool fast, const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs, KeyManager *keyManager,
    const transport::Configuration *config, int64_t myProcessId, proto::ConcurrencyControl::Result myResult,
    Verifier *verifier, mainThreadCallback mcb, Transport *transport, bool multithread = false);

void asyncValidateP1Replies(proto::CommitDecision decision, bool fast, const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs, KeyManager *keyManager,
    const transport::Configuration *config, int64_t myProcessId, proto::ConcurrencyControl::Result myResult,
    Verifier *verifier, mainThreadCallback mcb, Transport *transport, bool multithread = false);

void asyncValidateP1RepliesCallback(asyncVerification* verifyObj, uint32_t groupId, void* result);
//void ThreadLocalAsyncValidateP1RepliesCallback(asyncVerification* verifyObj, uint32_t groupId, void* result);

bool ValidateP1Replies(proto::CommitDecision decision, bool fast,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::ConcurrencyControl::Result myResult, Verifier *verifier);

bool ValidateP1Replies(proto::CommitDecision decision, bool fast,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::ConcurrencyControl::Result myResult,
    Latency_t &lat, Verifier *verifier);

void* ValidateP2RepliesWrapper(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier);

void asyncBatchValidateP2Replies(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread = false);

void asyncValidateP2Replies(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread = false);

void asyncValidateP2RepliesCallback(asyncVerification* verifyObj, uint32_t groupId, void* result);
//void ThreadLocalAsyncValidateP2RepliesCallback(asyncVerification* verifyObj, uint32_t groupId, void* result);

bool ValidateP2Replies(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier);

bool ValidateP2Replies(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::GroupedSignatures &groupedSigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision,
    Latency_t &lat, Verifier *verifier);

//Fallback verifications:

void asyncValidateFBP2Replies(proto::CommitDecision decision,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::P2Replies &p2Replies,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread = false);

bool VerifyFBViews(uint64_t proposed_view, bool catch_up, uint64_t logGrp,
    const std::string *txnDigest, const proto::SignedMessages &signed_messages,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, uint64_t myCurrentView, Verifier *verifier);

void asyncVerifyFBViews(uint64_t view, bool catch_up, uint64_t logGrp,
    const std::string *txnDigest, const proto::SignedMessages &signed_messages,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, uint64_t myView, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread);

bool ValidateFBDecision(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::Signatures &sigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier);

void asyncValidateFBDecision(proto::CommitDecision decision, uint64_t view,
    const proto::Transaction *txn,
    const std::string *txnDigest, const proto::Signatures &sigs,
    KeyManager *keyManager, const transport::Configuration *config,
    int64_t myProcessId, proto::CommitDecision myDecision, Verifier *verifier,
    mainThreadCallback mcb, Transport* transport, bool multithread);

//END Fallback verifications

bool ValidateTransactionWrite(const proto::CommittedProof &proof,
    const std::string *txnDigest, const std::string &key, const std::string &val, const Timestamp &timestamp,
    const transport::Configuration *config, bool signedMessages,
    KeyManager *keyManager, Verifier *verifier);

void asyncValidateTransactionWrite(const proto::CommittedProof &proof,
    const std::string *txnDigest,
    const std::string &key, const std::string &val, const Timestamp &timestamp,
    const transport::Configuration *config, bool signedMessages,
    KeyManager *keyManager, Verifier *verifier, mainThreadCallback cb, Transport* transport,
    bool multithread);

// check must validate that proof replies are from all involved shards
bool ValidateProofCommit1(const proto::CommittedProof &proof,
    const std::string &txnDigest,
    const transport::Configuration *config, bool signedMessages,
    KeyManager *keyManager);

bool ValidateProofAbort(const proto::CommittedProof &proof,
    const transport::Configuration *config, bool signedMessages,
    bool hashDigest, KeyManager *keyManager);

bool ValidateP1RepliesCommit(
    const std::map<int, std::vector<proto::Phase1Reply>> &groupedP1Replies,
    const std::string &txnDigest, const proto::Transaction &txn,
    const transport::Configuration *config);

bool ValidateP2RepliesCommit(
    const std::vector<proto::Phase2Reply> &p2Replies,
    const std::string &txnDigest, const proto::Transaction &txn,
    const transport::Configuration *config);

bool ValidateP1RepliesAbort(
    const std::map<int, std::vector<proto::Phase1Reply>> &groupedP1Replies,
    const std::string &txnDigest, const proto::Transaction &txn,
    const transport::Configuration *config, bool signedMessages, bool hashDigest,
    KeyManager *keyManager);

bool ValidateP2RepliesAbort(
    const std::vector<proto::Phase2Reply> &p2Replies,
    const std::string &txnDigest, const proto::Transaction &txn,
    const transport::Configuration *config);


bool ValidateDependency(const proto::Dependency &dep,
    const transport::Configuration *config, uint64_t readDepSize,
    KeyManager *keyManager, Verifier *verifier);

bool operator==(const proto::Write &pw1, const proto::Write &pw2);

bool operator!=(const proto::Write &pw1, const proto::Write &pw2);

std::string TransactionDigest(const proto::Transaction &txn, bool hashDigest);

std::string QueryDigest(const proto::Query &query, bool queryHashDigest);
std::string QueryRetryId(const std::string &queryId, const uint64_t &retry_version, bool queryHashDigest);

std::string generateReadSetSingleHash(const proto::ReadSet &query_read_set); 
std::string generateReadSetSingleHash(const std::map<std::string, TimestampMessage> &read_set);
std::string generateReadSetMerkleRoot(const std::map<std::string, TimestampMessage> &read_set, uint64_t branch_factor);


void CompressTxnIds(std::vector<uint64_t>&txn_ts);
std::vector<uint64_t> DecompressTxnIds();

std::string BytesToHex(const std::string &bytes, size_t maxLength);

bool TransactionsConflict(const proto::Transaction &a,
    const proto::Transaction &b);

uint64_t QuorumSize(const transport::Configuration *config);
uint64_t FastQuorumSize(const transport::Configuration *config);
uint64_t SlowCommitQuorumSize(const transport::Configuration *config);
uint64_t FastAbortQuorumSize(const transport::Configuration *config);
uint64_t SlowAbortQuorumSize(const transport::Configuration *config);
bool IsReplicaInGroup(uint64_t id, uint32_t group,
    const transport::Configuration *config);

int64_t GetLogGroup(const proto::Transaction &txn, const std::string &txnDigest);

inline static bool sortReadSetByKey(const ReadMessage &lhs, const ReadMessage &rhs) { 
    //UW_ASSERT(lhs.key() != rhs.key());  //Read Set should not contain same key twice (doomed to abort) 
                                          //==> Currenty this might happen since different queries might read the same read set & read sets are stored as list currently instead of a set
                                          //"Hacky way": Simulate set by checking whether list contains entry using std::find, e.g. std::find(read_set.begin(), read_set.end(), ReadMsg) == fields.end()
    if(lhs.key() == rhs.key()){
        //Panic("duplicate read set key"); //FIXME: Just for testing.
        //If a tx reads a key twice with different versions throw exception ==> Since we never add duplicates to the ReadSet, this case will never get triggered client side.
        if(lhs.readtime().timestamp() != rhs.readtime().timestamp() || lhs.readtime().id() != rhs.readtime().id()){ 
        //Note: What about an app corner case in which you want to read your own write? Such reads don't have to be added to Read Set -- they are valid by default.
        //Note: See ShardClient "BufferGet" -- we either read our own write, or read previously read value => thus it is impossible to read 2 different TS. We don't add such reads to ReadSet
             //Panic("duplicate read set key with different TS");
             throw std::runtime_error("Read set contains two reads of the same key with different timestamp");
        }
        //return (lhs.readtime().timestamp() == rhs.readtime().timestamp()) ? lhs.readtime().id() < rhs.readtime().id() : lhs.readtime().timestamp() < rhs.readtime().timestamp(); 
    }
    return lhs.key() < rhs.key(); 
}

inline static bool sortWriteSetByKey(const WriteMessage &lhs, const WriteMessage &rhs) { 
    //UW_ASSERT(lhs.key() != rhs.key()); //FIXME: Shouldn't write the same key twice. ==> Currently might happen since we store Write Set as List instead of Set.
    return lhs.key() < rhs.key(); 
}

inline static bool sortDepSet(const proto::Dependency &lhs, const proto::Dependency &rhs) { 
    return (lhs.write().prepared_txn_digest() == rhs.write().prepared_txn_digest() ? lhs.involved_group() < rhs.involved_group() : lhs.write().prepared_txn_digest() < rhs.write().prepared_txn_digest()) ; 
}


inline static bool equalReadMsg(const ReadMessage &lhs, const ReadMessage &rhs){
    return (lhs.key() == rhs.key()) && (lhs.readtime().timestamp() == rhs.readtime().timestamp()) && (lhs.readtime().id() == rhs.readtime().id());
}

inline static bool equalWriteMsg(const WriteMessage &lhs, const WriteMessage &rhs) {
    return lhs.key() == rhs.key(); 
}

inline static bool equalDep(const proto::Dependency &lhs, const proto::Dependency &rhs) { 
    return (lhs.write().prepared_txn_digest() == rhs.write().prepared_txn_digest() && lhs.involved_group() == rhs.involved_group()); 
}
inline static bool equalDepPtr(const proto::Dependency *&lhs, const proto::Dependency *&rhs) { 
    return equalDep(*lhs, *rhs);
    //return (lhs->write().prepared_txn_digest() == rhs->write().prepared_txn_digest() && lhs->involved_group() == rhs->involved_group()); 
}
inline static bool compDepWritePtr(const proto::Write *lhs, const proto::Write *rhs) { 
    return lhs->prepared_txn_digest() < rhs->prepared_txn_digest();
    //return (lhs->write().prepared_txn_digest() == rhs->write().prepared_txn_digest() && lhs->involved_group() == rhs->involved_group()); 
}



inline static bool compareReadSets (google::protobuf::RepeatedPtrField<ReadMessage> const &lhs, google::protobuf::RepeatedPtrField<ReadMessage> const &rhs){ // (proto::ReadSet const &lhs, proto::ReadSet const &rhs) {
    return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(), equalReadMsg); 
}


struct QueryReadSetMgr {
        QueryReadSetMgr(){}
        QueryReadSetMgr(proto::ReadSet *read_set, const uint64_t &groupIdx, const bool &useOptimisticId): read_set(read_set), groupIdx(groupIdx), useOptimisticId(useOptimisticId){
            read_set->Clear(); //Reset read set -- e.g. if we've already done eagerexec, and then we do snapshot read after
        }
        ~QueryReadSetMgr(){}

        void AddToReadSet(const std::string &key, const TimestampMessage &readtime, bool is_table_col_ver = false){
          Debug("Adding to ReadSet. Key: %s, with TS:[%lu:%lu]", key.c_str(), readtime.timestamp(), readtime.id());
          ReadMessage *read = read_set->add_read_set();
          //ReadMessage *read = query_md->queryResult->mutable_query_read_set()->add_read_set();
          read->set_key(key);
          *read->mutable_readtime() = readtime;

          if(is_table_col_ver) read->set_is_table_col_version(true);
        }

        void AddToReadSet(std::string &&key, const Timestamp &readtime){
        Debug("Adding to ReadSet. Key: %s, with TS:[%lu:%lu]", key.c_str(), readtime.getTimestamp(), readtime.getID());
           ReadMessage *read = read_set->add_read_set();
          //ReadMessage *read = query_md->queryResult->mutable_query_read_set()->add_read_set();
          read->set_key(std::move(key));
          readtime.serialize(read->mutable_readtime());
        }

        void AddToDepSet(const std::string &tx_id, const TimestampMessage &tx_ts){
            proto::Dependency *add_dep = read_set->add_deps();
            add_dep->set_involved_group(groupIdx);
            add_dep->mutable_write()->set_prepared_txn_digest(tx_id);
            Debug("Adding Dep: %s", BytesToHex(tx_id, 16).c_str());
            //Note: Send merged TS.
            if(useOptimisticId){
                //MergeTimestampId(txn->timestamp().timestamp(), txn->timestamp().id()
                *add_dep->mutable_write()->mutable_prepared_timestamp() = tx_ts;
                // add_dep->mutable_write()->mutable_prepared_timestamp()->set_timestamp(txn->timestamp().timestamp());
                // add_dep->mutable_write()->mutable_prepared_timestamp()->set_id(txn->timestamp().id());
            }
        }

        void AddToDepSet(const std::string &tx_id, const Timestamp &tx_ts){
            proto::Dependency *add_dep = read_set->add_deps();
            add_dep->set_involved_group(groupIdx);
            add_dep->mutable_write()->set_prepared_txn_digest(tx_id);
            Debug("Adding Dep: %s", BytesToHex(tx_id, 16).c_str());
            //Note: Send merged TS.
            if(useOptimisticId){
                //MergeTimestampId(txn->timestamp().timestamp(), txn->timestamp().id()
                add_dep->mutable_write()->mutable_prepared_timestamp()->set_timestamp(tx_ts.getTimestamp());
                add_dep->mutable_write()->mutable_prepared_timestamp()->set_id(tx_ts.getID());
            }
        }

      proto::ReadSet *read_set;
      uint64_t groupIdx;
      bool useOptimisticId;
};

// enum InjectFailureType {
//   CLIENT_EQUIVOCATE = 0,
//   CLIENT_CRASH = 1,
//   CLIENT_EQUIVOCATE_SIMULATE = 2,
//   CLIENT_STALL_AFTER_P1 = 3,
//   CLIENT_SEND_PARTIAL_P1 = 4
// };

// struct InjectFailure {
//   InjectFailure() { }
//   InjectFailure(const InjectFailure &failure) : type(failure.type),
//       timeMs(failure.timeMs), enabled(failure.enabled), frequency(failure.frequency) { }

//   InjectFailureType type;
//   uint32_t timeMs;
//   bool enabled;
//   uint32_t frequency;
// };



typedef struct QueryParameters {
    const bool sql_mode; //false ==> KV-store; true ==> SQL-store
    //protocol parameters
    const uint64_t syncQuorum; //number of replies necessary to form a sync quorum
    const uint64_t queryMessages; //number of query messages sent to replicas to request sync replies
    const uint64_t mergeThreshold; //number of tx instances required to observe to include in sync snapshot
    const uint64_t syncMessages;    //number of sync messages sent to replicas to request result replies
    const uint64_t resultQuorum ;   //number of matching query replies necessary to return
    
    const size_t snapshotPrepared_k; //number of prepared reads to include in the snapshot (before reaching first committed version)

    const bool eagerExec;   //Perform eager execution on Queries
    const bool eagerPointExec;  //Perform query style eager execution on point queries (instead of using proof)
    const bool eagerPlusSnapshot; //Perform eager exec and snapshot simultaneously
    
    const bool readPrepared; //read only committed or also prepared values in query?
    const bool cacheReadSet; //return query read set to client, or cache it locally at servers?
    const bool optimisticTxID; //use unique hash tx ids (normal ids), or optimistically use timestamp as identifier?
    const bool compressOptimisticTxIDs; //compress the ts Ids using integer compression.
   

    const bool mergeActiveAtClient; //When not caching read sets, merge query read sets at client

    const bool signClientQueries;
    const bool signReplicaToReplicaSync;

    //performance parameters
    const bool parallel_queries;

    QueryParameters(bool sql_mode, uint64_t syncQuorum, uint64_t queryMessages, uint64_t mergeThreshold, uint64_t syncMessages, uint64_t resultQuorum, size_t snapshotPrepared_k,
        bool eagerExec, bool eagerPointExec, bool eagerPlusSnapshot, bool readPrepared, bool cacheReadSet, bool optimisticTxID, bool compressOptimisticTxIDs, bool mergeActiveAtClient, 
        bool signClientQueries, bool signReplicaToReplicaSync, bool parallel_queries) : 
        sql_mode(sql_mode), syncQuorum(syncQuorum), queryMessages(queryMessages), mergeThreshold(mergeThreshold), syncMessages(syncMessages), resultQuorum(resultQuorum), snapshotPrepared_k(snapshotPrepared_k),
        eagerExec(eagerExec), eagerPointExec(eagerPointExec), eagerPlusSnapshot(eagerPlusSnapshot), readPrepared(readPrepared), cacheReadSet(cacheReadSet), optimisticTxID(optimisticTxID), compressOptimisticTxIDs(compressOptimisticTxIDs), mergeActiveAtClient(mergeActiveAtClient), 
        signClientQueries(signClientQueries), signReplicaToReplicaSync(signReplicaToReplicaSync), parallel_queries(parallel_queries) {
            if(eagerPlusSnapshot) UW_ASSERT(eagerExec); 
        }

} QueryParameters;

uint64_t MergeTimestampId(const uint64_t &timestamp, const uint64_t &id);

class TimestampCompressor {   //TODO: Re-factor TimestampCompressor to just be a functional interface (hold no data) --> 4 functions: CompressLocal, DecompressLocal, CompressMerged, DecompressMerged
                              //If we want to use 32 bit id's -> need buckets = need data. But currently only using 64 bit ids
 public:
    TimestampCompressor();
    virtual ~TimestampCompressor();
    void InitializeLocal(proto::LocalSnapshot *local_ss, bool compressOptimisticTxIds = false);
    void AddToBucket(const TimestampMessage &ts);
    void ClearLocal();
    void CompressLocal(proto::LocalSnapshot *local_ss);
    void DecompressLocal(proto::LocalSnapshot *local_ss);
    void CompressAll();
    void DecompressAll();
    //TODO: Add Merged
    std::vector<uint64_t> out_timestamps; //TODO: replace with the repeated field from local_ss
 private:
   proto::LocalSnapshot *local_ss;
   //google::protobuf::RepeatedPtrField<google::protobuf::bytes> *ts_ids;
   bool compressOptimisticTxIds;
   uint64_t num_timestamps;
   std::vector<uint64_t> timestamps; //TODO: replace with the repeated field from local_ss
   std::vector<uint64_t> ids;
   std::vector<uint8_t> _compressed_timestamps;
   std::vector<unsigned char> compressed_timestamps;
   //store to an ordered_set if Valid compressable TS. valid if 64bit time and 64bit cid can be merged into 1 64 bit number.
   // upon CompressAll -> split set into buckets (thus each bucket is sorted) --> then on each bucket, run integer compression. Add to bucket only if delta < 32bit
    //Buckets. Each bucket is a vecotr + delta off-set. Store buckets in order (linked-list?). Find correct bucket to insert by iterating through list(acces first, last for ordering)
    // better -> store buckets in a map<front, bucket>. Find correct bucket by upper/lower-bound ops. Insert new bucket where appropriate  (you learn left bucket min/max and right bucket min - if inbetween, make new bucket)
};

//could add directly to end of bucket, but not to right position. iirc buckets need to be sorted?


class SnapshotManager {
//TODO: Store this as part of QueryMetaData.
public:
  SnapshotManager(const QueryParameters *query_params); //
  virtual ~SnapshotManager();
  //Local Snapshot operations:
  void InitLocalSnapshot(proto::LocalSnapshot *local_ss, const uint64_t &query_seq_num, const uint64_t &client_id, const uint64_t &replica_id, bool useOptimisticTxId = false);
  void ResetLocalSnapshot(bool useOptimisticTxId = false);
  void AddToLocalSnapshot(const proto::Transaction &txn, bool hash_param, bool committed_or_prepared);
  void AddToLocalSnapshot(const std::string &txnDigest, const proto::Transaction *txn, bool committed_or_prepared = true); //For local snapshot; //TODO: Define something similar for merged? Should merged be a separate class?
    void AddToLocalSnapshot(const std::string &txnDigest, const uint64_t &timestamp, const uint64_t &id, bool committed_or_prepared);
  void SealLocalSnapshot();
  void OpenLocalSnapshot(proto::LocalSnapshot *local_ss);
  
  //Merged Snapshot operations:
  void InitMergedSnapshot(proto::MergedSnapshot *merged_ss, const uint64_t &query_seq_num, const uint64_t &client_id, const uint64_t &retry_version, const uint64_t &config_f); //->if retry version > 0, useOptimisticTxId = false
  bool ProcessReplicaLocalSnapshot(proto::LocalSnapshot* local_ss);
  void SealMergedSnapshot();
  void OpenMergedSnapshot(proto::MergedSnapshot *merged_ss);

private:
    const QueryParameters *query_params;
    //TODO: Alternatively deifine and pass only the params we want (then QueryParam definition can move below SnapshotManager)
    // const bool param_optimisticTxId;
    //const bool param_compressOptimisticTxId;
    // const uint64_t *param_syncQuorum;
    // const uint64_t *param_mergeThreshold;

    //const transport::Configuration *config;
    uint64_t config_f; 
    bool useOptimisticTxId;

    TimestampCompressor ts_comp;

    proto::LocalSnapshot *local_ss; //For replica to client   //TODO: Needs to have a field for compressed values.

    proto::MergedSnapshot *merged_ss; //For client to replica
    
    uint64_t numSnapshotReplies;
    std::unordered_map<std::string, std::set<uint64_t>> txn_freq; //replicas that have txn committed.
    std::unordered_map<uint64_t, std::set<uint64_t>> ts_freq; //replicas that have txn committed.
};

typedef std::function<void(const std::string &, const Timestamp &, bool, QueryReadSetMgr *, bool, SnapshotManager *)> find_table_version;
typedef std::function<bool(const std::string &)> read_prepared_pred; // This is a function that, given a txnDigest of a prepared tx, evals to true if it is readable, and false if not.



typedef struct Parameters {

  //protocol and microbenchmark parameters
  const bool signedMessages;
  const bool validateProofs;
  const bool hashDigest;
  const bool verifyDeps;
  const int signatureBatchSize;
  const int64_t maxDepDepth;
  const uint64_t readDepSize;
  const bool readReplyBatch;
  const bool adjustBatchSize;
  const bool sharedMemBatches;
  const bool sharedMemVerify;
  const uint64_t merkleBranchFactor;
  const InjectFailure injectFailure;

  const bool multiThreading;
  const bool batchVerification;
  const int verificationBatchSize;

  //performance parameters
  const bool mainThreadDispatching;
  const bool dispatchMessageReceive;
  const bool parallel_reads;
  const bool parallel_CCC;
  const bool dispatchCallbacks;

  //fallback parameters
  const bool all_to_all_fb;
  const bool no_fallback;
  const uint64_t relayP1_timeout;
  const bool replicaGossip;

  const bool signClientProposals;
  const uint32_t rtsMode;

  const QueryParameters query_params;

  Parameters(bool signedMessages, bool validateProofs, bool hashDigest, bool _verifyDeps,
    int signatureBatchSize, int64_t maxDepDepth, uint64_t readDepSize,
    bool readReplyBatch, bool adjustBatchSize, bool sharedMemBatches,
    bool sharedMemVerify, uint64_t merkleBranchFactor, const InjectFailure &injectFailure,
    bool multiThreading, bool batchVerification, int verificationBatchSize,
    bool mainThreadDispatching, bool dispatchMessageReceive,
    bool parallel_reads,
    bool parallel_CCC,
    bool dispatchCallbacks,
    bool all_to_all_fb,
    bool no_fallback,
    uint64_t relayP1_timeout,
    bool replicaGossip,
    bool signClientProposals,
    uint32_t rtsMode,
    QueryParameters query_params) :
    signedMessages(signedMessages), validateProofs(validateProofs),
    hashDigest(hashDigest), verifyDeps(false), signatureBatchSize(signatureBatchSize),
    maxDepDepth(maxDepDepth), readDepSize(readDepSize),
    readReplyBatch(readReplyBatch), adjustBatchSize(adjustBatchSize),
    sharedMemBatches(sharedMemBatches), sharedMemVerify(sharedMemVerify),
    merkleBranchFactor(merkleBranchFactor), injectFailure(injectFailure),
    multiThreading(multiThreading), batchVerification(batchVerification),
    verificationBatchSize(verificationBatchSize),
    mainThreadDispatching(mainThreadDispatching),
    dispatchMessageReceive(dispatchMessageReceive),
    parallel_reads(parallel_reads),
    parallel_CCC(parallel_CCC),
    dispatchCallbacks(dispatchCallbacks),
    all_to_all_fb(all_to_all_fb),
    no_fallback(no_fallback),
    relayP1_timeout(relayP1_timeout),
    replicaGossip(replicaGossip),
    signClientProposals(signClientProposals),
    rtsMode(rtsMode),
    query_params(query_params) { 
        UW_ASSERT(!(mainThreadDispatching && dispatchMessageReceive)); //They should not be true at the same time.

        UW_ASSERT(!verifyDeps);
        if(_verifyDeps){
            Warning("VerifyDeps Parameter is deprecated in Pequinstore -- automatically setting to false. Always doing serverside local verification");
            //Note: Cannot support non-local verification (proofs for deps) if write equality is only for prepares. 
            //Since signature is for whole write, verifyDeps will not be able to correctly verify a dependency that was formed by 
            //f+1 Write messages with the same prepare value, but different committed values
        }
    }
} Parameters;

} // namespace pequinstore

#endif /* PEQUIN_COMMON_H */
