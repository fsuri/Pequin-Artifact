#ifndef _AUGUSTUS_SERVER_H_
#define _AUGUSTUS_SERVER_H_

#include <memory>
#include <map>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>

#include "store/augustusstore/app.h"
#include "store/augustusstore/server-proto.pb.h"
#include "store/server.h"
#include "lib/keymanager.h"
#include "lib/configuration.h"
#include "store/common/backend/versionstore.h"
#include "store/common/partitioner.h"
#include "store/common/truetime.h"
#include "lib/transport.h"

namespace augustusstore {

    class Server;
    class Augustus {
    public:
        struct rwLock {
            enum  {
                FREE,
                LOCKED_READ,
                LOCKED_WRITE
            } state;

            std::unordered_set<std::string> owners;
            rwLock(): state(FREE) {}
        };

        std::unordered_map<std::string, rwLock> locks;
        std::unordered_map<std::string, std::string> store;

        bool TryLock(const proto::Transaction& txn, class Server* server);
        bool ReleaseLock(const proto::Transaction& txn, class Server* server);
    };

    class Server : public App, public ::Server {
    public:
        Server(const transport::Configuration& config, KeyManager *keyManager, int groupIdx, int idx, int numShards,
               int numGroups, bool signMessages, bool validateProofs, uint64_t timeDelta, Partitioner *part, Transport* tp,
               bool order_commit = false, bool validate_abort = false,
               TrueTime timeServer = TrueTime(0, 0));
        ~Server();

        std::vector<::google::protobuf::Message*> Execute(const std::string& type, const std::string& msg);
        ::google::protobuf::Message* HandleMessage(const std::string& type, const std::string& msg);

        void Load(const std::string &key, const std::string &value,
                  const Timestamp timestamp);

        Stats &GetStats();

        Stats* mutableStats();
        // return true if this key is owned by this shard
        inline bool IsKeyOwned(const std::string &key) const {
            std::vector<int> txnGroups;
            return static_cast<int>((*part)(key, numShards, groupIdx, txnGroups) % numGroups) == groupIdx;
        }

    private:
        Transport* tp;
        Stats stats;
        transport::Configuration config;
        KeyManager* keyManager;
        int groupIdx;
        int idx;
        int id;
        int numShards;
        int numGroups;
        bool signMessages;
        bool validateProofs;
        uint64_t timeDelta;
        Partitioner *part;
        TrueTime timeServer;

        // Augustus
        Augustus augustus;

        //addtional knobs: 1) order commit, 2) validate abort
        bool order_commit;
        bool validate_abort;

        std::shared_mutex atomicMutex;

        struct ValueAndProof {
            std::string value;
            std::shared_ptr<proto::CommitProof> commitProof;
        };

        std::shared_ptr<proto::CommitProof> dummyProof;

        VersionedKVStore<Timestamp, ValueAndProof> commitStore;


        std::vector<::google::protobuf::Message*> HandleTransaction(const proto::Transaction& transaction);

        ::google::protobuf::Message* HandleRead(const proto::Read& read);

        ::google::protobuf::Message* HandleGroupedCommitDecision(const proto::GroupedDecision& gdecision);

        ::google::protobuf::Message* HandleGroupedAbortDecision(const proto::GroupedDecision& gdecision);

        ::google::protobuf::Message* returnMessage(::google::protobuf::Message* msg);

        // map from tx digest to transaction
        std::unordered_map<std::string, proto::Transaction> pendingTransactions;
        // map from key to ordered map of prepared tx timestamps to read timestamps
        std::unordered_map<std::string, std::map<Timestamp, Timestamp>> preparedReads;
        // map from key to ordered set of prepared transaction timestamps that write the key
        std::unordered_map<std::string, std::set<Timestamp>> preparedWrites;

        // map from key to ordered map of committed timestamps to read timestamp
        // so if a transaction with timestamp 5 reads version 3 of key A, we have A -> 5 -> 3
        // we wont have key collisions for the map because there each transaction has at
        // most 1 read for a key
        std::unordered_map<std::string, std::map<Timestamp, Timestamp>> committedReads;

        bool CCC(const proto::Transaction& txn);
        bool CCC2(const proto::Transaction& txn);

        void cleanupPendingTx(std::string digest);

        std::unordered_map<std::string, proto::GroupedDecision> bufferedGDecs;
        std::unordered_set<std::string> abortedTxs;
    };

}

#endif
