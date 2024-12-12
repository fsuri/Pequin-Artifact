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
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <google/protobuf/util/message_differencer.h>

#include "lib/assert.h"
#include "lib/transport.h"
#include "store/common/partitioner.h"
#include "store/sintrstore/server.h"
#include "store/sintrstore/tests/common.h"

#define F 1
#define G 3
#define S 3

namespace sintrstore {

class MockTransportAddress : public TransportAddress {
 public:
  MOCK_METHOD(TransportAddress *, clone, (), (const, override));
};

class MockTransport : public Transport 
 public:
  MockTransport() {}
  MOCK_METHOD(void, Register, (TransportReceiver *receiver,
        const transport::Configuration &config, int groupIdx, int replicaIdx),
      (override));
  MOCK_METHOD(bool, SendMessage, (TransportReceiver *src,
        const TransportAddress &dst, const Message &m), (override));
  MOCK_METHOD(bool, SendMessageToReplica, (TransportReceiver *src,
        int replicaIdx, const Message &m), (override));
  MOCK_METHOD(bool, SendMessageToReplica, (TransportReceiver *src, int groupIdx,
        int replicaIdx, const Message &m), (override));
  MOCK_METHOD(bool, SendMessageToAll, (TransportReceiver *src,
        const Message &m), (override));
  MOCK_METHOD(bool, SendMessageToAllGroups, (TransportReceiver *src,
        const Message &m), (override));
  MOCK_METHOD(bool, SendMessageToGroups, (TransportReceiver *src,
        const std::vector<int> &groups, const Message &m), (override));
  MOCK_METHOD(bool, SendMessageToGroup, (TransportReceiver *src, int groupIdx,
        const Message &m), (override));
  MOCK_METHOD(bool, OrderedMulticast, (TransportReceiver *src,
        const std::vector<int> &groups, const Message &m), (override));
  MOCK_METHOD(bool, OrderedMulticast, (TransportReceiver *src, const Message &m),
      (override));
  MOCK_METHOD(bool, SendMessageToFC, (TransportReceiver *src, const Message &m),
      (override));
  MOCK_METHOD(int, Timer, (uint64_t ms, timer_callback_t cb), (override));
  MOCK_METHOD(bool, CancelTimer, (int id), (override));
  MOCK_METHOD(void, CancelAllTimers, (), (override));
  MOCK_METHOD(void, Run, (), (override));
  MOCK_METHOD(void, Stop, (), (override));
};

class ServerTest : public ::testing::Test {
 public:
  ServerTest() { }
  virtual ~ServerTest() { }

  virtual void SetUp() {
    int groupIdx = 0;
    int idx = 0;

    sintrstore::QueryParameters query_params(0,
                                                 0,
                                                 0,
                                                 0,
                                                 0,
                                                 false, //FLAGS_sintr_query_read_prepared,
                                                 false, //FLAGS_sintr_query_optimistic_txid,
                                                 true, //FLAGS_sintr_query_cache_read_set,
                                                 false, //FLAGS_sintr_query_merge_active_at_client,
                                                 false, //FLAGS_sintr_sign_client_queries,
                                                 false, //FLAGS_sintr_parallel_queries);
    );

    sintrstore::Parameters params(false, //FLAGS_indicus_sign_messages,
                                    false,  // FLAGS_indicus_validate_proofs,
                                    true,   // FLAGS_indicus_hash_digest,
                                    false, 2,  // FLAGS_indicus_verify_deps, FLAGS_indicus_sig_batch,
                                    -1,  1, // FLAGS_indicus_max_dep_depth, readDepSize,
                                    false, false,  // FLAGS_indicus_read_reply_batch, FLAGS_indicus_adjust_batch_size,
                                    false, false,  // FLAGS_indicus_shared_mem_batch, FLAGS_indicus_shared_mem_verify,
                                    2, InjectFailure(), // FLAGS_indicus_merkle_branch_factor, InjectFailure(),
                                    true, false,  // FLAGS_indicus_multi_threading, FLAGS_indicus_batch_verification,
																		2,	// FLAGS_indicus_batch_verification_size,
																		true,	// FLAGS_indicus_mainThreadDispatching,
																		false,	// FLAGS_indicus_dispatchMessageReceive,
																		true,	// FLAGS_indicus_parallel_reads,
																		true,	// FLAGS_indicus_parallel_CCC,
																		true,	// FLAGS_indicus_dispatchCallbacks,
																		false,	// FLAGS_indicus_all_to_all_fb,
																		false, 10,  // FLAGS_indicus_no_fallback, FLAGS_indicus_relayP1_timeout,
																		false,  // FLAGS_indicus_replica_gossip,
                                    false,  // FLAGS_indicus_sign_client_proposals,
                                    1,  // FLAGS_indicus_rts_mode,
                                      query_params);
    bool signedMessages = false;
    bool validateProofs = false;
    uint64_t timeDelta = 100UL;
    OCCType occType = MVTSO;

    std::stringstream configSS;
    GenerateTestConfig(1, F, configSS);
    config = new transport::Configuration(configSS);
    transport = new MockTransport();
    keyManager = new KeyManager("./", crypto::DONNA, true, 0, 0, 0);
    server = new Server(*config, groupIdx, idx, G, S, transport, keyManager,
      params, timeDelta, occType, default_partitioner, 0);
  }

  virtual void TearDown() {
    delete server;
    delete keyManager;
    delete transport;
    delete config;
  }

 protected:
  void HandleRead(const TransportAddress &remote, const proto::Read &msg) {
    server->HandleRead(remote, msg);
  }

  void HandlePhase1(const TransportAddress &remote, const proto::Phase1 &msg) {
    server->HandlePhase1(remote, msg);
  }

  void Prepare(const proto::Transaction &txn) {
    std::string txnDigest = TransactionDigest(txn);
    server->Prepare(txnDigest, txn);
  }

  void Commit(const proto::Transaction &txn) {
    std::string txnDigest = TransactionDigest(txn);
    server->Commit(txnDigest, txn);
  }

  void Abort(const proto::Transaction &txn) {
    std::string txnDigest = TransactionDigest(txn);
    server->Abort(txnDigest);
  }

  MockTransportAddress clientAddress;
  MockTransport *transport;
  Server *server;

 private:
  transport::Configuration *config;
  KeyManager *keyManager;

};


MATCHER_P(ExpectedMessage, expected, "") {
  return google::protobuf::util::MessageDifferencer::Equals(arg, expected);
}

TEST_F(ServerTest, ReadNoData) {
  proto::Read read;
  read.set_req_id(3);
  read.set_key("key0");
  Timestamp timestamp(100, 2);
  timestamp.serialize(read.mutable_timestamp());

  proto::ReadReply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_FAIL);
  expectedReply.set_key("key0");

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandleRead(clientAddress, read);
}

TEST_F(ServerTest, ReadCommittedData) {
  proto::Transaction txn;
  Timestamp wts(50, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);
  Commit(txn);

  proto::Read read;
  read.set_req_id(3);
  read.set_key("key0");
  Timestamp timestamp(100, 2);
  timestamp.serialize(read.mutable_timestamp());

  proto::ReadReply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_key("key0");
  expectedReply.mutable_committed()->set_value("val0");
  wts.serialize(expectedReply.mutable_committed()->mutable_timestamp());

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandleRead(clientAddress, read);
}

TEST_F(ServerTest, ReadPreparedData) {
  proto::Transaction txn;
  Timestamp wts(50, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);
  Prepare(txn);

  proto::Read read;
  read.set_req_id(3);
  read.set_key("key0");
  Timestamp timestamp(100, 2);
  timestamp.serialize(read.mutable_timestamp());

  proto::ReadReply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_key("key0");
  expectedReply.mutable_prepared()->set_value("val0");
  wts.serialize(expectedReply.mutable_prepared()->mutable_timestamp());
  *expectedReply.mutable_prepared()->mutable_txn_digest() = TransactionDigest(txn);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandleRead(clientAddress, read);
}

TEST_F(ServerTest, Phase1Commit) {
  proto::Transaction txn;
  Timestamp wts(50, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 must abort if there is a committed conflicting transaction
 *   T_2 with a read version such that read version < T_1.ts < T_2.ts
 */
TEST_F(ServerTest, Phase1CommittedReadConflictAbort) {
  proto::Transaction committedTxn;
  Timestamp committedRts(55, 2);
  PopulateTransaction({{"key0", Timestamp(45,2)}}, {}, committedRts, {0},
      committedTxn);
  Commit(committedTxn);

  proto::Transaction txn;
  Timestamp wts(50, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::ABORT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 is allowed to commit if T_2.ts < T_1.ts with committed
 *   conflicting transaction T_2.
 */
TEST_F(ServerTest, Phase1CommittedReadConflictCommitNewerTS) {
  proto::Transaction committedTxn;
  Timestamp committedRts(55, 2);
  PopulateTransaction({{"key0", Timestamp(45,2)}}, {}, committedRts, {0},
      committedTxn);
  Commit(committedTxn);

  proto::Transaction txn;
  Timestamp wts(60, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 is allowed to commit if T_1.ts < read version in committed
 *   transaction T_2.
 */
TEST_F(ServerTest, Phase1CommittedReadConflictCommitOlderTS) {
  proto::Transaction committedTxn;
  Timestamp committedRts(55, 2);
  PopulateTransaction({{"key0", Timestamp(45,2)}}, {}, committedRts, {0},
      committedTxn);
  Commit(committedTxn);

  proto::Transaction txn;
  Timestamp wts(40, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 must abort if T_1 has a read version that conflicts with a
 *   committed transaction T_2 and read version < T_2.ts < T_1.ts.
 */
TEST_F(ServerTest, Phase1CommittedWriteConflictAbort) {
  proto::Transaction committedTxn;
  Timestamp committedRts(50, 2);
  PopulateTransaction({}, {{"key0", "val0"}}, committedRts, {0}, committedTxn);
  Commit(committedTxn);

  proto::Transaction txn;
  Timestamp wts(55, 1);
  PopulateTransaction({{"key0", Timestamp(45, 1)}}, {}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::ABORT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 can commit if T_1 has a read version that conflicts with a
 *   committed transaction T_2, but T_2.ts < read version.
 */
TEST_F(ServerTest, Phase1CommittedWriteConflictCommitNewerReadVersion) {
  proto::Transaction committedTxn;
  Timestamp committedRts(50, 2);
  PopulateTransaction({}, {{"key0", "val0"}}, committedRts, {0}, committedTxn);
  Commit(committedTxn);

  proto::Transaction txn;
  Timestamp wts(55, 1);
  PopulateTransaction({{"key0", Timestamp(52, 1)}}, {}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 can commit if T_1 has a read version that conflicts with a
 *   committed transaction T_2, but T_1.ts < T_2.ts.
 */
TEST_F(ServerTest, Phase1CommittedWriteConflictCommitOlderTS) {
  proto::Transaction committedTxn;
  Timestamp committedRts(50, 2);
  PopulateTransaction({}, {{"key0", "val0"}}, committedRts, {0}, committedTxn);
  Commit(committedTxn);

  proto::Transaction txn;
  Timestamp wts(49, 1);
  PopulateTransaction({{"key0", Timestamp(48, 1)}}, {}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 must abstain if there is a prepared conflicting transaction
 *   T_2 with a read version such that read version < T_1.ts < T_2.ts
 */
TEST_F(ServerTest, Phase1PreparedReadConflictAbort) {
  proto::Transaction preparedTxn;
  Timestamp committedRts(55, 2);
  PopulateTransaction({{"key0", Timestamp(45,2)}}, {}, committedRts, {0},
      preparedTxn);
  Prepare(preparedTxn);

  proto::Transaction txn;
  Timestamp wts(50, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::ABSTAIN);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 is allowed to commit if T_2.ts < T_1.ts with prepared
 *   conflicting transaction T_2.
 */
TEST_F(ServerTest, Phase1PreparedReadConflictCommitNewerTS) {
  proto::Transaction preparedTxn;
  Timestamp committedRts(55, 2);
  PopulateTransaction({{"key0", Timestamp(45,2)}}, {}, committedRts, {0},
      preparedTxn);
  Prepare(preparedTxn);

  proto::Transaction txn;
  Timestamp wts(60, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 is allowed to commit if T_1.ts < read version in prepared
 *   transaction T_2.
 */
TEST_F(ServerTest, Phase1PreparedReadConflictCommitOlderTS) {
  proto::Transaction preparedTxn;
  Timestamp committedRts(55, 2);
  PopulateTransaction({{"key0", Timestamp(45,2)}}, {}, committedRts, {0},
      preparedTxn);
  Prepare(preparedTxn);

  proto::Transaction txn;
  Timestamp wts(40, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 must abstain if T_1 has a read version that conflicts with a
 *   prepared transaction T_2 and read version < T_2.ts < T_1.ts.
 */
TEST_F(ServerTest, Phase1PreparedWriteConflictAbort) {
  proto::Transaction preparedTxn;
  Timestamp committedRts(50, 2);
  PopulateTransaction({}, {{"key0", "val0"}}, committedRts, {0}, preparedTxn);
  Prepare(preparedTxn);

  proto::Transaction txn;
  Timestamp wts(55, 1);
  PopulateTransaction({{"key0", Timestamp(45, 1)}}, {}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::ABSTAIN);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 can commit if T_1 has a read version that conflicts with a
 *   prepared transaction T_2, but T_2.ts < read version.
 */
TEST_F(ServerTest, Phase1PreparedWriteConflictCommitNewerReadVersion) {
  proto::Transaction preparedTxn;
  Timestamp committedRts(50, 2);
  PopulateTransaction({}, {{"key0", "val0"}}, committedRts, {0}, preparedTxn);
  Prepare(preparedTxn);

  proto::Transaction txn;
  Timestamp wts(55, 1);
  PopulateTransaction({{"key0", Timestamp(52, 1)}}, {}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

/**
 * Transaction T_1 can commit if T_1 has a read version that conflicts with a
 *   prepared transaction T_2, but T_1.ts < T_2.ts.
 */
TEST_F(ServerTest, Phase1PreparedWriteConflictCommitOlderTS) {
  proto::Transaction preparedTxn;
  Timestamp committedRts(50, 2);
  PopulateTransaction({}, {{"key0", "val0"}}, committedRts, {0},  preparedTxn);
  Prepare(preparedTxn);

  proto::Transaction txn;
  Timestamp wts(49, 1);
  PopulateTransaction({{"key0", Timestamp(48, 1)}}, {}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

TEST_F(ServerTest, Phase1RTSConflictAbort) {
  proto::Read read;
  read.set_req_id(1);
  read.set_key("key0");
  Timestamp timestamp(100, 2);
  timestamp.serialize(read.mutable_timestamp());
  HandleRead(clientAddress, read);

  proto::Transaction txn;
  Timestamp wts(50, 1);
  PopulateTransaction({}, {{"key0", "val0"}}, wts, {0}, txn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::ABSTAIN);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  HandlePhase1(clientAddress, phase1);
}

TEST_F(ServerTest, Phase1DepWait) {
  proto::Transaction preparedTxn;
  Timestamp preparedTs(50, 2);
  PopulateTransaction({}, {{"key0", "val0"}}, preparedTs, {0}, preparedTxn);
  Prepare(preparedTxn);

  proto::Transaction txn;
  Timestamp ts(100, 1);
  PopulateTransaction({{"key0", Timestamp(50, 2)}}, {}, ts, {0}, txn);
  proto::Dependency *dep = txn.add_deps();
  dep->mutable_prepared()->set_value("val0");
  preparedTs.serialize(dep->mutable_prepared()->mutable_timestamp());
  *dep->mutable_prepared()->mutable_txn_digest() = TransactionDigest(preparedTxn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;

  EXPECT_CALL(*transport, SendMessage(::testing::_, ::testing::_,
        ::testing::_)).Times(0);

  HandlePhase1(clientAddress, phase1);
}

TEST_F(ServerTest, Phase1DepCommit) {
  proto::Transaction preparedTxn;
  Timestamp preparedTs(50, 2);
  PopulateTransaction({}, {{"key0", "val0"}}, preparedTs, {0}, preparedTxn);
  Prepare(preparedTxn);

  proto::Transaction txn;
  Timestamp ts(100, 1);
  PopulateTransaction({{"key0", Timestamp(50, 2)}}, {}, ts, {0}, txn);
  proto::Dependency *dep = txn.add_deps();
  dep->mutable_prepared()->set_value("val0");
  preparedTs.serialize(dep->mutable_prepared()->mutable_timestamp());
  *dep->mutable_prepared()->mutable_txn_digest() = TransactionDigest(preparedTxn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;
  HandlePhase1(clientAddress, phase1);

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::COMMIT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  Commit(preparedTxn);
}

TEST_F(ServerTest, Phase1DepAbort) {
  proto::Transaction preparedTxn;
  Timestamp preparedTs(50, 2);
  PopulateTransaction({}, {{"key0", "val0"}}, preparedTs, {0}, preparedTxn);
  Prepare(preparedTxn);

  proto::Transaction txn;
  Timestamp ts(100, 1);
  PopulateTransaction({{"key0", Timestamp(50, 2)}}, {}, ts, {0}, txn);
  proto::Dependency *dep = txn.add_deps();
  dep->mutable_prepared()->set_value("val0");
  preparedTs.serialize(dep->mutable_prepared()->mutable_timestamp());
  *dep->mutable_prepared()->mutable_txn_digest() = TransactionDigest(preparedTxn);

  proto::Phase1 phase1;
  phase1.set_req_id(3);
  phase1.set_txn_digest(TransactionDigest(txn));
  *phase1.mutable_txn() = txn;
  HandlePhase1(clientAddress, phase1);

  proto::Phase1Reply expectedReply;
  expectedReply.set_req_id(3);
  expectedReply.set_status(REPLY_OK);
  expectedReply.set_ccr(proto::Phase1Reply::ABORT);

  EXPECT_CALL(*transport, SendMessage(server, ::testing::_,
        ExpectedMessage(expectedReply)));

  Abort(preparedTxn);
}

}
