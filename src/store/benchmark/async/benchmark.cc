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
// -*- mode: c++; c-file-style: "k&r"; c-basic-offset: 4 -*-
/***********************************************************************
 *
 * store/benchmark/tpccClient.cc:
 *   Benchmarking client for tpcc.
 *
 **********************************************************************/

#include "lib/keymanager.h"
#include "lib/latency.h"
#include "lib/timeval.h"
#include "lib/tcptransport.h"
#include "store/common/truetime.h"
#include "store/common/stats.h"
#include "store/common/partitioner.h"
#include "store/common/failures.h"
#include "store/common/frontend/sync_client.h"
#include "store/common/frontend/async_client.h"
#include "store/common/frontend/async_adapter_client.h"
#include "store/strongstore/client.h"
#include "store/weakstore/client.h"
#include "store/tapirstore/client.h"

//benchmark clients
#include "store/benchmark/async/bench_client.h"
#include "store/benchmark/async/common/key_selector.h"
#include "store/benchmark/async/common/uniform_key_selector.h"
#include "store/benchmark/async/retwis/retwis_client.h"
#include "store/benchmark/async/rw/rw_client.h"
#include "store/benchmark/async/tpcc/sync/tpcc_client.h"
#include "store/benchmark/async/tpcc/async/tpcc_client.h"
#include "store/benchmark/async/sql/tpcc/tpcc_client.h"
#include "store/benchmark/async/sql/seats/seats_client.h"
#include "store/benchmark/async/sql/auctionmark/auctionmark_client.h"
#include "store/benchmark/async/sql/tpcch/tpcch_client.h"
#include "store/benchmark/async/smallbank/smallbank_client.h"
#include "store/benchmark/async/toy/toy_client.h"
#include "store/benchmark/async/rw-sql/rw-sql_client.h"
#include "store/benchmark/async/rw-sync/sync/rw-sync_client.h"

// probs for tpcch 
#include "store/benchmark/async/sql/tpcch/tpcch_constants.h"

//protocol clients
//Blackhole test printer
#include "store/blackholestore/client.h"
// Sintr
#include "store/sintrstore/client.h"
//Pesto
#include "store/pequinstore/client.h"
// Basil
#include "store/indicusstore/client.h"
#include "store/pbftstore/client.h"
// HotStuff
#include "store/hotstuffstore/client.h"
// HotStuffPostgres
#include "store/pg_SMRstore/client.h"
// PelotonSMR
#include "store/pelotonstore/client.h"
// Augustus-Hotstuff
#include "store/augustusstore/client.h"
//BFTSmart
#include "store/bftsmartstore/client.h"
// Augustus-BFTSmart
#include "store/bftsmartstore_augustus/client.h"
#include "store/bftsmartstore_stable/client.h"
// Postgres
#include "store/postgresstore/client.h"
// CRDB
#include "store/cockroachdb/client.h"


#include "store/common/frontend/one_shot_client.h"
#include "store/common/frontend/async_one_shot_adapter_client.h"
#include "store/benchmark/async/common/zipf_key_selector.h"

#include <gflags/gflags.h>
#include <valgrind/callgrind.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <sstream>
#include <thread>
#include <vector>

#include "store/benchmark/async/json_table_writer.h"

enum protomode_t {
	PROTO_UNKNOWN,
  PROTO_BLACKHOLE,
	PROTO_TAPIR,
	PROTO_WEAK,
	PROTO_STRONG,
  PROTO_SINTR,
  PROTO_PEQUIN,
  PROTO_INDICUS,
	PROTO_PBFT,
  // HotStuff
  PROTO_HOTSTUFF,
  // Augustus-Hotstuff
  PROTO_AUGUSTUS,
  // Bftsmart
  PROTO_BFTSMART,
  // Augustus-Hotstuff
  PROTO_AUGUSTUS_SMART,
  PROTO_POSTGRES,
   // PG-SMR
  PROTO_PG_SMR,
  PROTO_PELOTON_SMR,
  PROTO_CRDB
};

enum benchmode_t {
  BENCH_UNKNOWN,
  BENCH_RETWIS,
  BENCH_TPCC,
  BENCH_SMALLBANK_SYNC,
  BENCH_RW,
  BENCH_TPCC_SYNC,
  BENCH_TOY,
  BENCH_TPCC_SQL,
  BENCH_RW_SQL, 
  BENCH_SEATS_SQL,
  BENCH_AUCTIONMARK_SQL,
  BENCH_TPCCH_SQL,
  BENCH_RW_SYNC
};

enum keysmode_t {
  KEYS_UNKNOWN,
  KEYS_UNIFORM,
  KEYS_ZIPF
};

enum transmode_t {
	TRANS_UNKNOWN,
  TRANS_UDP,
  TRANS_TCP,
};

enum read_quorum_t {
  READ_QUORUM_UNKNOWN,
  READ_QUORUM_ONE,
  READ_QUORUM_ONE_HONEST,
  READ_QUORUM_MAJORITY_HONEST,
  READ_QUORUM_MAJORITY,
  READ_QUORUM_ALL
};

enum read_dep_t {
  READ_DEP_UNKNOWN,
  READ_DEP_ONE,
  READ_DEP_ONE_HONEST
};

enum read_messages_t {
  READ_MESSAGES_UNKNOWN,
  READ_MESSAGES_READ_QUORUM,
  READ_MESSAGES_MAJORITY,
  READ_MESSAGES_ALL
};

/**
 * System settings.
 */
DEFINE_uint64(client_id, 0, "unique identifier for client process");
DEFINE_string(config_path, "", "path to shard configuration file");
DEFINE_uint64(num_shards, 1, "number of shards in the system");
DEFINE_uint64(num_groups, 1, "number of replica groups in the system");
DEFINE_bool(ping_replicas, false, "determine latency to replicas via pings");

DEFINE_string(experiment_name, "pequin", "Cloudlab experiment name");

DEFINE_bool(pg_replicated, false, "postgres operates in replication mode");

DEFINE_bool(tapir_sync_commit, true, "wait until commit phase completes before"
    " sending additional transactions (for TAPIR)");

const std::string read_quorum_args[] = {
	"one",
  "one-honest",
  "majority-honest",
  "majority",
  "all"
};
const read_quorum_t read_quorums[] {
	READ_QUORUM_ONE,
  READ_QUORUM_ONE_HONEST,
  READ_QUORUM_MAJORITY_HONEST,
  READ_QUORUM_MAJORITY,
  READ_QUORUM_ALL
};
static bool ValidateReadQuorum(const char* flagname,
    const std::string &value) {
  int n = sizeof(read_quorum_args);
  for (int i = 0; i < n; ++i) {
    if (value == read_quorum_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(indicus_read_quorum, read_quorum_args[1], "size of read quorums"
    " (for Indicus)");
DEFINE_validator(indicus_read_quorum, &ValidateReadQuorum);

DEFINE_bool(indicus_optimistic_read_quorum, true, "if true = read only from read_quorum many replicas; if false = read from f more for liveness");

const std::string read_dep_args[] = {
  "one-honest",
	"one"
};
const read_dep_t read_deps[] {
  READ_DEP_ONE_HONEST,
  READ_DEP_ONE
};
static bool ValidateReadDep(const char* flagname,
    const std::string &value) {
  int n = sizeof(read_dep_args);
  for (int i = 0; i < n; ++i) {
    if (value == read_dep_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(indicus_read_dep, read_dep_args[0], "number of identical prepared"
    " to claim dependency (for Indicus)");
DEFINE_validator(indicus_read_dep, &ValidateReadDep);
const std::string read_messages_args[] = {
	"read-quorum",
  "majority",
  "all"
};
const read_messages_t read_messagess[] {
  READ_MESSAGES_READ_QUORUM,
  READ_MESSAGES_MAJORITY,
  READ_MESSAGES_ALL
};
static bool ValidateReadMessages(const char* flagname,
    const std::string &value) {
  int n = sizeof(read_messages_args);
  for (int i = 0; i < n; ++i) {
    if (value == read_messages_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(indicus_read_messages, read_messages_args[0], "number of replicas to send messages for reads (for Indicus)");
DEFINE_validator(indicus_read_messages, &ValidateReadMessages);
DEFINE_bool(indicus_sign_messages, true, "add signatures to messages as necessary to prevent impersonation (for Indicus)");
DEFINE_bool(indicus_validate_proofs, true, "send and validate proofs as necessary to check Byzantine behavior (for Indicus)");
DEFINE_bool(indicus_hash_digest, false, "use hash function compute transaction digest (for Indicus)");
DEFINE_bool(indicus_verify_deps, true, "check signatures of transaction depdendencies (for Indicus)");
DEFINE_uint64(indicus_sig_batch, 2, "signature batch size sig batch size (for Indicus)");
DEFINE_uint64(indicus_merkle_branch_factor, 2, "branch factor of merkle tree of batch (for Indicus)");
DEFINE_string(indicus_key_path, "", "path to directory containing public and private keys (for Indicus)");
DEFINE_int64(indicus_max_dep_depth, -1, "maximum length of dependency chain allowed by honest replicas [-1 is no maximum, -2 is no deps] (for Indicus)");
DEFINE_uint64(indicus_key_type, 4, "key type (see create keys for mappings) key type (for Indicus)");

DEFINE_bool(indicus_sign_client_proposals, false, "add signatures to client proposals -- used for optimistic tx-ids. Can be used for access control (unimplemented)");

//Client failure configurations    
DEFINE_uint64(indicus_inject_failure_ms, 0, "number of milliseconds to wait"
    " before injecting a failure (for Indicus)");
DEFINE_uint64(indicus_inject_failure_proportion, 0, "proportion of clients that"
    " will inject a failure (for Indicus)"); //default 0
DEFINE_uint64(indicus_inject_failure_freq, 100, "number of transactions per ONE failure"
		    " in a Byz client (for Indicus)"); //default 100

DEFINE_uint64(indicus_phase1_decision_timeout, 1000UL, "p1 timeout before going slowpath");
//Verification computation configurations
DEFINE_bool(indicus_multi_threading, false, "dispatch crypto to parallel threads");
DEFINE_bool(indicus_batch_verification, false, "using ed25519 donna batch verification");
DEFINE_uint64(indicus_batch_verification_size, 64, "batch size for ed25519 donna batch verification");
DEFINE_uint64(indicus_batch_verification_timeout, 5, "batch verification timeout, ms");

DEFINE_bool(pbft_order_commit, true, "order commit writebacks as well");
DEFINE_bool(pbft_validate_abort, true, "validate abort writebacks as well");


DEFINE_string(bftsmart_codebase_dir, "", "path to directory containing bftsmart configurations");

DEFINE_bool(indicus_parallel_CCC, true, "sort read/write set for parallel CCC locking at server");

DEFINE_bool(indicus_hyper_threading, true, "use hyperthreading");

//PG-SM / PELOTON-SMR
DEFINE_bool(pg_fake_SMR, true, "Indicate if server is asynchronous or not. If so, will return leader's results for consistency");
DEFINE_uint64(pg_SMR_mode, 0, "Indicate with SMR protocol to use: 0 = off, 1 = Hotstuff, 2 = BFTSmart");

//Indicus failure handling and injection
DEFINE_bool(indicus_no_fallback, false, "turn off fallback protocol");
DEFINE_uint64(indicus_max_consecutive_abstains, 1, "number of consecutive conflicts before fallback is triggered");
DEFINE_bool(indicus_all_to_all_fb, false, "use the all to all view change method");
DEFINE_uint64(indicus_relayP1_timeout, 1, "time (ms) after which to send RelayP1");
//
const std::string if_args[] = {
  "client-crash",
	"client-equivocate",
	"client-equivocate-simulated",
	"client-stall-after-p1",
	"client-send-partial-p1"
};
const InjectFailureType iff[] {
  InjectFailureType::CLIENT_CRASH, //client sends p1, but does not wait reply
  InjectFailureType::CLIENT_EQUIVOCATE, //client receives p1 replies, and tried to equiv only if both commit and abort p2 quorums available
	InjectFailureType::CLIENT_EQUIVOCATE_SIMULATE, //client receives p1 replies, and always equivs p2 (even though it is not allowed to -- we simualate as if)
	InjectFailureType::CLIENT_STALL_AFTER_P1, //client sends p1, and waits for p1 replies --> finishes fallback of other transactions to unblock itself
	InjectFailureType::CLIENT_SEND_PARTIAL_P1 //client sends p1 to only some replicas, does not wait.
};
static bool ValidateInjectFailureType(const char* flagname,
    const std::string &value) {
  int n = sizeof(if_args);
  for (int i = 0; i < n; ++i) {
    if (value == if_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(indicus_inject_failure_type, if_args[0], "type of failure to"
    " inject (for Indicus)");
DEFINE_validator(indicus_inject_failure_type, &ValidateInjectFailureType);

//Pequin Sync protocol
enum query_sync_quorum_t {
  QUERY_SYNC_QUORUM_UNKNOWN,
  QUERY_SYNC_QUROUM_ONE,
  QUERY_SYNC_QUORUM_ONE_HONEST,  //at least f+1 --> 1 honest
  QUERY_SYNC_QUORUM_MAJORITY_HONEST, //at least 2f+1 --> f+1 honest
  QUERY_SYNC_QUORUM_MAJORITY, // at least 3f+1 --> 2f+1 honest (supermajority)
  QUERY_SYNC_QUORUM_ALL_POSSIBLE // at least 4f+1 (all that one can expect to receive)
};
const std::string query_sync_quorum_args[] = {
	"query-one",                   //1
  "query-one-honest",            //f+1
  "query-majority-honest",       //2f+1
  "query-super-majority-honest", //3f+1
  "query-all-possible"           //4f+1
};
const query_sync_quorum_t query_sync_quorums[] {
  QUERY_SYNC_QUROUM_ONE,         //1
  QUERY_SYNC_QUORUM_ONE_HONEST,  //at least f+1 --> 1 honest
  QUERY_SYNC_QUORUM_MAJORITY_HONEST, //at least 2f+1 --> f+1 honest
  QUERY_SYNC_QUORUM_MAJORITY, // at least 3f+1 --> 2f+1 honest (supermajority)
  QUERY_SYNC_QUORUM_ALL_POSSIBLE // at least 4f+1 (all that one can expect to receive, i.e. n-f)
};
static bool ValidateQuerySyncQuorum(const char* flagname,
    const std::string &value) {
  int n = sizeof(query_sync_quorum_args);
  for (int i = 0; i < n; ++i) {
    if (value == query_sync_quorum_args[i]) return true;
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}

enum query_messages_t {
  QUERY_MESSAGES_UNKNOWN,
  QUERY_MESSAGES_QUERY_QUORUM, //send to no only #Query_sync_quorum many replicas
  QUERY_MESSAGES_PESSIMISTIC_BONUS, //send to f additional replicas
  QUERY_MESSAGES_ALL //send to all replicas
};
const std::string query_messages_args[] = {
	"query-quorum",               //quorum+0
  "query-pessimistic-bonus",    //quorum+f
  "query-all"                   //n
};
const query_messages_t query_messagess[] {
  QUERY_MESSAGES_QUERY_QUORUM,
  QUERY_MESSAGES_PESSIMISTIC_BONUS,
  QUERY_MESSAGES_ALL
};
static bool ValidateQueryMessages(const char* flagname,
    const std::string &value) {
  int n = sizeof(query_messages_args);
  for (int i = 0; i < n; ++i) {
    if (value == query_messages_args[i]) return true;
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}

/*
 Pequin settings
*/

DEFINE_string(pequin_query_sync_quorum, query_sync_quorum_args[2], "number of replica replies required for sync quorum"); //by default: SyncClient should wait for 2f+1 replies
DEFINE_validator(pequin_query_sync_quorum, &ValidateQuerySyncQuorum);

DEFINE_string(pequin_query_messages, query_messages_args[0], "number of replicas to send query to"); //by default: To receive 2f+1 replies, need to send to 2f+1 
DEFINE_validator(pequin_query_messages, &ValidateQueryMessages);

DEFINE_string(pequin_query_merge_threshold, query_sync_quorum_args[1], "number of replica votes necessary to include tx in sync snapshot");  //by default: f+1
//Can be optimistic and set it to 1 == include all tx ==> utmost freshness, but no guarantee of validity; can be pessimistic and set it higher ==> less fresh tx, but more widely prepared.
            // Note: This only affects the client progress. Servers will only adopt committed values if they are certified, and prepared values only if they locally vote Prepare.
DEFINE_validator(pequin_query_merge_threshold, &ValidateQuerySyncQuorum);

DEFINE_bool(pequin_query_result_honest, true, "require at least 1 honest replica reply"); //by default: true
//-->if false, use first reply; if true, wait for f+1 matching. Keep waiting (or retry) depending on sync_message size.

DEFINE_string(pequin_sync_messages, query_messages_args[0], "number of replicas to send sync snapshot to for execution"); 
//send to at least as many as results are required; f additional if optimistic ids enabled; 
 //By default: Sync Client then sends sync CP to f+1, and waits for f+1 matching replies  (For liveness --pessimistic bonus -- must send to 2+1)
                                                  //Send to f additional when using optimistic tx-ids.
                                                  //Send to 5f+1 if we want to cache read set.

DEFINE_validator(pequin_sync_messages, &ValidateQueryMessages);

DEFINE_int32(pequin_retry_limit, -1, "max number of retries before aborting Tx.");

DEFINE_uint32(pequin_snapshot_prepared_k, 1, "number of prepared reads to include in snapshot (before reaching first committed version)");

DEFINE_bool(pequin_query_eager_exec, true, "skip query sync protocol and execute optimistically on local state");
DEFINE_bool(pequin_query_point_eager_exec, false, "use eager query exec instead of proof based point read");

DEFINE_bool(pequin_eager_plus_snapshot, true, "perform a snapshot and eager execution simultaneously; proceed with sync only if eager fails");
DEFINE_bool(pequin_simulate_fail_eager_plus_snapshot, false, "simulate failing eager even though it may be successful");

DEFINE_bool(pequin_query_read_prepared, true, "allow query to read prepared values");
DEFINE_bool(pequin_query_cache_read_set, true, "cache query read set at replicas"); // Send syncMessages to all if read set caching is enabled -- but still only sync_messages many replicas are tasked to execute and reply.

DEFINE_bool(pequin_query_optimistic_txid, true, "use optimistic tx-id for sync protocol");
DEFINE_bool(pequin_query_compress_optimistic_txid, false, "compress optimistic tx-id for sync protocol");


DEFINE_bool(pequin_query_merge_active_at_client, true, "merge active query read sets client-side");

DEFINE_bool(pequin_sign_client_queries, false, "sign query and sync messages"); //proves non-equivocation of query contents, and query snapshot respectively. 
//DEFINE_bool(pequin_sign_replica_to_replica_sync, false, "sign inter replica sync messages with HMACs"); //proves authenticity of channels.
//Note: Should not be necessary. Unique hash of query should suffice for non-equivocation; autheniticated channels would suffice for authentication. 

//DEFINE_bool(pequin_parallel_queries, false, "dispatch queries to parallel worker threads"); -- only serverside arg

DEFINE_bool(pequin_use_semantic_cc, true, "use SemanticCC"); //Non-semantic mode is deprecated.
DEFINE_bool(pequin_use_active_read_set, true, "store only keys that are Active w.r.t. to query predicate");


// Sintr specific args
DEFINE_string(clients_config_path, "", "path to client transport configuration file");
DEFINE_uint64(sintr_max_val_threads, 1, "sintr max number of validation threads");
DEFINE_bool(sintr_sign_fwd_read_results, true, "sintr sign forward read results");
DEFINE_bool(sintr_sign_finish_validation, true, "sintr sign finish validation message");
DEFINE_bool(sintr_debug_endorse_check, true, "sintr do a full debug validation txn endorsement check");
DEFINE_bool(sintr_client_check_evidence, true, "sintr client check prepared committed evidence on forward read results");
DEFINE_string(sintr_policy_function_name, "basic_id", "sintr policy function to use");
DEFINE_string(sintr_policy_config_path, "", "path to sintr policy configuration file");
DEFINE_uint32(sintr_read_include_policy, 0, "number indicates period of including policy in read messages, 0 indicates never");
DEFINE_uint64(sintr_min_enable_pull_policies, 0, "minimum number of replicas needed to enable policy retrieval on retry, 0 indicates never");

// given an estimated txn policy, how many other clients to contact?
const std::string sintr_client_validation_args[] = {
	"client-validation-exact",
  "client-validation-one-more",
  "client-validation-all"
};
const sintrstore::CLIENT_VALIDATION_HEURISTIC sintr_client_validations[] {
  sintrstore::CLIENT_VALIDATION_HEURISTIC::EXACT,
  sintrstore::CLIENT_VALIDATION_HEURISTIC::ONE_MORE,
  sintrstore::CLIENT_VALIDATION_HEURISTIC::ALL
};
static bool ValidateSintrClientValidation(const char* flagname,
    const std::string &value) {
  int n = sizeof(sintr_client_validation_args);
  for (int i = 0; i < n; ++i) {
    if (value == sintr_client_validation_args[i]) return true;
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
// by default, contact exactly the estimated number
DEFINE_string(sintr_client_validation, sintr_client_validation_args[0], "sintr number of clients to contact for validation");
DEFINE_validator(sintr_client_validation, &ValidateSintrClientValidation);

DEFINE_bool(sintr_client_pin_cores, false, "sintr pin client cores for validation");


///////////////////////////////////////////////////////////

DEFINE_bool(debug_stats, false, "record stats related to debugging");

const std::string trans_args[] = {
  "udp",
	"tcp"
};

const transmode_t transmodes[] {
  TRANS_UDP,
	TRANS_TCP
};
static bool ValidateTransMode(const char* flagname,
    const std::string &value) {
  int n = sizeof(trans_args);
  for (int i = 0; i < n; ++i) {
    if (value == trans_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(trans_protocol, trans_args[1], "transport protocol to use for"
		" passing messages");
DEFINE_validator(trans_protocol, &ValidateTransMode);

const std::string protocol_args[] = {
  "blackhole",
	"txn-l",
  "txn-s",
  "qw",
  "occ",
  "lock",
  "span-occ",
  "span-lock",
  "sintr",
  "pequin",
  "indicus",
	"pbft",
// HotStuff
    "hotstuff",
// Augustus-Hotstuff
    "augustus-hs",
// BFTSmart
  "bftsmart",
// Augustus-BFTSmart
	"augustus",
  "pg",
  "pg-smr", //Formerly: hotstuffpg
  "peloton-smr",
  "crdb"
};
const protomode_t protomodes[] {
  PROTO_BLACKHOLE,
  PROTO_TAPIR,
  PROTO_TAPIR,
  PROTO_WEAK,
  PROTO_STRONG,
  PROTO_STRONG,
  PROTO_STRONG,
  PROTO_STRONG,
  //
  PROTO_SINTR,
  PROTO_PEQUIN,
  PROTO_INDICUS,
  PROTO_PBFT,
  // HotStuff
  PROTO_HOTSTUFF,
  // Augustus-Hotstuff
  PROTO_AUGUSTUS,
  // BFTSmart
  PROTO_BFTSMART,
  // Augustus-BFTSmart
	PROTO_AUGUSTUS_SMART,
  PROTO_POSTGRES,
   // PG-SMR
  PROTO_PG_SMR,
  // Peloton-SMR,
  PROTO_PELOTON_SMR,
  // Cockroach Database
  PROTO_CRDB
};

//Note: this should match the size of protomodes
const strongstore::Mode strongmodes[] {
  strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_OCC,
  strongstore::Mode::MODE_LOCK,
  strongstore::Mode::MODE_SPAN_OCC,
  strongstore::Mode::MODE_SPAN_LOCK,
  strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_UNKNOWN,
	strongstore::Mode::MODE_UNKNOWN,
	strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_UNKNOWN,
	strongstore::Mode::MODE_UNKNOWN, 
  strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_UNKNOWN,
  strongstore::Mode::MODE_UNKNOWN
};
static bool ValidateProtocolMode(const char* flagname,
    const std::string &value) {

  int n = sizeof(protocol_args);
  for (int i = 0; i < n; ++i) {
    if (value == protocol_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(protocol_mode, protocol_args[0],	"the mode of the protocol to"
    " use during this experiment");
DEFINE_validator(protocol_mode, &ValidateProtocolMode);

const std::string benchmark_args[] = {
	"retwis",
  "tpcc",
  "smallbank",
  "rw",
  "tpcc-sync",
  "toy",
  "tpcc-sql",
  "rw-sql",
  "seats-sql",
  "auctionmark-sql",
  "tpcch-sql",
  "rw-sync"
};
const benchmode_t benchmodes[] {
  BENCH_RETWIS,
  BENCH_TPCC,
  BENCH_SMALLBANK_SYNC,
  BENCH_RW,
  BENCH_TPCC_SYNC,
  BENCH_TOY,
  BENCH_TPCC_SQL,
  BENCH_RW_SQL,
  BENCH_SEATS_SQL,
  BENCH_AUCTIONMARK_SQL,
  BENCH_TPCCH_SQL,
  BENCH_RW_SYNC
};
static bool ValidateBenchmark(const char* flagname, const std::string &value) {
  int n = sizeof(benchmark_args);
  for (int i = 0; i < n; ++i) {
    if (value == benchmark_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(benchmark, benchmark_args[0],	"the mode of the protocol to use"
    " during this experiment");
DEFINE_validator(benchmark, &ValidateBenchmark);

/**
 * Experiment settings.
 */
DEFINE_uint64(exp_duration, 30, "duration (in seconds) of experiment");
DEFINE_uint64(warmup_secs, 5, "time (in seconds) to warm up system before"
    " recording stats");
DEFINE_uint64(cooldown_secs, 5, "time (in seconds) to cool down system after"
    " recording stats");
DEFINE_uint64(tput_interval, 0, "time (in seconds) between throughput"
    " measurements");
DEFINE_uint64(num_client_threads, 1, "number of client threads to run on each process");
DEFINE_uint64(num_client_hosts, 1, "number of client processes across all nodes and servers");
DEFINE_uint64(num_requests, -1, "number of requests (transactions) per"
    " client");
DEFINE_int32(closest_replica, -1, "index of the replica closest to the client");
DEFINE_string(closest_replicas, "", "space-separated list of replica indices in"
    " order of proximity to client(s)");
DEFINE_uint64(delay, 0, "simulated communication delay");
DEFINE_int32(clock_skew, 0, "difference between real clock and TrueTime");
DEFINE_int32(clock_error, 0, "maximum error for clock");
DEFINE_string(stats_file, "", "path to output stats file.");
DEFINE_uint64(abort_backoff, 100, "sleep exponentially increasing amount after abort.");
DEFINE_bool(retry_aborted, true, "retry aborted transactions.");
DEFINE_int64(max_attempts, -1, "max number of attempts per transaction (or -1"
    " for unlimited).");
DEFINE_uint64(message_timeout, 10000, "length of timeout for messages in ms.");
DEFINE_uint64(max_backoff, 5000, "max time to sleep after aborting.");

const std::string partitioner_args[] = {
	"default",
  "warehouse_dist_items",
  "warehouse",
  "rw_sql",
};
const partitioner_t parts[] {
  DEFAULT,
  WAREHOUSE_DIST_ITEMS,
  WAREHOUSE,
  RW_SQL
};
static bool ValidatePartitioner(const char* flagname,
    const std::string &value) {
  int n = sizeof(partitioner_args);
  for (int i = 0; i < n; ++i) {
    if (value == partitioner_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(partitioner, partitioner_args[0],	"the partitioner to use during this"
    " experiment");
DEFINE_validator(partitioner, &ValidatePartitioner);


DEFINE_bool(store_mode, true, "true => Runs Table-store + CC-store (SQL); false => Runs pure KV-store");
/**
 * SQL Benchmark settings
*/
DEFINE_string(data_file_path, "", "path to file containing Table information to be loaded");
DEFINE_bool(sql_bench, false, "Register Tables for SQL benchmarks. Input file is JSON Table args");

/**
 * Postgres settings
*/
DEFINE_string(connection_str, "postgres://postgres:password@localhost:5432/tpccdb", "connection string to postgres database");

/**
 * Retwis settings.
 */
DEFINE_string(keys_path, "", "path to file containing keys in the system"
		" (for retwis)");
DEFINE_uint64(num_keys, 0, "number of keys to generate (for retwis");

/**
 * Seats/Auctionmark setting
*/
DEFINE_uint32(benchbase_scale_factor, 1, "scale factor for seats/auctionmark");

const std::string keys_args[] = {
	"uniform",
  "zipf"
};
const keysmode_t keysmodes[] {
  KEYS_UNIFORM,
  KEYS_ZIPF
};
static bool ValidateKeys(const char* flagname, const std::string &value) {
  int n = sizeof(keys_args);
  for (int i = 0; i < n; ++i) {
    if (value == keys_args[i]) {
      return true;
    }
  }
  std::cerr << "Invalid value for --" << flagname << ": " << value << std::endl;
  return false;
}
DEFINE_string(key_selector, keys_args[0],	"the distribution from which to "
    "select keys.");
DEFINE_validator(key_selector, &ValidateKeys);

DEFINE_double(zipf_coefficient, 0.5, "the coefficient of the zipf distribution "
    "for key selection.");

/**
 * RW settings.
 */
DEFINE_uint64(num_ops_txn, 1, "number of ops in each txn"
    " (for rw)");
DEFINE_bool(rw_read_only, false, "only do read operations");
// RW benchmark also uses same config parameters as Retwis.

/**
 * RW-sql additional settings.
 */
DEFINE_uint64(rw_read_only_rate, 0, "percentage of read only operations");
DEFINE_bool(rw_secondary_condition, true, "whether the read/update has a condition on a secondary key");

DEFINE_uint64(num_tables, 1, "number of tables for rw-sql");
DEFINE_uint64(num_keys_per_table, 1000, "number of keys per table for rw-sql");
DEFINE_int32(value_size, 10, "-1 = value is int, > 0, value is string => if value is string: size of the value strings"); //Currently not supported. Requires rw-sql input that is value String and not bigint
DEFINE_int32(value_categories, 50, "number of unique states value can be in; -1 = unlimited");

DEFINE_bool(rw_no_wrap, true, "disallow scans that wrap around. E.g. >= 97 AND < 5");
DEFINE_bool(fixed_range, true, "whether each read should have the same range");
DEFINE_uint64(max_range, 100, "max amount of reads in a single scan for rw-sql");
DEFINE_uint64(point_op_freq, 0, "percentage of times an operation is a point operation (the others are scan)");

DEFINE_bool(scan_as_point, true, "whether to execute all logical scans via individual point operations");
DEFINE_bool(scan_as_point_parallel, false, "whether to execute the individual point operations of a scan in parallel");
DEFINE_bool(rw_simulate_point_kv, false, "whether to simulate point read execution in Pesto by not invoking the Table store, but just storing in the KV store");


/**
 * TPCC settings.
 */
DEFINE_int32(warehouse_per_shard, 1, "number of warehouses per shard"
		" (for tpcc)");
DEFINE_int32(clients_per_warehouse, 1, "number of clients per warehouse"
		" (for tpcc)");
DEFINE_int32(remote_item_milli_p, 0, "remote item milli p (for tpcc)");

DEFINE_bool(tpcc_run_sequential, false, "run all queries within a TPCC transaction sequentiallly");
DEFINE_int32(tpcc_num_warehouses, 1, "number of warehouses (for tpcc)");
DEFINE_int32(tpcc_w_id, 1, "home warehouse id for this client (for tpcc)");
DEFINE_int32(tpcc_C_c_id, 1, "C value for NURand() when selecting"
    " random customer id (for tpcc)");
DEFINE_int32(tpcc_C_c_last, 1, "C value for NURand() when selecting"
    " random customer last name (for tpcc)");
DEFINE_int32(tpcc_new_order_ratio, 45, "ratio of new_order transactions to other"
    " transaction types (for tpcc)");
DEFINE_int32(tpcc_delivery_ratio, 4, "ratio of delivery transactions to other"
    " transaction types (for tpcc)");
DEFINE_int32(tpcc_stock_level_ratio, 4, "ratio of stock_level transactions to other"
    " transaction types (for tpcc)");
DEFINE_int32(tpcc_payment_ratio, 43, "ratio of payment transactions to other"
    " transaction types (for tpcc)");
DEFINE_int32(tpcc_order_status_ratio, 4, "ratio of order_status transactions to other"
    " transaction types (for tpcc)");
DEFINE_bool(static_w_id, false, "force clients to use same w_id for each treansaction");

/**
 * TPC-CH settings.
 */
DEFINE_double(ch_client_proportion, tpcch_sql::PROB_TPCCH_CLIENT, "proportion of CH clients (rest are TPC-C clients)");

/**
 * Smallbank settings.
 */

DEFINE_int32(balance_ratio, 60, "percentage of balance transactions"
    " (for smallbank)");
DEFINE_int32(deposit_checking_ratio, 10, "percentage of deposit checking"
    " transactions (for smallbank)");
DEFINE_int32(transact_saving_ratio, 10, "percentage of transact saving"
    " transactions (for smallbank)");
DEFINE_int32(amalgamate_ratio, 10, "percentage of deposit checking"
    " transactions (for smallbank)");
DEFINE_int32(write_check_ratio, 10, "percentage of write check transactions"
    " (for smallbank)");
DEFINE_int32(num_hotspots, 1000, "# of hotspots (for smallbank)");
DEFINE_int32(num_customers, 18000, "# of customers (for smallbank)");
DEFINE_double(hotspot_probability, 0.9, "probability of ending in hotspot");
DEFINE_int32(timeout, 5000, "timeout in ms (for smallbank)");
DEFINE_string(customer_name_file_path, "smallbank_names", "path to file"
    " containing names to be loaded (for smallbank)");

DEFINE_LATENCY(op);

std::vector<::AsyncClient *> asyncClients;
std::vector<::SyncClient *> syncClients;
std::vector<::Client *> clients;
std::vector<::OneShotClient *> oneShotClients;
std::vector<::BenchmarkClient *> benchClients;
std::vector<std::thread *> threads;
Transport *tport;
transport::Configuration *config;
transport::Configuration *clients_config;
KeyManager *keyManager;
Partitioner *part;
KeySelector *keySelector;
QuerySelector *querySelector;

void Cleanup(int signal);
void FlushStats();

int main(int argc, char **argv) {
  gflags::SetUsageMessage(
           "executes transactions from various transactional workload\n"
"           benchmarks against various distributed replicated transaction\n"
"           processing systems.");
	gflags::ParseCommandLineFlags(&argc, &argv, true);

  // parse transport protocol
  transmode_t trans = TRANS_UNKNOWN;
  int numTransModes = sizeof(trans_args);
  for (int i = 0; i < numTransModes; ++i) {
    if (FLAGS_trans_protocol == trans_args[i]) {
      trans = transmodes[i];
      break;
    }
  }
  if (trans == TRANS_UNKNOWN) {
    std::cerr << "Unknown transport protocol." << std::endl;
    return 1;
  }

  // parse protocol and mode
  protomode_t mode = PROTO_UNKNOWN;
  strongstore::Mode strongmode = strongstore::Mode::MODE_UNKNOWN;
  int numProtoModes = sizeof(protocol_args);
  for (int i = 0; i < numProtoModes; ++i) {
    if (FLAGS_protocol_mode == protocol_args[i]) {
      mode = protomodes[i];
      if(i < (sizeof(strongmodes)/sizeof(strongmode))) 
        strongmode = strongmodes[i];
      break;
    }
  }
  if (mode == PROTO_UNKNOWN || (mode == PROTO_STRONG
      && strongmode == strongstore::Mode::MODE_UNKNOWN)) {
    std::cerr << "Unknown protocol or unknown strongmode." << std::endl;
    return 1;
  }

  // parse benchmark
  benchmode_t benchMode = BENCH_UNKNOWN;
  int numBenchs = sizeof(benchmark_args);
  for (int i = 0; i < numBenchs; ++i) {
    if (FLAGS_benchmark == benchmark_args[i]) {
      benchMode = benchmodes[i];
      break;
    }
  }
  if (benchMode == BENCH_UNKNOWN) {
    Warning("Unknown benchmark.");
    return 1;
  }

  // parse partitioner
  partitioner_t partType = DEFAULT;
  int numParts = sizeof(partitioner_args);
  for (int i = 0; i < numParts; ++i) {
    if (FLAGS_partitioner == partitioner_args[i]) {
      partType = parts[i];
      break;
    }
  }

  // parse key selector
  keysmode_t keySelectionMode = KEYS_UNKNOWN;
  int numKeySelectionModes = sizeof(keys_args);
  for (int i = 0; i < numKeySelectionModes; ++i) {
    if (FLAGS_key_selector == keys_args[i]) {
      keySelectionMode = keysmodes[i];
      break;
    }
  }
  if (keySelectionMode == KEYS_UNKNOWN) {
    std::cerr << "Unknown key selector." << std::endl;
    return 1;
  }

  // parse read quorum
  read_quorum_t read_quorum = READ_QUORUM_UNKNOWN;
  int numReadQuorums = sizeof(read_quorum_args);
  for (int i = 0; i < numReadQuorums; ++i) {
    if (FLAGS_indicus_read_quorum == read_quorum_args[i]) {
      read_quorum = read_quorums[i];
      break;
    }
  }
  if (mode != PROTO_TAPIR && read_quorum == READ_QUORUM_UNKNOWN) { //All other ProtoModes require a ReadQuorum Size as input.
    std::cerr << "Unknown read quorum." << std::endl;
    return 1;
  }

  // parse read messages
  read_messages_t read_messages = READ_MESSAGES_UNKNOWN;
  int numReadMessagess = sizeof(read_messages_args);
  for (int i = 0; i < numReadMessagess; ++i) {
    if (FLAGS_indicus_read_messages == read_messages_args[i]) {
      read_messages = read_messagess[i];
      break;
    }
  }
  if ((mode != PROTO_TAPIR && mode != PROTO_PBFT) && read_messages == READ_MESSAGES_UNKNOWN) { //All other protocols require a ReadMessage Size as input
    std::cerr << "Unknown read messages." << std::endl;
    return 1;
  }

  // parse inject failure
  InjectFailureType injectFailureType = InjectFailureType::CLIENT_EQUIVOCATE;
  int numInjectFailure = sizeof(if_args);
  for (int i = 0; i < numInjectFailure; ++i) {
    if (FLAGS_indicus_inject_failure_type == if_args[i]) {
      injectFailureType = iff[i];
      break;
    }
  }

  // parse read dep
  read_dep_t read_dep = READ_DEP_UNKNOWN;
  int numReadDeps = sizeof(read_dep_args);
  for (int i = 0; i < numReadDeps; ++i) {
    if (FLAGS_indicus_read_dep == read_dep_args[i]) {
      read_dep = read_deps[i];
      break;
    }
  }
  if ((mode == PROTO_INDICUS || mode == PROTO_PEQUIN || mode == PROTO_SINTR) && read_dep == READ_DEP_UNKNOWN) {
    std::cerr << "Unknown read dep." << std::endl;
    return 1;
  }

  // parse query sync quorum
  query_sync_quorum_t query_sync_quorum = QUERY_SYNC_QUORUM_UNKNOWN;
  int numQuerySyncQuorums = sizeof(query_sync_quorum_args);
  for (int i = 0; i < numQuerySyncQuorums; ++i) {
    if (FLAGS_pequin_query_sync_quorum == query_sync_quorum_args[i]) {
      query_sync_quorum = query_sync_quorums[i];
      break;
    }
  }
  if ((mode == PROTO_PEQUIN || mode == PROTO_SINTR) && query_sync_quorum == QUERY_SYNC_QUORUM_UNKNOWN) { 
    std::cerr << "Unknown query sync quorum." << std::endl;
    return 1;
  }
  
  // parse query messages
  query_messages_t query_messages = QUERY_MESSAGES_UNKNOWN;
  int numQueryMessages = sizeof(query_messages_args);
  for (int i = 0; i < numQueryMessages; ++i) {
    if (FLAGS_pequin_query_messages == query_messages_args[i]) {
      query_messages = query_messagess[i];
      break;
    }
  }
  if ((mode == PROTO_PEQUIN || mode == PROTO_SINTR) && query_messages == QUERY_MESSAGES_UNKNOWN) { 
    std::cerr << "Unknown query messages." << std::endl;
    return 1;
  }

    // parse query sync quorum merge threshold
  query_sync_quorum_t query_merge_threshold = QUERY_SYNC_QUORUM_UNKNOWN;
  int numQueryMergeThresholds = sizeof(query_sync_quorum_args);
  for (int i = 0; i < numQueryMergeThresholds; ++i) {
    if (FLAGS_pequin_query_merge_threshold == query_sync_quorum_args[i]) {
      query_merge_threshold = query_sync_quorums[i];
      break;
    }
  }
  if ((mode == PROTO_PEQUIN || mode == PROTO_SINTR) && query_merge_threshold == QUERY_SYNC_QUORUM_UNKNOWN) { 
    std::cerr << "Unknown query merge threshold." << std::endl;
    return 1;
  }

  // parse sync messages
  query_messages_t sync_messages = QUERY_MESSAGES_UNKNOWN;
  int numSyncMessages = sizeof(query_messages_args);
  for (int i = 0; i < numSyncMessages; ++i) {
    if (FLAGS_pequin_sync_messages == query_messages_args[i]) {
      sync_messages = query_messagess[i];
      break;
    }
  }
  if ((mode == PROTO_PEQUIN || mode == PROTO_SINTR) && sync_messages == QUERY_MESSAGES_UNKNOWN) { 
    std::cerr << "Unknown sync messages." << std::endl;
    return 1;
  }

  // parse sintr client validation setting
  sintrstore::CLIENT_VALIDATION_HEURISTIC sintr_client_validation = sintrstore::CLIENT_VALIDATION_HEURISTIC::EXACT;
  int numSintrClientValidations = sizeof(sintr_client_validation_args);
  for (int i = 0; i < numSintrClientValidations; ++i) {
    if (FLAGS_sintr_client_validation == sintr_client_validation_args[i]) {
      sintr_client_validation = sintr_client_validations[i];
      break;
    }
  }


//////////////////////////

  // parse closest replicas
  std::vector<int> closestReplicas;
  std::stringstream iss(FLAGS_closest_replicas);
  int replica;
  iss >> replica;
  while (!iss.fail()) {
    std::cerr << "Next closest replica: " << replica << std::endl;
    closestReplicas.push_back(replica);
    iss >> replica;
  }


  // parse retwis settings
  std::vector<std::string> keys;
  if (benchMode == BENCH_RETWIS || benchMode == BENCH_RW || benchMode == BENCH_RW_SYNC) {
    if (FLAGS_keys_path.empty()) {
      if (FLAGS_num_keys > 0) {
        for (size_t i = 0; i < FLAGS_num_keys; ++i) {
          keys.push_back(std::to_string(i));
        }
      } else {
        std::cerr << "Specified neither keys file nor number of keys."
                  << std::endl;
        return 1;
      }
    } else {
      std::ifstream in;
      in.open(FLAGS_keys_path);
      if (!in) {
        std::cerr << "Could not read keys from: " << FLAGS_keys_path
                  << std::endl;
        return 1;
      }
      std::string key;
      while (std::getline(in, key)) {
        keys.push_back(key);
      }
      in.close();
    }
  }

  switch (trans) {
    case TRANS_TCP:
      tport = new TCPTransport(0.0, 0.0, 0, false, 0, 1, FLAGS_indicus_hyper_threading, false, 0);
      break;
    case TRANS_UDP:
      tport = new UDPTransport(0.0, 0.0, 0, nullptr);
      break;
    default:
      NOT_REACHABLE();
  }

  Debug("transport protocol used: %d",trans);

  if(FLAGS_zipf_coefficient == 1.0) Panic("Use a Zipf coefficient != 1.0. E.g. 0.99 or 1.01. 1.0 is not supported");

  switch (keySelectionMode) {
    case KEYS_UNIFORM:
      keySelector = new UniformKeySelector(keys);
      break;
    case KEYS_ZIPF:
      keySelector = new ZipfKeySelector(keys, FLAGS_zipf_coefficient);
      break;
    default:
      NOT_REACHABLE();
  }

  /// QuerySelector
  if(FLAGS_sql_bench && benchMode == BENCH_RW_SQL){
    //Create QuerySelector
    KeySelector *tableSelector;
    KeySelector *baseSelector;
    KeySelector *rangeSelector = new UniformKeySelector(keys, FLAGS_max_range); //doesn't make sense really to have a zipfean range selector - does not strongly correlate to contention. The bigger the range = the bigger contention

    auto maxBase = FLAGS_rw_no_wrap? FLAGS_num_keys_per_table - (FLAGS_max_range-1) : FLAGS_num_keys_per_table;
    Notice("maxBase: %d", maxBase);

    //Note: "keys" is an empty/un-used argument for this setup.
    switch (keySelectionMode) {
      case KEYS_UNIFORM:
      {
        Notice("Using Uniform selector");
        tableSelector = new UniformKeySelector(keys, FLAGS_num_tables);
       
        baseSelector = new UniformKeySelector(keys, maxBase);
        //rangeSelector = new UniformKeySelector(keys, FLAGS_max_range);
        break;
      }
      case KEYS_ZIPF:
      {
        Notice("Using Zipf selector. Coefficient: %f", FLAGS_zipf_coefficient);
        tableSelector = new ZipfKeySelector(keys, FLAGS_zipf_coefficient, FLAGS_num_tables);
        baseSelector = new ZipfKeySelector(keys, FLAGS_zipf_coefficient, maxBase);
        //rangeSelector = new ZipfKeySelector(keys, FLAGS_zipf_coefficient, FLAGS_max_range);
        break;
      }
      default:
        NOT_REACHABLE();
    }

    querySelector = new QuerySelector(FLAGS_num_keys_per_table, tableSelector, baseSelector, rangeSelector, FLAGS_point_op_freq);


     //RW-SQL ==> auto-generate TableRegistry
    FLAGS_data_file_path = std::filesystem::path(FLAGS_data_file_path).replace_filename("rw-sql-gen-client" + std::to_string(FLAGS_client_id));
    TableWriter table_writer(FLAGS_data_file_path, false);

    //Set up a bunch of Tables: Num_tables many; with num_items...
    const std::vector<std::pair<std::string, std::string>> &column_names_and_types = {{"key", "INT"}, {"value", (FLAGS_value_size < 0 ? "INT" : "VARCHAR")}}; //switch value type depending on FLAG
    const std::vector<uint32_t> &primary_key_col_idx = {0};
        //Create Table
        
    for(int i=0; i<FLAGS_num_tables; ++i){
      //string table_name = "table_" + std::to_string(i);
      string table_name = "t" + std::to_string(i);
      table_writer.add_table(table_name, column_names_and_types, primary_key_col_idx);
    }

    table_writer.flush();
    FLAGS_data_file_path += "-tables-schema.json";
    //Read in a TableRegistry? (Probably not needed, but can add)
  }



  ///


  std::mt19937 rand(FLAGS_client_id); // TODO: is this safe?

  switch (partType) {
    case DEFAULT:
    {
      if(FLAGS_sql_bench){
        part = new DefaultSQLPartitioner();
      }
      else{
        part = new DefaultPartitioner();
      }
      break;
    }
    case WAREHOUSE_DIST_ITEMS:
    {
      if(FLAGS_sql_bench) Panic("deprecated partitioner for sql bench");
      part = new WarehouseDistItemsPartitioner(FLAGS_tpcc_num_warehouses);
      break;
    }
    case WAREHOUSE:
    {
      if(FLAGS_sql_bench){
        UW_ASSERT(benchMode == BENCH_TPCC_SQL);
        part = new WarehouseSQLPartitioner(FLAGS_tpcc_num_warehouses, rand);
      }
      else{
        UW_ASSERT(benchMode == BENCH_TPCC_SYNC || benchMode == BENCH_TPCC);
        part = new WarehousePartitioner(FLAGS_tpcc_num_warehouses, rand);
      }
      break;
    }
    case RW_SQL:
    {
      UW_ASSERT(benchMode == BENCH_RW_SQL);
      part = new RWSQLPartitioner(FLAGS_num_tables);
      break;
    }
    default:
      NOT_REACHABLE();
  }
  
	std::string latencyFile;
  std::string latencyRawFile;
  std::vector<uint64_t> latencies;
  std::atomic<size_t> clientsDone(0UL);

  bench_done_callback bdcb = [&]() {
    ++clientsDone;
    if (clientsDone == FLAGS_num_client_threads) {
      Latency_t sum;
      _Latency_Init(&sum, "total");
      for (unsigned int i = 0; i < benchClients.size(); i++) {
        Latency_Sum(&sum, &benchClients[i]->latency);
      }

      Latency_Dump(&sum);
      if (latencyFile.size() > 0) {
        Latency_FlushTo(latencyFile.c_str());
      }

      latencyRawFile = latencyFile+".raw";
      std::ofstream rawFile(latencyRawFile.c_str(),
          std::ios::out | std::ios::binary);
      for (auto x : benchClients) {
        rawFile.write((char *)&x->latencies[0],
            (x->latencies.size()*sizeof(x->latencies[0])));
        if (!rawFile) {
          Warning("Failed to write raw latency output");
        }
      }

      tport->Stop();
    }
  };

  std::ifstream configStream(FLAGS_config_path);
  if (configStream.fail()) {
    std::cerr << "Unable to read configuration file: " << FLAGS_config_path
              << std::endl;
    return -1;
  }
  config = new transport::Configuration(configStream);

  std::ifstream clientsConfigStream(FLAGS_clients_config_path);
  if (mode == PROTO_SINTR && clientsConfigStream.fail()) {
    std::cerr << "Did not provide valid clients config path for Sintr: " << FLAGS_clients_config_path
              << std::endl;
    return -1;
  }
  else if(mode == PROTO_SINTR) {
    clients_config = new transport::Configuration(clientsConfigStream);
  }

	crypto::KeyType keyType;
  switch (FLAGS_indicus_key_type) {
  case 0:
    keyType = crypto::RSA;
    break;
  case 1:
    keyType = crypto::ECDSA;
    break;
  case 2:
    keyType = crypto::ED25;
    break;
  case 3:
    keyType = crypto::SECP;
    break;
	case 4:
	  keyType = crypto::DONNA;
	  break;
  default:
    throw "unimplemented";
  }

  uint64_t replica_total = FLAGS_num_shards * config->n;
  uint64_t client_total = FLAGS_num_client_hosts * FLAGS_num_client_threads;

  KeyManager *keyManager = new KeyManager(FLAGS_indicus_key_path, keyType, true, replica_total, client_total, FLAGS_num_client_hosts);
  //keyManager->PreLoadPubKeys(false);

  if (closestReplicas.size() > 0 && closestReplicas.size() != static_cast<size_t>(config->n)) {
    std::cerr << "If specifying closest replicas, must specify all "
               << config->n << "; only specified "
               << closestReplicas.size() << std::endl;
    return 1;
  }

  if (FLAGS_num_client_threads > (1 << 6)) {
    std::cerr << "Only support up to " << (1 << 6) << " clients in one process." << std::endl;
    Panic("Unsupported number of client threads.");
    return 1;
  }

  Notice("num hosts=%d; num_threads=%d", FLAGS_num_client_hosts, FLAGS_num_client_threads);
  
  for (size_t i = 0; i < FLAGS_num_client_threads; i++) {
    Client *client = nullptr;
    AsyncClient *asyncClient = nullptr;
    SyncClient *syncClient = nullptr;
    OneShotClient *oneShotClient = nullptr;

    //uint64_t clientId = (FLAGS_client_id << 6) | i;
    //uint64_t clientId = (FLAGS_client_id << 2) | i;
    //uint64_t clientId = FLAGS_client_id | i;
    uint64_t clientId = FLAGS_client_id + FLAGS_num_client_hosts * i;
  
    //keyManager->PreLoadPrivKey(clientId, true);
    // Alternatively: uint64_t clientId = FLAGS_client_id * FLAGS_num_client_threads + i;
    
    ////////////////// PROTOCOL CLIENTS

    ///////////////// Additional parameter configurations
    uint64_t readQuorumSize = 0; //number of replies necessary to form a read quorum
    uint64_t readMessages = 0; //number of read messages sent to replicas to request replies
    uint64_t pessimistic_quorum_bonus = FLAGS_indicus_optimistic_read_quorum? 0 : config->f; //by default only sends read to the same amount of replicas that we need replies from; if there are faults, we may need to send to more.
    uint64_t readDepSize = 0; //number of replica replies needed to form dependency  
    InjectFailure failure; //Type of Failure to be injected

    uint64_t syncQuorumSize = 0; //number of replies necessary to form a sync quorum
    uint64_t queryMessages = 0; //number of query messages sent to replicas to request sync replies
    uint64_t mergeThreshold = 1; //number of tx instances required to observe to include in sync snapshot
    uint64_t syncMessages = 0;    //number of sync messages sent to replicas to request result replies
    uint64_t resultQuorum = FLAGS_pequin_query_result_honest? config->f + 1 : 1;

    switch (mode) {
      case PROTO_POSTGRES:
      case PROTO_CRDB:
      case PROTO_TAPIR:
           break;
      case PROTO_SINTR:
      case PROTO_PEQUIN:
         switch (query_sync_quorum) {
          case QUERY_SYNC_QUROUM_ONE:
            syncQuorumSize = 1;
            break;
          case QUERY_SYNC_QUORUM_ONE_HONEST:
            syncQuorumSize = config->f + 1;
            break;
          case QUERY_SYNC_QUORUM_MAJORITY_HONEST:
            syncQuorumSize = config->f * 2 + 1;  //majority of quorum will be honest
            break;
          case QUERY_SYNC_QUORUM_MAJORITY:
            syncQuorumSize = config->f * 3 + 1; //== majority of all honest will be included in quorum
            break;
          case QUERY_SYNC_QUORUM_ALL_POSSIBLE:
            syncQuorumSize = config->f * 4 + 1;
            break;
          default: 
            NOT_REACHABLE();
         }
         switch (query_merge_threshold) { 
          case QUERY_SYNC_QUROUM_ONE:
            mergeThreshold = 1;
            break;
          case QUERY_SYNC_QUORUM_ONE_HONEST:
            mergeThreshold = config->f + 1;
            break;
          //NOTE: These cases realistically will never be used -- but supported here anyways
          case QUERY_SYNC_QUORUM_MAJORITY_HONEST:
            mergeThreshold = config->f * 2 + 1;  //majority of quorum will be honest
            break;
          case QUERY_SYNC_QUORUM_MAJORITY:
            mergeThreshold = config->f * 3 + 1; //== majority of all honest will be included in quorum
            break;
          case QUERY_SYNC_QUORUM_ALL_POSSIBLE:
            mergeThreshold = config->f * 4 + 1;
            break;
          default: 
            NOT_REACHABLE();
         }
         if(mergeThreshold > syncQuorumSize) Panic("Merge Threshold for Query Sync cannot be larger than Quorum itself");
         if(mergeThreshold + config->f > syncQuorumSize) std::cerr << "WARNING: Under given Config Query Sync Merge is not live in presence of byzantine replies in Query Sync Quorum" << std::endl;
        
         switch (query_messages) {
          case QUERY_MESSAGES_QUERY_QUORUM:
              queryMessages = syncQuorumSize;
              break;
          case QUERY_MESSAGES_PESSIMISTIC_BONUS:
              queryMessages = syncQuorumSize + config->f;
              break;
          case QUERY_MESSAGES_ALL:
              queryMessages = config->n;
              break;
          default:
              NOT_REACHABLE();
          }
          if(syncQuorumSize > queryMessages) Panic("Query Quorum size cannot be larger than number of Query requests sent");
          if(syncQuorumSize + config->f > queryMessages) std::cerr << "WARNING: Under given Config Query Sync is not live in presence of byzantine replies witholding query sync replies (omission faults)" << std::endl;
         
         switch (sync_messages) {
          case QUERY_MESSAGES_QUERY_QUORUM:
              syncMessages = resultQuorum;
              break;
          case QUERY_MESSAGES_PESSIMISTIC_BONUS:
              syncMessages = resultQuorum + config->f;
              break;
          case QUERY_MESSAGES_ALL:
              syncMessages = config->n;
              break;
          default:
              NOT_REACHABLE();
          }

          if(resultQuorum > syncMessages) Panic("Query Result Quorum size cannot be larger than number of Sync Messages sent");
          if(resultQuorum + config->f > syncMessages) std::cerr << "WARNING: Under given Config Query Result is not live in presence of byzantine replies witholding query results (omission faults)" << std::endl;
         
         
         // ==> Moved to client logic in order to account for retries.
         //if(FLAGS_pequin_query_optimistic_txid) syncMessages = std::max((uint64_t) config->n, syncMessages + config->f); //If optimisticTxID enabled send to f additional replicas to guarantee result. 
         // if read_cache is True:--> send sync to all, but still only ask syncMessages many to execute.

        Debug("Configuring Pequin to send query messages to %lu replicas and wait for %lu replies. Merge Threshold is %lu. %lu Sync messages are being sent", queryMessages, syncQuorumSize, mergeThreshold, syncMessages);

      case PROTO_INDICUS:
         switch (read_quorum) {
            case READ_QUORUM_ONE:
                readQuorumSize = 1;
                break;
            case READ_QUORUM_ONE_HONEST:
                readQuorumSize = config->f + 1;
                break;
            case READ_QUORUM_MAJORITY_HONEST:
                readQuorumSize = config->f * 2 + 1;
                break;
            case READ_QUORUM_MAJORITY:
                readQuorumSize = (config->n + 1) / 2;
                break;
            case READ_QUORUM_ALL:
                readQuorumSize = config->f * 4 + 1;
                break;
            default:
                NOT_REACHABLE();
        }

        switch (read_messages) {
          case READ_MESSAGES_READ_QUORUM:
              readMessages = readQuorumSize + pessimistic_quorum_bonus;
              break;
          case READ_MESSAGES_MAJORITY:
              readMessages = (config->n + 1) / 2;
              break;
          case READ_MESSAGES_ALL:
              readMessages = config->n;
              break;
          default:
              NOT_REACHABLE();
          }
          Debug("Configuring Indicus to send read messages to %lu replicas and wait for %lu replies.", readMessages, readQuorumSize);
          UW_ASSERT(readMessages >= readQuorumSize);

          switch (read_dep) {
          case READ_DEP_ONE:
              readDepSize = 1;
              break;
          case READ_DEP_ONE_HONEST:
              readDepSize = config->f + 1;
              break;
          default:
              NOT_REACHABLE();
          }

          failure.type = injectFailureType;
          failure.timeMs = FLAGS_indicus_inject_failure_ms + rand() % 100; //offset client failures a bit.
          //	std::cerr << "client_id = " << FLAGS_client_id << " < ?" << (72* FLAGS_indicus_inject_failure_proportion/100) << ". Failure enabled: "<< failure.enabled <<  std::endl;
          failure.enabled = FLAGS_num_client_hosts * i + FLAGS_client_id < floor(FLAGS_num_client_hosts * FLAGS_num_client_threads * FLAGS_indicus_inject_failure_proportion / 100);
            std::cerr << "client_id = " << clientId << ", client_process = " << FLAGS_client_id << ", thread_id = " << i << ". Failure enabled: "<< failure.enabled <<  std::endl;
          failure.frequency = FLAGS_indicus_inject_failure_freq;
        break;
      case PROTO_PBFT:
      case PROTO_HOTSTUFF:
      case PROTO_PG_SMR:
      case PROTO_PELOTON_SMR:
      case PROTO_BFTSMART:
      case PROTO_AUGUSTUS_SMART:
      case PROTO_AUGUSTUS:
        switch (read_quorum) {
          case READ_QUORUM_ONE:
              readQuorumSize = 1;
              break;
          case READ_QUORUM_ONE_HONEST:
              readQuorumSize = config->f + 1;
              break;
          case READ_QUORUM_MAJORITY_HONEST:
              readQuorumSize = config->f * 2 + 1;
              break;
          default:
              NOT_REACHABLE();
        }
				
        switch (read_messages) {
          case READ_MESSAGES_READ_QUORUM:
              readMessages = readQuorumSize + pessimistic_quorum_bonus; 
              break;
          case READ_MESSAGES_MAJORITY:
              readMessages = (config->n + 1) / 2;
              break;
          case READ_MESSAGES_ALL:
              readMessages = config->n;
              break;
          default:
              NOT_REACHABLE();
        }
    }

   
    if(true){
      //TODO: Parameterize better: This version by default always uses pessimistic bonus.
      //If SemanticCC enabled need at least these Quorum sizes.
      if(FLAGS_pequin_use_semantic_cc && !FLAGS_pequin_query_cache_read_set){
        if(FLAGS_pequin_query_eager_exec){ //TODO: Even when query does not use eager, might want to increase quorum sizes for retries
          queryMessages = std::max((int)queryMessages, 4 * config->f + 1);
          syncQuorumSize = std::max((int)syncQuorumSize, 3 * config->f + 1); 
          //Note: syncQuorum size does not necessarily need to increase, but might as well for better freshness since we already have larger queryMessages
        }
        syncMessages = std::max((int)syncMessages, 4 * config->f + 1);
        resultQuorum = 3 * config->f + 1;
      }
    }
    else{
      //NEW version: parameterized.
      if(FLAGS_pequin_use_semantic_cc && !FLAGS_pequin_query_cache_read_set){
        Notice("Upgrading number of messages and Quorum sizes for SemanticCC");
        resultQuorum = 3 * config->f + 1;

        switch (sync_messages) {
          case QUERY_MESSAGES_QUERY_QUORUM:
              syncMessages = resultQuorum;
              break;
          case QUERY_MESSAGES_PESSIMISTIC_BONUS:
              syncMessages = resultQuorum + config->f;
              break;
          case QUERY_MESSAGES_ALL:
              syncMessages = config->n;
              break;
          default:
              NOT_REACHABLE();
        }

        if(FLAGS_pequin_query_eager_exec){
            switch (query_messages) {
              case QUERY_MESSAGES_QUERY_QUORUM:
                  queryMessages = std::max(syncQuorumSize, syncMessages);
                  break;
              case QUERY_MESSAGES_PESSIMISTIC_BONUS:
                  queryMessages = std::max(syncQuorumSize, syncMessages) + config->f;
                  break;
              case QUERY_MESSAGES_ALL:
                  queryMessages = config->n;
                  break;
              default:
                  NOT_REACHABLE();
              }
              if(syncQuorumSize > queryMessages) Panic("Query Quorum size cannot be larger than number of Query requests sent");
              if(syncQuorumSize + config->f > queryMessages) std::cerr << "WARNING: Under given Config Query Sync is not live in presence of byzantine replies witholding query sync replies (omission faults)" << std::endl;
        }
      }
    }

    Notice("Quorum sizes:");
    Notice("queryMessages: %d", queryMessages);
    Notice("syncQuorum: %d", syncQuorumSize);
    Notice("mergeThreshold: %d", mergeThreshold);

    Notice("syncMessages: %d", syncMessages);
    Notice("resultQuorum: %d", resultQuorum);
    Notice("optimistic Id: %d",  FLAGS_pequin_query_optimistic_txid);


//Declare Protocol Clients

    switch (mode) {
    case PROTO_BLACKHOLE: {
        client = new blackhole::Client();
        break;
    }
    case PROTO_TAPIR: {
        client = new tapirstore::Client(config, clientId,
                                        FLAGS_num_shards, FLAGS_num_groups, FLAGS_closest_replica,
                                        tport, part, FLAGS_ping_replicas, FLAGS_tapir_sync_commit,
                                        TrueTime(FLAGS_clock_skew,
                                                 FLAGS_clock_error));
        break;
    }
    case PROTO_SINTR: {
      // non flag parameters are server only
      sintrstore::SintrParameters sintr_params(
        FLAGS_sintr_max_val_threads,
        FLAGS_sintr_sign_fwd_read_results,
        FLAGS_sintr_sign_finish_validation,
        FLAGS_sintr_debug_endorse_check,
        FLAGS_sintr_client_check_evidence,
        FLAGS_sintr_policy_function_name,
        FLAGS_sintr_policy_config_path,
        FLAGS_sintr_read_include_policy,
        sintr_client_validation, true,
        FLAGS_sintr_client_pin_cores, FLAGS_sintr_min_enable_pull_policies
      );

      sintrstore::QueryParameters query_params(FLAGS_store_mode,
                                                syncQuorumSize,
                                                queryMessages,
                                                mergeThreshold,
                                                syncMessages,
                                                resultQuorum,
                                                FLAGS_pequin_retry_limit,
                                                FLAGS_pequin_snapshot_prepared_k,
                                                FLAGS_pequin_query_eager_exec,
                                                FLAGS_pequin_query_point_eager_exec,
                                                FLAGS_pequin_eager_plus_snapshot,
                                                FLAGS_pequin_simulate_fail_eager_plus_snapshot,
                                                false, //ForceReadFromSnapshot
                                                FLAGS_pequin_query_read_prepared,
                                                FLAGS_pequin_query_cache_read_set,
                                                FLAGS_pequin_query_optimistic_txid,
                                                FLAGS_pequin_query_compress_optimistic_txid, 
                                                FLAGS_pequin_query_merge_active_at_client,
                                                FLAGS_pequin_sign_client_queries,
                                                false,    // FLAGS_pequin_sign_replica_to_replica_sync,
                                                false,   //FLAGS_pequin_parallel_queries);
                                                FLAGS_pequin_use_semantic_cc,
                                                FLAGS_pequin_use_active_read_set,
                                                0UL, 0UL); //monotonicity grace (first & second)

        sintrstore::Parameters params(FLAGS_indicus_sign_messages,
                                        FLAGS_indicus_validate_proofs, FLAGS_indicus_hash_digest,
                                        FLAGS_indicus_verify_deps, FLAGS_indicus_sig_batch,
                                        FLAGS_indicus_max_dep_depth, readDepSize,
																				false, false,
																				false, false,
                                        FLAGS_indicus_merkle_branch_factor, failure,
                                        FLAGS_indicus_multi_threading, FLAGS_indicus_batch_verification,
																				FLAGS_indicus_batch_verification_size,
																				false,
																				false,
																				false,
																				FLAGS_indicus_parallel_CCC,
																				false,
																				FLAGS_indicus_all_to_all_fb,
																			  FLAGS_indicus_no_fallback,
																				FLAGS_indicus_relayP1_timeout,
																			  false,
                                        FLAGS_indicus_sign_client_proposals,
                                        0,
                                        query_params,
                                        sintr_params);

        Notice("Warmup secs: %d", FLAGS_warmup_secs);
        client = new sintrstore::Client(config, clientId,
                                          FLAGS_num_shards,
                                          FLAGS_num_groups, closestReplicas, FLAGS_ping_replicas, tport, part,
                                          FLAGS_tapir_sync_commit, 
                                          readMessages, readQuorumSize,
                                          params, 
                                          FLAGS_data_file_path, //table_registry
                                          keyManager, 
                                          FLAGS_indicus_phase1_decision_timeout,
                                          FLAGS_warmup_secs,
																					FLAGS_indicus_max_consecutive_abstains,
                                          FLAGS_sql_bench,
																					TrueTime(FLAGS_clock_skew, FLAGS_clock_error),
                                          clients_config,
                                          keys); // for benchmarks that need keys, need to give the validating client access
        break;
    }
    case PROTO_PEQUIN: {
      pequinstore::QueryParameters query_params(FLAGS_store_mode,
                                                 syncQuorumSize,
                                                 queryMessages,
                                                 mergeThreshold,
                                                 syncMessages,
                                                 resultQuorum,
                                                 FLAGS_pequin_retry_limit,
                                                 FLAGS_pequin_snapshot_prepared_k,
                                                 FLAGS_pequin_query_eager_exec,
                                                 FLAGS_pequin_query_point_eager_exec,
                                                 FLAGS_pequin_eager_plus_snapshot,
                                                 FLAGS_pequin_simulate_fail_eager_plus_snapshot,
                                                 false, //ForceReadFromSnapshot
                                                 FLAGS_pequin_query_read_prepared,
                                                 FLAGS_pequin_query_cache_read_set,
                                                 FLAGS_pequin_query_optimistic_txid,
                                                 FLAGS_pequin_query_compress_optimistic_txid, 
                                                 FLAGS_pequin_query_merge_active_at_client,
                                                 FLAGS_pequin_sign_client_queries,
                                                 false,    // FLAGS_pequin_sign_replica_to_replica_sync,
                                                 false,   //FLAGS_pequin_parallel_queries);
                                                 FLAGS_pequin_use_semantic_cc,
                                                 FLAGS_pequin_use_active_read_set,
                                                 0UL, 0UL); //monotonicity grace (first & second)

        pequinstore::Parameters params(FLAGS_indicus_sign_messages,
                                        FLAGS_indicus_validate_proofs, FLAGS_indicus_hash_digest,
                                        FLAGS_indicus_verify_deps, FLAGS_indicus_sig_batch,
                                        FLAGS_indicus_max_dep_depth, readDepSize,
																				false, false,
																				false, false,
                                        FLAGS_indicus_merkle_branch_factor, failure,
                                        FLAGS_indicus_multi_threading, FLAGS_indicus_batch_verification,
																				FLAGS_indicus_batch_verification_size,
																				false,
																				false,
																				false,
																				FLAGS_indicus_parallel_CCC,
																				false,
																				FLAGS_indicus_all_to_all_fb,
																			  FLAGS_indicus_no_fallback,
																				FLAGS_indicus_relayP1_timeout,
																			  false,
                                        FLAGS_indicus_sign_client_proposals,
                                        0,
                                        query_params);

        Notice("Warmup secs: %d", FLAGS_warmup_secs);
        client = new pequinstore::Client(config, clientId,
                                          FLAGS_num_shards,
                                          FLAGS_num_groups, closestReplicas, FLAGS_ping_replicas, tport, part,
                                          FLAGS_tapir_sync_commit, 
                                          readMessages, readQuorumSize,
                                          params, 
                                          FLAGS_data_file_path, //table_registry
                                          keyManager, 
                                          FLAGS_indicus_phase1_decision_timeout,
                                          FLAGS_warmup_secs,
																					FLAGS_indicus_max_consecutive_abstains,
                                          FLAGS_sql_bench,
																					TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
        break;
    }
    case PROTO_INDICUS: {
        indicusstore::Parameters params(FLAGS_indicus_sign_messages,
                                        FLAGS_indicus_validate_proofs, FLAGS_indicus_hash_digest,
                                        FLAGS_indicus_verify_deps, FLAGS_indicus_sig_batch,
                                        FLAGS_indicus_max_dep_depth, readDepSize,
																				false, false,
																				false, false,
                                        FLAGS_indicus_merkle_branch_factor, failure,
                                        FLAGS_indicus_multi_threading, FLAGS_indicus_batch_verification,
																				FLAGS_indicus_batch_verification_size,
																				false,
																				false,
																				false,
																				FLAGS_indicus_parallel_CCC,
																				false,
																				FLAGS_indicus_all_to_all_fb,
																			  FLAGS_indicus_no_fallback,
																				FLAGS_indicus_relayP1_timeout,
																			  false,
                                        FLAGS_indicus_sign_client_proposals,
                                        0);

        client = new indicusstore::Client(config, clientId,
                                          FLAGS_num_shards,
                                          FLAGS_num_groups, closestReplicas, FLAGS_ping_replicas, tport, part,
                                          FLAGS_tapir_sync_commit, readMessages, readQuorumSize,
                                          params, keyManager, FLAGS_indicus_phase1_decision_timeout,
																					FLAGS_indicus_max_consecutive_abstains,
																					TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
        break;
    }
    case PROTO_PBFT: {
      //Currently deprecated
      //Note: does not use readMessage size as input parameter. Should have this option. 
        client = new pbftstore::Client(*config, FLAGS_num_shards,
                                       FLAGS_num_groups, tport, part,
                                       readQuorumSize,
                                       FLAGS_indicus_sign_messages, FLAGS_indicus_validate_proofs,
                                       keyManager,
																			 FLAGS_pbft_order_commit, FLAGS_pbft_validate_abort,
																			 TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
        break;
    }

    // HotStuff
    case PROTO_HOTSTUFF: {
        client = new hotstuffstore::Client(*config, clientId, FLAGS_num_shards,
                                       FLAGS_num_groups, closestReplicas,
																			  tport, part,
                                       readMessages, readQuorumSize,
                                       FLAGS_indicus_sign_messages, FLAGS_indicus_validate_proofs,
                                       keyManager,
																			 FLAGS_pbft_order_commit, FLAGS_pbft_validate_abort,
																			 TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
        break;
    }

    // HotStuff Postgres
    case PROTO_PG_SMR: {
        client = new pg_SMRstore::Client(*config, clientId, FLAGS_num_shards,
                                       FLAGS_num_groups, closestReplicas,
																			  tport, part,
                                       readMessages, readQuorumSize,
                                       FLAGS_indicus_sign_messages, FLAGS_indicus_validate_proofs,
                                       keyManager,
																			 TrueTime(FLAGS_clock_skew, FLAGS_clock_error), FLAGS_pg_fake_SMR, FLAGS_pg_SMR_mode, FLAGS_bftsmart_codebase_dir);
        break;
    }

    // Peloton SMR
    case PROTO_PELOTON_SMR: {
        client = new pelotonstore::Client(*config, clientId, FLAGS_num_shards,
                                       FLAGS_num_groups, closestReplicas,
																			  tport, part,
                                       readMessages, readQuorumSize,
                                       FLAGS_indicus_sign_messages, FLAGS_indicus_validate_proofs,
                                       keyManager,
																			 TrueTime(FLAGS_clock_skew, FLAGS_clock_error), FLAGS_pg_fake_SMR, FLAGS_pg_SMR_mode, FLAGS_bftsmart_codebase_dir);
        break;
    }

		// BFTSmart
		    case PROTO_BFTSMART: {
		        client = new bftsmartstore::Client(*config, clientId, FLAGS_num_shards,
		                                       FLAGS_num_groups, closestReplicas,
																					  tport, part,
		                                       readMessages, readQuorumSize,
		                                       FLAGS_indicus_sign_messages, FLAGS_indicus_validate_proofs,
		                                       keyManager, FLAGS_bftsmart_codebase_dir,
																					 FLAGS_pbft_order_commit, FLAGS_pbft_validate_abort,
																					 TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
		        break;
		    }

    //Augustus on top of BFTSmart
		case PROTO_AUGUSTUS_SMART: {
				client = new bftsmartstore_stable::Client(*config, clientId, FLAGS_num_shards,
																			 FLAGS_num_groups, closestReplicas,
																				tport, part,
																			 readMessages, readQuorumSize,
																			 FLAGS_indicus_sign_messages, FLAGS_indicus_validate_proofs,
																			 keyManager,
																			 FLAGS_pbft_order_commit, FLAGS_pbft_validate_abort,
																			 TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
				break;
		}

// Augustus on top of Hotstuff
    case PROTO_AUGUSTUS: {
        client = new augustusstore::Client(*config, clientId, FLAGS_num_shards,
                                       FLAGS_num_groups, closestReplicas,
																			  tport, part,
                                       readMessages, readQuorumSize,
                                       FLAGS_indicus_sign_messages, FLAGS_indicus_validate_proofs,
                                       keyManager,
																			 FLAGS_pbft_order_commit, FLAGS_pbft_validate_abort,
																			 TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
        break;
    }

    case PROTO_POSTGRES: {
      client = new postgresstore::Client(FLAGS_connection_str, FLAGS_experiment_name, FLAGS_pg_replicated, clientId);
      break;
    }

    case PROTO_CRDB: {
      client = new cockroachdb::Client(
            *config, clientId, FLAGS_num_shards, FLAGS_num_groups, tport,
            TrueTime(FLAGS_clock_skew, FLAGS_clock_error));
        break;
    }

    default:
        NOT_REACHABLE();
    }

//////////////////////////////////// Benchmark Clients
////////////////////////////
///////// Structure:  
////////              Protocol Proxy: syncClient/asyncClient (depending on whether benchmark uses synchronous or asynchronous design --> common/frontend/async_adapter_client.cc or sync_client.cc)
/////////             Workload Driver: bench = BenchmarkTypeClient (benchmark/async/workload/workload_client.cc, e.g. smallbank_client) --> inherits Sync/AsyncTransactionBenchClient (benchmark/async/async_transaction_bench_client.cc) 
//                                             --> inherits BenchmarkClient (benchmark/async/bench_client.cc).   BenchmarkClient manages warump/cooldown, measures latencies
///                                                
//////// Workflow: 
///////               Sync:  bench->Start() calls BenchmarkClient->Start() --> calls SyncTransactionBenchClient-->SendNext() --> calls BenchmarkTypeClient --> GetNextTransaction which returns a txn to execute (type_transaction.cc); 
///////                      SendNext() also calls txn->Execute(syncClient) which in turn calls syncClient->Begin/Get/Put/Commit(operation) -- returns a Commit/Abort result for the txn. Retries Txn if aborted by system
//////                       Execution loop keeps calling SendNext() until workload->isFullyDone() ()
//////                Async: works mostly the same, but calls are wrapped in callbacks for asynchronous workflow: AsyncTransactionBenchClient->SendNext() calls GetNextTransaction and asyncClient.Execute(txn, callback) which in turn 
/////                         calls asyncAdapterClient->Begin/Put/Get/Commit (in an async fashion). Callback calls BenchmarkClient-->OnReply(result), which then calls AsyncTransactionBenchClient->SendNext() again

    switch (benchMode) {
      case BENCH_RETWIS:
      case BENCH_TPCC:
      case BENCH_RW:
        if (asyncClient == nullptr) {
          UW_ASSERT(client != nullptr);
          asyncClient = new AsyncAdapterClient(client, FLAGS_message_timeout);
        }
        break;
      case BENCH_TOY: 
      case BENCH_RW_SQL:
      case BENCH_RW_SYNC:
      case BENCH_SMALLBANK_SYNC:
      case BENCH_TPCC_SYNC:
      case BENCH_TPCC_SQL:
      case BENCH_SEATS_SQL:
      case BENCH_AUCTIONMARK_SQL:
      case BENCH_TPCCH_SQL:
        if (syncClient == nullptr) {
          UW_ASSERT(client != nullptr);
          syncClient = new SyncClient(client);
        }
        break;
      default:
        NOT_REACHABLE();
    }

    uint32_t seed = (FLAGS_client_id << 4) | i;
	  BenchmarkClient *bench;
    
	  switch (benchMode) {
      case BENCH_RETWIS:
        UW_ASSERT(asyncClient != nullptr);
        bench = new retwis::RetwisClient(keySelector, *asyncClient, *tport,
            seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts);
        break;
      case BENCH_TPCC:
        UW_ASSERT(asyncClient != nullptr);
        bench = new tpcc::AsyncTPCCClient(*asyncClient, *tport,
            seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_tpcc_num_warehouses, FLAGS_tpcc_w_id, FLAGS_tpcc_C_c_id,
            FLAGS_tpcc_C_c_last, FLAGS_tpcc_new_order_ratio,
            FLAGS_tpcc_delivery_ratio, FLAGS_tpcc_payment_ratio,
            FLAGS_tpcc_order_status_ratio, FLAGS_tpcc_stock_level_ratio,
            FLAGS_static_w_id, FLAGS_abort_backoff,
            FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts);
        break;
      case BENCH_TPCC_SYNC:
        UW_ASSERT(syncClient != nullptr);
        bench = new tpcc::SyncTPCCClient(*syncClient, *tport,
            seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_tpcc_num_warehouses, FLAGS_tpcc_w_id, FLAGS_tpcc_C_c_id,
            FLAGS_tpcc_C_c_last, FLAGS_tpcc_new_order_ratio,
            FLAGS_tpcc_delivery_ratio, FLAGS_tpcc_payment_ratio,
            FLAGS_tpcc_order_status_ratio, FLAGS_tpcc_stock_level_ratio,
            FLAGS_static_w_id, FLAGS_abort_backoff,
            FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts, FLAGS_message_timeout);
        break;
      case BENCH_TPCC_SQL:
        UW_ASSERT(syncClient != nullptr);
        bench = new tpcc_sql::TPCCSQLClient(FLAGS_tpcc_run_sequential, *syncClient, *tport,
            seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_tpcc_num_warehouses, FLAGS_tpcc_w_id, FLAGS_tpcc_C_c_id,
            FLAGS_tpcc_C_c_last, FLAGS_tpcc_new_order_ratio,
            FLAGS_tpcc_delivery_ratio, FLAGS_tpcc_payment_ratio,
            FLAGS_tpcc_order_status_ratio, FLAGS_tpcc_stock_level_ratio,
            FLAGS_static_w_id, FLAGS_abort_backoff,
            FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts, FLAGS_message_timeout);
        break;
      case BENCH_SMALLBANK_SYNC:
        UW_ASSERT(syncClient != nullptr);
        bench = new smallbank::SmallbankClient(*syncClient, *tport,
            seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts,
            FLAGS_timeout, FLAGS_balance_ratio, FLAGS_deposit_checking_ratio,
            FLAGS_transact_saving_ratio, FLAGS_amalgamate_ratio,
            FLAGS_num_hotspots, FLAGS_num_customers - FLAGS_num_hotspots, FLAGS_hotspot_probability,
            FLAGS_customer_name_file_path);
        break;
      case BENCH_RW:
        UW_ASSERT(asyncClient != nullptr);
        bench = new rw::RWClient(keySelector, FLAGS_num_ops_txn, FLAGS_rw_read_only,
            *asyncClient, *tport, seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff,
            FLAGS_max_attempts);
        break;
      case BENCH_RW_SYNC:
        UW_ASSERT(syncClient != nullptr);
        bench = new rwsync::RWSyncClient(keySelector, FLAGS_num_ops_txn, FLAGS_rw_read_only,
            *syncClient, *tport, seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff,
            FLAGS_max_attempts, FLAGS_timeout);
        break;
      case BENCH_TOY:
        UW_ASSERT(syncClient != nullptr);
        bench = new toy::ToyClient(*syncClient, *tport,
            seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts,
            FLAGS_timeout);
        break;
      case BENCH_RW_SQL:
        UW_ASSERT(syncClient != nullptr);
        bench = new rwsql::RWSQLClient(FLAGS_num_ops_txn, querySelector, FLAGS_rw_read_only, FLAGS_rw_read_only_rate, FLAGS_rw_secondary_condition, 
                                      FLAGS_fixed_range, FLAGS_value_size, FLAGS_value_categories, FLAGS_scan_as_point, FLAGS_scan_as_point_parallel,
            *syncClient, *tport, seed,
            FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
            FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
            FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts,
            FLAGS_timeout);
        break;
      case BENCH_SEATS_SQL:
        {
          UW_ASSERT(syncClient != nullptr);
          std::string profile_file_path = std::filesystem::path(FLAGS_data_file_path).replace_filename(seats_sql::PROFILE_LOCATION);
          bench = new seats_sql::SEATSSQLClient( *syncClient, *tport, profile_file_path, FLAGS_benchbase_scale_factor,
              seed, FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
              FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
              FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts, FLAGS_message_timeout);
          break;
        }
      case BENCH_AUCTIONMARK_SQL:
        {
          UW_ASSERT(syncClient != nullptr);
          std::string profile_file_path = std::filesystem::path(FLAGS_data_file_path).replace_filename(auctionmark::PROFILE_FILE_NAME);
          bench = new auctionmark::AuctionMarkClient( *syncClient, *tport, profile_file_path, FLAGS_benchbase_scale_factor,
              clientId, client_total, FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
              FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
              FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts, FLAGS_message_timeout);
          break;
        }
      case BENCH_TPCCH_SQL: {
          UW_ASSERT(syncClient != nullptr);
          int id = FLAGS_num_client_hosts * i + FLAGS_client_id;
          int num_tpcch_threads = std::max(static_cast<int>(FLAGS_ch_client_proportion * client_total), 0);
          if (id < num_tpcch_threads) {
            std::mt19937 gen_tpcch(seed); //Seems to be unused.
            bench = new tpcch_sql::TPCCHSQLClient(*syncClient, *tport,
                seed, FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
                FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
                FLAGS_abort_backoff, FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts, FLAGS_message_timeout);
          } else {
            bench = new tpcc_sql::TPCCSQLClient(FLAGS_tpcc_run_sequential, *syncClient, *tport, seed,
                FLAGS_num_requests, FLAGS_exp_duration, FLAGS_delay,
                FLAGS_warmup_secs, FLAGS_cooldown_secs, FLAGS_tput_interval,
                FLAGS_tpcc_num_warehouses, FLAGS_tpcc_w_id, FLAGS_tpcc_C_c_id,
                FLAGS_tpcc_C_c_last, FLAGS_tpcc_new_order_ratio,
                FLAGS_tpcc_delivery_ratio, FLAGS_tpcc_payment_ratio,
                FLAGS_tpcc_order_status_ratio, FLAGS_tpcc_stock_level_ratio,
                FLAGS_static_w_id, FLAGS_abort_backoff,
                FLAGS_retry_aborted, FLAGS_max_backoff, FLAGS_max_attempts, FLAGS_message_timeout);
          }
          break;
      }
      default:
        NOT_REACHABLE();
    }

    switch (benchMode) {
      case BENCH_RETWIS:
      case BENCH_TPCC:
      case BENCH_RW:
        // async benchmarks
	      tport->Timer(0, [bench, bdcb]() { bench->Start(bdcb); });
        break;
      case BENCH_RW_SQL:
      case BENCH_RW_SYNC:
      case BENCH_SMALLBANK_SYNC:
      case BENCH_SEATS_SQL:
      case BENCH_AUCTIONMARK_SQL:
      case BENCH_TPCC_SQL:
      case BENCH_TPCCH_SQL:
      case BENCH_TPCC_SYNC: {
        SyncTransactionBenchClient *syncBench = dynamic_cast<SyncTransactionBenchClient *>(bench);
        UW_ASSERT(syncBench != nullptr);
        // for profiling sintr isolate one client as executing and other as validating only
        bool skip = false; // (mode == PROTO_SINTR) && (clientId > 0);
        threads.push_back(new std::thread([syncBench, bdcb, skip](){
            syncBench->Start([](){});
            while (!syncBench->IsFullyDone()) {
              syncBench->StartLatency();
              if (!skip) {
                transaction_status_t result;
                syncBench->SendNext(&result);
                syncBench->IncrementSent(result);
              }
              else {
                syncBench->IncrementSent(1);
              }
            }
            bdcb();
        }));
        break;
      }
      case BENCH_TOY: {
       SyncTransactionBenchClient *syncBench = dynamic_cast<SyncTransactionBenchClient *>(bench);
        toy::ToyClient *toyClient =  dynamic_cast<toy::ToyClient *>(syncBench);
        threads.push_back(new std::thread([toyClient, bdcb](){
          //Simply calls whatever toy code is declared in ExecuteToy.
          //Could extend toyClient interface to declare toy code as explicit transaction, and run transaction multiple times.
            toyClient->ExecuteToy();
            bdcb();
        }));
        break;
      }
      default:
        NOT_REACHABLE();
    }

    if (asyncClient != nullptr) {
      asyncClients.push_back(asyncClient);
    }
    if (syncClient != nullptr) {
      syncClients.push_back(syncClient);
    }
    if (client != nullptr) {
      clients.push_back(client);
    }
    if (oneShotClient != nullptr) {
      oneShotClients.push_back(oneShotClient);
    }
    benchClients.push_back(bench);
  }

  if (threads.size() > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }

  tport->Timer(FLAGS_exp_duration * 1000 - 1000, FlushStats);

  std::signal(SIGKILL, Cleanup); //signal 9
  std::signal(SIGTERM, Cleanup); //signal 15
  std::signal(SIGINT, Cleanup);

  CALLGRIND_START_INSTRUMENTATION;

  tport->Run();

  CALLGRIND_STOP_INSTRUMENTATION;
  CALLGRIND_DUMP_STATS;

  Cleanup(0);

	return 0;
}

void Cleanup(int signal) {
  Notice("clean up with signal %d", signal);
  FlushStats();
  delete config;
  //keyManager->Cleanup();
  delete keyManager; //somehow destructor is not being triggered
  delete keySelector;

  for (auto i : threads) {
    i->join();
    delete i;
  }
  for (auto i : syncClients) {
    delete i;
  }
  for (auto i : oneShotClients) {
    delete i;
  }
  for (auto i : asyncClients) {
    delete i;
  }
  for (auto i : clients) {
    delete i;
  }
  for (auto i : benchClients) {
    delete i;
  }
  tport->Stop();
  delete tport;
  delete part;

  if(FLAGS_sql_bench && querySelector != nullptr) delete querySelector;
  Notice("Finished Cleanup. Exiting");
  //exit(0); Allow segfault for duplicate config deletion to mask printing endless ASAN leaks that we can't fix...
}

void FlushStats() {
  if (FLAGS_stats_file.size() > 0) {
    Stats total;
    for (unsigned int i = 0; i < benchClients.size(); i++) {
      total.Merge(benchClients[i]->GetStats());
    }
    for (unsigned int i = 0; i < asyncClients.size(); i++) {
      total.Merge(asyncClients[i]->GetStats());
    }
    for (unsigned int i = 0; i < clients.size(); i++) {
      total.Merge(clients[i]->GetStats());
    }

    total.ExportJSON(FLAGS_stats_file);
  }
}
