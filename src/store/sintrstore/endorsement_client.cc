/***********************************************************************
 *
 * Copyright 2024 Austin Li <atl63@cornell.edu>
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

#include "store/sintrstore/endorsement_client.h"
#include "store/sintrstore/common.h"
#include "lib/message.h"

#include <algorithm>
#include <google/protobuf/util/message_differencer.h>

namespace sintrstore {

EndorsementClient::EndorsementClient(uint64_t client_id, KeyManager *keyManager, policy_id_function policyIdFunction) : 
    client_id(client_id), keyManager(keyManager), policyIdFunction(policyIdFunction) {
  policyClient = new PolicyClient();
}
EndorsementClient::~EndorsementClient() {
  delete policyClient;
  for (const auto &idPolicy : policyCache) {
    delete idPolicy.second;
  }
}

void EndorsementClient::SetClientSeqNum(uint64_t client_seq_num) {
  this->client_seq_num = client_seq_num;
}

const std::vector<proto::SignedMessage> &EndorsementClient::GetEndorsements() const {
  std::shared_lock lock(mutex);
  return endorsements;
}

void EndorsementClient::SetExpectedTxnOutput(const std::string &expectedTxnDigest) {
  std::unique_lock lock(mutex);
  this->expectedTxnDigest = expectedTxnDigest;
  // add self as an endorsement
  client_ids_received.insert(client_id);
  
  // now also check pendingEndorsements
  for (auto const &it : pendingEndorsements) {
    if (expectedTxnDigest == it.second.first) {
      client_ids_received.insert(it.first);
      endorsements.push_back(it.second.second);
    }
    else {
      Debug(
        "No match on pending endorsement from client id %lu, txn digest %s; expected txn digest %s",
        it.first,
        BytesToHex(it.second.first, 16).c_str(),
        BytesToHex(expectedTxnDigest, 16).c_str()
      );
    }
  }

  pendingEndorsements.clear();
}

void EndorsementClient::DebugSetExpectedTxnOutput(const proto::Transaction &expectedTxn) {
  this->expectedTxn = expectedTxn;

  for (auto const &txn : pendingTxns) {
    DebugCheck(txn);
  }
  pendingTxns.clear();
}

void EndorsementClient::DebugCheck(const proto::Transaction &txn) {
  if (!expectedTxn.IsInitialized()) {
    pendingTxns.push_back(txn);
    return;
  }
  Debug(
    "DebugCheck for EndorsementClient client id %lu, seq num %lu",
    expectedTxn.client_id(),
    expectedTxn.client_seq_num()
  );

  if (txn.client_id() != expectedTxn.client_id()) {
    Debug("client id mismatch: received %lu, expected %lu", txn.client_id(), expectedTxn.client_id());
  }

  if (txn.client_seq_num() != expectedTxn.client_seq_num()) {
    Debug("client seq num mismatch: received %lu, expected %lu", txn.client_seq_num(), expectedTxn.client_seq_num());
  }

  if (txn.involved_groups_size() != expectedTxn.involved_groups_size()) {
    Debug("involved groups mismatch: received size %d, expected size %d", txn.involved_groups_size(), expectedTxn.involved_groups_size());
  }
  for (int i = 0; i < expectedTxn.involved_groups_size(); i++) {
    if (txn.involved_groups(i) != expectedTxn.involved_groups(i)) {
      Debug("involved groups mismatch: received group %ld, expected group %ld", txn.involved_groups(i), expectedTxn.involved_groups(i));
    }
  }

  if (txn.read_set_size() != expectedTxn.read_set_size()) {
    Debug("read set mismatch: received size %d, expected size %d", txn.read_set_size(), expectedTxn.read_set_size());
  }
  for (int i = 0; i < expectedTxn.read_set_size(); i++) {
    if (!google::protobuf::util::MessageDifferencer::Equals(txn.read_set(i), expectedTxn.read_set(i))) {
      Debug(
        "read set mismatch: received key %s, ts %lu.%lu, expected key %s, ts %lu.%lu",
        txn.read_set(i).key().c_str(),
        txn.read_set(i).readtime().timestamp(),
        txn.read_set(i).readtime().id(),
        expectedTxn.read_set(i).key().c_str(),
        expectedTxn.read_set(i).readtime().timestamp(),
        expectedTxn.read_set(i).readtime().id()
      );
    }
  }

  if (txn.write_set_size() != expectedTxn.write_set_size()) {
    Debug("write set mismatch: received size %d, expected size %d", txn.write_set_size(), expectedTxn.write_set_size());
  }
  for (int i = 0; i < expectedTxn.write_set_size(); i++) {
    if (txn.write_set(i).key() != expectedTxn.write_set(i).key()) {
      Debug(
        "write set mismatch[%d]: received key %s, expected key %s",
        i,
        txn.write_set(i).key().c_str(),
        expectedTxn.write_set(i).key().c_str()
      );
    }
    if (txn.write_set(i).value() != expectedTxn.write_set(i).value()) {
      Debug(
        "write set mismatch[%d]: received value %s, expected value %s",
        i,
        BytesToHex(txn.write_set(i).value(), 16).c_str(),
        BytesToHex(expectedTxn.write_set(i).value(), 16).c_str()
      );
    }
    // if (!google::protobuf::util::MessageDifferencer::Equals(txn.write_set(i), expectedTxn.write_set(i))) {
    //   Debug(
    //     "write set mismatch: received %s, expected %s",
    //     txn.write_set(i).ShortDebugString().c_str(),
    //     expectedTxn.write_set(i).ShortDebugString().c_str()
    //   );
    // }
  }

  if (txn.deps_size() != expectedTxn.deps_size()) {
    Debug("dependencies mismatch: received size %d, expected size %d", txn.deps_size(), expectedTxn.deps_size());
  }
  for (int i = 0; i < expectedTxn.deps_size(); i++) {
    if (txn.deps(i).write().prepared_txn_digest() != expectedTxn.deps(i).write().prepared_txn_digest()) {
      Debug("dependencies mismatch: index %d", i);
    }
  }

  if (!google::protobuf::util::MessageDifferencer::Equals(txn.timestamp(), expectedTxn.timestamp())) {
    Debug(
      "timestamp mismatch: received %lu.%lu, expected %lu.%lu",
      txn.timestamp().timestamp(),
      txn.timestamp().id(),
      expectedTxn.timestamp().timestamp(),
      expectedTxn.timestamp().id()
    );
  }

  // query stuff
  if (txn.query_set_size() != expectedTxn.query_set_size()) {
    Debug("query set mismatch: received size %d, expected size %d", txn.query_set_size(), expectedTxn.query_set_size());
  }
  for (int i = 0; i < expectedTxn.query_set_size(); i++) {
    if (!google::protobuf::util::MessageDifferencer::Equals(txn.query_set(i), expectedTxn.query_set(i))) {
      Debug(
        "query set mismatch: received id %s, expected id %s",
        BytesToHex(txn.query_set(i).query_id(), 16).c_str(),
        BytesToHex(txn.query_set(i).query_id(), 16).c_str()
      );
      Debug(
        "query set mismatch: received %s, expected %s",
        txn.query_set(i).ShortDebugString().c_str(),
        expectedTxn.query_set(i).ShortDebugString().c_str()
      );
    }
  }

  if (txn.read_predicates_size() != expectedTxn.read_predicates_size()) {
    Debug("read predicates mismatch: received size %d, expected size %d", txn.read_predicates_size(), expectedTxn.read_predicates_size());
  }
  for (int i = 0; i < expectedTxn.read_predicates_size(); i++) {
    if (!google::protobuf::util::MessageDifferencer::Equals(txn.read_predicates(i), expectedTxn.read_predicates(i))) {
      Debug("read predicates mismatch: on index %d", i);
    }
  }

  // protobuf map has undefined order, so must sort first
  std::vector<std::pair<const std::string*, const TableWrite*>> tt;
  for (const auto &[table, table_write]: txn.table_writes()) {
    tt.emplace_back(&table, &table_write);
  }
  std::sort(tt.begin(), tt.end(), [](auto l, auto r){ return (*l.first) < (*r.first); });

  std::vector<std::pair<const std::string*, const TableWrite*>> expectedTt;
  for (const auto &[table, table_write]: expectedTxn.table_writes()) {
    expectedTt.emplace_back(&table, &table_write);
  }
  std::sort(expectedTt.begin(), expectedTt.end(), [](auto l, auto r){ return (*l.first) < (*r.first); });

  if (tt.size() != expectedTt.size()) {
    Debug("table writes mismatch: received size %d, expected size %d", tt.size(), expectedTt.size());
  }
  for (int i = 0; i < expectedTt.size(); i++) {
    if (*tt[i].first != *expectedTt[i].first) {
      Debug(
        "table writes mismatch: received table %s, expected table %s",
        (*tt[i].first).c_str(),
        (*expectedTt[i].first).c_str()
      );
    }
    if (tt[i].second->rows_size() != expectedTt[i].second->rows_size()) {
      Debug(
        "table writes mismatch: received rows size %d, expected rows size %d",
        tt[i].second->rows_size(),
        expectedTt[i].second->rows_size()
      );
    }

    std::vector<const RowUpdates*> rows;
    for (int j = 0; j < tt[i].second->rows_size(); j++) {
      rows.push_back(&tt[i].second->rows(j));
    }

    std::vector<const RowUpdates*> expectedRows;
    for (int j = 0; j < expectedTt[i].second->rows_size(); j++) {
      expectedRows.push_back(&expectedTt[i].second->rows(j));
    }

    for (int j = 0; j < expectedRows.size(); j++) {
      if (rows[j]->has_deletion() != expectedRows[j]->has_deletion()) {
        Debug(
          "table writes mismatch: received deletion %d, expected deletion %d",
          rows[j]->has_deletion(),
          expectedRows[j]->has_deletion()
        );
      }
      if (rows[j]->column_values_size() != expectedRows[j]->column_values_size()) {
        Debug(
          "table writes mismatch: received column values size %d, expected column values size %d",
          rows[j]->column_values_size(),
          expectedRows[j]->column_values_size()
        );
      }
      for (int k = 0; k < expectedRows[j]->column_values_size(); k++) {
        if (rows[j]->column_values(k) != expectedRows[j]->column_values(k)) {
          Debug(
            "table writes mismatch[%d][%d][%d]: received column value %s, expected column value %s",
            i, j, k,
            rows[j]->column_values(k).c_str(),
            expectedRows[j]->column_values(k).c_str()
          );
        }
      }
    }
  }
}

void EndorsementClient::UpdateRequirement(const Policy *policy) {
  UW_ASSERT(policy != nullptr);
  policyClient->AddPolicy(policy);
}

std::vector<int> EndorsementClient::DifferenceToSatisfied(const std::set<uint64_t> &potentialEndorsements) const {
  return policyClient->DifferenceToSatisfied(potentialEndorsements);
}

void EndorsementClient::AddValidation(const uint64_t peer_client_id, const std::string &valTxnDigest,
    const proto::SignedMessage &signedValTxnDigest) {
  std::unique_lock lock(mutex);
  // if new peer
  if (client_ids_received.find(peer_client_id) == client_ids_received.end()) {
    if (expectedTxnDigest.length() > 0) {
      // must match expected digest
      if (valTxnDigest == expectedTxnDigest) {
        client_ids_received.insert(peer_client_id);
        endorsements.push_back(signedValTxnDigest);
      }
      else {
        Debug(
          "No match on endorsement from client id %lu, txn digest %s; expected txn digest %s",
          peer_client_id,
          BytesToHex(valTxnDigest, 16).c_str(),
          BytesToHex(expectedTxnDigest, 16).c_str()
        );
      }
    }
    else {
      // possible for expected digest to be uninitialized, in which case record a pending endorsement
      pendingEndorsements[peer_client_id] = std::make_pair(valTxnDigest, signedValTxnDigest);
      Debug("No expectedTxnDigest yet");
    }
  }
}

bool EndorsementClient::IsSatisfied() const {
  std::shared_lock lock(mutex);
  bool satisfied = policyClient->IsSatisfied(client_ids_received);
  if (!satisfied) {
    // Debug("policy not satisfied, received %lu endorsements", client_ids_received.size());
  }
  return satisfied;
}

void EndorsementClient::Reset() {
  expectedTxnDigest.clear();
  expectedTxn.Clear();
  policyClient->Reset();
  client_ids_received.clear();
  endorsements.clear();
  pendingEndorsements.clear();
}

bool EndorsementClient::GetPolicyFromCache(const std::string &key, const Policy *&policy) const {
  uint64_t policyId = policyIdFunction(key, "");
  auto it = policyCache.find(policyId);
  if (it == policyCache.end()) {
    Panic("Policy cache is missing policy with id %lu", policyId);
  }

  policy = it->second;
  return true;
}

bool EndorsementClient::GetPolicyFromCache(uint64_t policyId, const Policy *&policy) const {
  auto it = policyCache.find(policyId);
  if (it == policyCache.end()) {
    Panic("Policy cache is missing policy with id %lu", policyId);
  }

  policy = it->second;
  return true;
}

void EndorsementClient::UpdatePolicyCache(uint64_t policyId, Policy *policy) {
  UW_ASSERT(policy != nullptr);
  if (policyCache.find(policyId) != policyCache.end()) {
    delete policyCache[policyId];
  }
  policyCache[policyId] = policy;
}

void EndorsementClient::InitializePolicyCache(const std::map<uint64_t, Policy *> &policies) {
  UW_ASSERT(this->policyCache.empty());
  for (const auto &p : policies) {
    policyCache[p.first] = p.second;
  }
}

} // namespace sintrstore
