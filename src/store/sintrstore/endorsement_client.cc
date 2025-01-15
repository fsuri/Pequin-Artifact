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

EndorsementClient::EndorsementClient(uint64_t client_id, KeyManager *keyManager) : 
    client_id(client_id), keyManager(keyManager) {
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

std::vector<proto::SignedMessage> EndorsementClient::GetEndorsements() {
  return endorsements;
}

void EndorsementClient::SetExpectedTxnOutput(const std::string &expectedTxnDigest) {
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
        BytesToHex(txn.read_set(i).key(), 16).c_str(),
        txn.read_set(i).readtime().timestamp(),
        txn.read_set(i).readtime().id(),
        BytesToHex(expectedTxn.read_set(i).key(), 16).c_str(),
        expectedTxn.read_set(i).readtime().timestamp(),
        expectedTxn.read_set(i).readtime().id()
      );
    }
  }

  if (txn.write_set_size() != expectedTxn.write_set_size()) {
    Debug("write set mismatch: received size %d, expected size %d", txn.write_set_size(), expectedTxn.write_set_size());
  }
  for (int i = 0; i < expectedTxn.write_set_size(); i++) {
    if (!google::protobuf::util::MessageDifferencer::Equals(txn.write_set(i), expectedTxn.write_set(i))) {
      Debug(
        "write set mismatch: received key %s, value %s, expected key %s, value %s",
        BytesToHex(txn.write_set(i).key(), 16).c_str(),
        BytesToHex(txn.write_set(i).value(), 16).c_str(),
        BytesToHex(expectedTxn.write_set(i).key(), 16).c_str(),
        BytesToHex(expectedTxn.write_set(i).value(), 16).c_str()
      );
    }
  }

  if (txn.deps_size() != expectedTxn.deps_size()) {
    Debug("dependencies mismatch: received size %d, expected size %d", txn.deps_size(), expectedTxn.deps_size());
  }
  for (int i = 0; i < expectedTxn.deps_size(); i++) {
    if (!google::protobuf::util::MessageDifferencer::Equals(txn.deps(i), expectedTxn.deps(i))) {
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
}

std::vector<int> EndorsementClient::UpdateRequirement(const Policy *policy) {
  UW_ASSERT(policy != nullptr);
  std::vector<int> diff = policyClient->DifferenceToPolicy(policy);
  policyClient->AddPolicy(policy);
  return diff;
}

void EndorsementClient::AddValidation(const uint64_t peer_client_id, const std::string &valTxnDigest,
    const proto::SignedMessage &signedValTxnDigest) {
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

bool EndorsementClient::IsSatisfied() {
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

bool EndorsementClient::GetPolicyFromCache(const std::string &key, Policy **policy) {
  // it is possible that the key is not in the cache
  // but if the key is, then the policyId should be in the cache
  auto it = keyPolicyIdCache.find(key);
  if (it == keyPolicyIdCache.end()) {
    return false;
  }
  uint64_t policyId = it->second;
  auto it2 = policyCache.find(policyId);
  if (it2 == policyCache.end()) {
    Panic("Policy cache is missing policy with id %lu", policyId);
  }

  *policy = it2->second->Clone();
  return true;
}

bool EndorsementClient::GetPolicyFromCache(uint64_t policyId, Policy **policy) {
  auto it = policyCache.find(policyId);
  if (it == policyCache.end()) {
    Panic("Policy cache is missing policy with id %lu", policyId);
  }

  *policy = it->second->Clone();
  return true;
}

void EndorsementClient::UpdateKeyPolicyIdCache(const std::string &key, uint64_t policyId) {
  keyPolicyIdCache[key] = policyId;
}

void EndorsementClient::UpdatePolicyCache(uint64_t policyId, const Policy *policy) {
  UW_ASSERT(policy != nullptr);
  if (policyCache.find(policyId) != policyCache.end()) {
    delete policyCache[policyId];
  }
  policyCache[policyId] = policy->Clone();
}

void EndorsementClient::InitializePolicyCache(const std::map<uint64_t, Policy *> &policies) {
  UW_ASSERT(this->policyCache.empty());
  for (const auto &p : policies) {
    policyCache[p.first] = std::move(p.second);
  }
}

} // namespace sintrstore
