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

#ifndef _SINTR_ENDORSEMENT_CLIENT_H_
#define _SINTR_ENDORSEMENT_CLIENT_H_

#include "store/sintrstore/policy/policy_client.h"
#include "store/sintrstore/sintr-proto.pb.h"
#include "lib/keymanager.h"

#include <vector>
#include <set>
#include <map>

namespace sintrstore {

// this class keeps state for an ongoing transaction endorsement
class EndorsementClient {
 public:
  EndorsementClient(uint64_t client_id, KeyManager *keyManager);
  ~EndorsementClient();

  std::vector<proto::SignedMessage> GetEndorsements();
  void SetClientSeqNum(uint64_t client_seq_num);
  void SetExpectedTxnOutput(const std::string &expectedTxnDigest);
  void DebugSetExpectedTxnOutput(const proto::Transaction &expectedTxn);
  void DebugCheck(const proto::Transaction &txn);
  // update current policy by merging with passed in policy
  void UpdateRequirement(const Policy *policy);
  // what additional client ids are needed so that this policy is satisfied by potentialEndorsements
  // if potentialEndorsements is good enough, return empty vector
  std::vector<int> DifferenceToSatisfied(const std::set<int> &potentialEndorsements);
  void AddValidation(const uint64_t peer_client_id, const std::string &valTxnDigest, 
    const proto::SignedMessage &signedValTxnDigest);
  // check if the policy is satisfied by actual endorsements collected so far
  bool IsSatisfied();
  void Reset();

  // return true if policy exists for key, false otherwise
  bool GetPolicyFromCache(const std::string &key, Policy **policy);
  bool GetPolicyFromCache(uint64_t policyId, Policy **policy);
  void UpdateKeyPolicyIdCache(const std::string &key, uint64_t policyId);
  void UpdatePolicyCache(uint64_t policyId, const Policy *policy);
  void InitializePolicyCache(const std::map<uint64_t, Policy *> &policyCache);

 private:
  // this client information
  const uint64_t client_id;
  KeyManager *keyManager;

  // client side cache of policy store
  std::map<std::string, uint64_t> keyPolicyIdCache;
  std::map<uint64_t, Policy *> policyCache;
  
  // transaction specific
  uint64_t client_seq_num;
  // expected validation transaction digest
  std::string expectedTxnDigest;
  // debug by checking entire validation txn
  proto::Transaction expectedTxn;
  // policy client tracks the policy which must be satisfied
  PolicyClient *policyClient;
  // which peer clients have endorsed
  std::set<uint64_t> client_ids_received;
  // confirmed endorsement signatures to send to server
  std::vector<proto::SignedMessage> endorsements;
  // also maintain pending endorsements if endorsement comes back before expectedValTxnDigest is set
  // map from client id to (digest, signed message)
  std::map<uint64_t, std::pair<std::string, proto::SignedMessage>> pendingEndorsements;
  // debug pending transactions
  std::vector<proto::Transaction> pendingTxns;
};

} // namespace sintrstore

#endif /* _SINTR_ENDORSEMENT_CLIENT_H_ */
