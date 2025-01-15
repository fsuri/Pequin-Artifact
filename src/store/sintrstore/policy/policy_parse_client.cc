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

#include "store/sintrstore/policy/policy_parse_client.h"
#include "store/sintrstore/policy/policy-proto.pb.h"
#include "store/sintrstore/policy/weight_policy.h"
#include "store/sintrstore/policy/acl_policy.h"
#include "lib/message.h"

#include <fstream>
#include <sstream>

namespace sintrstore {

std::map<uint64_t, Policy *> PolicyParseClient::ParseConfigFile(const std::string &configFilePath) {
  std::map<uint64_t, Policy *> policies;

  std::ifstream policyStoreFile(configFilePath);
  if (policyStoreFile.fail()) {
    Panic("Cannot open policy store file %s", configFilePath.c_str());
  }

  std::string line;
  while (std::getline(policyStoreFile, line)) {
    // expected format is "policyId policyType args..."
    uint64_t policyId;
    std::string policyType;
    std::vector<std::string> args;

    // parse line
    std::istringstream iss(line);
    std::string temp;
    int i = 0;
    while (std::getline(iss, temp, ' ')) {
      if (i == 0) {
        policyId = std::stoull(temp);
      } else if (i == 1) {
        policyType = temp;
      } else {
        args.push_back(temp);
      }
      i++;
    }

    // create policy
    Policy *policy = Create(policyType, args);

    // add to policies
    auto result = policies.insert(std::make_pair(policyId, policy));
    if (!result.second) {
      Panic("Policy id %lu occurs twice in config file", policyId);
    }
  }

  return policies;
}

Policy *PolicyParseClient::Create(const std::string &policyType, const std::vector<std::string> &policyArgs) {
  if (policyType == "weight") {
    if (policyArgs.size() != 1) {
      Panic("Weight policy requires exactly one argument");
    }
    return new WeightPolicy(std::stoull(policyArgs[0]));
  } else if (policyType == "acl") {
    std::set<uint64_t> access_control_list;
    for (const std::string &arg : policyArgs) {
      access_control_list.insert(std::stoull(arg));
    }
    return new ACLPolicy(access_control_list);
  } else {
    Panic("Received unexpected policy type: %s", policyType.c_str());
  }
}

Policy *PolicyParseClient::Parse(const proto::EndorsementPolicyMessage &endorsePolicyMsg) {
  switch (endorsePolicyMsg.policy_type()) {
    case proto::EndorsementPolicyMessage::WEIGHT_POLICY: {
      proto::WeightPolicyMessage weightPolicyMsg;
      weightPolicyMsg.ParseFromString(endorsePolicyMsg.policy_data());
      return new WeightPolicy(weightPolicyMsg.weight());
    }
    case proto::EndorsementPolicyMessage::ACL_POLICY: {
      proto::ACLPolicyMessage aclPolicyMsg;
      aclPolicyMsg.ParseFromString(endorsePolicyMsg.policy_data());
      std::set<uint64_t> access_control_list(aclPolicyMsg.access_control_list().begin(), aclPolicyMsg.access_control_list().end());
      return new ACLPolicy(access_control_list);
    }
    default:
      Panic("Received unexpected policy type: %d", endorsePolicyMsg.policy_type());
  }
}

} // namespace sintrstore
