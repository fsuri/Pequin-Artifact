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

namespace sintrstore {

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
