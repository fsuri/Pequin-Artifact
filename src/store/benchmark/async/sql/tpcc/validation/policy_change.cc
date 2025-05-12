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

#include "store/benchmark/async/sql/tpcc/validation/policy_change.h"
#include "store/benchmark/async/sql/tpcc/tpcc_utils.h"
#include "store/sintrstore/sintr-proto.pb.h"
#include "store/sintrstore/policy/policy-proto.pb.h"


namespace tpcc_sql {

ValidationSQLPolicyChange::ValidationSQLPolicyChange(uint32_t timeout, uint32_t w_id) :
  ValidationTPCCSQLTransaction(timeout) {
  this->w_id = w_id;
}

ValidationSQLPolicyChange::ValidationSQLPolicyChange(uint32_t timeout, const validation::proto::PolicyChange &valPolicyChangeMsg) : 
  ValidationTPCCSQLTransaction(timeout) {
  w_id = valPolicyChangeMsg.w_id();
  randWeight = valPolicyChangeMsg.random_weight();
}

ValidationSQLPolicyChange::~ValidationSQLPolicyChange() {
}

transaction_status_t ValidationSQLPolicyChange::Validate(::SyncClient &client) {
  Debug("POLICY_CHANGE");
  Debug("Warehouse: %u", w_id);

  client.Begin(timeout);

  // distict table has policy id 1, change it to be policy of weight 3
  ::sintrstore::proto::PolicyObject policy;
  policy.set_policy_type(::sintrstore::proto::PolicyObject::WEIGHT_POLICY);
  ::sintrstore::proto::WeightPolicyMessage weight_policy;
  weight_policy.set_weight(randWeight);
  weight_policy.SerializeToString(policy.mutable_policy_data());
  
  std::string policy_str;
  policy.SerializeToString(&policy_str);
  client.Put("p0", policy_str, timeout);

  return client.Commit(timeout);
}

} // namespace tpcc
