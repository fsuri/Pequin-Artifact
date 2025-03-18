/***********************************************************************
 *
 * Copyright 2025 Austin Li <atl63@cornell.edu>
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

#ifndef SQL_TPCC_VALDIATION_POLICY_CHANGE_H
#define SQL_TPCC_VALDIATION_POLICY_CHANGE_H

#include "store/benchmark/async/sql/tpcc/policy_change.h"
#include "store/benchmark/async/sql/tpcc/validation/tpcc_transaction.h"
#include "store/benchmark/async/sql/tpcc/tpcc-sql-validation-proto.pb.h"
#include "store/common/frontend/sync_client.h"


namespace tpcc_sql {

// TODO: maybe have only one transaction type for policy change

class ValidationSQLPolicyChange : public ValidationTPCCSQLTransaction, public PolicyChange {
 public:
  // constructor with no randomness (all fields directly initialized)
  ValidationSQLPolicyChange(uint32_t timeout, uint32_t w_id);
  ValidationSQLPolicyChange(uint32_t timeout, const validation::proto::PolicyChange &valPolicyChangeMsg);
  virtual ~ValidationSQLPolicyChange();
  virtual transaction_status_t Validate(::SyncClient &client);
};

} // namespace tpcc

#endif /* SQL_TPCC_VALDIATION_POLICY_CHANGE_H */
