/***********************************************************************
 *
 * Copyright 2025 Daniel Lee <dhl93@cornell.edu>
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
#ifndef VALIDATION_SQL_PAYMENT_H
#define VALIDATION_SQL_PAYMENT_H

#include "store/benchmark/async/sql/tpcc/payment.h"
#include "store/benchmark/async/sql/tpcc/validation/tpcc_transaction.h"
#include "store/benchmark/async/sql/tpcc/tpcc-sql-validation-proto.pb.h"
#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcc_sql {

class ValidationSQLPayment : public ValidationTPCCSQLTransaction, public SQLPayment {
 public:
  // even though gen is not needed for Validate, need it for calling SQLPayment constructor
  ValidationSQLPayment(uint32_t timeout, std::mt19937 &gen, const validation::proto::Payment &valPaymentMsg);
  virtual ~ValidationSQLPayment();
  virtual transaction_status_t Validate(SyncClient &client);
};

class ValidationSQLPaymentSequential : public ValidationTPCCSQLTransaction, public SQLPaymentSequential {
 public:
  ValidationSQLPaymentSequential(uint32_t timeout, std::mt19937 &gen, const validation::proto::Payment &valPaymentMsg);
  virtual ~ValidationSQLPaymentSequential();
  virtual transaction_status_t Validate(SyncClient &client);
};

} // namespace tpcc_sql

#endif /* VALIDATION_SQL_PAYMENT_H */
