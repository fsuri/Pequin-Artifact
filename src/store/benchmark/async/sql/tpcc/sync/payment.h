/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
 *                Liam Arzola <lma77@cornell.edu>
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
#ifndef SYNC_SQL_PAYMENT_H
#define SYNC_SQL_PAYMENT_H

#include "store/benchmark/async/sql/tpcc/sync/tpcc_transaction.h"
#include "store/benchmark/async/sql/tpcc/payment.h"
#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcc_sql {

class SyncSQLPayment : public SyncTPCCSQLTransaction, public SQLPayment {
 public:
  SyncSQLPayment(uint32_t timeout, uint32_t w_id, uint32_t c_c_last,
      uint32_t c_c_id, uint32_t num_warehouses, std::mt19937 &gen);
  virtual ~SyncSQLPayment();
  virtual transaction_status_t Execute(SyncClient &client);
  virtual std::vector<TPCC_Table> HeuristicFunction() override;
  virtual void SerializeTxnState(std::string &txnState) override;
};

class SyncSQLPaymentSequential : public SyncTPCCSQLTransaction, public SQLPaymentSequential {
 public:
  SyncSQLPaymentSequential(uint32_t timeout, uint32_t w_id, uint32_t c_c_last,
      uint32_t c_c_id, uint32_t num_warehouses, std::mt19937 &gen);
  virtual ~SyncSQLPaymentSequential();
  virtual transaction_status_t Execute(SyncClient &client);
  virtual std::vector<TPCC_Table> HeuristicFunction() override;
  virtual void SerializeTxnState(std::string &txnState) override;
};

} // namespace tpcc_sql

#endif /* SYNC_SQL_PAYMENT_H */
