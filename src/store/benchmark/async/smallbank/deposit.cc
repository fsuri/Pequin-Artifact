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
#include "store/benchmark/async/smallbank/deposit.h"

#include "store/benchmark/async/smallbank/smallbank_transaction.h"
#include "store/benchmark/async/smallbank/utils.h"

namespace smallbank {

DepositChecking::DepositChecking(const std::string &cust, const int32_t value,
                                 const uint32_t timeout)
    : SmallbankTransaction(DEPOSIT),
      cust(cust),
      value(value),
      timeout(timeout) {}
      
DepositChecking::~DepositChecking() {}

transaction_status_t DepositChecking::Execute(SyncClient &client) {
  Debug("DepositChecking for name %s with val %d", cust.c_str(), value);
  if (value < 0) {
    client.Abort(timeout);
    Debug("Aborted DepositChecking (- val)");
    return ABORTED_USER;
  }
  proto::AccountRow accountRow;
  proto::CheckingRow checkingRow;

  client.Begin(timeout);
  if (!ReadAccountRow(client, cust, accountRow, timeout)) {
    client.Abort(timeout);
    Debug("Aborted DepositChecking (AccountRow)");
    return ABORTED_USER;
  }
  const uint32_t customerId = accountRow.customer_id();
  if (!ReadCheckingRow(client, customerId, checkingRow, timeout)) {
    client.Abort(timeout);
    Debug("Aborted DepositChecking (CheckingRow)");
    return ABORTED_USER;
  }
  Debug("DepositChecking old value %d", checkingRow.checking_balance());
  InsertCheckingRow(client, customerId, checkingRow.checking_balance() + value,
                    timeout);
  return client.Commit(timeout);
}

}  // namespace smallbank
