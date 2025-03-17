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
#ifndef TPCC_SQL_COMMON_H
#define TPCC_SQL_COMMON_H

#include "lib/message.h"
#include "store/benchmark/async/sql/tpcc/tpcc_client.h"

#include <string>

namespace tpcc_sql {

const std::string BENCHMARK_NAME = "tpcc-sql";

inline std::string GetBenchmarkTxnTypeName(SQLTPCCTransactionType txn_type) {
  switch (txn_type) {
    case SQL_TXN_DELIVERY:
      return "delivery";
    case SQL_TXN_NEW_ORDER:
      return "new_order";
    case SQL_TXN_ORDER_STATUS:
      return "order_status";
    case SQL_TXN_PAYMENT:
      return "payment";
    case SQL_TXN_STOCK_LEVEL:
      return "stock_level";
    case SQL_TXN_POLICY_CHANGE:
      return "policy_change";
    case SQL_TXN_DELIVERY_SEQUENTIAL:
      return "policy_change";
    case SQL_TXN_NEW_ORDER_SEQUENTIAL:
      return "policy_change";
    case SQL_TXN_PAYMENT_SEQUENTIAL:
      return "policy_change";
    default:
      Panic("Received unexpected txn type: %d", txn_type);
  }
}

inline SQLTPCCTransactionType GetBenchmarkTxnTypeEnum(std::string &txn_type) {
  if (txn_type == "delivery") {
    return SQL_TXN_DELIVERY;
  }
  else if (txn_type == "new_order") {
    return SQL_TXN_NEW_ORDER;
  }
  else if (txn_type == "order_status") {
    return SQL_TXN_ORDER_STATUS;
  }
  else if (txn_type == "payment") {
    return SQL_TXN_PAYMENT;
  }
  else if (txn_type == "stock_level") {
    return SQL_TXN_STOCK_LEVEL;
  }
  else if (txn_type == "policy_change") {
    return SQL_TXN_POLICY_CHANGE;
  }
  else if(txn_type == "delivery_sequential") {
    return SQL_TXN_DELIVERY_SEQUENTIAL;
  }
  else if(txn_type == "new_order_sequential") {
    return SQL_TXN_NEW_ORDER_SEQUENTIAL;
  }
  else if(txn_type == "payment_sequential") {
    return SQL_TXN_PAYMENT_SEQUENTIAL;
  }
  else {
    Panic("Received unexpected txn type: %s", txn_type.c_str());
  }
}

}

#endif /* TPCC_SQL_COMMON_H */
