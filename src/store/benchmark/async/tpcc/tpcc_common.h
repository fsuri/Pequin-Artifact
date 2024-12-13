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
#ifndef TPCC_COMMON_H
#define TPCC_COMMON_H

#include "lib/message.h"
#include "store/benchmark/async/tpcc/tpcc_client.h"

#include <string>

namespace tpcc {

const std::string BENCHMARK_NAME = "tpcc";

inline std::string GetBenchmarkTxnTypeName(TPCCTransactionType txn_type) {
  switch (txn_type) {
    case TXN_DELIVERY:
      return "delivery";
    case TXN_NEW_ORDER:
      return "new_order";
    case TXN_ORDER_STATUS:
      return "order_status";
    case TXN_PAYMENT:
      return "payment";
    case TXN_STOCK_LEVEL:
      return "stock_level";
    default:
      Panic("Received unexpected txn type: %d", txn_type);
  }
}

inline TPCCTransactionType GetBenchmarkTxnTypeEnum(std::string &txn_type) {
  if (txn_type == "delivery") {
    return TXN_DELIVERY;
  }
  else if (txn_type == "new_order") {
    return TXN_NEW_ORDER;
  }
  else if (txn_type == "order_status") {
    return TXN_ORDER_STATUS;
  }
  else if (txn_type == "payment") {
    return TXN_PAYMENT;
  }
  else if (txn_type == "stock_level") {
    return TXN_STOCK_LEVEL;
  }
  else {
    Panic("Received unexpected txn type: %s", txn_type.c_str());
  }
}

}

#endif /* TPCC_COMMON_H */
