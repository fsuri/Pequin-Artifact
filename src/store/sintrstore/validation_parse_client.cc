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

#include "store/sintrstore/validation_parse_client.h"
#include "store/benchmark/async/tpcc/validation/tpcc_transaction.h"
#include "store/benchmark/async/tpcc/validation/delivery.h"
#include "store/benchmark/async/tpcc/validation/new_order.h"
#include "store/benchmark/async/tpcc/validation/order_status.h"
#include "store/benchmark/async/tpcc/validation/payment.h"
#include "store/benchmark/async/tpcc/validation/stock_level.h"
#include "store/benchmark/async/tpcc/tpcc-validation-proto.pb.h"
#include "store/benchmark/async/tpcc/tpcc_common.h"


namespace sintrstore {
  
ValidationTransaction *ValidationParseClient::Parse(const TxnState& txnState) {
  std::string txn_name(txnState.txn_name());
  
  size_t pos = txn_name.find("_");
  if (pos == std::string::npos) {
    Panic("Received unexpected txn name: %s", txn_name.c_str());
  }

  std::string txn_bench = txn_name.substr(0, pos);
  std::string txn_type = txn_name.substr(pos+1);

  if (txn_bench == ::tpcc::BENCHMARK_NAME) {
    ::tpcc::TPCCTransactionType tpcc_txn_type = ::tpcc::GetBenchmarkTxnTypeEnum(txn_type);
    switch (tpcc_txn_type) {
      case ::tpcc::TXN_DELIVERY: {
        ::tpcc::validation::proto::Delivery valTxnData = ::tpcc::validation::proto::Delivery();
        valTxnData.ParseFromString(txnState.txn_data());
        return new ::tpcc::ValidationDelivery(timeout, valTxnData);
      }
      case ::tpcc::TXN_NEW_ORDER: {
        ::tpcc::validation::proto::NewOrder valTxnData = ::tpcc::validation::proto::NewOrder();
        valTxnData.ParseFromString(txnState.txn_data());
        return new ::tpcc::ValidationNewOrder(timeout, valTxnData);
      }
      case ::tpcc::TXN_ORDER_STATUS: {
        ::tpcc::validation::proto::OrderStatus valTxnData = ::tpcc::validation::proto::OrderStatus();
        valTxnData.ParseFromString(txnState.txn_data());
        return new ::tpcc::ValidationOrderStatus(timeout, valTxnData);
      }
      case ::tpcc::TXN_PAYMENT: {
        ::tpcc::validation::proto::Payment valTxnData = ::tpcc::validation::proto::Payment();
        valTxnData.ParseFromString(txnState.txn_data());
        return new ::tpcc::ValidationPayment(timeout, valTxnData);
      }
      case ::tpcc::TXN_STOCK_LEVEL: {
        ::tpcc::validation::proto::StockLevel valTxnData = ::tpcc::validation::proto::StockLevel();
        valTxnData.ParseFromString(txnState.txn_data());
        return new ::tpcc::ValidationStockLevel(timeout, valTxnData);
      }
      default:
        Panic("Received unexpected txn type: %s", txn_type.c_str());
    }
  }
  else {
    Panic("Received unexpected txn benchmark: %s", txn_bench.c_str());
  }
};

} // namespace sintrstore
