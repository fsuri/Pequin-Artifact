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
#include "store/benchmark/async/sql/tpcc/tpcc_client.h"

#include <random>

#include "store/benchmark/async/sql/tpcc/new_order.h"
#include "store/benchmark/async/sql/tpcc/payment.h"
#include "store/benchmark/async/sql/tpcc/order_status.h"
#include "store/benchmark/async/sql/tpcc/stock_level.h"
#include "store/benchmark/async/sql/tpcc/delivery.h"

namespace tpcc_sql {

TPCCSQLClient::TPCCSQLClient(SyncClient &client, Transport &transport,
    uint64_t seed, int numRequests, int expDuration, uint64_t delay, int warmupSec,
    int cooldownSec, int tputInterval,  uint32_t num_warehouses, uint32_t w_id,
    uint32_t C_c_id, uint32_t C_c_last, uint32_t new_order_ratio,
    uint32_t delivery_ratio, uint32_t payment_ratio, uint32_t order_status_ratio,
    uint32_t stock_level_ratio, bool static_w_id,
    uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts, uint32_t timeout,
    const std::string &latencyFilename) :
      SyncTransactionBenchClient(client, transport, seed, numRequests,
        expDuration, delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
        retryAborted, maxBackoff, maxAttempts, timeout, latencyFilename), 
      num_warehouses(num_warehouses), w_id(w_id), C_c_id(C_c_id),
      C_c_last(C_c_last), new_order_ratio(new_order_ratio),
      delivery_ratio(delivery_ratio), payment_ratio(payment_ratio),
      order_status_ratio(order_status_ratio), stock_level_ratio(stock_level_ratio),
      static_w_id(static_w_id), delivery(false) {
  stockLevelDId = std::uniform_int_distribution<uint32_t>(1, 10)(GetRand());
}

TPCCSQLClient::~TPCCSQLClient() {
}

SyncTransaction* TPCCSQLClient::GetNextTransaction() {
  uint32_t wid, did;
  std::mt19937 gen = GetRand();
  if (delivery && deliveryDId < 10) {
    deliveryDId++;
    wid = deliveryWId;
    did = deliveryDId;
    lastOp = "delivery";
    return new SQLDelivery(GetTimeout(), wid, did, GetRand());
  } else {
    delivery = false;
  }

  uint32_t total = new_order_ratio + delivery_ratio + payment_ratio
      + order_status_ratio + stock_level_ratio;
  uint32_t ttype = std::uniform_int_distribution<uint32_t>(0, total - 1)(gen);
  uint32_t freq = 0;

  if (static_w_id) {
    wid = w_id;
  } else {
    wid = std::uniform_int_distribution<uint32_t>(1, num_warehouses)(gen);
  }
  if (ttype < (freq = new_order_ratio)) {
    lastOp = "new_order";
    return new SQLNewOrder(GetTimeout(), wid, C_c_id, num_warehouses, GetRand());
  } else if (ttype < (freq += new_order_ratio)) {
    lastOp = "payment";
    return new SQLPayment(GetTimeout(), wid, C_c_last, C_c_id, num_warehouses, GetRand());
  } else if (ttype < (freq += new_order_ratio)) {
    lastOp = "order_status";
    return new SQLOrderStatus(GetTimeout(), wid, C_c_last, C_c_id, GetRand());
  } else if (ttype < (freq += new_order_ratio)) {
    if (static_w_id) {
      did = stockLevelDId;
    } else {
      did = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
    }
    lastOp = "stock_level";
    return new SQLStockLevel(GetTimeout(), wid, did, GetRand());
  } else {
    deliveryDId = 1;
    deliveryWId = wid;
    did = deliveryDId;
    delivery = true;
    lastOp = "delivery";
    return new SQLDelivery(GetTimeout(), wid, did, GetRand());
  }
}

std::string TPCCSQLClient::GetLastOp() const {
  return lastOp;
}


} //namespace tpcc_sql
