#include "store/benchmark/async/sql/tpcch/tpcch_client.h"

#include <random>

#include "store/benchmark/async/sql/tpcc/new_order.h"
#include "store/benchmark/async/sql/tpcc/payment.h"
#include "store/benchmark/async/sql/tpcc/order_status.h"
#include "store/benchmark/async/sql/tpcc/stock_level.h"
#include "store/benchmark/async/sql/tpcc/delivery.h"

namespace tpcch_sql {

TPCCHSQLClient::TPCCHSQLClient(SyncClient &client, Transport &transport,
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

TPCCHSQLClient::~TPCCHSQLClient() {
}

SyncTransaction* TPCCHSQLClient::GetNextTransaction() {
  uint32_t wid, did;
  std::mt19937 gen = GetRand();
  if (delivery && deliveryDId < 10) {
    deliveryDId++;
    wid = deliveryWId;
    did = deliveryDId;
    return new tpcc_sql::SQLDelivery(GetTimeout(), wid, did, GetRand());
  } else {
    delivery = false;
  }

  uint32_t total = new_order_ratio + delivery_ratio + payment_ratio
      + order_status_ratio + stock_level_ratio;
  uint32_t ttype = std::uniform_int_distribution<uint32_t>(0, total - 1)(gen);
  if (static_w_id) {
    wid = w_id;
  } else {
    wid = std::uniform_int_distribution<uint32_t>(1, num_warehouses)(gen);
  }
  if (ttype < new_order_ratio) {
    lastOp = "new_order";
    return new tpcc_sql::SQLNewOrder(GetTimeout(), wid, C_c_id, num_warehouses, GetRand());
  } else if (ttype < new_order_ratio + payment_ratio) {
    lastOp = "payment";
    return new tpcc_sql::SQLPayment(GetTimeout(), wid, C_c_last, C_c_id, num_warehouses, GetRand());
  } else if (ttype < new_order_ratio + payment_ratio + order_status_ratio) {
    lastOp = "order_status";
    return new tpcc_sql::SQLOrderStatus(GetTimeout(), wid, C_c_last, C_c_id, GetRand());
  } else if (ttype < new_order_ratio + payment_ratio + order_status_ratio
      + stock_level_ratio) {
    if (static_w_id) {
      did = stockLevelDId;
    } else {
      did = std::uniform_int_distribution<uint32_t>(1, 10)(gen);
    }
    lastOp = "stock_level";
    return new tpcc_sql::SQLStockLevel(GetTimeout(), wid, did, GetRand());
  } else {
    deliveryDId = 1;
    deliveryWId = wid;
    did = deliveryDId;
    delivery = true;
    lastOp = "delivery";
    return new tpcc_sql::SQLDelivery(GetTimeout(), wid, did, GetRand());
  }
}

std::string TPCCHSQLClient::GetLastOp() const {
  return lastOp;
}


} //namespace tpcc_sql
