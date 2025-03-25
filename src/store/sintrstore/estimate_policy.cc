/***********************************************************************
 *
 * Copyright 2024 Daniel Lee <dhl93@cornell.edu>
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

#include "store/sintrstore/estimate_policy.h"
#include "store/benchmark/async/tpcc/validation/tpcc_transaction.h"
#include "store/benchmark/async/tpcc/validation/delivery.h"
#include "store/benchmark/async/tpcc/validation/new_order.h"
#include "store/benchmark/async/tpcc/validation/payment.h"
#include "store/benchmark/async/tpcc/tpcc-validation-proto.pb.h"
#include "store/benchmark/async/tpcc/tpcc_common.h"
#include "store/benchmark/async/rw-sql/rw-sql_base_transaction.h"
#include "store/benchmark/async/rw-sql/rw-sql-validation-proto.pb.h"
#include "store/benchmark/async/rw-sql/validation/rw-sql_transaction.h"
#include "store/benchmark/async/rw-sync/rw-base_transaction.h"
#include "store/benchmark/async/rw-sync/rw-validation-proto.pb.h"
#include "store/benchmark/async/rw-sync/validation/rw-val_transaction.h"
#include "store/benchmark/async/sql/tpcc/tpcc_common.h"
#include "store/benchmark/async/sql/tpcc/validation/delivery.h"
#include "store/benchmark/async/sql/tpcc/validation/new_order.h"
#include "store/benchmark/async/sql/tpcc/validation/payment.h"
#include "store/benchmark/async/sql/tpcc/tpcc-sql-validation-proto.pb.h"


namespace sintrstore {

  void EstimatePolicy::EstimateTxnPolicy(const TxnState &protoTxnState, PolicyClient *policyClient, const EndorsementClient *endorseClient) const {
    std::string txn_name(protoTxnState.txn_name());

    size_t pos = txn_name.find("_");
    if (pos == std::string::npos)
    {
      Panic("Received unexpected txn name: %s", txn_name.c_str());
    }

    std::string txn_bench = txn_name.substr(0, pos);
    std::string txn_type = txn_name.substr(pos + 1);

    if (txn_bench == ::tpcc::BENCHMARK_NAME) {
      // TODO: see if there's a better way to associate Table to policy...
      // right now hardcoded table to policy ID
      ::google::protobuf::RepeatedField<::google::protobuf::uint32> repeated_values;

      ::tpcc::TPCCTransactionType tpcc_txn_type = ::tpcc::GetBenchmarkTxnTypeEnum(txn_type);
      switch (tpcc_txn_type) {
        case ::tpcc::TXN_DELIVERY:
        {
          ::tpcc::validation::proto::Delivery valTxnData;
          UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
          repeated_values = valTxnData.est_tables();
          break;
        }
        case ::tpcc::TXN_NEW_ORDER:
        {
          ::tpcc::validation::proto::NewOrder valTxnData;
          UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
          repeated_values = valTxnData.est_tables();
          break;
        }
        case ::tpcc::TXN_PAYMENT:
        {
          ::tpcc::validation::proto::Payment valTxnData;
          UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
          repeated_values = valTxnData.est_tables();
          break;
        }
        default:
        {
          // don't change policyClient, keep policy stored as null
          break;
        }
      }
      if (repeated_values.size() > 0) {
        for (int const &value : repeated_values) {
          const Policy *temp_policy;
          UW_ASSERT(endorseClient->GetPolicyFromCache(EstimatePolicy::TableToPolicyID(value), temp_policy));
          policyClient->AddPolicy(temp_policy);
        }
      }
    }
    else if (txn_bench == ::rwsync::BENCHMARK_NAME) {
      ::rwsync::validation::proto::RWSync valTxnData;
      UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
      if (valTxnData.read_only() || valTxnData.num_ops() <= 1) {
        // txn will only have reads, so keep it as the default initialized policy
      }
      else {
        // txn will have writes
        const Policy *temp_policy;
        UW_ASSERT(endorseClient->GetPolicyFromCache(0, temp_policy));
        policyClient->AddPolicy(temp_policy);
      }
    }
    else if (txn_bench == ::tpcc_sql::BENCHMARK_NAME) {
      ::google::protobuf::RepeatedField<::google::protobuf::uint32> repeated_values;

      ::tpcc_sql::SQLTPCCTransactionType tpcc_txn_type = ::tpcc_sql::GetBenchmarkTxnTypeEnum(txn_type);
      switch (tpcc_txn_type) {
        case ::tpcc_sql::SQL_TXN_DELIVERY: {
          ::tpcc_sql::validation::proto::Delivery valTxnData;
          UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
          repeated_values = valTxnData.est_tables();
          break;
        }
        case ::tpcc_sql::SQL_TXN_DELIVERY_SEQUENTIAL: {
          ::tpcc_sql::validation::proto::Delivery valTxnData;
          UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
          repeated_values = valTxnData.est_tables();
          break;
        }
        case ::tpcc_sql::SQL_TXN_NEW_ORDER: {
          ::tpcc_sql::validation::proto::NewOrder valTxnData;
          UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
          repeated_values = valTxnData.est_tables();
          break;
        }
        case ::tpcc_sql::SQL_TXN_NEW_ORDER_SEQUENTIAL: {
          ::tpcc_sql::validation::proto::NewOrder valTxnData;
          UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
          repeated_values = valTxnData.est_tables();
          break;
        }
        case ::tpcc_sql::SQL_TXN_PAYMENT: {
          ::tpcc_sql::validation::proto::Payment valTxnData;
          UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
          repeated_values = valTxnData.est_tables();
          break;
        }
        case ::tpcc_sql::SQL_TXN_PAYMENT_SEQUENTIAL: {
          ::tpcc_sql::validation::proto::Payment valTxnData;
          UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
          repeated_values = valTxnData.est_tables();
          break;
        }
        default:
          break;
      }
      if (repeated_values.size() > 0) {
        for (int const &value : repeated_values) {
          const Policy *temp_policy;
          UW_ASSERT(endorseClient->GetPolicyFromCache(EstimatePolicy::TableToPolicyID(value), temp_policy));
          policyClient->AddPolicy(temp_policy);
        }
      }
    } else if (txn_bench == ::rwsql::BENCHMARK_NAME) {
      ::rwsql::validation::proto::RWSql valTxnData;
      UW_ASSERT(valTxnData.ParseFromString(protoTxnState.txn_data()));
      if (!valTxnData.read_only() && valTxnData.num_ops() > 1) {
        // txn will have writes
        const Policy *temp_policy;
        UW_ASSERT(endorseClient->GetPolicyFromCache(0, temp_policy));
        policyClient->AddPolicy(temp_policy);
      }
    }
    else {
      // return default policy ID (policy ID 0)
      const Policy *temp_policy;
      UW_ASSERT(endorseClient->GetPolicyFromCache(0, temp_policy));
      policyClient->AddPolicy(temp_policy);
    }
  }

  uint64_t EstimatePolicy::TableToPolicyID(const int &t) const {
    ::tpcc::Tables table = static_cast<::tpcc::Tables>(t);
    switch (table) {
    case ::tpcc::Tables::WAREHOUSE:
      return 0;
    case ::tpcc::Tables::DISTRICT:
      return 0;
    case ::tpcc::Tables::CUSTOMER:
      return 0;
    case ::tpcc::Tables::HISTORY:
      return 0;
    case ::tpcc::Tables::NEW_ORDER:
      return 0;
    case ::tpcc::Tables::ORDER:
      return 0;
    case ::tpcc::Tables::ORDER_LINE:
      return 0;
    case ::tpcc::Tables::ITEM:
      return 0;
    case ::tpcc::Tables::STOCK:
      return 0;
    case ::tpcc::Tables::ORDER_BY_CUSTOMER:
      return 0;
    case ::tpcc::Tables::EARLIEST_NEW_ORDER:
      return 0;
    default:
      Panic("Received unexpected table type: %d", t);
    }
  }

} // namespace sintrstore