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
#include "store/benchmark/async/rw-sync/sync/rw-sync_transaction.h"
#include "store/benchmark/async/rw-sync/rw-validation-proto.pb.h"

namespace rwsync {

RWSyncTransaction::RWSyncTransaction(KeySelector *keySelector, int numOps, bool readOnly,
    std::mt19937 &rand) : SyncTransaction(10000), RWBaseTransaction(keySelector, numOps, readOnly, rand) {
}

RWSyncTransaction::~RWSyncTransaction() {
}

transaction_status_t RWSyncTransaction::Execute(SyncClient &client) {
  std::string txnState;
  SerializeTxnState(txnState);

  client.Begin(timeout, txnState);

  for (size_t op = 0; op < GetNumOps(); op++) {
    if (readOnly || op % 2 == 0) {
      std::string str;
      client.Get(GetKey(op), str, timeout);
      readValues.insert(std::make_pair(GetKey(op), str));
    }
    else {
      auto strValueItr = readValues.find(GetKey(op));
      UW_ASSERT(strValueItr != readValues.end());
      std::string strValue = strValueItr->second;
      std::string writeValue;
      if (strValue.length() == 0) {
        writeValue = std::string(100, '\0'); //make a longer string
      }
      else {
        uint64_t intValue = 0;
        for (int i = 0; i < 100; ++i) {
          intValue = intValue | (static_cast<uint64_t>(strValue[i]) << ((99 - i) * 8));
        }
        intValue++;
        for (int i = 0; i < 100; ++i) {
          writeValue += static_cast<char>((intValue >> (99 - i) * 8) & 0xFF);
        }
      }
      client.Put(GetKey(op), writeValue, timeout);
    }
  }

  return client.Commit(timeout);
}

void RWSyncTransaction::SerializeTxnState(std::string &txnState) {
  TxnState currTxnState;
  std::string txn_name;
  txn_name.append(BENCHMARK_NAME);
  currTxnState.set_txn_name(txn_name);
  txn_name.push_back('_');

  validation::proto::RWSync curr_txn;
  curr_txn.set_num_ops(GetNumOps());
  curr_txn.set_read_only(readOnly);
  for (size_t i = 0; i < GetNumOps(); ++i) {
    curr_txn.add_key_idxs(getKeyIdxs()[i]);
  }

  curr_txn.SerializeToString(currTxnState.mutable_txn_data());
  currTxnState.SerializeToString(&txnState);
}

} // namespace rwsync
