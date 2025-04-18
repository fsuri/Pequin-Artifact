#include "store/benchmark/async/sql/tpcch/tpcch_client.h"
#include "store/benchmark/async/sql/tpcch/tpcch_constants.h"
#include "store/benchmark/async/sql/tpcch/q1.h"
#include "store/benchmark/async/sql/tpcch/q2.h"
#include "store/benchmark/async/sql/tpcch/q3.h"
#include "store/benchmark/async/sql/tpcch/q4.h"
#include "store/benchmark/async/sql/tpcch/q5.h"
#include "store/benchmark/async/sql/tpcch/q6.h"
#include "store/benchmark/async/sql/tpcch/q7.h"
#include "store/benchmark/async/sql/tpcch/q8.h"
#include "store/benchmark/async/sql/tpcch/q9.h"
#include "store/benchmark/async/sql/tpcch/q10.h"
#include "store/benchmark/async/sql/tpcch/q11.h"
#include "store/benchmark/async/sql/tpcch/q12.h"
#include "store/benchmark/async/sql/tpcch/q13.h"
#include "store/benchmark/async/sql/tpcch/q14.h"
#include "store/benchmark/async/sql/tpcch/q15.h"
#include "store/benchmark/async/sql/tpcch/q16.h"
#include "store/benchmark/async/sql/tpcch/q17.h"
#include "store/benchmark/async/sql/tpcch/q18.h"
#include "store/benchmark/async/sql/tpcch/q19.h"
#include "store/benchmark/async/sql/tpcch/q20.h"
#include "store/benchmark/async/sql/tpcch/q21.h"
#include "store/benchmark/async/sql/tpcch/q22.h"
#include <random>

namespace tpcch_sql {

TPCCHSQLClient::TPCCHSQLClient(SyncClient &client, Transport &transport, uint64_t id,
      uint64_t numRequests, uint64_t expDuration, uint64_t delay, uint64_t warmupSec,
      uint64_t cooldownSec, uint64_t tputInterval, uint32_t abortBackoff, bool retryAborted, 
      uint64_t maxBackoff, int64_t maxAttempts,
      uint64_t timeout, const std::string &latencyFilename) :       
      SyncTransactionBenchClient(client, transport, id, numRequests,
        expDuration, delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
        retryAborted, maxBackoff, maxAttempts, timeout, latencyFilename) {
          Debug("TPCCH Client %d created", id);

  gen.seed(id + time(NULL));
}

TPCCHSQLClient::~TPCCHSQLClient() {
}

SyncTransaction* TPCCHSQLClient::ConvertTIDToTransaction(int t_id) {
  switch(t_id) {
    case 1:
      return new Q1(GetTimeout());
    case 2:
      return new Q2(GetTimeout());
    case 3:
      return new Q3(GetTimeout());
    case 4:
      return new Q4(GetTimeout());
    case 5:
      return new Q5(GetTimeout());
    case 6:
      return new Q6(GetTimeout());
    case 7:
      return new Q7(GetTimeout());
    case 8:
      return new Q8(GetTimeout());
    case 9:
      return new Q9(GetTimeout());
    case 10:
      return new Q10(GetTimeout());
    case 11:
      return new Q11(GetTimeout());
    case 12:
      return new Q12(GetTimeout());
    case 13:
      return new Q13(GetTimeout());
    case 14:
      return new Q14(GetTimeout());
    case 15:
      return new Q15(GetTimeout());
    case 16:
      return new Q16(GetTimeout());
    case 17:
      return new Q17(GetTimeout());
    case 18:
      return new Q18(GetTimeout());
    case 19:
      return new Q19(GetTimeout());
    case 20:
      return new Q20(GetTimeout());
    case 21:
      return new Q21(GetTimeout());
    case 22:
      return new Q22(GetTimeout());
    default:
      Panic("Invalid Transaction ID");
  }
}

SyncTransaction* TPCCHSQLClient::GetNextTransaction() {
  size_t t_rand = std::uniform_int_distribution<size_t>(1, 100)(gen);
  size_t weight = 0;
  for (size_t t = 0; t < DEFAULT_Q_WEIGHTS.size(); t++) {
    weight += DEFAULT_Q_WEIGHTS[t];
    if (t_rand <= weight) {
      int t_type = t + 1;
      Debug("Transaction %d", t_type);
      lastOp = "q" + std::to_string(t_type);
      return ConvertTIDToTransaction(t_type);
    }
  }
  // if somehow none of the weights work
  Debug("weighting is wrong");
  lastOp = "q22";
  return new Q22(GetTimeout());
}

std::string TPCCHSQLClient::GetLastOp() const {
  return lastOp;
}

} //namespace tpcch_sql
