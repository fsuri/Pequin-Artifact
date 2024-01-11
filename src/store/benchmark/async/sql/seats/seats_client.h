#ifndef SEATS_SQL_CLIENT_H
#define SEATS_SQL_CLIENT_H

#include <random>
#include <queue>
#include "store/benchmark/async/sync_transaction_bench_client.h"
#include "store/benchmark/async/sql/seats/reservation.h"

namespace seats_sql {

class SEATSSQLClient : public SyncTransactionBenchClient {
 public:
  SEATSSQLClient(SyncClient &client, Transport &transport, uint64_t id,
      uint64_t numRequests, uint64_t expDuration, uint64_t delay, uint64_t warmupSec,
      uint64_t cooldownSec, uint64_t tputInterval, uint32_t abortBackoff, bool retryAborted, 
      uint64_t maxBackoff, int64_t maxAttempts,
      uint64_t timeout, const std::string &latencyFilename="");
  virtual ~SEATSSQLClient();
  std::queue<SEATSReservation> insert_reservations;        // queue of <f_id, seat> pairs that are thought to be reservable
  std::queue<SEATSReservation> existing_reservation;    //TODO: Shouldn't this distinguish between Delete and Updates (add to them randomly)


 protected:
  virtual SyncTransaction *GetNextTransaction();
  virtual std::string GetLastOp() const;

  std::string lastOp;
  std::mt19937_64 gen;      
  uint64_t seats_id;              // need this for generating res id
  int64_t num_res_made;       // number of reservations made by client
  bool started_workload;      
  std::string last_op_;
};

} //namespace seats_sql

#endif 
