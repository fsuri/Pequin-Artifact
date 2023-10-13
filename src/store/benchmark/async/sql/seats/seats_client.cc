#include "store/benchmark/async/sql/seats/seats_client.h"
#include "store/benchmark/async/sql/seats/delete_reservation.h"
#include "store/benchmark/async/sql/seats/find_flights.h"
#include "store/benchmark/async/sql/seats/find_open_seats.h"
#include "store/benchmark/async/sql/seats/new_reservation.h" 
#include "store/benchmark/async/sql/seats/update_customer.h"
#include "store/benchmark/async/sql/seats/update_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include <cmath>

namespace seats_sql {

SEATSSQLClient::SEATSSQLClient(SyncClient &client, Transport &transport, uint64_t id,
      int numRequests, int expDuration, uint64_t delay, int warmupSec,
      int cooldownSec, int tputInterval, double min_reserved_ratio, double max_reserved_ratio,
      uint32_t abortBackoff, bool retryAborted, uint32_t maxBackoff, uint32_t maxAttempts,
      uint32_t timeout, const std::string &latencyFilename = "") :       
      SyncTransactionBenchClient(client, transport, id, numRequests,
        expDuration, delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
        retryAborted, maxBackoff, maxAttempts, timeout, latencyFilename), 
        min_reserved_ratio(min_reserved_ratio), max_reserved_ratio(max_reserved_ratio) {
            gen.seed(time(nullptr));
            num_res_made = 0;
            seats_id = id;
}

SEATSSQLClient::~SEATSSQLClient() {}

SyncTransaction* SEATSSQLClient::GetNextTransaction() {
    int64_t t_type = std::uniform_int_distribution<int64_t>(1, 100)(gen);
    int freq = 0;
    if (t_type <= (freq = FREQUENCY_DELETE_RESERVATION)) {
      return new SQLDeleteReservation(GetTimeout(), gen);
    } else if (t_type <= (freq += FREQUENCY_FIND_FLIGHTS)) {
      return new SQLFindFlights(GetTimeout(), gen);
    } else if (t_type <= (freq += FREQUENCY_FIND_OPEN_SEATS)) {
      return new SQLFindOpenSeats(GetTimeout(), gen);
    } else if (t_type <= (freq += FREQUENCY_NEW_RESERVATION)) {
      return new SQLNewReservation(GetTimeout(), gen, (int64_t) (seats_id | (num_res_made++ << 32)));
    } else if (t_type <= (freq += FREQUENCY_UPDATE_CUSTOMER)) {
      return new SQLUpdateCustomer(GetTimeout(), gen);
    } else {
      return new SQLUpdateReservation(GetTimeout(), gen);
    }
}

std::string SEATSSQLClient::GetLastOp() const {
  return lastOp;
}

} 
