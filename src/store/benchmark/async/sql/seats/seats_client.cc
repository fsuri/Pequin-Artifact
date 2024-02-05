#include "store/benchmark/async/sql/seats/seats_client.h"
#include "store/benchmark/async/sql/seats/delete_reservation.h"
#include "store/benchmark/async/sql/seats/find_flights.h"
#include "store/benchmark/async/sql/seats/find_open_seats.h"
#include "store/benchmark/async/sql/seats/new_reservation.h" 
#include "store/benchmark/async/sql/seats/update_customer.h"
#include "store/benchmark/async/sql/seats/update_reservation.h"
#include "store/benchmark/async/sql/seats/seats_constants.h"
#include "store/benchmark/async/sql/seats/reservation.h"
#include <cmath>
#include <queue>

namespace seats_sql {

SEATSSQLClient::SEATSSQLClient(SyncClient &client, Transport &transport, uint64_t id,
      uint64_t numRequests, uint64_t expDuration, uint64_t delay, uint64_t warmupSec,
      uint64_t cooldownSec, uint64_t tputInterval, uint32_t abortBackoff, bool retryAborted, 
      uint64_t maxBackoff, int64_t maxAttempts,
      uint64_t timeout, const std::string &latencyFilename) :       
      SyncTransactionBenchClient(client, transport, id, numRequests,
        expDuration, delay, warmupSec, cooldownSec, tputInterval, abortBackoff,
        retryAborted, maxBackoff, maxAttempts, timeout, latencyFilename) {
            gen.seed(id);
            num_res_made = 0;
            seats_id = id;
            started_workload = false;
}

SEATSSQLClient::~SEATSSQLClient() {}

SyncTransaction* SEATSSQLClient::GetNextTransaction() {

  // need to populate reservations first
  std::cerr << "Select Next Transactions" << std::endl;
  if (!started_workload) {
    started_workload = true; 
    return new SQLFindOpenSeats(GetTimeout(), gen, insert_reservations);
  }

  // keep going until we get a valid operation
  while (true) {
    int64_t t_type = std::uniform_int_distribution<int64_t>(1, 100)(gen);
    int freq = 0;
    if (t_type <= (freq = FREQUENCY_DELETE_RESERVATION)) {
      std::cerr << "Try Delete_Res. Is empty? " << (delete_reservation.empty()) << std::endl; 
      if (delete_reservation.empty()) 
        continue;
      last_op_ = "delete_reservation";
      return new SQLDeleteReservation(GetTimeout(), gen, delete_reservation, insert_reservations);
    } 
    else if (t_type <= (freq += FREQUENCY_FIND_FLIGHTS)) {
      last_op_ = "find_flight";
      return new SQLFindFlights(GetTimeout(), gen);
    } 
    else if (t_type <= (freq += FREQUENCY_FIND_OPEN_SEATS)) {
      last_op_ = "find_open_seats";
      return new SQLFindOpenSeats(GetTimeout(), gen, insert_reservations);
    } 
    else if (t_type <= (freq += FREQUENCY_NEW_RESERVATION)) {
      if (insert_reservations.empty())
        continue; 
      last_op_ = "new_reservation";
      int64_t r_id = ((int64_t) seats_id | (num_res_made++) << 32);
      return new SQLNewReservation(GetTimeout(), gen, r_id, insert_reservations, update_reservation, delete_reservation);
    } 
    else if (t_type <= (freq += FREQUENCY_UPDATE_CUSTOMER)) {
      last_op_ = "update_customer";
      return new SQLUpdateCustomer(GetTimeout(), gen);
    } 
    else {
      std::cerr << "Try Update_Res. Is empty? " << (delete_reservation.empty()) << std::endl; 
      if (update_reservation.empty()){}
        continue;
      last_op_ = "update_reservation";
      return new SQLUpdateReservation(GetTimeout(), gen, update_reservation, delete_reservation);
    }
  }
}

std::string SEATSSQLClient::GetLastOp() const {
  return last_op_;
}

} 
